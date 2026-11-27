//! Captured machine transitions (sleep, boots) share the state-interval
//! product and admission path. Unit runs are [`crate::unit_interval`].
//!
//! A boot is fenced and a sleep ended by a later record of the same host. A
//! record that names no host opens no interval: nothing could be matched to
//! it.
use super::*;
use sinex_model::record::stable;
const SLEEP: &[&str] = &["sleep.started", "sleep.ended", "boot.started"];

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineKey {
    pub state: String,
    pub day: jiff::civil::Date,
    pub host: Option<String>,
    /// Always null in a key made now. It is part of the key's stored form;
    /// earlier versions keyed a record that named no host by it.
    pub unknown_record: Option<String>,
}

pub(super) fn keys(input: &KeyView<'_>, env: &Env) -> Vec<MachineKey> {
    let states: &[&str] = match input.event_type {
        "boot.started" => &["machine.boot", "machine.suspend"],
        "sleep.started" | "sleep.ended" => &["machine.suspend"],
        _ => return vec![],
    };
    let Some(at) = input.ts_orig else {
        return vec![];
    };
    let Ok(day) = env.time_settings.day(at) else {
        return vec![];
    };
    let Some(host) = input.payload.get("host").and_then(Value::as_str) else {
        return vec![];
    };
    states
        .iter()
        .map(|state| MachineKey {
            state: (*state).into(),
            day,
            host: Some(host.to_owned()),
            unknown_record: None,
        })
        .collect()
}
pub(super) fn inputs(key: &MachineKey, env: &Env) -> Option<InputQuery> {
    let host = key.host.as_deref()?;
    let day = key.day;
    let start = env.time_settings.day_start(day).ok()?;
    let end = env.time_settings.day_end(day).ok()?;
    let kinds = match key.state.as_str() {
        "machine.suspend" => SLEEP,
        "machine.boot" => &["boot.started"],
        _ => return None,
    };
    let mut query = InputQuery::window(kinds, start, end).only("host", json!(host));
    query.range = Some(InputRange::UntilNext {
        start,
        after: Some(end),
        kinds: vec![Kind {
            event_type: "boot.started".into(),
            equal: vec![("host".into(), json!(host))],
        }],
        field: "started_at",
    });
    query.after.push(Neighbour {
        kinds: vec![Kind {
            event_type: "boot.started".into(),
            equal: vec![("host".into(), json!(host))],
        }],
        per: None,
        count: 1,
        lifetime: None,
    });
    query.before.push(Neighbour {
        kinds: kinds
            .iter()
            .map(|kind| Kind {
                event_type: (*kind).into(),
                equal: vec![("host".into(), json!(host))],
            })
            .collect(),
        per: None,
        count: 1,
        lifetime: None,
    });
    Some(query)
}
fn interval(
    start: &Event,
    end: Option<&Event>,
    fence: Option<&Event>,
    state: &str,
) -> (Timestamp, Result<Event, sinex_model::BuildError>) {
    let mut attributes = BTreeMap::new();
    for field in ["host", "boot_id", "state"] {
        if let Some(value) = scalar(&start.payload, field) {
            attributes.insert(field.into(), value);
        }
    }
    let subject = start.payload["host"].as_str().map(str::to_owned);
    let label = if state == "machine.boot" {
        Some("booted".into())
    } else {
        Some("suspended".into())
    };
    let at = start.ts_orig.at.unwrap();
    (
        at,
        derived(
            &StateInterval {
                interval_id: format!("{state}:{}", stable(start)),
                state_kind: state.into(),
                subject_id: subject,
                label,
                started_at: at,
                ended_at: end.and_then(|event| event.ts_orig.at),
                started_at_evidence: Some(time_evidence(start, IntervalTimeBoundary::Transition)),
                ended_at_evidence: end
                    .filter(|event| event.ts_orig.at.is_some())
                    .map(|event| time_evidence(event, IntervalTimeBoundary::Transition)),
                fenced_at: fence.and_then(|event| event.ts_orig.at),
                start_event_type: start.event_type.clone(),
                end_event_type: end.or(fence).map(|event| event.event_type.clone()),
                attributes,
            },
            StateIntervals::NAME,
            [start].into_iter().chain(end).chain(fence),
        ),
    )
}

pub(super) fn compute(key: &MachineKey, inputs: &[Event], env: &Env) -> Computed {
    let state = key.state.as_str();
    let day = key.day;
    let (Some(host), Ok(from), Ok(until)) = (
        key.host.as_deref(),
        env.time_settings.day_start(day),
        env.time_settings.day_end(day),
    ) else {
        return Computed::default();
    };
    let mut events: Vec<_> = inputs
        .iter()
        .filter(|event| {
            event.ts_orig.at.is_some()
                && event.payload.get("host").and_then(Value::as_str) == Some(host)
        })
        .collect();
    events.sort_by_cached_key(|event| (event.ts_orig.at, event.reading_order, stable(event)));
    let mut outputs = vec![];
    let (begin, finish) = if state == "machine.boot" {
        ("boot.started", "boot.started")
    } else {
        ("sleep.started", "sleep.ended")
    };
    let mut open: Option<&Event> = None;
    for event in &events {
        if event.event_type == "boot.started"
            && let Some(previous) = open.take()
        {
            if previous.ts_orig.at < event.ts_orig.at {
                outputs.push(interval(previous, None, Some(event), state));
            }
        }
        if event.event_type == begin {
            if let Some(previous) = open.replace(event) {
                outputs.push(interval(previous, Some(event), None, state));
            }
        } else if event.event_type == finish
            && let Some(previous) = open.take()
        {
            if previous.payload["boot_id"] == event.payload["boot_id"] {
                outputs.push(interval(previous, Some(event), None, state));
            } else {
                open = Some(previous);
            }
        }
    }
    outputs.extend(open.map(|start| interval(start, None, None, state)));
    // Neighbour intervals outside this key do not participate in its result,
    // including a build failure for one of those discarded intervals.
    outputs.retain(|(at, _)| *at >= from && *at < until);
    let mut computed = Computed::from_outputs(outputs.into_iter().map(|(_, event)| event), false);
    computed.waiting = computed.outputs.iter().any(|event| {
        event.payload["ended_at"].is_null()
            && event.payload.get("fenced_at").is_none_or(Value::is_null)
    });
    computed
}
