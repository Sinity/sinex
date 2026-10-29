//! Physical input bouts and coverage-qualified daily workload.
use crate::activity::{SECOND, stable, union_seconds};
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::event_types::portfolio::{InputBout, InputWorkload};
use sinex_model::{Event, Timestamp};

const KINDS: &[&str] = &[
    "key.pressed",
    "pointer.button",
    "pointer.moved",
    "pointer.scrolled",
    "input.coverage",
];
fn at(payload: &Value, field: &str) -> Option<Timestamp> {
    serde_json::from_value(payload.get(field)?.clone()).ok()
}
fn physical(event: &Event) -> bool {
    event
        .payload
        .pointer("/extra/layer")
        .and_then(Value::as_str)
        == Some("evdev")
        && event
            .payload
            .pointer("/extra/virtual")
            .and_then(Value::as_bool)
            == Some(false)
}
fn scope(payload: &Value, kind: &str) -> String {
    crate::activity::scope(payload, kind)
}

pub struct InputWorkloads;
impl Keyed for InputWorkloads {
    const NAME: &'static str = "input.workloads";
    const VERSION: u32 = 1;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = KINDS;
    const OUTPUTS: &'static [&'static str] = &["input.bout", "input.workload"];
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        let Some(start) = input.ts_orig else {
            return vec![];
        };
        let Ok(mut day) = env.time_settings.day(start) else {
            return vec![];
        };
        let until = at(input.payload, "ended_at").unwrap_or(start);
        let Ok(last) = env.time_settings.day(until) else {
            return vec![];
        };
        let scope = scope(input.payload, input.event_type);
        let mut keys = vec![];
        loop {
            keys.push(json!({"day":day.to_string(),"scope":scope}));
            if day >= last {
                break;
            }
            let Ok(next) = day.tomorrow() else { break };
            day = next;
        }
        keys
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let day = key["day"].as_str()?.parse().ok()?;
        let start = env.time_settings.day_start(day).ok()?;
        let end = env.time_settings.day_end(day).ok()?;
        let mut query = InputQuery::window(
            KINDS,
            Timestamp::from_unix_ns(start.unix_ns() - 30 * SECOND).ok()?,
            Timestamp::from_unix_ns(end.unix_ns() + 30 * SECOND).ok()?,
        );
        query.intervals = vec![(
            "input.coverage",
            sinex_model::clock::IntervalEnd::At("ended_at"),
        )];
        Some(query)
    }
    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        let end = key["day"]
            .as_str()
            .and_then(|day| day.parse().ok())
            .and_then(|day| env.time_settings.day_end(day).ok());
        if end.is_some_and(|end| end <= env.now) {
            KeyState::Closed
        } else {
            KeyState::Open
        }
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some(day) = key["day"].as_str() else {
            return Computed::default();
        };
        let Some(captured_scope) = key["scope"].as_str() else {
            return Computed::default();
        };
        let Ok(date) = day.parse() else {
            return Computed::default();
        };
        let (Ok(start), Ok(end)) = (
            env.time_settings.day_start(date),
            env.time_settings.day_end(date),
        ) else {
            return Computed::default();
        };
        let mut points: Vec<_> = inputs
            .iter()
            .filter(|event| {
                event.event_type != "input.coverage"
                    && physical(event)
                    && scope(&event.payload, &event.event_type) == captured_scope
            })
            .filter_map(|event| Some((event.ts_orig.at?, event)))
            .collect();
        points.sort_by_cached_key(|(at, event)| (*at, stable(event)));
        let coverage: Vec<_> = inputs
            .iter()
            .filter(|event| {
                event.event_type == "input.coverage"
                    && scope(&event.payload, &event.event_type) == captured_scope
            })
            .filter_map(|event| {
                Some((
                    event,
                    at(&event.payload, "started_at")?,
                    at(&event.payload, "ended_at")?,
                ))
            })
            .filter(|(_, from, until)| *until > start && *from < end)
            .collect();
        let mut groups: Vec<Vec<(Timestamp, &Event)>> = vec![];
        for point in points {
            if groups.last().is_none_or(|group| {
                point.0.unix_ns() - group.last().unwrap().0.unix_ns() > 30 * SECOND
            }) {
                groups.push(vec![])
            }
            groups.last_mut().unwrap().push(point);
        }
        let mut outputs = vec![];
        let mut keys = 0;
        let mut pointers = 0;
        let mut support = vec![];
        let mut typing = vec![];
        let mut parents = vec![];
        let mut bouts = 0;
        for group in groups {
            let from = group[0].0.max(start);
            let until = group.last().unwrap().0.min(end);
            let counted: Vec<_> = group
                .iter()
                .filter(|(at, _)| *at >= start && *at < end)
                .collect();
            if counted.is_empty() && until <= from {
                continue;
            }
            let key_instants: Vec<_> = counted
                .iter()
                .filter(|(_, event)| event.event_type == "key.pressed")
                .map(|(at, _)| *at)
                .collect();
            let count = key_instants.len() as u64;
            let pointer_events = counted.len() as u64 - count;
            keys += count;
            pointers += pointer_events;
            bouts += 1;
            if until > from {
                support.push((from.unix_ns(), until.unix_ns()))
            }
            for pair in key_instants.windows(2) {
                if pair[1].unix_ns() - pair[0].unix_ns() < 2 * SECOND && pair[1] > pair[0] {
                    typing.push((pair[0], pair[1]));
                }
            }
            parents.extend(group.iter().map(|(_, event)| *event));
            outputs.push(derived(
                &InputBout {
                    bout: json!([captured_scope, day, stable(group[0].1)]).to_string(),
                    scope: captured_scope.into(),
                    day: day.into(),
                    started_at: from,
                    ended_at: until,
                    key_presses: count,
                    pointer_events,
                    key_instants,
                    profile: "physical-input/v1".into(),
                },
                Self::NAME,
                group.iter().map(|(_, event)| *event),
            ));
        }
        if parents.is_empty() && coverage.is_empty() {
            return Computed::default();
        }
        let covered = union_seconds(
            coverage
                .iter()
                .map(|(_, from, until)| (from.max(&start).unix_ns(), until.min(&end).unix_ns()))
                .collect(),
        );
        let total = (end.unix_ns() - start.unix_ns()) as f64 / SECOND as f64;
        let supported_seconds = union_seconds(support);
        let typing_seconds = (keys >= 200).then(|| {
            union_seconds(
                typing
                    .iter()
                    .map(|(a, b)| (a.unix_ns(), b.unix_ns()))
                    .collect(),
            )
        });
        parents.extend(coverage.iter().map(|(event, _, _)| *event));
        outputs.push(derived(
            &InputWorkload {
                scope: captured_scope.into(),
                day: day.into(),
                period_start: start,
                period_end: end,
                key_presses: keys,
                pointer_events: pointers,
                bouts,
                supported_seconds,
                coverage_seconds: covered,
                missing_seconds: (total - covered).max(0.0),
                typing_intervals: typing,
                typing_seconds,
                estimation: if keys >= 200 {
                    "estimated"
                } else {
                    "fewer_than_200_key_presses"
                }
                .into(),
                coverage: if covered >= total {
                    "observed"
                } else if covered > 0.0 {
                    "partial"
                } else {
                    "unknown"
                }
                .into(),
                profile: "physical-input/v1".into(),
            },
            Self::NAME,
            parents,
        ));
        Computed {
            error: None,
            outputs,
            waiting: end > env.now,
        }
    }
}
