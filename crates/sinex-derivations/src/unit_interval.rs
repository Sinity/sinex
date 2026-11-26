//! Unit runs: a systemd unit's starts and stops become `state.interval`
//! events of state `machine.unit`, one per run.
//!
//! The key is the unit's scope: host, boot, manager, user and unit name.
//! Every rule is between events of one scope: a stop closes the start of its
//! invocation, a repeated start closes the previous run as a restart, a start
//! whose invocation already stopped opens nothing. A unit event therefore
//! dirties exactly one key, and that key holds all of the unit's runs in the
//! boot. Events are not reachable by unit, so the key reads its stored
//! inputs.
//!
//! The one relation across keys is the boot fence: a later boot of the host
//! bounds a run left open, leaving its end unknown. An open run waits and
//! names its host's boots key, which a `boot.started` dirties.
//!
//! A unit record that names no host has no key and opens no interval: no
//! stop could be matched to it and no boot could fence it.
use crate::derived_event::derived;
use crate::state_interval::{scalar, time_evidence};
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Kind};
use serde_json::{Value, json};
use sinex_model::event_types::{IntervalTimeBoundary, StateInterval};
use sinex_model::record::stable;
use sinex_model::{Event, EventPayload};
use std::collections::BTreeMap;

pub const STATE: &str = "machine.unit";

pub struct UnitIntervals;

/// One unit in one boot of one host, as its events name it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeKey {
    pub host: String,
    pub boot: Value,
    pub manager: Value,
    pub uid: Value,
    pub unit: Value,
}

/// The boots of one host: dirtied by each, it wakes the host's open runs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootsKey {
    pub boots_of: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum UnitKey {
    Scope(ScopeKey),
    Boots(BootsKey),
}

fn scope_of(payload: &Value) -> Option<ScopeKey> {
    Some(ScopeKey {
        host: payload.get("host")?.as_str()?.to_owned(),
        boot: payload["boot_id"].clone(),
        manager: payload["manager"].clone(),
        uid: payload["uid"].clone(),
        unit: payload["unit"].clone(),
    })
}

/// Whether a stop belongs to the run this start opened.
fn same_invocation(start: &Event, stop: &Event) -> bool {
    match (
        start.payload["invocation_id"].as_str(),
        stop.payload["invocation_id"].as_str(),
    ) {
        (Some(a), Some(b)) => a == b,
        (None, None) => true,
        _ => false,
    }
}

fn interval(
    start: &Event,
    end: Option<&Event>,
    fence: Option<&Event>,
) -> Result<Event, sinex_model::BuildError> {
    let mut attributes = BTreeMap::new();
    for field in [
        "host",
        "boot_id",
        "manager",
        "uid",
        "unit",
        "invocation_id",
        "outcome",
        "state",
    ] {
        if let Some(value) = scalar(&start.payload, field) {
            attributes.insert(field.into(), value);
        }
    }
    let at = start.ts_orig.at.expect("folded events have a time");
    derived(
        &StateInterval {
            interval_id: format!("{STATE}:{}", stable(start)),
            state_kind: STATE.into(),
            subject_id: Some(
                json!([
                    start.payload["host"],
                    start.payload["boot_id"],
                    start.payload["manager"],
                    start.payload["uid"],
                    start.payload["unit"]
                ])
                .to_string(),
            ),
            label: start.payload["unit"].as_str().map(str::to_owned),
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
        UnitIntervals::NAME,
        [start].into_iter().chain(end).chain(fence),
    )
}

/// The runs of one scope, from its unit events and its host's boots. Returns
/// the intervals and whether the last run is open and not fenced.
fn runs(scope: &ScopeKey, inputs: &[Event]) -> (Vec<Result<Event, sinex_model::BuildError>>, bool) {
    let mut events: Vec<&Event> = inputs
        .iter()
        .filter(|event| event.ts_orig.at.is_some())
        .filter(|event| match event.event_type.as_str() {
            "boot.started" => event.payload["host"].as_str() == Some(scope.host.as_str()),
            _ => scope_of(&event.payload).as_ref() == Some(scope),
        })
        .collect();
    events.sort_by_cached_key(|event| (event.ts_orig.at, event.reading_order, stable(event)));
    let mut outputs = Vec::new();
    let mut open: Option<&Event> = None;
    for &event in &events {
        match event.event_type.as_str() {
            "boot.started" => {
                if let Some(start) = open.take_if(|start| {
                    start.ts_orig.at < event.ts_orig.at
                        && start.payload["boot_id"] != event.payload["boot_id"]
                }) {
                    outputs.push(interval(start, None, Some(event)));
                }
            }
            "unit.started" => {
                // A oneshot's exit can precede its start job's completion.
                if event.payload["invocation_id"].is_string()
                    && events.iter().any(|stop| {
                        stop.event_type == "unit.stopped"
                            && stop.ts_orig.at <= event.ts_orig.at
                            && same_invocation(event, stop)
                    })
                {
                    continue;
                }
                match open {
                    // The same run reported again.
                    Some(previous)
                        if previous.payload["invocation_id"].is_string()
                            && previous.payload["invocation_id"]
                                == event.payload["invocation_id"] => {}
                    Some(previous) => {
                        outputs.push(interval(previous, Some(event), None));
                        open = Some(event);
                    }
                    None => open = Some(event),
                }
            }
            "unit.stopped" => {
                if let Some(start) = open.take_if(|start| same_invocation(start, event)) {
                    outputs.push(interval(start, Some(event), None));
                }
            }
            _ => {}
        }
    }
    let waiting = open.is_some();
    outputs.extend(open.map(|start| interval(start, None, None)));
    (outputs, waiting)
}

impl Keyed for UnitIntervals {
    type Key = UnitKey;
    const NAME: &'static str = "unit.intervals";
    const VERSION: u32 = 2;
    const INPUTS: &'static [&'static str] = &["unit.started", "unit.stopped", "boot.started"];
    const OUTPUTS: &'static [&'static str] = &[StateInterval::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Self::Key> {
        if input.ts_orig.is_none() {
            return vec![];
        }
        match input.event_type {
            "unit.started" | "unit.stopped" => scope_of(input.payload)
                .map(UnitKey::Scope)
                .into_iter()
                .collect(),
            "boot.started" => input
                .payload
                .get("host")
                .and_then(Value::as_str)
                .map(|host| {
                    UnitKey::Boots(BootsKey {
                        boots_of: host.to_owned(),
                    })
                })
                .into_iter()
                .collect(),
            _ => vec![],
        }
    }

    fn stores_inputs(&self, key: &Self::Key) -> bool {
        matches!(key, UnitKey::Scope(_))
    }

    /// What no unit event names a scope for: the boots of its host, which
    /// fence a run left open. A host has few boots.
    fn inputs(&self, key: &Self::Key, _: &Env) -> Option<InputQuery> {
        let UnitKey::Scope(scope) = key else {
            return None;
        };
        Some(InputQuery::all(vec![Kind {
            event_type: "boot.started".into(),
            equal: vec![("host".into(), json!(scope.host))],
        }]))
    }

    fn compute(&self, key: &Self::Key, inputs: &[Event], _: &Env) -> Computed {
        match key {
            UnitKey::Scope(scope) => {
                let (outputs, waiting) = runs(scope, inputs);
                let mut computed = Computed::from_outputs(outputs, waiting);
                if computed.waiting {
                    computed.woken_by = Some(
                        serde_json::to_value(UnitKey::Boots(BootsKey {
                            boots_of: scope.host.clone(),
                        }))
                        .expect("derivation keys serialize"),
                    );
                }
                computed
            }
            UnitKey::Boots(_) => Computed::default(),
        }
    }

    fn key_state(&self, _: &Self::Key, _: &Env) -> KeyState {
        // A unit can run for the whole boot; joins read `ended_at`.
        KeyState::Open
    }
}

#[cfg(test)]
mod tests;
