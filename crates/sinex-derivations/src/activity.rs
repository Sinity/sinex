//! Hourly operator windows, cross-day sessions and elementary agency signals.
//! Focus, commands, visits and copies describe context; they do not establish
//! who generated it. Physical input requires recorded non-virtual provenance.
use crate::derived_event::{derived, record_identity};
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::clock::IntervalEnd;
use sinex_model::event_types::activity::{ActivityCounts, ActivityEvidence};
use sinex_model::{Event, Timestamp};
use std::collections::BTreeMap;

mod coverage;
pub use coverage::InputCoverage;
mod sessions;
mod signals;
mod spans;
mod windows;
pub use sessions::{ActivityProfile, ActivitySessions};
pub use spans::ActivitySpans;
pub use windows::ActivityWindows;

pub const GAP_NS: i128 = 300 * SECOND;
pub(crate) const SECOND: i128 = 1_000_000_000;
const BOUT_NS: i128 = 30 * SECOND;

const INPUTS: &[&str] = &[
    "key.pressed",
    "pointer.button",
    "pointer.moved",
    "pointer.scrolled",
    "presence.span",
    "focus.fused",
    "command.fused",
    "visit.fused",
    "clipboard.fused",
    "play.fused",
    "rest.fused",
    "state.interval",
    "journal.entry",
    "work.act",
    "input.coverage",
    "operator_log.entry",
];

pub(crate) fn timestamp(ns: i128) -> Timestamp {
    Timestamp::from_unix_ns(ns).expect("an instant within a recorded interval")
}

pub(crate) fn stable(event: &Event) -> String {
    serde_json::to_string(&(
        &event.source,
        &event.event_type,
        event
            .equivalence_key
            .as_ref()
            .map(|key| key.0.clone())
            .unwrap_or_else(|| record_identity(event)),
    ))
    .expect("record identity serializes")
}

pub(crate) fn evidence(event: &Event) -> ActivityEvidence {
    ActivityEvidence {
        event: event.id,
        record: stable(event),
        event_type: event.event_type.clone(),
        source: event.source.clone(),
    }
}

/// Unknown devices or source scopes are kept separate from known hosts.
fn scope(payload: &Value, kind: &str) -> String {
    if let Some(scope) = payload
        .get("scope")
        .or_else(|| payload.pointer("/extra/scope"))
        .and_then(Value::as_str)
        .filter(|scope| !scope.is_empty())
    {
        return scope.to_owned();
    }
    let host = [
        "/fields/host",
        "/host",
        "/hostname",
        "/machine",
        "/attributes/host",
        "/extra/host",
    ]
    .iter()
    .find_map(|path| payload.pointer(path).and_then(Value::as_str))
    .filter(|host| !host.is_empty());
    let seat = payload
        .pointer("/fields/seat")
        .or_else(|| payload.pointer("/extra/seat"))
        .or_else(|| payload.pointer("/attributes/seat"))
        .or_else(|| payload.get("seat"))
        .and_then(Value::as_str);
    if let Some(host) = host {
        return json!(["host", host, seat]).to_string();
    }
    let subject = ["bucket", "session", "record"]
        .iter()
        .find_map(|field| payload.get(*field).and_then(Value::as_str));
    json!(["unknown", kind, subject]).to_string()
}

fn interval_end(kind: &str) -> Option<IntervalEnd> {
    match kind {
        "command.fused" => Some(IntervalEnd::AfterNs("duration_ns")),
        "work.act" => Some(IntervalEnd::EndOrInstant("ended_at")),
        "input.coverage" | "presence.span" | "presence.interval" | "focus.fused" | "play.fused"
        | "rest.fused" | "state.interval" => Some(IntervalEnd::At("ended_at")),
        _ => None,
    }
}

fn geometry(kind: &str, payload: &Value, start: Timestamp) -> Option<(i128, i128)> {
    let end = interval_end(kind)
        .and_then(|end| end.at(start, payload))
        .unwrap_or(start);
    (end >= start).then_some((start.unix_ns(), end.unix_ns()))
}

fn relevant(kind: &str, payload: &Value) -> bool {
    match kind {
        "journal.entry" => payload["identifier"] == "systemd-sleep",
        "state.interval" => matches!(
            payload["state_kind"].as_str(),
            Some("desktop.lock" | "desktop.display" | "machine.suspend")
        ),
        _ => true,
    }
}

fn kinds(types: &[&'static str]) -> Vec<crate::Kind> {
    types
        .iter()
        .flat_map(|kind| match *kind {
            "journal.entry" => vec![crate::Kind::only(kind, &[("identifier", "systemd-sleep")])],
            "state.interval" => ["desktop.lock", "desktop.display", "machine.suspend"]
                .iter()
                .map(|state| crate::Kind::only(kind, &[("state_kind", state)]))
                .collect(),
            _ => vec![crate::Kind::all(kind)],
        })
        .collect()
}

fn hours(input: &KeyView<'_>, env: &Env) -> Vec<Value> {
    if !relevant(input.event_type, input.payload) {
        return vec![];
    }
    let Some(start) = input.ts_orig else {
        return vec![];
    };
    let Some((from, until)) = geometry(input.event_type, input.payload, start) else {
        return vec![];
    };
    // The adjacent partitions see connections across their edges, including
    // late evidence that changes a continuation flag.
    let mut hour = env
        .time_settings
        .hour_start(timestamp(from - GAP_NS))
        .unwrap();
    let last = until + GAP_NS;
    let mut keys = Vec::new();
    while hour.unix_ns() <= last {
        keys.push(json!({"hour": hour}));
        let Ok(next) = env.time_settings.hour_end(hour) else {
            break;
        };
        if next <= hour {
            break;
        }
        hour = next;
    }
    keys
}

fn partition(key: &Value, env: &Env) -> Option<(i128, i128)> {
    let start: Timestamp = serde_json::from_value(key.get("hour")?.clone()).ok()?;
    if env.time_settings.hour_start(start).ok()? != start {
        return None;
    }
    Some((
        start.unix_ns(),
        env.time_settings.hour_end(start).ok()?.unix_ns(),
    ))
}

fn inputs(key: &Value, env: &Env) -> Option<InputQuery> {
    let (from, until) = partition(key, env)?;
    let mut query = InputQuery::of(
        kinds(INPUTS),
        timestamp(from - GAP_NS),
        timestamp(until + GAP_NS),
    );
    query.intervals = INPUTS
        .iter()
        .filter_map(|kind| interval_end(kind).map(|end| (*kind, end)))
        .collect();
    Some(query)
}

fn key_state(key: &Value, env: &Env) -> KeyState {
    match partition(key, env) {
        Some((_, until)) if env.now.unix_ns() >= until + GAP_NS => KeyState::Closed,
        Some(_) => KeyState::Open,
        None => KeyState::Closed,
    }
}

fn counts<'a>(
    events: impl IntoIterator<Item = &'a Event>,
    from: i128,
    until: i128,
) -> ActivityCounts {
    let mut counts = ActivityCounts::default();
    let mut seen = std::collections::BTreeSet::new();
    for event in events {
        if !event
            .ts_orig
            .at
            .is_some_and(|at| from <= at.unix_ns() && at.unix_ns() < until)
            || !seen.insert(stable(event))
        {
            continue;
        }
        *counts.records.entry(event.event_type.clone()).or_default() += 1;
        *counts.sources.entry(event.source.clone()).or_default() += 1;
        if let Some(observers) = event.payload.get("observed_by").and_then(Value::as_array) {
            let observers: std::collections::BTreeSet<_> =
                observers.iter().filter_map(Value::as_str).collect();
            for observer in observers {
                *counts.observers.entry(observer.into()).or_default() += 1;
            }
        }
    }
    counts
}

fn union_ranges(mut ranges: Vec<(i128, i128)>) -> Vec<(i128, i128)> {
    ranges.sort_unstable();
    let mut union: Vec<(i128, i128)> = Vec::new();
    for (start, end) in ranges {
        if end < start {
            continue;
        }
        if let Some((_, latest)) = union.last_mut()
            && start <= *latest
        {
            *latest = (*latest).max(end);
        } else {
            union.push((start, end));
        }
    }
    union
}

pub(crate) fn union_seconds(ranges: Vec<(i128, i128)>) -> f64 {
    union_ranges(ranges)
        .into_iter()
        .map(|(start, end)| (end - start) as f64 / SECOND as f64)
        .sum()
}

#[cfg(test)]
mod tests;
