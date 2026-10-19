//! Hourly and daily activity summaries: for one civil hour or logical day,
//! the fused commands started in it, the shell sessions and desktop states
//! overlapping it, and the time each took inside the period. Every counted
//! event is a parent, so `trace` leads from a day's numbers to the commands
//! and intervals behind them.
//!
//! Both derivations are keyed by logical day; the hourly one emits that day's
//! civil hours (23 to 25 of them) in one computation. A day reads its own
//! window plus, for each desktop state kind, the latest interval that started
//! before it: states of one kind follow each other, so that is the only one
//! that can still be running when the day starts. Shell sessions never cross
//! a day start.
//!
//! Desktop states (`desktop.*`) count; systemd units do not, since a service
//! running for weeks is not activity. A state with no end yet counts up to now,
//! but never past the end of the logical day it started in: missing evidence is
//! not continued activity, and a history that simply stopped must not count
//! as one focus held until today.
use crate::output::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Neighbour};
use serde_json::{Value, json};
use sinex_model::kinds::{ActivitySummary, CommandFused, ShellSession, StateInterval};
use sinex_model::{Event, EventPayload, Timestamp};
use std::collections::BTreeMap;

const INPUTS: &[&str] = &[
    CommandFused::EVENT_TYPE,
    ShellSession::EVENT_TYPE,
    StateInterval::EVENT_TYPE,
];

/// Entries in each "top" list of a summary.
const TOP: usize = 10;

/// The desktop state kinds the interval derivation lifts.
fn desktop_kinds() -> Vec<&'static str> {
    crate::interval::RULES
        .iter()
        .map(|r| r.state_kind)
        .filter(|k| k.starts_with("desktop."))
        .collect()
}

/// Where a state with no end stops counting: now, or the end of the logical
/// day it started in, whichever is earlier.
fn open_end(start: Timestamp, env: &Env) -> Timestamp {
    let day_end = env
        .civil
        .day(start)
        .and_then(|d| env.civil.day_end(d))
        .unwrap_or(start);
    env.now.min(day_end).max(start)
}

/// A span an input covers, `None` for inputs a summary does not count.
fn span(e: &Event, env: &Env) -> Option<(i128, i128)> {
    if let Some(c) = e.payload_as::<CommandFused>() {
        let s = c.started_at.unix_ns();
        return Some((s, s));
    }
    if let Some(s) = e.payload_as::<ShellSession>() {
        return Some((s.started_at.unix_ns(), s.ended_at.unix_ns()));
    }
    if let Some(i) = e.payload_as::<StateInterval>() {
        if !i.state_kind.starts_with("desktop.") {
            return None;
        }
        let end = i
            .ended_at
            .unwrap_or_else(|| open_end(i.started_at, env))
            .max(i.started_at);
        return Some((i.started_at.unix_ns(), end.unix_ns()));
    }
    None
}

fn overlap(a: (i128, i128), b: (i128, i128)) -> i128 {
    (a.1.min(b.1) - a.0.max(b.0)).max(0)
}

fn day_of(key: &Value) -> Option<jiff::civil::Date> {
    key.get("day")?.as_str()?.parse().ok()
}

/// Every logical day an input covers.
fn days(input: &KeyView<'_>, env: &Env) -> Vec<Value> {
    let Some(start) = input.ts_orig else {
        return vec![];
    };
    let ended = |field: &str| {
        input
            .payload
            .get(field)
            .and_then(|v| serde_json::from_value::<Option<Timestamp>>(v.clone()).ok())
            .flatten()
    };
    let end = match input.event_type {
        "shell.session" => ended("ended_at").unwrap_or(start),
        "state.interval" => {
            let desktop = input
                .payload
                .get("state_kind")
                .and_then(Value::as_str)
                .is_some_and(|k| k.starts_with("desktop."));
            if !desktop {
                return vec![];
            }
            ended("ended_at")
                .unwrap_or_else(|| open_end(start, env))
                .max(start)
        }
        _ => start,
    };
    let mut out = Vec::new();
    let Ok(mut day) = env.civil.day(start) else {
        return out;
    };
    loop {
        out.push(json!({"day": day.to_string()}));
        let Ok(next_start) = env.civil.day_end(day) else {
            break;
        };
        if next_start >= end {
            break;
        }
        let Ok(next) = day.tomorrow() else { break };
        day = next;
    }
    out
}

/// What counted inside one period.
fn summarize<'a>(
    grain: &str,
    period: (Timestamp, Timestamp),
    day: &str,
    inputs: &'a [Event],
    env: &Env,
) -> Option<(ActivitySummary, Vec<&'a Event>)> {
    let p = (period.0.unix_ns(), period.1.unix_ns());
    let mut counted: Vec<&Event> = Vec::new();
    let mut commands = 0u32;
    let mut failed = 0u32;
    let mut cwds: BTreeMap<String, u32> = BTreeMap::new();
    let mut sessions = 0u32;
    let mut session_ns = 0i128;
    let mut state_ns: BTreeMap<String, i128> = BTreeMap::new();
    let mut subjects: BTreeMap<(String, String), i128> = BTreeMap::new();

    for e in inputs {
        let Some(s) = span(e, env) else { continue };
        if let Some(c) = e.payload_as::<CommandFused>() {
            if !(p.0..p.1).contains(&s.0) {
                continue;
            }
            commands += 1;
            failed += u32::from(c.exit_code.is_some_and(|x| x != 0));
            if let Some(cwd) = c.cwd {
                *cwds.entry(cwd).or_default() += 1;
            }
            counted.push(e);
        } else if e.event_type == ShellSession::EVENT_TYPE {
            let o = overlap(s, p);
            if o == 0 && !(p.0..p.1).contains(&s.0) {
                continue;
            }
            sessions += 1;
            session_ns += o;
            counted.push(e);
        } else if let Some(i) = e.payload_as::<StateInterval>() {
            let o = overlap(s, p);
            if o == 0 {
                continue;
            }
            *state_ns.entry(i.state_kind.clone()).or_default() += o;
            let label = i.label.or(i.subject_id).unwrap_or_default();
            *subjects.entry((i.state_kind, label)).or_default() += o;
            counted.push(e);
        }
    }
    if counted.is_empty() {
        return None;
    }
    let secs = |ns: i128| i64::try_from(ns / 1_000_000_000).unwrap_or(i64::MAX);
    let mut top_cwds: Vec<(String, u32)> = cwds.into_iter().collect();
    top_cwds.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    top_cwds.truncate(TOP);
    let mut top_subjects: Vec<(String, String, i64)> = subjects
        .into_iter()
        .map(|((k, l), ns)| (k, l, secs(ns)))
        .collect();
    top_subjects.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)).then(a.1.cmp(&b.1)));
    top_subjects.truncate(TOP);
    Some((
        ActivitySummary {
            grain: grain.to_owned(),
            period_start: period.0,
            period_end: period.1,
            day: day.to_owned(),
            commands,
            failed_commands: failed,
            sessions,
            session_seconds: secs(session_ns),
            top_cwds,
            state_seconds: state_ns.into_iter().map(|(k, ns)| (k, secs(ns))).collect(),
            top_subjects,
        },
        counted,
    ))
}

/// The day's window, plus the latest interval of each desktop kind before it.
fn day_query(key: &Value, env: &Env) -> Option<InputQuery> {
    let day = day_of(key)?;
    let mut q = InputQuery::window(
        INPUTS,
        env.civil.day_start(day).ok()?,
        env.civil.day_end(day).ok()?,
    );
    for kind in desktop_kinds() {
        q.before.push(Neighbour {
            event_types: vec![StateInterval::EVENT_TYPE],
            equal: vec![("state_kind", json!(kind))],
            per: None,
            count: 1,
        });
    }
    Some(q)
}

fn day_state(key: &Value, env: &Env) -> KeyState {
    match day_of(key).and_then(|d| env.civil.day_end(d).ok()) {
        Some(end) if env.now >= end => KeyState::Closed,
        Some(_) => KeyState::Open,
        None => KeyState::Closed,
    }
}

pub struct HourlySummary;
pub struct DailySummary;

impl Keyed for HourlySummary {
    const NAME: &'static str = "activity.hourly";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = INPUTS;
    const OUTPUTS: &'static [&'static str] = &[ActivitySummary::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        days(input, env)
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        day_query(key, env)
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some(day) = day_of(key) else {
            return Computed::default();
        };
        let (Ok(start), Ok(end)) = (env.civil.day_start(day), env.civil.day_end(day)) else {
            return Computed::default();
        };
        let label = day.to_string();
        let mut outputs = Vec::new();
        let mut hour = start;
        while hour < end {
            let Ok(next) = env.civil.hour_end(hour) else {
                break;
            };
            if let Some((payload, counted)) = summarize("hour", (hour, next), &label, inputs, env) {
                outputs.push(derived(&payload, Self::NAME, counted));
            }
            hour = next;
        }
        Computed {
            outputs,
            waiting: false,
        }
    }
    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        day_state(key, env)
    }
}

impl Keyed for DailySummary {
    const NAME: &'static str = "activity.daily";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = INPUTS;
    const OUTPUTS: &'static [&'static str] = &[ActivitySummary::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        days(input, env)
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        day_query(key, env)
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some(day) = day_of(key) else {
            return Computed::default();
        };
        let (Ok(start), Ok(end)) = (env.civil.day_start(day), env.civil.day_end(day)) else {
            return Computed::default();
        };
        let outputs = summarize("day", (start, end), &day.to_string(), inputs, env)
            .map(|(payload, counted)| derived(&payload, Self::NAME, counted))
            .into_iter()
            .collect();
        Computed {
            outputs,
            waiting: false,
        }
    }
    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        day_state(key, env)
    }
}

#[cfg(test)]
mod tests;
