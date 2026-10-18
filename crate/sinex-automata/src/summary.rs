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
mod tests {
    use super::*;
    use std::collections::BTreeMap as Map;

    const T: i64 = 1_718_438_400; // 2024-06-15 10:00 CEST

    fn parent() -> Event {
        crate::fusion::tests::zsh("p", T as f64)
    }

    fn command(secs: i64, cwd: &str, exit: i64) -> Event {
        let p = parent();
        derived(
            &CommandFused {
                record: format!("r{secs}"),
                command: "c".into(),
                host: "box".into(),
                started_at: Timestamp::from_unix_secs(T + secs).unwrap(),
                cwd: Some(cwd.into()),
                session: None,
                duration_ns: None,
                exit_code: Some(exit),
                observed_by: vec![],
            },
            "command.fusion",
            [&p],
        )
    }

    fn interval(kind: &str, label: &str, from: i64, to: Option<i64>) -> Event {
        let p = parent();
        derived(
            &StateInterval {
                interval_id: format!("{kind}:{label}:{from}"),
                state_kind: kind.into(),
                subject_id: Some(label.into()),
                label: Some(label.into()),
                started_at: Timestamp::from_unix_secs(T + from).unwrap(),
                ended_at: to.map(|t| Timestamp::from_unix_secs(T + t).unwrap()),
                start_event_type: "window.focused".into(),
                end_event_type: None,
                attributes: Map::new(),
            },
            "state.intervals",
            [&p],
        )
    }

    fn summaries(d: &dyn crate::AnyKeyed, inputs: &[Event], now: i64) -> Vec<ActivitySummary> {
        let env = Env::at(Timestamp::from_unix_secs(T + now).unwrap());
        d.compute(&json!({"day": "2024-06-15"}), inputs, &env)
            .outputs
            .iter()
            .map(|e| e.payload_as::<ActivitySummary>().unwrap())
            .collect()
    }

    fn hour(inputs: &[Event], now: i64) -> Option<ActivitySummary> {
        summaries(&HourlySummary, inputs, now)
            .into_iter()
            .find(|s| s.period_start == Timestamp::from_unix_secs(T).unwrap())
    }

    #[test]
    fn an_hour_counts_its_commands_and_state_time_inside_it() {
        let s = hour(
            &[
                command(10, "/a", 0),
                command(20, "/a", 1),
                command(30, "/b", 0),
                command(3700, "/c", 0),
                // 20 minutes before the hour to 10 minutes into it.
                interval("desktop.focus", "vim", -1200, Some(600)),
                interval("desktop.focus", "web", 600, Some(3000)),
                interval("system.systemd.unit", "cron", 0, Some(3600)),
            ],
            100_000,
        )
        .unwrap();
        assert_eq!(s.commands, 3);
        assert_eq!(s.failed_commands, 1);
        assert_eq!(s.top_cwds[0], ("/a".into(), 2));
        assert_eq!(s.state_seconds.get("desktop.focus"), Some(&3000));
        assert!(!s.state_seconds.contains_key("system.systemd.unit"));
        assert_eq!(
            s.top_subjects[0],
            ("desktop.focus".into(), "web".into(), 2400)
        );
        assert_eq!(
            s.period_end.0.unix_timestamp() - s.period_start.0.unix_timestamp(),
            3600
        );
    }

    #[test]
    fn a_day_key_emits_only_its_busy_hours() {
        let out = summaries(
            &HourlySummary,
            &[command(10, "/a", 0), command(7200, "/a", 0)],
            100_000,
        );
        assert_eq!(out.len(), 2, "hours with nothing counted have no summary");
        assert!(
            out.iter()
                .all(|s| s.grain == "hour" && s.day == "2024-06-15")
        );
    }

    #[test]
    fn open_states_count_up_to_now() {
        let s = hour(&[interval("desktop.focus", "vim", 0, None)], 900).unwrap();
        assert_eq!(s.state_seconds.get("desktop.focus"), Some(&900));
    }

    #[test]
    fn an_old_open_state_stops_counting_at_its_day_end() {
        // Opened at 10:00 two years ago and never ended: it covers that day
        // until 04:00 the next morning, not every day until today.
        let env = Env::at(Timestamp::from_unix_secs(T + 2 * 365 * 86_400).unwrap());
        let e = interval("desktop.focus", "vim", 0, None);
        let view = KeyView {
            event_type: e.event_type,
            ts_orig: e.ts_orig.at,
            payload: &e.payload,
        };
        assert_eq!(
            DailySummary.keys_for(&view, &env),
            vec![json!({"day": "2024-06-15"})]
        );
        let s = summaries(&DailySummary, &[e], 2 * 365 * 86_400);
        assert_eq!(s[0].state_seconds.get("desktop.focus"), Some(&(18 * 3600)));
    }

    #[test]
    fn a_long_state_dirties_every_day_it_covers_and_units_none() {
        let env = Env::at(Timestamp::from_unix_secs(T + 100_000).unwrap());
        let e = interval("desktop.focus", "vim", 0, Some(2 * 86_400));
        let view = KeyView {
            event_type: e.event_type,
            ts_orig: e.ts_orig.at,
            payload: &e.payload,
        };
        assert_eq!(HourlySummary.keys_for(&view, &env).len(), 3);
        let unit = interval("system.systemd.unit", "cron", 0, Some(10));
        let view = KeyView {
            event_type: unit.event_type,
            ts_orig: unit.ts_orig.at,
            payload: &unit.payload,
        };
        assert!(HourlySummary.keys_for(&view, &env).is_empty());
    }

    #[test]
    fn a_day_asks_for_the_last_state_of_each_kind_before_it() {
        let env = Env::at(Timestamp::now());
        let q = DailySummary
            .inputs(&json!({"day": "2024-06-15"}), &env)
            .unwrap();
        assert_eq!(q.before.len(), desktop_kinds().len());
        assert!(q.before.iter().all(|n| n.count == 1));
    }

    #[test]
    fn a_day_is_the_logical_day() {
        let out = summaries(
            &DailySummary,
            &[command(0, "/a", 0), command(-7 * 3600, "/early", 0)],
            100_000,
        );
        assert_eq!(out[0].commands, 1, "03:00 belongs to the 14th");
        assert_eq!(out[0].grain, "day");
    }
}
