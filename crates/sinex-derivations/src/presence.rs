//! Presence from ActivityWatch's AFK watcher, checked against input. The
//! watcher needs two heartbeats inside a hard five second window and does not
//! retry, so under overnight load it misses idle transitions and reports hours
//! of `not-afk` while nobody is there. Each reported span becomes a
//! `presence.interval`, cut at logical-day boundaries:
//!
//! - `afk` is `absent`;
//! - a `not-afk` span shorter than [`THRESHOLDS`]'s `long` is `present`;
//! - a long `not-afk` span with no corroborating input is `uncertain`;
//! - a long one with input is `present`, and `sparse` when the input is
//!   thinner than one event per `dense_every`.
//!
//! The spans as reported stay as read; this is a separate judgment with its
//! evidence counted, never a silent guess.
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Kind, Neighbour};
use serde_json::{Value, json};
use sinex_model::event_types::desktop::{PresenceInterval, PresenceSpan};
use sinex_model::event_types::{CommandExecuted, CommandImported};
use sinex_model::{Event, EventPayload, Timestamp};

/// The correction's parameters, in one place.
#[derive(Clone, Copy, Debug)]
pub struct PresenceThresholds {
    /// A `not-afk` span at least this long must be corroborated by input.
    pub long: time::Duration,
    /// Input thinner than one event per this long makes a present span
    /// sparse (the documented genuine night: 12 commands in 343 minutes).
    pub dense_every: time::Duration,
    /// Event types that show a person at the machine.
    pub evidence: &'static [&'static str],
}

pub const THRESHOLDS: PresenceThresholds = PresenceThresholds {
    long: time::Duration::minutes(60),
    dense_every: time::Duration::minutes(10),
    evidence: &[
        CommandExecuted::EVENT_TYPE,
        CommandImported::EVENT_TYPE,
        "window.focused",
        "workspace.switched",
    ],
};

pub struct Presence;

fn day_of(key: &Value) -> Option<jiff::civil::Date> {
    key.get("day")?.as_str()?.parse().ok()
}

impl Keyed for Presence {
    const NAME: &'static str = "presence.intervals";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &[
        PresenceSpan::EVENT_TYPE,
        CommandExecuted::EVENT_TYPE,
        CommandImported::EVENT_TYPE,
        "window.focused",
        "workspace.switched",
    ];
    const OUTPUTS: &'static [&'static str] = &[PresenceInterval::EVENT_TYPE];

    /// Every logical day an input touches: a presence span every day it
    /// covers, an input event its own day.
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        let Some(start) = input.ts_orig else {
            return vec![];
        };
        let end = if input.event_type == PresenceSpan::EVENT_TYPE {
            input
                .payload
                .get("ended_at")
                .and_then(|v| serde_json::from_value::<Timestamp>(v.clone()).ok())
                .unwrap_or(start)
                .max(start)
        } else {
            start
        };
        let mut out = Vec::new();
        let Ok(mut day) = env.time_settings.day(start) else {
            return out;
        };
        loop {
            out.push(json!({"day": day.to_string()}));
            match env.time_settings.day_end(day) {
                Ok(next) if next < end => {}
                _ => break,
            }
            let Ok(next) = day.tomorrow() else { break };
            day = next;
        }
        out
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let day = day_of(key)?;
        let mut q = InputQuery::window(
            Self::INPUTS,
            env.time_settings.day_start(day).ok()?,
            env.time_settings.day_end(day).ok()?,
        );
        // A span that began on an earlier day and still covers this one.
        q.before.push(Neighbour {
            kinds: vec![Kind::all(PresenceSpan::EVENT_TYPE)],
            per: Some("bucket"),
            count: 1,
        });
        Some(q)
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some(day) = day_of(key) else {
            return Computed::default();
        };
        let (Ok(day_start), Ok(day_end)) = (
            env.time_settings.day_start(day),
            env.time_settings.day_end(day),
        ) else {
            return Computed::default();
        };
        let evidence: Vec<(i128, &Event)> = inputs
            .iter()
            .filter(|e| THRESHOLDS.evidence.contains(&e.event_type))
            .filter_map(|e| Some((e.ts_orig.at?.unix_ns(), e)))
            .collect();
        let mut outputs = Vec::new();
        for (span_event, span) in inputs
            .iter()
            .filter(|e| e.event_type == PresenceSpan::EVENT_TYPE)
            .filter_map(|e| Some((e, e.payload_as::<PresenceSpan>()?)))
        {
            // Cut at the day's boundaries.
            let start = span.started_at.max(day_start);
            let end = span.ended_at.min(day_end);
            if end <= start {
                continue;
            }
            let (s, e) = (start.unix_ns(), end.unix_ns());
            let inside: Vec<&Event> = evidence
                .iter()
                .filter(|(t, _)| *t >= s && *t < e)
                .map(|(_, ev)| *ev)
                .collect();
            let n = u32::try_from(inside.len()).unwrap_or(u32::MAX);
            let length = end.0 - start.0;
            let (state, sparse) = match span.status.as_str() {
                "afk" => ("absent", false),
                _ if length < THRESHOLDS.long => ("present", false),
                _ if n == 0 => ("uncertain", false),
                _ => {
                    let needed = length.as_seconds_f64() / THRESHOLDS.dense_every.as_seconds_f64();
                    ("present", f64::from(n) < needed)
                }
            };
            let payload = PresenceInterval {
                day: day.to_string(),
                started_at: start,
                ended_at: end,
                state: state.into(),
                sparse,
                reported: span.status.clone(),
                evidence: n,
            };
            outputs.push(derived(
                &payload,
                Self::NAME,
                std::iter::once(span_event).chain(inside.iter().copied()),
            ));
        }
        Computed {
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        match day_of(key).and_then(|d| env.time_settings.day_end(d).ok()) {
            Some(end) if env.now >= end => KeyState::Closed,
            Some(_) => KeyState::Open,
            None => KeyState::Closed,
        }
    }
}

#[cfg(test)]
mod tests;
