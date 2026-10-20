//! Shell sessions: fused commands on one host grouped by a gap rule. Each
//! command is a recorded span (start plus duration; a command without a
//! duration is a point). A new session starts when a command begins more than
//! `GAP` after the latest end in the current session, so overlapping and nested
//! commands stay together.
//!
//! The key is the host and the logical day (operator's zone, days from the
//! configured start, 04:00 by default). A session never crosses a day start;
//! the rule says nothing about attention or real task boundaries.
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::event_types::{CommandFused, ShellSession};
use sinex_model::{Event, EventPayload, Timestamp};

/// Gap between one command's end and the next command's start that ends a
/// session.
pub const GAP_NS: i128 = 300 * 1_000_000_000;

pub struct ShellSessions;

fn key_parts(key: &Value) -> Option<(String, jiff::civil::Date)> {
    let host = key.get("host")?.as_str()?.to_owned();
    let day = key.get("day")?.as_str()?.parse().ok()?;
    Some((host, day))
}

impl Keyed for ShellSessions {
    const NAME: &'static str = "shell.sessions";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &[CommandFused::EVENT_TYPE];
    const OUTPUTS: &'static [&'static str] = &[ShellSession::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        let (Some(host), Some(t)) = (
            input.payload.get("host").and_then(Value::as_str),
            input.ts_orig,
        ) else {
            return vec![];
        };
        match env.civil.day(t) {
            Ok(day) => vec![json!({"host": host, "day": day.to_string()})],
            Err(_) => vec![],
        }
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let (host, day) = key_parts(key)?;
        let mut q = InputQuery::window(
            Self::INPUTS,
            env.civil.day_start(day).ok()?,
            env.civil.day_end(day).ok()?,
        );
        q.equal.push(("host", Value::String(host)));
        Some(q)
    }

    fn compute(&self, key: &Value, inputs: &[Event], _env: &Env) -> Computed {
        let Some((host, day)) = key_parts(key) else {
            return Computed::default();
        };
        let mut commands: Vec<(&Event, CommandFused)> = inputs
            .iter()
            .filter_map(|e| e.payload_as::<CommandFused>().map(|c| (e, c)))
            .filter(|(_, c)| c.host == host)
            .collect();
        commands.sort_by_key(|(e, c)| (c.started_at, e.id));

        let mut groups: Vec<Vec<(&Event, CommandFused)>> = Vec::new();
        let mut latest_end: i128 = i128::MIN;
        for (e, c) in commands {
            let start = c.started_at.unix_ns();
            let end = start + i128::from(c.duration_ns.unwrap_or(0).max(0));
            if groups.is_empty() || start - latest_end > GAP_NS {
                groups.push(Vec::new());
                latest_end = end;
            } else {
                latest_end = latest_end.max(end);
            }
            groups.last_mut().expect("pushed above").push((e, c));
        }

        let outputs = groups
            .iter()
            .map(|g| {
                let started_at = g[0].1.started_at;
                let ended_ns = g
                    .iter()
                    .map(|(_, c)| {
                        c.started_at.unix_ns() + i128::from(c.duration_ns.unwrap_or(0).max(0))
                    })
                    .max()
                    .unwrap_or_else(|| started_at.unix_ns());
                let mut cwds: Vec<String> = Vec::new();
                for (_, c) in g {
                    if let Some(cwd) = &c.cwd
                        && !cwds.contains(cwd)
                    {
                        cwds.push(cwd.clone());
                    }
                }
                let payload = ShellSession {
                    host: host.clone(),
                    started_at,
                    ended_at: Timestamp::from_unix_ns(ended_ns).unwrap_or(started_at),
                    day: day.to_string(),
                    commands: u32::try_from(g.len()).unwrap_or(u32::MAX),
                    failed: u32::try_from(
                        g.iter()
                            .filter(|(_, c)| c.exit_code.is_some_and(|x| x != 0))
                            .count(),
                    )
                    .unwrap_or(u32::MAX),
                    cwds,
                };
                derived(&payload, Self::NAME, g.iter().map(|(e, _)| *e))
            })
            .collect();
        Computed {
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        match key_parts(key).and_then(|(_, day)| env.civil.day_end(day).ok()) {
            Some(end) if env.now.unix_ns() >= end.unix_ns() + GAP_NS => KeyState::Closed,
            Some(_) => KeyState::Open,
            None => KeyState::Closed,
        }
    }
}

#[cfg(test)]
mod tests;
