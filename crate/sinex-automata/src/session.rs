//! Shell sessions: fused commands on one host grouped by a gap rule. Each
//! command is a recorded span (start plus duration; a command without a
//! duration is a point). A new session starts when a command begins more than
//! `GAP` after the latest end in the current session, so overlapping and nested
//! commands stay together.
//!
//! The key is the host and the logical day (operator's zone, days from the
//! configured start, 04:00 by default). A session never crosses a day start;
//! the rule says nothing about attention or real task boundaries.
use crate::output::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::kinds::{CommandFused, ShellSession};
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
mod tests {
    use super::*;
    use sinex_model::kinds::CommandFused;

    fn fused(secs: i64, duration_s: Option<i64>, exit: i64) -> Event {
        let parent = crate::fusion::tests::zsh("x", secs as f64);
        derived(
            &CommandFused {
                command: format!("c{secs}"),
                host: "box".into(),
                started_at: Timestamp::from_unix_secs(secs).unwrap(),
                cwd: Some(format!("/p{}", secs % 2)),
                session: None,
                duration_ns: duration_s.map(|d| d * 1_000_000_000),
                exit_code: Some(exit),
                observed_by: vec!["terminal.zsh".into()],
            },
            "command.fusion",
            [&parent],
        )
    }

    // 2024-06-15 10:00 CEST.
    const T: i64 = 1_718_438_400;

    fn sessions(inputs: &[Event]) -> Vec<ShellSession> {
        let env = Env::at(Timestamp::from_unix_secs(T + 86_400 * 3).unwrap());
        let key = json!({"host": "box", "day": "2024-06-15"});
        ShellSessions
            .compute(&key, inputs, &env)
            .outputs
            .iter()
            .map(|e| e.payload_as::<ShellSession>().unwrap())
            .collect()
    }

    #[test]
    fn gap_after_the_latest_end_splits_sessions() {
        let s = sessions(&[
            fused(T, Some(10), 0),
            fused(T + 200, None, 1),
            // A long build ends at T+1300; a command 299 s later stays.
            fused(T + 300, Some(1000), 0),
            fused(T + 1599, None, 0),
            fused(T + 1900, None, 0),
        ]);
        assert_eq!(s.len(), 2, "{s:?}");
        assert_eq!(s[0].commands, 4);
        assert_eq!(s[0].failed, 1);
        assert_eq!(s[0].ended_at, Timestamp::from_unix_secs(T + 1599).unwrap());
        assert_eq!(s[0].cwds, vec!["/p0", "/p1"]);
        assert_eq!(s[1].commands, 1);
        assert_eq!(s[1].day, "2024-06-15");
    }

    #[test]
    fn key_is_host_and_logical_day() {
        let env = Env::at(Timestamp::now());
        let e = fused(T, None, 0);
        let keys = ShellSessions.keys_for(
            &KeyView {
                event_type: e.event_type,
                ts_orig: e.ts_orig.at,
                payload: &e.payload,
            },
            &env,
        );
        assert_eq!(keys, vec![json!({"host": "box", "day": "2024-06-15"})]);
        // 03:30 local belongs to the previous logical day.
        let early = fused(T - 6 * 3600 - 1800, None, 0);
        let keys = ShellSessions.keys_for(
            &KeyView {
                event_type: early.event_type,
                ts_orig: early.ts_orig.at,
                payload: &early.payload,
            },
            &env,
        );
        assert_eq!(keys[0]["day"], "2024-06-14");
    }

    #[test]
    fn every_member_is_a_parent() {
        let env = Env::at(Timestamp::now());
        let inputs = [fused(T, None, 0), fused(T + 10, None, 0)];
        let out =
            ShellSessions.compute(&json!({"host": "box", "day": "2024-06-15"}), &inputs, &env);
        match &out.outputs[0].origin {
            sinex_model::Origin::Derived { parents } => {
                assert_eq!(parents, &vec![inputs[0].id, inputs[1].id]);
            }
            other => panic!("{other:?}"),
        }
    }
}
