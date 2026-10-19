//! Presence from ActivityWatch's AFK watcher, checked against input
//! (`y0o3.13`, defect 2). The watcher needs two heartbeats inside a hard five
//! second window and does not retry, so under overnight load it misses idle
//! transitions and reports hours of `not-afk` while nobody is there. Each
//! reported span becomes a `presence.interval`, cut at logical-day boundaries:
//!
//! - `afk` is `absent`;
//! - a `not-afk` span shorter than [`SPEC`]'s `long` is `present`;
//! - a long `not-afk` span with no corroborating input is `uncertain`;
//! - a long one with input is `present`, and `sparse` when the input is
//!   thinner than one event per `dense_every`.
//!
//! The spans as reported stay as read; this is a separate judgment with its
//! evidence counted, never a silent guess.
use crate::output::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Neighbour};
use serde_json::{Value, json};
use sinex_model::kinds::desktop::{PresenceInterval, PresenceSpan};
use sinex_model::kinds::{CommandExecuted, CommandImported};
use sinex_model::{Event, EventPayload, Timestamp};

/// The correction's parameters, in one place (`y0o3.13` acceptance 4).
#[derive(Clone, Copy, Debug)]
pub struct PresenceSpec {
    /// A `not-afk` span at least this long must be corroborated by input.
    pub long: time::Duration,
    /// Input thinner than one event per this long makes a present span
    /// sparse (the documented genuine night: 12 commands in 343 minutes).
    pub dense_every: time::Duration,
    /// Event types that show a person at the machine.
    pub evidence: &'static [&'static str],
}

pub const SPEC: PresenceSpec = PresenceSpec {
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
        let Ok(mut day) = env.civil.day(start) else {
            return out;
        };
        loop {
            out.push(json!({"day": day.to_string()}));
            match env.civil.day_end(day) {
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
            env.civil.day_start(day).ok()?,
            env.civil.day_end(day).ok()?,
        );
        // A span that began on an earlier day and still covers this one.
        q.before.push(Neighbour {
            event_types: vec![PresenceSpan::EVENT_TYPE],
            equal: vec![],
            per: Some("bucket"),
            count: 1,
        });
        Some(q)
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some(day) = day_of(key) else {
            return Computed::default();
        };
        let (Ok(day_start), Ok(day_end)) = (env.civil.day_start(day), env.civil.day_end(day))
        else {
            return Computed::default();
        };
        let evidence: Vec<(i128, &Event)> = inputs
            .iter()
            .filter(|e| SPEC.evidence.contains(&e.event_type))
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
                _ if length < SPEC.long => ("present", false),
                _ if n == 0 => ("uncertain", false),
                _ => {
                    let needed = length.as_seconds_f64() / SPEC.dense_every.as_seconds_f64();
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
        match day_of(key).and_then(|d| env.civil.day_end(d).ok()) {
            Some(end) if env.now >= end => KeyState::Closed,
            Some(_) => KeyState::Open,
            None => KeyState::Closed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

    // 2026-08-22 is the documented artifact night: 01:52-03:42 CEST.
    fn at(hhmm: &str, day: &str) -> Timestamp {
        let s = format!("{day}T{hhmm}:00+02:00");
        Timestamp(
            time::OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339)
                .unwrap(),
        )
    }

    fn located() -> Origin {
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
        }
    }

    fn span(row: i64, start: Timestamp, end: Timestamp, status: &str) -> Event {
        Event::build(
            &PresenceSpan {
                bucket: "aw-watcher-afk".into(),
                row,
                started_at: start,
                ended_at: end,
                status: status.into(),
            },
            "desktop.activitywatch",
            located(),
            ReadingOrder(row),
            *blake3::hash(&row.to_le_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap()
    }

    fn command(at: Timestamp, n: u64) -> Event {
        Event::build(
            &CommandImported {
                command: format!("c{n}"),
                hostname: "box".into(),
                started_at: at,
                header_elapsed: 0,
            },
            "terminal.zsh",
            located(),
            ReadingOrder(0),
            *blake3::hash(&n.to_le_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap()
    }

    fn env() -> Env {
        Env::at(at("12:00", "2026-10-01"))
    }

    /// The intervals of the logical day `of` falls in.
    fn intervals(of: Timestamp, inputs: &[Event]) -> Vec<PresenceInterval> {
        let day = env().civil.day(of).unwrap().to_string();
        let mut sorted = inputs.to_vec();
        sorted.sort_by_key(|e| e.ts_orig.at);
        Presence
            .compute(&json!({ "day": day }), &sorted, &env())
            .outputs
            .iter()
            .map(|e| e.payload_as().unwrap())
            .collect()
    }

    #[test]
    fn a_long_not_afk_night_with_no_input_is_uncertain() {
        let d = "2026-08-22";
        let out = intervals(
            at("01:52", d),
            &[span(1, at("01:52", d), at("03:42", d), "not-afk")],
        );
        assert_eq!(out.len(), 1);
        assert_eq!((out[0].state.as_str(), out[0].evidence), ("uncertain", 0));
    }

    #[test]
    fn a_long_night_with_a_few_commands_is_present_and_sparse() {
        let d = "2026-08-20";
        // 343 minutes with 12 real commands.
        // 20:00 to 01:43: inside one logical day (days start at 04:00).
        let start = at("20:00", d);
        let mut inputs = vec![span(
            1,
            start,
            Timestamp(start.0 + time::Duration::minutes(343)),
            "not-afk",
        )];
        for n in 0..12 {
            inputs.push(command(
                Timestamp(start.0 + time::Duration::minutes(10 + 27 * n as i64)),
                n,
            ));
        }
        let out = intervals(start, &inputs);
        assert_eq!(out.len(), 1);
        assert_eq!(
            (out[0].state.as_str(), out[0].sparse, out[0].evidence),
            ("present", true, 12)
        );
    }

    #[test]
    fn short_spans_are_taken_as_reported_and_afk_is_absent() {
        let d = "2026-08-21";
        let out = intervals(
            at("10:00", d),
            &[
                span(1, at("10:00", d), at("10:20", d), "not-afk"),
                span(2, at("10:20", d), at("12:00", d), "afk"),
            ],
        );
        let states: Vec<&str> = out.iter().map(|i| i.state.as_str()).collect();
        assert_eq!(states, ["present", "absent"]);
    }

    #[test]
    fn a_span_is_cut_at_logical_day_boundaries() {
        // 02:00 on the 22nd to 06:00 on the 23rd crosses two day starts.
        let s = span(
            1,
            at("02:00", "2026-08-22"),
            at("06:00", "2026-08-23"),
            "not-afk",
        );
        let view = KeyView {
            event_type: s.event_type,
            ts_orig: s.ts_orig.at,
            payload: &s.payload,
        };
        assert_eq!(Presence.keys_for(&view, &env()).len(), 3);
        let middle = intervals(at("12:00", "2026-08-22"), &[s]);
        assert_eq!(middle.len(), 1);
        let day = env().civil.day(at("12:00", "2026-08-22")).unwrap();
        assert_eq!(middle[0].started_at, env().civil.day_start(day).unwrap());
        assert_eq!(middle[0].ended_at, env().civil.day_end(day).unwrap());
    }
}
