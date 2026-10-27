//! Activity spans: the attention stream. The operator's own activity, as the
//! fused kinds record it whichever sources saw it (commands with their
//! duration, window focus with its end, page visits and copies as moments),
//! is joined into spans while each piece starts no more than [`GAP_NS`] after
//! the latest end so far; then the stretches presence calls `absent` are cut
//! out. A span names every record it covers as a parent and counts them by
//! type.
//!
//! The key is the logical day (operator's zone, days from the configured
//! start); a span never crosses a day start. The latest focus before the day
//! is read too, as are commands and absences that still overlap the day.
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::clock::IntervalEnd;
use sinex_model::event_types::desktop::PresenceInterval;
use sinex_model::event_types::fused::{ClipboardFused, FocusFused, VisitFused};
use sinex_model::event_types::{ActivitySpan, CommandFused};
use sinex_model::{Event, EventPayload, Timestamp};
use std::collections::BTreeMap;

/// The longest pause inside one span.
pub const GAP_NS: i128 = 300 * 1_000_000_000;

pub struct ActivitySpans;

/// What one input says: a stretch of activity, or a stretch nobody was there.
enum Piece {
    Active { start: i128, end: i128 },
    Absent { start: i128, end: i128 },
}

fn piece(e: &Event) -> Option<Piece> {
    let ns = |t: Timestamp| t.unix_ns();
    Some(match e.event_type {
        t if t == CommandFused::EVENT_TYPE => {
            let c: CommandFused = e.payload_as()?;
            let start = ns(c.started_at);
            Piece::Active {
                start,
                end: start + i128::from(c.duration_ns.unwrap_or(0).max(0)),
            }
        }
        t if t == FocusFused::EVENT_TYPE => {
            let f: FocusFused = e.payload_as()?;
            let start = ns(f.started_at);
            Piece::Active {
                start,
                end: f.ended_at.map_or(start, ns).max(start),
            }
        }
        t if t == VisitFused::EVENT_TYPE => {
            let at = ns(e.payload_as::<VisitFused>()?.visited_at);
            Piece::Active { start: at, end: at }
        }
        t if t == ClipboardFused::EVENT_TYPE => {
            let at = ns(e.payload_as::<ClipboardFused>()?.copied_at);
            Piece::Active { start: at, end: at }
        }
        t if t == PresenceInterval::EVENT_TYPE => {
            let p: PresenceInterval = e.payload_as()?;
            if p.state != "absent" {
                return None;
            }
            Piece::Absent {
                start: ns(p.started_at),
                end: ns(p.ended_at),
            }
        }
        _ => return None,
    })
}

fn day_of(key: &Value) -> Option<jiff::civil::Date> {
    key.get("day")?.as_str()?.parse().ok()
}

/// `spans` with every `absent` stretch taken out; both sorted by start. A
/// moment (a span that starts where it ends) inside an absent stretch goes.
fn cut(spans: Vec<(i128, i128)>, absent: &[(i128, i128)]) -> Vec<(i128, i128)> {
    let mut out = Vec::new();
    for (start, end) in spans {
        let mut from = start;
        let mut gone = false;
        for &(a, b) in absent {
            if b <= from || a > end {
                continue;
            }
            if a > from {
                out.push((from, a));
            }
            from = from.max(b);
            if from > end {
                gone = true;
                break;
            }
        }
        if !gone && from <= end {
            out.push((from, end));
        }
    }
    out
}

impl Keyed for ActivitySpans {
    const NAME: &'static str = "activity.spans";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &[
        CommandFused::EVENT_TYPE,
        FocusFused::EVENT_TYPE,
        VisitFused::EVENT_TYPE,
        ClipboardFused::EVENT_TYPE,
        PresenceInterval::EVENT_TYPE,
    ];
    const OUTPUTS: &'static [&'static str] = &[ActivitySpan::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        let Some(start) = input.ts_orig else {
            return vec![];
        };
        let end = match input.event_type {
            CommandFused::EVENT_TYPE => IntervalEnd::AfterNs("duration_ns"),
            FocusFused::EVENT_TYPE | PresenceInterval::EVENT_TYPE => IntervalEnd::At("ended_at"),
            _ => IntervalEnd::AfterNs("duration_ns"),
        }
        .at(start, input.payload)
        .unwrap_or(start)
        .max(start);
        let mut out = Vec::new();
        let (Ok(first), Ok(last)) = (env.time_settings.day(start), env.time_settings.day(end))
        else {
            return out;
        };
        let mut day = first;
        while day <= last {
            out.push(json!({ "day": day.to_string() }));
            match day.tomorrow() {
                Ok(next) => day = next,
                Err(_) => break,
            }
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
        q.intervals = vec![
            (
                CommandFused::EVENT_TYPE,
                IntervalEnd::AfterNs("duration_ns"),
            ),
            (FocusFused::EVENT_TYPE, IntervalEnd::At("ended_at")),
            (PresenceInterval::EVENT_TYPE, IntervalEnd::At("ended_at")),
        ];
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
        let (from, until) = (day_start.unix_ns(), day_end.unix_ns());
        let mut active: Vec<(i128, i128, &Event)> = Vec::new();
        let mut absent: Vec<(i128, i128)> = Vec::new();
        let mut absence_events: Vec<(i128, i128, &Event)> = Vec::new();
        for e in inputs {
            match piece(e) {
                Some(Piece::Active { start, end }) if end >= from && start < until => {
                    active.push((start.max(from), end.min(until), e));
                }
                Some(Piece::Absent { start, end }) => {
                    absent.push((start, end));
                    absence_events.push((start, end, e));
                }
                _ => {}
            }
        }
        active.sort_by_key(|(start, end, e)| {
            (*start, *end, crate::derived_event::record_identity(e))
        });
        absent.sort_unstable();

        // Join pieces while each starts within the gap of the latest end.
        let mut joined: Vec<(i128, i128)> = Vec::new();
        for &(start, end, _) in &active {
            match joined.last_mut() {
                Some((_, latest)) if start - *latest <= GAP_NS => *latest = (*latest).max(end),
                _ => joined.push((start, end)),
            }
        }
        let spans = cut(joined, &absent);

        let mut open = false;
        let outputs = spans
            .iter()
            .filter_map(|&(start, end)| {
                let members: Vec<&(i128, i128, &Event)> = active
                    .iter()
                    .filter(|(s, e, _)| {
                        if s == e {
                            *s >= start
                                && *s <= end
                                && !absent.iter().any(|(a, b)| *a <= *s && *s < *b)
                        } else {
                            *s < end && *e > start
                        }
                    })
                    .collect();
                if members.is_empty() {
                    return None;
                }
                let mut records: BTreeMap<String, u64> = BTreeMap::new();
                let mut apps: Vec<String> = Vec::new();
                for (_, _, e) in &members {
                    *records.entry(e.event_type.to_owned()).or_default() += 1;
                    if let Some(f) = e.payload_as::<FocusFused>() {
                        if f.ended_at.is_none() {
                            open = true;
                        }
                        if let Some(app) = f.app
                            && !apps.contains(&app)
                        {
                            apps.push(app);
                        }
                    }
                }
                let payload = ActivitySpan {
                    day: day.to_string(),
                    started_at: Timestamp::from_unix_ns(start).ok()?,
                    ended_at: Timestamp::from_unix_ns(end).ok()?,
                    records,
                    apps,
                };
                Some(derived(
                    &payload,
                    Self::NAME,
                    members.iter().map(|(_, _, e)| *e).chain(
                        absence_events
                            .iter()
                            .filter(|(a, b, _)| *a <= end && *b >= start)
                            .map(|(_, _, e)| *e),
                    ),
                ))
            })
            .collect();
        Computed {
            outputs,
            // A focus still held may end later, outside this day's inputs.
            waiting: open,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        match day_of(key).and_then(|day| env.time_settings.day_end(day).ok()) {
            Some(end) if env.now.unix_ns() >= end.unix_ns() + GAP_NS => KeyState::Closed,
            Some(_) => KeyState::Open,
            None => KeyState::Closed,
        }
    }
}

#[cfg(test)]
mod tests;
