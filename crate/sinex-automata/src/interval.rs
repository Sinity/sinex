//! Interval lift: transitions and observed spans become `state.interval`
//! events (ported from the predecessor's `automata/interval_lift.rs`). One rule
//! per state kind, as data; a new point, transition or span source adds a
//! rule, never a new derivation.
//!
//! Three rule shapes:
//!
//! - **adjacent transitions** (window focus, workspace): a state holds until
//!   the next transition to a different subject, or until a fence;
//! - **spans** (ActivityWatch window and presence spans, already joined at the
//!   source): each span is one interval, cut short by a fence inside it;
//! - **start/stop pairs** (systemd units): a start opens a subject, the matching
//!   stop closes it; a second start closes the first at its own time.
//!
//! Fences end desktop-attention states at their own time: a suspend (systemd
//! sleep and suspend units starting) ends focus, workspace and window spans;
//! going AFK ends focus and workspace.
//!
//! The key is the rule and the logical day. A key emits the intervals that
//! start inside its day; it reads the earliest transitions after the day to end
//! them. An interval still open at the end of the evidence has no end yet and
//! the key is recomputed whenever the derivation runs again. Interval identity
//! is the start evidence's record location ("ends move, starts do not", `y8v`).
use crate::output::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Neighbour};
use serde_json::{Value, json};
use sinex_model::kinds::StateInterval;
use sinex_model::{Event, EventPayload, Origin, Timestamp};
use std::collections::BTreeMap;

pub const SUSPEND_UNITS: &[&str] = &[
    "sleep.target",
    "suspend.target",
    "systemd-suspend.service",
    "hibernate.target",
    "systemd-hibernate.service",
    "hybrid-sleep.target",
    "systemd-hybrid-sleep.service",
    "suspend-then-hibernate.target",
    "systemd-suspend-then-hibernate.service",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fence {
    /// The machine suspends or hibernates.
    Suspend,
    /// The operator went AFK.
    Afk,
}

impl Fence {
    /// The neighbour query that finds the next fence of this kind.
    fn neighbour(self) -> Neighbour {
        match self {
            Self::Suspend => Neighbour {
                event_types: vec!["unit.started"],
                equal: vec![("unit_name", json!(SUSPEND_UNITS))],
                per: None,
                count: 1,
            },
            Self::Afk => Neighbour {
                event_types: vec!["presence.span"],
                equal: vec![("status", json!("afk"))],
                per: None,
                count: 1,
            },
        }
    }

    fn of(event_type: &str, payload: &Value) -> Option<Self> {
        let field = |f: &str| payload.get(f).and_then(Value::as_str);
        match event_type {
            "unit.started" if field("unit_name").is_some_and(|u| SUSPEND_UNITS.contains(&u)) => {
                Some(Self::Suspend)
            }
            "presence.span" if field("status") == Some("afk") => Some(Self::Afk),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Shape {
    AdjacentTransitions,
    /// The source already observed the span: the payload carries its end.
    Span {
        end: &'static str,
    },
    StartStopPair {
        start: &'static str,
        stop: &'static str,
    },
}

/// One state kind's lift rule.
#[derive(Clone, Copy)]
pub struct LiftRule {
    pub state_kind: &'static str,
    pub event_types: &'static [&'static str],
    pub shape: Shape,
    pub subject: fn(&Value) -> Option<String>,
    pub label: fn(&Value) -> Option<String>,
    /// Payload fields copied into the interval's attributes.
    pub attributes: &'static [&'static str],
    pub fenced_by: &'static [Fence],
}

fn text<'a>(p: &'a Value, f: &str) -> Option<&'a str> {
    p.get(f).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn scalar(p: &Value, f: &str) -> Option<String> {
    match p.get(f)? {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

/// How many transitions after the day a key reads to end its last state.
const AFTER_COUNT: u32 = 64;

/// "app: title", or whichever of the two is present.
fn app_and_title(p: &Value, app: &str, title: &str) -> Option<String> {
    match (text(p, app), text(p, title)) {
        (Some(a), Some(t)) => Some(format!("{a}: {t}")),
        (Some(a), None) => Some(a.to_owned()),
        (None, Some(t)) => Some(t.to_owned()),
        (None, None) => None,
    }
}

pub const RULES: &[LiftRule] = &[
    LiftRule {
        state_kind: "desktop.focus",
        event_types: &["window.focused"],
        shape: Shape::AdjacentTransitions,
        subject: |p| {
            text(p, "window")
                .map(str::to_owned)
                .or_else(|| text(p, "class").map(|c| format!("class:{c}")))
        },
        label: |p| app_and_title(p, "class", "title"),
        attributes: &["window", "class", "title"],
        fenced_by: &[Fence::Suspend, Fence::Afk],
    },
    LiftRule {
        state_kind: "desktop.workspace",
        event_types: &["workspace.switched"],
        shape: Shape::AdjacentTransitions,
        subject: |p| {
            scalar(p, "workspace_id")
                .or_else(|| scalar(p, "workspace_name"))
                .map(|w| format!("workspace:{w}"))
        },
        label: |p| {
            text(p, "workspace_name")
                .map(str::to_owned)
                .or_else(|| scalar(p, "workspace_id"))
        },
        attributes: &["workspace_id", "workspace_name"],
        fenced_by: &[Fence::Suspend, Fence::Afk],
    },
    LiftRule {
        state_kind: "desktop.activitywatch.window",
        event_types: &["window.span"],
        shape: Shape::Span { end: "ended_at" },
        subject: |p| match (text(p, "app"), text(p, "title")) {
            (Some(a), Some(t)) => Some(format!("app:{a}|title:{t}")),
            (Some(a), None) => Some(format!("app:{a}")),
            (None, Some(t)) => Some(format!("title:{t}")),
            (None, None) => None,
        },
        label: |p| app_and_title(p, "app", "title"),
        attributes: &["bucket", "app", "title"],
        fenced_by: &[Fence::Suspend],
    },
    LiftRule {
        state_kind: "desktop.activitywatch.afk",
        event_types: &["presence.span"],
        shape: Shape::Span { end: "ended_at" },
        subject: |p| text(p, "status").map(|s| format!("status:{s}")),
        label: |p| text(p, "status").map(str::to_owned),
        attributes: &["bucket", "status"],
        fenced_by: &[Fence::Suspend],
    },
    // The systemd source emits `unit.started` and `unit.stopped` with
    // `unit_name`, `unit_type`, `active_state`, `sub_state` and, when known,
    // `main_pid` or `exit_code` (the predecessor's payloads).
    LiftRule {
        state_kind: "system.systemd.unit",
        event_types: &["unit.started", "unit.stopped"],
        shape: Shape::StartStopPair {
            start: "unit.started",
            stop: "unit.stopped",
        },
        subject: |p| text(p, "unit_name").map(str::to_owned),
        label: |p| text(p, "unit_name").map(str::to_owned),
        attributes: &[
            "unit_name",
            "unit_type",
            "active_state",
            "sub_state",
            "main_pid",
            "exit_code",
        ],
        fenced_by: &[],
    },
];

pub fn rule(state_kind: &str) -> Option<&'static LiftRule> {
    RULES.iter().find(|r| r.state_kind == state_kind)
}

/// Every event type any rule reads, as transitions, spans or fences.
pub const INPUT_TYPES: &[&str] = &[
    "window.focused",
    "workspace.switched",
    "window.span",
    "presence.span",
    "unit.started",
    "unit.stopped",
];

/// An open state during the fold.
#[derive(Clone, Debug)]
struct Open<'a> {
    start: &'a Event,
    at: Timestamp,
    subject: Option<String>,
    label: Option<String>,
    attributes: BTreeMap<String, String>,
}

impl<'a> Open<'a> {
    fn new(rule: &LiftRule, event: &'a Event, at: Timestamp) -> Self {
        let p = &event.payload;
        Self {
            start: event,
            at,
            subject: (rule.subject)(p),
            label: (rule.label)(p),
            attributes: rule
                .attributes
                .iter()
                .filter_map(|f| scalar(p, f).map(|v| ((*f).to_owned(), v)))
                .collect(),
        }
    }

    fn same_subject(&self, other: &Self) -> bool {
        self.subject.is_some() && self.subject == other.subject
    }

    /// A later observation of the same state: keep the start, take the newer
    /// label and attributes.
    fn refresh(&mut self, other: &Self) {
        if other.label.is_some() {
            self.label.clone_from(&other.label);
        }
        self.attributes.extend(other.attributes.clone());
    }
}

/// An interval produced by the fold, before it becomes an event.
struct Lifted<'a> {
    open: Open<'a>,
    end: Option<(Timestamp, &'a str)>,
    closers: Vec<&'a Event>,
}

fn interval_id(state_kind: &str, open: &Open<'_>) -> String {
    let subject = open.subject.as_deref().unwrap_or("unknown-subject");
    match &open.start.origin {
        Origin::Record { location, ordinal } => format!(
            "{state_kind}:{subject}:{}:{}:{ordinal}",
            location.material, location.anchor
        ),
        Origin::Derived { .. } => format!("{state_kind}:{subject}:ts:{}", open.at.unix_ns()),
    }
}

fn to_event(rule: &LiftRule, l: Lifted<'_>) -> Event {
    let payload = StateInterval {
        interval_id: interval_id(rule.state_kind, &l.open),
        state_kind: rule.state_kind.to_owned(),
        subject_id: l.open.subject.clone(),
        label: l.open.label.clone(),
        started_at: l.open.at,
        ended_at: l.end.map(|(t, _)| t),
        start_event_type: l.open.start.event_type.to_owned(),
        end_event_type: l.end.map(|(_, t)| t.to_owned()),
        attributes: l.open.attributes,
    };
    derived(
        &payload,
        IntervalLift::NAME,
        std::iter::once(l.open.start).chain(l.closers),
    )
}

fn fence_applies(rule: &LiftRule, e: &Event) -> bool {
    Fence::of(e.event_type, &e.payload).is_some_and(|f| rule.fenced_by.contains(&f))
}

/// Fold one rule over time-sorted events. Returns every interval whose start
/// is among `events`, closed or still open.
fn fold<'a>(rule: &LiftRule, events: &[&'a Event]) -> Vec<Lifted<'a>> {
    let mut out: Vec<Lifted<'a>> = Vec::new();
    match rule.shape {
        Shape::AdjacentTransitions => {
            let mut open: Option<Open<'a>> = None;
            for &e in events {
                let Some(at) = e.ts_orig.at else { continue };
                if fence_applies(rule, e)
                    && let Some(o) = open.take_if(|o| o.at <= at)
                {
                    out.push(Lifted {
                        open: o,
                        end: Some((at, e.event_type)),
                        closers: vec![e],
                    });
                }
                if !rule.event_types.contains(&e.event_type) {
                    continue;
                }
                let current = Open::new(rule, e, at);
                match &mut open {
                    None => open = Some(current),
                    // A tie: the later-committed transition takes the state's
                    // place; no zero-length interval.
                    Some(o) if o.at == at => *o = current,
                    Some(o) if o.same_subject(&current) => o.refresh(&current),
                    Some(_) => {
                        let o = open.replace(current).expect("matched Some");
                        out.push(Lifted {
                            open: o,
                            end: Some((at, e.event_type)),
                            closers: vec![e],
                        });
                    }
                }
            }
            if let Some(o) = open {
                out.push(Lifted {
                    open: o,
                    end: None,
                    closers: vec![],
                });
            }
        }
        Shape::Span { end } => {
            let fences: Vec<(&'a Event, Timestamp)> = events
                .iter()
                .filter(|e| fence_applies(rule, e))
                .filter_map(|&e| Some((e, e.ts_orig.at?)))
                .collect();
            for &e in events {
                if !rule.event_types.contains(&e.event_type) {
                    continue;
                }
                let Some(at) = e.ts_orig.at else { continue };
                let observed: Option<Timestamp> = e
                    .payload
                    .get(end)
                    .and_then(|v| serde_json::from_value(v.clone()).ok());
                let observed = observed.map(|t| t.max(at));
                // A fence inside the span ends it there.
                let fenced = fences
                    .iter()
                    .find(|(_, f)| *f > at && observed.is_none_or(|o| *f < o));
                let (end, closers) = match fenced {
                    Some(&(fence, f)) => (Some((f, fence.event_type)), vec![fence]),
                    None => (observed.map(|o| (o, e.event_type)), vec![]),
                };
                out.push(Lifted {
                    open: Open::new(rule, e, at),
                    end,
                    closers,
                });
            }
        }
        Shape::StartStopPair { start, stop } => {
            let mut open: BTreeMap<String, Open<'a>> = BTreeMap::new();
            for &e in events {
                let Some(at) = e.ts_orig.at else { continue };
                let current = Open::new(rule, e, at);
                let Some(subject) = current.subject.clone() else {
                    continue;
                };
                if e.event_type == start {
                    // A second start means the previous instance ran until now.
                    if let Some(previous) = open.remove(&subject) {
                        out.push(Lifted {
                            open: previous,
                            end: Some((at, e.event_type)),
                            closers: vec![e],
                        });
                    }
                    open.insert(subject, current);
                } else if e.event_type == stop {
                    // A stop with no open start belongs to an interval an
                    // earlier key emits; nothing to close here.
                    if let Some(mut previous) = open.remove(&subject) {
                        previous.attributes.extend(current.attributes);
                        // A stop at or before its start is a zero-length run.
                        let end = at.max(previous.at);
                        out.push(Lifted {
                            open: previous,
                            end: Some((end, e.event_type)),
                            closers: vec![e],
                        });
                    }
                }
            }
            for (_, o) in open {
                out.push(Lifted {
                    open: o,
                    end: None,
                    closers: vec![],
                });
            }
        }
    }
    out
}

pub struct IntervalLift;

fn key_parts(key: &Value) -> Option<(&'static LiftRule, jiff::civil::Date)> {
    let r = rule(key.get("state")?.as_str()?)?;
    let day = key.get("day")?.as_str()?.parse().ok()?;
    Some((r, day))
}

impl Keyed for IntervalLift {
    const NAME: &'static str = "state.intervals";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = INPUT_TYPES;
    const OUTPUTS: &'static [&'static str] = &[StateInterval::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        let Some(t) = input.ts_orig else {
            return vec![];
        };
        let Ok(day) = env.civil.day(t) else {
            return vec![];
        };
        let fence = Fence::of(input.event_type, input.payload);
        RULES
            .iter()
            .filter(|r| {
                r.event_types.contains(&input.event_type)
                    || fence.is_some_and(|f| r.fenced_by.contains(&f))
            })
            .map(|r| json!({"state": r.state_kind, "day": day.to_string()}))
            .collect()
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let (r, day) = key_parts(key)?;
        let start = env.civil.day_start(day).ok()?;
        let end = env.civil.day_end(day).ok()?;
        let mut types: Vec<&'static str> = r.event_types.to_vec();
        for f in r.fenced_by {
            for t in f.neighbour().event_types {
                if !types.contains(&t) {
                    types.push(t);
                }
            }
        }
        let mut q = InputQuery::window(&types, start, end);
        match r.shape {
            Shape::AdjacentTransitions => q.after.push(Neighbour {
                event_types: r.event_types.to_vec(),
                equal: vec![],
                per: None,
                count: AFTER_COUNT,
            }),
            Shape::Span { .. } => {}
            Shape::StartStopPair { .. } => q.after.push(Neighbour {
                event_types: r.event_types.to_vec(),
                equal: vec![],
                per: Some("unit_name"),
                count: 1,
            }),
        }
        for f in r.fenced_by {
            q.after.push(f.neighbour());
        }
        Some(q)
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some((r, day)) = key_parts(key) else {
            return Computed::default();
        };
        let (Ok(start), Ok(end)) = (env.civil.day_start(day), env.civil.day_end(day)) else {
            return Computed::default();
        };
        let mut events: Vec<&Event> = inputs.iter().filter(|e| e.ts_orig.at.is_some()).collect();
        events.sort_by_key(|e| e.ts_orig.at);
        let mut waiting = false;
        let outputs = fold(r, &events)
            .into_iter()
            .filter(|l| l.open.at >= start && l.open.at < end)
            .map(|l| {
                waiting |= l.end.is_none();
                to_event(r, l)
            })
            .collect();
        Computed { outputs, waiting }
    }

    fn key_state(&self, _key: &Value, _env: &Env) -> KeyState {
        // A state can stay open for days; joins read `ended_at`, not the key.
        KeyState::Open
    }
}

#[cfg(test)]
mod tests;
