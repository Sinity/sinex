//! Derived activity: state intervals, metric figures, and hourly and daily
//! health figures.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A span during which some state held: a focused window, a workspace, an
/// ActivityWatch bout. Derived from transitions by the
/// interval derivation. Identified by its start evidence; the end may still
/// be unknown (`ended_at` is `None` while the state holds).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "state.interval",
    key = ["interval_id"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct StateInterval {
    /// `<state kind>:<subject>:<start record location or start time>`.
    pub interval_id: String,
    pub state_kind: String,
    pub subject_id: Option<String>,
    pub label: Option<String>,
    pub started_at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub start_event_type: String,
    /// What ended it: the next transition, a fence, a stop, or the observed
    /// duration (`None` while open).
    pub end_event_type: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub attributes: std::collections::BTreeMap<String, String>,
}

/// One metric over one civil hour or logical day: how many input events it
/// counted and the metric's number for them, in total or for one value of a
/// breakdown field. Hourly figures are computed from the metric's input
/// events, daily ones from the day's hourly figures. A period nothing was
/// counted in has no figure; whether that means zero or unobserved depends on
/// whether the sources were observing, which is read with the figures.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "metric.figure",
    key = ["metric", "grain", "period_start", "dimension", "dimension_value"],
    replace = newer,
    time = period_start,
    quality = intrinsic
)]
pub struct MetricFigure {
    /// The metric's name (`shell.commands`).
    pub metric: String,
    /// `hour` or `day`.
    pub grain: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    /// The logical day the period belongs to (`YYYY-MM-DD`).
    pub day: String,
    /// The breakdown field (`cwd`), and the value this figure counts for;
    /// both empty for the total.
    pub dimension: String,
    pub dimension_value: String,
    /// The metric's number; `None` where it is undefined (a ratio of zero
    /// commands with a known exit code).
    pub value: Option<f64>,
    /// A ratio's parts, summed from hours to the day.
    pub numerator: Option<f64>,
    pub denominator: Option<f64>,
    /// Input events counted.
    pub rows: u64,
}

/// Figures over one series of health points in one civil hour or logical
/// day: how many points started in the period and, for each numeric value
/// position, their count, sum, minimum and maximum. Hourly figures are
/// computed from the points, daily ones from the day's hourly figures.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "health.figure",
    key = ["grain", "period_start", "provider", "series"],
    replace = newer,
    time = period_start,
    quality = intrinsic
)]
pub struct HealthFigure {
    /// `hour` or `day`.
    pub grain: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    /// The logical day the period belongs to (`YYYY-MM-DD`).
    pub day: String,
    pub provider: String,
    /// The points' data type.
    pub category: String,
    /// Where the points came from: a Fit data source.
    pub series: String,
    pub points: u64,
    pub values: Vec<FigureValue>,
}

/// One numeric value position of a series' points, over a period.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FigureValue {
    pub position: u32,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

/// A stretch of continuous attention at the machine on one logical day: the
/// operator's own activity (commands, window focus, page visits, copies)
/// joined across sources while the gaps between it stay short, minus the time
/// presence says nobody was there. It says that someone was active, never
/// what the activity meant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "activity.span",
    key = ["day", "started_at"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct ActivitySpan {
    /// The logical day it belongs to (`YYYY-MM-DD`); a span never crosses a
    /// day start.
    pub day: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    /// Activity records inside it, by event type.
    pub records: std::collections::BTreeMap<String, u64>,
    /// The applications focused in it, in the order first focused.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub apps: Vec<String>,
}

/// A tag assigned by declared rules. The parent is the reading the rules
/// matched, and the record identity survives replay. Each rule is recorded
/// as its JSON declaration; an unknown input time stays unknown.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "tag.applied", key = ["tag", "record"], replace = newer, time = at, quality = intrinsic)]
pub struct TagApplied {
    pub tag: String,
    pub record: String,
    pub event_type: String,
    pub rules: Vec<String>,
    pub at: Option<Timestamp>,
}

/// One conversation's messages on one logical day: a chat buffer, a message
/// thread (Messenger, SMS) or a mail thread, with who wrote and when. A
/// thread over many days is its days.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "conversation.day",
    key = ["channel", "conversation", "day"],
    replace = newer,
    time = first_at,
    quality = intrinsic
)]
pub struct ConversationDay {
    /// `chat`, `messages` or `mail`.
    pub channel: String,
    /// The buffer, the thread's participants, or a mail identity prefixed
    /// with `thread:` for a recorded thread ID and `message:` otherwise.
    pub conversation: String,
    /// The logical day (`YYYY-MM-DD`).
    pub day: String,
    pub messages: u64,
    /// Who wrote, sorted.
    pub senders: Vec<String>,
    pub first_at: Timestamp,
    pub last_at: Timestamp,
}
