//! Derived activity: state intervals and hourly and daily summaries.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A span during which some state held: a focused window, a workspace, an
/// ActivityWatch bout, a running systemd unit. Lifted from transitions by the
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

/// What happened in one civil hour or logical day: commands, shell sessions
/// and time per state, each counted from the derived events of that period.
/// A count of zero means the source was read and saw nothing; a period with
/// no inputs at all has no summary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "activity.summary",
    key = ["grain", "period_start"],
    replace = newer,
    time = period_start,
    quality = intrinsic
)]
pub struct ActivitySummary {
    /// `hour` or `day`.
    pub grain: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    /// The logical day the period belongs to (`YYYY-MM-DD`).
    pub day: String,
    pub commands: u32,
    pub failed_commands: u32,
    pub sessions: u32,
    /// Seconds of shell-session time inside the period.
    pub session_seconds: i64,
    /// Working directories by command count, most first.
    pub top_cwds: Vec<(String, u32)>,
    /// Seconds per state kind inside the period (closed intervals and the
    /// elapsed part of open ones).
    pub state_seconds: std::collections::BTreeMap<String, i64>,
    /// The longest-held subjects: (state kind, label, seconds).
    pub top_subjects: Vec<(String, String, i64)>,
}
