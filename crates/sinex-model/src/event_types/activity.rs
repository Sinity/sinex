//! Operator activity envelopes and the elementary signals inside them.
use crate::{EventId, Timestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Agency {
    Operator,
    Supervising,
    Delegated,
    Automated,
    Passive,
    Unknown,
    Mixed,
}

/// An exact reading and the stable record it read.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActivityEvidence {
    pub event: EventId,
    pub record: String,
    pub event_type: String,
    pub source: String,
}

/// Counts belong to the partition containing each record's start.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActivityCounts {
    pub records: BTreeMap<String, u64>,
    pub sources: BTreeMap<String, u64>,
    pub observers: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "activity.window", key = ["window_id"], replace = newer, time = started_at, quality = intrinsic)]
pub struct ActivityWindow {
    pub window_id: String,
    /// A captured host and seat, or an explicitly unknown scope.
    pub scope: String,
    pub day: String,
    pub partition_start: Timestamp,
    pub partition_end: Timestamp,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    /// Context counts describe records, not seconds of operator activity.
    pub counts: ActivityCounts,
    pub apps: Vec<String>,
    pub close_reason: Option<String>,
    pub continuation_before: bool,
    pub continuation_after: bool,
    pub provisional: bool,
    /// Union of operator evidence, excluding quiet gaps, in seconds.
    pub operator_present_s: f64,
    pub operator_intervals: Vec<(Timestamp, Timestamp)>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActivityMember {
    /// `window` or `short_break`.
    pub kind: String,
    pub window: Option<String>,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "activity.session", key = ["session_id"], replace = newer, time = started_at, quality = intrinsic)]
pub struct ActivitySession {
    pub session_id: String,
    pub scope: String,
    /// Ownership only; the session may cross any number of logical days.
    pub day: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub windows: u64,
    pub members: Vec<ActivityMember>,
    /// Context counts describe records, not seconds of operator activity.
    pub counts: ActivityCounts,
    pub close_reason: Option<String>,
    pub provisional: bool,
    pub operator_present_s: f64,
    pub short_breaks: u64,
    pub agency_seconds: BTreeMap<String, f64>,
    pub delegated_overlap_s: Option<f64>,
    pub profile: String,
    pub break_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "activity.span", key = ["span_id"], replace = newer, time = started_at, quality = intrinsic)]
pub struct ActivitySpan {
    pub span_id: String,
    pub scope: String,
    pub day: String,
    pub partition_start: Timestamp,
    pub partition_end: Timestamp,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub window: Option<String>,
    pub agency: Agency,
    /// Delegated work can overlap any operator-channel class.
    pub delegated: bool,
    pub agency_evidence: Vec<ActivityEvidence>,
    pub presence: String,
    pub presence_method: String,
    pub presence_claims: Vec<String>,
    pub focus: Vec<String>,
    pub apps: Vec<String>,
    pub titles: Vec<String>,
    pub display: String,
    pub playback: bool,
    pub input_records: u64,
    pub operator_present_s: f64,
    pub profile: String,
    pub continuation_before: bool,
    pub continuation_after: bool,
    pub provisional: bool,
}

/// A recorded start and stop of a non-virtual physical input recorder.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "input.coverage", key = ["coverage_id"], replace = newer, time = started_at, quality = captured)]
pub struct InputCoverageInterval {
    pub coverage_id: String,
    pub scope: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
}
