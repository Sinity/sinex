//! Stored non-model input, rhythm and mark products.
use crate::Timestamp;
use super::activity::ActivityEvidence;
use schemars::JsonSchema;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type="input.bout",key=["bout"],replace=newer,time=started_at,quality=intrinsic)]
pub struct InputBout {
    pub bout: String,
    pub scope: String,
    pub day: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub key_presses: u64,
    pub pointer_events: u64,
    pub key_instants: Vec<Timestamp>,
    pub profile: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type="input.workload",key=["scope","day"],replace=newer,time=period_start,quality=intrinsic)]
pub struct InputWorkload {
    pub scope: String,
    pub day: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    pub key_presses: u64,
    pub pointer_events: u64,
    pub bouts: u64,
    pub supported_seconds: f64,
    pub coverage_seconds: f64,
    pub missing_seconds: f64,
    pub typing_intervals: Vec<(Timestamp,Timestamp)>,
    pub typing_seconds: Option<f64>,
    pub estimation: String,
    pub coverage: String,
    pub profile: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type="rhythm.deviation",key=["scope","day","measure"],replace=newer,time=period_start,quality=intrinsic)]
pub struct RhythmDeviation {
    pub scope: String,
    pub day: String,
    pub measure: String,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    pub baseline_from: Timestamp,
    pub baseline_until: Timestamp,
    pub prior_observed_days: u64,
    pub value: Option<f64>,
    pub baseline: Option<f64>,
    pub deviation: Option<f64>,
    pub estimation: String,
    pub profile: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type="reference.about",key=["mark_record","target_record"],replace=newer,time=at,quality=intrinsic)]
pub struct MarkLink {
    pub mark_record: String,
    pub target_record: String,
    pub at: Timestamp,
    pub mark: ActivityEvidence,
    pub target: ActivityEvidence,
    pub method: String,
}
