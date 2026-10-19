//! Health records from devices and apps (Sleep as Android, Samsung Health).
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One record of a health app: a sleep session, a step count, a heart-rate
/// sample. The category names what it measures; the values keep the app's own
/// fields, typed as the app wrote them. Negative sentinels the app uses for
/// "absent" are absent here, never numbers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(
    type = "health.record",
    key = ["provider", "category", "native_id"],
    replace = newer,
    time = start,
    quality = intrinsic
)]
pub struct HealthRecord {
    /// The app: `sleep-as-android`, `samsung-health`.
    pub provider: String,
    /// What it measures: `sleep`, `steps`, `heart_rate`.
    pub category: String,
    pub native_id: String,
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    /// The UTC offset the record's local times were written in, in minutes,
    /// when the record shows it.
    pub offset_minutes: Option<i32>,
    pub values: Map<String, Value>,
    /// Markers and samples within the record, in order (sleep phases, heart
    /// rate samples), as `{kind, at, value?}`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<Value>,
}
