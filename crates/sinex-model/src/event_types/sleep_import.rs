//! Imported joined sleep sessions, separate from native health records.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "sleep.session_imported", key = ["dataset", "provider_session_id"], replace = newer, time = started_at, quality = intrinsic)]
pub struct SleepSessionImported {
    pub dataset: String,
    pub provider_session_id: String,
    pub start_local: String,
    pub end_local: String,
    pub zone: String,
    pub started_at: Option<Timestamp>,
    pub ended_at: Option<Timestamp>,
    /// Why a valid local time could not identify one instant.
    pub start_unresolved: Option<String>,
    pub end_unresolved: Option<String>,
    pub sh_duration_minutes: Option<f64>,
    pub sa_vs_sh_duration_minutes: Option<f64>,
    pub hr_avg: Option<f64>,
    pub hr_min: Option<f64>,
    pub hr_max: Option<f64>,
    pub trimmed_event_count: Option<u64>,
    pub events_hr: Option<u64>,
    pub events_light: Option<u64>,
    pub events_deep: Option<u64>,
    pub events_rem: Option<u64>,
    pub sa_comment: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[cfg(test)]
mod tests;
