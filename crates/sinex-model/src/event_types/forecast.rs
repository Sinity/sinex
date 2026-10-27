//! Submitted binary forecasts and separately recorded outcomes.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(type = "forecast.issued", key = ["forecast"], replace = repeat, time = at, quality = intrinsic)]
pub struct Issued {
    pub forecast: String,
    pub target: String,
    pub criteria: String,
    pub probability: f64,
    pub horizon: Timestamp,
    pub information_cutoff: Timestamp,
    /// Visible event IDs available when the forecast was issued.
    pub information: Vec<String>,
    pub information_position: i64,
    pub by: String,
    pub at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(type = "forecast.resolved", key = location, replace = repeat, time = at, quality = intrinsic)]
pub struct Resolved {
    pub forecast: String,
    /// An unknown outcome is recorded explicitly and is never scored as false.
    pub outcome: Option<bool>,
    pub outcome_at: Option<Timestamp>,
    pub evidence: Vec<String>,
    pub by: String,
    /// When this resolution was recorded, distinct from the outcome's time.
    pub at: Timestamp,
}

crate::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum ForecastRecord {
        Issued(Issued),
        Resolved(Resolved),
    }
}
