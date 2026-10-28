//! The room: air quality sensors.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One air-quality sample. The sensor's own clock identifies it, so the same
/// sample read twice is a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "air.sampled", key = ["sampled_at"], replace = repeat, time = sampled_at, quality = intrinsic)]
pub struct AirSampled {
    pub sampled_at: Timestamp,
    /// The sensor's overall score, 0 to 100.
    pub score: Option<f64>,
    pub temperature_c: Option<f64>,
    /// Relative humidity, percent.
    pub humidity: Option<f64>,
    pub co2_ppm: Option<f64>,
    /// Volatile organic compounds, ppb.
    pub voc_ppb: Option<f64>,
    pub pm25: Option<f64>,
    /// The sensor's other readings (estimates, baselines, raw values), by
    /// the name it gave them.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
