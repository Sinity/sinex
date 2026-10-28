//! Source read outcomes whose retained bytes can parent derived coverage.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CoverageState {
    Observed,
    Partial,
    RecordedUnknownCoverage,
    Missing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CoverageCompleteness {
    Complete,
    Sample,
    Unknown,
}

/// A successful read proves access, including when the input was unchanged.
/// It establishes temporal coverage only when the source declares its bounds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "source.coverage", key = ["observation"], replace = repeat, time = ended_at, quality = captured)]
pub struct SourceCoverage {
    pub observation: String,
    pub source: String,
    pub binding: String,
    pub host: String,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub state: CoverageState,
    pub reason: Option<String>,
    pub completeness: CoverageCompleteness,
    pub bounded_by: String,
    pub events: u64,
    pub failures: u64,
}
