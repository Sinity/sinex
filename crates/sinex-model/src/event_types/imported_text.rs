//! Imported text refers to external inputs and runs without asserting local provenance.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(
    Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload,
)]
#[event(version = 2, type = "transcript.cue", key = location, replace = newer, time = at, quality = intrinsic)]
pub struct TranscriptCue {
    pub input: String,
    pub provider_id: Option<String>,
    pub text: String,
    /// Decimal seconds exactly as written in the import.
    pub start_seconds: Option<String>,
    pub end_seconds: Option<String>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub speaker: Option<String>,
    pub language: Option<String>,
    pub confidence: Option<f64>,
    pub confidence_scale: Option<String>,
    pub model: Option<String>,
    pub external_run: Option<String>,
    pub at: Option<Timestamp>,
    #[serde(default)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BoundingBox {
    pub left: f64,
    pub top: f64,
    pub width: f64,
    pub height: f64,
    pub units: String,
}

#[derive(
    Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload,
)]
#[event(version = 2, type = "ocr.text", key = location, replace = newer)]
pub struct OcrText {
    pub input: String,
    pub provider_id: Option<String>,
    pub text: String,
    pub page: Option<u64>,
    pub block: Option<u64>,
    pub paragraph: Option<u64>,
    pub line: Option<u64>,
    pub word: Option<u64>,
    pub bounding_box: Option<BoundingBox>,
    pub display: Option<String>,
    pub window: Option<String>,
    pub confidence: Option<f64>,
    pub confidence_scale: Option<String>,
    pub engine: Option<String>,
    pub external_run: Option<String>,
    #[serde(default)]
    pub extra: Map<String, Value>,
}

#[cfg(test)]
mod tests;
