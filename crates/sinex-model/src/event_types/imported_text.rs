//! Imported text refers to external inputs and runs without asserting local provenance.
use crate::{EventPayload, KeyKind, ReplaceRule, TimeQuality, Timestamp, TsOrig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TranscriptSegment {
    pub input: String,
    pub provider_segment_id: Option<String>,
    pub segment_index: u64,
    pub text: String,
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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OcrSegment {
    pub input: String,
    pub provider_segment_id: Option<String>,
    pub segment_index: u64,
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

impl EventPayload for TranscriptSegment {
    const EVENT_TYPE: &'static str = "transcript.segment";
    const KEY: KeyKind =
        KeyKind::Fields(&["input", "external_run", "model", "provider_segment_id"]);
    const REPLACE: ReplaceRule = ReplaceRule::ReplaceWhenNewer;
    fn key_kind(&self) -> KeyKind {
        if self.provider_segment_id.is_some() {
            Self::KEY
        } else {
            KeyKind::Location
        }
    }
    fn ts_orig(&self) -> TsOrig {
        TsOrig::maybe(self.at, TimeQuality::Intrinsic)
    }
    fn payload_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("schemas serialize")
    }
}

impl EventPayload for OcrSegment {
    const EVENT_TYPE: &'static str = "ocr.segment";
    const KEY: KeyKind =
        KeyKind::Fields(&["input", "external_run", "engine", "provider_segment_id"]);
    const REPLACE: ReplaceRule = ReplaceRule::ReplaceWhenNewer;
    fn key_kind(&self) -> KeyKind {
        if self.provider_segment_id.is_some() {
            Self::KEY
        } else {
            KeyKind::Location
        }
    }
    fn ts_orig(&self) -> TsOrig {
        TsOrig::unknown()
    }
    fn payload_schema() -> Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("schemas serialize")
    }
}

#[cfg(test)]
mod tests;
