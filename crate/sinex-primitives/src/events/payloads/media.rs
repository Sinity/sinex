//! Media transcript and OCR observation payloads (#1043).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::temporal::Timestamp;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "media.audio",
    event_type = "media.audio.transcript_segment_observed"
)]
pub struct AudioTranscriptSegmentObservedPayload {
    pub segment_index: u32,
    pub text: String,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub speaker_label: Option<String>,
    pub language: Option<String>,
    pub confidence: Option<f64>,
    pub source_file: Option<String>,
    pub raw_material_id: String,
    pub model_id: Option<String>,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "media.screen",
    event_type = "media.screen.ocr_segment_observed"
)]
pub struct ScreenOcrSegmentObservedPayload {
    pub segment_index: u32,
    pub text: String,
    pub bbox: Option<Vec<i64>>,
    pub confidence: Option<f64>,
    pub display_id: Option<String>,
    pub window_title: Option<String>,
    pub source_file: Option<String>,
    pub raw_material_id: String,
    pub engine: Option<String>,
    pub observed_at: Timestamp,
}
