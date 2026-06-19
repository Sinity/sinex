//! Media capture, transcript, and OCR observation payloads (#1043).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::temporal::Timestamp;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "media.audio", event_type = "media.audio.recording_observed")]
pub struct AudioRecordingObservedPayload {
    pub raw_material_id: String,
    pub file_format: Option<String>,
    pub codec: Option<String>,
    pub duration_ms: Option<u64>,
    pub channel_count: Option<u32>,
    pub sample_rate_hz: Option<u32>,
    pub capture_session_id: Option<String>,
    pub source_file: Option<String>,
    pub policy_posture: String,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "media.audio",
    event_type = "media.audio.capture_session_started"
)]
pub struct MediaAudioCaptureSessionStartedPayload {
    pub capture_session_id: String,
    pub scope: String,
    pub reason: String,
    pub operator_binding_id: String,
    pub policy_posture: String,
    pub started_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "media.audio",
    event_type = "media.audio.capture_session_ended"
)]
pub struct MediaAudioCaptureSessionEndedPayload {
    pub capture_session_id: String,
    pub reason: Option<String>,
    pub ended_at: Timestamp,
    pub duration_ms: Option<u64>,
    pub final_state: String,
    pub policy_posture: String,
}

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
    pub producer_run_id: Option<String>,
    pub timestamp_quality: Option<String>,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "media.audio",
    event_type = "media.audio.transcription_run_observed"
)]
pub struct AudioTranscriptionRunObservedPayload {
    pub producer_run_id: String,
    pub model_id: String,
    pub model_version: Option<String>,
    pub input_material_ids: Vec<String>,
    pub output_refs: Vec<String>,
    pub duration_ms: Option<u64>,
    pub resource_posture: String,
    pub failure_class: Option<String>,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "media.screen",
    event_type = "media.screen.screenshot_observed"
)]
pub struct ScreenScreenshotObservedPayload {
    pub raw_material_id: String,
    pub display_id: Option<String>,
    pub window_title: Option<String>,
    pub region: Option<Vec<i64>>,
    pub width_px: u32,
    pub height_px: u32,
    pub capture_session_id: Option<String>,
    pub source_file: Option<String>,
    pub policy_posture: String,
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
    pub producer_run_id: Option<String>,
    pub timestamp_quality: Option<String>,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "media.screen", event_type = "media.screen.ocr_run_observed")]
pub struct ScreenOcrRunObservedPayload {
    pub producer_run_id: String,
    pub engine_id: String,
    pub engine_version: Option<String>,
    pub input_material_ids: Vec<String>,
    pub output_refs: Vec<String>,
    pub duration_ms: Option<u64>,
    pub resource_posture: String,
    pub failure_class: Option<String>,
    pub observed_at: Timestamp,
}
