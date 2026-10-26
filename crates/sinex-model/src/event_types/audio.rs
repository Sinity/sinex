//! Audio capture: which devices are being recorded, the audio graph's
//! changes, and where speech was found in recorded segments. Audio is
//! recorded, never heard: nothing here says what anyone attended to.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The recorder's state: the devices it records, excludes or failed on.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "audio.recording_state", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct AudioRecordingState {
    pub received_at: Timestamp,
    /// Why it was reported: a change, or a periodic `heartbeat`.
    pub reason: Option<String>,
    /// The input speech recognition listens to.
    pub speech_input: Option<String>,
    pub recording: Vec<RecordedDevice>,
    pub excluded: Vec<ExcludedDevice>,
    /// Devices whose recording is finishing.
    pub draining: Vec<String>,
    pub failed: Vec<FailedDevice>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordedDevice {
    pub node_name: Option<String>,
    /// `source` (an input) or `sink` (an output).
    pub device_kind: Option<String>,
    pub channel: Option<String>,
    pub description: Option<String>,
    pub since: Option<Timestamp>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ExcludedDevice {
    pub key: Option<String>,
    pub pattern: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FailedDevice {
    pub key: Option<String>,
    pub error: Option<String>,
    pub at: Option<Timestamp>,
}

/// A node, port or link of the audio graph (PipeWire) was added or removed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "audio.graph_changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct AudioGraphChanged {
    pub received_at: Timestamp,
    /// `node`, `port` or `link`.
    pub object: String,
    /// `added` or `removed`.
    pub action: String,
    /// The object's id in the graph.
    pub object_id: i64,
    pub node_name: Option<String>,
    pub node_description: Option<String>,
    pub media_class: Option<String>,
    pub port_name: Option<String>,
    pub port_direction: Option<String>,
    pub node_id: Option<String>,
    pub output_node_id: Option<String>,
    pub output_port_id: Option<String>,
    pub input_node_id: Option<String>,
    pub input_port_id: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// Where a recorded audio segment holds speech, found by voice-activity
/// detection, or why the segment could not be decoded.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "speech.indexed", key = location, replace = repeat, time = segment_start, quality = intrinsic)]
pub struct SpeechIndexed {
    pub segment_start: Timestamp,
    pub channel: Option<String>,
    /// The segment's file name.
    pub segment: Option<String>,
    /// The segment file's path, as the capture wrote it.
    pub path: Option<String>,
    pub decoded_seconds: Option<f64>,
    pub span_seconds: Option<f64>,
    /// The share of the segment decoded.
    pub coverage: Option<f64>,
    pub speech: Vec<SpeechSpan>,
    pub decode_error: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpeechSpan {
    pub start: Timestamp,
    pub end: Timestamp,
}
