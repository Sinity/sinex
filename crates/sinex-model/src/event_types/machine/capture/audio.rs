use super::supplied;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Devices {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asr_source: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recording: Option<Option<Vec<Recording>>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excluded: Option<Option<Vec<Excluded>>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub draining: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed: Option<Option<Vec<Failed>>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Recording {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_kind: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub since: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Excluded {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Failed {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub at: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Node {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "node.name")]
    pub node_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "node.description")]
    pub node_description: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "media.class")]
    pub media_class: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Port {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "port.name")]
    pub port_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "port.direction")]
    pub port_direction: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "node.id")]
    pub node_id: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Link {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "output-node-id")]
    pub output_node_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "output-port-id")]
    pub output_port_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "input-node-id")]
    pub input_node_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "input-port-id")]
    pub input_port_id: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpeechIndex {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decode_error: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub segment_start: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub decoded_seconds: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub span_seconds: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub coverage: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speech_spans: Option<Option<Vec<SpeechSpan>>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpeechSpan {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub start: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub end: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

impl Devices {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
        if let Some(Some(values)) = &self.recording {
            for (i, value) in values.iter().enumerate() {
                value.collect_unknown(&format!("{path}/recording/{i}"), fields);
            }
        }
        if let Some(Some(values)) = &self.excluded {
            for (i, value) in values.iter().enumerate() {
                value.collect_unknown(&format!("{path}/excluded/{i}"), fields);
            }
        }
        if let Some(Some(values)) = &self.failed {
            for (i, value) in values.iter().enumerate() {
                value.collect_unknown(&format!("{path}/failed/{i}"), fields);
            }
        }
    }
}

impl Recording {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}

impl Excluded {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}

impl Failed {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}

impl Node {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}

impl Port {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}

impl Link {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}

impl SpeechIndex {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
        if let Some(Some(values)) = &self.speech_spans {
            for (i, value) in values.iter().enumerate() {
                value.collect_unknown(&format!("{path}/speech_spans/{i}"), fields);
            }
        }
    }
}

impl SpeechSpan {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}
