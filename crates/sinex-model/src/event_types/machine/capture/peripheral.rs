use super::supplied;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Battery {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_address: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_alias: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub percentage: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Transport {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_address: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_alias: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub codec: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub volume: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Logitech {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub devices: Option<Option<Vec<LogitechDevice>>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct LogitechDevice {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub battery_status: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub battery_percent: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub battery_mv: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

impl Battery {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}

impl Transport {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}

impl Logitech {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
        if let Some(Some(values)) = &self.devices {
            for (i, value) in values.iter().enumerate() {
                value.collect_unknown(&format!("{path}/devices/{i}"), fields);
            }
        }
    }
}

impl LogitechDevice {
    pub(crate) fn collect_unknown(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.unknown, path, fields);
    }
}
