use super::supplied;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The source header label and its nullable, normalized sample.
pub type ActigraphySample = (Option<String>, Option<serde_json::Number>);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SleepSession {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tz_column: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub hours: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub rating: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub framerate: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub snore: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub noise: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub cycles: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub deep_sleep: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub len_adjust: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geo: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<Vec<(Option<String>, Option<f64>)>>>")]
    pub actigraphy: Option<Option<Vec<ActigraphySample>>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SleepNoise {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<Vec<Option<f64>>>>")]
    pub noise: Option<Option<Vec<Option<serde_json::Number>>>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}
