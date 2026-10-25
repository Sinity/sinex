use super::{Scalar, supplied};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

// Google Fit's exported value wrapper and direct value forms are both kept.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Fit {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "fitValue")]
    pub fit_value: Option<Option<Vec<FitValue>>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "modifiedTimeMillis")]
    pub modified_time_millis: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "originDataSourceId")]
    pub origin_data_source_id: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FitValue {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Option<Box<FitValue>>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    #[serde(rename = "intVal")]
    pub int_val: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    #[serde(rename = "fpVal")]
    pub fp_val: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "stringVal")]
    pub string_val: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "mapVal")]
    pub map_val: Option<Option<Vec<MapEntry>>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MapEntry {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Option<FitValue>>,
    #[serde(flatten)]
    pub(crate) unknown: Map<String, Value>,
}

impl Fit {
    pub(crate) fn nested_unknown_fields(&self) -> Vec<String> {
        let mut fields = Vec::new();
        if let Some(Some(values)) = &self.fit_value {
            for (i, value) in values.iter().enumerate() {
                value.unknown_fields(&format!("/values/fitValue/{i}"), &mut fields);
            }
        }
        fields
    }
}
impl FitValue {
    fn unknown_fields(&self, path: &str, fields: &mut Vec<String>) {
        fields.extend(
            self.unknown
                .keys()
                .map(|key| format!("{path}/{}", super::pointer(key))),
        );
        if let Some(Some(value)) = &self.value {
            value.unknown_fields(&format!("{path}/value"), fields);
        }
        if let Some(Some(entries)) = &self.map_val {
            for (i, entry) in entries.iter().enumerate() {
                let path = format!("{path}/mapVal/{i}");
                fields.extend(
                    entry
                        .unknown
                        .keys()
                        .map(|key| format!("{path}/{}", super::pointer(key))),
                );
                if let Some(Some(value)) = &entry.value {
                    value.unknown_fields(&format!("{path}/value"), fields);
                }
            }
        }
    }
}
