use super::{Scalar, supplied};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

// Google Fit's exported value wrapper and direct value forms are both retained.
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
    pub(crate) uninterpreted: Map<String, Value>,
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
    pub(crate) uninterpreted: Map<String, Value>,
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
    pub(crate) uninterpreted: Map<String, Value>,
}

impl Fit {
    pub(crate) fn nested_uninterpreted_fields(&self) -> Vec<String> {
        let mut fields = Vec::new();
        if self
            .fit_value
            .as_ref()
            .and_then(Option::as_ref)
            .is_some_and(|values| values.iter().any(FitValue::has_uninterpreted))
        {
            fields.push("/values/fitValue".into());
        }
        fields
    }
}

impl FitValue {
    fn has_uninterpreted(&self) -> bool {
        !self.uninterpreted.is_empty()
            || self
                .value
                .as_ref()
                .and_then(Option::as_ref)
                .is_some_and(|v| v.has_uninterpreted())
            || self
                .map_val
                .as_ref()
                .and_then(Option::as_ref)
                .is_some_and(|rows| {
                    rows.iter().any(|row| {
                        !row.uninterpreted.is_empty()
                            || row
                                .value
                                .as_ref()
                                .and_then(Option::as_ref)
                                .is_some_and(FitValue::has_uninterpreted)
                    })
                })
    }
}
