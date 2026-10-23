use super::supplied;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Focus {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TextChanged {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_type: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Subtree {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tree: Option<Option<AccessibleNode>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AccessibleNode {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Option<Vec<AccessibleNode>>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

impl Focus {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl TextChanged {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl Subtree {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(value)) = &self.tree {
            value.collect_uninterpreted(&format!("{path}/tree"), fields);
        }
    }
}

impl AccessibleNode {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(values)) = &self.children {
            for (i, value) in values.iter().enumerate() {
                value.collect_uninterpreted(&format!("{path}/children/{i}"), fields);
            }
        }
    }
}
