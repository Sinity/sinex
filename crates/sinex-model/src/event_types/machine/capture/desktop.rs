use super::supplied;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Selection {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub size: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_window: Option<Option<SourceWindow>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceWindow {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Notification {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_icon: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub replaces_id: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub urgency: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub expire_timeout: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub timestamp: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actions: Option<Option<Vec<NotificationAction>>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hints: Option<Option<Map<String, Value>>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NotificationAction {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Media {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub player: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub album: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub position_seconds: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub duration_seconds: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Frame {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_class: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_title: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitor: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub ts: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geometry: Option<Option<Geometry>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Geometry {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub y: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<Option<i64>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Navigation {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_url: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_title: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_url: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_title: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor_text: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub ts: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Reading {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_url: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_title: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor_text: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub pushed_at: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Monitor {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub brightness: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub contrast: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub power_sl: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub input_sl: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Environment {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub score: Option<Option<serde_json::Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub co2: Option<Option<serde_json::Number>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

impl Selection {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(value)) = &self.source_window {
            value.collect_uninterpreted(&format!("{path}/source_window"), fields);
        }
    }
}

impl SourceWindow {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl Notification {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(values)) = &self.actions {
            for (i, value) in values.iter().enumerate() {
                value.collect_uninterpreted(&format!("{path}/actions/{i}"), fields);
            }
        }
        if let Some(Some(values)) = &self.hints {
            super::extend_fields(values, &format!("{path}/hints"), fields);
        }
    }
}

impl NotificationAction {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl Media {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl Frame {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(value)) = &self.geometry {
            value.collect_uninterpreted(&format!("{path}/geometry"), fields);
        }
    }
}

impl Geometry {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl Navigation {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl Reading {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl Monitor {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl Environment {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}
