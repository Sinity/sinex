use super::supplied;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Idle {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Option<Provenance>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Provenance {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hyprland_clients: Option<Option<ClientInventory>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub systemd_logind: Option<Option<SystemInhibitors>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_window: Option<Option<ActiveWindow>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other_session_dbus: Option<Option<Unavailable>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ClientInventory {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inhibitors: Option<Option<Vec<Window>>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SystemInhibitors {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inhibitors: Option<Option<Vec<SystemInhibitor>>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActiveWindow {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<Option<Window>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Unavailable {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<Option<i64>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Window {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<Option<Workspace>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Workspace {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Option<String>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SystemInhibitor {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub who: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub what: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<Option<i64>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

impl Idle {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(value)) = &self.provenance {
            value.collect_uninterpreted(&format!("{path}/provenance"), fields);
        }
    }
}

impl Provenance {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(value)) = &self.hyprland_clients {
            value.collect_uninterpreted(&format!("{path}/hyprland_clients"), fields);
        }
        if let Some(Some(value)) = &self.systemd_logind {
            value.collect_uninterpreted(&format!("{path}/systemd_logind"), fields);
        }
        if let Some(Some(value)) = &self.active_window {
            value.collect_uninterpreted(&format!("{path}/active_window"), fields);
        }
        if let Some(Some(value)) = &self.other_session_dbus {
            value.collect_uninterpreted(&format!("{path}/other_session_dbus"), fields);
        }
    }
}

impl ClientInventory {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(values)) = &self.inhibitors {
            for (i, value) in values.iter().enumerate() {
                value.collect_uninterpreted(&format!("{path}/inhibitors/{i}"), fields);
            }
        }
    }
}

impl SystemInhibitors {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(values)) = &self.inhibitors {
            for (i, value) in values.iter().enumerate() {
                value.collect_uninterpreted(&format!("{path}/inhibitors/{i}"), fields);
            }
        }
    }
}

impl ActiveWindow {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(value)) = &self.window {
            value.collect_uninterpreted(&format!("{path}/window"), fields);
        }
    }
}

impl Unavailable {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl Window {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
        if let Some(Some(value)) = &self.workspace {
            value.collect_uninterpreted(&format!("{path}/workspace"), fields);
        }
    }
}

impl Workspace {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}

impl SystemInhibitor {
    pub(crate) fn collect_uninterpreted(&self, path: &str, fields: &mut Vec<String>) {
        super::extend_fields(&self.uninterpreted, path, fields);
    }
}
