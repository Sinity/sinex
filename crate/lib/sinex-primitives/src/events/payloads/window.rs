//! Window manager event payloads
//!
//! Note: Different window managers have different payloads

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

// Hyprland window manager payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.opened")]
pub struct HyprlandWindowOpenedPayload {
    pub window_id: String,
    pub window_class: String,
    pub window_title: String,
    pub workspace_id: i32,
    pub monitor_id: i32,
    pub geometry: WindowGeometry,
    pub floating: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.closed")]
pub struct HyprlandWindowClosedPayload {
    pub window_id: String,
    pub window_class: String,
    pub window_title: String,
    pub workspace_id: i32,
    pub close_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.focused")]
pub struct HyprlandWindowFocusedPayload {
    pub window_id: String,
    pub window_class: String,
    pub window_title: String,
    pub workspace_id: i32,
    pub previous_window_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.title_changed")]
pub struct HyprlandWindowTitleChangedPayload {
    pub window_id: String,
    pub window_title: String,
    pub previous_window_title: Option<String>,
    pub window_class: Option<String>,
    pub workspace_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "workspace.switched")]
pub struct HyprlandWorkspaceSwitchedPayload {
    pub from_workspace_id: i32,
    pub to_workspace_id: i32,
    pub monitor_id: i32,
    pub active_window_id: Option<String>,
}

// Additional Hyprland events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.moved")]
pub struct HyprlandWindowMovedPayload {
    pub window_address: String,
    pub new_workspace_id: i32,
    pub moved_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "monitor.focused")]
pub struct HyprlandMonitorFocusedPayload {
    pub monitor_id: i32,
    pub workspace_id: i32,
    pub previous_monitor: Option<i32>,
    pub focused_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "state.captured")]
pub struct HyprlandStateCapturedPayload {
    pub windows: Vec<serde_json::Value>,
    pub workspaces: Vec<serde_json::Value>,
    pub monitors: Vec<serde_json::Value>,
    pub current_workspace: i32,
    pub current_monitor: i32,
    pub captured_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "wm.unhandled")]
pub struct HyprlandUnhandledPayload {
    pub event_type: String,
    pub event_data: String,
}

// Common types

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowGeometry {
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl HyprlandWindowFocusedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            window_id: "test-window-id".into(),
            window_class: "test-class".into(),
            window_title: "Test Window".into(),
            workspace_id: 0,
            previous_window_id: None,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl HyprlandWindowOpenedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            window_id: "test-window-id".into(),
            window_class: "test-class".into(),
            window_title: "Test Window".into(),
            workspace_id: 0,
            monitor_id: 0,
            geometry: WindowGeometry {
                x: 0,
                y: 0,
                width: 800,
                height: 600,
            },
            floating: false,
        }
    }
}
