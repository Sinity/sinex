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
    /// Hyprland sends workspace_id as a string; parsed to i32.
    pub workspace_id: Option<i32>,
    pub workspace_name: Option<String>,
    pub monitor_id: Option<i32>,
    pub geometry: Option<WindowGeometry>,
    pub floating: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.closed")]
pub struct HyprlandWindowClosedPayload {
    /// Only field Hyprland provides in the closewindow IPC event.
    pub window_id: String,
    /// Not available from Hyprland's closewindow event.
    pub window_class: Option<String>,
    /// Not available from Hyprland's closewindow event.
    pub window_title: Option<String>,
    /// Not available from Hyprland's closewindow event.
    pub workspace_id: Option<i32>,
    pub close_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.focused")]
pub struct HyprlandWindowFocusedPayload {
    /// From activewindowv2 (merged). May be absent if v2 never arrives.
    pub window_id: Option<String>,
    /// From activewindow (v1).
    pub window_class: Option<String>,
    /// From activewindow (v1).
    pub window_title: Option<String>,
    pub workspace_id: Option<i32>,
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
#[event_payload(source = "wm.hyprland", event_type = "window.urgent")]
pub struct HyprlandWindowUrgentPayload {
    pub window_id: String,
    pub window_class: Option<String>,
    pub window_title: Option<String>,
    pub workspace_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.fullscreen_changed")]
pub struct HyprlandWindowFullscreenChangedPayload {
    pub state: i32,
    pub fullscreen: bool,
    pub window_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.floating_changed")]
pub struct HyprlandWindowFloatingChangedPayload {
    pub window_id: String,
    pub floating: bool,
    pub window_class: Option<String>,
    pub window_title: Option<String>,
    pub workspace_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.minimize")]
pub struct HyprlandWindowMinimizePayload {
    pub window_id: String,
    pub minimized: bool,
    pub window_class: Option<String>,
    pub window_title: Option<String>,
    pub workspace_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "screencast")]
pub struct HyprlandScreencastPayload {
    pub active: bool,
    pub owner: Option<String>,
}

/// Hyprland `workspace`/`workspacev2` event: only the destination workspace is known.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "workspace.switched")]
pub struct HyprlandWorkspaceSwitchedPayload {
    /// Destination workspace ID (parsed from the Hyprland workspace IPC string).
    pub to_workspace_id: i32,
    pub workspace_name: Option<String>,
    /// Not provided by Hyprland's workspace IPC event.
    pub from_workspace_id: Option<i32>,
    /// Not provided by Hyprland's workspace IPC event.
    pub monitor_id: Option<i32>,
    pub active_window_id: Option<String>,
}

// Additional Hyprland events

/// Hyprland `movewindow` event: address + destination workspace.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "window.moved")]
pub struct HyprlandWindowMovedPayload {
    pub window_id: String,
    pub workspace_id: Option<i32>,
    pub workspace_name: Option<String>,
}

/// Hyprland `focusedmon`/`focusedmonv2` event: monitor name + workspace name (not IDs).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "monitor.focused")]
pub struct HyprlandMonitorFocusedPayload {
    /// Monitor name as provided by Hyprland (e.g. "DP-1"), not an integer ID.
    pub monitor_name: String,
    /// Workspace name as provided by Hyprland.
    pub workspace_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "layer.opened")]
pub struct HyprlandLayerOpenedPayload {
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "layer.closed")]
pub struct HyprlandLayerClosedPayload {
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "wm.hyprland", event_type = "submap.changed")]
pub struct HyprlandSubmapChangedPayload {
    pub submap: String,
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
            window_id: Some("test-window-id".into()),
            window_class: Some("test-class".into()),
            window_title: Some("Test Window".into()),
            workspace_id: Some(0),
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
            workspace_id: Some(0),
            workspace_name: None,
            monitor_id: Some(0),
            geometry: Some(WindowGeometry {
                x: 0,
                y: 0,
                width: 800,
                height: 600,
            }),
            floating: Some(false),
        }
    }
}
