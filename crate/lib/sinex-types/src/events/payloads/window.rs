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

// Common types

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WindowGeometry {
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
}

impl WindowGeometry {
    /// Create a test geometry with sensible defaults
    pub fn test_default() -> Self {
        Self {
            x: 0,
            y: 0,
            width: 800,
            height: 600,
        }
    }
    
    /// Builder-style method for position
    pub fn with_position(mut self, x: i32, y: i32) -> Self {
        self.x = x;
        self.y = y;
        self
    }
    
    /// Builder-style method for size
    pub fn with_size(mut self, width: u32, height: u32) -> Self {
        self.width = width;
        self.height = height;
        self
    }
}

impl HyprlandWindowOpenedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(window_id: impl Into<String>, window_class: impl Into<String>) -> Self {
        Self {
            window_id: window_id.into(),
            window_class: window_class.into(),
            window_title: "Test Window".to_string(),
            workspace_id: 1,
            monitor_id: 0,
            geometry: WindowGeometry::test_default(),
            floating: false,
        }
    }
    
    /// Builder-style method for window title
    pub fn with_window_title(mut self, title: impl Into<String>) -> Self {
        self.window_title = title.into();
        self
    }
    
    /// Builder-style method for workspace
    pub fn with_workspace_id(mut self, workspace_id: i32) -> Self {
        self.workspace_id = workspace_id;
        self
    }
    
    /// Builder-style method for monitor
    pub fn with_monitor_id(mut self, monitor_id: i32) -> Self {
        self.monitor_id = monitor_id;
        self
    }
    
    /// Builder-style method for geometry
    pub fn with_geometry(mut self, geometry: WindowGeometry) -> Self {
        self.geometry = geometry;
        self
    }
    
    /// Builder-style method for floating
    pub fn with_floating(mut self, floating: bool) -> Self {
        self.floating = floating;
        self
    }
}

impl HyprlandWindowClosedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(window_id: impl Into<String>, window_class: impl Into<String>) -> Self {
        Self {
            window_id: window_id.into(),
            window_class: window_class.into(),
            window_title: "Test Window".to_string(),
            workspace_id: 1,
            close_reason: None,
        }
    }
    
    /// Builder-style method for window title
    pub fn with_window_title(mut self, title: impl Into<String>) -> Self {
        self.window_title = title.into();
        self
    }
    
    /// Builder-style method for workspace
    pub fn with_workspace_id(mut self, workspace_id: i32) -> Self {
        self.workspace_id = workspace_id;
        self
    }
    
    /// Builder-style method for close reason
    pub fn with_close_reason(mut self, reason: impl Into<String>) -> Self {
        self.close_reason = Some(reason.into());
        self
    }
}

impl HyprlandWindowFocusedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(window_id: impl Into<String>, window_class: impl Into<String>) -> Self {
        Self {
            window_id: window_id.into(),
            window_class: window_class.into(),
            window_title: "Test Window".to_string(),
            workspace_id: 1,
            previous_window_id: None,
        }
    }
    
    /// Builder-style method for window title
    pub fn with_window_title(mut self, title: impl Into<String>) -> Self {
        self.window_title = title.into();
        self
    }
    
    /// Builder-style method for workspace
    pub fn with_workspace_id(mut self, workspace_id: i32) -> Self {
        self.workspace_id = workspace_id;
        self
    }
    
    /// Builder-style method for previous window
    pub fn with_previous_window_id(mut self, prev_id: impl Into<String>) -> Self {
        self.previous_window_id = Some(prev_id.into());
        self
    }
}

impl HyprlandWorkspaceSwitchedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(from_workspace_id: i32, to_workspace_id: i32) -> Self {
        Self {
            from_workspace_id,
            to_workspace_id,
            monitor_id: 0,
            active_window_id: None,
        }
    }
    
    /// Builder-style method for monitor
    pub fn with_monitor_id(mut self, monitor_id: i32) -> Self {
        self.monitor_id = monitor_id;
        self
    }
    
    /// Builder-style method for active window
    pub fn with_active_window_id(mut self, window_id: impl Into<String>) -> Self {
        self.active_window_id = Some(window_id.into());
        self
    }
}

impl HyprlandWindowMovedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(window_address: impl Into<String>, new_workspace_id: i32) -> Self {
        Self {
            window_address: window_address.into(),
            new_workspace_id,
            moved_at: chrono::Utc::now().to_rfc3339(),
        }
    }
    
    /// Builder-style method for moved timestamp
    pub fn with_moved_at(mut self, timestamp: impl Into<String>) -> Self {
        self.moved_at = timestamp.into();
        self
    }
}

impl HyprlandMonitorFocusedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(monitor_id: i32, workspace_id: i32) -> Self {
        Self {
            monitor_id,
            workspace_id,
            previous_monitor: None,
            focused_at: chrono::Utc::now().to_rfc3339(),
        }
    }
    
    /// Builder-style method for previous monitor
    pub fn with_previous_monitor(mut self, prev_monitor: i32) -> Self {
        self.previous_monitor = Some(prev_monitor);
        self
    }
    
    /// Builder-style method for focused timestamp
    pub fn with_focused_at(mut self, timestamp: impl Into<String>) -> Self {
        self.focused_at = timestamp.into();
        self
    }
}

impl HyprlandStateCapturedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default() -> Self {
        Self {
            windows: vec![],
            workspaces: vec![],
            monitors: vec![],
            current_workspace: 1,
            current_monitor: 0,
            captured_at: chrono::Utc::now().to_rfc3339(),
        }
    }
    
    /// Builder-style method for windows
    pub fn with_windows(mut self, windows: Vec<serde_json::Value>) -> Self {
        self.windows = windows;
        self
    }
    
    /// Builder-style method for workspaces
    pub fn with_workspaces(mut self, workspaces: Vec<serde_json::Value>) -> Self {
        self.workspaces = workspaces;
        self
    }
    
    /// Builder-style method for monitors
    pub fn with_monitors(mut self, monitors: Vec<serde_json::Value>) -> Self {
        self.monitors = monitors;
        self
    }
    
    /// Builder-style method for current workspace
    pub fn with_current_workspace(mut self, workspace_id: i32) -> Self {
        self.current_workspace = workspace_id;
        self
    }
    
    /// Builder-style method for current monitor
    pub fn with_current_monitor(mut self, monitor_id: i32) -> Self {
        self.current_monitor = monitor_id;
        self
    }
}
