//! Desktop environment event payloads

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "desktop", event_type = "desktop.monitoring_started")]
pub struct DesktopMonitoringStartedPayload {
    pub clipboard_enabled: bool,
    pub window_manager_enabled: bool,
    pub start_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "desktop", event_type = "desktop.snapshot")]
pub struct DesktopSnapshotPayload {
    pub active_watchers: usize,
    pub clipboard_enabled: bool,
    pub window_manager_enabled: bool,
    pub snapshot_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "desktop", event_type = "clipboard.historical")]
pub struct ClipboardHistoricalPayload {
    pub source: String,
    pub scan_type: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "desktop", event_type = "window.wm_historical")]
pub struct WindowManagerHistoricalPayload {
    pub source: String,
    pub wm_type: String,
    pub scan_type: String,
    pub note: String,
}

impl DesktopMonitoringStartedPayload {
    /// Builder-style method for clipboard enabled
    pub fn with_clipboard_enabled(mut self, enabled: bool) -> Self {
        self.clipboard_enabled = enabled;
        self
    }

    /// Builder-style method for window manager enabled
    pub fn with_window_manager_enabled(mut self, enabled: bool) -> Self {
        self.window_manager_enabled = enabled;
        self
    }

    /// Builder-style method for start time
    pub fn with_start_time(mut self, time: DateTime<Utc>) -> Self {
        self.start_time = time;
        self
    }
}

impl DesktopSnapshotPayload {
    /// Builder-style method for active watchers
    pub fn with_active_watchers(mut self, count: usize) -> Self {
        self.active_watchers = count;
        self
    }

    /// Builder-style method for clipboard enabled
    pub fn with_clipboard_enabled(mut self, enabled: bool) -> Self {
        self.clipboard_enabled = enabled;
        self
    }

    /// Builder-style method for window manager enabled
    pub fn with_window_manager_enabled(mut self, enabled: bool) -> Self {
        self.window_manager_enabled = enabled;
        self
    }

    /// Builder-style method for snapshot time
    pub fn with_snapshot_time(mut self, time: DateTime<Utc>) -> Self {
        self.snapshot_time = time;
        self
    }
}

impl ClipboardHistoricalPayload {
    /// Builder-style method for scan type
    pub fn with_scan_type(mut self, scan_type: impl Into<String>) -> Self {
        self.scan_type = scan_type.into();
        self
    }

    /// Builder-style method for note
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = note.into();
        self
    }
}

impl WindowManagerHistoricalPayload {
    /// Builder-style method for scan type
    pub fn with_scan_type(mut self, scan_type: impl Into<String>) -> Self {
        self.scan_type = scan_type.into();
        self
    }

    /// Builder-style method for note
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = note.into();
        self
    }
}
