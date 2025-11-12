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
