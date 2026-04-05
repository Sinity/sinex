//! Desktop environment event payloads

use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "desktop", event_type = "desktop.monitoring_started")]
pub struct DesktopMonitoringStartedPayload {
    pub clipboard_enabled: bool,
    pub window_manager_enabled: bool,
    pub start_time: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "desktop", event_type = "desktop.snapshot")]
pub struct DesktopSnapshotPayload {
    pub active_watchers: usize,
    pub clipboard_enabled: bool,
    pub window_manager_enabled: bool,
    pub snapshot_time: Timestamp,
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "activitywatch", event_type = "window.active")]
pub struct ActivityWatchWindowActivePayload {
    pub app: String,
    pub title: String,
    pub duration_ms: u64,
    pub bucket_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "activitywatch", event_type = "browser.tab.active")]
pub struct ActivityWatchBrowserTabActivePayload {
    pub browser: String,
    pub title: String,
    pub url: String,
    pub duration_ms: u64,
    pub bucket_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "webhistory", event_type = "browser.history.imported")]
pub struct BrowserHistoryImportedPayload {
    pub browser: String,
    pub title: String,
    pub url: String,
    pub normalized_url: Option<String>,
    pub source_file: String,
    pub line_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "activitywatch", event_type = "afk.changed")]
pub struct ActivityWatchAfkChangedPayload {
    pub status: String,
    pub duration_ms: u64,
    pub bucket_id: String,
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl DesktopMonitoringStartedPayload {
    #[must_use]
    pub fn test_default() -> Self {
        Self {
            clipboard_enabled: true,
            window_manager_enabled: true,
            start_time: crate::temporal::now(),
        }
    }
}
