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

/// ActivityWatch mutates its most-recently-inserted row per bucket in place
/// via heartbeat merging (`endtime` — and therefore `duration_ms` — grows
/// without a new `SQLite` row). `supersede_on_change` lets a later re-read of
/// the same start-anchored occurrence (see
/// `ActivityWatchParser::parse_record`'s `occurrence_key`, keyed on
/// `bucket_id` + start timestamp, never `endtime`) archive the stale short
/// interpretation and admit the grown one instead of duplicating it
/// (sinex-h3g, ruling sinex-y8v).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "activitywatch",
    event_type = "window.active",
    revision_policy = "supersede_on_change"
)]
pub struct ActivityWatchWindowActivePayload {
    pub app: String,
    pub title: String,
    pub duration_ms: u64,
    pub bucket_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "activitywatch",
    event_type = "browser.tab.active",
    revision_policy = "supersede_on_change"
)]
pub struct ActivityWatchBrowserTabActivePayload {
    pub browser: String,
    pub title: String,
    pub url: String,
    pub duration_ms: u64,
    pub bucket_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "activitywatch",
    event_type = "afk.changed",
    revision_policy = "supersede_on_change"
)]
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
