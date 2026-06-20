//! Browser and web navigation event payloads.

use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "webhistory", event_type = "page.visited")]
pub struct PageVisitedPayload {
    pub browser: String,
    pub title: String,
    pub url: String,
    pub normalized_url: Option<String>,
    pub visit_time: Timestamp,
    pub referrer: Option<String>,
    pub transition: Option<String>,
    pub visit_id: Option<String>,
    pub visit_duration_ms: Option<u64>,
    pub source_file: String,
    pub line_number: Option<u64>,
    pub db_row_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "browser", event_type = "navigation.observed")]
pub struct BrowserNavigationObservedPayload {
    pub profile_id: String,
    pub producer_instance_id: String,
    pub batch_id: String,
    pub sequence: u64,
    pub observed_at: Timestamp,
    pub url: String,
    pub title: Option<String>,
    pub tab_id: Option<i64>,
    pub window_id: Option<i64>,
    pub transition: Option<String>,
    pub referrer: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "browser", event_type = "tab.activated")]
pub struct BrowserTabActivatedPayload {
    pub profile_id: String,
    pub producer_instance_id: String,
    pub batch_id: String,
    pub sequence: u64,
    pub observed_at: Timestamp,
    pub tab_id: i64,
    pub window_id: Option<i64>,
    pub url: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "browser", event_type = "download.observed")]
pub struct BrowserDownloadObservedPayload {
    pub profile_id: String,
    pub producer_instance_id: String,
    pub batch_id: String,
    pub sequence: u64,
    pub observed_at: Timestamp,
    pub download_id: String,
    pub url: String,
    pub filename: Option<String>,
    pub state: Option<String>,
    pub total_bytes: Option<u64>,
}
