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
