//! Event RPC types for `events.*` methods.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventsAnnotateRequest {
    pub event_id: String,
    pub annotation_type: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventsAnnotateResponse {
    pub id: String,
    pub event_id: String,
    pub annotation_type: String,
    pub content: String,
    pub metadata: Value,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
}
