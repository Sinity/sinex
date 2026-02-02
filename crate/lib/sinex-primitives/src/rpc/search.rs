//! Search types

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─────────────────────────────────────────────────────────────
// search.search_events
// ─────────────────────────────────────────────────────────────

/// Request: `search.search_events`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchEventsRequest {
    /// Text to search for
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Filter by sources
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
    /// Filter by event types
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_types: Vec<String>,
    /// Start time (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    /// End time (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    /// Maximum results (default: 100)
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Offset for pagination
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    100
}

/// Search result entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: String,
    pub source: String,
    pub event_type: String,
    pub timestamp: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// Response: `search.search_events`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEventsResponse {
    pub results: Vec<SearchResult>,
    pub total: i64,
}
