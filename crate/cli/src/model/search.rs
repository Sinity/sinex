use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::Ulid;

/// Search query parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub text: Option<String>,
    pub sources: Vec<String>,
    pub event_types: Vec<String>,
    pub start_time: Option<Timestamp>,
    pub end_time: Option<Timestamp>,
    pub limit: i32,
    pub offset: i32,
}

impl Default for SearchQuery {
    fn default() -> Self {
        Self {
            text: None,
            sources: Vec::new(),
            event_types: Vec::new(),
            start_time: None,
            end_time: None,
            limit: 100,
            offset: 0,
        }
    }
}

/// Search result entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub event_id: Ulid,
    pub source: String,
    pub event_type: String,
    pub host: String,
    pub timestamp: Timestamp,
    pub snippet: String,
    pub score: f64,
}
