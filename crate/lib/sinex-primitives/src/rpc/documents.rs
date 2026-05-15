//! RPC request/response types for the `documents.*` method namespace.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::Timestamp;
use crate::Uuid;

/// Request for `documents.search`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsSearchRequest {
    /// Free-text query parsed by `websearch_to_tsquery('english', ...)`.
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document_ids: Option<Vec<Uuid>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub natural_key_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_after: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_before: Option<Timestamp>,
    /// Page size; capped at 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Zero-based page offset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
}

/// A single ranked chunk hit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsSearchResult {
    pub document_id: Uuid,
    pub kind: String,
    pub natural_key: String,
    pub chunk_index: i32,
    /// `ts_headline` with `<mark>`/`</mark>` tags (raw chunk text on trigram path).
    pub headline: String,
    /// Full chunk text.
    pub text: String,
    pub score: f64,
    pub byte_offset_start: i64,
    pub byte_offset_end: i64,
    pub extraction_version: i32,
    pub side_data: JsonValue,
    pub updated_at: Timestamp,
}

/// Response for `documents.search`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsSearchResponse {
    pub results: Vec<DocumentsSearchResult>,
    /// `"fts"` or `"trigram_fallback"`.
    pub search_mode: String,
}

/// Request for `documents.get`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsGetRequest {
    pub id: Uuid,
}

/// Request for `documents.get_chunks`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsGetChunksRequest {
    pub document_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
}
