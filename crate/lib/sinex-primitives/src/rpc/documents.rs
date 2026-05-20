//! RPC request/response types for the `documents.*` method namespace.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::Timestamp;
use crate::Uuid;
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};

pub const DOCUMENTS_SEARCH_METHOD: RpcMethod<DocumentsSearchRequest, DocumentsSearchResponse> =
    RpcMethod::new(
        methods::DOCUMENTS_SEARCH,
        RpcRole::ReadOnly,
        RpcDomain::Documents,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const DOCUMENTS_GET_METHOD: RpcMethod<DocumentsGetRequest, DocumentsGetResponse> =
    RpcMethod::new(
        methods::DOCUMENTS_GET,
        RpcRole::ReadOnly,
        RpcDomain::Documents,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const DOCUMENTS_GET_CHUNKS_METHOD: RpcMethod<
    DocumentsGetChunksRequest,
    DocumentsGetChunksResponse,
> = RpcMethod::new(
    methods::DOCUMENTS_GET_CHUNKS,
    RpcRole::ReadOnly,
    RpcDomain::Documents,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

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
    /// True when another page can be requested with `next_offset`.
    #[serde(default)]
    pub has_more: bool,
    /// Offset to pass on the next request when `has_more` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u64>,
    /// `"no_indexed_text"` or `"no_match"` when `results` is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty_reason: Option<String>,
}

/// Request for `documents.get`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsGetRequest {
    pub id: Uuid,
}

/// A single document record returned by `documents.get`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsGetResponse {
    pub id: Uuid,
    pub kind: String,
    pub natural_key: String,
    pub extraction_version: i32,
    pub chunk_count: i32,
    pub text_byte_len: i64,
    pub side_data: JsonValue,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
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

/// A single chunk record returned within `documents.get_chunks`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsChunkEntry {
    pub document_id: Uuid,
    pub chunk_index: i32,
    pub text: String,
    pub byte_offset_start: i64,
    pub byte_offset_end: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_anchor_start: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_anchor_end: Option<i64>,
}

/// Response for `documents.get_chunks`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentsGetChunksResponse {
    pub document_id: Uuid,
    pub chunks: Vec<DocumentsChunkEntry>,
}
