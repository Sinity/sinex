//! Content/blob types

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────
// content.store_blob
// ─────────────────────────────────────────────────────────────

/// Request: content.store_blob
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreBlobRequest {
    /// Base64-encoded content
    pub content: String,
    /// Filename
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// MIME content type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Source identifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Response: content.store_blob
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreBlobResponse {
    /// Annex key for retrieval
    pub key: String,
    /// Size in bytes
    pub size: u64,
    /// SHA256 hash
    pub hash: String,
}

// ─────────────────────────────────────────────────────────────
// content.retrieve_blob
// ─────────────────────────────────────────────────────────────

/// Request: content.retrieve_blob
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrieveBlobRequest {
    /// Annex key
    pub key: String,
}

/// Response: content.retrieve_blob
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrieveBlobResponse {
    /// Base64-encoded content
    pub content: String,
    /// MIME content type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Size in bytes
    pub size: u64,
}
