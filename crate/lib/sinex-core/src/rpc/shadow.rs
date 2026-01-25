//! Shadow consumer types for The Tether

use serde::{Deserialize, Serialize};

/// Shadow consumer info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowConsumerInfo {
    pub consumer_name: String,
    pub stream_name: String,
    pub subject_filter: String,
    pub num_pending: u64,
    pub first_sequence: u64,
}

// ─────────────────────────────────────────────────────────────
// shadow.create
// ─────────────────────────────────────────────────────────────

/// Request: shadow.create
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowCreateRequest {
    /// Unique identifier (must start with "dev-")
    pub consumer_name: String,
    /// Subject filter pattern
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_filter: Option<String>,
    /// Start from beginning of stream
    pub from_beginning: bool,
    /// Start from specific sequence
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_sequence: Option<u64>,
}

/// Response: shadow.create
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowCreateResponse {
    pub consumer: ShadowConsumerInfo,
}

// ─────────────────────────────────────────────────────────────
// shadow.list
// ─────────────────────────────────────────────────────────────

/// Request: shadow.list
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShadowListRequest {
    /// Optional prefix filter
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

/// Response: shadow.list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowListResponse {
    pub consumers: Vec<ShadowConsumerInfo>,
}

// ─────────────────────────────────────────────────────────────
// shadow.delete
// ─────────────────────────────────────────────────────────────

/// Request: shadow.delete
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowDeleteRequest {
    pub consumer_name: String,
}

/// Response: shadow.delete
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowDeleteResponse {
    pub status: String,
    pub consumer_name: String,
}
