//! DLQ (Dead Letter Queue) management types

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────
// dlq.list
// ─────────────────────────────────────────────────────────────

/// Request: dlq.list (no params required)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DlqListRequest {}

/// Response: dlq.list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqListResponse {
    pub total_messages: u64,
    pub total_bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
}

// ─────────────────────────────────────────────────────────────
// dlq.peek
// ─────────────────────────────────────────────────────────────

/// Request: dlq.peek
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqPeekRequest {
    #[serde(default = "default_peek_limit")]
    pub limit: usize,
}

fn default_peek_limit() -> usize {
    10
}

impl Default for DlqPeekRequest {
    fn default() -> Self {
        Self {
            limit: default_peek_limit(),
        }
    }
}

/// A single DLQ message preview
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessagePeek {
    pub subject: String,
    pub sequence: u64,
    pub retry_count: u32,
    pub original_subject: Option<String>,
    pub payload_preview: String,
}

/// Response: dlq.peek
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqPeekResponse {
    pub messages: Vec<DlqMessagePeek>,
}

// ─────────────────────────────────────────────────────────────
// dlq.requeue
// ─────────────────────────────────────────────────────────────

/// Request: dlq.requeue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRequeueRequest {
    /// Optional event ID to requeue specific message
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// Requeue all DLQ messages
    #[serde(default)]
    pub all: bool,
}

/// Response: dlq.requeue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRequeueResponse {
    pub status: String,
    pub requeued_count: u64,
}

// ─────────────────────────────────────────────────────────────
// dlq.purge
// ─────────────────────────────────────────────────────────────

/// Request: dlq.purge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqPurgeRequest {
    /// Must be true to confirm purge
    pub confirm: bool,
}

/// Response: dlq.purge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqPurgeResponse {
    pub status: String,
    pub purged_count: u64,
}
