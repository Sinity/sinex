//! Raw-ingest DLQ management types

use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use crate::runtime_pressure::{RuntimePressureAction, RuntimePressureLevel};
use crate::views::CaveatView;
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────
// dlq.list
// ─────────────────────────────────────────────────────────────

pub const DLQ_LIST_METHOD: RpcMethod<DlqListRequest, DlqListResponse> = RpcMethod::new(
    methods::DLQ_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Dlq,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

/// Request: `dlq.list` for the raw-ingest DLQ (no params required)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DlqListRequest {}

/// Structured pressure signal for raw-ingest DLQ depth.
///
/// This is intentionally scoped to the DLQ response instead of introducing a
/// parallel runtime scheduler model. It gives operator surfaces a typed view of
/// the same decision already exposed by `pressure_level`, `recommended_action`,
/// and `action_reason`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqPressureSignal {
    pub pressure_level: RuntimePressureLevel,
    pub runtime_action: RuntimePressureAction,
    pub pending_messages: u64,
    pub pending_bytes: u64,
    pub retry_batch_size: u64,
    pub recommended_action: String,
    pub reason: String,
}

/// Response: dlq.list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqListResponse {
    pub total_messages: u64,
    pub total_bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    /// Depth-derived pressure classification for operator surfaces.
    ///
    /// Empty DLQ is `nominal`; non-empty DLQ is `warning`; depth beyond the
    /// retry batch size is `critical` because recovery requires more than one
    /// paced operator retry batch.
    pub pressure_level: RuntimePressureLevel,
    /// Structured pressure signal for runtime/operator surfaces.
    pub resource_pressure: DlqPressureSignal,
    /// Sequence span covered by pending DLQ messages. This is the DLQ lag
    /// signal when timestamps are not available from the stream summary.
    pub pending_sequence_span: u64,
    /// Next practical operator action for the current DLQ state.
    pub recommended_action: String,
    /// Human-readable reason for the recommended action or its absence.
    pub action_reason: String,
}

// ─────────────────────────────────────────────────────────────
// dlq.peek
// ─────────────────────────────────────────────────────────────

pub const DLQ_PEEK_METHOD: RpcMethod<DlqPeekRequest, DlqPeekResponse> = RpcMethod::new(
    methods::DLQ_PEEK,
    RpcRole::ReadOnly,
    RpcDomain::Dlq,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

/// Request: dlq.peek
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqPeekRequest {
    #[serde(default = "default_peek_limit")]
    pub limit: usize,
    /// Maximum characters of each sanitized payload preview to return.
    ///
    /// The server applies disclosure policy before truncation. Keep the default
    /// compact for human tables, and let projection commands request a wider
    /// bounded preview when the failure classifier depends on nested context.
    #[serde(default = "default_payload_preview_chars")]
    pub payload_preview_chars: usize,
    /// Optional DLQ stream sequence to start peeking from.
    ///
    /// When omitted, the server returns the oldest retained DLQ messages.
    /// Operator surfaces can use this to inspect the current tail without
    /// introducing a parallel "latest failures" endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_sequence: Option<u64>,
}

fn default_peek_limit() -> usize {
    10
}

fn default_payload_preview_chars() -> usize {
    200
}

impl Default for DlqPeekRequest {
    fn default() -> Self {
        Self {
            limit: default_peek_limit(),
            payload_preview_chars: default_payload_preview_chars(),
            start_sequence: None,
        }
    }
}

/// A single raw-ingest DLQ message preview
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessagePeek {
    pub subject: String,
    pub sequence: u64,
    pub retry_count: u32,
    pub original_subject: Option<String>,
    pub payload_preview: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub payload_redacted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub privacy_caveats: Vec<CaveatView>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Grouped explanation of similar raw-ingest DLQ message previews.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqMessageGroup {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_subject: Option<String>,
    pub reason_bucket: String,
    pub count: usize,
    pub first_sequence: u64,
    pub last_sequence: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_previews: Vec<String>,
}

impl DlqMessageGroup {
    fn new(message: &DlqMessagePeek) -> Self {
        Self {
            original_subject: message.original_subject.clone(),
            reason_bucket: dlq_reason_bucket(&message.payload_preview),
            count: 1,
            first_sequence: message.sequence,
            last_sequence: message.sequence,
            sample_previews: vec![message.payload_preview.clone()],
        }
    }

    fn add(&mut self, message: &DlqMessagePeek) {
        self.count += 1;
        self.first_sequence = self.first_sequence.min(message.sequence);
        self.last_sequence = self.last_sequence.max(message.sequence);
        if self.sample_previews.len() < 3
            && !self
                .sample_previews
                .iter()
                .any(|preview| preview == &message.payload_preview)
        {
            self.sample_previews.push(message.payload_preview.clone());
        }
    }
}

/// Response: dlq.peek
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqPeekResponse {
    pub messages: Vec<DlqMessagePeek>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<DlqMessageGroup>,
}

impl DlqPeekResponse {
    pub fn from_messages(messages: Vec<DlqMessagePeek>) -> Self {
        let groups = group_dlq_messages(&messages);
        Self { messages, groups }
    }
}

fn group_dlq_messages(messages: &[DlqMessagePeek]) -> Vec<DlqMessageGroup> {
    let mut groups: Vec<DlqMessageGroup> = Vec::new();
    for message in messages {
        let reason_bucket = dlq_reason_bucket(&message.payload_preview);
        if let Some(existing) = groups.iter_mut().find(|group| {
            group.original_subject == message.original_subject && group.reason_bucket == reason_bucket
        }) {
            existing.add(message);
        } else {
            groups.push(DlqMessageGroup::new(message));
        }
    }
    groups.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| a.first_sequence.cmp(&b.first_sequence))
    });
    groups
}

fn dlq_reason_bucket(preview: &str) -> String {
    if preview.contains("equivalence_key") && preview.contains("already exists") {
        "occurrence_duplicate.equivalence_key_exists".to_string()
    } else if let Some(error_code) = preview_error_code(preview) {
        format!("error_payload.{error_code}")
    } else if preview.contains("\"error\"") {
        "error_payload.unparsed".to_string()
    } else if preview.contains("[payload contains dangerous Unicode characters]") {
        "unsafe_unicode_preview".to_string()
    } else if preview.is_empty() {
        "empty_preview".to_string()
    } else {
        "unclassified_preview".to_string()
    }
}

fn preview_error_code(preview: &str) -> Option<String> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(preview)
        && let Some(error) = value.get("error").and_then(|error| error.as_str())
    {
        return Some(sanitize_reason_token(error));
    }
    None
}

fn sanitize_reason_token(value: &str) -> String {
    let mut token = String::with_capacity(value.len().min(64));
    for ch in value.chars().take(64) {
        if ch.is_ascii_alphanumeric() {
            token.push(ch.to_ascii_lowercase());
        } else if matches!(ch, '_' | '-' | '.') {
            token.push(ch);
        } else if !token.ends_with('_') {
            token.push('_');
        }
    }
    token.trim_matches('_').to_string()
}

// ─────────────────────────────────────────────────────────────
// dlq.requeue
// ─────────────────────────────────────────────────────────────

pub const DLQ_REQUEUE_METHOD: RpcMethod<DlqRequeueRequest, DlqRequeueResponse> = RpcMethod::new(
    methods::DLQ_REQUEUE,
    RpcRole::Admin,
    RpcDomain::Dlq,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

/// Request: dlq.requeue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRequeueRequest {
    /// Optional event ID to requeue a specific raw-ingest DLQ message
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// Inclusive first DLQ stream sequence to requeue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_sequence: Option<u64>,
    /// Inclusive last DLQ stream sequence to requeue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_sequence: Option<u64>,
    /// Requeue all raw-ingest DLQ messages
    #[serde(default)]
    pub all: bool,
}

/// Response: dlq.requeue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRequeueResponse {
    pub status: String,
    pub requeued_count: u64,
    pub operation_id: String,
}

// ─────────────────────────────────────────────────────────────
// dlq.purge
// ─────────────────────────────────────────────────────────────

pub const DLQ_PURGE_METHOD: RpcMethod<DlqPurgeRequest, DlqPurgeResponse> = RpcMethod::new(
    methods::DLQ_PURGE,
    RpcRole::Admin,
    RpcDomain::Dlq,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

/// Request: dlq.purge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqPurgeRequest {
    /// Must be true to confirm purge
    pub confirm: bool,
    /// Inclusive first DLQ stream sequence to purge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_sequence: Option<u64>,
    /// Inclusive last DLQ stream sequence to purge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_sequence: Option<u64>,
}

/// Response: dlq.purge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqPurgeResponse {
    pub status: String,
    pub purged_count: u64,
    pub operation_id: String,
}
