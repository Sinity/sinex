//! Integration bridge event payloads.
//!
//! Covers metadata-only events emitted by external producer daemons that bridge
//! external data sources into sinex via NATS without depending on the Rust SDK.
//!
//! # Polylogue bridge (#1122)
//!
//! The Polylogue daemon publishes [`PolylogueConversationIndexedPayload`] events
//! when a conversation is indexed or re-indexed. Only metadata is included —
//! raw conversation text is never sent through sinex.

use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

// ─────────────────────────────────────────────────────────────────────────────
// Polylogue bridge
// ─────────────────────────────────────────────────────────────────────────────

/// Metadata snapshot emitted by the Polylogue daemon when a conversation is
/// indexed or re-indexed in the archive.
///
/// Raw conversation text is **not** included. Only structural metadata and
/// derived signals are present, keeping the payload at privacy tier Sensitive
/// rather than Secret.
///
/// Published to:
/// `{env}.sinex.events.raw.integration.polylogue.conversation_indexed`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "integration.polylogue",
    event_type = "integration.polylogue.conversation_indexed"
)]
pub struct PolylogueConversationIndexedPayload {
    /// Stable polylogue conversation identifier (opaque string, typically a UUID).
    pub conversation_id: String,

    /// AI provider that hosted the conversation.
    ///
    /// Known values: `"claude"`, `"chatgpt"`, `"codex"`, `"gemini"`.
    pub provider: String,

    /// Conversation title as set by the provider or the user.
    ///
    /// Absent for untitled conversations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// User-applied and auto-inferred tags.
    #[serde(default)]
    pub tags: Vec<String>,

    /// SHA-256 hex digest of the canonical conversation content.
    ///
    /// Computed by the Polylogue daemon. Changes when any message is added,
    /// edited, or deleted. Used as the stable occurrence identity together with
    /// `conversation_id`.
    pub content_hash: String,

    /// When the conversation was created (from provider metadata).
    pub created_at: Timestamp,

    /// When the conversation was last updated (from provider metadata).
    pub updated_at: Timestamp,

    /// Total number of messages in the conversation.
    pub message_count: u32,

    /// Estimated conversation cost in USD, if the provider exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,

    /// Primary model slug used in the conversation (e.g. `"claude-opus-4-5"`).
    ///
    /// Absent when the provider does not expose model information.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_slug: Option<String>,
}
