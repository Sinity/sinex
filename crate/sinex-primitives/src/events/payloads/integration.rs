//! Integration bridge event payloads.
//!
//! Covers metadata-only events emitted by external producer daemons that bridge
//! external data sources into sinex via NATS without depending on the Rust runtime.
//!
//! # Polylogue bridge (#1122)
//!
//! The Polylogue daemon publishes [`PolylogueSessionIndexedPayload`] events
//! when a session is indexed or re-indexed. Only metadata is included —
//! raw session text is never sent through sinex.

use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

// ─────────────────────────────────────────────────────────────────────────────
// Polylogue bridge
// ─────────────────────────────────────────────────────────────────────────────

/// Metadata snapshot emitted by the Polylogue daemon when a session is
/// indexed or re-indexed in the archive.
///
/// Raw session text is **not** included. Only structural metadata and
/// derived signals are present, keeping the payload at privacy tier Sensitive
/// rather than Secret.
///
/// Published to:
/// `{env}.sinex.events.raw.integration.polylogue.session_indexed`
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(
    source = "integration.polylogue",
    event_type = "integration.polylogue.session_indexed"
)]
pub struct PolylogueSessionIndexedPayload {
    /// Stable polylogue session identifier (opaque string, typically a UUID).
    pub session_id: String,

    /// AI provider origin that hosted the session.
    ///
    /// Known values: `"claude"`, `"chatgpt"`, `"codex"`, `"gemini"`.
    pub origin: String,

    /// Session title as set by the provider or the user.
    ///
    /// Absent for untitled sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// User-applied and auto-inferred tags.
    #[serde(default)]
    pub tags: Vec<String>,

    /// SHA-256 hex digest of the canonical session content.
    ///
    /// Computed by the Polylogue daemon. Changes when any message is added,
    /// edited, or deleted. Used as the stable occurrence identity together with
    /// `session_id`.
    pub content_hash: String,

    /// When the session was created (from provider metadata).
    pub created_at: Timestamp,

    /// When the session was last updated (from provider metadata).
    pub updated_at: Timestamp,

    /// Total number of messages in the session.
    pub message_count: u32,

    /// Estimated session cost in USD, if the provider exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,

    /// Primary model slug used in the session (e.g. `"claude-opus-4-5"`).
    ///
    /// Absent when the provider does not expose model information.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_slug: Option<String>,
}
