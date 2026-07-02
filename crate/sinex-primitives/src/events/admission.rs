//! Event admission envelope for durable transport.
//!
//! This module defines the `EventIntent` envelope that producers construct
//! to declare "I've done my admission checks, here's the payload." The envelope
//! complements (does not replace) the event-engine-side admission boundary extracted in
//! #1056 — the producer uses the envelope type (compile-time guard), and event_engine
//! validates it on receipt (runtime guard).
//!
//! # Envelope versioning
//!
//! The envelope carries an explicit version string. event_engine rejects unknown
//! versions, giving us a forward-compatible migration path.

use crate::domain::HostName;
use crate::events::Event;
use crate::primitives::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Current envelope version emitted by all producers.
pub const CURRENT_ENVELOPE_VERSION: &str = "1";

/// Envelope versions that event_engine will accept on receipt.
pub const ACCEPTED_ENVELOPE_VERSIONS: &[&str] = &["1"];

/// Default envelope version for backward-compatible deserialization.
///
/// Pre-#1149 producers did not include `envelope_version` in the serialized
/// envelope. Serde uses this function when the field is absent, so old messages
/// round-trip correctly instead of failing with "missing field".
fn default_envelope_version() -> String {
    CURRENT_ENVELOPE_VERSION.to_string()
}

/// The kind of anchor that identifies an occurrence within source material.
///
/// Each variant describes a different coordinate system for locating a stable
/// real-world occurrence within a source material artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OccurrenceAnchorKind {
    /// Byte offset from the start of the material.
    ByteOffset,
    /// `SQLite` row ID within a database table.
    SqliteRow,
    /// Line number (1-based) within a text stream.
    LineNumber,
    /// Sequence number within an ordered stream.
    SequenceNumber,
    /// Domain-specific natural key (stored in the `natural_key` column).
    NaturalKey,
    /// Opaque cursor or continuation token from a paginated API.
    CursorToken,
    /// Git object identifier (full SHA).
    GitOid,
    /// Frame identifier within a multiplexed stream.
    StreamFrame,
}

impl OccurrenceAnchorKind {
    /// Wire-format string for this anchor kind.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            OccurrenceAnchorKind::ByteOffset => "byte_offset",
            OccurrenceAnchorKind::SqliteRow => "sqlite_row",
            OccurrenceAnchorKind::LineNumber => "line_number",
            OccurrenceAnchorKind::SequenceNumber => "sequence_number",
            OccurrenceAnchorKind::NaturalKey => "natural_key",
            OccurrenceAnchorKind::CursorToken => "cursor_token",
            OccurrenceAnchorKind::GitOid => "git_oid",
            OccurrenceAnchorKind::StreamFrame => "stream_frame",
        }
    }

    /// Parse from a wire-format string.
    pub fn try_from_str(s: &str) -> Result<Self, crate::SinexError> {
        match s {
            "byte_offset" => Ok(OccurrenceAnchorKind::ByteOffset),
            "sqlite_row" => Ok(OccurrenceAnchorKind::SqliteRow),
            "line_number" => Ok(OccurrenceAnchorKind::LineNumber),
            "sequence_number" => Ok(OccurrenceAnchorKind::SequenceNumber),
            "natural_key" => Ok(OccurrenceAnchorKind::NaturalKey),
            "cursor_token" => Ok(OccurrenceAnchorKind::CursorToken),
            "git_oid" => Ok(OccurrenceAnchorKind::GitOid),
            "stream_frame" => Ok(OccurrenceAnchorKind::StreamFrame),
            _ => Err(
                crate::SinexError::validation("invalid occurrence anchor kind")
                    .with_context("anchor_kind", s.to_string()),
            ),
        }
    }
}

impl std::fmt::Display for OccurrenceAnchorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for OccurrenceAnchorKind {
    type Err = crate::SinexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from_str(s)
    }
}

/// An admitted event intent — the producer's declaration that admission checks
/// have been performed on these events.
///
/// This is the envelope that producers publish to NATS `JetStream` instead of raw
/// `Event` batches. It carries:
/// - **Envelope metadata** (version, source, parser identity)
/// - **The admitted events** — one or more `Event<JsonValue>` entries
///
/// The type system makes it hard to accidentally publish raw events to durable
/// transport: every normal publish path requires this envelope.
///
/// For tests, fixtures, and bootstrap, use `NatsPublisher::publish_raw_event_batch`
/// — a grep-detectable escape hatch that is absent from production producer code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventIntent {
    /// Envelope version — currently "1".
    ///
    /// Pre-#1149 messages did not carry this field; `default_envelope_version`
    /// supplies `"1"` so they deserialize correctly rather than hitting DLQ.
    #[serde(default = "default_envelope_version")]
    pub envelope_version: String,

    /// Source package / producer binding identifier (e.g., "fs-watcher",
    /// "terminal-source").
    ///
    /// This is transport and parser provenance for the producer that emitted
    /// the intent. It is not the semantic event contract coordinate. When a
    /// registered source contract exists, this should reference
    /// `SourceContract::id`; admission policy should still use explicit event
    /// contract ids for semantic authority.
    pub source_id: String,

    /// Parser that interpreted the source material (e.g., "inotify-watcher",
    /// "atuin-history-parser").
    pub parser_id: String,

    /// Parser version (semver, e.g., "1.2.0").
    pub parser_version: String,

    /// The events that passed producer-side admission checks.
    /// Must contain at least one event.
    pub events: Vec<Event<JsonValue>>,

    /// Wall-clock time when this intent was created (host local).
    pub admitted_at: Timestamp,

    /// Host that performed the admission checks.
    pub admitted_by: HostName,
}

impl EventIntent {
    /// Create a new admitted event intent with the current envelope version.
    pub fn new(
        source_id: impl Into<String>,
        parser_id: impl Into<String>,
        parser_version: impl Into<String>,
        events: Vec<Event<JsonValue>>,
        admitted_by: HostName,
    ) -> Self {
        Self {
            envelope_version: CURRENT_ENVELOPE_VERSION.to_string(),
            source_id: source_id.into(),
            parser_id: parser_id.into(),
            parser_version: parser_version.into(),
            events,
            admitted_at: Timestamp::now(),
            admitted_by,
        }
    }

    /// Check whether this envelope version is accepted by event_engine.
    #[must_use]
    pub fn is_version_accepted(&self) -> bool {
        ACCEPTED_ENVELOPE_VERSIONS.contains(&self.envelope_version.as_str())
    }

    /// Validate that this intent has the required fields populated.
    pub fn validate(&self) -> Result<(), crate::SinexError> {
        if self.envelope_version.trim().is_empty() {
            return Err(crate::SinexError::validation(
                "admitted event intent missing envelope_version",
            ));
        }
        if self.source_id.trim().is_empty() {
            return Err(crate::SinexError::validation(
                "admitted event intent missing source_id",
            ));
        }
        if self.parser_id.trim().is_empty() {
            return Err(crate::SinexError::validation(
                "admitted event intent missing parser_id",
            ));
        }
        if self.parser_version.trim().is_empty() {
            return Err(crate::SinexError::validation(
                "admitted event intent missing parser_version",
            ));
        }
        if self.events.is_empty() {
            return Err(crate::SinexError::validation(
                "admitted event intent has no events",
            ));
        }
        Ok(())
    }

    /// Number of events in this intent.
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Collect all event IDs from this intent.
    #[must_use]
    pub fn event_ids(&self) -> Vec<crate::primitives::Uuid> {
        self.events
            .iter()
            .filter_map(|e| e.id.as_ref().map(|id| *id.as_uuid()))
            .collect()
    }
}

#[cfg(test)]
#[path = "admission_test.rs"]
mod tests;
