//! Event admission envelope for durable transport.
//!
//! This module defines the `EventIntent` envelope that producers construct
//! to declare "I've done my admission checks, here's the payload." The envelope
//! complements (does not replace) the ingestd-side admission boundary extracted in
//! #1056 — the producer uses the envelope type (compile-time guard), and ingestd
//! validates it on receipt (runtime guard).
//!
//! # Envelope versioning
//!
//! The envelope carries an explicit version string. ingestd rejects unknown
//! versions, giving us a forward-compatible migration path.

use crate::domain::HostName;
use crate::events::Event;
use crate::primitives::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Current envelope version emitted by all producers.
pub const CURRENT_ENVELOPE_VERSION: &str = "1";

/// Envelope versions that ingestd will accept on receipt.
pub const ACCEPTED_ENVELOPE_VERSIONS: &[&str] = &["1"];

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
/// - **Envelope metadata** (version, source unit, parser identity)
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
    pub envelope_version: String,

    /// Source unit identifier (e.g., "fs-watcher", "terminal-ingestor").
    /// Must match a registered `SourceUnitDescriptor::source_unit_id`.
    pub source_unit_id: String,

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
        source_unit_id: impl Into<String>,
        parser_id: impl Into<String>,
        parser_version: impl Into<String>,
        events: Vec<Event<JsonValue>>,
        admitted_by: HostName,
    ) -> Self {
        Self {
            envelope_version: CURRENT_ENVELOPE_VERSION.to_string(),
            source_unit_id: source_unit_id.into(),
            parser_id: parser_id.into(),
            parser_version: parser_version.into(),
            events,
            admitted_at: Timestamp::now(),
            admitted_by,
        }
    }

    /// Check whether this envelope version is accepted by ingestd.
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
        if self.source_unit_id.trim().is_empty() {
            return Err(crate::SinexError::validation(
                "admitted event intent missing source_unit_id",
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
mod tests {
    use super::*;
    use crate::events::builder::Provenance;
    use crate::{Id, Uuid};

    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn envelope_validation_rejects_empty_version() -> TestResult<()> {
        let mut intent = EventIntent {
            envelope_version: String::new(),
            source_unit_id: "test".into(),
            parser_id: "test-parser".into(),
            parser_version: "1.0.0".into(),
            events: vec![minimal_event()],
            admitted_at: Timestamp::now(),
            admitted_by: crate::domain::HostName::from_static("test-host"),
        };
        assert!(intent.validate().is_err());

        intent.envelope_version = "   ".into();
        assert!(intent.validate().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn envelope_validation_rejects_empty_events() -> TestResult<()> {
        let intent = EventIntent {
            envelope_version: "1".into(),
            source_unit_id: "test".into(),
            parser_id: "test-parser".into(),
            parser_version: "1.0.0".into(),
            events: vec![],
            admitted_at: Timestamp::now(),
            admitted_by: crate::domain::HostName::from_static("test-host"),
        };
        assert!(intent.validate().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn envelope_validation_passes_for_valid_intent() -> TestResult<()> {
        let intent = EventIntent {
            envelope_version: "1".into(),
            source_unit_id: "test-unit".into(),
            parser_id: "test-parser".into(),
            parser_version: "1.0.0".into(),
            events: vec![minimal_event()],
            admitted_at: Timestamp::now(),
            admitted_by: crate::domain::HostName::from_static("test-host"),
        };
        assert!(intent.validate().is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn is_version_accepted_returns_true_for_v1() -> TestResult<()> {
        let intent = EventIntent {
            envelope_version: "1".into(),
            source_unit_id: "test".into(),
            parser_id: "test-parser".into(),
            parser_version: "1.0.0".into(),
            events: vec![minimal_event()],
            admitted_at: Timestamp::now(),
            admitted_by: crate::domain::HostName::from_static("test-host"),
        };
        assert!(intent.is_version_accepted());
        Ok(())
    }

    #[sinex_test]
    async fn is_version_accepted_rejects_unknown_version() -> TestResult<()> {
        let intent = EventIntent {
            envelope_version: "999".into(),
            source_unit_id: "test".into(),
            parser_id: "test-parser".into(),
            parser_version: "1.0.0".into(),
            events: vec![minimal_event()],
            admitted_at: Timestamp::now(),
            admitted_by: crate::domain::HostName::from_static("test-host"),
        };
        assert!(!intent.is_version_accepted());
        Ok(())
    }

    #[sinex_test]
    async fn event_ids_collects_all_ids() -> TestResult<()> {
        let ev1_id = Uuid::now_v7();
        let ev2_id = Uuid::now_v7();
        let mut ev1 = minimal_event();
        ev1.id = Some(Id::from_uuid(ev1_id));
        let mut ev2 = minimal_event();
        ev2.id = Some(Id::from_uuid(ev2_id));

        let intent = EventIntent {
            envelope_version: "1".into(),
            source_unit_id: "test".into(),
            parser_id: "test-parser".into(),
            parser_version: "1.0.0".into(),
            events: vec![ev1, ev2],
            admitted_at: Timestamp::now(),
            admitted_by: crate::domain::HostName::from_static("test-host"),
        };
        let ids = intent.event_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&ev1_id));
        assert!(ids.contains(&ev2_id));
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_anchor_kind_roundtrips() -> TestResult<()> {
        for kind in &[
            OccurrenceAnchorKind::ByteOffset,
            OccurrenceAnchorKind::SqliteRow,
            OccurrenceAnchorKind::LineNumber,
            OccurrenceAnchorKind::SequenceNumber,
            OccurrenceAnchorKind::NaturalKey,
            OccurrenceAnchorKind::CursorToken,
            OccurrenceAnchorKind::GitOid,
            OccurrenceAnchorKind::StreamFrame,
        ] {
            let s = kind.as_str();
            let parsed = OccurrenceAnchorKind::try_from_str(s)?;
            assert_eq!(*kind, parsed);
        }
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_anchor_kind_rejects_invalid() -> TestResult<()> {
        assert!(OccurrenceAnchorKind::try_from_str("bogus_kind").is_err());
        Ok(())
    }

    fn minimal_event() -> Event<JsonValue> {
        let provenance = Provenance::from_material(
            Id::<crate::events::SourceMaterial>::from_uuid(Uuid::now_v7()),
            0,
            None,
            None,
        );
        Event {
            id: Some(Id::from_uuid(Uuid::now_v7())),
            source: crate::domain::EventSource::from_static("test.source"),
            event_type: crate::domain::EventType::from_static("test.type"),
            payload: serde_json::json!({"key": "value"}),
            ts_orig: Some(Timestamp::now()),
            ts_quality: None,
            host: crate::domain::HostName::from_static("test-host"),
            source_run_id: None,
            payload_schema_id: None,
            provenance,
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
            anchor_payload_hash: None,
        }
    }
}
