//! Occurrence and material interpretation types.
//!
//! These types define the schema-level vocabulary for stable occurrence slots
//! and material interpretation records. They complement the existing
//! `core.events` XOR provenance model — occurrences are the stable replay
//! surface, event IDs remain interpretation IDs.
//!
//! # Relationship to the parser substrate
//!
//! The `parser` module (`crate::parser`) defines `MaterialAnchor` and
//! `OccurrenceKey` for parser authors. This module defines `AnchorKind`
//! and database-level types for the schema/repository layer. The two
//! are aligned but serve different consumers.

use serde::{Deserialize, Serialize};

/// Marker type for occurrence identifiers (`Id<Occurrence>`).
///
/// An `Occurrence` is a stable, source-unit-scoped logical slot in a
/// source material. It identifies *what happened in the world*, not
/// *how sinex interpreted it* (that's the event).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Occurrence;

/// Marker type for material interpretation identifiers
/// (`Id<MaterialInterpretation>`).
///
/// A `MaterialInterpretation` records that a specific parser version
/// interpreted a specific occurrence and produced a specific event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterialInterpretation;

/// How an occurrence is located within its source material.
///
/// This enum provides the database-level vocabulary for anchor kinds.
/// It is broader than `OffsetKind` (which only covers bytes/lines/rows)
/// because staged parsers may locate records via git OIDs, cursor tokens,
/// or name-based keys.
///
/// The corresponding `anchor_data` jsonb column carries the actual
/// anchor value in a kind-appropriate shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnchorKind {
    /// Byte-offset range within a material blob.
    /// `anchor_data`: `{"start": <u64>, "len": <u64>}`
    ByteOffset,

    /// A row in a SQLite table.
    /// `anchor_data`: `{"table": "<name>", "rowid": <i64>}`
    SqliteRow,

    /// A line number within a text material.
    /// `anchor_data`: `{"byte_start": <u64>, "line": <u64>}`
    LineNumber,

    /// A monotonic sequence number (e.g., entry index in a JSON array).
    /// `anchor_data`: `{"seq": <u64>}`
    SequenceNumber,

    /// A domain-specific natural key (e.g., an email Message-ID).
    /// `anchor_data`: `{"key": "<string>"}`
    /// Also stored in the `natural_key` column for indexing.
    NaturalKey,

    /// A cursor or page token for API-paginated sources.
    /// `anchor_data`: `{"token": "<string>"}`
    CursorToken,

    /// A git object identifier (40-char hex SHA).
    /// `anchor_data`: `{"oid": "<40-char-hex>"}`
    GitOid,

    /// A frame within a byte stream.
    /// `anchor_data`: `{"material_offset": <u64>, "frame_index": <u64>}`
    StreamFrame,
}

impl AnchorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            AnchorKind::ByteOffset => "byte_offset",
            AnchorKind::SqliteRow => "sqlite_row",
            AnchorKind::LineNumber => "line_number",
            AnchorKind::SequenceNumber => "sequence_number",
            AnchorKind::NaturalKey => "natural_key",
            AnchorKind::CursorToken => "cursor_token",
            AnchorKind::GitOid => "git_oid",
            AnchorKind::StreamFrame => "stream_frame",
        }
    }

    /// Parse from a database-stored string.
    pub fn try_from_str(s: &str) -> Result<Self, crate::SinexError> {
        match s {
            "byte_offset" => Ok(AnchorKind::ByteOffset),
            "sqlite_row" => Ok(AnchorKind::SqliteRow),
            "line_number" => Ok(AnchorKind::LineNumber),
            "sequence_number" => Ok(AnchorKind::SequenceNumber),
            "natural_key" => Ok(AnchorKind::NaturalKey),
            "cursor_token" => Ok(AnchorKind::CursorToken),
            "git_oid" => Ok(AnchorKind::GitOid),
            "stream_frame" => Ok(AnchorKind::StreamFrame),
            _ => Err(crate::SinexError::validation("invalid anchor kind")
                .with_context("anchor_kind", s.to_string())),
        }
    }
}

impl std::fmt::Display for AnchorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AnchorKind {
    type Err = crate::SinexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        AnchorKind::try_from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_kind_roundtrips_through_str() {
        let variants = [
            AnchorKind::ByteOffset,
            AnchorKind::SqliteRow,
            AnchorKind::LineNumber,
            AnchorKind::SequenceNumber,
            AnchorKind::NaturalKey,
            AnchorKind::CursorToken,
            AnchorKind::GitOid,
            AnchorKind::StreamFrame,
        ];

        for kind in variants {
            let s = kind.as_str();
            let parsed = AnchorKind::try_from_str(s).expect("roundtrip");
            assert_eq!(kind, parsed);
        }
    }

    #[test]
    fn anchor_kind_rejects_invalid() {
        assert!(AnchorKind::try_from_str("invalid").is_err());
        assert!(AnchorKind::try_from_str("").is_err());
    }
}
