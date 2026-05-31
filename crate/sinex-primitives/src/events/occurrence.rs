//! Occurrence and material interpretation types.
//!
//! This module re-exports occurrence-related types that are shared across
//! the primitives layer. The DB-layer record types live in `sinex-schema`.

pub use super::admission::OccurrenceAnchorKind;

use crate::events::SourceMaterial;
use crate::ids::Id;

/// Physical occurrence coordinates for a source event.
///
/// Identifies a real-world datapoint by its location within a source material.
/// This is **occurrence identity** (stable across replay, addresses the same
/// real-world observation) — distinct from **interpretation identity** (the
/// event `id`, a random UUIDv7 that is new on each replay).
///
/// # Identity model
///
/// - `id` (event primary key) = interpretation identity — random UUIDv7, changes on replay.
/// - `(source_material_id, anchor_byte)` = occurrence identity — these columns are stable
///   across replay. Two events with the same `MaterialOccurrenceKey` are two interpretations
///   of the same real-world datapoint.
///
/// # Non-goals
///
/// - NOT the event primary key. Do not use this as a DB key or for idempotency on the
///   event `id` field.
/// - NOT the natural-key dedup type. For semantic field-tuple dedup, see
///   [`sinex_primitives::parser::OccurrenceKey`].
/// - NOT occurrence dedup wiring — that is #1570 Prong C / #1050, downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MaterialOccurrenceKey {
    /// ID of the source material this event was derived from.
    pub source_material_id: Id<SourceMaterial>,
    /// Byte offset within the source material that anchors this event.
    pub anchor_byte: i64,
}

impl MaterialOccurrenceKey {
    /// Construct a new occurrence key from its components.
    #[must_use]
    pub fn new(source_material_id: impl Into<Id<SourceMaterial>>, anchor_byte: i64) -> Self {
        Self {
            source_material_id: source_material_id.into(),
            anchor_byte,
        }
    }
}

impl std::fmt::Display for MaterialOccurrenceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.source_material_id.as_uuid(), self.anchor_byte)
    }
}
