//! Polylogue's archive in `primary` mode (architecture §9). Polylogue keeps
//! the meaning of AI-session data; Sinex keeps its evidence. Each unit of
//! Polylogue's `archive.db` rows (a revision with its input, extents,
//! observations and blobs; a note with its targets and relations; an
//! excision; a setting; a delivery) arrives as a record-batch material plus
//! one `polylogue.rows` event anchored at its start. The input bytes the rows
//! cite are materials of their own, placed in the content store before the
//! event. Sinex never parses either: a deleted Polylogue archive is restored
//! by reading these batches back in `local_seq` order.
mod replacement;
pub use replacement::{ArchiveUnit, ExcisionPublication, UnitReplacement};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One unit of a Polylogue archive's rows. Units are numbered 1, 2, 3… in
/// the archive's write order (`local_seq`, the write counter, then unit kind
/// and key, since one write can store several units). The archive only
/// appends to that order, so a unit's position never changes, and Sinex
/// holds the archive whole up to the first missing position.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "polylogue.rows", key = ["archive", "position"], replace = repeat)]
pub struct PolylogueRows {
    /// The archive's identity (a UUID), so two archives never collide.
    pub archive: String,
    pub position: u64,
    /// The archive's write counter at the unit.
    pub local_seq: i64,
    /// `revision`, `note`, `excision`, `setting`, `delivery`,
    /// `observation` or `delivery_event`.
    pub unit: String,
    /// The unit's own key: `<input>/<number>` for a revision, the row ID
    /// otherwise; delivery events use their encoded compound key.
    pub key: String,
    /// `blake3:<hex>` of the record-batch material: the unit's rows as JSON
    /// lines, one `{"table": …, "row": {…}}` per line.
    pub batch: String,
    /// How many rows the batch holds. The batch is checked whole against it.
    pub rows: u64,
    /// `blake3:<hex>` of every input blob the rows cite, each a material
    /// placed before this event.
    pub blobs: Vec<String>,
    /// Confirmed current replacements or tombstones. Restore selects these
    /// before opening old batches, whose bytes may already be retired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excision: Option<ExcisionPublication>,
}

#[cfg(test)]
mod tests;
