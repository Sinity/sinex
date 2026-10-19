//! Polylogue's archive in `primary` mode (architecture §9). Polylogue keeps
//! the meaning of AI-session data; Sinex keeps its evidence. Each unit of
//! Polylogue's `archive.db` rows (a revision with its input, extents,
//! observations and blobs; a note with its targets and relations; an
//! excision; a setting; a delivery) arrives as a record-batch material plus
//! one `polylogue.rows` event anchored at its start. The input bytes the rows
//! cite are materials of their own, placed in the content store before the
//! event. Sinex never parses either: a deleted Polylogue archive is restored
//! by reading these batches back in `local_seq` order.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One unit of a Polylogue archive's rows. The archive's write counter at the
/// unit (`local_seq`) and at the unit published before it (`prev_local_seq`,
/// 0 for the first) let Sinex tell a gap from a missing unit: everything up
/// to the largest `local_seq` whose chain of `prev_local_seq` reaches 0 is
/// held whole.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "polylogue.rows", key = ["archive", "local_seq"], replace = repeat)]
pub struct PolylogueRows {
    /// The archive's identity (a UUID), so two archives never collide.
    pub archive: String,
    pub local_seq: i64,
    pub prev_local_seq: i64,
    /// `revision`, `note`, `excision`, `setting` or `delivery`.
    pub unit: String,
    /// The unit's own key: `<input>/<number>` for a revision, the row ID
    /// otherwise.
    pub key: String,
    /// `blake3:<hex>` of the record-batch material: the unit's rows as JSON
    /// lines, one `{"table": …, "row": {…}}` per line.
    pub batch: String,
    /// How many rows the batch holds. The batch is checked whole against it.
    pub rows: u64,
    /// `blake3:<hex>` of every input blob the rows cite, each a material
    /// placed before this event.
    pub blobs: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventPayload, KeyKind, ReplaceRule};

    #[test]
    fn a_unit_is_keyed_by_archive_and_write_counter_and_never_replaced() {
        assert_eq!(PolylogueRows::EVENT_TYPE, "polylogue.rows");
        assert!(matches!(
            PolylogueRows::REPLACE,
            ReplaceRule::SuppressRepeat
        ));
        assert!(matches!(PolylogueRows::KEY, KeyKind::Fields(_)));
        let schema = PolylogueRows::payload_schema();
        assert!(schema.to_string().contains("prev_local_seq"));
    }
}
