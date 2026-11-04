//! The structures admission writes for every event, beside its row: the
//! primary key on `id`, `(event_type, ts_orig)` for kind and time questions,
//! the commit-order index every commit-order reader follows, and a
//! block-range index on record location. Commit-log rows stand only for
//! positions that admitted no event; an event's position is on its own row.
//!
//! Per-source and all-kind time questions read the `(event_type, ts_orig)`
//! ranges, chunk skipping on `ts_orig`, or the source and run counters, so
//! events carry no `(source, ts_orig)`, `(ts_orig)` or per-run index.

/// Record locations follow physical order: materials are UUIDv7 and anchors
/// ascend within a material, so a block-range summary finds a record's rows
/// without a B-tree entry per event. Small ranges keep a point lookup to a
/// few pages.
pub(crate) const RECORD_LOCATION: &str = "CREATE INDEX events_record_location ON core.events USING brin (material_id, anchor) WITH (pages_per_range = 32)";

/// A position taken by a transaction that admitted no event: lifecycle
/// changes, tombstones and empty transactions.
pub(crate) const COMMIT_LOG: &str = "CREATE TABLE core.commit_log (
  commit_seq bigint PRIMARY KEY,
  persisted_at timestamptz DEFAULT now()
)";

const PREVIOUS_COMMIT_LOG: &str = "CREATE TABLE core.commit_log (
  commit_seq bigint PRIMARY KEY,
  event_id uuid,
  event_type text,
  persisted_at timestamptz DEFAULT now(),
  CHECK ((event_id IS NULL) = (event_type IS NULL))
)";

/// Event indexes before this change, as the earlier declarations created them.
const PREVIOUS_INDEXES: &[&str] = &[
    "CREATE INDEX ON core.events (ts_orig)",
    "CREATE INDEX ON core.events (source, ts_orig)",
    "CREATE INDEX ON core.events (material_id, anchor)",
    crate::description::version_nine::COVERAGE_RUNS,
];

/// The forward step. A tombstoned event's admission position moves to its
/// tombstone before the commit log forgets which event held it; positions
/// still held by an event are on that event's row.
pub(crate) fn statements() -> Vec<String> {
    vec![
        "UPDATE core.event_tombstones t SET original_commit_seq = c.commit_seq
         FROM core.commit_log c
         WHERE c.event_id = t.event_id AND t.original_commit_seq IS NULL"
            .into(),
        "DELETE FROM core.commit_log c USING core.events e
         WHERE e.id = c.event_id AND e.commit_seq = c.commit_seq"
            .into(),
        "ALTER TABLE core.commit_log DROP COLUMN event_id, DROP COLUMN event_type".into(),
        "DROP INDEX core.events_ts_orig_idx".into(),
        "DROP INDEX core.events_source_ts_orig_idx".into(),
        "DROP INDEX core.events_material_id_anchor_idx".into(),
        "DROP INDEX IF EXISTS core.events_coverage".into(),
        "DROP TABLE IF EXISTS sinex_schema.events_coverage_pending".into(),
        RECORD_LOCATION.into(),
    ]
}

/// The tables and indexes as declared before this step, for the
/// descriptions earlier steps are computed from.
pub(crate) fn previous(tables: &mut [String], after_tables: &mut Vec<String>) {
    for statement in tables {
        if statement == COMMIT_LOG {
            *statement = PREVIOUS_COMMIT_LOG.into();
        }
    }
    after_tables.retain(|statement| statement != RECORD_LOCATION);
    after_tables.extend(
        PREVIOUS_INDEXES
            .iter()
            .map(|statement| (*statement).to_owned()),
    );
}
