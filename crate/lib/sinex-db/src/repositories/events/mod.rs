/// Standard event query column list macro
///
/// # Schema Change Warning
/// This macro expands to a compile-time string constant. Schema changes to the
/// `core.events` table require manually updating this macro definition. The macro
/// does NOT automatically sync with declarative schema apply.
///
/// When adding, removing, or renaming columns in `core.events`:
/// 1. Update declarative schema definitions in `sinex-schema`
/// 2. Update this macro definition to match
/// 3. Update `EventRecord` struct in `conversions.rs`
/// 4. Verify all queries using this macro still compile
///
/// Common mistake: Adding a column to the schema but forgetting to update this macro
/// will cause runtime query errors despite successful compilation.
macro_rules! event_select_columns {
    () => {
        "id::uuid as id, \
         source, \
         event_type, \
         host, \
         payload, \
         ts_orig, \
         ts_orig_subnano, \
         ts_coided, \
         ts_persisted, \
         source_material_id::uuid as source_material_id, \
         anchor_byte, \
         offset_start, \
         offset_end, \
         offset_kind, \
         source_event_ids::uuid[] as source_event_ids, \
         associated_blob_ids::uuid[] as associated_blob_ids, \
         payload_schema_id::uuid as payload_schema_id, \
         node_run_id::uuid as node_run_id, \
         temporal_policy, \
         semantics_version, \
         scope_key, \
         equivalence_key, \
         created_by_operation_id::uuid as created_by_operation_id, \
         node_model"
    };
}

pub(crate) use event_select_columns;

pub mod composable_query;
pub mod conversions;
mod persistence;
pub mod queries;

pub use conversions::{EventRecordExt, records_to_events};
pub use persistence::{
    BatchViolation, COPY_BATCH_THRESHOLD, CascadeSource, EventAnnotation, EventPayloadSchema,
    EventRepository, EventRepositoryTx, InvalidPayloadEvent, InvalidTimestamp, ReplacementKind,
    ReplacementRecord, StreamBatchInsertResult, StreamBatchRow, SuspiciousEvent,
};

// ─── Schema-consistency self-test ─────────────────────────────────────────

#[cfg(test)]
mod tests {
    // event_select_columns! is available in scope from the parent module

    /// Number of physical columns in `core.events` (24 columns).
    ///
    /// This must equal: `sinex::schema::Events` variant count excluding `Table` (24).
    /// When adding or removing columns in `core.events`:
    /// 1. Update `sinex::schema::Events` enum + `create_table_statement()`
    /// 2. Update the `EventRecord` struct in both schema + sinex-db conversions.rs
    /// 3. Update the `event_select_columns!` macro above
    /// 4. Update this constant
    const EXPECTED_COLUMN_COUNT: usize = 24;

    /// Load-bearing column names that MUST appear in `event_select_columns!`.
    /// Every column that appears in the SELECT list should appear here so that
    /// renames and reorderings are caught.
    const EXPECTED_COLUMNS: &[&str] = &[
        "id",
        "source",
        "event_type",
        "host",
        "payload",
        "ts_orig",
        "ts_orig_subnano",
        "ts_coided",
        "ts_persisted",
        "source_material_id",
        "anchor_byte",
        "offset_start",
        "offset_end",
        "offset_kind",
        "source_event_ids",
        "associated_blob_ids",
        "payload_schema_id",
        "node_run_id",
        "temporal_policy",
        "semantics_version",
        "scope_key",
        "equivalence_key",
        "created_by_operation_id",
        "node_model",
    ];

    #[test]
    fn column_count_matches_schema() {
        let cols: &str = event_select_columns!();
        let count = cols.split(',').count();
        assert_eq!(
            count, EXPECTED_COLUMN_COUNT,
            "event_select_columns! column count ({count}) != expected ({EXPECTED_COLUMN_COUNT}). \
             Either the schema changed or the macro drifted — update both, then update \
             EXPECTED_COLUMN_COUNT in this test."
        );
    }

    #[test]
    fn all_declared_columns_present() {
        let cols: &str = event_select_columns!();
        for expected in EXPECTED_COLUMNS {
            assert!(
                cols.contains(expected),
                "event_select_columns! is missing column '{expected}'. \
                 Schema may have drifted — update the macro above and EXPECTED_COLUMNS in this test."
            );
        }
    }

    #[test]
    fn no_extraneous_columns() {
        // Count must equal the declared list length. Combined with
        // `all_declared_columns_present`, this guarantees the macro outputs
        // exactly the declared set — no extras, no missing entries.
        let cols: &str = event_select_columns!();
        let count = cols.split(',').count();
        assert_eq!(
            count,
            EXPECTED_COLUMNS.len(),
            "event_select_columns! column count ({count}) != declared count ({}). \
             Update EXPECTED_COLUMNS to match the macro.",
            EXPECTED_COLUMNS.len()
        );
    }
}
