/// Standard event query column list macro
///
/// # Schema Change Warning
/// This macro expands to a compile-time string constant. Schema changes to the
/// `core.events` table require manually updating this macro definition. The macro
/// does NOT automatically sync with schema migrations.
///
/// When adding, removing, or renaming columns in `core.events`:
/// 1. Update the migration in `sinex-schema`
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
         node_version"
    };
}

pub(crate) use event_select_columns;

pub mod composable_query;
pub mod conversions;
mod persistence;
pub mod queries;

pub use conversions::{EventRecordExt, records_to_events};
pub use persistence::{
    BatchViolation, CascadeSource, EventAnnotation, EventPayloadSchema, EventRepository,
    EventRepositoryTx, InvalidPayloadEvent, InvalidTimestamp, StreamBatchInsertResult,
    StreamBatchRow, SuspiciousEvent,
};
