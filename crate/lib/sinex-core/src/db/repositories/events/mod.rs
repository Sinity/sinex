macro_rules! event_select_columns {
    () => {
        "id::uuid as id, \
         source, \
         event_type, \
         host, \
         payload, \
         ts_orig, \
         ts_orig_subnano, \
         ts_ingest, \
         source_material_id::uuid as source_material_id, \
         anchor_byte, \
         offset_start, \
         offset_end, \
         offset_kind, \
         source_event_ids::uuid[] as source_event_ids, \
         associated_blob_ids::uuid[] as associated_blob_ids, \
         payload_schema_id::uuid as payload_schema_id, \
         ingestor_version"
    };
}

pub(crate) use event_select_columns;

pub mod conversions;
mod persistence;
pub mod queries;

pub(crate) use conversions::EventRecordExt;
pub use conversions::EventSearchRow;
pub use persistence::{
    BatchViolation, CommandCount, EventAnnotation, EventPayloadSchema, EventRepository,
    EventRepositoryTx, EventTypeCount, InvalidPayloadEvent, InvalidTimestamp, NewSchema,
    SourceActivity, SuspiciousEvent,
};
