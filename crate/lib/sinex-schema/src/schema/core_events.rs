//! Schema definitions for core events tables

use sea_query::{Alias, ColumnDef, Iden, Table};

/// Events table (main events storage)
#[derive(Iden)]
pub enum Events {
    Table,
    Id,
    TsIngest,
    TsOrig,
    Source,
    EventType,
    Host,
    Payload,
    IngestorVersion,
    PayloadSchemaId,
    PayloadSchemaName,
    PayloadSchemaVersion,
    SourceEventIds,
    SourceMaterialId,
    SourceMaterialOffsetStart,
    SourceMaterialOffsetEnd,
    AnchorByte,
    AssociatedBlobIds,
}

impl Events {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), Events::Table))
            .if_not_exists()
            // Primary key - ULID for time-ordered distribution
            .col(
                ColumnDef::new(Events::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            // Timestamp columns
            .col(ColumnDef::new(Events::TsOrig).timestamp_with_time_zone())
            // Basic event metadata
            .col(ColumnDef::new(Events::Source).text().not_null())
            .col(ColumnDef::new(Events::EventType).text().not_null())
            .col(ColumnDef::new(Events::Host).text().not_null())
            // Payload and schema
            .col(ColumnDef::new(Events::Payload).json_binary().not_null())
            .col(ColumnDef::new(Events::IngestorVersion).text())
            .col(ColumnDef::new(Events::PayloadSchemaId).uuid())
            .col(ColumnDef::new(Events::PayloadSchemaName).text())
            .col(ColumnDef::new(Events::PayloadSchemaVersion).text())
            // Provenance fields (XOR constraint)
            .col(ColumnDef::new(Events::SourceEventIds).array(sea_query::ColumnType::Uuid))
            .col(ColumnDef::new(Events::SourceMaterialId).uuid())
            .col(ColumnDef::new(Events::SourceMaterialOffsetStart).big_integer())
            .col(ColumnDef::new(Events::SourceMaterialOffsetEnd).big_integer())
            .col(ColumnDef::new(Events::AnchorByte).big_integer())
            // Associated data
            .col(ColumnDef::new(Events::AssociatedBlobIds).array(sea_query::ColumnType::Uuid))
            .to_owned()
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            "CREATE INDEX IF NOT EXISTS idx_core_events_ts_ingest ON core.events (ts_ingest);".to_string(),
            "CREATE INDEX IF NOT EXISTS idx_core_events_source ON core.events (source);".to_string(),
            "CREATE INDEX IF NOT EXISTS idx_core_events_event_type ON core.events (event_type);".to_string(),
            "CREATE INDEX IF NOT EXISTS idx_core_events_ts_orig ON core.events (ts_orig) WHERE ts_orig IS NOT NULL;".to_string(),
            "CREATE INDEX IF NOT EXISTS idx_core_events_source_material ON core.events (source_material_id) WHERE source_material_id IS NOT NULL;".to_string(),
            "CREATE INDEX IF NOT EXISTS idx_core_events_source_events ON core.events USING GIN (source_event_ids) WHERE source_event_ids IS NOT NULL;".to_string(),
        ]
    }

    /// Create XOR constraint for provenance fields with anchor_byte requirement
    pub fn create_provenance_constraint() -> String {
        r#"ALTER TABLE core.events 
           ADD CONSTRAINT chk_events_provenance_xor 
           CHECK (
               -- Material events: MUST have source_material_id AND anchor_byte
               (source_material_id IS NOT NULL AND anchor_byte IS NOT NULL AND source_event_ids IS NULL) OR
               -- Synthesis events: MUST have source_event_ids, NO material fields
               (source_event_ids IS NOT NULL AND source_material_id IS NULL AND anchor_byte IS NULL)
           )"#
        .to_string()
    }
}
