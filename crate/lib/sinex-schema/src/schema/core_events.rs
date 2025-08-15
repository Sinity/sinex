//! Schema definitions for core events tables

use crate::schema::metadata::{ColumnSchema, Constraint, HasSchema, SqlType, TableSchema};
use sea_orm_migration::prelude::*;

/// Events table (main events storage)
#[derive(Iden, Copy, Clone)]
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
    ProcessorManifestId,
}

impl Events {
    // SCREAMING_SNAKE_CASE constants for compatibility with existing repository code
    pub const SCHEMA: &'static str = "core";
    pub const TABLE: &'static str = "events";
    pub const ID: &'static str = "id";
    pub const TS_INGEST: &'static str = "ts_ingest";
    pub const TS_ORIG: &'static str = "ts_orig";
    pub const SOURCE: &'static str = "source";
    pub const EVENT_TYPE: &'static str = "event_type";
    pub const HOST: &'static str = "host";
    pub const PAYLOAD: &'static str = "payload";
    pub const INGESTOR_VERSION: &'static str = "ingestor_version";
    pub const PAYLOAD_SCHEMA_ID: &'static str = "payload_schema_id";
    pub const PAYLOAD_SCHEMA_NAME: &'static str = "payload_schema_name";
    pub const PAYLOAD_SCHEMA_VERSION: &'static str = "payload_schema_version";
    pub const SOURCE_EVENT_IDS: &'static str = "source_event_ids";
    pub const SOURCE_MATERIAL_ID: &'static str = "source_material_id";
    pub const SOURCE_MATERIAL_OFFSET_START: &'static str = "source_material_offset_start";
    pub const SOURCE_MATERIAL_OFFSET_END: &'static str = "source_material_offset_end";
    pub const ANCHOR_BYTE: &'static str = "anchor_byte";
    pub const ASSOCIATED_BLOB_IDS: &'static str = "associated_blob_ids";
    pub const PROCESSOR_MANIFEST_ID: &'static str = "processor_manifest_id";

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
            .col(
                ColumnDef::new(Events::SourceEventIds).array(sea_query::ColumnType::Custom(
                    Alias::new("ULID").into_iden(),
                )),
            )
            .col(ColumnDef::new(Events::SourceMaterialId).uuid())
            .col(ColumnDef::new(Events::SourceMaterialOffsetStart).big_integer())
            .col(ColumnDef::new(Events::SourceMaterialOffsetEnd).big_integer())
            .col(ColumnDef::new(Events::AnchorByte).big_integer())
            // Associated data
            .col(ColumnDef::new(Events::AssociatedBlobIds).array(sea_query::ColumnType::Uuid))
            // Processor manifest for synthesis events
            .col(ColumnDef::new(Events::ProcessorManifestId).integer())
            .to_owned()
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on ts_ingest (DESC)
            "CREATE INDEX IF NOT EXISTS idx_core_events_ts_ingest ON core.events (ts_ingest DESC);".to_string(),
            // Index on source
            "CREATE INDEX IF NOT EXISTS idx_core_events_source ON core.events (source);".to_string(),
            // Index on event_type
            "CREATE INDEX IF NOT EXISTS idx_core_events_event_type ON core.events (event_type);".to_string(),
            // Composite index on source and ts_ingest (DESC)
            "CREATE INDEX IF NOT EXISTS idx_core_events_source_ts ON core.events (source, ts_ingest DESC);".to_string(),
            // Composite index on event_type and ts_ingest (DESC)
            "CREATE INDEX IF NOT EXISTS idx_core_events_event_type_ts ON core.events (event_type, ts_ingest DESC);".to_string(),
            // Composite index on (source, event_type, ts_ingest DESC)
            "CREATE INDEX IF NOT EXISTS idx_core_events_source_type_ts ON core.events (source, event_type, ts_ingest DESC);".to_string(),
            // Partial index on ts_orig (DESC) WHERE ts_orig IS NOT NULL
            "CREATE INDEX IF NOT EXISTS idx_core_events_ts_orig ON core.events (ts_orig DESC) WHERE ts_orig IS NOT NULL;".to_string(),
            // Index on source_material_id
            "CREATE INDEX IF NOT EXISTS idx_core_events_source_material ON core.events (source_material_id) WHERE source_material_id IS NOT NULL;".to_string(),
            // GIN index on payload
            "CREATE INDEX IF NOT EXISTS idx_core_events_payload ON core.events USING GIN (payload);".to_string(),
            // GIN index on source_event_ids
            "CREATE INDEX IF NOT EXISTS idx_core_events_source_events ON core.events USING GIN (source_event_ids) WHERE source_event_ids IS NOT NULL;".to_string(),
            // Unique index on (source_material_id, anchor_byte, id) for first-order idempotency
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_material_anchor ON core.events (source_material_id, anchor_byte, id) WHERE source_material_id IS NOT NULL AND anchor_byte IS NOT NULL;".to_string(),
        ]
    }

    /// Add generated column for ts_ingest
    pub fn add_generated_column() -> String {
        format!(
            "ALTER TABLE {}.{} ADD COLUMN {} TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS (ulid_to_timestamp({})) STORED",
            Self::SCHEMA, Self::TABLE, Self::TS_INGEST, Self::ID
        )
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

impl HasSchema for Events {
    fn schema() -> &'static TableSchema {
        &EVENTS_SCHEMA
    }
}

/// Schema metadata for the Events table
static EVENTS_SCHEMA: TableSchema = TableSchema {
    name: "events",
    schema: "core",
    columns: &[
        ColumnSchema {
            name: "id",
            rust_type: "Uuid",
            sql_type: SqlType::Custom("ULID"),
            nullable: false,
            constraints: &[Constraint::PrimaryKey],
        },
        ColumnSchema {
            name: "ts_ingest",
            rust_type: "DateTime<Utc>",
            sql_type: SqlType::TimestampWithTimeZone,
            nullable: false,
            constraints: &[Constraint::Generated("ulid_timestamp(id)"), Constraint::Index],
        },
        ColumnSchema {
            name: "ts_orig",
            rust_type: "Option<DateTime<Utc>>",
            sql_type: SqlType::TimestampWithTimeZone,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "source",
            rust_type: "String",
            sql_type: SqlType::Text,
            nullable: false,
            constraints: &[Constraint::NotNull, Constraint::Index],
        },
        ColumnSchema {
            name: "event_type",
            rust_type: "String",
            sql_type: SqlType::Text,
            nullable: false,
            constraints: &[Constraint::NotNull, Constraint::Index],
        },
        ColumnSchema {
            name: "host",
            rust_type: "String",
            sql_type: SqlType::Text,
            nullable: false,
            constraints: &[Constraint::NotNull],
        },
        ColumnSchema {
            name: "payload",
            rust_type: "serde_json::Value",
            sql_type: SqlType::Json,
            nullable: false,
            constraints: &[Constraint::NotNull],
        },
        ColumnSchema {
            name: "ingestor_version",
            rust_type: "Option<String>",
            sql_type: SqlType::Text,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "payload_schema_id",
            rust_type: "Option<Uuid>",
            sql_type: SqlType::Uuid,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "payload_schema_name",
            rust_type: "Option<String>",
            sql_type: SqlType::Text,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "payload_schema_version",
            rust_type: "Option<String>",
            sql_type: SqlType::Text,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "source_event_ids",
            rust_type: "Option<Vec<Uuid>>",
            sql_type: SqlType::UlidArray,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "source_material_id",
            rust_type: "Option<Uuid>",
            sql_type: SqlType::Uuid,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "source_material_offset_start",
            rust_type: "Option<i64>",
            sql_type: SqlType::BigInteger,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "source_material_offset_end",
            rust_type: "Option<i64>",
            sql_type: SqlType::BigInteger,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "anchor_byte",
            rust_type: "Option<i64>",
            sql_type: SqlType::BigInteger,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "associated_blob_ids",
            rust_type: "Option<Vec<Uuid>>",
            sql_type: SqlType::UuidArray,
            nullable: true,
            constraints: &[],
        },
        ColumnSchema {
            name: "processor_manifest_id",
            rust_type: "Option<i32>",
            sql_type: SqlType::Integer,
            nullable: true,
            constraints: &[],
        },
    ],
    table_constraints: &[
        "CHECK ((source_material_id IS NOT NULL AND anchor_byte IS NOT NULL AND source_event_ids IS NULL) OR (source_event_ids IS NOT NULL AND source_material_id IS NULL AND anchor_byte IS NULL))",
    ],
};

/// Archived events table schema definition
#[derive(Iden, Copy, Clone)]
pub enum ArchivedEvents {
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
    ProcessorManifestId,
    ArchivedAt,
    ArchivedBy,
    SupersededByEventId,
    OperationId,
}

impl ArchivedEvents {
    pub const TABLE: &'static str = "archived_events";
    pub const SCHEMA: &'static str = "audit";

    /// Create the archived events table (matches events structure plus archive metadata)
    pub fn create_table() -> String {
        // Create as a copy of the events table structure
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.{} (
                LIKE {}.{} INCLUDING ALL,
                archived_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                archived_by TEXT,
                superseded_by_event_id ULID,
                operation_id ULID
            )
            "#,
            Self::SCHEMA,
            Self::TABLE,
            Events::SCHEMA,
            Events::TABLE
        )
    }

    /// Create indexes for the archived events table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on archived_at for efficient cleanup queries
            format!(
                "CREATE INDEX IF NOT EXISTS idx_archived_events_archived_at ON {}.{} (archived_at)",
                Self::SCHEMA,
                Self::TABLE
            ),
            // Index on operation_id
            format!(
                "CREATE INDEX IF NOT EXISTS idx_archived_events_operation_id ON {}.{} (operation_id) WHERE operation_id IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE
            ),
            // Index on superseded_by_event_id
            format!(
                "CREATE INDEX IF NOT EXISTS idx_archived_events_superseded_by ON {}.{} (superseded_by_event_id) WHERE superseded_by_event_id IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE
            ),
            // Index on archived_by
            format!(
                "CREATE INDEX IF NOT EXISTS idx_archived_events_archived_by ON {}.{} (archived_by) WHERE archived_by IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE
            ),
        ]
    }
}
