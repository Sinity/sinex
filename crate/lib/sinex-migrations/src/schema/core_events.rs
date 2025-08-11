use crate::schema::TableDef;
use sea_query::{Alias, ColumnDef, Expr, Index, IndexOrder, IntoIden, PostgresQueryBuilder, Table};

/// Events table schema definition
#[derive(Copy, Clone)]
pub struct Events;

impl TableDef for Events {
    fn table_name() -> &'static str {
        "events"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl Events {
    pub const TABLE: &'static str = "events";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const SOURCE: &'static str = "source";
    pub const EVENT_TYPE: &'static str = "event_type";
    pub const HOST: &'static str = "host";
    pub const PAYLOAD: &'static str = "payload";
    pub const TS_ORIG: &'static str = "ts_orig";
    pub const TS_INGEST: &'static str = "ts_ingest";
    pub const INGESTOR_VERSION: &'static str = "ingestor_version";
    pub const PAYLOAD_SCHEMA_ID: &'static str = "payload_schema_id";
    pub const SOURCE_EVENT_IDS: &'static str = "source_event_ids";
    pub const SOURCE_MATERIAL_ID: &'static str = "source_material_id";
    pub const SOURCE_MATERIAL_OFFSET_START: &'static str = "source_material_offset_start";
    pub const SOURCE_MATERIAL_OFFSET_END: &'static str = "source_material_offset_end";
    pub const ANCHOR_BYTE: &'static str = "anchor_byte";
    pub const ASSOCIATED_BLOB_IDS: &'static str = "associated_blob_ids";
    pub const PAYLOAD_SCHEMA_NAME: &'static str = "payload_schema_name";
    pub const PAYLOAD_SCHEMA_VERSION: &'static str = "payload_schema_version";
    pub const PROCESSOR_MANIFEST_ID: &'static str = "processor_manifest_id";

    /// Create the events table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SOURCE))
                    .text()
                    .not_null()
                    .check(Expr::cust("length(TRIM(BOTH FROM source)) > 0")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_TYPE))
                    .text()
                    .not_null()
                    .check(Expr::cust("length(TRIM(BOTH FROM event_type)) > 0")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::HOST))
                    .text()
                    .not_null()
                    .check(Expr::cust("length(TRIM(BOTH FROM host)) > 0")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PAYLOAD))
                    .json_binary()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::TS_ORIG)).timestamp_with_time_zone())
            // ts_ingest is added as a generated column via ALTER TABLE
            .col(ColumnDef::new(Alias::new(Self::INGESTOR_VERSION)).text())
            .col(ColumnDef::new(Alias::new(Self::PAYLOAD_SCHEMA_ID)).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Alias::new(Self::PAYLOAD_SCHEMA_NAME)).text())
            .col(ColumnDef::new(Alias::new(Self::PAYLOAD_SCHEMA_VERSION)).text())
            .col(ColumnDef::new(Alias::new(Self::SOURCE_EVENT_IDS)).array(
                sea_query::ColumnType::Custom(Alias::new("ULID").into_iden()),
            ))
            .col(ColumnDef::new(Alias::new(Self::SOURCE_MATERIAL_ID)).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Alias::new(Self::SOURCE_MATERIAL_OFFSET_START)).big_integer())
            .col(ColumnDef::new(Alias::new(Self::SOURCE_MATERIAL_OFFSET_END)).big_integer())
            .col(ColumnDef::new(Alias::new(Self::ANCHOR_BYTE)).big_integer())
            .col(ColumnDef::new(Alias::new(Self::ASSOCIATED_BLOB_IDS)).array(
                sea_query::ColumnType::Custom(Alias::new("ULID").into_iden()),
            ))
            .col(ColumnDef::new(Alias::new(Self::PROCESSOR_MANIFEST_ID)).integer())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the events table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on ts_ingest (DESC)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_events_ts_ingest")
                .col((Alias::new(Self::TS_INGEST), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Partial index on ts_orig (DESC) WHERE ts_orig IS NOT NULL
            format!(
                "CREATE INDEX idx_core_events_ts_orig ON {}.{} ({} DESC) WHERE {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::TS_ORIG, Self::TS_ORIG
            ),
            // Index on source and ts_ingest (DESC)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_events_source_ts")
                .col(Alias::new(Self::SOURCE))
                .col((Alias::new(Self::TS_INGEST), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Index on event_type and ts_ingest (DESC)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_events_event_type_ts")
                .col(Alias::new(Self::EVENT_TYPE))
                .col((Alias::new(Self::TS_INGEST), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Composite index on (source, event_type, ts_ingest DESC)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_events_source_type_ts")
                .col(Alias::new(Self::SOURCE))
                .col(Alias::new(Self::EVENT_TYPE))
                .col((Alias::new(Self::TS_INGEST), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // GIN index on payload
            format!(
                "CREATE INDEX idx_core_events_payload ON {}.{} USING GIN ({})",
                Self::SCHEMA, Self::TABLE, Self::PAYLOAD
            ),
            // GIN index on source_event_ids  
            format!(
                "CREATE INDEX idx_core_events_source_event_ids ON {}.{} USING GIN ({})",
                Self::SCHEMA, Self::TABLE, Self::SOURCE_EVENT_IDS
            ),
            // Index on source_material_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_events_source_material")
                .col(Alias::new(Self::SOURCE_MATERIAL_ID))
                .build(PostgresQueryBuilder),
            // Unique index on (source_material_id, anchor_byte, id) for first-order idempotency
            // Note: id column is required for TimescaleDB hypertable partitioning
            format!(
                "CREATE UNIQUE INDEX idx_events_material_anchor ON {}.{} ({}, {}, {}) WHERE {} IS NOT NULL AND {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::SOURCE_MATERIAL_ID, Self::ANCHOR_BYTE, Self::ID,
                Self::SOURCE_MATERIAL_ID, Self::ANCHOR_BYTE
            ),
        ]
    }

    /// Add generated column for ts_ingest
    pub fn add_generated_column() -> String {
        format!(
            "ALTER TABLE {}.{} ADD COLUMN {} TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS (ulid_to_timestamp({})) STORED",
            Self::SCHEMA, Self::TABLE, Self::TS_INGEST, Self::ID
        )
    }

    /// Create constraints (none needed for events table itself)
    pub fn create_constraints() -> Vec<String> {
        vec![]
    }
}

/// Archived events table schema definition  
#[derive(Copy, Clone)]
pub struct ArchivedEvents;

impl TableDef for ArchivedEvents {
    fn table_name() -> &'static str {
        "archived_events"
    }
    fn schema_name() -> &'static str {
        "audit"
    }
    fn primary_key() -> &'static str {
        "id"
    }
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
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_archived_events_archived_at")
                .col(Alias::new("archived_at"))
                .build(PostgresQueryBuilder),
            // Index on operation_id
            format!(
                "CREATE INDEX idx_archived_events_operation_id ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                "operation_id",
                "operation_id"
            ),
            // Index on superseded_by_event_id
            format!(
                "CREATE INDEX idx_archived_events_superseded_by ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                "superseded_by_event_id",
                "superseded_by_event_id"
            ),
            // Index on archived_by
            format!(
                "CREATE INDEX idx_archived_events_archived_by ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                "archived_by",
                "archived_by"
            ),
        ]
    }
}
