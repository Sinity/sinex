//! Database schema definitions using SeaQuery
//!
//! This module provides type-safe schema definitions for all database tables
//! using SeaQuery's table definition API.

use sea_query::{Alias, PostgresQueryBuilder};
use sea_query::{ColumnDef, Expr, Index, IndexOrder, IntoIden, Table};

// Note: artifact_tables module removed as part of Phase 1.3 cleanup
// The artifact system has been replaced by the synthesis architecture

pub mod event_embeddings;
pub use event_embeddings::*;

/// Trait for table definitions that can be used with generic repository operations
pub trait TableDef: Copy + Clone {
    /// Get the table name
    fn table_name() -> &'static str;

    /// Get the schema name
    fn schema_name() -> &'static str;

    /// Get the primary key column name
    fn primary_key() -> &'static str;

    /// Get the full table identifier (schema.table)
    fn table_iden() -> (Alias, Alias) {
        (
            Alias::new(Self::schema_name()),
            Alias::new(Self::table_name()),
        )
    }
}

/// Macro to implement TableDef trait for table structs
macro_rules! impl_table_def {
    ($struct_name:ident, $table:expr, $schema:expr, $primary_key:expr) => {
        impl TableDef for $struct_name {
            fn table_name() -> &'static str {
                $table
            }

            fn schema_name() -> &'static str {
                $schema
            }

            fn primary_key() -> &'static str {
                $primary_key
            }
        }
    };
}

/// Processor manifests table schema definition
#[derive(Copy, Clone)]
pub struct ProcessorManifests;

impl_table_def!(ProcessorManifests, "processor_manifests", "core", "id");

impl ProcessorManifests {
    pub const TABLE: &'static str = "processor_manifests";
    pub const SCHEMA: &'static str = "core";

    pub const MANIFEST_ID: &'static str = "id";
    pub const PROCESSOR_NAME: &'static str = "processor_name";
    pub const PROCESSOR_VERSION: &'static str = "processor_version";
    pub const PROCESSOR_TYPE: &'static str = "processor_type";
    pub const HOSTNAME: &'static str = "hostname";
    pub const START_TIME: &'static str = "start_time";
    pub const END_TIME: &'static str = "end_time";
    pub const CONFIG: &'static str = "config";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the processor manifests table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::MANIFEST_ID))
                    .integer()
                    .not_null()
                    .primary_key()
                    .auto_increment(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PROCESSOR_NAME))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PROCESSOR_VERSION))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PROCESSOR_TYPE))
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "processor_type IN ('ingestor', 'automaton', 'system')",
                    )),
            )
            .col(ColumnDef::new(Alias::new(Self::HOSTNAME)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::START_TIME))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::END_TIME)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::CONFIG)).json_binary())
            .col(ColumnDef::new(Alias::new(Self::METADATA)).json_binary())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the processor manifests table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Partial index on active processors (where end_time IS NULL)
            format!(
                "CREATE INDEX idx_processor_manifests_active ON {}.{} ({}, {}) WHERE {} IS NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::PROCESSOR_NAME,
                Self::HOSTNAME,
                Self::END_TIME
            ),
            // Index on time range
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_processor_manifests_time_range")
                .col(Alias::new(Self::START_TIME))
                .col(Alias::new(Self::END_TIME))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create constraints for the processor manifests table
    pub fn create_constraints() -> Vec<String> {
        vec![format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT unique_processor_instance UNIQUE ({}, {}, {}, {})",
            Self::SCHEMA,
            Self::TABLE,
            Self::PROCESSOR_NAME,
            Self::PROCESSOR_VERSION,
            Self::HOSTNAME,
            Self::START_TIME
        )]
    }
}

/// Event table schema definition
#[derive(Copy, Clone)]
pub struct Events;

impl_table_def!(Events, "events", "core", "id");

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
            // Index on source, event_type and ts_ingest (DESC)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_events_source_type_ts")
                .col(Alias::new(Self::SOURCE))
                .col(Alias::new(Self::EVENT_TYPE))
                .col((Alias::new(Self::TS_INGEST), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Index on host and ts_ingest (DESC)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_events_host_ts")
                .col(Alias::new(Self::HOST))
                .col((Alias::new(Self::TS_INGEST), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Partial index on payload_schema_name WHERE payload_schema_name IS NOT NULL
            format!(
                "CREATE INDEX idx_core_events_schema_name ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::PAYLOAD_SCHEMA_NAME, Self::PAYLOAD_SCHEMA_NAME
            ),
            // Partial index on source_material_id WHERE source_material_id IS NOT NULL
            format!(
                "CREATE INDEX idx_core_events_source_material ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::SOURCE_MATERIAL_ID, Self::SOURCE_MATERIAL_ID
            ),
            // GIN index on source_event_ids WHERE source_event_ids IS NOT NULL
            format!(
                "CREATE INDEX idx_core_events_provenance ON {}.{} USING GIN ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::SOURCE_EVENT_IDS, Self::SOURCE_EVENT_IDS
            ),
            // Partial index on ts_ingest (DESC) WHERE source_event_ids IS NULL (raw events)
            format!(
                "CREATE INDEX idx_core_events_raw_events ON {}.{} ({} DESC) WHERE {} IS NULL",
                Self::SCHEMA, Self::TABLE, Self::TS_INGEST, Self::SOURCE_EVENT_IDS
            ),
            // Partial index on ts_ingest (DESC) WHERE source_event_ids IS NOT NULL (synthesis events)
            format!(
                "CREATE INDEX idx_core_events_synthesis_events ON {}.{} ({} DESC) WHERE {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::TS_INGEST, Self::SOURCE_EVENT_IDS
            ),
            // GIN index on associated_blob_ids WHERE associated_blob_ids IS NOT NULL
            format!(
                "CREATE INDEX idx_core_events_associated_blobs ON {}.{} USING GIN ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::ASSOCIATED_BLOB_IDS, Self::ASSOCIATED_BLOB_IDS
            ),
            // GIN index on payload for JSONB path operations
            format!(
                "CREATE INDEX idx_core_events_payload_gin ON {}.{} USING GIN ({} jsonb_path_ops)",
                Self::SCHEMA, Self::TABLE, Self::PAYLOAD
            ),
        ]
    }

    /// Create foreign key constraints for the events table
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Foreign key to source_material_registry
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_events_source_material FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::SOURCE_MATERIAL_ID,
                SourceMaterials::SCHEMA, SourceMaterials::TABLE, SourceMaterials::BLOB_ID
            ),
            // Foreign key to processor_manifests
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_events_processor_manifest FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::PROCESSOR_MANIFEST_ID,
                ProcessorManifests::SCHEMA, ProcessorManifests::TABLE, ProcessorManifests::MANIFEST_ID
            ),
        ]
    }
}

/// Processor checkpoints table schema definition
#[derive(Copy, Clone)]
pub struct ProcessorCheckpoints;

impl_table_def!(ProcessorCheckpoints, "processor_checkpoints", "core", "id");

impl ProcessorCheckpoints {
    pub const TABLE: &'static str = "processor_checkpoints";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const PROCESSOR_NAME: &'static str = "processor_name";
    pub const CONSUMER_GROUP: &'static str = "consumer_group";
    pub const CONSUMER_NAME: &'static str = "consumer_name";
    pub const LAST_PROCESSED_ID: &'static str = "last_processed_id";
    pub const LAST_PROCESSED_TS: &'static str = "last_processed_ts";
    pub const PROCESSED_COUNT: &'static str = "processed_count";
    pub const CHECKPOINT_DATA: &'static str = "checkpoint_data";
    pub const STATE_DATA: &'static str = "state_data";
    pub const CHECKPOINT_VERSION: &'static str = "checkpoint_version";
    pub const LAST_ACTIVITY: &'static str = "last_activity";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";

    /// Create the processor checkpoints table
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
                ColumnDef::new(Alias::new(Self::PROCESSOR_NAME))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CONSUMER_GROUP))
                    .text()
                    .not_null()
                    .default("'default'"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CONSUMER_NAME))
                    .text()
                    .not_null()
                    .default("'default'"),
            )
            .col(ColumnDef::new(Alias::new(Self::LAST_PROCESSED_ID)).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Alias::new(Self::LAST_PROCESSED_TS)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::PROCESSED_COUNT))
                    .big_integer()
                    .not_null()
                    .default(0),
            )
            .col(ColumnDef::new(Alias::new(Self::CHECKPOINT_DATA)).json_binary())
            .col(ColumnDef::new(Alias::new(Self::STATE_DATA)).json_binary())
            .col(
                ColumnDef::new(Alias::new(Self::CHECKPOINT_VERSION))
                    .integer()
                    .not_null()
                    .default(1),
            )
            .col(
                ColumnDef::new(Alias::new(Self::LAST_ACTIVITY))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the processor checkpoints table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on updated_at for time-based queries
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_processor_checkpoints_updated")
                .col((Alias::new(Self::UPDATED_AT), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Index on processor_name for lookups
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_processor_checkpoints_processor")
                .col(Alias::new(Self::PROCESSOR_NAME))
                .build(PostgresQueryBuilder),
            // Index on consumer group and name
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_processor_checkpoints_consumer")
                .col(Alias::new(Self::CONSUMER_GROUP))
                .col(Alias::new(Self::CONSUMER_NAME))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create unique constraint
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Using raw SQL for the unique constraint as SeaQuery doesn't have direct support
            format!("ALTER TABLE {}.{} ADD CONSTRAINT unique_processor_consumer UNIQUE (processor_name, consumer_group, consumer_name)", 
                Self::SCHEMA, Self::TABLE)
        ]
    }
}

/// Schema registry table definition
#[derive(Copy, Clone)]
pub struct EventPayloadSchemas;

impl_table_def!(
    EventPayloadSchemas,
    "event_payload_schemas",
    "sinex_schemas",
    "id"
);

impl EventPayloadSchemas {
    pub const TABLE: &'static str = "event_payload_schemas";
    pub const SCHEMA: &'static str = "sinex_schemas";

    pub const ID: &'static str = "id";
    pub const SCHEMA_NAME: &'static str = "schema_name";
    pub const SCHEMA_VERSION: &'static str = "schema_version";
    pub const SCHEMA_CONTENT: &'static str = "schema_content";
    pub const IS_ACTIVE: &'static str = "is_active";
    pub const EVENT_TYPES: &'static str = "event_types";
    pub const DESCRIPTION: &'static str = "description";
    pub const EXAMPLES: &'static str = "examples";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const DEPRECATED_AT: &'static str = "deprecated_at";
    pub const DEPRECATION_REASON: &'static str = "deprecation_reason";
    // Added in migration 12
    pub const CONTENT_HASH: &'static str = "content_hash";
    pub const SOURCE: &'static str = "source";
    pub const EVENT_TYPE: &'static str = "event_type";

    /// Create the event payload schemas table
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
                ColumnDef::new(Alias::new(Self::SCHEMA_NAME))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SCHEMA_VERSION))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SCHEMA_CONTENT))
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::IS_ACTIVE))
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_TYPES))
                    .array(sea_query::ColumnType::Text)
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::DESCRIPTION)).text())
            .col(ColumnDef::new(Alias::new(Self::EXAMPLES)).json_binary())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::DEPRECATED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::DEPRECATION_REASON)).text())
            // Note: content_hash, source, and event_type are added by migration m20240108_000008
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event payload schemas table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on active schemas
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_schemas_active")
                .col(Alias::new(Self::SCHEMA_NAME))
                .col(Alias::new(Self::SCHEMA_VERSION))
                .index_type(sea_query::IndexType::BTree)
                .build(PostgresQueryBuilder)
                + " WHERE is_active = true",
            // GIN index on event_types array - using raw SQL due to SeaQuery limitation
            format!(
                "CREATE INDEX idx_schemas_event_types ON {}.{} USING GIN ({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::EVENT_TYPES
            ),
            // Note: idx_schemas_content_hash is added by migration m20240108_000008
        ]
    }

    /// Create constraints
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Unique constraint on schema name and version
            format!("ALTER TABLE {}.{} ADD CONSTRAINT unique_schema_name_version UNIQUE (schema_name, schema_version)",
                Self::SCHEMA, Self::TABLE),
            // Note: unique_content_hash and unique_schema_identity are added by migration m20240108_000008
        ]
    }
}

/// Source material registry table definition
#[derive(Copy, Clone)]
pub struct SourceMaterials;

impl_table_def!(
    SourceMaterials,
    "source_material_registry",
    "raw",
    "blob_id"
);

impl SourceMaterials {
    pub const TABLE: &'static str = "source_material_registry";
    pub const SCHEMA: &'static str = "raw";

    pub const BLOB_ID: &'static str = "blob_id";
    pub const MATERIAL_TYPE: &'static str = "material_type";
    pub const SOURCE_URI: &'static str = "source_uri";
    pub const INGESTION_TIME: &'static str = "ingestion_time";
    pub const FILE_SIZE_BYTES: &'static str = "file_size_bytes";
    pub const CHECKSUM_BLAKE3: &'static str = "checksum_blake3";
    pub const MIME_TYPE: &'static str = "mime_type";
    pub const ENCODING: &'static str = "encoding";
    pub const METADATA: &'static str = "metadata";
    pub const CONTENT_PREVIEW: &'static str = "content_preview";
    pub const IS_ARCHIVED: &'static str = "is_archived";
    pub const ARCHIVE_TIME: &'static str = "archive_time";
    pub const RETENTION_POLICY: &'static str = "retention_policy";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";

    /// Create the source materials table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::BLOB_ID))
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::MATERIAL_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::SOURCE_URI)).text())
            .col(
                ColumnDef::new(Alias::new(Self::INGESTION_TIME))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::FILE_SIZE_BYTES)).big_integer())
            .col(ColumnDef::new(Alias::new(Self::CHECKSUM_BLAKE3)).text())
            .col(ColumnDef::new(Alias::new(Self::MIME_TYPE)).text())
            .col(ColumnDef::new(Alias::new(Self::ENCODING)).text())
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::json")),
            )
            .col(ColumnDef::new(Alias::new(Self::CONTENT_PREVIEW)).text())
            .col(
                ColumnDef::new(Alias::new(Self::IS_ARCHIVED))
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(ColumnDef::new(Alias::new(Self::ARCHIVE_TIME)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::RETENTION_POLICY)).text())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the source materials table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on material_type and ingestion_time
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_source_material_type_time")
                .col(Alias::new(Self::MATERIAL_TYPE))
                .col((Alias::new(Self::INGESTION_TIME), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Partial index on source_uri
            format!(
                "CREATE INDEX idx_source_material_uri ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::SOURCE_URI,
                Self::SOURCE_URI
            ),
            // Partial index on checksum_blake3
            format!(
                "CREATE INDEX idx_source_material_checksum ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::CHECKSUM_BLAKE3,
                Self::CHECKSUM_BLAKE3
            ),
        ]
    }
}

/// Outbox table definition for transactional outbox pattern
#[derive(Copy, Clone)]
pub struct Outbox;

impl_table_def!(Outbox, "outbox", "core", "id");

impl Outbox {
    pub const TABLE: &'static str = "outbox";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const SUBJECT: &'static str = "subject";
    pub const PAYLOAD: &'static str = "payload";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the outbox table
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
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::SUBJECT)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::PAYLOAD))
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the outbox table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on created_at for processing order
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_outbox_created_at")
                .col(Alias::new(Self::CREATED_AT))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Operations log table definition
#[derive(Copy, Clone)]
pub struct OperationsLog;

impl_table_def!(OperationsLog, "operations_log", "core", "id");

impl OperationsLog {
    pub const TABLE: &'static str = "operations_log";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const ACTOR: &'static str = "actor";
    pub const SCOPE: &'static str = "scope";
    pub const STATE: &'static str = "state";
    pub const PREVIEW_SUMMARY: &'static str = "preview_summary";
    pub const CHECKPOINT: &'static str = "checkpoint";
    pub const APPROVED_BY: &'static str = "approved_by";
    pub const APPROVED_AT: &'static str = "approved_at";
    pub const EXECUTOR_NODE: &'static str = "executor_node";
    pub const STARTED_AT: &'static str = "started_at";
    pub const FINISHED_AT: &'static str = "finished_at";
    pub const OUTCOME: &'static str = "outcome";
    pub const ERROR_DETAILS: &'static str = "error_details";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the operations log table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Alias::new(Self::ACTOR)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::SCOPE))
                    .json_binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::STATE))
                    .text()
                    .not_null()
                    .default("planning"),
            )
            .col(ColumnDef::new(Alias::new(Self::PREVIEW_SUMMARY)).json_binary())
            .col(ColumnDef::new(Alias::new(Self::CHECKPOINT)).json_binary())
            .col(ColumnDef::new(Alias::new(Self::APPROVED_BY)).text())
            .col(ColumnDef::new(Alias::new(Self::APPROVED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::EXECUTOR_NODE)).text())
            .col(ColumnDef::new(Alias::new(Self::STARTED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::FINISHED_AT)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::OUTCOME))
                    .text()
                    .check(Expr::cust("outcome IN ('success', 'error', 'cancelled')")),
            )
            .col(ColumnDef::new(Alias::new(Self::ERROR_DETAILS)).text())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Add generated column for operation_ts (no longer needed)
    pub fn add_generated_column() -> String {
        // No generated column needed with new schema
        String::new()
    }

    /// Create indexes for the operations log table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on actor and started_at
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_actor_started")
                .col(Alias::new(Self::ACTOR))
                .col(Alias::new(Self::STARTED_AT))
                .build(PostgresQueryBuilder),
            // Index on outcome and started_at
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_outcome_started")
                .col(Alias::new(Self::OUTCOME))
                .col(Alias::new(Self::STARTED_AT))
                .build(PostgresQueryBuilder),
            // Index on started_at
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_log_started_at")
                .col(Alias::new(Self::STARTED_AT))
                .build(PostgresQueryBuilder),
            // Partial index on state for active states
            format!(
                "CREATE INDEX idx_operations_log_state ON {}.{} (state) WHERE state IN ('planning', 'previewed', 'approved', 'executing')",
                Self::SCHEMA, Self::TABLE
            ),
        ]
    }
}

/// Archived events table definition
#[derive(Copy, Clone)]
pub struct ArchivedEvents;

impl_table_def!(ArchivedEvents, "archived_events", "audit", "id");

impl ArchivedEvents {
    pub const TABLE: &'static str = "archived_events";
    pub const SCHEMA: &'static str = "audit";

    pub const ARCHIVED_AT: &'static str = "archived_at";
    pub const ARCHIVE_REASON: &'static str = "archive_reason";

    /// Create the archived events table - using LIKE syntax
    pub fn create_table() -> String {
        // SeaQuery doesn't support PostgreSQL's LIKE table INCLUDING ALL syntax,
        // so we need to use raw SQL
        format!(
            r#"CREATE TABLE IF NOT EXISTS {}.{} (
    LIKE {}.{} INCLUDING ALL,
    {} TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    {} TEXT
)"#,
            Self::SCHEMA,
            Self::TABLE,
            Events::SCHEMA,
            Events::TABLE,
            Self::ARCHIVED_AT,
            Self::ARCHIVE_REASON
        )
    }

    /// Create indexes for the archived events table
    pub fn create_indexes() -> Vec<String> {
        // No indexes defined in the SQL migration for archived_events
        vec![]
    }
}

/// Entities table definition (Knowledge Graph)
#[derive(Copy, Clone)]
pub struct Entities;

impl_table_def!(Entities, "entities", "core", "id");

impl Entities {
    pub const TABLE: &'static str = "entities";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const TYPE: &'static str = "type";
    pub const NAME: &'static str = "name";
    pub const CANONICAL_NAME: &'static str = "canonical_name";
    pub const ALIASES: &'static str = "aliases";
    pub const DESCRIPTION: &'static str = "description";
    pub const METADATA: &'static str = "metadata";
    pub const MERGED_INTO_ID: &'static str = "merged_into_id";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const CREATED_FROM_EVENT_ID: &'static str = "created_from_event_id";

    /// Create the entities table
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
            .col(ColumnDef::new(Alias::new(Self::TYPE)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::NAME)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::CANONICAL_NAME)).text())
            .col(ColumnDef::new(Alias::new(Self::ALIASES)).array(sea_query::ColumnType::Text))
            .col(ColumnDef::new(Alias::new(Self::DESCRIPTION)).text())
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::json")),
            )
            .col(ColumnDef::new(Alias::new(Self::MERGED_INTO_ID)).custom(Alias::new("ULID")))
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::CREATED_FROM_EVENT_ID)).custom(Alias::new("ULID")))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the entities table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entities_type")
                .col(Alias::new(Self::TYPE))
                .build(PostgresQueryBuilder),
            // Index on name
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entities_name")
                .col(Alias::new(Self::NAME))
                .build(PostgresQueryBuilder),
            // Partial index on canonical_name WHERE canonical_name IS NOT NULL
            format!(
                "CREATE INDEX idx_entities_canonical ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::CANONICAL_NAME,
                Self::CANONICAL_NAME
            ),
            // Partial index on created_from_event_id WHERE created_from_event_id IS NOT NULL
            format!(
                "CREATE INDEX idx_entities_created_from ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::CREATED_FROM_EVENT_ID,
                Self::CREATED_FROM_EVENT_ID
            ),
        ]
    }

    /// Create constraints for the entities table
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Unique constraint on name and type
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_entity_name_type UNIQUE ({}, {})",
                Self::SCHEMA, Self::TABLE, Self::NAME, Self::TYPE
            ),
            // Self-referencing foreign key for merged_into_id
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_entities_merged_into FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::MERGED_INTO_ID,
                Self::SCHEMA, Self::TABLE, Self::ID
            ),
        ]
    }
}

/// Entity relations table definition (Knowledge Graph)
#[derive(Copy, Clone)]
pub struct EntityRelations;

impl_table_def!(EntityRelations, "entity_relations", "core", "id");

impl EntityRelations {
    pub const TABLE: &'static str = "entity_relations";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const FROM_ENTITY_ID: &'static str = "from_entity_id";
    pub const TO_ENTITY_ID: &'static str = "to_entity_id";
    pub const RELATION_TYPE: &'static str = "relation_type";
    pub const STRENGTH: &'static str = "strength";
    pub const METADATA: &'static str = "metadata";
    pub const VALID_FROM: &'static str = "valid_from";
    pub const VALID_UNTIL: &'static str = "valid_until";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const CREATED_FROM_EVENT_ID: &'static str = "created_from_event_id";

    /// Create the entity relations table
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
                ColumnDef::new(Alias::new(Self::FROM_ENTITY_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TO_ENTITY_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::RELATION_TYPE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::STRENGTH))
                    .double()
                    .check(Expr::cust("strength >= 0 AND strength <= 1")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::json")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::VALID_FROM))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::VALID_UNTIL)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::CREATED_FROM_EVENT_ID)).custom(Alias::new("ULID")))
            .check(Expr::cust("from_entity_id != to_entity_id"))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the entity relations table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on from_entity_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entity_relations_from")
                .col(Alias::new(Self::FROM_ENTITY_ID))
                .build(PostgresQueryBuilder),
            // Index on to_entity_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entity_relations_to")
                .col(Alias::new(Self::TO_ENTITY_ID))
                .build(PostgresQueryBuilder),
            // Index on relation_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entity_relations_type")
                .col(Alias::new(Self::RELATION_TYPE))
                .build(PostgresQueryBuilder),
            // Partial index on created_from_event_id WHERE created_from_event_id IS NOT NULL
            format!(
                "CREATE INDEX idx_entity_relations_created_from ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::CREATED_FROM_EVENT_ID,
                Self::CREATED_FROM_EVENT_ID
            ),
        ]
    }

    /// Create constraints for the entity relations table
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Unique constraint on from_entity_id, to_entity_id, relation_type
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_entity_relation UNIQUE ({}, {}, {})",
                Self::SCHEMA, Self::TABLE, Self::FROM_ENTITY_ID, Self::TO_ENTITY_ID, Self::RELATION_TYPE
            ),
            // Foreign key from_entity_id to entities
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_entity_relations_from FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::FROM_ENTITY_ID,
                Entities::SCHEMA, Entities::TABLE, Entities::ID
            ),
            // Foreign key to_entity_id to entities
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_entity_relations_to FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::TO_ENTITY_ID,
                Entities::SCHEMA, Entities::TABLE, Entities::ID
            ),
            // Check constraint no_self_relation is already added inline
        ]
    }
}

/// Event annotations table definition
#[derive(Copy, Clone)]
pub struct EventAnnotations;

impl_table_def!(EventAnnotations, "event_annotations", "core", "id");

impl EventAnnotations {
    pub const TABLE: &'static str = "event_annotations";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const ANNOTATION_TYPE: &'static str = "annotation_type";
    pub const CONTENT: &'static str = "content";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const CREATED_BY: &'static str = "created_by";

    /// Create the event annotations table
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
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ANNOTATION_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::CONTENT)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_BY))
                    .text()
                    .not_null()
                    .default("'user'"),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event annotations table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on event_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_annotations_event")
                .col(Alias::new(Self::EVENT_ID))
                .build(PostgresQueryBuilder),
            // Index on annotation_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_annotations_type")
                .col(Alias::new(Self::ANNOTATION_TYPE))
                .build(PostgresQueryBuilder),
            // Index on created_at
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_annotations_created")
                .col(Alias::new(Self::CREATED_AT))
                .build(PostgresQueryBuilder),
            // GIN index for full-text search on content
            format!(
                "CREATE INDEX idx_event_annotations_search ON {}.{} USING gin(to_tsvector('english', {}))",
                Self::SCHEMA, Self::TABLE, Self::CONTENT
            ),
        ]
    }

    /// Create constraints for the event annotations table
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Foreign key to events table
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_annotations_event FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::ID
            ),
        ]
    }
}
/// Embedding models table definition
#[derive(Copy, Clone)]
pub struct EmbeddingModels;

impl_table_def!(EmbeddingModels, "embedding_models", "core", "id");

impl EmbeddingModels {
    pub const TABLE: &'static str = "embedding_models";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const PROVIDER: &'static str = "provider";
    pub const MODEL_NAME: &'static str = "model_name";
    pub const DIMENSIONS: &'static str = "dimensions";
    pub const MAX_INPUT_TOKENS: &'static str = "max_input_tokens";
    pub const COST_PER_1K_TOKENS: &'static str = "cost_per_1k_tokens";
    pub const IS_ACTIVE: &'static str = "is_active";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the embedding models table
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
            .col(ColumnDef::new(Alias::new(Self::PROVIDER)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::MODEL_NAME))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::DIMENSIONS))
                    .integer()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::MAX_INPUT_TOKENS)).integer())
            .col(ColumnDef::new(Alias::new(Self::COST_PER_1K_TOKENS)).decimal_len(10, 6))
            .col(
                ColumnDef::new(Alias::new(Self::IS_ACTIVE))
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the embedding models table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on is_active and provider
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_embedding_models_active")
                .col(Alias::new(Self::IS_ACTIVE))
                .col(Alias::new(Self::PROVIDER))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create constraints for the embedding models table
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Unique constraint on provider and model_name
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_embedding_model UNIQUE({}, {})",
                Self::SCHEMA,
                Self::TABLE,
                Self::PROVIDER,
                Self::MODEL_NAME
            ),
        ]
    }
}

/// Embedding cache table definition
#[derive(Copy, Clone)]
pub struct EmbeddingCache;

impl_table_def!(EmbeddingCache, "embedding_cache", "core", "id");

impl EmbeddingCache {
    pub const TABLE: &'static str = "embedding_cache";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const TEXT_HASH: &'static str = "text_hash";
    pub const EMBEDDING_MODEL_ID: &'static str = "embedding_model_id";
    pub const EMBEDDING: &'static str = "embedding";
    pub const TEXT_SAMPLE: &'static str = "text_sample";
    pub const USE_COUNT: &'static str = "use_count";
    pub const CREATED_AT: &'static str = "created_at";
    pub const LAST_USED_AT: &'static str = "last_used_at";

    /// Create the embedding cache table
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
                ColumnDef::new(Alias::new(Self::TEXT_HASH))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EMBEDDING_MODEL_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EMBEDDING))
                    .custom(Alias::new("vector(1536)"))
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::TEXT_SAMPLE)).text())
            .col(
                ColumnDef::new(Alias::new(Self::USE_COUNT))
                    .integer()
                    .not_null()
                    .default(1),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::LAST_USED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the embedding cache table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on text_hash
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_embedding_cache_hash")
                .col(Alias::new(Self::TEXT_HASH))
                .build(PostgresQueryBuilder),
            // Index on last_used_at
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_embedding_cache_lru")
                .col(Alias::new(Self::LAST_USED_AT))
                .build(PostgresQueryBuilder),
            // Vector index for similarity search
            format!(
                "CREATE INDEX idx_embedding_cache_vector ON {}.{} USING ivfflat ({} vector_cosine_ops) WITH (lists = 100)",
                Self::SCHEMA, Self::TABLE, Self::EMBEDDING
            ),
        ]
    }

    /// Create constraints for the embedding cache table
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Foreign key to embedding_models
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_embedding_cache_model FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::EMBEDDING_MODEL_ID,
                EmbeddingModels::SCHEMA, EmbeddingModels::TABLE, EmbeddingModels::ID
            ),
            // Unique constraint on text_hash and embedding_model_id
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_text_model_embedding UNIQUE({}, {})",
                Self::SCHEMA, Self::TABLE, Self::TEXT_HASH, Self::EMBEDDING_MODEL_ID
            ),
        ]
    }
}

/// Blobs table definition
#[derive(Copy, Clone)]
pub struct Blobs;

impl_table_def!(Blobs, "blobs", "core", "id");

impl Blobs {
    pub const TABLE: &'static str = "blobs";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const ANNEX_KEY: &'static str = "annex_key";
    pub const ORIGINAL_FILENAME: &'static str = "original_filename";
    pub const SIZE_BYTES: &'static str = "size_bytes";
    pub const MIME_TYPE: &'static str = "mime_type";
    pub const CHECKSUM_SHA256: &'static str = "checksum_sha256";
    pub const CHECKSUM_BLAKE3: &'static str = "checksum_blake3";
    pub const STORAGE_BACKEND: &'static str = "storage_backend";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";
    pub const LAST_VERIFIED_AT: &'static str = "last_verified_at";
    pub const VERIFICATION_STATUS: &'static str = "verification_status";

    /// Create the blobs table
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
                ColumnDef::new(Alias::new(Self::ANNEX_KEY))
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ORIGINAL_FILENAME))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SIZE_BYTES))
                    .big_integer()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::MIME_TYPE)).text())
            .col(
                ColumnDef::new(Alias::new(Self::CHECKSUM_SHA256))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::CHECKSUM_BLAKE3)).text())
            .col(
                ColumnDef::new(Alias::new(Self::STORAGE_BACKEND))
                    .text()
                    .not_null()
                    .default("'git-annex'"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::LAST_VERIFIED_AT)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::VERIFICATION_STATUS))
                    .text()
                    .check(Expr::cust(
                        "verification_status IN ('pending', 'verified', 'missing', 'corrupted')",
                    )),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the blobs table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on annex_key
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_blobs_annex_key")
                .col(Alias::new(Self::ANNEX_KEY))
                .build(PostgresQueryBuilder),
            // Index on checksum_sha256
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_blobs_checksum_sha256")
                .col(Alias::new(Self::CHECKSUM_SHA256))
                .build(PostgresQueryBuilder),
            // Partial index on checksum_blake3 WHERE checksum_blake3 IS NOT NULL
            format!(
                "CREATE INDEX idx_blobs_checksum_blake3 ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::CHECKSUM_BLAKE3,
                Self::CHECKSUM_BLAKE3
            ),
            // Index on verification_status and last_verified_at
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_blobs_verification")
                .col(Alias::new(Self::VERIFICATION_STATUS))
                .col(Alias::new(Self::LAST_VERIFIED_AT))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create constraints for the blobs table
    pub fn create_constraints() -> Vec<String> {
        vec![]
    }
}

/// Tags table definition
#[derive(Copy, Clone)]
pub struct Tags;

impl_table_def!(Tags, "tags", "core", "id");

impl Tags {
    pub const TABLE: &'static str = "tags";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const NAME: &'static str = "name";
    pub const DISPLAY_NAME: &'static str = "display_name";
    pub const COLOR: &'static str = "color";
    pub const ICON: &'static str = "icon";
    pub const PARENT_ID: &'static str = "parent_id";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the tags table
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
                ColumnDef::new(Alias::new(Self::NAME))
                    .text()
                    .not_null()
                    .unique_key(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::DISPLAY_NAME))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::COLOR)).text())
            .col(ColumnDef::new(Alias::new(Self::ICON)).text())
            .col(ColumnDef::new(Alias::new(Self::PARENT_ID)).custom(Alias::new("ULID")))
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the tags table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Partial index on parent_id WHERE parent_id IS NOT NULL
            format!(
                "CREATE INDEX idx_tags_parent ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::PARENT_ID,
                Self::PARENT_ID
            ),
            // Index on name
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_tags_name")
                .col(Alias::new(Self::NAME))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create constraints for the tags table
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Foreign key to self for parent_id
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_tags_parent FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::PARENT_ID,
                Self::SCHEMA, Self::TABLE, Self::ID
            ),
        ]
    }
}

/// Event relations table definition
#[derive(Copy, Clone)]
pub struct EventRelations;

impl_table_def!(EventRelations, "event_relations", "core", "id");

impl EventRelations {
    pub const TABLE: &'static str = "event_relations";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const FROM_EVENT_ID: &'static str = "from_event_id";
    pub const TO_EVENT_ID: &'static str = "to_event_id";
    pub const RELATION_TYPE: &'static str = "relation_type";
    pub const CONFIDENCE: &'static str = "confidence";
    pub const DETECTED_BY: &'static str = "detected_by";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the event relations table
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
                ColumnDef::new(Alias::new(Self::FROM_EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TO_EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::RELATION_TYPE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CONFIDENCE))
                    .float()
                    .default(1.0)
                    .check(Expr::cust("confidence >= 0 AND confidence <= 1")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::DETECTED_BY))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .check(Expr::cust("from_event_id != to_event_id"))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event relations table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on from_event_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_relations_from")
                .col(Alias::new(Self::FROM_EVENT_ID))
                .build(PostgresQueryBuilder),
            // Index on to_event_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_relations_to")
                .col(Alias::new(Self::TO_EVENT_ID))
                .build(PostgresQueryBuilder),
            // Index on relation_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_relations_type")
                .col(Alias::new(Self::RELATION_TYPE))
                .build(PostgresQueryBuilder),
            // Partial index on confidence WHERE confidence < 1.0
            format!(
                "CREATE INDEX idx_event_relations_confidence ON {}.{} ({}) WHERE {} < 1.0",
                Self::SCHEMA,
                Self::TABLE,
                Self::CONFIDENCE,
                Self::CONFIDENCE
            ),
        ]
    }

    /// Create constraints for the event relations table
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Foreign key from_event_id to events
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_relations_from FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::FROM_EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::ID
            ),
            // Foreign key to_event_id to events
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_relations_to FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::TO_EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::ID
            ),
            // Unique constraint on from_event_id, to_event_id, relation_type
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_event_relation UNIQUE({}, {}, {})",
                Self::SCHEMA, Self::TABLE, Self::FROM_EVENT_ID, Self::TO_EVENT_ID, Self::RELATION_TYPE
            ),
        ]
    }
}

/// Event clusters table definition
#[derive(Copy, Clone)]
pub struct EventClusters;

impl_table_def!(EventClusters, "event_clusters", "core", "id");

impl EventClusters {
    pub const TABLE: &'static str = "event_clusters";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const NAME: &'static str = "name";
    pub const CLUSTER_TYPE: &'static str = "cluster_type";
    pub const SUMMARY: &'static str = "summary";
    pub const TIME_START: &'static str = "time_start";
    pub const TIME_END: &'static str = "time_end";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";

    /// Create the event clusters table
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
            .col(ColumnDef::new(Alias::new(Self::NAME)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::CLUSTER_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::SUMMARY)).text())
            .col(
                ColumnDef::new(Alias::new(Self::TIME_START))
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TIME_END))
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event clusters table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on cluster_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_clusters_type")
                .col(Alias::new(Self::CLUSTER_TYPE))
                .build(PostgresQueryBuilder),
            // Index on time_start and time_end
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_clusters_time")
                .col(Alias::new(Self::TIME_START))
                .col(Alias::new(Self::TIME_END))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create constraints for the event clusters table
    pub fn create_constraints() -> Vec<String> {
        vec![]
    }
}

/// Event cluster members table definition
#[derive(Copy, Clone)]
pub struct EventClusterMembers;

impl_table_def!(
    EventClusterMembers,
    "event_cluster_members",
    "core",
    "(cluster_id, event_id)"
);

impl EventClusterMembers {
    pub const TABLE: &'static str = "event_cluster_members";
    pub const SCHEMA: &'static str = "core";

    pub const CLUSTER_ID: &'static str = "cluster_id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const ROLE: &'static str = "role";
    pub const ADDED_AT: &'static str = "added_at";
    pub const METADATA: &'static str = "metadata";

    /// Create the event cluster members table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::CLUSTER_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::ROLE)).text())
            .col(
                ColumnDef::new(Alias::new(Self::ADDED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .primary_key(
                Index::create()
                    .col(Alias::new(Self::CLUSTER_ID))
                    .col(Alias::new(Self::EVENT_ID)),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event cluster members table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on event_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_cluster_members_event")
                .col(Alias::new(Self::EVENT_ID))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create constraints for the event cluster members table
    pub fn create_constraints() -> Vec<String> {
        vec![
            // Foreign key cluster_id to event_clusters
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_cluster_members_cluster FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::CLUSTER_ID,
                EventClusters::SCHEMA, EventClusters::TABLE, EventClusters::ID
            ),
            // Foreign key event_id to events
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_cluster_members_event FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::ID
            ),
        ]
    }
}

/// Schema compatibility table definition
#[derive(Copy, Clone)]
pub struct SchemaCompatibility;

impl_table_def!(
    SchemaCompatibility,
    "schema_compatibility",
    "sinex_schemas",
    "id"
);

impl SchemaCompatibility {
    pub const TABLE: &'static str = "schema_compatibility";
    pub const SCHEMA: &'static str = "sinex_schemas";

    pub const ID: &'static str = "id";
    pub const FROM_SCHEMA_ID: &'static str = "from_schema_id";
    pub const TO_SCHEMA_ID: &'static str = "to_schema_id";
    pub const COMPATIBILITY_TYPE: &'static str = "compatibility_type";
    pub const MIGRATION_STRATEGY: &'static str = "migration_strategy";
    pub const NOTES: &'static str = "notes";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the schema compatibility table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::FROM_SCHEMA_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TO_SCHEMA_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::COMPATIBILITY_TYPE))
                    .text()
                    .not_null()
                    .check(Expr::cust(
                        "compatibility_type IN ('backward', 'forward', 'full', 'none')",
                    )),
            )
            .col(ColumnDef::new(Alias::new(Self::MIGRATION_STRATEGY)).json_binary())
            .col(ColumnDef::new(Alias::new(Self::NOTES)).text())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the schema compatibility table
    pub fn create_indexes() -> Vec<String> {
        vec![
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_schema_compat_from")
                .col(Alias::new(Self::FROM_SCHEMA_ID))
                .build(PostgresQueryBuilder),
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_schema_compat_to")
                .col(Alias::new(Self::TO_SCHEMA_ID))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create constraints for the schema compatibility table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_schema_pair UNIQUE ({}, {})",
                Self::SCHEMA, Self::TABLE, Self::FROM_SCHEMA_ID, Self::TO_SCHEMA_ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT no_self_reference CHECK ({} != {})",
                Self::SCHEMA, Self::TABLE, Self::FROM_SCHEMA_ID, Self::TO_SCHEMA_ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_from_schema FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::FROM_SCHEMA_ID,
                EventPayloadSchemas::SCHEMA, EventPayloadSchemas::TABLE, EventPayloadSchemas::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_to_schema FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::TO_SCHEMA_ID,
                EventPayloadSchemas::SCHEMA, EventPayloadSchemas::TABLE, EventPayloadSchemas::ID
            ),
        ]
    }
}

/// GitOps schema sources table definition
#[derive(Copy, Clone)]
pub struct GitopsSchemaSource;

impl_table_def!(
    GitopsSchemaSource,
    "gitops_schema_sources",
    "sinex_schemas",
    "id"
);

impl GitopsSchemaSource {
    pub const TABLE: &'static str = "gitops_schema_sources";
    pub const SCHEMA: &'static str = "sinex_schemas";

    pub const ID: &'static str = "id";
    pub const REPOSITORY_URL: &'static str = "repository_url";
    pub const BRANCH: &'static str = "branch";
    pub const PATH_PATTERN: &'static str = "path_pattern";
    pub const SYNC_ENABLED: &'static str = "sync_enabled";
    pub const LAST_SYNC_AT: &'static str = "last_sync_at";
    pub const LAST_SYNC_COMMIT: &'static str = "last_sync_commit";
    pub const SYNC_FREQUENCY_MINUTES: &'static str = "sync_frequency_minutes";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";

    /// Create the gitops schema sources table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::REPOSITORY_URL))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::BRANCH))
                    .text()
                    .not_null()
                    .default("'main'"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PATH_PATTERN))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SYNC_ENABLED))
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .col(ColumnDef::new(Alias::new(Self::LAST_SYNC_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::LAST_SYNC_COMMIT)).text())
            .col(
                ColumnDef::new(Alias::new(Self::SYNC_FREQUENCY_MINUTES))
                    .integer()
                    .not_null()
                    .default(60),
            )
            .col(ColumnDef::new(Alias::new(Self::METADATA)).json_binary())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the gitops schema sources table
    pub fn create_indexes() -> Vec<String> {
        vec![
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_gitops_sources_sync")
                .col(Alias::new(Self::SYNC_ENABLED))
                .col(Alias::new(Self::LAST_SYNC_AT))
                .build(PostgresQueryBuilder)
                + " WHERE sync_enabled = true",
        ]
    }

    /// Create constraints for the gitops schema sources table
    pub fn create_constraints() -> Vec<String> {
        vec![format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT unique_repo_branch_path UNIQUE ({}, {}, {})",
            Self::SCHEMA,
            Self::TABLE,
            Self::REPOSITORY_URL,
            Self::BRANCH,
            Self::PATH_PATTERN
        )]
    }
}

/// Validation cache table definition
#[derive(Copy, Clone)]
pub struct ValidationCache;

impl_table_def!(ValidationCache, "validation_cache", "sinex_schemas", "id");

impl ValidationCache {
    pub const TABLE: &'static str = "validation_cache";
    pub const SCHEMA: &'static str = "sinex_schemas";

    pub const ID: &'static str = "id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const SCHEMA_ID: &'static str = "schema_id";
    pub const IS_VALID: &'static str = "is_valid";
    pub const VALIDATION_ERRORS: &'static str = "validation_errors";
    pub const VALIDATED_AT: &'static str = "validated_at";

    /// Create the validation cache table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SCHEMA_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::IS_VALID))
                    .boolean()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::VALIDATION_ERRORS)).json_binary())
            .col(
                ColumnDef::new(Alias::new(Self::VALIDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the validation cache table
    pub fn create_indexes() -> Vec<String> {
        vec![
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_validation_cache_event")
                .col(Alias::new(Self::EVENT_ID))
                .build(PostgresQueryBuilder),
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_validation_cache_schema")
                .col(Alias::new(Self::SCHEMA_ID))
                .build(PostgresQueryBuilder),
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_validation_cache_invalid")
                .col(Alias::new(Self::SCHEMA_ID))
                .col(Alias::new(Self::VALIDATED_AT))
                .build(PostgresQueryBuilder)
                + " WHERE is_valid = false",
        ]
    }

    /// Create constraints for the validation cache table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_event_schema_validation UNIQUE ({}, {})",
                Self::SCHEMA, Self::TABLE, Self::EVENT_ID, Self::SCHEMA_ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_validation_schema FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::SCHEMA_ID,
                EventPayloadSchemas::SCHEMA, EventPayloadSchemas::TABLE, EventPayloadSchemas::ID
            ),
        ]
    }
}
