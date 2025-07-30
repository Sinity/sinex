//! Database schema definitions using SeaQuery
//!
//! This module provides type-safe schema definitions for all database tables
//! using SeaQuery's table definition API.

use sea_query::{Alias, PostgresQueryBuilder};
use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Index, Table};

/// Event table schema definition
#[derive(Copy, Clone)]
pub struct Events;

impl Events {
    pub const TABLE: &'static str = "events";
    pub const SCHEMA: &'static str = "core";

    pub const EVENT_ID: &'static str = "event_id";
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

    /// Create the events table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Alias::new(Self::SOURCE)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::HOST)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::PAYLOAD)).json().not_null())
            .col(ColumnDef::new(Alias::new(Self::TS_ORIG)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::TS_INGEST))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(ColumnDef::new(Alias::new(Self::INGESTOR_VERSION)).text())
            .col(ColumnDef::new(Alias::new(Self::PAYLOAD_SCHEMA_ID)).uuid())
            .col(
                ColumnDef::new(Alias::new(Self::SOURCE_EVENT_IDS))
                    .array(sea_query::ColumnType::Uuid),
            )
            .col(ColumnDef::new(Alias::new(Self::SOURCE_MATERIAL_ID)).uuid())
            .col(ColumnDef::new(Alias::new(Self::SOURCE_MATERIAL_OFFSET_START)).big_integer())
            .col(ColumnDef::new(Alias::new(Self::SOURCE_MATERIAL_OFFSET_END)).big_integer())
            .col(ColumnDef::new(Alias::new(Self::ANCHOR_BYTE)).big_integer())
            .col(
                ColumnDef::new(Alias::new(Self::ASSOCIATED_BLOB_IDS))
                    .array(sea_query::ColumnType::Uuid),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the events table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on source and event_type for filtering
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_events_source_type")
                .col(Alias::new(Self::SOURCE))
                .col(Alias::new(Self::EVENT_TYPE))
                .build(PostgresQueryBuilder),
            // Index on ts_orig for time-based queries
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_events_ts_orig")
                .col(Alias::new(Self::TS_ORIG))
                .build(PostgresQueryBuilder),
            // Index on ts_ingest for ingestion order
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_events_ts_ingest")
                .col(Alias::new(Self::TS_INGEST))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Checkpoint table schema definition
#[derive(Copy, Clone)]
pub struct Checkpoints;

impl Checkpoints {
    pub const TABLE: &'static str = "checkpoints";
    pub const SCHEMA: &'static str = "core";

    pub const CHECKPOINT_ID: &'static str = "checkpoint_id";
    pub const SATELLITE_ID: &'static str = "satellite_id";
    pub const STAGE_ID: &'static str = "stage_id";
    pub const STATE_TYPE: &'static str = "state_type";
    pub const STATE_DATA: &'static str = "state_data";
    pub const EVENT_ID_CHECKPOINT: &'static str = "event_id_checkpoint";
    pub const PROCESSING_STATE: &'static str = "processing_state";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const VERSION: &'static str = "version";
    pub const METADATA: &'static str = "metadata";

    /// Create the checkpoints table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::CHECKPOINT_ID))
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SATELLITE_ID))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::STAGE_ID)).text())
            .col(
                ColumnDef::new(Alias::new(Self::STATE_TYPE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::STATE_DATA))
                    .json()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::EVENT_ID_CHECKPOINT)).uuid())
            .col(ColumnDef::new(Alias::new(Self::PROCESSING_STATE)).json())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::VERSION))
                    .integer()
                    .not_null()
                    .default(1),
            )
            .col(ColumnDef::new(Alias::new(Self::METADATA)).json())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the checkpoints table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on satellite_id for lookups
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_checkpoints_satellite_id")
                .col(Alias::new(Self::SATELLITE_ID))
                .build(PostgresQueryBuilder),
            // Unique index on satellite_id and stage_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_checkpoints_satellite_stage")
                .unique()
                .col(Alias::new(Self::SATELLITE_ID))
                .col(Alias::new(Self::STAGE_ID))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Schema registry table definition
#[derive(Copy, Clone)]
pub struct Schemas;

impl Schemas {
    pub const TABLE: &'static str = "payload_schemas";
    pub const SCHEMA: &'static str = "core";

    pub const SCHEMA_ID: &'static str = "schema_id";
    pub const SCHEMA_NAME: &'static str = "schema_name";
    pub const SCHEMA_VERSION: &'static str = "schema_version";
    pub const SCHEMA_CONTENT: &'static str = "schema_content";
    pub const EVENT_SOURCE: &'static str = "event_source";
    pub const EVENT_TYPE: &'static str = "event_type";
    pub const CREATED_AT: &'static str = "created_at";
    pub const DEPRECATED_AT: &'static str = "deprecated_at";
    pub const MIGRATION_NOTES: &'static str = "migration_notes";

    /// Create the schemas table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::SCHEMA_ID))
                    .uuid()
                    .not_null()
                    .primary_key(),
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
                    .json()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_SOURCE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_TYPE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(ColumnDef::new(Alias::new(Self::DEPRECATED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::MIGRATION_NOTES)).text())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the schemas table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on schema name for lookups
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_schemas_name")
                .col(Alias::new(Self::SCHEMA_NAME))
                .build(PostgresQueryBuilder),
            // Index on source and type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_schemas_source_type")
                .col(Alias::new(Self::EVENT_SOURCE))
                .col(Alias::new(Self::EVENT_TYPE))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Source material registry table definition
#[derive(Copy, Clone)]
pub struct SourceMaterials;

impl SourceMaterials {
    pub const TABLE: &'static str = "source_material_registry";
    pub const SCHEMA: &'static str = "raw";

    pub const SOURCE_MATERIAL_ID: &'static str = "source_material_id";
    pub const MATERIAL_TYPE: &'static str = "material_type";
    pub const CONTENT_HASH: &'static str = "content_hash";
    pub const SIZE_BYTES: &'static str = "size_bytes";
    pub const URI: &'static str = "uri";
    pub const METADATA: &'static str = "metadata";
    pub const CAPTURED_AT: &'static str = "captured_at";
    pub const PROCESSING_STATUS: &'static str = "processing_status";
    pub const ERROR_MESSAGE: &'static str = "error_message";
    pub const SOURCE_SATELLITE: &'static str = "source_satellite";
    pub const REGISTERED_AT: &'static str = "registered_at";
    pub const COMPLETED_AT: &'static str = "completed_at";

    /// Create the source materials table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::SOURCE_MATERIAL_ID))
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::MATERIAL_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::CONTENT_HASH)).text())
            .col(ColumnDef::new(Alias::new(Self::SIZE_BYTES)).big_integer())
            .col(ColumnDef::new(Alias::new(Self::URI)).text())
            .col(ColumnDef::new(Alias::new(Self::METADATA)).json())
            .col(ColumnDef::new(Alias::new(Self::CAPTURED_AT)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::PROCESSING_STATUS))
                    .text()
                    .not_null()
                    .default("'pending'"),
            )
            .col(ColumnDef::new(Alias::new(Self::ERROR_MESSAGE)).text())
            .col(
                ColumnDef::new(Alias::new(Self::SOURCE_SATELLITE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::REGISTERED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(ColumnDef::new(Alias::new(Self::COMPLETED_AT)).timestamp_with_time_zone())
            .build(PostgresQueryBuilder)
    }
}

/// Operations log table definition
#[derive(Copy, Clone)]
pub struct OperationsLog;

impl OperationsLog {
    pub const TABLE: &'static str = "operations_log";
    pub const SCHEMA: &'static str = "core";

    pub const OPERATION_ID: &'static str = "operation_id";
    pub const OPERATION_TYPE: &'static str = "operation_type";
    pub const OPERATOR: &'static str = "operator";
    pub const PERFORMED_AT: &'static str = "performed_at";
    pub const PARAMETERS: &'static str = "parameters";
    pub const OUTCOME: &'static str = "outcome";
    pub const ERROR_MESSAGE: &'static str = "error_message";
    pub const AFFECTED_EVENT_IDS: &'static str = "affected_event_ids";
    pub const DURATION_MS: &'static str = "duration_ms";

    /// Create the operations log table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::OPERATION_ID))
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::OPERATION_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::OPERATOR)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::PERFORMED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PARAMETERS))
                    .json()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::OUTCOME)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::ERROR_MESSAGE)).text())
            .col(
                ColumnDef::new(Alias::new(Self::AFFECTED_EVENT_IDS))
                    .array(sea_query::ColumnType::Uuid),
            )
            .col(ColumnDef::new(Alias::new(Self::DURATION_MS)).integer())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the operations log table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on operation type for filtering
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_type")
                .col(Alias::new(Self::OPERATION_TYPE))
                .build(PostgresQueryBuilder),
            // Index on performed_at for time-based queries
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_operations_performed_at")
                .col(Alias::new(Self::PERFORMED_AT))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Archived events table definition
#[derive(Copy, Clone)]
pub struct ArchivedEvents;

impl ArchivedEvents {
    pub const TABLE: &'static str = "archived_events";
    pub const SCHEMA: &'static str = "audit";

    pub const ARCHIVE_ID: &'static str = "archive_id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const SOURCE: &'static str = "source";
    pub const EVENT_TYPE: &'static str = "event_type";
    pub const HOST: &'static str = "host";
    pub const PAYLOAD: &'static str = "payload";
    pub const TS_ORIG: &'static str = "ts_orig";
    pub const TS_INGEST: &'static str = "ts_ingest";
    pub const ARCHIVED_AT: &'static str = "archived_at";
    pub const ARCHIVE_REASON: &'static str = "archive_reason";
    pub const ARCHIVED_BY: &'static str = "archived_by";
    pub const REPLACEMENT_EVENT_ID: &'static str = "replacement_event_id";

    /// Create the archived events table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ARCHIVE_ID))
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Alias::new(Self::EVENT_ID)).uuid().not_null())
            .col(ColumnDef::new(Alias::new(Self::SOURCE)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::HOST)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::PAYLOAD)).json().not_null())
            .col(ColumnDef::new(Alias::new(Self::TS_ORIG)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::TS_INGEST))
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ARCHIVED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ARCHIVE_REASON))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ARCHIVED_BY))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::REPLACEMENT_EVENT_ID)).uuid())
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the archived events table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on original event_id for lookups
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_archived_events_event_id")
                .col(Alias::new(Self::EVENT_ID))
                .build(PostgresQueryBuilder),
            // Index on archived_at for time-based queries
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_archived_events_archived_at")
                .col(Alias::new(Self::ARCHIVED_AT))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Entities table definition (Knowledge Graph)
#[derive(Copy, Clone)]
pub struct Entities;

impl Entities {
    pub const TABLE: &'static str = "entities";
    pub const SCHEMA: &'static str = "core";

    pub const ENTITY_ID: &'static str = "entity_id";
    pub const ENTITY_TYPE: &'static str = "entity_type";
    pub const NAME: &'static str = "name";
    pub const PROPERTIES: &'static str = "properties";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const SOURCE_EVENT_IDS: &'static str = "source_event_ids";
    pub const IS_ACTIVE: &'static str = "is_active";

    /// Create the entities table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ENTITY_ID))
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ENTITY_TYPE))
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::NAME)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::PROPERTIES))
                    .json()
                    .not_null()
                    .default("'{}'"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SOURCE_EVENT_IDS))
                    .array(sea_query::ColumnType::Uuid),
            )
            .col(
                ColumnDef::new(Alias::new(Self::IS_ACTIVE))
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the entities table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on entity_type for filtering
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entities_type")
                .col(Alias::new(Self::ENTITY_TYPE))
                .build(PostgresQueryBuilder),
            // Index on name for search
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entities_name")
                .col(Alias::new(Self::NAME))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Entity relations table definition (Knowledge Graph)
#[derive(Copy, Clone)]
pub struct EntityRelations;

impl EntityRelations {
    pub const TABLE: &'static str = "entity_relations";
    pub const SCHEMA: &'static str = "core";

    pub const RELATION_ID: &'static str = "relation_id";
    pub const FROM_ENTITY_ID: &'static str = "from_entity_id";
    pub const TO_ENTITY_ID: &'static str = "to_entity_id";
    pub const RELATION_TYPE: &'static str = "relation_type";
    pub const PROPERTIES: &'static str = "properties";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const SOURCE_EVENT_IDS: &'static str = "source_event_ids";
    pub const IS_ACTIVE: &'static str = "is_active";

    /// Create the entity relations table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::RELATION_ID))
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::FROM_ENTITY_ID))
                    .uuid()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TO_ENTITY_ID))
                    .uuid()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::RELATION_TYPE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PROPERTIES))
                    .json()
                    .not_null()
                    .default("'{}'"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("CURRENT_TIMESTAMP"),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SOURCE_EVENT_IDS))
                    .array(sea_query::ColumnType::Uuid),
            )
            .col(
                ColumnDef::new(Alias::new(Self::IS_ACTIVE))
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        (Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)),
                        Alias::new(Self::FROM_ENTITY_ID),
                    )
                    .to(
                        (Alias::new(Entities::SCHEMA), Alias::new(Entities::TABLE)),
                        Alias::new(Entities::ENTITY_ID),
                    )
                    .on_delete(ForeignKeyAction::Cascade)
                    .on_update(ForeignKeyAction::Cascade),
            )
            .foreign_key(
                ForeignKey::create()
                    .from(
                        (Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)),
                        Alias::new(Self::TO_ENTITY_ID),
                    )
                    .to(
                        (Alias::new(Entities::SCHEMA), Alias::new(Entities::TABLE)),
                        Alias::new(Entities::ENTITY_ID),
                    )
                    .on_delete(ForeignKeyAction::Cascade)
                    .on_update(ForeignKeyAction::Cascade),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the entity relations table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on from_entity_id for traversal
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entity_relations_from")
                .col(Alias::new(Self::FROM_ENTITY_ID))
                .build(PostgresQueryBuilder),
            // Index on to_entity_id for reverse traversal
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entity_relations_to")
                .col(Alias::new(Self::TO_ENTITY_ID))
                .build(PostgresQueryBuilder),
            // Index on relation_type for filtering
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entity_relations_type")
                .col(Alias::new(Self::RELATION_TYPE))
                .build(PostgresQueryBuilder),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::prelude::*;

    #[sinex_test]
    async fn test_events_table_creation(_ctx: TestContext) -> Result<()> {
        let sql = Events::create_table();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(sql.contains("core.events"));
        assert!(sql.contains("event_id"));
        assert!(sql.contains("payload"));
        Ok(())
    }

    #[sinex_test]
    async fn test_checkpoints_table_creation(_ctx: TestContext) -> Result<()> {
        let sql = Checkpoints::create_table();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(sql.contains("core.checkpoints"));
        assert!(sql.contains("checkpoint_id"));
        assert!(sql.contains("satellite_id"));
        Ok(())
    }

    #[sinex_test]
    async fn test_index_creation(_ctx: TestContext) -> Result<()> {
        let indexes = Events::create_indexes();
        assert!(!indexes.is_empty());
        assert!(indexes[0].contains("CREATE INDEX"));
        assert!(indexes[0].contains("idx_events_source_type"));
        Ok(())
    }

    #[sinex_test]
    async fn test_all_table_schemas(_ctx: TestContext) -> Result<()> {
        // Test all table creation SQL
        let tables = vec![
            Events::create_table(),
            Checkpoints::create_table(),
            Schemas::create_table(),
            SourceMaterials::create_table(),
            OperationsLog::create_table(),
            ArchivedEvents::create_table(),
            Entities::create_table(),
            EntityRelations::create_table(),
        ];

        for sql in tables {
            assert!(sql.contains("CREATE TABLE IF NOT EXISTS"));
            assert!(!sql.is_empty());
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_foreign_key_constraints(_ctx: TestContext) -> Result<()> {
        let entity_relations_sql = EntityRelations::create_table();

        // Check foreign key constraints are properly defined
        assert!(entity_relations_sql.contains("FOREIGN KEY"));
        assert!(entity_relations_sql.contains("from_entity_id"));
        assert!(entity_relations_sql.contains("to_entity_id"));
        assert!(entity_relations_sql.contains("ON DELETE CASCADE"));

        Ok(())
    }
}
