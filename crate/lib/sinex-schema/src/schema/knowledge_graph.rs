//! Schema definitions for knowledge graph tables

use sea_orm_migration::prelude::*;

/// Knowledge graph entities table
#[derive(Iden, Copy, Clone)]
pub enum KgEntities {
    Table,
    Id,
    EntityType,
    CanonicalName,
    Aliases,
    Properties,
    SourceEventIds,
    CreatedAt,
    UpdatedAt,
    ConfidenceScore,
    IsMerged,
    MergedIntoId,
}

impl KgEntities {
    pub const TABLE: &'static str = "entities";
    pub const SCHEMA: &'static str = "kg";

    pub const ID: &'static str = "id";
    pub const ENTITY_TYPE: &'static str = "entity_type";
    pub const CANONICAL_NAME: &'static str = "canonical_name";
    pub const ALIASES: &'static str = "aliases";
    pub const PROPERTIES: &'static str = "properties";
    pub const SOURCE_EVENT_IDS: &'static str = "source_event_ids";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const CONFIDENCE_SCORE: &'static str = "confidence_score";
    pub const IS_MERGED: &'static str = "is_merged";
    pub const MERGED_INTO_ID: &'static str = "merged_into_id";

    /// Create the entities table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), KgEntities::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(KgEntities::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(ColumnDef::new(KgEntities::EntityType).text().not_null())
            .col(ColumnDef::new(KgEntities::CanonicalName).text().not_null())
            .col(
                ColumnDef::new(KgEntities::Aliases)
                    .array(sea_query::ColumnType::Text)
                    .default(Expr::cust("'{}'::text[]")),
            )
            .col(
                ColumnDef::new(KgEntities::Properties)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(KgEntities::SourceEventIds)
                    .array(sea_query::ColumnType::Custom(
                        Alias::new("ULID").into_iden(),
                    ))
                    .not_null()
                    .default(Expr::cust("'{}'::ulid[]")),
            )
            .col(
                ColumnDef::new(KgEntities::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(KgEntities::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(KgEntities::ConfidenceScore)
                    .double()
                    .not_null()
                    .default(1.0),
            )
            .col(
                ColumnDef::new(KgEntities::IsMerged)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(ColumnDef::new(KgEntities::MergedIntoId).custom(Alias::new("ULID")))
            .to_string(PostgresQueryBuilder)
    }

    /// Create indexes for the entities table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on entity_type
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_entities_type ON {}.{} (entity_type)",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on canonical_name
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_entities_canonical_name ON {}.{} (canonical_name)",
                Self::SCHEMA, Self::TABLE
            ),
            // GIN index on aliases
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_entities_aliases ON {}.{} USING GIN (aliases)",
                Self::SCHEMA, Self::TABLE
            ),
            // GIN index on properties
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_entities_properties ON {}.{} USING GIN (properties)",
                Self::SCHEMA, Self::TABLE
            ),
            // GIN index on source_event_ids
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_entities_source_events ON {}.{} USING GIN (source_event_ids)",
                Self::SCHEMA, Self::TABLE
            ),
            // Partial index on merged entities
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_entities_merged ON {}.{} (merged_into_id) WHERE is_merged = true",
                Self::SCHEMA, Self::TABLE
            ),
        ]
    }
}

/// Knowledge graph relations table
#[derive(Iden, Copy, Clone)]
pub enum KgRelations {
    Table,
    Id,
    FromEntityId,
    ToEntityId,
    RelationType,
    Properties,
    SourceEventIds,
    CreatedAt,
    UpdatedAt,
    ConfidenceScore,
    IsActive,
}

impl KgRelations {
    pub const TABLE: &'static str = "relations";
    pub const SCHEMA: &'static str = "kg";

    pub const ID: &'static str = "id";
    pub const FROM_ENTITY_ID: &'static str = "from_entity_id";
    pub const TO_ENTITY_ID: &'static str = "to_entity_id";
    pub const RELATION_TYPE: &'static str = "relation_type";
    pub const PROPERTIES: &'static str = "properties";
    pub const SOURCE_EVENT_IDS: &'static str = "source_event_ids";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const CONFIDENCE_SCORE: &'static str = "confidence_score";
    pub const IS_ACTIVE: &'static str = "is_active";

    /// Create the relations table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), KgRelations::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(KgRelations::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(
                ColumnDef::new(KgRelations::FromEntityId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(KgRelations::ToEntityId)
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(ColumnDef::new(KgRelations::RelationType).text().not_null())
            .col(
                ColumnDef::new(KgRelations::Properties)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(KgRelations::SourceEventIds)
                    .array(sea_query::ColumnType::Custom(
                        Alias::new("ULID").into_iden(),
                    ))
                    .not_null()
                    .default(Expr::cust("'{}'::ulid[]")),
            )
            .col(
                ColumnDef::new(KgRelations::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(KgRelations::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(KgRelations::ConfidenceScore)
                    .double()
                    .not_null()
                    .default(1.0),
            )
            .col(
                ColumnDef::new(KgRelations::IsActive)
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .to_string(PostgresQueryBuilder)
    }

    /// Create indexes for the relations table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on from_entity_id
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_relations_from ON {}.{} (from_entity_id)",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on to_entity_id
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_relations_to ON {}.{} (to_entity_id)",
                Self::SCHEMA, Self::TABLE
            ),
            // Composite index on (from_entity_id, relation_type)
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_relations_from_type ON {}.{} (from_entity_id, relation_type)",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on relation_type
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_relations_type ON {}.{} (relation_type)",
                Self::SCHEMA, Self::TABLE
            ),
            // GIN index on properties
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_relations_properties ON {}.{} USING GIN (properties)",
                Self::SCHEMA, Self::TABLE
            ),
            // GIN index on source_event_ids
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_relations_source_events ON {}.{} USING GIN (source_event_ids)",
                Self::SCHEMA, Self::TABLE
            ),
            // Partial index on active relations
            format!(
                "CREATE INDEX IF NOT EXISTS idx_kg_relations_active ON {}.{} (from_entity_id, to_entity_id) WHERE is_active = true",
                Self::SCHEMA, Self::TABLE
            ),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_kg_relations_from_entity FOREIGN KEY (from_entity_id) REFERENCES {}.{} (id)",
                Self::SCHEMA, Self::TABLE, KgEntities::SCHEMA, KgEntities::TABLE
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_kg_relations_to_entity FOREIGN KEY (to_entity_id) REFERENCES {}.{} (id)",
                Self::SCHEMA, Self::TABLE, KgEntities::SCHEMA, KgEntities::TABLE
            ),
        ]
    }
}
