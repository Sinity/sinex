use crate::schema::TableDef;
use sea_query::{Alias, ColumnDef, Expr, Index, PostgresQueryBuilder, Table};

/// Entities table schema definition
#[derive(Copy, Clone)]
pub struct Entities;

impl TableDef for Entities {
    fn table_name() -> &'static str {
        "entities"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

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
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const MERGED_INTO_ID: &'static str = "merged_into_id";

    /// Create the entities table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(ColumnDef::new(Alias::new(Self::TYPE)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::NAME)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::CANONICAL_NAME)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::ALIASES))
                    .array(sea_query::ColumnType::Text)
                    .not_null()
                    .default(Expr::cust("'{}'::text[]")),
            )
            .col(ColumnDef::new(Alias::new(Self::DESCRIPTION)).text())
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
            .col(ColumnDef::new(Alias::new(Self::MERGED_INTO_ID)).custom(Alias::new("ULID")))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the entities table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Unique index on (type, canonical_name)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entities_type_canonical")
                .col(Alias::new(Self::TYPE))
                .col(Alias::new(Self::CANONICAL_NAME))
                .unique()
                .build(PostgresQueryBuilder),
            // Index on type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entities_type")
                .col(Alias::new(Self::TYPE))
                .build(PostgresQueryBuilder),
            // Index on canonical_name
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entities_canonical_name")
                .col(Alias::new(Self::CANONICAL_NAME))
                .build(PostgresQueryBuilder),
            // GIN index on metadata
            format!(
                "CREATE INDEX idx_entities_metadata ON {}.{} USING GIN ({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::METADATA
            ),
        ]
    }
}

/// Entity relations table schema definition
#[derive(Copy, Clone)]
pub struct EntityRelations;

impl TableDef for EntityRelations {
    fn table_name() -> &'static str {
        "entity_relations"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EntityRelations {
    pub const TABLE: &'static str = "entity_relations";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const FROM_ENTITY_ID: &'static str = "from_entity_id";
    pub const TO_ENTITY_ID: &'static str = "to_entity_id";
    pub const RELATION_TYPE: &'static str = "relation_type";
    pub const STRENGTH: &'static str = "strength";
    pub const PROPERTIES: &'static str = "properties";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";

    /// Create the entity relations table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
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
            .col(ColumnDef::new(Alias::new(Self::RELATION_TYPE)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::STRENGTH))
                    .float()
                    .default(1.0),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PROPERTIES))
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
            // Composite index on (from_entity_id, relation_type)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entity_relations_from_type")
                .col(Alias::new(Self::FROM_ENTITY_ID))
                .col(Alias::new(Self::RELATION_TYPE))
                .build(PostgresQueryBuilder),
            // Index on relation_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entity_relations_type")
                .col(Alias::new(Self::RELATION_TYPE))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_entity_relations_from FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::FROM_ENTITY_ID,
                Entities::SCHEMA, Entities::TABLE, Entities::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_entity_relations_to FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::TO_ENTITY_ID,
                Entities::SCHEMA, Entities::TABLE, Entities::ID
            ),
        ]
    }
}