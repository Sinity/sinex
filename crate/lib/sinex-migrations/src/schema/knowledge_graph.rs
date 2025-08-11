use crate::schema::TableDef;
use sea_query::{Alias, ColumnDef, Expr, Index, IntoIden, PostgresQueryBuilder, Table};

/// Knowledge graph entities table schema definition
#[derive(Copy, Clone)]
pub struct KgEntities;

impl TableDef for KgEntities {
    fn table_name() -> &'static str {
        "entities"
    }
    fn schema_name() -> &'static str {
        "kg"
    }
    fn primary_key() -> &'static str {
        "id"
    }
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
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ENTITY_TYPE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CANONICAL_NAME))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ALIASES))
                    .array(sea_query::ColumnType::Text)
                    .default(Expr::cust("'{}'::text[]")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PROPERTIES))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SOURCE_EVENT_IDS))
                    .array(sea_query::ColumnType::Custom(
                        Alias::new("ULID").into_iden(),
                    ))
                    .not_null()
                    .default(Expr::cust("'{}'::ulid[]")),
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
                ColumnDef::new(Alias::new(Self::CONFIDENCE_SCORE))
                    .double()
                    .default(1.0),
            )
            .col(
                ColumnDef::new(Alias::new(Self::IS_MERGED))
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(ColumnDef::new(Alias::new(Self::MERGED_INTO_ID)).custom(Alias::new("ULID")))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the entities table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on entity_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entities_type")
                .col(Alias::new(Self::ENTITY_TYPE))
                .build(PostgresQueryBuilder),
            // Index on canonical_name
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_entities_canonical_name")
                .col(Alias::new(Self::CANONICAL_NAME))
                .build(PostgresQueryBuilder),
            // GIN index on aliases
            format!(
                "CREATE INDEX idx_entities_aliases ON {}.{} USING GIN ({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::ALIASES
            ),
            // GIN index on properties
            format!(
                "CREATE INDEX idx_entities_properties ON {}.{} USING GIN ({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::PROPERTIES
            ),
            // GIN index on source_event_ids
            format!(
                "CREATE INDEX idx_entities_source_events ON {}.{} USING GIN ({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::SOURCE_EVENT_IDS
            ),
            // Partial index on merged entities
            format!(
                "CREATE INDEX idx_entities_merged ON {}.{} ({}) WHERE {} = true",
                Self::SCHEMA,
                Self::TABLE,
                Self::MERGED_INTO_ID,
                Self::IS_MERGED
            ),
        ]
    }
}

/// Knowledge graph entity relations table schema definition
#[derive(Copy, Clone)]
pub struct KgEntityRelations;

impl TableDef for KgEntityRelations {
    fn table_name() -> &'static str {
        "entity_relations"
    }
    fn schema_name() -> &'static str {
        "kg"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl KgEntityRelations {
    pub const TABLE: &'static str = "entity_relations";
    pub const SCHEMA: &'static str = "kg";

    pub const ID: &'static str = "id";
    pub const FROM_ENTITY_ID: &'static str = "from_entity_id";
    pub const TO_ENTITY_ID: &'static str = "to_entity_id";
    pub const RELATION_TYPE: &'static str = "relation_type";
    pub const PROPERTIES: &'static str = "properties";
    pub const SOURCE_EVENT_IDS: &'static str = "source_event_ids";
    pub const CONFIDENCE_SCORE: &'static str = "confidence_score";
    pub const VALID_FROM: &'static str = "valid_from";
    pub const VALID_TO: &'static str = "valid_to";
    pub const CREATED_AT: &'static str = "created_at";

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
            .col(
                ColumnDef::new(Alias::new(Self::RELATION_TYPE))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::PROPERTIES))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::SOURCE_EVENT_IDS))
                    .array(sea_query::ColumnType::Custom(
                        Alias::new("ULID").into_iden(),
                    ))
                    .not_null()
                    .default(Expr::cust("'{}'::ulid[]")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CONFIDENCE_SCORE))
                    .double()
                    .default(1.0),
            )
            .col(ColumnDef::new(Alias::new(Self::VALID_FROM)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::VALID_TO)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
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
            // GIN index on source_event_ids
            format!(
                "CREATE INDEX idx_entity_relations_source_events ON {}.{} USING GIN ({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::SOURCE_EVENT_IDS
            ),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_entity_relations_from FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::FROM_ENTITY_ID,
                KgEntities::SCHEMA, KgEntities::TABLE, KgEntities::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_entity_relations_to FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::TO_ENTITY_ID,
                KgEntities::SCHEMA, KgEntities::TABLE, KgEntities::ID
            ),
        ]
    }
}
