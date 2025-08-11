use super::{Events, TableDef};
use sea_query::{Alias, ColumnDef, Expr, Index, PostgresQueryBuilder, Table};

/// Embedding models table schema definition
#[derive(Copy, Clone)]
pub struct EmbeddingModels;

impl TableDef for EmbeddingModels {
    fn table_name() -> &'static str {
        "embedding_models"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EmbeddingModels {
    pub const TABLE: &'static str = "embedding_models";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const NAME: &'static str = "name";
    pub const PROVIDER: &'static str = "provider";
    pub const MODEL_VERSION: &'static str = "model_version";
    pub const DIMENSIONS: &'static str = "dimensions";
    pub const MAX_TOKENS: &'static str = "max_tokens";
    pub const CONFIGURATION: &'static str = "configuration";
    pub const IS_ACTIVE: &'static str = "is_active";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the embedding models table
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
                ColumnDef::new(Alias::new(Self::NAME))
                    .text()
                    .not_null()
                    .unique(),
            )
            .col(ColumnDef::new(Alias::new(Self::PROVIDER)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::MODEL_VERSION))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::DIMENSIONS))
                    .integer()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::MAX_TOKENS)).integer())
            .col(
                ColumnDef::new(Alias::new(Self::CONFIGURATION))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Alias::new(Self::IS_ACTIVE))
                    .boolean()
                    .not_null()
                    .default(true),
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
            // Index on is_active for filtering active models
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_embedding_models_active")
                .col(Alias::new(Self::IS_ACTIVE))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Embedding cache table schema definition
#[derive(Copy, Clone)]
pub struct EmbeddingCache;

impl TableDef for EmbeddingCache {
    fn table_name() -> &'static str {
        "embedding_cache"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EmbeddingCache {
    pub const TABLE: &'static str = "embedding_cache";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const TEXT_HASH: &'static str = "text_hash";
    pub const MODEL_ID: &'static str = "model_id";
    pub const EMBEDDING: &'static str = "embedding";
    pub const CREATED_AT: &'static str = "created_at";
    pub const LAST_ACCESSED: &'static str = "last_accessed";
    pub const ACCESS_COUNT: &'static str = "access_count";

    /// Create the embedding cache table
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
                ColumnDef::new(Alias::new(Self::TEXT_HASH))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::MODEL_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EMBEDDING))
                    .custom(Alias::new("vector(1536)"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::LAST_ACCESSED))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ACCESS_COUNT))
                    .integer()
                    .not_null()
                    .default(0),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the embedding cache table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Unique index on (text_hash, model_id)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_embedding_cache_unique")
                .col(Alias::new(Self::TEXT_HASH))
                .col(Alias::new(Self::MODEL_ID))
                .unique()
                .build(PostgresQueryBuilder),
            // Index on last_accessed for cache eviction
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_embedding_cache_accessed")
                .col(Alias::new(Self::LAST_ACCESSED))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT fk_embedding_cache_model FOREIGN KEY ({}) REFERENCES {}.{}({})",
            Self::SCHEMA,
            Self::TABLE,
            Self::MODEL_ID,
            EmbeddingModels::SCHEMA,
            EmbeddingModels::TABLE,
            EmbeddingModels::ID
        )]
    }
}

/// Event embeddings table definition
#[derive(Copy, Clone)]
pub struct EventEmbeddings;

impl TableDef for EventEmbeddings {
    fn table_name() -> &'static str {
        "event_embeddings"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EventEmbeddings {
    pub const TABLE: &'static str = "event_embeddings";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const EMBEDDING_MODEL_ID: &'static str = "embedding_model_id";
    pub const EMBEDDED_TEXT: &'static str = "embedded_text";
    pub const EMBEDDING: &'static str = "embedding";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the event embeddings table
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
                ColumnDef::new(Alias::new(Self::EMBEDDING_MODEL_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EMBEDDED_TEXT))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EMBEDDING))
                    .custom(Alias::new("vector(1536)"))
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
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event embeddings table
    pub fn create_indexes() -> Vec<String> {
        vec![
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_embeddings_event")
                .col(Alias::new(Self::EVENT_ID))
                .build(PostgresQueryBuilder),
            format!(
                "CREATE INDEX idx_event_embeddings_vector ON {}.{} USING ivfflat ({} vector_cosine_ops) WITH (lists = 100)",
                Self::SCHEMA, Self::TABLE, Self::EMBEDDING
            ),
        ]
    }

    /// Create constraints for the event embeddings table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_event_embedding UNIQUE({}, {})",
                Self::SCHEMA, Self::TABLE, Self::EVENT_ID, Self::EMBEDDING_MODEL_ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_embeddings_event FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_embeddings_model FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::EMBEDDING_MODEL_ID,
                EmbeddingModels::SCHEMA, EmbeddingModels::TABLE, EmbeddingModels::ID
            ),
        ]
    }
}
