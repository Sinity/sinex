use super::{EmbeddingModels, Events};
use crate::schema::TableDef;
use sea_query::{Alias, ColumnDef, Expr, Index, PostgresQueryBuilder, Table};

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
