use super::{Blobs, EmbeddingModels, Events, Tags};
use crate::schema::TableDef;
use sea_query::{Alias, ColumnDef, Expr, Index, IndexCreateStatement, PostgresQueryBuilder, Table};

/// Artifacts table definition
#[derive(Copy, Clone)]
pub struct Artifacts;

impl TableDef for Artifacts {
    fn table_name() -> &'static str {
        "artifacts"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl Artifacts {
    pub const TABLE: &'static str = "artifacts";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const TYPE: &'static str = "type";
    pub const TITLE: &'static str = "title";
    pub const SOURCE_URL: &'static str = "source_url";
    pub const ORIGINAL_PATH: &'static str = "original_path";
    pub const MIME_TYPE: &'static str = "mime_type";
    pub const SIZE_BYTES: &'static str = "size_bytes";
    pub const CHECKSUM: &'static str = "checksum";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const DELETED_AT: &'static str = "deleted_at";
    pub const CREATED_FROM_EVENT_ID: &'static str = "created_from_event_id";
    pub const BLOB_ID: &'static str = "blob_id";

    /// Create the artifacts table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ID))
                    .custom(Alias::new("ULID"))
                    .primary_key()
                    .extra("DEFAULT gen_ulid()")
            )
            .col(
                ColumnDef::new(Alias::new(Self::TYPE))
                    .text()
                    .not_null()
                    .check(Expr::cust("type IN ('note', 'webpage', 'email', 'file', 'document', 'code', 'media', 'pkm_note', 'task_item')"))
            )
            .col(
                ColumnDef::new(Alias::new(Self::TITLE))
                    .text()
                    .not_null()
            )
            .col(ColumnDef::new(Alias::new(Self::SOURCE_URL)).text())
            .col(ColumnDef::new(Alias::new(Self::ORIGINAL_PATH)).text())
            .col(ColumnDef::new(Alias::new(Self::MIME_TYPE)).text())
            .col(ColumnDef::new(Alias::new(Self::SIZE_BYTES)).big_integer())
            .col(ColumnDef::new(Alias::new(Self::CHECKSUM)).text())
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb"))
            )
            .col(
                ColumnDef::new(Alias::new(Self::CREATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp())
            )
            .col(
                ColumnDef::new(Alias::new(Self::UPDATED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp())
            )
            .col(ColumnDef::new(Alias::new(Self::DELETED_AT)).timestamp_with_time_zone())
            .col(ColumnDef::new(Alias::new(Self::CREATED_FROM_EVENT_ID)).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Alias::new(Self::BLOB_ID)).custom(Alias::new("ULID")))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the artifacts table
    pub fn create_indexes() -> Vec<String> {
        vec![
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_artifacts_type")
                .col(Alias::new(Self::TYPE))
                .build(PostgresQueryBuilder),
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_artifacts_created_at")
                .col(Alias::new(Self::CREATED_AT))
                .build(PostgresQueryBuilder),
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_artifacts_updated_at")
                .col(Alias::new(Self::UPDATED_AT))
                .build(PostgresQueryBuilder),
            format!(
                "CREATE INDEX idx_core_artifacts_metadata ON {}.{} USING gin({})",
                Self::SCHEMA,
                Self::TABLE,
                Self::METADATA
            ),
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_artifacts_deleted_at")
                .col(Alias::new(Self::DELETED_AT))
                .build(PostgresQueryBuilder)
                + " WHERE deleted_at IS NULL",
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_core_artifacts_blob_id")
                .col(Alias::new(Self::BLOB_ID))
                .build(PostgresQueryBuilder)
                + " WHERE blob_id IS NOT NULL",
        ]
    }

    /// Create constraints for the artifacts table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifacts_event FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::CREATED_FROM_EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::EVENT_ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifacts_blob FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::BLOB_ID,
                Blobs::SCHEMA, Blobs::TABLE, Blobs::ID
            ),
        ]
    }
}

/// Artifact contents table definition
#[derive(Copy, Clone)]
pub struct ArtifactContents;

impl TableDef for ArtifactContents {
    fn table_name() -> &'static str {
        "artifact_contents"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl ArtifactContents {
    pub const TABLE: &'static str = "artifact_contents";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const ARTIFACT_ID: &'static str = "artifact_id";
    pub const VERSION: &'static str = "version";
    pub const CONTENT: &'static str = "content";
    pub const CONTENT_TYPE: &'static str = "content_type";
    pub const EXTRACTED_TEXT: &'static str = "extracted_text";
    pub const WORD_COUNT: &'static str = "word_count";
    pub const CHAR_COUNT: &'static str = "char_count";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";
    pub const CREATED_FROM_EVENT_ID: &'static str = "created_from_event_id";

    /// Create the artifact contents table
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
                ColumnDef::new(Alias::new(Self::ARTIFACT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::VERSION))
                    .integer()
                    .not_null()
                    .default(1),
            )
            .col(ColumnDef::new(Alias::new(Self::CONTENT)).text().not_null())
            .col(
                ColumnDef::new(Alias::new(Self::CONTENT_TYPE))
                    .text()
                    .not_null()
                    .default("'text/plain'"),
            )
            .col(ColumnDef::new(Alias::new(Self::EXTRACTED_TEXT)).text())
            .col(ColumnDef::new(Alias::new(Self::WORD_COUNT)).integer())
            .col(ColumnDef::new(Alias::new(Self::CHAR_COUNT)).integer())
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
            .col(ColumnDef::new(Alias::new(Self::CREATED_FROM_EVENT_ID)).custom(Alias::new("ULID")))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the artifact contents table
    pub fn create_indexes() -> Vec<String> {
        vec![
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_artifact_contents_artifact_id")
                .col(Alias::new(Self::ARTIFACT_ID))
                .build(PostgresQueryBuilder),
            format!(
                "CREATE INDEX idx_artifact_contents_content_search ON {}.{} USING gin(to_tsvector('english', {}))",
                Self::SCHEMA, Self::TABLE, Self::CONTENT
            ),
            format!(
                "CREATE INDEX idx_artifact_contents_extracted_search ON {}.{} USING gin(to_tsvector('english', {})) WHERE {} IS NOT NULL",
                Self::SCHEMA, Self::TABLE, Self::EXTRACTED_TEXT, Self::EXTRACTED_TEXT
            ),
        ]
    }

    /// Create constraints for the artifact contents table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_artifact_version UNIQUE ({}, {})",
                Self::SCHEMA, Self::TABLE, Self::ARTIFACT_ID, Self::VERSION
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_contents_artifact FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::ARTIFACT_ID,
                Artifacts::SCHEMA, Artifacts::TABLE, Artifacts::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_contents_event FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::CREATED_FROM_EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::EVENT_ID
            ),
        ]
    }
}

/// Artifact tags table definition
#[derive(Copy, Clone)]
pub struct ArtifactTags;

impl TableDef for ArtifactTags {
    fn table_name() -> &'static str {
        "artifact_tags"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "(artifact_id, tag_id)"
    }
}

impl ArtifactTags {
    pub const TABLE: &'static str = "artifact_tags";
    pub const SCHEMA: &'static str = "core";

    pub const ARTIFACT_ID: &'static str = "artifact_id";
    pub const TAG_ID: &'static str = "tag_id";
    pub const TAGGED_AT: &'static str = "tagged_at";
    pub const TAGGED_FROM_EVENT_ID: &'static str = "tagged_from_event_id";

    /// Create the artifact tags table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ARTIFACT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TAG_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TAGGED_AT))
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Alias::new(Self::TAGGED_FROM_EVENT_ID)).custom(Alias::new("ULID")))
            .primary_key(
                IndexCreateStatement::new()
                    .col(Alias::new(Self::ARTIFACT_ID))
                    .col(Alias::new(Self::TAG_ID)),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the artifact tags table
    pub fn create_indexes() -> Vec<String> {
        vec![Index::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .name("idx_artifact_tags_tag")
            .col(Alias::new(Self::TAG_ID))
            .build(PostgresQueryBuilder)]
    }

    /// Create constraints for the artifact tags table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_tags_artifact FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::ARTIFACT_ID,
                Artifacts::SCHEMA, Artifacts::TABLE, Artifacts::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_tags_tag FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::TAG_ID,
                Tags::SCHEMA, Tags::TABLE, Tags::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_tags_event FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::TAGGED_FROM_EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::EVENT_ID
            ),
        ]
    }
}

/// Artifact relations table definition
#[derive(Copy, Clone)]
pub struct ArtifactRelations;

impl TableDef for ArtifactRelations {
    fn table_name() -> &'static str {
        "artifact_relations"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl ArtifactRelations {
    pub const TABLE: &'static str = "artifact_relations";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const FROM_ARTIFACT_ID: &'static str = "from_artifact_id";
    pub const TO_ARTIFACT_ID: &'static str = "to_artifact_id";
    pub const RELATION_TYPE: &'static str = "relation_type";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the artifact relations table
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
                ColumnDef::new(Alias::new(Self::FROM_ARTIFACT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::TO_ARTIFACT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::RELATION_TYPE))
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
            .check(Expr::cust("from_artifact_id != to_artifact_id"))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the artifact relations table
    pub fn create_indexes() -> Vec<String> {
        vec![
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_artifact_relations_from")
                .col(Alias::new(Self::FROM_ARTIFACT_ID))
                .build(PostgresQueryBuilder),
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_artifact_relations_to")
                .col(Alias::new(Self::TO_ARTIFACT_ID))
                .build(PostgresQueryBuilder),
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_artifact_relations_type")
                .col(Alias::new(Self::RELATION_TYPE))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create constraints for the artifact relations table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT no_self_artifact_relations CHECK ({} != {})",
                Self::SCHEMA, Self::TABLE, Self::FROM_ARTIFACT_ID, Self::TO_ARTIFACT_ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_artifact_relation UNIQUE({}, {}, {})",
                Self::SCHEMA, Self::TABLE, Self::FROM_ARTIFACT_ID, Self::TO_ARTIFACT_ID, Self::RELATION_TYPE
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_relations_from FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::FROM_ARTIFACT_ID,
                Artifacts::SCHEMA, Artifacts::TABLE, Artifacts::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_relations_to FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::TO_ARTIFACT_ID,
                Artifacts::SCHEMA, Artifacts::TABLE, Artifacts::ID
            ),
        ]
    }
}

/// Artifact event sources table definition
#[derive(Copy, Clone)]
pub struct ArtifactEventSources;

impl TableDef for ArtifactEventSources {
    fn table_name() -> &'static str {
        "artifact_event_sources"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "(artifact_id, event_id)"
    }
}

impl ArtifactEventSources {
    pub const TABLE: &'static str = "artifact_event_sources";
    pub const SCHEMA: &'static str = "core";

    pub const ARTIFACT_ID: &'static str = "artifact_id";
    pub const EVENT_ID: &'static str = "event_id";
    pub const DERIVATION_TYPE: &'static str = "derivation_type";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the artifact event sources table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::ARTIFACT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::DERIVATION_TYPE))
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
            .primary_key(
                IndexCreateStatement::new()
                    .col(Alias::new(Self::ARTIFACT_ID))
                    .col(Alias::new(Self::EVENT_ID)),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the artifact event sources table
    pub fn create_indexes() -> Vec<String> {
        vec![Index::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .name("idx_artifact_event_sources_event")
            .col(Alias::new(Self::EVENT_ID))
            .build(PostgresQueryBuilder)]
    }

    /// Create constraints for the artifact event sources table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_event_sources_artifact FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::ARTIFACT_ID,
                Artifacts::SCHEMA, Artifacts::TABLE, Artifacts::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_event_sources_event FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::EVENT_ID
            ),
        ]
    }
}

/// Event artifact refs table definition
#[derive(Copy, Clone)]
pub struct EventArtifactRefs;

impl TableDef for EventArtifactRefs {
    fn table_name() -> &'static str {
        "event_artifact_refs"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "(event_id, artifact_id)"
    }
}

impl EventArtifactRefs {
    pub const TABLE: &'static str = "event_artifact_refs";
    pub const SCHEMA: &'static str = "core";

    pub const EVENT_ID: &'static str = "event_id";
    pub const ARTIFACT_ID: &'static str = "artifact_id";
    pub const REFERENCE_TYPE: &'static str = "reference_type";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the event artifact refs table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::EVENT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::ARTIFACT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::REFERENCE_TYPE))
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
            .primary_key(
                IndexCreateStatement::new()
                    .col(Alias::new(Self::EVENT_ID))
                    .col(Alias::new(Self::ARTIFACT_ID)),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event artifact refs table
    pub fn create_indexes() -> Vec<String> {
        vec![Index::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .name("idx_event_artifact_refs_artifact")
            .col(Alias::new(Self::ARTIFACT_ID))
            .build(PostgresQueryBuilder)]
    }

    /// Create constraints for the event artifact refs table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_artifact_refs_event FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::EVENT_ID,
                Events::SCHEMA, Events::TABLE, Events::EVENT_ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_artifact_refs_artifact FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::ARTIFACT_ID,
                Artifacts::SCHEMA, Artifacts::TABLE, Artifacts::ID
            ),
        ]
    }
}

/// Artifact embeddings table definition
#[derive(Copy, Clone)]
pub struct ArtifactEmbeddings;

impl TableDef for ArtifactEmbeddings {
    fn table_name() -> &'static str {
        "artifact_embeddings"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl ArtifactEmbeddings {
    pub const TABLE: &'static str = "artifact_embeddings";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const ARTIFACT_ID: &'static str = "artifact_id";
    pub const ARTIFACT_CONTENT_ID: &'static str = "artifact_content_id";
    pub const EMBEDDING_MODEL_ID: &'static str = "embedding_model_id";
    pub const CHUNK_INDEX: &'static str = "chunk_index";
    pub const CHUNK_TEXT: &'static str = "chunk_text";
    pub const EMBEDDING: &'static str = "embedding";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the artifact embeddings table
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
                ColumnDef::new(Alias::new(Self::ARTIFACT_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::ARTIFACT_CONTENT_ID)).custom(Alias::new("ULID")))
            .col(
                ColumnDef::new(Alias::new(Self::EMBEDDING_MODEL_ID))
                    .custom(Alias::new("ULID"))
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CHUNK_INDEX))
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(Alias::new(Self::CHUNK_TEXT))
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

    /// Create indexes for the artifact embeddings table
    pub fn create_indexes() -> Vec<String> {
        vec![
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_artifact_embeddings_artifact")
                .col(Alias::new(Self::ARTIFACT_ID))
                .build(PostgresQueryBuilder),
            format!(
                "CREATE INDEX idx_artifact_embeddings_vector ON {}.{} USING ivfflat ({} vector_cosine_ops) WITH (lists = 100)",
                Self::SCHEMA, Self::TABLE, Self::EMBEDDING
            ),
        ]
    }

    /// Create constraints for the artifact embeddings table
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT unique_artifact_chunk_embedding UNIQUE({}, {}, {})",
                Self::SCHEMA, Self::TABLE, Self::ARTIFACT_ID, Self::EMBEDDING_MODEL_ID, Self::CHUNK_INDEX
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_embeddings_artifact FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::ARTIFACT_ID,
                Artifacts::SCHEMA, Artifacts::TABLE, Artifacts::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_embeddings_content FOREIGN KEY ({}) REFERENCES {}.{}({}) ON DELETE CASCADE",
                Self::SCHEMA, Self::TABLE, Self::ARTIFACT_CONTENT_ID,
                ArtifactContents::SCHEMA, ArtifactContents::TABLE, ArtifactContents::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_artifact_embeddings_model FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::EMBEDDING_MODEL_ID,
                EmbeddingModels::SCHEMA, EmbeddingModels::TABLE, EmbeddingModels::ID
            ),
        ]
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
                Events::SCHEMA, Events::TABLE, Events::EVENT_ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_event_embeddings_model FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::EMBEDDING_MODEL_ID,
                EmbeddingModels::SCHEMA, EmbeddingModels::TABLE, EmbeddingModels::ID
            ),
        ]
    }
}
