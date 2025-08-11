use crate::schema::TableDef;
use sea_query::{Alias, ColumnDef, Expr, Index, PostgresQueryBuilder, Table};

/// Event payload schemas table schema definition
#[derive(Copy, Clone)]
pub struct EventPayloadSchemas;

impl TableDef for EventPayloadSchemas {
    fn table_name() -> &'static str {
        "event_payload_schemas"
    }
    fn schema_name() -> &'static str {
        "sinex_schemas"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl EventPayloadSchemas {
    pub const TABLE: &'static str = "event_payload_schemas";
    pub const SCHEMA: &'static str = "sinex_schemas";

    pub const ID: &'static str = "id";
    pub const SCHEMA_NAME: &'static str = "schema_name";
    pub const SCHEMA_VERSION: &'static str = "schema_version";
    pub const JSON_SCHEMA: &'static str = "json_schema";
    pub const DESCRIPTION: &'static str = "description";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    pub const DEPRECATED_AT: &'static str = "deprecated_at";
    pub const SUCCESSOR_ID: &'static str = "successor_id";
    pub const CONTENT_HASH: &'static str = "content_hash";
    pub const IS_ACTIVE: &'static str = "is_active";

    /// Create the event payload schemas table
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
                ColumnDef::new(Alias::new(Self::JSON_SCHEMA))
                    .json_binary()
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::DESCRIPTION)).text())
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
            .col(ColumnDef::new(Alias::new(Self::SUCCESSOR_ID)).custom(Alias::new("ULID")))
            .col(ColumnDef::new(Alias::new(Self::CONTENT_HASH)).text())
            .col(
                ColumnDef::new(Alias::new(Self::IS_ACTIVE))
                    .boolean()
                    .not_null()
                    .default(true),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the event payload schemas table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Unique index on (schema_name, schema_version)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_payload_schemas_unique")
                .col(Alias::new(Self::SCHEMA_NAME))
                .col(Alias::new(Self::SCHEMA_VERSION))
                .unique()
                .build(PostgresQueryBuilder),
            // Index on content_hash for deduplication
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_event_payload_schemas_hash")
                .col(Alias::new(Self::CONTENT_HASH))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Schema compatibility table schema definition
#[derive(Copy, Clone)]
pub struct SchemaCompatibility;

impl TableDef for SchemaCompatibility {
    fn table_name() -> &'static str {
        "schema_compatibility"
    }
    fn schema_name() -> &'static str {
        "sinex_schemas"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl SchemaCompatibility {
    pub const TABLE: &'static str = "schema_compatibility";
    pub const SCHEMA: &'static str = "sinex_schemas";

    pub const ID: &'static str = "id";
    pub const FROM_SCHEMA_ID: &'static str = "from_schema_id";
    pub const TO_SCHEMA_ID: &'static str = "to_schema_id";
    pub const COMPATIBILITY_TYPE: &'static str = "compatibility_type";
    pub const TRANSFORMATION_RULES: &'static str = "transformation_rules";
    pub const IS_AUTOMATIC: &'static str = "is_automatic";
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
                    .default(Expr::cust("gen_ulid()")),
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
                    .not_null(),
            )
            .col(ColumnDef::new(Alias::new(Self::TRANSFORMATION_RULES)).json_binary())
            .col(
                ColumnDef::new(Alias::new(Self::IS_AUTOMATIC))
                    .boolean()
                    .not_null()
                    .default(false),
            )
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
            // Unique index on (from_schema_id, to_schema_id)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_schema_compatibility_unique")
                .col(Alias::new(Self::FROM_SCHEMA_ID))
                .col(Alias::new(Self::TO_SCHEMA_ID))
                .unique()
                .build(PostgresQueryBuilder),
            // Index on from_schema_id
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_schema_compatibility_from")
                .col(Alias::new(Self::FROM_SCHEMA_ID))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_schema_compat_from FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::FROM_SCHEMA_ID,
                EventPayloadSchemas::SCHEMA, EventPayloadSchemas::TABLE, EventPayloadSchemas::ID
            ),
            format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT fk_schema_compat_to FOREIGN KEY ({}) REFERENCES {}.{}({})",
                Self::SCHEMA, Self::TABLE, Self::TO_SCHEMA_ID,
                EventPayloadSchemas::SCHEMA, EventPayloadSchemas::TABLE, EventPayloadSchemas::ID
            ),
        ]
    }
}

/// GitOps schema source table schema definition
#[derive(Copy, Clone)]
pub struct GitopsSchemaSource;

impl TableDef for GitopsSchemaSource {
    fn table_name() -> &'static str {
        "gitops_schema_source"
    }
    fn schema_name() -> &'static str {
        "sinex_schemas"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl GitopsSchemaSource {
    pub const TABLE: &'static str = "gitops_schema_source";
    pub const SCHEMA: &'static str = "sinex_schemas";

    pub const ID: &'static str = "id";
    pub const REPOSITORY_URL: &'static str = "repository_url";
    pub const BRANCH: &'static str = "branch";
    pub const PATH: &'static str = "path";
    pub const LAST_COMMIT_SHA: &'static str = "last_commit_sha";
    pub const LAST_SYNC_AT: &'static str = "last_sync_at";
    pub const SYNC_STATUS: &'static str = "sync_status";
    pub const ERROR_MESSAGE: &'static str = "error_message";
    pub const IS_ACTIVE: &'static str = "is_active";
    pub const CREATED_AT: &'static str = "created_at";

    /// Create the GitOps schema source table
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
                ColumnDef::new(Alias::new(Self::REPOSITORY_URL))
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(Alias::new(Self::BRANCH))
                    .text()
                    .not_null()
                    .default("main"),
            )
            .col(ColumnDef::new(Alias::new(Self::PATH)).text().not_null())
            .col(ColumnDef::new(Alias::new(Self::LAST_COMMIT_SHA)).text())
            .col(ColumnDef::new(Alias::new(Self::LAST_SYNC_AT)).timestamp_with_time_zone())
            .col(
                ColumnDef::new(Alias::new(Self::SYNC_STATUS))
                    .text()
                    .default("pending"),
            )
            .col(ColumnDef::new(Alias::new(Self::ERROR_MESSAGE)).text())
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

    /// Create indexes for the GitOps schema source table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Unique index on (repository_url, branch, path)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_gitops_schema_source_unique")
                .col(Alias::new(Self::REPOSITORY_URL))
                .col(Alias::new(Self::BRANCH))
                .col(Alias::new(Self::PATH))
                .unique()
                .build(PostgresQueryBuilder),
            // Index on is_active
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_gitops_schema_source_active")
                .col(Alias::new(Self::IS_ACTIVE))
                .build(PostgresQueryBuilder),
        ]
    }
}

/// Validation cache table schema definition
#[derive(Copy, Clone)]
pub struct ValidationCache;

impl TableDef for ValidationCache {
    fn table_name() -> &'static str {
        "validation_cache"
    }
    fn schema_name() -> &'static str {
        "sinex_schemas"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

impl ValidationCache {
    pub const TABLE: &'static str = "validation_cache";
    pub const SCHEMA: &'static str = "sinex_schemas";

    pub const ID: &'static str = "id";
    pub const PAYLOAD_HASH: &'static str = "payload_hash";
    pub const SCHEMA_ID: &'static str = "schema_id";
    pub const IS_VALID: &'static str = "is_valid";
    pub const VALIDATION_ERRORS: &'static str = "validation_errors";
    pub const VALIDATED_AT: &'static str = "validated_at";
    pub const TTL_SECONDS: &'static str = "ttl_seconds";

    /// Create the validation cache table
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
                ColumnDef::new(Alias::new(Self::PAYLOAD_HASH))
                    .text()
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
            .col(
                ColumnDef::new(Alias::new(Self::TTL_SECONDS))
                    .integer()
                    .default(3600),
            )
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the validation cache table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Unique index on (payload_hash, schema_id)
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_validation_cache_unique")
                .col(Alias::new(Self::PAYLOAD_HASH))
                .col(Alias::new(Self::SCHEMA_ID))
                .unique()
                .build(PostgresQueryBuilder),
            // Index on validated_at for expiry queries
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_validation_cache_validated")
                .col(Alias::new(Self::VALIDATED_AT))
                .build(PostgresQueryBuilder),
        ]
    }

    /// Create foreign key constraints
    pub fn create_constraints() -> Vec<String> {
        vec![format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT fk_validation_cache_schema FOREIGN KEY ({}) REFERENCES {}.{}({})",
            Self::SCHEMA, Self::TABLE, Self::SCHEMA_ID,
            EventPayloadSchemas::SCHEMA, EventPayloadSchemas::TABLE, EventPayloadSchemas::ID
        )]
    }
}
