use crate::schema::TableDef;
use sea_query::{Alias, ColumnDef, Expr, Index, IndexOrder, PostgresQueryBuilder, Table};

/// Source materials table schema definition
#[derive(Copy, Clone)]
pub struct SourceMaterials;

impl TableDef for SourceMaterials {
    fn table_name() -> &'static str {
        "source_material_registry"
    }
    fn schema_name() -> &'static str {
        "raw"
    }
    fn primary_key() -> &'static str {
        "source_material_id"
    }
}

impl SourceMaterials {
    pub const TABLE: &'static str = "source_material_registry";
    pub const SCHEMA: &'static str = "raw";

    pub const SOURCE_MATERIAL_ID: &'static str = "source_material_id";
    pub const SOURCE_URI: &'static str = "source_uri";
    pub const INGESTION_TIME: &'static str = "ingestion_time";
    pub const ENCODING: &'static str = "encoding";
    pub const METADATA: &'static str = "metadata";
    pub const CONTENT_PREVIEW: &'static str = "content_preview";
    pub const IS_ARCHIVED: &'static str = "is_archived";
    pub const ARCHIVE_TIME: &'static str = "archive_time";
    pub const RETENTION_POLICY: &'static str = "retention_policy";
    pub const CREATED_AT: &'static str = "created_at";
    pub const UPDATED_AT: &'static str = "updated_at";
    // Removed temporal_ledger_id - no longer used in the schema
    pub const OPTIONAL_BLOB_ID: &'static str = "optional_blob_id";
    pub const MATERIAL_TYPE: &'static str = "material_type";

    /// Create the source materials table
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
            .if_not_exists()
            .col(
                ColumnDef::new(Alias::new(Self::SOURCE_MATERIAL_ID))
                    .custom(Alias::new("ULID"))
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
            .col(ColumnDef::new(Alias::new(Self::ENCODING)).text())
            .col(
                ColumnDef::new(Alias::new(Self::METADATA))
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
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
            .col(ColumnDef::new(Alias::new(Self::OPTIONAL_BLOB_ID)).custom(Alias::new("ULID")))
            .build(PostgresQueryBuilder)
    }

    /// Create indexes for the source materials table
    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on source_uri
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_source_material_source_uri")
                .col(Alias::new(Self::SOURCE_URI))
                .build(PostgresQueryBuilder),
            // Index on ingestion_time
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_source_material_ingestion_time")
                .col((Alias::new(Self::INGESTION_TIME), IndexOrder::Desc))
                .build(PostgresQueryBuilder),
            // Index on material_type
            Index::create()
                .table((Alias::new(Self::SCHEMA), Alias::new(Self::TABLE)))
                .name("idx_source_material_type")
                .col(Alias::new(Self::MATERIAL_TYPE))
                .build(PostgresQueryBuilder),
            // Index on optional_blob_id
            format!(
                "CREATE INDEX idx_source_material_blob ON {}.{} ({}) WHERE {} IS NOT NULL",
                Self::SCHEMA,
                Self::TABLE,
                Self::OPTIONAL_BLOB_ID,
                Self::OPTIONAL_BLOB_ID
            ),
        ]
    }
}
