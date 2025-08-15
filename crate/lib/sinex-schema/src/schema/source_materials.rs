//! Schema definitions for source materials tables

use sea_orm_migration::prelude::*;

#[derive(Iden, Copy, Clone)]
pub enum SourceMaterials {
    #[iden = "source_material_registry"]
    Table,
    #[iden = "id"]
    Id,
    #[iden = "source_identifier"]
    SourceIdentifier,
    #[iden = "source_uri"]
    SourceUri,
    #[iden = "ingestion_time"]
    IngestionTime,
    #[iden = "encoding"]
    Encoding,
    #[iden = "metadata"]
    Metadata,
    #[iden = "content_preview"]
    ContentPreview,
    #[iden = "is_archived"]
    IsArchived,
    #[iden = "archive_time"]
    ArchiveTime,
    #[iden = "retention_policy"]
    RetentionPolicy,
    #[iden = "created_at"]
    CreatedAt,
    #[iden = "updated_at"]
    UpdatedAt,
    #[iden = "optional_blob_id"]
    OptionalBlobId,
    #[iden = "material_type"]
    MaterialType,
}

impl SourceMaterials {
    pub const TABLE: &'static str = "source_material_registry";
    pub const SCHEMA: &'static str = "raw";

    pub const ID: &'static str = "id";
    pub const SOURCE_IDENTIFIER: &'static str = "source_identifier";
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
    pub const OPTIONAL_BLOB_ID: &'static str = "optional_blob_id";
    pub const MATERIAL_TYPE: &'static str = "material_type";

    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new(Self::SCHEMA), SourceMaterials::Table))
            .if_not_exists()
            // Primary key - ULID for time-ordered distribution
            .col(
                ColumnDef::new(SourceMaterials::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            // Core columns
            .col(
                ColumnDef::new(SourceMaterials::MaterialType)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterials::SourceIdentifier)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(SourceMaterials::SourceUri).text())
            .col(
                ColumnDef::new(SourceMaterials::IngestionTime)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::Encoding).text())
            .col(
                ColumnDef::new(SourceMaterials::Metadata)
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(ColumnDef::new(SourceMaterials::ContentPreview).text())
            .col(
                ColumnDef::new(SourceMaterials::IsArchived)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(ColumnDef::new(SourceMaterials::ArchiveTime).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::RetentionPolicy).text())
            .col(
                ColumnDef::new(SourceMaterials::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(SourceMaterials::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::OptionalBlobId).custom(Alias::new("ULID")))
            .to_owned()
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_check_constraints() -> Vec<String> {
        vec![]
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            // Index on source_uri
            format!(
                "CREATE INDEX IF NOT EXISTS idx_source_material_source_uri ON {}.{} (source_uri)",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on ingestion_time (DESC)
            format!(
                "CREATE INDEX IF NOT EXISTS idx_source_material_ingestion_time ON {}.{} (ingestion_time DESC)",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on material_type
            format!(
                "CREATE INDEX IF NOT EXISTS idx_source_material_type ON {}.{} (material_type)",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on optional_blob_id
            format!(
                "CREATE INDEX IF NOT EXISTS idx_source_material_blob ON {}.{} (optional_blob_id) WHERE optional_blob_id IS NOT NULL",
                Self::SCHEMA, Self::TABLE
            ),
            // Index on source_identifier
            format!(
                "CREATE INDEX IF NOT EXISTS idx_source_material_identifier ON {}.{} (source_identifier)",
                Self::SCHEMA, Self::TABLE
            ),
        ]
    }

    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new(Self::SCHEMA), SourceMaterials::Table))
            .if_not_exists()
            // Primary key - ULID for time-ordered distribution
            .col(
                ColumnDef::new(SourceMaterials::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            // Core columns
            .col(
                ColumnDef::new(SourceMaterials::MaterialType)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterials::SourceIdentifier)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(SourceMaterials::SourceUri).text())
            .col(
                ColumnDef::new(SourceMaterials::IngestionTime)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::Encoding).text())
            .col(
                ColumnDef::new(SourceMaterials::Metadata)
                    .json_binary()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(ColumnDef::new(SourceMaterials::ContentPreview).text())
            .col(
                ColumnDef::new(SourceMaterials::IsArchived)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(ColumnDef::new(SourceMaterials::ArchiveTime).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::RetentionPolicy).text())
            .col(
                ColumnDef::new(SourceMaterials::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(SourceMaterials::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::OptionalBlobId).custom(Alias::new("ULID")))
            .to_owned()
    }
}
