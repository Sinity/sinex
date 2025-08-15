//! Schema definitions for source materials tables

use sea_orm_migration::prelude::*;

#[derive(Iden, Copy, Clone)]
pub enum SourceMaterials {
    #[iden = "source_material_registry"]
    Table,
    #[iden = "source_material_id"]
    SourceMaterialId,
    #[iden = "source_identifier"]
    SourceIdentifier,
    #[iden = "acquired_at"]
    AcquiredAt,
    #[iden = "data"]
    Data,
    #[iden = "size_bytes"]
    SizeBytes,
    #[iden = "mime_type"]
    MimeType,
    #[iden = "content_hash_blake3"]
    ContentHashBlake3,
    #[iden = "metadata"]
    Metadata,
    // Legacy columns
    #[iden = "checksum"]
    Checksum,
    #[iden = "source_type"]
    SourceType,
    #[iden = "source_path"]
    SourcePath,
    #[iden = "content_type"]
    ContentType,
    #[iden = "status"]
    Status,
    #[iden = "total_bytes"]
    TotalBytes,
    #[iden = "created_at"]
    CreatedAt,
    #[iden = "finalized_at"]
    FinalizedAt,
    #[iden = "staged_at"]
    StagedAt,
    #[iden = "optional_blob_id"]
    OptionalBlobId,
    // New columns from migrations
    #[iden = "id"]
    Id,
    #[iden = "material_type"]
    MaterialType,
    #[iden = "content_preview"]
    ContentPreview,
    #[iden = "source_uri"]
    SourceUri,
    #[iden = "encoding"]
    Encoding,
    #[iden = "is_archived"]
    IsArchived,
    #[iden = "retention_policy"]
    RetentionPolicy,
    #[iden = "ingestion_time"]
    IngestionTime,
    #[iden = "archive_time"]
    ArchiveTime,
    #[iden = "updated_at"]
    UpdatedAt,
}

impl SourceMaterials {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("raw"), SourceMaterials::Table))
            .if_not_exists()
            // Primary key - ULID for time-ordered distribution
            .col(
                ColumnDef::new(SourceMaterials::SourceMaterialId)
                    .custom("ULID")
                    .not_null()
                    .primary_key(),
            )
            // Core columns
            .col(
                ColumnDef::new(SourceMaterials::SourceIdentifier)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterials::AcquiredAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::Data).binary())
            .col(
                ColumnDef::new(SourceMaterials::SizeBytes)
                    .big_integer()
                    .not_null(),
            )
            .col(ColumnDef::new(SourceMaterials::MimeType).text())
            .col(ColumnDef::new(SourceMaterials::ContentHashBlake3).binary())
            .col(
                ColumnDef::new(SourceMaterials::Metadata)
                    .json_binary()
                    .default("{}"),
            )
            // Legacy columns for compatibility
            .col(ColumnDef::new(SourceMaterials::Checksum).text())
            .col(ColumnDef::new(SourceMaterials::SourceType).text())
            .col(ColumnDef::new(SourceMaterials::SourcePath).text())
            .col(ColumnDef::new(SourceMaterials::ContentType).text())
            .col(ColumnDef::new(SourceMaterials::Status).text())
            .col(ColumnDef::new(SourceMaterials::TotalBytes).big_integer())
            .col(ColumnDef::new(SourceMaterials::CreatedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::FinalizedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::StagedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::OptionalBlobId).custom("ULID"))
            // New columns from migrations
            .col(ColumnDef::new(SourceMaterials::Id).custom("ULID"))
            .col(
                ColumnDef::new(SourceMaterials::MaterialType)
                    .text()
                    .not_null()
                    .default("generic"),
            )
            .col(ColumnDef::new(SourceMaterials::ContentPreview).text())
            .col(
                ColumnDef::new(SourceMaterials::SourceUri)
                    .text()
                    .not_null()
                    .default(""),
            )
            .col(
                ColumnDef::new(SourceMaterials::Encoding)
                    .text()
                    .not_null()
                    .default("utf-8"),
            )
            .col(
                ColumnDef::new(SourceMaterials::IsArchived)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(
                ColumnDef::new(SourceMaterials::RetentionPolicy)
                    .text()
                    .default("permanent"),
            )
            .col(
                ColumnDef::new(SourceMaterials::IngestionTime)
                    .timestamp_with_time_zone()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::ArchiveTime).timestamp_with_time_zone())
            .col(
                ColumnDef::new(SourceMaterials::UpdatedAt)
                    .timestamp_with_time_zone()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_check_constraints() -> Vec<String> {
        vec![
            format!(
                r#"ALTER TABLE raw.{} ADD CONSTRAINT chk_source_materials_status 
                   CHECK (status IN ('sensing', 'completed', 'failed', 'archived'))"#,
                SourceMaterials::Table.to_string()
            ),
            format!(
                r#"ALTER TABLE raw.{} ADD CONSTRAINT chk_source_materials_data_xor_blob 
                   CHECK ((data IS NULL) <> (optional_blob_id IS NULL))"#,
                SourceMaterials::Table.to_string()
            ),
        ]
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sm_registry_checksum 
                   ON raw.{} (checksum) WHERE checksum IS NOT NULL"#,
                SourceMaterials::Table.to_string()
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sm_registry_srcid 
                   ON raw.{} (source_identifier, staged_at DESC)"#,
                SourceMaterials::Table.to_string()
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sm_registry_status 
                   ON raw.{} (status, created_at)"#,
                SourceMaterials::Table.to_string()
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS ix_sm_registry_source_type 
                   ON raw.{} (source_type)"#,
                SourceMaterials::Table.to_string()
            ),
        ]
    }

    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("raw"), SourceMaterials::Table))
            .if_not_exists()
            // Primary key - ULID for time-ordered distribution
            .col(
                ColumnDef::new(SourceMaterials::SourceMaterialId)
                    .custom("ULID")
                    .not_null()
                    .primary_key(),
            )
            // Core columns
            .col(
                ColumnDef::new(SourceMaterials::SourceIdentifier)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterials::AcquiredAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::Data).binary())
            .col(
                ColumnDef::new(SourceMaterials::SizeBytes)
                    .big_integer()
                    .not_null(),
            )
            .col(ColumnDef::new(SourceMaterials::MimeType).text())
            .col(ColumnDef::new(SourceMaterials::ContentHashBlake3).binary())
            .col(
                ColumnDef::new(SourceMaterials::Metadata)
                    .json_binary()
                    .default("{}"),
            )
            // Legacy columns for compatibility
            .col(ColumnDef::new(SourceMaterials::Checksum).text())
            .col(ColumnDef::new(SourceMaterials::SourceType).text())
            .col(ColumnDef::new(SourceMaterials::SourcePath).text())
            .col(ColumnDef::new(SourceMaterials::ContentType).text())
            .col(ColumnDef::new(SourceMaterials::Status).text())
            .col(ColumnDef::new(SourceMaterials::TotalBytes).big_integer())
            .col(ColumnDef::new(SourceMaterials::CreatedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::FinalizedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::StagedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::OptionalBlobId).custom("ULID"))
            // New columns from migrations
            .col(ColumnDef::new(SourceMaterials::Id).custom("ULID"))
            .col(
                ColumnDef::new(SourceMaterials::MaterialType)
                    .text()
                    .not_null()
                    .default("generic"),
            )
            .col(ColumnDef::new(SourceMaterials::ContentPreview).text())
            .col(
                ColumnDef::new(SourceMaterials::SourceUri)
                    .text()
                    .not_null()
                    .default(""),
            )
            .col(
                ColumnDef::new(SourceMaterials::Encoding)
                    .text()
                    .not_null()
                    .default("utf-8"),
            )
            .col(
                ColumnDef::new(SourceMaterials::IsArchived)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(
                ColumnDef::new(SourceMaterials::RetentionPolicy)
                    .text()
                    .default("permanent"),
            )
            .col(
                ColumnDef::new(SourceMaterials::IngestionTime)
                    .timestamp_with_time_zone()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::ArchiveTime).timestamp_with_time_zone())
            .col(
                ColumnDef::new(SourceMaterials::UpdatedAt)
                    .timestamp_with_time_zone()
                    .default(Expr::current_timestamp()),
            )
            .to_owned()
    }
}
