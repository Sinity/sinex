//! Schema definitions for source materials tables

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum SourceMaterials {
    #[iden = "source_material_registry"]
    Table,
    #[iden = "blob_id"]
    BlobId,
    #[iden = "checksum"]
    Checksum,
    #[iden = "source_identifier"]
    SourceIdentifier,
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
    #[iden = "metadata"]
    Metadata,
    #[iden = "data"]
    Data,
    #[iden = "optional_blob_id"]
    OptionalBlobId,
}

impl SourceMaterials {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("raw"), SourceMaterials::Table))
            .if_not_exists()
            // Primary key - ULID for time-ordered distribution
            .col(
                ColumnDef::new(SourceMaterials::BlobId)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            // Content integrity
            .col(ColumnDef::new(SourceMaterials::Checksum).text())
            // Source identification
            .col(
                ColumnDef::new(SourceMaterials::SourceIdentifier)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterials::SourceType)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(SourceMaterials::SourcePath).text())
            .col(ColumnDef::new(SourceMaterials::ContentType).text())
            // Lifecycle status
            .col(
                ColumnDef::new(SourceMaterials::Status)
                    .text()
                    .not_null()
                    .default("'sensing'"),
            )
            // Size tracking
            .col(ColumnDef::new(SourceMaterials::TotalBytes).big_integer())
            // Timestamps
            .col(
                ColumnDef::new(SourceMaterials::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::FinalizedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::StagedAt).timestamp_with_time_zone())
            // Flexible metadata
            .col(
                ColumnDef::new(SourceMaterials::Metadata)
                    .json_binary()
                    .default("'{}'"),
            )
            // Optional inline data storage for small materials
            .col(ColumnDef::new(SourceMaterials::Data).blob())
            // Optional reference to external blob storage
            .col(ColumnDef::new(SourceMaterials::OptionalBlobId).custom(Alias::new("ULID")))
            .to_owned()
            .to_string(sea_query::PostgresQueryBuilder)
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
                ColumnDef::new(SourceMaterials::BlobId)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            // Content integrity
            .col(ColumnDef::new(SourceMaterials::Checksum).text())
            // Source identification
            .col(
                ColumnDef::new(SourceMaterials::SourceIdentifier)
                    .text()
                    .not_null(),
            )
            .col(
                ColumnDef::new(SourceMaterials::SourceType)
                    .text()
                    .not_null(),
            )
            .col(ColumnDef::new(SourceMaterials::SourcePath).text())
            .col(ColumnDef::new(SourceMaterials::ContentType).text())
            // Lifecycle status
            .col(
                ColumnDef::new(SourceMaterials::Status)
                    .text()
                    .not_null()
                    .default("'sensing'"),
            )
            // Size tracking
            .col(ColumnDef::new(SourceMaterials::TotalBytes).big_integer())
            // Timestamps
            .col(
                ColumnDef::new(SourceMaterials::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(SourceMaterials::FinalizedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(SourceMaterials::StagedAt).timestamp_with_time_zone())
            // Flexible metadata
            .col(
                ColumnDef::new(SourceMaterials::Metadata)
                    .json_binary()
                    .default("'{}'"),
            )
            // Optional inline data storage for small materials
            .col(ColumnDef::new(SourceMaterials::Data).blob())
            // Optional reference to external blob storage
            .col(ColumnDef::new(SourceMaterials::OptionalBlobId).custom(Alias::new("ULID")))
            .to_owned()
    }
}
