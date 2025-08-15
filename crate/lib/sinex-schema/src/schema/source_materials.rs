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
    #[iden = "metadata"]
    Metadata,
    #[iden = "optional_blob_id"]
    OptionalBlobId,
    #[iden = "material_type"]
    MaterialType,
    #[iden = "content_preview"]
    ContentPreview,
    #[iden = "source_uri"]
    SourceUri,
    #[iden = "encoding"]
    Encoding,
    #[iden = "created_at"]
    CreatedAt,
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
                ColumnDef::new(SourceMaterials::Id)
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
                ColumnDef::new(SourceMaterials::Metadata)
                    .json_binary()
                    .default("{}"),
            )
            .col(ColumnDef::new(SourceMaterials::OptionalBlobId).custom("ULID"))
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
            .to_owned()
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_check_constraints() -> Vec<String> {
        vec![]
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }

    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table((Alias::new("raw"), SourceMaterials::Table))
            .if_not_exists()
            // Primary key - ULID for time-ordered distribution
            .col(
                ColumnDef::new(SourceMaterials::Id)
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
                ColumnDef::new(SourceMaterials::Metadata)
                    .json_binary()
                    .default("{}"),
            )
            .col(ColumnDef::new(SourceMaterials::OptionalBlobId).custom("ULID"))
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
            .to_owned()
    }
}
