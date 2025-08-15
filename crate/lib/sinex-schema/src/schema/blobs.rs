//! Schema definitions for blob storage tables

use sea_orm_migration::prelude::*;

/// Blobs table
#[derive(Iden)]
pub enum Blobs {
    Table,
    Id,
    AnnexKey,
    OriginalFilename,
    SizeBytes,
    MimeType,
    ChecksumSha256,
    ChecksumBlake3,
    StorageBackend,
    Metadata,
    CreatedAt,
    LastVerifiedAt,
    VerificationStatus,
    // Legacy fields
    UpdatedAt,
    ContentHash,
    StoredAt,
    ContentType,
}

impl Blobs {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), Blobs::Table))
            .if_not_exists()
            .col(ColumnDef::new(Blobs::Id).uuid().not_null().primary_key())
            .col(ColumnDef::new(Blobs::AnnexKey).text().not_null())
            .col(ColumnDef::new(Blobs::OriginalFilename).text())
            .col(ColumnDef::new(Blobs::SizeBytes).big_integer().not_null())
            .col(ColumnDef::new(Blobs::MimeType).text())
            .col(ColumnDef::new(Blobs::ChecksumSha256).text().not_null())
            .col(ColumnDef::new(Blobs::ChecksumBlake3).text().not_null())
            .col(ColumnDef::new(Blobs::StorageBackend).text().not_null())
            .col(ColumnDef::new(Blobs::Metadata).json_binary())
            .col(
                ColumnDef::new(Blobs::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default("NOW()"),
            )
            .col(ColumnDef::new(Blobs::LastVerifiedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Blobs::VerificationStatus).text().not_null())
            // Legacy columns for compatibility
            .col(ColumnDef::new(Blobs::UpdatedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Blobs::ContentHash).text())
            .col(ColumnDef::new(Blobs::StoredAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Blobs::ContentType).text())
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            "CREATE INDEX IF NOT EXISTS idx_blobs_content_hash ON core.blobs (content_hash);"
                .to_string(),
        ]
    }
}
