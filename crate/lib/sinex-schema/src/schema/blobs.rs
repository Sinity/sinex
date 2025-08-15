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
    pub const TABLE: &'static str = "blobs";
    pub const SCHEMA: &'static str = "core";

    pub const ID: &'static str = "id";
    pub const ANNEX_KEY: &'static str = "annex_key";
    pub const ORIGINAL_FILENAME: &'static str = "original_filename";
    pub const SIZE_BYTES: &'static str = "size_bytes";
    pub const MIME_TYPE: &'static str = "mime_type";
    pub const CHECKSUM_SHA256: &'static str = "checksum_sha256";
    pub const CHECKSUM_BLAKE3: &'static str = "checksum_blake3";
    pub const STORAGE_BACKEND: &'static str = "storage_backend";
    pub const METADATA: &'static str = "metadata";
    pub const CREATED_AT: &'static str = "created_at";
    pub const LAST_VERIFIED_AT: &'static str = "last_verified_at";
    pub const VERIFICATION_STATUS: &'static str = "verification_status";

    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), Blobs::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(Blobs::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key()
                    .default(Expr::cust("gen_ulid()")),
            )
            .col(ColumnDef::new(Blobs::AnnexKey).text().not_null())
            .col(ColumnDef::new(Blobs::OriginalFilename).text().not_null())
            .col(ColumnDef::new(Blobs::SizeBytes).big_integer().not_null())
            .col(ColumnDef::new(Blobs::MimeType).text())
            .col(ColumnDef::new(Blobs::ChecksumSha256).text().not_null())
            .col(ColumnDef::new(Blobs::ChecksumBlake3).text())
            .col(
                ColumnDef::new(Blobs::StorageBackend)
                    .text()
                    .not_null()
                    .default("git-annex"),
            )
            .col(
                ColumnDef::new(Blobs::Metadata)
                    .json_binary()
                    .not_null()
                    .default(Expr::cust("'{}'::jsonb")),
            )
            .col(
                ColumnDef::new(Blobs::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(ColumnDef::new(Blobs::LastVerifiedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Blobs::VerificationStatus).text())
            // Legacy columns for compatibility
            .col(ColumnDef::new(Blobs::UpdatedAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Blobs::ContentHash).text())
            .col(ColumnDef::new(Blobs::StoredAt).timestamp_with_time_zone())
            .col(ColumnDef::new(Blobs::ContentType).text())
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![
            // Unique index on annex_key
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_blobs_annex_key ON core.blobs (annex_key);".to_string(),
            // Index on checksum_sha256 for deduplication
            "CREATE INDEX IF NOT EXISTS idx_blobs_checksum_sha256 ON core.blobs (checksum_sha256);".to_string(),
            // Index on checksum_blake3 for deduplication
            "CREATE INDEX IF NOT EXISTS idx_blobs_checksum_blake3 ON core.blobs (checksum_blake3) WHERE checksum_blake3 IS NOT NULL;".to_string(),
            // Legacy index on content_hash
            "CREATE INDEX IF NOT EXISTS idx_blobs_content_hash ON core.blobs (content_hash) WHERE content_hash IS NOT NULL;".to_string(),
        ]
    }
}
