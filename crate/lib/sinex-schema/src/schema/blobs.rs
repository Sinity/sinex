//! The Canonical Database Schema for Content-Addressed Storage (`core.blobs`).
//!
//! This module defines the schema for managing metadata about large binary objects
//! (blobs) that are stored externally, primarily in git-annex. It acts as a
//! high-performance index and metadata cache for the content-addressed store.

use crate::primitives::{Timestamp, Uuid};
use crate::schema::{SourceMaterialRegistry, TableDef};
use sea_query::{
    Alias, ColumnDef, ConditionalStatement, Expr, ForeignKey, ForeignKeyAction,
    ForeignKeyCreateStatement, Iden, Index, IndexCreateStatement, Table, TableCreateStatement,
};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

// =============================================================================
// The `core.blobs` Table
// =============================================================================

/// **Table: `core.blobs`**
///
/// This table stores metadata for large binary objects. The actual content is stored
/// in an external content-addressed system like git-annex. This table provides a
///
/// fast, queryable index into that store.
///
/// **Design Rationale:**
/// - **Surrogate vs. Natural Key:** A `UUID` surrogate key (`id`) is used as the
///   primary key for performance. `UUID`s (which `UUIDv7` IDs are stored as) are fixed-size
///   (16 bytes) and excellent for join performance. The `annex_key` is a long,
///   variable-length string, making it a poor choice for a primary key that will
///   be referenced by many foreign keys.
/// - **Decomposed `annex_key`:** The `annex_key` string is decomposed into its
///   constituent parts (`annex_backend`, `content_hash`, `size_bytes`) to allow for
///   typed storage and efficient, direct querying on these attributes. A `UNIQUE`
///   constraint on `(annex_backend, content_hash)` preserves the natural key's integrity.
/// - **Dual Checksums:** The table stores both a cryptographic hash (from the annex
///   key) for integrity and a faster, non-cryptographic hash (`checksum_blake3`)
///   for high-speed deduplication checks during ingestion.
#[derive(Iden, Copy, Clone)]
pub enum Blobs {
    Table,
    Id,
    // Decomposed annex key components
    AnnexBackend,
    ContentHash,
    SizeBytes,
    // Fast deduplication hash
    ChecksumBlake3,
    // Essential metadata
    OriginalFilename,
    MimeType,
    // Rich intrinsic metadata
    Metadata,
    // Operational status
    CreatedAt,
    LastVerifiedAt,
    VerificationStatus,
}

impl TableDef for Blobs {
    fn table_name() -> &'static str {
        "blobs"
    }
    fn schema_name() -> &'static str {
        "core"
    }
    fn primary_key() -> &'static str {
        "id"
    }
}

/// The Rust struct representation of a row from `core.blobs`.
/// This is used by `sqlx::query_as!` for deserializing database results.
///
/// ## Serialization Support
///
/// When the `serde` feature is enabled, this struct supports JSON serialization
/// and deserialization, making it suitable for API responses and content management.
#[derive(Debug, FromRow)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlobRecord {
    pub id: Uuid,
    pub annex_backend: String,
    pub content_hash: String,
    pub size_bytes: i64,
    pub checksum_blake3: Option<String>,
    pub original_filename: String,
    pub mime_type: Option<String>,
    pub metadata: JsonValue,
    pub created_at: Timestamp,
    pub last_verified_at: Option<Timestamp>,
    pub verification_status: Option<String>,
}

impl Blobs {
    /// Generates the `CREATE TABLE` statement for `core.blobs`.
    #[must_use]
    pub fn create_table_statement() -> TableCreateStatement {
        Table::create()
            .table(Self::table_iden())
            .if_not_exists()
            .col(
                ColumnDef::new(Blobs::Id)
                    .custom(Alias::new("UUID"))
                    .primary_key()
                    .extra("DEFAULT uuidv7()"),
            )
            .col(ColumnDef::new(Blobs::AnnexBackend).text().not_null())
            .col(ColumnDef::new(Blobs::ContentHash).text().not_null())
            .col(ColumnDef::new(Blobs::SizeBytes).big_integer().not_null())
            .col(ColumnDef::new(Blobs::ChecksumBlake3).text())
            .col(ColumnDef::new(Blobs::OriginalFilename).text().not_null())
            .col(ColumnDef::new(Blobs::MimeType).text())
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
            .col(
                ColumnDef::new(Blobs::VerificationStatus)
                    .text()
                    .check(Expr::cust(
                        "verification_status IN ('pending', 'verified', 'corrupted')",
                    )),
            )
            // The true natural key is enforced via unique index - see create_indexes()
            .to_owned()
    }

    /// Generates all necessary indexes for `core.blobs`.
    #[must_use]
    pub fn create_indexes() -> Vec<IndexCreateStatement> {
        vec![
            // The true natural key of the annexed content is the combination of its hashing algorithm and the resulting hash.
            Index::create()
                .if_not_exists()
                .name("uk_blobs_annex_backend_content_hash")
                .table(Self::table_iden())
                .col(Blobs::AnnexBackend)
                .col(Blobs::ContentHash)
                .unique()
                .to_owned(),
            // An index on the BLAKE3 checksum is critical for the high-speed deduplication check performed during ingestion.
            // This is a unique index to ensure no duplicate content
            Index::create()
                .if_not_exists()
                .name("uk_blobs_checksum_blake3")
                .table(Self::table_iden())
                .col(Blobs::ChecksumBlake3)
                .unique()
                .cond_where(Expr::col(Blobs::ChecksumBlake3).is_not_null())
                .to_owned(),
            // Index for finding blobs that need periodic integrity verification.
            Index::create()
                .if_not_exists()
                .name("ix_blobs_verification_status")
                .table(Self::table_iden())
                .col(Blobs::VerificationStatus)
                .col(Blobs::LastVerifiedAt)
                .to_owned(),
        ]
    }
}

// =============================================================================
// Foreign Key Integration
//
// Defines the relationship from `raw.source_material_registry` to `core.blobs`.
// This must be run *after* both tables have been created.
// =============================================================================

impl SourceMaterialRegistry {
    /// Generates the `ALTER TABLE` statement to add the foreign key to `core.blobs`.
    #[must_use]
    pub fn create_blob_foreign_key() -> ForeignKeyCreateStatement {
        ForeignKey::create()
            .from(Self::table_iden(), Alias::new("optional_blob_id"))
            .to(Blobs::table_iden(), Blobs::Id)
            .on_delete(ForeignKeyAction::SetNull) // If a blob is deleted, don't delete the source material record, just nullify the link.
            .to_owned()
    }
}
