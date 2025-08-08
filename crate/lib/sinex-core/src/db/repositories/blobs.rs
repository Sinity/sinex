//! Repository for blob management
//!
//! Provides access to core.blobs table for managing binary large objects
//! stored in git-annex with metadata in PostgreSQL.

use crate::types::{ulid::Ulid, Id};
use chrono::Utc;
use color_eyre::eyre::{Context, Result};
use num_traits::ToPrimitive;
use sqlx::PgPool;
use tracing::instrument;

use crate::models::{Blob, BlobRecord};

/// Repository for blob operations
#[derive(Debug, Clone)]
pub struct BlobRepository {
    pool: PgPool,
}

impl BlobRepository {
    /// Create a new blob repository
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert a new blob
    #[instrument(skip(self, blob))]
    pub async fn insert(&self, blob: Blob) -> Result<Blob> {
        let record: BlobRecord = blob.into();

        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            INSERT INTO core.blobs (
                id, annex_key, original_filename, size_bytes, mime_type,
                checksum_sha256, checksum_blake3, storage_backend, metadata,
                created_at, last_verified_at, verification_status
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
            )
            RETURNING 
                id as "id: Ulid",
                annex_key,
                original_filename,
                size_bytes,
                mime_type,
                checksum_sha256,
                checksum_blake3,
                storage_backend,
                metadata,
                created_at,
                last_verified_at,
                verification_status
            "#,
            record.id as Ulid,
            record.annex_key,
            record.original_filename,
            record.size_bytes,
            record.mime_type,
            record.checksum_sha256,
            record.checksum_blake3,
            record.storage_backend,
            record.metadata,
            record.created_at,
            record.last_verified_at,
            record.verification_status
        )
        .fetch_one(&self.pool)
        .await
        .wrap_err("Failed to insert blob")?;

        Ok(result.into())
    }

    /// Get a blob by ID
    #[instrument(skip(self))]
    pub async fn get_by_id(&self, id: Id<Blob>) -> Result<Option<Blob>> {
        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            SELECT 
                id as "id: Ulid",
                annex_key,
                original_filename,
                size_bytes,
                mime_type,
                checksum_sha256,
                checksum_blake3,
                storage_backend,
                metadata,
                created_at,
                last_verified_at,
                verification_status
            FROM core.blobs
            WHERE id = $1
            "#,
            *id.as_ulid() as _
        )
        .fetch_optional(&self.pool)
        .await
        .wrap_err("Failed to get blob by ID")?;

        Ok(result.map(Into::into))
    }

    /// Find blob by BLAKE3 checksum (for deduplication)
    #[instrument(skip(self))]
    pub async fn find_by_blake3(&self, blake3_hash: &str) -> Result<Option<Blob>> {
        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            SELECT 
                id as "id: Ulid",
                annex_key,
                original_filename,
                size_bytes,
                mime_type,
                checksum_sha256,
                checksum_blake3,
                storage_backend,
                metadata,
                created_at,
                last_verified_at,
                verification_status
            FROM core.blobs
            WHERE checksum_blake3 = $1
            LIMIT 1
            "#,
            blake3_hash
        )
        .fetch_optional(&self.pool)
        .await
        .wrap_err("Failed to find blob by BLAKE3")?;

        Ok(result.map(Into::into))
    }

    /// Find blob by annex key
    #[instrument(skip(self))]
    pub async fn find_by_annex_key(&self, annex_key: &str) -> Result<Option<Blob>> {
        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            SELECT 
                id as "id: Ulid",
                annex_key,
                original_filename,
                size_bytes,
                mime_type,
                checksum_sha256,
                checksum_blake3,
                storage_backend,
                metadata,
                created_at,
                last_verified_at,
                verification_status
            FROM core.blobs
            WHERE annex_key = $1
            "#,
            annex_key
        )
        .fetch_optional(&self.pool)
        .await
        .wrap_err("Failed to find blob by annex key")?;

        Ok(result.map(Into::into))
    }

    /// Update blob verification status
    #[instrument(skip(self))]
    pub async fn update_verification_status(&self, id: Id<Blob>, status: &str) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE core.blobs
            SET 
                verification_status = $1,
                last_verified_at = $2
            WHERE id = $3
            "#,
            status,
            Utc::now(),
            *id.as_ulid() as _
        )
        .execute(&self.pool)
        .await
        .wrap_err("Failed to update verification status")?;

        Ok(())
    }

    /// Add an original filename to the metadata array
    #[instrument(skip(self))]
    pub async fn add_original_filename(&self, id: Id<Blob>, filename: &str) -> Result<()> {
        // Update the metadata JSON to include the filename in an array
        sqlx::query!(
            r#"
            UPDATE core.blobs
            SET metadata = jsonb_set(
                metadata,
                '{original_filenames}',
                COALESCE(metadata->'original_filenames', '[]'::jsonb) || to_jsonb($1::text),
                true
            )
            WHERE id = $2
            "#,
            filename,
            *id.as_ulid() as _
        )
        .execute(&self.pool)
        .await
        .wrap_err("Failed to add original filename")?;

        Ok(())
    }

    /// Get storage statistics
    #[instrument(skip(self))]
    pub async fn get_storage_stats(&self) -> Result<StorageStats> {
        let stats = sqlx::query!(
            r#"
            SELECT 
                COUNT(*) as "total_blobs!",
                COALESCE(SUM(size_bytes), 0) as "total_size!",
                COUNT(DISTINCT checksum_blake3) as "unique_blobs!",
                COALESCE(SUM(CASE WHEN checksum_blake3 IN (
                    SELECT checksum_blake3 
                    FROM core.blobs 
                    GROUP BY checksum_blake3 
                    HAVING COUNT(*) > 1
                ) THEN size_bytes ELSE 0 END), 0) as "duplicate_size!"
            FROM core.blobs
            "#
        )
        .fetch_one(&self.pool)
        .await
        .wrap_err("Failed to get storage statistics")?;

        Ok(StorageStats {
            total_blobs: stats.total_blobs,
            total_size_bytes: stats.total_size.to_i64().unwrap_or(0),
            unique_blobs: stats.unique_blobs,
            duplicate_size_bytes: stats.duplicate_size.to_i64().unwrap_or(0),
        })
    }
}

/// Storage statistics for blobs
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub total_blobs: i64,
    pub total_size_bytes: i64,
    pub unique_blobs: i64,
    pub duplicate_size_bytes: i64,
}

impl StorageStats {
    /// Calculate deduplication ratio
    pub fn deduplication_ratio(&self) -> f64 {
        if self.total_size_bytes == 0 {
            0.0
        } else {
            self.duplicate_size_bytes as f64 / self.total_size_bytes as f64
        }
    }
}
