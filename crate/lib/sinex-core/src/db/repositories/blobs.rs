//! Repository for blob management
//!
//! Provides access to core.blobs table for managing binary large objects
//! stored in git-annex with metadata in PostgreSQL.

use chrono::Utc;
use color_eyre::eyre::{Context, Result};
use num_traits::ToPrimitive;
use sqlx::PgPool;
use tracing::instrument;

use crate::models::Blob;
use crate::types::Id;
use crate::BlobRecord;

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
                annex_backend, content_hash, original_filename, size_bytes, 
                mime_type, checksum_blake3, metadata,
                created_at, last_verified_at, verification_status
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
            )
            RETURNING 
                id as "id: sinex_schema::ulid::Ulid",
                annex_backend,
                content_hash,
                original_filename,
                size_bytes,
                mime_type,
                checksum_blake3,
                metadata,
                created_at,
                last_verified_at,
                verification_status
            "#,
            record.annex_backend,
            record.content_hash,
            record.original_filename,
            record.size_bytes,
            record.mime_type,
            record.checksum_blake3,
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
        let id_uuid = sinex_schema::ulid_conversions::to_db(id.as_ulid());
        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            SELECT 
                id as "id: sinex_schema::ulid::Ulid",
                annex_backend,
                content_hash,
                original_filename,
                size_bytes,
                mime_type,
                checksum_blake3,
                metadata,
                created_at,
                last_verified_at,
                verification_status
            FROM core.blobs
            WHERE id = $1
            "#,
            id_uuid as _
        )
        .fetch_optional(&self.pool)
        .await
        .wrap_err("Failed to get blob by id")?;

        Ok(result.map(Into::into))
    }

    /// Get a blob by content hash and backend (reconstruct annex key)
    #[instrument(skip(self))]
    pub async fn get_by_content(
        &self,
        backend: &str,
        hash: &str,
        size: i64,
    ) -> Result<Option<Blob>> {
        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            SELECT 
                id as "id: sinex_schema::ulid::Ulid",
                annex_backend,
                content_hash,
                original_filename,
                size_bytes,
                mime_type,
                checksum_blake3,
                metadata,
                created_at,
                last_verified_at,
                verification_status
            FROM core.blobs
            WHERE annex_backend = $1 AND content_hash = $2 AND size_bytes = $3
            "#,
            backend,
            hash,
            size
        )
        .fetch_optional(&self.pool)
        .await
        .wrap_err("Failed to get blob by content")?;

        Ok(result.map(Into::into))
    }

    /// Find blob by BLAKE3 checksum (for deduplication)
    #[instrument(skip(self))]
    pub async fn find_by_blake3(&self, blake3_hash: &str) -> Result<Option<Blob>> {
        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            SELECT 
                id as "id: sinex_schema::ulid::Ulid",
                annex_backend,
                content_hash,
                original_filename,
                size_bytes,
                mime_type,
                checksum_blake3,
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

    /// Update blob verification status
    #[instrument(skip(self))]
    pub async fn update_verification_status(&self, id: Id<Blob>, status: &str) -> Result<()> {
        let id_uuid = sinex_schema::ulid_conversions::to_db(id.as_ulid());
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
            id_uuid as _
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
        let id_uuid = sinex_schema::ulid_conversions::to_db(id.as_ulid());
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
            id_uuid as _
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
            total_blobs: stats.total_blobs.to_i64().unwrap_or(0),
            total_size_bytes: stats.total_size.to_i64().unwrap_or(0),
            unique_blobs: stats.unique_blobs.to_i64().unwrap_or(0),
            duplicate_size_bytes: stats.duplicate_size.to_i64().unwrap_or(0),
        })
    }
}

/// Storage statistics
#[derive(Debug)]
pub struct StorageStats {
    pub total_blobs: i64,
    pub total_size_bytes: i64,
    pub unique_blobs: i64,
    pub duplicate_size_bytes: i64,
}
