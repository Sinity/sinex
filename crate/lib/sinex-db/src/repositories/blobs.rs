//! Repository for blob management
//!
//! Provides access to core.blobs table for managing binary large objects
//! stored by the SDK content store with metadata in `PostgreSQL`.

use num_traits::ToPrimitive;
use sqlx::PgPool;
use tracing::instrument;

use crate::models::Blob;
use crate::repositories::common::{DbResult, db_error};
use crate::{BlobRecord, SinexError, Timestamp};
use sinex_primitives::Id;
use sinex_primitives::domain::BlobVerificationStatus;

/// Repository for blob operations
#[derive(Debug, Clone)]
pub struct BlobRepository {
    pool: PgPool,
}

impl BlobRepository {
    fn decode_record(record: BlobRecord, operation: &'static str) -> DbResult<Blob> {
        Blob::try_from(record).map_err(|err| SinexError::database(format!("{operation}: {err}")))
    }

    /// Create a new blob repository
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert a new blob
    #[instrument(skip(self, blob))]
    pub async fn insert(&self, blob: Blob) -> DbResult<Blob> {
        self.insert_with_executor(&self.pool, blob).await
    }

    /// Insert a new blob with a specific executor (e.g. for transactions)
    #[instrument(skip(self, executor, blob))]
    pub async fn insert_with_executor<'e, E>(&self, executor: E, blob: Blob) -> DbResult<Blob>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let record: BlobRecord = blob.into();

        let record = if record.checksum_blake3.is_some() {
            sqlx::query_as!(
                BlobRecord,
                r#"
                INSERT INTO core.blobs (
                    annex_backend, content_hash, original_filename, size_bytes,
                    mime_type, checksum_blake3, metadata,
                    created_at, last_verified_at, verification_status
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                )
                ON CONFLICT (checksum_blake3) WHERE checksum_blake3 IS NOT NULL DO UPDATE
                SET original_filename = core.blobs.original_filename
                RETURNING
                    id as "id!: uuid::Uuid",
                    annex_backend,
                    content_hash,
                    original_filename,
                    size_bytes,
                    mime_type,
                    checksum_blake3,
                    metadata,
                    created_at as "created_at: Timestamp",
                    last_verified_at as "last_verified_at: Timestamp",
                    verification_status
                "#,
                record.annex_backend,
                record.content_hash,
                record.original_filename,
                record.size_bytes,
                record.mime_type,
                record.checksum_blake3,
                record.metadata,
                record.created_at.inner(),
                record.last_verified_at.map(|ts| ts.inner()),
                record.verification_status
            )
            .fetch_one(executor)
            .await
        } else {
            sqlx::query_as!(
                BlobRecord,
                r#"
                INSERT INTO core.blobs (
                    annex_backend, content_hash, original_filename, size_bytes,
                    mime_type, checksum_blake3, metadata,
                    created_at, last_verified_at, verification_status
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                )
                ON CONFLICT (annex_backend, content_hash) DO UPDATE
                SET original_filename = core.blobs.original_filename
                RETURNING
                    id as "id!: uuid::Uuid",
                    annex_backend,
                    content_hash,
                    original_filename,
                    size_bytes,
                    mime_type,
                    checksum_blake3,
                    metadata,
                    created_at as "created_at: Timestamp",
                    last_verified_at as "last_verified_at: Timestamp",
                    verification_status
                "#,
                record.annex_backend,
                record.content_hash,
                record.original_filename,
                record.size_bytes,
                record.mime_type,
                record.checksum_blake3,
                record.metadata,
                record.created_at.inner(),
                record.last_verified_at.map(|ts| ts.inner()),
                record.verification_status
            )
            .fetch_one(executor)
            .await
        }
        .map_err(|err| {
            SinexError::database(format!(
                "Failed to insert blob (backend={}, hash={}): {err}",
                record.annex_backend, record.content_hash
            ))
        })?;

        Self::decode_record(record, "insert blob")
    }

    /// Get a blob by ID
    #[instrument(skip(self))]
    pub async fn get_by_id(&self, id: Id<Blob>) -> DbResult<Option<Blob>> {
        let id_uuid = id.to_uuid();
        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            SELECT 
                id as "id!: uuid::Uuid",
                annex_backend,
                content_hash,
                original_filename,
                size_bytes,
                mime_type,
                checksum_blake3,
                metadata,
                created_at as "created_at: Timestamp",
                last_verified_at as "last_verified_at: Timestamp",
                verification_status
            FROM core.blobs
            WHERE id = $1
            "#,
            id_uuid as _
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| db_error(e, "get blob by id"))?;

        result
            .map(|record| Self::decode_record(record, "get blob by id"))
            .transpose()
    }

    /// Get a blob by content hash and backend (reconstruct annex key)
    #[instrument(skip(self))]
    pub async fn get_by_content(
        &self,
        backend: &str,
        hash: &str,
        size: i64,
    ) -> DbResult<Option<Blob>> {
        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            SELECT 
                id as "id!: uuid::Uuid",
                annex_backend,
                content_hash,
                original_filename,
                size_bytes,
                mime_type,
                checksum_blake3,
                metadata,
                created_at as "created_at: Timestamp",
                last_verified_at as "last_verified_at: Timestamp",
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
        .map_err(|e| db_error(e, "get blob by content"))?;

        result
            .map(|record| Self::decode_record(record, "get blob by content"))
            .transpose()
    }

    /// Find blob by BLAKE3 checksum (for deduplication)
    #[instrument(skip(self))]
    pub async fn find_by_blake3(&self, blake3_hash: &str) -> DbResult<Option<Blob>> {
        let result = sqlx::query_as!(
            BlobRecord,
            r#"
            SELECT 
                id as "id!: uuid::Uuid",
                annex_backend,
                content_hash,
                original_filename,
                size_bytes,
                mime_type,
                checksum_blake3,
                metadata,
                created_at as "created_at: Timestamp",
                last_verified_at as "last_verified_at: Timestamp",
                verification_status
            FROM core.blobs
            WHERE checksum_blake3 = $1
            LIMIT 1
            "#,
            blake3_hash
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| db_error(e, "find blob by BLAKE3"))?;

        result
            .map(|record| Self::decode_record(record, "find blob by BLAKE3"))
            .transpose()
    }

    /// Update blob verification status
    #[instrument(skip(self))]
    pub async fn update_verification_status(
        &self,
        id: Id<Blob>,
        status: BlobVerificationStatus,
    ) -> DbResult<()> {
        let id_uuid = id.to_uuid();
        let status_str = status.to_string();
        sqlx::query!(
            r#"
            UPDATE core.blobs
            SET
                verification_status = $1,
                last_verified_at = $2
            WHERE id = $3::uuid
            "#,
            status_str,
            Timestamp::now().inner(),
            id_uuid as _
        )
        .execute(&self.pool)
        .await
        .map_err(|e| db_error(e, "update verification status"))?;

        Ok(())
    }

    /// Add an original filename to the metadata array
    #[instrument(skip(self))]
    pub async fn add_original_filename(&self, id: Id<Blob>, filename: &str) -> DbResult<()> {
        // Update the metadata JSON to include the filename in an array
        let id_uuid = id.to_uuid();
        sqlx::query!(
            r#"
            UPDATE core.blobs
            SET metadata = jsonb_set(
                metadata,
                '{original_filenames}',
                COALESCE(metadata->'original_filenames', '[]'::jsonb) || to_jsonb($1::text),
                true
            )
            WHERE id = $2::uuid
            "#,
            filename,
            id_uuid as _
        )
        .execute(&self.pool)
        .await
        .map_err(|e| db_error(e, "add original filename"))?;

        Ok(())
    }

    /// Get storage statistics
    #[instrument(skip(self))]
    pub async fn get_storage_stats(&self) -> DbResult<StorageStats> {
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
                ) THEN size_bytes ELSE 0 END), 0) as "duplicate_size!",
                COUNT(CASE WHEN verification_status = 'corrupted' THEN 1 END) as "failed_verifications!"
            FROM core.blobs
            "#
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| db_error(e, "get storage statistics"))?;

        Ok(StorageStats {
            total_blobs: stats.total_blobs.to_i64().unwrap_or(0),
            total_size_bytes: stats.total_size.to_i64().unwrap_or(0),
            unique_blobs: stats.unique_blobs.to_i64().unwrap_or(0),
            duplicate_size_bytes: stats.duplicate_size.to_i64().unwrap_or(0),
            failed_verifications: stats.failed_verifications.to_i64().unwrap_or(0),
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
    /// Number of blobs that failed verification
    pub failed_verifications: i64,
}
