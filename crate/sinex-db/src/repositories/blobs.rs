//! Repository for blob management
//!
//! Provides access to core.blobs table for managing binary large objects
//! stored by the SDK content store with metadata in `PostgreSQL`.

use num_traits::ToPrimitive;
use sqlx::{Executor, PgPool, Postgres};
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

    /// Get a blob by content hash and backend (reconstruct content-store key)
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

    /// Check whether a blob still has any live reference other than the
    /// given source material.
    ///
    /// Content-addressed dedup means a blob can be shared by multiple
    /// `raw.source_material_registry` rows (no UNIQUE constraint on
    /// `optional_blob_id`) and can also be referenced directly by
    /// `core.events`/`audit.archived_events` via `associated_blob_ids`
    /// (derived events that carry blob provenance without going through a
    /// material). Delete-on-tombstone must only drop CAS content once the
    /// reference count across ALL of these surfaces is genuinely zero --
    /// see sinex-audit-cas-shared-blob-delete.
    #[instrument(skip(self))]
    pub async fn is_referenced_excluding_material(
        &self,
        blob_id: Id<Blob>,
        excluding_material_id: uuid::Uuid,
    ) -> DbResult<bool> {
        self.is_referenced_excluding_material_with_executor(
            &self.pool,
            blob_id,
            excluding_material_id,
        )
        .await
    }

    /// Same check as [`Self::is_referenced_excluding_material`], but against
    /// a caller-supplied executor so it can be run inside an existing
    /// transaction -- see [`Self::lock_by_id_for_update`] for why that
    /// matters for delete-on-tombstone.
    #[instrument(skip(self, executor))]
    pub async fn is_referenced_excluding_material_with_executor<'e, E>(
        &self,
        executor: E,
        blob_id: Id<Blob>,
        excluding_material_id: uuid::Uuid,
    ) -> DbResult<bool>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let blob_uuid = blob_id.to_uuid();
        let referenced = sqlx::query_scalar!(
            r#"
            SELECT (
                EXISTS (
                    SELECT 1 FROM raw.source_material_registry m
                    WHERE m.optional_blob_id = $1 AND m.id <> $2
                )
                OR EXISTS (
                    SELECT 1 FROM core.events e
                    WHERE e.associated_blob_ids IS NOT NULL
                      AND e.associated_blob_ids @> ARRAY[$1]::uuid[]
                )
                OR EXISTS (
                    SELECT 1 FROM audit.archived_events ae
                    WHERE ae.associated_blob_ids IS NOT NULL
                      AND ae.associated_blob_ids @> ARRAY[$1]::uuid[]
                )
            ) AS "referenced!"
            "#,
            blob_uuid,
            excluding_material_id
        )
        .fetch_one(executor)
        .await
        .map_err(|e| db_error(e, "check blob reference count"))?;

        Ok(referenced)
    }

    /// Lock a blob row (`SELECT ... FOR UPDATE`) inside an existing
    /// transaction, returning it if it still exists.
    ///
    /// This is the fix for the delete-on-tombstone TOCTOU race
    /// (sinex-audit-cas-refcheck-toctou): `is_referenced_excluding_material`
    /// followed later by `delete_by_id` as two unguarded round-trips lets a
    /// concurrent writer create a brand-new live reference to the blob in
    /// between (e.g. `ContentStoreManager::check_dedup`'s dedup path
    /// registering a new `raw.source_material_registry` row with
    /// `optional_blob_id` pointing at this blob) -- that new reference is
    /// then silently orphaned by `optional_blob_id`'s `ON DELETE SET NULL`
    /// once the delete lands, even though the CAS bytes are already gone.
    ///
    /// Taking `FOR UPDATE` on the row here, and running the recheck plus the
    /// eventual `delete_by_id`/CAS-drop inside the same transaction, closes
    /// that window for the FK-backed reference: Postgres enforces
    /// `raw.source_material_registry`'s FK on `optional_blob_id` by taking an
    /// implicit `FOR KEY SHARE` lock on the referenced `core.blobs` row when
    /// a new material row is inserted, and `FOR KEY SHARE` conflicts with
    /// `FOR UPDATE`. A concurrent dedup insert therefore blocks until this
    /// transaction resolves; if this transaction goes on to delete the row,
    /// the blocked insert then fails with an explicit foreign-key violation
    /// instead of silently landing a reference to already-deleted content.
    ///
    /// `core.events`/`audit.archived_events.associated_blob_ids` are plain
    /// array columns with no FK, so this lock does not by itself serialize
    /// against a concurrent event carrying `associated_blob_ids` for this
    /// blob -- no production automaton sets that field today (verified via
    /// `rg associated_blob_ids` across `crate/sinexd/src/automata` and
    /// `crate/sinexd/src/sources`), so this closes the one live race; see
    /// the tracking bead for the array-column path.
    #[instrument(skip(self, executor))]
    pub async fn lock_by_id_for_update<'e, E>(
        &self,
        executor: E,
        id: Id<Blob>,
    ) -> DbResult<Option<Blob>>
    where
        E: Executor<'e, Database = Postgres>,
    {
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
            FOR UPDATE
            "#,
            id_uuid as _
        )
        .fetch_optional(executor)
        .await
        .map_err(|e| db_error(e, "lock blob for update"))?;

        result
            .map(|record| Self::decode_record(record, "lock blob for update"))
            .transpose()
    }

    /// List blob rows with no remaining source-material or event reference.
    ///
    /// This is the durable retry input for registry GC: if a process dies after
    /// removing a stale registry row but before cleaning its CAS/blob row, the
    /// next sweep rediscovers that orphan instead of leaking it permanently.
    #[instrument(skip(self))]
    pub async fn list_unreferenced_ids(&self, limit: i64) -> DbResult<Vec<Id<Blob>>> {
        let ids = sqlx::query_scalar!(
            r#"
            SELECT b.id AS "id!: uuid::Uuid"
            FROM core.blobs b
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.source_material_registry sm
                WHERE sm.optional_blob_id = b.id
            )
              AND NOT EXISTS (
                SELECT 1 FROM core.events e
                WHERE e.associated_blob_ids IS NOT NULL
                  AND e.associated_blob_ids @> ARRAY[b.id]::uuid[]
              )
              AND NOT EXISTS (
                SELECT 1 FROM audit.archived_events ae
                WHERE ae.associated_blob_ids IS NOT NULL
                  AND ae.associated_blob_ids @> ARRAY[b.id]::uuid[]
              )
            ORDER BY b.created_at ASC, b.id ASC
            LIMIT $1
            "#,
            limit,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| db_error(e, "list unreferenced blob ids"))?;
        Ok(ids.into_iter().map(Id::from_uuid).collect())
    }

    /// Delete a blob row by ID. Returns `true` if a row was actually removed.
    ///
    /// Caller is responsible for confirming the blob has zero remaining live
    /// references (see [`Self::is_referenced_excluding_material`]) and for
    /// dropping the associated CAS content separately -- this only removes
    /// the `core.blobs` row. Without this, delete-on-tombstone drops the CAS
    /// file but leaves a zombie row behind forever (sinex-audit-cas-zombie-blob-rows).
    #[instrument(skip(self))]
    pub async fn delete_by_id(&self, id: Id<Blob>) -> DbResult<bool> {
        self.delete_by_id_with_executor(&self.pool, id).await
    }

    /// Same as [`Self::delete_by_id`], but against a caller-supplied executor
    /// so it can run inside the same transaction that holds the
    /// [`Self::lock_by_id_for_update`] row lock.
    #[instrument(skip(self, executor))]
    pub async fn delete_by_id_with_executor<'e, E>(
        &self,
        executor: E,
        id: Id<Blob>,
    ) -> DbResult<bool>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let id_uuid = id.to_uuid();
        let result = sqlx::query!(
            r#"
            DELETE FROM core.blobs
            WHERE id = $1
            "#,
            id_uuid
        )
        .execute(executor)
        .await
        .map_err(|e| db_error(e, "delete blob by id"))?;

        Ok(result.rows_affected() > 0)
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

#[cfg(test)]
#[path = "blobs_test.rs"]
mod tests;
