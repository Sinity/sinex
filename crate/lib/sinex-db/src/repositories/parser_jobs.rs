//! Parser job repository for managing finite parser job execution.
//!
//! CRUD and lease operations for `raw.parser_jobs`. Each row tracks the
//! lifecycle of a parse job: one source material parsed by one parser
//! version. Workers claim jobs with `FOR UPDATE SKIP LOCKED`, complete
//! them, or record failures for retry.

use super::common::{DbResult, Repository, db_error};
use sinex_primitives::domain::OperationRunStatus;
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use uuid::Uuid;

/// A typed row from `raw.parser_jobs` with the status decoded as
/// [`OperationRunStatus`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ParserJobRow {
    pub id: Uuid,
    pub source_material_id: Uuid,
    pub source_binding_id: Option<Uuid>,
    pub source_unit_id: String,
    pub parser_id: String,
    pub parser_version: String,
    pub input_shape_kind: String,
    pub status: String,
    pub cursor: Option<serde_json::Value>,
    pub high_watermark: Option<serde_json::Value>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<Timestamp>,
    pub operation_id: Option<Uuid>,
    pub timing_policy: serde_json::Value,
    pub error_class: Option<String>,
    pub error_summary: Option<String>,
    pub queued_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub updated_at: Timestamp,
}

impl ParserJobRow {
    /// Decode the `status` field as a typed [`OperationRunStatus`].
    ///
    /// # Panics
    ///
    /// Panics if the status value in the database is not a valid
    /// `OperationRunStatus` variant. This should be impossible due to
    /// the CHECK constraint on the column.
    #[must_use]
    pub fn status_typed(&self) -> OperationRunStatus {
        self.status
            .parse()
            .expect("invalid parser job status in database")
    }
}

pub struct ParserJobRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for ParserJobRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl ParserJobRepository<'_> {
    /// Get a single parser job by ID.
    pub async fn get_job_by_id(&self, id: Uuid) -> DbResult<Option<ParserJobRow>> {
        sqlx::query_as::<_, ParserJobRow>(
            r#"
            SELECT
                id, source_material_id, source_binding_id, source_unit_id,
                parser_id, parser_version, input_shape_kind, status,
                cursor, high_watermark, attempts, max_attempts,
                lease_owner, lease_expires_at, operation_id,
                timing_policy, error_class, error_summary,
                queued_at, started_at, completed_at, updated_at
            FROM raw.parser_jobs
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get parser job by id"))
    }

    /// List parser jobs filtered by status.
    pub async fn list_jobs_by_status(
        &self,
        status: OperationRunStatus,
    ) -> DbResult<Vec<ParserJobRow>> {
        let status_str = status.as_str();
        sqlx::query_as::<_, ParserJobRow>(
            r#"
            SELECT
                id, source_material_id, source_binding_id, source_unit_id,
                parser_id, parser_version, input_shape_kind, status,
                cursor, high_watermark, attempts, max_attempts,
                lease_owner, lease_expires_at, operation_id,
                timing_policy, error_class, error_summary,
                queued_at, started_at, completed_at, updated_at
            FROM raw.parser_jobs
            WHERE status = $1
            ORDER BY queued_at ASC
            "#,
        )
        .bind(status_str)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list parser jobs by status"))
    }

    /// List parser jobs for a specific source material.
    pub async fn list_jobs_by_material(
        &self,
        source_material_id: Uuid,
    ) -> DbResult<Vec<ParserJobRow>> {
        sqlx::query_as::<_, ParserJobRow>(
            r#"
            SELECT
                id, source_material_id, source_binding_id, source_unit_id,
                parser_id, parser_version, input_shape_kind, status,
                cursor, high_watermark, attempts, max_attempts,
                lease_owner, lease_expires_at, operation_id,
                timing_policy, error_class, error_summary,
                queued_at, started_at, completed_at, updated_at
            FROM raw.parser_jobs
            WHERE source_material_id = $1
            ORDER BY queued_at ASC
            "#,
        )
        .bind(source_material_id)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list parser jobs by material"))
    }

    /// Claim the next queued job for a worker using `FOR UPDATE SKIP LOCKED`.
    ///
    /// This implements a lock-free worker queue: multiple workers can call
    /// this concurrently without contention. Each call claims a different
    /// row. Returns `None` when no queued jobs are available.
    ///
    /// The returned job is still in `queued` status — the caller should
    /// immediately call [`lease_job`](Self::lease_job) to transition to
    /// `leased`.
    pub async fn claim_next_job(
        &self,
        lease_owner: &str,
        lease_ttl_seconds: i32,
    ) -> DbResult<Option<ParserJobRow>> {
        let mut tx = self.pool.begin().await.map_err(|e| db_error(e, "begin claim tx"))?;

        let row: Option<ParserJobRow> = sqlx::query_as::<_, ParserJobRow>(
            r#"
            SELECT
                id, source_material_id, source_binding_id, source_unit_id,
                parser_id, parser_version, input_shape_kind, status,
                cursor, high_watermark, attempts, max_attempts,
                lease_owner, lease_expires_at, operation_id,
                timing_policy, error_class, error_summary,
                queued_at, started_at, completed_at, updated_at
            FROM raw.parser_jobs
            WHERE status IN ('queued', 'retry_wait')
            ORDER BY queued_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| db_error(e, "claim next parser job"))?;

        let Some(job) = row else {
            tx.commit()
                .await
                .map_err(|e| db_error(e, "commit empty claim"))?;
            return Ok(None);
        };

        // Atomically transition to leased within the same transaction.
        sqlx::query(
            r#"
            UPDATE raw.parser_jobs
            SET status = 'leased',
                lease_owner = $2,
                lease_expires_at = now() + ($3 || ' seconds')::interval,
                attempts = attempts + 1,
                started_at = CASE WHEN started_at IS NULL THEN now() ELSE started_at END,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(job.id)
        .bind(lease_owner)
        .bind(lease_ttl_seconds)
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e, "lease claimed parser job"))?;

        tx.commit()
            .await
            .map_err(|e| db_error(e, "commit claim"))?;

        // Re-fetch to get the updated row.
        self.get_job_by_id(job.id).await
    }

    /// Transition a job to `running` from `leased`.
    pub async fn lease_job(
        &self,
        id: Uuid,
        lease_owner: &str,
        lease_ttl_seconds: i32,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.parser_jobs
            SET status = 'leased',
                lease_owner = $2,
                lease_expires_at = now() + ($3 || ' seconds')::interval,
                attempts = attempts + 1,
                started_at = CASE WHEN started_at IS NULL THEN now() ELSE started_at END,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(lease_owner)
        .bind(lease_ttl_seconds)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "lease parser job"))?;

        Ok(())
    }

    /// Transition a job to `running` from `leased`.
    pub async fn start_job(&self, id: Uuid) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.parser_jobs
            SET status = 'running',
                started_at = CASE WHEN started_at IS NULL THEN now() ELSE started_at END,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "start parser job"))?;

        Ok(())
    }

    /// Mark a job as completed successfully.
    pub async fn complete_job(&self, id: Uuid) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.parser_jobs
            SET status = 'completed',
                completed_at = now(),
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "complete parser job"))?;

        Ok(())
    }

    /// Mark a job as completed with caveats (partial success, warnings).
    pub async fn complete_job_with_caveats(
        &self,
        id: Uuid,
        error_summary: Option<&str>,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.parser_jobs
            SET status = 'completed_with_caveats',
                error_summary = $2,
                completed_at = now(),
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(error_summary)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "complete parser job with caveats"))?;

        Ok(())
    }

    /// Mark a job as failed, determining whether it should be retried
    /// based on attempt count vs max_attempts.
    ///
    /// If `attempts < max_attempts`, the job transitions to `failed_retryable`
    /// and will be re-claimed by `claim_next_job` (which includes `retry_wait`).
    /// If `attempts >= max_attempts`, it transitions to `failed_permanent`.
    pub async fn fail_job(
        &self,
        id: Uuid,
        error_class: &str,
        error_summary: &str,
        max_attempts_override: Option<i32>,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.parser_jobs
            SET status = CASE
                    WHEN $2 IS NOT NULL AND attempts >= $2 THEN 'failed_permanent'
                    WHEN attempts >= max_attempts THEN 'failed_permanent'
                    ELSE 'retry_wait'
                END,
                error_class = $3,
                error_summary = $4,
                completed_at = CASE
                    WHEN $2 IS NOT NULL AND attempts >= $2 THEN now()
                    WHEN attempts >= max_attempts THEN now()
                    ELSE completed_at
                END,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(max_attempts_override)
        .bind(error_class)
        .bind(error_summary)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "fail parser job"))?;

        Ok(())
    }

    /// Update the cursor position for a running job.
    ///
    /// The cursor tracks progress through the source material so that
    /// restarted jobs can resume from where they left off.
    pub async fn update_cursor(
        &self,
        id: Uuid,
        cursor: &serde_json::Value,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.parser_jobs
            SET cursor = $2,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(cursor)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update parser job cursor"))?;

        Ok(())
    }

    /// Cancel a job that is still pending or leaseable.
    pub async fn cancel_job(&self, id: Uuid) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.parser_jobs
            SET status = 'cancelled',
                completed_at = now(),
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "cancel parser job"))?;

        Ok(())
    }
}
