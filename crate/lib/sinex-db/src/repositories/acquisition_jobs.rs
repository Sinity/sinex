//! Acquisition job repository for managing declared source-material acquisition work.
//!
//! CRUD operations for `raw.acquisition_jobs`. Each job records what to acquire,
//! how to acquire it, and the current state of that acquisition.

use super::common::{DbResult, Repository, db_error};
use sinex_primitives::error::SinexError;
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use uuid::Uuid;

/// A row from `raw.acquisition_jobs`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AcquisitionJobRow {
    pub id: Uuid,
    pub source_binding_id: Uuid,
    pub source_identifier: String,
    pub acquisition_mode: String,
    pub input_shape: String,
    pub parser_binding_id: Option<Uuid>,
    pub material_format_hint: Option<String>,
    pub timing_policy: serde_json::Value,
    pub raw_material_policy: serde_json::Value,
    pub cursor_state: serde_json::Value,
    pub status: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub material_id: Option<Uuid>,
    pub material_staged_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

pub struct AcquisitionJobRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for AcquisitionJobRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl AcquisitionJobRepository<'_> {
    /// List all acquisition jobs, ordered by creation time.
    pub async fn list_jobs(&self) -> DbResult<Vec<AcquisitionJobRow>> {
        sqlx::query_as::<_, AcquisitionJobRow>(
            r#"
            SELECT
                id,
                source_binding_id,
                source_identifier,
                acquisition_mode,
                input_shape,
                parser_binding_id,
                material_format_hint,
                timing_policy,
                raw_material_policy,
                cursor_state,
                status,
                attempts,
                last_error,
                started_at,
                completed_at,
                material_id,
                material_staged_at,
                created_at,
                updated_at
            FROM raw.acquisition_jobs
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list acquisition jobs"))
    }

    /// Get a single acquisition job by ID.
    pub async fn get_job_by_id(&self, id: Uuid) -> DbResult<Option<AcquisitionJobRow>> {
        sqlx::query_as::<_, AcquisitionJobRow>(
            r#"
            SELECT
                id,
                source_binding_id,
                source_identifier,
                acquisition_mode,
                input_shape,
                parser_binding_id,
                material_format_hint,
                timing_policy,
                raw_material_policy,
                cursor_state,
                status,
                attempts,
                last_error,
                started_at,
                completed_at,
                material_id,
                material_staged_at,
                created_at,
                updated_at
            FROM raw.acquisition_jobs
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get acquisition job by id"))
    }

    /// List acquisition jobs filtered by status.
    pub async fn list_jobs_by_status(&self, status: &str) -> DbResult<Vec<AcquisitionJobRow>> {
        sqlx::query_as::<_, AcquisitionJobRow>(
            r#"
            SELECT
                id,
                source_binding_id,
                source_identifier,
                acquisition_mode,
                input_shape,
                parser_binding_id,
                material_format_hint,
                timing_policy,
                raw_material_policy,
                cursor_state,
                status,
                attempts,
                last_error,
                started_at,
                completed_at,
                material_id,
                material_staged_at,
                created_at,
                updated_at
            FROM raw.acquisition_jobs
            WHERE status = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(status)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list acquisition jobs by status"))
    }

    /// List acquisition jobs for a specific source binding.
    pub async fn list_jobs_by_binding(
        &self,
        source_binding_id: Uuid,
    ) -> DbResult<Vec<AcquisitionJobRow>> {
        sqlx::query_as::<_, AcquisitionJobRow>(
            r#"
            SELECT
                id,
                source_binding_id,
                source_identifier,
                acquisition_mode,
                input_shape,
                parser_binding_id,
                material_format_hint,
                timing_policy,
                raw_material_policy,
                cursor_state,
                status,
                attempts,
                last_error,
                started_at,
                completed_at,
                material_id,
                material_staged_at,
                created_at,
                updated_at
            FROM raw.acquisition_jobs
            WHERE source_binding_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(source_binding_id)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list acquisition jobs by binding"))
    }

    /// Create a new acquisition job.
    ///
    /// Returns the generated UUID.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_job(
        &self,
        source_binding_id: Uuid,
        source_identifier: &str,
        acquisition_mode: &str,
        input_shape: &str,
        parser_binding_id: Option<Uuid>,
        material_format_hint: Option<&str>,
        timing_policy: &serde_json::Value,
        raw_material_policy: &serde_json::Value,
    ) -> DbResult<Uuid> {
        let row: (Uuid,) = sqlx::query_as(
            r#"
            INSERT INTO raw.acquisition_jobs
                (source_binding_id, source_identifier, acquisition_mode, input_shape,
                 parser_binding_id, material_format_hint, timing_policy, raw_material_policy)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            "#,
        )
        .bind(source_binding_id)
        .bind(source_identifier)
        .bind(acquisition_mode)
        .bind(input_shape)
        .bind(parser_binding_id)
        .bind(material_format_hint)
        .bind(timing_policy)
        .bind(raw_material_policy)
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "create acquisition job"))?;

        Ok(row.0)
    }

    /// Update the status of an acquisition job.
    pub async fn update_job_status(
        &self,
        id: Uuid,
        status: &str,
        last_error: Option<&str>,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.acquisition_jobs
            SET status = $2,
                last_error = $3,
                attempts = CASE WHEN $2 = 'running' THEN attempts + 1 ELSE attempts END,
                started_at = CASE WHEN $2 = 'running' AND started_at IS NULL THEN now() ELSE started_at END,
                completed_at = CASE WHEN $2 IN ('completed', 'failed', 'cancelled', 'drained') THEN now() ELSE completed_at END,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(status)
        .bind(last_error)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update acquisition job status"))?;

        Ok(())
    }

    /// Update the cursor state for a running job.
    pub async fn update_cursor_state(
        &self,
        id: Uuid,
        cursor_state: &serde_json::Value,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.acquisition_jobs
            SET cursor_state = $2,
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(cursor_state)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update acquisition job cursor"))?;

        Ok(())
    }

    /// Record completion with material staging evidence.
    pub async fn record_material_staged(
        &self,
        id: Uuid,
        material_id: Uuid,
        status: &str,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.acquisition_jobs
            SET material_id = $2,
                material_staged_at = now(),
                status = $3,
                completed_at = now(),
                updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(material_id)
        .bind(status)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "record acquisition job material staged"))?;

        Ok(())
    }
}
