use super::common::{DbResult, Repository, db_error};
use serde_json::Value;
use sinex_primitives::domain::OperationStatus;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

/// Upsert payload for the general live-session control state of a source mode.
///
/// This is the shared control plane for operator-driven enable/disable/pause of
/// any session-capable source (screen/audio capture today). Email keeps its
/// richer, domain-specialized `email_provider_state`; this table carries only
/// the control-plane columns common to every live session.
#[derive(Debug, Clone)]
pub struct SourceSessionStateUpsert {
    pub source_id: String,
    pub mode_id: String,
    pub session_scope: String,
    pub operation_id: Uuid,
    pub result_status: OperationStatus,
    /// `enabled` | `disabled` | `paused`.
    pub lifecycle_state: String,
    /// e.g. `visible_capture_active` | `idle` | `suspended`.
    pub visibility_state: String,
    pub private_mode_blocked: bool,
    pub runtime_state_ref: String,
    pub coverage_ref: String,
    pub debt_ref: String,
    pub requested_by: Option<String>,
    pub reason: Option<String>,
    pub detail: Value,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SourceSessionStateRecord {
    pub id: Uuid,
    pub source_id: String,
    pub mode_id: String,
    pub session_scope: String,
    pub operation_id: Uuid,
    pub result_status: OperationStatus,
    pub lifecycle_state: String,
    pub visibility_state: String,
    pub private_mode_blocked: bool,
    pub runtime_state_ref: String,
    pub coverage_ref: String,
    pub debt_ref: String,
    pub requested_by: Option<String>,
    pub reason: Option<String>,
    pub detail: Value,
    pub observed_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

pub struct SourceSessionStateRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for SourceSessionStateRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl SourceSessionStateRepository<'_> {
    pub async fn upsert(
        &self,
        state: SourceSessionStateUpsert,
    ) -> DbResult<SourceSessionStateRecord> {
        sqlx::query_as!(
            SourceSessionStateRecord,
            r#"
            INSERT INTO core.source_session_state (
                source_id,
                mode_id,
                session_scope,
                operation_id,
                result_status,
                lifecycle_state,
                visibility_state,
                private_mode_blocked,
                runtime_state_ref,
                coverage_ref,
                debt_ref,
                requested_by,
                reason,
                detail,
                observed_at
            )
            VALUES (
                $1, $2, $3, $4::uuid, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, NOW()
            )
            ON CONFLICT (source_id, mode_id, session_scope)
            DO UPDATE SET
                operation_id = EXCLUDED.operation_id,
                result_status = EXCLUDED.result_status,
                lifecycle_state = EXCLUDED.lifecycle_state,
                visibility_state = EXCLUDED.visibility_state,
                private_mode_blocked = EXCLUDED.private_mode_blocked,
                runtime_state_ref = EXCLUDED.runtime_state_ref,
                coverage_ref = EXCLUDED.coverage_ref,
                debt_ref = EXCLUDED.debt_ref,
                requested_by = EXCLUDED.requested_by,
                reason = EXCLUDED.reason,
                detail = EXCLUDED.detail,
                observed_at = EXCLUDED.observed_at
            RETURNING
                id,
                source_id,
                mode_id,
                session_scope,
                operation_id,
                result_status as "result_status!: OperationStatus",
                lifecycle_state,
                visibility_state,
                private_mode_blocked,
                runtime_state_ref,
                coverage_ref,
                debt_ref,
                requested_by,
                reason,
                detail,
                observed_at,
                updated_at
            "#,
            state.source_id,
            state.mode_id,
            state.session_scope,
            state.operation_id,
            state.result_status.to_string(),
            state.lifecycle_state,
            state.visibility_state,
            state.private_mode_blocked,
            state.runtime_state_ref,
            state.coverage_ref,
            state.debt_ref,
            state.requested_by,
            state.reason,
            state.detail
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "upsert source session state"))
    }

    pub async fn list_current_by_source(
        &self,
        source_id: &str,
    ) -> DbResult<Vec<SourceSessionStateRecord>> {
        sqlx::query_as!(
            SourceSessionStateRecord,
            r#"
            SELECT
                id,
                source_id,
                mode_id,
                session_scope,
                operation_id,
                result_status as "result_status!: OperationStatus",
                lifecycle_state,
                visibility_state,
                private_mode_blocked,
                runtime_state_ref,
                coverage_ref,
                debt_ref,
                requested_by,
                reason,
                detail,
                observed_at,
                updated_at
            FROM core.source_session_state
            WHERE source_id = $1
            ORDER BY mode_id, session_scope, observed_at DESC
            "#,
            source_id
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list source session state"))
    }

    /// Resolve the current control state for one specific session scope.
    pub async fn current_for_scope(
        &self,
        source_id: &str,
        mode_id: &str,
        session_scope: &str,
    ) -> DbResult<Option<SourceSessionStateRecord>> {
        sqlx::query_as!(
            SourceSessionStateRecord,
            r#"
            SELECT
                id,
                source_id,
                mode_id,
                session_scope,
                operation_id,
                result_status as "result_status!: OperationStatus",
                lifecycle_state,
                visibility_state,
                private_mode_blocked,
                runtime_state_ref,
                coverage_ref,
                debt_ref,
                requested_by,
                reason,
                detail,
                observed_at,
                updated_at
            FROM core.source_session_state
            WHERE source_id = $1 AND mode_id = $2 AND session_scope = $3
            "#,
            source_id,
            mode_id,
            session_scope
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|error| db_error(error, "current source session state"))
    }
}
