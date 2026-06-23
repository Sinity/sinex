use super::common::{DbResult, Repository, db_error};
use serde_json::Value;
use sinex_primitives::domain::OperationStatus;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct EmailProviderStateUpsert {
    pub source_id: String,
    pub mode_id: String,
    pub provider: String,
    pub account_binding_ref: String,
    pub mailbox_scope: String,
    pub operation_id: Uuid,
    pub result_status: OperationStatus,
    pub auth_state: String,
    pub network_state: String,
    pub sync_state: String,
    pub rate_limit_state: Option<String>,
    pub runtime_state_ref: String,
    pub coverage_ref: String,
    pub debt_ref: String,
    pub cursor_kind: Option<String>,
    pub cursor_value: Option<String>,
    pub continuity_state: Option<String>,
    pub provider_runtime: Value,
    pub provider_cursor: Option<Value>,
    pub provider_failure: Option<Value>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EmailProviderStateRecord {
    pub id: Uuid,
    pub source_id: String,
    pub mode_id: String,
    pub provider: String,
    pub account_binding_ref: String,
    pub mailbox_scope: String,
    pub operation_id: Uuid,
    pub result_status: OperationStatus,
    pub auth_state: String,
    pub network_state: String,
    pub sync_state: String,
    pub rate_limit_state: Option<String>,
    pub runtime_state_ref: String,
    pub coverage_ref: String,
    pub debt_ref: String,
    pub cursor_kind: Option<String>,
    pub cursor_value: Option<String>,
    pub continuity_state: Option<String>,
    pub provider_runtime: Value,
    pub provider_cursor: Option<Value>,
    pub provider_failure: Option<Value>,
    pub observed_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

pub struct EmailProviderStateRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for EmailProviderStateRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl EmailProviderStateRepository<'_> {
    pub async fn upsert(
        &self,
        state: EmailProviderStateUpsert,
    ) -> DbResult<EmailProviderStateRecord> {
        sqlx::query_as!(
            EmailProviderStateRecord,
            r#"
            INSERT INTO core.email_provider_state (
                source_id,
                mode_id,
                provider,
                account_binding_ref,
                mailbox_scope,
                operation_id,
                result_status,
                auth_state,
                network_state,
                sync_state,
                rate_limit_state,
                runtime_state_ref,
                coverage_ref,
                debt_ref,
                cursor_kind,
                cursor_value,
                continuity_state,
                provider_runtime,
                provider_cursor,
                provider_failure,
                observed_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6::uuid, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, NOW()
            )
            ON CONFLICT (source_id, mode_id, provider, account_binding_ref, mailbox_scope)
            DO UPDATE SET
                operation_id = EXCLUDED.operation_id,
                result_status = EXCLUDED.result_status,
                auth_state = EXCLUDED.auth_state,
                network_state = EXCLUDED.network_state,
                sync_state = EXCLUDED.sync_state,
                rate_limit_state = EXCLUDED.rate_limit_state,
                runtime_state_ref = EXCLUDED.runtime_state_ref,
                coverage_ref = EXCLUDED.coverage_ref,
                debt_ref = EXCLUDED.debt_ref,
                cursor_kind = EXCLUDED.cursor_kind,
                cursor_value = EXCLUDED.cursor_value,
                continuity_state = EXCLUDED.continuity_state,
                provider_runtime = EXCLUDED.provider_runtime,
                provider_cursor = EXCLUDED.provider_cursor,
                provider_failure = EXCLUDED.provider_failure,
                observed_at = EXCLUDED.observed_at
            RETURNING
                id,
                source_id,
                mode_id,
                provider,
                account_binding_ref,
                mailbox_scope,
                operation_id,
                result_status as "result_status!: OperationStatus",
                auth_state,
                network_state,
                sync_state,
                rate_limit_state,
                runtime_state_ref,
                coverage_ref,
                debt_ref,
                cursor_kind,
                cursor_value,
                continuity_state,
                provider_runtime,
                provider_cursor,
                provider_failure,
                observed_at,
                updated_at
            "#,
            state.source_id,
            state.mode_id,
            state.provider,
            state.account_binding_ref,
            state.mailbox_scope,
            state.operation_id,
            state.result_status.to_string(),
            state.auth_state,
            state.network_state,
            state.sync_state,
            state.rate_limit_state,
            state.runtime_state_ref,
            state.coverage_ref,
            state.debt_ref,
            state.cursor_kind,
            state.cursor_value,
            state.continuity_state,
            state.provider_runtime,
            state.provider_cursor,
            state.provider_failure
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "upsert email provider state"))
    }

    pub async fn list_current_by_source(
        &self,
        source_id: &str,
    ) -> DbResult<Vec<EmailProviderStateRecord>> {
        sqlx::query_as!(
            EmailProviderStateRecord,
            r#"
            SELECT
                id,
                source_id,
                mode_id,
                provider,
                account_binding_ref,
                mailbox_scope,
                operation_id,
                result_status as "result_status!: OperationStatus",
                auth_state,
                network_state,
                sync_state,
                rate_limit_state,
                runtime_state_ref,
                coverage_ref,
                debt_ref,
                cursor_kind,
                cursor_value,
                continuity_state,
                provider_runtime,
                provider_cursor,
                provider_failure,
                observed_at,
                updated_at
            FROM core.email_provider_state
            WHERE source_id = $1
            ORDER BY mode_id, observed_at DESC, provider, account_binding_ref, mailbox_scope
            "#,
            source_id
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list email provider state"))
    }
}
