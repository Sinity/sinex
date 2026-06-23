//! Tombstone operation persistence.
//!
//! Tombstone lifecycle state is stored in `operations_log` with
//! `operation_type = "tombstone"` and the full workflow state serialized into
//! the operation scope JSONB.

use super::{Operation, OperationRecord, StateRepository};
use crate::models::Event;
use crate::repositories::common::{DbResult, db_error};
use crate::{Id, JsonValue};
use sinex_primitives::Timestamp;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::error::SinexError;
use sinex_primitives::rpc::lifecycle::{TombstoneOperation, TombstoneOperationState};
use std::str::FromStr;
use uuid::Uuid;

impl StateRepository<'_> {
    fn parse_tombstone_scope(
        operation_id: &str,
        scope: Option<JsonValue>,
    ) -> DbResult<TombstoneOperation> {
        let scope = scope.ok_or_else(|| {
            SinexError::invalid_state(format!(
                "Tombstone operation {operation_id} is missing scope"
            ))
        })?;
        serde_json::from_value(scope).map_err(|error| {
            SinexError::invalid_state(format!(
                "Failed to deserialize tombstone operation {operation_id}: {error}"
            ))
        })
    }

    fn tombstone_operation_duration_ms(
        operation: &TombstoneOperation,
        finished_at: Timestamp,
    ) -> DbResult<Option<i32>> {
        let created_at = Timestamp::parse_rfc3339(&operation.created_at).map_err(|error| {
            SinexError::invalid_state(format!(
                "Tombstone operation {} has invalid created_at '{}': {error}",
                operation.operation_id, operation.created_at
            ))
        })?;
        let elapsed_ms = (finished_at - created_at).whole_milliseconds();
        if elapsed_ms < 0 {
            return Err(SinexError::invalid_state(format!(
                "Tombstone operation {} finished before its created_at timestamp",
                operation.operation_id
            )));
        }
        let duration_ms = i32::try_from(elapsed_ms).map_err(|_| {
            SinexError::invalid_state(format!(
                "Tombstone operation {} duration overflowed i32 milliseconds",
                operation.operation_id
            ))
        })?;
        Ok(Some(duration_ms))
    }

    fn tombstone_preview_summary_with_message(
        preview_summary: Option<JsonValue>,
        message: &str,
    ) -> Option<JsonValue> {
        let mut preview_summary = preview_summary?;
        if let Some(object) = preview_summary.as_object_mut() {
            object.insert(
                "message".to_string(),
                JsonValue::String(message.to_string()),
            );
        }
        Some(preview_summary)
    }

    /// Create a new tombstone operation record.
    ///
    /// The full `TombstoneOperation` is serialized into the `scope` field,
    /// with `result_status` tracking the operation state.
    pub async fn create_tombstone_operation(
        &self,
        operation_id: &str,
        operator: &str,
        scope: JsonValue,
        preview_summary: JsonValue,
    ) -> DbResult<OperationRecord> {
        let operation_uuid = Uuid::from_str(operation_id)
            .map_err(|_| SinexError::validation(format!("Invalid operation ID: {operation_id}")))?;
        let id = Id::<Operation>::from_uuid(operation_uuid);

        let record = sqlx::query_as!(
            OperationRecord,
            r#"
            INSERT INTO core.operations_log (
                id, operation_type, operator, scope, result_status, preview_summary
            ) VALUES (
                $1::uuid, 'tombstone', $2, $3, 'running', $4
            )
            RETURNING
                id as "id!: Id<Operation>",
                operation_type,
                operator,
                scope,
                result_status,
                result_message,
                preview_summary,
                duration_ms
            "#,
            id.to_uuid(),
            operator,
            scope,
            preview_summary,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "create tombstone operation"))?;

        Ok(record)
    }

    /// Get a tombstone operation by ID.
    pub async fn get_tombstone_operation(
        &self,
        operation_id: &str,
    ) -> DbResult<Option<OperationRecord>> {
        let operation_uuid = Uuid::from_str(operation_id)
            .map_err(|_| SinexError::validation(format!("Invalid operation ID: {operation_id}")))?;
        let id = Id::<Operation>::from_uuid(operation_uuid);

        sqlx::query_as!(
            OperationRecord,
            r#"
            SELECT
                id as "id!: Id<Operation>",
                operation_type,
                operator,
                scope,
                result_status,
                result_message,
                preview_summary,
                duration_ms
            FROM core.operations_log
            WHERE id = $1::uuid AND operation_type = 'tombstone'
            "#,
            id.to_uuid()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get tombstone operation"))
    }

    /// Update a tombstone operation's status and scope.
    pub async fn update_tombstone_operation(
        &self,
        operation_id: &str,
        result_status: OperationStatus,
        scope: JsonValue,
        preview_summary: Option<JsonValue>,
        result_message: Option<&str>,
        duration_ms: Option<i32>,
    ) -> DbResult<()> {
        let operation_uuid = Uuid::from_str(operation_id)
            .map_err(|_| SinexError::validation(format!("Invalid operation ID: {operation_id}")))?;
        let id = Id::<Operation>::from_uuid(operation_uuid);

        let result = sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                scope = $3,
                preview_summary = COALESCE($4, preview_summary),
                result_message = COALESCE($5, result_message),
                duration_ms = COALESCE($6, duration_ms)
            WHERE id = $1::uuid AND operation_type = 'tombstone'
            "#,
            id.to_uuid(),
            result_status.to_string(),
            scope,
            preview_summary,
            result_message,
            duration_ms,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update tombstone operation"))?;
        if result.rows_affected() == 0 {
            return Err(SinexError::not_found(format!(
                "Tombstone operation not found: {operation_id}"
            )));
        }

        Ok(())
    }

    /// Cancel a tombstone operation while keeping persisted scope state honest.
    pub async fn cancel_tombstone_operation(
        &self,
        operation_id: &str,
        reason: Option<&str>,
    ) -> DbResult<OperationRecord> {
        let record = self
            .get_tombstone_operation(operation_id)
            .await?
            .ok_or_else(|| {
                SinexError::not_found(format!("Tombstone operation not found: {operation_id}"))
            })?;

        let mut operation = Self::parse_tombstone_scope(operation_id, record.scope.clone())?;
        let now = Timestamp::now();
        if !operation.state.is_terminal()
            && let Ok(expires_at) = Timestamp::parse_rfc3339(&operation.expires_at)
            && now > expires_at
        {
            operation.state = TombstoneOperationState::Expired;
            operation.phase = operation.state.into();
            operation.finished_at = Some(now.format_rfc3339());
            operation.error_details = Some("Expired before approval".to_string());

            self.update_tombstone_operation(
                operation_id,
                OperationStatus::Cancelled,
                serde_json::to_value(&operation)?,
                Self::tombstone_preview_summary_with_message(
                    record.preview_summary.clone(),
                    "Tombstone operation expired",
                ),
                Some("Tombstone operation expired"),
                Self::tombstone_operation_duration_ms(&operation, now)?,
            )
            .await?;

            return Err(SinexError::invalid_state(format!(
                "Tombstone operation {operation_id} has expired"
            )));
        }
        if !operation.state.is_cancellable() {
            return Err(SinexError::invalid_state(format!(
                "Operation cannot be cancelled (state: {:?})",
                operation.state
            )));
        }

        operation.state = TombstoneOperationState::Cancelled;
        operation.phase = operation.state.into();
        let finished_at = now;
        operation.finished_at = Some(finished_at.format_rfc3339());
        operation.error_details = reason.map(|reason| format!("Cancelled: {reason}"));

        self.update_tombstone_operation(
            operation_id,
            OperationStatus::Cancelled,
            serde_json::to_value(&operation)?,
            record.preview_summary,
            Some("Tombstone operation cancelled"),
            Self::tombstone_operation_duration_ms(&operation, finished_at)?,
        )
        .await?;

        self.get_tombstone_operation(operation_id)
            .await?
            .ok_or_else(|| SinexError::database("tombstone operation disappeared after cancel"))
    }

    /// Count how many archived rows currently exist for the given event IDs.
    pub async fn count_archived_event_ids(&self, event_ids: &[Id<Event>]) -> DbResult<i64> {
        if event_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = event_ids
            .iter()
            .map(|event_id| *event_id.as_uuid())
            .collect();
        sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint as "count!" FROM audit.archived_events WHERE id = ANY($1::uuid[])"#,
            &ids
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count archived event ids"))
    }

    /// List tombstone operations with canonical filtering on persisted scope phase.
    pub async fn list_tombstone_operations(
        &self,
        state: Option<TombstoneOperationState>,
        limit: i64,
    ) -> DbResult<Vec<OperationRecord>> {
        let phase = state
            .map(serde_json::to_value)
            .transpose()?
            .and_then(|value| value.as_str().map(str::to_string));

        let mut qb = sqlx::QueryBuilder::new(
            r"
            SELECT
                id,
                operation_type,
                operator,
                scope,
                result_status,
                result_message,
                preview_summary,
                duration_ms
            FROM core.operations_log
            WHERE operation_type = 'tombstone'
            ",
        );

        if let Some(phase) = phase {
            qb.push(" AND COALESCE(scope->>'phase', LOWER(scope->>'state')) = ");
            qb.push_bind(phase);
        }

        qb.push(" ORDER BY id DESC LIMIT ");
        qb.push_bind(limit);

        qb.build_query_as::<OperationRecord>()
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "list tombstone operations"))
    }
}
