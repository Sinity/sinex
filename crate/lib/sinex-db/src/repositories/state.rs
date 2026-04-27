//! State repository for managing system state including checkpoints and operations log
//!
//! This repository combines management of:
//! - Node checkpoints (tracking progress of event processing)
//! - Operations log (audit trail of system operations)

use super::common::{DbResult, EnhancedRepository, Repository, db_error};
use crate::schema::OperationsLog;
use crate::{Id, JsonValue};
use crate::{IdempotentTransaction, RetryConfig, with_retry_transaction_idempotent};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::{NodeName, NodeState, NodeType, OperationStatus};
use sinex_primitives::error::SinexError;
use sinex_primitives::rpc::lifecycle::{TombstoneOperation, TombstoneOperationState};
use sinex_primitives::{Seconds, Timestamp};
use sqlx::postgres::types::PgRange;
use sqlx::{Executor, FromRow, PgPool, Postgres};
use std::ops::Bound;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

/// Database record for `operations_log` table
/// NOTE: The actual table only has: id, `operation_type`, operator, scope,
/// `result_status`, `result_message`, `preview_summary`, `duration_ms`
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: Id<Operation>,
    pub operation_type: String,
    pub operator: String,
    pub scope: Option<JsonValue>,
    pub result_status: OperationStatus,
    pub result_message: Option<String>,
    pub preview_summary: Option<JsonValue>,
    pub duration_ms: Option<i32>,
}

/// Operation log entry for creating operations
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct Operation {
    /// Operation ID - None when creating, Some when from DB
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(skip)]
    pub id: Option<Id<Operation>>,

    pub operation_type: String,
    pub operator: String,
    pub scope: Option<JsonValue>,
    pub result_status: OperationStatus,
    pub result_message: Option<String>,
    pub preview_summary: Option<JsonValue>,
    pub duration_ms: Option<i32>,
}

/// State repository combining checkpoints and operations
pub struct StateRepository<'a> {
    pool: &'a PgPool,
}

const DEFAULT_NODE_HEARTBEAT_STALE_SECS: Seconds = Seconds::from_secs(120);
const SQLSTATE_UNDEFINED_FUNCTION: &str = "42883";
const MANAGED_OPERATION_TYPES: &[&str] = &["replay", "archive", "restore", "purge", "tombstone"];

fn node_heartbeat_stale_after() -> DbResult<Duration> {
    match std::env::var("SINEX_NODE_HEARTBEAT_STALE_SECS") {
        Ok(raw) => {
            let value = raw.parse::<u64>().map_err(|error| {
                SinexError::configuration(
                    "SINEX_NODE_HEARTBEAT_STALE_SECS must be a positive integer",
                )
                .with_std_error(&error)
                .with_context("value", raw.clone())
            })?;

            if value == 0 {
                return Err(SinexError::configuration(
                    "SINEX_NODE_HEARTBEAT_STALE_SECS must be greater than zero",
                )
                .with_context("value", raw));
            }

            Ok(Duration::from_secs(value))
        }
        Err(std::env::VarError::NotPresent) => Ok(Duration::from_secs(
            DEFAULT_NODE_HEARTBEAT_STALE_SECS.as_secs(),
        )),
        Err(std::env::VarError::NotUnicode(_)) => Err(SinexError::configuration(
            "SINEX_NODE_HEARTBEAT_STALE_SECS must be valid UTF-8",
        )),
    }
}

fn probe_health<T>(result: DbResult<T>) -> (Option<T>, Option<String>) {
    match result {
        Ok(value) => (Some(value), None),
        Err(error) => {
            let message = error.to_string();
            (None, Some(message))
        }
    }
}

fn probe_health_bool(result: DbResult<bool>) -> (bool, Option<String>) {
    match result {
        Ok(value) => (value, None),
        Err(error) => (false, Some(error.to_string())),
    }
}

impl<'a> Repository<'a> for StateRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl<'a> EnhancedRepository<'a> for StateRepository<'a> {
    type Table = OperationsLog;
}

// Note: Removed TransactionSupport implementation due to lifetime complexity.
// Use the transaction methods directly on StateRepositoryTx instead.

impl StateRepository<'_> {
    // ===== Operations Log Helpers for Replay =====

    /// Start a replay operation via `core.start_operation` and return the operation Id
    pub async fn start_replay_operation(
        &self,
        operator: &str,
        scope: JsonValue,
        scope_window: Option<(Timestamp, Timestamp)>,
    ) -> DbResult<Id<Operation>> {
        let scope_window_range = scope_window.map(|(start, end)| {
            PgRange::from((Bound::Included(start.inner()), Bound::Included(end.inner())))
        });

        let op_uuid: Uuid = match sqlx::query_scalar!(
            r#"SELECT core.start_operation($1, $2, $3::jsonb, $4::tstzrange)::uuid as "id!: Uuid""#,
            "replay",
            operator,
            scope,
            scope_window_range
        )
        .fetch_one(self.pool)
        .await
        {
            Ok(uuid) => uuid,
            Err(e) => return Err(db_error(e, "start replay operation")),
        };
        let op_uuid_id = op_uuid;
        Ok(Id::<Operation>::from_uuid(op_uuid_id))
    }

    /// Update `result_status`, `result_message` and `preview_summary` for an operation
    pub async fn update_operation_meta(
        &self,
        id: &Id<Operation>,
        result_status: OperationStatus,
        result_message: Option<&str>,
        preview_summary: JsonValue,
    ) -> DbResult<()> {
        let result = sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                result_message = $3,
                preview_summary = $4
            WHERE id = $1::uuid
            "#,
            id.to_uuid(),
            result_status.to_string(),
            result_message,
            preview_summary
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update operation meta"))?;
        if result.rows_affected() == 0 {
            return Err(SinexError::not_found(format!(
                "Operation not found: {}",
                id.to_uuid()
            )));
        }
        Ok(())
    }

    /// Complete an operation via `core.complete_operation(summary)`
    pub async fn complete_operation(&self, id: &Id<Operation>, summary: JsonValue) -> DbResult<()> {
        let _ = sqlx::query_scalar!(
            r#"SELECT core.complete_operation($1::uuid, $2::jsonb) as result"#,
            id.to_uuid(),
            summary
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "complete operation"))?;
        Ok(())
    }

    /// Fail an operation via `core.fail_operation(error)`
    pub async fn fail_operation(&self, id: &Id<Operation>, error: JsonValue) -> DbResult<()> {
        let _ = sqlx::query_scalar!(
            r#"SELECT core.fail_operation($1::uuid, $2::jsonb) as result"#,
            id.to_uuid(),
            error
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "fail operation"))?;
        Ok(())
    }
    // ===== Validation Methods =====

    /// Validate an operation ID is not null/empty
    pub fn validate_operation_id(id: &Id<Operation>) -> DbResult<()> {
        // UUIDv7 IDs are always valid once created, but we can check for zero UUIDv7
        if id.to_uuid().into_bytes() == [0u8; 16] {
            return Err(SinexError::validation("Operation ID cannot be zero"));
        }
        Ok(())
    }

    fn validate_managed_operation_type(operation_type: &str) -> DbResult<()> {
        if MANAGED_OPERATION_TYPES.contains(&operation_type) {
            Ok(())
        } else {
            Err(SinexError::validation(format!(
                "Unsupported operation type '{operation_type}'. Allowed types: {}",
                MANAGED_OPERATION_TYPES.join(", ")
            )))
        }
    }

    fn validate_audit_operation_type(operation_type: &str) -> DbResult<()> {
        if operation_type.is_empty() {
            return Err(SinexError::validation("Operation type cannot be empty"));
        }

        let is_valid = operation_type
            .chars()
            .enumerate()
            .all(|(index, ch)| match index {
                0 => ch.is_ascii_lowercase(),
                _ => {
                    ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
                }
            });

        if is_valid {
            Ok(())
        } else {
            Err(SinexError::validation(format!(
                "Operation type '{operation_type}' must match ^[a-z][a-z0-9_.-]*$"
            )))
        }
    }

    /// Validate a `ReplayScope` JSON object
    pub fn validate_replay_scope(scope: &JsonValue) -> DbResult<()> {
        // Required fields for replay scope
        let obj = scope
            .as_object()
            .ok_or_else(|| SinexError::validation("ReplayScope must be a JSON object"))?;

        // Check required fields
        if !obj.contains_key("target_type") {
            return Err(SinexError::validation(
                "ReplayScope missing required field: target_type",
            ));
        }

        // Validate target_type is a string and one of allowed values
        if let Some(target_type) = obj.get("target_type").and_then(|v| v.as_str()) {
            match target_type {
                "event" | "time_range" | "cascade" | "operation" => {}
                _ => {
                    return Err(SinexError::validation(format!(
                        "Invalid target_type: {target_type}. Must be one of: event, time_range, cascade, operation"
                    )));
                }
            }

            // Validate type-specific fields
            match target_type {
                "event" => {
                    if !obj.contains_key("event_id") {
                        return Err(SinexError::validation("Event scope requires event_id"));
                    }
                }
                "time_range" => {
                    if !obj.contains_key("start_time") || !obj.contains_key("end_time") {
                        return Err(SinexError::validation(
                            "Time range scope requires start_time and end_time",
                        ));
                    }
                }
                "cascade" => {
                    if !obj.contains_key("root_event_id") {
                        return Err(SinexError::validation(
                            "Cascade scope requires root_event_id",
                        ));
                    }
                }
                "operation" => {
                    if !obj.contains_key("operation_id") {
                        return Err(SinexError::validation(
                            "Operation scope requires operation_id",
                        ));
                    }
                }
                _ => {}
            }
        } else {
            return Err(SinexError::validation("target_type must be a string"));
        }

        Ok(())
    }

    // ===== Operations Log Methods =====

    /// Log an operation
    pub async fn log_operation(&self, operation: Operation) -> DbResult<OperationRecord> {
        Self::validate_log_operation(&operation)?;

        let result = with_retry_transaction_idempotent(
            self.pool,
            RetryConfig::default(),
            IdempotentTransaction::new(),
            |tx| {
                let operation = operation.clone();
                Box::pin(
                    async move { Self::insert_operation_with_executor(&mut **tx, operation).await },
                )
            },
        )
        .await?;

        Ok(result)
    }

    /// Log an operation using an existing transaction/executor.
    pub async fn log_operation_with_executor<'e, E>(
        &self,
        executor: E,
        operation: Operation,
    ) -> DbResult<OperationRecord>
    where
        E: Executor<'e, Database = Postgres>,
    {
        Self::validate_log_operation(&operation)?;
        Self::insert_operation_with_executor(executor, operation).await
    }

    fn validate_log_operation(operation: &Operation) -> DbResult<()> {
        Self::validate_audit_operation_type(&operation.operation_type)?;
        if operation.operation_type == "replay"
            && let Some(ref scope) = operation.scope
        {
            Self::validate_replay_scope(scope)?;
        }
        Ok(())
    }

    async fn insert_operation_with_executor<'e, E>(
        executor: E,
        operation: Operation,
    ) -> DbResult<OperationRecord>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let id = Id::<Operation>::new();
        sqlx::query_as!(
            OperationRecord,
            r#"
            INSERT INTO core.operations_log (
                id, operation_type, operator, scope, result_status, result_message, preview_summary, duration_ms
            ) VALUES (
                $1::uuid, $2, $3, $4, $5, $6, $7, $8
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
            operation.operation_type,
            operation.operator,
            operation.scope,
            operation.result_status.to_string(),
            operation.result_message,
            operation.preview_summary,
            operation.duration_ms
        )
        .fetch_one(executor)
        .await
        .map_err(|e| db_error(e, "log operation"))
    }

    /// Check if an operation exists by ID (lightweight check without full record fetch)
    pub async fn operation_exists(&self, id: &Id<Operation>) -> DbResult<bool> {
        Self::validate_operation_id(id)?;
        let exists = sqlx::query_scalar!(
            r#"SELECT EXISTS(SELECT 1 FROM core.operations_log WHERE id = $1::uuid) as "exists!""#,
            id.to_uuid()
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "check operation exists"))?;

        Ok(exists)
    }

    /// Get operation by ID
    pub async fn get_operation(&self, id: &Id<Operation>) -> DbResult<Option<OperationRecord>> {
        Self::validate_operation_id(id)?;
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
            WHERE id = $1::uuid
            "#,
            id.to_uuid()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get operation"))
    }

    /// Get recent operations
    pub async fn get_recent_operations(&self, limit: i64) -> DbResult<Vec<OperationRecord>> {
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
            ORDER BY id DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent operations"))
    }

    /// Get operations by operator and scope
    pub async fn get_operations_by_actor_and_scope(
        &self,
        operator: Option<&str>,
        scope_filter: Option<JsonValue>,
        limit: Option<i64>,
    ) -> DbResult<Vec<OperationRecord>> {
        let limit = limit.unwrap_or(100);

        let mut query_builder = sqlx::QueryBuilder::new(
            "SELECT id, operation_type, operator, scope, result_status, result_message, preview_summary, duration_ms FROM core.operations_log WHERE 1=1",
        );

        if let Some(operator) = operator {
            query_builder.push(" AND operator = ");
            query_builder.push_bind(operator);
        }

        if let Some(scope) = scope_filter {
            query_builder.push(" AND scope @> ");
            query_builder.push_bind(scope);
        }

        query_builder.push(" ORDER BY id DESC LIMIT ");
        query_builder.push_bind(limit);

        let query = query_builder.build_query_as::<OperationRecord>();
        query
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "get operations by operator and scope"))
    }

    /// Get operations by scope filter (searches JSONB scope field)
    pub async fn get_operations_by_scope(
        &self,
        scope_filter: JsonValue,
        limit: Option<i64>,
    ) -> DbResult<Vec<OperationRecord>> {
        let limit = limit.unwrap_or(100);

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
            WHERE scope @> $1
            ORDER BY id DESC
            LIMIT $2
            "#,
            scope_filter,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get operations by scope"))
    }

    /// Get operations by operator
    pub async fn get_operations_by_actor(
        &self,
        operator: &str,
        limit: Option<i64>,
    ) -> DbResult<Vec<OperationRecord>> {
        let limit = limit.unwrap_or(100);

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
            WHERE operator = $1
            ORDER BY id DESC
            LIMIT $2
            "#,
            operator,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get operations by operator"))
    }

    /// Get failed operations
    pub async fn get_failed_operations(
        &self,
        since: Option<Timestamp>,
        limit: Option<i64>,
    ) -> DbResult<Vec<OperationRecord>> {
        let limit = limit.unwrap_or(100);

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
            WHERE result_status = 'failure'
              AND ($1::timestamptz IS NULL OR uuid_extract_timestamp(id) >= $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
            since.map(|timestamp| *timestamp),
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get failed operations"))
    }

    /// Get operation statistics
    pub async fn get_operation_statistics(
        &self,
        since: Option<Timestamp>,
    ) -> DbResult<OperationStatistics> {
        let result = sqlx::query!(
            r#"
            SELECT
                COUNT(*) as "total!",
                COUNT(*) FILTER (WHERE result_status = 'success') as "successful!",
                COUNT(*) FILTER (WHERE result_status = 'failure') as "failed!",
                COUNT(*) FILTER (WHERE result_status = 'cancelled') as "cancelled!",
                AVG(duration_ms) as "avg_duration_ms?"
            FROM core.operations_log
            WHERE ($1::timestamptz IS NULL OR uuid_extract_timestamp(id) >= $1)
            "#,
            since.map(|timestamp| *timestamp)
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get operation statistics"))?;

        Ok(OperationStatistics {
            total: result.total,
            successful: result.successful,
            failed: result.failed,
            cancelled: result.cancelled,
            avg_duration_ms: result
                .avg_duration_ms
                .and_then(|duration| duration.to_f64())
                .map(f64::round)
                .and_then(|duration| i64::try_from(duration as i128).ok()),
        })
    }

    // ===== Generic Operations =====

    /// Start a new operation via the `core.start_operation()` database function.
    ///
    /// Unlike `log_operation()` which does a raw INSERT, this calls the DB function
    /// that handles default scoping and returns the generated ID.
    pub async fn start_operation(
        &self,
        operation_type: &str,
        operator: &str,
        scope: JsonValue,
    ) -> DbResult<OperationRecord> {
        Self::validate_managed_operation_type(operation_type)?;
        let op_uuid = sqlx::query_scalar!(
            r#"SELECT core.start_operation($1, $2, $3)::uuid as "id!""#,
            operation_type,
            operator,
            scope,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "start operation"))?;

        let op_id = Id::<Operation>::from_uuid(op_uuid);

        self.get_operation(&op_id).await?.ok_or_else(|| {
            SinexError::database("operation created by core.start_operation() not found")
        })
    }

    /// List operations with optional type and status filters.
    pub async fn list_operations(
        &self,
        operation_type: Option<&str>,
        status: Option<OperationStatus>,
        limit: i64,
    ) -> DbResult<Vec<OperationRecord>> {
        let status_str = status.map(|s| s.to_string());

        let mut qb = sqlx::QueryBuilder::new(
            r"SELECT
                id,
                operation_type, operator, scope,
                result_status, result_message, preview_summary, duration_ms
            FROM core.operations_log WHERE 1=1",
        );

        if let Some(op_type) = operation_type {
            qb.push(" AND operation_type = ");
            qb.push_bind(op_type.to_string());
        }

        if let Some(ref status) = status_str {
            qb.push(" AND result_status = ");
            qb.push_bind(status.clone());
        }

        qb.push(" ORDER BY id DESC LIMIT ");
        qb.push_bind(limit);

        qb.build_query_as::<OperationRecord>()
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "list operations"))
    }

    /// Cancel a running operation.
    ///
    /// Returns the updated record, or an error if the operation is not found
    /// or not in a cancellable state.
    pub async fn cancel_operation(
        &self,
        id: &Id<Operation>,
        reason: &str,
    ) -> DbResult<OperationRecord> {
        // Check current status
        let record = self
            .get_operation(id)
            .await?
            .ok_or_else(|| SinexError::not_found(format!("Operation not found: {id}")))?;

        if record.result_status != OperationStatus::Running {
            return Err(SinexError::invalid_state(format!(
                "Operation cannot be cancelled (status: {})",
                record.result_status
            )));
        }

        match record.operation_type.as_str() {
            "replay" => {
                crate::replay::state_machine::ReplayStateMachine::new(self.pool.clone())
                    .cancel(id.to_uuid(), reason.to_string())
                    .await?;
                return self
                    .get_operation(id)
                    .await?
                    .ok_or_else(|| SinexError::database("operation disappeared after cancel"));
            }
            "tombstone" => {
                return self
                    .cancel_tombstone_operation(&id.to_string(), Some(reason))
                    .await;
            }
            _ => {}
        }

        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = 'cancelled',
                result_message = $2,
                duration_ms = EXTRACT(MILLISECONDS FROM (NOW() - uuid_extract_timestamp(id)))::integer
            WHERE id = $1
            "#,
            id.to_uuid(),
            reason
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "cancel operation"))?;

        self.get_operation(id)
            .await?
            .ok_or_else(|| SinexError::database("operation disappeared after cancel"))
    }

    // ========== Node Manifests ==========

    /// Register a node in the manifest
    pub async fn register_node(
        &self,
        node_name: &NodeName,
        node_type: NodeType,
        version: &str,
        description: Option<&str>,
    ) -> DbResult<NodeManifest> {
        sqlx::query_as!(
            NodeManifest,
            r#"
            INSERT INTO core.node_manifests (
                node_name, version, node_type, description
            ) VALUES (
                $1, $2, $3, $4
            )
            ON CONFLICT (node_name, version) DO UPDATE
            SET node_type = EXCLUDED.node_type,
                description = EXCLUDED.description,
                status = 'active'
            RETURNING
                id,
                node_name,
                node_type,
                version,
                description,
                anchor_rule_version,
                config_schema,
                created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                status,
                last_heartbeat_at as "last_heartbeat_at: sinex_primitives::temporal::Timestamp"
            "#,
            node_name.as_ref(),
            version,
            node_type.to_string(),
            description
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "register node"))
    }

    /// Get all nodes in the manifest
    pub async fn get_all_nodes(&self) -> DbResult<Vec<NodeManifest>> {
        sqlx::query_as!(
            NodeManifest,
            r#"
            SELECT
                id,
                node_name,
                node_type,
                version,
                description,
                anchor_rule_version,
                config_schema,
                created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                status,
                last_heartbeat_at as "last_heartbeat_at: sinex_primitives::temporal::Timestamp"
            FROM core.node_manifests
            ORDER BY node_name, version
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get all nodes"))
    }

    /// Get nodes by type
    pub async fn get_nodes_by_type(&self, node_type: NodeType) -> DbResult<Vec<NodeManifest>> {
        sqlx::query_as!(
            NodeManifest,
            r#"
            SELECT
                id,
                node_name,
                node_type,
                version,
                description,
                anchor_rule_version,
                config_schema,
                created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                status,
                last_heartbeat_at as "last_heartbeat_at: sinex_primitives::temporal::Timestamp"
            FROM core.node_manifests
            WHERE node_type = $1
            ORDER BY node_name, version
            "#,
            node_type.to_string()
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get nodes by type"))
    }

    /// Update a specific node manifest heartbeat timestamp and set status to 'active'.
    ///
    /// Returns whether a matching manifest row was updated.
    pub async fn update_node_heartbeat_for_version(
        &self,
        node_name: &NodeName,
        version: &str,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE core.node_manifests
            SET last_heartbeat_at = NOW(),
                status = 'active'
            WHERE node_name = $1
              AND version = $2
            "#,
            node_name as &NodeName,
            version,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update node heartbeat"))?;
        Ok(result.rows_affected() > 0)
    }

    /// Mark a specific node manifest as inactive.
    ///
    /// Returns whether a matching manifest row was updated.
    pub async fn mark_node_inactive_for_version(
        &self,
        node_name: &NodeName,
        version: &str,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE core.node_manifests
            SET status = 'inactive'
            WHERE node_name = $1
              AND version = $2
            "#,
            node_name as &NodeName,
            version,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "mark node inactive"))?;
        Ok(result.rows_affected() > 0)
    }

    /// Insert a concrete node-run row for a single process execution.
    pub async fn start_node_run(
        &self,
        node_manifest_id: i32,
        service_name: &str,
        instance_id: &str,
        host: &str,
        effective_config_hash: Option<&str>,
        effective_config: Option<&JsonValue>,
    ) -> DbResult<NodeRun> {
        sqlx::query_as!(
            NodeRun,
            r#"
            INSERT INTO core.node_runs (
                node_manifest_id,
                service_name,
                instance_id,
                host,
                status,
                last_heartbeat_at,
                effective_config_hash,
                effective_config
            ) VALUES (
                $1,
                $2,
                $3,
                $4,
                'running',
                NOW(),
                $5,
                $6
            )
            RETURNING
                id as "id!: uuid::Uuid",
                node_manifest_id,
                service_name,
                instance_id,
                host,
                started_at as "started_at!: sinex_primitives::temporal::Timestamp",
                ended_at as "ended_at: sinex_primitives::temporal::Timestamp",
                status,
                last_heartbeat_at as "last_heartbeat_at: sinex_primitives::temporal::Timestamp",
                effective_config_hash,
                effective_config
            "#,
            node_manifest_id,
            service_name,
            instance_id,
            host,
            effective_config_hash,
            effective_config
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "start node run"))
    }

    /// Refresh the heartbeat timestamp for a node run and keep it in `running`.
    pub async fn update_node_run_heartbeat(&self, node_run_id: Uuid) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE core.node_runs
            SET last_heartbeat_at = NOW(),
                status = 'running'
            WHERE id = $1::uuid
              AND status = 'running'
            "#,
            node_run_id,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update node run heartbeat"))?;
        Ok(result.rows_affected() > 0)
    }

    /// Mark a node run as terminal or transitional.
    pub async fn update_node_run_status(
        &self,
        node_run_id: Uuid,
        status: NodeState,
    ) -> DbResult<bool> {
        let ended_at = matches!(status, NodeState::Failed | NodeState::Stopped)
            .then(Timestamp::now)
            .map(|timestamp| timestamp.inner());

        let result = sqlx::query!(
            r#"
            UPDATE core.node_runs
            SET status = $2,
                last_heartbeat_at = NOW(),
                ended_at = CASE
                    WHEN $3::timestamptz IS NULL THEN ended_at
                    ELSE COALESCE(ended_at, $3::timestamptz)
                END
            WHERE id = $1::uuid
            "#,
            node_run_id,
            status.to_string(),
            ended_at,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update node run status"))?;
        Ok(result.rows_affected() > 0)
    }

    /// List live node presence, preferring concrete run rows and falling back
    /// to manifest heartbeats for services that do not yet register runs.
    pub async fn list_live_node_presence(
        &self,
        stale_after: Duration,
    ) -> DbResult<Vec<LiveNodePresence>> {
        let stale_secs = stale_after.as_secs() as f64;
        sqlx::query_as!(
            LiveNodePresence,
            r#"
            WITH active_runs AS (
                SELECT
                    nm.node_name::text as node_name,
                    nm.node_type::text as node_type,
                    nm.version,
                    nm.description,
                    nr.service_name,
                    nr.instance_id,
                    nr.id as node_run_id,
                    nr.host,
                    nr.status,
                    nr.last_heartbeat_at,
                    nr.started_at,
                    'run'::text as heartbeat_source
                FROM core.node_runs nr
                JOIN core.node_manifests nm ON nm.id = nr.node_manifest_id
                WHERE nr.status = 'running'
                  AND nr.last_heartbeat_at > NOW() - make_interval(secs => $1::float8)
            )
            SELECT
                live_nodes.node_name as "node_name!: NodeName",
                live_nodes.node_type as "node_type!: NodeType",
                live_nodes.version as "version!",
                description,
                service_name,
                instance_id,
                node_run_id as "node_run_id: uuid::Uuid",
                host,
                live_nodes.status as "status!",
                last_heartbeat_at as "last_heartbeat_at: sinex_primitives::temporal::Timestamp",
                started_at as "started_at: sinex_primitives::temporal::Timestamp",
                live_nodes.heartbeat_source as "heartbeat_source!"
            FROM (
                SELECT
                    node_name,
                    node_type,
                    version,
                    description,
                    service_name,
                    instance_id,
                    node_run_id,
                    host,
                    status,
                    last_heartbeat_at,
                    started_at,
                    heartbeat_source
                FROM active_runs

                UNION ALL

                SELECT
                    nm.node_name::text as node_name,
                    nm.node_type::text as node_type,
                    nm.version,
                    nm.description,
                    NULL::text as service_name,
                    NULL::text as instance_id,
                    NULL::uuid as node_run_id,
                    NULL::text as host,
                    nm.status,
                    nm.last_heartbeat_at,
                    NULL::timestamptz as started_at,
                    'manifest'::text as heartbeat_source
                FROM core.node_manifests nm
                WHERE nm.status = 'active'
                  AND nm.last_heartbeat_at > NOW() - make_interval(secs => $1::float8)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM active_runs ar
                      WHERE ar.node_name = nm.node_name::text
                        AND ar.version = nm.version
                  )
            ) live_nodes
            "#,
            stale_secs
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list live node presence"))
        .map(|mut nodes| {
            nodes.sort_by(|left, right| {
                left.node_name
                    .as_ref()
                    .cmp(right.node_name.as_ref())
                    .then_with(|| left.version.cmp(&right.version))
                    .then_with(|| left.service_name.cmp(&right.service_name))
                    .then_with(|| left.instance_id.cmp(&right.instance_id))
                    .then_with(|| left.started_at.cmp(&right.started_at))
            });
            nodes
        })
    }

    /// Get node health status
    pub async fn get_node_health(&self, stale_after: Duration) -> DbResult<NodeHealthSummary> {
        let cutoff = Timestamp::now()
            - sinex_primitives::temporal::Duration::seconds(stale_after.as_secs() as i64);

        let row = sqlx::query!(
            r#"
            WITH active_runs AS (
                SELECT
                    nm.node_name,
                    COUNT(*)::bigint AS active_run_count,
                    MAX(nr.last_heartbeat_at) AS latest_heartbeat_at
                FROM core.node_runs nr
                JOIN core.node_manifests nm ON nm.id = nr.node_manifest_id
                WHERE nr.status = 'running'
                  AND nr.last_heartbeat_at IS NOT NULL
                  AND nr.last_heartbeat_at >= $1
                GROUP BY nm.node_name
            ),
            manifest_only_live AS (
                SELECT
                    nm.node_name,
                    MAX(nm.last_heartbeat_at) AS latest_heartbeat_at
                FROM core.node_manifests nm
                WHERE nm.status = 'active'
                  AND nm.last_heartbeat_at IS NOT NULL
                  AND nm.last_heartbeat_at >= $1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM active_runs ar
                      WHERE ar.node_name = nm.node_name
                  )
                GROUP BY nm.node_name
            ),
            node_inventory AS (
                SELECT DISTINCT node_name
                FROM core.node_manifests

                UNION

                SELECT DISTINCT nm.node_name
                FROM core.node_runs nr
                JOIN core.node_manifests nm ON nm.id = nr.node_manifest_id
            ),
            node_status AS (
                SELECT
                    ni.node_name,
                    COALESCE(ar.active_run_count, 0) AS active_run_count,
                    COALESCE(ar.latest_heartbeat_at, mol.latest_heartbeat_at) AS latest_heartbeat_at,
                    (ar.node_name IS NOT NULL OR mol.node_name IS NOT NULL) AS has_live_instance
                FROM node_inventory ni
                LEFT JOIN active_runs ar ON ar.node_name = ni.node_name
                LEFT JOIN manifest_only_live mol ON mol.node_name = ni.node_name
            )
            SELECT
                COUNT(*) FILTER (WHERE has_live_instance) as "active_count!",
                COUNT(*) FILTER (WHERE NOT has_live_instance) as "inactive_count!",
                COUNT(*) as "unique_nodes!",
                COALESCE(SUM(active_run_count), 0)::bigint as "active_run_count!",
                MIN(latest_heartbeat_at) FILTER (WHERE has_live_instance) as "oldest_heartbeat: sinex_primitives::temporal::Timestamp"
            FROM node_status
            "#,
            *cutoff
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get node health"))?;

        Ok(NodeHealthSummary {
            active_count: row.active_count,
            inactive_count: row.inactive_count,
            unique_nodes: row.unique_nodes,
            active_run_count: row.active_run_count,
            oldest_heartbeat: row.oldest_heartbeat,
        })
    }

    /// List operator-facing status for registered automata.
    ///
    /// The durable base is the node registry (`node_manifests` + latest
    /// `node_runs`). Derived-node-specific runtime details come from SDK
    /// self-observation events in `core.events`, keyed by node/run labels.
    pub async fn list_automata_status(
        &self,
        stale_after: Duration,
        recent_window: Duration,
    ) -> DbResult<Vec<AutomataStatusRow>> {
        let stale_secs = stale_after.as_secs() as f64;
        let recent_secs = recent_window.as_secs() as f64;

        sqlx::query_as!(
            AutomataStatusRow,
            r#"
            SELECT
                nm.node_name::text as "node_name!: NodeName",
                nm.version as "version!",
                nm.description,
                nm.status as "manifest_status!",
                (
                    (nr.id IS NOT NULL
                        AND nr.status = 'running'
                        AND nr.last_heartbeat_at > NOW() - make_interval(secs => $1::float8))
                    OR
                    (nr.id IS NULL
                        AND nm.status = 'active'
                        AND nm.last_heartbeat_at > NOW() - make_interval(secs => $1::float8))
                ) as "live!",
                nr.service_name,
                nr.instance_id,
                nr.id as "node_run_id: uuid::Uuid",
                nr.host,
                nr.status as run_status,
                nr.started_at as "started_at: sinex_primitives::temporal::Timestamp",
                COALESCE(nr.last_heartbeat_at, nm.last_heartbeat_at)
                    as "last_heartbeat_at: sinex_primitives::temporal::Timestamp",
                processed.events_processed_current_run as "events_processed_current_run?",
                checkpoint.checkpoint_kind as "checkpoint_kind?",
                checkpoint.checkpoint_position as "checkpoint_position?",
                checkpoint.checkpoint_revision as "checkpoint_revision?",
                checkpoint.checkpoint_recorded_at
                    as "checkpoint_recorded_at?: sinex_primitives::temporal::Timestamp",
                pending.pending_invalidation_count as "pending_invalidation_count?",
                error_rate.error_rate_5m as "error_rate_5m?",
                lag_p50.event_lag_p50_ms as "event_lag_p50_ms?",
                lag_p99.event_lag_p99_ms as "event_lag_p99_ms?",
                tick_p99.tick_runtime_p99_ms as "tick_runtime_p99_ms?",
                throughput.throughput_eps as "throughput_eps?",
                COALESCE(outputs.recent_output_count, 0)::bigint as "recent_output_count!",
                outputs.last_output_at as "last_output_at?: sinex_primitives::temporal::Timestamp",
                outputs.last_replay_at as "last_replay_at?: sinex_primitives::temporal::Timestamp"
            FROM core.node_manifests nm
            LEFT JOIN LATERAL (
                SELECT
                    nr.id,
                    nr.service_name,
                    nr.instance_id,
                    nr.host,
                    nr.status,
                    nr.started_at,
                    nr.last_heartbeat_at
                FROM core.node_runs nr
                WHERE nr.node_manifest_id = nm.id
                ORDER BY nr.started_at DESC
                LIMIT 1
            ) nr ON true
            LEFT JOIN LATERAL (
                SELECT
                    FLOOR((e.payload->>'value')::float8)::bigint
                        AS events_processed_current_run
                FROM core.events e
                WHERE e.source = 'sinex'
                  AND e.event_type = 'metric.gauge'
                  AND e.payload->>'name' = 'derived.events_processed.run'
                  AND e.payload->'labels'->>'node' = nm.node_name::text
                  AND (nr.id IS NULL OR e.payload->'labels'->>'node_run_id' = nr.id::text)
                ORDER BY e.id DESC
                LIMIT 1
            ) processed ON true
            LEFT JOIN LATERAL (
                SELECT
                    e.payload->'labels'->>'checkpoint_kind' AS checkpoint_kind,
                    e.payload->'labels'->>'checkpoint_position' AS checkpoint_position,
                    FLOOR((e.payload->>'value')::float8)::bigint AS checkpoint_revision,
                    e.ts_coided AS checkpoint_recorded_at
                FROM core.events e
                WHERE e.source = 'sinex'
                  AND e.event_type = 'metric.gauge'
                  AND e.payload->>'name' = 'derived.checkpoint.revision'
                  AND e.payload->'labels'->>'node' = nm.node_name::text
                  AND (nr.id IS NULL OR e.payload->'labels'->>'node_run_id' = nr.id::text)
                ORDER BY e.id DESC
                LIMIT 1
            ) checkpoint ON true
            LEFT JOIN LATERAL (
                SELECT
                    FLOOR((e.payload->>'value')::float8)::bigint
                        AS pending_invalidation_count
                FROM core.events e
                WHERE e.source = 'sinex'
                  AND e.event_type = 'metric.gauge'
                  AND e.payload->>'name' = 'derived.invalidations.pending'
                  AND e.payload->'labels'->>'node' = nm.node_name::text
                  AND (nr.id IS NULL OR e.payload->'labels'->>'node_run_id' = nr.id::text)
                ORDER BY e.id DESC
                LIMIT 1
            ) pending ON true
            LEFT JOIN LATERAL (
                SELECT
                    (e.payload->>'value')::float8 AS error_rate_5m
                FROM core.events e
                WHERE e.source = 'sinex'
                  AND e.event_type = 'metric.gauge'
                  AND e.payload->>'name' = 'derived.error_rate_5m'
                  AND e.payload->'labels'->>'node' = nm.node_name::text
                  AND (nr.id IS NULL OR e.payload->'labels'->>'node_run_id' = nr.id::text)
                ORDER BY e.id DESC
                LIMIT 1
            ) error_rate ON true
            LEFT JOIN LATERAL (
                SELECT
                    (e.payload->>'value')::float8 AS event_lag_p50_ms
                FROM core.events e
                WHERE e.source = 'sinex'
                  AND e.event_type = 'metric.gauge'
                  AND e.payload->>'name' = 'derived.event_lag_p50_ms'
                  AND e.payload->'labels'->>'node' = nm.node_name::text
                  AND (nr.id IS NULL OR e.payload->'labels'->>'node_run_id' = nr.id::text)
                ORDER BY e.id DESC
                LIMIT 1
            ) lag_p50 ON true
            LEFT JOIN LATERAL (
                SELECT
                    (e.payload->>'value')::float8 AS event_lag_p99_ms
                FROM core.events e
                WHERE e.source = 'sinex'
                  AND e.event_type = 'metric.gauge'
                  AND e.payload->>'name' = 'derived.event_lag_p99_ms'
                  AND e.payload->'labels'->>'node' = nm.node_name::text
                  AND (nr.id IS NULL OR e.payload->'labels'->>'node_run_id' = nr.id::text)
                ORDER BY e.id DESC
                LIMIT 1
            ) lag_p99 ON true
            LEFT JOIN LATERAL (
                SELECT
                    (e.payload->>'value')::float8 AS tick_runtime_p99_ms
                FROM core.events e
                WHERE e.source = 'sinex'
                  AND e.event_type = 'metric.gauge'
                  AND e.payload->>'name' = 'derived.tick_runtime_p99_ms'
                  AND e.payload->'labels'->>'node' = nm.node_name::text
                  AND (nr.id IS NULL OR e.payload->'labels'->>'node_run_id' = nr.id::text)
                ORDER BY e.id DESC
                LIMIT 1
            ) tick_p99 ON true
            LEFT JOIN LATERAL (
                SELECT
                    (e.payload->>'value')::float8 AS throughput_eps
                FROM core.events e
                WHERE e.source = 'sinex'
                  AND e.event_type = 'metric.gauge'
                  AND e.payload->>'name' = 'derived.throughput_eps'
                  AND e.payload->'labels'->>'node' = nm.node_name::text
                  AND (nr.id IS NULL OR e.payload->'labels'->>'node_run_id' = nr.id::text)
                ORDER BY e.id DESC
                LIMIT 1
            ) throughput ON true
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (
                        WHERE e.ts_coided > NOW() - make_interval(secs => $2::float8)
                    ) AS recent_output_count,
                    MAX(e.ts_coided) AS last_output_at,
                    MAX(e.ts_coided) FILTER (WHERE e.created_by_operation_id IS NOT NULL)
                        AS last_replay_at
                FROM core.events e
                WHERE nr.id IS NOT NULL
                  AND e.node_run_id = nr.id
                  AND e.source_event_ids IS NOT NULL
            ) outputs ON true
            WHERE nm.node_type = 'automaton'
            ORDER BY nm.node_name, nm.version
            "#,
            stale_secs,
            recent_secs
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list automata status"))
    }

    // ========== System Verification Methods ==========

    /// Test UUID generation functionality
    pub async fn test_uuid_generation(&self) -> DbResult<sqlx::types::Uuid> {
        let row = sqlx::query!("SELECT gen_random_uuid() as test_uuid")
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "test UUID generation"))?;

        row.test_uuid
            .ok_or_else(|| db_error(sqlx::Error::RowNotFound, "UUID generation returned NULL"))
    }

    /// Test `UUIDv7` generation functionality
    pub async fn test_uuid_v7_generation(&self) -> DbResult<uuid::Uuid> {
        let row = sqlx::query!("SELECT uuidv7() as \"test_uuid!: Uuid\"")
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "test UUIDv7 generation"))?;

        Ok(row.test_uuid)
    }

    /// Check `TimescaleDB` extension version
    pub async fn get_timescaledb_version(&self) -> DbResult<Option<String>> {
        let row = sqlx::query!("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            .fetch_optional(self.pool)
            .await
            .map_err(|e| db_error(e, "check TimescaleDB version"))?;

        Ok(row.map(|r| r.extversion))
    }

    /// Test JSON schema validation functionality
    pub async fn test_json_schema_validation(&self) -> DbResult<bool> {
        let result = sqlx::query_scalar::<_, Option<bool>>(
            r#"SELECT json_matches_schema('{"type": "object"}', '{}')"#,
        )
        .fetch_one(self.pool)
        .await;

        match result {
            Ok(value) => Ok(value.unwrap_or(false)),
            Err(err) => {
                if let sqlx::Error::Database(db_err) = &err
                    && db_err
                        .code()
                        .as_deref()
                        .is_some_and(|code| code == SQLSTATE_UNDEFINED_FUNCTION)
                {
                    return Ok(false);
                }
                Err(db_error(err, "test JSON schema validation"))
            }
        }
    }

    /// Check if a table exists
    pub async fn table_exists(&self, schema: &str, table_name: &str) -> DbResult<bool> {
        let row = sqlx::query!(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = $1 
                AND table_name = $2
            ) as exists
            "#,
            schema,
            table_name
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "check table existence"))?;

        Ok(row.exists.unwrap_or(false))
    }

    /// Count events by source and phase (for testing)
    pub async fn count_events_by_source_and_phase(
        &self,
        source: &str,
        event_type: &str,
        phase: &str,
    ) -> DbResult<i64> {
        let result = sqlx::query!(
            r#"
            SELECT COUNT(*) as count
            FROM core.events
            WHERE source = $1 
              AND event_type = $2 
              AND payload->>'phase' = $3
            "#,
            source,
            event_type,
            phase
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count events by source and phase"))?;

        Ok(result.count.unwrap_or(0))
    }

    /// Run basic system health checks
    pub async fn run_system_health_checks(&self) -> DbResult<SystemHealthReport> {
        let stale_after = node_heartbeat_stale_after()?;

        // Check database connectivity
        let (db_connected, db_connect_error) =
            match sqlx::query!("SELECT 1 as one").fetch_one(self.pool).await {
                Ok(_) => (true, None),
                Err(error) => (
                    false,
                    Some(db_error(error, "check database connectivity").to_string()),
                ),
            };

        // Check extensions
        let (timescaledb_version, timescaledb_error) =
            probe_health(self.get_timescaledb_version().await);
        let (uuid_v7_generation, uuid_v7_error) =
            probe_health(self.test_uuid_v7_generation().await);
        let (json_schema_works, json_schema_error) =
            probe_health_bool(self.test_json_schema_validation().await);

        // Check critical tables
        let (events_table_exists, events_table_error) =
            probe_health_bool(self.table_exists("core", "events").await);

        // Get node health
        let (node_health, node_health_error) =
            probe_health(self.get_node_health(stale_after).await);

        Ok(SystemHealthReport {
            db_connected,
            db_connect_error,
            timescaledb_version: timescaledb_version.flatten(),
            timescaledb_error,
            uuid_v7_generation_works: uuid_v7_generation.is_some(),
            uuid_v7_error,
            json_schema_extension_works: json_schema_works,
            json_schema_error,
            events_table_exists,
            events_table_error,
            node_health,
            node_health_error,
        })
    }
}

/// Node manifest record
#[derive(Debug, sqlx::FromRow)]
pub struct NodeManifest {
    pub id: i32,
    pub node_name: NodeName,
    pub node_type: NodeType,
    pub version: String,
    pub description: Option<String>,
    pub anchor_rule_version: Option<i32>,
    pub config_schema: Option<JsonValue>,
    pub created_at: Timestamp,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
}

/// Node run record
#[derive(Debug, sqlx::FromRow)]
pub struct NodeRun {
    pub id: Uuid,
    pub node_manifest_id: i32,
    pub service_name: String,
    pub instance_id: String,
    pub host: String,
    pub started_at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
    pub effective_config_hash: Option<String>,
    pub effective_config: Option<JsonValue>,
}

/// Live node presence for operator-facing status surfaces.
#[derive(Debug, sqlx::FromRow)]
pub struct LiveNodePresence {
    pub node_name: NodeName,
    pub node_type: NodeType,
    pub version: String,
    pub description: Option<String>,
    pub service_name: Option<String>,
    pub instance_id: Option<String>,
    pub node_run_id: Option<Uuid>,
    pub host: Option<String>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
    pub started_at: Option<Timestamp>,
    pub heartbeat_source: String,
}

/// Node health summary
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeHealthSummary {
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_nodes: i64,
    pub active_run_count: i64,
    pub oldest_heartbeat: Option<Timestamp>,
}

/// Operator-facing automaton status row.
#[derive(Debug, sqlx::FromRow)]
pub struct AutomataStatusRow {
    pub node_name: NodeName,
    pub version: String,
    pub description: Option<String>,
    pub manifest_status: String,
    pub live: bool,
    pub service_name: Option<String>,
    pub instance_id: Option<String>,
    pub node_run_id: Option<Uuid>,
    pub host: Option<String>,
    pub run_status: Option<String>,
    pub started_at: Option<Timestamp>,
    pub last_heartbeat_at: Option<Timestamp>,
    pub events_processed_current_run: Option<i64>,
    pub checkpoint_kind: Option<String>,
    pub checkpoint_position: Option<String>,
    pub checkpoint_revision: Option<i64>,
    pub checkpoint_recorded_at: Option<Timestamp>,
    pub pending_invalidation_count: Option<i64>,
    pub error_rate_5m: Option<f64>,
    pub event_lag_p50_ms: Option<f64>,
    pub event_lag_p99_ms: Option<f64>,
    pub tick_runtime_p99_ms: Option<f64>,
    pub throughput_eps: Option<f64>,
    pub recent_output_count: i64,
    pub last_output_at: Option<Timestamp>,
    pub last_replay_at: Option<Timestamp>,
}

/// Operation statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationStatistics {
    pub total: i64,
    pub successful: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub avg_duration_ms: Option<i64>,
}

/// System health report
#[derive(Debug, Serialize, Deserialize)]
pub struct SystemHealthReport {
    pub db_connected: bool,
    pub db_connect_error: Option<String>,
    pub timescaledb_version: Option<String>,
    pub timescaledb_error: Option<String>,
    pub uuid_v7_generation_works: bool,
    pub uuid_v7_error: Option<String>,
    pub json_schema_extension_works: bool,
    pub json_schema_error: Option<String>,
    pub events_table_exists: bool,
    pub events_table_error: Option<String>,

    pub node_health: Option<NodeHealthSummary>,
    pub node_health_error: Option<String>,
}

// ============================================================================
// Tombstone Operation Persistence
// ============================================================================

/// Tombstone operation stored in `operations_log`.
///
/// Uses `operation_type` = "tombstone" and stores full state in scope JSONB.
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
    pub async fn count_archived_event_ids(&self, event_ids: &[Uuid]) -> DbResult<i64> {
        if event_ids.is_empty() {
            return Ok(0);
        }

        sqlx::query_scalar!(
            r#"SELECT COUNT(*)::bigint as "count!" FROM audit.archived_events WHERE id = ANY($1::uuid[])"#,
            event_ids
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

#[cfg(test)]
mod tests {
    // Inline because this covers local env/default and report helper semantics.
    use super::{node_heartbeat_stale_after, probe_health, probe_health_bool};
    use sinex_primitives::error::SinexError;
    use xtask::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

    #[sinex_serial_test]
    async fn node_heartbeat_stale_after_defaults_invalid_override() -> xtask::sandbox::TestResult<()>
    {
        let mut env = EnvGuard::new();
        env.set("SINEX_NODE_HEARTBEAT_STALE_SECS", "bogus");

        let error = node_heartbeat_stale_after().expect_err("invalid override should fail");
        assert!(
            error
                .to_string()
                .contains("SINEX_NODE_HEARTBEAT_STALE_SECS must be a positive integer")
        );
        Ok(())
    }

    #[sinex_serial_test]
    async fn node_heartbeat_stale_after_defaults_zero_override() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_NODE_HEARTBEAT_STALE_SECS", "0");

        let error = node_heartbeat_stale_after().expect_err("zero override should fail");
        assert!(
            error
                .to_string()
                .contains("SINEX_NODE_HEARTBEAT_STALE_SECS must be greater than zero")
        );
        Ok(())
    }

    #[sinex_test]
    async fn probe_health_preserves_error_text() -> xtask::sandbox::TestResult<()> {
        let (_value, error) = probe_health::<()>(Err(SinexError::configuration("probe failed")));
        assert_eq!(error.as_deref(), Some("Configuration error: probe failed"));
        Ok(())
    }

    #[sinex_test]
    async fn probe_health_bool_preserves_error_text() -> xtask::sandbox::TestResult<()> {
        let (value, error) = probe_health_bool(Err(SinexError::configuration("probe failed")));
        assert!(!value);
        assert_eq!(error.as_deref(), Some("Configuration error: probe failed"));
        Ok(())
    }
}
