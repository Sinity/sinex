//! State repository for managing system state including checkpoints and operations log
//!
//! This repository combines management of:
//! - Node checkpoints (tracking progress of event processing)
//! - Operations log (audit trail of system operations)

use super::common::{DbResult, EnhancedRepository, Repository, db_error};
use crate::schema::OperationsLog;
use crate::{Id, JsonValue};
use crate::{IdempotentTransaction, RetryConfig, with_retry_transaction_idempotent};
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::{NodeName, NodeType, OperationStatus};
use sinex_primitives::error::SinexError;
use sinex_primitives::rpc::lifecycle::{TombstoneOperation, TombstoneOperationState};
use sinex_primitives::{Seconds, Timestamp};
use sqlx::postgres::types::PgRange;
use sqlx::types::BigDecimal;
use sqlx::{FromRow, PgPool};
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
const VALID_OPERATION_TYPES: &[&str] = &["replay", "archive", "restore", "purge", "tombstone"];

fn node_heartbeat_stale_after() -> Duration {
    std::env::var("SINEX_NODE_HEARTBEAT_STALE_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map_or(
            Duration::from_secs(DEFAULT_NODE_HEARTBEAT_STALE_SECS.as_secs()),
            Duration::from_secs,
        )
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

    fn validate_operation_type(operation_type: &str) -> DbResult<()> {
        if VALID_OPERATION_TYPES.contains(&operation_type) {
            Ok(())
        } else {
            Err(SinexError::validation(format!(
                "Unsupported operation type '{operation_type}'. Allowed types: {}",
                VALID_OPERATION_TYPES.join(", ")
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
        Self::validate_operation_type(&operation.operation_type)?;
        // Validate replay-specific scope only for replay operations; allow other shapes otherwise
        if operation.operation_type == "replay"
            && let Some(ref scope) = operation.scope
        {
            Self::validate_replay_scope(scope)?;
        }

        let id = Id::<Operation>::new();
        let operation_type = operation.operation_type.clone();
        let operator = operation.operator.clone();
        let scope = operation.scope.clone();
        let result_status = operation.result_status;
        let result_message = operation.result_message.clone();
        let preview_summary = operation.preview_summary.clone();
        let duration_ms = operation.duration_ms;

        let result = with_retry_transaction_idempotent(
            self.pool,
            RetryConfig::default(),
            IdempotentTransaction::new(),
            |tx| {
                let operation_type = operation_type.clone();
                let operator = operator.clone();
                let scope = scope.clone();
                let result_message = result_message.clone();
                let preview_summary = preview_summary.clone();
                Box::pin(async move {
                    let record = sqlx::query_as!(
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
                        operation_type,
                        operator,
                        scope,
                        result_status.to_string(),
                        result_message,
                        preview_summary,
                        duration_ms
                    )
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(|e| db_error(e, "log operation"))?;
                    Ok(record)
                })
            },
        )
        .await?;

        Ok(result)
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
        _since: Option<Timestamp>,
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
            ORDER BY id DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get failed operations"))
    }

    /// Get operation statistics
    pub async fn get_operation_statistics(
        &self,
        _since: Option<Timestamp>,
    ) -> DbResult<OperationStatistics> {
        let result = sqlx::query!(
            r#"
            SELECT
                COUNT(*) as "total!",
                COUNT(*) FILTER (WHERE result_status = 'success') as "successful!",
                COUNT(*) FILTER (WHERE result_status = 'failure') as "failed!",
                COUNT(*) FILTER (WHERE result_status = 'cancelled') as "cancelled!",
                AVG(duration_ms) as "avg_duration_ms"
            FROM core.operations_log
            "#
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get operation statistics"))?;

        Ok(OperationStatistics {
            total: result.total,
            successful: result.successful,
            failed: result.failed,
            cancelled: result.cancelled,
            avg_duration_ms: result.avg_duration_ms.and_then(|d: BigDecimal| {
                use std::str::FromStr;
                i64::from_str(&d.to_string()).ok()
            }),
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
        Self::validate_operation_type(operation_type)?;
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
            r#"SELECT
                id,
                operation_type, operator, scope,
                result_status, result_message, preview_summary, duration_ms
            FROM core.operations_log WHERE 1=1"#,
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
                return self.cancel_tombstone_operation(&id.to_string(), Some(reason)).await;
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

    /// Get active nodes based on status column and recent heartbeat.
    ///
    /// Returns nodes where `status = 'active'` AND `last_heartbeat_at`
    /// is within the last 5 minutes (or configured stale threshold).
    pub async fn get_active_nodes(&self) -> DbResult<Vec<NodeManifest>> {
        let stale_secs = node_heartbeat_stale_after().as_secs() as i64;
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
            WHERE status = 'active'
              AND last_heartbeat_at > NOW() - make_interval(secs => $1::float8)
            ORDER BY node_name, version
            "#,
            stale_secs as f64
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get active nodes"))
    }

    /// Get node health status
    pub async fn get_node_health(&self, stale_after: Duration) -> DbResult<NodeHealthSummary> {
        let cutoff = Timestamp::now()
            - sinex_primitives::temporal::Duration::seconds(stale_after.as_secs() as i64);

        let row = sqlx::query!(
            r#"
            WITH node_status AS (
                SELECT
                    node_name,
                    BOOL_OR(
                        status = 'active'
                        AND last_heartbeat_at IS NOT NULL
                        AND last_heartbeat_at >= $1
                    ) AS has_live_version,
                    MAX(last_heartbeat_at) AS latest_heartbeat_at
                FROM core.node_manifests
                GROUP BY node_name
            )
            SELECT
                COUNT(*) FILTER (WHERE has_live_version) as "active_count!",
                COUNT(*) FILTER (WHERE NOT has_live_version) as "inactive_count!",
                COUNT(*) as "unique_nodes!",
                MIN(latest_heartbeat_at) as "oldest_heartbeat: sinex_primitives::temporal::Timestamp"
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
            oldest_heartbeat: row.oldest_heartbeat,
        })
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
        // Check database connectivity
        let db_connected = sqlx::query!("SELECT 1 as one")
            .fetch_one(self.pool)
            .await
            .is_ok();

        // Check extensions
        let timescaledb_version = self.get_timescaledb_version().await.ok().flatten();
        let uuid_v7_works = self.test_uuid_v7_generation().await.is_ok();
        let json_schema_works = self.test_json_schema_validation().await.is_ok();

        // Check critical tables
        let events_table_exists = self.table_exists("core", "events").await.unwrap_or(false);

        // Get node health
        let node_health = self
            .get_node_health(node_heartbeat_stale_after())
            .await
            .ok();

        Ok(SystemHealthReport {
            db_connected,
            timescaledb_version,
            uuid_v7_generation_works: uuid_v7_works,
            json_schema_extension_works: json_schema_works,
            events_table_exists,
            node_health,
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

/// Node health summary
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeHealthSummary {
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_nodes: i64,
    pub oldest_heartbeat: Option<Timestamp>,
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
    pub timescaledb_version: Option<String>,
    pub uuid_v7_generation_works: bool,
    pub json_schema_extension_works: bool,
    pub events_table_exists: bool,

    pub node_health: Option<NodeHealthSummary>,
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
        if !operation.state.is_cancellable() {
            return Err(SinexError::invalid_state(format!(
                "Operation cannot be cancelled (state: {:?})",
                operation.state
            )));
        }

        operation.state = TombstoneOperationState::Cancelled;
        operation.phase = operation.state.into();
        operation.finished_at = Some(Timestamp::now().format_rfc3339());
        operation.error_details = reason.map(|reason| format!("Cancelled: {reason}"));

        self.update_tombstone_operation(
            operation_id,
            OperationStatus::Cancelled,
            serde_json::to_value(&operation)?,
            record.preview_summary,
            Some("Tombstone operation cancelled"),
            None,
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
        let phase = state.map(|state| serde_json::to_value(state))
            .transpose()?
            .and_then(|value| value.as_str().map(str::to_string));

        let mut qb = sqlx::QueryBuilder::new(
            r#"
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
            "#,
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
