//! State repository for managing system state including checkpoints and operations log
//!
//! This repository combines management of:
//! - Processor checkpoints (tracking progress of event processing)
//! - Operations log (audit trail of system operations)

use super::checkpoints::{Checkpoint as CheckpointInput, CheckpointRecord};
use super::common::{db_error, DbResult, EnhancedRepository, Repository};
use crate::db::schema::OperationsLog;
use crate::types::domain::{ConsumerGroup, ConsumerName, EventSource, EventType, ProcessorName};
use crate::types::error::SinexError;
use crate::{Event, Id, JsonValue};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_schema::ulid_conversions::uuid_to_ulid;
use sqlx::types::{BigDecimal, Uuid};
use sqlx::{FromRow, PgPool, Postgres, Transaction};

/// Database record for operations_log table
/// NOTE: The actual table only has: id, operation_type, operator, scope,
/// result_status, result_message, preview_summary, duration_ms
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: Id<Operation>,
    pub operation_type: String,
    pub operator: String,
    pub scope: Option<JsonValue>,
    pub result_status: String,
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
    pub result_status: String,
    pub result_message: Option<String>,
    pub preview_summary: Option<JsonValue>,
    pub duration_ms: Option<i32>,
}

/// State repository combining checkpoints and operations
pub struct StateRepository<'a> {
    pool: &'a PgPool,
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

impl<'a> StateRepository<'a> {
    // ===== Operations Log Helpers for Replay =====

    /// Start a replay operation via core.start_operation and return the operation Id
    pub async fn start_replay_operation(
        &self,
        operator: &str,
        scope: JsonValue,
    ) -> DbResult<Id<Operation>> {
        let op_uuid: Uuid = sqlx::query_scalar!(
            r#"SELECT core.start_operation($1, $2, $3::jsonb) as "id!: Uuid""#,
            "replay",
            operator,
            scope
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "start replay operation"))?;
        let op_ulid = uuid_to_ulid(op_uuid);
        Ok(Id::<Operation>::from_ulid(op_ulid))
    }

    /// Update result_status, result_message and preview_summary for an operation
    pub async fn update_operation_meta(
        &self,
        id: &Id<Operation>,
        result_status: &str,
        result_message: Option<&str>,
        preview_summary: JsonValue,
    ) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                result_message = $3,
                preview_summary = $4
            WHERE id::uuid = $1::uuid
            "#,
            id.to_uuid(),
            result_status,
            result_message,
            preview_summary
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update operation meta"))?;
        Ok(())
    }

    /// Complete an operation via core.complete_operation(summary)
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

    /// Fail an operation via core.fail_operation(error)
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
        // ULIDs are always valid once created, but we can check for zero ULID
        if id.as_ulid().to_bytes() == [0u8; 16] {
            return Err(SinexError::validation("Operation ID cannot be zero"));
        }
        Ok(())
    }

    /// Validate a ReplayScope JSON object
    pub fn validate_replay_scope(scope: &JsonValue) -> DbResult<()> {
        // Required fields for replay scope
        if !scope.is_object() {
            return Err(SinexError::validation("ReplayScope must be a JSON object"));
        }

        let obj = scope.as_object().unwrap();

        // Check required fields
        if !obj.contains_key("target_type") {
            return Err(SinexError::validation(
                "ReplayScope missing required field: target_type",
            ));
        }

        // Validate target_type is a string and one of allowed values
        if let Some(target_type) = obj.get("target_type").and_then(|v| v.as_str()) {
            match target_type {
                "event" | "time_range" | "cascade" | "operation" => {},
                _ => return Err(SinexError::validation(
                    format!("Invalid target_type: {target_type}. Must be one of: event, time_range, cascade, operation")
                )),
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

    // ===== Checkpoint Methods (from CheckpointRepository) =====

    /// Save a checkpoint for a processor
    pub async fn save_checkpoint(&self, checkpoint: CheckpointInput) -> DbResult<CheckpointRecord> {
        let consumer_group = checkpoint
            .consumer_group
            .unwrap_or_else(|| "default".into());
        let consumer_name = checkpoint.consumer_name.unwrap_or_else(|| "default".into());

        // Start transaction to ensure atomicity of event emission and state change
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin checkpoint transaction"))?;

        // Check if this is an update or create operation
        let existing_checkpoint = sqlx::query!(
            r#"SELECT id::uuid as "id!", processed_count FROM core.processor_checkpoints WHERE processor_name = $1 AND consumer_group = $2 AND consumer_name = $3"#,
            checkpoint.processor_name.as_ref(),
            consumer_group.as_ref(),
            consumer_name.as_ref()
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| db_error(e, "check existing checkpoint"))?;

        // Determine operation type (reserved for future audit/logging enhancements)
        let _operation_type = if existing_checkpoint.is_some() {
            "update"
        } else {
            "create"
        };

        // Perform the checkpoint upsert
        let result = sqlx::query_as!(
            CheckpointRecord,
            r#"
            INSERT INTO core.processor_checkpoints (
                processor_name, consumer_group, consumer_name,
                last_processed_id, checkpoint_data, processed_count
            ) VALUES (
                $1, $2, $3, $4::uuid, $5, 1
            )
            ON CONFLICT (processor_name, consumer_group, consumer_name) DO UPDATE SET
                last_processed_id = EXCLUDED.last_processed_id,
                checkpoint_data = EXCLUDED.checkpoint_data,
                processed_count = core.processor_checkpoints.processed_count + 1,
                last_activity = NOW(),
                updated_at = NOW()
            RETURNING 
                id::uuid as "id!: Id<CheckpointRecord>",
                processor_name as "processor_name: ProcessorName",
                consumer_group as "consumer_group: ConsumerGroup",
                consumer_name as "consumer_name: ConsumerName",
                last_processed_id::uuid as "last_processed_id?: Id<Event<JsonValue>>",
                processed_count,
                checkpoint_data,
                last_activity,
                updated_at
            "#,
            checkpoint.processor_name.as_ref(),
            consumer_group.as_ref(),
            consumer_name.as_ref(),
            checkpoint.last_processed_id.map(|id| id.to_uuid()),
            checkpoint.checkpoint_data
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| db_error(e, "save checkpoint"))?;

        tx.commit()
            .await
            .map_err(|e| db_error(e, "commit checkpoint transaction"))?;

        Ok(result)
    }

    /// Get checkpoint for a specific processor
    pub async fn get_checkpoint(&self, processor_name: &str) -> DbResult<Option<CheckpointRecord>> {
        sqlx::query_as!(
            CheckpointRecord,
            r#"
            SELECT 
                id::uuid as "id!: Id<CheckpointRecord>",
                processor_name as "processor_name: ProcessorName",
                consumer_group as "consumer_group: ConsumerGroup",
                consumer_name as "consumer_name: ConsumerName",
                last_processed_id::uuid as "last_processed_id?: Id<Event<JsonValue>>",
                processed_count,
                checkpoint_data,
                last_activity,
                updated_at
            FROM core.processor_checkpoints 
            WHERE processor_name = $1 AND consumer_group = 'default' AND consumer_name = 'default'
            "#,
            processor_name
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get checkpoint"))
    }

    /// Get all checkpoints
    pub async fn get_all_checkpoints(&self) -> DbResult<Vec<CheckpointRecord>> {
        sqlx::query_as!(
            CheckpointRecord,
            r#"
            SELECT 
                id::uuid as "id!: Id<CheckpointRecord>",
                processor_name as "processor_name: ProcessorName",
                consumer_group as "consumer_group: ConsumerGroup",
                consumer_name as "consumer_name: ConsumerName",
                last_processed_id::uuid as "last_processed_id?: Id<Event<JsonValue>>",
                processed_count,
                checkpoint_data,
                last_activity,
                updated_at
            FROM core.processor_checkpoints 
            WHERE consumer_group = 'default' AND consumer_name = 'default'
            ORDER BY processor_name
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get all checkpoints"))
    }

    /// Delete checkpoint for a processor
    pub async fn delete_checkpoint(&self, processor_name: &str) -> DbResult<bool> {
        self.delete_checkpoint_with_reason(processor_name, "system", "Processor cleanup")
            .await
    }

    /// Delete checkpoint with reason logging
    pub async fn delete_checkpoint_with_reason(
        &self,
        processor_name: &str,
        operator: &str,
        reason: &str,
    ) -> DbResult<bool> {
        // Start transaction to ensure atomicity of operation logging and deletion
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin delete checkpoint transaction"))?;

        // Get the checkpoint details before deletion for logging
        let checkpoint_to_delete = sqlx::query!(
            r#"
            SELECT 
                id::uuid as "id!: Id<CheckpointRecord>",
                processed_count,
                consumer_group,
                consumer_name
            FROM core.processor_checkpoints 
            WHERE processor_name = $1 
              AND consumer_group = 'default' 
              AND consumer_name = 'default'
            "#,
            processor_name
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| db_error(e, "fetch checkpoint before deletion"))?;

        if let Some(checkpoint) = checkpoint_to_delete {
            // Log the deletion operation to operations_log
            let op_id = Id::<Operation>::new();

            sqlx::query!(
                r#"
                INSERT INTO core.operations_log (
                    id, operation_type, operator, scope, result_status, result_message
                ) VALUES (
                    $1::uuid, $2, $3, $4, $5, $6
                )
                "#,
                op_id.to_uuid(),
                "delete_checkpoint",
                operator,
                serde_json::json!({
                    "processor_name": processor_name,
                    "checkpoint_id": checkpoint.id.as_ulid().to_string(),
                    "reason": reason
                }),
                "success",
                None::<String>
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "log deletion operation"))?;

            // Now perform the actual hard deletion
            let result = sqlx::query!(
                r#"
                DELETE FROM core.processor_checkpoints 
                WHERE processor_name = $1 
                  AND consumer_group = 'default' 
                  AND consumer_name = 'default'
                "#,
                processor_name
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "delete checkpoint"))?;

            tx.commit()
                .await
                .map_err(|e| db_error(e, "commit delete checkpoint transaction"))?;

            Ok(result.rows_affected() > 0)
        } else {
            // No checkpoint to delete
            tx.rollback().await.ok();
            Ok(false)
        }
    }

    // ===== Operations Log Methods =====

    /// Log an operation
    pub async fn log_operation(&self, operation: Operation) -> DbResult<OperationRecord> {
        // Validate replay-specific scope only for replay operations; allow other shapes otherwise
        if operation.operation_type == "replay" {
            if let Some(ref scope) = operation.scope {
                Self::validate_replay_scope(scope)?;
            }
        }

        // Start transaction to ensure atomicity of event emission and state change
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin operation logging transaction"))?;

        let id = Id::<Operation>::new();

        // Perform the operation logging
        let result = sqlx::query_as!(
            OperationRecord,
            r#"
            INSERT INTO core.operations_log (
                id, operation_type, operator, scope, result_status, result_message, preview_summary, duration_ms
            ) VALUES (
                $1::uuid, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING 
                id::uuid as "id!: Id<Operation>",
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
            operation.result_status,
            operation.result_message,
            operation.preview_summary,
            operation.duration_ms
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| db_error(e, "log operation"))?;

        tx.commit()
            .await
            .map_err(|e| db_error(e, "commit operation logging transaction"))?;

        Ok(result)
    }

    /// Get operation by ID
    pub async fn get_operation(&self, id: &Id<Operation>) -> DbResult<Option<OperationRecord>> {
        Self::validate_operation_id(id)?;
        sqlx::query_as!(
            OperationRecord,
            r#"
            SELECT 
                id::uuid as "id!: Id<Operation>",
                operation_type,
                operator,
                scope,
                result_status,
                result_message,
                preview_summary,
                duration_ms
            FROM core.operations_log 
            WHERE id::uuid = $1::uuid
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
                id::uuid as "id!: Id<Operation>",
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
            "SELECT id, operation_type, operator, scope, result_status, result_message, preview_summary, duration_ms FROM core.operations_log WHERE 1=1"
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
                id::uuid as "id!: Id<Operation>",
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
                id::uuid as "id!: Id<Operation>",
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
        _since: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> DbResult<Vec<OperationRecord>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            OperationRecord,
            r#"
            SELECT 
                id::uuid as "id!: Id<Operation>",
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
        _since: Option<DateTime<Utc>>,
    ) -> DbResult<OperationStatistics> {
        let result = sqlx::query!(
            r#"
            SELECT
                COUNT(*) as "total!",
                COUNT(*) FILTER (WHERE result_status = 'success') as "successful!",
                COUNT(*) FILTER (WHERE result_status = 'failure') as "failed!",
                COUNT(*) FILTER (WHERE result_status = 'partial') as "cancelled!",
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

    // ========== Processor Manifests ==========

    /// Register a processor in the manifest
    pub async fn register_processor(
        &self,
        processor_name: &ProcessorName,
        processor_type: &str,
        version: &str,
        description: Option<&str>,
    ) -> DbResult<ProcessorManifest> {
        sqlx::query_as!(
            ProcessorManifest,
            r#"
            INSERT INTO core.processor_manifests (
                processor_name, version, processor_type, description
            ) VALUES (
                $1, $2, $3, $4
            )
            RETURNING 
                id,
                processor_name,
                processor_type,
                version,
                description,
                anchor_rule_version,
                config_schema,
                created_at
            "#,
            processor_name.as_ref(),
            version,
            processor_type,
            description
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "register processor"))
    }

    /// Get all active processors
    pub async fn get_active_processors(&self) -> DbResult<Vec<ProcessorManifest>> {
        sqlx::query_as!(
            ProcessorManifest,
            r#"
            SELECT 
                id,
                processor_name,
                processor_type,
                version,
                description,
                anchor_rule_version,
                config_schema,
                created_at
            FROM core.processor_manifests
            ORDER BY processor_name, version
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get active processors"))
    }

    /// Get processors by type
    pub async fn get_processors_by_type(
        &self,
        processor_type: &str,
    ) -> DbResult<Vec<ProcessorManifest>> {
        sqlx::query_as!(
            ProcessorManifest,
            r#"
            SELECT 
                id,
                processor_name,
                processor_type,
                version,
                description,
                anchor_rule_version,
                config_schema,
                created_at
            FROM core.processor_manifests
            WHERE processor_type = $1
            ORDER BY processor_name, version
            "#,
            processor_type
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get processors by type"))
    }

    /// Update processor deployment status
    pub async fn update_processor_status(
        &self,
        processor_name: &ProcessorName,
        version: &str,
        _status: &str,
    ) -> DbResult<bool> {
        // Since deployment_status column doesn't exist, just check if processor exists
        let processor_exists = sqlx::query!(
            "SELECT processor_name FROM core.processor_manifests WHERE processor_name = $1 AND version = $2",
            processor_name.as_ref(),
            version
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "check processor exists"))?;

        Ok(processor_exists.is_some())
    }

    /// Mark stale processors as inactive
    pub async fn mark_stale_processors(&self, _stale_threshold: DateTime<Utc>) -> DbResult<i64> {
        // Since deployment_status column doesn't exist, just return 0
        Ok(0)
    }

    /// Get processor health status
    pub async fn get_processor_health(&self) -> DbResult<ProcessorHealthSummary> {
        let row = sqlx::query!(
            r#"
            SELECT 
                COUNT(*) as "active_count!",
                0::BIGINT as "inactive_count!",
                COUNT(DISTINCT processor_name) as "unique_processors!",
                MIN(created_at) as oldest_heartbeat
            FROM core.processor_manifests
            "#
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get processor health"))?;

        Ok(ProcessorHealthSummary {
            active_count: row.active_count,
            inactive_count: row.inactive_count,
            unique_processors: row.unique_processors,
            oldest_heartbeat: row.oldest_heartbeat,
        })
    }

    // ========== System Verification Methods (from old verification module) ==========

    /// Test UUID generation functionality
    pub async fn test_uuid_generation(&self) -> DbResult<sqlx::types::Uuid> {
        let row = sqlx::query!("SELECT gen_random_uuid() as test_uuid")
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "test UUID generation"))?;

        row.test_uuid
            .ok_or_else(|| db_error(sqlx::Error::RowNotFound, "UUID generation returned NULL"))
    }

    /// Test ULID generation functionality
    pub async fn test_ulid_generation(&self) -> DbResult<String> {
        let row = sqlx::query!("SELECT gen_ulid()::text as test_ulid")
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "test ULID generation"))?;

        row.test_ulid
            .ok_or_else(|| db_error(sqlx::Error::RowNotFound, "ULID generation returned NULL"))
    }

    /// Check TimescaleDB extension version
    pub async fn get_timescaledb_version(&self) -> DbResult<Option<String>> {
        let row = sqlx::query!("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            .fetch_optional(self.pool)
            .await
            .map_err(|e| db_error(e, "check TimescaleDB version"))?;

        Ok(row.map(|r| r.extversion))
    }

    /// Test JSON schema validation functionality
    pub async fn test_json_schema_validation(&self) -> DbResult<bool> {
        let row =
            sqlx::query!(r#"SELECT json_matches_schema('{"type": "object"}', '{}') as valid"#)
                .fetch_one(self.pool)
                .await
                .map_err(|e| db_error(e, "test JSON schema validation"))?;

        Ok(row.valid.unwrap_or(false))
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

    /// Create test event for verification (returns EventId)
    pub async fn create_test_event(
        &self,
        source: &str,
        event_type: &str,
        host: &str,
        payload: JsonValue,
    ) -> DbResult<Id<Event<JsonValue>>> {
        let id = Id::<crate::db::models::Event<JsonValue>>::new();

        sqlx::query!(
            r#"
            INSERT INTO core.events (id, source, event_type, host, payload)
            VALUES ($1, $2, $3, $4, $5)
            "#,
            *id.as_ulid() as _,
            source,
            event_type,
            host,
            payload
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "create test event"))?;

        Ok(id)
    }

    /// Delete test events by source
    pub async fn cleanup_test_events_by_source(&self, source: &EventSource) -> DbResult<u64> {
        let result = sqlx::query!("DELETE FROM core.events WHERE source = $1", source.as_str())
            .execute(self.pool)
            .await
            .map_err(|e| db_error(e, "cleanup test events by source"))?;

        Ok(result.rows_affected())
    }

    /// Delete test events by source and event type
    pub async fn cleanup_test_events(
        &self,
        source: &EventSource,
        event_type: &EventType,
    ) -> DbResult<u64> {
        let result = sqlx::query!(
            "DELETE FROM core.events WHERE source = $1 AND event_type = $2",
            source.as_str(),
            event_type.as_str()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "cleanup test events"))?;

        Ok(result.rows_affected())
    }

    /// Create test checkpoint for verification
    pub async fn create_test_checkpoint(
        &self,
        processor_name: &str,
        _processed_count: i64,
        state_data: JsonValue,
    ) -> DbResult<Id<CheckpointRecord>> {
        let checkpoint = CheckpointInput {
            processor_name: processor_name.into(),
            consumer_group: Some("test".into()),
            consumer_name: Some("test".into()),
            last_processed_id: None,
            checkpoint_data: Some(state_data),
        };

        let result = self.save_checkpoint(checkpoint).await?;
        Ok(result.id)
    }

    /// Delete test checkpoint
    pub async fn delete_test_checkpoint(&self, id: Id<CheckpointRecord>) -> DbResult<bool> {
        let result = sqlx::query!(
            "DELETE FROM core.processor_checkpoints WHERE id = $1",
            *id.as_ulid() as _
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete test checkpoint"))?;

        Ok(result.rows_affected() > 0)
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
        let ulid_works = self.test_ulid_generation().await.is_ok();
        let json_schema_works = self.test_json_schema_validation().await.is_ok();

        // Check critical tables
        let events_table_exists = self.table_exists("core", "events").await.unwrap_or(false);
        let checkpoints_table_exists = self
            .table_exists("core", "processor_checkpoints")
            .await
            .unwrap_or(false);

        // Get processor health
        let processor_health = self.get_processor_health().await.ok();

        Ok(SystemHealthReport {
            db_connected,
            timescaledb_version,
            ulid_extension_works: ulid_works,
            json_schema_extension_works: json_schema_works,
            events_table_exists,
            checkpoints_table_exists,
            processor_health,
        })
    }
}

/// Transaction-scoped state repository
pub struct StateRepositoryTx<'a> {
    tx: &'a mut Transaction<'a, Postgres>,
}

impl<'a> StateRepositoryTx<'a> {
    /// Save checkpoint within transaction
    pub async fn save_checkpoint(
        &mut self,
        checkpoint: CheckpointInput,
    ) -> DbResult<CheckpointRecord> {
        let consumer_group = checkpoint
            .consumer_group
            .unwrap_or_else(|| "default".into());
        let consumer_name = checkpoint.consumer_name.unwrap_or_else(|| "default".into());

        sqlx::query_as!(
            CheckpointRecord,
            r#"
            INSERT INTO core.processor_checkpoints (
                processor_name, consumer_group, consumer_name,
                last_processed_id, checkpoint_data
            ) VALUES (
                $1, $2, $3, $4, $5
            )
            ON CONFLICT (processor_name, consumer_group, consumer_name) DO UPDATE SET
                last_processed_id = EXCLUDED.last_processed_id,
                checkpoint_data = EXCLUDED.checkpoint_data,
                processed_count = core.processor_checkpoints.processed_count + 1,
                last_activity = NOW(),
                updated_at = NOW()
            RETURNING 
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name: ProcessorName",
                consumer_group as "consumer_group: ConsumerGroup",
                consumer_name as "consumer_name: ConsumerName",
                last_processed_id as "last_processed_id?: Id<Event<JsonValue>>",
                processed_count,
                checkpoint_data,
                last_activity,
                updated_at
            "#,
            checkpoint.processor_name.as_ref(),
            consumer_group.as_ref(),
            consumer_name.as_ref(),
            checkpoint.last_processed_id.map(|id| *id.as_ulid()) as _,
            checkpoint.checkpoint_data
        )
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "save checkpoint with tx"))
    }

    /// Log operation within transaction
    pub async fn log_operation(&mut self, operation: Operation) -> DbResult<OperationRecord> {
        // Validate the scope if provided
        if let Some(ref scope) = operation.scope {
            StateRepository::validate_replay_scope(scope)?;
        }

        let id = Id::<Operation>::new();

        let result = sqlx::query_as!(
            OperationRecord,
            r#"
            INSERT INTO core.operations_log (
                id, operation_type, operator, scope, result_status, result_message, preview_summary, duration_ms
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING 
                id as "id: Id<Operation>",
                operation_type,
                operator,
                scope,
                result_status,
                result_message,
                preview_summary,
                duration_ms
            "#,
            *id.as_ulid() as _,
            operation.operation_type,
            operation.operator,
            operation.scope,
            operation.result_status,
            operation.result_message,
            operation.preview_summary,
            operation.duration_ms
        )
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "log operation with tx"))?;

        Ok(result)
    }
}

/// Processor manifest record
#[derive(Debug, sqlx::FromRow)]
pub struct ProcessorManifest {
    pub id: i32,
    pub processor_name: String,
    pub processor_type: String,
    pub version: String,
    pub description: Option<String>,
    pub anchor_rule_version: Option<i32>,
    pub config_schema: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
}

/// Processor health summary
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessorHealthSummary {
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_processors: i64,
    pub oldest_heartbeat: Option<DateTime<Utc>>,
}

/// Checkpoint gap information
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CheckpointGap {
    pub processor_name: String,
    pub last_processed_id: Option<crate::EventId>,
    pub processed_count: Option<i64>,
    pub last_activity: Option<DateTime<Utc>>,
    pub events_after_checkpoint: Option<i64>,
    pub first_unprocessed_event_time: Option<DateTime<Utc>>,
    pub last_unprocessed_event_time: Option<DateTime<Utc>>,
    pub processing_delay_seconds: Option<i64>,
}

/// Processor status check result
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ProcessorStatusCheck {
    pub processor_name: String,
    pub last_checkpoint: Option<DateTime<Utc>>,
    pub minutes_since_checkpoint: Option<f64>,
    pub is_stale: bool,
    pub expected_type: Option<String>,
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
    pub ulid_extension_works: bool,
    pub json_schema_extension_works: bool,
    pub events_table_exists: bool,
    pub checkpoints_table_exists: bool,
    pub processor_health: Option<ProcessorHealthSummary>,
}
