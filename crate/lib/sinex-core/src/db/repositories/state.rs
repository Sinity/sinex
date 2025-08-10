//! State repository for managing system state including checkpoints and operations log
//!
//! This repository combines management of:
//! - Processor checkpoints (tracking progress of event processing)
//! - Operations log (audit trail of system operations)

use super::checkpoints::{Checkpoint as CheckpointInput, CheckpointRecord};
use super::common::{db_error, DbResult, EnhancedRepository, Repository};
use crate::db::schema::OperationsLog;
use crate::models::RawEvent;
use crate::types::domain::{ConsumerGroup, ConsumerName, EventSource, EventType, ProcessorName};
use crate::types::Id;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::types::BigDecimal;
use sqlx::{FromRow, PgPool, Postgres, Transaction};

/// Operation log entry matching core.operations_log per TARGET_canonical.md
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Operation {
    pub operation_id: Id<Operation>,
    pub actor: String,
    pub scope: JsonValue, // { processor, mode: ingestor|automaton, window/blob filters }
    pub preview_summary: Option<JsonValue>, // { counts, cascades, churn_percent, time_quality_flips }
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub outcome: Option<String>, // success|error|cancelled
    pub error_details: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// New operation to log per TARGET_canonical.md
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewOperation {
    pub actor: String,                      // e.g., 'user@host' or 'system'
    pub scope: JsonValue, // { processor, mode: ingestor|automaton, window/blob filters }
    pub preview_summary: Option<JsonValue>, // { counts, cascades, churn_percent, time_quality_flips }
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub outcome: Option<String>, // success|error|cancelled
    pub error_details: Option<String>,
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
    // ===== Checkpoint Methods (from CheckpointRepository) =====

    /// Save a checkpoint for a processor
    pub async fn save_checkpoint(&self, checkpoint: CheckpointInput) -> DbResult<CheckpointRecord> {
        let id = Id::<CheckpointRecord>::new();
        let consumer_group = checkpoint
            .consumer_group
            .unwrap_or_else(|| "default".into());
        let consumer_name = checkpoint.consumer_name.unwrap_or_else(|| "default".into());

        sqlx::query_as!(
            CheckpointRecord,
            r#"
            INSERT INTO core.processor_checkpoints (
                id, processor_name, consumer_group, consumer_name,
                last_processed_id, last_processed_ts, checkpoint_data, state_data
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT ON CONSTRAINT unique_processor_consumer DO UPDATE SET
                last_processed_id = EXCLUDED.last_processed_id,
                last_processed_ts = EXCLUDED.last_processed_ts,
                checkpoint_data = EXCLUDED.checkpoint_data,
                state_data = EXCLUDED.state_data,
                processed_count = core.processor_checkpoints.processed_count + 1,
                last_activity = NOW(),
                updated_at = NOW(),
                checkpoint_version = core.processor_checkpoints.checkpoint_version + 1
            RETURNING 
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name: ProcessorName",
                consumer_group as "consumer_group: ConsumerGroup",
                consumer_name as "consumer_name: ConsumerName",
                last_processed_id as "last_processed_id?: Id<RawEvent>",
                last_processed_ts,
                processed_count,
                checkpoint_data,
                state_data,
                checkpoint_version,
                last_activity,
                created_at,
                updated_at
            "#,
            *id.as_ulid() as _,
            checkpoint.processor_name.as_ref(),
            consumer_group.as_ref(),
            consumer_name.as_ref(),
            checkpoint.last_processed_id.map(|id| *id.as_ulid()) as _,
            checkpoint.last_processed_ts,
            checkpoint.checkpoint_data,
            checkpoint.state_data
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "save checkpoint"))
    }

    /// Get checkpoint for a specific processor
    pub async fn get_checkpoint(&self, processor_name: &str) -> DbResult<Option<CheckpointRecord>> {
        sqlx::query_as!(
            CheckpointRecord,
            r#"
            SELECT 
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name: ProcessorName",
                consumer_group as "consumer_group: ConsumerGroup",
                consumer_name as "consumer_name: ConsumerName",
                last_processed_id as "last_processed_id?: Id<RawEvent>",
                last_processed_ts,
                processed_count,
                checkpoint_data,
                state_data,
                checkpoint_version,
                last_activity,
                created_at,
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
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name: ProcessorName",
                consumer_group as "consumer_group: ConsumerGroup",
                consumer_name as "consumer_name: ConsumerName",
                last_processed_id as "last_processed_id?: Id<RawEvent>",
                last_processed_ts,
                processed_count,
                checkpoint_data,
                state_data,
                checkpoint_version,
                last_activity,
                created_at,
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
        let result = sqlx::query!(
            "DELETE FROM core.processor_checkpoints WHERE processor_name = $1 AND consumer_group = 'default' AND consumer_name = 'default'",
            processor_name
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete checkpoint"))?;

        Ok(result.rows_affected() > 0)
    }

    // ===== Operations Log Methods =====

    /// Log an operation
    pub async fn log_operation(&self, operation: NewOperation) -> DbResult<Operation> {
        let id = Id::<Operation>::new();
        let started_at = operation.started_at.unwrap_or_else(Utc::now);

        let result = sqlx::query_as!(
            Operation,
            r#"
            INSERT INTO core.operations_log (
                operation_id, actor, scope, preview_summary,
                started_at, finished_at, outcome, error_details
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING 
                operation_id as "operation_id: Id<Operation>",
                actor,
                scope,
                preview_summary,
                started_at,
                finished_at,
                outcome,
                error_details,
                created_at
            "#,
            *id.as_ulid() as _,
            operation.actor,
            operation.scope,
            operation.preview_summary,
            started_at,
            operation.finished_at,
            operation.outcome,
            operation.error_details
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "log operation"))?;

        Ok(result)
    }

    /// Get operation by ID
    pub async fn get_operation(&self, id: &Id<Operation>) -> DbResult<Option<Operation>> {
        sqlx::query_as!(
            Operation,
            r#"
            SELECT 
                operation_id as "operation_id: Id<Operation>",
                actor,
                scope,
                preview_summary,
                started_at,
                finished_at,
                outcome,
                error_details,
                created_at
            FROM core.operations_log 
            WHERE operation_id = $1
            "#,
            *id.as_ulid() as _
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get operation"))
    }

    /// Get recent operations
    pub async fn get_recent_operations(&self, limit: i64) -> DbResult<Vec<Operation>> {
        sqlx::query_as!(
            Operation,
            r#"
            SELECT 
                operation_id as "operation_id: Id<Operation>",
                actor,
                scope,
                preview_summary,
                started_at,
                finished_at,
                outcome,
                error_details,
                created_at
            FROM core.operations_log 
            ORDER BY started_at DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent operations"))
    }

    /// Get operations by actor and scope
    pub async fn get_operations_by_actor_and_scope(
        &self,
        actor: Option<&str>,
        scope_filter: Option<JsonValue>,
        limit: Option<i64>,
    ) -> DbResult<Vec<Operation>> {
        let limit = limit.unwrap_or(100);

        let mut query_builder = sqlx::QueryBuilder::new(
            "SELECT operation_id, actor, scope, preview_summary, started_at, finished_at, outcome, error_details, created_at FROM core.operations_log WHERE 1=1"
        );

        if let Some(actor) = actor {
            query_builder.push(" AND actor = ");
            query_builder.push_bind(actor);
        }

        if let Some(scope) = scope_filter {
            query_builder.push(" AND scope @> ");
            query_builder.push_bind(scope);
        }

        query_builder.push(" ORDER BY started_at DESC LIMIT ");
        query_builder.push_bind(limit);

        let query = query_builder.build_query_as::<Operation>();
        query
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "get operations by actor and scope"))
    }

    /// Get operations by scope filter (searches JSONB scope field)
    pub async fn get_operations_by_scope(
        &self,
        scope_filter: JsonValue,
        limit: Option<i64>,
    ) -> DbResult<Vec<Operation>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            Operation,
            r#"
            SELECT 
                operation_id as "operation_id: Id<Operation>",
                actor,
                scope,
                preview_summary,
                started_at,
                finished_at,
                outcome,
                error_details,
                created_at
            FROM core.operations_log 
            WHERE scope @> $1
            ORDER BY started_at DESC
            LIMIT $2
            "#,
            scope_filter,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get operations by scope"))
    }

    /// Get operations by actor
    pub async fn get_operations_by_actor(
        &self,
        actor: &str,
        limit: Option<i64>,
    ) -> DbResult<Vec<Operation>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            Operation,
            r#"
            SELECT 
                operation_id as "operation_id: Id<Operation>",
                actor,
                scope,
                preview_summary,
                started_at,
                finished_at,
                outcome,
                error_details,
                created_at
            FROM core.operations_log 
            WHERE actor = $1
            ORDER BY started_at DESC
            LIMIT $2
            "#,
            actor,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get operations by actor"))
    }

    /// Get failed operations
    pub async fn get_failed_operations(
        &self,
        since: Option<DateTime<Utc>>,
        limit: Option<i64>,
    ) -> DbResult<Vec<Operation>> {
        let limit = limit.unwrap_or(100);
        let since = since.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));

        sqlx::query_as!(
            Operation,
            r#"
            SELECT 
                operation_id as "operation_id: Id<Operation>",
                actor,
                scope,
                preview_summary,
                started_at,
                finished_at,
                outcome,
                error_details,
                created_at
            FROM core.operations_log 
            WHERE outcome = 'error' AND started_at > $1
            ORDER BY started_at DESC
            LIMIT $2
            "#,
            since,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get failed operations"))
    }

    /// Get operation statistics
    pub async fn get_operation_statistics(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> DbResult<OperationStatistics> {
        let since = since.unwrap_or_else(|| Utc::now() - chrono::Duration::days(30));

        let result = sqlx::query!(
            r#"
            SELECT
                COUNT(*) as "total!",
                COUNT(*) FILTER (WHERE outcome = 'success') as "successful!",
                COUNT(*) FILTER (WHERE outcome = 'error') as "failed!",
                COUNT(*) FILTER (WHERE outcome = 'cancelled') as "cancelled!",
                AVG(EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000) as "avg_duration_ms"
            FROM core.operations_log
            WHERE started_at > $1
            "#,
            since
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
        processor_version: &str,
        hostname: &str,
    ) -> DbResult<ProcessorManifest> {
        sqlx::query_as!(
            ProcessorManifest,
            r#"
            INSERT INTO core.processor_manifests (
                processor_name, processor_version, processor_type, hostname
            ) VALUES (
                $1, $2, $3, $4
            )
            RETURNING 
                manifest_id,
                processor_name,
                processor_version,
                processor_type,
                hostname,
                start_time,
                end_time,
                config,
                metadata,
                created_at
            "#,
            processor_name.as_ref(),
            processor_version,
            processor_type,
            hostname
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
                manifest_id,
                processor_name,
                processor_version,
                processor_type,
                hostname,
                start_time,
                end_time,
                config,
                metadata,
                created_at
            FROM core.processor_manifests
            WHERE end_time IS NULL
            ORDER BY processor_name, hostname
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
                manifest_id,
                processor_name,
                processor_version,
                processor_type,
                hostname,
                start_time,
                end_time,
                config,
                metadata,
                created_at
            FROM core.processor_manifests
            WHERE processor_type = $1 AND end_time IS NULL
            ORDER BY processor_name, hostname
            "#,
            processor_type
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get processors by type"))
    }

    /// Update processor heartbeat by marking the end time and creating a new entry
    pub async fn update_processor_heartbeat(
        &self,
        processor_name: &ProcessorName,
        hostname: &str,
    ) -> DbResult<bool> {
        // First, mark the current processor as ended
        let _ = sqlx::query!(
            r#"
            UPDATE core.processor_manifests
            SET end_time = NOW()
            WHERE processor_name = $1 AND hostname = $2 AND end_time IS NULL
            "#,
            processor_name.as_ref(),
            hostname
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "end processor manifest"))?;

        // Create a new manifest entry to signal the processor is still alive
        let result = sqlx::query!(
            r#"
            INSERT INTO core.processor_manifests (processor_name, processor_version, processor_type, hostname)
            SELECT processor_name, processor_version, processor_type, hostname
            FROM core.processor_manifests
            WHERE processor_name = $1 AND hostname = $2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            processor_name.as_ref(),
            hostname
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update processor heartbeat"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Mark stale processors as ended
    pub async fn mark_stale_processors(&self, stale_threshold: DateTime<Utc>) -> DbResult<i64> {
        let result = sqlx::query!(
            r#"
            UPDATE core.processor_manifests
            SET end_time = NOW()
            WHERE end_time IS NULL AND start_time < $1
            "#,
            stale_threshold
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "mark stale processors"))?;

        Ok(result.rows_affected() as i64)
    }

    /// Get processor health status
    pub async fn get_processor_health(&self) -> DbResult<ProcessorHealthSummary> {
        let row = sqlx::query!(
            r#"
            SELECT 
                COUNT(*) FILTER (WHERE end_time IS NULL) as "active_count!",
                COUNT(*) FILTER (WHERE end_time IS NOT NULL) as "inactive_count!",
                COUNT(DISTINCT processor_name) as "unique_processors!",
                MIN(start_time) FILTER (WHERE end_time IS NULL) as oldest_heartbeat
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
    ) -> DbResult<Id<RawEvent>> {
        let id = Id::<crate::models::RawEvent>::new();

        sqlx::query!(
            r#"
            INSERT INTO core.events (event_id, source, event_type, host, payload)
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
            last_processed_ts: None,
            checkpoint_data: None,
            state_data: Some(state_data),
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
        let id = Id::<CheckpointRecord>::new();
        let consumer_group = checkpoint
            .consumer_group
            .unwrap_or_else(|| "default".into());
        let consumer_name = checkpoint.consumer_name.unwrap_or_else(|| "default".into());

        sqlx::query_as!(
            CheckpointRecord,
            r#"
            INSERT INTO core.processor_checkpoints (
                id, processor_name, consumer_group, consumer_name,
                last_processed_id, last_processed_ts, checkpoint_data, state_data
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT ON CONSTRAINT unique_processor_consumer DO UPDATE SET
                last_processed_id = EXCLUDED.last_processed_id,
                last_processed_ts = EXCLUDED.last_processed_ts,
                checkpoint_data = EXCLUDED.checkpoint_data,
                state_data = EXCLUDED.state_data,
                processed_count = core.processor_checkpoints.processed_count + 1,
                last_activity = NOW(),
                updated_at = NOW(),
                checkpoint_version = core.processor_checkpoints.checkpoint_version + 1
            RETURNING 
                id as "id: Id<CheckpointRecord>",
                processor_name as "processor_name: ProcessorName",
                consumer_group as "consumer_group: ConsumerGroup",
                consumer_name as "consumer_name: ConsumerName",
                last_processed_id as "last_processed_id?: Id<RawEvent>",
                last_processed_ts,
                processed_count,
                checkpoint_data,
                state_data,
                checkpoint_version,
                last_activity,
                created_at,
                updated_at
            "#,
            *id.as_ulid() as _,
            checkpoint.processor_name.as_ref(),
            consumer_group.as_ref(),
            consumer_name.as_ref(),
            checkpoint.last_processed_id.map(|id| *id.as_ulid()) as _,
            checkpoint.last_processed_ts,
            checkpoint.checkpoint_data,
            checkpoint.state_data
        )
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "save checkpoint with tx"))
    }

    /// Log operation within transaction
    pub async fn log_operation(&mut self, operation: NewOperation) -> DbResult<Operation> {
        let id = Id::<Operation>::new();
        let started_at = operation.started_at.unwrap_or_else(Utc::now);

        let result = sqlx::query_as!(
            Operation,
            r#"
            INSERT INTO core.operations_log (
                operation_id, actor, scope, preview_summary,
                started_at, finished_at, outcome, error_details
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING 
                operation_id as "operation_id: Id<Operation>",
                actor,
                scope,
                preview_summary,
                started_at,
                finished_at,
                outcome,
                error_details,
                created_at
            "#,
            *id.as_ulid() as _,
            operation.actor,
            operation.scope,
            operation.preview_summary,
            started_at,
            operation.finished_at,
            operation.outcome,
            operation.error_details
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
    pub manifest_id: i32,
    pub processor_name: String,
    pub processor_version: String,
    pub processor_type: String,
    pub hostname: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub config: Option<JsonValue>,
    pub metadata: Option<JsonValue>,
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
    pub last_processed_id: Option<Id<RawEvent>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repositories::DbPoolExt;
    use crate::types::{Id, Ulid};
    use chrono::Utc;
    use color_eyre::eyre::Result;
    use serde_json::json;
    use sinex_test_utils::{sinex_test, TestContext};

    #[sinex_test]
    async fn test_checkpoint_operations(ctx: TestContext) -> Result<()> {
        let repo = &ctx.pool.state();

        // Create a checkpoint
        let id = Id::<crate::models::RawEvent>::new();
        let checkpoint = CheckpointInput {
            processor_name: "test-processor".into(),
            consumer_group: None,
            consumer_name: None,
            last_processed_id: Some(id),
            last_processed_ts: Some(Utc::now()),
            checkpoint_data: Some(serde_json::json!({ "batch_size": 100 })),
            state_data: None,
        };

        let saved = repo.save_checkpoint(checkpoint).await?;
        assert_eq!(saved.processor_name.as_ref(), "test-processor");
        assert_eq!(saved.checkpoint_version, 1);

        // Update the checkpoint
        let new_id = Id::<crate::models::RawEvent>::new();
        let update = CheckpointInput::new("test-processor")
            .with_last_processed_id(new_id.clone())
            .with_last_processed_ts(Utc::now())
            .with_checkpoint_data(serde_json::json!({ "batch_size": 200 }));

        let updated = repo.save_checkpoint(update).await?;
        assert_eq!(updated.processor_name.as_ref(), "test-processor");
        assert_eq!(updated.checkpoint_version, 2);
        assert_eq!(updated.last_processed_id, Some(new_id));

        // Get checkpoint
        let retrieved = repo.get_checkpoint("test-processor").await?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().checkpoint_version, 2);

        // Delete checkpoint
        let deleted = repo.delete_checkpoint("test-processor").await?;
        assert!(deleted);

        let gone = repo.get_checkpoint("test-processor").await?;
        assert!(gone.is_none());

        Ok(())
    }

    #[sinex_test]
    async fn test_operation_logging(ctx: TestContext) -> Result<()> {
        let repo = &ctx.pool.state();

        // Log a successful operation
        let operation = NewOperation {
            actor: "ingestd@localhost".to_string(),
            scope: json!({
                "processor": "ingestd",
                "mode": "ingestor",
                "source": "fs-watcher"
            }),
            preview_summary: Some(json!({
                "events_count": 1,
                "types": ["file.created"]
            })),
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            outcome: Some("success".to_string()),
            error_details: None,
        };

        let logged = repo.log_operation(operation).await?;
        assert_eq!(logged.actor, "ingestd@localhost");
        assert_eq!(logged.outcome.as_deref(), Some("success"));
        assert!(logged.error_details.is_none());

        // Log a failed operation
        let failed_op = NewOperation {
            actor: "api-user@localhost".to_string(),
            scope: json!({
                "processor": "schema-manager",
                "mode": "automaton",
                "target": "test-schema-1.0.0"
            }),
            preview_summary: None,
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            outcome: Some("error".to_string()),
            error_details: Some("Invalid JSON schema".to_string()),
        };

        let failed_logged = repo.log_operation(failed_op).await?;
        assert_eq!(failed_logged.outcome.as_deref(), Some("error"));
        assert_eq!(
            failed_logged.error_details.as_deref(),
            Some("Invalid JSON schema")
        );

        // Get recent operations
        let recent = repo.get_recent_operations(10).await?;
        assert_eq!(recent.len(), 2);

        // Get operations by actor
        let by_actor = repo
            .get_operations_by_actor("ingestd@localhost", None)
            .await?;
        assert_eq!(by_actor.len(), 1);

        // Get failed operations
        let failed = repo.get_failed_operations(None, None).await?;
        assert_eq!(failed.len(), 1);

        Ok(())
    }

    #[sinex_test]
    async fn test_operation_statistics(ctx: TestContext) -> Result<()> {
        let repo = &ctx.pool.state();

        // Log various operations
        let operations = vec![
            ("success", None),
            ("success", None),
            ("success", None),
            ("error", Some("Test error".to_string())),
            ("cancelled", None),
        ];

        for (outcome, error_details) in operations {
            let started = Utc::now();
            let operation = NewOperation {
                actor: "test-service@localhost".to_string(),
                scope: json!({
                    "processor": "test",
                    "mode": "automaton"
                }),
                preview_summary: None,
                started_at: Some(started),
                finished_at: Some(started + chrono::Duration::milliseconds(100)),
                outcome: Some(outcome.to_string()),
                error_details,
            };

            repo.log_operation(operation).await?;
        }

        // Get statistics
        let stats = repo.get_operation_statistics(None).await?;
        assert_eq!(stats.total, 5);
        assert_eq!(stats.successful, 3);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.cancelled, 1);
        // avg_duration_ms should be around 100
        assert!(stats.avg_duration_ms.is_some());

        Ok(())
    }
}
