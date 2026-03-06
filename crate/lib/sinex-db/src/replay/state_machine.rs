use crate::Uuid;
use crate::repositories::DbPoolExt;
use serde::{Deserialize, Serialize};
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{NodeName, OperationStatus, ReplayOutcome};
use sinex_primitives::error::{Result, SinexError};
use sqlx::{Executor, PgPool, Postgres, QueryBuilder, Row, Transaction};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Helper function to extract lock ID from UUID for advisory locks
fn uuid_to_lock_id(uuid: Uuid) -> i64 {
    let bytes = uuid.as_bytes();
    i64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

/// Replay operation states with well-defined transitions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text")]
pub enum ReplayState {
    /// Initial state, gathering scope and planning
    #[sqlx(rename = "planning")]
    Planning,
    /// Preview computed, awaiting approval
    #[sqlx(rename = "previewed")]
    Previewed,
    /// Approved for execution
    #[sqlx(rename = "approved")]
    Approved,
    /// Active replay in progress
    #[sqlx(rename = "executing")]
    Executing,
    /// Finalizing changes
    #[sqlx(rename = "committing")]
    Committing,
    /// Successfully finished
    #[sqlx(rename = "completed")]
    Completed,
    /// Error occurred
    #[sqlx(rename = "failed")]
    Failed,
    /// User cancelled
    #[sqlx(rename = "cancelled")]
    Cancelled,
}

impl ReplayState {
    /// Check if transition to target state is valid
    pub fn can_transition_to(&self, target: ReplayState) -> bool {
        match (self, target) {
            // From Planning
            (ReplayState::Planning, ReplayState::Previewed) => true,
            (ReplayState::Planning, ReplayState::Cancelled) => true,

            // From Previewed
            (ReplayState::Previewed, ReplayState::Approved) => true,
            (ReplayState::Previewed, ReplayState::Cancelled) => true,
            (ReplayState::Previewed, ReplayState::Planning) => true, // Re-plan

            // From Approved
            (ReplayState::Approved, ReplayState::Executing) => true,
            (ReplayState::Approved, ReplayState::Cancelled) => true,

            // From Executing
            (ReplayState::Executing, ReplayState::Committing) => true,
            (ReplayState::Executing, ReplayState::Failed) => true,
            (ReplayState::Executing, ReplayState::Cancelled) => true,
            (ReplayState::Executing, ReplayState::Executing) => true, // Pause/resume

            // From Committing
            (ReplayState::Committing, ReplayState::Completed) => true,
            (ReplayState::Committing, ReplayState::Failed) => true,

            // Terminal states can't transition
            (ReplayState::Completed, _) => false,
            (ReplayState::Failed, ReplayState::Planning) => true, // Retry
            (ReplayState::Cancelled, ReplayState::Planning) => true, // Restart

            _ => false,
        }
    }

    /// Check if state is terminal
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled
        )
    }
}

/// Scope defining what to replay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayScope {
    /// Node ID to replay
    pub node_id: String,
    /// Optional time window
    pub time_window: Option<(Timestamp, Timestamp)>,
    /// Optional material filter
    pub material_filter: Option<Vec<Uuid>>,
    /// Additional filters as JSON
    #[serde(default)]
    pub filters: HashMap<String, serde_json::Value>,
}

/// Checkpoint for resumable execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayCheckpoint {
    /// Number of events processed
    pub processed_events: u64,
    /// Total events to process
    pub total_events: u64,
    /// Last processed event ID
    pub last_event_id: Option<Uuid>,
    /// Current batch number
    pub batch_number: u32,
    /// PostgreSQL savepoint ID if in transaction
    pub savepoint_id: Option<String>,
    /// Timestamp of last update
    pub updated_at: Timestamp,
}

impl Default for ReplayCheckpoint {
    fn default() -> Self {
        Self {
            processed_events: 0,
            total_events: 0,
            last_event_id: None,
            batch_number: 0,
            savepoint_id: None,
            updated_at: sinex_primitives::temporal::now(),
        }
    }
}

/// Complete replay operation record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayOperation {
    /// Unique operation ID
    pub operation_id: Uuid,
    /// Current state
    pub state: ReplayState,
    /// Replay scope
    pub scope: ReplayScope,
    /// Preview results (if computed)
    pub preview_summary: Option<serde_json::Value>,
    /// Execution checkpoint
    pub checkpoint: ReplayCheckpoint,
    /// Who created this operation
    pub actor: String,
    /// When operation was created
    pub created_at: Timestamp,
    /// Who approved (if approved)
    pub approved_by: Option<String>,
    /// When approved
    pub approved_at: Option<Timestamp>,
    /// Which node is executing
    pub executor_node: Option<NodeName>,
    /// When execution started
    pub started_at: Option<Timestamp>,
    /// When execution finished
    pub finished_at: Option<Timestamp>,
    /// Outcome of a terminal replay operation
    pub outcome: Option<ReplayOutcome>,
    /// Error details if failed
    pub error_details: Option<String>,
}

/// State machine for managing replay operations
pub struct ReplayStateMachine {
    pool: PgPool,
}

impl ReplayStateMachine {
    /// Get a reference to the database pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn resolve_time_window(scope: &ReplayScope) -> (Timestamp, Timestamp) {
        if let Some(window) = scope.time_window {
            window
        } else {
            let end = sinex_primitives::temporal::now();
            let start = end - time::Duration::hours(24);
            (start, end)
        }
    }

    fn build_filter_query<'a>(
        scope: &'a ReplayScope,
        window: (Timestamp, Timestamp),
        base: &'static str,
    ) -> QueryBuilder<'a, Postgres> {
        let mut builder = QueryBuilder::<Postgres>::new(base);
        builder.push(" WHERE source = ");
        builder.push_bind(scope.node_id.as_str());
        builder.push(" AND ts_coided >= ");
        builder.push_bind(window.0);
        builder.push(" AND ts_coided <= ");
        builder.push_bind(window.1);

        if let Some(materials) = scope.material_filter.as_ref() {
            let ids = materials.to_vec();
            if !ids.is_empty() {
                builder.push(" AND source_material_id = ANY(");
                builder.push_bind(ids);
                builder.push(")");
            }
        }

        if let Some(event_types) = scope.filters.get("event_types").and_then(|v| v.as_array()) {
            let names: Vec<String> = event_types
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            if !names.is_empty() {
                builder.push(" AND event_type = ANY(");
                builder.push_bind(names);
                builder.push(")");
            }
        }

        builder
    }

    /// Create new state machine
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a new replay operation
    pub async fn create_operation(
        &self,
        scope: ReplayScope,
        actor: String,
    ) -> Result<ReplayOperation> {
        let now = sinex_primitives::temporal::now();
        let state_repo = self.pool.state();
        let op_id = state_repo
            .start_replay_operation(&actor, serde_json::to_value(&scope)?, scope.time_window)
            .await
            .map_err(|e| {
                SinexError::database("Failed to start replay operation")
                    .with_source(e.to_string())
                    .with_operation("start_replay_operation")
            })?;
        let operation_id = op_id.to_uuid();

        let mut operation = ReplayOperation {
            operation_id,
            state: ReplayState::Planning,
            scope: scope.clone(),
            preview_summary: None,
            checkpoint: ReplayCheckpoint::default(),
            actor: actor.clone(),
            created_at: now,
            approved_by: None,
            approved_at: None,
            executor_node: None,
            started_at: None,
            finished_at: None,
            outcome: None,
            error_details: None,
        };
        // Encode initial meta JSON into preview_summary column
        let meta = MetaJson {
            state: operation.state,
            checkpoint: operation.checkpoint.clone(),
            actor: operation.actor.clone(),
            created_at: operation.created_at,
            approved_by: operation.approved_by.clone(),
            approved_at: operation.approved_at,
            executor_node: operation.executor_node.clone(),
            started_at: operation.started_at,
            finished_at: operation.finished_at,
            outcome: operation.outcome.clone(),
            error_details: operation.error_details.clone(),
            preview: None,
        };
        let meta_json = serde_json::to_value(&meta)?;
        operation.preview_summary = Some(meta_json.clone());

        state_repo
            .update_operation_meta(
                &op_id,
                OperationStatus::Running,
                Some("planning"),
                meta_json,
            )
            .await
            .map_err(|e| {
                SinexError::database("Failed to update operation metadata")
                    .with_source(e.to_string())
                    .with_operation("update_operation_meta")
                    .with_id("operation_id", op_id.to_string())
            })?;

        info!(
            "Created replay operation {} in Planning state",
            operation.operation_id
        );

        Ok(operation)
    }

    /// Load existing operation
    pub async fn load_operation(&self, operation_id: Uuid) -> Result<ReplayOperation> {
        let row = sqlx::query!(
            r#"
            SELECT operator, scope, preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            "#,
            operation_id
        )
        .fetch_one(&self.pool)
        .await?;

        let preview = row.preview_summary;
        let meta_val = preview.unwrap_or(serde_json::json!({"state": "planning"}));

        let scope_val = row.scope.unwrap_or_else(|| serde_json::json!({}));
        let op = Self::decode_meta_to_operation(operation_id, row.operator, scope_val, meta_val)?;
        Ok(op)
    }

    /// Transition to new state
    pub async fn transition(&self, operation_id: Uuid, new_state: ReplayState) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        set_repeatable_read(&mut tx).await?;
        self.transition_with_tx(&mut tx, operation_id, new_state)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Transition with existing transaction
    pub async fn transition_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation_id: Uuid,
        new_state: ReplayState,
    ) -> Result<()> {
        // Load current meta JSON
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(&mut **tx)
        .await?;
        let preview = row.preview_summary;
        let mut meta = Self::decode_meta_json(preview)?;

        if !meta.state.can_transition_to(new_state) {
            return Err(
                SinexError::invalid_state("Invalid state transition for replay operation")
                    .with_context("from_state", format!("{:?}", meta.state))
                    .with_context("to_state", format!("{new_state:?}"))
                    .with_operation("transition_state"),
            );
        }

        let now = sinex_primitives::temporal::now();
        meta.state = new_state;
        if meta.started_at.is_none() && matches!(new_state, ReplayState::Executing) {
            meta.started_at = Some(now);
        }
        if matches!(
            new_state,
            ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled
        ) {
            meta.finished_at = Some(now);
        }
        if matches!(new_state, ReplayState::Completed) {
            meta.outcome = Some(ReplayOutcome::Success);
            meta.error_details = None;
        }

        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                result_message = $3,
                preview_summary = $4
            WHERE id = $1::uuid
            "#,
            operation_id,
            status,
            msg,
            meta_json
        )
        .execute(&mut **tx)
        .await?;

        info!("Transitioned operation {} to {:?}", operation_id, new_state);

        Ok(())
    }

    /// Update preview summary
    pub async fn update_preview(
        &self,
        operation_id: Uuid,
        preview: serde_json::Value,
    ) -> Result<()> {
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            "#,
            operation_id
        )
        .fetch_one(&self.pool)
        .await?;
        let mut meta = Self::decode_meta_json(row.preview_summary)?;
        if meta.state == ReplayState::Planning {
            meta.state = ReplayState::Previewed;
        }
        meta.preview = Some(preview);
        let meta_json = serde_json::to_value(&meta)?;
        let (status, msg) = Self::map_state_to_status(&meta.state);
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET preview_summary = $2,
                result_status = $3,
                result_message = $4
            WHERE id = $1::uuid
            "#,
            operation_id,
            meta_json,
            status,
            msg
        )
        .execute(&self.pool)
        .await?;

        info!("Updated preview for operation {}", operation_id);
        Ok(())
    }

    /// Generate a preview summary for a given scope
    pub async fn generate_preview_summary(&self, scope: &ReplayScope) -> Result<serde_json::Value> {
        let window = Self::resolve_time_window(scope);

        let mut count_query = Self::build_filter_query(
            scope,
            window,
            "SELECT COUNT(*)::bigint as total FROM core.events",
        );
        let total: i64 = count_query
            .build_query_scalar::<Option<i64>>()
            .fetch_one(&self.pool)
            .await?
            .unwrap_or(0);

        let mut event_type_query = Self::build_filter_query(
            scope,
            window,
            "SELECT event_type, COUNT(*)::bigint as count FROM core.events",
        );
        event_type_query.push(" GROUP BY event_type ORDER BY count DESC LIMIT 5");
        let top_types: Vec<EventTypeCountRow> = event_type_query
            .build_query_as()
            .fetch_all(&self.pool)
            .await?;

        let mut material_summary = serde_json::Value::Null;
        if let Some(materials) = scope.material_filter.as_ref() {
            if !materials.is_empty() {
                let mut material_query = Self::build_filter_query(
                    scope,
                    window,
                    "SELECT COUNT(DISTINCT source_material_id)::bigint as count FROM core.events",
                );
                let distinct: i64 = material_query
                    .build_query_scalar::<Option<i64>>()
                    .fetch_one(&self.pool)
                    .await?
                    .unwrap_or(0);

                material_summary = serde_json::json!({
                    "requested": materials.len(),
                    "observed": distinct,
                });
            }
        }

        let preview = serde_json::json!({
            "node_id": scope.node_id,
            "time_window": {
                "start": window.0,
                "end": window.1,
            },
            "total_events": total,
            "top_event_types": top_types
                .into_iter()
                .map(|row| serde_json::json!({
                    "event_type": row.event_type,
                    "count": row.count,
                }))
                .collect::<Vec<_>>(),
            "material_filter": material_summary,
        });

        Ok(preview)
    }

    /// Approve operation for execution
    pub async fn approve(&self, operation_id: Uuid, approver: String) -> Result<()> {
        let now = sinex_primitives::temporal::now();
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            "#,
            operation_id
        )
        .fetch_one(&self.pool)
        .await?;
        let mut meta = Self::decode_meta_json(row.preview_summary)?;
        if meta.state != ReplayState::Previewed {
            return Err(SinexError::invalid_state(
                "Operation must be in Previewed state to approve",
            )
            .with_context("current_state", format!("{:?}", meta.state))
            .with_id("operation_id", operation_id.to_string())
            .with_operation("approve_operation"));
        }
        meta.state = ReplayState::Approved;
        meta.approved_by = Some(approver.clone());
        meta.approved_at = Some(now);
        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                result_message = $3,
                preview_summary = $4
            WHERE id = $1::uuid
            "#,
            operation_id,
            status,
            msg,
            meta_json
        )
        .execute(&self.pool)
        .await?;

        info!("Operation {} approved by {}", operation_id, approver);
        Ok(())
    }

    /// Update checkpoint
    pub async fn update_checkpoint(
        &self,
        operation_id: Uuid,
        checkpoint: &ReplayCheckpoint,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        set_repeatable_read(&mut tx).await?;
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(&mut *tx)
        .await?;
        let mut meta = Self::decode_meta_json(row.preview_summary)?;
        meta.checkpoint = checkpoint.clone();
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET preview_summary = $2
            WHERE id = $1::uuid
            "#,
            operation_id,
            meta_json
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        debug!(
            "Updated checkpoint for operation {}: {}/{}",
            operation_id, checkpoint.processed_events, checkpoint.total_events
        );
        Ok(())
    }

    /// Mark operation as failed
    pub async fn mark_failed(&self, operation_id: Uuid, error: String) -> Result<()> {
        let row =
            sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1::uuid")
                .bind(operation_id)
                .fetch_one(&self.pool)
                .await?;
        let mut meta = Self::decode_meta_json(row.try_get("preview_summary").unwrap_or(None))?;
        meta.state = ReplayState::Failed;
        meta.finished_at = Some(sinex_primitives::temporal::now());
        meta.outcome = Some(ReplayOutcome::Failed);
        meta.error_details = Some(error.clone());
        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query(
            "UPDATE core.operations_log SET result_status = $2, result_message = $3, preview_summary = $4 WHERE id = $1::uuid",
        )
        .bind(operation_id)
        .bind(status)
        .bind(msg)
        .bind(meta_json)
        .execute(&self.pool)
        .await?;

        warn!("Operation {} failed: {}", operation_id, error);
        Ok(())
    }

    /// Mark operation as cancelled
    pub async fn cancel(&self, operation_id: Uuid, reason: String) -> Result<()> {
        let row =
            sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1::uuid")
                .bind(operation_id)
                .fetch_one(&self.pool)
                .await?;
        let mut meta = Self::decode_meta_json(row.try_get("preview_summary").unwrap_or(None))?;
        if meta.state.is_terminal() {
            return Ok(());
        }
        meta.state = ReplayState::Cancelled;
        meta.finished_at = Some(sinex_primitives::temporal::now());
        meta.outcome = Some(ReplayOutcome::Cancelled);
        meta.error_details = Some(reason.clone());
        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query(
            "UPDATE core.operations_log SET result_status = $2, result_message = $3, preview_summary = $4 WHERE id = $1::uuid",
        )
        .bind(operation_id)
        .bind(status)
        .bind(msg)
        .bind(meta_json)
        .execute(&self.pool)
        .await?;

        info!("Operation {} cancelled: {}", operation_id, reason);
        Ok(())
    }

    /// Acquire distributed lock for operation
    pub async fn acquire_execution_lock(
        &self,
        operation_id: Uuid,
        executor_node: NodeName,
    ) -> Result<bool> {
        // Use PostgreSQL advisory lock based on operation_id hash
        let lock_id = uuid_to_lock_id(operation_id);

        let acquired = sqlx::query!(
            r#"
            SELECT pg_try_advisory_lock($1) as acquired
            "#,
            lock_id,
        )
        .fetch_one(&self.pool)
        .await?
        .acquired
        .unwrap_or(false);

        if acquired {
            // Update executor_node in meta JSON
            let row =
                sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1::uuid")
                    .bind(operation_id)
                    .fetch_one(&self.pool)
                    .await?;
            let mut meta = Self::decode_meta_json(row.try_get("preview_summary").unwrap_or(None))?;
            meta.executor_node = Some(executor_node.clone());
            let meta_json = serde_json::to_value(&meta)?;
            sqlx::query("UPDATE core.operations_log SET preview_summary = $2 WHERE id = $1::uuid")
                .bind(operation_id)
                .bind(meta_json)
                .execute(&self.pool)
                .await?;

            info!(
                "Node {} acquired lock for operation {}",
                executor_node, operation_id
            );
        }

        Ok(acquired)
    }

    /// Release execution lock
    pub async fn release_execution_lock(&self, operation_id: Uuid) -> Result<()> {
        let lock_id = uuid_to_lock_id(operation_id);

        sqlx::query!(
            r#"
            SELECT pg_advisory_unlock($1) as released
            "#,
            lock_id,
        )
        .fetch_one(&self.pool)
        .await?;

        debug!("Released lock for operation {}", operation_id);
        Ok(())
    }

    /// List operations optionally filtered by state
    pub async fn list_operations(
        &self,
        filter_state: Option<ReplayState>,
    ) -> Result<Vec<ReplayOperation>> {
        self.list_operations_with_executor(&self.pool, filter_state)
            .await
    }

    /// List operations optionally filtered by state using a provided executor.
    pub async fn list_operations_with_executor<'e, E>(
        &self,
        executor: E,
        filter_state: Option<ReplayState>,
    ) -> Result<Vec<ReplayOperation>>
    where
        E: Executor<'e, Database = Postgres>,
    {
        let rows = sqlx::query(
            "SELECT id::uuid as id, operator, scope, preview_summary FROM core.operations_log WHERE operation_type = 'replay' ORDER BY id DESC",
        )
        .fetch_all(executor)
        .await?;

        let mut operations = Vec::new();
        for row in rows {
            let uuid: sqlx::types::Uuid = row.try_get("id")?;
            let operation_id = uuid;
            let operator: String = row.try_get("operator")?;
            let scope_val: serde_json::Value = row.try_get("scope")?;
            let preview: Option<serde_json::Value> = row.try_get("preview_summary").unwrap_or(None);
            let meta = Self::decode_meta_json(preview)?;

            if let Some(target) = filter_state {
                if meta.state != target {
                    continue;
                }
            }

            let op = Self::decode_meta_to_operation(
                operation_id,
                operator,
                scope_val,
                serde_json::to_value(meta)?,
            )?;
            operations.push(op);
        }

        Ok(operations)
    }
}

async fn set_repeatable_read(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

impl ReplayStateMachine {
    fn map_state_to_status(state: &ReplayState) -> (&'static str, &'static str) {
        match state {
            ReplayState::Completed => ("success", "completed"),
            ReplayState::Failed => ("failure", "failed"),
            ReplayState::Cancelled => ("partial", "cancelled"),
            ReplayState::Planning => ("running", "planning"),
            ReplayState::Previewed => ("running", "previewed"),
            ReplayState::Approved => ("running", "approved"),
            ReplayState::Executing => ("running", "executing"),
            ReplayState::Committing => ("running", "committing"),
        }
    }

    fn decode_meta_json(v: Option<serde_json::Value>) -> Result<MetaJson> {
        if let Some(val) = v {
            Ok(serde_json::from_value(val)?)
        } else {
            Ok(MetaJson {
                state: ReplayState::Planning,
                checkpoint: ReplayCheckpoint::default(),
                actor: "unknown".into(),
                created_at: sinex_primitives::temporal::now(),
                approved_by: None,
                approved_at: None,
                executor_node: None,
                started_at: None,
                finished_at: None,
                outcome: None,
                error_details: None,
                preview: None,
            })
        }
    }

    fn decode_meta_to_operation(
        operation_id: Uuid,
        operator: String,
        scope_val: serde_json::Value,
        meta_val: serde_json::Value,
    ) -> Result<ReplayOperation> {
        let meta: MetaJson = serde_json::from_value(meta_val)?;
        Ok(ReplayOperation {
            operation_id,
            state: meta.state,
            scope: serde_json::from_value(scope_val)?,
            preview_summary: meta.preview.clone(),
            checkpoint: meta.checkpoint,
            actor: operator,
            created_at: meta.created_at,
            approved_by: meta.approved_by,
            approved_at: meta.approved_at,
            executor_node: meta.executor_node,
            started_at: meta.started_at,
            finished_at: meta.finished_at,
            outcome: meta.outcome,
            error_details: meta.error_details,
        })
    }
}

#[derive(sqlx::FromRow)]
struct EventTypeCountRow {
    event_type: String,
    count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetaJson {
    state: ReplayState,
    checkpoint: ReplayCheckpoint,
    actor: String,
    created_at: Timestamp,
    approved_by: Option<String>,
    approved_at: Option<Timestamp>,
    executor_node: Option<NodeName>,
    started_at: Option<Timestamp>,
    finished_at: Option<Timestamp>,
    outcome: Option<ReplayOutcome>,
    error_details: Option<String>,
    preview: Option<serde_json::Value>,
}
