#![doc = include_str!("../doc/replay_state_machine.md")]

use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::types::ulid::Ulid;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Helper function to extract lock ID from ULID for advisory locks
#[allow(dead_code)]
fn ulid_to_lock_id(ulid: Ulid) -> i64 {
    let bytes = ulid.to_bytes();
    i64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

/// Replay operation states with well-defined transitions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
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

    /// Check if state allows execution
    pub fn can_execute(&self) -> bool {
        matches!(self, ReplayState::Approved | ReplayState::Executing)
    }
}

/// Scope defining what to replay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayScope {
    /// Processor ID to replay
    pub processor_id: String,
    /// Optional time window
    pub time_window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    /// Optional material filter
    pub material_filter: Option<Vec<Ulid>>,
    /// Additional filters as JSON
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
    pub last_event_id: Option<Ulid>,
    /// Current batch number
    pub batch_number: u32,
    /// PostgreSQL savepoint ID if in transaction
    pub savepoint_id: Option<String>,
    /// Timestamp of last update
    pub updated_at: DateTime<Utc>,
}

impl Default for ReplayCheckpoint {
    fn default() -> Self {
        Self {
            processed_events: 0,
            total_events: 0,
            last_event_id: None,
            batch_number: 0,
            savepoint_id: None,
            updated_at: Utc::now(),
        }
    }
}

/// Complete replay operation record
#[derive(Debug, Clone)]
pub struct ReplayOperation {
    /// Unique operation ID
    pub operation_id: Ulid,
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
    pub created_at: DateTime<Utc>,
    /// Who approved (if approved)
    pub approved_by: Option<String>,
    /// When approved
    pub approved_at: Option<DateTime<Utc>>,
    /// Which node is executing
    pub executor_node: Option<String>,
    /// When execution started
    pub started_at: Option<DateTime<Utc>>,
    /// When execution finished
    pub finished_at: Option<DateTime<Utc>>,
    /// Outcome (success, error, cancelled)
    pub outcome: Option<String>,
    /// Error details if failed
    pub error_details: Option<String>,
}

/// State machine for managing replay operations
pub struct ReplayStateMachine {
    pool: PgPool,
}

impl ReplayStateMachine {
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
        let operation_id = Ulid::new();
        let now = Utc::now();

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
        let meta_json = serde_json::json!({
            "state": "planning",
            "checkpoint": operation.checkpoint,
            "actor": operation.actor,
            "created_at": operation.created_at,
            "approved_by": null,
            "approved_at": null,
            "executor_node": null,
            "started_at": null,
            "finished_at": null,
            "outcome": null,
            "error_details": null,
            "preview": null
        });
        operation.preview_summary = Some(meta_json.clone());

        // Create entry via repository helper and then set meta
        let state_repo = self.pool.state();
        let op_id = state_repo
            .start_replay_operation(&actor, serde_json::to_value(&scope)?)
            .await
            .map_err(|e| eyre!("start_replay_operation failed: {}", e))?;
        let meta = serde_json::json!({
            "state": "planning",
            "checkpoint": operation.checkpoint,
            "actor": operation.actor,
            "created_at": operation.created_at,
            "approved_by": null,
            "approved_at": null,
            "executor_node": null,
            "started_at": null,
            "finished_at": null,
            "outcome": null,
            "error_details": null,
            "preview": null
        });
        state_repo
            .update_operation_meta(&op_id, "running", Some("planning"), meta)
            .await
            .map_err(|e| eyre!("update_operation_meta failed: {}", e))?;

        info!(
            "Created replay operation {} in Planning state",
            operation_id
        );

        Ok(operation)
    }

    /// Load existing operation
    pub async fn load_operation(&self, operation_id: Ulid) -> Result<ReplayOperation> {
        let row = sqlx::query(
            "SELECT operator, scope, preview_summary FROM core.operations_log WHERE id = $1",
        )
        .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
        .fetch_one(&self.pool)
        .await?;

        let operator: String = row.try_get("operator")?;
        let scope_val: serde_json::Value = row.try_get("scope")?;
        let preview: Option<serde_json::Value> = row.try_get("preview_summary").unwrap_or(None);
        let meta_val = preview.unwrap_or(serde_json::json!({"state": "planning"}));

        let op = Self::decode_meta_to_operation(operation_id, operator, scope_val, meta_val)?;
        Ok(op)
    }

    /// Transition to new state
    pub async fn transition(&self, operation_id: Ulid, new_state: ReplayState) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.transition_with_tx(&mut tx, operation_id, new_state)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Transition with existing transaction
    pub async fn transition_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation_id: Ulid,
        new_state: ReplayState,
    ) -> Result<()> {
        // Load current meta JSON
        let row =
            sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1 FOR UPDATE")
                .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
                .fetch_one(&mut **tx)
                .await?;
        let preview: Option<serde_json::Value> = row.try_get("preview_summary").unwrap_or(None);
        let mut meta = Self::decode_meta_json(preview)?;

        if !meta.state.can_transition_to(new_state) {
            return Err(eyre!(
                "Invalid state transition: {:?} -> {:?}",
                meta.state,
                new_state
            ));
        }

        let now = Utc::now();
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

        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query(
            "UPDATE core.operations_log SET result_status = $2, result_message = $3, preview_summary = $4 WHERE id = $1",
        )
        .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
        .bind(status)
        .bind(msg)
        .bind(meta_json)
        .execute(&mut **tx)
        .await?;

        info!("Transitioned operation {} to {:?}", operation_id, new_state);

        Ok(())
    }

    /// Update preview summary
    pub async fn update_preview(
        &self,
        operation_id: Ulid,
        preview: serde_json::Value,
    ) -> Result<()> {
        let row = sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1")
            .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
            .fetch_one(&self.pool)
            .await?;
        let mut meta = Self::decode_meta_json(row.try_get("preview_summary").unwrap_or(None))?;
        if meta.state == ReplayState::Planning {
            meta.state = ReplayState::Previewed;
        }
        meta.preview = Some(preview);
        let meta_json = serde_json::to_value(&meta)?;
        let (status, msg) = Self::map_state_to_status(&meta.state);
        sqlx::query(
            "UPDATE core.operations_log SET preview_summary = $2, result_status = $3, result_message = $4 WHERE id = $1",
        )
        .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
        .bind(meta_json)
        .bind(status)
        .bind(msg)
        .execute(&self.pool)
        .await?;

        info!("Updated preview for operation {}", operation_id);
        Ok(())
    }

    /// Approve operation for execution
    pub async fn approve(&self, operation_id: Ulid, approver: String) -> Result<()> {
        let now = Utc::now();
        let row = sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1")
            .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
            .fetch_one(&self.pool)
            .await?;
        let mut meta = Self::decode_meta_json(row.try_get("preview_summary").unwrap_or(None))?;
        if meta.state != ReplayState::Previewed {
            return Err(eyre!("Operation must be in Previewed state to approve"));
        }
        meta.state = ReplayState::Approved;
        meta.approved_by = Some(approver.clone());
        meta.approved_at = Some(now);
        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query(
            "UPDATE core.operations_log SET result_status = $2, result_message = $3, preview_summary = $4 WHERE id = $1",
        )
        .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
        .bind(status)
        .bind(msg)
        .bind(meta_json)
        .execute(&self.pool)
        .await?;

        info!("Operation {} approved by {}", operation_id, approver);
        Ok(())
    }

    /// Update checkpoint
    pub async fn update_checkpoint(
        &self,
        operation_id: Ulid,
        checkpoint: &ReplayCheckpoint,
    ) -> Result<()> {
        let row = sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1")
            .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
            .fetch_one(&self.pool)
            .await?;
        let mut meta = Self::decode_meta_json(row.try_get("preview_summary").unwrap_or(None))?;
        meta.checkpoint = checkpoint.clone();
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query("UPDATE core.operations_log SET preview_summary = $2 WHERE id = $1")
            .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
            .bind(meta_json)
            .execute(&self.pool)
            .await?;

        debug!(
            "Updated checkpoint for operation {}: {}/{}",
            operation_id, checkpoint.processed_events, checkpoint.total_events
        );
        Ok(())
    }

    /// Mark operation as failed
    pub async fn mark_failed(&self, operation_id: Ulid, error: String) -> Result<()> {
        let row = sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1")
            .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
            .fetch_one(&self.pool)
            .await?;
        let mut meta = Self::decode_meta_json(row.try_get("preview_summary").unwrap_or(None))?;
        meta.state = ReplayState::Failed;
        meta.finished_at = Some(Utc::now());
        meta.error_details = Some(error.clone());
        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query(
            "UPDATE core.operations_log SET result_status = $2, result_message = $3, preview_summary = $4 WHERE id = $1",
        )
        .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
        .bind(status)
        .bind(msg)
        .bind(meta_json)
        .execute(&self.pool)
        .await?;

        warn!("Operation {} failed: {}", operation_id, error);
        Ok(())
    }

    /// Mark operation as cancelled
    pub async fn cancel(&self, operation_id: Ulid, reason: String) -> Result<()> {
        let row = sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1")
            .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
            .fetch_one(&self.pool)
            .await?;
        let mut meta = Self::decode_meta_json(row.try_get("preview_summary").unwrap_or(None))?;
        if meta.state.is_terminal() {
            return Ok(());
        }
        meta.state = ReplayState::Cancelled;
        meta.finished_at = Some(Utc::now());
        meta.outcome = Some("cancelled".into());
        meta.error_details = Some(reason.clone());
        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        sqlx::query(
            "UPDATE core.operations_log SET result_status = $2, result_message = $3, preview_summary = $4 WHERE id = $1",
        )
        .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
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
        operation_id: Ulid,
        executor_node: String,
    ) -> Result<bool> {
        // Use PostgreSQL advisory lock based on operation_id hash
        // Convert first 8 bytes of ULID to i64 for lock ID
        let bytes = operation_id.to_bytes();
        let lock_id = i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

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
            let row = sqlx::query("SELECT preview_summary FROM core.operations_log WHERE id = $1")
                .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
                .fetch_one(&self.pool)
                .await?;
            let mut meta = Self::decode_meta_json(row.try_get("preview_summary").unwrap_or(None))?;
            meta.executor_node = Some(executor_node.clone());
            let meta_json = serde_json::to_value(&meta)?;
            sqlx::query("UPDATE core.operations_log SET preview_summary = $2 WHERE id = $1")
                .bind(sqlx::types::Uuid::from_bytes(operation_id.to_bytes()))
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
    pub async fn release_execution_lock(&self, operation_id: Ulid) -> Result<()> {
        // Convert first 8 bytes of ULID to i64 for lock ID
        let bytes = operation_id.to_bytes();
        let lock_id = i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

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

    /// List operations by state
    pub async fn list_by_state(&self, state: ReplayState) -> Result<Vec<ReplayOperation>> {
        // Fetch running operations and filter by embedded state
        let rows = sqlx::query(
            "SELECT id, operator, scope, preview_summary FROM core.operations_log WHERE result_status = 'running' ORDER BY id DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut operations = Vec::new();
        for row in rows {
            let uuid: sqlx::types::Uuid = row.try_get("id")?;
            let id_ulid = Ulid::from_bytes(*uuid.as_bytes())?;
            let operator: String = row.try_get("operator")?;
            let scope_val: serde_json::Value = row.try_get("scope")?;
            let preview: Option<serde_json::Value> = row.try_get("preview_summary").unwrap_or(None);
            let meta = Self::decode_meta_json(preview)?;
            if meta.state == state {
                let op = Self::decode_meta_to_operation(
                    id_ulid,
                    operator,
                    scope_val,
                    serde_json::to_value(meta)?,
                )?;
                operations.push(op);
            }
        }

        Ok(operations)
    }
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
                created_at: Utc::now(),
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
        operation_id: Ulid,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetaJson {
    state: ReplayState,
    checkpoint: ReplayCheckpoint,
    actor: String,
    created_at: DateTime<Utc>,
    approved_by: Option<String>,
    approved_at: Option<DateTime<Utc>>,
    executor_node: Option<String>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    outcome: Option<String>,
    error_details: Option<String>,
    preview: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_transitions() {
        // Valid transitions
        assert!(ReplayState::Planning.can_transition_to(ReplayState::Previewed));
        assert!(ReplayState::Previewed.can_transition_to(ReplayState::Approved));
        assert!(ReplayState::Approved.can_transition_to(ReplayState::Executing));
        assert!(ReplayState::Executing.can_transition_to(ReplayState::Committing));
        assert!(ReplayState::Committing.can_transition_to(ReplayState::Completed));

        // Invalid transitions
        assert!(!ReplayState::Planning.can_transition_to(ReplayState::Executing));
        assert!(!ReplayState::Completed.can_transition_to(ReplayState::Planning));
        assert!(!ReplayState::Previewed.can_transition_to(ReplayState::Completed));

        // Terminal states
        assert!(ReplayState::Completed.is_terminal());
        assert!(ReplayState::Failed.is_terminal());
        assert!(ReplayState::Cancelled.is_terminal());
        assert!(!ReplayState::Executing.is_terminal());
    }

    #[test]
    fn test_replay_checkpoint_serialization() {
        use sinex_core::types::ulid::Ulid;

        // Create a checkpoint with all fields populated
        let checkpoint = ReplayCheckpoint {
            processed_events: 12345,
            total_events: 50000,
            last_event_id: Some(Ulid::new()),
            batch_number: 42,
            savepoint_id: Some("sp_12345".to_string()),
            updated_at: Utc::now(),
        };

        // Serialize to JSON
        let json = serde_json::to_string(&checkpoint).unwrap();

        // Deserialize back
        let deserialized: ReplayCheckpoint = serde_json::from_str(&json).unwrap();

        // Verify all fields match
        assert_eq!(checkpoint.processed_events, deserialized.processed_events);
        assert_eq!(checkpoint.total_events, deserialized.total_events);
        assert_eq!(checkpoint.last_event_id, deserialized.last_event_id);
        assert_eq!(checkpoint.batch_number, deserialized.batch_number);
        assert_eq!(checkpoint.savepoint_id, deserialized.savepoint_id);
    }

    #[test]
    fn test_replay_checkpoint_partial_serialization() {
        // Create checkpoint with optional fields as None
        let checkpoint = ReplayCheckpoint {
            processed_events: 100,
            total_events: 1000,
            last_event_id: None,
            batch_number: 1,
            savepoint_id: None,
            updated_at: Utc::now(),
        };

        // Serialize and deserialize
        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: ReplayCheckpoint = serde_json::from_str(&json).unwrap();

        // Verify None values preserved
        assert!(deserialized.last_event_id.is_none());
        assert!(deserialized.savepoint_id.is_none());
        assert_eq!(checkpoint.processed_events, deserialized.processed_events);
    }

    #[test]
    fn test_replay_scope_serialization() {
        use chrono::{TimeZone, Utc};
        use sinex_core::types::ulid::Ulid;
        use std::collections::HashMap;

        let mut filters = HashMap::new();
        filters.insert("source".to_string(), serde_json::json!("filesystem"));
        filters.insert("max_size".to_string(), serde_json::json!(1024));

        let scope = ReplayScope {
            processor_id: "test-processor".to_string(),
            time_window: Some((
                Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap(),
            )),
            material_filter: Some(vec![Ulid::new(), Ulid::new()]),
            filters,
        };

        // Test round-trip serialization
        let json = serde_json::to_string(&scope).unwrap();
        let deserialized: ReplayScope = serde_json::from_str(&json).unwrap();

        assert_eq!(scope.processor_id, deserialized.processor_id);
        assert_eq!(scope.time_window, deserialized.time_window);
        assert_eq!(
            scope.material_filter.as_ref().map(|v| v.len()),
            deserialized.material_filter.as_ref().map(|v| v.len())
        );
        assert_eq!(scope.filters.len(), deserialized.filters.len());
    }

    #[test]
    fn test_replay_operation_creation() {
        use sinex_core::types::ulid::Ulid;
        use std::collections::HashMap;

        let scope = ReplayScope {
            processor_id: "test-processor".to_string(),
            time_window: None,
            material_filter: Some(vec![Ulid::new()]),
            filters: HashMap::new(),
        };

        let operation_id = Ulid::new();
        let actor = "test-actor".to_string();
        let now = Utc::now();

        let operation = ReplayOperation {
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

        // Verify operation state
        assert_eq!(operation.state, ReplayState::Planning);
        assert_eq!(operation.actor, actor);
        assert_eq!(operation.scope.processor_id, scope.processor_id);
        assert!(operation.approved_by.is_none());
        assert!(operation.finished_at.is_none());
    }

    #[test]
    fn test_replay_state_serialization() {
        // Test state serialization for persistence
        let states = vec![
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Approved,
            ReplayState::Executing,
            ReplayState::Committing,
            ReplayState::Completed,
            ReplayState::Failed,
            ReplayState::Cancelled,
        ];

        for state in states {
            let json = serde_json::to_string(&state).unwrap();
            let deserialized: ReplayState = serde_json::from_str(&json).unwrap();

            // States should serialize to their string representation
            match state {
                ReplayState::Planning => assert_eq!(json, "\"Planning\""),
                ReplayState::Previewed => assert_eq!(json, "\"Previewed\""),
                ReplayState::Approved => assert_eq!(json, "\"Approved\""),
                ReplayState::Executing => assert_eq!(json, "\"Executing\""),
                ReplayState::Committing => assert_eq!(json, "\"Committing\""),
                ReplayState::Completed => assert_eq!(json, "\"Completed\""),
                ReplayState::Failed => assert_eq!(json, "\"Failed\""),
                ReplayState::Cancelled => assert_eq!(json, "\"Cancelled\""),
            }

            // Deserialized state should match original
            assert_eq!(format!("{:?}", state), format!("{:?}", deserialized));
        }
    }
}
