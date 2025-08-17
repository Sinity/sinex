//! Replay State Machine for persistent replay operation tracking
//!
//! This module implements a distributed state machine for replay operations,
//! enabling pause/resume, collaborative approval, and failure recovery.
//!
//! ## State Machine Overview
//!
//! The replay state machine manages the lifecycle of replay operations with these states:
//!
//! - **Planning**: Initial state, gathering scope and planning the operation
//! - **Previewed**: Preview computed, awaiting approval from authorized user
//! - **Approved**: Operation approved for execution
//! - **Executing**: Active replay in progress with checkpoint tracking
//! - **Committing**: Finalizing changes and cleanup
//! - **Completed**: Successfully finished
//! - **Failed**: Error occurred during execution
//! - **Cancelled**: User cancelled the operation
//!
//! ## State Transitions
//!
//! Valid transitions ensure operational safety:
//! ```text
//! Planning → Previewed → Approved → Executing → Committing → Completed
//!     ↓          ↓         ↓          ↓            ↓
//! Cancelled  Cancelled  Cancelled   Failed      Failed
//!     ↓          ↓         
//! Planning   Planning   
//! ```
//!
//! ## Distributed Coordination
//!
//! - PostgreSQL advisory locks prevent concurrent execution conflicts
//! - Checkpoints enable pause/resume functionality
//! - Node tracking identifies which executor is running operations
//! - Approval workflow ensures human oversight of destructive operations
//!
//! ## Error Handling and Recovery
//!
//! - Failed operations can be restarted from Planning state
//! - Checkpoints contain savepoint information for transaction rollback
//! - Detailed error logging helps with troubleshooting
//! - Operations can be cancelled at any non-terminal state

use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use sinex_core::db::query_helpers::db_error;
use sinex_core::types::ulid::Ulid;
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Helper function to extract lock ID from ULID for advisory locks
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

        // Insert into operations_log
        sqlx::query!(
            r#"
            INSERT INTO core.operations_log (
                id, actor, scope, state, checkpoint, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            operation_id as _,
            actor,
            serde_json::to_value(&scope)?,
            ReplayState::Planning as _,
            serde_json::to_value(&operation.checkpoint)?,
            now,
        )
        .execute(&self.pool)
        .await?;

        info!(
            "Created replay operation {} in Planning state",
            operation_id
        );

        Ok(operation)
    }

    /// Load existing operation
    pub async fn load_operation(&self, operation_id: Ulid) -> Result<ReplayOperation> {
        let row = sqlx::query!(
            r#"
            SELECT 
                id as "operation_id: Ulid",
                actor,
                scope,
                state as "state: ReplayState",
                preview_summary,
                checkpoint,
                created_at,
                approved_by,
                approved_at,
                executor_node,
                started_at,
                finished_at,
                outcome,
                error_details
            FROM core.operations_log
            WHERE id = $1
            "#,
            operation_id as _,
        )
        .fetch_one(&self.pool)
        .await?;

        let operation = ReplayOperation {
            operation_id: row.operation_id,
            state: row.state,
            scope: serde_json::from_value(row.scope)?,
            preview_summary: row.preview_summary,
            checkpoint: row
                .checkpoint
                .map(|c| serde_json::from_value(c))
                .transpose()?
                .unwrap_or_default(),
            actor: row.actor,
            created_at: row.created_at,
            approved_by: row.approved_by,
            approved_at: row.approved_at,
            executor_node: row.executor_node,
            started_at: row.started_at,
            finished_at: row.finished_at,
            outcome: row.outcome,
            error_details: row.error_details,
        };

        Ok(operation)
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
        // Load current state
        let current = sqlx::query!(
            r#"
            SELECT state as "state: ReplayState"
            FROM core.operations_log
            WHERE id = $1
            FOR UPDATE
            "#,
            operation_id as _,
        )
        .fetch_one(&mut **tx)
        .await?;

        // Validate transition
        if !current.state.can_transition_to(new_state) {
            return Err(eyre!(
                "Invalid state transition: {:?} -> {:?}",
                current.state,
                new_state
            ));
        }

        // Update state
        let now = Utc::now();
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET state = $2,
                started_at = CASE 
                    WHEN $2 = 'executing' AND started_at IS NULL 
                    THEN $3 
                    ELSE started_at 
                END,
                finished_at = CASE 
                    WHEN $2 IN ('completed', 'failed', 'cancelled') 
                    THEN $3 
                    ELSE finished_at 
                END
            WHERE id = $1
            "#,
            operation_id as _,
            new_state as _,
            now,
        )
        .execute(&mut **tx)
        .await?;

        info!(
            "Transitioned operation {} from {:?} to {:?}",
            operation_id, current.state, new_state
        );

        Ok(())
    }

    /// Update preview summary
    pub async fn update_preview(
        &self,
        operation_id: Ulid,
        preview: serde_json::Value,
    ) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET preview_summary = $2,
                state = CASE 
                    WHEN state = 'planning' THEN 'previewed'::text 
                    ELSE state 
                END
            WHERE id = $1
            "#,
            operation_id as _,
            preview,
        )
        .execute(&self.pool)
        .await?;

        info!("Updated preview for operation {}", operation_id);
        Ok(())
    }

    /// Approve operation for execution
    pub async fn approve(&self, operation_id: Ulid, approver: String) -> Result<()> {
        let now = Utc::now();

        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET state = 'approved',
                approved_by = $2,
                approved_at = $3
            WHERE id = $1
            AND state = 'previewed'
            "#,
            operation_id as _,
            approver,
            now,
        )
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
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET checkpoint = $2
            WHERE id = $1
            "#,
            operation_id as _,
            serde_json::to_value(checkpoint)?,
        )
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
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET state = 'failed',
                outcome = 'error',
                error_details = $2,
                finished_at = NOW()
            WHERE id = $1
            "#,
            operation_id as _,
            error,
        )
        .execute(&self.pool)
        .await?;

        warn!("Operation {} failed: {}", operation_id, error);
        Ok(())
    }

    /// Mark operation as cancelled
    pub async fn cancel(&self, operation_id: Ulid, reason: String) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET state = 'cancelled',
                outcome = 'cancelled',
                error_details = $2,
                finished_at = NOW()
            WHERE id = $1
            AND state NOT IN ('completed', 'failed', 'cancelled')
            "#,
            operation_id as _,
            reason,
        )
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
            // Update executor node
            sqlx::query!(
                r#"
                UPDATE core.operations_log
                SET executor_node = $2
                WHERE id = $1
                "#,
                operation_id as _,
                executor_node,
            )
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
        let rows = sqlx::query!(
            r#"
            SELECT 
                id as "operation_id: Ulid",
                actor,
                scope,
                state as "state: ReplayState",
                preview_summary,
                checkpoint,
                created_at,
                approved_by,
                approved_at,
                executor_node,
                started_at,
                finished_at,
                outcome,
                error_details
            FROM core.operations_log
            WHERE state = $1
            ORDER BY created_at DESC
            "#,
            state as _,
        )
        .fetch_all(&self.pool)
        .await?;

        let operations = rows
            .into_iter()
            .map(|row| {
                Ok(ReplayOperation {
                    operation_id: row.operation_id,
                    state: row.state,
                    scope: serde_json::from_value(row.scope)?,
                    preview_summary: row.preview_summary,
                    checkpoint: row
                        .checkpoint
                        .map(|c| serde_json::from_value(c))
                        .transpose()?
                        .unwrap_or_default(),
                    actor: row.actor,
                    created_at: row.created_at,
                    approved_by: row.approved_by,
                    approved_at: row.approved_at,
                    executor_node: row.executor_node,
                    started_at: row.started_at,
                    finished_at: row.finished_at,
                    outcome: row.outcome,
                    error_details: row.error_details,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(operations)
    }
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
