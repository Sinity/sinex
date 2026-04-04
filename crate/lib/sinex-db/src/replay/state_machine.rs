use crate::advisory_lock::AdvisoryLock;
use serde::{Deserialize, Serialize};
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{NodeName, OperationStatus, ReplayOutcome};
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::utils::ResourceGuard;
use sinex_primitives::validation::query_validation::validate_time_range;
use sqlx::postgres::types::PgRange;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Transaction};
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use tracing::{debug, info, warn};
use uuid::Uuid;

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
    /// Operator requested cancellation; executor is still unwinding.
    #[sqlx(rename = "cancelling")]
    Cancelling,
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
    #[must_use]
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
            (ReplayState::Approved, ReplayState::Failed) => true,
            (ReplayState::Approved, ReplayState::Cancelled) => true,

            // From Executing
            (ReplayState::Executing, ReplayState::Cancelling) => true,
            (ReplayState::Executing, ReplayState::Committing) => true,
            (ReplayState::Executing, ReplayState::Failed) => true,
            (ReplayState::Executing, ReplayState::Executing) => true, // Pause/resume

            // From Cancelling
            (ReplayState::Cancelling, ReplayState::Cancelled) => true,
            (ReplayState::Cancelling, ReplayState::Committing) => true,
            (ReplayState::Cancelling, ReplayState::Failed) => true,

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
    #[must_use]
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

/// Normalized replay scope filters shared by preview and execute paths.
#[derive(Debug, Clone, Default)]
pub struct ReplayScopeFilters {
    pub material_ids: Option<Vec<Uuid>>,
    pub event_types: Option<Vec<String>>,
}

impl ReplayScope {
    pub fn validate(&self) -> Result<()> {
        if let Some((start, end)) = self.time_window {
            validate_time_range(Some(start), Some(end)).map_err(|error| {
                SinexError::validation("invalid replay time_window").with_std_error(&error)
            })?;
        }
        Ok(())
    }

    /// Normalize scope filters (drop empties, dedupe values) to keep preview/execute semantics aligned.
    #[must_use]
    pub fn normalized_filters(&self) -> ReplayScopeFilters {
        let material_ids = self.material_filter.as_ref().and_then(|ids| {
            let mut seen = HashSet::new();
            let mut deduped = Vec::new();
            for id in ids {
                if seen.insert(*id) {
                    deduped.push(*id);
                }
            }
            (!deduped.is_empty()).then_some(deduped)
        });

        let event_types = self
            .filters
            .get("event_types")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                let mut seen = HashSet::new();
                let mut deduped = Vec::new();
                for value in values {
                    if let Some(name) = value.as_str() {
                        if name.trim().is_empty() {
                            continue;
                        }
                        let name = name.to_string();
                        if seen.insert(name.clone()) {
                            deduped.push(name);
                        }
                    }
                }
                deduped
            })
            .and_then(|deduped| (!deduped.is_empty()).then_some(deduped));

        ReplayScopeFilters {
            material_ids,
            event_types,
        }
    }
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
    /// `PostgreSQL` savepoint ID if in transaction
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
    fn state_json_label(state: ReplayState) -> &'static str {
        match state {
            ReplayState::Planning => "Planning",
            ReplayState::Previewed => "Previewed",
            ReplayState::Approved => "Approved",
            ReplayState::Executing => "Executing",
            ReplayState::Cancelling => "Cancelling",
            ReplayState::Committing => "Committing",
            ReplayState::Completed => "Completed",
            ReplayState::Failed => "Failed",
            ReplayState::Cancelled => "Cancelled",
        }
    }

    fn duration_ms(created_at: Timestamp, finished_at: Timestamp) -> i32 {
        let elapsed_ms = (finished_at - created_at).whole_milliseconds();
        elapsed_ms.clamp(0, i128::from(i32::MAX)) as i32
    }

    fn meta_duration_ms(meta: &MetaJson) -> Option<i32> {
        meta.finished_at
            .map(|finished_at| Self::duration_ms(meta.created_at, finished_at))
    }

    /// Get a reference to the database pool
    #[must_use]
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
        let normalized = scope.normalized_filters();
        let mut builder = QueryBuilder::<Postgres>::new(base);
        builder.push(" WHERE source = ");
        builder.push_bind(scope.node_id.as_str());
        builder.push(" AND ts_coided >= ");
        builder.push_bind(window.0);
        builder.push(" AND ts_coided <= ");
        builder.push_bind(window.1);
        // Replay execution replays material-root events via node scan; derived rows are rebuilt
        // causally from the fresh roots and are never used as replay roots themselves.
        builder.push(" AND source_material_id IS NOT NULL");
        builder.push(" AND source_event_ids IS NULL");

        if let Some(ids) = normalized.material_ids {
            builder.push(" AND source_material_id = ANY(");
            builder.push_bind(ids);
            builder.push(")");
        }

        if let Some(names) = normalized.event_types {
            builder.push(" AND event_type = ANY(");
            builder.push_bind(names);
            builder.push(")");
        }

        builder
    }

    /// Create new state machine
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Collect the root event IDs that match the given replay scope.
    ///
    /// These are the material-root events (non-derived, tied to a source material) that
    /// the scope filter selects. The same set is used internally by `generate_preview_summary`
    /// and by the execution engine. Callers can use this to run additional analysis (e.g.,
    /// cascade integrity checks) against the same root set without re-specifying the query.
    pub async fn collect_scope_root_ids(&self, scope: &ReplayScope) -> Result<Vec<Uuid>> {
        let window = Self::resolve_time_window(scope);
        let mut builder =
            Self::build_filter_query(scope, window, "SELECT id::uuid FROM core.events");
        let rows: Vec<(Uuid,)> = builder
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SinexError::database(format!("collect scope root ids: {e}")))?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Create a new replay operation.
    ///
    /// Rejects the request if a non-terminal (running) operation already exists
    /// for the same `node_id`. This prevents accidental duplicate replays that
    /// would compete for the advisory lock and confuse operators.
    pub async fn create_operation(
        &self,
        scope: ReplayScope,
        actor: String,
    ) -> Result<ReplayOperation> {
        let mut tx = self.pool.begin().await?;
        sqlx::query_scalar!(
            r#"SELECT pg_advisory_xact_lock(hashtext($1)::bigint) as "lock!""#,
            &scope.node_id
        )
        .fetch_one(tx.as_mut())
        .await
        .map_err(|e| {
            SinexError::database("Failed to acquire replay creation guard")
                .with_source(e.to_string())
                .with_context("node_id", &scope.node_id)
                .with_operation("create_replay_operation")
        })?;

        // Idempotency guard: reject if an active operation exists for this node.
        // All non-terminal replay states map to result_status = 'running'.
        let existing = sqlx::query!(
            r#"
            SELECT id::uuid AS "id!"
            FROM core.operations_log
            WHERE operation_type = 'replay'
              AND scope->>'node_id' = $1
              AND result_status = 'running'
            LIMIT 1
            "#,
            &scope.node_id
        )
        .fetch_optional(tx.as_mut())
        .await
        .map_err(|e| {
            SinexError::database("Failed to check for active replay operations")
                .with_source(e.to_string())
                .with_operation("idempotency_guard")
        })?;
        if let Some(row) = existing {
            return Err(SinexError::invalid_state(
                "A replay operation for this node is already active",
            )
            .with_context("node_id", &scope.node_id)
            .with_id("existing_operation_id", row.id.to_string())
            .with_operation("create_replay_operation"));
        }

        let now = sinex_primitives::temporal::now();
        let scope_json = serde_json::to_value(&scope)?;
        let scope_window_range = scope.time_window.map(|(start, end)| {
            PgRange::from((Bound::Included(start.inner()), Bound::Included(end.inner())))
        });
        let operation_id = sqlx::query_scalar!(
            r#"SELECT core.start_operation($1, $2, $3::jsonb, $4::tstzrange)::uuid as "id!: Uuid""#,
            "replay",
            actor,
            scope_json,
            scope_window_range
        )
        .fetch_one(tx.as_mut())
        .await
        .map_err(|e| {
            SinexError::database("Failed to start replay operation")
                .with_source(e.to_string())
                .with_operation("start_replay_operation")
        })?;

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
            outcome: operation.outcome,
            error_details: operation.error_details.clone(),
            preview: None,
        };
        let meta_json = serde_json::to_value(&meta)?;
        operation.preview_summary = Some(meta_json.clone());

        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                result_message = $3,
                preview_summary = $4
            WHERE id = $1::uuid
            "#,
            operation_id,
            OperationStatus::Running.to_string(),
            Some("planning"),
            meta_json
        )
        .execute(tx.as_mut())
        .await
        .map_err(|e| {
            SinexError::database("Failed to update operation metadata")
                .with_source(e.to_string())
                .with_operation("update_operation_meta")
                .with_id("operation_id", operation_id.to_string())
        })?;
        tx.commit().await.map_err(|e| {
            SinexError::database("Failed to commit replay operation creation")
                .with_source(e.to_string())
                .with_operation("create_replay_operation")
                .with_id("operation_id", operation_id.to_string())
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

        let meta_val = row.preview_summary.ok_or_else(|| {
            SinexError::processing("Replay operation is missing preview_summary metadata")
                .with_operation("load_replay_operation")
                .with_id("operation_id", operation_id.to_string())
        })?;

        let scope_val = row.scope.ok_or_else(|| {
            SinexError::processing("Replay operation is missing scope")
                .with_operation("load_replay_operation")
                .with_id("operation_id", operation_id.to_string())
        })?;
        let op = Self::decode_meta_to_operation(operation_id, row.operator, scope_val, meta_val)?;
        Ok(op)
    }

    /// Transition to new state
    pub async fn transition(&self, operation_id: Uuid, new_state: ReplayState) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        // READ COMMITTED (default) + FOR UPDATE is sufficient for single-row
        // read-modify-write. REPEATABLE READ causes spurious serialization
        // errors when the row was recently modified by a prior transaction.
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
        .fetch_one(tx.as_mut())
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
        let duration_ms = Self::meta_duration_ms(&meta);
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                result_message = $3,
                preview_summary = $4,
                duration_ms = COALESCE($5, duration_ms)
            WHERE id = $1::uuid
            "#,
            operation_id,
            status,
            msg,
            meta_json,
            duration_ms,
        )
        .execute(tx.as_mut())
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
        let mut tx = self.pool.begin().await?;
        // READ COMMITTED (default) + FOR UPDATE is sufficient here:
        // we only read-modify-write a single row. REPEATABLE READ would
        // reject the UPDATE if any concurrent transaction modified the row
        // after our snapshot, causing spurious serialization errors.
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(tx.as_mut())
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
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;

        info!("Updated preview for operation {}", operation_id);
        Ok(())
    }

    /// Generate a preview summary for a given scope
    pub async fn generate_preview_summary(&self, scope: &ReplayScope) -> Result<serde_json::Value> {
        let window = Self::resolve_time_window(scope);
        let mut root_event_ids = self.collect_scope_root_ids(scope).await?;
        root_event_ids.sort_unstable();
        root_event_ids.dedup();

        let mut count_query = Self::build_filter_query(
            scope,
            window,
            "SELECT COUNT(*)::bigint as total FROM core.events",
        );
        count_query.push(" AND source_material_id IS NOT NULL");
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
        event_type_query.push(" AND source_material_id IS NOT NULL");
        event_type_query.push(" GROUP BY event_type ORDER BY count DESC LIMIT 5");
        let top_types: Vec<EventTypeCountRow> = event_type_query
            .build_query_as()
            .fetch_all(&self.pool)
            .await?;

        let normalized = scope.normalized_filters();
        let mut material_summary = serde_json::Value::Null;
        if let Some(materials) = normalized.material_ids.as_ref() {
            let mut material_query = Self::build_filter_query(
                scope,
                window,
                "SELECT COUNT(DISTINCT source_material_id)::bigint as count FROM core.events",
            );
            material_query.push(" AND source_material_id IS NOT NULL");
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

        // Cascade impact: expand from root IDs to find all downstream derived events
        let cascade_impact = self.preview_cascade_impact(scope).await;

        let preview = serde_json::json!({
            "node_id": scope.node_id,
            "time_window": {
                "start": window.0,
                "end": window.1,
            },
            "total_events": total,
            "root_event_ids": root_event_ids,
            "top_event_types": top_types
                .into_iter()
                .map(|row| serde_json::json!({
                    "event_type": row.event_type,
                    "count": row.count,
                }))
                .collect::<Vec<_>>(),
            "material_filter": material_summary,
            "cascade_impact": cascade_impact,
            "replay_semantics": "reexecute_material_roots_via_node_scan",
        });

        Ok(preview)
    }

    async fn load_cascade_affected_nodes(
        tx: &mut Transaction<'_, Postgres>,
        derived_ids: &[Uuid],
    ) -> Result<Vec<String>> {
        if derived_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query!(
            "SELECT DISTINCT source FROM core.events WHERE id = ANY($1::uuid[])",
            derived_ids
        )
        .fetch_all(tx.as_mut())
        .await
        .map_err(|error| {
            SinexError::database("Failed to load cascade affected nodes")
                .with_source(error.to_string())
                .with_context("derived_event_count", derived_ids.len().to_string())
                .with_operation("preview_cascade_impact")
        })
        .map(|rows| rows.into_iter().map(|row| row.source).collect())
    }

    async fn load_cascade_affected_scopes(
        tx: &mut Transaction<'_, Postgres>,
        derived_ids: &[Uuid],
    ) -> Result<Vec<(String, String)>> {
        if derived_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query!(
            "SELECT DISTINCT event_type, scope_key FROM core.events \
             WHERE id = ANY($1::uuid[]) AND scope_key IS NOT NULL",
            derived_ids
        )
        .fetch_all(tx.as_mut())
        .await
        .map_err(|error| {
            SinexError::database("Failed to load cascade affected scopes")
                .with_source(error.to_string())
                .with_context("derived_event_count", derived_ids.len().to_string())
                .with_operation("preview_cascade_impact")
        })
        .map(|rows| {
            rows.into_iter()
                .filter_map(|row| row.scope_key.map(|scope_key| (row.event_type, scope_key)))
                .collect()
        })
    }

    /// Compute cascade impact for preview: how many derived events would be archived.
    ///
    /// Uses the same cascade expansion as real execution but in a read-only transaction
    /// that gets rolled back. Returns a JSON blob with cascade stats, or null on error
    /// (preview remains useful even without cascade data).
    async fn preview_cascade_impact(&self, scope: &ReplayScope) -> serde_json::Value {
        let cascade_result: std::result::Result<serde_json::Value, SinexError> = async {
            use crate::repositories::EventRepositoryTx;

            let root_ids = self.collect_scope_root_ids(scope).await?;
            if root_ids.is_empty() {
                return Ok(serde_json::Value::Null);
            }

            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| SinexError::database(format!("cascade preview begin: {e}")))?;

            // Phase 1: cascade expansion via repo_tx (borrows &mut tx)
            let (all_cascade_ids, derived_ids) = {
                let mut repo_tx = EventRepositoryTx::new(&mut tx);
                let session_id = format!("preview_{}", Uuid::now_v7().simple());

                let table_name = repo_tx
                    .prepare_cascade_session(&session_id, false)
                    .await
                    .map_err(|e| SinexError::database(format!("prepare cascade: {e}")))?;
                repo_tx
                    .populate_cascade_roots(&table_name, &root_ids)
                    .await
                    .map_err(|e| SinexError::database(format!("populate roots: {e}")))?;
                repo_tx
                    .expand_cascade(&table_name, 64)
                    .await
                    .map_err(|e| SinexError::database(format!("expand cascade: {e}")))?;

                let deps = repo_tx
                    .get_event_dependencies(&table_name)
                    .await
                    .map_err(|e| SinexError::database(format!("get deps: {e}")))?;

                repo_tx
                    .cleanup_cascade_session(&table_name)
                    .await
                    .map_err(|e| SinexError::database(format!("cleanup cascade: {e}")))?;

                let all_ids: Vec<Uuid> = deps.iter().map(|(id, _)| *id).collect();
                let root_set: HashSet<Uuid> = root_ids.iter().copied().collect();
                let derived: Vec<Uuid> = all_ids
                    .iter()
                    .filter(|id| !root_set.contains(id))
                    .copied()
                    .collect();
                (all_ids, derived)
            };
            // repo_tx dropped — tx is free to use directly

            // Phase 2: query metadata for derived events
            let affected_nodes = Self::load_cascade_affected_nodes(&mut tx, &derived_ids).await?;
            let affected_scopes = Self::load_cascade_affected_scopes(&mut tx, &derived_ids).await?;

            // Roll back — this is preview only, no persistent state change
            tx.rollback()
                .await
                .map_err(|e| SinexError::database(format!("cascade preview rollback: {e}")))?;

            Ok(serde_json::json!({
                "cascade_total": all_cascade_ids.len(),
                "direct_events": root_ids.len(),
                "derived_events": derived_ids.len(),
                "affected_nodes": affected_nodes,
                "affected_scopes": affected_scopes.into_iter()
                    .map(|(et, sk)| serde_json::json!({"event_type": et, "scope_key": sk}))
                    .collect::<Vec<_>>(),
            }))
        }
        .await;

        match cascade_result {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "Failed to compute cascade impact for preview");
                serde_json::Value::Null
            }
        }
    }

    /// Approve operation for execution
    pub async fn approve(&self, operation_id: Uuid, approver: String) -> Result<()> {
        let now = sinex_primitives::temporal::now();
        let mut tx = self.pool.begin().await?;
        // READ COMMITTED (default) + FOR UPDATE is sufficient for single-row
        // read-modify-write. REPEATABLE READ causes spurious serialization
        // errors when the row was recently modified by a prior transaction.
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(tx.as_mut())
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
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;

        info!("Operation {} approved by {}", operation_id, approver);
        Ok(())
    }

    /// Atomically approve a previewed operation and transition it into execution.
    pub async fn submit_previewed_for_execution(
        &self,
        operation_id: Uuid,
        approver: String,
        executor_node: NodeName,
    ) -> Result<ReplayOperation> {
        let now = sinex_primitives::temporal::now();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query!(
            r#"
            SELECT operator, scope, preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(tx.as_mut())
        .await?;

        let scope_val = row.scope.ok_or_else(|| {
            SinexError::processing("Replay operation is missing scope")
                .with_operation("submit_replay_operation")
                .with_id("operation_id", operation_id.to_string())
        })?;
        let mut meta = Self::decode_meta_json(row.preview_summary)?;
        if meta.state != ReplayState::Previewed {
            return Err(SinexError::invalid_state(
                "Operation must be in Previewed state to submit",
            )
            .with_context("current_state", format!("{:?}", meta.state))
            .with_id("operation_id", operation_id.to_string())
            .with_operation("submit_replay_operation"));
        }

        let preview = meta.preview.clone().ok_or_else(|| {
            SinexError::invalid_state(
                "Operation is missing preview summary; run preview before submit",
            )
            .with_id("operation_id", operation_id.to_string())
            .with_operation("submit_replay_operation")
        })?;
        let preview_summary: StoredReplayPreviewSummary =
            serde_json::from_value(preview).map_err(|error| {
                SinexError::invalid_state("Replay preview summary is invalid")
                    .with_id("operation_id", operation_id.to_string())
                    .with_operation("submit_replay_operation")
                    .with_std_error(&error)
            })?;
        if preview_summary.total_events == 0 {
            return Err(SinexError::invalid_state(
                "Operation preview matches zero events; refresh preview before submit",
            )
            .with_id("operation_id", operation_id.to_string())
            .with_operation("submit_replay_operation"));
        }
        if preview_summary.root_event_ids.is_empty() {
            return Err(SinexError::invalid_state(
                "Operation preview is missing root_event_ids; refresh preview before submit",
            )
            .with_id("operation_id", operation_id.to_string())
            .with_operation("submit_replay_operation"));
        }
        if preview_summary.root_event_ids.len() as u64 != preview_summary.total_events {
            return Err(SinexError::invalid_state(
                "Operation preview summary is inconsistent with total_events",
            )
            .with_context("total_events", preview_summary.total_events.to_string())
            .with_context(
                "root_event_ids",
                preview_summary.root_event_ids.len().to_string(),
            )
            .with_id("operation_id", operation_id.to_string())
            .with_operation("submit_replay_operation"));
        }

        meta.state = ReplayState::Executing;
        meta.approved_by = Some(approver.clone());
        meta.approved_at = Some(now);
        meta.started_at = Some(now);
        meta.finished_at = None;
        meta.outcome = None;
        meta.error_details = None;
        meta.executor_node = Some(executor_node.clone());
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
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;

        info!(
            operation_id = %operation_id,
            approver = %approver,
            executor_node = %executor_node,
            "Atomically submitted replay operation for execution"
        );

        Self::decode_meta_to_operation(operation_id, row.operator, scope_val, meta_json)
    }

    /// Update checkpoint
    pub async fn update_checkpoint(
        &self,
        operation_id: Uuid,
        checkpoint: &ReplayCheckpoint,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        // READ COMMITTED (default) + FOR UPDATE is sufficient for single-row
        // read-modify-write. REPEATABLE READ causes spurious serialization
        // errors when the row was recently modified by a prior transaction.
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(tx.as_mut())
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
        .execute(tx.as_mut())
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
        let mut tx = self.pool.begin().await?;
        // READ COMMITTED (default) + FOR UPDATE is sufficient for single-row
        // read-modify-write. REPEATABLE READ causes spurious serialization
        // errors when the row was recently modified by a prior transaction.
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(tx.as_mut())
        .await?;
        let mut meta = Self::decode_meta_json(row.preview_summary)?;
        if meta.state.is_terminal() {
            tx.commit().await?;
            tracing::warn!(operation_id = %operation_id, current_status = ?meta.state, "Cannot mark already-terminal operation as failed — failure report not persisted");
            return Ok(());
        }
        if !meta.state.can_transition_to(ReplayState::Failed) {
            tx.commit().await?;
            return Err(SinexError::invalid_state(
                "Operation cannot transition to Failed from current state",
            )
            .with_context("current_state", format!("{:?}", meta.state))
            .with_id("operation_id", operation_id.to_string())
            .with_operation("mark_failed"));
        }
        meta.state = ReplayState::Failed;
        meta.finished_at = Some(sinex_primitives::temporal::now());
        meta.outcome = Some(ReplayOutcome::Failed);
        meta.error_details = Some(error.clone());
        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        let duration_ms = Self::meta_duration_ms(&meta);
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                result_message = $3,
                preview_summary = $4,
                duration_ms = COALESCE($5, duration_ms)
            WHERE id = $1::uuid
            "#,
            operation_id,
            status,
            msg,
            meta_json,
            duration_ms,
        )
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;

        warn!("Operation {} failed: {}", operation_id, error);
        Ok(())
    }

    /// Mark operation as cancelled
    pub async fn cancel(&self, operation_id: Uuid, reason: String) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        // READ COMMITTED (default) + FOR UPDATE is sufficient for single-row
        // read-modify-write. REPEATABLE READ causes spurious serialization
        // errors when the row was recently modified by a prior transaction.
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(tx.as_mut())
        .await?;
        let mut meta = Self::decode_meta_json(row.preview_summary)?;
        if meta.state.is_terminal() {
            tx.commit().await?;
            return Ok(());
        }
        let target_state = if meta.state == ReplayState::Executing {
            ReplayState::Cancelling
        } else {
            ReplayState::Cancelled
        };

        if !meta.state.can_transition_to(target_state) {
            tx.commit().await?;
            return Err(SinexError::invalid_state(
                "Operation cannot transition to Cancelled from current state",
            )
            .with_context("current_state", format!("{:?}", meta.state))
            .with_id("operation_id", operation_id.to_string())
            .with_operation("cancel_operation"));
        }
        meta.state = target_state;
        meta.error_details = Some(reason.clone());
        if target_state == ReplayState::Cancelled {
            meta.finished_at = Some(sinex_primitives::temporal::now());
            meta.outcome = Some(ReplayOutcome::Cancelled);
        } else {
            meta.finished_at = None;
            meta.outcome = None;
        }
        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        let duration_ms = Self::meta_duration_ms(&meta);
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                result_message = $3,
                preview_summary = $4,
                duration_ms = COALESCE($5, duration_ms)
            WHERE id = $1::uuid
            "#,
            operation_id,
            status,
            msg,
            meta_json,
            duration_ms,
        )
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;

        match target_state {
            ReplayState::Cancelled => info!("Operation {} cancelled: {}", operation_id, reason),
            ReplayState::Cancelling => info!(
                "Operation {} cancellation requested while executing: {}",
                operation_id, reason
            ),
            _ => unreachable!("cancel target state must be cancelled or cancelling"),
        }
        Ok(())
    }

    /// Finalize a previously requested cancellation after execution has actually stopped.
    pub async fn finish_cancellation(&self, operation_id: Uuid) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(tx.as_mut())
        .await?;
        let mut meta = Self::decode_meta_json(row.preview_summary)?;
        if meta.state == ReplayState::Cancelled {
            tx.commit().await?;
            return Ok(());
        }
        if meta.state != ReplayState::Cancelling {
            tx.commit().await?;
            return Err(SinexError::invalid_state(
                "Operation is not awaiting cancellation finalization",
            )
            .with_context("current_state", format!("{:?}", meta.state))
            .with_id("operation_id", operation_id.to_string())
            .with_operation("finish_cancel_operation"));
        }

        meta.state = ReplayState::Cancelled;
        meta.finished_at = Some(sinex_primitives::temporal::now());
        meta.outcome = Some(ReplayOutcome::Cancelled);
        let (status, msg) = Self::map_state_to_status(&meta.state);
        let meta_json = serde_json::to_value(&meta)?;
        let duration_ms = Self::meta_duration_ms(&meta);
        sqlx::query!(
            r#"
            UPDATE core.operations_log
            SET result_status = $2,
                result_message = $3,
                preview_summary = $4,
                duration_ms = COALESCE($5, duration_ms)
            WHERE id = $1::uuid
            "#,
            operation_id,
            status,
            msg,
            meta_json,
            duration_ms,
        )
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;

        info!("Operation {} cancellation finalized", operation_id);
        Ok(())
    }

    /// Acquire distributed lock for operation
    pub async fn acquire_execution_lock(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<ResourceGuard<AdvisoryLock>>> {
        let lock_key = format!("replay-execution:{operation_id}");
        let Some(lock_guard) = AdvisoryLock::try_acquire(&self.pool, &lock_key).await? else {
            return Ok(None);
        };

        Ok(Some(lock_guard))
    }

    /// Persist the executor node after execution has actually entered the Executing state.
    pub async fn set_executor_node(
        &self,
        operation_id: Uuid,
        executor_node: NodeName,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        // READ COMMITTED (default) + FOR UPDATE is sufficient for single-row
        // read-modify-write. REPEATABLE READ causes spurious serialization
        // errors when the row was recently modified by a prior transaction.
        let row = sqlx::query!(
            r#"
            SELECT preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            FOR UPDATE
            "#,
            operation_id
        )
        .fetch_one(tx.as_mut())
        .await?;
        let mut meta = Self::decode_meta_json(row.preview_summary)?;
        if meta.state != ReplayState::Executing {
            return Err(SinexError::invalid_state(
                "Cannot set replay executor node unless the operation is executing",
            )
            .with_context("current_state", format!("{:?}", meta.state))
            .with_id("operation_id", operation_id.to_string())
            .with_operation("set_replay_executor_node"));
        }
        meta.executor_node = Some(executor_node.clone());
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
        .execute(tx.as_mut())
        .await?;
        tx.commit().await?;

        info!(
            "Node {} executing replay operation {}",
            executor_node, operation_id
        );
        Ok(())
    }

    /// Recover operations stuck in Executing or Committing state, likely due to process crash.
    /// Transitions operations older than `stale_threshold` to Failed with a crash
    /// recovery reason. Returns the count of recovered operations.
    pub async fn recover_stale_executing(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<usize> {
        let threshold_secs = stale_threshold.as_secs_f64();

        // Find all running operations whose execution phase is stale.
        // started_at is stored in the preview_summary JSON blob under MetaJson.
        let stale_rows = sqlx::query!(
            r#"
            SELECT id::uuid AS "id!",
                   (preview_summary->>'started_at') AS started_at_str
            FROM core.operations_log
            WHERE operation_type = 'replay'
              AND result_status = 'running'
              AND result_message IN ('executing', 'cancelling', 'committing')
              AND (preview_summary->>'started_at')::timestamptz
                  < NOW() - make_interval(secs => $1)
            "#,
            threshold_secs,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            SinexError::database("Failed to query for stale executing replay operations")
                .with_source(e.to_string())
                .with_operation("recover_stale_executing")
        })?;

        let mut recovered = 0usize;
        for stale in stale_rows {
            let operation_id = stale.id;

            let mut tx = self.pool.begin().await.map_err(|e| {
                SinexError::database("Failed to begin recovery transaction")
                    .with_source(e.to_string())
                    .with_id("operation_id", operation_id.to_string())
                    .with_operation("recover_stale_executing")
            })?;

            let row = sqlx::query!(
                r#"
                SELECT preview_summary
                FROM core.operations_log
                WHERE id = $1::uuid
                FOR UPDATE
                "#,
                operation_id
            )
            .fetch_one(tx.as_mut())
            .await
            .map_err(|e| {
                SinexError::database("Failed to lock stale operation row")
                    .with_source(e.to_string())
                    .with_id("operation_id", operation_id.to_string())
                    .with_operation("recover_stale_executing")
            })?;

            let mut meta = Self::decode_meta_json(row.preview_summary)?;

            // Re-check state after acquiring the row lock — another gateway instance
            // may have recovered or completed this operation between our initial scan
            // and this transaction.
            if !matches!(
                meta.state,
                ReplayState::Executing | ReplayState::Cancelling | ReplayState::Committing
            ) {
                if let Err(error) = tx.rollback().await {
                    warn!(
                        operation_id = %operation_id,
                        error = %error,
                        "Failed to rollback replay recovery transaction after state changed"
                    );
                }
                continue;
            }

            let recovered_state = meta.state;
            let staleness = meta.started_at.map(|started| {
                let now = sinex_primitives::temporal::now();
                now - started
            });

            meta.state = ReplayState::Failed;
            meta.finished_at = Some(sinex_primitives::temporal::now());
            meta.outcome = Some(ReplayOutcome::Failed);
            meta.executor_node = None;
            meta.error_details = Some(format!(
                "recovered from stale {} state (likely process crash)",
                Self::state_json_label(recovered_state).to_ascii_lowercase()
            ));

            let (status, msg) = Self::map_state_to_status(&meta.state);
            let meta_json = serde_json::to_value(&meta).map_err(|e| {
                SinexError::processing("Failed to serialize recovery meta")
                    .with_source(e.to_string())
                    .with_id("operation_id", operation_id.to_string())
                    .with_operation("recover_stale_executing")
            })?;
            let duration_ms = Self::meta_duration_ms(&meta);

            sqlx::query!(
                r#"
                UPDATE core.operations_log
                SET result_status = $2,
                    result_message = $3,
                    preview_summary = $4,
                    duration_ms = COALESCE($5, duration_ms)
                WHERE id = $1::uuid
                "#,
                operation_id,
                status,
                msg,
                meta_json,
                duration_ms,
            )
            .execute(tx.as_mut())
            .await
            .map_err(|e| {
                SinexError::database("Failed to update stale operation to Failed")
                    .with_source(e.to_string())
                    .with_id("operation_id", operation_id.to_string())
                    .with_operation("recover_stale_executing")
            })?;

            tx.commit().await.map_err(|e| {
                SinexError::database("Failed to commit recovery transaction")
                    .with_source(e.to_string())
                    .with_id("operation_id", operation_id.to_string())
                    .with_operation("recover_stale_executing")
            })?;

            let staleness_desc = staleness
                .map(|d| {
                    let total_secs = d.whole_seconds().unsigned_abs();
                    format!("{}m{}s", total_secs / 60, total_secs % 60)
                })
                .unwrap_or_else(|| "unknown".to_string());

            warn!(
                operation_id = %operation_id,
                recovered_state = Self::state_json_label(recovered_state),
                stale_for = %staleness_desc,
                "Recovered stale replay operation (likely process crash)"
            );

            recovered += 1;
        }

        Ok(recovered)
    }

    /// List operations with optional filters.
    pub async fn list_operations(
        &self,
        filter_state: Option<ReplayState>,
        filter_node: Option<&str>,
        limit: Option<i64>,
    ) -> Result<Vec<ReplayOperation>> {
        let mut qb: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "SELECT id::uuid as id, operator, scope, preview_summary \
             FROM core.operations_log WHERE operation_type = 'replay'",
        );

        if let Some(node) = filter_node {
            qb.push(" AND scope->>'node_id' = ");
            qb.push_bind(node.to_string());
        }

        if let Some(state) = filter_state {
            qb.push(" AND preview_summary->>'state' = ");
            qb.push_bind(Self::state_json_label(state));
        }

        qb.push(" ORDER BY id DESC");

        if let Some(lim) = limit {
            qb.push(" LIMIT ");
            qb.push_bind(lim);
        }

        let rows = qb.build().fetch_all(&self.pool).await?;

        let mut operations = Vec::new();
        for row in rows {
            let uuid: sqlx::types::Uuid = row.try_get("id")?;
            let operation_id = uuid;
            let operator: String = row.try_get("operator")?;
            let scope_val: serde_json::Value = row.try_get("scope")?;
            let preview: Option<serde_json::Value> = row.try_get("preview_summary")?;
            let meta = Self::decode_meta_json(preview)?;

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

impl ReplayStateMachine {
    fn map_state_to_status(state: &ReplayState) -> (&'static str, &'static str) {
        match state {
            ReplayState::Completed => ("success", "completed"),
            ReplayState::Failed => ("failure", "failed"),
            ReplayState::Cancelled => ("cancelled", "cancelled"),
            ReplayState::Planning => ("running", "planning"),
            ReplayState::Previewed => ("running", "previewed"),
            ReplayState::Approved => ("running", "approved"),
            ReplayState::Executing => ("running", "executing"),
            ReplayState::Cancelling => ("running", "cancelling"),
            ReplayState::Committing => ("running", "committing"),
        }
    }

    fn decode_meta_json(v: Option<serde_json::Value>) -> Result<MetaJson> {
        let val = v.ok_or_else(|| {
            SinexError::processing("Replay operation is missing preview_summary metadata")
                .with_operation("decode_replay_meta")
        })?;
        Ok(serde_json::from_value(val)?)
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

#[derive(Debug, Deserialize)]
struct StoredReplayPreviewSummary {
    total_events: u64,
    #[serde(default)]
    root_event_ids: Vec<Uuid>,
}
