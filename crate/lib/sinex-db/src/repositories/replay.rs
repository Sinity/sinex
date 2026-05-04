//! Replay repository for replay operation persistence.
//!
//! Encapsulates all raw database access for replay operations, freeing the
//! [`ReplayStateMachine`](crate::replay::ReplayStateMachine) to focus on
//! business logic (state transitions, validation, meta-JSON manipulation).

use super::common::Repository;
use crate::replay::state_machine::{
    MetaJson, ReplayScope, ReplayState,
};
use serde_json::Value as JsonValue;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::Timestamp;
use sinex_primitives::utils::ResourceGuard;
use sqlx::postgres::types::PgRange;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Transaction};
use uuid::Uuid;

/// Repository for replay-operation database access.
pub struct ReplayRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for ReplayRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

// ── Common query helpers ────────────────────────────────────────────────

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

fn duration_ms(created_at: Timestamp, finished_at: Timestamp) -> i32 {
    let elapsed_ms = (finished_at - created_at).whole_milliseconds();
    elapsed_ms.clamp(0, i128::from(i32::MAX)) as i32
}

fn meta_duration_ms(meta: &MetaJson) -> Option<i32> {
    meta.finished_at
        .map(|finished_at| duration_ms(meta.created_at, finished_at))
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
    let replay_sources = scope.replay_event_sources();
    let mut builder = QueryBuilder::<Postgres>::new(base);
    builder.push(" WHERE source = ANY(");
    builder.push_bind(replay_sources);
    builder.push(")");
    builder.push(" AND ts_coided >= ");
    builder.push_bind(window.0);
    builder.push(" AND ts_coided <= ");
    builder.push_bind(window.1);
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

// ── ReplayRepository methods ─────────────────────────────────────────────

impl<'a> ReplayRepository<'a> {
    // ── Advisory lock ────────────────────────────────────────────────────

    /// Acquire the per-node advisory lock for replay operation creation.
    pub async fn acquire_creation_guard(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        node_id: &str,
    ) -> Result<()> {
        sqlx::query_scalar!(
            r#"SELECT pg_advisory_xact_lock(hashtext($1)::bigint) as "lock!""#,
            node_id
        )
        .fetch_one(tx.as_mut())
        .await
        .map_err(|e| {
            SinexError::database("Failed to acquire replay creation guard")
                .with_source(e.to_string())
                .with_context("node_id", node_id)
                .with_operation("create_replay_operation")
        })?;
        Ok(())
    }

    /// Acquire a distributed execution lock (session-level advisory lock).
    pub async fn try_acquire_execution_lock(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<ResourceGuard<crate::advisory_lock::AdvisoryLock>>> {
        let lock_key = format!("replay-execution:{operation_id}");
        crate::advisory_lock::AdvisoryLock::try_acquire(self.pool, &lock_key).await
    }

    // ── Operation CRUD ───────────────────────────────────────────────────

    /// Check whether an active (non-terminal) replay operation already exists
    /// for the given node. Returns the existing operation ID if found.
    pub async fn check_active_operation(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        node_id: &str,
    ) -> Result<Option<Uuid>> {
        let row = sqlx::query!(
            r#"
            SELECT id::uuid AS "id!"
            FROM core.operations_log
            WHERE operation_type = 'replay'
              AND scope->>'node_id' = $1
              AND result_status = 'running'
            LIMIT 1
            "#,
            node_id
        )
        .fetch_optional(tx.as_mut())
        .await
        .map_err(|e| {
            SinexError::database("Failed to check for active replay operations")
                .with_source(e.to_string())
                .with_operation("idempotency_guard")
        })?;
        Ok(row.map(|r| r.id))
    }

    /// Create a new operation via `core.start_operation` and return its ID.
    pub async fn start_operation(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation_type: &str,
        actor: &str,
        scope_json: JsonValue,
        window_range: Option<PgRange<time::OffsetDateTime>>,
    ) -> Result<Uuid> {
        sqlx::query_scalar!(
            r#"SELECT core.start_operation($1, $2, $3::jsonb, $4::tstzrange)::uuid as "id!: Uuid""#,
            operation_type,
            actor,
            scope_json,
            window_range
        )
        .fetch_one(tx.as_mut())
        .await
        .map_err(|e| {
            SinexError::database("Failed to start replay operation")
                .with_source(e.to_string())
                .with_operation("start_replay_operation")
        })
    }

    /// Write the initial meta JSON after operation creation.
    pub async fn set_initial_meta(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation_id: Uuid,
        meta_json: JsonValue,
    ) -> Result<()> {
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
        Ok(())
    }

    /// Load an operation row (operator, scope, preview_summary) by ID.
    pub async fn load_operation_row(
        &self,
        operation_id: Uuid,
    ) -> Result<(String, JsonValue, Option<JsonValue>)> {
        let row = sqlx::query!(
            r#"
            SELECT operator, scope, preview_summary
            FROM core.operations_log
            WHERE id = $1::uuid
            "#,
            operation_id
        )
        .fetch_one(self.pool)
        .await?;

        let scope_val = row.scope.ok_or_else(|| {
            SinexError::processing("Replay operation is missing scope")
                .with_operation("load_replay_operation")
                .with_id("operation_id", operation_id.to_string())
        })?;

        let meta_val = row.preview_summary.ok_or_else(|| {
            SinexError::processing("Replay operation is missing preview_summary metadata")
                .with_operation("load_replay_operation")
                .with_id("operation_id", operation_id.to_string())
        })?;

        Ok((row.operator, scope_val, Some(meta_val)))
    }

    /// Load an operation row with full fields (used by submit_previewed_for_execution).
    pub async fn load_operation_row_full(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation_id: Uuid,
    ) -> Result<(String, JsonValue, Option<JsonValue>)> {
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

        Ok((row.operator, scope_val, row.preview_summary))
    }

    /// Fetch preview_summary with FOR UPDATE row lock.
    pub async fn fetch_meta_for_update(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation_id: Uuid,
    ) -> Result<JsonValue> {
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

        row.preview_summary.ok_or_else(|| {
            SinexError::processing("Replay operation is missing preview_summary metadata")
                .with_operation("fetch_replay_meta")
                .with_id("operation_id", operation_id.to_string())
        })
    }

    /// Write updated meta JSON and status/message back to the operation row.
    pub async fn update_operation_meta(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation_id: Uuid,
        meta_json: JsonValue,
        status: &str,
        msg: &str,
        duration_ms: Option<i32>,
    ) -> Result<()> {
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
            SinexError::database("Failed to update operation metadata")
                .with_source(e.to_string())
                .with_id("operation_id", operation_id.to_string())
        })?;
        Ok(())
    }

    /// Write meta JSON only (no status or duration update).
    pub async fn update_operation_meta_only(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation_id: Uuid,
        meta_json: JsonValue,
    ) -> Result<()> {
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
        Ok(())
    }

    // ── Scope queries (preview) ──────────────────────────────────────────

    /// Collect the root event IDs matching a replay scope.
    pub async fn collect_scope_root_ids(&self, scope: &ReplayScope) -> Result<Vec<Uuid>> {
        let window = resolve_time_window(scope);
        let mut builder = build_filter_query(scope, window, "SELECT id::uuid FROM core.events");
        let rows: Vec<(Uuid,)> = builder
            .build_query_as()
            .fetch_all(self.pool)
            .await
            .map_err(|e| SinexError::database(format!("collect scope root ids: {e}")))?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Count total material-root events matching a replay scope.
    pub async fn count_scope_events(&self, scope: &ReplayScope) -> Result<i64> {
        let window = resolve_time_window(scope);
        let mut qb = build_filter_query(
            scope,
            window,
            "SELECT COUNT(*)::bigint as total FROM core.events",
        );
        qb.push(" AND source_material_id IS NOT NULL");
        Ok(qb
            .build_query_scalar::<Option<i64>>()
            .fetch_one(self.pool)
            .await?
            .unwrap_or(0))
    }

    /// Get the top 5 event types for a scope (for preview display).
    pub async fn get_top_event_types(
        &self,
        scope: &ReplayScope,
    ) -> Result<Vec<EventTypeCountRow>> {
        let window = resolve_time_window(scope);
        let mut qb = build_filter_query(
            scope,
            window,
            "SELECT event_type, COUNT(*)::bigint as count FROM core.events",
        );
        qb.push(" AND source_material_id IS NOT NULL");
        qb.push(" GROUP BY event_type ORDER BY count DESC LIMIT 5");
        Ok(qb.build_query_as().fetch_all(self.pool).await?)
    }

    /// Count distinct source materials for a material-filtered scope.
    pub async fn count_distinct_materials(
        &self,
        scope: &ReplayScope,
    ) -> Result<i64> {
        let window = resolve_time_window(scope);
        let mut qb = build_filter_query(
            scope,
            window,
            "SELECT COUNT(DISTINCT source_material_id)::bigint as count FROM core.events",
        );
        qb.push(" AND source_material_id IS NOT NULL");
        Ok(qb
            .build_query_scalar::<Option<i64>>()
            .fetch_one(self.pool)
            .await?
            .unwrap_or(0))
    }

    // ── Cascade query helpers ────────────────────────────────────────────

    /// Load distinct source names for derived events.
    pub async fn load_cascade_affected_nodes(
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

    /// Load distinct (event_type, scope_key) pairs for derived events.
    pub async fn load_cascade_affected_scopes(
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

    // ── Recovery ─────────────────────────────────────────────────────────

    /// Find IDs of operations stuck in executing/cancelling/committing state
    /// whose `started_at` is older than the given threshold (in seconds).
    pub async fn find_stale_executing(
        &self,
        threshold_secs: f64,
    ) -> Result<Vec<Uuid>> {
        let rows = sqlx::query!(
            r#"
            SELECT id::uuid AS "id!"
            FROM core.operations_log
            WHERE operation_type = 'replay'
              AND result_status = 'running'
              AND result_message IN ('executing', 'cancelling', 'committing')
              AND (preview_summary->>'started_at')::timestamptz
                  < NOW() - make_interval(secs => $1)
            "#,
            threshold_secs,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| {
            SinexError::database("Failed to query for stale executing replay operations")
                .with_source(e.to_string())
                .with_operation("recover_stale_executing")
        })?;
        Ok(rows.into_iter().map(|r| r.id).collect())
    }

    // ── List operations ──────────────────────────────────────────────────

    /// List replay operations with optional filters.
    pub async fn list_operations(
        &self,
        filter_state: Option<ReplayState>,
        filter_node: Option<&str>,
        limit: Option<i64>,
    ) -> Result<Vec<(Uuid, String, JsonValue, Option<JsonValue>)>> {
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
            qb.push_bind(state_json_label(state));
        }

        qb.push(" ORDER BY id DESC");

        if let Some(lim) = limit {
            qb.push(" LIMIT ");
            qb.push_bind(lim);
        }

        let rows = qb.build().fetch_all(self.pool).await?;

        let mut operations = Vec::new();
        for row in rows {
            let uuid: sqlx::types::Uuid = row.try_get("id")?;
            let operator: String = row.try_get("operator")?;
            let scope_val: JsonValue = row.try_get("scope")?;
            let preview: Option<JsonValue> = row.try_get("preview_summary")?;
            operations.push((uuid, operator, scope_val, preview));
        }

        Ok(operations)
    }

    // ── Begin transaction helpers ────────────────────────────────────────

    /// Begin a new transaction on the pool.
    pub async fn begin(
        &self,
    ) -> Result<Transaction<'_, Postgres>> {
        self.pool
            .begin()
            .await
            .map_err(|e| SinexError::database(format!("begin transaction: {e}")))
    }

    /// Begin a new transaction on a pooled connection, with a contextual error message.
    pub async fn begin_context(
        &self,
        context: &str,
    ) -> Result<Transaction<'_, Postgres>> {
        self.pool
            .begin()
            .await
            .map_err(|e| SinexError::database(format!("begin transaction ({context}): {e}")))
    }
}

// ── Supporting types ─────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
pub struct EventTypeCountRow {
    pub event_type: String,
    pub count: i64,
}

// ── Public helpers ───────────────────────────────────────────────────────
