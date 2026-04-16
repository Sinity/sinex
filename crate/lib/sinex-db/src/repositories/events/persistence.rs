use super::conversions::{EventRecordExt, extract_provenance};
use crate::JsonValue;
use crate::models::Event;
use crate::postgres_copy::{
    event_copy_column_list_sql, event_copy_insert_select_sql, event_copy_staging_columns_sql,
};
use crate::repositories::common::{DbResult, EnhancedRepository, Repository, db_error};
use crate::schema::Events;
use crate::{EventRecord, SinexError};
use sinex_primitives::domain::{DataTier, EventSource, EventType, HostName, SchemaVersion};
use sinex_primitives::events::{EventId, SourceMaterial};
use sinex_primitives::{Id, Timestamp};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use serde::{Deserialize, Serialize};
use sqlx::{Executor, FromRow, PgPool, Postgres, QueryBuilder, Row, Transaction};

/// Minimum batch size that routes to the COPY-based insert path.
///
/// Below this threshold the `QueryBuilder` (VALUES) approach has lower latency
/// because it avoids the staging-table round-trips.  Above it, COPY's lack of
/// a 65 535-parameter limit and lower per-row protocol overhead dominate.
/// Minimum batch size to use COPY protocol instead of `QueryBuilder` (VALUES).
/// Below this threshold the `QueryBuilder` approach has lower latency because it
/// avoids the staging-table round-trips. Above it, COPY's lack of a 65 535-parameter
/// limit and lower per-row protocol overhead dominate.
pub const COPY_BATCH_THRESHOLD: usize = 50;
use tracing::instrument;

/// Lightweight DTO for stream batch inserts from ingestd.
///
/// This struct provides a minimal representation of event data for high-throughput
/// batch inserts, avoiding the overhead of the full `Event<T>` type tree.
/// All fields are pre-validated and pre-parsed by the caller.
#[derive(Debug, Clone)]
pub struct StreamBatchRow {
    /// Pre-parsed `UUIDv7` for the event
    pub id: Uuid,
    /// Event source identifier
    pub source: EventSource,
    /// Event type identifier
    pub event_type: EventType,
    /// Pre-parsed timestamp
    pub ts_orig: Timestamp,
    /// Hostname where event originated
    pub host: HostName,
    /// Event payload as JSON
    pub payload: JsonValue,
    /// Source material ID (for material provenance)
    pub source_material_id: Option<Id<SourceMaterial>>,
    /// Anchor byte offset into source material
    pub anchor_byte: Option<i64>,
    /// Start offset within source material
    pub offset_start: Option<i64>,
    /// End offset within source material
    pub offset_end: Option<i64>,
    /// Offset kind (e.g., "byte", "line")
    pub offset_kind: Option<String>,
    /// Parent event IDs (for synthesis provenance)
    pub source_event_ids: Option<Vec<EventId>>,
    /// Schema ID for payload validation
    pub payload_schema_id: Option<Uuid>,
    /// UUID of the node run session that produced this event
    pub node_run_id: Option<Uuid>,
    /// Associated blob IDs
    pub associated_blob_ids: Option<Vec<Uuid>>,

    // Synthetic event metadata (nullable — only set for derived/synthesized events)
    /// Temporal policy used for `ts_orig` derivation
    pub temporal_policy: Option<String>,
    /// Version of the node logic that produced this event
    pub semantics_version: Option<String>,
    /// Scope identifier for scope-reconciler replacement
    pub scope_key: Option<String>,
    /// Output slot identifier for targeted replacement
    pub equivalence_key: Option<String>,
    /// Which replay/operation created this event
    pub created_by_operation_id: Option<Uuid>,
    /// Which derived node model produced this event
    pub node_model: Option<String>,
}

/// Result of a stream batch insert operation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamBatchInsertResult {
    /// Number of rows successfully inserted
    pub inserted_count: usize,
    /// IDs of events that were actually inserted (excludes conflicts).
    /// Only populated when using ON CONFLICT DO NOTHING.
    pub inserted_ids: Option<Vec<Uuid>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamBatchInsertStrategy {
    QueryBuilder,
    Copy,
    Synthesis,
}

/// Event repository for database operations
pub struct EventRepository<'a> {
    pub(super) pool: &'a PgPool,
}

/// Validate that a synthesis event does not directly reference itself.
///
/// # Why only the direct self-reference check here?
///
/// Events are identified by `UUIDv7`, which is monotonically increasing in
/// time. A newly-created event ID is unique and cannot yet exist in the
/// database. Therefore:
///
/// - A cycle of the form `NEW → A → NEW` is impossible: `NEW` has never
///   been persisted, so no existing event can have `NEW` in its
///   `source_event_ids`.
/// - The only reachable existing-graph cycle case is `NEW → NEW` (the event
///   listing itself as its own parent), which this function detects with an
///   O(n) scan.
///
/// The previous implementation ran a `WITH RECURSIVE` CTE to walk the full
/// ancestry graph on every synthesis insert. That check added a full
/// recursive DB round-trip per batch row for a condition that `UUIDv7`
/// monotonicity already makes structurally impossible. It has been removed.
///
/// Batch-local cycles are still possible when a caller inserts multiple new
/// synthesis events with explicit IDs in the same batch. Those are rejected by
/// `ensure_no_intra_batch_synthesis_cycles` before insert.
///
/// Array-size limits are retained because large `source_event_ids` arrays have
/// real query-performance implications irrespective of cycles.
async fn ensure_no_synthesis_cycles<'e, E>(
    _executor: E,
    event_id: &Id<Event<JsonValue>>,
    source_event_ids: &[EventId],
) -> DbResult<()>
where
    E: Executor<'e, Database = Postgres>,
{
    if source_event_ids.is_empty() {
        return Ok(());
    }

    // Array-size guards: large parent arrays degrade lineage query performance.
    const WARN_THRESHOLD: usize = 100;
    const HARD_LIMIT: usize = 1000;

    if source_event_ids.len() > HARD_LIMIT {
        return Err(SinexError::database(format!(
            "source_event_ids array exceeds hard limit of {} parents (got {}). \
             This indicates a pathological synthesis pattern that will cause performance issues.",
            HARD_LIMIT,
            source_event_ids.len()
        )));
    }

    if source_event_ids.len() > WARN_THRESHOLD {
        tracing::warn!(
            event_id = %event_id,
            parent_count = source_event_ids.len(),
            threshold = WARN_THRESHOLD,
            hard_limit = HARD_LIMIT,
            "Event has unusually large number of parent events. \
             This may indicate a synthesis anti-pattern and will impact query performance."
        );
    }

    // Direct self-reference: the one cycle the UUIDv7 argument cannot rule out.
    if source_event_ids
        .iter()
        .any(|source_id| source_id == event_id)
    {
        return Err(SinexError::database(
            "cycle detected in synthesis provenance",
        ));
    }

    Ok(())
}

fn ensure_no_intra_batch_synthesis_cycles(
    synthesis_checks: &[(Id<Event<JsonValue>>, Vec<EventId>)],
) -> DbResult<()> {
    if synthesis_checks.len() < 2 {
        return Ok(());
    }

    let batch_ids: HashSet<Uuid> = synthesis_checks
        .iter()
        .map(|(event_id, _)| *event_id.as_uuid())
        .collect();
    let local_edges: HashMap<Uuid, Vec<Uuid>> = synthesis_checks
        .iter()
        .filter_map(|(event_id, source_ids)| {
            let local_parents = source_ids
                .iter()
                .map(EventId::to_uuid)
                .filter(|source_id| batch_ids.contains(source_id))
                .collect::<Vec<_>>();
            if local_parents.is_empty() {
                None
            } else {
                Some((*event_id.as_uuid(), local_parents))
            }
        })
        .collect();

    if local_edges.is_empty() {
        return Ok(());
    }

    let mut finished = HashSet::new();
    let mut stack = Vec::new();
    let mut nodes = local_edges.keys().copied().collect::<Vec<_>>();
    nodes.sort_unstable();

    for node in nodes {
        if let Some(cycle) =
            detect_intra_batch_synthesis_cycle(node, &local_edges, &mut finished, &mut stack)
        {
            let cycle = cycle
                .into_iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(" -> ");
            return Err(SinexError::database(format!(
                "cycle detected in synthesis provenance within batch: {cycle}"
            )));
        }
    }

    Ok(())
}

fn detect_intra_batch_synthesis_cycle(
    node: Uuid,
    local_edges: &HashMap<Uuid, Vec<Uuid>>,
    finished: &mut HashSet<Uuid>,
    stack: &mut Vec<Uuid>,
) -> Option<Vec<Uuid>> {
    if finished.contains(&node) {
        return None;
    }

    if let Some(position) = stack.iter().position(|current| *current == node) {
        let mut cycle = stack[position..].to_vec();
        cycle.push(node);
        return Some(cycle);
    }

    stack.push(node);
    if let Some(parents) = local_edges.get(&node) {
        for parent in parents {
            if let Some(cycle) =
                detect_intra_batch_synthesis_cycle(*parent, local_edges, finished, stack)
            {
                return Some(cycle);
            }
        }
    }
    stack.pop();
    finished.insert(node);
    None
}

fn ensure_batch_event_ids(events: &mut [Event<JsonValue>]) {
    for event in events {
        if event.id.is_none() {
            event.id = Some(Id::<Event<JsonValue>>::new());
        }
    }
}

fn collect_synthesis_checks(
    events: &[Event<JsonValue>],
) -> DbResult<Vec<(Id<Event<JsonValue>>, Vec<EventId>)>> {
    let mut synthesis_checks = Vec::new();

    for event in events {
        let Some(event_id) = event.id.as_ref() else {
            return Err(db_error(
                sqlx::Error::Protocol("batch insert event missing id".into()),
                "insert batch",
            ));
        };

        let (source_event_ids_raw, _, _, _, _, _) = extract_provenance(event)?;
        if let Some(source_ids) = source_event_ids_raw.filter(|source_ids| !source_ids.is_empty()) {
            synthesis_checks.push((
                Id::<Event<JsonValue>>::from_uuid(*event_id.as_uuid()),
                source_ids,
            ));
        }
    }

    Ok(synthesis_checks)
}

fn resolved_created_by_operation_id(event: &Event<JsonValue>) -> DbResult<Option<Uuid>> {
    let provenance_operation_id = event.provenance.operation_uuid();

    match (event.created_by_operation_id, provenance_operation_id) {
        (Some(event_level), Some(provenance_level)) if event_level != provenance_level => {
            Err(SinexError::invalid_state(format!(
                "operation lineage mismatch: event.created_by_operation_id={event_level} \
                 but provenance.operation_id={provenance_level}"
            )))
        }
        (Some(event_level), _) => Ok(Some(event_level)),
        (None, Some(provenance_level)) => Ok(Some(provenance_level)),
        (None, None) => Ok(None),
    }
}

impl<'a> Repository<'a> for EventRepository<'a> {
    fn pool(&self) -> &'a PgPool {
        self.pool
    }

    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }
}

impl<'a> EnhancedRepository<'a> for EventRepository<'a> {
    type Table = Events;
}

/// Event payload schema record from the database.
///
/// Represents a JSON schema definition for validating event payloads from a specific `source/event_type` combination.
/// Schemas are versioned and can be marked inactive when superseded.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, FromRow)]
pub struct EventPayloadSchema {
    /// Unique schema identifier
    pub id: Id<EventPayloadSchema>,
    /// Event source (e.g., "fs-watcher")
    pub source: EventSource,
    /// Event type (e.g., "file.created")
    pub event_type: EventType,
    /// Semantic version of this schema
    pub schema_version: SchemaVersion,
    /// JSON Schema content for validation
    pub schema_content: JsonValue,
    /// Blake3 hash of the schema content for deduplication
    pub content_hash: String,
    /// Whether this schema is currently active for new events
    pub is_active: bool,
    /// Timestamp of the last update
    pub updated_at: Timestamp,
}

/// User annotation or note attached to an event.
///
/// Allows attaching arbitrary metadata, comments, or tags to events for analytical or investigative purposes.
#[derive(Debug, FromRow)]
pub struct EventAnnotation {
    /// Unique annotation identifier
    pub id: Id<EventAnnotation>,
    /// ID of the event being annotated
    pub event_id: Id<Event<JsonValue>>,
    /// Type/category of the annotation (e.g., "comment", "tag", "flag")
    pub annotation_type: String,
    /// Annotation content or text
    pub content: String,
    /// Additional structured metadata for the annotation
    pub metadata: JsonValue,
    /// User or system that created this annotation
    pub created_by: String,
    /// Timestamp when the annotation was created
    pub created_at: Timestamp,
    /// Timestamp of the last update to this annotation
    pub updated_at: Timestamp,
}

/// Record of an event with a payload that failed validation against its schema.
#[derive(Debug)]
pub struct InvalidPayloadEvent {
    /// ID of the event with invalid payload
    pub event_id: Id<Event<JsonValue>>,
    /// Event source
    pub source: EventSource,
    /// Event type
    pub event_type: EventType,
    /// Ingestion timestamp
    pub ts_coided: Timestamp,
    /// The invalid JSON payload
    pub payload: JsonValue,
}

/// Record indicating a violation of event ordering constraints within a batch.
///
/// Used to detect temporal anomalies where events from the same source arrive out of order.
#[derive(Debug, FromRow)]
pub struct BatchViolation {
    /// ID of the event with the constraint violation
    pub event_id: Option<Id<Event<JsonValue>>>,
    /// ID of the previous event in the sequence
    pub prev_event_id: Option<Id<Event<JsonValue>>>,
    /// Original timestamp of the current event
    pub ts_orig: Option<Timestamp>,
    /// Original timestamp of the previous event
    pub prev_ts_orig: Option<Timestamp>,
    /// Event source
    pub source: EventSource,
    /// Row number in the batch where violation occurred
    pub row_num: Option<i64>,
}

/// Record of an event flagged as suspicious based on anomaly detection.
///
/// Used to identify unusual events that may indicate malicious activity or data quality issues.
#[derive(Debug, FromRow)]
pub struct SuspiciousEvent {
    /// ID of the suspicious event
    pub event_id: Id<Event<JsonValue>>,
    /// Event source
    pub source: EventSource,
    /// Event type
    pub event_type: EventType,
    /// Event payload
    pub payload: JsonValue,
    /// Detected payload type (if analyzable)
    pub payload_type: Option<String>,
    /// Size of the payload in bytes
    pub payload_size: Option<i32>,
}

/// Record of an event with a timestamp that violates business rules or constraints.
#[derive(Debug)]
pub struct InvalidTimestamp {
    /// ID of the event with invalid timestamp
    pub event_id: Id<Event<JsonValue>>,
    /// Original event timestamp (may be None or invalid)
    pub ts_orig: Option<Timestamp>,
    /// Ingestion timestamp (typically valid)
    pub ts_coided: Timestamp,
}

/// Source table for cascade graph traversal operations.
///
/// The cascade graph can be expanded from either the live event store
/// (`core.events`) or the archive (`audit.archived_events`). This enum
/// makes callers explicit and allows the pair of populate/expand methods
/// to be unified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeSource {
    /// Traverse from live events in `core.events`.
    Live,
    /// Traverse from archived events in `audit.archived_events`.
    Archive,
}

impl CascadeSource {
    fn table_name(self) -> &'static str {
        match self {
            CascadeSource::Live => "core.events",
            CascadeSource::Archive => "audit.archived_events",
        }
    }
}

/// Validate a cascade session table name produced by `prepare_cascade_session`.
///
/// Table names must contain only ASCII alphanumerics, underscores, and at most
/// one dot (for schema qualification). This prevents format!()-based SQL injection
/// in the dynamic cascade queries.
fn validate_cascade_table_name(table_name: &str) -> DbResult<()> {
    if table_name.is_empty()
        || table_name.starts_with('.')
        || table_name.ends_with('.')
        || table_name.contains("..")
        || !table_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(SinexError::validation(format!(
            "invalid cascade table name: {table_name:?}"
        )));
    }
    Ok(())
}

impl<'a> EventRepository<'a> {
    fn stream_batch_insert_strategy(batch: &[StreamBatchRow]) -> Option<StreamBatchInsertStrategy> {
        if batch.is_empty() {
            return None;
        }

        let has_synthesis = batch.iter().any(|row| {
            row.source_event_ids
                .as_ref()
                .is_some_and(|ids| !ids.is_empty())
        });

        if has_synthesis {
            Some(StreamBatchInsertStrategy::Synthesis)
        } else if batch.len() >= COPY_BATCH_THRESHOLD {
            Some(StreamBatchInsertStrategy::Copy)
        } else {
            Some(StreamBatchInsertStrategy::QueryBuilder)
        }
    }

    // === Cascade helpers ===

    pub async fn prepare_cascade_session(
        &self,
        session_id: &str,
        drop_on_commit: bool,
    ) -> DbResult<String> {
        sqlx::query_scalar!(
            r#"SELECT core.prepare_cascade_session($1, $2) AS "table_name!""#,
            session_id,
            drop_on_commit
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| {
            db_error(
                e,
                &format!("Failed to prepare cascade session '{session_id}'"),
            )
        })
    }

    pub async fn populate_cascade_roots(
        &self,
        table_name: &str,
        event_ids: &[Uuid],
    ) -> DbResult<()> {
        let ids: Vec<Uuid> = event_ids.to_vec();
        sqlx::query_scalar::<_, i64>(
            r"SELECT core.cascade_populate_roots($1, $2::uuid[]) as inserted",
        )
        .bind(table_name)
        .bind(&ids)
        .fetch_one(self.pool)
        .await
        .map_err(|e| {
            db_error(
                e,
                &format!(
                    "Failed to populate cascade roots: {} event IDs into table '{}'",
                    event_ids.len(),
                    table_name
                ),
            )
        })?;
        Ok(())
    }

    /// Expand cascade graph to find all descendants
    ///
    /// # Cycle Detection
    /// IMPORTANT: The database function `core.expand_cascade` MUST implement cycle detection
    /// to prevent infinite loops when circular event dependencies exist. The implementation should:
    /// - Track visited nodes during traversal
    /// - Stop expansion when a node is encountered twice
    /// - Respect the `max_depth` limit as a safety bound
    ///
    /// Without proper cycle detection, circular references (A -> B -> C -> A) will cause
    /// the function to loop indefinitely or exceed `max_depth`.
    pub async fn expand_cascade(&self, table_name: &str, max_depth: i32) -> DbResult<usize> {
        let depth = sqlx::query_scalar!(
            r#"SELECT core.expand_cascade($1, $2)"#,
            table_name,
            max_depth
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| {
            db_error(
                e,
                &format!(
                    "Failed to expand cascade graph for table '{table_name}' (max_depth={max_depth})"
                ),
            )
        })?
        .unwrap_or(0);
        Ok(depth as usize)
    }

    pub async fn cascade_depth_histogram(&self, table_name: &str) -> DbResult<Vec<(i32, i64)>> {
        let rows = sqlx::query!(
            r#"SELECT depth as "depth!", node_count as "node_count!" FROM core.cascade_depth_histogram($1)"#,
            table_name
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "cascade depth histogram"))?;
        Ok(rows
            .into_iter()
            .map(|row| (row.depth, row.node_count))
            .collect())
    }

    pub async fn cascade_node_count(&self, table_name: &str) -> DbResult<i64> {
        sqlx::query_scalar!(
            r#"SELECT core.cascade_count_nodes($1) as "count!""#,
            table_name
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count cascade nodes"))
    }

    pub async fn cascade_integrity_violations(
        &self,
        table_name: &str,
        limit: i32,
    ) -> DbResult<Vec<(Uuid, Uuid)>> {
        #[derive(sqlx::FromRow)]
        struct ViolationRow {
            live_event_id: Uuid,
            archived_event_id: Uuid,
        }

        let rows = sqlx::query_as::<_, ViolationRow>(
            "SELECT live_event_id, archived_event_id FROM core.cascade_find_integrity_violations($1, $2)",
        )
        .bind(table_name)
        .bind(limit)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find cascade integrity violations"))?;

        Ok(rows
            .into_iter()
            .map(|row| (row.live_event_id, row.archived_event_id))
            .collect())
    }

    pub async fn cleanup_cascade_session(&self, table_name: &str) -> DbResult<()> {
        sqlx::query!("SELECT core.cleanup_cascade_session($1)", table_name)
            .execute(self.pool)
            .await
            .map_err(|e| db_error(e, "cleanup cascade session"))?;
        Ok(())
    }

    pub fn as_tx<'t>(&'a self, tx: &'a mut Transaction<'t, Postgres>) -> EventRepositoryTx<'a, 't> {
        EventRepositoryTx { tx }
    }

    #[instrument(skip(self, event))]
    pub async fn insert<T>(&self, event: Event<T>) -> DbResult<Event<JsonValue>>
    where
        T: serde::Serialize,
    {
        use crate::query_helpers::{
            IdempotentTransaction, RetryConfig, set_repeatable_read,
            with_retry_transaction_idempotent,
        };

        // Convert typed event to JSON event for storage, preserving any explicit ID.
        let event_id = event
            .id
            .as_ref()
            .map(|id| Id::<Event<JsonValue>>::from_uuid(*id.as_uuid()));
        let mut event = event.to_json_event().map_err(|e| {
            SinexError::database("Failed to serialize event payload").with_source(e)
        })?;
        if event.id.is_none() {
            event.id = event_id;
        }
        let id = *event.id.get_or_insert_with(Id::<Event<JsonValue>>::new);

        // Extract provenance into separate fields
        let (
            source_event_ids,
            source_material_id,
            offset_start,
            offset_end,
            offset_kind,
            anchor_byte,
        ) = extract_provenance(&event)?;

        // Convert IDs to UUIDs
        let source_event_uuids = source_event_ids.as_ref().map(|ids| {
            ids.iter()
                .map(sinex_primitives::Id::to_uuid)
                .collect::<Vec<_>>()
        });
        let associated_blob_uuids = event.associated_blob_ids.clone();

        // Prepare timestamps
        let (ts_orig, ts_orig_subnano) = match event.ts_orig {
            Some(ts) => {
                let (pg, sub) = ts.to_postgres_parts();
                (Some(pg), Some(sub))
            }
            None => (None, None),
        };

        // Clone data needed for the closure
        let event_source = event.source.clone();
        let event_type = event.event_type.clone();
        let host = event.host.clone();
        let payload = event.payload.clone();
        let node_run_id = event.node_run_id;
        let payload_schema_id = event.payload_schema_id;
        let created_by_operation_id = resolved_created_by_operation_id(&event)?;

        // Synthetic event metadata
        let temporal_policy_str = event.temporal_policy.map(|p| p.to_string());
        let semantics_version = event.semantics_version.clone();
        let scope_key = event.scope_key.clone();
        let equivalence_key = event.equivalence_key.clone();
        let node_model_str = event.node_model.map(|m| m.to_string());

        // Execute with retry logic
        with_retry_transaction_idempotent(
            self.pool,
            RetryConfig::default(),
            IdempotentTransaction::new(),
            move |tx| {
                let id = id;
                let source_event_ids = source_event_ids.clone();
                let source_material_id = source_material_id;
                let source_event_uuids = source_event_uuids.clone();
                let associated_blob_uuids = associated_blob_uuids.clone();
                let event_source = event_source.clone();
                let event_type = event_type.clone();
                let host = host.clone();
                let payload = payload.clone();
                let node_run_id = node_run_id;
                let offset_kind = offset_kind.clone();
                let temporal_policy_str = temporal_policy_str.clone();
                let semantics_version = semantics_version.clone();
                let scope_key = scope_key.clone();
                let equivalence_key = equivalence_key.clone();
                let node_model_str = node_model_str.clone();

                Box::pin(async move {
                    // Enforce REPEATABLE READ for consistent view during cycle check
                    set_repeatable_read(tx).await?;

                    if let Some(source_event_ids) = source_event_ids.as_ref() {
                        ensure_no_synthesis_cycles(&mut **tx, &id, source_event_ids).await?;
                    }

                    let record = sqlx::query_as!(
                        EventRecord,
                        r#"
                        INSERT INTO core.events (
                            id, source, event_type, host, payload,
                            ts_orig, ts_orig_subnano, node_run_id, payload_schema_id, source_event_ids,
                            source_material_id, offset_start, offset_end, offset_kind,
                            anchor_byte, associated_blob_ids,
                            temporal_policy, semantics_version, scope_key, equivalence_key,
                            created_by_operation_id, node_model
                        ) VALUES (
                            $1::uuid, $2, $3, $4, $5,
                            $6, $7, $8, $9::uuid, $10::uuid[],
                            $11::uuid, $12, $13, $14,
                            $15, $16::uuid[],
                            $17, $18, $19, $20,
                            $21::uuid, $22
                        )
                        RETURNING
                            id as "id!: uuid::Uuid",
                            source as "source!",
                            event_type as "event_type!",
                            ts_coided as "ts_coided!: Timestamp",
                            ts_persisted as "ts_persisted!: Timestamp",
                            ts_orig as "ts_orig!: Timestamp",
                            ts_orig_subnano,
                            host as "host!",
                            node_run_id::uuid as "node_run_id: uuid::Uuid",
                            payload_schema_id::uuid as "payload_schema_id: uuid::Uuid",
                            payload as "payload!",
                            source_event_ids::uuid[] as "source_event_ids: Vec<uuid::Uuid>",
                            source_material_id::uuid as "source_material_id: uuid::Uuid",
                            offset_start,
                            offset_end,
                            offset_kind,
                            anchor_byte,
                            associated_blob_ids::uuid[] as "associated_blob_ids: Vec<uuid::Uuid>",
                            temporal_policy,
                            semantics_version,
                            scope_key,
                            equivalence_key,
                            created_by_operation_id::uuid as "created_by_operation_id: uuid::Uuid",
                            node_model
                        "#,
                        id.to_uuid(),
                        event_source.as_str(),
                        event_type.as_str(),
                        host.as_str(),
                        payload,
                        ts_orig,
                        ts_orig_subnano,
                        node_run_id,
                        payload_schema_id,
                        source_event_uuids.as_deref(),
                        source_material_id.map(|id| id.to_uuid()),
                        offset_start,
                        offset_end,
                        offset_kind.as_deref(),
                        anchor_byte,
                        associated_blob_uuids.as_deref(),
                        temporal_policy_str,
                        semantics_version,
                        scope_key,
                        equivalence_key,
                        created_by_operation_id,
                        node_model_str
                    )
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(|e| db_error(e, "insert event"))?;

                    record.try_to_event()
                })
            },
        )
        .await
    }

    // Query helpers live in queries.rs.

    #[instrument(skip(self, tx, event), fields(event_source = %event.source, event_type = %event.event_type))]
    pub async fn insert_with_tx<T>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        event: Event<T>,
    ) -> DbResult<Event<JsonValue>>
    where
        T: serde::Serialize,
    {
        // Convert typed event to JSON event for storage, preserving any explicit ID.
        let event_id = event
            .id
            .as_ref()
            .map(|id| Id::<Event<JsonValue>>::from_uuid(*id.as_uuid()));
        let mut event = event.to_json_event().map_err(|e| {
            SinexError::database("Failed to serialize event payload").with_source(e)
        })?;
        if event.id.is_none() {
            event.id = event_id;
        }
        let id = *event.id.get_or_insert_with(Id::<Event<JsonValue>>::new);

        // Extract provenance into separate fields for database
        let (
            source_event_ids,
            source_material_id,
            offset_start,
            offset_end,
            offset_kind,
            anchor_byte,
        ) = extract_provenance(&event)?;

        if let Some(source_event_ids) = source_event_ids.as_ref() {
            ensure_no_synthesis_cycles(&mut **tx, &id, source_event_ids).await?;
        }

        // Convert IDs to UUIDs before the query to avoid temporary value issues
        let source_event_uuids = source_event_ids.as_ref().map(|ids| {
            ids.iter()
                .map(sinex_primitives::Id::to_uuid)
                .collect::<Vec<_>>()
        });
        let associated_blob_uuids = event.associated_blob_ids.clone();

        // Postgres timestamps are microsecond precision. Persist the sub-microsecond
        // remainder separately so we can reconstruct full nanosecond timestamps on read.
        let (ts_orig, ts_orig_subnano) = match event.ts_orig {
            Some(ts) => {
                let (pg, sub) = ts.to_postgres_parts();
                (Some(pg), Some(sub))
            }
            None => (None, None),
        };

        // Synthetic event metadata
        let temporal_policy_str = event.temporal_policy.map(|p| p.to_string());
        let created_by_operation_id = resolved_created_by_operation_id(&event)?;
        let node_model_str = event.node_model.map(|m| m.to_string());

        let record = sqlx::query_as!(
            EventRecord,
            r#"
            INSERT INTO core.events (
                id, source, event_type, host, payload,
                ts_orig, ts_orig_subnano, node_run_id, payload_schema_id, source_event_ids,
                source_material_id, offset_start, offset_end, offset_kind,
                anchor_byte, associated_blob_ids,
                temporal_policy, semantics_version, scope_key, equivalence_key,
                created_by_operation_id, node_model
            ) VALUES (
                $1::uuid, $2, $3, $4, $5,
                $6, $7, $8, $9::uuid, $10::uuid[],
                $11::uuid, $12, $13, $14,
                $15, $16::uuid[],
                $17, $18, $19, $20,
                $21::uuid, $22
            )
            RETURNING
                id as "id!: uuid::Uuid",
                source as "source!",
                event_type as "event_type!",
                ts_coided as "ts_coided!: Timestamp",
                ts_persisted as "ts_persisted!: Timestamp",
                ts_orig as "ts_orig!: Timestamp",
                ts_orig_subnano,
                host as "host!",
                node_run_id::uuid as "node_run_id: uuid::Uuid",
                payload_schema_id::uuid as "payload_schema_id: uuid::Uuid",
                payload as "payload!",
                source_event_ids::uuid[] as "source_event_ids: Vec<uuid::Uuid>",
                source_material_id::uuid as "source_material_id: uuid::Uuid",
                offset_start,
                offset_end,
                offset_kind,
                anchor_byte,
                associated_blob_ids::uuid[] as "associated_blob_ids: Vec<uuid::Uuid>",
                temporal_policy,
                semantics_version,
                scope_key,
                equivalence_key,
                created_by_operation_id::uuid as "created_by_operation_id: uuid::Uuid",
                node_model
            "#,
            id.to_uuid(),
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            ts_orig,
            ts_orig_subnano,
            event.node_run_id,
            event.payload_schema_id,
            source_event_uuids.as_deref(),
            source_material_id.map(|id| id.to_uuid()),
            offset_start,
            offset_end,
            offset_kind.as_deref(),
            anchor_byte,
            associated_blob_uuids.as_deref(),
            temporal_policy_str,
            event.semantics_version,
            event.scope_key,
            event.equivalence_key,
            created_by_operation_id,
            node_model_str
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error(e, "insert event with tx"))?;

        record.try_to_event()
    }

    #[instrument(skip(self, events), fields(batch_size = events.len()))]
    pub async fn insert_batch<T>(&self, events: Vec<Event<T>>) -> DbResult<Vec<Event<JsonValue>>>
    where
        T: serde::Serialize,
    {
        // Convert all typed events to JSON events, preserving any explicit IDs.
        let mut json_events = Vec::with_capacity(events.len());
        for event in events {
            let event_id = event
                .id
                .as_ref()
                .map(|id| Id::<Event<JsonValue>>::from_uuid(*id.as_uuid()));
            let mut json_event = event.to_json_event().map_err(|e| {
                SinexError::database("Failed to serialize event payload").with_source(e)
            })?;
            if json_event.id.is_none() {
                json_event.id = event_id;
            }
            json_events.push(json_event);
        }
        let mut events = json_events;
        if events.is_empty() {
            return Ok(Vec::new());
        }
        ensure_batch_event_ids(&mut events);

        // For small batches, use the optimized single-transaction approach
        if events.len() <= 50 {
            return self.insert_batch_unnest(events).await;
        }

        // For larger batches, still chunk the VALUES statement size, but keep the
        // whole insert inside one transaction so cross-chunk failures roll back
        // cleanly during replay and backfill.
        let chunk_size = 50; // Optimal chunk size for batch processing
        let mut results = Vec::with_capacity(events.len());
        let total_events = events.len();
        let mut processed = 0;
        let synthesis_checks = collect_synthesis_checks(&events)?;

        let mut tx = self.pool.begin().await.map_err(|e| {
            db_error(
                e,
                &format!("Failed to begin transaction for batch insert of {total_events} events"),
            )
        })?;

        crate::query_helpers::set_repeatable_read(&mut tx).await?;
        ensure_no_intra_batch_synthesis_cycles(&synthesis_checks)?;

        for chunk in events.chunks(chunk_size) {
            let mut chunk_results = self
                .insert_batch_unnest_in_tx(&mut tx, chunk.to_vec())
                .await?;
            processed += chunk_results.len();
            results.append(&mut chunk_results);

            if processed % 1000 == 0 || processed == total_events {
                tracing::debug!(
                    processed = processed,
                    total = total_events,
                    progress_pct = (processed as f64 / total_events as f64 * 100.0) as u32,
                    "Batch insert progress"
                );
            }
        }

        tx.commit().await.map_err(|e| {
            db_error(
                e,
                &format!("Failed to commit batch insert of {total_events} events"),
            )
        })?;

        Ok(results)
    }

    /// Optimized batch insert with transaction batching for better performance
    async fn insert_batch_unnest(
        &self,
        events: Vec<Event<JsonValue>>,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            db_error(
                e,
                &format!(
                    "Failed to begin transaction for batch insert of {} events",
                    events.len()
                ),
            )
        })?;

        crate::query_helpers::set_repeatable_read(&mut tx).await?;

        let events = self.insert_batch_unnest_in_tx(&mut tx, events).await?;

        tx.commit().await.map_err(|e| {
            db_error(
                e,
                &format!("Failed to commit batch insert of {} events", events.len()),
            )
        })?;

        Ok(events)
    }

    async fn insert_batch_unnest_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mut events: Vec<Event<JsonValue>>,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // For very small batches, use individual inserts to avoid overhead
        if events.len() == 1 {
            let event = events.remove(0);
            let inserted = self.insert_with_tx(tx, event).await?;
            return Ok(vec![inserted]);
        }

        ensure_batch_event_ids(&mut events);
        let synthesis_checks = collect_synthesis_checks(&events)?;

        let mut ids = Vec::with_capacity(events.len());
        let mut sources = Vec::with_capacity(events.len());
        let mut event_types = Vec::with_capacity(events.len());
        let mut hosts = Vec::with_capacity(events.len());
        let mut payloads = Vec::with_capacity(events.len());
        let mut ts_orig_values = Vec::with_capacity(events.len());
        let mut ts_orig_subnanos = Vec::with_capacity(events.len());
        let mut node_run_ids: Vec<Option<Uuid>> = Vec::with_capacity(events.len());
        let mut payload_schema_ids = Vec::with_capacity(events.len());
        let mut source_event_ids = Vec::with_capacity(events.len());
        let mut source_material_ids = Vec::with_capacity(events.len());
        let mut offset_starts = Vec::with_capacity(events.len());
        let mut offset_ends = Vec::with_capacity(events.len());
        let mut offset_kinds = Vec::with_capacity(events.len());
        let mut anchor_bytes = Vec::with_capacity(events.len());
        let mut associated_blob_ids = Vec::with_capacity(events.len());
        let mut temporal_policies: Vec<Option<String>> = Vec::with_capacity(events.len());
        let mut semantics_versions: Vec<Option<String>> = Vec::with_capacity(events.len());
        let mut scope_keys: Vec<Option<String>> = Vec::with_capacity(events.len());
        let mut equivalence_keys: Vec<Option<String>> = Vec::with_capacity(events.len());
        let mut created_by_operation_ids: Vec<Option<Uuid>> = Vec::with_capacity(events.len());
        let mut node_models: Vec<Option<String>> = Vec::with_capacity(events.len());

        for event in &events {
            let event_id = event
                .id
                .as_ref()
                .ok_or_else(|| {
                    db_error(
                        sqlx::Error::Protocol("batch insert event missing id".into()),
                        "insert batch",
                    )
                })?
                .as_uuid()
                .to_owned();

            // Extract provenance
            let (
                source_event_ids_raw,
                source_material_id,
                offset_start,
                offset_end,
                offset_kind,
                anchor_byte,
            ) = extract_provenance(event)?;

            let source_event_uuids = source_event_ids_raw
                .map(|ids| ids.into_iter().map(|id| id.to_uuid()).collect::<Vec<_>>());
            let associated_blob_uuids = event.associated_blob_ids.clone();

            // Postgres timestamps are microsecond precision. Persist the sub-microsecond
            // remainder separately so we can reconstruct full nanosecond timestamps on read.
            let (ts_orig, ts_orig_subnano) = match event.ts_orig {
                Some(ts) => {
                    let (pg_ts, sub_nano) = ts.to_postgres_parts();
                    (Some(pg_ts), Some(sub_nano))
                }
                None => (None, None),
            };

            ids.push(event_id);
            sources.push(event.source.as_str().to_string());
            event_types.push(event.event_type.as_str().to_string());
            hosts.push(event.host.as_str().to_string());
            payloads.push(event.payload.clone());
            ts_orig_values.push(ts_orig);
            ts_orig_subnanos.push(ts_orig_subnano);
            node_run_ids.push(event.node_run_id);
            payload_schema_ids.push(event.payload_schema_id);
            source_event_ids.push(source_event_uuids);
            source_material_ids.push(source_material_id.map(|id| id.to_uuid()));
            offset_starts.push(offset_start);
            offset_ends.push(offset_end);
            offset_kinds.push(offset_kind);
            anchor_bytes.push(anchor_byte);
            associated_blob_ids.push(associated_blob_uuids);
            temporal_policies.push(event.temporal_policy.map(|p| p.to_string()));
            semantics_versions.push(event.semantics_version.clone());
            scope_keys.push(event.scope_key.clone());
            equivalence_keys.push(event.equivalence_key.clone());
            created_by_operation_ids.push(resolved_created_by_operation_id(event)?);
            node_models.push(event.node_model.map(|m| m.to_string()));
        }

        ensure_no_intra_batch_synthesis_cycles(&synthesis_checks)?;

        // Enforce synthesis cycle detection (parity with insert/insert_stream_batch)
        for (event_id, source_ids) in &synthesis_checks {
            ensure_no_synthesis_cycles(&mut **tx, event_id, source_ids).await?;
        }

        // QueryBuilder is required here because UNNEST cannot represent ragged arrays
        // (source_event_ids/associated_blob_ids) and `query!` rejects array nulls.
        let mut builder = QueryBuilder::new(
            "INSERT INTO core.events (
                id, source, event_type, host, payload,
                ts_orig, ts_orig_subnano, node_run_id, payload_schema_id, source_event_ids,
                source_material_id, offset_start, offset_end, offset_kind,
                anchor_byte, associated_blob_ids,
                temporal_policy, semantics_version, scope_key, equivalence_key,
                created_by_operation_id, node_model
            ) ",
        );
        builder.push_values(0..ids.len(), |mut b, idx| {
            b.push_bind(ids[idx]).push_unseparated("::uuid");
            b.push_bind(&sources[idx]);
            b.push_bind(&event_types[idx]);
            b.push_bind(&hosts[idx]);
            b.push_bind(&payloads[idx]);
            b.push_bind(ts_orig_values[idx]);
            b.push_bind(ts_orig_subnanos[idx]);
            b.push_bind(node_run_ids[idx]).push_unseparated("::uuid");
            b.push_bind(payload_schema_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(&source_event_ids[idx])
                .push_unseparated("::uuid[]");
            b.push_bind(source_material_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(offset_starts[idx]);
            b.push_bind(offset_ends[idx]);
            b.push_bind(&offset_kinds[idx]);
            b.push_bind(anchor_bytes[idx]);
            b.push_bind(&associated_blob_ids[idx])
                .push_unseparated("::uuid[]");
            b.push_bind(&temporal_policies[idx]);
            b.push_bind(&semantics_versions[idx]);
            b.push_bind(&scope_keys[idx]);
            b.push_bind(&equivalence_keys[idx]);
            b.push_bind(created_by_operation_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(&node_models[idx]);
        });

        builder.build().execute(&mut **tx).await.map_err(|e| {
            db_error(
                e,
                &format!("Failed to insert batch of {} events", ids.len()),
            )
        })?;

        Ok(events)
    }

    // ========== Stream Batch Insert (for ingestd) ==========

    /// Insert a batch of pre-validated events from the stream consumer.
    ///
    /// This method is optimized for high-throughput ingestion from `JetStream`.
    /// It uses `ON CONFLICT DO NOTHING` to handle duplicate IDs gracefully
    /// and returns the IDs of events that were actually inserted.
    ///
    /// Unlike `insert_batch`, this method:
    /// - Accepts pre-parsed/pre-validated data via `StreamBatchRow`
    /// - Uses `ON CONFLICT DO NOTHING` instead of failing on duplicates
    /// - Returns which IDs were inserted vs skipped
    ///
    /// # Arguments
    /// * `batch` - Slice of pre-validated event rows
    ///
    /// # Returns
    /// * `StreamBatchInsertResult` with inserted count and IDs
    #[instrument(skip(self, batch), fields(batch_size = batch.len()))]
    pub async fn insert_stream_batch(
        &self,
        batch: &[StreamBatchRow],
    ) -> DbResult<StreamBatchInsertResult> {
        use crate::query_helpers::set_repeatable_read;

        match Self::stream_batch_insert_strategy(batch) {
            None => Ok(StreamBatchInsertResult::default()),
            // Synthesis batches: wrap in REPEATABLE READ for cycle detection.
            // COPY cannot be mixed with cycle-detection queries in the same
            // transaction easily, so synthesis batches use the VALUES path.
            Some(StreamBatchInsertStrategy::Synthesis) => {
                let synthesis_checks = batch
                    .iter()
                    .filter_map(|row| {
                        row.source_event_ids
                            .as_ref()
                            .filter(|source_ids| !source_ids.is_empty())
                            .map(|source_ids| {
                                (Id::<Event<JsonValue>>::from(row.id), source_ids.clone())
                            })
                    })
                    .collect::<Vec<_>>();
                ensure_no_intra_batch_synthesis_cycles(&synthesis_checks)?;

                let mut tx = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| db_error(e, "begin stream batch transaction"))?;
                set_repeatable_read(&mut tx).await?;

                for (event_id, source_ids) in &synthesis_checks {
                    ensure_no_synthesis_cycles(&mut *tx, event_id, source_ids).await?;
                }

                let result = Self::execute_batch_insert(&mut *tx, batch).await?;
                tx.commit()
                    .await
                    .map_err(|e| db_error(e, "commit stream batch"))?;
                Ok(result)
            }
            // Large material-only batch: use COPY for maximum throughput.
            // Avoids the 65 535-parameter limit of parameterised VALUES queries
            // and has significantly lower per-row protocol overhead.
            Some(StreamBatchInsertStrategy::Copy) => {
                Self::execute_batch_insert_copy(self.pool, batch).await
            }
            // Small material-only batch: QueryBuilder is faster (no staging
            // table overhead).
            Some(StreamBatchInsertStrategy::QueryBuilder) => {
                Self::execute_batch_insert(self.pool, batch).await
            }
        }
    }

    /// Build and execute the batch INSERT query against the given executor.
    ///
    /// Extracted so both the transactional (synthesis) and direct (material)
    /// paths can share the same query construction logic.
    #[instrument(skip(executor, batch), fields(batch_size = batch.len(), path = "query_builder"))]
    async fn execute_batch_insert<'e, E>(
        executor: E,
        batch: &[StreamBatchRow],
    ) -> DbResult<StreamBatchInsertResult>
    where
        E: Executor<'e, Database = Postgres>,
    {
        // Build vectors for QueryBuilder
        let mut ids = Vec::with_capacity(batch.len());
        let mut sources = Vec::with_capacity(batch.len());
        let mut event_types = Vec::with_capacity(batch.len());
        let mut ts_orig_values = Vec::with_capacity(batch.len());
        let mut ts_orig_subnanos = Vec::with_capacity(batch.len());
        let mut hosts = Vec::with_capacity(batch.len());
        let mut payloads = Vec::with_capacity(batch.len());
        let mut source_material_ids = Vec::with_capacity(batch.len());
        let mut anchor_bytes = Vec::with_capacity(batch.len());
        let mut offset_starts = Vec::with_capacity(batch.len());
        let mut offset_ends = Vec::with_capacity(batch.len());
        let mut offset_kinds = Vec::with_capacity(batch.len());
        let mut source_event_ids = Vec::with_capacity(batch.len());
        let mut payload_schema_ids = Vec::with_capacity(batch.len());
        let mut node_run_ids: Vec<Option<Uuid>> = Vec::with_capacity(batch.len());
        let mut associated_blob_ids = Vec::with_capacity(batch.len());

        for row in batch {
            // Postgres timestamps are microsecond precision. Store sub-microsecond
            // remainder separately so we can reconstruct full nanosecond timestamps.
            let (ts_truncated, ts_orig_subnano) = row.ts_orig.to_postgres_parts();

            ids.push(row.id);
            sources.push(row.source.clone());
            event_types.push(row.event_type.clone());
            ts_orig_values.push(ts_truncated);
            ts_orig_subnanos.push(ts_orig_subnano);
            hosts.push(row.host.clone());
            payloads.push(row.payload.clone());
            source_material_ids.push(row.source_material_id.map(|id| id.to_uuid()));
            anchor_bytes.push(row.anchor_byte);
            offset_starts.push(row.offset_start);
            offset_ends.push(row.offset_end);
            offset_kinds.push(row.offset_kind.clone());
            source_event_ids.push(row.source_event_ids.as_ref().map(|ids| {
                ids.iter()
                    .map(sinex_primitives::Id::to_uuid)
                    .collect::<Vec<_>>()
            }));
            payload_schema_ids.push(row.payload_schema_id);
            node_run_ids.push(row.node_run_id);
            associated_blob_ids.push(row.associated_blob_ids.clone());
        }

        // Synthetic event metadata vectors
        let temporal_policies: Vec<_> = batch.iter().map(|r| r.temporal_policy.clone()).collect();
        let semantics_versions: Vec<_> =
            batch.iter().map(|r| r.semantics_version.clone()).collect();
        let scope_keys: Vec<_> = batch.iter().map(|r| r.scope_key.clone()).collect();
        let equivalence_keys: Vec<_> = batch.iter().map(|r| r.equivalence_key.clone()).collect();
        let created_by_op_ids: Vec<_> = batch.iter().map(|r| r.created_by_operation_id).collect();
        let node_models: Vec<_> = batch.iter().map(|r| r.node_model.clone()).collect();

        // Build INSERT with VALUES using QueryBuilder (required for ragged arrays)
        let mut builder = QueryBuilder::new(
            "INSERT INTO core.events (
                id, source, event_type, ts_orig, ts_orig_subnano, host, payload,
                source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
                source_event_ids, payload_schema_id, node_run_id, associated_blob_ids,
                temporal_policy, semantics_version, scope_key, equivalence_key,
                created_by_operation_id, node_model
            ) ",
        );

        builder.push_values(0..batch.len(), |mut b, idx| {
            b.push_bind(ids[idx]).push_unseparated("::uuid");
            b.push_bind(&sources[idx]);
            b.push_bind(&event_types[idx]);
            b.push_bind(ts_orig_values[idx]);
            b.push_bind(ts_orig_subnanos[idx]);
            b.push_bind(&hosts[idx]);
            b.push_bind(&payloads[idx]);
            b.push_bind(source_material_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(anchor_bytes[idx]);
            b.push_bind(offset_starts[idx]);
            b.push_bind(offset_ends[idx]);
            b.push_bind(&offset_kinds[idx]);
            b.push_bind(&source_event_ids[idx])
                .push_unseparated("::uuid[]");
            b.push_bind(payload_schema_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(node_run_ids[idx]).push_unseparated("::uuid");
            b.push_bind(&associated_blob_ids[idx])
                .push_unseparated("::uuid[]");
            b.push_bind(&temporal_policies[idx]);
            b.push_bind(&semantics_versions[idx]);
            b.push_bind(&scope_keys[idx]);
            b.push_bind(&equivalence_keys[idx]);
            b.push_bind(created_by_op_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(&node_models[idx]);
        });

        builder.push(" ON CONFLICT (id) DO NOTHING RETURNING id::uuid");

        let rows: Vec<(Uuid,)> =
            builder
                .build_query_as()
                .fetch_all(executor)
                .await
                .map_err(|e| {
                    db_error(
                        e,
                        &format!("Failed to insert stream batch of {} events", batch.len()),
                    )
                })?;

        let inserted_ids: Vec<Uuid> = rows.into_iter().map(|(uuid,)| uuid).collect();

        Ok(StreamBatchInsertResult {
            inserted_count: inserted_ids.len(),
            inserted_ids: Some(inserted_ids),
        })
    }

    // ========== COPY-based stream batch insert ==========

    /// COPY-based batch insert using a temporary UUID staging table.
    ///
    /// # Protocol
    /// 1. Open a transaction (`BEGIN`).
    /// 2. Create `sinex_batch_staging` if it doesn't exist (`IF NOT EXISTS`), then
    ///    `TRUNCATE` it so repeated calls on the same pooled connection start clean.
    /// 3. `COPY FROM STDIN` the serialised rows (text format, tab-delimited).
    /// 4. `INSERT INTO core.events … SELECT … FROM sinex_batch_staging ON CONFLICT DO NOTHING`
    ///    with `RETURNING id::uuid` to learn which IDs were actually inserted.
    /// 5. `COMMIT` — the temp table survives (for step 2 reuse) but the data is gone.
    ///
    /// # Why not query params?
    /// `PostgreSQL`'s protocol limits a single statement to 65 535 bind parameters.
    /// With 22 writable event columns per row that caps VALUES batches at ~2 900 rows. COPY has no
    /// such limit and has lower per-row overhead.
    ///
    /// # Why not synthesis batches?
    /// Synthesis batches require a REPEATABLE READ transaction for cycle detection.
    /// Combining that with COPY (which also monopolises the connection while active)
    /// is possible but adds complexity. The caller already routes synthesis batches
    /// through `execute_batch_insert`, so this function handles material-only batches.
    #[instrument(skip(pool, batch), fields(batch_size = batch.len(), path = "copy"))]
    async fn execute_batch_insert_copy(
        pool: &PgPool,
        batch: &[StreamBatchRow],
    ) -> DbResult<StreamBatchInsertResult> {
        use sqlx::postgres::PgConnection;

        // Serialise all rows first — no DB round-trips yet.
        let mut buf: Vec<u8> = Vec::with_capacity(batch.len() * 512);
        for row in batch {
            crate::postgres_copy::ToPostgresCopy::write_copy_row(row, &mut buf)
                .map_err(|e| db_error(e, "serialise batch row for COPY insert"))?;
        }

        let staging_columns_sql = event_copy_staging_columns_sql();
        let copy_columns_sql = event_copy_column_list_sql();
        let insert_select_sql = event_copy_insert_select_sql();

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin transaction for COPY batch insert"))?;

        // Create staging table once per connection, reuse on subsequent calls via TRUNCATE.
        // Column types are plain SQL types (UUID, TEXT, JSONB …) so COPY text format
        // can write them without UUIDv7-type complications.  The INSERT SELECT below
        // applies `::uuid` casts when copying into `core.events`.
        let create_staging_sql = format!(
            "CREATE TEMP TABLE IF NOT EXISTS sinex_batch_staging (
                {staging_columns_sql}
            )"
        );
        sqlx::query(&create_staging_sql)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "create staging table for COPY batch insert"))?;

        sqlx::query("TRUNCATE sinex_batch_staging")
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "truncate staging table for COPY batch insert"))?;

        // COPY into staging — the block scope ensures the mutable borrow of `tx`
        // through `conn` is fully released before we run the INSERT SELECT.
        {
            let conn: &mut PgConnection = &mut tx;
            let copy_sql = format!("COPY sinex_batch_staging ({copy_columns_sql}) FROM STDIN");
            let mut copy_writer = conn
                .copy_in_raw(&copy_sql)
                .await
                .map_err(|e| db_error(e, "start COPY for batch insert"))?;

            copy_writer
                .send(buf.as_slice())
                .await
                .map_err(|e| db_error(e, "send COPY data for batch insert"))?;

            // `finish` consumes `copy_writer`, releasing the borrow of `conn`.
            copy_writer
                .finish()
                .await
                .map_err(|e| db_error(e, "finish COPY for batch insert"))?;
        } // `conn` dropped here → `tx` exclusively accessible again

        // Move rows from staging into core.events, applying UUIDv7 casts.
        let insert_sql = format!(
            "INSERT INTO core.events ({copy_columns_sql})
            SELECT
                {insert_select_sql}
            FROM sinex_batch_staging
            ON CONFLICT (id) DO NOTHING
            RETURNING id::uuid"
        );
        let rows: Vec<(uuid::Uuid,)> = sqlx::query_as(&insert_sql)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| db_error(e, "insert-select from staging for COPY batch insert"))?;

        tx.commit()
            .await
            .map_err(|e| db_error(e, "commit COPY batch insert transaction"))?;

        let inserted_ids: Vec<Uuid> = rows.into_iter().map(|(uuid,)| uuid).collect();

        Ok(StreamBatchInsertResult {
            inserted_count: inserted_ids.len(),
            inserted_ids: Some(inserted_ids),
        })
    }

    // ========== Event Annotations ==========

    /// Add an annotation to an event
    pub async fn add_annotation(
        &self,
        event_id: Id<Event<JsonValue>>,
        annotation_type: &str,
        content: &str,
        metadata: serde_json::Value,
        created_by: &str,
    ) -> DbResult<EventAnnotation> {
        let id = Id::<EventAnnotation>::new();

        sqlx::query_as!(
            EventAnnotation,
            r#"
            INSERT INTO core.event_annotations (
                id, event_id, annotation_type, content, metadata, created_by
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            RETURNING
                id as "id!: Id<EventAnnotation>",
                event_id::uuid as "event_id!: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at: Timestamp",
                updated_at as "updated_at!"
            "#,
            *id.as_uuid(),
            *event_id.as_uuid(),
            annotation_type,
            content,
            metadata,
            created_by
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "add annotation"))
    }

    /// Update annotation content
    pub async fn update_annotation(
        &self,
        annotation_id: Id<EventAnnotation>,
        content: &str,
    ) -> DbResult<EventAnnotation> {
        sqlx::query_as!(
            EventAnnotation,
            r#"
            UPDATE core.event_annotations
            SET content = $2, updated_at = CURRENT_TIMESTAMP
            WHERE id = $1
            RETURNING
                id as "id!: Id<EventAnnotation>",
                event_id::uuid as "event_id!: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at: Timestamp",
                updated_at as "updated_at!"
            "#,
            *annotation_id.as_uuid(),
            content
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "update annotation"))
    }

    /// Delete an annotation (soft delete)
    pub async fn delete_annotation(&self, id: Id<EventAnnotation>) -> DbResult<bool> {
        self.delete_annotation_with_context(id, "system", "Programmatic deletion")
            .await
    }

    /// Delete an annotation with audit context (hard delete with structured logging).
    ///
    /// The `deleted_by` and `deletion_reason` parameters are logged at INFO level so the
    /// intent is preserved in application logs. A schema update is needed before these
    /// can be persisted to a `deleted_by`/`deletion_reason` column.
    pub async fn delete_annotation_with_context(
        &self,
        id: Id<EventAnnotation>,
        deleted_by: &str,
        deletion_reason: &str,
    ) -> DbResult<bool> {
        tracing::info!(
            annotation_id = %id.as_uuid(),
            deleted_by = deleted_by,
            deletion_reason = deletion_reason,
            "Deleting event annotation"
        );

        let result = sqlx::query!(
            "DELETE FROM core.event_annotations WHERE id = $1",
            *id.as_uuid()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete annotation"))?;

        Ok(result.rows_affected() > 0)
    }

    // ========== Event Deletion Operations ==========

    /// Delete events with filter and audit context
    ///
    /// This method deletes events matching the provided source and/or `event_type` filters,
    /// with proper audit trail tracking. It includes a safety constraint to only delete
    /// events that appear to be test events (source/type contains "test", payload has
    /// {"test": true}, or host matches "test").
    ///
    /// # Arguments
    /// * `source` - Optional source filter
    /// * `event_type` - Optional event type filter
    /// * `deleted_by` - Audit trail: who is performing the deletion
    /// * `deletion_reason` - Audit trail: why the deletion is happening
    pub async fn delete_events_with_filter(
        &self,
        source: Option<&EventSource>,
        event_type: Option<&EventType>,
        deleted_by: &str,
        deletion_reason: &str,
    ) -> DbResult<u64> {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Generate a unique operation ID for audit tracking
        #[allow(clippy::expect_used)] // system clock is always after UNIX epoch
        let operation_id = format!(
            "cleanup_{}_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after UNIX epoch")
                .as_millis(),
            rand::random::<u32>()
        );

        // Begin transaction and set audit context
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "Failed to begin transaction for test event cleanup"))?;
        crate::query_helpers::set_repeatable_read(&mut tx).await?;

        // Set session variables for audit trail
        sqlx::query("SELECT pg_catalog.set_config('sinex.operation_id', $1, true)")
            .bind(&operation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set operation_id"))?;

        sqlx::query("SELECT pg_catalog.set_config('sinex.archived_by', $1, true)")
            .bind(deleted_by)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set archived_by"))?;

        sqlx::query("SELECT pg_catalog.set_config('sinex.archive_reason', $1, true)")
            .bind(deletion_reason)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set archive_reason"))?;

        // Build dynamic DELETE query based on filters
        let mut query_parts = vec!["DELETE FROM core.events WHERE 1=1".to_string()];
        let mut _bind_index = 1;

        if source.is_some() {
            query_parts.push(format!(" AND source = ${_bind_index}"));
            _bind_index += 1;
        }

        if event_type.is_some() {
            query_parts.push(format!(" AND event_type = ${_bind_index}"));
            _bind_index += 1;
        }

        // Add safety constraint to only delete test events
        query_parts.push(
            " AND (source ILIKE '%test%' \
                  OR event_type ILIKE '%test%' \
                  OR payload @> '{\"test\": true}' \
                  OR host ~* '\\\\ytest\\\\y')"
                .to_string(),
        );

        let query_sql = query_parts.join("");

        // Execute the deletion query
        let mut query = sqlx::query(&query_sql);

        if let Some(source) = source {
            query = query.bind(source.as_str());
        }

        if let Some(event_type) = event_type {
            query = query.bind(event_type.as_str());
        }

        let result = query
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "delete events with filter"))?;

        let deleted_count = result.rows_affected();

        // Commit the transaction
        tx.commit().await.map_err(|e| {
            db_error(
                e,
                &format!(
                    "Failed to commit event deletion transaction (deleted {deleted_count} events)"
                ),
            )
        })?;

        tracing::info!(
            operation_id = %operation_id,
            deleted_by = %deleted_by,
            deletion_reason = %deletion_reason,
            deleted_count = %deleted_count,
            "Deleted events with audit trail"
        );

        Ok(deleted_count)
    }

    // ========== Analytics Queries ==========

    /// Delete all events from a specific source (with audit trail).
    ///
    /// This still goes through the archive trigger path; the helper sets
    /// `sinex.operation_id` and the trigger moves deleted rows into
    /// `audit.archived_events`.
    pub async fn delete_by_source(&self, source: &EventSource) -> DbResult<u64> {
        self.delete_events_with_filter(Some(source), None, "system", "Delete by source")
            .await
    }

    // ========== Data Lifecycle Operations ==========

    /// Get status of all lifecycle tiers (live, archive, tombstone).
    ///
    /// Returns event counts, age distributions, and source diversity for each tier.
    pub async fn lifecycle_tier_status(&self) -> DbResult<Vec<LifecycleTierStatus>> {
        // Use runtime query since the function is created by declarative apply SQL
        let rows = sqlx::query_as::<_, LifecycleTierStatus>(
            r"
            SELECT
                tier,
                event_count,
                oldest_ts,
                newest_ts,
                distinct_sources
            FROM core.lifecycle_tier_status()
            ",
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get lifecycle tier status"))?;

        Ok(rows)
    }

    /// Execute cascade tombstone operation.
    ///
    /// Moves archived events and their cascade to tombstones.
    /// This is a ONE-WAY operation - data is permanently gone after this.
    ///
    /// # Arguments
    /// * `archived_ids` - IDs of archived events to tombstone (must be complete cascade)
    /// * `reason` - Human-readable reason for tombstoning
    /// * `operation_id` - `UUIDv7` for audit correlation
    ///
    /// # Returns
    /// Number of tombstones created
    pub async fn execute_cascade_tombstone(
        &self,
        archived_ids: &[Uuid],
        reason: &str,
        operation_id: Uuid,
    ) -> DbResult<u64> {
        if archived_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = archived_ids.to_vec();
        // Use runtime query since the function is created by declarative apply SQL
        let count: i64 =
            sqlx::query_scalar(r"SELECT core.execute_cascade_tombstone($1::uuid[], $2, $3::uuid)")
                .bind(&ids)
                .bind(reason)
                .bind(operation_id)
                .fetch_one(self.pool)
                .await
                .map_err(|e| db_error(e, "execute cascade tombstone"))?;

        Ok(count as u64)
    }

    /// Execute cascade restore operation.
    ///
    /// Moves archived events and their cascade back to live (core.events).
    ///
    /// # Arguments
    /// * `archived_ids` - IDs of archived events to restore (must be complete cascade)
    /// * `operation_id` - Operation ID for audit context
    ///
    /// # Returns
    /// Number of events restored
    pub async fn execute_cascade_restore(
        &self,
        archived_ids: &[Uuid],
        operation_id: &str,
    ) -> DbResult<u64> {
        if archived_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = archived_ids.to_vec();
        // Use runtime query since the function is created by declarative apply SQL
        let count: i64 = sqlx::query_scalar(r"SELECT core.execute_cascade_restore($1::uuid[], $2)")
            .bind(&ids)
            .bind(operation_id)
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "execute cascade restore"))?;

        Ok(count as u64)
    }

    /// Populate cascade roots from a source table.
    ///
    /// Inserts the given event IDs into the cascade working table at depth 0,
    /// selecting their `source_event_ids` from `source`. Use `CascadeSource::Live`
    /// for archive operations (walking `core.events`) and `CascadeSource::Archive`
    /// for restore/tombstone analysis (walking `audit.archived_events`).
    pub async fn populate_cascade_roots_from(
        &self,
        table_name: &str,
        event_ids: &[Uuid],
        source: CascadeSource,
    ) -> DbResult<()> {
        if event_ids.is_empty() {
            return Ok(());
        }
        validate_cascade_table_name(table_name)?;

        let ids: Vec<Uuid> = event_ids.to_vec();
        let src = source.table_name();

        sqlx::query(&format!(
            r"
            INSERT INTO {table_name} (id, depth, parent_ids, processed)
            SELECT s.id, 0, COALESCE(s.source_event_ids, '{{}}'::UUID[]), FALSE
            FROM {src} s
            WHERE s.id = ANY($1::uuid[])
            ON CONFLICT (id) DO NOTHING
            "
        ))
        .bind(&ids)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "populate cascade roots"))?;

        Ok(())
    }

    /// Expand the cascade graph from a source table.
    ///
    /// Iteratively walks `source` to find events that reference nodes already in
    /// the cascade working table, inserting newly discovered children until no new
    /// rows are added or `max_depth` is reached.
    pub async fn expand_cascade_from(
        &self,
        table_name: &str,
        max_depth: i32,
        source: CascadeSource,
    ) -> DbResult<usize> {
        validate_cascade_table_name(table_name)?;

        let src = source.table_name();
        let mut current_depth = 0;

        while current_depth < max_depth {
            let rows_inserted = sqlx::query_scalar::<_, i64>(&format!(
                r"
                WITH new_children AS (
                    INSERT INTO {table_name} (id, depth, parent_ids, processed)
                    SELECT DISTINCT s.id, $1 + 1, COALESCE(s.source_event_ids, '{{}}'::UUID[]), FALSE
                    FROM {src} s
                    JOIN {table_name} ct ON s.source_event_ids && ARRAY[ct.id]
                    WHERE ct.depth = $1 AND ct.processed = FALSE
                    AND NOT EXISTS (SELECT 1 FROM {table_name} ex WHERE ex.id = s.id)
                    ON CONFLICT (id) DO NOTHING
                    RETURNING 1
                )
                SELECT COUNT(*)::BIGINT FROM new_children
                "
            ))
            .bind(current_depth)
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "expand cascade"))?;

            sqlx::query(&format!(
                "UPDATE {table_name} SET processed = TRUE WHERE depth = $1"
            ))
            .bind(current_depth)
            .execute(self.pool)
            .await
            .map_err(|e| db_error(e, "mark cascade depth processed"))?;

            if rows_inserted == 0 {
                break;
            }

            current_depth += 1;
        }

        Ok(current_depth as usize)
    }

    /// Get all event IDs in a cascade table (for execution).
    pub async fn get_cascade_ids(&self, table_name: &str) -> DbResult<Vec<Uuid>> {
        let rows = sqlx::query_scalar::<_, Uuid>(&format!(
            "SELECT id::uuid FROM {table_name} ORDER BY depth DESC"
        ))
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get cascade ids"))?;

        Ok(rows)
    }

    /// Count archived events matching filters.
    pub async fn count_archived_events(
        &self,
        source: Option<&EventSource>,
        before: Option<Timestamp>,
    ) -> DbResult<i64> {
        // Build query dynamically based on filters
        let query = match (source.is_some(), before.is_some()) {
            (true, true) => {
                "SELECT COUNT(*)::BIGINT FROM audit.archived_events WHERE source = $1 AND ts_orig < $2"
            }
            (true, false) => "SELECT COUNT(*)::BIGINT FROM audit.archived_events WHERE source = $1",
            (false, true) => {
                "SELECT COUNT(*)::BIGINT FROM audit.archived_events WHERE ts_orig < $1"
            }
            (false, false) => "SELECT COUNT(*)::BIGINT FROM audit.archived_events",
        };

        let mut q = sqlx::query_scalar::<_, i64>(query);
        if let Some(s) = source {
            q = q.bind(s.as_str());
        }
        if let Some(b) = before {
            q = q.bind(*b);
        }

        let count = q
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "count archived events"))?;

        Ok(count)
    }

    /// Get archived event IDs matching filters (for cascade analysis).
    pub async fn get_archived_event_ids(
        &self,
        source: Option<&EventSource>,
        before: Option<Timestamp>,
        limit: i64,
    ) -> DbResult<Vec<Uuid>> {
        let rows = match (source, before) {
            (Some(s), Some(b)) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM audit.archived_events WHERE source = $1 AND ts_orig < $2 ORDER BY ts_orig, id LIMIT $3"#,
                    s.as_str(),
                    *b,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (Some(s), None) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM audit.archived_events WHERE source = $1 ORDER BY ts_orig, id LIMIT $2"#,
                    s.as_str(),
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (None, Some(b)) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM audit.archived_events WHERE ts_orig < $1 ORDER BY ts_orig, id LIMIT $2"#,
                    *b,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (None, None) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM audit.archived_events ORDER BY ts_orig, id LIMIT $1"#,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
        }
        .map_err(|e| db_error(e, "get archived event ids"))?;

        Ok(rows)
    }

    // ========== Live Tier Operations (for Archive) ==========

    /// Get live event IDs matching filters (for archive operation).
    pub async fn get_live_event_ids(
        &self,
        source: Option<&EventSource>,
        before: Option<Timestamp>,
        limit: i64,
    ) -> DbResult<Vec<Uuid>> {
        let rows = match (source, before) {
            (Some(s), Some(b)) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM core.events WHERE source = $1 AND ts_orig < $2 ORDER BY ts_orig, id LIMIT $3"#,
                    s.as_str(),
                    *b,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (Some(s), None) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM core.events WHERE source = $1 ORDER BY ts_orig, id LIMIT $2"#,
                    s.as_str(),
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (None, Some(b)) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM core.events WHERE ts_orig < $1 ORDER BY ts_orig, id LIMIT $2"#,
                    *b,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (None, None) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM core.events ORDER BY ts_orig, id LIMIT $1"#,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
        }
        .map_err(|e| db_error(e, "get live event ids"))?;

        Ok(rows)
    }

    /// Execute cascade archive operation.
    ///
    /// Archives live events and their cascade by DELETE (trigger handles copy to archive).
    /// This requires setting session variables for audit context.
    ///
    /// # Arguments
    /// * `live_ids` - IDs of live events to archive (must be complete cascade)
    /// * `reason` - Human-readable reason for archiving
    /// * `operation_id` - `UUIDv7` for audit correlation
    /// * `archived_by` - Who initiated the archive (token prefix)
    ///
    /// # Returns
    /// Number of events archived
    pub async fn execute_cascade_archive(
        &self,
        live_ids: &[Uuid],
        reason: &str,
        operation_id: &str,
        archived_by: &str,
    ) -> DbResult<u64> {
        if live_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = live_ids.to_vec();

        // Begin transaction and set audit context
        let mut tx = self.pool.begin().await.map_err(|e| {
            db_error(
                e,
                &format!(
                    "Failed to begin transaction for archive of {} events",
                    live_ids.len()
                ),
            )
        })?;

        // Set session variables for audit trail (the trigger reads these)
        sqlx::query("SELECT pg_catalog.set_config('sinex.operation_id', $1, true)")
            .bind(operation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set operation_id"))?;

        sqlx::query("SELECT pg_catalog.set_config('sinex.archived_by', $1, true)")
            .bind(archived_by)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set archived_by"))?;

        sqlx::query("SELECT pg_catalog.set_config('sinex.archive_reason', $1, true)")
            .bind(reason)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "set archive_reason"))?;

        // Delete events - the trigger fn_archive_before_delete copies them to archive
        // Process in reverse depth order (children first, then parents) to avoid FK issues
        let result = sqlx::query("DELETE FROM core.events WHERE id = ANY($1::uuid[])")
            .bind(&ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "execute cascade archive"))?;

        let archived_count = result.rows_affected();

        tx.commit().await.map_err(|e| {
            db_error(
                e,
                &format!("Failed to commit archive transaction (archived {archived_count} events)"),
            )
        })?;

        tracing::info!(
            operation_id = %operation_id,
            archived_by = %archived_by,
            reason = %reason,
            archived_count = %archived_count,
            "Archived events via cascade operation"
        );

        Ok(archived_count)
    }
}

/// Relation kind for event replacements.
///
/// Describes how old events relate to their replacement events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplacementKind {
    /// 1:1 — one old event directly replaced by one new event
    Superseded,
    /// many:1 — multiple old events collapsed into one new event
    Collapsed,
    /// 1:many — one old event split into multiple new events
    Split,
    /// No confident equivalence match; linked by operation only
    Recomputed,
}

impl ReplacementKind {
    fn as_str(&self) -> &'static str {
        match self {
            ReplacementKind::Superseded => "superseded",
            ReplacementKind::Collapsed => "collapsed",
            ReplacementKind::Split => "split",
            ReplacementKind::Recomputed => "recomputed",
        }
    }
}

/// A single replacement relation to be recorded.
#[derive(Debug, Clone)]
pub struct ReplacementRecord {
    pub old_event_id: Uuid,
    pub new_event_id: Uuid,
    pub relation_kind: ReplacementKind,
    pub scope_key: Option<String>,
    pub equivalence_key: Option<String>,
}

impl EventRepository<'_> {
    /// Record event replacement relations for a replay operation.
    ///
    /// Inserts rows into `audit.event_replacements` linking archived (old) events
    /// to their replacement (new) events under a given operation.
    pub async fn record_replacements(
        &self,
        operation_id: Uuid,
        replacements: &[ReplacementRecord],
    ) -> DbResult<u64> {
        if replacements.is_empty() {
            return Ok(0);
        }

        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO audit.event_replacements \
             (old_event_id, new_event_id, operation_id, relation_kind, scope_key, equivalence_key) ",
        );

        builder.push_values(replacements, |mut b, r| {
            b.push_bind(r.old_event_id)
                .push_bind(r.new_event_id)
                .push_bind(operation_id)
                .push_bind(r.relation_kind.as_str())
                .push_bind(r.scope_key.as_deref())
                .push_bind(r.equivalence_key.as_deref());
        });

        let result = builder
            .build()
            .execute(self.pool)
            .await
            .map_err(|e| db_error(e, "record event replacements"))?;

        Ok(result.rows_affected())
    }

    /// Query replacement relations for a specific operation.
    pub async fn get_replacements_by_operation(
        &self,
        operation_id: Uuid,
    ) -> DbResult<Vec<(Uuid, Uuid, String, Option<String>, Option<String>)>> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, String, Option<String>, Option<String>)>(
            "SELECT old_event_id, new_event_id, relation_kind, scope_key, equivalence_key \
             FROM audit.event_replacements WHERE operation_id = $1 ORDER BY replaced_at",
        )
        .bind(operation_id)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get replacements by operation"))?;
        Ok(rows)
    }

    /// Query what replaced a specific archived event.
    pub async fn get_replacements_for_event(
        &self,
        old_event_id: Uuid,
    ) -> DbResult<Vec<(Uuid, String, Uuid)>> {
        let rows = sqlx::query_as::<_, (Uuid, String, Uuid)>(
            "SELECT new_event_id, relation_kind, operation_id \
             FROM audit.event_replacements WHERE old_event_id = $1 ORDER BY replaced_at",
        )
        .bind(old_event_id)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get replacements for event"))?;
        Ok(rows)
    }
}

/// Lifecycle tier status record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct LifecycleTierStatus {
    pub tier: DataTier,
    pub event_count: i64,
    pub oldest_ts: Option<Timestamp>,
    pub newest_ts: Option<Timestamp>,
    pub distinct_sources: i64,
}

pub struct EventRepositoryTx<'a, 't> {
    tx: &'a mut Transaction<'t, Postgres>,
}

impl<'a, 't> EventRepositoryTx<'a, 't> {
    pub fn new(tx: &'a mut Transaction<'t, Postgres>) -> Self {
        Self { tx }
    }

    pub async fn prepare_cascade_session(
        &mut self,
        session_id: &str,
        drop_on_commit: bool,
    ) -> DbResult<String> {
        sqlx::query_scalar!(
            r#"SELECT core.prepare_cascade_session($1, $2) AS "table_name!""#,
            session_id,
            drop_on_commit
        )
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "prepare cascade session"))
    }

    pub async fn populate_cascade_roots(
        &mut self,
        table_name: &str,
        event_ids: &[Uuid],
    ) -> DbResult<()> {
        let ids: Vec<Uuid> = event_ids.to_vec();
        sqlx::query_scalar::<_, i64>(
            r"SELECT core.cascade_populate_roots($1, $2::uuid[]) as inserted",
        )
        .bind(table_name)
        .bind(&ids)
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "populate cascade roots"))?;
        Ok(())
    }

    pub async fn populate_cascade_roots_from(
        &mut self,
        table_name: &str,
        event_ids: &[Uuid],
        source: CascadeSource,
    ) -> DbResult<()> {
        if event_ids.is_empty() {
            return Ok(());
        }
        validate_cascade_table_name(table_name)?;

        let ids: Vec<Uuid> = event_ids.to_vec();
        let src = source.table_name();

        sqlx::query(&format!(
            r"
            INSERT INTO {table_name} (id, depth, parent_ids, processed)
            SELECT s.id, 0, COALESCE(s.source_event_ids, '{{}}'::UUID[]), FALSE
            FROM {src} s
            WHERE s.id = ANY($1::uuid[])
            ON CONFLICT (id) DO NOTHING
            "
        ))
        .bind(&ids)
        .execute(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "populate cascade roots"))?;

        Ok(())
    }

    /// Expand cascade graph to find all descendants (transaction version)
    ///
    /// # Cycle Detection
    /// IMPORTANT: The database function `core.expand_cascade` MUST implement cycle detection
    /// to prevent infinite loops when circular event dependencies exist. See the non-transaction
    /// version for detailed requirements.
    pub async fn expand_cascade(&mut self, table_name: &str, max_depth: i32) -> DbResult<usize> {
        let depth = sqlx::query_scalar!(
            r#"SELECT core.expand_cascade($1, $2)"#,
            table_name,
            max_depth
        )
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "expand cascade graph"))?
        .unwrap_or(0);
        Ok(depth as usize)
    }

    pub async fn expand_cascade_from(
        &mut self,
        table_name: &str,
        max_depth: i32,
        source: CascadeSource,
    ) -> DbResult<usize> {
        validate_cascade_table_name(table_name)?;

        let src = source.table_name();
        let mut current_depth = 0;

        while current_depth < max_depth {
            let rows_inserted = sqlx::query_scalar::<_, i64>(&format!(
                r"
                WITH new_children AS (
                    INSERT INTO {table_name} (id, depth, parent_ids, processed)
                    SELECT DISTINCT s.id, $1 + 1, COALESCE(s.source_event_ids, '{{}}'::UUID[]), FALSE
                    FROM {src} s
                    JOIN {table_name} ct ON s.source_event_ids && ARRAY[ct.id]
                    WHERE ct.depth = $1 AND ct.processed = FALSE
                    AND NOT EXISTS (SELECT 1 FROM {table_name} ex WHERE ex.id = s.id)
                    ON CONFLICT (id) DO NOTHING
                    RETURNING 1
                )
                SELECT COUNT(*)::BIGINT FROM new_children
                "
            ))
            .bind(current_depth)
            .fetch_one(&mut **self.tx)
            .await
            .map_err(|e| db_error(e, "expand cascade"))?;

            sqlx::query(&format!(
                "UPDATE {table_name} SET processed = TRUE WHERE depth = $1"
            ))
            .bind(current_depth)
            .execute(&mut **self.tx)
            .await
            .map_err(|e| db_error(e, "mark cascade depth processed"))?;

            if rows_inserted == 0 {
                break;
            }

            current_depth += 1;
        }

        Ok(current_depth as usize)
    }

    pub async fn cascade_depth_histogram(&mut self, table_name: &str) -> DbResult<Vec<(i32, i64)>> {
        let rows = sqlx::query!(
            r#"SELECT depth as "depth!", node_count as "node_count!" FROM core.cascade_depth_histogram($1)"#,
            table_name
        )
        .fetch_all(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "cascade depth histogram"))?;
        Ok(rows
            .into_iter()
            .map(|row| (row.depth, row.node_count))
            .collect())
    }

    pub async fn cascade_node_count(&mut self, table_name: &str) -> DbResult<i64> {
        sqlx::query_scalar!(
            r#"SELECT core.cascade_count_nodes($1) as "count!""#,
            table_name
        )
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "count cascade nodes"))
    }

    pub async fn cascade_integrity_violations(
        &mut self,
        table_name: &str,
        limit: i32,
    ) -> DbResult<Vec<(Uuid, Uuid)>> {
        #[derive(sqlx::FromRow)]
        struct ViolationRow {
            live_event_id: Uuid,
            archived_event_id: Uuid,
        }

        let rows = sqlx::query_as::<_, ViolationRow>(
            "SELECT live_event_id, archived_event_id FROM core.cascade_find_integrity_violations($1, $2)",
        )
        .bind(table_name)
        .bind(limit)
        .fetch_all(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "find cascade integrity violations"))?;

        Ok(rows
            .into_iter()
            .map(|row| (row.live_event_id, row.archived_event_id))
            .collect())
    }

    pub async fn cascade_integrity_violations_paginated(
        &mut self,
        table_name: &str,
        limit: i32,
        offset: i32,
    ) -> DbResult<Vec<(Uuid, Uuid)>> {
        #[derive(sqlx::FromRow)]
        struct ViolationRow {
            live_event_id: Uuid,
            archived_event_id: Uuid,
        }

        let rows = sqlx::query_as::<_, ViolationRow>(
            "SELECT live_event_id, archived_event_id FROM core.cascade_find_integrity_violations_paginated($1, $2, $3)"
        )
        .bind(table_name)
        .bind(limit)
        .bind(offset)
        .fetch_all(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "find cascade integrity violations paginated"))?;

        Ok(rows
            .into_iter()
            .map(|row| (row.live_event_id, row.archived_event_id))
            .collect())
    }

    pub async fn get_event_dependencies(
        &mut self,
        table_name: &str,
    ) -> DbResult<Vec<(Uuid, Vec<Uuid>)>> {
        let query = format!(
            r"
            SELECT
                id::uuid as event_id,
                parent_ids::uuid[] as parent_ids
            FROM {table_name}
            "
        );

        let rows = sqlx::query(&query)
            .fetch_all(&mut **self.tx)
            .await
            .map_err(|e| db_error(e, "get event dependencies"))?;

        let mut result = Vec::new();
        for row in rows {
            let event_id: Uuid = row
                .try_get("event_id")
                .map_err(|e| db_error(e, "parse event_id"))?;
            let parent_ids: Vec<Uuid> = row
                .try_get("parent_ids")
                .map_err(|e| db_error(e, "parse parent_ids"))?;
            result.push((event_id, parent_ids));
        }

        Ok(result)
    }

    pub async fn get_cascade_ids(&mut self, table_name: &str) -> DbResult<Vec<Uuid>> {
        validate_cascade_table_name(table_name)?;

        sqlx::query_scalar::<_, Uuid>(&format!(
            "SELECT id::uuid FROM {table_name} ORDER BY depth DESC"
        ))
        .fetch_all(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "get cascade ids"))
    }

    pub async fn cleanup_cascade_session(&mut self, table_name: &str) -> DbResult<()> {
        sqlx::query!("SELECT core.cleanup_cascade_session($1)", table_name)
            .execute(&mut **self.tx)
            .await
            .map_err(|e| db_error(e, "cleanup cascade session"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::sinex_test;

    fn base_stream_batch_row() -> color_eyre::Result<StreamBatchRow> {
        Ok(StreamBatchRow {
            id: Uuid::now_v7(),
            source: EventSource::new("test.source")?,
            event_type: EventType::new("test.event")?,
            ts_orig: Timestamp::now(),
            host: HostName::from_static("localhost"),
            payload: json!({"ok": true}),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: None,
            payload_schema_id: None,
            node_run_id: None,
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        })
    }

    fn base_record() -> EventRecord {
        let ts = Timestamp::now();
        let subnano = ts.nanosecond() as i32;
        EventRecord {
            id: uuid::Uuid::now_v7(),
            source: "test.source".to_string(),
            event_type: "test.event".to_string(),
            host: "localhost".to_string(),
            payload: json!({"ok": true}),
            ts_orig: ts,
            ts_orig_subnano: Some(subnano),
            ts_coided: Timestamp::now(),
            ts_persisted: Timestamp::now(),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: None,
            associated_blob_ids: None,
            payload_schema_id: None,
            node_run_id: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        }
    }

    #[sinex_test]
    async fn missing_provenance_is_rejected() -> color_eyre::Result<()> {
        let record = base_record();
        let err = record.try_to_event().expect_err("should fail");
        assert!(format!("{err}").contains("missing provenance"));
        Ok(())
    }

    #[sinex_test]
    async fn material_provenance_requires_anchor() -> color_eyre::Result<()> {
        let mut record = base_record();
        record.source_material_id = Some(uuid::Uuid::now_v7());
        let err = record.try_to_event().expect_err("should fail");
        assert!(format!("{err}").contains("anchor"));
        Ok(())
    }

    #[sinex_test]
    async fn valid_material_provenance_passes() -> color_eyre::Result<()> {
        let mut record = base_record();
        record.source_material_id = Some(uuid::Uuid::now_v7());
        record.anchor_byte = Some(42);
        assert!(record.try_to_event().is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn invalid_material_offset_kind_is_rejected() -> color_eyre::Result<()> {
        let mut record = base_record();
        record.source_material_id = Some(uuid::Uuid::now_v7());
        record.anchor_byte = Some(42);
        record.offset_kind = Some("mystery".to_string());
        let err = record.try_to_event().expect_err("should fail");
        assert!(format!("{err}").contains("invalid offset kind"));
        Ok(())
    }

    #[sinex_test]
    async fn synthesis_provenance_requires_non_empty_sources() -> color_eyre::Result<()> {
        let mut record = base_record();
        record.source_event_ids = Some(vec![]);
        let err = record.try_to_event().expect_err("should fail");
        assert!(format!("{err}").contains("source_event_ids"));
        Ok(())
    }

    #[sinex_test]
    async fn synthesis_operation_lineage_round_trips_from_record() -> color_eyre::Result<()> {
        let mut record = base_record();
        let parent_id = uuid::Uuid::now_v7();
        let operation_id = uuid::Uuid::now_v7();
        record.source_event_ids = Some(vec![parent_id]);
        record.created_by_operation_id = Some(operation_id);

        let event = record.try_to_event()?;

        match &event.provenance {
            crate::models::Provenance::Synthesis {
                source_event_ids,
                operation_id: provenance_operation_id,
            } => {
                assert_eq!(
                    source_event_ids.as_slice(),
                    &[sinex_primitives::events::EventId::from_uuid(parent_id)]
                );
                assert_eq!(
                    provenance_operation_id.as_ref().map(Id::to_uuid),
                    Some(operation_id)
                );
            }
            other => panic!("expected synthesis provenance, got {other:?}"),
        }
        assert_eq!(event.created_by_operation_id, Some(operation_id));

        Ok(())
    }

    #[sinex_test]
    async fn mismatched_operation_lineage_is_rejected() -> color_eyre::Result<()> {
        let parent_id = Id::<Event<JsonValue>>::new();
        let provenance_operation_id = Id::<sinex_primitives::events::builder::Operation>::new();
        let event = Event {
            id: Some(Id::new()),
            source: EventSource::new("test.source")?,
            event_type: EventType::new("test.event")?,
            host: HostName::from_static("localhost"),
            payload: json!({"ok": true}),
            ts_orig: Some(Timestamp::now()),
            node_run_id: None,
            payload_schema_id: None,
            provenance: crate::models::Provenance::from_synthesis_safe(parent_id, vec![])
                .with_operation(provenance_operation_id),
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: Some(uuid::Uuid::now_v7()),
            node_model: None,
        };

        let err = resolved_created_by_operation_id(&event).expect_err("should fail");
        assert!(format!("{err}").contains("operation lineage mismatch"));

        Ok(())
    }

    #[sinex_test]
    async fn stream_batch_insert_strategy_prefers_query_builder_for_small_material_batches()
    -> color_eyre::Result<()> {
        let batch = vec![base_stream_batch_row()?];
        assert_eq!(
            EventRepository::stream_batch_insert_strategy(&batch),
            Some(StreamBatchInsertStrategy::QueryBuilder)
        );
        Ok(())
    }

    #[sinex_test]
    async fn stream_batch_insert_strategy_prefers_copy_for_large_material_batches()
    -> color_eyre::Result<()> {
        let batch = (0..COPY_BATCH_THRESHOLD)
            .map(|_| base_stream_batch_row())
            .collect::<color_eyre::Result<Vec<_>>>()?;
        assert_eq!(
            EventRepository::stream_batch_insert_strategy(&batch),
            Some(StreamBatchInsertStrategy::Copy)
        );
        Ok(())
    }

    #[sinex_test]
    async fn stream_batch_insert_strategy_prefers_synthesis_for_parent_batches()
    -> color_eyre::Result<()> {
        let mut row = base_stream_batch_row()?;
        row.source_event_ids = Some(vec![EventId::from_uuid(Uuid::now_v7())]);
        let batch = vec![row];
        assert_eq!(
            EventRepository::stream_batch_insert_strategy(&batch),
            Some(StreamBatchInsertStrategy::Synthesis)
        );
        Ok(())
    }
}
