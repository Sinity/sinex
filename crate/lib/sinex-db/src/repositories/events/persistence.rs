use super::conversions::{extract_provenance, EventRecordExt};
use crate::models::{Event, JsonValue};
use crate::query_helpers::ulid_to_uuid;
use crate::repositories::common::{db_error, DbResult, EnhancedRepository, Repository};
use crate::schema::Events;
use crate::{EventRecord, SinexError};
use sinex_primitives::domain::{EventSource, EventType, SchemaVersion};
use sinex_primitives::{Id, Timestamp, Ulid};

use serde::{Deserialize, Serialize};
use sqlx::{Executor, FromRow, PgPool, Postgres, QueryBuilder, Transaction};
use tracing::instrument;
use uuid::Uuid;

/// Lightweight DTO for stream batch inserts from ingestd.
///
/// This struct provides a minimal representation of event data for high-throughput
/// batch inserts, avoiding the overhead of the full `Event<T>` type tree.
/// All fields are pre-validated and pre-parsed by the caller.
#[derive(Debug, Clone)]
pub struct StreamBatchRow {
    /// Pre-parsed ULID for the event
    pub id: Ulid,
    /// Event source identifier
    pub source: String,
    /// Event type identifier
    pub event_type: String,
    /// Pre-parsed timestamp
    pub ts_orig: Timestamp,
    /// Hostname where event originated
    pub host: String,
    /// Event payload as JSON
    pub payload: JsonValue,
    /// Source material ID (for material provenance)
    pub source_material_id: Option<Uuid>,
    /// Anchor byte offset into source material
    pub anchor_byte: Option<i64>,
    /// Start offset within source material
    pub offset_start: Option<i64>,
    /// End offset within source material
    pub offset_end: Option<i64>,
    /// Offset kind (e.g., "byte", "line")
    pub offset_kind: Option<String>,
    /// Parent event IDs (for synthesis provenance)
    pub source_event_ids: Option<Vec<Uuid>>,
    /// Schema ID for payload validation
    pub payload_schema_id: Option<Uuid>,
    /// Version of the ingestor that produced this event
    pub ingestor_version: Option<String>,
    /// Associated blob IDs
    pub associated_blob_ids: Option<Vec<Uuid>>,
}

/// Result of a stream batch insert operation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamBatchInsertResult {
    /// Number of rows successfully inserted
    pub inserted_count: usize,
    /// IDs of events that were actually inserted (excludes conflicts).
    /// Only populated when using ON CONFLICT DO NOTHING.
    pub inserted_ids: Option<Vec<Ulid>>,
}

/// Event repository for database operations
pub struct EventRepository<'a> {
    pub(super) pool: &'a PgPool,
}

async fn ensure_no_synthesis_cycles<'e, E>(
    executor: E,
    event_id: &Id<Event<JsonValue>>,
    source_event_ids: &[Ulid],
) -> DbResult<()>
where
    E: Executor<'e, Database = Postgres>,
{
    if source_event_ids.is_empty() {
        return Ok(());
    }

    // Warn about unbounded array growth
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

    if source_event_ids
        .iter()
        .any(|source_id| source_id == event_id.as_ulid())
    {
        return Err(SinexError::database(
            "cycle detected in synthesis provenance",
        ));
    }

    let source_event_uuids: Vec<Uuid> = source_event_ids.iter().map(|id| id.as_uuid()).collect();
    let has_cycle = sqlx::query_scalar!(
        r#"
        WITH RECURSIVE parents AS (
            SELECT id, source_event_ids
            FROM core.events
            WHERE id = ANY($1::uuid[]::ulid[])
            UNION
            SELECT e.id, e.source_event_ids
            FROM core.events e
            JOIN parents p ON e.id = ANY(p.source_event_ids)
        )
        SELECT EXISTS (
            SELECT 1 FROM parents WHERE $2::uuid::ulid = ANY(source_event_ids)
        ) AS "has_cycle!"
        "#,
        &source_event_uuids,
        event_id.as_ulid().as_uuid()
    )
    .fetch_one(executor)
    .await
    .map_err(|e| db_error(e, "check synthesis cycle"))?;

    if has_cycle {
        return Err(SinexError::database(
            "cycle detected in synthesis provenance",
        ));
    }

    Ok(())
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

/// Event payload schema record
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, FromRow)]
pub struct EventPayloadSchema {
    pub id: Id<EventPayloadSchema>,
    pub source: String,
    pub event_type: String,
    pub schema_version: SchemaVersion,
    pub schema_content: JsonValue,
    pub content_hash: String,
    pub is_active: bool,
    pub updated_at: Timestamp,
}

/// New schema input structure
#[derive(Debug)]
pub struct NewSchema {
    pub source: String,
    pub event_type: String,
    pub schema_version: SchemaVersion,
    pub schema_content: JsonValue,
    pub content_hash: String,
    pub is_active: bool,
}

/// Event annotation record
#[derive(Debug, FromRow)]
pub struct EventAnnotation {
    pub id: Id<EventAnnotation>,
    pub event_id: Id<Event<JsonValue>>,
    pub annotation_type: String,
    pub content: String,
    pub metadata: JsonValue,
    pub created_by: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Invalid payload event record
#[derive(Debug)]
pub struct InvalidPayloadEvent {
    pub event_id: Id<Event<JsonValue>>,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: Timestamp,
    pub payload: JsonValue,
}

/// Batch violation record
#[derive(Debug, FromRow)]
pub struct BatchViolation {
    pub event_id: Option<Id<Event<JsonValue>>>,
    pub prev_event_id: Option<Id<Event<JsonValue>>>,
    pub ts_orig: Option<Timestamp>,
    pub prev_ts_orig: Option<Timestamp>,
    pub source: String,
    pub row_num: Option<i64>,
}

/// Suspicious event record
#[derive(Debug, FromRow)]
pub struct SuspiciousEvent {
    pub event_id: Id<Event<JsonValue>>,
    pub source: String,
    pub event_type: String,
    pub payload: JsonValue,
    pub payload_type: Option<String>,
    pub payload_size: Option<i32>,
}

/// Invalid timestamp record
#[derive(Debug)]
pub struct InvalidTimestamp {
    pub event_id: Id<Event<JsonValue>>,
    pub ts_orig: Option<Timestamp>,
    pub ts_ingest: Timestamp,
}

/// Command count for analytics
#[derive(Debug)]
pub struct CommandCount {
    pub command: String,
    pub count: i64,
}

/// Source activity statistics
#[derive(Debug)]
pub struct SourceActivity {
    pub source: String,
    pub event_count: i64,
    pub first_event: Option<Timestamp>,
    pub last_event: Option<Timestamp>,
}

/// Event type count
#[derive(Debug)]
pub struct EventTypeCount {
    pub event_type: String,
    pub count: i64,
}

impl<'a> EventRepository<'a> {
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
                &format!("Failed to prepare cascade session '{}'", session_id),
            )
        })
    }

    pub async fn populate_cascade_roots(
        &self,
        table_name: &str,
        event_ids: &[Ulid],
    ) -> DbResult<()> {
        let ids: Vec<Uuid> = event_ids.iter().map(|id| id.to_uuid()).collect();
        sqlx::query_scalar::<_, i64>(
            r#"SELECT core.cascade_populate_roots($1, $2::ulid[]) as inserted"#,
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
    /// - Respect the max_depth limit as a safety bound
    ///
    /// Without proper cycle detection, circular references (A -> B -> C -> A) will cause
    /// the function to loop indefinitely or exceed max_depth.
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
                    "Failed to expand cascade graph for table '{}' (max_depth={})",
                    table_name, max_depth
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
    ) -> DbResult<Vec<(Ulid, Ulid)>> {
        sqlx::query!(
            r#"
            SELECT
                live_event_id as "live_event_id!: Ulid",
                archived_event_id as "archived_event_id!: Ulid"
            FROM core.cascade_find_integrity_violations($1, $2)
            "#,
            table_name,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find cascade integrity violations"))
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.live_event_id, row.archived_event_id))
                .collect()
        })
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
            set_repeatable_read, with_retry_transaction_idempotent, IdempotentTransaction,
            RetryConfig,
        };

        // Convert typed event to JSON event for storage, preserving any explicit ID.
        let event_id = event
            .id
            .as_ref()
            .map(|id| Id::<Event<JsonValue>>::from_ulid(*id.as_ulid()));
        let mut event = event.to_json_event().map_err(|e| {
            SinexError::database("Failed to serialize event payload").with_source(e)
        })?;
        if event.id.is_none() {
            event.id = event_id;
        }
        let id = event
            .id
            .get_or_insert_with(Id::<Event<JsonValue>>::new)
            .clone();

        // Extract provenance into separate fields
        let (
            source_event_ids,
            source_material_id,
            offset_start,
            offset_end,
            offset_kind,
            anchor_byte,
        ) = extract_provenance(&event)?;

        // Convert ULIDs to UUIDs
        let source_event_uuids = source_event_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());
        let associated_blob_uuids = event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());

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
        let ingestor_version = event.ingestor_version.clone();
        let payload_schema_id = event.payload_schema_id.map(|id| id.as_uuid());

        // Execute with retry logic
        with_retry_transaction_idempotent(
            self.pool,
            RetryConfig::default(),
            IdempotentTransaction::new(),
            move |tx| {
                let id = id.clone();
                let source_event_ids = source_event_ids.clone();
                let source_material_id = source_material_id;
                let source_event_uuids = source_event_uuids.clone();
                let associated_blob_uuids = associated_blob_uuids.clone();
                let event_source = event_source.clone();
                let event_type = event_type.clone();
                let host = host.clone();
                let payload = payload.clone();
                let ingestor_version = ingestor_version.clone();
                let offset_kind = offset_kind.clone();

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
                            ts_orig, ts_orig_subnano, ingestor_version, payload_schema_id, source_event_ids,
                            source_material_id, offset_start, offset_end, offset_kind,
                            anchor_byte, associated_blob_ids
                        ) VALUES (
                            $1::uuid::ulid, $2, $3, $4, $5,
                            $6, $7, $8, $9::uuid::ulid, $10::uuid[]::ulid[],
                            $11::uuid::ulid, $12, $13, $14,
                            $15, $16::uuid[]::ulid[]
                        )
                        RETURNING
                            id::uuid as "id!: sinex_schema::ulid::Ulid",
                            source as "source!",
                            event_type as "event_type!",
                            ts_ingest as "ts_ingest: Timestamp",
                            ts_orig as "ts_orig: Timestamp",
                            ts_orig_subnano,
                            host as "host!",
                            ingestor_version,
                            payload_schema_id::uuid as "payload_schema_id: sinex_schema::ulid::Ulid",
                            payload as "payload!",
                            source_event_ids::uuid[] as "source_event_ids: Vec<sinex_schema::ulid::Ulid>",
                            source_material_id::uuid as "source_material_id: sinex_schema::ulid::Ulid",
                            offset_start,
                            offset_end,
                            offset_kind,
                            anchor_byte,
                            associated_blob_ids::uuid[] as "associated_blob_ids: Vec<sinex_schema::ulid::Ulid>"
                        "#,
                        id.as_ulid().as_uuid(),
                        event_source.as_str(),
                        event_type.as_str(),
                        host.as_str(),
                        payload,
                        ts_orig,
                        ts_orig_subnano,
                        ingestor_version,
                        payload_schema_id,
                        source_event_uuids.as_deref(),
                        source_material_id.map(|id| id.as_uuid()),
                        offset_start,
                        offset_end,
                        offset_kind.as_deref(),
                        anchor_byte,
                        associated_blob_uuids.as_deref()
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
            .map(|id| Id::<Event<JsonValue>>::from_ulid(*id.as_ulid()));
        let mut event = event.to_json_event().map_err(|e| {
            SinexError::database("Failed to serialize event payload").with_source(e)
        })?;
        if event.id.is_none() {
            event.id = event_id;
        }
        let id = event
            .id
            .get_or_insert_with(Id::<Event<JsonValue>>::new)
            .clone();

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

        // Convert ULIDs to UUIDs before the query to avoid temporary value issues
        let source_event_uuids = source_event_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());
        let associated_blob_uuids = event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());

        // Postgres timestamps are microsecond precision. Persist the sub-microsecond
        // remainder separately so we can reconstruct full nanosecond timestamps on read.
        let (ts_orig, ts_orig_subnano) = match event.ts_orig {
            Some(ts) => {
                let (pg, sub) = ts.to_postgres_parts();
                (Some(pg), Some(sub))
            }
            None => (None, None),
        };

        let record = sqlx::query_as!(
            EventRecord,
            r#"
            INSERT INTO core.events (
                id, source, event_type, host, payload,
                ts_orig, ts_orig_subnano, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, offset_start, offset_end, offset_kind,
                anchor_byte, associated_blob_ids
            ) VALUES (
                $1::uuid::ulid, $2, $3, $4, $5,
                $6, $7, $8, $9::uuid::ulid, $10::uuid[]::ulid[],
                $11::uuid::ulid, $12, $13, $14,
                $15, $16::uuid[]::ulid[]
            )
            RETURNING
                id::uuid as "id!: sinex_schema::ulid::Ulid",
                source as "source!",
                event_type as "event_type!",
                ts_ingest as "ts_ingest: Timestamp",
                ts_orig as "ts_orig: Timestamp",
                ts_orig_subnano,
                host as "host!",
                ingestor_version,
                payload_schema_id::uuid as "payload_schema_id: sinex_schema::ulid::Ulid",
                payload as "payload!",
                source_event_ids::uuid[] as "source_event_ids: Vec<sinex_schema::ulid::Ulid>",
                source_material_id::uuid as "source_material_id: sinex_schema::ulid::Ulid",
                offset_start,
                offset_end,
                offset_kind,
                anchor_byte,
                associated_blob_ids::uuid[] as "associated_blob_ids: Vec<sinex_schema::ulid::Ulid>"
            "#,
            id.as_ulid().as_uuid(),
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            ts_orig,
            ts_orig_subnano,
            event.ingestor_version,
            event.payload_schema_id.map(|id| id.as_uuid()),
            source_event_uuids.as_deref(),
            source_material_id.map(|id| id.as_uuid()),
            offset_start,
            offset_end,
            offset_kind.as_deref(),
            anchor_byte,
            associated_blob_uuids.as_deref()
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error(e, "insert event with tx"))?;

        Ok(record.try_to_event()?)
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
                .map(|id| Id::<Event<JsonValue>>::from_ulid(*id.as_ulid()));
            let mut json_event = event.to_json_event().map_err(|e| {
                SinexError::database("Failed to serialize event payload").with_source(e)
            })?;
            if json_event.id.is_none() {
                json_event.id = event_id;
            }
            json_events.push(json_event);
        }
        let events = json_events;
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // For small batches, use the optimized single-transaction approach
        if events.len() <= 50 {
            return self.insert_batch_unnest(events).await;
        }

        // For larger batches, chunk them to avoid overwhelming the database
        let chunk_size = 50; // Optimal chunk size for batch processing
        let max_concurrent_chunks = 3; // Conservative concurrency to avoid pool exhaustion

        let mut results = Vec::with_capacity(events.len());

        // Process chunks with controlled concurrency
        let total_events = events.len();
        let mut processed = 0;

        for chunk_batch in events.chunks(chunk_size * max_concurrent_chunks) {
            let mut chunk_futures = Vec::new();

            for chunk in chunk_batch.chunks(chunk_size) {
                let chunk_vec = chunk.to_vec();
                chunk_futures.push(self.insert_batch_unnest(chunk_vec));
            }

            // Wait for this batch of chunks to complete
            let chunk_results = futures::future::join_all(chunk_futures).await;

            // Collect results and propagate any errors immediately
            for result in chunk_results {
                match result {
                    Ok(mut chunk_results) => {
                        processed += chunk_results.len();
                        results.append(&mut chunk_results);

                        // Log progress every 1000 events for visibility on large batches
                        if processed % 1000 == 0 || processed == total_events {
                            tracing::debug!(
                                processed = processed,
                                total = total_events,
                                progress_pct =
                                    (processed as f64 / total_events as f64 * 100.0) as u32,
                                "Batch insert progress"
                            );
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(results)
    }

    /// Optimized batch insert with transaction batching for better performance
    async fn insert_batch_unnest(
        &self,
        mut events: Vec<Event<JsonValue>>,
    ) -> DbResult<Vec<Event<JsonValue>>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // For very small batches, use individual inserts to avoid overhead
        if events.len() == 1 {
            let Some(event) = events.into_iter().next() else {
                return Err(db_error(
                    sqlx::Error::Protocol("single-element batch missing event".into()),
                    "insert batch",
                ));
            };
            let inserted = self.insert(event).await?;
            return Ok(vec![inserted]);
        }

        // Ensure all events have IDs
        for event in &mut events {
            if event.id.is_none() {
                event.id = Some(Id::<Event<JsonValue>>::new());
            }
        }

        // Collect events needing synthesis cycle detection (populated during
        // vector build, checked after transaction begins).
        let mut synthesis_checks: Vec<(Id<Event<JsonValue>>, Vec<Ulid>)> = Vec::new();

        let mut ids = Vec::with_capacity(events.len());
        let mut sources = Vec::with_capacity(events.len());
        let mut event_types = Vec::with_capacity(events.len());
        let mut hosts = Vec::with_capacity(events.len());
        let mut payloads = Vec::with_capacity(events.len());
        let mut ts_orig_values = Vec::with_capacity(events.len());
        let mut ts_orig_subnanos = Vec::with_capacity(events.len());
        let mut ingestor_versions = Vec::with_capacity(events.len());
        let mut payload_schema_ids = Vec::with_capacity(events.len());
        let mut source_event_ids = Vec::with_capacity(events.len());
        let mut source_material_ids = Vec::with_capacity(events.len());
        let mut offset_starts = Vec::with_capacity(events.len());
        let mut offset_ends = Vec::with_capacity(events.len());
        let mut offset_kinds = Vec::with_capacity(events.len());
        let mut anchor_bytes = Vec::with_capacity(events.len());
        let mut associated_blob_ids = Vec::with_capacity(events.len());

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
                .as_ulid()
                .as_uuid();

            // Extract provenance
            let (
                source_event_ids_raw,
                source_material_id,
                offset_start,
                offset_end,
                offset_kind,
                anchor_byte,
            ) = extract_provenance(event)?;

            // Track events with synthesis provenance for cycle detection
            if let Some(ref ulids) = source_event_ids_raw {
                if !ulids.is_empty() {
                    synthesis_checks.push((
                        event.id.clone().unwrap(), // guaranteed set above
                        ulids.clone(),
                    ));
                }
            }

            let source_event_uuids = source_event_ids_raw
                .map(|ids| ids.into_iter().map(|id| id.as_uuid()).collect::<Vec<_>>());
            let associated_blob_uuids = event
                .associated_blob_ids
                .as_ref()
                .map(|ids| ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>());

            // Postgres timestamps are microsecond precision. Persist the sub-microsecond
            // remainder separately so we can reconstruct full nanosecond timestamps on read.
            let ts_orig = event.ts_orig;
            let ts_orig_subnano = ts_orig.map(|ts| (ts.nanosecond() % 1_000) as i32);
            let ts_orig = ts_orig.map(|ts| {
                let truncated = (ts.nanosecond() / 1_000) * 1_000;
                ts.replace_nanosecond(truncated)
                    .map(Timestamp::new)
                    .unwrap_or(ts)
            });

            ids.push(event_id);
            sources.push(event.source.as_str().to_string());
            event_types.push(event.event_type.as_str().to_string());
            hosts.push(event.host.as_str().to_string());
            payloads.push(event.payload.clone());
            ts_orig_values.push(ts_orig);
            ts_orig_subnanos.push(ts_orig_subnano);
            ingestor_versions.push(event.ingestor_version.clone());
            payload_schema_ids.push(event.payload_schema_id.map(|id| id.as_uuid()));
            source_event_ids.push(source_event_uuids);
            source_material_ids.push(source_material_id.map(|id| id.as_uuid()));
            offset_starts.push(offset_start);
            offset_ends.push(offset_end);
            offset_kinds.push(offset_kind);
            anchor_bytes.push(anchor_byte);
            associated_blob_ids.push(associated_blob_uuids);
        }

        // Begin transaction for atomicity
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

        // Enforce synthesis cycle detection (parity with insert/insert_stream_batch)
        for (event_id, source_ulids) in &synthesis_checks {
            ensure_no_synthesis_cycles(&mut *tx, event_id, source_ulids).await?;
        }

        // QueryBuilder is required here because UNNEST cannot represent ragged arrays
        // (source_event_ids/associated_blob_ids) and `query!` rejects array nulls.
        let mut builder = QueryBuilder::new(
            "INSERT INTO core.events (
                id, source, event_type, host, payload,
                ts_orig, ts_orig_subnano, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, offset_start, offset_end, offset_kind,
                anchor_byte, associated_blob_ids
            ) ",
        );
        builder.push_values(0..ids.len(), |mut b, idx| {
            b.push_bind(&ids[idx]).push_unseparated("::uuid::ulid");
            b.push_bind(&sources[idx]);
            b.push_bind(&event_types[idx]);
            b.push_bind(&hosts[idx]);
            b.push_bind(&payloads[idx]);
            b.push_bind(&ts_orig_values[idx]);
            b.push_bind(&ts_orig_subnanos[idx]);
            b.push_bind(&ingestor_versions[idx]);
            b.push_bind(&payload_schema_ids[idx])
                .push_unseparated("::uuid::ulid");
            b.push_bind(&source_event_ids[idx])
                .push_unseparated("::uuid[]::ulid[]");
            b.push_bind(&source_material_ids[idx])
                .push_unseparated("::uuid::ulid");
            b.push_bind(&offset_starts[idx]);
            b.push_bind(&offset_ends[idx]);
            b.push_bind(&offset_kinds[idx]);
            b.push_bind(&anchor_bytes[idx]);
            b.push_bind(&associated_blob_ids[idx])
                .push_unseparated("::uuid[]::ulid[]");
        });

        builder.build().execute(&mut *tx).await.map_err(|e| {
            db_error(
                e,
                &format!("Failed to insert batch of {} events", ids.len()),
            )
        })?;

        tx.commit().await.map_err(|e| {
            db_error(
                e,
                &format!("Failed to commit batch insert of {} events", events.len()),
            )
        })?;

        Ok(events)
    }

    // ========== Stream Batch Insert (for ingestd) ==========

    /// Insert a batch of pre-validated events from the stream consumer.
    ///
    /// This method is optimized for high-throughput ingestion from JetStream.
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

        if batch.is_empty() {
            return Ok(StreamBatchInsertResult::default());
        }

        // Check whether any rows carry synthesis provenance (source_event_ids).
        // Material-only batches (the common case for ingestors) skip cycle
        // detection entirely for maximum throughput.
        let has_synthesis = batch.iter().any(|row| {
            row.source_event_ids
                .as_ref()
                .is_some_and(|ids| !ids.is_empty())
        });

        if has_synthesis {
            // Wrap in a REPEATABLE READ transaction so the cycle check and
            // insert see a consistent snapshot (same pattern as single-event insert).
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| db_error(e, "begin stream batch transaction"))?;
            set_repeatable_read(&mut tx).await?;

            for row in batch {
                if let Some(ref source_ids) = row.source_event_ids {
                    if !source_ids.is_empty() {
                        let source_ulids: Vec<Ulid> =
                            source_ids.iter().map(|uuid| Ulid::from(*uuid)).collect();
                        let event_id: Id<Event<JsonValue>> = Id::from(row.id);
                        ensure_no_synthesis_cycles(&mut *tx, &event_id, &source_ulids).await?;
                    }
                }
            }

            let result = Self::execute_batch_insert(&mut *tx, batch).await?;
            tx.commit()
                .await
                .map_err(|e| db_error(e, "commit stream batch"))?;
            Ok(result)
        } else {
            // Fast path: no synthesis provenance, no cycle detection needed.
            Self::execute_batch_insert(self.pool, batch).await
        }
    }

    /// Build and execute the batch INSERT query against the given executor.
    ///
    /// Extracted so both the transactional (synthesis) and direct (material)
    /// paths can share the same query construction logic.
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
        let mut ingestor_versions = Vec::with_capacity(batch.len());
        let mut associated_blob_ids = Vec::with_capacity(batch.len());

        for row in batch {
            // Postgres timestamps are microsecond precision. Store sub-microsecond
            // remainder separately so we can reconstruct full nanosecond timestamps.
            let (ts_truncated, ts_orig_subnano) = row.ts_orig.to_postgres_parts();

            ids.push(row.id.as_uuid());
            sources.push(row.source.clone());
            event_types.push(row.event_type.clone());
            ts_orig_values.push(ts_truncated);
            ts_orig_subnanos.push(ts_orig_subnano);
            hosts.push(row.host.clone());
            payloads.push(row.payload.clone());
            source_material_ids.push(row.source_material_id);
            anchor_bytes.push(row.anchor_byte);
            offset_starts.push(row.offset_start);
            offset_ends.push(row.offset_end);
            offset_kinds.push(row.offset_kind.clone());
            source_event_ids.push(row.source_event_ids.clone());
            payload_schema_ids.push(row.payload_schema_id);
            ingestor_versions.push(row.ingestor_version.clone());
            associated_blob_ids.push(row.associated_blob_ids.clone());
        }

        // Build INSERT with VALUES using QueryBuilder (required for ragged arrays)
        let mut builder = QueryBuilder::new(
            "INSERT INTO core.events (
                id, source, event_type, ts_orig, ts_orig_subnano, host, payload,
                source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
                source_event_ids, payload_schema_id, ingestor_version, associated_blob_ids
            ) ",
        );

        builder.push_values(0..batch.len(), |mut b, idx| {
            b.push_bind(&ids[idx]).push_unseparated("::uuid::ulid");
            b.push_bind(&sources[idx]);
            b.push_bind(&event_types[idx]);
            b.push_bind(&ts_orig_values[idx]);
            b.push_bind(&ts_orig_subnanos[idx]);
            b.push_bind(&hosts[idx]);
            b.push_bind(&payloads[idx]);
            b.push_bind(&source_material_ids[idx])
                .push_unseparated("::uuid::ulid");
            b.push_bind(&anchor_bytes[idx]);
            b.push_bind(&offset_starts[idx]);
            b.push_bind(&offset_ends[idx]);
            b.push_bind(&offset_kinds[idx]);
            b.push_bind(&source_event_ids[idx])
                .push_unseparated("::uuid[]::ulid[]");
            b.push_bind(&payload_schema_ids[idx])
                .push_unseparated("::uuid::ulid");
            b.push_bind(&ingestor_versions[idx]);
            b.push_bind(&associated_blob_ids[idx])
                .push_unseparated("::uuid[]::ulid[]");
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

        let inserted_ids: Vec<Ulid> = rows.into_iter().map(|(uuid,)| Ulid::from(uuid)).collect();

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
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at: Timestamp",
                updated_at as "updated_at!"
            "#,
            *id.as_ulid() as _,
            *event_id.as_ulid() as _,
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
                id as "id: Id<EventAnnotation>",
                event_id as "event_id: Id<Event<JsonValue>>",
                annotation_type as "annotation_type!",
                content as "content!",
                metadata as "metadata!",
                created_by as "created_by!",
                created_at as "created_at: Timestamp",
                updated_at as "updated_at!"
            "#,
            *annotation_id.as_ulid() as _,
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

    /// Delete an annotation with audit context (soft delete)
    pub async fn delete_annotation_with_context(
        &self,
        id: Id<EventAnnotation>,
        _deleted_by: &str,
        _deletion_reason: &str,
    ) -> DbResult<bool> {
        let result = sqlx::query!(
            "DELETE FROM core.event_annotations WHERE id = $1",
            *id.as_ulid() as _
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete annotation"))?;

        Ok(result.rows_affected() > 0)
    }

    // ========== Event Deletion Operations ==========

    /// Delete events with filter and audit context
    ///
    /// This method deletes events matching the provided source and/or event_type filters,
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
        let operation_id = format!(
            "cleanup_{}_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
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
                    "Failed to commit event deletion transaction (deleted {} events)",
                    deleted_count
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

    /// Delete all events from a specific source (with audit trail)
    ///
    /// Note: This includes a safety constraint that only deletes events that appear
    /// to be test events. Use `hard_delete_by_source` for unconditional deletion.
    pub async fn delete_by_source(&self, source: &EventSource) -> DbResult<u64> {
        self.delete_events_with_filter(Some(source), None, "system", "Delete by source")
            .await
    }

    /// Hard delete events from a specific source (ADMIN USE ONLY)
    ///
    /// This bypasses audit controls and permanently removes data.
    /// Only use for test cleanup or administrative operations where
    /// you need to actually reclaim disk space.
    pub async fn hard_delete_by_source(&self, source: &EventSource) -> DbResult<u64> {
        // Perform the hard delete
        let result = sqlx::query!("DELETE FROM core.events WHERE source = $1", source.as_str())
            .execute(self.pool)
            .await;

        let result = result.map_err(|e| db_error(e, "hard delete by source"))?;
        Ok(result.rows_affected())
    }

    // ========== Data Lifecycle Operations ==========

    /// Get status of all lifecycle tiers (live, archive, tombstone).
    ///
    /// Returns event counts, age distributions, and source diversity for each tier.
    pub async fn lifecycle_tier_status(&self) -> DbResult<Vec<LifecycleTierStatus>> {
        // Use runtime query since the function is created by migration
        let rows = sqlx::query_as::<_, LifecycleTierStatus>(
            r#"
            SELECT
                tier,
                event_count,
                oldest_ts,
                newest_ts,
                distinct_sources
            FROM core.lifecycle_tier_status()
            "#,
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
    /// * `operation_id` - ULID for audit correlation
    ///
    /// # Returns
    /// Number of tombstones created
    pub async fn execute_cascade_tombstone(
        &self,
        archived_ids: &[Ulid],
        reason: &str,
        operation_id: Ulid,
    ) -> DbResult<u64> {
        if archived_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = archived_ids.iter().map(|id| id.to_uuid()).collect();
        // Use runtime query since the function is created by migration
        let count: i64 = sqlx::query_scalar(
            r#"SELECT core.execute_cascade_tombstone($1::ulid[], $2, $3::uuid::ulid)"#,
        )
        .bind(&ids)
        .bind(reason)
        .bind(operation_id.as_uuid())
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
        archived_ids: &[Ulid],
        operation_id: &str,
    ) -> DbResult<u64> {
        if archived_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = archived_ids.iter().map(|id| id.to_uuid()).collect();
        // Use runtime query since the function is created by migration
        let count: i64 =
            sqlx::query_scalar(r#"SELECT core.execute_cascade_restore($1::ulid[], $2)"#)
                .bind(&ids)
                .bind(operation_id)
                .fetch_one(self.pool)
                .await
                .map_err(|e| db_error(e, "execute cascade restore"))?;

        Ok(count as u64)
    }

    /// Populate cascade roots from archived events table.
    ///
    /// Similar to `populate_cascade_roots` but sources from audit.archived_events
    /// instead of core.events. Used for restore and tombstone cascade analysis.
    pub async fn populate_cascade_roots_from_archive(
        &self,
        table_name: &str,
        archived_ids: &[Ulid],
    ) -> DbResult<()> {
        if archived_ids.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = archived_ids.iter().map(|id| id.to_uuid()).collect();

        // Insert archived events into cascade table with depth 0
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (id, depth, parent_ids, processed)
            SELECT ae.id, 0, COALESCE(ae.source_event_ids, '{{}}'::ULID[]), FALSE
            FROM audit.archived_events ae
            WHERE ae.id = ANY($1::ulid[])
            ON CONFLICT (id) DO NOTHING
            "#,
            table_name
        ))
        .bind(&ids)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "populate cascade roots from archive"))?;

        Ok(())
    }

    /// Expand cascade graph from archived events.
    ///
    /// Iteratively finds children in audit.archived_events that reference
    /// events already in the cascade table.
    pub async fn expand_cascade_from_archive(
        &self,
        table_name: &str,
        max_depth: i32,
    ) -> DbResult<usize> {
        let mut current_depth = 0;

        while current_depth < max_depth {
            // Find archived events that reference events at current depth
            let rows_inserted = sqlx::query_scalar::<_, i64>(&format!(
                r#"
                WITH new_children AS (
                    INSERT INTO {} (id, depth, parent_ids, processed)
                    SELECT DISTINCT ae.id, $1 + 1, COALESCE(ae.source_event_ids, '{{}}'::ULID[]), FALSE
                    FROM audit.archived_events ae
                    JOIN {} ct ON ae.source_event_ids && ARRAY[ct.id]
                    WHERE ct.depth = $1 AND ct.processed = FALSE
                    AND NOT EXISTS (SELECT 1 FROM {} ex WHERE ex.id = ae.id)
                    ON CONFLICT (id) DO NOTHING
                    RETURNING 1
                )
                SELECT COUNT(*)::BIGINT FROM new_children
                "#,
                table_name, table_name, table_name
            ))
            .bind(current_depth)
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "expand cascade from archive"))?;

            // Mark current depth as processed
            sqlx::query(&format!(
                "UPDATE {} SET processed = TRUE WHERE depth = $1",
                table_name
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
    pub async fn get_cascade_ids(&self, table_name: &str) -> DbResult<Vec<Ulid>> {
        let rows = sqlx::query_scalar::<_, Uuid>(&format!(
            "SELECT id::uuid FROM {} ORDER BY depth DESC",
            table_name
        ))
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get cascade ids"))?;

        Ok(rows.into_iter().map(Ulid::from).collect())
    }

    /// Count archived events matching filters.
    pub async fn count_archived_events(
        &self,
        source: Option<&EventSource>,
        before: Option<Timestamp>,
    ) -> DbResult<i64> {
        // Build query dynamically based on filters
        let (query, needs_source, needs_before) = match (source.is_some(), before.is_some()) {
            (true, true) => (
                "SELECT COUNT(*)::BIGINT FROM audit.archived_events WHERE source = $1 AND ts_orig < $2",
                true,
                true,
            ),
            (true, false) => (
                "SELECT COUNT(*)::BIGINT FROM audit.archived_events WHERE source = $1",
                true,
                false,
            ),
            (false, true) => (
                "SELECT COUNT(*)::BIGINT FROM audit.archived_events WHERE ts_orig < $1",
                false,
                true,
            ),
            (false, false) => (
                "SELECT COUNT(*)::BIGINT FROM audit.archived_events",
                false,
                false,
            ),
        };

        let mut q = sqlx::query_scalar::<_, i64>(query);
        if needs_source {
            q = q.bind(source.unwrap().as_str());
        }
        if needs_before {
            q = q.bind(*before.unwrap());
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
    ) -> DbResult<Vec<Ulid>> {
        let rows = match (source, before) {
            (Some(s), Some(b)) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM audit.archived_events WHERE source = $1 AND ts_orig < $2 ORDER BY ts_orig LIMIT $3"#,
                    s.as_str(),
                    b as _,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (Some(s), None) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM audit.archived_events WHERE source = $1 ORDER BY ts_orig LIMIT $2"#,
                    s.as_str(),
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (None, Some(b)) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM audit.archived_events WHERE ts_orig < $1 ORDER BY ts_orig LIMIT $2"#,
                    b as _,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (None, None) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM audit.archived_events ORDER BY ts_orig LIMIT $1"#,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
        }
        .map_err(|e| db_error(e, "get archived event ids"))?;

        Ok(rows.into_iter().map(Ulid::from).collect())
    }

    // ========== Live Tier Operations (for Archive) ==========

    /// Get live event IDs matching filters (for archive operation).
    pub async fn get_live_event_ids(
        &self,
        source: Option<&EventSource>,
        before: Option<Timestamp>,
        limit: i64,
    ) -> DbResult<Vec<Ulid>> {
        let rows = match (source, before) {
            (Some(s), Some(b)) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM core.events WHERE source = $1 AND ts_orig < $2 ORDER BY ts_orig LIMIT $3"#,
                    s.as_str(),
                    b as _,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (Some(s), None) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM core.events WHERE source = $1 ORDER BY ts_orig LIMIT $2"#,
                    s.as_str(),
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (None, Some(b)) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM core.events WHERE ts_orig < $1 ORDER BY ts_orig LIMIT $2"#,
                    b as _,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
            (None, None) => {
                sqlx::query_scalar!(
                    r#"SELECT id::uuid as "id!" FROM core.events ORDER BY ts_orig LIMIT $1"#,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
        }
        .map_err(|e| db_error(e, "get live event ids"))?;

        Ok(rows.into_iter().map(Ulid::from).collect())
    }

    /// Populate cascade roots from live events table.
    ///
    /// Similar to `populate_cascade_roots_from_archive` but sources from core.events
    /// instead of audit.archived_events. Used for archive cascade analysis.
    pub async fn populate_cascade_roots_from_live(
        &self,
        table_name: &str,
        live_ids: &[Ulid],
    ) -> DbResult<()> {
        if live_ids.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = live_ids.iter().map(|id| id.to_uuid()).collect();

        // Insert live events into cascade table with depth 0
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (id, depth, parent_ids, processed)
            SELECT e.id, 0, COALESCE(e.source_event_ids, '{{}}'::ULID[]), FALSE
            FROM core.events e
            WHERE e.id = ANY($1::ulid[])
            ON CONFLICT (id) DO NOTHING
            "#,
            table_name
        ))
        .bind(&ids)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "populate cascade roots from live"))?;

        Ok(())
    }

    /// Expand cascade graph from live events.
    ///
    /// Iteratively finds children in core.events that reference
    /// events already in the cascade table.
    pub async fn expand_cascade_from_live(
        &self,
        table_name: &str,
        max_depth: i32,
    ) -> DbResult<usize> {
        let mut current_depth = 0;

        while current_depth < max_depth {
            // Find live events that reference events at current depth
            let rows_inserted = sqlx::query_scalar::<_, i64>(&format!(
                r#"
                WITH new_children AS (
                    INSERT INTO {} (id, depth, parent_ids, processed)
                    SELECT DISTINCT e.id, $1 + 1, COALESCE(e.source_event_ids, '{{}}'::ULID[]), FALSE
                    FROM core.events e
                    JOIN {} ct ON e.source_event_ids && ARRAY[ct.id]
                    WHERE ct.depth = $1 AND ct.processed = FALSE
                    AND NOT EXISTS (SELECT 1 FROM {} ex WHERE ex.id = e.id)
                    ON CONFLICT (id) DO NOTHING
                    RETURNING 1
                )
                SELECT COUNT(*)::BIGINT FROM new_children
                "#,
                table_name, table_name, table_name
            ))
            .bind(current_depth)
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "expand cascade from live"))?;

            // Mark current depth as processed
            sqlx::query(&format!(
                "UPDATE {} SET processed = TRUE WHERE depth = $1",
                table_name
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

    /// Execute cascade archive operation.
    ///
    /// Archives live events and their cascade by DELETE (trigger handles copy to archive).
    /// This requires setting session variables for audit context.
    ///
    /// # Arguments
    /// * `live_ids` - IDs of live events to archive (must be complete cascade)
    /// * `reason` - Human-readable reason for archiving
    /// * `operation_id` - ULID for audit correlation
    /// * `archived_by` - Who initiated the archive (token prefix)
    ///
    /// # Returns
    /// Number of events archived
    pub async fn execute_cascade_archive(
        &self,
        live_ids: &[Ulid],
        reason: &str,
        operation_id: &str,
        archived_by: &str,
    ) -> DbResult<u64> {
        if live_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = live_ids.iter().map(|id| id.to_uuid()).collect();

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
        let result = sqlx::query("DELETE FROM core.events WHERE id = ANY($1::ulid[])")
            .bind(&ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "execute cascade archive"))?;

        let archived_count = result.rows_affected();

        tx.commit().await.map_err(|e| {
            db_error(
                e,
                &format!(
                    "Failed to commit archive transaction (archived {} events)",
                    archived_count
                ),
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

/// Lifecycle tier status record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct LifecycleTierStatus {
    pub tier: String,
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
        event_ids: &[Ulid],
    ) -> DbResult<()> {
        let ids: Vec<Uuid> = event_ids.iter().map(|id| id.to_uuid()).collect();
        sqlx::query_scalar::<_, i64>(
            r#"SELECT core.cascade_populate_roots($1, $2::ulid[]) as inserted"#,
        )
        .bind(table_name)
        .bind(&ids)
        .fetch_one(&mut **self.tx)
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
    ) -> DbResult<Vec<(Ulid, Ulid)>> {
        sqlx::query!(
            r#"
            SELECT
                live_event_id as "live_event_id!: Ulid",
                archived_event_id as "archived_event_id!: Ulid"
            FROM core.cascade_find_integrity_violations($1, $2)
            "#,
            table_name,
            limit
        )
        .fetch_all(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "find cascade integrity violations"))
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.live_event_id, row.archived_event_id))
                .collect()
        })
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

    fn base_record() -> EventRecord {
        let ts = Timestamp::now();
        let subnano = ts.nanosecond() as i32;
        EventRecord {
            id: sinex_schema::ulid::Ulid::new(),
            source: "test.source".to_string(),
            event_type: "test.event".to_string(),
            host: "localhost".to_string(),
            payload: json!({"ok": true}),
            ts_orig: ts,
            ts_orig_subnano: Some(subnano),
            ts_ingest: Timestamp::now(),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: None,
            associated_blob_ids: None,
            payload_schema_id: None,
            ingestor_version: None,
        }
    }

    #[sinex_test]
    fn missing_provenance_is_rejected() -> color_eyre::Result<()> {
        let record = base_record();
        let err = record.try_to_event().expect_err("should fail");
        assert!(format!("{err}").contains("missing provenance"));
        Ok(())
    }

    #[sinex_test]
    fn material_provenance_requires_anchor() -> color_eyre::Result<()> {
        let mut record = base_record();
        record.source_material_id = Some(sinex_schema::ulid::Ulid::new());
        let err = record.try_to_event().expect_err("should fail");
        assert!(format!("{err}").contains("anchor"));
        Ok(())
    }

    #[sinex_test]
    fn valid_material_provenance_passes() -> color_eyre::Result<()> {
        let mut record = base_record();
        record.source_material_id = Some(sinex_schema::ulid::Ulid::new());
        record.anchor_byte = Some(42);
        assert!(record.try_to_event().is_ok());
        Ok(())
    }

    #[sinex_test]
    fn synthesis_provenance_requires_non_empty_sources() -> color_eyre::Result<()> {
        let mut record = base_record();
        record.source_event_ids = Some(vec![]);
        let err = record.try_to_event().expect_err("should fail");
        assert!(format!("{err}").contains("source_event_ids"));
        Ok(())
    }
}
