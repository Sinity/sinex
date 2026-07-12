use super::conversions::{EventRecordExt, extract_provenance};
use crate::JsonValue;
use crate::models::Event;
use crate::postgres_copy::{
    event_copy_column_list_sql, event_copy_insert_select_sql, event_copy_staging_columns_sql,
};
use crate::repositories::common::{DbResult, EnhancedRepository, Repository, db_error};
use crate::schema::Events;
use crate::{EventRecord, SinexError};
use sinex_primitives::domain::{DataTier, EventSource};
use sinex_primitives::{Id, Timestamp};
use std::collections::HashSet;
use uuid::Uuid;

use sqlx::{Executor, PgPool, Postgres, QueryBuilder, Row, Transaction};
use tracing::instrument;

mod replacements;
mod types;
mod validation;

pub use replacements::{ReplacementKind, ReplacementRecord};
use types::StreamBatchInsertStrategy;
pub use types::{
    BatchViolation, COPY_BATCH_THRESHOLD, CascadeSource, EventAnnotation, EventPayloadSchema,
    EventStorageLane, InvalidPayloadEvent, InvalidTimestamp, StreamBatchInsertResult,
    StreamBatchRow, SuspiciousEvent,
};
use validation::{
    collect_synthesis_checks, ensure_batch_event_ids, ensure_no_intra_batch_synthesis_cycles,
    ensure_no_synthesis_cycles, ensure_source_event_ids_are_live, resolved_created_by_operation_id,
    validate_cascade_table_name,
};

/// Event repository for database operations
pub struct EventRepository<'a> {
    pub(super) pool: &'a PgPool,
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
            Some(StreamBatchInsertStrategy::Derived)
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

        // Prepare timestamps.
        //
        // #1570 Prong B: a material event can carry `ts_orig = None` (the
        // "derive at persistence" deferral). Source-unit events resolve that at
        // ingestd admission and reach the DB via the batch/COPY path, never this
        // single-event insert. This direct path is used by API handlers (which
        // always set `at_time`) and tests; a direct insert with no explicit
        // timestamp gets the creation-time default — the pre-#1570 builder
        // behaviour, relocated to the insert boundary so the NOT-NULL column is
        // always satisfied without re-introducing a parse-time `now()` for the
        // quality-derived source pipeline.
        let (pg, sub) = event
            .ts_orig
            .unwrap_or_else(Timestamp::now)
            .to_postgres_parts();
        let (ts_orig, ts_orig_subnano) = (Some(pg), Some(sub));

        // Clone data needed for the closure
        let event_source = event.source.clone();
        let event_type = event.event_type.clone();
        let host = event.host.clone();
        let payload = event.payload.clone();
        let module_run_id = event.module_run_id;
        let payload_schema_id = event.payload_schema_id;
        let anchor_payload_hash = event.anchor_payload_hash.clone();
        let created_by_operation_id = resolved_created_by_operation_id(&event)?;

        // Synthetic event metadata
        let temporal_policy_str = event.temporal_policy.map(|p| p.to_string());
        let semantics_version = event.semantics_version.clone();
        let scope_key = event.scope_key.clone();
        let equivalence_key = event.equivalence_key.clone();
        let automaton_model_str = event.automaton_model.map(|m| m.to_string());
        let ts_quality_str = event.ts_quality.map(|q| q.to_string());

        // Derivation control plane (sinex-0vx.4 / sinex-8cr.2)
        let product_class_str = event.product_class.map(|p| p.to_string());
        let claim_support_json = event
            .claim_support
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| {
                SinexError::database("Failed to serialize event claim_support").with_source(e)
            })?;
        let derivation_declaration_id = event.derivation_declaration_id.clone();
        let derivation_epoch_id = event.derivation_epoch_id;
        let derivation_lane_id = event.derivation_lane_id;
        let adjudication_event_id = event.adjudication_event_id;

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
                let module_run_id = module_run_id;
                let anchor_payload_hash = anchor_payload_hash.clone();
                let offset_kind = offset_kind.clone();
                let temporal_policy_str = temporal_policy_str.clone();
                let semantics_version = semantics_version.clone();
                let scope_key = scope_key.clone();
                let equivalence_key = equivalence_key.clone();
                let automaton_model_str = automaton_model_str.clone();
                let ts_quality_str = ts_quality_str.clone();
                let product_class_str = product_class_str.clone();
                let claim_support_json = claim_support_json.clone();
                let derivation_declaration_id = derivation_declaration_id.clone();
                let derivation_epoch_id = derivation_epoch_id;
                let derivation_lane_id = derivation_lane_id;
                let adjudication_event_id = adjudication_event_id;

                Box::pin(async move {
                    // Enforce REPEATABLE READ for consistent view during cycle check
                    set_repeatable_read(tx).await?;

                    if let Some(source_event_ids) = source_event_ids.as_ref() {
                        ensure_no_synthesis_cycles(&mut **tx, &id, source_event_ids)?;
                        ensure_source_event_ids_are_live(&mut **tx, &id, source_event_ids, None)
                            .await?;
                    }

                    let record = sqlx::query_as!(
                        EventRecord,
                        r#"
                        INSERT INTO core.events (
                            id, source, event_type, host, payload,
                            ts_orig, ts_orig_subnano, module_run_id, payload_schema_id, source_event_ids,
                            source_material_id, offset_start, offset_end, offset_kind,
                            anchor_byte, associated_blob_ids,
                            temporal_policy, semantics_version, scope_key, equivalence_key,
                            created_by_operation_id, automaton_model, anchor_payload_hash, ts_quality,
                            product_class, claim_support, derivation_declaration_id,
                            derivation_epoch_id, derivation_lane_id, adjudication_event_id
                        ) VALUES (
                            $1::uuid, $2, $3, $4, $5,
                            $6, $7, $8, $9::uuid, $10::uuid[],
                            $11::uuid, $12, $13, $14,
                            $15, $16::uuid[],
                            $17, $18, $19, $20,
                            $21::uuid, $22, $23, $24,
                            $25, $26, $27,
                            $28::uuid, $29::uuid, $30::uuid
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
                            module_run_id::uuid as "module_run_id: uuid::Uuid",
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
                            automaton_model,
                            anchor_payload_hash as "anchor_payload_hash: Vec<u8>",
                            ts_quality,
                            product_class,
                            claim_support,
                            derivation_declaration_id,
                            derivation_epoch_id::uuid as "derivation_epoch_id: uuid::Uuid",
                            derivation_lane_id::uuid as "derivation_lane_id: uuid::Uuid",
                            adjudication_event_id::uuid as "adjudication_event_id: uuid::Uuid"
                        "#,
                        id.to_uuid(),
                        event_source.as_str(),
                        event_type.as_str(),
                        host.as_str(),
                        payload,
                        ts_orig,
                        ts_orig_subnano,
                        module_run_id,
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
                        automaton_model_str,
                        anchor_payload_hash,
                        ts_quality_str,
                        // Derivation control plane (sinex-0vx.4 / sinex-8cr.2):
                        // real values, read off Event<T> above. derivation_epoch_id/
                        // derivation_lane_id stay None on every live path today —
                        // no canonical-epoch-id resolution mechanism exists yet
                        // (0vx.5/0vx.6/0vx.7/0vx.9) — and adjudication_event_id is
                        // set only by the future curation finalizer (0vx.5).
                        product_class_str,
                        claim_support_json,
                        derivation_declaration_id,
                        derivation_epoch_id,
                        derivation_lane_id,
                        adjudication_event_id
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
            ensure_no_synthesis_cycles(&mut **tx, &id, source_event_ids)?;
            ensure_source_event_ids_are_live(&mut **tx, &id, source_event_ids, None).await?;
        }

        // Convert IDs to UUIDs before the query to avoid temporary value issues
        let source_event_uuids = source_event_ids.as_ref().map(|ids| {
            ids.iter()
                .map(sinex_primitives::Id::to_uuid)
                .collect::<Vec<_>>()
        });
        let associated_blob_uuids = event.associated_blob_ids.clone();
        let anchor_payload_hash = event.anchor_payload_hash.clone();

        // Postgres timestamps are microsecond precision. Persist the sub-microsecond
        // remainder separately so we can reconstruct full nanosecond timestamps on read.
        // #1570 Prong B: deferred (`None`) material ts_orig is resolved at ingestd
        // admission via the batch path; this single-event direct insert defaults
        // an absent timestamp to creation time (see `insert`).
        let (pg, sub) = event
            .ts_orig
            .unwrap_or_else(Timestamp::now)
            .to_postgres_parts();
        let (ts_orig, ts_orig_subnano) = (Some(pg), Some(sub));

        // Synthetic event metadata
        let temporal_policy_str = event.temporal_policy.map(|p| p.to_string());
        let created_by_operation_id = resolved_created_by_operation_id(&event)?;
        let automaton_model_str = event.automaton_model.map(|m| m.to_string());
        let ts_quality_str = event.ts_quality.map(|q| q.to_string());

        // Derivation control plane (sinex-0vx.4 / sinex-8cr.2)
        let product_class_str = event.product_class.map(|p| p.to_string());
        let claim_support_json = event
            .claim_support
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| {
                SinexError::database("Failed to serialize event claim_support").with_source(e)
            })?;

        let record = sqlx::query_as!(
            EventRecord,
            r#"
            INSERT INTO core.events (
                id, source, event_type, host, payload,
                ts_orig, ts_orig_subnano, module_run_id, payload_schema_id, source_event_ids,
                source_material_id, offset_start, offset_end, offset_kind,
                anchor_byte, associated_blob_ids,
                temporal_policy, semantics_version, scope_key, equivalence_key,
                created_by_operation_id, automaton_model, anchor_payload_hash, ts_quality,
                product_class, claim_support, derivation_declaration_id,
                derivation_epoch_id, derivation_lane_id, adjudication_event_id
            ) VALUES (
                $1::uuid, $2, $3, $4, $5,
                $6, $7, $8, $9::uuid, $10::uuid[],
                $11::uuid, $12, $13, $14,
                $15, $16::uuid[],
                $17, $18, $19, $20,
                $21::uuid, $22, $23, $24,
                $25, $26, $27,
                $28::uuid, $29::uuid, $30::uuid
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
                module_run_id::uuid as "module_run_id: uuid::Uuid",
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
                automaton_model,
                anchor_payload_hash as "anchor_payload_hash: Vec<u8>",
                ts_quality,
                product_class,
                claim_support,
                derivation_declaration_id,
                derivation_epoch_id::uuid as "derivation_epoch_id: uuid::Uuid",
                derivation_lane_id::uuid as "derivation_lane_id: uuid::Uuid",
                adjudication_event_id::uuid as "adjudication_event_id: uuid::Uuid"
            "#,
            id.to_uuid(),
            event.source.as_str(),
            event.event_type.as_str(),
            event.host.as_str(),
            event.payload,
            ts_orig,
            ts_orig_subnano,
            event.module_run_id,
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
            automaton_model_str,
            anchor_payload_hash,
            ts_quality_str,
            // Derivation control plane (sinex-0vx.4 / sinex-8cr.2): see the
            // matching comment in `insert` above.
            product_class_str,
            claim_support_json,
            event.derivation_declaration_id,
            event.derivation_epoch_id,
            event.derivation_lane_id,
            event.adjudication_event_id
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
        let mut module_run_ids: Vec<Option<Uuid>> = Vec::with_capacity(events.len());
        let mut payload_schema_ids = Vec::with_capacity(events.len());
        let mut anchor_payload_hashes = Vec::with_capacity(events.len());
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
        let mut automaton_models: Vec<Option<String>> = Vec::with_capacity(events.len());
        let mut ts_qualities: Vec<Option<String>> = Vec::with_capacity(events.len());
        let mut product_classes: Vec<Option<String>> = Vec::with_capacity(events.len());
        let mut claim_supports: Vec<Option<JsonValue>> = Vec::with_capacity(events.len());
        let mut derivation_declaration_ids: Vec<Option<String>> = Vec::with_capacity(events.len());
        let mut derivation_epoch_ids: Vec<Option<Uuid>> = Vec::with_capacity(events.len());
        let mut derivation_lane_ids: Vec<Option<Uuid>> = Vec::with_capacity(events.len());
        let mut adjudication_event_ids: Vec<Option<Uuid>> = Vec::with_capacity(events.len());

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
            // #1570 Prong B: a deferred (`None`) material ts_orig on this direct
            // QueryBuilder batch path defaults to creation time (see `insert`);
            // source-driver deferral resolves at ingestd admission.
            let (pg_ts, sub_nano) = event
                .ts_orig
                .unwrap_or_else(Timestamp::now)
                .to_postgres_parts();
            let (ts_orig, ts_orig_subnano) = (Some(pg_ts), Some(sub_nano));

            ids.push(event_id);
            sources.push(event.source.as_str().to_string());
            event_types.push(event.event_type.as_str().to_string());
            hosts.push(event.host.as_str().to_string());
            payloads.push(event.payload.clone());
            ts_orig_values.push(ts_orig);
            ts_orig_subnanos.push(ts_orig_subnano);
            module_run_ids.push(event.module_run_id);
            payload_schema_ids.push(event.payload_schema_id);
            anchor_payload_hashes.push(event.anchor_payload_hash.clone());
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
            automaton_models.push(event.automaton_model.map(|m| m.to_string()));
            ts_qualities.push(event.ts_quality.map(|q| q.to_string()));
            product_classes.push(event.product_class.map(|p| p.to_string()));
            claim_supports.push(
                event
                    .claim_support
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .map_err(|e| {
                        db_error(
                            sqlx::Error::Protocol(format!(
                                "failed to serialize event claim_support: {e}"
                            )),
                            "insert batch",
                        )
                    })?,
            );
            derivation_declaration_ids.push(event.derivation_declaration_id.clone());
            derivation_epoch_ids.push(event.derivation_epoch_id);
            derivation_lane_ids.push(event.derivation_lane_id);
            adjudication_event_ids.push(event.adjudication_event_id);
        }

        ensure_no_intra_batch_synthesis_cycles(&synthesis_checks)?;
        let batch_event_ids = ids.iter().copied().collect::<HashSet<_>>();

        // Enforce derived cycle detection (parity with insert/insert_stream_batch)
        for (event_id, source_ids) in &synthesis_checks {
            ensure_no_synthesis_cycles(&mut **tx, event_id, source_ids)?;
            ensure_source_event_ids_are_live(
                &mut **tx,
                event_id,
                source_ids,
                Some(&batch_event_ids),
            )
            .await?;
        }

        // QueryBuilder is required here because UNNEST cannot represent ragged arrays
        // (source_event_ids/associated_blob_ids) and `query!` rejects array nulls.
        //
        // Column list is derived from EVENT_COPY_COLUMNS (the SSOT) so that adding
        // a new core.events column only requires updating postgres_copy.rs — not this
        // site. Bind order MUST match EVENT_COPY_COLUMNS order exactly. (#1575)
        let mut builder = QueryBuilder::new(format!(
            "INSERT INTO core.events ({}) ",
            event_copy_column_list_sql()
        ));
        builder.push_values(0..ids.len(), |mut b, idx| {
            // Bind order: id, source, event_type, ts_orig, ts_orig_subnano, host, payload,
            // source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
            // source_event_ids, payload_schema_id, module_run_id, anchor_payload_hash,
            // associated_blob_ids, temporal_policy, semantics_version, scope_key,
            // equivalence_key, created_by_operation_id, automaton_model, ts_quality,
            // product_class, claim_support, derivation_declaration_id,
            // derivation_epoch_id, derivation_lane_id, adjudication_event_id
            // — matches EVENT_COPY_COLUMNS order.
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
            b.push_bind(module_run_ids[idx]).push_unseparated("::uuid");
            b.push_bind(&anchor_payload_hashes[idx]);
            b.push_bind(&associated_blob_ids[idx])
                .push_unseparated("::uuid[]");
            b.push_bind(&temporal_policies[idx]);
            b.push_bind(&semantics_versions[idx]);
            b.push_bind(&scope_keys[idx]);
            b.push_bind(&equivalence_keys[idx]);
            b.push_bind(created_by_operation_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(&automaton_models[idx]);
            b.push_bind(&ts_qualities[idx]);
            b.push_bind(&product_classes[idx]);
            b.push_bind(&claim_supports[idx]);
            b.push_bind(&derivation_declaration_ids[idx]);
            b.push_bind(derivation_epoch_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(derivation_lane_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(adjudication_event_ids[idx])
                .push_unseparated("::uuid");
        });

        builder.build().execute(&mut **tx).await.map_err(|e| {
            db_error(
                e,
                &format!("Failed to insert batch of {} events", ids.len()),
            )
        })?;

        Ok(events)
    }

    // ========== Stream Batch Insert (for event_engine) ==========

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
        self.insert_stream_batch_into(EventStorageLane::Activity, batch)
            .await
    }

    /// Insert a batch of pre-validated stream events into the selected physical lane.
    ///
    /// Activity events land in `core.events`; reflection/self-observation events land in
    /// `reflection.events`. The two tables share the same event column contract.
    #[instrument(skip(self, batch), fields(batch_size = batch.len(), lane = ?lane))]
    pub async fn insert_stream_batch_into(
        &self,
        lane: EventStorageLane,
        batch: &[StreamBatchRow],
    ) -> DbResult<StreamBatchInsertResult> {
        use crate::query_helpers::set_repeatable_read;

        match Self::stream_batch_insert_strategy(batch) {
            None => Ok(StreamBatchInsertResult::default()),
            // Derived batches: wrap in REPEATABLE READ for cycle detection.
            // COPY cannot be mixed with cycle-detection queries in the same
            // transaction easily, so derived batches use the VALUES path.
            //
            // PostgreSQL caps bound parameters at 65 535 per query. With 24
            // bound parameters per event (see execute_batch_insert's column
            // list), each chunk must stay ≤ ⌊65535 / 24⌋ = 2730 events to
            // stay within the wire-protocol limit. Large mixed batches
            // (material + derived) arrive during burst replay and would
            // otherwise exceed the limit and fail.
            Some(StreamBatchInsertStrategy::Derived) => {
                // Intra-batch cycle check runs on the full batch before splitting.
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

                // 24 binds per row × SYNTHESIS_CHUNK_MAX ≤ 65535 PostgreSQL
                // param limit (⌊65535 / 24⌋ = 2730). Keep headroom so future
                // column additions don't immediately overflow.
                const SYNTHESIS_CHUNK_MAX: usize = 2700;
                let mut total = StreamBatchInsertResult::default();
                for chunk in batch.chunks(SYNTHESIS_CHUNK_MAX) {
                    let chunk_synthesis_checks = chunk
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

                    let mut tx = self
                        .pool
                        .begin()
                        .await
                        .map_err(|e| db_error(e, "begin stream batch transaction"))?;
                    set_repeatable_read(&mut tx).await?;
                    let chunk_event_ids = chunk.iter().map(|row| row.id).collect::<HashSet<_>>();

                    for (event_id, source_ids) in &chunk_synthesis_checks {
                        ensure_no_synthesis_cycles(&mut *tx, event_id, source_ids)?;
                        ensure_source_event_ids_are_live(
                            &mut *tx,
                            event_id,
                            source_ids,
                            Some(&chunk_event_ids),
                        )
                        .await?;
                    }

                    let result = Self::execute_batch_insert(&mut *tx, lane, chunk).await?;
                    tx.commit()
                        .await
                        .map_err(|e| db_error(e, "commit stream batch"))?;

                    total.inserted_count += result.inserted_count;
                    if let Some(ids) = result.inserted_ids {
                        total.inserted_ids.get_or_insert_with(Vec::new).extend(ids);
                    }
                }
                Ok(total)
            }
            // Large material-only batch: use COPY for maximum throughput.
            // Avoids the 65 535-parameter limit of parameterised VALUES queries
            // and has significantly lower per-row protocol overhead.
            Some(StreamBatchInsertStrategy::Copy) => {
                Self::execute_batch_insert_copy(self.pool, lane, batch).await
            }
            // Small material-only batch: QueryBuilder is faster (no staging
            // table overhead).
            Some(StreamBatchInsertStrategy::QueryBuilder) => {
                Self::execute_batch_insert(self.pool, lane, batch).await
            }
        }
    }

    /// Build and execute the batch INSERT query against the given executor.
    ///
    /// Extracted so both the transactional (derived) and direct (material)
    /// paths can share the same query construction logic.
    #[instrument(skip(executor, batch), fields(batch_size = batch.len(), path = "query_builder"))]
    async fn execute_batch_insert<'e, E>(
        executor: E,
        lane: EventStorageLane,
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
        let mut module_run_ids: Vec<Option<Uuid>> = Vec::with_capacity(batch.len());
        let mut associated_blob_ids = Vec::with_capacity(batch.len());
        let mut anchor_payload_hashes = Vec::with_capacity(batch.len());

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
            module_run_ids.push(row.module_run_id);
            associated_blob_ids.push(row.associated_blob_ids.clone());
            anchor_payload_hashes.push(row.anchor_payload_hash.clone());
        }

        // Synthetic event metadata vectors
        let temporal_policies: Vec<_> = batch.iter().map(|r| r.temporal_policy.clone()).collect();
        let semantics_versions: Vec<_> =
            batch.iter().map(|r| r.semantics_version.clone()).collect();
        let scope_keys: Vec<_> = batch.iter().map(|r| r.scope_key.clone()).collect();
        let equivalence_keys: Vec<_> = batch.iter().map(|r| r.equivalence_key.clone()).collect();
        let created_by_op_ids: Vec<_> = batch.iter().map(|r| r.created_by_operation_id).collect();
        let automaton_models: Vec<_> = batch.iter().map(|r| r.automaton_model.clone()).collect();
        let ts_qualities: Vec<_> = batch.iter().map(|r| r.ts_quality.clone()).collect();
        // Derivation control plane (sinex-0vx.4 / sinex-8cr.2).
        let product_classes: Vec<_> = batch.iter().map(|r| r.product_class.clone()).collect();
        let claim_supports: Vec<_> = batch.iter().map(|r| r.claim_support.clone()).collect();
        let derivation_declaration_ids: Vec<_> = batch
            .iter()
            .map(|r| r.derivation_declaration_id.clone())
            .collect();
        let derivation_epoch_ids: Vec<_> = batch.iter().map(|r| r.derivation_epoch_id).collect();
        let derivation_lane_ids: Vec<_> = batch.iter().map(|r| r.derivation_lane_id).collect();
        let adjudication_event_ids: Vec<_> =
            batch.iter().map(|r| r.adjudication_event_id).collect();

        // Build INSERT with VALUES using QueryBuilder (required for ragged arrays).
        //
        // Column list is derived from EVENT_COPY_COLUMNS (the SSOT) so that adding
        // a new core.events column only requires updating postgres_copy.rs — not this
        // site. Bind order MUST match EVENT_COPY_COLUMNS order exactly. (#1575)
        let mut builder = QueryBuilder::new(format!(
            "INSERT INTO {} ({}) ",
            lane.table_name(),
            event_copy_column_list_sql()
        ));

        builder.push_values(0..batch.len(), |mut b, idx| {
            // Bind order: id, source, event_type, ts_orig, ts_orig_subnano, host, payload,
            // source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
            // source_event_ids, payload_schema_id, module_run_id, anchor_payload_hash,
            // associated_blob_ids, temporal_policy, semantics_version, scope_key,
            // equivalence_key, created_by_operation_id, automaton_model, ts_quality,
            // product_class, claim_support, derivation_declaration_id,
            // derivation_epoch_id, derivation_lane_id, adjudication_event_id
            // — matches EVENT_COPY_COLUMNS order.
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
            b.push_bind(module_run_ids[idx]).push_unseparated("::uuid");
            b.push_bind(&anchor_payload_hashes[idx]);
            b.push_bind(&associated_blob_ids[idx])
                .push_unseparated("::uuid[]");
            b.push_bind(&temporal_policies[idx]);
            b.push_bind(&semantics_versions[idx]);
            b.push_bind(&scope_keys[idx]);
            b.push_bind(&equivalence_keys[idx]);
            b.push_bind(created_by_op_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(&automaton_models[idx]);
            b.push_bind(&ts_qualities[idx]);
            b.push_bind(&product_classes[idx]);
            b.push_bind(&claim_supports[idx]);
            b.push_bind(&derivation_declaration_ids[idx]);
            b.push_bind(derivation_epoch_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(derivation_lane_ids[idx])
                .push_unseparated("::uuid");
            b.push_bind(adjudication_event_ids[idx])
                .push_unseparated("::uuid");
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
    /// 2. Probe the connection-local `pg_temp` namespace and create
    ///    `sinex_batch_staging` only when absent, then `TRUNCATE` it so
    ///    repeated calls on the same pooled connection start clean.
    /// 3. `COPY FROM STDIN` the serialised rows (text format, tab-delimited).
    /// 4. `INSERT INTO <lane table> … SELECT … FROM sinex_batch_staging ON CONFLICT DO NOTHING`
    ///    with `RETURNING id::uuid` to learn which IDs were actually inserted.
    /// 5. `COMMIT` — the temp table survives (for step 2 reuse) but the data is gone.
    ///
    /// # Why not query params?
    /// `PostgreSQL`'s protocol limits a single statement to 65 535 bind parameters.
    /// With 24 writable event columns per row that caps VALUES batches at ~2 730 rows. COPY has no
    /// such limit and has lower per-row overhead.
    ///
    /// # Why not derived batches?
    /// Derived batches require a REPEATABLE READ transaction for cycle detection.
    /// Combining that with COPY (which also monopolises the connection while active)
    /// is possible but adds complexity. The caller already routes derived batches
    /// through `execute_batch_insert`, so this function handles material-only batches.
    #[instrument(skip(pool, batch), fields(batch_size = batch.len(), path = "copy"))]
    async fn execute_batch_insert_copy(
        pool: &PgPool,
        lane: EventStorageLane,
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
        //
        // Do not use `CREATE TEMP TABLE IF NOT EXISTS` here. PostgreSQL emits
        // `NOTICE: relation "sinex_batch_staging" already exists, skipping`
        // every time the table already exists, and sqlx forwards that NOTICE
        // to the service logs. On the COPY hot path that notice dominated
        // sinexd journald volume (#1841).
        let staging_exists =
            sqlx::query_scalar::<_, Option<String>>(Self::copy_staging_exists_sql())
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| db_error(e, "probe staging table for COPY batch insert"))?;

        if staging_exists.is_none() {
            let create_staging_sql = Self::copy_staging_create_sql(&staging_columns_sql);
            sqlx::query(&create_staging_sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| db_error(e, "create staging table for COPY batch insert"))?;
        }

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

        // Move rows from staging into the selected event table, applying UUIDv7 casts.
        let insert_sql = format!(
            "INSERT INTO {} ({copy_columns_sql})
            SELECT
                {insert_select_sql}
            FROM sinex_batch_staging
            ON CONFLICT (id) DO NOTHING
            RETURNING id::uuid",
            lane.table_name()
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

    fn copy_staging_exists_sql() -> &'static str {
        "SELECT to_regclass('pg_temp.sinex_batch_staging')::text"
    }

    fn copy_staging_create_sql(staging_columns_sql: &str) -> String {
        format!(
            "CREATE TEMP TABLE sinex_batch_staging (
                {staging_columns_sql}
            )"
        )
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

    // ========== Analytics Queries ==========

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
                    JOIN (
                        SELECT ARRAY_AGG(ct.id) AS ids
                        FROM {table_name} ct
                        WHERE ct.depth = $1 AND ct.processed = FALSE
                    ) frontier ON frontier.ids IS NOT NULL
                    WHERE s.source_event_ids && frontier.ids
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

        if current_depth >= max_depth {
            // Mirror of core.expand_cascade: probe whether we would have
            // inserted anything at the next depth and refuse to silently
            // truncate the cascade if so.
            let pending = sqlx::query_scalar::<_, i64>(&format!(
                r"
                SELECT COUNT(*)::BIGINT
                FROM {src} s
                JOIN (
                    SELECT ARRAY_AGG(ct.id) AS ids
                    FROM {table_name} ct
                    WHERE ct.depth = $1 AND ct.processed = FALSE
                ) frontier ON frontier.ids IS NOT NULL
                WHERE s.source_event_ids && frontier.ids
                AND NOT EXISTS (SELECT 1 FROM {table_name} ex WHERE ex.id = s.id)
                "
            ))
            .bind(current_depth)
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "probe cascade truncation"))?;

            if pending > 0 {
                return Err(db_error(
                    sqlx::Error::Protocol(format!(
                        "cascade exceeds max depth {max_depth} ({pending} pending children at limit)"
                    )),
                    "cascade truncation",
                ));
            }
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
        let requested_count = ids.len() as u64;

        // Pre-validate: ensure all requested live IDs exist before any mutation.
        // This prevents the transaction from committing partial work and then
        // returning Err (the #1134 atomicity gap).
        let existing_count: i64 = sqlx::query_scalar(
            "SELECT count(*)::bigint FROM core.events WHERE id = ANY($1::uuid[])",
        )
        .bind(&ids)
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "pre-validate archive roots"))?;

        if existing_count as u64 != requested_count {
            let missing_count = requested_count - existing_count as u64;
            return Err(db_error(
                sqlx::Error::RowNotFound,
                &format!(
                    "archive_cascade: {missing_count} of {requested_count} requested root IDs \
                     were not present in core.events — archive aborted before any mutation"
                ),
            ));
        }

        let mut tx = self.pool.begin().await.map_err(|e| {
            db_error(
                e,
                &format!(
                    "Failed to begin transaction for archive of {} events",
                    live_ids.len()
                ),
            )
        })?;

        let archived_count =
            execute_cascade_archive_in_tx(&mut tx, &ids, reason, operation_id, archived_by).await?;

        tx.commit().await.map_err(|e| {
            db_error(
                e,
                &format!("Failed to commit archive transaction for {requested_count} events"),
            )
        })?;

        tracing::info!(
            operation_id = %operation_id,
            archived_by = %archived_by,
            reason = %reason,
            archived_count,
            "Archived events via cascade operation"
        );

        Ok(archived_count)
    }
}

async fn execute_cascade_archive_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    ids: &[Uuid],
    reason: &str,
    operation_id: &str,
    archived_by: &str,
) -> DbResult<u64> {
    if ids.is_empty() {
        return Ok(0);
    }

    let requested_count = ids.len() as u64;

    // Set session variables for audit trail (the trigger reads these)
    sqlx::query("SELECT pg_catalog.set_config('sinex.operation_id', $1, true)")
        .bind(operation_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set operation_id"))?;

    sqlx::query("SELECT pg_catalog.set_config('sinex.archived_by', $1, true)")
        .bind(archived_by)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set archived_by"))?;

    sqlx::query("SELECT pg_catalog.set_config('sinex.archive_reason', $1, true)")
        .bind(reason)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set archive_reason"))?;

    // Copy annotations to archive before the DELETE cascade destroys them.
    let annotation_count = sqlx::query(
        r"INSERT INTO audit.archived_annotations
              SELECT a.*, now(), $2, $3
              FROM core.event_annotations a
              WHERE a.event_id = ANY($1::uuid[])",
    )
    .bind(ids)
    .bind(archived_by)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .map_err(|e| db_error(e, "archive annotations"))?
    .rows_affected();

    // Copy embeddings to archive before the DELETE cascade destroys them.
    let embedding_count = sqlx::query(
        r"INSERT INTO audit.archived_embeddings
              SELECT e.*, now(), $2, $3
              FROM core.event_embeddings e
              WHERE e.event_id = ANY($1::uuid[])",
    )
    .bind(ids)
    .bind(archived_by)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .map_err(|e| db_error(e, "archive embeddings"))?
    .rows_affected();

    // Copy tagged_items referencing the archived events, then clean up the
    // live table. Unlike annotations/embeddings, tagged_items has no FK to
    // events, so the DELETE below will not cascade — we must remove
    // dangling references explicitly.
    let tagged_count = sqlx::query(
        r"INSERT INTO audit.archived_tagged_items
              SELECT t.*, now(), $2, $3
              FROM core.tagged_items t
              WHERE t.item_type = 'event' AND t.item_id = ANY($1::uuid[])",
    )
    .bind(ids)
    .bind(archived_by)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .map_err(|e| db_error(e, "archive tagged items"))?
    .rows_affected();

    if tagged_count > 0 {
        sqlx::query(
            r"DELETE FROM core.tagged_items
                  WHERE item_type = 'event' AND item_id = ANY($1::uuid[])",
        )
        .bind(ids)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "cleanup archived tagged items"))?;
    }

    // Delete events - the trigger fn_archive_before_delete copies them to archive.
    // Pre-validation (above) guarantees all IDs exist, so this DELETE cannot
    // come up short due to missing roots.
    let archived_count = sqlx::query("DELETE FROM core.events WHERE id = ANY($1::uuid[])")
        .bind(ids)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "execute cascade archive"))?
        .rows_affected();

    if archived_count != requested_count {
        return Err(db_error(
            sqlx::Error::RowNotFound,
            &format!(
                "archive_cascade: archived {archived_count} of {requested_count} requested live IDs"
            ),
        ));
    }

    tracing::debug!(
        operation_id = %operation_id,
        archived_by = %archived_by,
        reason = %reason,
        archived_count,
        annotations_archived = annotation_count,
        embeddings_archived = embedding_count,
        tagged_items_archived = tagged_count,
        "Archived cascade rows inside caller transaction"
    );

    Ok(archived_count)
}

async fn execute_cascade_archive_table_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    table_name: &str,
    reason: &str,
    operation_id: &str,
    archived_by: &str,
    requested_count: u64,
) -> DbResult<u64> {
    validate_cascade_table_name(table_name)?;

    sqlx::query("SELECT pg_catalog.set_config('sinex.operation_id', $1, true)")
        .bind(operation_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set operation_id"))?;

    sqlx::query("SELECT pg_catalog.set_config('sinex.archived_by', $1, true)")
        .bind(archived_by)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set archived_by"))?;

    sqlx::query("SELECT pg_catalog.set_config('sinex.archive_reason', $1, true)")
        .bind(reason)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set archive_reason"))?;

    let annotation_count = sqlx::query(&format!(
        r"INSERT INTO audit.archived_annotations
              SELECT a.*, now(), $1, $2
              FROM core.event_annotations a
              JOIN {table_name} c ON c.id = a.event_id"
    ))
    .bind(archived_by)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .map_err(|e| db_error(e, "archive annotations"))?
    .rows_affected();

    let embedding_count = sqlx::query(&format!(
        r"INSERT INTO audit.archived_embeddings
              SELECT e.*, now(), $1, $2
              FROM core.event_embeddings e
              JOIN {table_name} c ON c.id = e.event_id"
    ))
    .bind(archived_by)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .map_err(|e| db_error(e, "archive embeddings"))?
    .rows_affected();

    let tagged_count = sqlx::query(&format!(
        r"INSERT INTO audit.archived_tagged_items
              SELECT t.*, now(), $1, $2
              FROM core.tagged_items t
              JOIN {table_name} c ON c.id = t.item_id
              WHERE t.item_type = 'event'"
    ))
    .bind(archived_by)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .map_err(|e| db_error(e, "archive tagged items"))?
    .rows_affected();

    if tagged_count > 0 {
        sqlx::query(&format!(
            r"DELETE FROM core.tagged_items t
                  USING {table_name} c
                  WHERE t.item_type = 'event' AND t.item_id = c.id"
        ))
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "cleanup archived tagged items"))?;
    }

    let archived_count = sqlx::query(&format!(
        r"DELETE FROM core.events e
              USING {table_name} c
              WHERE e.id = c.id"
    ))
    .execute(&mut **tx)
    .await
    .map_err(|e| db_error(e, "execute cascade archive"))?
    .rows_affected();

    if archived_count != requested_count {
        return Err(db_error(
            sqlx::Error::RowNotFound,
            &format!(
                "archive_cascade: archived {archived_count} of {requested_count} requested live IDs"
            ),
        ));
    }

    tracing::debug!(
        operation_id = %operation_id,
        archived_by = %archived_by,
        reason = %reason,
        archived_count,
        annotations_archived = annotation_count,
        embeddings_archived = embedding_count,
        tagged_items_archived = tagged_count,
        "Archived cascade rows from working table"
    );

    Ok(archived_count)
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

    pub fn transaction(&mut self) -> &mut Transaction<'t, Postgres> {
        self.tx
    }

    pub async fn execute_cascade_archive(
        &mut self,
        live_ids: &[Uuid],
        reason: &str,
        operation_id: &str,
        archived_by: &str,
    ) -> DbResult<u64> {
        execute_cascade_archive_in_tx(&mut *self.tx, live_ids, reason, operation_id, archived_by)
            .await
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

    /// Populate cascade roots directly from all events tied to one source
    /// material.
    ///
    /// This avoids building a large root-event UUID array in Rust for
    /// material-scoped lifecycle operations.
    pub async fn populate_cascade_roots_for_source_material_from(
        &mut self,
        table_name: &str,
        source_material_id: Uuid,
        source: CascadeSource,
    ) -> DbResult<i64> {
        validate_cascade_table_name(table_name)?;

        let src = source.table_name();
        sqlx::query_scalar::<_, i64>(&format!(
            r"
            WITH inserted AS (
                INSERT INTO {table_name} (id, depth, parent_ids, processed)
                SELECT s.id, 0, COALESCE(s.source_event_ids, '{{}}'::UUID[]), FALSE
                FROM {src} s
                WHERE s.source_material_id = $1
                ON CONFLICT (id) DO NOTHING
                RETURNING 1
            )
            SELECT COUNT(*)::BIGINT FROM inserted
            "
        ))
        .bind(source_material_id)
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "populate cascade roots for source material"))
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
                    JOIN (
                        SELECT ARRAY_AGG(ct.id) AS ids
                        FROM {table_name} ct
                        WHERE ct.depth = $1 AND ct.processed = FALSE
                    ) frontier ON frontier.ids IS NOT NULL
                    WHERE s.source_event_ids && frontier.ids
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

        if current_depth >= max_depth {
            // Mirror of core.expand_cascade: probe whether we would have
            // inserted anything at the next depth and refuse to silently
            // truncate the cascade if so.
            let pending = sqlx::query_scalar::<_, i64>(&format!(
                r"
                SELECT COUNT(*)::BIGINT
                FROM {src} s
                JOIN (
                    SELECT ARRAY_AGG(ct.id) AS ids
                    FROM {table_name} ct
                    WHERE ct.depth = $1 AND ct.processed = FALSE
                ) frontier ON frontier.ids IS NOT NULL
                WHERE s.source_event_ids && frontier.ids
                AND NOT EXISTS (SELECT 1 FROM {table_name} ex WHERE ex.id = s.id)
                "
            ))
            .bind(current_depth)
            .fetch_one(&mut **self.tx)
            .await
            .map_err(|e| db_error(e, "probe cascade truncation"))?;

            if pending > 0 {
                return Err(db_error(
                    sqlx::Error::Protocol(format!(
                        "cascade exceeds max depth {max_depth} ({pending} pending children at limit)"
                    )),
                    "cascade truncation",
                ));
            }
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

    pub async fn get_cascade_id_sample(
        &mut self,
        table_name: &str,
        limit: i64,
    ) -> DbResult<Vec<Uuid>> {
        validate_cascade_table_name(table_name)?;

        sqlx::query_scalar::<_, Uuid>(&format!(
            "SELECT id::uuid FROM {table_name} ORDER BY depth DESC LIMIT $1"
        ))
        .bind(limit)
        .fetch_all(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "get cascade id sample"))
    }

    pub async fn execute_cascade_archive_from_table(
        &mut self,
        table_name: &str,
        reason: &str,
        operation_id: &str,
        archived_by: &str,
    ) -> DbResult<u64> {
        validate_cascade_table_name(table_name)?;

        let requested_count = self.cascade_node_count(table_name).await? as u64;
        if requested_count == 0 {
            return Ok(0);
        }

        execute_cascade_archive_table_in_tx(
            &mut *self.tx,
            table_name,
            reason,
            operation_id,
            archived_by,
            requested_count,
        )
        .await
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
#[path = "persistence_test.rs"]
mod tests;
