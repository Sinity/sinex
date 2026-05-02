//! Scope/output event collection, cascade resolution, archive/restore, and
//! abort handling for `ReplayExecutionEngine`. See `execution/mod.rs` for the
//! engine type itself and the public-API entry points.

#![allow(unused_imports)]

use super::*;
use async_nats::jetstream;
use sinex_db::repositories::{DbPoolExt, EventRepositoryTx};
use sinex_node_sdk::derived_node::invalidation::{DerivedScopeInvalidation, INVALIDATION_SUBJECT};
use sinex_node_sdk::runtime::stream::{
    Checkpoint, MaterialReplayContext, NodeScanAck, NodeScanCommand, NodeScanProgress,
    ReplayScopeFilters as NodeReplayScopeFilters, ResolvedReplayMaterial, ScanArgs, TimeHorizon,
};
use sinex_primitives::domain::{EventSource, EventType, NodeName};
use sinex_primitives::events::{Event as StoredEvent, Provenance};
use sinex_primitives::{Id, SinexError, Timestamp, Uuid};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use sinex_db::replay::state_machine::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState, ReplayStateMachine,
};

impl ReplayExecutionEngine {
    pub(crate) async fn collect_scope_events(
        &self,
        scope: &ReplayScope,
        _execution_window: (Timestamp, Timestamp),
        pool: &sqlx::PgPool,
    ) -> Result<Vec<StoredEvent>> {
        let root_ids = self
            .replay
            .collect_scope_root_ids(scope)
            .await
            .map_err(|e| eyre!("Failed to collect replay scope root ids: {e}"))?;
        let event_ids: Vec<Id<StoredEvent>> = root_ids
            .into_iter()
            .map(Id::<StoredEvent>::from_uuid)
            .collect();

        // get_by_ids silently clamps to 1000; chunk to avoid the truncation.
        const CHUNK_SIZE: usize = 1000;
        if event_ids.len() <= CHUNK_SIZE {
            return pool
                .events()
                .get_by_ids(&event_ids)
                .await
                .map_err(|e| eyre!("Failed to hydrate replay scope events: {e}"));
        }

        let mut all_events = Vec::with_capacity(event_ids.len());
        for chunk in event_ids.chunks(CHUNK_SIZE) {
            let chunk_events = pool
                .events()
                .get_by_ids(chunk)
                .await
                .map_err(|e| eyre!("Failed to hydrate replay scope events (chunk): {e}"))?;
            all_events.extend(chunk_events);
        }
        Ok(all_events)
    }

    pub(crate) async fn collect_operation_output_events(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
    ) -> Result<Vec<OperationOutputEvent>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id AS "id!",
                equivalence_key
            FROM core.events
            WHERE created_by_operation_id = $1::uuid
            ORDER BY id
            "#,
            operation_id,
        )
        .fetch_all(pool)
        .await
        .map_err(|e| eyre!("Failed to query replay operation outputs: {e}"))?;

        Ok(rows
            .into_iter()
            .map(|row| OperationOutputEvent {
                id: row.id,
                equivalence_key: row.equivalence_key,
            })
            .collect())
    }

    pub(crate) fn expected_replay_outputs(
        material_roots: &[StoredEvent],
    ) -> Result<ExpectedReplayOutputs> {
        if material_roots.is_empty() {
            return Err(eyre!(
                "Replay output expectations require at least one material root"
            ));
        }

        let mut sources = HashSet::new();
        let mut event_types = HashSet::new();

        for event in material_roots {
            sources.insert(event.source.as_ref().to_string());
            event_types.insert(event.event_type.as_ref().to_string());
            match &event.provenance {
                Provenance::Material { .. } => {}
                Provenance::Synthesis { .. } => {
                    return Err(eyre!(
                        "Replay scope included non-material root '{}' / '{}'",
                        event.source,
                        event.event_type
                    ));
                }
            }
        }

        let mut sources: Vec<_> = sources.into_iter().collect();
        sources.sort_unstable();
        let mut event_types: Vec<_> = event_types.into_iter().collect();
        event_types.sort_unstable();

        Ok(ExpectedReplayOutputs {
            minimum_visible_count: 0,
            sources,
            event_types,
            logical_source_identifiers: Vec::new(),
        })
    }

    pub(crate) fn logical_source_identifier(material: &ResolvedReplayMaterial) -> &str {
        material
            .material_metadata
            .get("logical_source_identifier")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| {
                material
                    .source_identifier
                    .split("#material=")
                    .next()
                    .unwrap_or(material.source_identifier.as_str())
            })
    }

    pub(crate) fn with_logical_source_identifiers(
        mut expected: ExpectedReplayOutputs,
        replay_materials: &[ResolvedReplayMaterial],
    ) -> Result<ExpectedReplayOutputs> {
        let mut logical_source_identifiers = replay_materials
            .iter()
            .map(Self::logical_source_identifier)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        logical_source_identifiers.sort_unstable();
        logical_source_identifiers.dedup();

        if logical_source_identifiers.is_empty() {
            return Err(eyre!(
                "Replay output expectations require at least one logical source identifier"
            ));
        }

        expected.minimum_visible_count = logical_source_identifiers.len() as u64;
        expected.logical_source_identifiers = logical_source_identifiers;
        Ok(expected)
    }

    pub(crate) async fn count_visible_replay_outputs(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        expected: &ExpectedReplayOutputs,
    ) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            r"
            SELECT COUNT(DISTINCT COALESCE(
                    smr.metadata->>'logical_source_identifier',
                    split_part(smr.source_identifier, '#material=', 1)
                  ))::bigint
            FROM core.events
            INNER JOIN raw.source_material_registry smr
                ON smr.id = core.events.source_material_id
            WHERE created_by_operation_id = $1::uuid
              AND source = ANY($2::text[])
              AND event_type = ANY($3::text[])
              AND COALESCE(
                    smr.metadata->>'logical_source_identifier',
                    split_part(smr.source_identifier, '#material=', 1)
                  ) = ANY($4::text[])
            ",
        )
        .bind(operation_id)
        .bind(&expected.sources)
        .bind(&expected.event_types)
        .bind(&expected.logical_source_identifiers)
        .fetch_one(pool)
        .await
        .map_err(|e| eyre!("Failed to count visible replay outputs: {e}"))
    }

    pub(crate) async fn wait_for_replay_outputs_visible(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        expected: &ExpectedReplayOutputs,
    ) -> Result<()> {
        let timeout = self
            .scan_completion_timeout
            .min(REPLAY_OUTPUT_VISIBILITY_TIMEOUT);

        let wait_result = tokio::time::timeout(timeout, async {
            loop {
                let visible_count = self
                    .count_visible_replay_outputs(pool, operation_id, expected)
                    .await?;
                if visible_count >= expected.minimum_visible_count as i64 {
                    debug!(
                        operation_id = %operation_id,
                        visible_count,
                        minimum_visible_count = expected.minimum_visible_count,
                        "Replay outputs are query-visible"
                    );
                    return Ok::<(), color_eyre::eyre::Report>(());
                }

                tokio::time::sleep(Self::EXECUTION_STATE_POLL_INTERVAL).await;
            }
        })
        .await;

        match wait_result {
            Ok(result) => result,
            Err(_timeout) => {
                let visible_count = self
                    .count_visible_replay_outputs(pool, operation_id, expected)
                    .await
                    .unwrap_or(-1);
                Err(eyre!(
                    "Replay outputs were not query-visible after successful scan within {:?} (visible={}, minimum_visible={}, sources={}, event_types={}, logical_sources={})",
                    timeout,
                    visible_count,
                    expected.minimum_visible_count,
                    expected.sources.join(","),
                    expected.event_types.join(","),
                    expected.logical_source_identifiers.join(","),
                ))
            }
        }
    }

    pub(crate) async fn resolve_replay_materials(
        &self,
        pool: &sqlx::PgPool,
        material_ids: &[Uuid],
    ) -> Result<Vec<ResolvedReplayMaterial>> {
        let mut resolved = Vec::with_capacity(material_ids.len());
        let mut missing = Vec::new();

        for material_id in material_ids {
            let record = pool
                .source_materials()
                .get_by_id(Id::from_uuid(*material_id))
                .await
                .map_err(|e| eyre!("{e}"))
                .wrap_err("Failed to resolve source material for replay")?;

            match record {
                Some(record) => resolved.push(ResolvedReplayMaterial::from(record)),
                None => missing.push(*material_id),
            }
        }

        if !missing.is_empty() {
            return Err(eyre!(
                "Replay scope referenced missing source materials: {}",
                missing
                    .iter()
                    .map(Uuid::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        Ok(resolved)
    }

    pub(crate) async fn derive_cascade_ids(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        root_ids: &[Uuid],
    ) -> Result<Vec<Uuid>> {
        let mut tx = pool
            .begin()
            .await
            .wrap_err("Failed to begin transaction for cascade expansion")?;
        let mut repo_tx = EventRepositoryTx::new(&mut tx);
        let session_id = format!("replay_{}", operation_id.simple());

        let table_name = repo_tx
            .prepare_cascade_session(&session_id, false)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to prepare replay cascade session")?;
        repo_tx
            .populate_cascade_roots(&table_name, root_ids)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to populate replay cascade roots")?;
        repo_tx
            .expand_cascade(
                &table_name,
                i32::try_from(sinex_primitives::constants::replay::DEFAULT_CASCADE_MAX_DEPTH)
                    .unwrap_or(i32::MAX),
            )
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to expand replay cascade")?;

        let mut cascade_ids: Vec<Uuid> = repo_tx
            .get_event_dependencies(&table_name)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to read replay cascade members")?
            .into_iter()
            .map(|(event_id, _)| event_id)
            .collect();

        repo_tx
            .cleanup_cascade_session(&table_name)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to cleanup replay cascade session")?;
        tx.commit()
            .await
            .wrap_err("Failed to commit replay cascade transaction")?;

        cascade_ids.sort_unstable();
        cascade_ids.dedup();
        Ok(cascade_ids)
    }

    pub(crate) async fn archive_cascade(
        &self,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
        operation_id: Uuid,
        archived_by: &str,
    ) -> Result<u64> {
        if cascade_ids.is_empty() {
            return Ok(0);
        }

        pool.events()
            .execute_cascade_archive(
                cascade_ids,
                "superseded by replay re-execution",
                &operation_id.to_string(),
                archived_by,
            )
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to archive replay cascade")
    }

    /// Collect scope metadata from events about to be archived.
    ///
    /// Returns `(event_type, scope_keys)` pairs grouped by `event_type`.
    /// Called before `archive_cascade` so we can emit invalidation signals after.
    pub(crate) async fn collect_cascade_scope_metadata(
        &self,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
    ) -> Result<Vec<ScopeInvalidationBucket>> {
        if cascade_ids.is_empty() {
            return Ok(Vec::new());
        }

        self.maybe_fail_scope_metadata_collection()
            .wrap_err("Failed to collect replay cascade scope metadata")?;

        // Query scope metadata for cascade events that have scope_keys so invalidations
        // stay bucketed by the archived event source + type pair.
        let rows = sqlx::query!(
            "SELECT id, source, event_type, scope_key, \
                    (source_event_ids IS NOT NULL) AS \"has_lineage!: bool\" \
             FROM core.events \
             WHERE id = ANY($1::uuid[]) AND scope_key IS NOT NULL",
            cascade_ids,
        )
        .fetch_all(pool)
        .await
        .map_err(|e| eyre!("Failed to collect cascade scope metadata: {e}"))?;

        let mut grouped: HashMap<(EventSource, EventType, bool), ScopeInvalidationBucket> =
            HashMap::new();
        for row in rows {
            if let Some(sk) = row.scope_key {
                let event_source = EventSource::new(row.source.clone()).map_err(|error| {
                    eyre!(
                        "Invalid event source '{}' in replay cascade scope metadata: {error}",
                        row.source
                    )
                })?;
                let event_type = EventType::new(row.event_type.clone()).map_err(|error| {
                    eyre!(
                        "Invalid event type '{}' in replay cascade scope metadata: {error}",
                        row.event_type
                    )
                })?;
                let bucket = grouped
                    .entry((event_source.clone(), event_type.clone(), row.has_lineage))
                    .or_insert_with(|| ScopeInvalidationBucket {
                        event_ids: Vec::new(),
                        event_source,
                        event_type,
                        has_lineage: row.has_lineage,
                        scope_keys: Vec::new(),
                    });
                bucket.event_ids.push(row.id);
                bucket.scope_keys.push(sk);
            }
        }

        for bucket in grouped.values_mut() {
            bucket.event_ids.sort_unstable();
            bucket.event_ids.dedup();
            bucket.scope_keys.sort_unstable();
            bucket.scope_keys.dedup();
        }

        Ok(grouped.into_values().collect())
    }

    /// Publish scope invalidation signals for archived events.
    ///
    /// Notifies derived nodes that scopes need recomputation because events
    /// were archived. Only publishes for events that had `scope_keys`.
    pub(crate) async fn publish_scope_invalidations(
        &self,
        scope_metadata: &[ScopeInvalidationBucket],
        operation_id: Uuid,
    ) -> Result<()> {
        if scope_metadata.is_empty() {
            return Ok(());
        }

        let invalidation_subject = self.env.nats_subject(INVALIDATION_SUBJECT);

        for bucket in scope_metadata {
            let invalidation = DerivedScopeInvalidation::archived(
                bucket.event_ids.clone(),
                bucket.event_source.clone(),
                bucket.event_type.clone(),
            )
            .with_has_lineage(bucket.has_lineage)
            .with_operation(operation_id)
            .with_scope_keys(bucket.scope_keys.clone());

            match serde_json::to_vec(&invalidation) {
                Ok(payload) => {
                    self.maybe_fail_scope_invalidation_publish()?;
                    // transport::Class::Invalidation — JetStream-backed scope
                    // fan-out; failure propagated to caller (replay operation
                    // decides abort/continue). No Sinex-Traffic-Class header on
                    // the plain js.publish path (no header map variant here).
                    if let Err(e) = self
                        .js
                        .publish(invalidation_subject.clone(), payload.into())
                        .await
                    {
                        return Err(eyre!(
                            "Failed to publish replay scope invalidation for event type '{}' (scope_count={}): {e}",
                            bucket.event_type,
                            bucket.scope_keys.len()
                        ));
                    }
                    debug!(
                        operation_id = %operation_id,
                        event_type = %bucket.event_type,
                        scope_count = bucket.scope_keys.len(),
                        "Published scope invalidation"
                    );
                }
                Err(e) => {
                    return Err(eyre!(
                        "Failed to serialize replay scope invalidation for event type '{}' (scope_count={}): {e}",
                        bucket.event_type,
                        bucket.scope_keys.len()
                    ));
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn restore_cascade(
        &self,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
        operation_id: Uuid,
    ) -> Result<()> {
        if cascade_ids.is_empty() {
            return Ok(());
        }

        pool.events()
            .execute_cascade_restore(cascade_ids, &operation_id.to_string())
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to restore archived replay cascade after replay dispatch failure")?;
        Ok(())
    }

    pub(crate) async fn abort_before_scan_ack(
        &self,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
        scope_metadata: &[ScopeInvalidationBucket],
        operation_id: Uuid,
        error: color_eyre::eyre::Report,
    ) -> Result<u64> {
        if let Err(restore_error) = self.restore_cascade(pool, cascade_ids, operation_id).await {
            return Err(error.wrap_err(format!(
                "Replay dispatch failed before node acknowledgement, and restoring the archived cascade also failed: {restore_error}"
            )));
        }

        if let Err(invalidation_error) = self
            .publish_scope_invalidations(scope_metadata, operation_id)
            .await
        {
            return Err(error.wrap_err(format!(
                "Replay dispatch failed before node acknowledgement, restored the archived cascade, but failed to publish compensating scope invalidations: {invalidation_error}"
            )));
        }

        Err(error.wrap_err(
            "Replay dispatch failed before node acknowledgement; restored archived cascade and published compensating scope invalidations",
        ))
    }

    /// Timeout for the node to acknowledge the scan command.
    pub(crate) const SCAN_ACK_TIMEOUT: Duration = Duration::from_secs(10);
    /// Timeout for the entire scan operation to complete.
    pub(crate) const SCAN_COMPLETION_TIMEOUT: Duration = Duration::from_mins(10);
}
