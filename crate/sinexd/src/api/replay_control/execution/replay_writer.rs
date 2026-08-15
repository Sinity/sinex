//! Event-replacement recording and the replay scan/loop core for
//! `ReplayExecutionEngine`. See `execution/mod.rs` for the engine type.

use super::collect::ReplayExecutionBatch;
use super::{
    ExpectedReplayOutputs, ExtendedMaterialOccurrenceKey, OperationOutputEvent,
    REPLAY_OUTPUT_VISIBILITY_TIMEOUT, ReplayExecutionEngine, ReplayPreviewSummary, StreamExt,
};
use crate::runtime::stream::{
    Checkpoint, MaterialReplayContext, ReplayScopeFilters as SourceReplayScopeFilters, ScanArgs,
    SourceScanAck, SourceScanCancel, SourceScanCommand, SourceScanProgress, TimeHorizon,
};
use crate::sources::parse_listener::SourceParseAck;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::ControlSubject;
use sinex_primitives::events::ScopeKey;
use sinex_primitives::{Result, SinexError, Timestamp, Uuid};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use sinex_db::replay::state_machine::{ReplayCheckpoint, ReplayScope, ReplayState};

pub(super) fn material_occurrence_key(
    event: &OperationOutputEvent,
) -> Option<ExtendedMaterialOccurrenceKey> {
    Some(ExtendedMaterialOccurrenceKey {
        source_material_id: event.source_material_id?,
        anchor_byte: event.anchor_byte?,
        offset_start: event.offset_start,
        offset_end: event.offset_end,
        offset_kind: event.offset_kind.clone(),
    })
}

/// Outcome of waiting for a staged-source replay's parsed outputs to become
/// query-visible (see [`ReplayExecutionEngine::wait_for_staged_replay_outputs_or_cancel`]).
enum StagedReplayWait {
    Visible,
    Cancelled,
    Error(String),
}

fn replacement_relation_kind(
    old_count: usize,
    new_count: usize,
) -> sinex_db::repositories::ReplacementKind {
    use sinex_db::repositories::ReplacementKind;

    match (old_count, new_count) {
        (1, 1) => ReplacementKind::Superseded,
        (1, _) => ReplacementKind::Split,
        (_, 1) => ReplacementKind::Collapsed,
        _ => ReplacementKind::Recomputed,
    }
}

impl ReplayExecutionEngine {
    /// Best-effort request that the source runtime stop an in-flight
    /// dispatched scan (sinex-audit-replay-cancel-orphan).
    ///
    /// Publishes `SourceScanCancel` to `sinex.control.sources.<name>.cancel`.
    /// Fire-and-forget: failures are logged, never propagated, because the
    /// caller's own polling loop already treats `Cancelling`/`Cancelled`
    /// operation state as terminal regardless of whether the runtime ever
    /// receives this signal (e.g. it may not be running, or the message may
    /// be dropped) — this is the propagation half of the fix, not the sole
    /// safety net.
    pub(crate) async fn publish_scan_cancel(&self, control_source_name: &str, operation_id: Uuid) {
        let subject = self
            .env
            .nats_subject(&ControlSubject::source_cancel(control_source_name));
        let payload = match serde_json::to_vec(&SourceScanCancel { operation_id }) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    operation_id = %operation_id,
                    error = %error,
                    "Failed to encode scan cancel command"
                );
                return;
            }
        };
        if let Err(error) = self
            .nats_client
            .publish(subject.clone(), payload.into())
            .await
        {
            warn!(
                operation_id = %operation_id,
                subject = %subject,
                error = %error,
                "Failed to publish scan cancel command"
            );
        }
    }

    /// Record replacement relations between archived material events and newly-created events.
    ///
    /// After a successful replay scan, this queries for:
    /// - Old events: from `audit.archived_events` matching the durable archive reason
    /// - New events: from `core.events` with `created_by_operation_id = operation_id`
    ///
    /// Matching strategy: material replay uses physical source occurrence coordinates:
    /// `(source_material_id, anchor_byte, offset_start, offset_end, offset_kind)`.
    /// `equivalence_key` is a derived-output slot concept and is intentionally not
    /// part of material replay lineage.
    pub(crate) async fn record_event_replacements(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        archive_reason: &str,
    ) -> Result<()> {
        use sinex_db::repositories::ReplacementRecord;

        // Query physical occurrence coordinates for archived material events.
        let old_rows = sqlx::query!(
            r#"SELECT
                id as "id!",
                scope_key,
                source_material_id,
                anchor_byte,
                offset_start,
                offset_end,
                offset_kind,
                anchor_payload_hash AS "anchor_payload_hash: Vec<u8>"
             FROM audit.archived_events
             WHERE archive_reason = $1
               AND source_material_id IS NOT NULL
               AND anchor_byte IS NOT NULL"#,
            archive_reason,
        )
        .fetch_all(pool)
        .await
        .map_err(|err| {
            SinexError::database("Failed to query archived events for replacement matching")
                .with_std_error(&err)
        })?;
        let old_row_count = old_rows.len();

        // Query the actual events emitted by this replay operation. Re-querying
        // the original scope window can miss replacements or bind unrelated
        // live rows once the replay finishes.
        let new_events = self
            .collect_operation_output_events(pool, operation_id)
            .await?;

        if new_events.is_empty() {
            debug!(
                operation_id = %operation_id,
                old_count = old_rows.len(),
                "No new events found after replay scan — skipping replacement recording"
            );
            return Ok(());
        }

        // Build source occurrence → new_event_ids index, preserving every output
        // at the same occurrence. Multiple outputs at the same physical position
        // are represented as split/collapsed/recomputed relations by count.
        // Also build id→hash lookup for integrity verification.
        let mut occurrence_to_new: HashMap<ExtendedMaterialOccurrenceKey, Vec<Uuid>> =
            HashMap::new();
        let mut new_hash_by_id: HashMap<Uuid, Option<Vec<u8>>> = HashMap::new();
        for event in &new_events {
            new_hash_by_id.insert(event.id, event.anchor_payload_hash.clone());
            if let Some(key) = material_occurrence_key(event) {
                occurrence_to_new.entry(key).or_default().push(event.id);
            }
        }

        let mut old_by_occurrence: HashMap<ExtendedMaterialOccurrenceKey, Vec<_>> = HashMap::new();
        let mut skipped_old_count = 0usize;
        for row in old_rows {
            let Some(source_material_id) = row.source_material_id else {
                skipped_old_count += 1;
                continue;
            };
            let Some(anchor_byte) = row.anchor_byte else {
                skipped_old_count += 1;
                continue;
            };
            old_by_occurrence
                .entry(ExtendedMaterialOccurrenceKey {
                    source_material_id,
                    anchor_byte,
                    offset_start: row.offset_start,
                    offset_end: row.offset_end,
                    offset_kind: row.offset_kind,
                })
                .or_default()
                .push((row.id, row.scope_key, row.anchor_payload_hash));
        }

        let mut replacements = Vec::with_capacity(old_row_count);
        let mut unmatched_count = 0usize;
        let mut integrity_mismatch_count = 0usize;
        for (key, old_events) in old_by_occurrence {
            let Some(new_event_ids) = occurrence_to_new.get(&key) else {
                unmatched_count += old_events.len();
                continue;
            };

            let relation_kind = replacement_relation_kind(old_events.len(), new_event_ids.len());
            for (old_event_id, scope_key, old_hash) in &old_events {
                for &new_event_id in new_event_ids {
                    // Verify anchor_payload_hash integrity when both old and new carry one.
                    // Mismatch means source material bytes changed between original
                    // ingestion and replay — corruption, tampering, or rewritten material.
                    let new_hash = new_hash_by_id.get(&new_event_id).and_then(|h| h.as_deref());
                    if let (Some(old_bytes), Some(new_bytes)) = (old_hash.as_deref(), new_hash)
                        && old_bytes != new_bytes
                    {
                        integrity_mismatch_count += 1;
                        let to_hex = |bytes: &[u8]| -> String {
                            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
                        };
                        warn!(
                            operation_id = %operation_id,
                            source_material_id = %key.source_material_id,
                            anchor_byte = key.anchor_byte,
                            old_event_id = %old_event_id,
                            new_event_id = %new_event_id,
                            old_hash = %to_hex(old_bytes),
                            new_hash = %to_hex(new_bytes),
                            "IntegrityMismatch: anchor_payload_hash changed between original ingestion and replay"
                        );
                    }
                    replacements.push(ReplacementRecord {
                        old_event_id: *old_event_id,
                        new_event_id,
                        relation_kind,
                        scope_key: scope_key.clone().map(ScopeKey::from),
                        equivalence_key: None,
                    });
                }
            }
        }

        if unmatched_count > 0 || skipped_old_count > 0 || integrity_mismatch_count > 0 {
            warn!(
                operation_id = %operation_id,
                unmatched_count,
                skipped_old_count,
                integrity_mismatch_count,
                archive_reason,
                new_count = new_events.len(),
                "Skipped or mismatched replay replacement records detected"
            );
        }

        if replacements.is_empty() {
            debug!(
                operation_id = %operation_id,
                archive_reason,
                new_count = new_events.len(),
                "No replay replacement matches found — skipping replacement recording"
            );
            return Ok(());
        }

        self.maybe_fail_replacement_recording().map_err(|err| {
            SinexError::service("Failed to record replay replacement relations").with_source(err)
        })?;

        let count = pool
            .events()
            .record_replacements(operation_id, &replacements)
            .await
            .map_err(|err| {
                SinexError::database("Failed to record replay replacement relations")
                    .with_source(err)
            })?;

        info!(
            operation_id = %operation_id,
            replacement_count = count,
            archive_reason,
            new_events = new_events.len(),
            "Recorded event replacement relations"
        );

        Ok(())
    }

    /// Archive outputs emitted by a replay that is being compensated before
    /// restoring the pre-replay cascade. This prevents partial replacement
    /// interpretations from remaining live alongside the restored originals.
    async fn archive_partial_replay_outputs(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
    ) -> Result<u64> {
        let output_ids = self
            .collect_operation_output_events(pool, operation_id)
            .await?
            .into_iter()
            .map(|event| event.id)
            .collect::<Vec<_>>();
        if output_ids.is_empty() {
            return Ok(0);
        }

        pool.events()
            .execute_cascade_archive(
                &output_ids,
                "archive replay outputs during compensation",
                &operation_id.to_string(),
                "replay-compensation",
            )
            .await
            .map_err(|err| {
                SinexError::database("Failed to archive partial replay outputs during compensation")
                    .with_source(err)
            })
    }

    /// Dispatch a replay by telling the source runtime to re-scan source material.
    ///
    /// Instead of republishing stored event rows to NATS (reinjection), this:
    /// 1. Archives the affected cascade (existing events + derivatives)
    /// 2. Sends a `SourceScanCommand` to the running source via NATS request-reply
    /// 3. Waits for the source to acknowledge and complete the scan
    /// 4. The source re-reads source material and emits fresh events through normal flow
    /// 5. Downstream automatons process the new events naturally via `JetStream`
    ///
    /// ## Transaction-boundary note
    ///
    /// Replay cascade expansion, scope-metadata collection, and live-row archive
    /// execute inside one database transaction. That transaction takes a narrow
    /// `core.events` archive critical section so newly-arriving derived events
    /// cannot interleave between cascade selection and deletion.
    ///
    /// NATS invalidation publish and source scan dispatch remain outside the DB
    /// transaction. Failures after the archive commit are handled by the replay
    /// saga (`abort_before_scan_ack`) rather than holding database locks across
    /// request-reply messaging.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn replay_events(
        &self,
        operation_id: Uuid,
        scope: &ReplayScope,
        execution_window: (Timestamp, Timestamp),
        expected_total_events: u64,
        preview_roots: &ReplayPreviewSummary,
        pool: &sqlx::PgPool,
        checkpoint: &mut ReplayCheckpoint,
        executor_name: &str,
        rate_budget: Option<sinex_primitives::pacing::RateBudget>,
    ) -> Result<u64> {
        let current_roots = self.replay.scope_root_snapshot(scope).await?;
        if current_roots.root_event_count != preview_roots.root_event_count
            || current_roots.root_event_id_fingerprint != preview_roots.root_event_id_fingerprint
        {
            return Err(SinexError::invalid_state(
                "Replay preview is stale: the matching root set changed; refresh preview before execution",
            )
            .with_context("operation_id", operation_id.to_string())
            .with_context("expected_root_event_count", preview_roots.root_event_count.to_string())
            .with_context("actual_root_event_count", current_roots.root_event_count.to_string()));
        }

        let normalized = scope.normalized_filters();

        if let Err(error) = self.validate_scope_material_authority(scope).await {
            return Err(SinexError::service(
                "Replay source-material authority validation failed before archive",
            )
            .with_source(error));
        }
        self.validate_scope_replay_inputs(pool, scope).await?;

        // Step 1: Archive the affected cascade
        let archived_cascade = self
            .archive_replay_cascade_atomically(
                pool,
                operation_id,
                scope,
                execution_window,
                expected_total_events,
                executor_name,
            )
            .await?;
        let archive_reason = archived_cascade.archive_reason;
        let archived_count = archived_cascade.archived_count;
        let scoped_event_count = archived_cascade.scoped_event_count;
        info!(
            operation_id = %operation_id,
            material_roots = expected_total_events,
            archived_count,
            "Archived replay cascade, dispatching scan to source"
        );

        if scoped_event_count > 0 {
            if let Err(error) = self
                .stale_projection_registry_for_scopes(
                    pool,
                    &archive_reason,
                    scoped_event_count,
                    operation_id,
                )
                .await
            {
                return self
                    .compensate_after_archive_failure(
                        pool,
                        &archive_reason,
                        archived_count,
                        scoped_event_count,
                        operation_id,
                        error,
                    )
                    .await;
            }
        }

        // The archive transaction records scope invalidations as pending in
        // operation metadata before committing archived rows. NATS publication
        // remains outside the DB transaction; a crash in that boundary leaves a
        // durable recovery marker that ops/debt views can surface instead of a
        // silent stale-projection gap.

        // Publish scope invalidation signals for archived derived events
        if scoped_event_count > 0
            && let Err(invalidation_error) = self
                .publish_scope_invalidations(&archive_reason, scoped_event_count, operation_id)
                .await
        {
            error!(
                target: "sinex_metrics",
                metric = "gateway.replay_invalidation_failures_total",
                operation_id = %operation_id,
                archived_count,
                scoped_event_count,
                "Replay scope invalidation publish failed after archive commit; restoring cascade: {invalidation_error}"
            );
            return self
                    .abort_before_scan_ack(
                        pool,
                        &archive_reason,
                        archived_count,
                        scoped_event_count,
                        operation_id,
                        SinexError::nats_publish(format!(
                            "Failed to publish replay scope invalidations before dispatch: {invalidation_error}"
                        ))
                        .with_source(invalidation_error),
                    )
                    .await;
        }
        if scoped_event_count > 0
            && let Err(record_error) = self
                .replay
                .record_scope_invalidations_published(operation_id)
                .await
        {
            warn!(
                operation_id = %operation_id,
                archived_count,
                scoped_event_count,
                error = %record_error,
                "Published replay scope invalidations but failed to clear the durable pending marker; recovery/debt views will continue reporting it"
            );
        }

        checkpoint.total_events = expected_total_events;

        let mut after_root_id = None;
        let mut dispatched_root_count = 0_u64;
        let mut scan_processed_count = 0_u64;
        let mut batch_number = 0_u32;
        let mut expected_replay_outputs = ExpectedReplayOutputs {
            minimum_visible_count: 0,
            sources: Vec::new(),
            event_types: Vec::new(),
            logical_source_identifiers: Vec::new(),
            expected_outputs: Vec::new(),
            source_material_ids: Vec::new(),
        };
        let mut control_source_name: Option<String> = None;

        // Step 2: Route staged-source scopes through source, not live source scan.
        // RuntimeModule scan publishes a SourceScanCommand to sinex.control.sources.{source}.scan;
        // staged-source replay creates a source_run and dispatches to the source
        // host (#1081) via a parse command. The routing decision is made here so both
        // paths share the archive + invalidation + checkpoint machinery above.
        if scope.is_staged_source_scope() {
            loop {
                let batch = match self
                    .collect_archived_replay_root_batch(pool, &archive_reason, after_root_id)
                    .await
                {
                    Ok(batch) => batch,
                    Err(error) => {
                        return self
                            .compensate_after_archive_failure(
                                pool,
                                &archive_reason,
                                archived_count,
                                scoped_event_count,
                                operation_id,
                                error,
                            )
                            .await;
                    }
                };
                let Some(batch) = batch else {
                    break;
                };
                dispatched_root_count += batch.material_roots.len() as u64;
                after_root_id = Some(batch.last_root_id);
                Self::merge_expected_replay_outputs(
                    &mut expected_replay_outputs,
                    batch.expected_outputs,
                );
            }
            if dispatched_root_count != expected_total_events {
                return self
                    .compensate_after_archive_failure(
                        pool,
                        &archive_reason,
                        archived_count,
                        scoped_event_count,
                        operation_id,
                        SinexError::invalid_state(
                            "Archived replay roots no longer match the approved preview",
                        )
                        .with_context(
                            "expected_root_event_count",
                            expected_total_events.to_string(),
                        )
                        .with_context(
                            "archived_root_event_count",
                            dispatched_root_count.to_string(),
                        ),
                    )
                    .await;
            }
            return self
                .dispatch_staged_source_replay(
                    operation_id,
                    scope,
                    pool,
                    &archive_reason,
                    archived_count,
                    scoped_event_count,
                    executor_name,
                    &expected_replay_outputs,
                )
                .await;
        }

        loop {
            let batch = match self
                .collect_archived_replay_root_batch(pool, &archive_reason, after_root_id)
                .await
            {
                Ok(batch) => batch,
                Err(error) => {
                    return self
                        .compensate_after_archive_failure(
                            pool,
                            &archive_reason,
                            archived_count,
                            scoped_event_count,
                            operation_id,
                            error,
                        )
                        .await;
                }
            };
            let Some(batch) = batch else {
                break;
            };
            dispatched_root_count += batch.material_roots.len() as u64;
            after_root_id = Some(batch.last_root_id);
            batch_number += 1;
            checkpoint.batch_number = batch_number;

            let batch_control_source =
                match Self::scan_control_source_name(scope, &batch.replay_materials) {
                    Ok(source) => source,
                    Err(error) => {
                        return self
                            .compensate_after_archive_failure(
                                pool,
                                &archive_reason,
                                archived_count,
                                scoped_event_count,
                                operation_id,
                                error,
                            )
                            .await;
                    }
                };
            if let Some(expected_source) = &control_source_name
                && expected_source != &batch_control_source
            {
                return self.compensate_after_archive_failure(
                    pool,
                    &archive_reason,
                    archived_count,
                    scoped_event_count,
                    operation_id,
                    SinexError::invalid_state("Replay scope spans multiple source runtime identities across execution batches"),
                ).await;
            }
            control_source_name.get_or_insert(batch_control_source.clone());
            Self::merge_expected_replay_outputs(
                &mut expected_replay_outputs,
                batch.expected_outputs.clone(),
            );

            match self
                .dispatch_live_replay_scan_batch(
                    operation_id,
                    execution_window,
                    &normalized,
                    batch,
                    &batch_control_source,
                    checkpoint,
                    scan_processed_count,
                    rate_budget,
                )
                .await
            {
                Ok(processed) => scan_processed_count += processed,
                Err(error) => {
                    return self
                        .compensate_after_archive_failure(
                            pool,
                            &archive_reason,
                            archived_count,
                            scoped_event_count,
                            operation_id,
                            error,
                        )
                        .await;
                }
            }
        }

        if dispatched_root_count != expected_total_events {
            return self
                .compensate_after_archive_failure(
                    pool,
                    &archive_reason,
                    archived_count,
                    scoped_event_count,
                    operation_id,
                    SinexError::invalid_state(
                        "Archived replay roots no longer match the approved preview",
                    )
                    .with_context(
                        "expected_root_event_count",
                        expected_total_events.to_string(),
                    )
                    .with_context(
                        "archived_root_event_count",
                        dispatched_root_count.to_string(),
                    ),
                )
                .await;
        }
        if let Err(error) = self
            .wait_for_replay_outputs_visible(
                pool,
                operation_id,
                &archive_reason,
                &expected_replay_outputs,
            )
            .await
        {
            return self
                .compensate_after_archive_failure(
                    pool,
                    &archive_reason,
                    archived_count,
                    scoped_event_count,
                    operation_id,
                    error,
                )
                .await;
        }
        if let Err(error) = self
            .record_event_replacements(pool, operation_id, &archive_reason)
            .await
        {
            return self
                .compensate_after_archive_failure(
                    pool,
                    &archive_reason,
                    archived_count,
                    scoped_event_count,
                    operation_id,
                    error,
                )
                .await;
        }
        Ok(scan_processed_count)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn dispatch_live_replay_scan_batch(
        &self,
        operation_id: Uuid,
        execution_window: (Timestamp, Timestamp),
        normalized: &sinex_db::replay::state_machine::ReplayScopeFilters,
        batch: ReplayExecutionBatch,
        control_source_name: &str,
        checkpoint: &mut ReplayCheckpoint,
        processed_offset: u64,
        rate_budget: Option<sinex_primitives::pacing::RateBudget>,
    ) -> Result<u64> {
        let progress_subject = self
            .env
            .nats_subject(&ControlSubject::replay_progress(operation_id));
        let mut progress_sub =
            self.nats_client
                .subscribe(progress_subject)
                .await
                .map_err(|error| {
                    SinexError::nats_subscribe("Failed to subscribe to replay progress")
                        .with_std_error(&error)
                })?;
        let replay_context = MaterialReplayContext {
            operation_id,
            materials: batch.replay_materials,
            occurrences: batch.replay_occurrences,
            replay_scope: SourceReplayScopeFilters {
                material_ids: normalized.material_ids.clone(),
                event_types: normalized.event_types.clone(),
            },
        };
        let command = SourceScanCommand {
            operation_id,
            from: Checkpoint::None,
            until: TimeHorizon::Historical {
                end_time: execution_window.1,
            },
            args: ScanArgs {
                targets: vec![control_source_name.to_string()],
                dry_run: false,
                interactive: false,
                max_events: 0,
                skip_duplicates: true,
                config: HashMap::new(),
                replay: Some(replay_context),
                rate_budget,
            },
        };
        let payload = serde_json::to_vec(&command).map_err(|error| {
            SinexError::serialization("Failed to serialize bounded SourceScanCommand")
                .with_std_error(&error)
        })?;
        let subject = self
            .env
            .nats_subject(&ControlSubject::source_scan(control_source_name));
        let ack_message = tokio::time::timeout(
            self.scan_ack_timeout,
            self.nats_client.request(subject.clone(), payload.into()),
        )
        .await
        .map_err(|_| {
            SinexError::timeout(format!(
                "Timed out waiting for scan ack from source '{control_source_name}' after {:?}",
                self.scan_ack_timeout
            ))
        })?
        .map_err(|error| {
            SinexError::nats(format!("NATS request to {subject} failed")).with_std_error(&error)
        })?;
        let ack: SourceScanAck = serde_json::from_slice(&ack_message.payload).map_err(|error| {
            SinexError::serialization("Failed to deserialize SourceScanAck").with_std_error(&error)
        })?;
        if !ack.accepted {
            return Err(SinexError::invalid_state(format!(
                "RuntimeModule '{}' rejected scan command: {}",
                ack.module_name,
                ack.error.unwrap_or_else(|| "unknown reason".to_string())
            )));
        }

        let replay = self.replay.clone();
        let target_source_name = ack.module_name;
        tokio::time::timeout(self.scan_completion_timeout, async {
            loop {
                tokio::select! {
                    maybe_message = progress_sub.next() => {
                        let message = maybe_message.ok_or_else(|| SinexError::nats(format!(
                            "Replay progress stream closed before source '{target_source_name}' reported completion"
                        )))?;
                        let progress: SourceScanProgress = match serde_json::from_slice(&message.payload) {
                            Ok(progress) => progress,
                            Err(error) => {
                                warn!(error = %error, "Failed to parse replay progress message");
                                continue;
                            }
                        };
                        if let Some(error) = progress.error {
                            return Err(if progress.cancelled {
                                SinexError::cancelled(format!("RuntimeModule '{}' scan for operation {operation_id} was cancelled: {error}", progress.module_name))
                            } else {
                                SinexError::processing(format!("RuntimeModule '{}' failed replay scan: {error}", progress.module_name))
                            });
                        }
                        checkpoint.processed_events = processed_offset.saturating_add(progress.events_processed);
                        checkpoint.updated_at = sinex_primitives::temporal::now();
                        self.persist_replay_checkpoint(
                            operation_id,
                            checkpoint,
                            "Failed to persist replay progress checkpoint",
                        ).await?;
                        if let Some(report) = progress.final_report {
                            return Ok(report.events_processed);
                        }
                    }
                    () = tokio::time::sleep(Self::EXECUTION_STATE_POLL_INTERVAL) => match replay.load_operation(operation_id).await? {
                        operation if operation.state == ReplayState::Executing => {}
                        operation if matches!(operation.state, ReplayState::Cancelling | ReplayState::Cancelled) => {
                            self.publish_scan_cancel(control_source_name, operation_id).await;
                            return Err(SinexError::cancelled(format!("Replay operation {operation_id} was cancelled during execution")));
                        }
                        operation => return Err(SinexError::invalid_state(format!(
                            "Replay operation {operation_id} left Executing state unexpectedly: {:?}", operation.state
                        ))),
                    }
                }
            }
        }).await.map_err(|_| SinexError::timeout(format!(
            "Replay scan timed out waiting for source '{target_source_name}' to report completion after {:?}", self.scan_completion_timeout
        )))?
    }

    /// Dispatches a staged-source replay through the source host.
    ///
    /// Publishes a parse command to the source NATS control subject and
    /// waits for the parsed events to become durably query-visible. The
    /// source's parse listener (`crate::sources::parse_listener`) is a
    /// single synchronous request/reply: by the time the ack arrives, every
    /// parsed record has already been dispatched into admission. There is no
    /// live task left to poll for a `ReplayState` transition, and
    /// `ReplayState::Completed`/`Failed` can only ever be set by
    /// `finalize_operation` (execution/mod.rs), which our own caller invokes
    /// *after* this function returns (sinex-2vve) — waiting for that state
    /// here would be a structural deadlock, not a race. This mirrors the
    /// non-staged scan path's success handling instead: wait for outputs to
    /// become visible (bounded, cancel-aware), record replacements, and let
    /// the caller's normal completion/compensation machinery run.
    async fn dispatch_staged_source_replay(
        &self,
        operation_id: Uuid,
        scope: &ReplayScope,
        pool: &sqlx::PgPool,
        archive_reason: &str,
        archived_count: u64,
        scoped_event_count: u64,
        executor_name: &str,
        expected_replay_outputs: &ExpectedReplayOutputs,
    ) -> Result<u64> {
        let source_id = scope.source_id.as_deref().unwrap_or("unknown");
        let parse_subject = self
            .env
            .nats_subject(&ControlSubject::source_parse(source_id));

        let parse_command = serde_json::json!({
            "operation_id": operation_id,
            "source_id": source_id,
            "source_material_id": scope.source_material_id,
            "source_version": scope.source_version,
            "executor": executor_name,
        });

        let command_payload = match serde_json::to_vec(&parse_command) {
            Ok(payload) => payload,
            Err(err) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        archive_reason,
                        archived_count,
                        scoped_event_count,
                        operation_id,
                        SinexError::serialization("Failed to serialize source parse command")
                            .with_std_error(&err),
                    )
                    .await;
            }
        };

        info!(
            operation_id = %operation_id,
            source_id = source_id,
            subject = %parse_subject,
            "Dispatching staged-source replay to source"
        );

        let ack_msg = match tokio::time::timeout(
            self.scan_ack_timeout,
            self.nats_client
                .request(parse_subject.clone(), command_payload.into()),
        )
        .await
        {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        archive_reason,
                        archived_count,
                        scoped_event_count,
                        operation_id,
                        SinexError::nats(format!("NATS request to {parse_subject} failed"))
                            .with_std_error(&error),
                    )
                    .await;
            }
            Err(_) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        archive_reason,
                        archived_count,
                        scoped_event_count,
                        operation_id,
                        SinexError::timeout(format!(
                            "Timed out waiting for source parse ack from '{source_id}' after {:?}",
                            self.scan_ack_timeout
                        )),
                    )
                    .await;
            }
        };

        let ack: SourceParseAck = match serde_json::from_slice(&ack_msg.payload) {
            Ok(ack) => ack,
            Err(err) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        archive_reason,
                        archived_count,
                        scoped_event_count,
                        operation_id,
                        SinexError::serialization("Failed to deserialize source parse ack")
                            .with_std_error(&err),
                    )
                    .await;
            }
        };

        if !ack.accepted {
            return self
                .abort_before_scan_ack(
                    pool,
                    archive_reason,
                    archived_count,
                    scoped_event_count,
                    operation_id,
                    SinexError::invalid_state(format!(
                        "Source '{source_id}' rejected parse command: {}",
                        ack.error.as_deref().unwrap_or("unknown reason")
                    )),
                )
                .await;
        }

        info!(
            operation_id = %operation_id,
            source_id = source_id,
            parsed_event_count = ?ack.event_count,
            "Source accepted parse command, waiting for outputs to become visible"
        );

        match self
            .wait_for_staged_replay_outputs_or_cancel(
                pool,
                operation_id,
                archive_reason,
                expected_replay_outputs,
            )
            .await
        {
            StagedReplayWait::Visible => {
                if let Err(link_error) = self
                    .record_event_replacements(pool, operation_id, archive_reason)
                    .await
                {
                    return self
                        .compensate_after_archive_failure(
                            pool,
                            archive_reason,
                            archived_count,
                            scoped_event_count,
                            operation_id,
                            SinexError::service(format!(
                                "Staged-source replay parse succeeded and outputs became visible, but linking replacement events failed: {link_error}"
                            )),
                        )
                        .await;
                }
                Ok(u64::try_from(ack.event_count.unwrap_or(0)).unwrap_or(u64::MAX))
            }
            StagedReplayWait::Cancelled => {
                let cancellation_error = SinexError::cancelled(format!(
                    "Staged-source replay {operation_id} was cancelled while waiting for parsed outputs to become visible"
                ));
                if let Err(compensation_error) = self
                    .compensate_staged_replay_failure(
                        pool,
                        archive_reason,
                        archived_count,
                        scoped_event_count,
                        operation_id,
                    )
                    .await
                {
                    return Err(SinexError::service(format!(
                        "{cancellation_error}; {compensation_error}"
                    ))
                    .with_source(cancellation_error)
                    .with_source(compensation_error));
                }
                Err(cancellation_error)
            }
            StagedReplayWait::Error(wait_error) => {
                self.compensate_after_archive_failure(
                    pool,
                    archive_reason,
                    archived_count,
                    scoped_event_count,
                    operation_id,
                    SinexError::service(format!(
                        "Staged-source replay for operation {operation_id} failed waiting for parsed outputs to become visible: {wait_error}"
                    )),
                )
                .await
            }
        }
    }

    /// Bounded, cancel-aware wait for a staged-source replay's parsed
    /// outputs to become query-visible.
    ///
    /// Unlike [`super::collect::ReplayExecutionEngine::wait_for_replay_outputs_visible`]
    /// (used by the non-staged scan path, which has its own independent
    /// progress-subscription loop to observe operator cancellation), this is
    /// the *only* place a staged-source replay can ever notice a Cancel
    /// request — the parse itself is a single synchronous request/reply with
    /// nothing left in flight to interrupt once accepted.
    async fn wait_for_staged_replay_outputs_or_cancel(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        archive_reason: &str,
        expected: &ExpectedReplayOutputs,
    ) -> StagedReplayWait {
        let timeout = self
            .scan_completion_timeout
            .min(REPLAY_OUTPUT_VISIBILITY_TIMEOUT);
        let replay = self.replay.clone();

        let wait_result = tokio::time::timeout(timeout, async {
            loop {
                let validation = match self
                    .validate_replay_outputs(pool, operation_id, archive_reason, expected)
                    .await
                {
                    Ok(validation) => validation,
                    Err(error) => return StagedReplayWait::Error(error.to_string()),
                };
                if validation.complete() {
                    return StagedReplayWait::Visible;
                }

                match replay.load_operation(operation_id).await {
                    Ok(operation)
                        if matches!(
                            operation.state,
                            ReplayState::Cancelling | ReplayState::Cancelled
                        ) =>
                    {
                        return StagedReplayWait::Cancelled;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        return StagedReplayWait::Error(format!(
                            "failed to reload replay operation while waiting for visibility: {error}"
                        ));
                    }
                }

                tokio::time::sleep(Self::EXECUTION_STATE_POLL_INTERVAL).await;
            }
        })
        .await;

        match wait_result {
            Ok(outcome) => outcome,
            Err(_timeout_elapsed) => StagedReplayWait::Error(format!(
                "parsed outputs were not query-visible within {timeout:?}"
            )),
        }
    }

    /// Best-effort compensation for a staged-source replay that failed or
    /// was cancelled after the parse was accepted (sinex-xixl): restore the
    /// archived cascade, link whatever replacements did become visible
    /// before giving up, and publish compensating scope invalidations so
    /// automata reconcile against the resulting mixed/restored state.
    /// All compensation steps are attempted. If any step fails, the caller
    /// receives an explicit operator-recovery error instead of finalizing a
    /// cancellation while the archived cascade may still be stranded.
    async fn compensate_staged_replay_failure(
        &self,
        pool: &sqlx::PgPool,
        archive_reason: &str,
        archived_count: u64,
        scoped_event_count: u64,
        operation_id: Uuid,
    ) -> Result<()> {
        let mut compensation_errors = Vec::new();
        if let Err(link_error) = self
            .record_event_replacements(pool, operation_id, archive_reason)
            .await
        {
            warn!(
                operation_id = %operation_id,
                error = %link_error,
                "Failed to link partial replacement events during staged-replay failure compensation"
            );
            compensation_errors.push(link_error);
        }
        if let Err(output_error) = self
            .archive_partial_replay_outputs(pool, operation_id)
            .await
        {
            warn!(
                operation_id = %operation_id,
                error = %output_error,
                "Failed to archive partial replay outputs during staged-replay failure compensation"
            );
            compensation_errors.push(output_error);
        }
        let restored = match self
            .restore_cascade(pool, archive_reason, archived_count, operation_id)
            .await
        {
            Ok(restored) => restored,
            Err(restore_error) => {
                warn!(
                    operation_id = %operation_id,
                    error = %restore_error,
                    "Failed to restore archived cascade during staged-replay failure compensation"
                );
                compensation_errors.push(restore_error);
                0
            }
        };
        if restored != archived_count {
            compensation_errors.push(SinexError::service(format!(
                "restored only {restored}/{} archived cascade members; operator recovery is required",
                archived_count
            )));
        }
        if let Err(invalidation_error) = self
            .publish_scope_invalidations(archive_reason, scoped_event_count, operation_id)
            .await
        {
            warn!(
                operation_id = %operation_id,
                error = %invalidation_error,
                "Failed to publish compensating scope invalidations during staged-replay failure compensation"
            );
            compensation_errors.push(invalidation_error);
        }
        let restored = match self
            .restore_cascade(pool, archive_reason, archived_count, operation_id)
            .await
        {
            Ok(restored) => restored,
            Err(restore_error) => {
                warn!(
                    operation_id = %operation_id,
                    error = %restore_error,
                    "Failed to restore archived cascade during staged-replay failure compensation"
                );
                compensation_errors.push(restore_error);
                0
            }
        };
        if restored != archived_count {
            compensation_errors.push(SinexError::service(format!(
                "restored only {restored}/{} archived cascade members; operator recovery is required",
                archived_count
            )));
        }

        if compensation_errors.is_empty() {
            Ok(())
        } else {
            let mut error = SinexError::service(format!(
                "Staged-source replay compensation was incomplete; operator recovery is required for operation {operation_id}"
            ));
            for compensation_error in compensation_errors {
                error = error.with_source(compensation_error);
            }
            Err(error)
        }
    }

    /// Restore the archived cascade for any failure after the archive
    /// transaction committed. This shared path prevents clean failures from
    /// stranding the same durable journal that crash recovery uses.
    async fn compensate_after_archive_failure(
        &self,
        pool: &sqlx::PgPool,
        archive_reason: &str,
        archived_count: u64,
        scoped_event_count: u64,
        operation_id: Uuid,
        error: SinexError,
    ) -> Result<u64> {
        let was_cancelled = matches!(&error, SinexError::Cancelled(_));
        let link_error = self
            .record_event_replacements(pool, operation_id, archive_reason)
            .await
            .err();

        let output_archive_error = self
            .archive_partial_replay_outputs(pool, operation_id)
            .await
            .err();
        let invalidation_error = self
            .publish_scope_invalidations(archive_reason, scoped_event_count, operation_id)
            .await
            .err();

        let restored = match self
            .restore_cascade(pool, archive_reason, archived_count, operation_id)
            .await
        {
            Ok(restored) => restored,
            Err(restore_error) => {
                return Err(SinexError::service(format!(
                "Replay failed after archive, published compensating invalidations, and restoring the archived cascade also failed: {restore_error}; operator recovery is required for operation {operation_id}"
            ))
            .with_source(error)
            .with_source(restore_error));
            }
        };
        if restored != archived_count {
            return Err(SinexError::service(format!(
                "Replay failed after archive; restored only {restored}/{} cascade members and operator recovery is required",
                archived_count
            ))
            .with_source(error));
        }

        if let Some(link_error) = link_error {
            return Err(SinexError::service(format!(
                "Replay failed after archive; restored archived cascade and published compensating scope invalidations, but linking partial replacements failed: {link_error}"
            ))
            .with_source(error)
            .with_source(link_error));
        }

        if let Some(invalidation_error) = invalidation_error {
            return Err(SinexError::service(format!(
                "Replay failed after archive; restored archived cascade, but compensating scope invalidation failed: {invalidation_error}"
            ))
            .with_source(error)
            .with_source(invalidation_error));
        }

        if let Some(output_archive_error) = output_archive_error {
            return Err(SinexError::service(format!(
                "Replay failed after archive; restored archived cascade and published compensating scope invalidations, but archiving partial replacement outputs failed: {output_archive_error}"
            ))
            .with_source(error)
            .with_source(output_archive_error));
        }

        if was_cancelled {
            return Err(error);
        }

        Err(SinexError::service(
            "Replay failed after archive; restored archived cascade and published compensating scope invalidations",
        )
        .with_source(error))
    }
}
