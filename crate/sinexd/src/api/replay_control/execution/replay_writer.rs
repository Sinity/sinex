//! Event-replacement recording and the replay scan/loop core for
//! `ReplayExecutionEngine`. See `execution/mod.rs` for the engine type.

use super::{
    ExtendedMaterialOccurrenceKey, OperationOutputEvent, ReplayExecutionEngine,
    ScopeInvalidationBucket, StreamExt, replay_scope_drift_error,
    stale_preview_missing_root_ids_error,
};
use crate::runtime::stream::{
    Checkpoint, MaterialReplayContext, ReplayScopeFilters as SourceReplayScopeFilters, ScanArgs,
    SourceScanAck, SourceScanCommand, SourceScanProgress, TimeHorizon,
};
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::ControlSubject;
use sinex_primitives::events::{Provenance, ScopeKey};
use sinex_primitives::{Result, SinexError, Timestamp, Uuid};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use sinex_db::replay::state_machine::{ReplayCheckpoint, ReplayScope, ReplayState};

fn material_occurrence_key(event: &OperationOutputEvent) -> Option<ExtendedMaterialOccurrenceKey> {
    Some(ExtendedMaterialOccurrenceKey {
        source_material_id: event.source_material_id?,
        anchor_byte: event.anchor_byte?,
        offset_start: event.offset_start,
        offset_end: event.offset_end,
        offset_kind: event.offset_kind.clone(),
    })
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
    /// Record replacement relations between archived material events and newly-created events.
    ///
    /// After a successful replay scan, this queries for:
    /// - Old events: from `audit.archived_events` matching `cascade_ids`
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
        cascade_ids: &[Uuid],
    ) -> Result<()> {
        use sinex_db::repositories::ReplacementRecord;

        if cascade_ids.is_empty() {
            return Ok(());
        }

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
             WHERE id = ANY($1::uuid[])
               AND source_material_id IS NOT NULL
               AND anchor_byte IS NOT NULL"#,
            cascade_ids,
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
                old_count = cascade_ids.len(),
                new_count = new_events.len(),
                "Skipped or mismatched replay replacement records detected"
            );
        }

        if replacements.is_empty() {
            debug!(
                operation_id = %operation_id,
                old_count = cascade_ids.len(),
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
            old_events = cascade_ids.len(),
            new_events = new_events.len(),
            "Recorded event replacement relations"
        );

        Ok(())
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
    pub(crate) async fn replay_events(
        &self,
        operation_id: Uuid,
        scope: &ReplayScope,
        execution_window: (Timestamp, Timestamp),
        expected_total_events: u64,
        preview_root_ids: &[Uuid],
        pool: &sqlx::PgPool,
        checkpoint: &mut ReplayCheckpoint,
        executor_name: &str,
    ) -> Result<u64> {
        let material_roots = self
            .collect_scope_events(scope, execution_window, pool)
            .await?;
        if material_roots.is_empty() {
            return Err(SinexError::invalid_state(
                "Replay scope matched zero live events at execution time; preview is stale or the scoped rows were already replaced",
            ));
        }

        let mut root_ids: Vec<Uuid> = material_roots
            .iter()
            .filter_map(|event| event.id.map(|id| *id.as_uuid()))
            .collect();
        if root_ids.is_empty() {
            return Err(SinexError::invalid_state(
                "Replay scope material roots are missing persistent event ids",
            ));
        }
        root_ids.sort_unstable();
        root_ids.dedup();

        if preview_root_ids.is_empty() {
            // Stale preview: root_event_ids not available. Require a fresh preview
            // to enable ID-level staleness detection.
            return Err(stale_preview_missing_root_ids_error(
                operation_id,
                expected_total_events,
            ));
        }
        if root_ids.as_slice() != preview_root_ids {
            return Err(replay_scope_drift_error(
                operation_id,
                expected_total_events,
                preview_root_ids,
                &root_ids,
            ));
        }

        let normalized = scope.normalized_filters();
        let material_ids: Vec<Uuid> = material_roots
            .iter()
            .filter_map(|event| match &event.provenance {
                Provenance::Material { id, .. } => Some(*id.as_uuid()),
                _ => None,
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let replay_materials = self.resolve_replay_materials(pool, &material_ids).await?;
        let expected_replay_outputs = Self::with_logical_source_identifiers(
            Self::expected_replay_outputs(&material_roots)?,
            &replay_materials,
        )?;

        // Step 1: Archive the affected cascade
        let archived_cascade = self
            .archive_replay_cascade_atomically(pool, operation_id, &root_ids, executor_name)
            .await?;
        let cascade_ids = archived_cascade.cascade_ids;
        let scope_metadata = archived_cascade.scope_metadata;
        let archived_count = archived_cascade.archived_count;
        info!(
            operation_id = %operation_id,
            material_roots = material_roots.len(),
            archived_count,
            "Archived replay cascade, dispatching scan to source"
        );

        // The archive transaction records scope invalidations as pending in
        // operation metadata before committing archived rows. NATS publication
        // remains outside the DB transaction; a crash in that boundary leaves a
        // durable recovery marker that ops/debt views can surface instead of a
        // silent stale-projection gap.

        // Publish scope invalidation signals for archived derived events
        if !scope_metadata.is_empty()
            && let Err(invalidation_error) = self
                .publish_scope_invalidations(&scope_metadata, operation_id)
                .await
        {
            error!(
                target: "sinex_metrics",
                metric = "gateway.replay_invalidation_failures_total",
                operation_id = %operation_id,
                archived_count,
                scope_buckets = scope_metadata.len(),
                "Replay scope invalidation publish failed after archive commit; restoring cascade: {invalidation_error}"
            );
            return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        SinexError::nats_publish(format!(
                            "Failed to publish replay scope invalidations before dispatch: {invalidation_error}"
                        ))
                        .with_source(invalidation_error),
                    )
                    .await;
        }
        if !scope_metadata.is_empty()
            && let Err(record_error) = self
                .replay
                .record_scope_invalidations_published(operation_id)
                .await
        {
            warn!(
                operation_id = %operation_id,
                archived_count,
                scope_buckets = scope_metadata.len(),
                error = %record_error,
                "Published replay scope invalidations but failed to clear the durable pending marker; recovery/debt views will continue reporting it"
            );
        }

        checkpoint.total_events = material_roots.len() as u64;

        // Step 2: Route staged-source scopes through source, not live source scan.
        // RuntimeModule scan publishes a SourceScanCommand to sinex.control.sources.{source}.scan;
        // staged-source replay creates a source_run and dispatches to the source
        // host (#1081) via a parse command. The routing decision is made here so both
        // paths share the archive + invalidation + checkpoint machinery above.
        if scope.is_staged_source_scope() {
            return self
                .dispatch_staged_source_replay(
                    operation_id,
                    scope,
                    pool,
                    &cascade_ids,
                    &scope_metadata,
                    executor_name,
                )
                .await;
        }

        // Step 2: Build and send the scan command to the source runtime
        let scan_subject = self
            .env
            .nats_subject(&ControlSubject::source_scan(&scope.source_name));
        let progress_subject = self
            .env
            .nats_subject(&ControlSubject::replay_progress(operation_id));

        let mut progress_sub = match self.nats_client.subscribe(progress_subject.clone()).await {
            Ok(subscription) => subscription,
            Err(error) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        SinexError::nats_subscribe("Failed to subscribe to replay progress")
                            .with_std_error(&error),
                    )
                    .await;
            }
        };

        // Build MaterialReplayContext so the source knows this is a replay scan
        let replay_context = MaterialReplayContext {
            operation_id,
            materials: replay_materials,
            replay_scope: SourceReplayScopeFilters {
                material_ids: normalized.material_ids,
                event_types: normalized.event_types,
            },
        };

        let scan_command = SourceScanCommand {
            operation_id,
            from: Checkpoint::None,
            until: TimeHorizon::Historical {
                end_time: execution_window.1,
            },
            args: ScanArgs {
                targets: vec![scope.source_name.clone()],
                dry_run: false,
                interactive: false,
                max_events: 0,
                skip_duplicates: true,
                config: HashMap::new(),
                replay: Some(replay_context),
            },
        };

        let command_payload = serde_json::to_vec(&scan_command).map_err(|err| {
            SinexError::serialization("Failed to serialize SourceScanCommand").with_std_error(&err)
        })?;

        // Step 3: Send via NATS request-reply and wait for acknowledgement
        let ack_msg = match tokio::time::timeout(
            self.scan_ack_timeout,
            self.nats_client
                .request(scan_subject.clone(), command_payload.into()),
        )
        .await
        {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        SinexError::nats(format!("NATS request to {scan_subject} failed"))
                            .with_std_error(&error),
                    )
                    .await;
            }
            Err(_) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        SinexError::timeout(format!(
                            "Timed out waiting for scan ack from source '{}' after {:?}. Is the source running?",
                            scope.source_name,
                            self.scan_ack_timeout
                        )),
                    )
                    .await;
            }
        };

        let ack: SourceScanAck = match serde_json::from_slice(&ack_msg.payload) {
            Ok(ack) => ack,
            Err(error) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        SinexError::serialization("Failed to deserialize SourceScanAck")
                            .with_std_error(&error),
                    )
                    .await;
            }
        };

        if !ack.accepted {
            return self
                .abort_before_scan_ack(
                    pool,
                    &cascade_ids,
                    &scope_metadata,
                    operation_id,
                    SinexError::invalid_state(format!(
                        "RuntimeModule '{}' rejected scan command: {}",
                        ack.module_name,
                        ack.error.unwrap_or_else(|| "unknown reason".to_string())
                    )),
                )
                .await;
        }

        info!(
            operation_id = %operation_id,
            source = %ack.module_name,
            "RuntimeModule accepted scan command, waiting for completion"
        );

        let replay = self.replay.clone();
        let mut events_processed: u64 = 0;
        let mut events_emitted: u64 = 0;

        struct ReplayScanFailure {
            error: SinexError,
            emitted_count: u64,
            restore_archived_cascade: bool,
        }

        let target_source_name = ack.module_name.clone();
        let completion = match tokio::time::timeout(self.scan_completion_timeout, async {
            loop {
                tokio::select! {
                    maybe_msg = progress_sub.next() => {
                        let Some(msg) = maybe_msg else {
                            return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                error: SinexError::nats(format!(
                                    "Replay progress stream closed before source '{target_source_name}' reported completion"
                                )),
                                emitted_count: events_emitted,
                                restore_archived_cascade: events_emitted == 0,
                            });
                        };

                        match serde_json::from_slice::<SourceScanProgress>(&msg.payload) {
                            Ok(progress) => {
                                events_processed = progress.events_processed;
                                events_emitted = progress.events_emitted;
                                if let Some(error) = progress.error {
                                    return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                        error: SinexError::processing(format!(
                                            "RuntimeModule '{}' failed replay scan: {}",
                                            progress.module_name,
                                            error
                                        )),
                                        emitted_count: progress.events_emitted,
                                        restore_archived_cascade: progress.events_emitted == 0,
                                    });
                                }

                                debug!(
                                    operation_id = %operation_id,
                                    events_processed = progress.events_processed,
                                    events_emitted = progress.events_emitted,
                                    "Replay progress update"
                                );

                                // Update checkpoint with progress
                                checkpoint.processed_events = progress.events_processed;
                                checkpoint.updated_at = sinex_primitives::temporal::now();
                                if let Err(checkpoint_error) = self
                                    .persist_replay_checkpoint(
                                        operation_id,
                                        checkpoint,
                                        "Failed to persist replay progress checkpoint",
                                    )
                                    .await
                                {
                                    return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                        error: checkpoint_error,
                                        emitted_count: progress.events_emitted,
                                        restore_archived_cascade: progress.events_emitted == 0,
                                    });
                                }

                                // If final_report is present, the scan is complete
                                if let Some(report) = &progress.final_report {
                                    info!(
                                        operation_id = %operation_id,
                                        events_processed = report.events_processed,
                                        "RuntimeModule scan completed"
                                    );
                                    return Ok::<u64, ReplayScanFailure>(report.events_processed);
                                }
                            }
                            Err(err) => {
                                warn!(error = %err, "Failed to parse replay progress message");
                            }
                        }
                    }
                    () = tokio::time::sleep(Self::EXECUTION_STATE_POLL_INTERVAL) => {
                        match replay.load_operation(operation_id).await {
                            Ok(operation) if operation.state == ReplayState::Executing => {}
                            Ok(operation)
                                if matches!(
                                    operation.state,
                                    ReplayState::Cancelling | ReplayState::Cancelled
                                ) =>
                            {
                                return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                    error: SinexError::cancelled(format!(
                                        "Replay operation {operation_id} was cancelled during execution"
                                    )),
                                    emitted_count: events_emitted,
                                    restore_archived_cascade: events_emitted == 0,
                                });
                            }
                            Ok(operation) => {
                                return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                    error: SinexError::invalid_state(format!(
                                        "Replay operation {} left Executing state unexpectedly: {:?}",
                                        operation_id,
                                        operation.state
                                    )),
                                    emitted_count: events_emitted,
                                    restore_archived_cascade: false,
                                });
                            }
                            Err(error) => {
                                return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                    error: SinexError::service(format!(
                                        "Failed to reload replay operation {operation_id} while waiting for progress: {error}"
                                    ))
                                    .with_source(error),
                                    emitted_count: events_emitted,
                                    restore_archived_cascade: false,
                                });
                            }
                        }
                    }
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_timeout) => Err(ReplayScanFailure {
                error: SinexError::timeout(format!(
                    "Replay scan timed out waiting for source '{}' to report completion after {:?}",
                    target_source_name,
                    self.scan_completion_timeout
                )),
                emitted_count: events_emitted,
                restore_archived_cascade: false,
            }),
        };

        match completion {
            Ok(count) => {
                checkpoint.processed_events = count;
                checkpoint.updated_at = sinex_primitives::temporal::now();

                self.wait_for_replay_outputs_visible(pool, operation_id, &expected_replay_outputs)
                    .await?;

                // Record replacement relations between archived and newly-created events
                self.record_event_replacements(pool, operation_id, &cascade_ids)
                    .await?;

                Ok(count)
            }
            Err(failure) => {
                let cancelled = matches!(failure.error, SinexError::Cancelled(_));
                warn!(
                    operation_id = %operation_id,
                    error = %failure.error,
                    events_emitted = failure.emitted_count,
                    restore_archived_cascade = failure.restore_archived_cascade,
                    "Replay scan failed"
                );
                if failure.restore_archived_cascade
                    && let Err(restore_error) =
                        self.restore_cascade(pool, &cascade_ids, operation_id).await
                {
                    return Err(SinexError::service(format!(
                            "Replay scan failed before emitting replacement events, and restoring the archived cascade also failed: {restore_error}"
                        ))
                    .with_source(failure.error)
                    .with_source(restore_error));
                }
                // Publish compensating scope invalidations when either:
                // - we restored the cascade (so automata reconcile against restored events)
                // - events were emitted before failure (so automata reconcile the mixed state)
                if (failure.restore_archived_cascade || failure.emitted_count > 0)
                    && let Err(invalidation_error) = self
                        .publish_scope_invalidations(&scope_metadata, operation_id)
                        .await
                {
                    return Err(SinexError::service(format!(
                            "Replay scan failed and compensating scope invalidation also failed: {invalidation_error}"
                        ))
                    .with_source(failure.error)
                    .with_source(invalidation_error));
                }
                if cancelled {
                    return Err(failure.error);
                }
                let message = if failure.restore_archived_cascade {
                    "Replay scan failed before emitting replacement events; restored archived cascade and published compensating scope invalidations"
                } else if failure.emitted_count > 0 {
                    "Replay scan failed after partial event emission; published compensating scope invalidations for automata reconciliation"
                } else {
                    "Replay scan failed before emitting any replacement events; archived cascade left untouched"
                };
                Err(SinexError::service(message).with_source(failure.error))
            }
        }
    }

    /// Dispatches a staged-source replay through the source host.
    ///
    /// Publishes a parse command to the source NATS control subject
    /// and polls for operation completion. The source is responsible for
    /// invoking the parser capability and publishing admitted event intents.
    async fn dispatch_staged_source_replay(
        &self,
        operation_id: Uuid,
        scope: &ReplayScope,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
        scope_metadata: &[ScopeInvalidationBucket],
        executor_name: &str,
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

        let command_payload = serde_json::to_vec(&parse_command).map_err(|err| {
            SinexError::serialization("Failed to serialize source parse command")
                .with_std_error(&err)
        })?;

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
                        cascade_ids,
                        scope_metadata,
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
                        cascade_ids,
                        scope_metadata,
                        operation_id,
                        SinexError::timeout(format!(
                            "Timed out waiting for source parse ack from '{source_id}' after {:?}",
                            self.scan_ack_timeout
                        )),
                    )
                    .await;
            }
        };

        let ack: serde_json::Value = serde_json::from_slice(&ack_msg.payload).map_err(|err| {
            SinexError::serialization("Failed to deserialize source parse ack").with_std_error(&err)
        })?;

        if ack.get("accepted").and_then(serde_json::Value::as_bool) != Some(true) {
            let error_msg = ack
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown reason");
            return self
                .abort_before_scan_ack(
                    pool,
                    cascade_ids,
                    scope_metadata,
                    operation_id,
                    SinexError::invalid_state(format!(
                        "Source '{source_id}' rejected parse command: {error_msg}"
                    )),
                )
                .await;
        }

        info!(
            operation_id = %operation_id,
            source_id = source_id,
            "Source accepted parse command, waiting for completion"
        );

        // Poll replay operation state for terminal status. The source
        // processes the parse, publishes event intents through NATS → event_engine,
        // and the operation state machine transitions to Completed/Failed/Cancelled.
        let replay = self.replay.clone();
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;

            let operation = replay.load_operation(operation_id).await.map_err(|err| {
                SinexError::service(format!(
                    "Failed to load replay operation {operation_id} during source parse"
                ))
                .with_source(err)
            })?;

            match operation.state {
                ReplayState::Completed => {
                    info!(
                        operation_id = %operation_id,
                        "Staged-source replay completed"
                    );
                    let count = 0u64; // output_event_count removed in #1160
                    return Ok(count);
                }
                ReplayState::Failed => {
                    return Err(SinexError::processing(format!(
                        "Staged-source replay failed for operation {operation_id}: {}",
                        operation
                            .error_details
                            .unwrap_or_else(|| "unknown error".to_string())
                    )));
                }
                ReplayState::Cancelled => {
                    return Err(SinexError::cancelled(format!(
                        "Staged-source replay cancelled for operation {operation_id}",
                    )));
                }
                _ => {
                    debug!(
                        operation_id = %operation_id,
                        state = ?operation.state,
                        "Waiting for staged-source replay to complete"
                    );
                }
            }
        }
    }
}
