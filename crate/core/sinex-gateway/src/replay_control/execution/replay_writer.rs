//! Event-replacement recording and the replay scan/loop core for
//! `ReplayExecutionEngine`. See `execution/mod.rs` for the engine type.

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

    /// Record replacement relations between archived (old) events and newly-created events.
    ///
    /// After a successful replay scan, this queries for:
    /// - Old events: from `audit.archived_events` matching `cascade_ids`
    /// - New events: from `core.events` with `created_by_operation_id = operation_id`
    ///
    /// Matching strategy: events sharing the same `equivalence_key` are `Superseded`.
    /// Unmatched archived events are left without a replacement relation rather than
    /// fabricating a false old→new lineage edge.
    pub(crate) async fn record_event_replacements(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        cascade_ids: &[Uuid],
    ) -> Result<()> {
        use sinex_db::repositories::{ReplacementKind, ReplacementRecord};

        if cascade_ids.is_empty() {
            return Ok(());
        }

        // Query equivalence_key + scope_key for archived old events
        let old_rows = sqlx::query!(
            r#"SELECT id as "id!", scope_key, equivalence_key
             FROM audit.archived_events WHERE id = ANY($1::uuid[])"#,
            cascade_ids,
        )
        .fetch_all(pool)
        .await
        .map_err(|e| eyre!("Failed to query archived events for replacement matching: {e}"))?;

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

        // Build equivalence_key → new_event_ids index, preserving all outputs
        // with the same key (e.g. deterministic re-runs that produce two events
        // with the same equivalence_key must all be recorded, not collapsed).
        let mut eq_key_to_new: HashMap<String, Vec<Uuid>> = HashMap::new();
        for event in &new_events {
            if let Some(ref eq_key) = event.equivalence_key {
                eq_key_to_new
                    .entry(eq_key.clone())
                    .or_default()
                    .push(event.id);
            }
        }

        // Build replacement records
        let mut replacements = Vec::with_capacity(old_rows.len());
        let mut unmatched_count = 0usize;
        for row in &old_rows {
            let Some(new_event_ids) = row
                .equivalence_key
                .as_ref()
                .and_then(|eq| eq_key_to_new.get(eq))
            else {
                unmatched_count += 1;
                continue;
            };

            for &new_event_id in new_event_ids {
                replacements.push(ReplacementRecord {
                    old_event_id: row.id,
                    new_event_id,
                    relation_kind: ReplacementKind::Superseded,
                    scope_key: row.scope_key.clone(),
                    equivalence_key: row.equivalence_key.clone(),
                });
            }
        }

        if unmatched_count > 0 {
            warn!(
                operation_id = %operation_id,
                unmatched_count,
                old_count = old_rows.len(),
                new_count = new_events.len(),
                "Skipped replay replacement records without an equivalence-key match"
            );
        }

        if replacements.is_empty() {
            debug!(
                operation_id = %operation_id,
                old_count = old_rows.len(),
                new_count = new_events.len(),
                "No replay replacement matches found — skipping replacement recording"
            );
            return Ok(());
        }

        self.maybe_fail_replacement_recording()
            .wrap_err("Failed to record replay replacement relations")?;

        let count = pool
            .events()
            .record_replacements(operation_id, &replacements)
            .await
            .map_err(|e| eyre!("{e}"))
            .wrap_err("Failed to record replay replacement relations")?;

        info!(
            operation_id = %operation_id,
            replacement_count = count,
            old_events = old_rows.len(),
            new_events = new_events.len(),
            "Recorded event replacement relations"
        );

        Ok(())
    }

    /// Dispatch a replay by telling the ingestor node to re-scan source material.
    ///
    /// Instead of republishing stored event rows to NATS (reinjection), this:
    /// 1. Archives the affected cascade (existing events + derivatives)
    /// 2. Sends a `NodeScanCommand` to the running ingestor via NATS request-reply
    /// 3. Waits for the node to acknowledge and complete the scan
    /// 4. The node re-reads source material and emits fresh events through normal flow
    /// 5. Downstream automatons process the new events naturally via `JetStream`
    ///
    /// ## Transaction-boundary note (known-accepted race window)
    ///
    /// The cascade expansion (`derive_cascade_ids`) and the archive
    /// (`archive_cascade`) execute in **separate** database transactions.
    /// Between them, a newly-arriving event can reference an event that is
    /// about to be archived, creating a dangling `source_event_ids` reference.
    ///
    /// This window **cannot be closed** without a distributed-transaction
    /// protocol (2PC): steps after the archive publish invalidation signals
    /// and dispatch scan commands via NATS, which sit outside the database.
    /// Holding a DB transaction open across NATS request-reply would block
    /// the connection pool and risk indefinite locks on `core.events`.
    ///
    /// Mitigations that make this safe in practice:
    /// - `abort_before_scan_ack` restores the cascade and emits compensating
    ///   invalidations when the invalidation-publish or scan-command steps fail.
    /// - The cascade analyzer's integrity-violation check (`cascade_analyzer.rs`)
    ///   catches dangling references before the next replay of the same scope,
    ///   so the race is detectable and self-healing rather than silent.
    ///   so the window is narrow and the blast radius (one dangling reference
    ///   per replay) is negligible.
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
            return Err(eyre!(
                "Replay scope matched zero live events at execution time; preview is stale or the scoped rows were already replaced"
            ));
        }

        let mut root_ids: Vec<Uuid> = material_roots
            .iter()
            .filter_map(|event| event.id.map(|id| *id.as_uuid()))
            .collect();
        if root_ids.is_empty() {
            return Err(eyre!(
                "Replay scope material roots are missing persistent event ids"
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
        let cascade_ids = self
            .derive_cascade_ids(pool, operation_id, &root_ids)
            .await?;

        // Collect scope metadata before archiving (events move to audit after)
        let scope_metadata = self
            .collect_cascade_scope_metadata(pool, &cascade_ids)
            .await?;

        let archived_count = self
            .archive_cascade(pool, &cascade_ids, operation_id, executor_name)
            .await?;
        info!(
            operation_id = %operation_id,
            material_roots = material_roots.len(),
            archived_count,
            "Archived replay cascade, dispatching scan to node"
        );

        // TODO(#554): transactional outbox — archive_cascade commits to the DB above, but
        // publish_scope_invalidations below is a separate NATS operation with no transactional
        // coupling. If the process crashes between these two points, the archive is durable but
        // the scope invalidation signals are permanently lost, leaving derived nodes with stale
        // cached state until the next replay or manual reconciliation. A transactional outbox
        // (write invalidation rows inside the archive TX, publish-and-delete after commit with
        // retry on failure) would close this gap. For now, `abort_before_scan_ack` handles the
        // "process survives but NATS fails" case by restoring the cascade; crash recovery is not
        // covered.

        // Publish scope invalidation signals for archived derived events
        if !scope_metadata.is_empty()
            && let Err(invalidation_error) = self
                .publish_scope_invalidations(&scope_metadata, operation_id)
                .await
        {
            error!(
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
                        eyre!(
                            "Failed to publish replay scope invalidations before dispatch: {invalidation_error}"
                        ),
                    )
                    .await;
        }

        checkpoint.total_events = material_roots.len() as u64;

        // Step 2: Build and send the scan command to the ingestor node
        let scan_subject = self
            .env
            .nats_subject(&format!("sinex.control.nodes.{}.scan", scope.node_id));
        let progress_subject = self
            .env
            .nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

        let mut progress_sub = match self.nats_client.subscribe(progress_subject.clone()).await {
            Ok(subscription) => subscription,
            Err(error) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        eyre!("Failed to subscribe to replay progress: {error}"),
                    )
                    .await;
            }
        };

        // Build MaterialReplayContext so the node knows this is a replay scan
        let replay_context = MaterialReplayContext {
            operation_id,
            materials: replay_materials,
            replay_scope: NodeReplayScopeFilters {
                material_ids: normalized.material_ids,
                event_types: normalized.event_types,
            },
        };

        let scan_command = NodeScanCommand {
            operation_id,
            from: Checkpoint::None,
            until: TimeHorizon::Historical {
                end_time: execution_window.1,
            },
            args: ScanArgs {
                targets: vec![scope.node_id.clone()],
                dry_run: false,
                interactive: false,
                max_events: 0,
                skip_duplicates: true,
                config: HashMap::new(),
                replay: Some(replay_context),
            },
        };

        let command_payload = serde_json::to_vec(&scan_command)
            .map_err(|e| eyre!("Failed to serialize NodeScanCommand: {e}"))?;

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
                        eyre!("NATS request to {} failed: {error}", scan_subject),
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
                        eyre!(
                            "Timed out waiting for scan ack from node '{}' after {:?}. Is the node running?",
                            scope.node_id,
                            self.scan_ack_timeout
                        ),
                    )
                    .await;
            }
        };

        let ack: NodeScanAck = match serde_json::from_slice(&ack_msg.payload) {
            Ok(ack) => ack,
            Err(error) => {
                return self
                    .abort_before_scan_ack(
                        pool,
                        &cascade_ids,
                        &scope_metadata,
                        operation_id,
                        eyre!("Failed to deserialize NodeScanAck: {error}"),
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
                    eyre!(
                        "Node '{}' rejected scan command: {}",
                        ack.node_name,
                        ack.error.unwrap_or_else(|| "unknown reason".to_string())
                    ),
                )
                .await;
        }

        info!(
            operation_id = %operation_id,
            node = %ack.node_name,
            "Node accepted scan command, waiting for completion"
        );

        let replay = self.replay.clone();
        let mut events_processed: u64 = 0;
        let mut events_emitted: u64 = 0;

        struct ReplayScanFailure {
            error: color_eyre::eyre::Report,
            emitted_count: u64,
            restore_archived_cascade: bool,
        }

        let target_node_name = ack.node_name.clone();
        let completion = match tokio::time::timeout(self.scan_completion_timeout, async {
            loop {
                tokio::select! {
                    maybe_msg = progress_sub.next() => {
                        let Some(msg) = maybe_msg else {
                            return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                error: eyre!(
                                    "Replay progress stream closed before node '{}' reported completion",
                                    target_node_name
                                ),
                                emitted_count: events_emitted,
                                restore_archived_cascade: events_emitted == 0,
                            });
                        };

                        match serde_json::from_slice::<NodeScanProgress>(&msg.payload) {
                            Ok(progress) => {
                                events_processed = progress.events_processed;
                                events_emitted = progress.events_emitted;
                                if let Some(error) = progress.error {
                                    return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                        error: eyre!(
                                            "Node '{}' failed replay scan: {}",
                                            progress.node_name,
                                            error
                                        ),
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
                                        "Node scan completed"
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
                                    ))
                                    .into(),
                                    emitted_count: events_emitted,
                                    restore_archived_cascade: events_emitted == 0,
                                });
                            }
                            Ok(operation) => {
                                return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                    error: eyre!(
                                        "Replay operation {} left Executing state unexpectedly: {:?}",
                                        operation_id,
                                        operation.state
                                    ),
                                    emitted_count: events_emitted,
                                    restore_archived_cascade: false,
                                });
                            }
                            Err(error) => {
                                return Err::<u64, ReplayScanFailure>(ReplayScanFailure {
                                    error: eyre!(
                                        "Failed to reload replay operation {} while waiting for progress: {}",
                                        operation_id,
                                        error
                                    ),
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
                error: eyre!(
                    "Replay scan timed out waiting for node '{}' to report completion after {:?}",
                    target_node_name,
                    self.scan_completion_timeout
                ),
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
                    return Err(failure.error.wrap_err(format!(
                            "Replay scan failed before emitting replacement events, and restoring the archived cascade also failed: {restore_error}"
                        )));
                }
                // Publish compensating scope invalidations when either:
                // - we restored the cascade (so automata reconcile against restored events)
                // - events were emitted before failure (so automata reconcile the mixed state)
                if (failure.restore_archived_cascade || failure.emitted_count > 0)
                    && let Err(invalidation_error) = self
                        .publish_scope_invalidations(&scope_metadata, operation_id)
                        .await
                {
                    return Err(failure.error.wrap_err(format!(
                            "Replay scan failed and compensating scope invalidation also failed: {invalidation_error}"
                        )));
                }
                Err(failure.error).wrap_err(if failure.restore_archived_cascade {
                    "Replay scan failed before emitting replacement events; restored archived cascade and published compensating scope invalidations"
                } else if failure.emitted_count > 0 {
                    "Replay scan failed after partial event emission; published compensating scope invalidations for automata reconciliation"
                } else {
                    "Replay scan failed before emitting any replacement events; archived cascade left untouched"
                })
            }
        }
    }
}
