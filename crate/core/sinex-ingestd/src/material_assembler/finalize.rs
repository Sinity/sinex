//! Material finalization methods for `MaterialAssembler`.
//!
//! This module contains finalization orchestration, error routing, and cleanup
//! logic that executes when a material assembly completes or fails. The durable
//! source-material/blob/ledger commit boundary lives in `finalization_transaction`.

use serde::Serialize;
use sinex_db::repositories::{DbPoolExt, TemporalLedgerEntry, material_status};
use sinex_primitives::Timestamp;
use sinex_primitives::nats::{NatsTrafficClass, insert_traffic_class_header};
use sinex_primitives::transport;
use sinex_primitives::{Id, JsonValue, Uuid};
use sinex_schema::schema::records::SourceMaterialRecord;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::{IngestdResult, SinexError};

use super::assembly_state_machine::{
    AssemblyInput, AssemblyLogicalState, AssemblyStateMachine, AssemblyTransition,
};
use super::finalization_transaction::{FinalizationRequest, FinalizationTransaction};
use super::state::AssemblyPhase;
use super::{MaterialAssembler, MaterialEndMessage};
use std::{str::FromStr, sync::Arc};

#[derive(Clone, Copy)]
pub(super) enum PendingEndBehavior {
    #[cfg(test)]
    Error,
    Ignore,
}

fn final_material_status(metadata: &JsonValue) -> &'static str {
    metadata
        .as_object()
        .and_then(|map| map.get("cancelled"))
        .and_then(JsonValue::as_bool)
        .map_or(material_status::COMPLETED, |cancelled| {
            if cancelled {
                material_status::CANCELLED
            } else {
                material_status::COMPLETED
            }
        })
}

/// DLQ payload for material failures
#[derive(Debug, Serialize)]
struct MaterialDlqPayload {
    material_id: String,
    error: String,
    context: JsonValue,
    failed_at: Timestamp,
}

#[derive(Clone, Copy)]
enum FailureCleanupClaim {
    Claimed { resume_phase: AssemblyPhase },
    Skipped,
}

impl MaterialAssembler {
    fn is_duplicate_temporal_ledger_entry(error: &SinexError) -> bool {
        const TEMPORAL_LEDGER_UNIQUE_CONSTRAINT: &str =
            "uk_temporal_ledger_material_offset_source_type";

        matches!(error, SinexError::AlreadyExists(_))
            && error
                .context_map()
                .get("constraint")
                .is_some_and(|value| value == TEMPORAL_LEDGER_UNIQUE_CONSTRAINT)
    }

    async fn begin_failure_cleanup(&self, material_id: Uuid, reason: &str) -> FailureCleanupClaim {
        if let Some(state_handle) = self.get_state_handle(&material_id) {
            let mut state = state_handle.lock().await;
            if state.phase == AssemblyPhase::Finalizing {
                debug!(
                    material_id = %material_id,
                    failure_reason = reason,
                    "Skipping failed-material cleanup because terminal transition is already in progress"
                );
                return FailureCleanupClaim::Skipped;
            }
            let resume_phase = state.phase;
            state.phase = AssemblyPhase::Finalizing;
            return FailureCleanupClaim::Claimed { resume_phase };
        }

        match self.material_is_terminal(material_id).await {
            Ok(true) => {
                debug!(
                    material_id = %material_id,
                    failure_reason = reason,
                    "Skipping failed-material cleanup because material is already terminal"
                );
                FailureCleanupClaim::Skipped
            }
            Ok(false) => FailureCleanupClaim::Claimed {
                resume_phase: AssemblyPhase::Accumulating,
            },
            Err(error) => {
                warn!(
                    material_id = %material_id,
                    failure_reason = reason,
                    error = %error,
                    "Failed to confirm material terminal state before failure cleanup; proceeding"
                );
                FailureCleanupClaim::Claimed {
                    resume_phase: AssemblyPhase::Accumulating,
                }
            }
        }
    }

    async fn revert_failure_cleanup_start(&self, material_id: Uuid, resume_phase: AssemblyPhase) {
        if let Some(state_handle) = self.get_state_handle(&material_id) {
            let mut state = state_handle.lock().await;
            if state.phase == AssemblyPhase::Finalizing {
                state.phase = resume_phase;
            }
        }
    }

    /// Revert a finalization attempt back to the Accumulating phase.
    ///
    /// Called when a step inside `try_finalize_pending_end` fails after the phase was
    /// set to `Finalizing`. The WAL already holds the End message, so only in-memory
    /// state needs to be restored so the next delivery attempt can retry.
    async fn revert_finalization_start(
        state_handle: &Arc<Mutex<super::state::AssemblerState>>,
        end: MaterialEndMessage,
    ) {
        let mut state = state_handle.lock().await;
        state.phase = AssemblyPhase::Accumulating;
        state.pending_end = Some(end);
        // WAL is immutable — End message remains. In-memory state reverted.
    }

    /// Write an early `staged_at` ledger entry at material-begin time.
    ///
    /// This ensures `LedgerReader::derive_ts_orig()` can always resolve a
    /// persisted timestamp for events derived from this material, even before
    /// finalization. The `offset_end` is set to `i64::MAX` to cover all offsets;
    /// the precise `realtime_capture` entry written at finalization takes
    /// precedence when both exist in the ledger.
    pub(super) async fn record_staged_at_ledger_entry(
        &self,
        material_id: sinex_primitives::Uuid,
        started_at: Timestamp,
    ) -> IngestdResult<()> {
        let entry = TemporalLedgerEntry::staged_at(material_id, i64::MAX, started_at);

        match self
            .pool
            .source_materials()
            .append_temporal_ledger(entry)
            .await
        {
            Ok(()) => {
                debug!(material_id = %material_id, "Wrote staged_at ledger entry at begin time");
                Ok(())
            }
            Err(error) if Self::is_duplicate_temporal_ledger_entry(&error) => {
                debug!(
                    material_id = %material_id,
                    "Reused existing staged_at ledger entry at begin time"
                );
                Ok(())
            }
            Err(e) => Err({
                SinexError::database("Failed to append staged_at temporal ledger entry")
                    .with_source(e)
            }),
        }
    }

    /// Route material failure to DLQ
    pub(super) async fn route_material_error(
        &self,
        material_id: Uuid,
        error: impl Into<String>,
        context: JsonValue,
    ) {
        let payload = MaterialDlqPayload {
            material_id: material_id.to_string(),
            error: error.into(),
            context,
            failed_at: Timestamp::now(),
        };

        match serde_json::to_vec(&payload) {
            Ok(bytes) => {
                let mut headers = async_nats::HeaderMap::new();
                insert_traffic_class_header(&mut headers, NatsTrafficClass::RawIngestDlq);
                transport::insert_semantic_transport_class_header(
                    &mut headers,
                    transport::Class::SourceMaterial,
                );

                if let Err(e) = self
                    .nats_client
                    .publish_with_headers(self.dlq_subject.clone(), headers, bytes.into())
                    .await
                {
                    error!(
                        material_id = %material_id,
                        "Failed to publish material DLQ entry: {}",
                        e
                    );
                } else {
                    debug!(material_id = %material_id, "Routed to DLQ");
                }
            }
            Err(e) => {
                error!(
                    material_id = %material_id,
                    "Failed to encode DLQ payload: {}",
                    e
                );
            }
        }
    }

    /// Mark material as failed in the database to prevent reprocessing.
    async fn mark_material_failed_checked(
        &self,
        material_id: Uuid,
        reason: &str,
    ) -> IngestdResult<()> {
        let id: Id<SourceMaterialRecord> = Id::from_uuid(material_id);
        self.pool
            .source_materials()
            .mark_as_failed(id, reason)
            .await
            .map_err(|error| {
                SinexError::database("Failed to mark material as failed in database")
                    .with_context("material_id", material_id.to_string())
                    .with_context("failure_reason", reason)
                    .with_source(error)
            })
    }

    /// Finalize a failed material: mark as failed, clean up state, and remove from active map
    pub(super) async fn finalize_failed_material(&self, material_id: Uuid, reason: &str) {
        let FailureCleanupClaim::Claimed { resume_phase } =
            self.begin_failure_cleanup(material_id, reason).await
        else {
            return;
        };

        if let Err(error) = self
            .finalize_failed_material_claimed_checked(material_id, reason, resume_phase)
            .await
        {
            warn!(
                material_id = %material_id,
                failure_reason = reason,
                error = %error,
                "Failed-material cleanup could not durably land; preserving retry state"
            );
        }
    }

    pub(super) async fn finalize_failed_material_claimed_checked(
        &self,
        material_id: Uuid,
        reason: &str,
        resume_phase: AssemblyPhase,
    ) -> IngestdResult<()> {
        debug!(
            material_id = %material_id,
            failure_reason = reason,
            "Finalizing failed material after terminal ownership was claimed"
        );

        self.stats_inc_failed(); // Track failed assembly
        tracing::warn!(
            target: "sinex_metrics",
            metric = "assembly_failure",
            material_id = %material_id,
            failure_reason = reason,
        );

        if let Err(error) = self.mark_material_failed_checked(material_id, reason).await {
            self.revert_failure_cleanup_start(material_id, resume_phase)
                .await;
            return Err(error);
        }

        self.cleanup_state(material_id).await;
        let _ = self.assembler_state.remove(&material_id);
        Ok(())
    }

    async fn route_terminal_failure_with_retry(
        &self,
        material_id: Uuid,
        reason: &'static str,
        context: JsonValue,
        state_handle: &Arc<Mutex<super::state::AssemblerState>>,
        end: MaterialEndMessage,
    ) -> IngestdResult<()> {
        self.route_material_error(material_id, reason, context)
            .await;
        if let Err(error) = self
            .finalize_failed_material_claimed_checked(
                material_id,
                reason,
                AssemblyPhase::Accumulating,
            )
            .await
        {
            Self::revert_finalization_start(state_handle, end).await;
            return Err(error);
        }
        Ok(())
    }

    pub(super) async fn try_finalize_pending_end(
        &self,
        material_id: Uuid,
        state_handle: Arc<Mutex<super::state::AssemblerState>>,
        pending_behavior: PendingEndBehavior,
    ) -> IngestdResult<()> {
        use super::state::{build_finalize_metadata, parse_material_ended_at};

        let (final_state, assembled_bytes, slice_count, computed_hash, end, ended_at) = {
            let mut state = state_handle.lock().await;
            if state.phase == AssemblyPhase::Finalizing {
                debug!(material_id = %material_id, "Ignoring end message while finalizing");
                return Ok(());
            }

            let Some(end_preview) = state.pending_end.clone() else {
                return Ok(());
            };

            if state.phase == AssemblyPhase::PendingBegin {
                debug!(
                    material_id = %material_id,
                    "End recorded before begin; waiting for begin metadata"
                );
                return Ok(());
            }

            let ended_at = match parse_material_ended_at(
                material_id,
                &end_preview.ended_at,
                "pending_end",
            ) {
                Ok(ended_at) => ended_at,
                Err(error) => {
                    let context = serde_json::json!({
                        "ended_at": end_preview.ended_at,
                        "expected_bytes": end_preview.total_size_bytes,
                        "expected_slices": end_preview.total_slices,
                        "assembled_bytes": state.expected_offset,
                        "slice_count": state.slice_count,
                        "buffered_offsets": state.buffered_slices.keys().copied().collect::<Vec<_>>(),
                        "error": error.to_string(),
                    });
                    let resume_phase = state.phase;
                    state.phase = AssemblyPhase::Finalizing;
                    drop(state);
                    self.route_material_error(
                        material_id,
                        "material_end_timestamp_invalid",
                        context,
                    )
                    .await;
                    self.finalize_failed_material_claimed_checked(
                        material_id,
                        "material_end_timestamp_invalid",
                        resume_phase,
                    )
                    .await?;
                    return Ok(());
                }
            };

            let view = state.finalization_view();
            let assembled_bytes = view.expected_offset;
            let slice_count = view.slice_count;

            // Not complete yet: keep the end in state and ask JetStream to redeliver later.
            let expected_slices = end_preview.total_slices;
            let expected_bytes = end_preview.total_size_bytes;
            let seen_slices = view.slice_count.saturating_add(view.buffered_count);

            // If the end metadata makes the current buffered state impossible to finalize, treat
            // it as corruption and route to DLQ instead of NAK-looping forever.
            //
            // Example: a slice arrives with an offset beyond the claimed total byte size, or we
            // have already seen as many slices as the end claims exist but still can't assemble.
            let has_invalid_offsets = state
                .buffered_slices
                .keys()
                .any(|off| *off < 0 || *off >= expected_bytes);

            if expected_bytes < 0
                || view.expected_offset > expected_bytes
                || has_invalid_offsets
                || (seen_slices >= expected_slices && view.expected_offset != expected_bytes)
            {
                let reason = if expected_bytes < 0 {
                    format!("invalid end.total_size_bytes={expected_bytes}")
                } else if view.expected_offset > expected_bytes {
                    format!(
                        "assembled_bytes={} exceeds expected_bytes={}",
                        view.expected_offset, expected_bytes
                    )
                } else if has_invalid_offsets {
                    format!(
                        "buffered slice offsets outside expected_bytes={expected_bytes} (buffered_offsets={:?})",
                        state.buffered_slices.keys().copied().collect::<Vec<_>>()
                    )
                } else {
                    format!(
                        "cannot assemble full material: assembled_bytes={} expected_bytes={} slice_count={} buffered_count={} expected_slices={}",
                        view.expected_offset,
                        expected_bytes,
                        view.slice_count,
                        view.buffered_count,
                        expected_slices
                    )
                };

                let ctx = serde_json::json!({
                    "reason": reason,
                    "assembled_bytes": view.expected_offset,
                    "slice_count": view.slice_count,
                    "buffered_offsets": state.buffered_slices.keys().copied().collect::<Vec<_>>(),
                    "expected_bytes": expected_bytes,
                    "expected_slices": expected_slices,
                    "end": {
                        "ended_at": end_preview.ended_at,
                        "content_hash": end_preview.content_hash,
                    }
                });

                let resume_phase = state.phase;
                state.phase = AssemblyPhase::Finalizing;
                drop(state);
                self.route_material_error(
                    material_id,
                    "material assembly corruption detected",
                    ctx,
                )
                .await;
                self.finalize_failed_material_claimed_checked(
                    material_id,
                    "material assembly corruption detected",
                    resume_phase,
                )
                .await?;
                return Ok(());
            }

            if view.buffered_count > 0
                || view.expected_offset < expected_bytes
                || view.slice_count < expected_slices
            {
                if matches!(pending_behavior, PendingEndBehavior::Ignore) {
                    return Ok(());
                }
                return Err(SinexError::service(format!(
                    "end received before all slices were processed for {material_id}: assembled_bytes={} slice_count={} buffered={} expected_bytes={} expected_slices={}",
                    view.expected_offset,
                    view.slice_count,
                    view.buffered_count,
                    expected_bytes,
                    expected_slices
                ))
                .with_context(
                    super::redelivery_decision::REDELIVERY_ERROR_KIND_CONTEXT,
                    super::redelivery_decision::redelivery_error_class::ORDERING_INCOMPLETE,
                ));
            }

            // Complete: transition into finalization while holding the per-material lock so
            // no more slice writes can mutate the state we are about to snapshot.
            let transition = AssemblyStateMachine::transition_for_state(
                &state,
                AssemblyInput::StartFinalization,
            )
            .map_err(|error| error.into_sinex_error(material_id))?;
            debug!(
                material_id = %material_id,
                transition = ?transition,
                "Assembly state machine accepted finalization start"
            );
            state.phase = AssemblyPhase::Finalizing;
            let end = state.pending_end.take().ok_or_else(|| {
                SinexError::service(format!(
                    "State corruption: pending_end missing during finalization for material {material_id}"
                ))
            })?;

            if let Err(e) =
                super::io::sync_staged_file_for_finalization(self, &mut state, material_id).await
            {
                warn!(
                    material_id = %material_id,
                    "Failed to sync temp file during finalization: {}",
                    e
                );
            }
            drop(state.temp_file.take());

            let computed_hash = state.hasher.clone().finalize().to_hex().to_string();
            // WAL keeps the End message, so we don't need to persist implicit state changes here.
            // Unique-session crash recovery handles re-finalization.

            (
                view,
                assembled_bytes,
                slice_count,
                computed_hash,
                end,
                ended_at,
            )
        };

        // Finalization below is intentionally lock-free with respect to `state_handle`.
        // The lock only guarded the handoff into a stable `FinalizationState`; content-store
        // import, blob registration, and source-material updates must not run while holding it.
        debug!(
            material_id = %material_id,
            assembled_bytes,
            slice_count,
            reported_total = end.total_size_bytes,
            temp_path = %final_state.temp_path.display(),
            "Processing end message"
        );

        // If the payload claims zero bytes, avoid content-store/blob work and treat this as an
        // empty material. Persist a DLQ entry so publishers can diagnose.
        if end.total_size_bytes == 0 {
            warn!(
                material_id = %material_id,
                slices = slice_count,
                total_size = end.total_size_bytes,
                "Material ended with no content; skipping content-store import and routing to DLQ"
            );

            self.route_terminal_failure_with_retry(
                material_id,
                "empty_material",
                serde_json::json!({
                    "slice_count": slice_count,
                    "total_size": end.total_size_bytes,
                }),
                &state_handle,
                end,
            )
            .await?;
            return Ok(());
        }

        if end.total_size_bytes > self.max_material_size_bytes {
            warn!(
                material_id = %material_id,
                reported_total = end.total_size_bytes,
                max_material_size_bytes = self.max_material_size_bytes,
                "Material exceeded the configured per-material size limit"
            );
            self.route_terminal_failure_with_retry(
                material_id,
                "material_size_limit_exceeded",
                serde_json::json!({
                    "assembled_bytes": assembled_bytes,
                    "reported_total": end.total_size_bytes,
                    "max_material_size_bytes": self.max_material_size_bytes,
                    "slice_count": slice_count,
                }),
                &state_handle,
                end,
            )
            .await?;
            return Ok(());
        }

        // Verify the staged file size matches expectations before content-store import.
        // Edge case: File size mismatch can occur if:
        // - Disk writes were incomplete due to process crash during slice write
        // - Filesystem corruption or out-of-space errors during assembly
        // - Race between finalization and ongoing slice writes (prevented by finalizing flag)
        let file_size = match tokio::fs::metadata(&final_state.temp_path).await {
            Ok(m) => m.len() as i64,
            Err(error) => {
                warn!(
                    material_id = %material_id,
                    path = %final_state.temp_path.display(),
                    %error,
                    "Failed to stat assembled material file; routing to DLQ"
                );
                self.route_terminal_failure_with_retry(
                    material_id,
                    "material_stat_failed",
                    serde_json::json!({
                        "path": final_state.temp_path.display().to_string(),
                        "error": error.to_string(),
                    }),
                    &state_handle,
                    end,
                )
                .await?;
                return Ok(());
            }
        };
        if file_size != assembled_bytes {
            warn!(
                material_id = %material_id,
                file_size,
                assembled_bytes,
                "Assembled file size on disk does not match assembled bytes; routing to DLQ"
            );
            self.route_terminal_failure_with_retry(
                material_id,
                "material_size_mismatch_disk",
                serde_json::json!({
                    "assembled_bytes": assembled_bytes,
                    "file_size": file_size,
                    "reported_total": end.total_size_bytes,
                }),
                &state_handle,
                end,
            )
            .await?;
            return Ok(());
        }

        // Verify BLAKE3 hash matches the end message's claimed hash.
        // Edge case: Hash mismatch indicates:
        // - Network corruption during slice transmission (caught by NATS CRC but not impossible)
        // - Bug in publisher's hash calculation
        // - Slice ordering error (duplicate/missing slice despite offset tracking)
        // This is a critical integrity check - failures require investigation.
        if computed_hash != end.content_hash {
            warn!(
                material_id = %material_id,
                expected = %end.content_hash,
                actual = %computed_hash,
                "Material hash mismatch; routing to DLQ"
            );
            self.route_terminal_failure_with_retry(
                material_id,
                "material_hash_mismatch",
                serde_json::json!({
                    "expected_hash": end.content_hash,
                    "actual_hash": computed_hash,
                }),
                &state_handle,
                end,
            )
            .await?;
            return Ok(());
        }

        let finalize_metadata = match build_finalize_metadata(
            &final_state,
            &end.metadata,
            ended_at,
            end.total_size_bytes,
            &end.content_hash,
        ) {
            Ok(metadata) => metadata,
            Err(error) => {
                self.route_material_error(
                    material_id,
                    "material_finalize_metadata_invalid",
                    serde_json::json!({ "error": error.to_string() }),
                )
                .await;
                Self::revert_finalization_start(&state_handle, end).await;
                return Err(error);
            }
        };
        let final_status = final_material_status(&finalize_metadata);

        let content_key = match self.import_into_content_store(&final_state).await {
            Ok(result) => result,
            Err(e) => {
                let e = e.with_context(
                    super::redelivery_decision::REDELIVERY_ERROR_KIND_CONTEXT,
                    super::redelivery_decision::redelivery_error_class::CONTENT_STORE_TRANSIENT,
                );
                self.route_material_error(
                    material_id,
                    "content_store_import_failed",
                    serde_json::json!({ "error": e.to_string() }),
                )
                .await;
                Self::revert_finalization_start(&state_handle, end).await;
                return Err(e);
            }
        };

        let finalized = match FinalizationTransaction::new(self)
            .finalize(FinalizationRequest {
                final_state: &final_state,
                content_key: &content_key,
                content_hash: &end.content_hash,
                total_size_bytes: end.total_size_bytes,
                metadata: finalize_metadata,
                final_status,
            })
            .await
        {
            Ok(handle) => handle,
            Err(e) => {
                let commit_outcome_unknown = e.is_commit_outcome_unknown();
                let e = e.into_inner();
                if commit_outcome_unknown {
                    warn!(
                        material_id = %material_id,
                        error = %e,
                        "Material finalization commit outcome is unknown; preserving retry state without routing a terminal failure"
                    );
                } else {
                    self.route_material_error(
                        material_id,
                        "material_persist_failed",
                        serde_json::json!({ "error": e.to_string() }),
                    )
                    .await;
                }
                Self::revert_finalization_start(&state_handle, end).await;
                return Err(e);
            }
        };

        debug!(
            material_id = %material_id,
            blob_id = %finalized.blob_id.as_uuid(),
            reused_existing_commit = finalized.reused_existing_commit,
            "Material finalization transaction landed"
        );

        // Signal readiness only after the material registration/finalization transaction has
        // committed, so FK waiters never observe a phantom in-memory-ready state.
        if let Some(ref ready_set) = self.ready_set {
            ready_set.mark_ready(material_id);
        }

        self.cleanup_state(material_id).await;
        let _ = self.assembler_state.remove(&material_id);

        // Compute assembly duration from started_at to now
        let assembly_duration = Timestamp::now() - final_state.started_at;
        let duration_ms = assembly_duration.whole_milliseconds().max(0) as u64;

        if final_status == material_status::CANCELLED {
            self.stats_inc_cancelled(duration_ms as f64 / 1000.0, end.total_size_bytes as u64);

            tracing::info!(
                target: "sinex_metrics",
                metric = "assembly_cancelled",
                duration_ms = duration_ms,
                material_id = %material_id,
                slice_count = slice_count,
                size_bytes = end.total_size_bytes,
            );

            info!(
                material_id = %material_id,
                content_key = %content_key.key,
                size_bytes = end.total_size_bytes,
                slices = slice_count,
                duration_ms = duration_ms,
                "Material assembly cancelled and persisted to content store"
            );
        } else {
            self.stats_inc_completed(duration_ms as f64 / 1000.0, end.total_size_bytes as u64);

            tracing::info!(
                target: "sinex_metrics",
                metric = "assembly_completed",
                duration_ms = duration_ms,
                material_id = %material_id,
                slice_count = slice_count,
                size_bytes = end.total_size_bytes,
            );

            info!(
                material_id = %material_id,
                content_key = %content_key.key,
                size_bytes = end.total_size_bytes,
                slices = slice_count,
                duration_ms = duration_ms,
                "Material assembly complete and persisted to content store"
            );
        }

        Ok(())
    }

    /// Handle material finalization (end message)
    pub(super) async fn handle_end(&self, mut end: MaterialEndMessage) -> IngestdResult<()> {
        use super::state::normalize_metadata;

        end.metadata = normalize_metadata(end.metadata);
        let material_id = Uuid::from_str(&end.material_id).map_err(|e| {
            SinexError::parse(format!(
                "Invalid material_id '{}' in end message",
                end.material_id
            ))
            .with_source(e)
        })?;
        if self.pool.is_closed() {
            error!(
                material_id = %material_id,
                "Database pool closed before handling end message"
            );
            return Err(SinexError::database(
                "database pool closed before end processing".to_string(),
            ));
        }

        let state_handle = if let Some(existing) = self.get_state_handle(&material_id) {
            existing
        } else {
            let transition =
                if let Some(terminal_state) = self.material_terminal_state(material_id).await? {
                    AssemblyStateMachine::transition(terminal_state, AssemblyInput::EndFrame)
                } else {
                    AssemblyStateMachine::transition(
                        AssemblyLogicalState::Idle,
                        AssemblyInput::EndFrame,
                    )
                }
                .map_err(|error| error.into_sinex_error(material_id))?;

            if matches!(transition, AssemblyTransition::IgnoreTerminalFrame) {
                info!(
                    material_id = %material_id,
                    transition = ?transition,
                    "End message received after terminal material; skipping placeholder state"
                );
                return Ok(());
            }
            debug!(
                material_id = %material_id,
                transition = ?transition,
                "Assembly state machine accepted end for new material state"
            );
            // Preserve compatibility with redelivery, restored WAL state, and non-SDK publishers:
            // record the end even if local state is not present yet.
            warn!(
                material_id = %material_id,
                "End message received before material state existed; creating placeholder"
            );
            let placeholder = self.create_placeholder_state(material_id).await?;
            self.insert_state_handle(material_id, placeholder)
        };

        // Record end so a later redelivery or restored slice can complete the material.
        {
            let mut state = state_handle.lock().await;
            let transition =
                AssemblyStateMachine::transition_for_state(&state, AssemblyInput::EndFrame)
                    .map_err(|error| error.into_sinex_error(material_id))?;

            if matches!(transition, AssemblyTransition::IgnoreFinalizingFrame) {
                debug!(
                    material_id = %material_id,
                    transition = ?transition,
                    "Ignoring end message while finalizing"
                );
                return Ok(());
            }
            debug!(
                material_id = %material_id,
                transition = ?transition,
                "Assembly state machine accepted end for existing material state"
            );
            state.pending_end = Some(end.clone());
            super::io::append_wal_entry(self, &mut state, super::state::WalEntry::End(end.clone()))
                .await?;
        }

        self.try_finalize_pending_end(material_id, state_handle, PendingEndBehavior::Ignore)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::material_assembler::FinalizationState;
    use crate::material_assembler::finalization_transaction::{
        FinalizationErrorKind, FinalizationRequest, FinalizationTransaction,
    };
    use crate::material_assembler::{io, state};
    use serde_json::json;
    use sinex_db::{
        models::blob::Blob,
        repositories::{DbPoolExt, source_materials::status},
    };
    use sinex_node_sdk::content_store::ContentStoreKey;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;
    use xtask::sandbox::prelude::*;

    async fn test_assembler(
        ctx: &TestContext,
    ) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
        super::super::test_support::build_test_assembler(ctx, "finalize-test").await
    }

    #[sinex_test]
    async fn finalize_failed_material_skips_material_already_finalizing(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://finalizing"),
                json!({}),
                Timestamp::now(),
            )
            .await?;

        let mut state = assembler.create_placeholder_state(material_id).await?;
        state.phase = AssemblyPhase::Finalizing;
        assembler.insert_state_handle(material_id, state);

        assembler
            .finalize_failed_material(material_id, "slice_arrival_timeout")
            .await;

        let material = ctx
            .pool
            .source_materials()
            .get_by_id(Id::from_uuid(material_id))
            .await?
            .expect("material should exist");
        assert_eq!(material.status.as_str(), status::SENSING);
        assert!(assembler.assembler_state.contains_key(&material_id));
        Ok(())
    }

    #[sinex_test]
    async fn finalize_failed_material_skips_terminal_material_without_state(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::from_uuid(material_id);

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://completed"),
                json!({}),
                Timestamp::now(),
            )
            .await?;
        ctx.pool
            .source_materials()
            .finalize_in_flight(material_id_typed, None, None, None, Some(42))
            .await?;

        assembler
            .finalize_failed_material(material_id, "slice_arrival_timeout")
            .await;

        let material = ctx
            .pool
            .source_materials()
            .get_by_id(material_id_typed)
            .await?
            .expect("material should exist");
        assert_eq!(material.status.as_str(), status::COMPLETED);
        Ok(())
    }

    #[sinex_test]
    async fn finalize_failed_material_preserves_retry_state_when_failure_mark_is_not_durable(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();

        let mut state = assembler.create_placeholder_state(material_id).await?;
        let temp_path = state.temp_path.clone();
        tokio::fs::write(&temp_path, b"partial").await?;
        state.phase = AssemblyPhase::Accumulating;
        let state_handle = assembler.insert_state_handle(material_id, state);

        ctx.pool.close().await;

        let error = assembler
            .finalize_failed_material_claimed_checked(
                material_id,
                "material_hash_mismatch",
                AssemblyPhase::Accumulating,
            )
            .await
            .expect_err("cleanup should fail honestly when the durable failure mark cannot land");

        assert!(
            error
                .to_string()
                .contains("Failed to mark material as failed in database"),
            "unexpected error: {error}"
        );
        assert!(
            assembler.assembler_state.contains_key(&material_id),
            "retry state must be preserved until the failure mark lands durably"
        );
        assert!(
            temp_path.exists(),
            "staged material should remain on disk for retry"
        );
        assert_eq!(state_handle.lock().await.phase, AssemblyPhase::Accumulating);
        Ok(())
    }

    #[sinex_test]
    async fn try_finalize_pending_end_routes_invalid_end_timestamp_to_dlq(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::from_uuid(material_id);
        let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.ingestd");
        let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://invalid-ended-at"),
                json!({}),
                Timestamp::now(),
            )
            .await?;

        let mut state = assembler.create_placeholder_state(material_id).await?;
        state.material_kind = "test".to_string();
        state.source_identifier = "test://invalid-ended-at".to_string();
        state.phase = AssemblyPhase::Accumulating;
        state.expected_offset = 4;
        state.slice_count = 1;
        state.pending_end = Some(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: "not-a-timestamp".to_string(),
            content_hash: blake3::hash(b"data").to_hex().to_string(),
            total_slices: 1,
            total_size_bytes: 4,
            metadata: json!({}),
        });
        let state_handle = assembler.insert_state_handle(material_id, state);

        assembler
            .try_finalize_pending_end(material_id, state_handle, PendingEndBehavior::Error)
            .await?;

        let msg = timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("missing DLQ message"))?;
        let payload: JsonValue = serde_json::from_slice(&msg.payload)?;
        assert_eq!(payload["error"], "material_end_timestamp_invalid");
        assert_eq!(payload["material_id"], material_id.to_string());
        assert_eq!(payload["context"]["ended_at"], "not-a-timestamp");

        let material = ctx
            .pool
            .source_materials()
            .get_by_id(material_id_typed)
            .await?
            .expect("material should exist");
        assert_eq!(material.status.as_str(), status::FAILED);
        assert!(
            !assembler.assembler_state.contains_key(&material_id),
            "invalid end timestamp should clean up assembler state instead of retrying forever"
        );

        Ok(())
    }

    #[sinex_test]
    async fn try_finalize_pending_end_routes_missing_material_file_to_dlq(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::from_uuid(material_id);
        let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.ingestd");
        let mut dlq_sub = ctx.nats_client().subscribe(dlq_subject).await?;

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://missing-material-file"),
                json!({}),
                Timestamp::now(),
            )
            .await?;

        let mut state = assembler.create_placeholder_state(material_id).await?;
        tokio::fs::write(&state.temp_path, b"data").await?;
        let missing_path = state.temp_path.clone();
        tokio::fs::remove_file(&missing_path).await?;
        state.material_kind = "test".to_string();
        state.source_identifier = "test://missing-material-file".to_string();
        state.phase = AssemblyPhase::Accumulating;
        state.expected_offset = 4;
        state.slice_count = 1;
        state.pending_end = Some(MaterialEndMessage {
            material_id: material_id.to_string(),
            ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
            content_hash: blake3::hash(b"data").to_hex().to_string(),
            total_slices: 1,
            total_size_bytes: 4,
            metadata: json!({}),
        });
        let state_handle = assembler.insert_state_handle(material_id, state);

        assembler
            .try_finalize_pending_end(material_id, state_handle, PendingEndBehavior::Error)
            .await?;

        let msg = timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("missing DLQ message"))?;
        let payload: JsonValue = serde_json::from_slice(&msg.payload)?;
        assert_eq!(payload["error"], "material_stat_failed");
        assert_eq!(payload["material_id"], material_id.to_string());
        assert_eq!(
            payload["context"]["path"],
            missing_path.display().to_string()
        );

        let material = ctx
            .pool
            .source_materials()
            .get_by_id(material_id_typed)
            .await?
            .expect("material should exist");
        assert_eq!(material.status.as_str(), status::FAILED);
        assert!(
            !assembler.assembler_state.contains_key(&material_id),
            "missing staged material file should clean up assembler state"
        );

        Ok(())
    }

    #[sinex_test]
    async fn handle_end_before_slice_waits_for_missing_slice_instead_of_failing(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::from_uuid(material_id);
        let started_at = Timestamp::now();
        let payload = b"data".to_vec();

        assembler
            .handle_end(MaterialEndMessage {
                material_id: material_id.to_string(),
                ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
                content_hash: blake3::hash(&payload).to_hex().to_string(),
                total_slices: 1,
                total_size_bytes: payload.len() as i64,
                metadata: json!({}),
            })
            .await?;

        assert!(
            assembler.assembler_state.contains_key(&material_id),
            "out-of-order end should keep placeholder state for later slices"
        );

        state::handle_begin(
            &assembler,
            material_id,
            state::MaterialBeginMessage {
                material_id: material_id.to_string(),
                material_kind: "test".to_string(),
                source_identifier: "test://out-of-order-end".to_string(),
                metadata: json!({}),
                started_at: sinex_primitives::temporal::format_rfc3339(started_at),
            },
        )
        .await?;

        io::handle_slice(&assembler, material_id, 0, payload).await?;

        let material = ctx
            .pool
            .source_materials()
            .get_by_id(material_id_typed)
            .await?
            .expect("material should exist");
        assert_eq!(material.status.as_str(), status::COMPLETED);
        assert!(
            !assembler.assembler_state.contains_key(&material_id),
            "completed out-of-order assembly should clean up in-memory state"
        );

        Ok(())
    }

    #[sinex_test]
    async fn finalization_transaction_is_idempotent_after_commit_lands(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::from_uuid(material_id);
        let content_key = ContentStoreKey::parse("SHA256E-s4--hash")?;

        let blob = ctx
            .pool
            .blobs()
            .insert(
                Blob::builder()
                    .storage_backend(content_key.storage_backend().to_string())
                    .content_hash(content_key.digest.clone())
                    .original_filename("material.bin".to_string())
                    .size_bytes(content_key.size as i64)
                    .checksum_blake3("hash".to_string())
                    .metadata(json!({ "material_id": material_id }))
                    .build(),
            )
            .await?;

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://idempotent-finalize"),
                json!({}),
                Timestamp::now(),
            )
            .await?;
        ctx.pool
            .source_materials()
            .finalize_in_flight(
                material_id_typed,
                Some(blob.id),
                None,
                None,
                Some(content_key.size as i64),
            )
            .await?;
        ctx.pool
            .source_materials()
            .append_temporal_ledger(TemporalLedgerEntry::realtime_capture(
                material_id,
                content_key.size as i64,
                Timestamp::now(),
            ))
            .await?;

        let final_state = FinalizationState {
            material_id,
            temp_path: state_dir.path().join("material.bin"),
            expected_offset: content_key.size as i64,
            slice_count: 1,
            buffered_count: 0,
            metadata: json!({}),
            material_kind: "test".to_string(),
            source_identifier: "test://idempotent-finalize".to_string(),
            started_at: Timestamp::now(),
        };

        let end = MaterialEndMessage {
            material_id: material_id.to_string(),
            total_slices: 1,
            total_size_bytes: content_key.size as i64,
            content_hash: "hash".to_string(),
            metadata: json!({}),
            ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
        };

        let ledger_count_before = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i64" FROM raw.temporal_ledger WHERE source_material_id = $1"#,
            material_id
        )
        .fetch_one(&ctx.pool)
        .await?;

        let handle = FinalizationTransaction::new(&assembler)
            .finalize(FinalizationRequest {
                final_state: &final_state,
                content_key: &content_key,
                content_hash: &end.content_hash,
                total_size_bytes: end.total_size_bytes,
                metadata: json!({}),
                final_status: status::COMPLETED,
            })
            .await?;
        assert_eq!(*handle.blob_id.as_uuid(), *blob.id.as_uuid());
        assert!(
            handle.reused_existing_commit,
            "retrying a landed commit should report a reused committed handle"
        );

        let material = ctx
            .pool
            .source_materials()
            .get_by_id(material_id_typed)
            .await?
            .expect("material should still exist");
        assert_eq!(material.status.as_str(), status::COMPLETED);
        assert_eq!(material.optional_blob_id, Some(*blob.id.as_uuid()));

        let ledger_count_after = sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!: i64" FROM raw.temporal_ledger WHERE source_material_id = $1"#,
            material_id
        )
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            ledger_count_after, ledger_count_before,
            "retrying finalization after a landed commit should not duplicate ledger entries"
        );

        Ok(())
    }

    #[sinex_test]
    async fn record_staged_at_ledger_entry_is_idempotent(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, _state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let started_at = Timestamp::now();

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://staged-at-idempotent"),
                json!({}),
                started_at,
            )
            .await?;

        assembler
            .record_staged_at_ledger_entry(material_id, started_at)
            .await?;
        assembler
            .record_staged_at_ledger_entry(material_id, started_at)
            .await?;

        let staged_at_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!: i64"
            FROM raw.temporal_ledger
            WHERE source_material_id = $1
              AND source_type = 'staged_at'
            "#,
            material_id
        )
        .fetch_one(&ctx.pool)
        .await?;

        assert_eq!(
            staged_at_count, 1,
            "duplicate begin-time staged_at writes must collapse to one ledger row"
        );

        Ok(())
    }

    #[sinex_test]
    async fn finalization_transaction_rolls_back_blob_material_and_ledger_on_finalize_failure(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::<SourceMaterialRecord>::from_uuid(material_id);
        let content_key = ContentStoreKey::parse("SHA256E-s32--rollback-blob-hash")?;
        let started_at = Timestamp::now();

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://rollback-finalize"),
                json!({ "original": true }),
                started_at,
            )
            .await?;

        let final_state = FinalizationState {
            material_id,
            temp_path: state_dir.path().join("rollback-material.bin"),
            expected_offset: content_key.size as i64,
            slice_count: 1,
            buffered_count: 0,
            metadata: json!({ "original": true }),
            material_kind: "test".to_string(),
            source_identifier: "test://rollback-finalize".to_string(),
            started_at,
        };

        let error = FinalizationTransaction::new(&assembler)
            .finalize(FinalizationRequest {
                final_state: &final_state,
                content_key: &content_key,
                content_hash: "rollback-blake3",
                total_size_bytes: -1,
                metadata: json!({ "finalized": true }),
                final_status: status::COMPLETED,
            })
            .await
            .expect_err("negative total_bytes should fail source-material finalization");

        assert_eq!(error.kind(), FinalizationErrorKind::FinalizeMaterialRecord);
        assert!(
            error.to_string().contains("Failed to finalize material"),
            "unexpected error: {error}"
        );

        let material = ctx
            .pool
            .source_materials()
            .get_by_id(material_id_typed)
            .await?
            .expect("material should still exist");
        assert_eq!(material.status.as_str(), status::SENSING);
        assert_eq!(material.optional_blob_id, None);
        assert_eq!(material.metadata["original"], true);
        assert_eq!(material.metadata.get("finalized"), None);

        let blob = ctx
            .pool
            .blobs()
            .get_by_content(
                content_key.storage_backend(),
                &content_key.digest,
                content_key.size as i64,
            )
            .await?;
        assert!(
            blob.is_none(),
            "blob insert must roll back when finalization fails"
        );

        let ledger_entries = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!: i64"
            FROM raw.temporal_ledger
            WHERE source_material_id = $1
            "#,
            material_id
        )
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            ledger_entries, 0,
            "ledger write must not escape a failed transaction"
        );

        Ok(())
    }

    #[sinex_test]
    async fn finalization_transaction_reuses_existing_blob_inside_transaction(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::<SourceMaterialRecord>::from_uuid(material_id);
        let content_key = ContentStoreKey::parse("SHA256E-s32--existing-blob-hash")?;

        let existing_blob = ctx
            .pool
            .blobs()
            .insert(
                Blob::builder()
                    .storage_backend(content_key.storage_backend().to_string())
                    .content_hash(content_key.digest.clone())
                    .original_filename("existing-material.bin".to_string())
                    .size_bytes(content_key.size as i64)
                    .checksum_blake3("existing-blob-blake3".to_string())
                    .metadata(json!({ "seeded": true }))
                    .build(),
            )
            .await?;
        let started_at = Timestamp::now();

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://existing-blob-finalize"),
                json!({}),
                started_at,
            )
            .await?;
        assembler
            .record_staged_at_ledger_entry(material_id, started_at)
            .await?;

        let final_state = FinalizationState {
            material_id,
            temp_path: state_dir.path().join("existing-material.bin"),
            expected_offset: content_key.size as i64,
            slice_count: 1,
            buffered_count: 0,
            metadata: json!({}),
            material_kind: "test".to_string(),
            source_identifier: "test://existing-blob-finalize".to_string(),
            started_at,
        };

        let end = MaterialEndMessage {
            material_id: material_id.to_string(),
            total_slices: 1,
            total_size_bytes: content_key.size as i64,
            content_hash: "existing-blob-blake3".to_string(),
            metadata: json!({}),
            ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
        };

        let handle = FinalizationTransaction::new(&assembler)
            .finalize(FinalizationRequest {
                final_state: &final_state,
                content_key: &content_key,
                content_hash: &end.content_hash,
                total_size_bytes: end.total_size_bytes,
                metadata: json!({}),
                final_status: status::COMPLETED,
            })
            .await?;
        assert_eq!(*handle.blob_id.as_uuid(), *existing_blob.id.as_uuid());
        assert!(
            !handle.reused_existing_commit,
            "first successful transaction should not be reported as a pre-existing committed state"
        );

        let material = ctx
            .pool
            .source_materials()
            .get_by_id(material_id_typed)
            .await?
            .expect("material should exist");

        assert_eq!(material.status.as_str(), status::COMPLETED);
        assert_eq!(material.optional_blob_id, Some(*existing_blob.id.as_uuid()));

        let ledger_entries = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!: i64"
            FROM raw.temporal_ledger
            WHERE source_material_id = $1
            "#,
            material_id
        )
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            ledger_entries, 2,
            "staged_at + realtime_capture should both persist"
        );

        Ok(())
    }

    #[sinex_test]
    async fn finalization_transaction_reuses_existing_blob_by_blake3_inside_transaction(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _content_store_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::<SourceMaterialRecord>::from_uuid(material_id);
        let content_hash = "existing-blob-blake3";
        let content_key = ContentStoreKey::parse(&format!("SINEXBLAKE3-s32--{content_hash}"))?;

        let existing_blob = ctx
            .pool
            .blobs()
            .insert(
                Blob::builder()
                    .storage_backend("SHA256E".to_string())
                    .content_hash("existing-sha256-hash".to_string())
                    .original_filename("existing-material.bin".to_string())
                    .size_bytes(content_key.size as i64)
                    .checksum_blake3(content_hash.to_string())
                    .metadata(json!({ "seeded": true }))
                    .build(),
            )
            .await?;
        let started_at = Timestamp::now();

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://existing-blob-by-blake3-finalize"),
                json!({}),
                started_at,
            )
            .await?;
        assembler
            .record_staged_at_ledger_entry(material_id, started_at)
            .await?;

        let final_state = FinalizationState {
            material_id,
            temp_path: state_dir.path().join("existing-material-by-blake3.bin"),
            expected_offset: content_key.size as i64,
            slice_count: 1,
            buffered_count: 0,
            metadata: json!({}),
            material_kind: "test".to_string(),
            source_identifier: "test://existing-blob-by-blake3-finalize".to_string(),
            started_at,
        };

        let end = MaterialEndMessage {
            material_id: material_id.to_string(),
            total_slices: 1,
            total_size_bytes: content_key.size as i64,
            content_hash: content_hash.to_string(),
            metadata: json!({}),
            ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
        };

        let handle = FinalizationTransaction::new(&assembler)
            .finalize(FinalizationRequest {
                final_state: &final_state,
                content_key: &content_key,
                content_hash: &end.content_hash,
                total_size_bytes: end.total_size_bytes,
                metadata: json!({}),
                final_status: status::COMPLETED,
            })
            .await?;
        assert_eq!(*handle.blob_id.as_uuid(), *existing_blob.id.as_uuid());

        let material = ctx
            .pool
            .source_materials()
            .get_by_id(material_id_typed)
            .await?
            .expect("material should exist");

        assert_eq!(material.status.as_str(), status::COMPLETED);
        assert_eq!(material.optional_blob_id, Some(*existing_blob.id.as_uuid()));

        Ok(())
    }
}
