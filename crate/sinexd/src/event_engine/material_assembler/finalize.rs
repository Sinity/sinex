//! Material finalization methods for `MaterialAssembler`.
//!
//! This module contains finalization orchestration, error routing, and cleanup
//! logic that executes when a material assembly completes or fails. The durable
//! source-material/blob/ledger commit boundary lives in `finalization_transaction`.

use serde::Serialize;
use sinex_db::repositories::DbPoolExt;
use sinex_db::schema::defs::records::SourceMaterialRecord;
use sinex_primitives::Timestamp;
use sinex_primitives::nats::{NatsTrafficClass, insert_traffic_class_header};
use sinex_primitives::transport;
use sinex_primitives::{
    Id, JsonValue, MaterialStatus, Uuid, sources::is_self_observation_material_source,
};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::event_engine::{EventEngineResult, SinexError};
use crate::runtime::nats_payload::ensure_nats_payload_fits;

use super::assembly_state_machine::{
    AssemblyInput, AssemblyLogicalState, AssemblyStateMachine, AssemblyTransition,
};
use super::finalization_transaction::{FinalizationRequest, FinalizationTransaction};
use super::state::AssemblyPhase;
use super::{MaterialAssembler, MaterialEndMessage};
use std::{str::FromStr, sync::Arc};

pub(super) const ZERO_EVENT_SELF_OBSERVATION_TIMEOUT_RECOVERY_REASON: &str =
    "slice_arrival_timeout_zero_event_self_observation_recovered_partial";
pub(super) const ZERO_EVENT_SOURCE_MATERIAL_TIMEOUT_RECOVERY_REASON: &str =
    "slice_arrival_timeout_zero_event_source_material_recovered_partial";

#[derive(Clone, Copy)]
pub(super) enum PendingEndBehavior {
    #[cfg(test)]
    Error,
    Ignore,
}

fn final_material_status(metadata: &JsonValue) -> MaterialStatus {
    metadata
        .as_object()
        .and_then(|map| map.get("cancelled"))
        .and_then(JsonValue::as_bool)
        .map_or(MaterialStatus::Completed, |cancelled| {
            if cancelled {
                MaterialStatus::Cancelled
            } else {
                MaterialStatus::Completed
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

                let publish_result = ensure_nats_payload_fits(
                    "source-material DLQ entry",
                    &self.dlq_subject,
                    bytes.len(),
                )
                .map_err(|error| error.with_context("material_id", material_id.to_string()));

                if let Err(e) = publish_result {
                    error!(
                        target: "sinex_metrics",
                        metric = "event_engine.material_dlq_publish_failures_total",
                        material_id = %material_id,
                        error = %e,
                        "Failed to publish material DLQ entry"
                    );
                } else if let Err(e) = self
                    .nats_client
                    .publish_with_headers(self.dlq_subject.clone(), headers, bytes.into())
                    .await
                {
                    error!(
                        target: "sinex_metrics",
                        metric = "event_engine.material_dlq_publish_failures_total",
                        material_id = %material_id,
                        error = %e,
                        "Failed to publish material DLQ entry"
                    );
                } else {
                    debug!(material_id = %material_id, "Routed to DLQ");
                }
            }
            Err(e) => {
                error!(
                    target: "sinex_metrics",
                    metric = "event_engine.material_dlq_publish_failures_total",
                    material_id = %material_id,
                    error = %e,
                    "Failed to encode DLQ payload"
                );
            }
        }
    }

    /// Mark material as failed in the database to prevent reprocessing.
    async fn mark_material_failed_checked(
        &self,
        material_id: Uuid,
        reason: &str,
    ) -> EventEngineResult<()> {
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

    pub(super) async fn mark_timeout_material_recovered_partial_if_eventful(
        &self,
        material_id: Uuid,
        reason: &str,
    ) -> EventEngineResult<bool> {
        if reason != "slice_arrival_timeout" {
            return Ok(false);
        }

        let id: Id<SourceMaterialRecord> = Id::from_uuid(material_id);
        let parsed_event_count = self
            .pool
            .source_materials()
            .parsed_event_count(id)
            .await
            .map_err(|error| {
                SinexError::database("Failed to read material parsed event count")
                    .with_context("material_id", material_id.to_string())
                    .with_context("failure_reason", reason)
                    .with_source(error)
            })?;

        if parsed_event_count <= 0 {
            return Ok(false);
        }

        self.pool
            .source_materials()
            .mark_as_recovered_partial(
                id,
                "slice_arrival_timeout_with_admitted_events",
                serde_json::json!({
                    "failure_reason": reason,
                    "timeout_partial_recovery": {
                        "parsed_event_count": parsed_event_count,
                        "policy": "material_had_admitted_events_before_timeout"
                    }
                }),
            )
            .await
            .map_err(|error| {
                SinexError::database("Failed to mark timeout material recovered_partial")
                    .with_context("material_id", material_id.to_string())
                    .with_context("failure_reason", reason)
                    .with_context("parsed_event_count", parsed_event_count.to_string())
                    .with_source(error)
            })?;

        info!(
            material_id = %material_id,
            parsed_event_count,
            "Marked timed-out source material as recovered_partial because events were already admitted"
        );
        Ok(true)
    }

    pub(super) async fn mark_timeout_zero_event_material_recovered_partial(
        &self,
        material_id: Uuid,
        elapsed_secs: i64,
    ) -> EventEngineResult<bool> {
        let id: Id<SourceMaterialRecord> = Id::from_uuid(material_id);
        let material = self
            .pool
            .source_materials()
            .get_by_id(id)
            .await
            .map_err(|error| {
                SinexError::database("Failed to read timed-out source material")
                    .with_context("material_id", material_id.to_string())
                    .with_source(error)
            })?;

        let Some(material) = material else {
            return Ok(false);
        };

        let parsed_event_count = self
            .pool
            .source_materials()
            .parsed_event_count(id)
            .await
            .map_err(|error| {
                SinexError::database("Failed to read material parsed event count")
                    .with_context("material_id", material_id.to_string())
                    .with_context("source_identifier", material.source_identifier.clone())
                    .with_source(error)
            })?;

        if parsed_event_count != 0 {
            return Ok(false);
        }

        let is_self_observation =
            is_self_observation_material_source(&material.source_identifier);
        let (recovery_reason, metadata_key, dlq_policy) = if is_self_observation {
            (
                ZERO_EVENT_SELF_OBSERVATION_TIMEOUT_RECOVERY_REASON,
                "slice_arrival_timeout_zero_event_self_observation",
                "suppressed_zero_event_self_observation_timeout",
            )
        } else {
            (
                ZERO_EVENT_SOURCE_MATERIAL_TIMEOUT_RECOVERY_REASON,
                "slice_arrival_timeout_zero_event_source_material",
                "suppressed_zero_event_source_material_timeout",
            )
        };

        self.pool
            .source_materials()
            .mark_as_recovered_partial(
                id,
                recovery_reason,
                serde_json::json!({
                    metadata_key: {
                        "material_id": material_id.to_string(),
                        "source_identifier": material.source_identifier,
                        "elapsed_seconds": elapsed_secs,
                        "timeout_seconds": self.slice_arrival_timeout.as_secs(),
                        "parsed_event_count": parsed_event_count,
                        "dlq_policy": dlq_policy
                    }
                }),
            )
            .await
            .map_err(|error| {
                SinexError::database(
                    "Failed to mark zero-event timeout recovered_partial",
                )
                .with_context("material_id", material_id.to_string())
                .with_context("parsed_event_count", parsed_event_count.to_string())
                .with_source(error)
            })?;

        info!(
            material_id = %material_id,
            parsed_event_count,
            recovery_reason,
            "Marked timed-out zero-event source material as recovered_partial"
        );
        Ok(true)
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
    ) -> EventEngineResult<()> {
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

        match self
            .mark_timeout_material_recovered_partial_if_eventful(material_id, reason)
            .await
        {
            Ok(true) => {
                self.cleanup_state(material_id).await;
                let _ = self.assembler_state.remove(&material_id);
                return Ok(());
            }
            Ok(false) => {}
            Err(error) => {
                self.revert_failure_cleanup_start(material_id, resume_phase)
                    .await;
                return Err(error);
            }
        }

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
    ) -> EventEngineResult<()> {
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

    /// Decouple finalization from the ordered frame consumer (#2187 keystone).
    ///
    /// The END frame's durable state (staged bytes on disk + the WAL `End`
    /// entry) is already persisted by the caller before this runs, so the
    /// consumer can ACK the frame and continue immediately while the heavy
    /// finalize (content-store CAS copy + Postgres commit) executes on a
    /// semaphore-gated worker. This is what stops a single wedged finalize from
    /// head-of-line blocking the 400K-frame backlog.
    ///
    /// On transient failure the finalize path preserves retry state
    /// (`pending_end` is restored), and the maintenance loop re-drives it; a
    /// crash before commit is recovered by WAL replay on restart. Both retry
    /// channels are independent of the now-dropped NATS frame.
    pub(super) fn dispatch_finalize(
        &self,
        material_id: Uuid,
        state_handle: Arc<Mutex<super::state::AssemblerState>>,
    ) {
        self.finalize_in_flight
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let assembler = self.clone_for_task();
        let semaphore = self.finalize_semaphore.clone();
        let in_flight = self.finalize_in_flight.clone();
        tokio::spawn(async move {
            // Decrement exactly once when the worker finishes, even on panic or
            // early return, so the backpressure gate cannot leak permits.
            struct InFlightGuard(Arc<std::sync::atomic::AtomicUsize>);
            impl Drop for InFlightGuard {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                }
            }
            let _guard = InFlightGuard(in_flight);

            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => return,
            };
            if let Err(error) = assembler
                .try_finalize_pending_end(material_id, state_handle, PendingEndBehavior::Ignore)
                .await
            {
                warn!(
                    material_id = %material_id,
                    error = %error,
                    "Decoupled material finalize failed; retry state preserved for maintenance re-drive"
                );
            }
        });
    }

    pub(super) async fn try_finalize_pending_end(
        &self,
        material_id: Uuid,
        state_handle: Arc<Mutex<super::state::AssemblerState>>,
        pending_behavior: PendingEndBehavior,
    ) -> EventEngineResult<()> {
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

        let finalized = match tokio::time::timeout(
            self.finalize_timeout,
            FinalizationTransaction::new(self).finalize(FinalizationRequest {
                final_state: &final_state,
                content_key: &content_key,
                content_hash: &end.content_hash,
                total_size_bytes: end.total_size_bytes,
                metadata: finalize_metadata,
                final_status,
            }),
        )
        .await
        {
            Ok(Ok(handle)) => handle,
            Err(_elapsed) => {
                // Finalize exceeded its bound: commit outcome is unknown (the DB
                // transaction may still be in flight or wedged on a lock). Preserve
                // retry state and NAK for redelivery rather than pinning the
                // single-threaded material consumer for the full DB lock timeout
                // (#2187: tiny finalizes were observed taking ~15 min head-of-line).
                self.stats_inc_commit_outcome_unknown();
                warn!(
                    material_id = %material_id,
                    timeout_secs = self.finalize_timeout.as_secs(),
                    "Material finalization exceeded timeout; preserving retry state and NAKing for redelivery"
                );
                Self::revert_finalization_start(&state_handle, end).await;
                return Err(SinexError::processing("material finalization exceeded timeout")
                    .with_context("material_id", material_id.to_string())
                    .with_context("timeout_secs", self.finalize_timeout.as_secs().to_string())
                    .with_context("finalization_stage", "commit_outcome_unknown"));
            }
            Ok(Err(e)) => {
                let commit_outcome_unknown = e.is_commit_outcome_unknown();
                let e = e.into_inner();
                if commit_outcome_unknown {
                    self.stats_inc_commit_outcome_unknown();
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

        if final_status == MaterialStatus::Cancelled {
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
    pub(super) async fn handle_end(&self, mut end: MaterialEndMessage) -> EventEngineResult<()> {
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
                target: "sinex_metrics",
                metric = "event_engine.material_finalization_failures_total",
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
            // Preserve compatibility with redelivery, restored WAL state, and non-runtime publishers:
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

        // The END frame is now durable (staged bytes + WAL `End` entry recorded
        // above), so hand finalization to the decoupled worker set and let the
        // ordered consumer ACK this frame and move on (#2187). A complete material
        // finalizes promptly on a worker; an incomplete one no-ops until its
        // remaining slices arrive and re-drive finalization.
        self.dispatch_finalize(material_id, state_handle);
        Ok(())
    }
}

#[cfg(test)]
#[path = "finalize_test.rs"]
mod tests;
