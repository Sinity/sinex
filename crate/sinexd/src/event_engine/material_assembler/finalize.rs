//! Material finalization methods for `MaterialAssembler`.
//!
//! This module contains finalization orchestration, error routing, and cleanup
//! logic that executes when a material assembly completes or fails. The durable
//! source-material/blob/ledger commit boundary lives in `finalization_transaction`.

use camino::Utf8PathBuf;
use futures::FutureExt as _;
use serde::Serialize;
use sinex_db::repositories::DbPoolExt;
use sinex_db::schema::defs::records::SourceMaterialRecord;
use sinex_primitives::Timestamp;
use sinex_primitives::nats::{NatsTrafficClass, insert_traffic_class_header};
use sinex_primitives::transport;
use sinex_primitives::{
    Id, JsonValue, MaterialManifestV1, MaterialStatus, Uuid,
    sources::is_self_observation_material_source,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::event_engine::durable_failure::{DURABLE_FAILURE_ID_HEADER, persist_failure_evidence};
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
    #[serde(skip_serializing_if = "Option::is_none")]
    material_id: Option<String>,
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
            state.mark_finalizing();
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
                state.restore_phase(resume_phase);
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
        state.restore_phase(AssemblyPhase::Accumulating);
        state.pending_end = Some(end);
        // WAL is immutable — End message remains. In-memory state reverted.
    }

    async fn recover_finalization_worker_panic(
        state_handle: &Arc<Mutex<super::state::AssemblerState>>,
    ) {
        let mut state = state_handle.lock().await;
        if state.phase == AssemblyPhase::Finalizing {
            // `pending_end` remains retained throughout the worker attempt, so
            // returning to Accumulating makes the maintenance re-drive route
            // immediately usable after a detached worker panic.
            state.phase = AssemblyPhase::Accumulating;
        }
    }

    async fn observe_finalize_worker(
        &self,
        material_id: Uuid,
        state_handle: Arc<Mutex<super::state::AssemblerState>>,
        worker: JoinHandle<EventEngineResult<()>>,
    ) {
        match worker.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(
                material_id = %material_id,
                error = %error,
                "Decoupled material finalize failed; retry state preserved for maintenance re-drive"
            ),
            Err(error) => {
                warn!(
                    material_id = %material_id,
                    error = ?error,
                    "Decoupled material finalize worker panicked or was aborted; restoring maintenance recovery state"
                );
                Self::recover_finalization_worker_panic(&state_handle).await;
                // Keep the recovery action observable even if the worker
                // vanished before it could emit its own error.
                if let Err(dlq_error) = self
                    .route_material_error(
                        material_id,
                        "material_finalize_worker_terminated",
                        serde_json::json!({ "join_error": error.to_string() }),
                    )
                    .await
                {
                    warn!(
                        material_id = %material_id,
                        error = %dlq_error,
                        "Failed to publish material finalize worker termination evidence"
                    );
                }
            }
        }
    }

    /// Route material failure to DLQ.
    ///
    /// Mirrors the raw-event DLQ discipline (`jetstream_consumer::dlq::route_to_dlq`):
    /// the caller is responsible for deciding what happens when the DLQ publish
    /// itself fails. Never swallow a DLQ publish failure here and let a caller
    /// treat it as if durable failure evidence exists (sinex-wb1).
    pub(super) async fn route_material_error(
        &self,
        material_id: Uuid,
        error: impl Into<String>,
        context: JsonValue,
    ) -> EventEngineResult<Uuid> {
        self.route_material_frame_error(Some(material_id), error, context)
            .await
    }

    /// Route a material frame failure when decoding may not have produced a
    /// material ID. A generated witness ID keeps the malformed frame
    /// operator-visible and lets the caller settle it only after the confirmed
    /// JetStream publish succeeds.
    pub(super) async fn route_material_frame_error(
        &self,
        material_id: Option<Uuid>,
        error: impl Into<String>,
        context: JsonValue,
    ) -> EventEngineResult<Uuid> {
        let failure_event_id = material_id.unwrap_or_else(Uuid::now_v7);
        let payload = MaterialDlqPayload {
            material_id: material_id.map(|id| id.to_string()),
            error: error.into(),
            context,
            failed_at: Timestamp::now(),
        };

        let payload_json = serde_json::to_value(&payload).map_err(|error| {
            SinexError::serialization("Failed to encode material DLQ evidence").with_source(error)
        })?;
        let durable_failure_id = persist_failure_evidence(
            &self.pool,
            failure_event_id,
            "event-engine.material-assembler",
            "source-material",
            "material.assembly",
            "permanent",
            &payload.error,
            payload_json,
            serde_json::json!({
                "durability_source": "postgres_pre_material_dlq_settlement",
                "material_id": material_id,
                "failure_event_id": failure_event_id,
            }),
            0,
        )
        .await?;

        let bytes = serde_json::to_vec(&payload).map_err(|e| {
            error!(
                target: "sinex_metrics",
                metric = "event_engine.material_dlq_publish_failures_total",
                material_id = ?material_id,
                error = %e,
                "Failed to encode DLQ payload"
            );
            SinexError::serialization(format!("Failed to encode material DLQ payload: {e}"))
                .with_context(
                    "material_id",
                    material_id.map(|id| id.to_string()).unwrap_or_default(),
                )
        })?;

        let mut headers = async_nats::HeaderMap::new();
        insert_traffic_class_header(&mut headers, NatsTrafficClass::RawIngestDlq);
        transport::insert_semantic_transport_class_header(
            &mut headers,
            transport::Class::SourceMaterial,
        );

        ensure_nats_payload_fits("source-material DLQ entry", &self.dlq_subject, bytes.len())
            .map_err(|error| {
                let error = error.with_context(
                    "material_id",
                    material_id.map(|id| id.to_string()).unwrap_or_default(),
                );
                error!(
                    target: "sinex_metrics",
                    metric = "event_engine.material_dlq_publish_failures_total",
                    material_id = ?material_id,
                    error = %error,
                    "Failed to publish material DLQ entry"
                );
                error
            })?;

        let durable_failure_id_header = durable_failure_id.to_string();
        headers.insert(
            DURABLE_FAILURE_ID_HEADER,
            durable_failure_id_header.as_str(),
        );
        self.js
            .publish_with_headers(self.dlq_subject.clone(), headers, bytes.into())
            .await
            .map_err(|e| {
                error!(
                    target: "sinex_metrics",
                    metric = "event_engine.material_dlq_publish_failures_total",
                    material_id = ?material_id,
                    error = %e,
                    "Failed to publish material DLQ entry"
                );
                SinexError::network("Failed to publish material DLQ entry")
                    .with_context(
                        "material_id",
                        material_id.map(|id| id.to_string()).unwrap_or_default(),
                    )
                    .with_source(e)
            })?
            .await
            .map_err(|e| {
                error!(
                    target: "sinex_metrics",
                    metric = "event_engine.material_dlq_publish_failures_total",
                    material_id = ?material_id,
                    error = %e,
                    "Failed to confirm material DLQ entry"
                );
                SinexError::network("Failed to confirm material DLQ entry")
                    .with_context(
                        "material_id",
                        material_id.map(|id| id.to_string()).unwrap_or_default(),
                    )
                    .with_source(e)
            })?;

        debug!(material_id = ?material_id, failure_event_id = %failure_event_id, "Routed to DLQ");
        Ok(durable_failure_id)
    }

    /// Route a material failure to DLQ, then durably settle it as terminal-failed —
    /// but only if the DLQ publish actually succeeded. This is the "claimed" variant:
    /// the caller has already flipped `state.phase` to `Finalizing` under the
    /// per-material lock and is holding `resume_phase` to restore on failure.
    ///
    /// On DLQ failure, the in-memory phase is reverted to `resume_phase` and the DLQ
    /// error is propagated so the caller preserves retry state (redelivery / maintenance
    /// re-drive) instead of settling the material Failed with zero durable trace
    /// (sinex-wb1: a material that fails processing AND fails to DLQ must never
    /// silently vanish).
    pub(super) async fn route_material_error_and_finalize_failed_claimed(
        &self,
        material_id: Uuid,
        reason: &'static str,
        context: JsonValue,
        resume_phase: AssemblyPhase,
    ) -> EventEngineResult<()> {
        if let Err(error) = self.route_material_error(material_id, reason, context).await {
            warn!(
                material_id = %material_id,
                failure_reason = reason,
                error = %error,
                "DLQ publish failed for material failure; preserving retry state instead of settling terminal-failed"
            );
            self.revert_failure_cleanup_start(material_id, resume_phase)
                .await;
            return Err(error);
        }
        self.finalize_failed_material_claimed_checked(material_id, reason, resume_phase)
            .await
    }

    /// Route a material failure to DLQ, then durably settle it as terminal-failed —
    /// but only if the DLQ publish actually succeeded. This is the "unclaimed" variant
    /// used by callers (maintenance sweeps, the per-frame consumer) that have not
    /// pre-claimed `Finalizing` phase themselves; `finalize_failed_material` performs
    /// its own atomic claim internally. On DLQ failure the material is left untouched
    /// so the owning retry mechanism (next maintenance sweep, JetStream redelivery)
    /// picks it up again instead of it vanishing with no durable trace (sinex-wb1).
    pub(super) async fn route_material_error_then_finalize_failed(
        &self,
        material_id: Uuid,
        reason: impl Into<String>,
        context: JsonValue,
    ) -> EventEngineResult<()> {
        let reason = reason.into();
        self.route_material_error(material_id, reason.clone(), context)
            .await?;
        self.finalize_failed_material(material_id, &reason).await
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
    pub(super) async fn finalize_failed_material(
        &self,
        material_id: Uuid,
        reason: &str,
    ) -> EventEngineResult<()> {
        let FailureCleanupClaim::Claimed { resume_phase } =
            self.begin_failure_cleanup(material_id, reason).await
        else {
            return Ok(());
        };

        self
            .finalize_failed_material_claimed_checked(material_id, reason, resume_phase)
            .await
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
        if let Err(error) = self.route_material_error(material_id, reason, context).await {
            warn!(
                material_id = %material_id,
                failure_reason = reason,
                error = %error,
                "DLQ publish failed for material failure; preserving retry state instead of settling terminal-failed"
            );
            Self::revert_finalization_start(state_handle, end).await;
            return Err(error);
        }
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
        let recovery_state_handle = state_handle.clone();
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

            let outcome = std::panic::AssertUnwindSafe(async {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return,
                };
                if let Err(error) = assembler
                    .try_finalize_pending_end(
                        material_id,
                        state_handle,
                        PendingEndBehavior::Ignore,
                    )
                    .await
                {
                    warn!(
                        material_id = %material_id,
                        error = %error,
                        "Decoupled material finalize failed; retry state preserved for maintenance re-drive"
                    );
                }
            })
            .catch_unwind()
            .await;

            if outcome.is_err() {
                error!(
                    target: "sinex_metrics",
                    metric = "assembly_finalization_worker_panics_total",
                    material_id = %material_id,
                    "Detached material finalization worker panicked; restoring retry state"
                );
                assembler
                    .recover_panicked_finalize(material_id, recovery_state_handle)
                    .await;
            }
        });
    }

    async fn recover_panicked_finalize(
        &self,
        material_id: Uuid,
        state_handle: Arc<Mutex<super::state::AssemblerState>>,
    ) {
        let mut state = state_handle.lock().await;
        if state.phase == AssemblyPhase::Finalizing {
            state.restore_phase(AssemblyPhase::Accumulating);
            warn!(
                material_id = %material_id,
                pending_end = state.pending_end.is_some(),
                "Recovered panicked material finalization into retryable assembly state"
            );
        }
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
                    state.mark_finalizing();
                    drop(state);
                    self.route_material_error_and_finalize_failed_claimed(
                        material_id,
                        "material_end_timestamp_invalid",
                        context,
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
                state.mark_finalizing();
                drop(state);
                self.route_material_error_and_finalize_failed_claimed(
                    material_id,
                    "material assembly corruption detected",
                    ctx,
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
            state.mark_finalizing();
            let end = state.pending_end.clone().ok_or_else(|| {
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

        let mut finalize_metadata = match build_finalize_metadata(
            &final_state,
            &end.metadata,
            ended_at,
            end.total_size_bytes,
            &end.content_hash,
        ) {
            Ok(metadata) => metadata,
            Err(error) => {
                if let Err(dlq_error) = self
                    .route_material_error(
                        material_id,
                        "material_finalize_metadata_invalid",
                        serde_json::json!({ "error": error.to_string() }),
                    )
                    .await
                {
                    warn!(
                        material_id = %material_id,
                        error = %dlq_error,
                        "Failed to publish material DLQ entry for invalid finalize metadata; original error still preserves retry state"
                    );
                }
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
                if let Err(dlq_error) = self
                    .route_material_error(
                        material_id,
                        "content_store_import_failed",
                        serde_json::json!({ "error": e.to_string() }),
                    )
                    .await
                {
                    warn!(
                        material_id = %material_id,
                        error = %dlq_error,
                        "Failed to publish material DLQ entry for content-store import failure; original error still preserves retry state"
                    );
                }
                Self::revert_finalization_start(&state_handle, end).await;
                return Err(e);
            }
        };

        // Persist a canonical manifest beside the exact material bytes before the
        // registry transaction. The registry reference is committed atomically
        // with the blob/material rows below; an interrupted transaction leaves a
        // recoverable CAS object for the lifecycle reconciler rather than losing
        // the only copy of its metadata.
        let manifest_key = match self
            .persist_material_manifest(
                &final_state,
                &end.content_hash,
                end.total_size_bytes,
                &finalize_metadata,
                ended_at,
            )
            .await
        {
            Ok(key) => key,
            Err(error) => {
                if let Err(dlq_error) = self
                    .route_material_error(
                        material_id,
                        "material_manifest_store_failed",
                        serde_json::json!({ "error": error.to_string() }),
                    )
                    .await
                {
                    warn!(
                        material_id = %material_id,
                        error = %dlq_error,
                        "Failed to publish manifest-store DLQ entry; preserving retry state"
                    );
                }
                Self::revert_finalization_start(&state_handle, end).await;
                return Err(error);
            }
        };
        if let Some(object) = finalize_metadata.as_object_mut() {
            object.insert(
                "material_manifest".to_string(),
                serde_json::json!({
                    "manifest_type": sinex_primitives::MATERIAL_MANIFEST_V1,
                    "content_key": manifest_key.key,
                }),
            );
        }

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
                } else if let Err(dlq_error) = self
                    .route_material_error(
                        material_id,
                        "material_persist_failed",
                        serde_json::json!({ "error": e.to_string() }),
                    )
                    .await
                {
                    warn!(
                        material_id = %material_id,
                        error = %dlq_error,
                        "Failed to publish material DLQ entry for persist failure; original error still preserves retry state"
                    );
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

    async fn persist_material_manifest(
        &self,
        final_state: &super::state::FinalizationState,
        content_hash: &str,
        total_size_bytes: i64,
        metadata: &JsonValue,
        ended_at: Timestamp,
    ) -> EventEngineResult<crate::runtime::content_store::ContentStoreKey> {
        let total_size_bytes = u64::try_from(total_size_bytes).map_err(|error| {
            SinexError::validation("material size cannot be represented in a manifest")
                .with_std_error(&error)
        })?;
        let manifest = MaterialManifestV1::from_capture(
            final_state.material_id,
            final_state.source_identifier.clone(),
            final_state.material_kind.clone(),
            content_hash,
            total_size_bytes,
            metadata.clone(),
            final_state.started_at.format_rfc3339(),
            ended_at.format_rfc3339(),
        );
        manifest.validate().map_err(|error| {
            SinexError::validation("generated material manifest failed validation")
                .with_context("reason", error)
        })?;
        let bytes = manifest.canonical_bytes().map_err(|error| {
            SinexError::serialization("failed to encode material manifest")
                .with_std_error(&error)
        })?;

        let parent = final_state.temp_path.parent().ok_or_else(|| {
            SinexError::io("material staging path has no parent for manifest staging")
        })?;
        let manifest_path = parent.join(format!(
            "material-manifest-{}.json",
            final_state.material_id
        ));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&manifest_path)
                .await
                .map_err(SinexError::io)?;
            file.write_all(&bytes).await.map_err(SinexError::io)?;
            file.sync_all().await.map_err(SinexError::io)?;
            drop(file);

            let utf8_path = Utf8PathBuf::from_path_buf(manifest_path.clone()).map_err(|path| {
                SinexError::io(format!(
                    "manifest staging path is not valid UTF-8: {}",
                    path.display()
                ))
            })?;
            self.content_store.store_file(&utf8_path).await
        }
        .await;
        if let Err(error) = tokio::fs::remove_file(&manifest_path).await {
            warn!(
                path = %manifest_path.display(),
                error = %error,
                "Failed to remove temporary material manifest after CAS import"
            );
        }
        result.map_err(|error| {
            SinexError::io("material manifest CAS import failed").with_source(error)
        })
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
