//! Material finalization methods for `MaterialAssembler`.
//!
//! This module contains database finalization, blob management, error routing,
//! and cleanup logic that executes when a material assembly completes (or fails).

use serde::Serialize;
use sinex_db::{
    models::blob::Blob,
    repositories::{DbPoolExt, TemporalLedgerEntry, material_status},
};
use sinex_node_sdk::annex::AnnexKey;
use sinex_primitives::Timestamp;
use sinex_primitives::{Id, JsonValue, Uuid};
use sinex_schema::schema::records::SourceMaterialRecord;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::{IngestdResult, SinexError};

use super::state::AssemblyPhase;
use super::{FinalizationState, MaterialAssembler, MaterialEndMessage};
use std::{str::FromStr, sync::Arc};

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy)]
pub(super) enum PendingEndBehavior {
    Error,
    Ignore,
}

enum FinalizationCommitOutcome {
    Landed,
    NotLanded,
    Unknown(SinexError),
}

fn finalization_commit_outcome_unknown(error: &SinexError) -> bool {
    error
        .context_map()
        .get("commit_outcome")
        .is_some_and(|value| value == "unknown")
}

fn finalization_unknown_commit_error(
    commit_error: SinexError,
    reconcile_error: &SinexError,
    material_id: Uuid,
    annex_key: &AnnexKey,
    final_status: &str,
) -> SinexError {
    commit_error
        .with_context("commit_outcome", "unknown")
        .with_context(
            "recovery",
            "finalization retry is safe once database reachability is restored",
        )
        .with_context("retry_state_preserved", "true")
        .with_context("terminal_failure_routed", "false")
        .with_context("material_id", material_id.to_string())
        .with_context("annex_key", annex_key.key.clone())
        .with_context("final_status", final_status.to_string())
        .with_context("reconcile_error", reconcile_error.to_string())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "Internal error helper: error chain context"
)]
fn rollback_finalization_failure(
    original_error: SinexError,
    rollback_error: impl std::fmt::Display,
    stage: &'static str,
) -> SinexError {
    SinexError::database("Failed to rollback material finalization transaction")
        .with_source(rollback_error.to_string())
        .with_context("stage", stage)
        .with_context("original_error", original_error.to_string())
        .with_operation("persist_finalized_material")
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

    /// Insert or fetch blob metadata for the assembled material.
    ///
    /// # BLAKE3 Hash Collision Handling
    ///
    /// This function uses BLAKE3 hashes for content addressing. BLAKE3 collision resistance
    /// makes collisions cryptographically infeasible (2^128 security for 256-bit hashes).
    ///
    /// Collision handling strategy:
    /// - Primary deduplication: BLAKE3 checksum when present, matching the database uniqueness
    ///   invariant.
    /// - Legacy/no-checksum rows use the storage backend key (`annex_backend`, `content_hash`).
    /// - If a collision occurred (astronomically unlikely), the existing blob would be reused.
    /// - This is acceptable: a true collision means identical content by cryptographic assumption.
    ///
    /// The theoretical collision risk is negligible compared to hardware/cosmic ray bit flips.
    pub(super) async fn upsert_blob_with_executor(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        state: &FinalizationState,
        annex_key: &AnnexKey,
        content_hash: &str,
    ) -> IngestdResult<Id<Blob>> {
        let repo = self.pool.blobs();

        // No pre-check: insert_with_executor handles duplicates via unique-violation
        // fallback. A separate pre-read outside the transaction would create a TOCTOU
        // window where a concurrent delete could invalidate the returned blob ID.
        let metadata = serde_json::json!({
            "material_id": state.material_id.to_string(),
            "source_identifier": state.source_identifier,
            "material_kind": state.material_kind,
            "total_slices": state.slice_count,
        });

        let blob = Blob::builder()
            .annex_backend(annex_key.backend.clone())
            .content_hash(annex_key.hash.clone())
            .original_filename(state.source_identifier.clone())
            .size_bytes(annex_key.size as i64)
            .checksum_blake3(content_hash.to_string())
            .metadata(metadata)
            .build();

        let stored = repo
            .insert_with_executor(&mut **tx, blob)
            .await
            .map_err(|e| {
                error!(
                    material_id = %state.material_id,
                    backend = %annex_key.backend,
                    hash = %annex_key.hash,
                    size = annex_key.size,
                    error = %e,
                    error_debug = ?e,
                    "Failed to insert blob metadata"
                );
                SinexError::database("Failed to insert blob metadata").with_source(e)
            })?;

        Ok(stored.id)
    }

    /// Finalize source material registry and ledger
    pub(super) async fn finalize_material_record_with_executor(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        state: &FinalizationState,
        final_status: &str,
        blob_id: Id<Blob>,
        total_size_bytes: i64,
        metadata: JsonValue,
    ) -> IngestdResult<()> {
        let repo = self.pool.source_materials();
        let id: Id<SourceMaterialRecord> = Id::from_uuid(state.material_id);

        repo.update_metadata_with_executor(&mut **tx, id, metadata.clone())
            .await
            .map_err(|e| {
                SinexError::database("Failed to update material metadata").with_source(e)
            })?;

        let encoding_hint = metadata
            .as_object()
            .and_then(|map| map.get("encoding"))
            .and_then(|value| value.as_str())
            .map(std::string::ToString::to_string);
        let content_preview_hint = metadata
            .as_object()
            .and_then(|map| map.get("content_preview"))
            .and_then(|value| value.as_str())
            .map(std::string::ToString::to_string);

        repo.finalize_in_flight_as(
            &mut **tx,
            Id::from_uuid(state.material_id),
            final_status,
            Some(blob_id),
            encoding_hint.as_deref(),
            content_preview_hint.clone(),
            Some(total_size_bytes),
        )
        .await
        .map_err(|e| SinexError::database("Failed to finalize material").with_source(e))
    }

    /// Append a `realtime_capture` entry in `raw.temporal_ledger` at finalization.
    ///
    /// This records the precise byte coverage of the assembled material. A coarser
    /// `staged_at` entry is written earlier at begin-time by
    /// [`record_staged_at_ledger_entry`] so that `LedgerReader::derive_ts_orig()`
    /// never needs to fall back to ephemeral `Timestamp::now()`.
    pub(super) async fn record_ledger_entry_with_executor(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        state: &FinalizationState,
    ) -> IngestdResult<()> {
        let entry = TemporalLedgerEntry::realtime_capture(
            state.material_id,
            state.expected_offset,
            state.started_at,
        );

        self.pool
            .source_materials()
            .append_temporal_ledger_with_executor(&mut **tx, entry)
            .await
            .map_err(|e| {
                SinexError::database("Failed to append temporal ledger entry").with_source(e)
            })?;

        Ok(())
    }

    async fn cleanup_annex_import_failure(&self, annex_key: &AnnexKey) {
        match self
            .pool
            .blobs()
            .get_by_content(&annex_key.backend, &annex_key.hash, annex_key.size as i64)
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(error) = self.annex.drop_content(&annex_key.key, true).await {
                    warn!(
                        annex_key = %annex_key.key,
                        error = %error,
                        "Failed to roll back annex content after transactional finalization failure"
                    );
                }
            }
            Err(error) => {
                warn!(
                    annex_key = %annex_key.key,
                    error = %error,
                    "Failed to inspect blob metadata before annex rollback"
                );
            }
        }
    }

    async fn finalization_commit_landed(
        &self,
        final_state: &FinalizationState,
        annex_key: &AnnexKey,
        final_status: &str,
    ) -> IngestdResult<bool> {
        let material = self
            .pool
            .source_materials()
            .get_by_id(Id::from_uuid(final_state.material_id))
            .await
            .map_err(|error| {
                SinexError::database("Failed to inspect material state after commit error")
                    .with_source(error)
            })?;

        let Some(material) = material else {
            return Ok(false);
        };

        if material.status != final_status {
            return Ok(false);
        }

        let Some(material_blob_id) = material.optional_blob_id else {
            return Ok(false);
        };

        let blob = self
            .pool
            .blobs()
            .get_by_content(&annex_key.backend, &annex_key.hash, annex_key.size as i64)
            .await
            .map_err(|error| {
                SinexError::database("Failed to inspect blob state after commit error")
                    .with_source(error)
            })?;

        Ok(blob.is_some_and(|blob| *blob.id.as_uuid() == material_blob_id))
    }

    async fn finalization_commit_outcome(
        &self,
        final_state: &FinalizationState,
        annex_key: &AnnexKey,
        final_status: &str,
    ) -> FinalizationCommitOutcome {
        match self
            .finalization_commit_landed(final_state, annex_key, final_status)
            .await
        {
            Ok(true) => FinalizationCommitOutcome::Landed,
            Ok(false) => FinalizationCommitOutcome::NotLanded,
            Err(error) => FinalizationCommitOutcome::Unknown(error),
        }
    }

    async fn ensure_material_record_present_with_executor(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        final_state: &FinalizationState,
    ) -> IngestdResult<()> {
        let repo = self.pool.source_materials();
        let material_id = Id::from_uuid(final_state.material_id);

        if let Some(existing) = repo
            .get_by_id_with_executor(&mut **tx, material_id)
            .await
            .map_err(|error| {
                SinexError::database("Failed to inspect source material before finalization")
                    .with_source(error)
            })?
        {
            if existing.source_identifier != final_state.source_identifier {
                return Err(SinexError::invalid_state(
                    "Source material source_identifier changed before finalization",
                )
                .with_context("material_id", final_state.material_id.to_string())
                .with_context("expected_source_identifier", &final_state.source_identifier)
                .with_context("actual_source_identifier", &existing.source_identifier));
            }
            return Ok(());
        }

        repo.register_external_in_flight_with_executor(
            &mut **tx,
            final_state.material_id,
            &final_state.material_kind,
            Some(&final_state.source_identifier),
            final_state.metadata.clone(),
            final_state.started_at,
        )
        .await
        .map(|_| ())
        .map_err(|error| {
            SinexError::database("Failed to register source material for finalization")
                .with_source(error)
        })
    }

    async fn persist_finalized_material(
        &self,
        final_state: &FinalizationState,
        annex_key: &AnnexKey,
        end: &MaterialEndMessage,
        finalize_metadata: JsonValue,
        final_status: &str,
    ) -> IngestdResult<()> {
        match self
            .finalization_commit_outcome(final_state, annex_key, final_status)
            .await
        {
            FinalizationCommitOutcome::Landed => {
                info!(
                    material_id = %final_state.material_id,
                    annex_key = %annex_key.key,
                    "Material finalization already persisted; skipping duplicate finalization"
                );
                return Ok(());
            }
            FinalizationCommitOutcome::NotLanded => {}
            FinalizationCommitOutcome::Unknown(error) => {
                warn!(
                    material_id = %final_state.material_id,
                    annex_key = %annex_key.key,
                    error = %error,
                    "Unable to confirm material state before finalization; attempting transactional write"
                );
            }
        }

        let mut tx = self.pool.begin().await.map_err(|e| {
            SinexError::database("Failed to begin material finalization transaction").with_source(e)
        })?;

        if let Err(error) = self
            .ensure_material_record_present_with_executor(&mut tx, final_state)
            .await
        {
            let error = match tx.rollback().await {
                Ok(()) => error,
                Err(rollback_error) => rollback_finalization_failure(
                    error,
                    rollback_error,
                    "ensure_material_record_present",
                ),
            };
            self.cleanup_annex_import_failure(annex_key).await;
            return Err(error);
        }

        let blob_id = match self
            .upsert_blob_with_executor(&mut tx, final_state, annex_key, &end.content_hash)
            .await
        {
            Ok(id) => id,
            Err(error) => {
                let error = match tx.rollback().await {
                    Ok(()) => error,
                    Err(rollback_error) => {
                        rollback_finalization_failure(error, rollback_error, "upsert_blob")
                    }
                };
                self.cleanup_annex_import_failure(annex_key).await;
                return Err(error);
            }
        };

        if let Err(error) = self
            .finalize_material_record_with_executor(
                &mut tx,
                final_state,
                final_status,
                blob_id,
                end.total_size_bytes,
                finalize_metadata,
            )
            .await
        {
            let error = match tx.rollback().await {
                Ok(()) => error,
                Err(rollback_error) => {
                    rollback_finalization_failure(error, rollback_error, "finalize_material_record")
                }
            };
            self.cleanup_annex_import_failure(annex_key).await;
            return Err(error);
        }

        if let Err(error) = self
            .record_ledger_entry_with_executor(&mut tx, final_state)
            .await
        {
            let error = match tx.rollback().await {
                Ok(()) => error,
                Err(rollback_error) => {
                    rollback_finalization_failure(error, rollback_error, "record_ledger_entry")
                }
            };
            self.cleanup_annex_import_failure(annex_key).await;
            return Err(error);
        }

        match tx.commit().await {
            Ok(()) => Ok(()),
            Err(error) => {
                let commit_error =
                    SinexError::database("Failed to commit material finalization transaction")
                        .with_source(error);

                match self
                    .finalization_commit_outcome(final_state, annex_key, final_status)
                    .await
                {
                    FinalizationCommitOutcome::Landed => {
                        warn!(
                            material_id = %final_state.material_id,
                            annex_key = %annex_key.key,
                            "Material finalization commit returned an error, but the committed state was reconciled successfully"
                        );
                        Ok(())
                    }
                    FinalizationCommitOutcome::NotLanded => {
                        self.cleanup_annex_import_failure(annex_key).await;
                        Err(commit_error)
                    }
                    FinalizationCommitOutcome::Unknown(reconcile_error) => {
                        warn!(
                            material_id = %final_state.material_id,
                            annex_key = %annex_key.key,
                            error = %reconcile_error,
                            "Failed to reconcile material finalization after commit error"
                        );
                        Err(finalization_unknown_commit_error(
                            commit_error,
                            &reconcile_error,
                            final_state.material_id,
                            annex_key,
                            final_status,
                        ))
                    }
                }
            }
        }
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
                if let Err(e) = self
                    .nats_client
                    .publish(self.dlq_subject.clone(), bytes.into())
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
                )));
            }

            // Complete: transition into finalization while holding the per-material lock so
            // no more slice writes can mutate the state we are about to snapshot.
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

        let annex_key = match self.import_into_content_store(&final_state).await {
            Ok(result) => result,
            Err(e) => {
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

        if let Err(e) = self
            .persist_finalized_material(
                &final_state,
                &annex_key,
                &end,
                finalize_metadata,
                final_status,
            )
            .await
        {
            if finalization_commit_outcome_unknown(&e) {
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
                annex_key = %annex_key.key,
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
                annex_key = %annex_key.key,
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
            if self.material_is_terminal(material_id).await? {
                info!(
                    material_id = %material_id,
                    "End message received after completion; skipping placeholder state"
                );
                return Ok(());
            }
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
            if state.phase == AssemblyPhase::Finalizing {
                debug!(material_id = %material_id, "Ignoring end message while finalizing");
                return Ok(());
            }
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
    use crate::MaterialReadySet;
    use crate::material_assembler::{io, state};
    use camino::Utf8PathBuf;
    use serde_json::json;
    use sinex_db::repositories::{DbPoolExt, source_materials::status};
    use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
    use std::sync::Arc;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;
    use xtask::sandbox::prelude::*;

    async fn test_assembler(
        ctx: &TestContext,
    ) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
        let annex_dir = tempfile::tempdir()?;
        let repo_path = Utf8PathBuf::from_path_buf(annex_dir.path().to_path_buf())
            .map_err(|_| color_eyre::eyre::eyre!("tempdir path is not valid utf-8"))?;
        GitAnnex::init(&repo_path, Some("finalize-test")).await?;
        let annex = Arc::new(GitAnnex::new(AnnexConfig {
            repo_path,
            num_copies: None,
            large_files: None,
        })?);

        let state_dir = tempfile::tempdir()?;
        let assembler = MaterialAssembler::new(
            ctx.nats_client(),
            ctx.pool.clone(),
            annex,
            state_dir.path().to_path_buf(),
            Some(ctx.pipeline_namespace().prefix().to_string()),
            1_000,
            Some(MaterialReadySet::default()),
            100,
            512 * 1024 * 1024,
            300,
            3_600,
            90,
        )?;

        Ok((assembler, annex_dir, state_dir))
    }

    #[sinex_test]
    async fn finalize_failed_material_skips_material_already_finalizing(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, _state_dir) = test_assembler(&ctx).await?;
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
        let (assembler, _annex_dir, _state_dir) = test_assembler(&ctx).await?;
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
        let (assembler, _annex_dir, _state_dir) = test_assembler(&ctx).await?;
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
        let (assembler, _annex_dir, _state_dir) = test_assembler(&ctx).await?;
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
        let (assembler, _annex_dir, _state_dir) = test_assembler(&ctx).await?;
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
        let (assembler, _annex_dir, _state_dir) = test_assembler(&ctx).await?;
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
    async fn finalization_commit_landed_detects_completed_material_with_blob(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let annex_key = AnnexKey {
            key: "SHA256E-s4--hash".to_string(),
            backend: "SHA256E".to_string(),
            size: 4,
            hash: "hash".to_string(),
        };

        let blob = ctx
            .pool
            .blobs()
            .insert(
                Blob::builder()
                    .annex_backend(annex_key.backend.clone())
                    .content_hash(annex_key.hash.clone())
                    .original_filename("material.bin".to_string())
                    .size_bytes(annex_key.size as i64)
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
                Some("test://commit-landed"),
                json!({}),
                Timestamp::now(),
            )
            .await?;
        ctx.pool
            .source_materials()
            .finalize_in_flight(
                Id::from_uuid(material_id),
                Some(blob.id),
                None,
                None,
                Some(annex_key.size as i64),
            )
            .await?;

        let final_state = FinalizationState {
            material_id,
            temp_path: state_dir.path().join("material.bin"),
            expected_offset: annex_key.size as i64,
            slice_count: 1,
            buffered_count: 0,
            metadata: json!({}),
            material_kind: "test".to_string(),
            source_identifier: "test://commit-landed".to_string(),
            started_at: Timestamp::now(),
        };

        assert!(
            assembler
                .finalization_commit_landed(&final_state, &annex_key, status::COMPLETED)
                .await?,
            "completed material with matching blob metadata should reconcile as committed"
        );
        Ok(())
    }

    #[sinex_test]
    async fn finalization_commit_landed_detects_cancelled_material_with_blob(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let annex_key = AnnexKey {
            key: "SHA256E-s4--hash".to_string(),
            backend: "SHA256E".to_string(),
            size: 4,
            hash: "hash".to_string(),
        };

        let blob = ctx
            .pool
            .blobs()
            .insert(
                Blob::builder()
                    .annex_backend(annex_key.backend.clone())
                    .content_hash(annex_key.hash.clone())
                    .original_filename("material.bin".to_string())
                    .size_bytes(annex_key.size as i64)
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
                Some("test://commit-cancelled"),
                json!({ "cancelled": true }),
                Timestamp::now(),
            )
            .await?;
        ctx.pool
            .source_materials()
            .finalize_in_flight_as(
                &ctx.pool,
                Id::from_uuid(material_id),
                status::CANCELLED,
                Some(blob.id),
                None,
                None,
                Some(annex_key.size as i64),
            )
            .await?;

        let final_state = FinalizationState {
            material_id,
            temp_path: state_dir.path().join("material.bin"),
            expected_offset: annex_key.size as i64,
            slice_count: 1,
            buffered_count: 0,
            metadata: json!({ "cancelled": true }),
            material_kind: "test".to_string(),
            source_identifier: "test://commit-cancelled".to_string(),
            started_at: Timestamp::now(),
        };

        assert!(
            assembler
                .finalization_commit_landed(&final_state, &annex_key, status::CANCELLED)
                .await?,
            "cancelled material with matching blob metadata should reconcile as committed"
        );
        Ok(())
    }

    #[sinex_test]
    async fn finalization_commit_landed_rejects_non_terminal_material(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let annex_key = AnnexKey {
            key: "SHA256E-s4--hash".to_string(),
            backend: "SHA256E".to_string(),
            size: 4,
            hash: "hash".to_string(),
        };

        ctx.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                "test",
                Some("test://commit-pending"),
                json!({}),
                Timestamp::now(),
            )
            .await?;

        let final_state = FinalizationState {
            material_id,
            temp_path: state_dir.path().join("material.bin"),
            expected_offset: annex_key.size as i64,
            slice_count: 1,
            buffered_count: 0,
            metadata: json!({}),
            material_kind: "test".to_string(),
            source_identifier: "test://commit-pending".to_string(),
            started_at: Timestamp::now(),
        };

        assert!(
            !assembler
                .finalization_commit_landed(&final_state, &annex_key, status::COMPLETED)
                .await?,
            "non-terminal material state should not reconcile as a landed commit"
        );
        Ok(())
    }

    #[sinex_test]
    async fn persist_finalized_material_is_idempotent_after_commit_lands(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::from_uuid(material_id);
        let annex_key = AnnexKey {
            key: "SHA256E-s4--hash".to_string(),
            backend: "SHA256E".to_string(),
            size: 4,
            hash: "hash".to_string(),
        };

        let blob = ctx
            .pool
            .blobs()
            .insert(
                Blob::builder()
                    .annex_backend(annex_key.backend.clone())
                    .content_hash(annex_key.hash.clone())
                    .original_filename("material.bin".to_string())
                    .size_bytes(annex_key.size as i64)
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
                Some(annex_key.size as i64),
            )
            .await?;
        ctx.pool
            .source_materials()
            .append_temporal_ledger(TemporalLedgerEntry::realtime_capture(
                material_id,
                annex_key.size as i64,
                Timestamp::now(),
            ))
            .await?;

        let final_state = FinalizationState {
            material_id,
            temp_path: state_dir.path().join("material.bin"),
            expected_offset: annex_key.size as i64,
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
            total_size_bytes: annex_key.size as i64,
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

        assembler
            .persist_finalized_material(
                &final_state,
                &annex_key,
                &end,
                json!({}),
                status::COMPLETED,
            )
            .await?;

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
        let (assembler, _annex_dir, _state_dir) = test_assembler(&ctx).await?;
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
    async fn persist_finalized_material_reuses_existing_blob_inside_transaction(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::<SourceMaterialRecord>::from_uuid(material_id);
        let annex_key = AnnexKey {
            backend: "SHA256E".to_string(),
            hash: "existing-blob-hash".to_string(),
            size: 32,
            key: "SHA256E-s32--existing-blob-hash".to_string(),
        };

        let existing_blob = ctx
            .pool
            .blobs()
            .insert(
                Blob::builder()
                    .annex_backend(annex_key.backend.clone())
                    .content_hash(annex_key.hash.clone())
                    .original_filename("existing-material.bin".to_string())
                    .size_bytes(annex_key.size as i64)
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
            expected_offset: annex_key.size as i64,
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
            total_size_bytes: annex_key.size as i64,
            content_hash: "existing-blob-blake3".to_string(),
            metadata: json!({}),
            ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
        };

        assembler
            .persist_finalized_material(
                &final_state,
                &annex_key,
                &end,
                json!({}),
                status::COMPLETED,
            )
            .await?;

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
    async fn persist_finalized_material_reuses_existing_blob_by_blake3_inside_transaction(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
        let material_id = Uuid::now_v7();
        let material_id_typed = Id::<SourceMaterialRecord>::from_uuid(material_id);
        let content_hash = "existing-blob-blake3";
        let annex_key = AnnexKey {
            backend: "SINEXBLAKE3".to_string(),
            hash: content_hash.to_string(),
            size: 32,
            key: format!("SINEXBLAKE3-s32--{content_hash}"),
        };

        let existing_blob = ctx
            .pool
            .blobs()
            .insert(
                Blob::builder()
                    .annex_backend("SHA256E".to_string())
                    .content_hash("existing-sha256-hash".to_string())
                    .original_filename("existing-material.bin".to_string())
                    .size_bytes(annex_key.size as i64)
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
            expected_offset: annex_key.size as i64,
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
            total_size_bytes: annex_key.size as i64,
            content_hash: content_hash.to_string(),
            metadata: json!({}),
            ended_at: sinex_primitives::temporal::format_rfc3339(Timestamp::now()),
        };

        assembler
            .persist_finalized_material(
                &final_state,
                &annex_key,
                &end,
                json!({}),
                status::COMPLETED,
            )
            .await?;

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

    #[sinex_test]
    async fn rollback_finalization_failure_preserves_original_error_context() -> TestResult<()> {
        let error = rollback_finalization_failure(
            SinexError::validation("original finalize failure"),
            "rollback broke too",
            "record_ledger_entry",
        );

        let rendered = error.to_string();
        assert!(rendered.contains("Failed to rollback material finalization transaction"));
        assert!(rendered.contains("rollback broke too"));
        assert!(rendered.contains("original finalize failure"));
        assert!(rendered.contains("record_ledger_entry"));
        Ok(())
    }

    #[sinex_test]
    async fn finalization_unknown_commit_error_preserves_retry_context() -> TestResult<()> {
        let annex_key = AnnexKey {
            key: "SHA256E-s4--retry".to_string(),
            backend: "SHA256E".to_string(),
            size: 4,
            hash: "retry".to_string(),
        };
        let error = finalization_unknown_commit_error(
            SinexError::database("commit failed"),
            &SinexError::database("reconcile failed"),
            Uuid::now_v7(),
            &annex_key,
            status::COMPLETED,
        );

        assert!(finalization_commit_outcome_unknown(&error));
        assert_eq!(
            error.context_map().get("retry_state_preserved"),
            Some(&"true".to_string())
        );
        assert_eq!(
            error.context_map().get("terminal_failure_routed"),
            Some(&"false".to_string())
        );
        assert_eq!(
            error.context_map().get("final_status"),
            Some(&status::COMPLETED.to_string())
        );
        assert_eq!(error.context_map().get("annex_key"), Some(&annex_key.key),);
        assert!(
            error
                .context_map()
                .get("reconcile_error")
                .is_some_and(|value| value.contains("reconcile failed"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn finalization_commit_outcome_unknown_ignores_unflagged_errors() -> TestResult<()> {
        let error = SinexError::database("ordinary failure");
        assert!(
            !finalization_commit_outcome_unknown(&error),
            "only explicitly flagged commit-reconciliation failures should preserve retry state"
        );
        Ok(())
    }
}
