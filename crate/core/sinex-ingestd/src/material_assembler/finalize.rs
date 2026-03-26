//! Material finalization methods for `MaterialAssembler`.
//!
//! This module contains database finalization, blob management, error routing,
//! and cleanup logic that executes when a material assembly completes (or fails).

use serde::Serialize;
use sinex_db::{
    models::blob::Blob,
    repositories::{DbPoolExt, TemporalLedgerEntry},
};
use sinex_node_sdk::annex::AnnexKey;
use sinex_primitives::Timestamp;
use sinex_primitives::{Id, JsonValue, Uuid};
use sinex_schema::schema::records::SourceMaterialRecord;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::{IngestdResult, SinexError};

use super::state::AssemblyPhase;
use super::{FinalizationState, MaterialAssembler, MaterialEndMessage};
use std::{str::FromStr, sync::Arc};

#[derive(Clone, Copy)]
pub(super) enum PendingEndBehavior {
    Error,
    Ignore,
}

/// DLQ payload for material failures
#[derive(Debug, Serialize)]
struct MaterialDlqPayload {
    material_id: String,
    error: String,
    context: JsonValue,
    failed_at: Timestamp,
}

impl MaterialAssembler {
    async fn begin_failure_cleanup(&self, material_id: Uuid, reason: &str) -> bool {
        if let Some(state_handle) = self.get_state_handle(&material_id).await {
            let mut state = state_handle.lock().await;
            if state.phase == AssemblyPhase::Finalizing {
                debug!(
                    material_id = %material_id,
                    failure_reason = reason,
                    "Skipping failed-material cleanup because terminal transition is already in progress"
                );
                return false;
            }
            state.phase = AssemblyPhase::Finalizing;
            return true;
        }

        match self.material_is_terminal(material_id).await {
            Ok(true) => {
                debug!(
                    material_id = %material_id,
                    failure_reason = reason,
                    "Skipping failed-material cleanup because material is already terminal"
                );
                false
            }
            Ok(false) => true,
            Err(error) => {
                warn!(
                    material_id = %material_id,
                    failure_reason = reason,
                    error = %error,
                    "Failed to confirm material terminal state before failure cleanup; proceeding"
                );
                true
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

    /// Insert or fetch blob metadata for the assembled material
    ///
    /// # BLAKE3 Hash Collision Handling
    ///
    /// This function uses BLAKE3 hashes for content addressing. BLAKE3 collision resistance
    /// makes collisions cryptographically infeasible (2^128 security for 256-bit hashes).
    ///
    /// Collision handling strategy:
    /// - Primary deduplication: git-annex natural key (backend, hash, size)
    /// - BLAKE3 checksum stored for verification but not uniqueness enforcement
    /// - If a collision occurred (astronomically unlikely), the existing blob would be reused
    ///   since git-annex guarantees content identity via its own hash
    /// - This is acceptable: a true collision means identical content by cryptographic assumption
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

        if let Some(existing) = repo
            .get_by_content(&annex_key.backend, &annex_key.hash, annex_key.size as i64)
            .await
            .map_err(|e| {
                error!(
                    material_id = %state.material_id,
                    backend = %annex_key.backend,
                    hash = %annex_key.hash,
                    size = annex_key.size,
                    error = %e,
                    error_debug = ?e,
                    "Failed to query blob store"
                );
                SinexError::database("Failed to query blob store").with_source(e)
            })?
        {
            return Ok(existing.id);
        }

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

        repo.finalize_in_flight_with_executor(
            &mut **tx,
            Id::from_uuid(state.material_id),
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

        if material.status != sinex_db::repositories::source_materials::status::COMPLETED {
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

    async fn persist_finalized_material(
        &self,
        final_state: &FinalizationState,
        annex_key: &AnnexKey,
        end: &MaterialEndMessage,
        finalize_metadata: JsonValue,
    ) -> IngestdResult<()> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            SinexError::database("Failed to begin material finalization transaction")
                .with_source(e)
        })?;

        let repo = self.pool.source_materials();
        if let Err(error) = repo
            .register_external_in_flight_with_executor(
                &mut *tx,
                final_state.material_id,
                &final_state.material_kind,
                Some(&final_state.source_identifier),
                final_state.metadata.clone(),
                final_state.started_at,
            )
            .await
        {
            let _ = tx.rollback().await;
            self.cleanup_annex_import_failure(annex_key).await;
            return Err(
                SinexError::database("Failed to register source material for finalization")
                    .with_source(error),
            );
        }

        let blob_id = match self
            .upsert_blob_with_executor(&mut tx, final_state, annex_key, &end.content_hash)
            .await
        {
            Ok(id) => id,
            Err(error) => {
                let _ = tx.rollback().await;
                self.cleanup_annex_import_failure(annex_key).await;
                return Err(error);
            }
        };

        if let Err(error) = self
            .finalize_material_record_with_executor(
                &mut tx,
                final_state,
                blob_id,
                end.total_size_bytes,
                finalize_metadata,
            )
            .await
        {
            let _ = tx.rollback().await;
            self.cleanup_annex_import_failure(annex_key).await;
            return Err(error);
        }

        if let Err(error) = self.record_ledger_entry_with_executor(&mut tx, final_state).await {
            let _ = tx.rollback().await;
            self.cleanup_annex_import_failure(annex_key).await;
            return Err(error);
        }

        match tx.commit().await {
            Ok(()) => Ok(()),
            Err(error) => {
                let commit_error = SinexError::database(
                    "Failed to commit material finalization transaction",
                )
                .with_source(error);

                match self.finalization_commit_landed(final_state, annex_key).await {
                    Ok(true) => {
                        warn!(
                            material_id = %final_state.material_id,
                            annex_key = %annex_key.key,
                            "Material finalization commit returned an error, but the committed state was reconciled successfully"
                        );
                        Ok(())
                    }
                    Ok(false) => {
                        self.cleanup_annex_import_failure(annex_key).await;
                        Err(commit_error)
                    }
                    Err(reconcile_error) => {
                        warn!(
                            material_id = %final_state.material_id,
                            annex_key = %annex_key.key,
                            error = %reconcile_error,
                            "Failed to reconcile material finalization after commit error"
                        );
                        Err(commit_error)
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

        self.pool
            .source_materials()
            .append_temporal_ledger(entry)
            .await
            .map_err(|e| {
                SinexError::database("Failed to append staged_at temporal ledger entry")
                    .with_source(e)
            })?;

        debug!(material_id = %material_id, "Wrote staged_at ledger entry at begin time");
        Ok(())
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

    /// Mark material as failed in the database to prevent reprocessing
    pub(super) async fn mark_material_failed(&self, material_id: Uuid, reason: &str) {
        let id: Id<SourceMaterialRecord> = Id::from_uuid(material_id);
        if let Err(e) = self
            .pool
            .source_materials()
            .mark_as_failed(id, reason)
            .await
        {
            warn!(
                material_id = %material_id,
                error = %e,
                "Failed to mark material as failed in database"
            );
        }
    }

    /// Finalize a failed material: mark as failed, clean up state, and remove from active map
    pub(super) async fn finalize_failed_material(&self, material_id: Uuid, reason: &str) {
        if !self.begin_failure_cleanup(material_id, reason).await {
            return;
        }

        self.finalize_failed_material_claimed(material_id, reason).await;
    }

    /// Finalize a failed material after the caller has already claimed the terminal transition.
    pub(super) async fn finalize_failed_material_claimed(&self, material_id: Uuid, reason: &str) {
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
        let mark = self.mark_material_failed(material_id, reason);
        let cleanup = self.cleanup_state(material_id);
        tokio::join!(mark, cleanup);
        let _ = self.assembler_state.remove(&material_id);
    }

    pub(super) async fn try_finalize_pending_end(
        &self,
        material_id: Uuid,
        state_handle: Arc<Mutex<super::state::AssemblerState>>,
        pending_behavior: PendingEndBehavior,
    ) -> IngestdResult<()> {
        use super::state::build_finalize_metadata;

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

            let ended_at = Timestamp::parse_rfc3339(&end_preview.ended_at)
                .unwrap_or_else(|_| Timestamp::now());

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

                state.phase = AssemblyPhase::Finalizing;
                drop(state);
                self.route_material_error(
                    material_id,
                    "material assembly corruption detected",
                    ctx,
                )
                .await;
                self.finalize_failed_material_claimed(
                    material_id,
                    "material assembly corruption detected",
                )
                .await;
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

            if let Some(mut file) = state.temp_file.take()
                && let Err(e) = file.flush().await
            {
                warn!(
                    material_id = %material_id,
                    "Failed to flush temp file during finalization: {}",
                    e
                );
            }

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
        // The lock only guarded the handoff into a stable `FinalizationState`; annex import,
        // blob registration, and source-material updates must not run while holding it.
        debug!(
            material_id = %material_id,
            assembled_bytes,
            slice_count,
            reported_total = end.total_size_bytes,
            temp_path = %final_state.temp_path.display(),
            "Processing end message"
        );

        // If the payload claims zero bytes, avoid annex/blob work and treat this as an empty
        // material. Persist a DLQ entry so publishers can diagnose.
        if end.total_size_bytes == 0 {
            warn!(
                material_id = %material_id,
                slices = slice_count,
                total_size = end.total_size_bytes,
                "Material ended with no content; skipping annex import and routing to DLQ"
            );

            self.route_material_error(
                material_id,
                "empty_material",
                serde_json::json!({
                    "slice_count": slice_count,
                    "total_size": end.total_size_bytes,
                }),
            )
            .await;
            self.finalize_failed_material_claimed(material_id, "empty_material")
                .await;
            return Ok(());
        }

        if end.total_size_bytes > self.max_material_size_bytes {
            warn!(
                material_id = %material_id,
                reported_total = end.total_size_bytes,
                max_material_size_bytes = self.max_material_size_bytes,
                "Material exceeded the configured per-material size limit"
            );
            self.route_material_error(
                material_id,
                "material_size_limit_exceeded",
                serde_json::json!({
                    "assembled_bytes": assembled_bytes,
                    "reported_total": end.total_size_bytes,
                    "max_material_size_bytes": self.max_material_size_bytes,
                    "slice_count": slice_count,
                }),
            )
            .await;
            self.finalize_failed_material_claimed(material_id, "material_size_limit_exceeded")
                .await;
            return Ok(());
        }

        // Verify the staged file size matches expectations before annex import.
        // Edge case: File size mismatch can occur if:
        // - Disk writes were incomplete due to process crash during slice write
        // - Filesystem corruption or out-of-space errors during assembly
        // - Race between finalization and ongoing slice writes (prevented by finalizing flag)
        let file_size = tokio::fs::metadata(&final_state.temp_path)
            .await
            .map_or(0, |m| m.len() as i64);
        if file_size != assembled_bytes {
            warn!(
                material_id = %material_id,
                file_size,
                assembled_bytes,
                "Assembled file size on disk does not match assembled bytes; routing to DLQ"
            );
            self.route_material_error(
                material_id,
                "material_size_mismatch_disk",
                serde_json::json!({
                    "assembled_bytes": assembled_bytes,
                    "file_size": file_size,
                    "reported_total": end.total_size_bytes,
                }),
            )
            .await;
            self.finalize_failed_material_claimed(material_id, "material_size_mismatch_disk")
                .await;
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
            self.route_material_error(
                material_id,
                "material_hash_mismatch",
                serde_json::json!({
                    "expected_hash": end.content_hash,
                    "actual_hash": computed_hash,
                }),
            )
            .await;
            self.finalize_failed_material_claimed(material_id, "material_hash_mismatch")
                .await;
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

        let annex_key = match self.import_into_annex(&final_state).await {
            Ok(result) => result,
            Err(e) => {
                self.route_material_error(
                    material_id,
                    "annex_import_failed",
                    serde_json::json!({ "error": e.to_string() }),
                )
                .await;
                Self::revert_finalization_start(&state_handle, end).await;
                return Err(e);
            }
        };

        if let Err(e) = self
            .persist_finalized_material(&final_state, &annex_key, &end, finalize_metadata)
            .await
        {
            self.route_material_error(
                material_id,
                "material_persist_failed",
                serde_json::json!({ "error": e.to_string() }),
            )
            .await;
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

        self.stats_inc_completed(duration_ms as f64 / 1000.0, end.total_size_bytes as u64); // Track successful assembly

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
            "Material assembly complete and persisted to git-annex"
        );

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

        let state_handle = if let Some(existing) = self.get_state_handle(&material_id).await {
            existing
        } else {
            if self.material_is_terminal(material_id).await? {
                info!(
                    material_id = %material_id,
                    "End message received after completion; skipping placeholder state"
                );
                return Ok(());
            }
            // End may arrive before begin/slices (separate streams). Create a placeholder so we can
            // record the end and finalize once the missing slices arrive.
            warn!(
                material_id = %material_id,
                "End message received before material state existed; creating placeholder"
            );
            let placeholder = self.create_placeholder_state(material_id).await?;
            self.insert_state_handle(material_id, placeholder).await
        };

        // Record end so we can tolerate out-of-order delivery across begin/slices/end streams.
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

        self.try_finalize_pending_end(material_id, state_handle, PendingEndBehavior::Error)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MaterialReadySet;
    use camino::Utf8PathBuf;
    use serde_json::json;
    use sinex_db::repositories::{DbPoolExt, source_materials::status};
    use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
    use std::sync::Arc;
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
            50,
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
        assembler.insert_state_handle(material_id, state).await;

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
                .finalization_commit_landed(&final_state, &annex_key)
                .await?,
            "completed material with matching blob metadata should reconcile as committed"
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
                .finalization_commit_landed(&final_state, &annex_key)
                .await?,
            "non-terminal material state should not reconcile as a landed commit"
        );
        Ok(())
    }
}
