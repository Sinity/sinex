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
use sinex_primitives::{Id, JsonValue, Ulid};
use sinex_schema::schema::records::SourceMaterialRecord;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::{IngestdResult, SinexError};

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
    pub(super) async fn upsert_blob(
        &self,
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
                SinexError::database(format!("Failed to query blob store: {e}"))
            })?
        {
            return Ok(Id::from_ulid(*existing.id.as_ulid()));
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

        let stored = repo.insert(blob).await.map_err(|e| {
            error!(
                material_id = %state.material_id,
                backend = %annex_key.backend,
                hash = %annex_key.hash,
                size = annex_key.size,
                error = %e,
                error_debug = ?e,
                "Failed to insert blob metadata"
            );
            SinexError::database(format!("Failed to insert blob metadata: {e}"))
        })?;

        Ok(Id::from_ulid(*stored.id.as_ulid()))
    }

    /// Finalize source material registry and ledger
    pub(super) async fn finalize_material_record(
        &self,
        state: &FinalizationState,
        blob_id: Id<Blob>,
        total_size_bytes: i64,
        metadata: JsonValue,
    ) -> IngestdResult<()> {
        let repo = self.pool.source_materials();
        let id: Id<SourceMaterialRecord> = Id::from_ulid(state.material_id);

        repo.update_metadata(id, metadata.clone())
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to update material metadata: {e}"))
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

        repo.finalize_in_flight(
            Id::from_ulid(state.material_id),
            Some(blob_id),
            encoding_hint.as_deref(),
            content_preview_hint.clone(),
            Some(total_size_bytes),
        )
        .await
        .map_err(|e| SinexError::database(format!("Failed to finalize material: {e}")))
    }

    /// Append entry in `raw.temporal_ledger`
    pub(super) async fn record_ledger_entry(&self, state: &FinalizationState) -> IngestdResult<()> {
        let entry = TemporalLedgerEntry::realtime_capture(
            state.material_id,
            state.expected_offset,
            state.started_at,
        );

        self.pool
            .source_materials()
            .append_temporal_ledger(entry)
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to append temporal ledger entry: {e}"))
            })?;

        Ok(())
    }

    /// Route material failure to DLQ
    pub(super) async fn route_material_error(
        &self,
        material_id: Ulid,
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
    pub(super) async fn mark_material_failed(&self, material_id: Ulid, reason: &str) {
        let id: Id<SourceMaterialRecord> = Id::from_ulid(material_id);
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
    pub(super) async fn finalize_failed_material(&self, material_id: Ulid, reason: &str) {
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
        material_id: Ulid,
        state_handle: Arc<Mutex<super::state::AssemblerState>>,
        pending_behavior: PendingEndBehavior,
    ) -> IngestdResult<()> {
        use super::state::build_finalize_metadata;

        let (final_state, assembled_bytes, slice_count, computed_hash, end, ended_at) = {
            let mut state = state_handle.lock().await;
            if state.finalizing {
                debug!(material_id = %material_id, "Ignoring end message while finalizing");
                return Ok(());
            }

            let Some(end_preview) = state.pending_end.clone() else {
                return Ok(());
            };

            if !state.has_begin {
                debug!(
                    material_id = %material_id,
                    "End recorded before begin; waiting for begin metadata"
                );
                return Ok(());
            }

            let ended_at = time::OffsetDateTime::parse(
                &end_preview.ended_at,
                &time::format_description::well_known::Rfc3339,
            )
            .map_or_else(|_| Timestamp::now(), Timestamp::new);

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

                state.finalizing = true;
                drop(state);
                self.route_material_error(
                    material_id,
                    "material assembly corruption detected",
                    ctx,
                )
                .await;
                self.finalize_failed_material(material_id, "material assembly corruption detected")
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

            // Complete: transition into finalization. Prevent concurrent slice writes by taking
            // the file handle and marking finalizing.
            state.finalizing = true;
            let end = state.pending_end.take().ok_or_else(|| {
                SinexError::service(format!(
                    "State corruption: pending_end missing during finalization for material {material_id}"
                ))
            })?;

            if let Some(mut file) = state.temp_file.take() {
                if let Err(e) = file.flush().await {
                    warn!(
                        material_id = %material_id,
                        "Failed to flush temp file during finalization: {}",
                        e
                    );
                }
            }

            let computed_hash = state.hasher.clone().finalize().to_hex().to_string();
            // WAL keeps the End message, so we don't need to persist implicit state changes here.
            // unique session crash recovery handles re-finalization.

            (
                view,
                assembled_bytes,
                slice_count,
                computed_hash,
                end,
                ended_at,
            )
        };

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
            self.finalize_failed_material(material_id, "empty_material")
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
            self.finalize_failed_material(material_id, "material_size_mismatch_disk")
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
            self.finalize_failed_material(material_id, "material_hash_mismatch")
                .await;
            return Ok(());
        }

        let (annex_key, final_path) = match self.import_into_annex(&final_state).await {
            Ok(result) => result,
            Err(e) => {
                self.route_material_error(
                    material_id,
                    "annex_import_failed",
                    serde_json::json!({ "error": e.to_string() }),
                )
                .await;
                {
                    let mut state = state_handle.lock().await;
                    state.finalizing = false;
                    state.pending_end = Some(end);
                    // WAL is immutable, End message remains. In-memory state reverted.
                }
                return Err(e);
            }
        };

        // Ensure the record is registered before finalization (handles out-of-order Begin/End)
        self.register_material_record(
            material_id,
            &final_state.material_kind,
            &final_state.source_identifier,
            final_state.metadata.clone(),
            final_state.started_at,
        )
        .await?;

        // Signal readiness for any events still waiting on this material's FK target.
        if let Some(ref ready_set) = self.ready_set {
            ready_set.mark_ready(material_id);
        }

        let blob_id = match self
            .upsert_blob(&final_state, &annex_key, &end.content_hash)
            .await
        {
            Ok(id) => id,
            Err(e) => {
                self.route_material_error(
                    material_id,
                    "blob_registration_failed",
                    serde_json::json!({ "error": e.to_string() }),
                )
                .await;
                {
                    let mut state = state_handle.lock().await;
                    state.finalizing = false;
                    state.pending_end = Some(end);
                    // WAL is immutable, End message remains. In-memory state reverted.
                }
                return Err(e);
            }
        };

        let finalize_metadata = build_finalize_metadata(
            &final_state,
            &end.metadata,
            ended_at,
            end.total_size_bytes,
            &end.content_hash,
        )?;

        if let Err(e) = self
            .finalize_material_record(
                &final_state,
                blob_id,
                end.total_size_bytes,
                finalize_metadata,
            )
            .await
        {
            self.route_material_error(
                material_id,
                "material_finalize_failed",
                serde_json::json!({ "error": e.to_string() }),
            )
            .await;
            {
                let mut state = state_handle.lock().await;
                state.finalizing = false;
                state.pending_end = Some(end);
                // WAL is immutable, End message remains. In-memory state reverted.
            }
            return Err(e);
        }

        if let Err(e) = self.record_ledger_entry(&final_state).await {
            self.route_material_error(
                material_id,
                "ledger_append_failed",
                serde_json::json!({ "error": e.to_string() }),
            )
            .await;
            {
                let mut state = state_handle.lock().await;
                state.finalizing = false;
                state.pending_end = Some(end);
                // WAL is immutable, End message remains. In-memory state reverted.
            }
            return Err(e);
        }

        self.cleanup_state(material_id).await;
        let _ = self.assembler_state.remove(&material_id);

        self.stats_inc_completed(); // Track successful assembly

        // Compute assembly duration from started_at to now
        let assembly_duration = Timestamp::now() - final_state.started_at;
        let duration_ms = assembly_duration.whole_milliseconds().max(0) as u64;

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
            path = %final_path.display(),
            size_bytes = end.total_size_bytes,
            slices = slice_count,
            duration_ms = duration_ms,
            "Material assembly complete and persisted to annex"
        );

        Ok(())
    }

    /// Handle material finalization (end message)
    pub(super) async fn handle_end(&self, mut end: MaterialEndMessage) -> IngestdResult<()> {
        use super::state::normalize_metadata;

        end.metadata = normalize_metadata(end.metadata);
        let material_id = Ulid::from_str(&end.material_id).map_err(|e| {
            SinexError::parse(format!(
                "Invalid material_id '{}' in end message: {}",
                end.material_id, e
            ))
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
            if state.finalizing {
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
