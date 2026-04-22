//! Atomic material finalization transaction.
//!
//! This module owns the durable commit boundary for a completed source material:
//! content-store reconciliation, blob registration, source-material finalization,
//! and precise temporal-ledger coverage.

use serde_json::json;
use sinex_db::{
    models::blob::Blob,
    repositories::{DbPoolExt, TemporalLedgerEntry},
};
use sinex_node_sdk::annex::AnnexKey;
use sinex_primitives::{Id, JsonValue, Uuid};
use sinex_schema::schema::records::SourceMaterialRecord;
use tracing::{error, info, warn};

use crate::{IngestdResult, SinexError};

use super::{FinalizationState, MaterialAssembler};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FinalizationErrorKind {
    BeginTransaction,
    EnsureMaterialRecord,
    UpsertBlob,
    FinalizeMaterialRecord,
    RecordLedgerEntry,
    Commit,
    CommitOutcomeUnknown,
}

impl FinalizationErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::BeginTransaction => "begin_transaction",
            Self::EnsureMaterialRecord => "ensure_material_record",
            Self::UpsertBlob => "upsert_blob",
            Self::FinalizeMaterialRecord => "finalize_material_record",
            Self::RecordLedgerEntry => "record_ledger_entry",
            Self::Commit => "commit",
            Self::CommitOutcomeUnknown => "commit_outcome_unknown",
        }
    }
}

#[derive(Debug)]
pub(super) struct FinalizationError {
    kind: FinalizationErrorKind,
    error: SinexError,
}

impl FinalizationError {
    fn new(kind: FinalizationErrorKind, error: SinexError) -> Self {
        Self {
            kind,
            error: error.with_context("finalization_stage", kind.as_str()),
        }
    }

    #[cfg(test)]
    pub(super) fn kind(&self) -> FinalizationErrorKind {
        self.kind
    }

    pub(super) fn is_commit_outcome_unknown(&self) -> bool {
        finalization_commit_outcome_unknown(&self.error)
    }

    pub(super) fn into_inner(self) -> SinexError {
        self.error
    }
}

impl std::fmt::Display for FinalizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.error)
    }
}

impl std::error::Error for FinalizationError {}

enum FinalizationCommitOutcome {
    Landed(FinalizedHandle),
    NotLanded,
    Unknown(SinexError),
}

pub(super) struct FinalizationRequest<'a> {
    pub final_state: &'a FinalizationState,
    pub annex_key: &'a AnnexKey,
    pub content_hash: &'a str,
    pub total_size_bytes: i64,
    pub metadata: JsonValue,
    pub final_status: &'a str,
}

#[derive(Debug)]
pub(super) struct FinalizedHandle {
    pub blob_id: Id<Blob>,
    pub reused_existing_commit: bool,
}

pub(super) struct FinalizationTransaction<'a> {
    assembler: &'a MaterialAssembler,
}

impl<'a> FinalizationTransaction<'a> {
    pub(super) fn new(assembler: &'a MaterialAssembler) -> Self {
        Self { assembler }
    }

    pub(super) async fn finalize(
        &self,
        request: FinalizationRequest<'_>,
    ) -> Result<FinalizedHandle, FinalizationError> {
        match self
            .finalization_commit_outcome(
                request.final_state,
                request.annex_key,
                request.final_status,
            )
            .await
        {
            FinalizationCommitOutcome::Landed(handle) => {
                info!(
                    material_id = %request.final_state.material_id,
                    annex_key = %request.annex_key.key,
                    "Material finalization already persisted; skipping duplicate finalization"
                );
                return Ok(handle);
            }
            FinalizationCommitOutcome::NotLanded => {}
            FinalizationCommitOutcome::Unknown(error) => {
                warn!(
                    material_id = %request.final_state.material_id,
                    annex_key = %request.annex_key.key,
                    error = %error,
                    "Unable to confirm material state before finalization; attempting transactional write"
                );
            }
        }

        let mut tx = self.assembler.pool.begin().await.map_err(|e| {
            FinalizationError::new(
                FinalizationErrorKind::BeginTransaction,
                SinexError::database("Failed to begin material finalization transaction")
                    .with_source(e),
            )
        })?;

        if let Err(error) = self
            .ensure_material_record_present_with_executor(&mut tx, request.final_state)
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
            self.cleanup_annex_import_failure(request.annex_key).await;
            return Err(FinalizationError::new(
                FinalizationErrorKind::EnsureMaterialRecord,
                error,
            ));
        }

        let blob_id = match self
            .upsert_blob_with_executor(
                &mut tx,
                request.final_state,
                request.annex_key,
                request.content_hash,
            )
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
                self.cleanup_annex_import_failure(request.annex_key).await;
                return Err(FinalizationError::new(
                    FinalizationErrorKind::UpsertBlob,
                    error,
                ));
            }
        };

        if let Err(error) = self
            .finalize_material_record_with_executor(
                &mut tx,
                request.final_state,
                request.final_status,
                blob_id,
                request.total_size_bytes,
                request.metadata,
            )
            .await
        {
            let error = match tx.rollback().await {
                Ok(()) => error,
                Err(rollback_error) => {
                    rollback_finalization_failure(error, rollback_error, "finalize_material_record")
                }
            };
            self.cleanup_annex_import_failure(request.annex_key).await;
            return Err(FinalizationError::new(
                FinalizationErrorKind::FinalizeMaterialRecord,
                error,
            ));
        }

        if let Err(error) = self
            .record_ledger_entry_with_executor(&mut tx, request.final_state)
            .await
        {
            let error = match tx.rollback().await {
                Ok(()) => error,
                Err(rollback_error) => {
                    rollback_finalization_failure(error, rollback_error, "record_ledger_entry")
                }
            };
            self.cleanup_annex_import_failure(request.annex_key).await;
            return Err(FinalizationError::new(
                FinalizationErrorKind::RecordLedgerEntry,
                error,
            ));
        }

        match tx.commit().await {
            Ok(()) => Ok(FinalizedHandle {
                blob_id,
                reused_existing_commit: false,
            }),
            Err(error) => {
                let commit_error =
                    SinexError::database("Failed to commit material finalization transaction")
                        .with_source(error);

                match self
                    .finalization_commit_outcome(
                        request.final_state,
                        request.annex_key,
                        request.final_status,
                    )
                    .await
                {
                    FinalizationCommitOutcome::Landed(handle) => {
                        warn!(
                            material_id = %request.final_state.material_id,
                            annex_key = %request.annex_key.key,
                            "Material finalization commit returned an error, but the committed state was reconciled successfully"
                        );
                        Ok(handle)
                    }
                    FinalizationCommitOutcome::NotLanded => {
                        self.cleanup_annex_import_failure(request.annex_key).await;
                        Err(FinalizationError::new(
                            FinalizationErrorKind::Commit,
                            commit_error,
                        ))
                    }
                    FinalizationCommitOutcome::Unknown(reconcile_error) => {
                        warn!(
                            material_id = %request.final_state.material_id,
                            annex_key = %request.annex_key.key,
                            error = %reconcile_error,
                            "Failed to reconcile material finalization after commit error"
                        );
                        Err(FinalizationError::new(
                            FinalizationErrorKind::CommitOutcomeUnknown,
                            finalization_unknown_commit_error(
                                commit_error,
                                &reconcile_error,
                                request.final_state.material_id,
                                request.annex_key,
                                request.final_status,
                            ),
                        ))
                    }
                }
            }
        }
    }

    /// Insert or fetch blob metadata for the assembled material.
    ///
    /// BLAKE3 collision resistance makes true content-address collisions
    /// cryptographically infeasible. Deduplication therefore first uses BLAKE3
    /// checksum when present, then legacy backend/hash/size identity.
    async fn upsert_blob_with_executor(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        state: &FinalizationState,
        annex_key: &AnnexKey,
        content_hash: &str,
    ) -> IngestdResult<Id<Blob>> {
        let repo = self.assembler.pool.blobs();

        let metadata = json!({
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

    async fn finalize_material_record_with_executor(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        state: &FinalizationState,
        final_status: &str,
        blob_id: Id<Blob>,
        total_size_bytes: i64,
        metadata: JsonValue,
    ) -> IngestdResult<()> {
        let repo = self.assembler.pool.source_materials();
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

    async fn record_ledger_entry_with_executor(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        state: &FinalizationState,
    ) -> IngestdResult<()> {
        let entry = TemporalLedgerEntry::realtime_capture(
            state.material_id,
            state.expected_offset,
            state.started_at,
        );

        self.assembler
            .pool
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
            .assembler
            .pool
            .blobs()
            .get_by_content(&annex_key.backend, &annex_key.hash, annex_key.size as i64)
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(error) = self
                    .assembler
                    .annex
                    .drop_content(&annex_key.key, true)
                    .await
                {
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

    async fn committed_handle(
        &self,
        final_state: &FinalizationState,
        annex_key: &AnnexKey,
        final_status: &str,
    ) -> IngestdResult<Option<FinalizedHandle>> {
        let material = self
            .assembler
            .pool
            .source_materials()
            .get_by_id(Id::from_uuid(final_state.material_id))
            .await
            .map_err(|error| {
                SinexError::database("Failed to inspect material state after commit error")
                    .with_source(error)
            })?;

        let Some(material) = material else {
            return Ok(None);
        };

        if material.status != final_status {
            return Ok(None);
        }

        let Some(material_blob_id) = material.optional_blob_id else {
            return Ok(None);
        };

        let blob = self
            .assembler
            .pool
            .blobs()
            .get_by_content(&annex_key.backend, &annex_key.hash, annex_key.size as i64)
            .await
            .map_err(|error| {
                SinexError::database("Failed to inspect blob state after commit error")
                    .with_source(error)
            })?;

        Ok(blob.and_then(|blob| {
            if *blob.id.as_uuid() == material_blob_id {
                Some(FinalizedHandle {
                    blob_id: blob.id,
                    reused_existing_commit: true,
                })
            } else {
                None
            }
        }))
    }

    async fn finalization_commit_outcome(
        &self,
        final_state: &FinalizationState,
        annex_key: &AnnexKey,
        final_status: &str,
    ) -> FinalizationCommitOutcome {
        match self
            .committed_handle(final_state, annex_key, final_status)
            .await
        {
            Ok(Some(handle)) => FinalizationCommitOutcome::Landed(handle),
            Ok(None) => FinalizationCommitOutcome::NotLanded,
            Err(error) => FinalizationCommitOutcome::Unknown(error),
        }
    }

    async fn ensure_material_record_present_with_executor(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        final_state: &FinalizationState,
    ) -> IngestdResult<()> {
        let repo = self.assembler.pool.source_materials();
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
}

pub(super) fn finalization_commit_outcome_unknown(error: &SinexError) -> bool {
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
        .with_operation("FinalizationTransaction::finalize")
}

#[cfg(test)]
mod tests {
    use sinex_db::repositories::source_materials::status;
    use sinex_node_sdk::annex::AnnexKey;
    use sinex_primitives::Uuid;
    use xtask::sandbox::prelude::*;

    use super::*;

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
