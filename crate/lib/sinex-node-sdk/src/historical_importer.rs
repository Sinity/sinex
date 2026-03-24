//! Reusable helper for historical data import ingestors.
//!
//! Handles source material registration, provenance validation, batch error isolation,
//! and progress tracking — the mechanical parts that every historical importer needs.

use serde::{Deserialize, Serialize};
use sinex_db::repositories::StreamBatchInsertResult;
use sinex_db::{DbPool, DbPoolExt, Id, SourceMaterialRecord, repositories::StreamBatchRow};
use sinex_primitives::prelude::*;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Progress state for a historical import, stored in the ingestor's user_state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImportProgress {
    /// The registered source material UUID (set after first register call).
    pub material_id: Option<Uuid>,
    /// File-level byte offset for resume (0 if row-indexed).
    pub byte_offset: u64,
    /// Row-level index for resume (0 if byte-indexed).
    pub row_index: u64,
    /// Total events submitted so far.
    pub events_submitted: u64,
}

/// Helper for historical data import operations.
///
/// Handles the three blockers identified in the Wave 2 codebase audit:
/// 1. Source material must be pre-registered before events (hard FK)
/// 2. One bad row in a COPY batch kills the entire batch (needs pre-validation + bisect retry)
/// 3. Checkpoint tracking for resume-on-crash
pub struct HistoricalImporter {
    /// The registered source material UUID.
    pub material_id: Uuid,
    pool: DbPool,
    events_processed: u64,
    rows_quarantined: u64,
    progress_log_interval: u64,
}

impl HistoricalImporter {
    /// Generate a deterministic UUID v5 for a source file path.
    /// Two runs on the same path always produce the same UUID.
    pub fn material_uuid_for_path(path: &str) -> Uuid {
        Uuid::new_v5(&Uuid::NAMESPACE_URL, path.as_bytes())
    }

    /// Register (or re-register) source material for a historical import.
    ///
    /// Uses `register_external_in_flight` which is idempotent (upsert on source_identifier).
    /// Safe to call on every restart — same path always produces same UUID.
    pub async fn register(
        pool: &DbPool,
        source_path: &str,
        material_type: &str,
        metadata: serde_json::Value,
    ) -> Result<Self> {
        let material_id = Self::material_uuid_for_path(source_path);

        // register_external_in_flight is an idempotent upsert
        pool.source_materials()
            .register_external_in_flight(
                material_id,
                material_type,
                Some(source_path),
                metadata,
                Timestamp::now(),
            )
            .await
            .map_err(|e| {
                SinexError::database("failed to register source material for historical import")
                    .with_context("source_path", source_path)
                    .with_std_error(&e)
            })?;

        info!(
            material_id = %material_id,
            source_path = source_path,
            "Registered source material for historical import"
        );

        Ok(Self {
            material_id,
            pool: pool.clone(),
            events_processed: 0,
            rows_quarantined: 0,
            progress_log_interval: 5_000,
        })
    }

    /// Resume an existing import with a known material UUID.
    pub fn resume(pool: &DbPool, material_id: Uuid) -> Self {
        Self {
            material_id,
            pool: pool.clone(),
            events_processed: 0,
            rows_quarantined: 0,
            progress_log_interval: 5_000,
        }
    }

    /// Set the progress logging interval (default: every 5000 events).
    pub fn with_progress_interval(mut self, interval: u64) -> Self {
        self.progress_log_interval = interval;
        self
    }

    /// Total events successfully submitted.
    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    /// Total rows that failed validation or insertion and were quarantined.
    pub fn rows_quarantined(&self) -> u64 {
        self.rows_quarantined
    }

    /// Record a caller-side quarantine decision for a single source row.
    pub fn quarantine_row(&mut self, anchor_byte: Option<i64>, reason: &str) {
        warn!(
            material_id = %self.material_id,
            anchor_byte = ?anchor_byte,
            reason,
            "Quarantined historical import row before batch insert"
        );
        self.rows_quarantined += 1;
    }

    /// Validate that all rows in a batch have correct provenance XOR.
    ///
    /// Returns indices of invalid rows (rows where both or neither provenance fields are set).
    pub fn validate_provenance(batch: &[StreamBatchRow]) -> Vec<usize> {
        batch
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                let has_material = row.source_material_id.is_some();
                let has_parents = row
                    .source_event_ids
                    .as_ref()
                    .is_some_and(|ids| !ids.is_empty());
                // XOR: exactly one must be set
                has_material == has_parents
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// Submit a batch of events, with bisect-retry on failure.
    ///
    /// Pre-validates provenance XOR, then attempts COPY insert.
    /// On batch failure, splits in half and retries recursively.
    /// Individual bad rows are quarantined (logged + counted) rather than propagated.
    pub async fn submit_batch(&mut self, mut batch: Vec<StreamBatchRow>) -> Result<u64> {
        // Pre-validate provenance
        let invalid = Self::validate_provenance(&batch);
        if !invalid.is_empty() {
            warn!(
                count = invalid.len(),
                "Pre-validation caught rows with invalid provenance XOR — quarantining"
            );
            // Remove invalid rows in reverse order to preserve indices
            for &idx in invalid.iter().rev() {
                let row = batch.remove(idx);
                warn!(
                    material_id = ?row.source_material_id,
                    anchor_byte = ?row.anchor_byte,
                    "Quarantined row: invalid provenance XOR"
                );
                self.rows_quarantined += 1;
            }
        }

        if batch.is_empty() {
            return Ok(0);
        }

        let count = batch.len() as u64;
        match self.try_insert_batch(&batch).await {
            Ok(_result) => {
                self.events_processed += count;
                self.maybe_log_progress();
                Ok(count)
            }
            Err(e) => {
                debug!(
                    batch_size = batch.len(),
                    error = %e,
                    "Batch insert failed — starting bisect retry"
                );
                let inserted = self.bisect_retry(batch).await?;
                self.events_processed += inserted;
                self.maybe_log_progress();
                Ok(inserted)
            }
        }
    }

    /// Attempt to insert a batch via the standard persistence path.
    async fn try_insert_batch(&self, batch: &[StreamBatchRow]) -> Result<StreamBatchInsertResult> {
        self.pool
            .events()
            .insert_stream_batch(batch)
            .await
            .map_err(|e| SinexError::database("batch insert failed").with_std_error(&e))
    }

    /// Bisect-retry: split batch in half, retry each half.
    /// At size 1, try individual insert and quarantine on failure.
    async fn bisect_retry(&mut self, batch: Vec<StreamBatchRow>) -> Result<u64> {
        if batch.len() <= 1 {
            // Single row — try insert, quarantine on FK/constraint failure
            match self.try_insert_batch(&batch).await {
                Ok(_) => Ok(1),
                Err(e) => {
                    if Self::is_constraint_violation(&e) {
                        if let Some(row) = batch.first() {
                            warn!(
                                material_id = ?row.source_material_id,
                                anchor_byte = ?row.anchor_byte,
                                error = %e,
                                "Quarantined single row: constraint violation"
                            );
                        }
                        self.rows_quarantined += 1;
                        Ok(0)
                    } else {
                        // Non-constraint error (connection, pool exhaustion) — propagate
                        Err(e)
                    }
                }
            }
        } else {
            let mid = batch.len() / 2;
            let (left, right) = {
                let mut b = batch;
                let right = b.split_off(mid);
                (b, right)
            };

            let left_count = match self.try_insert_batch(&left).await {
                Ok(_) => left.len() as u64,
                Err(_) => Box::pin(self.bisect_retry(left)).await?,
            };

            let right_count = match self.try_insert_batch(&right).await {
                Ok(_) => right.len() as u64,
                Err(_) => Box::pin(self.bisect_retry(right)).await?,
            };

            Ok(left_count + right_count)
        }
    }

    /// Check if an error is a constraint violation (FK, CHECK, UNIQUE).
    ///
    /// Uses the structured sqlstate context attached by `sinex_db::error::db_error`.
    fn is_constraint_violation(e: &SinexError) -> bool {
        if let Some(sqlstate) = e.context_map().get("sqlstate") {
            // PostgreSQL constraint violation codes: 23xxx
            matches!(
                sqlstate.as_str(),
                "23503" // FK violation
                | "23514" // CHECK violation
                | "23505" // UNIQUE violation
            )
        } else {
            false
        }
    }

    fn maybe_log_progress(&self) {
        if self.events_processed % self.progress_log_interval == 0 && self.events_processed > 0 {
            info!(
                events = self.events_processed,
                quarantined = self.rows_quarantined,
                "Historical import progress"
            );
        }
    }

    /// Finalize the import — mark source material as completed.
    pub async fn finalize(&self, total_bytes: Option<i64>) -> Result<()> {
        let id: Id<SourceMaterialRecord> = Id::from_uuid(self.material_id);
        self.pool
            .source_materials()
            .finalize_in_flight(id, None, None, None, total_bytes)
            .await
            .map_err(|e| {
                SinexError::database("failed to finalize historical import source material")
                    .with_context("material_id", self.material_id.to_string())
                    .with_std_error(&e)
            })?;

        info!(
            material_id = %self.material_id,
            events = self.events_processed,
            quarantined = self.rows_quarantined,
            "Historical import finalized"
        );

        Ok(())
    }

    /// Mark the import as failed.
    pub async fn fail(&self, reason: &str) -> Result<()> {
        let id: Id<SourceMaterialRecord> = Id::from_uuid(self.material_id);
        self.pool
            .source_materials()
            .mark_as_failed(id, reason)
            .await
            .map_err(|e| {
                SinexError::database("failed to mark historical import as failed")
                    .with_context("material_id", self.material_id.to_string())
                    .with_context("reason", reason)
                    .with_std_error(&e)
            })?;

        warn!(
            material_id = %self.material_id,
            events = self.events_processed,
            quarantined = self.rows_quarantined,
            reason = reason,
            "Historical import failed"
        );

        Ok(())
    }
}
