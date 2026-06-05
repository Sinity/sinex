//! Ingestion helper utilities for source processing.
//!
//! This module provides helpers for sources to process `MaterialSliceStream`:
//! - `SliceAssembler` for record reassembly
//! - `LedgerReader` and `derive_ts_orig` for timestamp computation
//! - `SnapshotDiff` for snapshot sources (diff to inserts/updates/deletes)
//!
//! For typed occurrence identity, see [`sinex_primitives::MaterialOccurrenceKey`].

use crate::runtime::RuntimeResult;
use serde_json;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{
    EventSource, SourceMaterialTimingInfoType, TemporalPrecision, TemporalSourceType,
};
use sinex_primitives::temporal::Timestamp;
use std::collections::VecDeque;
use tracing::{debug, warn};

/// Material-tier timing summary, read from `raw.source_material_registry`.
///
/// This is the lower-precedence half of the #1570 Prong B two-sided join:
/// when no sub-material ledger entry covers an event's anchor, the material
/// registry's coarse timing category and `start_time`/`staged_at` resolve
/// `ts_orig`. `staged_at` is the guaranteed floor — it is always present, so
/// material-tier resolution never returns `None`.
#[derive(Debug, Clone)]
pub struct MaterialTiming {
    /// Coarse timing category from the registry (`timing_info_type`).
    pub timing_info_type: SourceMaterialTimingInfoType,
    /// Material-begin timestamp (best-known real-world time), if recorded.
    pub start_time: Option<Timestamp>,
    /// Time the material was staged — the guaranteed `staged_at` floor.
    pub staged_at: Timestamp,
}

impl MaterialTiming {
    /// Resolve the material-tier `(ts_orig, rung)` for this material.
    ///
    /// Uses `start_time` (with the category's mapped rung) when present and the
    /// category is timing-bearing; otherwise falls back to the `staged_at`
    /// floor at the `StagedAt` rung. Never returns `None`.
    #[must_use]
    pub fn resolve(&self) -> (Timestamp, TemporalSourceType) {
        let rung = self.timing_info_type.to_temporal_source();
        match (self.start_time, rung) {
            // No real-world begin time, or category is the floor itself.
            (None, _) | (_, TemporalSourceType::StagedAt) => {
                (self.staged_at, TemporalSourceType::StagedAt)
            }
            (Some(start), rung) => (start, rung),
        }
    }
}

/// `SliceAssembler` for record reassembly (e.g., line or JSON delimiter)
/// `TARGET_final.md` line 120
pub struct SliceAssembler {
    delimiter: Vec<u8>,
    buffer: Vec<u8>,
    max_record_size: usize,
}

impl SliceAssembler {
    /// Create a new assembler with the specified delimiter
    #[must_use]
    pub fn new(delimiter: Vec<u8>) -> Self {
        Self {
            delimiter,
            buffer: Vec::new(),
            max_record_size: 10 * 1024 * 1024, // 10MB default
        }
    }

    /// Create a line-based assembler
    #[must_use]
    pub fn line_based() -> Self {
        Self::new(b"\n".to_vec())
    }

    /// Create a JSON lines assembler
    #[must_use]
    pub fn jsonl() -> Self {
        Self::new(b"\n".to_vec())
    }

    /// Set maximum record size
    #[must_use]
    pub fn with_max_size(mut self, size: usize) -> Self {
        self.max_record_size = size;
        self
    }

    /// Add bytes and extract complete records
    pub fn push_bytes(&mut self, bytes: &[u8]) -> RuntimeResult<Vec<Vec<u8>>> {
        self.buffer.extend_from_slice(bytes);

        let mut records = Vec::new();

        while let Some(pos) = self.find_delimiter() {
            if pos > self.max_record_size {
                warn!(
                    "Record exceeds max size {}, truncating",
                    self.max_record_size
                );
            }

            let record = self.buffer.drain(..pos).collect::<Vec<u8>>();
            self.buffer.drain(..self.delimiter.len()); // Remove delimiter

            if !record.is_empty() {
                records.push(record);
            }
        }

        // Check if buffer is getting too large
        if self.buffer.len() > self.max_record_size {
            warn!("Buffer exceeds max size, may have incomplete record");
        }

        Ok(records)
    }

    /// Flush any remaining data as a final record
    pub fn flush(&mut self) -> Option<Vec<u8>> {
        if self.buffer.is_empty() {
            None
        } else {
            Some(self.buffer.drain(..).collect())
        }
    }

    fn find_delimiter(&self) -> Option<usize> {
        self.buffer
            .windows(self.delimiter.len())
            .position(|window| window == self.delimiter.as_slice())
    }
}

/// `LedgerReader` for accessing temporal ledger entries
/// `TARGET_final.md` line 121
pub struct LedgerReader {
    pub material_id: Uuid,
    pub entries: VecDeque<LedgerEntry>,
}

#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub offset_start: i64,
    pub offset_end: i64,
    pub ts_capture: Timestamp,
    pub precision: TemporalPrecision,
    pub source_type: TemporalSourceType,
}

impl LedgerReader {
    /// Create a new ledger reader with entries
    #[must_use]
    pub fn new(material_id: Uuid, entries: Vec<LedgerEntry>) -> Self {
        Self {
            material_id,
            entries: VecDeque::from(entries),
        }
    }

    /// Find the ledger entry for a given offset
    #[must_use]
    pub fn find_entry_for_offset(&self, offset: i64) -> Option<&LedgerEntry> {
        self.entries
            .iter()
            .filter(|e| offset >= e.offset_start && offset < e.offset_end)
            .min_by_key(|entry| {
                (
                    temporal_source_precedence(entry.source_type),
                    entry.offset_end - entry.offset_start,
                )
            })
    }

    /// Derive `ts_orig` and its quality rung for a material event at `offset`.
    ///
    /// This is the persistence-owned (lower-precedence) half of the #1570
    /// Prong B two-sided join. The parser already owns the `IntrinsicContent`
    /// case (via `#[timestamp]`); when it resolves the rung it sets `ts_orig`
    /// directly and persistence never calls this.
    ///
    /// Precedence here:
    /// 1. **sub-material ledger** — a `temporal_ledger` entry whose offset range
    ///    covers `offset` (genuine wrapped-stream / per-chunk timing). Its
    ///    recorded `source_type` is the rung.
    /// 2. **material tier** — the `raw.source_material_registry` timing summary,
    ///    with `staged_at` as the guaranteed floor.
    ///
    /// Never returns `None`: the `staged_at` floor always resolves.
    #[must_use]
    pub fn derive_ts_orig(
        &self,
        offset: i64,
        material_timing: &MaterialTiming,
    ) -> (Timestamp, TemporalSourceType) {
        // 1. Sub-material ledger entry covering this offset (wrapped streams).
        if let Some(entry) = self.find_entry_for_offset(offset) {
            return (entry.ts_capture, entry.source_type);
        }

        // 2. Material-tier registry timing, floored at staged_at.
        material_timing.resolve()
    }
}

fn temporal_source_precedence(source_type: TemporalSourceType) -> u8 {
    match source_type {
        TemporalSourceType::RealtimeCapture => 0,
        TemporalSourceType::IntrinsicContent => 1,
        TemporalSourceType::InferredMtime => 2,
        TemporalSourceType::InferredCtime => 3,
        TemporalSourceType::InferredUser => 4,
        TemporalSourceType::StagedAt => 5,
    }
}

/// Helper for computing deterministic anchor points
pub struct AnchorComputer {
    pub material_id: Uuid,
    pub anchor_rule_id: String,
    pub anchor_rule_version: String,
}

impl AnchorComputer {
    /// Compute anchor byte for a given offset in the material
    #[must_use]
    pub fn compute_anchor(&self, _offset: i64, record_boundary: i64) -> i64 {
        // For now, use the start of the record as the anchor
        // This ensures deterministic anchoring
        record_boundary
    }

    /// Validate that computed anchor matches expected
    pub fn validate_anchor(&self, computed: i64, expected: i64) -> bool {
        if computed == expected {
            true
        } else {
            warn!(
                "Anchor mismatch for material {}: computed={}, expected={}",
                self.material_id, computed, expected
            );
            false
        }
    }
}

/// `RowIdentitySpec` for defining how to identify unique rows in snapshot data
/// `TARGET_final.md` line 122
#[derive(Debug, Clone)]
pub struct RowIdentitySpec {
    /// Primary key columns or unique identifier columns
    pub key_columns: Vec<String>,
    /// Columns to track for change detection
    pub tracked_columns: Vec<String>,
    /// Optional timestamp column for versioning
    pub timestamp_column: Option<String>,
}

impl RowIdentitySpec {
    #[must_use]
    pub fn new(key_columns: Vec<String>) -> Self {
        Self {
            key_columns,
            tracked_columns: Vec::new(),
            timestamp_column: None,
        }
    }

    #[must_use]
    pub fn with_tracked_columns(mut self, columns: Vec<String>) -> Self {
        self.tracked_columns = columns;
        self
    }

    #[must_use]
    pub fn with_timestamp_column(mut self, column: String) -> Self {
        self.timestamp_column = Some(column);
        self
    }
}

/// Represents a single row in a snapshot
#[derive(Debug, Clone)]
pub struct SnapshotRow {
    /// The unique key for this row (composite of key columns)
    pub key: Vec<String>,
    /// The full row data
    pub data: serde_json::Value,
    /// Optional version/timestamp for this row
    pub version: Option<Timestamp>,
}

/// Types of changes detected in snapshot diffs
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
}

/// A detected change in the snapshot
#[derive(Debug, Clone)]
pub struct SnapshotChange {
    pub change_type: ChangeType,
    pub row_key: Vec<String>,
    pub old_data: Option<serde_json::Value>,
    pub new_data: Option<serde_json::Value>,
    pub changed_columns: Vec<String>,
}

/// `SnapshotDiff` for converting snapshot sources to inserts/updates/deletes
/// `TARGET_final.md` line 122
pub struct SnapshotDiff {
    identity_spec: RowIdentitySpec,
    previous_snapshot: std::collections::HashMap<Vec<String>, SnapshotRow>,
}

impl SnapshotDiff {
    /// Create a new `SnapshotDiff` with identity specification
    #[must_use]
    pub fn new(identity_spec: RowIdentitySpec) -> Self {
        Self {
            identity_spec,
            previous_snapshot: std::collections::HashMap::new(),
        }
    }

    /// Load the previous snapshot state
    pub fn load_previous_snapshot(&mut self, rows: Vec<SnapshotRow>) {
        self.previous_snapshot.clear();
        for row in rows {
            self.previous_snapshot.insert(row.key.clone(), row);
        }
        debug!(
            "Loaded {} rows into previous snapshot",
            self.previous_snapshot.len()
        );
    }

    /// Compute the diff between previous and current snapshots
    pub fn compute_diff(&mut self, current_rows: Vec<SnapshotRow>) -> Vec<SnapshotChange> {
        let mut changes = Vec::new();
        let mut seen_keys = std::collections::HashSet::new();

        // Check for inserts and updates
        for current_row in current_rows {
            seen_keys.insert(current_row.key.clone());

            match self.previous_snapshot.get(&current_row.key) {
                None => {
                    // This is a new row (INSERT)
                    changes.push(SnapshotChange {
                        change_type: ChangeType::Insert,
                        row_key: current_row.key.clone(),
                        old_data: None,
                        new_data: Some(current_row.data.clone()),
                        changed_columns: Vec::new(),
                    });
                }
                Some(previous_row) => {
                    // Check if the row has changed (UPDATE)
                    let changed_columns =
                        self.detect_changes(&previous_row.data, &current_row.data);
                    if !changed_columns.is_empty() {
                        changes.push(SnapshotChange {
                            change_type: ChangeType::Update,
                            row_key: current_row.key.clone(),
                            old_data: Some(previous_row.data.clone()),
                            new_data: Some(current_row.data.clone()),
                            changed_columns,
                        });
                    }
                }
            }

            // Update the previous snapshot with current data
            self.previous_snapshot
                .insert(current_row.key.clone(), current_row);
        }

        // Check for deletes
        let all_keys: Vec<Vec<String>> = self.previous_snapshot.keys().cloned().collect();
        for key in all_keys {
            if !seen_keys.contains(&key)
                && let Some(deleted_row) = self.previous_snapshot.remove(&key)
            {
                changes.push(SnapshotChange {
                    change_type: ChangeType::Delete,
                    row_key: key,
                    old_data: Some(deleted_row.data),
                    new_data: None,
                    changed_columns: Vec::new(),
                });
            }
        }

        debug!("Computed {} changes in snapshot diff", changes.len());
        changes
    }

    /// Detect which columns have changed between two JSON values
    fn detect_changes(
        &self,
        old_data: &serde_json::Value,
        new_data: &serde_json::Value,
    ) -> Vec<String> {
        let mut changed_columns = Vec::new();

        // If tracked_columns is specified, only check those
        let columns_to_check = if !self.identity_spec.tracked_columns.is_empty() {
            &self.identity_spec.tracked_columns
        } else if let (Some(old_obj), Some(new_obj)) = (old_data.as_object(), new_data.as_object())
        {
            // Check all columns if no specific tracking
            let all_keys: std::collections::HashSet<_> =
                old_obj.keys().chain(new_obj.keys()).cloned().collect();
            &all_keys.into_iter().collect::<Vec<_>>()
        } else {
            return vec!["_value".to_string()]; // Non-object comparison
        };

        if let (Some(old_obj), Some(new_obj)) = (old_data.as_object(), new_data.as_object()) {
            for column in columns_to_check {
                let old_val = old_obj.get(column);
                let new_val = new_obj.get(column);
                if old_val != new_val {
                    changed_columns.push(column.clone());
                }
            }
        } else if old_data != new_data {
            changed_columns.push("_value".to_string());
        }

        changed_columns
    }

    /// Convert a snapshot change to event payloads
    #[must_use]
    pub fn change_to_events(
        &self,
        change: &SnapshotChange,
        source: &EventSource,
    ) -> Vec<serde_json::Value> {
        match change.change_type {
            ChangeType::Insert => vec![serde_json::json!({
                "event_type": format!("{}.row.inserted", source.as_ref()),
                "row_key": change.row_key,
                "data": change.new_data,
            })],
            ChangeType::Update => vec![serde_json::json!({
                "event_type": format!("{}.row.updated", source.as_ref()),
                "row_key": change.row_key,
                "old_data": change.old_data,
                "new_data": change.new_data,
                "changed_columns": change.changed_columns,
            })],
            ChangeType::Delete => vec![serde_json::json!({
                "event_type": format!("{}.row.deleted", source.as_ref()),
                "row_key": change.row_key,
                "data": change.old_data,
            })],
        }
    }
}

#[cfg(test)]
mod ts_orig_resolution_tests {
    //! #1570 Prong B — `ts_orig` quality derivation (persistence-owned tier).
    //!
    //! These are pure-function tests: derivation depends only on the
    //! source-material timing row + sub-material ledger entries + the event's
    //! anchor offset. All of those are stable across replay, so determinism
    //! (same inputs → same output) is exactly the replay-stability contract.
    use super::*;
    use xtask::sandbox::prelude::*;

    fn ts(secs: i64) -> Timestamp {
        Timestamp::from_unix_timestamp(secs).expect("valid unix timestamp")
    }

    /// Material-tier: a timing-bearing category with a recorded `start_time`
    /// resolves to that time at the category's mapped rung.
    #[sinex_test]
    async fn material_timing_uses_start_time_with_category_rung() -> TestResult<()> {
        let timing = MaterialTiming {
            timing_info_type: SourceMaterialTimingInfoType::Intrinsic,
            start_time: Some(ts(1_000)),
            staged_at: ts(9_000),
        };
        assert_eq!(
            timing.resolve(),
            (ts(1_000), TemporalSourceType::IntrinsicContent)
        );
        Ok(())
    }

    /// Material-tier: no `start_time` falls back to the `staged_at` floor.
    #[sinex_test]
    async fn material_timing_falls_back_to_staged_floor() -> TestResult<()> {
        let timing = MaterialTiming {
            timing_info_type: SourceMaterialTimingInfoType::Inferred,
            start_time: None,
            staged_at: ts(9_000),
        };
        assert_eq!(timing.resolve(), (ts(9_000), TemporalSourceType::StagedAt));
        Ok(())
    }

    /// Material-tier: a category that itself maps to the `StagedAt` rung ignores
    /// any `start_time` and uses the floor (the floor *is* the best evidence).
    #[sinex_test]
    async fn material_timing_staged_category_ignores_start_time() -> TestResult<()> {
        let timing = MaterialTiming {
            timing_info_type: SourceMaterialTimingInfoType::StagedAt,
            start_time: Some(ts(1_000)),
            staged_at: ts(9_000),
        };
        assert_eq!(timing.resolve(), (ts(9_000), TemporalSourceType::StagedAt));
        Ok(())
    }

    fn ledger_entry(
        start: i64,
        end: i64,
        ts_capture: Timestamp,
        source_type: TemporalSourceType,
    ) -> LedgerEntry {
        LedgerEntry {
            offset_start: start,
            offset_end: end,
            ts_capture,
            precision: TemporalPrecision::Exact,
            source_type,
        }
    }

    /// A sub-material ledger entry covering the event's offset (a genuine
    /// wrapped-stream / per-chunk timing) takes precedence over the material tier.
    #[sinex_test]
    async fn derive_prefers_covering_ledger_entry() -> TestResult<()> {
        let reader = LedgerReader::new(
            Uuid::now_v7(),
            vec![ledger_entry(
                0,
                1_000,
                ts(2_000),
                TemporalSourceType::RealtimeCapture,
            )],
        );
        let timing = MaterialTiming {
            timing_info_type: SourceMaterialTimingInfoType::StagedAt,
            start_time: None,
            staged_at: ts(9_000),
        };
        assert_eq!(
            reader.derive_ts_orig(500, &timing),
            (ts(2_000), TemporalSourceType::RealtimeCapture),
            "covering ledger entry wins over the staged floor"
        );
        Ok(())
    }

    /// With no ledger entry covering the offset, derivation falls to the
    /// material tier — and never returns an ephemeral value.
    #[sinex_test]
    async fn derive_falls_back_to_material_tier() -> TestResult<()> {
        let reader = LedgerReader::new(Uuid::now_v7(), Vec::new());
        let timing = MaterialTiming {
            timing_info_type: SourceMaterialTimingInfoType::Intrinsic,
            start_time: Some(ts(1_000)),
            staged_at: ts(9_000),
        };
        assert_eq!(
            reader.derive_ts_orig(500, &timing),
            (ts(1_000), TemporalSourceType::IntrinsicContent)
        );
        Ok(())
    }

    /// Replay stability: re-deriving from the same material yields the same
    /// `(ts_orig, rung)`. Replay re-reads identical material timing + ledger, so
    /// the only thing that changes is the event id (`ts_coided`), never `ts_orig`.
    #[sinex_test]
    async fn derive_is_replay_stable() -> TestResult<()> {
        let entries = vec![ledger_entry(
            0,
            1_000,
            ts(2_000),
            TemporalSourceType::IntrinsicContent,
        )];
        let timing = MaterialTiming {
            timing_info_type: SourceMaterialTimingInfoType::Inferred,
            start_time: Some(ts(5_000)),
            staged_at: ts(9_000),
        };
        let first = LedgerReader::new(Uuid::now_v7(), entries.clone()).derive_ts_orig(500, &timing);
        let second = LedgerReader::new(Uuid::now_v7(), entries).derive_ts_orig(500, &timing);
        assert_eq!(first, second);
        Ok(())
    }
}
