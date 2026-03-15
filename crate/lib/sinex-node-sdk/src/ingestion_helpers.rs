//! Ingestion helper utilities as specified in `TARGET_final.md` section 5
//!
//! This module provides helpers for ingestors to process `MaterialSliceStream`:
//! - `SliceAssembler` for record reassembly
//! - `LedgerReader` and `derive_ts_orig` for timestamp computation
//! - `IdempotenceKey` for first-order event deduplication
//! - `SnapshotDiff` for snapshot sources (diff to inserts/updates/deletes)

use crate::NodeResult;
use serde_json;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EventSource, EventType, TemporalPrecision, TemporalSourceType};
use sinex_primitives::temporal::Timestamp;
use std::collections::VecDeque;
use tracing::{debug, warn};

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
    pub fn push_bytes(&mut self, bytes: &[u8]) -> NodeResult<Vec<Vec<u8>>> {
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
            .find(|e| offset >= e.offset_start && offset < e.offset_end)
    }

    /// Derive `ts_orig` and source type based on temporal ledger precedence.
    ///
    /// Precedence: `realtime_capture` > intrinsic content >
    ///            `inferred_mtime` > `inferred_ctime` > `inferred_user` > `staged_at`
    ///
    /// Returns `None` only when both the temporal ledger and intrinsic timestamp
    /// are missing — this indicates a bug (a `staged_at` ledger entry should have
    /// been written at material-begin time).
    #[must_use]
    pub fn derive_ts_orig(
        &self,
        offset: i64,
        intrinsic_timestamp: Option<Timestamp>,
    ) -> Option<(Timestamp, TemporalSourceType)> {
        // First check ledger entry
        if let Some(entry) = self.find_entry_for_offset(offset) {
            match entry.source_type {
                TemporalSourceType::RealtimeCapture => {
                    return Some((entry.ts_capture, TemporalSourceType::RealtimeCapture));
                }
                TemporalSourceType::IntrinsicContent => {
                    if let Some(ts) = intrinsic_timestamp {
                        return Some((ts, TemporalSourceType::IntrinsicContent));
                    }
                }
                TemporalSourceType::InferredMtime => {
                    return Some((entry.ts_capture, TemporalSourceType::InferredMtime));
                }
                TemporalSourceType::InferredCtime => {
                    return Some((entry.ts_capture, TemporalSourceType::InferredCtime));
                }
                TemporalSourceType::InferredUser => {
                    return Some((entry.ts_capture, TemporalSourceType::InferredUser));
                }
                TemporalSourceType::StagedAt => {
                    return Some((entry.ts_capture, TemporalSourceType::StagedAt));
                }
            }
        }

        // Fall back to intrinsic if available
        if let Some(ts) = intrinsic_timestamp {
            return Some((ts, TemporalSourceType::IntrinsicContent));
        }

        // No persisted temporal evidence and no intrinsic timestamp.
        // A `staged_at` ledger entry should have been written at material-begin
        // time. Returning None forces the caller to handle this explicitly rather
        // than silently using an ephemeral Timestamp::now().
        warn!(
            material_id = %self.material_id,
            offset,
            "derive_ts_orig: no ledger entry and no intrinsic timestamp — \
             missing staged_at ledger entry?"
        );
        None
    }
}

/// `IdempotenceKey` helper for first-order events
/// `TARGET_final.md` line 122
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotenceKey {
    pub material_id: Uuid,
    pub anchor_byte: i64,
    pub event_type: EventType,
}

impl IdempotenceKey {
    /// Create a new idempotence key
    #[must_use]
    pub fn new(material_id: Uuid, anchor_byte: i64, event_type: EventType) -> Self {
        Self {
            material_id,
            anchor_byte,
            event_type,
        }
    }

    /// Check if this key would conflict with existing events
    #[cfg(feature = "db")]
    pub async fn exists_in_db(&self, pool: &sqlx::PgPool) -> NodeResult<bool> {
        let result = sqlx::query!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM core.events 
                WHERE source_material_id::uuid = $1
                AND anchor_byte = $2
            ) as "exists!"
            "#,
            self.material_id as _,
            self.anchor_byte
        )
        .fetch_one(pool)
        .await?;

        Ok(result.exists)
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
