//! Ingestion helper utilities as specified in TARGET_final.md section 5
//!
//! This module provides helpers for ingestors to process MaterialSliceStream:
//! - SliceAssembler for record reassembly
//! - LedgerReader and derive_ts_orig for timestamp computation
//! - IdempotenceKey for first-order event deduplication
//! - SnapshotDiff for snapshot sources (diff to inserts/updates/deletes)

use chrono::{DateTime, Utc};
use color_eyre::eyre::Result;
use serde_json;
use sinex_core::types::{
    domain::{EventSource, EventType},
    ulid::Ulid,
};
use std::collections::VecDeque;
use tracing::{debug, warn};

/// SliceAssembler for record reassembly (e.g., line or JSON delimiter)
/// TARGET_final.md line 120
pub struct SliceAssembler {
    delimiter: Vec<u8>,
    buffer: Vec<u8>,
    max_record_size: usize,
}

impl SliceAssembler {
    /// Create a new assembler with the specified delimiter
    pub fn new(delimiter: Vec<u8>) -> Self {
        Self {
            delimiter,
            buffer: Vec::new(),
            max_record_size: 10 * 1024 * 1024, // 10MB default
        }
    }

    /// Create a line-based assembler
    pub fn line_based() -> Self {
        Self::new(b"\n".to_vec())
    }

    /// Create a JSON lines assembler
    pub fn jsonl() -> Self {
        Self::new(b"\n".to_vec())
    }

    /// Set maximum record size
    pub fn with_max_size(mut self, size: usize) -> Self {
        self.max_record_size = size;
        self
    }

    /// Add bytes and extract complete records
    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
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

/// Time quality indicator for ts_orig derivation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeQuality {
    /// Realtime capture with known precision
    RealtimeCapture,
    /// Timestamp from content (e.g., log timestamp)
    IntrinsicContent,
    /// Inferred from file mtime
    InferredMtime,
    /// Inferred from file ctime
    InferredCtime,
    /// User-provided timestamp
    InferredUser,
    /// Staged timestamp (fallback)
    StagedAt,
}

impl TimeQuality {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RealtimeCapture => "realtime_capture",
            Self::IntrinsicContent => "intrinsic_content",
            Self::InferredMtime => "inferred_mtime",
            Self::InferredCtime => "inferred_ctime",
            Self::InferredUser => "inferred_user",
            Self::StagedAt => "staged_at",
        }
    }
}

/// LedgerReader for accessing temporal ledger entries
/// TARGET_final.md line 121
pub struct LedgerReader {
    pub material_id: Ulid,
    pub entries: VecDeque<LedgerEntry>,
}

#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub offset_start: i64,
    pub offset_end: i64,
    pub ts_capture: DateTime<Utc>,
    pub precision: String,   // "exact" or "bounded"
    pub source_type: String, // realtime_capture, intrinsic_content, etc.
}

impl LedgerReader {
    /// Create a new ledger reader with entries
    pub fn new(material_id: Ulid, entries: Vec<LedgerEntry>) -> Self {
        Self {
            material_id,
            entries: VecDeque::from(entries),
        }
    }

    /// Find the ledger entry for a given offset
    pub fn find_entry_for_offset(&self, offset: i64) -> Option<&LedgerEntry> {
        self.entries
            .iter()
            .find(|e| offset >= e.offset_start && offset < e.offset_end)
    }

    /// Derive ts_orig and time_quality based on TARGET_final.md precedence (line 80-81)
    ///
    /// Precedence: temporal ledger (realtime_capture) > intrinsic content >
    ///            inferred_mtime > inferred_ctime > inferred_user > staged_at
    ///
    /// # Panics
    ///
    /// This function contains an internal `unwrap()` that is safe because it's protected
    /// by a condition check (`if intrinsic_timestamp.is_some()`), but the unwrap is used
    /// to extract the timestamp when processing "intrinsic_content" entries.
    pub fn derive_ts_orig(
        &self,
        offset: i64,
        intrinsic_timestamp: Option<DateTime<Utc>>,
    ) -> (DateTime<Utc>, TimeQuality) {
        // First check ledger entry
        if let Some(entry) = self.find_entry_for_offset(offset) {
            match entry.source_type.as_str() {
                "realtime_capture" => {
                    return (entry.ts_capture, TimeQuality::RealtimeCapture);
                }
                "intrinsic_content" if intrinsic_timestamp.is_some() => {
                    return (intrinsic_timestamp.unwrap(), TimeQuality::IntrinsicContent);
                }
                "inferred_mtime" => {
                    return (entry.ts_capture, TimeQuality::InferredMtime);
                }
                "inferred_ctime" => {
                    return (entry.ts_capture, TimeQuality::InferredCtime);
                }
                "inferred_user" => {
                    return (entry.ts_capture, TimeQuality::InferredUser);
                }
                _ => {}
            }
        }

        // Fall back to intrinsic if available
        if let Some(ts) = intrinsic_timestamp {
            return (ts, TimeQuality::IntrinsicContent);
        }

        // Ultimate fallback to current time (staged_at)
        (Utc::now(), TimeQuality::StagedAt)
    }
}

/// IdempotenceKey helper for first-order events
/// TARGET_final.md line 122
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotenceKey {
    pub material_id: Ulid,
    pub anchor_byte: i64,
    pub event_type: EventType,
}

impl IdempotenceKey {
    /// Create a new idempotence key
    pub fn new(material_id: Ulid, anchor_byte: i64, event_type: EventType) -> Self {
        Self {
            material_id,
            anchor_byte,
            event_type,
        }
    }

    /// Generate SQL for insert_or_ignore semantics
    pub fn to_insert_sql(&self) -> String {
        "INSERT INTO core.events (...) VALUES (...) 
            ON CONFLICT (source_material_id, anchor_byte) 
            WHERE source_material_id IS NOT NULL 
            DO NOTHING"
            .to_string()
    }

    /// Check if this key would conflict with existing events
    pub async fn exists_in_db(&self, pool: &sqlx::PgPool) -> Result<bool> {
        let result = sqlx::query!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM core.events 
                WHERE source_material_id = $1 
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
    pub material_id: Ulid,
    pub anchor_rule_id: String,
    pub anchor_rule_version: String,
}

impl AnchorComputer {
    /// Compute anchor byte for a given offset in the material
    pub fn compute_anchor(&self, _offset: i64, record_boundary: i64) -> i64 {
        // For now, use the start of the record as the anchor
        // This ensures deterministic anchoring
        record_boundary
    }

    /// Validate that computed anchor matches expected
    pub fn validate_anchor(&self, computed: i64, expected: i64) -> bool {
        if computed != expected {
            warn!(
                "Anchor mismatch for material {}: computed={}, expected={}",
                self.material_id, computed, expected
            );
            false
        } else {
            true
        }
    }
}

/// RowIdentitySpec for defining how to identify unique rows in snapshot data
/// TARGET_final.md line 122
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
    pub fn new(key_columns: Vec<String>) -> Self {
        Self {
            key_columns,
            tracked_columns: Vec::new(),
            timestamp_column: None,
        }
    }

    pub fn with_tracked_columns(mut self, columns: Vec<String>) -> Self {
        self.tracked_columns = columns;
        self
    }

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
    pub version: Option<DateTime<Utc>>,
}

/// Types of changes detected in snapshot diffs
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// SnapshotDiff for converting snapshot sources to inserts/updates/deletes
/// TARGET_final.md line 122
pub struct SnapshotDiff {
    identity_spec: RowIdentitySpec,
    previous_snapshot: std::collections::HashMap<Vec<String>, SnapshotRow>,
}

impl SnapshotDiff {
    /// Create a new SnapshotDiff with identity specification
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
            if !seen_keys.contains(&key) {
                if let Some(deleted_row) = self.previous_snapshot.remove(&key) {
                    changes.push(SnapshotChange {
                        change_type: ChangeType::Delete,
                        row_key: key,
                        old_data: Some(deleted_row.data),
                        new_data: None,
                        changed_columns: Vec::new(),
                    });
                }
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
            let all_keys: std::collections::HashSet<_> = old_obj
                .keys()
                .chain(new_obj.keys())
                .map(|s| s.to_string())
                .collect();
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
mod tests {
    use super::*;

    #[test]
    fn test_slice_assembler_lines() {
        let mut assembler = SliceAssembler::line_based();

        let records = assembler.push_bytes(b"line1\nline2\nline").unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], b"line1");
        assert_eq!(records[1], b"line2");

        let remaining = assembler.flush();
        assert_eq!(remaining, Some(b"line".to_vec()));
    }

    #[test]
    fn test_idempotence_key() {
        let key = IdempotenceKey::new(Ulid::new(), 12345, EventType::from_static("file.created"));

        assert_eq!(key.anchor_byte, 12345);
        assert!(key.to_insert_sql().contains("ON CONFLICT"));
    }

    #[test]
    fn test_time_quality_precedence() {
        let entries = vec![LedgerEntry {
            offset_start: 0,
            offset_end: 100,
            ts_capture: Utc::now(),
            precision: "exact".to_string(),
            source_type: "realtime_capture".to_string(),
        }];

        let reader = LedgerReader::new(Ulid::new(), entries);
        let (ts, quality) = reader.derive_ts_orig(50, None);

        assert_eq!(quality, TimeQuality::RealtimeCapture);
    }

    #[test]
    fn test_snapshot_diff_inserts() {
        let identity_spec = RowIdentitySpec::new(vec!["id".to_string()]);
        let mut diff = SnapshotDiff::new(identity_spec);

        let current_rows = vec![
            SnapshotRow {
                key: vec!["1".to_string()],
                data: serde_json::json!({"id": "1", "name": "Alice"}),
                version: None,
            },
            SnapshotRow {
                key: vec!["2".to_string()],
                data: serde_json::json!({"id": "2", "name": "Bob"}),
                version: None,
            },
        ];

        let changes = diff.compute_diff(current_rows);
        assert_eq!(changes.len(), 2);
        assert!(changes.iter().all(|c| c.change_type == ChangeType::Insert));
    }

    #[test]
    fn test_snapshot_diff_updates() {
        let identity_spec = RowIdentitySpec::new(vec!["id".to_string()])
            .with_tracked_columns(vec!["name".to_string(), "age".to_string()]);
        let mut diff = SnapshotDiff::new(identity_spec);

        // Load initial snapshot
        let initial_rows = vec![SnapshotRow {
            key: vec!["1".to_string()],
            data: serde_json::json!({"id": "1", "name": "Alice", "age": 30}),
            version: None,
        }];
        diff.load_previous_snapshot(initial_rows);

        // Apply update
        let current_rows = vec![SnapshotRow {
            key: vec!["1".to_string()],
            data: serde_json::json!({"id": "1", "name": "Alice", "age": 31}),
            version: None,
        }];

        let changes = diff.compute_diff(current_rows);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type, ChangeType::Update);
        assert_eq!(changes[0].changed_columns, vec!["age".to_string()]);
    }

    #[test]
    fn test_snapshot_diff_deletes() {
        let identity_spec = RowIdentitySpec::new(vec!["id".to_string()]);
        let mut diff = SnapshotDiff::new(identity_spec);

        // Load initial snapshot with 2 rows
        let initial_rows = vec![
            SnapshotRow {
                key: vec!["1".to_string()],
                data: serde_json::json!({"id": "1", "name": "Alice"}),
                version: None,
            },
            SnapshotRow {
                key: vec!["2".to_string()],
                data: serde_json::json!({"id": "2", "name": "Bob"}),
                version: None,
            },
        ];
        diff.load_previous_snapshot(initial_rows);

        // Current snapshot only has 1 row
        let current_rows = vec![SnapshotRow {
            key: vec!["1".to_string()],
            data: serde_json::json!({"id": "1", "name": "Alice"}),
            version: None,
        }];

        let changes = diff.compute_diff(current_rows);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type, ChangeType::Delete);
        assert_eq!(changes[0].row_key, vec!["2".to_string()]);
    }

    #[test]
    fn test_snapshot_diff_mixed_changes() {
        let identity_spec = RowIdentitySpec::new(vec!["id".to_string()]);
        let mut diff = SnapshotDiff::new(identity_spec);

        // Load initial snapshot
        let initial_rows = vec![
            SnapshotRow {
                key: vec!["1".to_string()],
                data: serde_json::json!({"id": "1", "name": "Alice"}),
                version: None,
            },
            SnapshotRow {
                key: vec!["2".to_string()],
                data: serde_json::json!({"id": "2", "name": "Bob"}),
                version: None,
            },
        ];
        diff.load_previous_snapshot(initial_rows);

        // Current snapshot has: 1 update, 1 insert, 1 delete (row 2 is gone)
        let current_rows = vec![
            SnapshotRow {
                key: vec!["1".to_string()],
                data: serde_json::json!({"id": "1", "name": "Alice Smith"}), // Updated
                version: None,
            },
            SnapshotRow {
                key: vec!["3".to_string()],
                data: serde_json::json!({"id": "3", "name": "Charlie"}), // New
                version: None,
            },
        ];

        let changes = diff.compute_diff(current_rows);
        assert_eq!(changes.len(), 3);

        let inserts: Vec<_> = changes
            .iter()
            .filter(|c| c.change_type == ChangeType::Insert)
            .collect();
        let updates: Vec<_> = changes
            .iter()
            .filter(|c| c.change_type == ChangeType::Update)
            .collect();
        let deletes: Vec<_> = changes
            .iter()
            .filter(|c| c.change_type == ChangeType::Delete)
            .collect();

        assert_eq!(inserts.len(), 1);
        assert_eq!(updates.len(), 1);
        assert_eq!(deletes.len(), 1);
    }
}
