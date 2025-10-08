use chrono::Utc;
use serde_json::json;
use sinex_core::types::events::EventType;
use sinex_core::types::ulid::Ulid;
use sinex_satellite_sdk::ingestion_helpers::{
    ChangeType, IdempotenceKey, LedgerEntry, LedgerReader, RowIdentitySpec, SliceAssembler,
    SnapshotDiff, SnapshotRow, TimeQuality,
};

#[test]
fn slice_assembler_emits_complete_lines() {
    let mut assembler = SliceAssembler::line_based();

    let records = assembler.push_bytes(b"line1\nline2\nline").unwrap();
    assert_eq!(records, vec![b"line1".to_vec(), b"line2".to_vec()]);

    let remaining = assembler.flush();
    assert_eq!(remaining, Some(b"line".to_vec()));
}

#[test]
fn idempotence_key_formats_insert_sql() {
    let key = IdempotenceKey::new(Ulid::new(), 12345, EventType::from_static("file.created"));
    assert_eq!(key.anchor_byte, 12345);
    assert!(key.to_insert_sql().contains("ON CONFLICT"));
}

#[test]
fn ledger_reader_prefers_realtime_capture_quality() {
    let entries = vec![LedgerEntry {
        offset_start: 0,
        offset_end: 100,
        ts_capture: Utc::now(),
        precision: "exact".to_string(),
        source_type: "realtime_capture".to_string(),
    }];

    let reader = LedgerReader::new(Ulid::new(), entries);
    let (_ts, quality) = reader.derive_ts_orig(50, None);

    assert_eq!(quality, TimeQuality::RealtimeCapture);
}

#[test]
fn snapshot_diff_detects_inserts() {
    let mut diff = SnapshotDiff::new(RowIdentitySpec::new(vec!["id".to_string()]));

    let current_rows = vec![
        SnapshotRow {
            key: vec!["1".to_string()],
            data: json!({"id": "1", "name": "Alice"}),
            version: None,
        },
        SnapshotRow {
            key: vec!["2".to_string()],
            data: json!({"id": "2", "name": "Bob"}),
            version: None,
        },
    ];

    let changes = diff.compute_diff(current_rows);
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|c| c.change_type == ChangeType::Insert));
}

#[test]
fn snapshot_diff_detects_updates() {
    let identity_spec = RowIdentitySpec::new(vec!["id".to_string()])
        .with_tracked_columns(vec!["name".to_string(), "age".to_string()]);
    let mut diff = SnapshotDiff::new(identity_spec);

    diff.load_previous_snapshot(vec![SnapshotRow {
        key: vec!["1".to_string()],
        data: json!({"id": "1", "name": "Alice", "age": 30}),
        version: None,
    }]);

    let current_rows = vec![SnapshotRow {
        key: vec!["1".to_string()],
        data: json!({"id": "1", "name": "Alice", "age": 31}),
        version: None,
    }];

    let changes = diff.compute_diff(current_rows);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].change_type, ChangeType::Update);
    assert_eq!(changes[0].changed_columns, vec!["age".to_string()]);
}

#[test]
fn snapshot_diff_detects_deletes() {
    let mut diff = SnapshotDiff::new(RowIdentitySpec::new(vec!["id".to_string()]));
    diff.load_previous_snapshot(vec![
        SnapshotRow {
            key: vec!["1".to_string()],
            data: json!({"id": "1", "name": "Alice"}),
            version: None,
        },
        SnapshotRow {
            key: vec!["2".to_string()],
            data: json!({"id": "2", "name": "Bob"}),
            version: None,
        },
    ]);

    let current_rows = vec![SnapshotRow {
        key: vec!["1".to_string()],
        data: json!({"id": "1", "name": "Alice"}),
        version: None,
    }];

    let changes = diff.compute_diff(current_rows);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].change_type, ChangeType::Delete);
    assert_eq!(changes[0].row_key, vec!["2".to_string()]);
}

#[test]
fn snapshot_diff_detects_mixed_changes() {
    let mut diff = SnapshotDiff::new(RowIdentitySpec::new(vec!["id".to_string()]));
    diff.load_previous_snapshot(vec![
        SnapshotRow {
            key: vec!["1".to_string()],
            data: json!({"id": "1", "name": "Alice"}),
            version: None,
        },
        SnapshotRow {
            key: vec!["2".to_string()],
            data: json!({"id": "2", "name": "Bob"}),
            version: None,
        },
    ]);

    let current_rows = vec![
        SnapshotRow {
            key: vec!["1".to_string()],
            data: json!({"id": "1", "name": "Alice"}),
            version: None,
        },
        SnapshotRow {
            key: vec!["2".to_string()],
            data: json!({"id": "2", "name": "Robert"}),
            version: None,
        },
        SnapshotRow {
            key: vec!["3".to_string()],
            data: json!({"id": "3", "name": "Charlie"}),
            version: None,
        },
    ];

    let changes = diff.compute_diff(current_rows);
    assert_eq!(changes.len(), 2);
    let change_types: std::collections::HashSet<_> =
        changes.iter().map(|c| c.change_type).collect();
    assert!(change_types.contains(&ChangeType::Insert));
    assert!(change_types.contains(&ChangeType::Update));
}
