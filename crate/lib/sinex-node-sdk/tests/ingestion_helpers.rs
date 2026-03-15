use serde_json::json;
use sinex_node_sdk::ingestion_helpers::{
    ChangeType, IdempotenceKey, LedgerEntry, LedgerReader, RowIdentitySpec, SliceAssembler,
    SnapshotDiff, SnapshotRow,
};
use sinex_primitives::domain::{TemporalPrecision, TemporalSourceType};
use sinex_primitives::{EventType, Timestamp, Uuid};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn slice_assembler_emits_complete_lines() -> color_eyre::Result<()> {
    let mut assembler = SliceAssembler::line_based();

    let records = assembler.push_bytes(b"line1\nline2\nline").unwrap();
    assert_eq!(records, vec![b"line1".to_vec(), b"line2".to_vec()]);

    let remaining = assembler.flush();
    assert_eq!(remaining, Some(b"line".to_vec()));

    Ok(())
}

#[sinex_test]
async fn idempotence_key_constructor_sets_fields() -> color_eyre::Result<()> {
    let key = IdempotenceKey::new(
        Uuid::now_v7(),
        12345,
        EventType::from_static("file.created"),
    );
    assert_eq!(key.anchor_byte, 12345);
    assert_eq!(key.event_type.as_str(), "file.created");

    Ok(())
}

#[sinex_test]
async fn ledger_reader_prefers_realtime_capture_quality() -> color_eyre::Result<()> {
    let entries = vec![LedgerEntry {
        offset_start: 0,
        offset_end: 100,
        ts_capture: Timestamp::now(),
        precision: TemporalPrecision::Exact,
        source_type: TemporalSourceType::RealtimeCapture,
    }];

    let reader = LedgerReader::new(Uuid::now_v7(), entries);
    let (_ts, source_type) = reader
        .derive_ts_orig(50, None)
        .expect("should resolve from ledger entry");

    assert_eq!(source_type, TemporalSourceType::RealtimeCapture);

    Ok(())
}

#[sinex_test]
async fn ledger_reader_staged_at_provides_fallback() -> color_eyre::Result<()> {
    let staged_ts = Timestamp::now();
    let entries = vec![LedgerEntry {
        offset_start: 0,
        offset_end: i64::MAX,
        ts_capture: staged_ts,
        precision: TemporalPrecision::Bounded,
        source_type: TemporalSourceType::StagedAt,
    }];

    let reader = LedgerReader::new(Uuid::now_v7(), entries);
    let (ts, source_type) = reader
        .derive_ts_orig(42, None)
        .expect("should resolve from staged_at entry");

    assert_eq!(source_type, TemporalSourceType::StagedAt);
    assert_eq!(ts, staged_ts);

    Ok(())
}

#[sinex_test]
async fn ledger_reader_returns_none_without_entries_or_intrinsic() -> color_eyre::Result<()> {
    let reader = LedgerReader::new(Uuid::now_v7(), vec![]);
    let result = reader.derive_ts_orig(0, None);

    assert!(
        result.is_none(),
        "should return None when no ledger entry and no intrinsic timestamp"
    );

    Ok(())
}

#[sinex_test]
async fn ledger_reader_intrinsic_overrides_when_no_ledger() -> color_eyre::Result<()> {
    let intrinsic_ts = Timestamp::now();
    let reader = LedgerReader::new(Uuid::now_v7(), vec![]);
    let (ts, source_type) = reader
        .derive_ts_orig(0, Some(intrinsic_ts))
        .expect("should resolve from intrinsic timestamp");

    assert_eq!(source_type, TemporalSourceType::IntrinsicContent);
    assert_eq!(ts, intrinsic_ts);

    Ok(())
}

#[sinex_test]
async fn snapshot_diff_detects_inserts() -> color_eyre::Result<()> {
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

    Ok(())
}

#[sinex_test]
async fn snapshot_diff_detects_updates() -> color_eyre::Result<()> {
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

    Ok(())
}

#[sinex_test]
async fn snapshot_diff_detects_deletes() -> color_eyre::Result<()> {
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

    Ok(())
}

#[sinex_test]
async fn snapshot_diff_detects_mixed_changes() -> color_eyre::Result<()> {
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
        changes.iter().map(|c| c.change_type.clone()).collect();
    assert!(change_types.contains(&ChangeType::Insert));
    assert!(change_types.contains(&ChangeType::Update));

    Ok(())
}
