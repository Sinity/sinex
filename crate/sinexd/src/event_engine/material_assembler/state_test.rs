use super::*;
use blake3::Hasher;
use std::collections::BTreeMap;
use tempfile::tempdir;
use xtask::sandbox::prelude::*;

fn test_state(material_id: Uuid) -> AssemblerState {
    let temp_dir = tempdir().expect("temp dir should be creatable");
    AssemblerState {
        material_id,
        temp_path: temp_dir.path().join(TEMP_FILE_NAME),
        temp_file: None,
        wal_file: None,
        wal_seq: 0,
        expected_offset: 0,
        slice_count: 0,
        buffered_slices: BTreeMap::new(),
        buffered_bytes: 0,
        state_dir: temp_dir.path().to_path_buf(),
        started_at: Timestamp::now(),
        material_kind: "test".to_string(),
        source_identifier: "test".to_string(),
        metadata: JsonValue::Null,
        phase: AssemblyPhase::PendingBegin,
        hasher: Hasher::new(),
        pending_write: None,
        pending_end: None,
        last_slice_received: Timestamp::now(),
        staged_bytes_since_sync: 0,
        wal_entries_since_sync: 0,
        wal_bytes_since_sync: 0,
        last_staged_sync: Instant::now(),
        last_wal_sync: Instant::now(),
    }
}

#[sinex_test]
async fn missing_buffered_slice_returns_error_instead_of_panic() -> TestResult<()> {
    let material_id = Uuid::now_v7();
    let mut state = test_state(material_id);

    let result = take_buffered_slice(&mut state, material_id, 42);

    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn buffered_slice_is_removed_and_returned() -> TestResult<()> {
    let material_id = Uuid::now_v7();
    let mut state = test_state(material_id);
    let buffer_path = state.state_dir.join("buffers/42.bin");
    state.buffered_slices.insert(42, buffer_path.clone());

    let result = take_buffered_slice(&mut state, material_id, 42).unwrap();

    assert_eq!(result, buffer_path);
    assert!(state.buffered_slices.is_empty());
    Ok(())
}

#[sinex_test]
async fn parse_material_started_at_rejects_invalid_timestamp() -> TestResult<()> {
    let material_id = Uuid::now_v7();

    let error = parse_material_started_at(material_id, "not-a-timestamp", "begin message")
        .expect_err("invalid started_at must fail honestly");

    assert!(error.to_string().contains("Invalid started_at"));
    assert!(error.to_string().contains("begin message"));
    Ok(())
}
