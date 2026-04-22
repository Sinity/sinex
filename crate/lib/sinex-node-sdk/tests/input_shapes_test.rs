use camino::Utf8PathBuf;
use sinex_node_sdk::{SqliteSourceCheckpointState, discover_importable_files_at_root};
use tempfile::tempdir;
use xtask::sandbox::prelude::*;

fn utf8_path(path: &std::path::Path) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(path.to_path_buf())
        .unwrap_or_else(|path| panic!("non-utf8 path: {}", path.display()))
}

#[sinex_test]
async fn sqlite_source_checkpoint_state_tracks_keyed_cursors() -> TestResult<()> {
    let mut state = SqliteSourceCheckpointState::default();

    state.set_cursor("browser::/tmp/history.sqlite", 2);
    state.advance_cursor("browser::/tmp/history.sqlite", 1);
    state.advance_cursor("browser::/tmp/history.sqlite", 5);

    assert_eq!(state.cursor("browser::/tmp/history.sqlite"), 5);
    assert_eq!(state.cursor("missing"), 0);
    assert!(!state.is_empty());
    Ok(())
}

#[sinex_test]
async fn sqlite_snapshot_checkpoint_state_tracks_keyed_evidence_state() -> TestResult<()> {
    let mut state = sinex_node_sdk::SqliteSnapshotCheckpointState::default();

    state
        .state_mut("browser::/tmp/history.sqlite")
        .record_success(sinex_primitives::temporal::Timestamp::now(), 42);

    assert_eq!(
        state
            .state("browser::/tmp/history.sqlite")
            .and_then(|state| state.last_snapshot_row_id),
        Some(42)
    );
    assert!(state.state("missing").is_none());
    assert!(!state.is_empty());
    Ok(())
}

#[sinex_test]
async fn discover_importable_files_remembers_scan_root() -> TestResult<()> {
    let dir = tempdir()?;
    let path = utf8_path(&dir.path().join("history.jsonl"));
    std::fs::write(path.as_std_path(), "{\"url\":\"https://example.com\"}\n")?;

    let root = utf8_path(dir.path());
    let mut state = sinex_node_sdk::BatchImporterState::default();
    let files = discover_importable_files_at_root(&mut state, &root, &[".jsonl"])?;

    assert_eq!(files.len(), 1);
    assert!(state.scan_roots.contains(&root));
    Ok(())
}
