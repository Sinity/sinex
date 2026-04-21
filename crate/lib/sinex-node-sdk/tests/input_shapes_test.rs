use camino::Utf8PathBuf;
use sinex_node_sdk::{SqliteSourceCheckpointState, discover_importable_files_at_root};
use tempfile::tempdir;
use xtask::sandbox::prelude::*;

fn utf8_path(path: &std::path::Path) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(path.to_path_buf())
        .unwrap_or_else(|path| panic!("non-utf8 path: {}", path.display()))
}

#[sinex_test]
async fn sqlite_source_checkpoint_state_tracks_keyed_cursors(_ctx: TestContext) -> TestResult<()> {
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
async fn discover_importable_files_remembers_scan_root(_ctx: TestContext) -> TestResult<()> {
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
