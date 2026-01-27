use std::fs;
use std::time::Duration;

use camino::Utf8PathBuf;
use sinex_core::types::utils::file_watcher::{FileChangeKind, FileWatcher, FileWatcherConfig};
use sinex_core::types::validation::FileWatchingSecurityPolicy;
use xtask::sandbox::sinex_test;
use tempfile::TempDir;
use tokio::time::sleep;

#[sinex_test]
async fn file_watcher_builder_creates_instance() -> TestResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let watcher = FileWatcher::new(
        FileWatcherConfig::builder()
            .watch_paths(vec![Utf8PathBuf::from_path_buf(
                temp_dir.path().to_path_buf(),
            )
            .unwrap()])
            .recursive(true)
            .debounce_delay(Duration::from_millis(50))
            .build(),
    );

    assert!(watcher.is_ok());
    Ok(())
}

#[sinex_test]
async fn file_watcher_surfaces_file_events() -> TestResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");

    let mut watcher = FileWatcher::new(
        FileWatcherConfig::builder()
            .watch_paths(vec![Utf8PathBuf::from_path_buf(
                temp_dir.path().to_path_buf(),
            )
            .unwrap()])
            .recursive(false)
            .event_kinds(vec![FileChangeKind::Created, FileChangeKind::Modified])
            .build(),
    )?;

    sleep(Duration::from_millis(100)).await;
    fs::write(&test_file, "test content")?;
    sleep(Duration::from_millis(100)).await;

    let mut events = Vec::new();
    while let Some(event) = watcher.try_next_event() {
        events.push(event);
    }

    assert!(!events.is_empty());
    assert!(events.iter().any(|e| e.path == test_file));
    Ok(())
}

#[sinex_test]
async fn file_watcher_enforces_security_validation() -> TestResult<()> {
    let dangerous_config = FileWatcherConfig::builder()
        .watch_paths(vec![Utf8PathBuf::from("/etc/passwd")])
        .security_policy(FileWatchingSecurityPolicy::default())
        .build();

    let watcher_result = FileWatcher::new(dangerous_config);
    assert!(watcher_result.is_err());
    assert!(watcher_result
        .unwrap_err()
        .to_string()
        .contains("validation failed"));
    Ok(())
}

#[sinex_test]
async fn file_watcher_respects_policy_modes() -> TestResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf()).unwrap();

    let permissive_config = FileWatcherConfig::builder()
        .watch_paths(vec![path.clone()])
        .security_policy(FileWatchingSecurityPolicy::permissive())
        .build();
    assert!(FileWatcher::new(permissive_config).is_ok());

    let restrictive_config = FileWatcherConfig::builder()
        .watch_paths(vec![path])
        .security_policy(FileWatchingSecurityPolicy::restrictive())
        .build();
    let restrictive_result = FileWatcher::new(restrictive_config);
    if restrictive_result.is_err() {
        tracing::debug!(
            "Restrictive policy rejected temp dir: {}",
            restrictive_result.unwrap_err()
        );
    }

    Ok(())
}
