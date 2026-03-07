#![cfg(feature = "messaging")]

use sinex_node_sdk::runtime::stream::Checkpoint;
use sinex_node_sdk::{CheckpointState, ShutdownHandler, default_checkpoint_path};
use sinex_primitives::Timestamp;
use tempfile::TempDir;
use uuid::Uuid;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_shutdown_handler_creation() -> TestResult<()> {
    let handler = ShutdownHandler::new("/tmp/test.checkpoint");
    assert!(!handler.signal().is_shutdown_requested());
    Ok(())
}

#[sinex_test]
async fn test_manual_shutdown() -> TestResult<()> {
    let handler = ShutdownHandler::new("/tmp/test.checkpoint");
    let signal = handler.signal();

    assert!(!signal.is_shutdown_requested());
    handler.trigger_shutdown();
    assert!(signal.is_shutdown_requested());
    Ok(())
}

#[sinex_test]
async fn test_state_save_load() -> TestResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("test.checkpoint.json");

    let handler = ShutdownHandler::new(&checkpoint_path);

    let state = CheckpointState {
        checkpoint: Checkpoint::internal(Uuid::now_v7(), 42),
        processed_count: 7,
        last_activity: Timestamp::now(),
        data: Some(serde_json::json!({
            "cursor": "abc",
            "done": true,
        })),
        version: 2,
        revision: 0,
    };
    handler.save_state(&state).await.unwrap();

    let loaded = handler
        .load_state()
        .await
        .expect("state should be present after save");
    assert_eq!(loaded.checkpoint, state.checkpoint);
    assert_eq!(loaded.processed_count, state.processed_count);
    assert_eq!(loaded.last_activity, state.last_activity);
    assert_eq!(loaded.data, state.data);
    assert_eq!(loaded.version, state.version);

    handler.clear_state().await.unwrap();
    assert!(handler.load_state().await.is_none());
    Ok(())
}

#[sinex_test]
async fn test_default_checkpoint_path() -> TestResult<()> {
    let path = default_checkpoint_path("my-node");
    assert!(path.to_string_lossy().ends_with("my-node.checkpoint.json"));
    Ok(())
}
