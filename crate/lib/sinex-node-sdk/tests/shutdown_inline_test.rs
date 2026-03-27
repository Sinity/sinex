#![cfg(feature = "messaging")]

use sinex_node_sdk::runtime::stream::Checkpoint;
use sinex_node_sdk::{CheckpointState, ShutdownConfig, SinexError, default_checkpoint_path};
use sinex_primitives::Timestamp;
use tempfile::TempDir;
use uuid::Uuid;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_shutdown_config_default_behavior() -> TestResult<()> {
    let config = ShutdownConfig::default();
    assert!(config.save_state_on_shutdown);
    assert!(config.restore_state_on_startup);
    assert_eq!(config.grace_period_secs, 30);
    Ok(())
}

#[sinex_test]
async fn test_shutdown_config_prefers_explicit_checkpoint_path() -> TestResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("test.checkpoint.json");
    let config = ShutdownConfig {
        checkpoint_path: Some(checkpoint_path.clone()),
        ..Default::default()
    };

    assert_eq!(config.checkpoint_path("ignored-node"), checkpoint_path);
    Ok(())
}

#[sinex_test]
async fn test_checkpoint_state_roundtrip_to_file() -> TestResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_path = temp_dir.path().join("test.checkpoint.json");
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
    state.save_to_file(&checkpoint_path).await.unwrap();

    let loaded = CheckpointState::load_from_file(&checkpoint_path)
        .await?
        .expect("state should be present after save");
    assert_eq!(loaded.checkpoint, state.checkpoint);
    assert_eq!(loaded.processed_count, state.processed_count);
    assert_eq!(loaded.last_activity, state.last_activity);
    assert_eq!(loaded.data, state.data);
    assert_eq!(loaded.version, state.version);

    CheckpointState::delete_file(&checkpoint_path)
        .await
        .unwrap();
    assert!(
        CheckpointState::load_from_file(&checkpoint_path)
            .await?
            .is_none()
    );
    Ok(())
}

#[sinex_test]
async fn test_checkpoint_state_load_from_file_surfaces_corrupt_file() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let checkpoint_path = temp_dir.path().join("corrupt.checkpoint.json");
    tokio::fs::write(&checkpoint_path, "{ definitely not valid json").await?;

    let error = CheckpointState::load_from_file(&checkpoint_path)
        .await
        .expect_err("corrupt checkpoint file should surface");
    let message = format!("{error:#}");
    assert!(matches!(error, SinexError::Serialization(_)));
    assert!(message.contains("Failed to parse checkpoint file"));
    assert!(message.contains(checkpoint_path.display().to_string().as_str()));
    Ok(())
}

#[sinex_test]
async fn test_default_checkpoint_path() -> TestResult<()> {
    let path = default_checkpoint_path("my-node");
    assert!(path.to_string_lossy().ends_with("my-node.checkpoint.json"));
    Ok(())
}

#[sinex_test]
async fn test_default_checkpoint_path_prefers_work_dir_when_runtime_dir_missing() -> TestResult<()>
{
    let temp_dir = TempDir::new()?;
    let mut env = EnvGuard::new();
    env.clear("SINEX_RUNTIME_DIR");
    env.set("SINEX_WORK_DIR", temp_dir.path().display().to_string());

    let path = default_checkpoint_path("my-node");
    assert_eq!(path, temp_dir.path().join("my-node.checkpoint.json"));
    Ok(())
}
