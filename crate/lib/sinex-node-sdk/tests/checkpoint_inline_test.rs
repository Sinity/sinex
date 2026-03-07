#![cfg(feature = "messaging")]

use serde_json::json;
use sinex_node_sdk::{CheckpointManager, CheckpointState, SinexError};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn save_checkpoint_rejects_processed_count_overflow(
    ctx: TestContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv,
        "node".to_string(),
        "group".to_string(),
        "consumer".to_string(),
    );
    let baseline = CheckpointState {
        processed_count: 7,
        data: Some(json!({ "marker": "baseline" })),
        version: 42,
        ..Default::default()
    };
    manager.save_checkpoint(&baseline).await?;

    let state = CheckpointState {
        processed_count: u64::MAX,
        ..Default::default()
    };

    let err = manager.save_checkpoint(&state).await.unwrap_err();
    assert!(matches!(err, SinexError::Checkpoint(_)));
    let loaded = manager.load_checkpoint().await?;
    assert_eq!(loaded.processed_count, baseline.processed_count);
    assert_eq!(loaded.data, baseline.data);
    assert_eq!(loaded.version, baseline.version);
    Ok(())
}

#[sinex_test]
async fn checkpoint_keys_accept_invalid_chars(
    ctx: TestContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv,
        "node:with:colons".to_string(),
        "group.with.dots".to_string(),
        "consumer name with spaces".to_string(),
    );
    let state = CheckpointState {
        processed_count: 1,
        data: Some(json!({
            "source": "checkpoint_keys_accept_invalid_chars",
            "path": "node:with:colons/group.with.dots/consumer name with spaces",
        })),
        version: 7,
        ..Default::default()
    };

    manager.save_checkpoint(&state).await?;
    let loaded = manager.load_checkpoint().await?;
    assert_eq!(loaded.processed_count, 1);
    assert_eq!(loaded.data, state.data);
    assert_eq!(loaded.version, 7);
    assert_eq!(loaded.checkpoint, state.checkpoint);
    Ok(())
}
