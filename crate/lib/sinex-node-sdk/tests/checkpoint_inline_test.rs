#![cfg(feature = "messaging")]

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
    let state = CheckpointState {
        processed_count: u64::MAX,
        ..Default::default()
    };

    let err = manager.save_checkpoint(&state).await.unwrap_err();
    assert!(matches!(err, SinexError::Checkpoint(_)));
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
        ..Default::default()
    };

    manager.save_checkpoint(&state).await?;
    let loaded = manager.load_checkpoint().await?;
    assert_eq!(loaded.processed_count, 1);
    Ok(())
}
