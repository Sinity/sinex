#![cfg(feature = "messaging")]

use futures::TryStreamExt;
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

#[sinex_test]
async fn save_checkpoint_accepts_idempotent_revision_zero_retry(
    ctx: TestContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let node = "idempotent-node".to_string();
    let group = "idempotent-group".to_string();
    let consumer = "idempotent-consumer".to_string();
    let writer = CheckpointManager::new(kv.clone(), node.clone(), group.clone(), consumer.clone());
    let retry = CheckpointManager::new(kv, node, group, consumer);

    let state = CheckpointState {
        processed_count: 3,
        data: Some(json!({ "marker": "same-state" })),
        version: 9,
        ..Default::default()
    };

    let revision = writer.save_checkpoint(&state).await?;
    let retry_revision = retry.save_checkpoint(&state).await?;
    assert_eq!(retry_revision, revision);

    let loaded = writer.load_checkpoint().await?;
    assert_eq!(loaded.processed_count, state.processed_count);
    assert_eq!(loaded.data, state.data);
    assert_eq!(loaded.version, state.version);
    Ok(())
}

#[sinex_test]
async fn load_checkpoint_surfaces_corrupt_kv_state(
    ctx: TestContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv.clone(),
        "corrupt-node".to_string(),
        "corrupt-group".to_string(),
        "corrupt-consumer".to_string(),
    );
    manager.save_checkpoint(&CheckpointState::default()).await?;

    let mut keys = kv.keys().await?;
    let key = keys.try_next().await?.expect("checkpoint key should exist");
    kv.put(&key, br"{ definitely not valid json".as_slice().into())
        .await?;

    let error = manager
        .load_checkpoint()
        .await
        .expect_err("corrupt checkpoint KV should surface");
    let message = format!("{error:#}");
    assert!(matches!(error, SinexError::Serialization(_)));
    assert!(message.contains("Failed to decode checkpoint from KV"));
    assert!(message.contains(&key));
    Ok(())
}

#[sinex_test]
async fn checkpoint_stats_surface_corrupt_kv_state(
    ctx: TestContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv.clone(),
        "corrupt-stats-node".to_string(),
        "corrupt-stats-group".to_string(),
        "corrupt-stats-consumer".to_string(),
    );
    manager.save_checkpoint(&CheckpointState::default()).await?;

    let mut keys = kv.keys().await?;
    let key = keys.try_next().await?.expect("checkpoint key should exist");
    kv.put(&key, br"{ definitely not valid json".as_slice().into())
        .await?;

    let error = manager
        .get_checkpoint_stats()
        .await
        .expect_err("corrupt checkpoint stats should surface");
    let message = format!("{error:#}");
    assert!(matches!(error, SinexError::Serialization(_)));
    assert!(message.contains("Failed to decode checkpoint from KV"));
    assert!(message.contains(&key));
    Ok(())
}

#[sinex_test]
async fn load_checkpoint_rejects_empty_kv_state(
    ctx: TestContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv.clone(),
        "empty-node".to_string(),
        "empty-group".to_string(),
        "empty-consumer".to_string(),
    );
    manager.save_checkpoint(&CheckpointState::default()).await?;

    let mut keys = kv.keys().await?;
    let key = keys.try_next().await?.expect("checkpoint key should exist");
    kv.put(&key, Vec::<u8>::new().into()).await?;

    let error = manager
        .load_checkpoint()
        .await
        .expect_err("empty checkpoint KV should not be treated as a fresh start");
    let message = format!("{error:#}");
    assert!(matches!(error, SinexError::Checkpoint(_)));
    assert!(message.contains("Checkpoint KV entry is empty"));
    assert!(message.contains(&key));
    Ok(())
}
