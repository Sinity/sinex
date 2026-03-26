//! Concurrent checkpoint update testing
//!
//! Exercises NATS KV checkpoint persistence under concurrent updates.

use uuid::Uuid;
use sinex_primitives::temporal::Timestamp;
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use xtask::sandbox::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
const DEFAULT_GROUP: &str = "concurrency";
const DEFAULT_CONSUMER: &str = "worker";

#[sinex_test]
async fn test_concurrent_checkpoint_updates_basic(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let node_name = format!("concurrent_test_node_{}", Uuid::now_v7().to_string().to_lowercase());
    let manager = CheckpointManager::new(
        kv,
        node_name,
        DEFAULT_GROUP.to_string(),
        DEFAULT_CONSUMER.to_string(),
    );

    let updates = 50u64;
    let successes = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for i in 0..updates {
        let manager = manager.clone();
        let successes = successes.clone();
        handles.push(tokio::spawn(async move {
            let mut state = CheckpointState::default();
            state.checkpoint = Checkpoint::internal(Uuid::now_v7(), i + 1);
            state.processed_count = i + 1;
            state.last_activity = Timestamp::now();
            if manager.save_checkpoint(&state).await.is_ok() {
                successes.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    futures::future::join_all(handles).await;

    let successful = successes.load(Ordering::SeqCst);
    assert_eq!(
        successful, 1,
        "exactly one revision-0 checkpoint create should succeed"
    );

    let loaded = manager.load_checkpoint().await?;
    assert!(loaded.processed_count >= 1, "should record progress");
    assert!(
        loaded.processed_count <= updates,
        "checkpoint count should stay within expected bounds"
    );

    Ok(())
}

#[sinex_test]
async fn test_stale_initial_revision_does_not_clobber_existing_checkpoint(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let node_name = format!("checkpoint_cas_node_{}", Uuid::now_v7().to_string().to_lowercase());
    let manager = CheckpointManager::new(
        kv,
        node_name,
        DEFAULT_GROUP.to_string(),
        DEFAULT_CONSUMER.to_string(),
    );

    let mut initial = CheckpointState::default();
    initial.checkpoint = Checkpoint::internal(Uuid::now_v7(), 1);
    initial.processed_count = 1;
    initial.last_activity = Timestamp::now();
    initial.data = Some(serde_json::json!({ "seq": 1 }));
    manager.save_checkpoint(&initial).await?;

    let mut stale = CheckpointState::default();
    stale.checkpoint = Checkpoint::internal(Uuid::now_v7(), 99);
    stale.processed_count = 99;
    stale.last_activity = Timestamp::now();
    stale.data = Some(serde_json::json!({ "seq": 99 }));
    let stale_error = manager
        .save_checkpoint(&stale)
        .await
        .expect_err("stale revision-0 save must not overwrite an existing checkpoint");
    assert!(
        stale_error.to_string().contains("Failed to create checkpoint"),
        "unexpected stale save error: {stale_error}"
    );

    let loaded = manager.load_checkpoint().await?;
    assert_eq!(
        loaded.processed_count, initial.processed_count,
        "stale revision-0 save must not clobber the established checkpoint"
    );
    assert_eq!(loaded.data, initial.data);

    Ok(())
}
