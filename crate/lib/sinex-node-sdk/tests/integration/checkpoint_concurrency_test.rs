//! Concurrent checkpoint update testing
//!
//! Exercises NATS KV checkpoint persistence under concurrent updates.

use sinex_primitives::Ulid;
use sinex_primitives::temporal::Timestamp;
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use xtask::sandbox::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_GROUP: &str = "concurrency";
const DEFAULT_CONSUMER: &str = "worker";

#[sinex_test]
async fn test_concurrent_checkpoint_updates_basic(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let processor_name = format!("concurrent_test_processor_{}", Ulid::new().to_string().to_lowercase());
    let manager = CheckpointManager::new(
        kv,
        processor_name,
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
            state.checkpoint = Checkpoint::internal(Ulid::new(), i + 1);
            state.processed_count = i + 1;
            state.last_activity = Timestamp::now();
            if manager.save_checkpoint(&state).await.is_ok() {
                successes.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    futures::future::join_all(handles).await;

    let successful = successes.load(Ordering::SeqCst);
    assert_eq!(successful, updates, "all concurrent saves should succeed");

    let loaded = manager.load_checkpoint().await?;
    assert!(loaded.processed_count >= 1, "should record progress");
    assert!(
        loaded.processed_count <= updates,
        "checkpoint count should stay within expected bounds"
    );

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_last_write_wins(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let processor_name = format!("last_write_processor_{}", Ulid::new().to_string().to_lowercase());
    let manager = CheckpointManager::new(
        kv,
        processor_name,
        DEFAULT_GROUP.to_string(),
        DEFAULT_CONSUMER.to_string(),
    );

    let updates = 10u64;
    let mut handles = Vec::new();

    for i in 0..updates {
        let manager = manager.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis((updates - i) * 10)).await;
            let mut state = CheckpointState::default();
            state.checkpoint = Checkpoint::internal(Ulid::new(), i + 1);
            state.processed_count = i + 1;
            state.last_activity = Timestamp::now();
            state.data = Some(serde_json::json!({"seq": i + 1}));
            manager.save_checkpoint(&state).await
        }));
    }

    for handle in handles {
        handle.await??;
    }

    let loaded = manager.load_checkpoint().await?;
    assert_eq!(
        loaded.processed_count, updates,
        "last writer should win with highest count"
    );

    Ok(())
}