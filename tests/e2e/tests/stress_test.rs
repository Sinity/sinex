// # System Stress Testing
//
// Focused stress tests that exercise production checkpoint persistence and
// event ingestion under concurrent load.

use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_primitives::Uuid;
use sinex_primitives::{DynamicPayload, Timestamp};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use xtask::sandbox::prelude::*;

const STRESS_GROUP: &str = "stress";

#[sinex_test(timeout = 120)]
#[ignore = "stress workload is excluded from the default suite"]
async fn test_checkpoint_kv_stress_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let kv = ctx.checkpoint_kv().await?;
    let node_name = format!("stress_node_{}", Uuid::now_v7().to_string().to_lowercase());

    let consumer_count = 16usize;
    let updates_per_consumer = 40u64;
    let total_updates = consumer_count as u64 * updates_per_consumer;

    let successes = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::new();

    for consumer_id in 0..consumer_count {
        let manager = CheckpointManager::new(
            kv.clone(),
            node_name.clone(),
            STRESS_GROUP.to_string(),
            format!("worker-{consumer_id}"),
        );
        let successes = successes.clone();
        handles.push(tokio::spawn(async move {
            for update in 0..updates_per_consumer {
                let mut state = CheckpointState::default();
                state.checkpoint = Checkpoint::internal(Uuid::now_v7(), update + 1);
                state.processed_count = update + 1;
                state.last_activity = Timestamp::now();
                if manager.save_checkpoint(&state).await.is_ok() {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    futures::future::join_all(handles).await;

    let duration = start.elapsed();
    let successful = successes.load(Ordering::Relaxed);
    println!("Checkpoint KV stress: {successful} updates in {duration:?}");

    assert_eq!(
        successful, total_updates,
        "all checkpoint updates should succeed"
    );

    for consumer_id in 0..consumer_count {
        let manager = CheckpointManager::new(
            kv.clone(),
            node_name.clone(),
            STRESS_GROUP.to_string(),
            format!("worker-{consumer_id}"),
        );
        let state = manager.load_checkpoint().await?;
        assert_eq!(
            state.processed_count, updates_per_consumer,
            "consumer {consumer_id} should report full progress"
        );
    }

    Ok(())
}

#[sinex_test(timeout = 120)]
#[ignore = "stress workload is excluded from the default suite"]
async fn test_event_ingestion_stress(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let total_events = 200usize;

    // Build all payloads upfront, then publish as a single batch.
    // publish_many() handles source materials, NATS transport, and DB persistence wait.
    let payloads: Vec<DynamicPayload> = (0..total_events)
        .map(|i| {
            DynamicPayload::new(
                "stress.ingestion",
                "bulk_load",
                serde_json::json!({"sequence": i}),
            )
        })
        .collect();

    let published = ctx.publish_many(payloads).await?;

    assert!(
        published.len() >= total_events,
        "expected at least {total_events} events, got {}",
        published.len()
    );

    Ok(())
}
