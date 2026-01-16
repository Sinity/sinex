// # System Stress Testing
//
// Focused stress tests that exercise production checkpoint persistence and
// event ingestion under concurrent load.

use sinex_core::db::models::EventFactory;
use sinex_core::types::ulid::Ulid;
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_test_utils::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const STRESS_GROUP: &str = "stress";

#[sinex_test(timeout = 120)]
async fn test_checkpoint_kv_stress_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let kv = ctx.checkpoint_kv().await?;
    let processor = format!("stress_processor_{}", Ulid::new());

    let consumer_count = 16usize;
    let updates_per_consumer = 40u64;
    let total_updates = consumer_count as u64 * updates_per_consumer;

    let successes = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::new();

    for consumer_id in 0..consumer_count {
        let manager = CheckpointManager::new(
            kv.clone(),
            processor.clone(),
            STRESS_GROUP.to_string(),
            format!("worker-{consumer_id}"),
        );
        let successes = successes.clone();
        handles.push(tokio::spawn(async move {
            for update in 0..updates_per_consumer {
                let mut state = CheckpointState::default();
                state.checkpoint = Checkpoint::internal(Ulid::new(), update + 1);
                state.processed_count = update + 1;
                state.last_activity = chrono::Utc::now();
                if manager.save_checkpoint(&state).await.is_ok() {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    futures::future::join_all(handles).await;

    let duration = start.elapsed();
    let successful = successes.load(Ordering::Relaxed);
    println!(
        "Checkpoint KV stress: {} updates in {:?}",
        successful, duration
    );

    assert_eq!(
        successful, total_updates,
        "all checkpoint updates should succeed"
    );

    for consumer_id in 0..consumer_count {
        let manager = CheckpointManager::new(
            kv.clone(),
            processor.clone(),
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
async fn test_event_ingestion_stress(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let total_events = 200usize;
    let mut handles = Vec::new();

    for i in 0..total_events {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut event = EventFactory::new("stress.ingestion")
                .create_event("bulk_load", serde_json::json!({"sequence": i}));
            event.host = "localhost".to_string();
            event.ingestor_version = Some("1.0.0".to_string());
            sinex_core::db::insert_event_with_validator(&pool, &event, None).await
        }));
    }

    for handle in handles {
        handle.await??;
    }

    let inserted: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source = $1",
        "stress.ingestion"
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    assert!(
        inserted >= total_events as i64,
        "expected at least {total_events} events, got {inserted}"
    );

    sqlx::query!("DELETE FROM core.events WHERE source = $1", "stress.ingestion")
        .execute(&pool)
        .await
        .ok();

    Ok(())
}