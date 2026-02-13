// # Concurrency Test Suite
//
// Comprehensive concurrency and race condition testing.
// This module tests system behavior under concurrent access patterns.
//
// ## Test Categories
// - **Race Conditions**: Worker claiming, event causality, data consistency
// - **Worker Coordination**: Synchronization, deadlock prevention, resource sharing
// - **Database Concurrency**: Transaction isolation, lock contention, deadlock detection
// - **Memory Concurrency**: Shared state, atomic operations

use futures::future::join_all;
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_primitives::ulid::Ulid;
use sinex_primitives::{DynamicPayload, EventSource, Timestamp};
use std::sync::Arc;
use tokio::sync::Barrier;
use xtask::sandbox::prelude::*;

// =============================================================================
// Race Condition Tests
// =============================================================================

/// Test that concurrent publishes at the exact same microsecond produce unique event IDs.
#[sinex_test]
async fn test_worker_claim_exact_same_microsecond(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let worker_count = 10;

    let handles: Vec<_> = (0..worker_count)
        .map(|worker_id| async move {
            let payload = DynamicPayload::new(
                "race-condition",
                "concurrent.claim",
                serde_json::json!({
                    "worker_id": worker_id,
                    "sequence": 0
                }),
            );
            ctx.publish(payload).await
        })
        .collect();

    let results = join_all(handles).await;

    // All should succeed, collect unique IDs
    let mut event_ids = Vec::new();
    for result in results {
        let event = result?;
        if let Some(id) = event.id {
            event_ids.push(id);
        }
    }

    // Verify all event IDs are unique
    let unique_count = event_ids
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(
        unique_count, worker_count,
        "all events must have unique IDs, got {unique_count} unique from {worker_count} publishes"
    );

    Ok(())
}

/// Test that event ULIDs are strictly monotonically increasing (no causality violations).
#[sinex_test]
async fn test_event_causality_violation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let event_count = 100;

    // Publish 100 events sequentially
    let mut ulids = Vec::new();
    for i in 0..event_count {
        let payload = DynamicPayload::new(
            "causality-test",
            "event.sequential",
            serde_json::json!({
                "sequence": i,
                "timestamp": Timestamp::now().to_string()
            }),
        );
        let event = ctx.publish(payload).await?;
        let id = event.id.expect("published event should have ID");
        ulids.push(id.as_ulid().clone());
    }

    // Verify ULIDs are strictly increasing
    for i in 1..ulids.len() {
        assert!(
            ulids[i] > ulids[i - 1],
            "ULID causality violation: ULID[{i}]={:?} not > ULID[{prev}]={:?}",
            ulids[i],
            ulids[i - 1],
            prev = i - 1
        );
    }

    Ok(())
}

/// Test concurrent checkpoint updates maintain consistency.
#[sinex_test(timeout = 60)]
async fn test_concurrent_checkpoint_updates(ctx: TestContext) -> TestResult<()> {
    let ctx_with_nats = ctx.with_nats().shared().await?;
    let kv = ctx_with_nats.checkpoint_kv().await?;

    let processor = format!("test_processor_{}", Ulid::new().to_string().to_lowercase());
    let worker_count = 5;
    let checkpoints_per_worker = 10;

    let mut handles = Vec::new();

    for worker_id in 0..worker_count {
        let kv = kv.clone();
        let processor = processor.clone();
        let worker_str = format!("worker-{worker_id}");

        handles.push(tokio::spawn(async move {
            let manager =
                CheckpointManager::new(kv, processor, "test_group".to_string(), worker_str);

            for checkpoint_num in 1..=checkpoints_per_worker {
                let mut state = CheckpointState::default();
                state.checkpoint = Checkpoint::internal(Ulid::new(), checkpoint_num as u64);
                state.processed_count = checkpoint_num as u64;
                state.last_activity = Timestamp::now();

                manager.save_checkpoint(&state).await?;
            }

            TestResult::Ok(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    // Verify final state for each worker
    for worker_id in 0..worker_count {
        let manager = CheckpointManager::new(
            kv.clone(),
            processor.clone(),
            "test_group".to_string(),
            format!("worker-{worker_id}"),
        );

        let state = manager.load_checkpoint().await?;
        assert_eq!(
            state.processed_count, checkpoints_per_worker as u64,
            "worker {worker_id} should have processed all checkpoints"
        );
    }

    Ok(())
}

// =============================================================================
// Worker Coordination Tests
// =============================================================================

/// Test synchronization barrier: all workers wait, then publish simultaneously.
#[sinex_test]
async fn test_worker_synchronization_barrier(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let barrier_count = 8;
    let barrier = Arc::new(Barrier::new(barrier_count));

    let handles: Vec<_> = (0..barrier_count)
        .map(|worker_id| {
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;

                let payload = DynamicPayload::new(
                    "barrier-test",
                    "worker.synchronized",
                    serde_json::json!({ "worker_id": worker_id }),
                );
                ctx.publish(payload).await
            }
        })
        .collect();

    let results = join_all(handles).await;

    for result in results {
        let event = result?;
        assert!(event.id.is_some(), "barrier worker should publish with ID");
    }

    Ok(())
}

/// Test deadlock prevention: concurrent operations complete without hanging.
#[sinex_test(timeout = 30)]
async fn test_deadlock_prevention(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let task_count = 4;
    let events_per_task = 25;

    let handles: Vec<_> = (0..task_count)
        .map(|task_id| async move {
            for i in 0..events_per_task {
                let payload = DynamicPayload::new(
                    format!("deadlock-test-{task_id}"),
                    "concurrent.operations",
                    serde_json::json!({
                        "task_id": task_id,
                        "sequence": i,
                    }),
                );
                ctx.publish(payload).await?;
            }
            TestResult::Ok(())
        })
        .collect();

    let results = join_all(handles).await;
    for result in results {
        result?;
    }

    Ok(())
}

/// Test resource sharing fairness: concurrent tasks get equal access.
#[sinex_test]
async fn test_resource_sharing_fairness(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let task_count = 5usize;
    let events_per_task = 20usize;

    let handles: Vec<_> = (0..task_count)
        .map(|task_id| async move {
            for i in 0..events_per_task {
                let payload = DynamicPayload::new(
                    format!("fairness-source-{task_id}"),
                    "resource.shared",
                    serde_json::json!({
                        "task_id": task_id,
                        "sequence": i,
                    }),
                );
                ctx.publish(payload).await?;
            }
            TestResult::Ok(())
        })
        .collect();

    let results = join_all(handles).await;
    for result in results {
        result?;
    }

    // Verify each source has exactly events_per_task events
    let pool = ctx.pool();
    for task_id in 0..task_count {
        let source = EventSource::from(format!("fairness-source-{task_id}"));
        let count = pool.events().count_by_source(&source).await?;
        assert_eq!(
            count, events_per_task as i64,
            "source fairness-source-{task_id} should have exactly {events_per_task} events, got {count}"
        );
    }

    Ok(())
}

// =============================================================================
// Database Concurrency Tests
// =============================================================================

/// Test transaction isolation: concurrent sources don't cross-contaminate.
#[sinex_test]
async fn test_transaction_isolation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let source_count = 3usize;
    let events_per_source = 10usize;

    let handles: Vec<_> = (0..source_count)
        .map(|source_id| async move {
            for i in 0..events_per_source {
                let payload = DynamicPayload::new(
                    format!("isolation-source-{source_id}"),
                    "transaction.isolated",
                    serde_json::json!({
                        "source_id": source_id,
                        "event_num": i,
                    }),
                );
                ctx.publish(payload).await?;
            }
            TestResult::Ok(())
        })
        .collect();

    let results = join_all(handles).await;
    for result in results {
        result?;
    }

    let pool = ctx.pool();
    for source_id in 0..source_count {
        let source = EventSource::from(format!("isolation-source-{source_id}"));
        let count = pool.events().count_by_source(&source).await?;
        assert_eq!(
            count, events_per_source as i64,
            "source isolation-source-{source_id} should have exactly {events_per_source} events, got {count}"
        );
    }

    Ok(())
}

/// Test lock contention handling: many concurrent operations succeed gracefully.
#[sinex_test]
async fn test_lock_contention_handling(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let concurrent_count = 20usize;
    let source = "contention-test";

    let handles: Vec<_> = (0..concurrent_count)
        .map(|i| async move {
            let payload = DynamicPayload::new(
                source,
                "lock.contention",
                serde_json::json!({ "sequence": i }),
            );
            ctx.publish(payload).await
        })
        .collect();

    let results = join_all(handles).await;

    let mut success_count = 0;
    for result in results {
        let event = result?;
        assert!(event.id.is_some(), "published event should have an ID");
        success_count += 1;
    }

    assert_eq!(
        success_count, concurrent_count,
        "all {concurrent_count} publishes should succeed under lock contention"
    );

    Ok(())
}

/// Test deadlock detection: batches of events complete successfully.
#[sinex_test]
async fn test_database_deadlock_detection(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let batch_count = 10usize;
    let events_per_batch = 10usize;

    let handles: Vec<_> = (0..batch_count)
        .map(|batch_id| async move {
            let payloads: Vec<DynamicPayload> = (0..events_per_batch)
                .map(|i| {
                    DynamicPayload::new(
                        format!("deadlock-detection-{batch_id}"),
                        "batch.event",
                        serde_json::json!({
                            "batch_id": batch_id,
                            "sequence": i,
                        }),
                    )
                })
                .collect();

            ctx.publish_many(payloads).await
        })
        .collect();

    let results = join_all(handles).await;

    let mut total_events = 0;
    for result in results {
        let events = result?;
        total_events += events.len();
    }

    assert_eq!(
        total_events,
        batch_count * events_per_batch,
        "all {} events should persist successfully",
        batch_count * events_per_batch
    );

    Ok(())
}

// =============================================================================
// Memory Concurrency Tests
// =============================================================================

/// Test shared state consistency: memory-backed state remains consistent.
#[sinex_test]
async fn test_shared_state_consistency(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let event_count = 50usize;
    let source = "shared-state-test";

    let payloads: Vec<DynamicPayload> = (0..event_count)
        .map(|i| DynamicPayload::new(source, "state.shared", serde_json::json!({ "sequence": i })))
        .collect();

    let published = ctx.publish_many(payloads).await?;
    assert_eq!(
        published.len(),
        event_count,
        "should publish all {event_count} events"
    );

    // Read them all back
    let pool = ctx.pool();
    let source_obj = EventSource::from(source);
    let count = pool.events().count_by_source(&source_obj).await?;
    assert_eq!(
        count, event_count as i64,
        "should have exactly {event_count} events, got {count}"
    );

    Ok(())
}

/// Test atomic operations: concurrent publishes produce correct total counts.
#[sinex_test]
async fn test_atomic_operations_correctness(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let task_count = 4usize;
    let events_per_task = 25usize;

    let handles: Vec<_> = (0..task_count)
        .map(|task_id| async move {
            for i in 0..events_per_task {
                let payload = DynamicPayload::new(
                    format!("atomic-task-{task_id}"),
                    "operations.atomic",
                    serde_json::json!({
                        "task_id": task_id,
                        "sequence": i,
                    }),
                );
                ctx.publish(payload).await?;
            }
            TestResult::Ok(())
        })
        .collect();

    let results = join_all(handles).await;
    for result in results {
        result?;
    }

    let pool = ctx.pool();
    let mut total_events: i64 = 0;
    for task_id in 0..task_count {
        let source = EventSource::from(format!("atomic-task-{task_id}"));
        let count = pool.events().count_by_source(&source).await?;
        assert_eq!(
            count, events_per_task as i64,
            "task {task_id} source should have exactly {events_per_task} events"
        );
        total_events += count;
    }

    assert_eq!(
        total_events,
        (task_count * events_per_task) as i64,
        "total events should be {}",
        task_count * events_per_task
    );

    Ok(())
}
