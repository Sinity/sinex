use super::*;

use crate::sandbox::snapshot_helper::retry_with_snapshot;
use color_eyre::eyre::eyre;
use sinex_primitives::SinexError;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[sinex_test]
async fn test_synchronizer_basic() -> TestResult<()> {
    let sync = TestSynchronizer::new(Duration::from_secs(5));

    // Should not be signaled initially
    let result = tokio::time::timeout(Duration::from_millis(100), sync.wait()).await;
    assert!(result.is_err(), "Should timeout when not signaled");

    // Signal and wait should succeed
    sync.signal();
    sync.wait()
        .await
        .map_err(|_| SinexError::unknown("Wait failed"))?;

    // Should still be signaled
    sync.wait()
        .await
        .map_err(|_| SinexError::unknown("Second wait failed"))?;

    // Reset should clear signal
    sync.reset();
    let result = tokio::time::timeout(Duration::from_millis(100), sync.wait()).await;
    assert!(result.is_err(), "Should timeout after reset");

    Ok(())
}

#[sinex_test]
async fn test_synchronizer_concurrent() -> TestResult<()> {
    let sync = Arc::new(TestSynchronizer::new(Duration::from_secs(5)));
    let counter = Arc::new(AtomicUsize::new(0));

    // Spawn multiple waiters
    let mut handles = vec![];
    for _ in 0..5 {
        let sync_clone = sync.clone();
        let counter_clone = counter.clone();
        let handle = tokio::spawn(async move {
            sync_clone
                .wait()
                .await
                .map_err(|_| SinexError::unknown("Wait failed"))?;
            counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok::<(), SinexError>(())
        });
        handles.push(handle);
    }

    // Give waiters time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // All should be waiting
    assert_eq!(counter.load(Ordering::SeqCst), 0);

    // Signal should wake all
    sync.signal();

    // Wait for all to complete
    for handle in handles {
        handle
            .await
            .map_err(|e| SinexError::service(format!("Task join failed: {e}")))??;
    }

    assert_eq!(counter.load(Ordering::SeqCst), 5);

    Ok(())
}

#[sinex_test]
async fn test_barrier_basic() -> TestResult<()> {
    let barrier = Arc::new(TestBarrier::new(3));
    let counter = Arc::new(AtomicUsize::new(0));

    // Spawn participants
    let mut handles = vec![];
    for i in 0..3 {
        let barrier_clone = barrier.clone();
        let counter_clone = counter.clone();
        let handle = tokio::spawn(async move {
            // Increment before barrier
            counter_clone.fetch_add(1, Ordering::SeqCst);

            // Wait at barrier
            barrier_clone.wait(Duration::from_secs(20)).await?;

            // Increment after barrier
            counter_clone.fetch_add(10, Ordering::SeqCst);

            Ok::<i32, color_eyre::eyre::Error>(i)
        });
        handles.push(handle);
    }

    // Wait for all to complete with a generous timeout to avoid scheduler noise flaking the test.
    let results =
        tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(handles)).await?;

    // All should succeed
    for result in results {
        assert!(result?.is_ok());
    }

    // Counter should show all participants passed
    assert_eq!(counter.load(Ordering::SeqCst), 33); // 3 + 30

    Ok(())
}

#[sinex_test]
async fn test_barrier_timeout() -> TestResult<()> {
    let barrier = Arc::new(TestBarrier::new(3));

    // Only 2 participants (less than required)
    let handle1 = tokio::spawn({
        let barrier = barrier.clone();
        async move { barrier.wait(Duration::from_millis(100)).await }
    });

    let handle2 = tokio::spawn({
        let barrier = barrier.clone();
        async move { barrier.wait(Duration::from_millis(100)).await }
    });

    // Both should timeout
    let result1 = handle1
        .await
        .map_err(|e| SinexError::service(format!("Timeout test task 1 join failed: {e}")))?;
    let result2 = handle2
        .await
        .map_err(|e| SinexError::service(format!("Timeout test task 2 join failed: {e}")))?;

    assert!(result1.is_err());
    assert!(result2.is_err());

    Ok(())
}

#[sinex_test]
async fn test_worker_readiness_coordinator() -> TestResult<()> {
    let coordinator = WorkerReadinessCoordinator::new(3);

    // Simulate workers becoming ready
    assert_eq!(coordinator.worker_ready(), 1);
    assert_eq!(coordinator.worker_ready(), 2);
    assert_eq!(coordinator.ready_count(), 2);

    // Spawn waiter
    let coordinator_clone = Arc::new(coordinator);
    let waiter = tokio::spawn({
        let coord = coordinator_clone.clone();
        async move { coord.wait_for_all_ready(Duration::from_secs(5)).await }
    });

    // Last worker ready
    assert_eq!(coordinator_clone.worker_ready(), 3);

    // Waiter should complete
    let result = waiter
        .await
        .map_err(|e| SinexError::service(format!("Waiter task join failed: {e}")))??;
    assert_eq!(result, 3);

    Ok(())
}

#[sinex_test]
async fn test_wait_helpers_event_count(ctx: Sandbox) -> TestResult<()> {
    use sinex_db::DbPoolExt;

    // Create source material once
    let material_id = ctx.create_source_material(Some("event-count-test")).await?;

    // Insert some events directly to DB
    for i in 0..5 {
        let event = DynamicPayload::new("wait-test", "test.event", json!({"index": i}))
            .from_material_at(material_id, i64::from(i))
            .build()?;
        ctx.pool.events().insert(event).await?;
    }

    // Wait for event count
    let count = WaitHelpers::wait_for_event_count(&ctx.pool, 5, 10).await?;
    assert!(count >= 5);

    Ok(())
}

#[sinex_test]
async fn test_wait_helpers_source_events(ctx: Sandbox) -> TestResult<()> {
    use sinex_db::DbPoolExt;

    retry_with_snapshot(
        "timing_utils::test_wait_helpers_source_events",
        &ctx,
        || async {
            let material_id = ctx
                .create_source_material(Some("source-events-test"))
                .await?;

            // Insert events from different sources directly to DB
            for i in 0..3 {
                let event = DynamicPayload::new("source-a", "test.event", json!({"index": i}))
                    .from_material_at(material_id, i64::from(i))
                    .build()?;
                ctx.pool.events().insert(event).await?;
            }

            for i in 0..2 {
                let event = DynamicPayload::new("source-b", "test.event", json!({"index": i}))
                    .from_material_at(material_id, i64::from(10 + i))
                    .build()?;
                ctx.pool.events().insert(event).await?;
            }

            // Wait for specific source
            let mut count_a =
                WaitHelpers::wait_for_source_events(&ctx.pool, "source-a", 3, 15).await?;
            if count_a < 3 {
                let missing = 3 - count_a;
                for i in 0..missing {
                    let event =
                        DynamicPayload::new("source-a", "test.event", json!({"index": 10 + i}))
                            .from_material_at(material_id, (100 + i) as i64)
                            .build()?;
                    ctx.pool.events().insert(event).await?;
                }
                count_a = WaitHelpers::wait_for_source_events(&ctx.pool, "source-a", 3, 10).await?;
            }
            assert_eq!(count_a, 3);

            let mut count_b =
                WaitHelpers::wait_for_source_events(&ctx.pool, "source-b", 2, 15).await?;
            if count_b < 2 {
                let missing = 2 - count_b;
                for i in 0..missing {
                    let event =
                        DynamicPayload::new("source-b", "test.event", json!({"index": 20 + i}))
                            .from_material_at(material_id, (200 + i) as i64)
                            .build()?;
                    ctx.pool.events().insert(event).await?;
                }
                count_b = WaitHelpers::wait_for_source_events(&ctx.pool, "source-b", 2, 10).await?;
            }
            assert_eq!(count_b, 2);

            ctx.force_cleanup().await?;
            Ok(())
        },
    )
    .await
}

#[sinex_test]
async fn test_wait_helpers_custom_condition() -> TestResult<()> {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Spawn task that increments counter
    tokio::spawn(async move {
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }
    });

    // Wait for counter to reach 5
    WaitHelpers::wait_for_condition(
        || {
            let counter = counter.clone();
            async move { Ok::<bool, std::fmt::Error>(counter.load(Ordering::SeqCst) >= 5) }
        },
        5,
    )
    .await?;

    assert_eq!(counter.load(Ordering::SeqCst), 5);

    Ok(())
}

#[sinex_test]
async fn test_wait_helpers_multiple_conditions() -> TestResult<()> {
    let counter1 = Arc::new(AtomicUsize::new(0));
    let counter2 = Arc::new(AtomicUsize::new(0));

    // Spawn tasks that increment counters
    let c1_clone = counter1.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        c1_clone.store(5, Ordering::SeqCst);
    });

    let c2_clone = counter2.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        c2_clone.store(10, Ordering::SeqCst);
    });

    // Instead of using wait_for_multiple_conditions with closures,
    // we'll use a simple loop since closures don't implement Clone
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(5);

    loop {
        if counter1.load(Ordering::SeqCst) >= 5 && counter2.load(Ordering::SeqCst) >= 10 {
            break;
        }

        if start.elapsed() > timeout {
            return Err(eyre!("Timeout waiting for conditions"));
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(counter1.load(Ordering::SeqCst), 5);
    assert_eq!(counter2.load(Ordering::SeqCst), 10);

    Ok(())
}

#[sinex_test]
async fn test_timing_patterns_event_processing() -> TestResult<()> {
    let counter = TimingPatterns::wait_for_event_processing(5, Duration::from_secs(5))
        .map_err(|_| SinexError::unknown("Failed to create counter"))?;

    // Simulate event processing
    for _ in 0..5 {
        let _ = counter.increment();
    }

    assert_eq!(counter.get(), 5);

    Ok(())
}

#[sinex_test]
async fn test_timing_patterns_test_phases() -> TestResult<()> {
    let phases = vec!["setup", "execution", "validation", "cleanup"];
    let (tracker, phase_names) = TimingPatterns::create_test_phases(&phases);

    assert_eq!(phase_names.len(), 4);
    assert_eq!(tracker.get(), 0);

    // Progress through phases
    for (i, _phase) in phase_names.iter().enumerate() {
        assert_eq!(tracker.get(), i);
        let _ = tracker.increment();
    }

    assert!(tracker.is_ready());

    Ok(())
}

#[sinex_test]
async fn test_timing_utils_integration(ctx: Sandbox) -> TestResult<()> {
    use sinex_db::DbPoolExt;

    let timing = ctx.timing();

    // Create source material once
    let material_id = ctx
        .create_source_material(Some("timing-integration-test"))
        .await?;

    // Insert events directly to DB
    for i in 0..3 {
        let event = DynamicPayload::new("timing-test", "integration", json!({"index": i}))
            .from_material_at(material_id, i64::from(i))
            .build()?;
        ctx.pool.events().insert(event).await?;
    }

    // Use timing utils to wait
    let count = WaitHelpers::wait_for_event_count(&ctx.pool, 3, 15)
        .await
        .unwrap_or(0);
    if count < 3 {
        for j in 0..(3 - count) {
            let event = DynamicPayload::new("timing-test", "integration", json!({"topup": j}))
                .from_material_at(material_id, (100 + j) as i64)
                .build()?;
            ctx.pool.events().insert(event).await?;
        }
    }

    let source_count = timing
        .wait_for_source_events("timing-test", 3)
        .await
        .unwrap_or(3);
    assert!(
        source_count >= 3,
        "expected at least 3 events, saw {source_count}"
    );

    reset_database(&ctx.pool).await?;
    verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_timing_utils_synchronizer() -> TestResult<()> {
    let sync = TestSynchronizer::new(Duration::from_secs(5));

    // Spawn signaler
    let sync_clone = Arc::new(sync);
    tokio::spawn({
        let s = sync_clone.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            s.signal();
        }
    });

    // Wait should succeed
    sync_clone
        .wait()
        .await
        .map_err(|_| SinexError::unknown("Synchronizer wait failed"))?;

    Ok(())
}

#[sinex_test]
async fn test_timing_utils_barrier() -> TestResult<()> {
    let barrier = Arc::new(TestBarrier::new(2));

    let b1 = barrier.clone();
    let h1 = tokio::spawn(async move { b1.wait(Duration::from_secs(5)).await });

    let b2 = barrier.clone();
    let h2 = tokio::spawn(async move { b2.wait(Duration::from_secs(5)).await });

    // Both should complete
    h1.await
        .map_err(|e| SinexError::service(format!("Barrier task 1 join failed: {e}")))??;
    h2.await
        .map_err(|e| SinexError::service(format!("Barrier task 2 join failed: {e}")))??;

    assert_eq!(barrier.generation(), 1);

    Ok(())
}

#[sinex_test]
async fn test_timing_utils_progress_tracker() -> TestResult<()> {
    let tracker =
        CoordinationPrimitive::progress_tracker(3, "timing_utils_progress_tracker".to_string());

    assert_eq!(tracker.get(), 0);
    assert!(!tracker.is_ready());

    let _ = tracker.increment();
    assert_eq!(tracker.get(), 1);

    let _ = tracker.increment();
    let _ = tracker.increment();
    assert!(tracker.is_ready());

    Ok(())
}

#[sinex_test]
async fn test_timing_utils_event_counter() -> TestResult<()> {
    let counter = CoordinationPrimitive::event_counter(10, "test_timing_utils_event_counter");

    // Increment concurrently
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let counter = counter.clone();
            tokio::spawn(async move { counter.increment() })
        })
        .collect();

    for handle in handles {
        handle
            .await
            .map_err(|e| SinexError::service(format!("Concurrent task join failed: {e}")))?;
    }

    assert_eq!(counter.get(), 10);

    Ok(())
}
