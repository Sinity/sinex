//! Unit tests for CoordinationPrimitive unified abstraction
//!
//! Tests all factory methods and behaviors:
//! - event_counter, barrier, synchronizer
//! - Thresholds, reset behaviors, atomic operations
//! - Backwards compatibility with EventCounter/ProgressTracker patterns

use sinex_test_utils::prelude::*;
use sinex_types::utils::CoordinationPrimitive;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

#[sinex_test]
async fn test_event_counter_factory_method(ctx: TestContext) -> color_eyre::Result<()> {
    let counter = CoordinationPrimitive::event_counter(100, "test_events");

    // Initial state
    assert_eq!(counter.get(), 0);
    assert!(!counter.is_ready());
    assert_eq!(counter.name(), "test_events");
    assert_eq!(counter.threshold(), 100);

    // Increment operations
    counter.add(50);
    assert_eq!(counter.get(), 50);
    assert!(!counter.is_ready());

    counter.add(30);
    assert_eq!(counter.get(), 80);
    assert!(!counter.is_ready());

    // Reach threshold
    counter.add(20);
    assert_eq!(counter.get(), 100);
    assert!(counter.is_ready());

    // Event counter never resets automatically
    counter.add(10);
    assert_eq!(counter.get(), 110);
    assert!(counter.is_ready());

    Ok(())
}

#[sinex_test]
async fn test_barrier_factory_method(ctx: TestContext) -> color_eyre::Result<()> {
    let barrier = CoordinationPrimitive::barrier(3, "worker_sync");

    // Initial state
    assert_eq!(barrier.get(), 0);
    assert!(!barrier.is_ready());
    assert_eq!(barrier.threshold(), 3);

    let initial_generation = barrier.generation();

    // Simulate 3 participants arriving at barrier using proper wait() API
    let handles = (0..3)
        .map(|i| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(i * 10)).await;
                barrier.wait(Duration::from_secs(1)).await
            })
        })
        .collect::<Vec<_>>();

    // All should complete
    for handle in handles {
        assert!(handle.await.unwrap().is_ok());
    }

    // Generation should have incremented (barrier reset)
    assert!(barrier.generation() > initial_generation);
    assert_eq!(barrier.get(), 0); // Should be reset

    Ok(())
}

#[sinex_test]
async fn test_synchronizer_factory_method(ctx: TestContext) -> color_eyre::Result<()> {
    let sync = CoordinationPrimitive::synchronizer("service_ready");

    // Initial state
    assert_eq!(sync.get(), 0);
    assert!(!sync.is_ready());
    assert_eq!(sync.threshold(), 1);

    // Signal readiness
    sync.signal();
    assert_eq!(sync.get(), 1);
    assert!(sync.is_ready());

    // Synchronizer stays signaled
    sync.signal(); // Additional signals ignored
    assert_eq!(sync.get(), 1);
    assert!(sync.is_ready());

    Ok(())
}

#[sinex_test]
async fn test_backwards_compatibility_type_aliases(ctx: TestContext) -> color_eyre::Result<()> {
    // EventCounter type alias should work exactly like factory method
    let counter1 = CoordinationPrimitive::event_counter(50, "events");
    let counter2 = CoordinationPrimitive::event_counter(50, "events");

    counter1.add(25);
    counter2.add(25);

    assert_eq!(counter1.get(), counter2.get());
    assert_eq!(counter1.is_ready(), counter2.is_ready());

    // ProgressTracker type alias should work
    let tracker = CoordinationPrimitive::barrier(5, "steps");
    tracker.add(3);
    assert_eq!(tracker.get(), 3);
    assert!(!tracker.is_ready());

    Ok(())
}

#[sinex_test]
async fn test_reset_behaviors(ctx: TestContext) -> color_eyre::Result<()> {
    // Event counter - never resets
    let counter = CoordinationPrimitive::event_counter(10, "never_reset");
    counter.add(15);
    let old_value = counter.get();
    counter.reset();
    assert_eq!(old_value, 15);
    assert_eq!(counter.get(), 0); // Manual reset only

    // Barrier - auto resets
    let barrier = CoordinationPrimitive::barrier(2, "auto_reset");
    let initial_generation = barrier.generation();

    // Use proper barrier wait API
    let handles = (0..2)
        .map(|_| {
            let barrier = barrier.clone();
            tokio::spawn(async move { barrier.wait(Duration::from_secs(1)).await })
        })
        .collect::<Vec<_>>();

    // All should complete
    for handle in handles {
        assert!(handle.await.unwrap().is_ok());
    }

    // Barrier should have auto-reset
    assert!(barrier.generation() > initial_generation);
    assert_eq!(barrier.get(), 0);

    // Synchronizer - stays signaled
    let sync = CoordinationPrimitive::synchronizer("stays_signaled");
    sync.signal();
    assert!(sync.is_ready());

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(sync.is_ready()); // Still complete

    Ok(())
}

#[sinex_test]
async fn test_concurrent_operations(ctx: TestContext) -> color_eyre::Result<()> {
    let counter = Arc::new(CoordinationPrimitive::event_counter(
        1000,
        "concurrent_test",
    ));
    let mut handles = vec![];

    // Spawn 10 tasks adding 100 each = 1000 total
    for _ in 0..10 {
        let counter_clone = counter.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..100 {
                counter_clone.add(1);
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(counter.get(), 1000);
    assert!(counter.is_ready());

    Ok(())
}

#[sinex_test]
async fn test_barrier_concurrent_workers(ctx: TestContext) -> color_eyre::Result<()> {
    let barrier = Arc::new(CoordinationPrimitive::barrier(5, "concurrent_barrier"));
    let mut handles = vec![];

    let initial_generation = barrier.generation();

    // Spawn 5 workers that all reach the barrier using proper wait() API
    for worker_id in 0..5 {
        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            // Simulate work
            tokio::time::sleep(Duration::from_millis(worker_id * 10)).await;

            // Wait at barrier
            barrier_clone.wait(Duration::from_secs(1)).await
        });
        handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in handles {
        assert!(handle.await.unwrap().is_ok());
    }

    // Generation should have incremented (barrier passed)
    assert!(barrier.generation() > initial_generation);

    // Barrier should have reset
    assert_eq!(barrier.get(), 0);

    Ok(())
}

#[sinex_test]
async fn test_wait_for_completion(ctx: TestContext) -> color_eyre::Result<()> {
    let counter = Arc::new(CoordinationPrimitive::event_counter(10, "wait_test"));
    let counter_clone = counter.clone();

    // Start task that will complete the counter after delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        counter_clone.add(10);
    });

    // Wait for completion with timeout
    let result = timeout(Duration::from_millis(200), async {
        while !counter.is_ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    assert!(result.is_ok());
    assert!(counter.is_ready());

    Ok(())
}

#[sinex_test]
async fn test_coordination_primitive_metadata(ctx: TestContext) -> color_eyre::Result<()> {
    let counter = CoordinationPrimitive::event_counter(100, "metadata_test");

    // Test name and threshold access
    assert_eq!(counter.name(), "metadata_test");
    assert_eq!(counter.threshold(), 100);

    // Test descriptive string - note: current implementation doesn't have description() method
    // so we'll just test the name and threshold directly
    assert!(counter.name().contains("metadata_test"));

    Ok(())
}

#[sinex_test]
async fn test_edge_cases(ctx: TestContext) -> color_eyre::Result<()> {
    // Zero threshold
    let zero_barrier = CoordinationPrimitive::barrier(0, "zero_test");
    assert!(zero_barrier.is_ready()); // Should be immediately complete

    // Large threshold
    let large_counter = CoordinationPrimitive::event_counter(usize::MAX, "large_test");
    large_counter.add(1000);
    assert!(!large_counter.is_ready());

    // Empty name
    let unnamed = CoordinationPrimitive::synchronizer("");
    assert_eq!(unnamed.name(), "");
    unnamed.signal();
    assert!(unnamed.is_ready());

    Ok(())
}

#[sinex_test]
async fn test_multiple_coordination_patterns(ctx: TestContext) -> color_eyre::Result<()> {
    // Simulate complex coordination scenario
    let startup_barrier = Arc::new(CoordinationPrimitive::barrier(3, "startup"));
    let event_counter = Arc::new(CoordinationPrimitive::event_counter(100, "events"));
    let shutdown_signal = Arc::new(CoordinationPrimitive::synchronizer("shutdown"));

    // Three workers coordinate startup, process events, then shutdown
    let mut handles = vec![];

    for worker_id in 0..3 {
        let barrier = startup_barrier.clone();
        let counter = event_counter.clone();
        let shutdown = shutdown_signal.clone();

        let handle = tokio::spawn(async move {
            // Wait for all workers to start using proper barrier wait API
            let _ = barrier.wait(Duration::from_secs(5)).await;

            // Process events
            for _ in 0..33 {
                counter.add(1);
                tokio::time::sleep(Duration::from_millis(1)).await;

                if shutdown.is_ready() {
                    break;
                }
            }

            worker_id
        });
        handles.push(handle);
    }

    // Let workers run briefly
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Signal shutdown
    shutdown_signal.signal();

    // Wait for all workers
    let worker_ids: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(worker_ids, vec![0, 1, 2]);
    // Startup barrier should have reset after all workers passed through
    assert_eq!(startup_barrier.get(), 0);
    assert!(shutdown_signal.is_ready());
    // Event counter should have some events (workers might not reach 100 due to shutdown)
    assert!(event_counter.get() > 0);

    Ok(())
}
