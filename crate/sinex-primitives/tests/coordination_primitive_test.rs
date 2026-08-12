use std::sync::Arc;
use std::time::Duration;

use sinex_primitives::utils::coordination::{CoordinationPrimitive, ResetBehavior};
use tokio::time::sleep;
use xtask::sandbox::sinex_test;
use xtask::sandbox::timing::Timeouts;

#[sinex_test]
async fn coordination_supports_event_counter_pattern() -> TestResult<()> {
    let counter = CoordinationPrimitive::event_counter(3, "test_counter");

    assert_eq!(counter.get(), 0);
    assert!(!counter.is_ready());

    assert_eq!(counter.increment(), 1);
    assert_eq!(counter.increment(), 2);
    assert!(!counter.is_ready());

    assert_eq!(counter.increment(), 3);
    assert!(counter.is_ready());

    let reached = counter
        .wait_for_threshold(Duration::from_millis(10))
        .await?;
    assert_eq!(reached, 3);
    Ok(())
}

#[sinex_test]
async fn coordination_supports_synchronizer_pattern() -> TestResult<()> {
    let sync = CoordinationPrimitive::synchronizer("test_sync");
    assert_eq!(sync.reset_behavior(), ResetBehavior::Manual);
    assert!(!sync.is_ready());

    let _ = sync.signal();
    assert!(sync.is_ready());
    sync.wait(Duration::from_millis(10)).await?;

    sync.reset();
    assert!(!sync.is_ready());
    Ok(())
}

#[sinex_test]
async fn coordination_barrier_increments_generation() -> TestResult<()> {
    let barrier = CoordinationPrimitive::barrier(3, "test_barrier");
    let baseline_generation = barrier.generation();

    let handles = (0..3)
        .map(|i| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(i * 10)).await;
                barrier.wait(Duration::from_secs(Timeouts::QUICK)).await
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.await.unwrap()?;
    }

    assert!(barrier.generation() > baseline_generation);
    assert_eq!(barrier.get(), 0);
    Ok(())
}

#[sinex_test]
async fn coordination_wait_times_out() -> TestResult<()> {
    let counter = CoordinationPrimitive::event_counter(5, "timeout_test");
    let err = counter
        .wait_for_threshold(Duration::from_millis(100))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("timeout"));
    Ok(())
}

#[sinex_test]
async fn coordination_event_counter_factory_tracks_progress() -> TestResult<()> {
    let counter = CoordinationPrimitive::event_counter(100, "test_events");
    assert_eq!(counter.name(), "test_events");
    assert_eq!(counter.threshold(), 100);

    let _ = counter.add(50);
    assert_eq!(counter.get(), 50);
    assert!(!counter.is_ready());

    let _ = counter.add(30);
    assert_eq!(counter.get(), 80);

    let _ = counter.add(20);
    assert!(counter.is_ready());
    assert_eq!(counter.get(), 100);

    let _ = counter.add(10);
    assert_eq!(counter.get(), 110);
    Ok(())
}

#[sinex_test]
async fn coordination_handles_concurrent_adders() -> TestResult<()> {
    let counter = Arc::new(CoordinationPrimitive::event_counter(1000, "concurrent"));
    let mut handles = Vec::new();
    for _ in 0..10 {
        let counter_clone = counter.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                let _ = counter_clone.add(1);
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(counter.get(), 1000);
    assert!(counter.is_ready());
    Ok(())
}

#[sinex_test]
async fn coordination_covers_edge_cases() -> TestResult<()> {
    let zero_barrier = CoordinationPrimitive::barrier(0, "zero_test");
    assert!(zero_barrier.is_ready());

    let large_counter = CoordinationPrimitive::event_counter(usize::MAX, "large_test");
    let _ = large_counter.add(1000);
    assert!(!large_counter.is_ready());

    let unnamed = CoordinationPrimitive::synchronizer("");
    assert_eq!(unnamed.name(), "");
    let _ = unnamed.signal();
    assert!(unnamed.is_ready());
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-y8vc open: CoordinationPrimitive::wait()'s Automatic-reset (barrier) path uses \
            tokio::time::timeout(timeout_duration / 10, ...) inside its wait loop and propagates the \
            first sub-timeout via `?`, so a barrier that never fills times out after ~1/10th of the \
            caller's requested budget instead of the full budget"]
async fn barrier_wait_honors_the_full_requested_timeout_budget() -> TestResult<()> {
    // Threshold 2, only ever incremented once below -- the barrier never
    // fills, so wait() must run out the *full* requested timeout before
    // erroring, not bail out after ~1/10th of it.
    let barrier = Arc::new(CoordinationPrimitive::barrier(2, "budget_test"));
    let requested_timeout = Duration::from_millis(600);

    let start = tokio::time::Instant::now();
    let result = barrier.wait(requested_timeout).await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "barrier should time out (never fills)");
    assert!(
        elapsed >= requested_timeout,
        "wait() returned after {elapsed:?} but was asked to wait the full {requested_timeout:?} -- \
         it bailed out early on an internal timeout_duration/10 sub-wait instead of honoring the \
         full requested budget"
    );
    Ok(())
}
