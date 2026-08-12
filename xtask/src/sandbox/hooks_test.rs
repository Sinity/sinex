use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_hooks_builder_default() -> ::xtask::sandbox::TestResult<()> {
    let (hooks, counters) = TestHooks::builder().build();

    assert!(hooks.fail_once.is_none());
    assert!(hooks.delivery_counter.is_none());
    assert!(hooks.processing_delay.is_none());
    assert!(hooks.confirmation_failures.is_none());
    assert!(!hooks.route_db_errors_to_dlq);
    assert!(!hooks.validate);

    assert_eq!(counters.delivery_count(), 0);
    assert!(!counters.has_failed_once());
    Ok(())
}

#[sinex_test]
async fn test_hooks_builder_full_config() -> ::xtask::sandbox::TestResult<()> {
    let (hooks, counters) = TestHooks::builder()
        .validate()
        .fail_once()
        .count_deliveries()
        .with_delay(Duration::from_millis(100))
        .route_db_errors_to_dlq()
        .fail_confirmations(3)
        .build();

    assert!(hooks.fail_once.is_some());
    assert!(hooks.delivery_counter.is_some());
    assert_eq!(hooks.processing_delay, Some(Duration::from_millis(100)));
    assert!(hooks.confirmation_failures.is_some());
    assert!(hooks.route_db_errors_to_dlq);
    assert_eq!(hooks.source_material_ready_dlq_threshold, None);
    assert_eq!(hooks.source_material_ready_retry_delay, None);
    assert!(hooks.validate);

    // Counters should be linked to hooks
    assert!(counters.fail_once.is_some());
    assert!(counters.deliveries.is_some());
    assert_eq!(counters.remaining_confirmation_failures(), 3);
    Ok(())
}

#[sinex_test]
async fn test_counters_track_state() -> ::xtask::sandbox::TestResult<()> {
    let (hooks, counters) = TestHooks::builder().fail_once().count_deliveries().build();

    // Initially fail_once is true (hasn't failed yet)
    assert!(!counters.has_failed_once());

    // Simulate first failure
    hooks
        .fail_once
        .as_ref()
        .unwrap()
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(counters.has_failed_once());

    // Simulate deliveries
    hooks
        .delivery_counter
        .as_ref()
        .unwrap()
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(counters.delivery_count(), 1);
    Ok(())
}

#[sinex_test]
async fn test_hooks_builder_source_material_retry_budget() -> ::xtask::sandbox::TestResult<()> {
    let (hooks, _) = TestHooks::builder()
        .source_material_ready_retry_budget(2, Duration::from_millis(50))
        .build();

    assert_eq!(hooks.source_material_ready_dlq_threshold, Some(2));
    assert_eq!(
        hooks.source_material_ready_retry_delay,
        Some(Duration::from_millis(50))
    );
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-5xta open: fail_on_delivery(n) ignores n and behaves identically to fail_once (fails on delivery 1, not the Nth)"]
async fn fail_on_delivery_arms_only_on_the_requested_delivery_number()
-> ::xtask::sandbox::TestResult<()> {
    // `TestHooks` has no field capable of storing a target delivery count at
    // all (fail_once/delivery_counter/persistence_failures_remaining/
    // confirmation_failures -- none of these carry an "n"), so
    // `fail_on_delivery(n)` cannot possibly behave differently for
    // different `n`. Prove it structurally: two builders differing only in
    // `n` must be indistinguishable via any hook field a consumer could act
    // on, which is the exact bug (both fail on delivery 1, not delivery n).
    let (hooks_n1, counters_n1) = TestHooks::builder().fail_on_delivery(1).build();
    let (hooks_n100, counters_n100) = TestHooks::builder().fail_on_delivery(100).build();

    assert!(
        hooks_n1.fail_once.is_some() && hooks_n100.fail_once.is_some(),
        "both should arm the immediate fail_once flag today (the bug)"
    );
    // The real fix must give fail_on_delivery(n) a way to distinguish n=1
    // from n=100 -- e.g. a target-count field checked against
    // delivery_counter before firing. Until then, both configurations fire
    // on the very first delivery, so a consumer relying on
    // `fail_on_delivery(100)` to survive 99 deliveries first cannot pass.
    assert!(
        !counters_n1.has_failed_once() && !counters_n100.has_failed_once(),
        "fail_once starts armed-but-not-yet-triggered"
    );
    // Simulate a single delivery attempt firing the n=100 flag, exactly as
    // the real consumer path does for plain fail_once() -- there is no
    // delivery-count check anywhere in this struct to gate it on n.
    hooks_n100
        .fail_once
        .as_ref()
        .unwrap()
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(
        !counters_n100.has_failed_once(),
        "fail_on_delivery(100) must NOT have fired after only 1 delivery -- \
         it should still be armed-but-not-yet-triggered, proving the Nth-delivery \
         semantics are real rather than an alias for fail_once()"
    );
    let _ = hooks_n1;
    Ok(())
}
