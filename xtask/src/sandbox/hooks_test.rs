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
