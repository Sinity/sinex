use crate::common::prelude::*;
use sinex_worker::calculate_backoff_secs;

#[sinex_test]
async fn test_calculate_backoff_basic(ctx: TestContext) -> TestResult {
    // Test that backoff increases exponentially
    let backoff_0 = calculate_backoff_secs(0);
    let backoff_1 = calculate_backoff_secs(1);
    let backoff_2 = calculate_backoff_secs(2);

    // Should be roughly 60s, 120s, 240s (with jitter)
    assert!((48.0..=72.0).contains(&backoff_0)); // 60 * 0.8 to 60 * 1.2
    assert!((96.0..=144.0).contains(&backoff_1)); // 120 * 0.8 to 120 * 1.2
    assert!((192.0..=288.0).contains(&backoff_2)); // 240 * 0.8 to 240 * 1.2
    Ok(())
}

#[sinex_test]
async fn test_calculate_backoff_min_max(ctx: TestContext) -> TestResult {
    // Test minimum bound
    let backoff_negative = calculate_backoff_secs(-10);
    assert!(backoff_negative >= 1.0);

    // Test maximum bound (should cap at 24 hours)
    let backoff_large = calculate_backoff_secs(20);
    assert!(backoff_large <= 24.0 * 3600.0);
    Ok(())
}

#[sinex_test]
async fn test_calculate_backoff_jitter(ctx: TestContext) -> TestResult {
    // Test that jitter produces different values
    let mut values = HashSet::new();
    for _ in 0..10 {
        values.insert((calculate_backoff_secs(1) * 1000.0) as i64);
    }
    // With jitter, we should get at least 2 different values
    assert!(values.len() >= 2);
    Ok(())
}
