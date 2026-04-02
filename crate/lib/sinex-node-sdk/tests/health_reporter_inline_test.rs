#![cfg(feature = "messaging")]

use sinex_node_sdk::{HealthMetrics, HealthThresholds};
use std::sync::atomic::Ordering;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_health_metrics_error_rate() -> TestResult<()> {
    let metrics = HealthMetrics::default();

    assert_eq!(metrics.error_rate(300), 0.0);

    metrics.events_processed.store(100, Ordering::Relaxed);
    metrics.errors.store(5, Ordering::Relaxed);
    metrics
        .last_error_monotonic
        .store(u64::MAX, Ordering::Relaxed);

    assert!((metrics.error_rate(300) - 0.05).abs() < 0.001);

    metrics.errors.store(20, Ordering::Relaxed);
    assert!((metrics.error_rate(300) - 0.20).abs() < 0.001);
    Ok(())
}

#[sinex_test]
async fn test_health_thresholds_defaults() -> TestResult<()> {
    let thresholds = HealthThresholds::default();
    assert_eq!(thresholds.error_rate_degraded, 0.05);
    assert_eq!(thresholds.error_rate_failed, 0.20);
    assert_eq!(thresholds.window_seconds, 300);
    Ok(())
}

#[sinex_test]
async fn test_process_status_calculation() -> TestResult<()> {
    let thresholds = HealthThresholds::default();
    let metrics = HealthMetrics::default();

    metrics.events_processed.store(100, Ordering::Relaxed);
    metrics.errors.store(0, Ordering::Relaxed);
    assert_eq!(metrics.error_rate(300), 0.0);

    metrics.errors.store(5, Ordering::Relaxed);
    metrics
        .last_error_monotonic
        .store(u64::MAX, Ordering::Relaxed);
    let rate = metrics.error_rate(300);
    assert!(rate >= thresholds.error_rate_degraded);
    assert!(rate < thresholds.error_rate_failed);

    metrics.errors.store(20, Ordering::Relaxed);
    let rate = metrics.error_rate(300);
    assert!(rate >= thresholds.error_rate_failed);
    Ok(())
}

#[sinex_test]
async fn test_health_thresholds_from_env_accepts_valid_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_HEALTH_ERROR_RATE_DEGRADED", "0.10");
    env.set("SINEX_HEALTH_ERROR_RATE_FAILED", "0.40");
    env.set("SINEX_HEALTH_WINDOW_SECONDS", "120");

    let thresholds = HealthThresholds::from_env()?;
    assert_eq!(thresholds.error_rate_degraded, 0.10);
    assert_eq!(thresholds.error_rate_failed, 0.40);
    assert_eq!(thresholds.window_seconds, 120);
    Ok(())
}

#[sinex_test]
async fn test_health_thresholds_from_env_rejects_invalid_override() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_HEALTH_ERROR_RATE_DEGRADED", "bogus");

    let error =
        HealthThresholds::from_env().expect_err("invalid threshold override must surface");

    assert!(error.to_string().contains("SINEX_HEALTH_ERROR_RATE_DEGRADED"));
    assert!(error.to_string().contains("bogus"));
    Ok(())
}
