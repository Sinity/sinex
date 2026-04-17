#![cfg(feature = "messaging")]

use sinex_node_sdk::SinexError;
use sinex_node_sdk::health_reporter::{HealthClock, HealthReporter, HealthThresholds};
use sinex_node_sdk::self_observation::{SelfObserver, SelfObserverConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use xtask::sandbox::prelude::*;

#[derive(Debug, Default)]
struct ManualHealthClock {
    elapsed_ms: AtomicU64,
}

impl ManualHealthClock {
    fn advance(&self, duration: Duration) {
        let millis = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self.elapsed_ms.fetch_add(millis, Ordering::Relaxed);
    }
}

impl HealthClock for ManualHealthClock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.elapsed_ms.load(Ordering::Relaxed))
    }
}

#[sinex_test]
async fn test_health_metrics_error_rate(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let clock = Arc::new(ManualHealthClock::default());
    let observer = Arc::new(SelfObserver::new(
        ctx.nats_client(),
        SelfObserverConfig {
            component: "health-inline".to_string(),
            subject_prefix: "events.raw".to_string(),
            enabled: true,
            min_emission_interval: Duration::ZERO,
        },
    ));
    let reporter = HealthReporter::new_with_clock(
        "health-inline".to_string(),
        observer,
        HealthThresholds {
            error_rate_degraded: 0.4,
            error_rate_failed: 0.75,
            window_seconds: 1,
        },
        Arc::clone(&clock) as Arc<dyn HealthClock>,
    );

    for _ in 0..100 {
        reporter.record_success();
    }
    clock.advance(Duration::from_secs(2));

    reporter.record_success();
    reporter.record_error(&SinexError::processing("recent failure"));

    let rate = reporter.metrics().error_rate(1);
    assert!(
        (rate - 0.5).abs() < 0.001,
        "error rate should use the active window, not dilute recent failures with expired history"
    );
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
async fn test_process_status_calculation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let observer = Arc::new(SelfObserver::new(
        ctx.nats_client(),
        SelfObserverConfig {
            component: "health-thresholds".to_string(),
            subject_prefix: "events.raw".to_string(),
            enabled: true,
            min_emission_interval: Duration::ZERO,
        },
    ));
    let thresholds = HealthThresholds {
        error_rate_degraded: 0.05,
        error_rate_failed: 0.20,
        window_seconds: 5,
    };
    let reporter = HealthReporter::new(
        "health-thresholds".to_string(),
        observer,
        thresholds.clone(),
    );

    for _ in 0..95 {
        reporter.record_success();
    }
    for _ in 0..5 {
        reporter.record_error(&SinexError::processing("recent failure"));
    }
    let rate = reporter.metrics().error_rate(thresholds.window_seconds);
    assert!(rate >= thresholds.error_rate_degraded);
    assert!(rate < thresholds.error_rate_failed);

    let failed_reporter = HealthReporter::new(
        "health-thresholds-failed".to_string(),
        Arc::new(SelfObserver::new(
            ctx.nats_client(),
            SelfObserverConfig {
                component: "health-thresholds-failed".to_string(),
                subject_prefix: "events.raw".to_string(),
                enabled: true,
                min_emission_interval: Duration::ZERO,
            },
        )),
        thresholds.clone(),
    );
    for _ in 0..80 {
        failed_reporter.record_success();
    }
    for _ in 0..20 {
        failed_reporter.record_error(&SinexError::processing("failed threshold"));
    }
    let rate = failed_reporter
        .metrics()
        .error_rate(thresholds.window_seconds);
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

    let error = HealthThresholds::from_env().expect_err("invalid threshold override must surface");

    assert!(
        error
            .to_string()
            .contains("SINEX_HEALTH_ERROR_RATE_DEGRADED")
    );
    assert!(error.to_string().contains("bogus"));
    Ok(())
}

#[sinex_test]
async fn test_health_thresholds_from_env_rejects_out_of_range_rates() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_HEALTH_ERROR_RATE_DEGRADED", "1.10");

    let error =
        HealthThresholds::from_env().expect_err("out-of-range threshold override must surface");

    assert!(
        error
            .to_string()
            .contains("health degraded threshold must be between 0.0 and 1.0"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn test_health_thresholds_from_env_rejects_inverted_thresholds() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_HEALTH_ERROR_RATE_DEGRADED", "0.60");
    env.set("SINEX_HEALTH_ERROR_RATE_FAILED", "0.40");

    let error = HealthThresholds::from_env()
        .expect_err("degraded threshold above failed threshold must surface");

    assert!(
        error
            .to_string()
            .contains("health degraded threshold must not exceed the failed threshold"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn test_health_thresholds_from_env_rejects_zero_window() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_HEALTH_WINDOW_SECONDS", "0");

    let error = HealthThresholds::from_env().expect_err("zero window must surface");

    assert!(
        error
            .to_string()
            .contains("health window must be greater than zero"),
        "unexpected error: {error}"
    );
    Ok(())
}
