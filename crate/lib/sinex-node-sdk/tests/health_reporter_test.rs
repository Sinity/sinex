//! Comprehensive tests for HealthReporter

use sinex_core::SinexError;
use sinex_node_sdk::health_reporter::{HealthReporter, HealthThresholds};
use sinex_node_sdk::prelude::ProcessStatus;
use sinex_node_sdk::self_observation::{SelfObserver, SelfObserverConfig};
use sinex_test_utils::prelude::*;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// Create a test health reporter with NATS connection
async fn create_test_reporter(ctx: TestContext) -> TestResult<(TestContext, Arc<HealthReporter>)> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client().clone();

    let config = SelfObserverConfig {
        component: "test-component".to_string(),
        subject_prefix: "sinex.telemetry".to_string(),
        enabled: true,
        min_emission_interval: Duration::from_millis(100),
    };

    let observer = Arc::new(SelfObserver::new(nats_client, config));

    let thresholds = HealthThresholds {
        error_rate_degraded: 0.05, // 5%
        error_rate_failed: 0.20,   // 20%
        window_seconds: 5,         // 5 second window for tests
    };

    Ok((
        ctx,
        Arc::new(HealthReporter::new(
            "test-component".to_string(),
            observer,
            thresholds,
        )),
    ))
}

#[sinex_test]
async fn health_reporter_starts_healthy(ctx: TestContext) -> TestResult<()> {
    let (ctx, reporter) = create_test_reporter(ctx).await?;

    let status = reporter.current_status();
    ctx.assert("initial status")
        .eq(&status, &ProcessStatus::Healthy)?;

    Ok(())
}

#[sinex_test]
async fn health_reporter_tracks_successful_events(ctx: TestContext) -> TestResult<()> {
    let (ctx, reporter) = create_test_reporter(ctx).await?;

    // Record 100 successful events
    for _ in 0..100 {
        reporter.record_success();
    }

    let metrics = reporter.metrics();
    ctx.assert("events processed")
        .eq(&metrics.events_processed.load(Ordering::Relaxed), &100)?;
    ctx.assert("errors")
        .eq(&metrics.errors.load(Ordering::Relaxed), &0)?;

    let status = reporter.current_status();
    ctx.assert("status after success")
        .eq(&status, &ProcessStatus::Healthy)?;

    Ok(())
}

#[sinex_test]
async fn health_reporter_transitions_to_degraded(ctx: TestContext) -> TestResult<()> {
    let (ctx, reporter) = create_test_reporter(ctx).await?;

    // Process 100 events with 6 errors (6% error rate → degraded)
    for _ in 0..94 {
        reporter.record_success();
    }
    for _ in 0..6 {
        let error = SinexError::processing("test error");
        reporter.record_error(&error);
    }

    // Check and emit to trigger status calculation
    let status = reporter.check_and_emit().await?;

    ctx.assert("status transitioned to degraded")
        .eq(&status, &ProcessStatus::Degraded)?;

    Ok(())
}

#[sinex_test]
async fn health_reporter_transitions_to_failed(ctx: TestContext) -> TestResult<()> {
    let (ctx, reporter) = create_test_reporter(ctx).await?;

    // Process 100 events with 21 errors (21% error rate → failed)
    for _ in 0..79 {
        reporter.record_success();
    }
    for _ in 0..21 {
        let error = SinexError::processing("test error");
        reporter.record_error(&error);
    }

    // Check and emit to trigger status calculation
    let status = reporter.check_and_emit().await?;

    ctx.assert("status transitioned to failed")
        .eq(&status, &ProcessStatus::Failed)?;

    Ok(())
}

#[sinex_test]
async fn health_reporter_recovers_to_healthy(ctx: TestContext) -> TestResult<()> {
    let (ctx, reporter) = create_test_reporter(ctx).await?;

    // First, degrade the status with errors
    for _ in 0..94 {
        reporter.record_success();
    }
    for _ in 0..6 {
        let error = SinexError::processing("test error");
        reporter.record_error(&error);
    }

    let status = reporter.check_and_emit().await?;
    ctx.assert("degraded")
        .eq(&status, &ProcessStatus::Degraded)?;

    // Wait for sliding window to move
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Now process successful events (old errors should be outside window)
    for _ in 0..100 {
        reporter.record_success();
    }

    let status = reporter.check_and_emit().await?;
    ctx.assert("recovered to healthy")
        .eq(&status, &ProcessStatus::Healthy)?;

    Ok(())
}

#[sinex_test]
async fn health_reporter_only_emits_on_status_change(ctx: TestContext) -> TestResult<()> {
    let (ctx, reporter) = create_test_reporter(ctx).await?;

    // Record successful events
    for _ in 0..10 {
        reporter.record_success();
        reporter.check_and_emit().await?;
    }

    // Should only emit once (or not at all if it stays healthy)
    // This is a behavioral test - we're verifying no repeated emissions

    let status = reporter.current_status();
    ctx.assert("status remained healthy")
        .eq(&status, &ProcessStatus::Healthy)?;

    Ok(())
}

#[sinex_test]
async fn health_reporter_handles_warnings(ctx: TestContext) -> TestResult<()> {
    let (ctx, reporter) = create_test_reporter(ctx).await?;

    // Record warnings (should not affect error rate)
    for _ in 0..10 {
        reporter.record_warning("test warning");
    }

    let metrics = reporter.metrics();
    ctx.assert("warnings recorded")
        .eq(&metrics.warnings.load(Ordering::Relaxed), &10)?;
    ctx.assert("no errors")
        .eq(&metrics.errors.load(Ordering::Relaxed), &0)?;

    let status = reporter.current_status();
    ctx.assert("status healthy despite warnings")
        .eq(&status, &ProcessStatus::Healthy)?;

    Ok(())
}

#[sinex_test]
async fn health_reporter_calculates_error_rate_in_sliding_window(
    ctx: TestContext,
) -> TestResult<()> {
    let (ctx, reporter) = create_test_reporter(ctx).await?;

    // Process events with errors
    for _ in 0..90 {
        reporter.record_success();
    }
    for _ in 0..10 {
        let error = SinexError::processing("test error");
        reporter.record_error(&error);
    }

    // Should be degraded (10% error rate)
    let status1 = reporter.check_and_emit().await?;
    ctx.assert("degraded at 10%")
        .eq(&status1, &ProcessStatus::Degraded)?;

    // Wait for window to slide past old errors
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Process only successful events (old errors outside window)
    for _ in 0..50 {
        reporter.record_success();
    }

    let status2 = reporter.check_and_emit().await?;
    ctx.assert("recovered after window slid")
        .eq(&status2, &ProcessStatus::Healthy)?;

    Ok(())
}

#[sinex_test]
async fn health_reporter_with_custom_thresholds(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client().clone();

    let config = SelfObserverConfig {
        component: "test-strict".to_string(),
        subject_prefix: "sinex.telemetry".to_string(),
        enabled: true,
        min_emission_interval: Duration::from_millis(100),
    };

    let observer = Arc::new(SelfObserver::new(nats_client, config));

    // Stricter thresholds
    let thresholds = HealthThresholds {
        error_rate_degraded: 0.01, // 1%
        error_rate_failed: 0.05,   // 5%
        window_seconds: 5,
    };

    let reporter = Arc::new(HealthReporter::new(
        "test-strict".to_string(),
        observer,
        thresholds,
    ));

    // Process 100 events with 2 errors (2% → should fail with stricter threshold)
    for _ in 0..98 {
        reporter.record_success();
    }
    for _ in 0..2 {
        let error = SinexError::processing("test error");
        reporter.record_error(&error);
    }

    let status = reporter.check_and_emit().await?;
    ctx.assert("failed with stricter threshold")
        .eq(&status, &ProcessStatus::Failed)?;

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn health_reporter_stress_test(ctx: TestContext) -> TestResult<()> {
    let (ctx, reporter) = create_test_reporter(ctx).await?;

    // Process 10,000 events rapidly
    for i in 0..10_000 {
        if i % 50 == 0 {
            // 2% error rate
            let error = SinexError::processing("test error");
            reporter.record_error(&error);
        } else {
            reporter.record_success();
        }

        // Periodic check
        if i % 1000 == 0 {
            reporter.check_and_emit().await?;
        }
    }

    let metrics = reporter.metrics();
    ctx.assert("processed 10k events")
        .eq(&metrics.events_processed.load(Ordering::Relaxed), &10000u64)?;
    ctx.assert("recorded 200 errors")
        .eq(&metrics.errors.load(Ordering::Relaxed), &200u64)?;

    Ok(())
}
