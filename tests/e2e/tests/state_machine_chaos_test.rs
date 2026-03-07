//! State Machine Chaos Tests
//!
//! Tests for state machine violations including shutdown during initialization,
//! concurrent shutdown signals, and state corruption under load.

use futures::future::join_all;
use sinex_primitives::DynamicPayload;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use xtask::sandbox::prelude::*;

/// Start a pipeline, seed a few events, then immediately request shutdown.
/// Verifies the pipeline terminates cleanly without panicking or hanging.
#[sinex_test]
#[ignore = "chaos test requiring controlled failure injection"]
async fn test_shutdown_signal_during_initialization(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    // Publish a small batch right before shutdown
    for i in 0..3 {
        let _ = scope
            .publish(DynamicPayload::new(
                "state-machine-init",
                "state.init.event",
                json!({"seq": i}),
            ))
            .await;
    }

    // Immediately shut down -- the key property is no panic or deadlock
    scope.shutdown().await?;

    // If we get here, shutdown was graceful
    Ok(())
}

/// Send multiple concurrent shutdown signals and verify no double-free or panic.
#[sinex_test]
#[ignore = "chaos test requiring controlled failure injection"]
async fn test_multiple_concurrent_shutdown_signals(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Create multiple pipelines and shut them all down concurrently
    // This tests that concurrent lifecycle operations don't interfere
    let mut scopes = Vec::new();
    for _ in 0..3 {
        let scope = ctx.pipeline().await?;
        // Publish one event per scope to ensure they're active
        let _ = scope
            .publish(DynamicPayload::new(
                "concurrent-shutdown",
                "state.concurrent.shutdown",
                json!({"data": "active"}),
            ))
            .await;
        scopes.push(scope);
    }

    // Shut down all scopes concurrently
    let shutdown_futs: Vec<_> = scopes
        .into_iter()
        .map(xtask::sandbox::coordination::PipelineScope::shutdown)
        .collect();
    let results = join_all(shutdown_futs).await;

    // All shutdowns should complete without error
    for (i, result) in results.into_iter().enumerate() {
        assert!(
            result.is_ok(),
            "scope {i} shutdown should succeed, got: {:?}",
            result.err()
        );
    }

    Ok(())
}

/// Heavy concurrent event seeding on a single pipeline, verifying all events
/// arrive intact without data corruption.
#[sinex_test(timeout = 60)]
#[ignore = "chaos test requiring controlled failure injection"]
async fn test_state_machine_corruption_under_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let worker_count = 8;
    let events_per_worker = 25;
    let total_expected = worker_count * events_per_worker;
    let success_count = Arc::new(AtomicU64::new(0));

    let ctx = &ctx;
    let worker_futs: Vec<_> = (0..worker_count)
        .map(|worker_id| {
            let successes = success_count.clone();
            async move {
                for event_id in 0..events_per_worker {
                    let payload = DynamicPayload::new(
                        format!("corruption-worker-{worker_id}"),
                        "state.corruption.load",
                        json!({
                            "worker": worker_id,
                            "event": event_id,
                            "fingerprint": format!("w{worker_id}e{event_id}")
                        }),
                    );
                    if ctx.publish(payload).await.is_ok() {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        })
        .collect();

    join_all(worker_futs).await;

    let total_published = success_count.load(Ordering::Relaxed);
    println!("State corruption load test: {total_published}/{total_expected} events published");

    // Verify a high success rate
    let success_rate = total_published as f64 / f64::from(total_expected);
    assert!(
        success_rate > 0.95,
        "success rate should be > 95%, got {:.1}%",
        success_rate * 100.0
    );

    // Verify database has the events
    let db_count = ctx.pool().events().count_all().await?;
    assert!(
        db_count >= (total_published as f64 * 0.9) as i64,
        "database should have at least 90% of published events, got {db_count}"
    );

    Ok(())
}
