//! # Database Degradation Tests
//!
//! Tests that verify:
//! - Graceful degradation under database connectivity issues
//! - Connection pool exhaustion handling
//! - System recovery after database failures
//!
//! ## Performance Expectations
//!
//! - **Individual tests**: 30-60 seconds
//! - **Resource usage**: High database load
//! - **Dependencies**: PostgreSQL

use futures::future::join_all;
use sinex_primitives::DynamicPayload;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use xtask::sandbox::prelude::*;

/// Simulate database slowdown by running heavy queries concurrently with event
/// publishing. Verify the system degrades gracefully (events still get published)
/// rather than crashing.
#[sinex_test(timeout = 60)]
#[ignore = "requires database degradation simulation"]
async fn test_graceful_degradation_database_failure(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let pool = ctx.pool().clone();

    let publish_success = Arc::new(AtomicU64::new(0));
    let publish_errors = Arc::new(AtomicU64::new(0));

    let ctx = &ctx;

    // Launch heavy queries to simulate degradation
    let heavy_futs: Vec<_> = (0..5)
        .map(|i| {
            let pool = pool.clone();
            async move {
                let _ = sqlx::query!("SELECT pg_sleep(0.5), $1::int as idx", i as i32)
                    .fetch_one(&pool)
                    .await;
            }
        })
        .collect();

    // Simultaneously publish events
    let successes = publish_success.clone();
    let errors = publish_errors.clone();
    let publish_futs: Vec<_> = (0..20)
        .map(|i| {
            let successes = successes.clone();
            let errors = errors.clone();
            async move {
                let payload = DynamicPayload::new(
                    "degradation-test",
                    "db.degradation.event",
                    json!({"seq": i, "test": "graceful_degradation"}),
                );
                match ctx.publish(payload).await {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        })
        .collect();

    // Run both concurrently
    let (_, _) = tokio::join!(join_all(heavy_futs), join_all(publish_futs));

    let ok = publish_success.load(Ordering::Relaxed);
    let err = publish_errors.load(Ordering::Relaxed);
    println!("Graceful degradation: {ok}/20 succeeded, {err} errors");

    // Under degradation, at least some events should still get through
    assert!(
        ok > 0,
        "system should degrade gracefully, not fail entirely"
    );

    Ok(())
}

/// Exhaust the connection pool by holding many connections simultaneously, then
/// release them and verify the pool recovers.
#[sinex_test(timeout = 60)]
#[ignore = "requires database degradation simulation"]
async fn test_connection_pool_recovery(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let pool = ctx.pool().clone();

    // Phase 1: Exhaust pool connections with blocking queries
    let blocking_futs: Vec<_> = (0..15)
        .map(|i| {
            let pool = pool.clone();
            async move {
                let _ = sqlx::query!("SELECT pg_sleep(0.2), $1::int as idx", i as i32)
                    .fetch_one(&pool)
                    .await;
            }
        })
        .collect();

    join_all(blocking_futs).await;

    // Phase 2: Pool should now be recovered -- verify normal operations work
    let recovery_start = Instant::now();
    let mut ok = 0u32;
    for i in 0..10 {
        let payload = DynamicPayload::new(
            "pool-exhaustion-test",
            "db.pool.recovery",
            json!({"seq": i, "phase": "recovery"}),
        );
        if ctx.publish(payload).await.is_ok() {
            ok += 1;
        }
    }
    let recovery_elapsed = recovery_start.elapsed();
    println!("Pool recovery: {ok}/10 in {recovery_elapsed:?}");

    assert!(
        ok >= 8,
        "pool should recover and handle at least 8/10 requests"
    );

    Ok(())
}

/// After heavy database load, verify the system can return to normal operation
/// and all subsequently published events persist correctly.
#[sinex_test(timeout = 60)]
#[ignore = "requires database degradation simulation"]
async fn test_system_recovery_after_failure(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let pool = ctx.pool().clone();

    // Phase 1: Heavy load
    let heavy_futs: Vec<_> = (0..10)
        .map(|i| {
            let pool = pool.clone();
            async move {
                // Mix of slow queries and fast queries
                if i % 2 == 0 {
                    let _ = sqlx::query!("SELECT pg_sleep(0.3), $1::int as idx", i as i32)
                        .fetch_one(&pool)
                        .await;
                } else {
                    let _ = sqlx::query!("SELECT 1 as val").fetch_one(&pool).await;
                }
            }
        })
        .collect();
    join_all(heavy_futs).await;

    // Phase 2: Recovery -- all events should persist cleanly
    let recovery_events = 15usize;
    let mut ids = Vec::new();
    for i in 0..recovery_events {
        let payload = DynamicPayload::new(
            "system-recovery-test",
            "db.system.recovery",
            json!({"seq": i, "phase": "post_failure"}),
        );
        let event = ctx.publish(payload).await?;
        ids.push(event.id.expect("event should have an id"));
    }

    // Verify all recovery events are in the database
    let source = sinex_primitives::EventSource::from("system-recovery-test");
    let count = ctx.pool().events().count_by_source(&source).await?;
    assert_eq!(
        count, recovery_events as i64,
        "all post-failure events should be persisted"
    );

    // Verify IDs are unique
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "all event IDs should be unique");

    Ok(())
}
