//! Service Recovery Tests
//!
//! These tests verify system resilience and recovery behavior that mirrors
//! what the NixOS VM tests validate, but at the integration test level.
//! This provides faster feedback for recovery-related regressions.
//!
//! ## Coverage Areas
//! - Database pool recovery after connection saturation
//! - Concurrent stress and recovery
//! - Pipeline event continuity

use futures::future::join_all;
use sinex_primitives::DynamicPayload;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use xtask::sandbox::prelude::*;

/// Saturate the DB pool with concurrent queries, then verify normal operations
/// resume once the burst subsides.
#[sinex_test(timeout = 60)]
#[ignore = "requires service failure simulation"]
async fn test_pool_recovery_after_connection_invalidation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let pool = ctx.pool().clone();

    // Phase 1: Saturate the pool with concurrent long queries
    let saturation_futs: Vec<_> = (0..20)
        .map(|i| {
            let pool = pool.clone();
            async move {
                let _ = sqlx::query!("SELECT pg_sleep(0.1), $1::int as idx", i as i32)
                    .fetch_one(&pool)
                    .await;
            }
        })
        .collect();

    join_all(saturation_futs).await;

    // Phase 2: Verify pool has recovered by performing normal operations
    let recovery_start = Instant::now();
    let mut recovery_successes = 0u32;

    for i in 0..10 {
        let payload = DynamicPayload::new(
            "pool-recovery-test",
            "recovery.after.saturation",
            json!({"seq": i, "phase": "recovery"}),
        );
        match ctx.publish(payload).await {
            Ok(_) => recovery_successes += 1,
            Err(e) => println!("Recovery publish {i} failed: {e}"),
        }
    }

    let recovery_duration = recovery_start.elapsed();
    println!(
        "Pool recovery: {recovery_successes}/10 events in {:?}",
        recovery_duration
    );

    assert!(
        recovery_successes >= 8,
        "should recover and publish at least 8/10 events after saturation"
    );

    Ok(())
}

/// Heavy concurrent operations interleaved with pool queries to verify
/// the system remains responsive under mixed pressure.
#[sinex_test(timeout = 60)]
#[ignore = "requires service failure simulation"]
async fn test_pool_concurrent_stress_recovery(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let worker_count = 10usize;
    let ops_per_worker = 20usize;
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let ctx = &ctx;
    let worker_futs: Vec<_> = (0..worker_count)
        .map(|worker_id| {
            let successes = success_count.clone();
            let errors = error_count.clone();
            async move {
                for op_id in 0..ops_per_worker {
                    // Alternate between event publishing and raw queries
                    if op_id % 3 == 0 {
                        // Raw query to add pool pressure
                        let pool = ctx.pool().clone();
                        let _ = sqlx::query!(
                            "SELECT COUNT(*) as count FROM core.events WHERE source = $1",
                            format!("stress-worker-{worker_id}")
                        )
                        .fetch_one(&pool)
                        .await;
                    }

                    let payload = DynamicPayload::new(
                        format!("stress-worker-{worker_id}"),
                        "recovery.concurrent.stress",
                        json!({"worker": worker_id, "op": op_id}),
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
            }
        })
        .collect();

    join_all(worker_futs).await;

    let total_ok = success_count.load(Ordering::Relaxed);
    let total_err = error_count.load(Ordering::Relaxed);
    let total = total_ok + total_err;
    println!("Concurrent stress: {total_ok}/{total} succeeded, {total_err} errors");

    let success_rate = total_ok as f64 / total as f64;
    assert!(
        success_rate > 0.90,
        "should maintain > 90% success rate under concurrent stress, got {:.1}%",
        success_rate * 100.0
    );

    Ok(())
}

/// Verify events flow through the pipeline without gaps when a pipeline scope
/// is created, used, and cleanly shut down.
#[sinex_test(timeout = 60)]
#[ignore = "requires service failure simulation"]
async fn test_ingestd_restart_event_continuity(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    // Phase 1: First pipeline scope -- publish and persist
    let scope1 = ctx.pipeline().await?;
    let phase1_count = 5usize;
    for i in 0..phase1_count {
        scope1
            .publish(DynamicPayload::new(
                "continuity-test",
                "continuity.phase1",
                json!({"seq": i, "phase": 1}),
            ))
            .await?;
    }
    scope1.wait_for_event_count(phase1_count).await?;
    scope1.shutdown().await?;

    // Phase 2: Second pipeline scope -- should work independently
    let scope2 = ctx.pipeline().await?;
    let phase2_count = 5usize;
    for i in 0..phase2_count {
        scope2
            .publish(DynamicPayload::new(
                "continuity-test",
                "continuity.phase2",
                json!({"seq": i, "phase": 2}),
            ))
            .await?;
    }
    scope2.wait_for_event_count(phase2_count).await?;

    // Verify total events from both phases
    let source = sinex_primitives::EventSource::from("continuity-test");
    let total = ctx.pool.events().count_by_source(&source).await?;
    assert_eq!(
        total,
        (phase1_count + phase2_count) as i64,
        "all events from both phases should be persisted"
    );

    scope2.shutdown().await?;
    Ok(())
}
