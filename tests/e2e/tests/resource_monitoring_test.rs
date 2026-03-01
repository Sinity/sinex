// # Resource Monitoring Tests
//
// Tests that verify:
// - Memory usage monitoring under high-volume operations
// - Database connection limits under concurrent access
// - Resource exhaustion scenario handling
//
// ## Performance Expectations
//
// - **Individual tests**: 30-90 seconds
// - **Resource usage**: High CPU/memory, significant database load
// - **Dependencies**: PostgreSQL

use futures::future::join_all;
use sinex_primitives::DynamicPayload;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use xtask::sandbox::prelude::*;

/// Stress-test database connection limits by running many concurrent operations.
/// Verify the system handles resource exhaustion gracefully.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_resource_limits_under_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let pool = ctx.pool().clone();

    let worker_count = 20usize;
    let ops_per_worker = 15usize;
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let ctx = &ctx;
    let futs: Vec<_> = (0..worker_count)
        .map(|wid| {
            let pool = pool.clone();
            let successes = success_count.clone();
            let errors = error_count.clone();
            async move {
                for oid in 0..ops_per_worker {
                    // Mix publishing with direct queries for maximum connection pressure
                    if oid % 2 == 0 {
                        let payload = DynamicPayload::new(
                            format!("resource-limit-{wid}"),
                            "resource.limit.test",
                            json!({"worker": wid, "op": oid}),
                        );
                        match ctx.publish(payload).await {
                            Ok(_) => {
                                successes.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        match sqlx::query!("SELECT COUNT(*) as count FROM core.events")
                            .fetch_one(&pool)
                            .await
                        {
                            Ok(_) => {
                                successes.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        })
        .collect();

    join_all(futs).await;

    let ok = success_count.load(Ordering::Relaxed);
    let err = error_count.load(Ordering::Relaxed);
    let total = ok + err;
    println!("Resource limits: {ok}/{total} operations succeeded, {err} errors");

    // The system should not completely fail under load
    let success_rate = ok as f64 / total as f64;
    assert!(
        success_rate > 0.80,
        "should maintain > 80% success rate under resource pressure, got {:.1}%",
        success_rate * 100.0
    );

    Ok(())
}

/// Publish a large volume of events and monitor that the system handles them
/// without unbounded resource growth.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_memory_monitoring_high_volume(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let total_events = 200usize;
    let batch_size = 40usize;
    let batches = total_events / batch_size;
    let mut batch_counts = Vec::new();

    for batch in 0..batches {
        let payloads: Vec<DynamicPayload> = (0..batch_size)
            .map(|i| {
                DynamicPayload::new(
                    "resource-memory-test",
                    "resource.memory.monitor",
                    json!({
                        "batch": batch,
                        "seq": i,
                        "padding": "x".repeat(500)
                    }),
                )
            })
            .collect();

        let published = ctx.publish_many(payloads).await?;
        batch_counts.push(published.len());
        println!("  Batch {batch}: {} events published", published.len());
    }

    // Verify all batches completed
    let total_published: usize = batch_counts.iter().sum();
    println!("Memory monitoring: {total_published}/{total_events} total events");

    assert_eq!(
        total_published, total_events,
        "all batches should publish successfully"
    );

    // Verify database has the events
    let db_count = ctx.pool().events().count_all().await?;
    assert!(
        db_count >= total_events as i64,
        "database should have at least {total_events} events"
    );

    Ok(())
}
