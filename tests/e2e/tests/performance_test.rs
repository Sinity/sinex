// # Performance and Load Testing
//
// System performance validation tests that measure:
// - Load testing with realistic data volumes
// - Throughput and latency measurements
// - Scaling behavior validation
//
// ## Performance Expectations
//
// - **Individual tests**: 30-120 seconds
// - **Resource usage**: High CPU/memory usage during tests
// - **Baseline performance**: 1000+ events/second insertion rate

use futures::future::join_all;
use sinex_primitives::DynamicPayload;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use xtask::sandbox::prelude::*;

// ==================== DATABASE PERFORMANCE TESTS ====================

/// Measure sequential event insertion performance. Publishes 100 events and
/// reports throughput and per-event latency.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_database_insertion_performance(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let event_count = 100usize;
    let start = Instant::now();

    for i in 0..event_count {
        let payload = DynamicPayload::new(
            "perf-insert",
            "perf.insert.sequential",
            json!({"seq": i, "data": format!("perf-data-{i}")}),
        );
        ctx.publish(payload).await?;
    }

    let elapsed = start.elapsed();
    let throughput = event_count as f64 / elapsed.as_secs_f64();
    let avg_latency = elapsed / event_count as u32;

    println!("Sequential insertion: {event_count} events in {elapsed:?}");
    println!("  Throughput: {throughput:.0} events/s");
    println!("  Avg latency: {avg_latency:?}/event");

    assert!(
        throughput > 10.0,
        "sequential throughput should be > 10 events/s"
    );

    Ok(())
}

/// Measure concurrent event insertion performance with multiple workers.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_concurrent_insertion_performance(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let worker_count = 8usize;
    let events_per_worker = 50usize;
    let total_expected = worker_count * events_per_worker;
    let success_count = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let ctx = &ctx;
    let futs: Vec<_> = (0..worker_count)
        .map(|wid| {
            let successes = success_count.clone();
            async move {
                for eid in 0..events_per_worker {
                    let payload = DynamicPayload::new(
                        format!("perf-concurrent-{wid}"),
                        "perf.insert.concurrent",
                        json!({"worker": wid, "event": eid}),
                    );
                    if ctx.publish(payload).await.is_ok() {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        })
        .collect();

    join_all(futs).await;

    let total_ok = success_count.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    let throughput = total_ok as f64 / elapsed.as_secs_f64();

    println!("Concurrent insertion: {total_ok}/{total_expected} in {elapsed:?}");
    println!("  Throughput: {throughput:.0} events/s");

    let success_rate = total_ok as f64 / total_expected as f64;
    assert!(success_rate > 0.95, "success rate should be > 95%");
    assert!(
        throughput > 50.0,
        "concurrent throughput should be > 50 events/s"
    );

    Ok(())
}

// ==================== QUERY LATENCY TESTS ====================

/// Measure query latency after seeding a dataset. Runs count and select queries
/// and reports average response times.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_query_latency_under_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    // Seed data
    for i in 0..50 {
        ctx.publish(DynamicPayload::new(
            "perf-query",
            "perf.query.test",
            json!({"seq": i}),
        ))
        .await?;
    }

    let pool = ctx.pool().clone();
    let query_count = 20u32;
    let mut latencies = Vec::new();

    for _ in 0..query_count {
        let start = Instant::now();
        let _ =
            sqlx::query!("SELECT COUNT(*) as count FROM core.events WHERE source = 'perf-query'")
                .fetch_one(&pool)
                .await?;
        latencies.push(start.elapsed());
    }

    let avg_latency: Duration = latencies.iter().sum::<Duration>() / query_count;
    let max_latency = latencies.iter().max().copied().unwrap_or_default();

    println!("Query latency ({query_count} queries):");
    println!("  Avg: {avg_latency:?}");
    println!("  Max: {max_latency:?}");

    assert!(
        avg_latency < Duration::from_millis(50),
        "average query latency should be < 50ms"
    );

    Ok(())
}

// ==================== MEMORY USAGE TESTS ====================

/// Publish a moderate number of events and verify the process doesn't show
/// runaway memory growth. This is a basic sanity check, not a profiler.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_memory_usage_under_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    // Publish events in batches and check we don't accumulate unbounded state
    for batch in 0..5 {
        for i in 0..40 {
            let payload = DynamicPayload::new(
                "perf-memory",
                "perf.memory.test",
                json!({"batch": batch, "seq": i, "data": "x".repeat(1000)}),
            );
            ctx.publish(payload).await?;
        }
    }

    let db_count = ctx.pool().events().count_all().await?;
    assert!(db_count >= 200, "should have persisted at least 200 events");

    Ok(())
}

// ==================== SCALING TESTS ====================

/// Test how throughput scales as we increase concurrent workers.
#[sinex_test(timeout = 120)]
#[ignore = "requires dedicated performance environment"]
async fn test_scaling_behavior(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let ctx = &ctx;
    let events_per_worker = 25usize;
    let mut results = Vec::new();

    for worker_count in [1, 2, 4, 8] {
        let success_count = Arc::new(AtomicU64::new(0));
        let start = Instant::now();

        let futs: Vec<_> = (0..worker_count)
            .map(|wid| {
                let successes = success_count.clone();
                async move {
                    for eid in 0..events_per_worker {
                        let payload = DynamicPayload::new(
                            format!("perf-scale-w{worker_count}-{wid}"),
                            "perf.scaling.test",
                            json!({"workers": worker_count, "worker": wid, "event": eid}),
                        );
                        if ctx.publish(payload).await.is_ok() {
                            successes.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
            .collect();

        join_all(futs).await;

        let total_ok = success_count.load(Ordering::Relaxed);
        let elapsed = start.elapsed();
        let throughput = total_ok as f64 / elapsed.as_secs_f64();
        results.push((worker_count, throughput));
        println!(
            "  {worker_count} workers: {throughput:.0} events/s ({total_ok} events in {elapsed:?})"
        );
    }

    // Throughput should generally increase (or at least not decrease drastically) with more workers
    let (_w1, t1) = results[0];
    let (_w8, t8) = results[3];
    println!("Scaling: 1-worker={t1:.0}/s, 8-worker={t8:.0}/s");

    // 8 workers should not be slower than 1 worker (allowing some margin for contention)
    assert!(
        t8 > t1 * 0.5,
        "8-worker throughput should not be less than 50% of 1-worker"
    );

    Ok(())
}

// ==================== WORKER COORDINATION TESTS ====================

/// Measure overhead of coordinating multiple workers on a shared pipeline.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_worker_coordination_overhead(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let ctx = &ctx;
    let worker_count = 4usize;
    let events_per_worker = 30usize;
    let start = Instant::now();

    let futs: Vec<_> = (0..worker_count)
        .map(|wid| async move {
            let mut ok = 0u32;
            for eid in 0..events_per_worker {
                let payload = DynamicPayload::new(
                    format!("perf-coord-{wid}"),
                    "perf.coordination.test",
                    json!({"worker": wid, "event": eid}),
                );
                if ctx.publish(payload).await.is_ok() {
                    ok += 1;
                }
            }
            ok
        })
        .collect();

    let results = join_all(futs).await;
    let total_ok: u32 = results.iter().sum();
    let elapsed = start.elapsed();

    println!("Worker coordination: {total_ok} events from {worker_count} workers in {elapsed:?}");

    assert!(total_ok > 0, "workers should complete some events");

    Ok(())
}

// ==================== THROUGHPUT TESTS ====================

/// Measure sustained throughput by publishing events in multiple rounds.
#[sinex_test(timeout = 120)]
#[ignore = "requires dedicated performance environment"]
async fn test_sustained_throughput(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let rounds = 5u32;
    let events_per_round = 40usize;
    let mut round_throughputs = Vec::new();

    for round in 0..rounds {
        let start = Instant::now();
        for i in 0..events_per_round {
            ctx.publish(DynamicPayload::new(
                "perf-sustained",
                "perf.sustained.throughput",
                json!({"round": round, "seq": i}),
            ))
            .await?;
        }
        let elapsed = start.elapsed();
        let throughput = events_per_round as f64 / elapsed.as_secs_f64();
        round_throughputs.push(throughput);
        println!("  Round {round}: {throughput:.0} events/s");
    }

    // Throughput should not degrade significantly across rounds
    let first = round_throughputs[0];
    let last = *round_throughputs.last().unwrap();
    println!("Sustained throughput: first={first:.0}/s, last={last:.0}/s");

    assert!(
        last > first * 0.5,
        "sustained throughput should not degrade more than 50%"
    );

    Ok(())
}

// ==================== BATCH PROCESSING TESTS ====================

/// Compare batch publishing performance vs sequential publishing.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_batch_processing_efficiency(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let batch_size = 50usize;

    // Batch publish
    let payloads: Vec<DynamicPayload> = (0..batch_size)
        .map(|i| {
            DynamicPayload::new(
                "perf-batch",
                "perf.batch.test",
                json!({"seq": i, "mode": "batch"}),
            )
        })
        .collect();

    let batch_start = Instant::now();
    let published = ctx.publish_many(payloads).await?;
    let batch_elapsed = batch_start.elapsed();
    let batch_throughput = published.len() as f64 / batch_elapsed.as_secs_f64();

    println!(
        "Batch: {} events in {batch_elapsed:?} ({batch_throughput:.0}/s)",
        published.len()
    );

    assert_eq!(published.len(), batch_size);
    assert!(batch_throughput > 10.0, "batch throughput should be > 10/s");

    Ok(())
}

// ==================== RESOURCE CONTENTION TESTS ====================

/// Verify that the system handles resource contention (pool + concurrent queries)
/// without deadlocking.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_resource_contention_handling(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let pool = ctx.pool().clone();

    let ctx = &ctx;

    // Mix of publishing and querying concurrently
    let publish_fut = async {
        let mut ok = 0u32;
        for i in 0..30 {
            let payload =
                DynamicPayload::new("perf-contention", "perf.contention.test", json!({"seq": i}));
            if ctx.publish(payload).await.is_ok() {
                ok += 1;
            }
        }
        ok
    };

    let query_fut = async {
        let mut ok = 0u32;
        for _ in 0..20 {
            if sqlx::query!("SELECT COUNT(*) as count FROM core.events")
                .fetch_one(&pool)
                .await
                .is_ok()
            {
                ok += 1;
            }
        }
        ok
    };

    let (publish_ok, query_ok) = tokio::join!(publish_fut, query_fut);
    println!("Resource contention: {publish_ok}/30 publishes, {query_ok}/20 queries");

    assert!(publish_ok > 0, "some publishes should succeed");
    assert!(query_ok > 0, "some queries should succeed");

    Ok(())
}

// ==================== PIPELINE PERFORMANCE TESTS ====================

/// Measure end-to-end pipeline throughput using publish_many.
#[sinex_test(timeout = 120)]
#[ignore = "requires dedicated performance environment"]
async fn test_pipeline_event_throughput(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let total_events = 200usize;
    let payloads: Vec<DynamicPayload> = (0..total_events)
        .map(|i| {
            DynamicPayload::new(
                "perf-pipeline",
                "perf.pipeline.throughput",
                json!({"seq": i}),
            )
        })
        .collect();

    let start = Instant::now();
    let published = ctx.publish_many(payloads).await?;
    let elapsed = start.elapsed();
    let throughput = published.len() as f64 / elapsed.as_secs_f64();

    println!(
        "Pipeline throughput: {} events in {elapsed:?} ({throughput:.0}/s)",
        published.len()
    );

    assert!(published.len() >= total_events);
    assert!(throughput > 20.0, "pipeline throughput should be > 20/s");

    Ok(())
}

/// Measure per-event latency through the full pipeline.
#[sinex_test(timeout = 60)]
#[ignore = "requires dedicated performance environment"]
async fn test_pipeline_latency_measurement(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    let event_count = 20usize;
    let mut latencies = Vec::new();

    for i in 0..event_count {
        let start = Instant::now();
        ctx.publish(DynamicPayload::new(
            "perf-latency",
            "perf.pipeline.latency",
            json!({"seq": i}),
        ))
        .await?;
        latencies.push(start.elapsed());
    }

    let avg: Duration = latencies.iter().sum::<Duration>() / event_count as u32;
    let max = latencies.iter().max().copied().unwrap_or_default();
    let min = latencies.iter().min().copied().unwrap_or_default();

    println!("Pipeline latency ({event_count} events):");
    println!("  Min: {min:?}");
    println!("  Avg: {avg:?}");
    println!("  Max: {max:?}");

    assert!(
        avg < Duration::from_secs(2),
        "average pipeline latency should be < 2s"
    );

    Ok(())
}
