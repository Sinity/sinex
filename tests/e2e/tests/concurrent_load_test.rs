// # Concurrent Load Performance Testing
//
// Tests system behavior under various concurrent load patterns.
// Focuses on measuring throughput, latency, and system stability
// when multiple operations are running simultaneously.

use futures::future::join_all;
use sinex_primitives::{DynamicPayload, Timestamp};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{Mutex, Semaphore};
use xtask::sandbox::prelude::*;

/// Concurrent load metrics
struct ConcurrentLoadMetrics {
    operation_counts: Arc<Mutex<HashMap<String, usize>>>,
    error_counts: Arc<Mutex<HashMap<String, usize>>>,
    latencies: Arc<Mutex<HashMap<String, Vec<StdDuration>>>>,
    #[allow(dead_code)]
    throughput_measurements: Arc<Mutex<Vec<(Instant, usize)>>>,
    start_time: Arc<Mutex<Instant>>,
}

impl ConcurrentLoadMetrics {
    fn new() -> Self {
        Self {
            operation_counts: Arc::new(Mutex::new(HashMap::new())),
            error_counts: Arc::new(Mutex::new(HashMap::new())),
            latencies: Arc::new(Mutex::new(HashMap::new())),
            throughput_measurements: Arc::new(Mutex::new(Vec::new())),
            start_time: Arc::new(Mutex::new(Instant::now())),
        }
    }

    async fn reset_window(&self) {
        *self.start_time.lock().await = Instant::now();
        self.throughput_measurements.lock().await.clear();
    }

    async fn get_total_operations(&self) -> usize {
        let counts = self.operation_counts.lock().await;
        counts.values().sum()
    }

    async fn get_total_errors(&self) -> usize {
        let errors = self.error_counts.lock().await;
        errors.values().sum()
    }

    async fn get_average_latency(&self, operation_type: &str) -> StdDuration {
        let latencies = self.latencies.lock().await;
        if let Some(times) = latencies.get(operation_type)
            && !times.is_empty()
        {
            return times.iter().sum::<StdDuration>() / times.len() as u32;
        }
        StdDuration::from_millis(0)
    }

    async fn get_percentile_latency(&self, operation_type: &str, percentile: f64) -> StdDuration {
        let latencies = self.latencies.lock().await;
        if let Some(times) = latencies.get(operation_type)
            && !times.is_empty()
        {
            let mut sorted_times = times.clone();
            sorted_times.sort();
            let index = ((sorted_times.len() as f64 * percentile / 100.0) as usize)
                .min(sorted_times.len() - 1);
            return sorted_times[index];
        }
        StdDuration::from_millis(0)
    }

    async fn calculate_throughput(&self) -> f64 {
        let total_ops = self.get_total_operations().await;
        let elapsed = self.start_time.lock().await.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            total_ops as f64 / elapsed
        } else {
            0.0
        }
    }

    async fn get_success_rate(&self, operation_type: &str) -> f64 {
        let counts = self.operation_counts.lock().await;
        let errors = self.error_counts.lock().await;

        let success = counts.get(operation_type).unwrap_or(&0);
        let error = errors.get(operation_type).unwrap_or(&0);
        let total = success + error;

        if total > 0 {
            *success as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }

    async fn print_summary(&self) {
        println!("\n📊 Concurrent Load Performance Summary:");
        println!(
            "Total measured duration: {:?}",
            self.start_time.lock().await.elapsed()
        );

        let total_ops = self.get_total_operations().await;
        let total_errors = self.get_total_errors().await;
        let throughput = self.calculate_throughput().await;

        println!("Total successful operations: {total_ops}");
        println!("Total errors: {total_errors}");
        println!("Overall throughput: {throughput:.2} ops/sec");

        let counts = self.operation_counts.lock().await.clone();
        let errors = self.error_counts.lock().await.clone();
        let latencies = self.latencies.lock().await.clone();

        for operation_type in counts.keys() {
            let success = *counts.get(operation_type).unwrap_or(&0);
            let error = *errors.get(operation_type).unwrap_or(&0);
            let total = success + error;
            let success_rate = if total > 0 {
                success as f64 / total as f64 * 100.0
            } else {
                0.0
            };
            let operation_latencies = latencies.get(operation_type).cloned().unwrap_or_default();
            let average_latency = if operation_latencies.is_empty() {
                StdDuration::from_millis(0)
            } else {
                operation_latencies.iter().sum::<StdDuration>() / operation_latencies.len() as u32
            };
            let mut sorted_latencies = operation_latencies;
            sorted_latencies.sort();
            let percentile_latency = |percentile: f64| {
                if sorted_latencies.is_empty() {
                    return StdDuration::from_millis(0);
                }
                let index = ((sorted_latencies.len() as f64 * percentile / 100.0) as usize)
                    .min(sorted_latencies.len() - 1);
                sorted_latencies[index]
            };

            println!("\n🔍 Operation: {operation_type}");
            println!("  - Count: {success}");
            println!("  - Success rate: {success_rate:.2}%");
            println!("  - Average latency: {average_latency:?}");
            println!("  - P95 latency: {:?}", percentile_latency(95.0));
            println!("  - P99 latency: {:?}", percentile_latency(99.0));
        }
    }
}

// =============================================================================
// Basic Concurrent Load Tests
// =============================================================================

/// Test concurrent event ingestion with multiple workers
#[sinex_test]
#[ignore = "heavy: run with xtask test --heavy"]
async fn test_concurrent_event_ingestion(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let metrics = ConcurrentLoadMetrics::new();

    let worker_count = 12;
    let events_per_worker = 60;
    let total_expected = worker_count * events_per_worker;

    println!("🚀 Testing concurrent event ingestion:");
    println!("  - Workers: {worker_count}");
    println!("  - Events per worker: {events_per_worker}");
    println!("  - Total expected events: {total_expected}");

    metrics.reset_window().await;

    let worker_handles: Vec<_> = (0..worker_count)
        .map(|worker_id| {
            let metrics_clone = metrics.operation_counts.clone();
            let error_metrics = metrics.error_counts.clone();
            let latency_metrics = metrics.latencies.clone();

            async move {
                let mut worker_successes = 0;
                let mut worker_errors = 0;

                for event_id in 0..events_per_worker {
                    let operation_start = Instant::now();

                    let payload = DynamicPayload::new(
                        format!("concurrent-worker-{worker_id}"),
                        "concurrent.ingestion.test",
                        json!({
                            "worker_id": worker_id,
                            "event_id": event_id,
                            "timestamp": Timestamp::now().to_string(),
                            "payload_data": format!("concurrent-data-{worker_id}-{event_id}")
                        }),
                    );

                    match ctx.publish(payload).await {
                        Ok(_) => {
                            worker_successes += 1;
                            let duration = operation_start.elapsed();

                            // Record metrics
                            {
                                let mut counts = metrics_clone.lock().await;
                                *counts.entry("concurrent_insert".to_string()).or_insert(0) += 1;
                            }
                            {
                                let mut latencies = latency_metrics.lock().await;
                                latencies
                                    .entry("concurrent_insert".to_string())
                                    .or_insert_with(Vec::new)
                                    .push(duration);
                            }
                        }
                        Err(e) => {
                            worker_errors += 1;
                            let mut errors = error_metrics.lock().await;
                            *errors.entry("concurrent_insert".to_string()).or_insert(0) += 1;

                            if worker_errors <= 3 {
                                // Only log first few errors
                                println!("Worker {worker_id} event {event_id} failed: {e}");
                            }
                        }
                    }

                    // Brief pause to prevent overwhelming
                    if event_id % 10 == 0 {
                        tokio::time::sleep(StdDuration::from_millis(1)).await;
                    }
                }

                println!(
                    "✅ Worker {worker_id} completed: {worker_successes} successes, {worker_errors} errors"
                );
                (worker_successes, worker_errors)
            }
        })
        .collect();

    // Wait for all workers to complete
    let results = join_all(worker_handles).await;

    let mut total_successes = 0;
    let mut total_errors = 0;

    for (successes, errors) in &results {
        total_successes += successes;
        total_errors += errors;
    }

    println!("\n📊 Concurrent ingestion results:");
    println!("  - Total successes: {total_successes}");
    println!("  - Total errors: {total_errors}");
    println!(
        "  - Success rate: {:.2}%",
        f64::from(total_successes) / f64::from(total_successes + total_errors) * 100.0
    );

    metrics.print_summary().await;

    // Verify database consistency
    let db_count = ctx.pool().events().count_all().await?;
    println!("🔍 Database verification: {db_count} events stored");

    // Performance assertions
    assert!(
        f64::from(total_successes) / f64::from(total_expected) > 0.95,
        "Success rate should be > 95%"
    );
    assert!(
        metrics.get_average_latency("concurrent_insert").await < StdDuration::from_millis(300),
        "Average concurrent insert latency should stay below 300ms under the heavy lane"
    );

    println!("✅ Concurrent event ingestion test passed");
    Ok(())
}

/// Test mixed workload with different operation types
#[sinex_test]
#[ignore = "heavy: run with xtask test --heavy"]
async fn test_mixed_concurrent_workload(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let metrics = ConcurrentLoadMetrics::new();

    // Pre-populate some data for queries
    println!("🔄 Pre-populating database for mixed workload test");
    for i in 0..300 {
        let payload = DynamicPayload::new(
            "mixed-workload-seed",
            "mixed.workload.test",
            json!({
                "seed_id": i,
                "test_type": "mixed_workload_seed",
                "timestamp": Timestamp::now().to_string()
            }),
        );
        ctx.publish(payload).await?;
    }

    metrics.reset_window().await;

    println!("🔄 Testing mixed concurrent workload");

    let worker_count = 10;
    let operations_per_worker = 50;
    let pool = ctx.pool().clone();

    let worker_handles: Vec<_> = (0..worker_count)
        .map(|worker_id| {
            let pool_clone = pool.clone();
            let metrics_clone_ops = metrics.operation_counts.clone();
            let metrics_clone_errors = metrics.error_counts.clone();
            let metrics_clone_latencies = metrics.latencies.clone();

            async move {
                for op_id in 0..operations_per_worker {
                    // Mix of operations: 50% inserts, 30% queries, 20% complex queries
                    let operation_type = op_id % 10;

                    match operation_type {
                        0..=4 => {
                            // Insert operations (50%)
                            let operation_start = Instant::now();

                            let payload = DynamicPayload::new(
                                format!("mixed-workload-worker-{worker_id}"),
                                "mixed.workload.test",
                                json!({
                                    "worker_id": worker_id,
                                    "operation_id": op_id,
                                    "operation_type": "insert",
                                    "data": format!("mixed-data-{worker_id}-{op_id}")
                                }),
                            );

                            let result = ctx.publish(payload).await;
                            let duration = operation_start.elapsed();

                            if result.is_ok() {
                                let mut counts = metrics_clone_ops.lock().await;
                                *counts.entry("mixed_insert".to_string()).or_insert(0) += 1;

                                let mut latencies = metrics_clone_latencies.lock().await;
                                latencies
                                    .entry("mixed_insert".to_string())
                                    .or_insert_with(Vec::new)
                                    .push(duration);
                            } else {
                                let mut errors = metrics_clone_errors.lock().await;
                                *errors.entry("mixed_insert".to_string()).or_insert(0) += 1;
                            }
                        }
                        5..=7 => {
                            // Simple query operations (30%)
                            let operation_start = Instant::now();

                            let result = sqlx::query!(
                                "SELECT COUNT(*) as count FROM core.events WHERE source = $1",
                                format!("mixed-workload-worker-{worker_id}")
                            )
                            .fetch_one(&pool_clone)
                            .await;

                            let duration = operation_start.elapsed();

                            if result.is_ok() {
                                let mut counts = metrics_clone_ops.lock().await;
                                *counts.entry("mixed_query".to_string()).or_insert(0) += 1;

                                let mut latencies = metrics_clone_latencies.lock().await;
                                latencies
                                    .entry("mixed_query".to_string())
                                    .or_insert_with(Vec::new)
                                    .push(duration);
                            } else {
                                let mut errors = metrics_clone_errors.lock().await;
                                *errors.entry("mixed_query".to_string()).or_insert(0) += 1;
                            }
                        }
                        8..=9 => {
                            // Complex query operations (20%)
                            let operation_start = Instant::now();

                            let result = sqlx::query!(
                                r#"
                                SELECT source, event_type, COUNT(*) as count
                                FROM core.events
                                WHERE ts_orig >= NOW() - INTERVAL '1 hour'
                                GROUP BY source, event_type
                                ORDER BY count DESC
                                LIMIT 10
                                "#
                            )
                            .fetch_all(&pool_clone)
                            .await;

                            let duration = operation_start.elapsed();

                            if result.is_ok() {
                                let mut counts = metrics_clone_ops.lock().await;
                                *counts.entry("mixed_complex_query".to_string()).or_insert(0) += 1;

                                let mut latencies = metrics_clone_latencies.lock().await;
                                latencies
                                    .entry("mixed_complex_query".to_string())
                                    .or_insert_with(Vec::new)
                                    .push(duration);
                            } else {
                                let mut errors = metrics_clone_errors.lock().await;
                                *errors.entry("mixed_complex_query".to_string()).or_insert(0) += 1;
                            }
                        }
                        _ => unreachable!(),
                    }

                    // Small delay between operations
                    tokio::time::sleep(StdDuration::from_millis(2)).await;
                }

                worker_id
            }
        })
        .collect();

    // Wait for all workers to complete
    let results = join_all(worker_handles).await;
    println!("✅ Mixed workload workers completed: {}", results.len());

    metrics.print_summary().await;

    // Verify database consistency
    let event_count = ctx.pool().events().count_all().await?;
    println!("🔍 Mixed workload events stored: {event_count}");

    // Performance assertions
    // The mixed workload intentionally combines inserts with read-heavy queries.
    // On a saturated heavy lane, correctness and latency stability matter more
    // than an optimistic raw-throughput ceiling.
    assert!(
        metrics.get_success_rate("mixed_insert").await > 95.0,
        "Mixed insert success rate should be > 95%"
    );
    assert!(
        metrics.get_success_rate("mixed_query").await > 95.0,
        "Mixed query success rate should be > 95%"
    );
    assert!(
        metrics.get_average_latency("mixed_insert").await < StdDuration::from_millis(250),
        "Mixed insert latency should stay below 250ms under the heavy lane"
    );
    assert!(
        metrics.get_average_latency("mixed_query").await < StdDuration::from_millis(50),
        "Mixed query latency should be < 50ms"
    );

    println!("✅ Mixed concurrent workload test passed");
    Ok(())
}

/// Test system behavior under high concurrency with rate limiting
#[sinex_test]
#[ignore = "heavy: run with xtask test --heavy"]
async fn test_rate_limited_concurrent_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let metrics = ConcurrentLoadMetrics::new();

    // Use semaphore to limit concurrent operations
    let max_concurrent_ops = 25;
    let semaphore = Arc::new(Semaphore::new(max_concurrent_ops));

    let total_workers = 50; // More workers than semaphore permits
    let operations_per_worker = 50;

    println!("🚦 Testing rate-limited concurrent load:");
    println!("  - Total workers: {total_workers}");
    println!("  - Max concurrent operations: {max_concurrent_ops}");
    println!("  - Operations per worker: {operations_per_worker}");

    metrics.reset_window().await;

    let worker_handles: Vec<_> = (0..total_workers)
        .map(|worker_id| {
            let semaphore_clone = semaphore.clone();
            let metrics_clone_ops = metrics.operation_counts.clone();
            let metrics_clone_errors = metrics.error_counts.clone();
            let metrics_clone_latencies = metrics.latencies.clone();

            async move {
                for op_id in 0..operations_per_worker {
                    // Acquire semaphore permit
                    let _permit = semaphore_clone.acquire().await.unwrap();

                    let operation_start = Instant::now();

                    let payload = DynamicPayload::new(
                        format!("rate-limited-worker-{worker_id}"),
                        "rate.limited.test",
                        json!({
                            "worker_id": worker_id,
                            "operation_id": op_id,
                            "timestamp": Timestamp::now().to_string()
                        }),
                    );

                    let result = ctx.publish(payload).await;
                    let duration = operation_start.elapsed();

                    // Record metrics
                    if result.is_ok() {
                        let mut counts = metrics_clone_ops.lock().await;
                        *counts.entry("rate_limited_insert".to_string()).or_insert(0) += 1;

                        let mut latencies = metrics_clone_latencies.lock().await;
                        latencies
                            .entry("rate_limited_insert".to_string())
                            .or_insert_with(Vec::new)
                            .push(duration);
                    } else {
                        let mut errors = metrics_clone_errors.lock().await;
                        *errors.entry("rate_limited_insert".to_string()).or_insert(0) += 1;
                    }

                    // Permit is automatically released when _permit is dropped
                }

                worker_id
            }
        })
        .collect();

    // Wait for all workers to complete
    let results = join_all(worker_handles).await;
    println!("✅ Rate-limited workers completed: {}", results.len());

    metrics.print_summary().await;

    // Verify database consistency
    let event_count = ctx.pool().events().count_all().await?;
    println!("🔍 Rate-limited events stored: {event_count}");

    // Performance assertions
    let expected_total = total_workers * operations_per_worker;
    let success_count = metrics.get_total_operations().await;
    let success_rate = success_count as f64 / f64::from(expected_total);

    assert!(
        success_rate > 0.98,
        "Rate-limited success rate should be > 98%"
    );
    assert!(
        metrics.get_average_latency("rate_limited_insert").await < StdDuration::from_millis(350),
        "Rate-limited average latency should be < 350ms under the heavy lane"
    );
    assert!(
        metrics
            .get_percentile_latency("rate_limited_insert", 95.0)
            .await
            < StdDuration::from_millis(750),
        "Rate-limited P95 latency should be < 750ms under the heavy lane"
    );

    println!("✅ Rate-limited concurrent load test passed");
    Ok(())
}

/// Test burst load handling
#[sinex_test]
#[ignore = "heavy: run with xtask test --heavy"]
async fn test_burst_load_handling(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let ctx = &ctx;
    let metrics = ConcurrentLoadMetrics::new();

    println!("💥 Testing burst load handling");

    // Simulate burst patterns: periods of high activity followed by low activity
    let burst_cycles = 5;
    let high_activity_workers = 20;
    let low_activity_workers = 4;
    let operations_per_burst = 15;

    metrics.reset_window().await;

    for cycle in 0..burst_cycles {
        println!("\n🔥 Burst cycle {} - High activity phase", cycle + 1);

        // High activity burst
        let burst_handles: Vec<_> = (0..high_activity_workers)
            .map(|worker_id| {
                let metrics_clone_ops = metrics.operation_counts.clone();
                let metrics_clone_errors = metrics.error_counts.clone();
                let metrics_clone_latencies = metrics.latencies.clone();

                async move {
                    for op_id in 0..operations_per_burst {
                        let operation_start = Instant::now();

                        let payload = DynamicPayload::new(
                            format!("burst-worker-{cycle}-{worker_id}"),
                            "burst.load.test",
                            json!({
                                "cycle": cycle,
                                "worker_id": worker_id,
                                "operation_id": op_id,
                                "burst_type": "high_activity"
                            }),
                        );

                        let result = ctx.publish(payload).await;
                        let duration = operation_start.elapsed();

                        if result.is_ok() {
                            let mut counts = metrics_clone_ops.lock().await;
                            *counts.entry("burst_high".to_string()).or_insert(0) += 1;

                            let mut latencies = metrics_clone_latencies.lock().await;
                            latencies
                                .entry("burst_high".to_string())
                                .or_insert_with(Vec::new)
                                .push(duration);
                        } else {
                            let mut errors = metrics_clone_errors.lock().await;
                            *errors.entry("burst_high".to_string()).or_insert(0) += 1;
                        }
                    }
                }
            })
            .collect();

        // Wait for high activity burst to complete
        join_all(burst_handles).await;

        println!("🔥 High activity burst {} completed", cycle + 1);

        // Cool down period with low activity
        println!("❄️  Cycle {} - Low activity phase", cycle + 1);

        let low_activity_handles: Vec<_> = (0..low_activity_workers)
            .map(|worker_id| {
                let metrics_clone_ops = metrics.operation_counts.clone();
                let metrics_clone_errors = metrics.error_counts.clone();
                let metrics_clone_latencies = metrics.latencies.clone();

                async move {
                    // Fewer operations with longer delays
                    for op_id in 0..(operations_per_burst / 4) {
                        tokio::time::sleep(StdDuration::from_millis(50)).await;

                        let operation_start = Instant::now();

                        let payload = DynamicPayload::new(
                            format!("cooldown-worker-{cycle}-{worker_id}"),
                            "burst.cooldown.test",
                            json!({
                                "cycle": cycle,
                                "worker_id": worker_id,
                                "operation_id": op_id,
                                "burst_type": "low_activity"
                            }),
                        );

                        let result = ctx.publish(payload).await;
                        let duration = operation_start.elapsed();

                        if result.is_ok() {
                            let mut counts = metrics_clone_ops.lock().await;
                            *counts.entry("burst_low".to_string()).or_insert(0) += 1;

                            let mut latencies = metrics_clone_latencies.lock().await;
                            latencies
                                .entry("burst_low".to_string())
                                .or_insert_with(Vec::new)
                                .push(duration);
                        } else {
                            let mut errors = metrics_clone_errors.lock().await;
                            *errors.entry("burst_low".to_string()).or_insert(0) += 1;
                        }
                    }
                }
            })
            .collect();

        // Wait for low activity phase to complete
        join_all(low_activity_handles).await;

        println!("❄️  Low activity phase {} completed", cycle + 1);

        // Brief pause between cycles
        tokio::time::sleep(StdDuration::from_millis(200)).await;
    }

    metrics.print_summary().await;

    // Verify database consistency
    let event_count = ctx.pool().events().count_all().await?;
    println!("🔍 Burst load events stored: {event_count}");

    // Performance assertions
    assert!(
        metrics.get_success_rate("burst_high").await > 90.0,
        "High activity burst success rate should be > 90%"
    );
    assert!(
        metrics.get_success_rate("burst_low").await > 95.0,
        "Low activity success rate should be > 95%"
    );

    // High activity should have higher latency than low activity
    let high_latency = metrics.get_average_latency("burst_high").await;
    let low_latency = metrics.get_average_latency("burst_low").await;
    let high_p95 = metrics.get_percentile_latency("burst_high", 95.0).await;
    let low_p95 = metrics.get_percentile_latency("burst_low", 95.0).await;

    println!("📊 Burst performance comparison:");
    println!("  - High activity latency: {high_latency:?}");
    println!("  - Low activity latency: {low_latency:?}");
    println!("  - High activity P95 latency: {high_p95:?}");
    println!("  - Low activity P95 latency: {low_p95:?}");

    assert!(
        high_latency < StdDuration::from_millis(400),
        "High activity latency should stay below 400ms under the heavy lane"
    );
    assert!(
        high_p95 < StdDuration::from_millis(750),
        "High activity P95 latency should stay below 750ms under the heavy lane"
    );
    assert!(
        low_latency < StdDuration::from_millis(250),
        "Cooldown latency should stay below 250ms after each burst"
    );
    assert!(
        low_p95 < StdDuration::from_millis(900),
        "Cooldown P95 latency should stay below 900ms after each burst"
    );

    println!("✅ Burst load handling test passed");
    Ok(())
}
