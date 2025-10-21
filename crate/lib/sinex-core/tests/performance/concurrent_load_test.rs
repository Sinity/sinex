// # Concurrent Load Performance Testing
//
// Tests system behavior under various concurrent load patterns.
// Focuses on measuring throughput, latency, and system stability
// when multiple operations are running simultaneously.

use color_eyre::eyre::Result;
use serde_json::json;
use sinex_core::db::queries::{CheckpointQueries, EventQueries};
use sinex_core::db::query_builder::{QueryBuilder, QueryParam};
use sinex_core::types::events::{event_types, sources, EventFactory};
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{Mutex, Semaphore};

/// Concurrent load metrics
struct ConcurrentLoadMetrics {
    operation_counts: Arc<Mutex<HashMap<String, usize>>>,
    error_counts: Arc<Mutex<HashMap<String, usize>>>,
    latencies: Arc<Mutex<HashMap<String, Vec<StdDuration>>>>,
    throughput_measurements: Arc<Mutex<Vec<(Instant, usize)>>>,
    start_time: Instant,
}

impl ConcurrentLoadMetrics {
    fn new() -> Self {
        Self {
            operation_counts: Arc::new(Mutex::new(HashMap::new())),
            error_counts: Arc::new(Mutex::new(HashMap::new())),
            latencies: Arc::new(Mutex::new(HashMap::new())),
            throughput_measurements: Arc::new(Mutex::new(Vec::new())),
            start_time: Instant::now(),
        }
    }

    async fn record_operation(&self, operation_type: &str, duration: StdDuration, success: bool) {
        if success {
            let mut counts = self.operation_counts.lock().await;
            *counts.entry(operation_type.to_string()).or_insert(0) += 1;

            let mut latencies = self.latencies.lock().await;
            latencies
                .entry(operation_type.to_string())
                .or_insert_with(Vec::new)
                .push(duration);
        } else {
            let mut errors = self.error_counts.lock().await;
            *errors.entry(operation_type.to_string()).or_insert(0) += 1;
        }
    }

    async fn record_throughput_measurement(&self, operations_completed: usize) {
        let mut measurements = self.throughput_measurements.lock().await;
        measurements.push((Instant::now(), operations_completed));
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
        if let Some(times) = latencies.get(operation_type) {
            if !times.is_empty() {
                return times.iter().sum::<StdDuration>() / times.len() as u32;
            }
        }
        StdDuration::from_millis(0)
    }

    async fn get_percentile_latency(&self, operation_type: &str, percentile: f64) -> StdDuration {
        let latencies = self.latencies.lock().await;
        if let Some(times) = latencies.get(operation_type) {
            if !times.is_empty() {
                let mut sorted_times = times.clone();
                sorted_times.sort();
                let index = ((sorted_times.len() as f64 * percentile / 100.0) as usize)
                    .min(sorted_times.len() - 1);
                return sorted_times[index];
            }
        }
        StdDuration::from_millis(0)
    }

    async fn calculate_throughput(&self) -> f64 {
        let total_ops = self.get_total_operations().await;
        let elapsed = self.start_time.elapsed().as_secs_f64();
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
        println!("Total test duration: {:?}", self.start_time.elapsed());

        let total_ops = self.get_total_operations().await;
        let total_errors = self.get_total_errors().await;
        let throughput = self.calculate_throughput().await;

        println!("Total successful operations: {}", total_ops);
        println!("Total errors: {}", total_errors);
        println!("Overall throughput: {:.2} ops/sec", throughput);

        let counts = self.operation_counts.lock().await;
        for operation_type in counts.keys() {
            println!("\n🔍 Operation: {}", operation_type);
            println!("  - Count: {}", counts.get(operation_type).unwrap_or(&0));
            println!(
                "  - Success rate: {:.2}%",
                self.get_success_rate(operation_type).await
            );
            println!(
                "  - Average latency: {:?}",
                self.get_average_latency(operation_type).await
            );
            println!(
                "  - P95 latency: {:?}",
                self.get_percentile_latency(operation_type, 95.0).await
            );
            println!(
                "  - P99 latency: {:?}",
                self.get_percentile_latency(operation_type, 99.0).await
            );
        }
    }
}

// =============================================================================
// Basic Concurrent Load Tests
// =============================================================================

/// Test concurrent event ingestion with multiple workers
#[sinex_bench]
async fn test_concurrent_event_ingestion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let metrics = ConcurrentLoadMetrics::new();

    let worker_count = 20;
    let events_per_worker = 100;
    let total_expected = worker_count * events_per_worker;

    println!("🚀 Testing concurrent event ingestion:");
    println!("  - Workers: {}", worker_count);
    println!("  - Events per worker: {}", events_per_worker);
    println!("  - Total expected events: {}", total_expected);

    let worker_handles = (0..worker_count)
        .map(|worker_id| {
            let pool_clone = pool.clone();
            let metrics_clone = metrics.operation_counts.clone();
            let error_metrics = metrics.error_counts.clone();
            let latency_metrics = metrics.latencies.clone();

            tokio::spawn(async move {
                let mut worker_successes = 0;
                let mut worker_errors = 0;

                for event_id in 0..events_per_worker {
                    let operation_start = Instant::now();

                    let factory = EventFactory::new(&format!("concurrent-worker-{}", worker_id));
                    let event = factory.create_event(
                        event_types::test::CONCURRENT_INGESTION_TEST,
                        json!({
                            "worker_id": worker_id,
                            "event_id": event_id,
                            "timestamp": chrono::Utc::now().to_rfc3339(),
                            "payload_data": format!("concurrent-data-{}-{}", worker_id, event_id)
                        }),
                    );

                    match sinex_core::db::insert_event_with_validator(&pool_clone, &event, None).await {
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
                                println!("Worker {} event {} failed: {}", worker_id, event_id, e);
                            }
                        }
                    }

                    // Brief pause to prevent overwhelming
                    if event_id % 10 == 0 {
                        tokio::time::sleep(StdDuration::from_millis(1)).await;
                    }
                }

                println!(
                    "✅ Worker {} completed: {} successes, {} errors",
                    worker_id, worker_successes, worker_errors
                );
                (worker_successes, worker_errors)
            })
        })
        .collect::<Vec<_>>();

    // Wait for all workers to complete
    let results = futures::future::join_all(worker_handles).await;

    let mut total_successes = 0;
    let mut total_errors = 0;

    for result in results {
        if let Ok((successes, errors)) = result {
            total_successes += successes;
            total_errors += errors;
        }
    }

    println!("\n📊 Concurrent ingestion results:");
    println!("  - Total successes: {}", total_successes);
    println!("  - Total errors: {}", total_errors);
    println!(
        "  - Success rate: {:.2}%",
        total_successes as f64 / (total_successes + total_errors) as f64 * 100.0
    );

    metrics.print_summary().await;

    // Verify database consistency using centralized query system
    let db_count = EventQueries::count_by_source_pattern(&pool, "concurrent-worker-%").await?;

    println!("🔍 Database verification: {} events stored", db_count);

    // Performance assertions
    assert!(
        total_successes as f64 / total_expected as f64 > 0.95,
        "Success rate should be > 95%"
    );
    assert!(
        metrics.calculate_throughput().await > 100.0,
        "Concurrent throughput should be > 100 ops/sec"
    );
    assert!(
        metrics.get_average_latency("concurrent_insert").await < StdDuration::from_millis(100),
        "Average concurrent insert latency should be < 100ms"
    );

    println!("✅ Concurrent event ingestion test passed");
    Ok(())
}

/// Test mixed workload with different operation types
#[sinex_bench]
async fn test_mixed_concurrent_workload(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let metrics = ConcurrentLoadMetrics::new();

    // Pre-populate some data for queries
    println!("🔄 Pre-populating database for mixed workload test");
    let factory = EventFactory::new("mixed-workload-seed");
    for i in 0..500 {
        let event = factory.create_event(
            event_types::test::MIXED_WORKLOAD_TEST,
            json!({
                "seed_id": i,
                "test_type": "mixed_workload_seed",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }),
        );
        sinex_core::db::insert_event_with_validator(pool, &event, None).await?;
    }

    println!("🔄 Testing mixed concurrent workload");

    let worker_count = 15;
    let operations_per_worker = 80;

    let worker_handles = (0..worker_count)
        .map(|worker_id| {
            let pool_clone = pool.clone();
            let metrics_clone_ops = metrics.operation_counts.clone();
            let metrics_clone_errors = metrics.error_counts.clone();
            let metrics_clone_latencies = metrics.latencies.clone();

            tokio::spawn(async move {
                for op_id in 0..operations_per_worker {
                    // Mix of operations: 50% inserts, 30% queries, 20% complex queries
                    let operation_type = op_id % 10;

                    match operation_type {
                        0..=4 => {
                            // Insert operations (50%)
                            let operation_start = Instant::now();

                            let factory =
                                EventFactory::new(&format!("mixed-workload-worker-{}", worker_id));
                            let event = factory.create_event(
                                event_types::test::MIXED_WORKLOAD_TEST,
                                json!({
                                    "worker_id": worker_id,
                                    "operation_id": op_id,
                                    "operation_type": "insert",
                                    "data": format!("mixed-data-{}-{}", worker_id, op_id)
                                }),
                            );

                            let result =
                                sinex_core::db::insert_event_with_validator(&pool_clone, &event, None)
                                    .await;
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
                                format!("mixed-workload-worker-{}", worker_id)
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
                                SELECT source, event_type, COUNT(*) as count,
                                       MAX(ts_orig) as latest_event
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
            })
        })
        .collect::<Vec<_>>();

    // Wait for all workers to complete
    let results = futures::future::join_all(worker_handles).await;
    println!("✅ Mixed workload workers completed: {}", results.len());

    metrics.print_summary().await;

    // Verify database consistency using centralized query system
    let mixed_workload_count =
        EventQueries::count_by_source_pattern(&pool, "mixed-workload-worker-%").await?;

    println!("🔍 Mixed workload events stored: {}", mixed_workload_count);

    // Performance assertions
    assert!(
        metrics.calculate_throughput().await > 80.0,
        "Mixed workload throughput should be > 80 ops/sec"
    );
    assert!(
        metrics.get_success_rate("mixed_insert").await > 95.0,
        "Mixed insert success rate should be > 95%"
    );
    assert!(
        metrics.get_success_rate("mixed_query").await > 95.0,
        "Mixed query success rate should be > 95%"
    );
    assert!(
        metrics.get_average_latency("mixed_insert").await < StdDuration::from_millis(100),
        "Mixed insert latency should be < 100ms"
    );
    assert!(
        metrics.get_average_latency("mixed_query").await < StdDuration::from_millis(50),
        "Mixed query latency should be < 50ms"
    );

    println!("✅ Mixed concurrent workload test passed");
    Ok(())
}

/// Test system behavior under high concurrency with rate limiting
#[sinex_bench]
async fn test_rate_limited_concurrent_load(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let metrics = ConcurrentLoadMetrics::new();

    // Use semaphore to limit concurrent operations
    let max_concurrent_ops = 25;
    let semaphore = Arc::new(Semaphore::new(max_concurrent_ops));

    let total_workers = 50; // More workers than semaphore permits
    let operations_per_worker = 50;

    println!("🚦 Testing rate-limited concurrent load:");
    println!("  - Total workers: {}", total_workers);
    println!("  - Max concurrent operations: {}", max_concurrent_ops);
    println!("  - Operations per worker: {}", operations_per_worker);

    let worker_handles = (0..total_workers)
        .map(|worker_id| {
            let pool_clone = pool.clone();
            let semaphore_clone = semaphore.clone();
            let metrics_clone_ops = metrics.operation_counts.clone();
            let metrics_clone_errors = metrics.error_counts.clone();
            let metrics_clone_latencies = metrics.latencies.clone();

            tokio::spawn(async move {
                for op_id in 0..operations_per_worker {
                    // Acquire semaphore permit
                    let _permit = semaphore_clone.acquire().await.unwrap();

                    let operation_start = Instant::now();

                    let factory = EventFactory::new(&format!("rate-limited-worker-{}", worker_id));
                    let event = factory.create_event(
                        event_types::test::RATE_LIMITED_TEST,
                        json!({
                            "worker_id": worker_id,
                            "operation_id": op_id,
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }),
                    );

                    let result =
                        sinex_core::db::insert_event_with_validator(&pool_clone, &event, None).await;
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
            })
        })
        .collect::<Vec<_>>();

    // Wait for all workers to complete
    let results = futures::future::join_all(worker_handles).await;
    println!("✅ Rate-limited workers completed: {}", results.len());

    metrics.print_summary().await;

    // Verify database consistency using centralized query system
    let rate_limited_count =
        EventQueries::count_by_source_pattern(&pool, "rate-limited-worker-%").await?;

    println!("🔍 Rate-limited events stored: {}", rate_limited_count);

    // Performance assertions
    let expected_total = total_workers * operations_per_worker;
    let success_count = metrics.get_total_operations().await;
    let success_rate = success_count as f64 / expected_total as f64;

    assert!(
        success_rate > 0.98,
        "Rate-limited success rate should be > 98%"
    );
    assert!(
        metrics.get_average_latency("rate_limited_insert").await < StdDuration::from_millis(150),
        "Rate-limited average latency should be < 150ms"
    );
    assert!(
        metrics
            .get_percentile_latency("rate_limited_insert", 95.0)
            .await
            < StdDuration::from_millis(500),
        "Rate-limited P95 latency should be < 500ms"
    );

    println!("✅ Rate-limited concurrent load test passed");
    Ok(())
}

/// Test burst load handling
#[sinex_bench]
async fn test_burst_load_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let metrics = ConcurrentLoadMetrics::new();

    println!("💥 Testing burst load handling");

    // Simulate burst patterns: periods of high activity followed by low activity
    let burst_cycles = 5;
    let high_activity_workers = 30;
    let low_activity_workers = 5;
    let operations_per_burst = 20;

    for cycle in 0..burst_cycles {
        println!("\n🔥 Burst cycle {} - High activity phase", cycle + 1);

        // High activity burst
        let burst_handles = (0..high_activity_workers)
            .map(|worker_id| {
                let pool_clone = pool.clone();
                let metrics_clone_ops = metrics.operation_counts.clone();
                let metrics_clone_errors = metrics.error_counts.clone();
                let metrics_clone_latencies = metrics.latencies.clone();

                tokio::spawn(async move {
                    for op_id in 0..operations_per_burst {
                        let operation_start = Instant::now();

                        let factory =
                            EventFactory::new(&format!("burst-worker-{}-{}", cycle, worker_id));
                        let event = factory.create_event(
                            event_types::test::BURST_LOAD_TEST,
                            json!({
                                "cycle": cycle,
                                "worker_id": worker_id,
                                "operation_id": op_id,
                                "burst_type": "high_activity"
                            }),
                        );

                        let result =
                            sinex_core::db::insert_event_with_validator(&pool_clone, &event, None).await;
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
                })
            })
            .collect::<Vec<_>>();

        // Wait for high activity burst to complete
        futures::future::join_all(burst_handles).await;

        println!("🔥 High activity burst {} completed", cycle + 1);

        // Cool down period with low activity
        println!("❄️  Cycle {} - Low activity phase", cycle + 1);

        let low_activity_handles = (0..low_activity_workers)
            .map(|worker_id| {
                let pool_clone = pool.clone();
                let metrics_clone_ops = metrics.operation_counts.clone();
                let metrics_clone_errors = metrics.error_counts.clone();
                let metrics_clone_latencies = metrics.latencies.clone();

                tokio::spawn(async move {
                    // Fewer operations with longer delays
                    for op_id in 0..(operations_per_burst / 4) {
                        tokio::time::sleep(StdDuration::from_millis(50)).await;

                        let operation_start = Instant::now();

                        let factory =
                            EventFactory::new(&format!("cooldown-worker-{}-{}", cycle, worker_id));
                        let event = factory.create_event(
                            event_types::test::BURST_COOLDOWN_TEST,
                            json!({
                                "cycle": cycle,
                                "worker_id": worker_id,
                                "operation_id": op_id,
                                "burst_type": "low_activity"
                            }),
                        );

                        let result =
                            sinex_core::db::insert_event_with_validator(&pool_clone, &event, None).await;
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
                })
            })
            .collect::<Vec<_>>();

        // Wait for low activity phase to complete
        futures::future::join_all(low_activity_handles).await;

        println!("❄️  Low activity phase {} completed", cycle + 1);

        // Brief pause between cycles
        tokio::time::sleep(StdDuration::from_millis(200)).await;
    }

    metrics.print_summary().await;

    // Verify database consistency using centralized query system
    let burst_worker_count = EventQueries::count_by_source_pattern(&pool, "burst-worker-%").await?;
    let cooldown_worker_count =
        EventQueries::count_by_source_pattern(&pool, "cooldown-worker-%").await?;
    let total_burst_events = burst_worker_count + cooldown_worker_count;

    println!("🔍 Burst load events stored: {}", total_burst_events);

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

    println!("📊 Burst performance comparison:");
    println!("  - High activity latency: {:?}", high_latency);
    println!("  - Low activity latency: {:?}", low_latency);

    assert!(
        high_latency > low_latency,
        "High activity latency should be higher than low activity"
    );
    assert!(
        high_latency < StdDuration::from_millis(200),
        "Even high activity latency should be < 200ms"
    );

    println!("✅ Burst load handling test passed");
    Ok(())
}
