// # Baseline Performance Testing
//
// Establishes and tracks performance baselines for key system operations.
// These tests create repeatable benchmarks that can be used to detect
// performance regressions and track improvements over time.

use redis::cmd;
use serde_json::json;
use sinex_db::queries::{CheckpointQueries, EventQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_events::{event_types, sources, EventFactory};
use sinex_satellite_sdk::RedisStreamClient;
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};

/// Performance baseline measurements
#[derive(Debug, Clone)]
pub struct PerformanceBaseline {
    pub operation_name: String,
    pub average_latency: StdDuration,
    pub percentile_95_latency: StdDuration,
    pub percentile_99_latency: StdDuration,
    pub throughput: f64,
    pub success_rate: f64,
    pub sample_size: usize,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub environment_info: EnvironmentInfo,
}

#[derive(Debug, Clone)]
pub struct EnvironmentInfo {
    pub test_data_size: usize,
    pub concurrent_operations: usize,
    pub database_pool_size: usize,
    pub system_load: String,
}

/// Baseline tracking and storage
pub struct BaselineTracker {
    baselines: HashMap<String, PerformanceBaseline>,
    measurements: HashMap<String, Vec<StdDuration>>,
    success_counts: HashMap<String, usize>,
    error_counts: HashMap<String, usize>,
    start_time: Instant,
}

impl BaselineTracker {
    pub fn new() -> Self {
        Self {
            baselines: HashMap::new(),
            measurements: HashMap::new(),
            success_counts: HashMap::new(),
            error_counts: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    pub fn record_measurement(&mut self, operation: &str, duration: StdDuration, success: bool) {
        self.measurements
            .entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(duration);

        if success {
            *self
                .success_counts
                .entry(operation.to_string())
                .or_insert(0) += 1;
        } else {
            *self.error_counts.entry(operation.to_string()).or_insert(0) += 1;
        }
    }

    pub fn calculate_baseline(
        &mut self,
        operation: &str,
        env_info: EnvironmentInfo,
    ) -> Option<PerformanceBaseline> {
        if let Some(measurements) = self.measurements.get(operation) {
            if measurements.len() < 10 {
                return None; // Not enough samples
            }

            let mut sorted_measurements = measurements.clone();
            sorted_measurements.sort();

            let average_latency =
                measurements.iter().sum::<StdDuration>() / measurements.len() as u32;

            let p95_index = (measurements.len() as f64 * 0.95) as usize;
            let p99_index = (measurements.len() as f64 * 0.99) as usize;

            let percentile_95_latency =
                sorted_measurements[p95_index.min(sorted_measurements.len() - 1)];
            let percentile_99_latency =
                sorted_measurements[p99_index.min(sorted_measurements.len() - 1)];

            let success_count = self.success_counts.get(operation).unwrap_or(&0);
            let error_count = self.error_counts.get(operation).unwrap_or(&0);
            let total_operations = success_count + error_count;

            let success_rate = if total_operations > 0 {
                *success_count as f64 / total_operations as f64 * 100.0
            } else {
                0.0
            };

            let throughput = *success_count as f64 / self.start_time.elapsed().as_secs_f64();

            let baseline = PerformanceBaseline {
                operation_name: operation.to_string(),
                average_latency,
                percentile_95_latency,
                percentile_99_latency,
                throughput,
                success_rate,
                sample_size: measurements.len(),
                timestamp: chrono::Utc::now(),
                environment_info: env_info,
            };

            self.baselines
                .insert(operation.to_string(), baseline.clone());
            Some(baseline)
        } else {
            None
        }
    }

    pub fn get_baseline(&self, operation: &str) -> Option<&PerformanceBaseline> {
        self.baselines.get(operation)
    }

    pub fn print_baselines(&self) {
        println!("\n📊 Performance Baselines Summary:");
        println!("Test duration: {:?}", self.start_time.elapsed());

        for (operation, baseline) in &self.baselines {
            println!("\n🎯 Baseline: {}", operation);
            println!("  - Average latency: {:?}", baseline.average_latency);
            println!("  - P95 latency: {:?}", baseline.percentile_95_latency);
            println!("  - P99 latency: {:?}", baseline.percentile_99_latency);
            println!("  - Throughput: {:.2} ops/sec", baseline.throughput);
            println!("  - Success rate: {:.2}%", baseline.success_rate);
            println!("  - Sample size: {}", baseline.sample_size);
            println!(
                "  - Test data size: {}",
                baseline.environment_info.test_data_size
            );
            println!(
                "  - Concurrent ops: {}",
                baseline.environment_info.concurrent_operations
            );
        }
    }

    pub fn save_baselines_to_database(
        &self,
        pool: &sqlx::PgPool,
    ) -> AnyhowResult<(), Box<dyn std::error::Error + Send + Sync>> {
        // In a real implementation, save baselines to a dedicated performance tracking table
        println!("💾 Baselines would be saved to database for historical tracking");
        Ok(())
    }
}

// =============================================================================
// Core Operation Baselines
// =============================================================================

/// Establish baseline for basic database operations
#[sinex_test]
async fn test_establish_database_operation_baselines(ctx: TestContext) -> anyhow::Result<()> {
    let pool = ctx.pool().clone();
    let mut tracker = BaselineTracker::new();

    println!("🎯 Establishing database operation baselines");

    // Test configuration
    let test_data_size = 1000;
    let test_iterations = 100;

    // Pre-populate test data
    println!("  Populating {} test events", test_data_size);
    let factory = EventFactory::new("baseline-test");
    for i in 0..test_data_size {
        let event = factory.create_event(
            event_types::test::GENERIC,
            json!({
                "test_id": i,
                "test_type": "baseline_population",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }),
        );
        sinex_db::insert_event_with_validator(pool, &event, None).await?;
    }

    let env_info = EnvironmentInfo {
        test_data_size,
        concurrent_operations: 1,
        database_pool_size: pool.size() as usize,
        system_load: "baseline".to_string(),
    };

    // Baseline 1: Single event insertion
    println!("\n📝 Baseline: Single event insertion");
    for i in 0..test_iterations {
        let start = Instant::now();

        let factory = EventFactory::new("baseline-test");
        let event = factory.create_event(
            event_types::test::BASELINE_INSERTION_TEST,
            json!({
                "iteration": i,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "baseline_type": "single_insertion"
            }),
        );

        let result = sinex_db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();

        tracker.record_measurement("single_event_insertion", duration, result.is_ok());

        if i % 20 == 0 {
            println!("    Completed {} insertion operations", i + 1);
        }
    }

    // Baseline 2: Primary key lookup
    println!("\n🔍 Baseline: Primary key lookup");
    for i in 0..test_iterations {
        let start = Instant::now();

        let test_event = &test_events[i % test_events.len()];
        // Keep as raw SQL for precise timing measurement
        let result = sqlx::query!(
            "SELECT * FROM core.events WHERE event_id = $1::uuid",
            test_event.id.to_uuid()
        )
        .fetch_optional(pool)
        .await;

        let duration = start.elapsed();
        tracker.record_measurement("primary_key_lookup", duration, result.is_ok());
    }

    // Baseline 3: Source-based query
    println!("\n🔎 Baseline: Source-based query");
    for i in 0..test_iterations {
        let start = Instant::now();

        let test_source = &test_events[i % test_events.len()].source;
        let result = sqlx::query!(
            "SELECT * FROM core.events WHERE source = $1 LIMIT 10",
            test_source
        )
        .fetch_all(pool)
        .await;

        let duration = start.elapsed();
        tracker.record_measurement("source_based_query", duration, result.is_ok());
    }

    // Baseline 4: Time range query
    println!("\n⏰ Baseline: Time range query");
    for i in 0..test_iterations {
        let start = Instant::now();

        let end_time = chrono::Utc::now();
        let start_time = end_time - chrono::Duration::hours(1);

        let result = sqlx::query!(
            "SELECT * FROM core.events WHERE ts_orig >= $1 AND ts_orig <= $2 LIMIT 50",
            start_time,
            end_time
        )
        .fetch_all(pool)
        .await;

        let duration = start.elapsed();
        tracker.record_measurement("time_range_query", duration, result.is_ok());
    }

    // Baseline 5: Aggregation query
    println!("\n📊 Baseline: Aggregation query");
    for i in 0..test_iterations {
        let start = Instant::now();

        let result = sqlx::query!(
            "SELECT source, COUNT(*) as count FROM core.events GROUP BY source ORDER BY count DESC LIMIT 20"
        ).fetch_all(pool).await;

        let duration = start.elapsed();
        tracker.record_measurement("aggregation_query", duration, result.is_ok());
    }

    // Calculate baselines
    let operations = vec![
        "single_event_insertion",
        "primary_key_lookup",
        "source_based_query",
        "time_range_query",
        "aggregation_query",
    ];

    for operation in operations {
        if let Some(baseline) = tracker.calculate_baseline(operation, env_info.clone()) {
            println!("\n✅ Baseline established for: {}", baseline.operation_name);

            // Store baseline assertions for regression testing
            match operation {
                "single_event_insertion" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(50),
                        "Single insertion baseline should be < 50ms"
                    );
                    assert!(
                        baseline.success_rate > 99.0,
                        "Single insertion success rate should be > 99%"
                    );
                }
                "primary_key_lookup" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(5),
                        "Primary key lookup baseline should be < 5ms"
                    );
                    assert!(
                        baseline.success_rate > 99.0,
                        "Primary key lookup success rate should be > 99%"
                    );
                }
                "source_based_query" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(20),
                        "Source query baseline should be < 20ms"
                    );
                }
                "time_range_query" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(100),
                        "Time range query baseline should be < 100ms"
                    );
                }
                "aggregation_query" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(200),
                        "Aggregation query baseline should be < 200ms"
                    );
                }
                _ => {}
            }
        }
    }

    tracker.print_baselines();

    println!("✅ Database operation baselines established");
    Ok(())
}

/// Establish baseline for Redis stream operations
#[sinex_test]
async fn test_establish_redis_stream_baselines(ctx: TestContext) -> anyhow::Result<()> {
    let redis_client = RedisStreamClient::new("redis://localhost:6379")?;
    let mut redis_conn = redis_client.get_connection().await?;
    let mut tracker = BaselineTracker::new();

    println!("🎯 Establishing Redis stream operation baselines");

    let stream_key = "sinex:baseline:stream-test";
    let consumer_group = "baseline-group";
    let test_iterations = 200;

    // Clean up existing stream
    let _ = redis_client.del(stream_key).await;

    let env_info = EnvironmentInfo {
        test_data_size: test_iterations,
        concurrent_operations: 1,
        database_pool_size: 0,
        system_load: "baseline".to_string(),
    };

    // Baseline 1: Stream write operations
    println!("\n✍️  Baseline: Stream write operations");
    for i in 0..test_iterations {
        let start = Instant::now();

        let message_data = json!({
            "message_id": i,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "event_type": event_types::test::BASELINE_STREAM_WRITE,
            "payload": format!("baseline-message-{}", i),
            "iteration": i
        });

        let result = redis_client.xadd(stream_key, "*", &message_data).await;
        let duration = start.elapsed();

        tracker.record_measurement("stream_write", duration, result.is_ok());

        if i % 50 == 0 {
            println!("    Completed {} write operations", i + 1);
        }
    }

    // Create consumer group for read operations
    match redis_client
        .xgroup_create(stream_key, consumer_group, "0", true)
        .await
    {
        Ok(_) => println!("  Created consumer group for baseline testing"),
        Err(e) => println!("  Consumer group creation: {}", e),
    }

    // Baseline 2: Stream read operations
    println!("\n📖 Baseline: Stream read operations");
    let mut messages_read = 0;
    let read_iterations = 40; // Reading in batches

    for i in 0..read_iterations {
        let start = Instant::now();

        match cmd("XREADGROUP")
            .arg("GROUP")
            .arg(consumer_group)
            .arg("baseline-consumer")
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async::<_, redis::streams::StreamReadReply>(&mut redis_conn)
            .await
        {
            Ok(messages) => {
                let duration = start.elapsed();
                tracker.record_measurement("stream_read_batch", duration, true);

                // ACK messages
                for stream in &messages.keys {
                    for message in &stream.ids {
                        let ack_start = Instant::now();
                        let ack_result = redis_client
                            .xack(stream_key, consumer_group, &message.id)
                            .await;
                        let ack_duration = ack_start.elapsed();
                        tracker.record_measurement("stream_ack", ack_duration, ack_result.is_ok());
                    }
                }

                messages_read += messages.keys.iter().map(|k| k.ids.len()).sum::<usize>();

                if messages.keys.is_empty() {
                    break;
                }
            }
            Err(e) => {
                let duration = start.elapsed();
                tracker.record_measurement("stream_read_batch", duration, false);
                println!("    Read operation {} failed: {}", i, e);
            }
        }
    }

    println!("  Total messages read: {}", messages_read);

    // Baseline 3: Stream info operations
    println!("\n📊 Baseline: Stream info operations");
    for i in 0..50 {
        let start = Instant::now();

        let result = redis_client.xlen::<_, usize>(stream_key).await;
        let duration = start.elapsed();

        tracker.record_measurement("stream_info", duration, result.is_ok());
    }

    // Calculate Redis baselines
    let redis_operations = vec![
        "stream_write",
        "stream_read_batch",
        "stream_ack",
        "stream_info",
    ];

    for operation in redis_operations {
        if let Some(baseline) = tracker.calculate_baseline(operation, env_info.clone()) {
            println!(
                "\n✅ Redis baseline established for: {}",
                baseline.operation_name
            );

            // Store baseline assertions
            match operation {
                "stream_write" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(10),
                        "Stream write baseline should be < 10ms"
                    );
                    assert!(
                        baseline.success_rate > 99.0,
                        "Stream write success rate should be > 99%"
                    );
                }
                "stream_read_batch" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(50),
                        "Stream read baseline should be < 50ms"
                    );
                }
                "stream_ack" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(5),
                        "Stream ACK baseline should be < 5ms"
                    );
                }
                "stream_info" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(5),
                        "Stream info baseline should be < 5ms"
                    );
                }
                _ => {}
            }
        }
    }

    tracker.print_baselines();

    // Cleanup
    let _ = redis_client.del(stream_key).await;

    println!("✅ Redis stream operation baselines established");
    Ok(())
}

/// Establish baseline for concurrent operations
#[sinex_test]
async fn test_establish_concurrent_operation_baselines(ctx: TestContext) -> anyhow::Result<()> {
    let pool = ctx.pool().clone();
    let mut tracker = BaselineTracker::new();

    println!("🎯 Establishing concurrent operation baselines");

    let concurrent_workers = 10;
    let operations_per_worker = 50;

    let env_info = EnvironmentInfo {
        test_data_size: concurrent_workers * operations_per_worker,
        concurrent_operations: concurrent_workers,
        database_pool_size: pool.size() as usize,
        system_load: "concurrent_baseline".to_string(),
    };

    println!(
        "  Configuration: {} workers, {} ops each",
        concurrent_workers, operations_per_worker
    );

    // Baseline: Concurrent database insertions
    println!("\n🔄 Baseline: Concurrent database insertions");

    let worker_handles = (0..concurrent_workers)
        .map(|worker_id| {
            let pool_clone = pool.clone();

            tokio::spawn(async move {
                let mut worker_measurements = Vec::new();
                let mut worker_successes = 0;
                let mut worker_errors = 0;

                for op_id in 0..operations_per_worker {
                    let start = Instant::now();

                    let factory =
                        EventFactory::new(&format!("concurrent-baseline-worker-{}", worker_id));
                    let event = factory.create_event(
                        event_types::test::CONCURRENT_BASELINE_TEST,
                        json!({
                            "worker_id": worker_id,
                            "operation_id": op_id,
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }),
                    );

                    let result =
                        sinex_db::insert_event_with_validator(&pool_clone, &event, None).await;
                    let duration = start.elapsed();

                    worker_measurements.push((duration, result.is_ok()));

                    if result.is_ok() {
                        worker_successes += 1;
                    } else {
                        worker_errors += 1;
                    }
                }

                (worker_measurements, worker_successes, worker_errors)
            })
        })
        .collect::<Vec<_>>();

    // Wait for all workers to complete
    let worker_results = futures::future::join_all(worker_handles).await;

    // Aggregate measurements
    let mut total_successes = 0;
    let mut total_errors = 0;

    for result in worker_results {
        if let Ok((measurements, successes, errors)) = result {
            total_successes += successes;
            total_errors += errors;

            for (duration, success) in measurements {
                tracker.record_measurement("concurrent_insertion", duration, success);
            }
        }
    }

    println!(
        "  Concurrent operations completed: {} successes, {} errors",
        total_successes, total_errors
    );

    // Calculate concurrent baseline
    if let Some(baseline) = tracker.calculate_baseline("concurrent_insertion", env_info.clone()) {
        println!(
            "\n✅ Concurrent baseline established: {}",
            baseline.operation_name
        );

        // Concurrent operations should be reasonably performant
        assert!(
            baseline.average_latency < StdDuration::from_millis(200),
            "Concurrent insertion baseline should be < 200ms"
        );
        assert!(
            baseline.success_rate > 95.0,
            "Concurrent insertion success rate should be > 95%"
        );
        assert!(
            baseline.throughput > 50.0,
            "Concurrent throughput should be > 50 ops/sec"
        );
    }

    tracker.print_baselines();

    println!("✅ Concurrent operation baselines established");
    Ok(())
}

/// Establish baseline for system recovery operations
#[sinex_test]
async fn test_establish_recovery_baselines(ctx: TestContext) -> anyhow::Result<()> {
    let pool = ctx.pool().clone();
    let mut tracker = BaselineTracker::new();

    println!("🎯 Establishing system recovery baselines");

    let env_info = EnvironmentInfo {
        test_data_size: 100,
        concurrent_operations: 1,
        database_pool_size: pool.size() as usize,
        system_load: "recovery_baseline".to_string(),
    };

    // Baseline 1: Database connection recovery
    println!("\n🔌 Baseline: Database connection recovery");

    for i in 0..20 {
        let start = Instant::now();

        // Acquire and immediately release connection to test pool behavior
        match pool.acquire().await {
            Ok(conn) => {
                drop(conn);
                let duration = start.elapsed();
                tracker.record_measurement("connection_recovery", duration, true);
            }
            Err(e) => {
                let duration = start.elapsed();
                tracker.record_measurement("connection_recovery", duration, false);
                println!("    Connection recovery {} failed: {}", i, e);
            }
        }

        // Small delay between attempts
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }

    // Baseline 2: Transaction recovery
    println!("\n💳 Baseline: Transaction recovery");

    for i in 0..20 {
        let start = Instant::now();

        let mut tx = pool.begin().await?;

        // Perform operation and commit
        let factory = EventFactory::new("recovery-baseline-test");
        let event = factory.create_event(
            event_types::test::RECOVERY_BASELINE_TEST,
            json!({
                "iteration": i,
                "test_type": "transaction_recovery"
            }),
        );

        let insert_result = sinex_db::insert_event_with_validator(&mut tx, &event, None).await;
        let commit_result = if insert_result.is_ok() {
            tx.commit().await
        } else {
            tx.rollback().await
        };

        let duration = start.elapsed();
        let success = insert_result.is_ok() && commit_result.is_ok();
        tracker.record_measurement("transaction_recovery", duration, success);
    }

    // Baseline 3: Redis reconnection
    println!("\n📡 Baseline: Redis reconnection");

    for i in 0..20 {
        let start = Instant::now();

        // Create new Redis client to test connection establishment
        match RedisStreamClient::new("redis://localhost:6379") {
            Ok(client) => {
                // Test simple operation
                let test_result = client
                    .xadd("baseline:recovery-test", "*", &json!({"test": i}))
                    .await;
                let duration = start.elapsed();
                tracker.record_measurement("redis_reconnection", duration, test_result.is_ok());

                // Cleanup
                let _ = client.del("baseline:recovery-test").await;
            }
            Err(e) => {
                let duration = start.elapsed();
                tracker.record_measurement("redis_reconnection", duration, false);
                println!("    Redis reconnection {} failed: {}", i, e);
            }
        }
    }

    // Calculate recovery baselines
    let recovery_operations = vec![
        "connection_recovery",
        "transaction_recovery",
        "redis_reconnection",
    ];

    for operation in recovery_operations {
        if let Some(baseline) = tracker.calculate_baseline(operation, env_info.clone()) {
            println!(
                "\n✅ Recovery baseline established for: {}",
                baseline.operation_name
            );

            // Recovery operations should be fast and reliable
            match operation {
                "connection_recovery" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(50),
                        "Connection recovery baseline should be < 50ms"
                    );
                    assert!(
                        baseline.success_rate > 99.0,
                        "Connection recovery success rate should be > 99%"
                    );
                }
                "transaction_recovery" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(100),
                        "Transaction recovery baseline should be < 100ms"
                    );
                    assert!(
                        baseline.success_rate > 95.0,
                        "Transaction recovery success rate should be > 95%"
                    );
                }
                "redis_reconnection" => {
                    assert!(
                        baseline.average_latency < StdDuration::from_millis(100),
                        "Redis reconnection baseline should be < 100ms"
                    );
                    assert!(
                        baseline.success_rate > 95.0,
                        "Redis reconnection success rate should be > 95%"
                    );
                }
                _ => {}
            }
        }
    }

    tracker.print_baselines();

    println!("✅ System recovery baselines established");
    Ok(())
}
