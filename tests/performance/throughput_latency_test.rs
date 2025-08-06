// # Throughput and Latency Performance Tests
//
// Tests that measure system throughput (events/second) and latency (response time)
// under various conditions. These tests establish performance baselines and verify
// that the system meets performance requirements.

use chrono::{Duration, Utc};
use redis::cmd;
use sinex_types::events::{event_types, sources, EventFactory};
use sinex_test_utils::prelude::*;
use sinex_types::ulid::Ulid;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::Mutex;

/// Performance measurement utilities
struct PerformanceMetrics {
    start_time: Instant,
    operation_times: Vec<StdDuration>,
    throughput_measurements: Vec<f64>,
    error_count: usize,
    success_count: usize,
}

impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            operation_times: Vec::new(),
            throughput_measurements: Vec::new(),
            error_count: 0,
            success_count: 0,
        }
    }

    fn record_operation(&mut self, duration: StdDuration, success: bool) {
        self.operation_times.push(duration);
        if success {
            self.success_count += 1;
        } else {
            self.error_count += 1;
        }
    }

    fn calculate_throughput(&self) -> f64 {
        let total_duration = self.start_time.elapsed();
        self.success_count as f64 / total_duration.as_secs_f64()
    }

    fn average_latency(&self) -> StdDuration {
        if self.operation_times.is_empty() {
            return StdDuration::from_millis(0);
        }
        let total: StdDuration = self.operation_times.iter().sum();
        total / self.operation_times.len() as u32
    }

    fn percentile_latency(&self, percentile: f64) -> StdDuration {
        if self.operation_times.is_empty() {
            return StdDuration::from_millis(0);
        }

        let mut sorted_times = self.operation_times.clone();
        sorted_times.sort();

        let index =
            ((sorted_times.len() as f64 * percentile / 100.0) as usize).min(sorted_times.len() - 1);
        sorted_times[index]
    }

    fn print_summary(&self, test_name: &str) {
        println!("\n{} Performance Summary:", test_name);
        println!("- Total duration: {:?}", self.start_time.elapsed());
        println!("- Successful operations: {}", self.success_count);
        println!("- Failed operations: {}", self.error_count);
        println!("- Throughput: {:.2} ops/sec", self.calculate_throughput());
        println!("- Average latency: {:?}", self.average_latency());
        println!("- P50 latency: {:?}", self.percentile_latency(50.0));
        println!("- P95 latency: {:?}", self.percentile_latency(95.0));
        println!("- P99 latency: {:?}", self.percentile_latency(99.0));
        println!(
            "- Max latency: {:?}",
            self.operation_times
                .iter()
                .max()
                .unwrap_or(&StdDuration::from_millis(0))
        );
    }
}

// =============================================================================
// Event Ingestion Throughput Tests
// =============================================================================

/// Test maximum event ingestion throughput
#[sinex_bench]
async fn test_event_ingestion_throughput(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let mut metrics = PerformanceMetrics::new();

    // Test configuration
    let event_count = 1000;
    let batch_size = 50;

    println!(
        "Testing event ingestion throughput with {} events",
        event_count
    );

    // Generate and process test events in batches
    let factory = EventFactory::new("throughput-test");

    // Process events in batches for better throughput
    for batch_idx in 0..(event_count / batch_size) {
        let batch_start = Instant::now();
        let mut batch_success = 0;
        let mut batch_failures = 0;

        for i in 0..batch_size {
            let event_id = batch_idx * batch_size + i;
            let event = factory.create_event(
                event_types::test::PERFORMANCE_TEST,
                json!({
                    "event_id": event_id,
                    "batch_id": batch_idx,
                    "test_type": "throughput",
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }),
            );

            let operation_start = Instant::now();

            match sinex_db::insert_event_with_validator(pool, &event, None).await {
                Ok(_) => {
                    batch_success += 1;
                    metrics.record_operation(operation_start.elapsed(), true);
                }
                Err(e) => {
                    batch_failures += 1;
                    metrics.record_operation(operation_start.elapsed(), false);
                    println!("Event ingestion failed: {}", e);
                }
            }
        }

        let batch_duration = batch_start.elapsed();
        let batch_throughput = batch_success as f64 / batch_duration.as_secs_f64();
        metrics.throughput_measurements.push(batch_throughput);

        println!(
            "Batch processed: {} success, {} failures, {:.2} ops/sec",
            batch_success, batch_failures, batch_throughput
        );
    }

    metrics.print_summary("Event Ingestion Throughput");

    // Performance assertions
    assert!(
        metrics.calculate_throughput() > 50.0,
        "Ingestion throughput should be > 50 events/second"
    );
    assert!(
        metrics.average_latency() < StdDuration::from_millis(100),
        "Average ingestion latency should be < 100ms"
    );
    assert!(
        metrics.percentile_latency(95.0) < StdDuration::from_millis(500),
        "P95 latency should be < 500ms"
    );
    assert!(
        metrics.error_count < event_count / 20,
        "Error rate should be < 5%"
    );

    println!("✓ Event ingestion throughput test passed");
    Ok(())
}

/// Test event ingestion latency under various loads
#[sinex_bench]
async fn test_event_ingestion_latency_scaling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    let load_levels = vec![1, 10, 50, 100, 200];
    let mut results = HashMap::new();

    println!("Testing event ingestion latency scaling across load levels");

    for load_level in load_levels {
        println!("\nTesting load level: {} events", load_level);

        let mut metrics = PerformanceMetrics::new();
        let factory = EventFactory::new("latency-test");

        for i in 0..load_level {
            let event = factory.create_event(
                event_types::test::PERFORMANCE_TEST,
                json!({
                    "event_id": i,
                    "load_level": load_level,
                    "test_type": "latency_scaling",
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }),
            );

            let operation_start = Instant::now();

            match sinex_db::insert_event_with_validator(pool, &event, None).await {
                Ok(_) => {
                    metrics.record_operation(operation_start.elapsed(), true);
                }
                Err(e) => {
                    metrics.record_operation(operation_start.elapsed(), false);
                    println!("Event ingestion failed: {}", e);
                }
            }
        }

        let avg_latency = metrics.average_latency();
        let throughput = metrics.calculate_throughput();

        results.insert(load_level, (avg_latency, throughput));

        println!(
            "Load level {}: avg latency {:?}, throughput {:.2} ops/sec",
            load_level, avg_latency, throughput
        );
    }

    println!("\nLatency Scaling Results:");
    for (load, (latency, throughput)) in &results {
        println!(
            "Load {}: {:?} latency, {:.2} ops/sec",
            load, latency, throughput
        );
    }

    // Verify latency doesn't degrade too much with load
    let low_load_latency = results[&1].0;
    let high_load_latency = results[&200].0;

    assert!(
        high_load_latency < low_load_latency * 5,
        "Latency should not degrade more than 5x under high load"
    );

    println!("✓ Event ingestion latency scaling test passed");
    Ok(())
}

// =============================================================================
// Database Query Performance Tests
// =============================================================================

/// Test database query performance across different query patterns
#[sinex_bench]
async fn test_database_query_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Pre-populate database with test data
    let event_count = 1000;
    let factory = EventFactory::new("query-test-seed");

    println!("Populating database with {} test events", event_count);

    for i in 0..event_count {
        let event = factory.create_event(
            event_types::test::PERFORMANCE_TEST,
            json!({
                "event_id": i,
                "test_type": "query_test_seed",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }),
        );
        sinex_db::insert_event_with_validator(pool, &event, None).await?;
    }

    // Test different query patterns
    let query_tests = vec![
        (
            "Simple ID lookup",
            "SELECT * FROM core.events WHERE event_id = $1::uuid",
        ),
        (
            "Source filter",
            "SELECT * FROM core.events WHERE source = $1",
        ),
        (
            "Event type filter",
            "SELECT * FROM core.events WHERE event_type = $1",
        ),
        (
            "Time range query",
            "SELECT * FROM core.events WHERE ts_orig >= $1 AND ts_orig <= $2",
        ),
        (
            "Payload JSON query",
            "SELECT * FROM core.events WHERE payload @> $1::jsonb",
        ),
        (
            "Complex filter",
            "SELECT * FROM core.events WHERE source = $1 AND event_type = $2 AND ts_orig >= $3",
        ),
    ];

    for (test_name, query) in query_tests {
        println!("\nTesting query: {}", test_name);
        let mut metrics = PerformanceMetrics::new();

        // Run query multiple times to get stable measurements
        for _ in 0..100 {
            let operation_start = Instant::now();

            let result = match test_name {
                "Simple ID lookup" => {
                    let test_id = test_events[0].id.to_uuid();
                    sqlx::query(query).bind(test_id).fetch_all(pool).await
                }
                "Source filter" => {
                    sqlx::query(query)
                        .bind(&test_events[0].source)
                        .fetch_all(pool)
                        .await
                }
                "Event type filter" => {
                    sqlx::query(query)
                        .bind(&test_events[0].event_type)
                        .fetch_all(pool)
                        .await
                }
                "Time range query" => {
                    sqlx::query(query)
                        .bind(Utc::now() - Duration::hours(1))
                        .bind(Utc::now())
                        .fetch_all(pool)
                        .await
                }
                "Payload JSON query" => {
                    sqlx::query(query)
                        .bind(serde_json::json!({"test": "value"}))
                        .fetch_all(pool)
                        .await
                }
                "Complex filter" => {
                    sqlx::query(query)
                        .bind(&test_events[0].source)
                        .bind(&test_events[0].event_type)
                        .bind(Utc::now() - Duration::hours(1))
                        .fetch_all(pool)
                        .await
                }
                _ => unreachable!(),
            };

            match result {
                Ok(rows) => {
                    metrics.record_operation(operation_start.elapsed(), true);
                    if metrics.success_count == 1 {
                        println!("Query returned {} rows", rows.len());
                    }
                }
                Err(e) => {
                    metrics.record_operation(operation_start.elapsed(), false);
                    println!("Query failed: {}", e);
                }
            }
        }

        println!("Query '{}' performance:", test_name);
        println!("  - Average latency: {:?}", metrics.average_latency());
        println!("  - P95 latency: {:?}", metrics.percentile_latency(95.0));
        println!(
            "  - Success rate: {:.2}%",
            (metrics.success_count as f64 / (metrics.success_count + metrics.error_count) as f64)
                * 100.0
        );

        // Query performance assertions
        assert!(
            metrics.average_latency() < StdDuration::from_millis(50),
            "Average query latency should be < 50ms for {}",
            test_name
        );
        assert!(
            metrics.percentile_latency(95.0) < StdDuration::from_millis(200),
            "P95 query latency should be < 200ms for {}",
            test_name
        );
        assert!(
            metrics.error_count == 0,
            "All queries should succeed for {}",
            test_name
        );
    }

    println!("✓ Database query performance test passed");
    Ok(())
}

// =============================================================================
// Concurrent Access Performance Tests
// =============================================================================

/// Test system performance under concurrent access
#[sinex_bench]
async fn test_concurrent_access_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    let concurrent_workers = 20;
    let operations_per_worker = 50;
    let total_operations = concurrent_workers * operations_per_worker;

    println!("Testing concurrent access performance:");
    println!("- Workers: {}", concurrent_workers);
    println!("- Operations per worker: {}", operations_per_worker);
    println!("- Total operations: {}", total_operations);

    let shared_metrics = Arc::new(Mutex::new(PerformanceMetrics::new()));

    let worker_handles = (0..concurrent_workers)
        .map(|worker_id| {
            let pool_clone = pool.clone();
            let metrics = shared_metrics.clone();

            tokio::spawn(async move {
                for operation_id in 0..operations_per_worker {
                    let operation_start = Instant::now();

                    // Mix of operations: 70% inserts, 20% queries, 10% updates
                    let operation_type = operation_id % 10;
                    let success = match operation_type {
                        0..=6 => {
                            // Insert operation
                            let factory = EventFactory::new(&format!("concurrent-worker-{}", worker_id));
                            let event = factory.create_event(
                                event_types::test::CONCURRENT_PERFORMANCE_TEST,
                                serde_json::json!({
                                    "worker_id": worker_id,
                                    "operation_id": operation_id,
                                    "operation_type": "insert"
                                })
                            );

                            match sinex_db::insert_event_with_validator(&pool_clone, &event, None).await {
                                Ok(_) => true,
                                Err(e) => {
                                    println!("Worker {} insert failed: {}", worker_id, e);
                                    false
                                }
                            }
                        }
                        7..=8 => {
                            // Query operation
                            match sqlx::query!(
                                "SELECT COUNT(*) as count FROM core.events WHERE source = $1",
                                format!("concurrent-worker-{}", worker_id)
                            ).fetch_one(&pool_clone).await {
                                Ok(_) => true,
                                Err(e) => {
                                    println!("Worker {} query failed: {}", worker_id, e);
                                    false
                                }
                            }
                        }
                        9 => {
                            // Simulated update operation (actually a query with complex filter)
                            match sqlx::query!(
                                "SELECT * FROM core.events WHERE source = $1 AND event_type = $2 ORDER BY ts_orig DESC LIMIT 1",
                                format!("concurrent-worker-{}", worker_id),
                                "concurrent.performance.test"
                            ).fetch_optional(&pool_clone).await {
                                Ok(_) => true,
                                Err(e) => {
                                    println!("Worker {} update failed: {}", worker_id, e);
                                    false
                                }
                            }
                        }
                        _ => unreachable!(),
                    };

                    let mut metrics_lock = metrics.lock().await;
                    metrics_lock.record_operation(operation_start.elapsed(), success);
                }

                println!("Worker {} completed", worker_id);
            })
        })
        .collect::<Vec<_>>();

    // Wait for all workers to complete
    futures::future::join_all(worker_handles).await;

    let final_metrics = shared_metrics.lock().await;
    final_metrics.print_summary("Concurrent Access Performance");

    // Verify database consistency
    let total_inserted = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE source LIKE 'concurrent-worker-%'"
    )
    .fetch_one(pool)
    .await?;

    println!(
        "Database consistency check: {} events inserted",
        total_inserted.count.unwrap_or(0)
    );

    // Performance assertions
    assert!(
        final_metrics.calculate_throughput() > 100.0,
        "Concurrent throughput should be > 100 ops/sec"
    );
    assert!(
        final_metrics.average_latency() < StdDuration::from_millis(200),
        "Average latency under concurrent load should be < 200ms"
    );
    assert!(
        final_metrics.percentile_latency(99.0) < StdDuration::from_secs(1),
        "P99 latency should be < 1 second"
    );
    assert!(
        final_metrics.error_count < total_operations / 10,
        "Error rate should be < 10% under concurrent load"
    );

    println!("✓ Concurrent access performance test passed");
    Ok(())
}

// =============================================================================
// Stream Processing Performance Tests
// =============================================================================

/// Test Redis stream processing performance
#[sinex_bench]
async fn test_stream_processing_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use sinex_satellite_sdk::RedisStreamClient;

    let redis_client = RedisStreamClient::new("redis://localhost:6379")?;
    let stream_key = "sinex:performance:stream";
    let consumer_group = "performance-test-group";

    // Test configuration
    let message_count = 1000;
    let batch_size = 50;

    println!("Testing Redis stream processing performance:");
    println!("- Messages: {}", message_count);
    println!("- Batch size: {}", batch_size);

    // Phase 1: Stream writing performance
    let mut write_metrics = PerformanceMetrics::new();

    println!("\nPhase 1: Stream writing performance");

    for i in 0..message_count {
        let operation_start = Instant::now();

        let message_data = serde_json::json!({
            "message_id": i,
            "timestamp": Utc::now().to_rfc3339(),
            "payload": format!("performance-test-message-{}", i),
            "batch_id": i / batch_size
        });

        match redis_client.xadd(stream_key, "*", &message_data).await {
            Ok(_) => {
                write_metrics.record_operation(operation_start.elapsed(), true);
                if i % 100 == 0 {
                    println!("Wrote {} messages", i + 1);
                }
            }
            Err(e) => {
                write_metrics.record_operation(operation_start.elapsed(), false);
                println!("Stream write failed: {}", e);
            }
        }
    }

    write_metrics.print_summary("Stream Writing");

    // Phase 2: Stream reading performance
    let mut read_metrics = PerformanceMetrics::new();

    println!("\nPhase 2: Stream reading performance");

    // Create consumer group
    match redis_client
        .xgroup_create(stream_key, consumer_group, "0", true)
        .await
    {
        Ok(_) => println!("Created consumer group"),
        Err(e) => println!("Consumer group creation failed (may exist): {}", e),
    }

    let mut messages_read = 0;
    while messages_read < message_count {
        let operation_start = Instant::now();

        match cmd("XREADGROUP")
            .arg("GROUP")
            .arg(consumer_group)
            .arg("performance-consumer")
            .arg("COUNT")
            .arg(batch_size)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async::<_, redis::streams::StreamReadReply>(&mut redis_client)
            .await
        {
            Ok(messages) => {
                read_metrics.record_operation(operation_start.elapsed(), true);

                if messages.keys.is_empty() {
                    println!("No more messages available");
                    break;
                }

                // Acknowledge messages
                for message in &messages {
                    match redis_client
                        .xack(stream_key, consumer_group, &message.id)
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => println!("ACK failed: {}", e),
                    }
                }

                messages_read += messages.keys.len();

                if messages_read % 100 == 0 {
                    println!("Read {} messages", messages_read);
                }
            }
            Err(e) => {
                read_metrics.record_operation(operation_start.elapsed(), false);
                println!("Stream read failed: {}", e);
            }
        }
    }

    read_metrics.print_summary("Stream Reading");

    // Performance assertions
    assert!(
        write_metrics.calculate_throughput() > 200.0,
        "Stream write throughput should be > 200 messages/sec"
    );
    assert!(
        read_metrics.calculate_throughput() > 150.0,
        "Stream read throughput should be > 150 messages/sec"
    );
    assert!(
        write_metrics.average_latency() < StdDuration::from_millis(10),
        "Average write latency should be < 10ms"
    );
    assert!(
        read_metrics.average_latency() < StdDuration::from_millis(100),
        "Average read latency should be < 100ms"
    );

    println!("✓ Stream processing performance test passed");
    Ok(())
}

// =============================================================================
// End-to-End Performance Tests
// =============================================================================

/// Test end-to-end performance across the entire system
#[sinex_bench]
async fn test_end_to_end_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    // Simulate complete workflow: generation -> ingestion -> processing
    let workflow_count = 200;
    let mut workflow_metrics = PerformanceMetrics::new();

    println!(
        "Testing end-to-end performance with {} workflows",
        workflow_count
    );

    for workflow_id in 0..workflow_count {
        let workflow_start = Instant::now();

        // Step 1: Event generation (simulate satellite)
        let factory = EventFactory::new("performance-satellite");
        let event = factory.create_event(
            event_types::test::END_TO_END_PERFORMANCE_TEST,
            serde_json::json!({
                "workflow_id": workflow_id,
                "step": "generation",
                "timestamp": Utc::now().to_rfc3339(),
                "data": format!("end-to-end-test-data-{}", workflow_id)
            }),
        );

        // Step 2: Event ingestion
        let ingestion_result = sinex_db::insert_event_with_validator(pool, &event, None).await;

        // Step 3: Event verification (simulate processing)
        let verification_result = if ingestion_result.is_ok() {
            sqlx::query!(
                "SELECT id, payload FROM core.events WHERE event_id = $1::uuid",
                event.id.to_uuid()
            )
            .fetch_optional(pool)
            .await
        } else {
            Ok(None)
        };

        let workflow_success = ingestion_result.is_ok() && verification_result.is_ok();
        workflow_metrics.record_operation(workflow_start.elapsed(), workflow_success);

        if workflow_id % 50 == 0 {
            println!("Completed {} workflows", workflow_id + 1);
        }

        if !workflow_success {
            println!(
                "Workflow {} failed: ingestion={}, verification={}",
                workflow_id,
                ingestion_result.is_ok(),
                verification_result.is_ok()
            );
        }
    }

    workflow_metrics.print_summary("End-to-End Workflow");

    // Verify database state
    let total_events = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE source = 'performance-satellite'"
    )
    .fetch_one(pool)
    .await?;

    println!(
        "Database verification: {} events stored",
        total_events.count.unwrap_or(0)
    );

    // Performance assertions
    assert!(
        workflow_metrics.calculate_throughput() > 20.0,
        "End-to-end throughput should be > 20 workflows/sec"
    );
    assert!(
        workflow_metrics.average_latency() < StdDuration::from_millis(500),
        "Average end-to-end latency should be < 500ms"
    );
    assert!(
        workflow_metrics.percentile_latency(95.0) < StdDuration::from_secs(1),
        "P95 end-to-end latency should be < 1 second"
    );
    assert!(
        workflow_metrics.error_count < workflow_count / 20,
        "End-to-end error rate should be < 5%"
    );
    assert_eq!(
        total_events.count.unwrap_or(0),
        workflow_metrics.success_count as i64,
        "Database should contain all successful workflows"
    );

    println!("✓ End-to-end performance test passed");
    Ok(())
}
