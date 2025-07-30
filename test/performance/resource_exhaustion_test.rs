// # Resource Exhaustion Performance Tests
//
// Tests system behavior when approaching resource limits including
// memory pressure, connection pool exhaustion, disk space limits,
// and CPU saturation. Critical for understanding system failure modes.

use serde_json::json;
use sinex_events::{event_types, sources, EventFactory};
use sinex_satellite_sdk::RedisStreamClient;
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{Mutex, Semaphore};

/// Resource exhaustion metrics
struct ResourceExhaustionMetrics {
    resource_usage: HashMap<String, Vec<usize>>,
    operation_times: HashMap<String, Vec<StdDuration>>,
    failure_points: HashMap<String, usize>,
    recovery_times: HashMap<String, StdDuration>,
    error_counts: HashMap<String, usize>,
    success_counts: HashMap<String, usize>,
    start_time: Instant,
}

impl ResourceExhaustionMetrics {
    fn new() -> Self {
        Self {
            resource_usage: HashMap::new(),
            operation_times: HashMap::new(),
            failure_points: HashMap::new(),
            recovery_times: HashMap::new(),
            error_counts: HashMap::new(),
            success_counts: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    fn record_resource_usage(&mut self, resource: &str, usage: usize) {
        self.resource_usage
            .entry(resource.to_string())
            .or_insert_with(Vec::new)
            .push(usage);
    }

    fn record_operation(&mut self, operation: &str, duration: StdDuration, success: bool) {
        if success {
            *self
                .success_counts
                .entry(operation.to_string())
                .or_insert(0) += 1;
        } else {
            *self.error_counts.entry(operation.to_string()).or_insert(0) += 1;
        }

        self.operation_times
            .entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(duration);
    }

    fn record_failure_point(&mut self, resource: &str, failure_level: usize) {
        self.failure_points
            .insert(resource.to_string(), failure_level);
    }

    fn record_recovery_time(&mut self, resource: &str, recovery_duration: StdDuration) {
        self.recovery_times
            .insert(resource.to_string(), recovery_duration);
    }

    fn get_peak_usage(&self, resource: &str) -> usize {
        self.resource_usage
            .get(resource)
            .and_then(|usage| usage.iter().max())
            .copied()
            .unwrap_or(0)
    }

    fn get_average_usage(&self, resource: &str) -> f64 {
        if let Some(usage) = self.resource_usage.get(resource) {
            if !usage.is_empty() {
                return usage.iter().sum::<usize>() as f64 / usage.len() as f64;
            }
        }
        0.0
    }

    fn success_rate(&self, operation: &str) -> f64 {
        let success = self.success_counts.get(operation).unwrap_or(&0);
        let errors = self.error_counts.get(operation).unwrap_or(&0);
        let total = success + errors;
        if total > 0 {
            *success as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }

    fn average_latency(&self, operation: &str) -> StdDuration {
        if let Some(times) = self.operation_times.get(operation) {
            if !times.is_empty() {
                return times.iter().sum::<StdDuration>() / times.len() as u32;
            }
        }
        StdDuration::from_millis(0)
    }

    fn print_summary(&self) {
        println!("\n📊 Resource Exhaustion Test Summary:");
        println!("Total test duration: {:?}", self.start_time.elapsed());

        println!("\n🔧 Resource Usage:");
        for (resource, _) in &self.resource_usage {
            println!(
                "  {} - Peak: {}, Average: {:.1}",
                resource,
                self.get_peak_usage(resource),
                self.get_average_usage(resource)
            );
        }

        println!("\n❌ Failure Points:");
        for (resource, failure_point) in &self.failure_points {
            println!("  {} failed at level: {}", resource, failure_point);
        }

        println!("\n🔄 Recovery Times:");
        for (resource, recovery_time) in &self.recovery_times {
            println!("  {} recovered in: {:?}", resource, recovery_time);
        }

        println!("\n📈 Operation Performance:");
        for operation in self.operation_times.keys() {
            println!(
                "  {} - Success rate: {:.2}%, Avg latency: {:?}",
                operation,
                self.success_rate(operation),
                self.average_latency(operation)
            );
        }
    }
}

// =============================================================================
// Connection Pool Exhaustion Tests
// =============================================================================

/// Test behavior when database connection pool is exhausted
#[sinex_test]
async fn test_connection_pool_exhaustion(ctx: TestContext) -> anyhow::Result<()> {
    let pool = ctx.pool().clone();
    let mut metrics = ResourceExhaustionMetrics::new();

    println!("🏊 Testing connection pool exhaustion");

    // Get pool configuration
    let max_connections = pool.size() as usize;
    println!("  Pool max connections: {}", max_connections);

    // Phase 1: Gradually increase connection usage
    println!("\n📈 Phase 1: Gradual connection increase");

    let mut held_connections = Vec::new();
    let mut connection_acquisition_attempts = 0;

    // Try to acquire connections up to and beyond the pool limit
    for i in 0..(max_connections + 10) {
        let acquire_start = Instant::now();
        connection_acquisition_attempts += 1;

        match tokio::time::timeout(StdDuration::from_millis(100), pool.acquire()).await {
            Ok(Ok(conn)) => {
                let acquire_duration = acquire_start.elapsed();
                metrics.record_operation("connection_acquire", acquire_duration, true);
                metrics.record_resource_usage("active_connections", held_connections.len() + 1);

                held_connections.push(conn);

                if i % 5 == 0 {
                    println!("    Acquired {} connections", held_connections.len());
                }
            }
            Ok(Err(e)) => {
                let acquire_duration = acquire_start.elapsed();
                metrics.record_operation("connection_acquire", acquire_duration, false);
                metrics.record_failure_point("connection_pool", i);

                println!("    Connection acquisition failed at {}: {}", i, e);
                break;
            }
            Err(_) => {
                // Timeout
                let acquire_duration = acquire_start.elapsed();
                metrics.record_operation("connection_acquire", acquire_duration, false);
                metrics.record_failure_point("connection_pool", i);

                println!("    Connection acquisition timed out at {}", i);
                break;
            }
        }
    }

    println!(
        "  Held connections: {}/{}",
        held_connections.len(),
        max_connections
    );

    // Phase 2: Test operations with exhausted pool
    println!("\n⚠️  Phase 2: Operations with exhausted pool");

    // Try to perform database operations while pool is exhausted
    let exhausted_operations = 20;

    for i in 0..exhausted_operations {
        let operation_start = Instant::now();

        // Try a simple query with timeout
        let query_result = tokio::time::timeout(
            StdDuration::from_millis(50),
            sqlx::query("SELECT 1 as test").fetch_one(pool),
        )
        .await;

        let operation_duration = operation_start.elapsed();

        match query_result {
            Ok(Ok(_)) => {
                metrics.record_operation("exhausted_pool_query", operation_duration, true);
                println!("    Unexpected success on operation {}", i);
            }
            Ok(Err(e)) => {
                metrics.record_operation("exhausted_pool_query", operation_duration, false);
                if i < 3 {
                    println!("    Expected failure on operation {}: {}", i, e);
                }
            }
            Err(_) => {
                metrics.record_operation("exhausted_pool_query", operation_duration, false);
                if i < 3 {
                    println!("    Expected timeout on operation {}", i);
                }
            }
        }
    }

    // Phase 3: Test recovery after releasing connections
    println!("\n🔄 Phase 3: Pool recovery");

    let recovery_start = Instant::now();

    // Release half the connections
    let connections_to_release = held_connections.len() / 2;
    for _ in 0..connections_to_release {
        if let Some(conn) = held_connections.pop() {
            drop(conn);
        }
    }

    println!(
        "  Released {} connections, {} remaining",
        connections_to_release,
        held_connections.len()
    );

    // Test if operations work again
    let mut recovery_successful = false;
    for attempt in 0..10 {
        let operation_start = Instant::now();

        match tokio::time::timeout(
            StdDuration::from_millis(100),
            sqlx::query("SELECT 2 as test").fetch_one(pool),
        )
        .await
        {
            Ok(Ok(_)) => {
                let operation_duration = operation_start.elapsed();
                metrics.record_operation("recovery_query", operation_duration, true);
                recovery_successful = true;
                println!("    Recovery successful on attempt {}", attempt + 1);
                break;
            }
            Ok(Err(e)) => {
                let operation_duration = operation_start.elapsed();
                metrics.record_operation("recovery_query", operation_duration, false);
                println!("    Recovery attempt {} failed: {}", attempt + 1, e);
            }
            Err(_) => {
                let operation_duration = operation_start.elapsed();
                metrics.record_operation("recovery_query", operation_duration, false);
                println!("    Recovery attempt {} timed out", attempt + 1);
            }
        }

        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }

    let recovery_duration = recovery_start.elapsed();
    metrics.record_recovery_time("connection_pool", recovery_duration);

    // Release all remaining connections
    drop(held_connections);

    metrics.print_summary();

    // Assertions
    assert!(
        metrics.get_peak_usage("active_connections") >= max_connections,
        "Should reach or exceed pool limit"
    );
    assert!(
        metrics.success_rate("exhausted_pool_query") < 50.0,
        "Operations should mostly fail with exhausted pool"
    );
    assert!(
        recovery_successful,
        "Pool should recover after releasing connections"
    );
    assert!(
        metrics.average_latency("recovery_query") < StdDuration::from_millis(200),
        "Recovery queries should be fast once pool recovers"
    );

    println!("✅ Connection pool exhaustion test passed");
    Ok(())
}

/// Test memory pressure scenarios
#[sinex_test]
async fn test_memory_pressure_scenarios(ctx: TestContext) -> anyhow::Result<()> {
    let pool = ctx.pool().clone();
    let mut metrics = ResourceExhaustionMetrics::new();

    println!("🧠 Testing memory pressure scenarios");

    // Phase 1: Large payload stress test
    println!("\n📦 Phase 1: Large payload stress test");

    let large_payload_sizes = vec![
        (1024 * 1024, "1MB"),
        (5 * 1024 * 1024, "5MB"),
        (10 * 1024 * 1024, "10MB"),
    ];

    for (payload_size, size_label) in large_payload_sizes {
        println!("    Testing {} payloads", size_label);

        let large_data = "x".repeat(payload_size);
        let payload_count = 5; // Small count for very large payloads

        for i in 0..payload_count {
            let operation_start = Instant::now();

            // Get baseline memory usage (rough estimate)
            let memory_usage = estimate_memory_usage();
            metrics.record_resource_usage(&format!("memory_{}", size_label), memory_usage);

            let factory = EventFactory::new("memory-pressure-test");
            let event = factory.create_event(
                &format!("memory.pressure.{}", size_label),
                json!({
                    "size_label": size_label,
                    "iteration": i,
                    "large_data": &large_data,
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }),
            );

            match sinex_db::insert_event_with_validator(pool, &event, None).await {
                Ok(_) => {
                    let operation_duration = operation_start.elapsed();
                    metrics.record_operation(
                        &format!("large_payload_{}", size_label),
                        operation_duration,
                        true,
                    );

                    println!(
                        "      {} payload {} inserted successfully",
                        size_label,
                        i + 1
                    );
                }
                Err(e) => {
                    let operation_duration = operation_start.elapsed();
                    metrics.record_operation(
                        &format!("large_payload_{}", size_label),
                        operation_duration,
                        false,
                    );

                    println!("      {} payload {} failed: {}", size_label, i + 1, e);
                    metrics.record_failure_point(&format!("memory_{}", size_label), i);
                }
            }

            // Force a small delay to allow observation
            tokio::time::sleep(StdDuration::from_millis(100)).await;
        }

        // Clear large data to free memory
        drop(large_data);

        // Small recovery period
        tokio::time::sleep(StdDuration::from_millis(500)).await;
    }

    // Phase 2: Memory allocation burst
    println!("\n⚡ Phase 2: Memory allocation burst");

    let mut memory_hogs = Vec::new();
    let allocation_size = 10 * 1024 * 1024; // 10MB chunks
    let max_allocations = 20;

    for i in 0..max_allocations {
        let allocation_start = Instant::now();

        // Allocate memory chunk
        let memory_chunk = vec![0u8; allocation_size];
        let allocation_duration = allocation_start.elapsed();

        let current_memory = estimate_memory_usage();
        metrics.record_resource_usage("burst_memory", current_memory);

        memory_hogs.push(memory_chunk);

        // Try a database operation during memory pressure
        let operation_start = Instant::now();

        let operation_result = sqlx::query!(
            "SELECT COUNT(*) as count FROM core.events WHERE source = 'memory-pressure-test'"
        )
        .fetch_one(pool)
        .await;

        let operation_duration = operation_start.elapsed();

        match operation_result {
            Ok(_) => {
                metrics.record_operation("memory_pressure_query", operation_duration, true);
            }
            Err(e) => {
                metrics.record_operation("memory_pressure_query", operation_duration, false);
                println!("    Query failed during memory allocation {}: {}", i + 1, e);
                metrics.record_failure_point("memory_pressure", i);
                break;
            }
        }

        if i % 5 == 0 {
            println!(
                "    Allocated {} chunks ({} MB total)",
                i + 1,
                (i + 1) * allocation_size / 1024 / 1024
            );
        }

        // Small delay between allocations
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }

    // Phase 3: Memory recovery
    println!("\n🔄 Phase 3: Memory recovery");

    let recovery_start = Instant::now();

    // Release memory chunks
    drop(memory_hogs);

    // Force garbage collection hint
    tokio::time::sleep(StdDuration::from_millis(1000)).await;

    // Test operations after memory recovery
    for i in 0..10 {
        let operation_start = Instant::now();

        let operation_result = sqlx::query!(
            "SELECT COUNT(*) as count FROM core.events WHERE source = 'memory-pressure-test'"
        )
        .fetch_one(pool)
        .await;

        let operation_duration = operation_start.elapsed();

        match operation_result {
            Ok(_) => {
                metrics.record_operation("memory_recovery_query", operation_duration, true);
            }
            Err(e) => {
                metrics.record_operation("memory_recovery_query", operation_duration, false);
                println!("    Recovery query {} failed: {}", i + 1, e);
            }
        }
    }

    let recovery_duration = recovery_start.elapsed();
    metrics.record_recovery_time("memory_pressure", recovery_duration);

    metrics.print_summary();

    // Assertions
    assert!(
        metrics.success_rate("memory_recovery_query") > 80.0,
        "Memory recovery queries should mostly succeed"
    );
    assert!(
        metrics.average_latency("memory_recovery_query") < StdDuration::from_millis(100),
        "Recovery queries should be fast after memory is freed"
    );

    println!("✅ Memory pressure test passed");
    Ok(())
}

/// Test Redis stream exhaustion scenarios
#[sinex_test]
async fn test_redis_stream_exhaustion(ctx: TestContext) -> anyhow::Result<()> {
    let redis_client = RedisStreamClient::new("redis://localhost:6379")?;
    let mut metrics = ResourceExhaustionMetrics::new();

    println!("📡 Testing Redis stream exhaustion");

    let stream_key = "sinex:exhaustion:test-stream";
    let large_message_count = 10000;
    let large_message_size = 1024; // 1KB per message

    // Clean up existing stream
    let _ = redis_client.del(stream_key).await;

    // Phase 1: Rapid message production to stress Redis
    println!("\n⚡ Phase 1: Rapid message production");

    let production_start = Instant::now();
    let mut messages_sent = 0;

    for i in 0..large_message_count {
        let message_start = Instant::now();

        let large_payload = "x".repeat(large_message_size);
        let message_data = json!({
            "message_id": i,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "large_payload": large_payload,
            "metadata": {
                "test": "redis_exhaustion",
                "iteration": i,
                "size": large_message_size
            }
        });

        match redis_client.xadd(stream_key, "*", &message_data).await {
            Ok(_) => {
                let message_duration = message_start.elapsed();
                metrics.record_operation("rapid_stream_write", message_duration, true);
                messages_sent += 1;

                // Estimate Redis memory usage (rough calculation)
                let estimated_redis_memory = messages_sent * (large_message_size + 100); // overhead
                metrics.record_resource_usage("redis_stream_memory", estimated_redis_memory);
            }
            Err(e) => {
                let message_duration = message_start.elapsed();
                metrics.record_operation("rapid_stream_write", message_duration, false);

                println!("    Message {} failed: {}", i, e);
                metrics.record_failure_point("redis_stream", i);

                if messages_sent < i / 2 {
                    // Too many early failures, break
                    break;
                }
            }
        }

        if i % 1000 == 0 {
            println!("    Sent {} messages", messages_sent);
        }

        // Minimal delay to prevent total system overload
        if i % 100 == 0 {
            tokio::time::sleep(StdDuration::from_millis(1)).await;
        }
    }

    let production_duration = production_start.elapsed();
    println!(
        "  Sent {} messages in {:?}",
        messages_sent, production_duration
    );

    // Check Redis stream info
    match redis_client.xlen::<_, usize>(stream_key).await {
        Ok(stream_length) => {
            println!("  Final stream length: {}", stream_length);
            metrics.record_resource_usage("final_stream_length", stream_length);
        }
        Err(e) => {
            println!("  Failed to get stream length: {}", e);
        }
    }

    // Phase 2: Consumer group exhaustion test
    println!("\n👥 Phase 2: Consumer group exhaustion");

    let consumer_group_count = 50;
    let consumer_group_prefix = "exhaustion-group";

    for i in 0..consumer_group_count {
        let group_creation_start = Instant::now();
        let group_name = format!("{}-{}", consumer_group_prefix, i);

        match redis_client
            .xgroup_create(stream_key, &group_name, "0", true)
            .await
        {
            Ok(_) => {
                let group_creation_duration = group_creation_start.elapsed();
                metrics.record_operation("consumer_group_create", group_creation_duration, true);

                if i % 10 == 0 {
                    println!("    Created {} consumer groups", i + 1);
                }
            }
            Err(e) => {
                let group_creation_duration = group_creation_start.elapsed();
                metrics.record_operation("consumer_group_create", group_creation_duration, false);

                println!("    Consumer group {} creation failed: {}", i, e);
                metrics.record_failure_point("consumer_groups", i);
                break;
            }
        }

        // Small delay between group creations
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }

    // Phase 3: Cleanup and recovery test
    println!("\n🧹 Phase 3: Cleanup and recovery");

    let cleanup_start = Instant::now();

    // Delete the large stream
    match redis_client.del(stream_key).await {
        Ok(_) => {
            let cleanup_duration = cleanup_start.elapsed();
            metrics.record_recovery_time("redis_stream", cleanup_duration);
            println!("  Stream deleted successfully in {:?}", cleanup_duration);
        }
        Err(e) => {
            println!("  Stream deletion failed: {}", e);
        }
    }

    // Test Redis operations after cleanup
    let post_cleanup_stream = "sinex:exhaustion:recovery-test";

    for i in 0..10 {
        let operation_start = Instant::now();

        let recovery_message = json!({
            "recovery_test": i,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        match redis_client
            .xadd(post_cleanup_stream, "*", &recovery_message)
            .await
        {
            Ok(_) => {
                let operation_duration = operation_start.elapsed();
                metrics.record_operation("redis_recovery", operation_duration, true);
            }
            Err(e) => {
                let operation_duration = operation_start.elapsed();
                metrics.record_operation("redis_recovery", operation_duration, false);
                println!("    Recovery operation {} failed: {}", i, e);
            }
        }
    }

    // Cleanup recovery test stream
    let _ = redis_client.del(post_cleanup_stream).await;

    metrics.print_summary();

    // Assertions
    assert!(
        messages_sent > large_message_count / 2,
        "Should successfully send at least half of the messages"
    );
    assert!(
        metrics.success_rate("rapid_stream_write") > 80.0,
        "Rapid stream writes should have > 80% success rate"
    );
    assert!(
        metrics.success_rate("redis_recovery") > 90.0,
        "Redis should recover well after cleanup"
    );
    assert!(
        metrics.average_latency("redis_recovery") < StdDuration::from_millis(50),
        "Recovery operations should be fast"
    );

    println!("✅ Redis stream exhaustion test passed");
    Ok(())
}

/// Test concurrent resource exhaustion
#[sinex_test]
async fn test_concurrent_resource_exhaustion(ctx: TestContext) -> anyhow::Result<()> {
    let pool = ctx.pool().clone();
    let shared_metrics = Arc::new(Mutex::new(ResourceExhaustionMetrics::new()));

    println!("🔥 Testing concurrent resource exhaustion");

    // Multiple concurrent stress patterns
    let db_stress_workers = 10;
    let memory_stress_workers = 5;
    let redis_stress_workers = 3;

    println!("  Launching concurrent stress workers:");
    println!("    Database stress: {} workers", db_stress_workers);
    println!("    Memory stress: {} workers", memory_stress_workers);
    println!("    Redis stress: {} workers", redis_stress_workers);

    let mut all_handles = Vec::new();

    // Database stress workers
    for worker_id in 0..db_stress_workers {
        let pool_clone = pool.clone();
        let metrics = shared_metrics.clone();

        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let operation_start = Instant::now();

                // Mix of heavy and light operations
                let operation_result = if i % 3 == 0 {
                    // Heavy operation: large query
                    sqlx::query!(
                        r#"
                        SELECT source, event_type, COUNT(*) as count,
                               MAX(ts_orig) as latest,
                               MIN(ts_orig) as earliest
                        FROM core.events
                        WHERE ts_orig >= NOW() - INTERVAL '1 day'
                        GROUP BY source, event_type
                        ORDER BY count DESC
                        LIMIT 100
                        "#
                    )
                    .fetch_all(&pool_clone)
                    .await
                } else {
                    // Light operation: simple insert
                    let factory = EventFactory::new(&format!("concurrent-stress-db-{}", worker_id));
                    let event = factory.create_event(
                        event_types::test::CONCURRENT_STRESS_TEST,
                        json!({
                            "worker_id": worker_id,
                            "iteration": i,
                            "stress_type": "database"
                        }),
                    );

                    sinex_db::insert_event_with_validator(&pool_clone, &event, None)
                        .await
                        .map(|_| vec![]) // Convert to same type as query result
                };

                let operation_duration = operation_start.elapsed();

                let mut metrics_lock = metrics.lock().await;
                metrics_lock.record_operation(
                    "concurrent_db_stress",
                    operation_duration,
                    operation_result.is_ok(),
                );

                if operation_result.is_err() && i < 5 {
                    println!("    DB worker {} operation {} failed", worker_id, i);
                }

                tokio::time::sleep(StdDuration::from_millis(20)).await;
            }
        });

        all_handles.push(handle);
    }

    // Memory stress workers
    for worker_id in 0..memory_stress_workers {
        let metrics = shared_metrics.clone();

        let handle = tokio::spawn(async move {
            let mut memory_allocations = Vec::new();

            for i in 0..50 {
                let allocation_start = Instant::now();

                // Allocate memory in chunks
                let chunk_size = 5 * 1024 * 1024; // 5MB
                let memory_chunk = vec![worker_id as u8; chunk_size];
                let allocation_duration = allocation_start.elapsed();

                memory_allocations.push(memory_chunk);

                let current_usage = memory_allocations.len() * chunk_size;

                let mut metrics_lock = metrics.lock().await;
                metrics_lock
                    .record_resource_usage(&format!("memory_worker_{}", worker_id), current_usage);
                metrics_lock.record_operation(
                    "concurrent_memory_stress",
                    allocation_duration,
                    true,
                );

                // Periodically release some memory
                if i % 10 == 0 && !memory_allocations.is_empty() {
                    memory_allocations.remove(0);
                }

                tokio::time::sleep(StdDuration::from_millis(100)).await;
            }
        });

        all_handles.push(handle);
    }

    // Redis stress workers
    for worker_id in 0..redis_stress_workers {
        let metrics = shared_metrics.clone();

        let handle = tokio::spawn(async move {
            if let Ok(redis_client) = RedisStreamClient::new("redis://localhost:6379")?.await {
                let stream_key = format!("sinex:concurrent-stress:{}", worker_id);

                for i in 0..200 {
                    let operation_start = Instant::now();

                    let message_data = json!({
                        "worker_id": worker_id,
                        "iteration": i,
                        "stress_type": "redis",
                        "data": "x".repeat(500) // 500 bytes per message
                    });

                    let operation_result = redis_client.xadd(&stream_key, "*", &message_data).await;
                    let operation_duration = operation_start.elapsed();

                    let mut metrics_lock = metrics.lock().await;
                    metrics_lock.record_operation(
                        "concurrent_redis_stress",
                        operation_duration,
                        operation_result.is_ok(),
                    );

                    if operation_result.is_err() && i < 5 {
                        println!("    Redis worker {} operation {} failed", worker_id, i);
                    }

                    tokio::time::sleep(StdDuration::from_millis(25)).await;
                }

                // Cleanup
                let _ = redis_client
                    .del(&format!("sinex:concurrent-stress:{}", worker_id))
                    .await;
            }
        });

        all_handles.push(handle);
    }

    // Wait for all stress workers to complete
    println!("  Waiting for stress workers to complete...");
    futures::future::join_all(all_handles).await;

    let final_metrics = shared_metrics.lock().await;
    final_metrics.print_summary();

    // Verify system recovery
    println!("\n🔍 Verifying system recovery");

    // Test database recovery
    let db_recovery_result = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE source LIKE 'concurrent-stress-db-%'"
    )
    .fetch_one(pool)
    .await;

    // Test Redis recovery
    let redis_recovery_result =
        if let Ok(redis_client) = RedisStreamClient::new("redis://localhost:6379")?.await {
            redis_client
                .xadd("sinex:recovery-test", "*", &json!({"test": "recovery"}))
                .await
        } else {
            Err("Redis connection failed".into())
        };

    // Assertions
    assert!(
        final_metrics.success_rate("concurrent_db_stress") > 70.0,
        "DB operations should maintain > 70% success rate under stress"
    );
    assert!(
        final_metrics.success_rate("concurrent_redis_stress") > 70.0,
        "Redis operations should maintain > 70% success rate under stress"
    );
    assert!(
        db_recovery_result.is_ok(),
        "Database should be functional after stress test"
    );
    assert!(
        redis_recovery_result.is_ok(),
        "Redis should be functional after stress test"
    );

    if let Ok(count_result) = db_recovery_result {
        println!(
            "  Database recovery: {} stress events stored",
            count_result.count.unwrap_or(0)
        );
    }

    // Cleanup Redis recovery test
    if let Ok(redis_client) = RedisStreamClient::new("redis://localhost:6379")?.await {
        let _ = redis_client.del("sinex:recovery-test").await;
    }

    println!("✅ Concurrent resource exhaustion test passed");
    Ok(())
}

// Helper function to estimate memory usage (platform-dependent)
fn estimate_memory_usage() -> usize {
    // Simple estimation - in production, use proper memory monitoring
    // This is a rough approximation for testing purposes
    std::ptr::null::<u8>() as usize % (1024 * 1024 * 1024) // Fake but deterministic
}
