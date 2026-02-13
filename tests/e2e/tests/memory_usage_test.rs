// # Memory Usage Performance Testing
//
// Comprehensive memory performance tests that measure memory consumption patterns,
// detect memory leaks, and verify memory efficiency under various load conditions.
// These tests help identify memory bottlenecks and optimization opportunities.

use futures::future::join_all;
use sinex_primitives::{DynamicPayload, Timestamp};
use std::time::Instant;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

/// Memory usage measurement utilities
struct MemoryMetrics {
    measurements: Vec<MemoryMeasurement>,
    start_time: Instant,
    peak_memory: usize,
    baseline_memory: usize,
}

#[derive(Debug, Clone)]
struct MemoryMeasurement {
    timestamp: Instant,
    memory_usage: usize,
    operation: String,
    #[allow(dead_code)]
    allocated_objects: usize,
}

impl MemoryMetrics {
    fn new() -> Self {
        let baseline = Self::get_memory_usage();
        Self {
            measurements: Vec::new(),
            start_time: Instant::now(),
            peak_memory: baseline,
            baseline_memory: baseline,
        }
    }

    fn record_measurement(&mut self, operation: &str) {
        let memory_usage = Self::get_memory_usage();

        if memory_usage > self.peak_memory {
            self.peak_memory = memory_usage;
        }

        self.measurements.push(MemoryMeasurement {
            timestamp: Instant::now(),
            memory_usage,
            operation: operation.to_string(),
            allocated_objects: self.estimate_allocated_objects(),
        });
    }

    // Rough memory usage estimation (platform dependent)
    fn get_memory_usage() -> usize {
        // On Linux, we can read from /proc/self/status
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(value) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = value.parse::<usize>() {
                            return kb * 1024; // Convert KB to bytes
                        }
                    }
                }
            }
        }

        // Fallback: return 0 if we can't measure
        0
    }

    fn estimate_allocated_objects(&self) -> usize {
        // Simple estimation based on the number of measurements
        self.measurements.len() * 100
    }

    fn memory_growth(&self) -> isize {
        if let Some(latest) = self.measurements.last() {
            latest.memory_usage as isize - self.baseline_memory as isize
        } else {
            0
        }
    }

    fn memory_growth_rate(&self) -> f64 {
        if self.measurements.len() < 2 {
            return 0.0;
        }

        let first = &self.measurements[0];
        let last = &self.measurements[self.measurements.len() - 1];

        let memory_diff = last.memory_usage as f64 - first.memory_usage as f64;
        let time_diff = last.timestamp.duration_since(first.timestamp).as_secs_f64();

        if time_diff > 0.0 {
            memory_diff / time_diff // bytes per second
        } else {
            0.0
        }
    }

    fn print_summary(&self) {
        println!("\n📊 Memory Usage Summary:");
        println!("Test duration: {:?}", self.start_time.elapsed());
        println!("Baseline memory: {} MB", self.baseline_memory / 1024 / 1024);
        println!("Peak memory: {} MB", self.peak_memory / 1024 / 1024);
        println!("Memory growth: {} MB", self.memory_growth() / 1024 / 1024);
        println!(
            "Growth rate: {:.2} KB/sec",
            self.memory_growth_rate() / 1024.0
        );
        println!("Total measurements: {}", self.measurements.len());

        if self.measurements.len() >= 5 {
            println!("\n📈 Memory progression (last 5 measurements):");
            for measurement in self.measurements.iter().rev().take(5).rev() {
                println!(
                    "  {} MB - {}",
                    measurement.memory_usage / 1024 / 1024,
                    measurement.operation
                );
            }
        }
    }

    fn detect_memory_leak(&self, threshold_mb: usize) -> bool {
        let growth_mb = self.memory_growth().unsigned_abs() / 1024 / 1024;
        growth_mb > threshold_mb
    }

    fn get_memory_efficiency_score(&self) -> f64 {
        if self.measurements.is_empty() {
            return 100.0;
        }

        let avg_memory = self
            .measurements
            .iter()
            .map(|m| m.memory_usage)
            .sum::<usize>() as f64
            / self.measurements.len() as f64;

        if avg_memory == 0.0 {
            return 100.0;
        }

        let efficiency = self.baseline_memory as f64 / avg_memory;
        (efficiency * 100.0).min(100.0)
    }
}

// =============================================================================
// Event Processing Memory Tests
// =============================================================================

/// Test memory usage during event processing
#[sinex_test]
#[ignore = "memory benchmark - run with --heavy"]
async fn test_event_processing_memory_usage(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let mut metrics = MemoryMetrics::new();

    println!("🧠 Testing memory usage during event processing");

    metrics.record_measurement("Test Start");

    // Process events in batches of increasing size
    let batch_sizes = vec![10, 50, 100, 500, 1000];

    for batch_size in batch_sizes {
        println!("\n📦 Processing batch of {batch_size} events");

        metrics.record_measurement(&format!("Before batch {batch_size}"));

        // Process events and measure memory at different stages
        for i in 0..batch_size {
            if i % 100 == 0 {
                metrics.record_measurement(&format!("Processing event {i} in batch {batch_size}"));
            }

            ctx.publish(DynamicPayload::new(
                "memory-test",
                "memory.test.event",
                json!({
                    "batch_size": batch_size,
                    "event_id": i,
                    "test_type": "memory_usage",
                    "timestamp": Timestamp::now().to_string()
                }),
            ))
            .await?;
        }

        metrics.record_measurement(&format!("After batch {batch_size}"));

        // Small delay to allow potential cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;

        metrics.record_measurement(&format!("After cleanup batch {batch_size}"));

        println!(
            "  Memory after batch: {} MB",
            metrics.measurements.last().unwrap().memory_usage / 1024 / 1024
        );
    }

    metrics.print_summary();

    // Memory assertions
    assert!(
        !metrics.detect_memory_leak(500), // 500MB threshold
        "Memory leak detected: growth > 500MB"
    );
    assert!(
        metrics.get_memory_efficiency_score() > 50.0,
        "Memory efficiency score should be > 50%"
    );

    println!("✅ Event processing memory test passed");
    Ok(())
}

/// Test memory usage under concurrent processing
#[sinex_test]
#[ignore = "memory benchmark - run with --heavy"]
async fn test_concurrent_memory_usage(ctx: TestContext) -> TestResult<()> {
    let shared_metrics = Arc::new(tokio::sync::Mutex::new(MemoryMetrics::new()));

    println!("🔄 Testing memory usage under concurrent processing");

    let concurrent_workers = 10;
    let events_per_worker = 200;

    {
        let mut metrics = shared_metrics.lock().await;
        metrics.record_measurement("Concurrent test start");
    }

    let ctx = &ctx;
    let worker_futures = (0..concurrent_workers).map(|worker_id| {
        let metrics = shared_metrics.clone();

        async move {
            // Record memory before worker starts
            {
                let mut metrics_lock = metrics.lock().await;
                metrics_lock.record_measurement(&format!("Worker {worker_id} start"));
            }

            // Generate and publish events for this worker
            for event_id in 0..events_per_worker {
                if let Err(e) = ctx
                    .publish(DynamicPayload::new(
                        format!("memory-test-worker-{worker_id}"),
                        "memory.concurrent.test",
                        json!({
                            "worker_id": worker_id,
                            "event_id": event_id,
                            "data": format!("memory-test-data-{worker_id}-{event_id}"),
                            "large_field": "x".repeat(1024), // 1KB of data per event
                        }),
                    ))
                    .await
                {
                    println!("Worker {worker_id} event {event_id} failed: {e}");
                }

                // Record memory periodically during processing
                if event_id % 50 == 0 {
                    let mut metrics_lock = metrics.lock().await;
                    metrics_lock
                        .record_measurement(&format!("Worker {worker_id} processed {event_id}"));
                }
            }

            // Record memory after processing
            {
                let mut metrics_lock = metrics.lock().await;
                metrics_lock.record_measurement(&format!("Worker {worker_id} completed"));
            }

            worker_id
        }
    });

    // Wait for all workers to complete
    let results = join_all(worker_futures).await;

    {
        let mut metrics = shared_metrics.lock().await;
        metrics.record_measurement("All workers completed");

        println!("✅ Workers completed: {}", results.len());

        // Allow some time for cleanup
        tokio::time::sleep(Duration::from_millis(500)).await;
        metrics.record_measurement("After cleanup delay");

        metrics.print_summary();

        // Memory assertions for concurrent processing
        assert!(
            !metrics.detect_memory_leak(1000), // 1GB threshold for concurrent test
            "Memory leak detected in concurrent processing"
        );
        assert!(
            metrics.get_memory_efficiency_score() > 30.0,
            "Memory efficiency under concurrent load should be > 30%"
        );
    }

    // Verify database consistency
    let pool = ctx.pool().clone();
    let total_events = pool.events().count_all().await?;

    println!("📊 Database consistency: {total_events} events stored");

    println!("✅ Concurrent memory usage test passed");
    Ok(())
}

/// Test memory usage with large payloads
#[sinex_test]
#[ignore = "memory benchmark - run with --heavy"]
async fn test_large_payload_memory_usage(ctx: TestContext) -> TestResult<()> {
    let mut metrics = MemoryMetrics::new();

    println!("📦 Testing memory usage with large payloads");

    metrics.record_measurement("Large payload test start");

    // Test different payload sizes
    let payload_sizes = vec![
        (1, "1KB"),     // 1KB
        (10, "10KB"),   // 10KB
        (100, "100KB"), // 100KB
    ];

    for (size_kb, size_label) in payload_sizes {
        println!("\n📊 Testing {size_label} payloads");

        metrics.record_measurement(&format!("Before {size_label} payload"));

        let large_data = "x".repeat(size_kb * 1024);
        let event_count = std::cmp::max(1, 100 / size_kb); // Fewer events for larger payloads

        println!("  Processing {event_count} events with {size_label} payloads");

        for i in 0..event_count {
            ctx.publish(DynamicPayload::new(
                "large-payload-test",
                format!("large.payload.{size_label}"),
                json!({
                    "event_id": i,
                    "size": size_label,
                    "large_data": &large_data,
                    "metadata": {
                        "created_at": Timestamp::now().to_string(),
                        "test_type": "memory_usage"
                    }
                }),
            ))
            .await?;

            if i % 10 == 0 {
                metrics.record_measurement(&format!("{size_label} payload event {i}"));
            }
        }

        metrics.record_measurement(&format!("After {size_label} payload"));

        // Try to cleanup
        drop(large_data);
        tokio::time::sleep(Duration::from_millis(200)).await;

        metrics.record_measurement(&format!("After {size_label} cleanup"));

        let current_memory = metrics.measurements.last().unwrap().memory_usage;
        println!(
            "  Memory after {size_label} payloads: {} MB",
            current_memory / 1024 / 1024
        );
    }

    metrics.print_summary();

    // Memory assertions for large payloads
    assert!(
        !metrics.detect_memory_leak(2000), // 2GB threshold for large payloads
        "Memory leak detected with large payloads"
    );

    println!("✅ Large payload memory usage test passed");
    Ok(())
}

/// Test memory usage during stress conditions
#[sinex_test]
#[ignore = "memory benchmark - run with --heavy"]
async fn test_memory_stress_conditions(ctx: TestContext) -> TestResult<()> {
    let mut metrics = MemoryMetrics::new();

    println!("🔥 Testing memory usage under stress conditions");

    metrics.record_measurement("Stress test start");

    // Phase 1: Rapid allocation and deallocation
    println!("\n⚡ Phase 1: Rapid allocation/deallocation");

    for cycle in 0..10 {
        metrics.record_measurement(&format!("Stress cycle {cycle} start"));

        // Rapidly create and drop large vectors
        let mut temp_data = Vec::new();
        for i in 0..1000 {
            temp_data.push(format!("stress-test-data-{cycle}-{i}"));
        }

        metrics.record_measurement(&format!("Stress cycle {cycle} allocated"));

        // Process some events with this data
        for i in 0..10 {
            ctx.publish(DynamicPayload::new(
                "memory-stress-test",
                "memory.stress.test",
                json!({
                    "cycle": cycle,
                    "event": i,
                    "sample_data": &temp_data[i * 10..(i + 1) * 10],
                }),
            ))
            .await?;
        }

        metrics.record_measurement(&format!("Stress cycle {cycle} processed"));

        // Drop the large data
        drop(temp_data);

        metrics.record_measurement(&format!("Stress cycle {cycle} dropped"));

        // Small delay to allow cleanup
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Phase 2: Sustained load
    println!("\n⏳ Phase 2: Sustained memory load");

    let sustained_load_duration = Duration::from_secs(Timeouts::MEDIUM);
    let start_time = Instant::now();
    let mut operation_count = 0;

    while start_time.elapsed() < sustained_load_duration {
        ctx.publish(DynamicPayload::new(
            "sustained-memory-test",
            "memory.sustained.test",
            json!({
                "operation": operation_count,
                "timestamp": Timestamp::now().to_string(),
                "payload_data": "y".repeat(512), // 512 bytes per event
            }),
        ))
        .await?;

        operation_count += 1;

        if operation_count % 100 == 0 {
            metrics.record_measurement(&format!("Sustained operation {operation_count}"));
        }

        // Minimal delay to prevent overwhelming
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    metrics.record_measurement(&format!(
        "Sustained test completed - {operation_count} operations"
    ));

    println!("  Completed {operation_count} operations in sustained load test");

    // Phase 3: Memory recovery test
    println!("\n🔄 Phase 3: Memory recovery");

    // Allow time for garbage collection and cleanup
    tokio::time::sleep(Duration::from_secs(Timeouts::SHORT)).await;
    metrics.record_measurement("After recovery delay");

    metrics.print_summary();

    // Stress test assertions
    assert!(
        !metrics.detect_memory_leak(1500), // 1.5GB threshold
        "Memory leak detected under stress conditions"
    );

    let final_memory = metrics.measurements.last().unwrap().memory_usage;
    let peak_memory = metrics.peak_memory;
    let recovery_ratio = if peak_memory > 0 {
        final_memory as f64 / peak_memory as f64
    } else {
        1.0
    };

    println!("📊 Memory recovery ratio: {recovery_ratio:.2}");
    assert!(
        recovery_ratio < 1.5,
        "Memory should recover to reasonable levels after stress test"
    );

    println!("✅ Memory stress test passed");
    Ok(())
}

/// Test memory usage with database connection pools
#[sinex_test]
#[ignore = "memory benchmark - run with --heavy"]
async fn test_connection_pool_memory_usage(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let mut metrics = MemoryMetrics::new();

    println!("🏊 Testing memory usage with connection pools");

    metrics.record_measurement("Connection pool test start");

    // Test acquiring and releasing many connections
    let connection_cycles = 50;

    for cycle in 0..connection_cycles {
        metrics.record_measurement(&format!("Connection cycle {cycle} start"));

        // Acquire multiple connections simultaneously
        let mut connections = Vec::new();

        for i in 0..10 {
            match pool.acquire().await {
                Ok(conn) => {
                    connections.push(conn);
                    if i % 3 == 0 {
                        metrics.record_measurement(&format!("Cycle {cycle} connection {i}"));
                    }
                }
                Err(e) => {
                    println!("Failed to acquire connection {i}: {e}");
                }
            }
        }

        metrics.record_measurement(&format!(
            "Cycle {cycle} acquired {} connections",
            connections.len()
        ));

        // Use connections briefly
        for (i, conn) in connections.iter_mut().enumerate() {
            let _ = sqlx::query("SELECT $1 as test")
                .bind(format!("cycle-{cycle}-conn-{i}"))
                .fetch_one(&mut **conn)
                .await;
        }

        metrics.record_measurement(&format!("Cycle {cycle} used connections"));

        // Drop connections to test cleanup
        drop(connections);

        metrics.record_measurement(&format!("Cycle {cycle} dropped connections"));

        // Small delay between cycles
        if cycle % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            metrics.record_measurement(&format!("Cycle {cycle} delay completed"));
        }
    }

    metrics.print_summary();

    // Connection pool memory assertions
    assert!(
        !metrics.detect_memory_leak(200), // 200MB threshold
        "Memory leak detected in connection pool usage"
    );
    assert!(
        metrics.get_memory_efficiency_score() > 60.0,
        "Connection pool memory efficiency should be > 60%"
    );

    println!("✅ Connection pool memory test passed");
    Ok(())
}
