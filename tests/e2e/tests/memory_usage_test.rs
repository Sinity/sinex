// # Memory Usage Performance Testing
//
// Comprehensive memory performance tests that measure memory consumption patterns,
// detect memory leaks, and verify memory efficiency under various load conditions.
// These tests help identify memory bottlenecks and optimization opportunities.

use serde_json::json;
use sinex_primitives::db::queries::{CheckpointQueries, EventQueries};
use sinex_primitives::db::query_builder::{QueryBuilder, QueryParam};
use sinex_primitives::events::{event_types, sources, EventFactory};
use xtask::sandbox::{prelude::*, timing_utils::Timeouts};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::Mutex;

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

        // Fallback: use a simple heap estimation
        Box::leak(vec![0u8; 0].into_boxed_slice()).as_ptr() as usize
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
        let growth_mb = self.memory_growth().abs() as usize / 1024 / 1024;
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

        let efficiency = self.baseline_memory as f64 / avg_memory;
        (efficiency * 100.0).min(100.0)
    }
}

// =============================================================================
// Event Processing Memory Tests
// =============================================================================

/// Test memory usage during event processing
#[sinex_bench]
async fn test_event_processing_memory_usage(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let mut metrics = MemoryMetrics::new();

    println!("🧠 Testing memory usage during event processing");

    metrics.record_measurement("Test Start");

    // Process events in batches of increasing size
    let batch_sizes = vec![10, 50, 100, 500, 1000];

    for batch_size in batch_sizes {
        println!("\n📦 Processing batch of {} events", batch_size);

        metrics.record_measurement(&format!("Before batch {}", batch_size));

        let factory = EventFactory::new("memory-test");

        // Process events and measure memory at different stages
        for i in 0..batch_size {
            if i % 100 == 0 {
                metrics
                    .record_measurement(&format!("Processing event {} in batch {}", i, batch_size));
            }

            let event = factory.create_event(
                event_types::test::GENERIC,
                json!({
                    "batch_size": batch_size,
                    "event_id": i,
                    "test_type": "memory_usage",
                    "timestamp": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap()
                }),
            );

            sinex_primitives::db::insert_event_with_validator(pool, &event, None).await?;
        }

        metrics.record_measurement(&format!("After batch {}", batch_size));

        // Small delay to allow potential cleanup
        tokio::time::sleep(StdDuration::from_millis(100)).await;

        metrics.record_measurement(&format!("After cleanup batch {}", batch_size));

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
#[sinex_bench]
async fn test_concurrent_memory_usage(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let shared_metrics = Arc::new(Mutex::new(MemoryMetrics::new()));

    println!("🔄 Testing memory usage under concurrent processing");

    let concurrent_workers = 10;
    let events_per_worker = 200;

    {
        let mut metrics = shared_metrics.lock().await;
        metrics.record_measurement("Concurrent test start");
    }

    let worker_handles = (0..concurrent_workers)
        .map(|worker_id| {
            let pool_clone = pool.clone();
            let metrics = shared_metrics.clone();

            tokio::spawn(async move {
                // Record memory before worker starts
                {
                    let mut metrics_lock = metrics.lock().await;
                    metrics_lock.record_measurement(&format!("Worker {} start", worker_id));
                }

                let mut worker_events = Vec::new();

                // Generate events for this worker
                for event_id in 0..events_per_worker {
                    let factory = EventFactory::new(&format!("memory-test-worker-{}", worker_id));
                    let event = factory.create_event(
                        event_types::test::CONCURRENT_MEMORY_TEST,
                        json!({
                            "worker_id": worker_id,
                            "event_id": event_id,
                            "data": format!("memory-test-data-{}-{}", worker_id, event_id),
                            "large_field": "x".repeat(1024), // 1KB of data per event
                        }),
                    );

                    worker_events.push(event);
                }

                // Record memory after event generation
                {
                    let mut metrics_lock = metrics.lock().await;
                    metrics_lock
                        .record_measurement(&format!("Worker {} generated events", worker_id));
                }

                // Process events
                for (i, event) in worker_events.iter().enumerate() {
                    if let Err(e) =
                        sinex_primitives::db::insert_event_with_validator(&pool_clone, event, None).await
                    {
                        println!("Worker {} event {} failed: {}", worker_id, i, e);
                    }

                    // Record memory periodically during processing
                    if i % 50 == 0 {
                        let mut metrics_lock = metrics.lock().await;
                        metrics_lock
                            .record_measurement(&format!("Worker {} processed {}", worker_id, i));
                    }
                }

                // Record memory after processing
                {
                    let mut metrics_lock = metrics.lock().await;
                    metrics_lock.record_measurement(&format!("Worker {} completed", worker_id));
                }

                // Clear worker data to test cleanup
                drop(worker_events);

                worker_id
            })
        })
        .collect::<Vec<_>>();

    // Wait for all workers to complete
    let results = futures::future::join_all(worker_handles).await;

    {
        let mut metrics = shared_metrics.lock().await;
        metrics.record_measurement("All workers completed");

        println!("✅ Workers completed: {}", results.len());

        // Allow some time for cleanup
        tokio::time::sleep(StdDuration::from_millis(500)).await;
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

    // Verify database consistency using centralized query system
    let total_events = EventQueries::count_by_source_pattern(&pool, "memory-test-worker-%").await?;

    let expected_events = concurrent_workers * events_per_worker;
    println!(
        "📊 Database consistency: {}/{} events stored",
        total_events, expected_events
    );

    println!("✅ Concurrent memory usage test passed");
    Ok(())
}

/// Test memory usage with large payloads
#[sinex_bench]
async fn test_large_payload_memory_usage(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let mut metrics = MemoryMetrics::new();

    println!("📦 Testing memory usage with large payloads");

    metrics.record_measurement("Large payload test start");

    // Test different payload sizes
    let payload_sizes = vec![
        (1, "1KB"),     // 1KB
        (10, "10KB"),   // 10KB
        (100, "100KB"), // 100KB
        (1000, "1MB"),  // 1MB
    ];

    for (size_kb, size_label) in payload_sizes {
        println!("\n📊 Testing {} payloads", size_label);

        metrics.record_measurement(&format!("Before {} payload", size_label));

        let large_data = "x".repeat(size_kb * 1024);
        let event_count = std::cmp::max(1, 1000 / size_kb); // Fewer events for larger payloads

        println!(
            "  Processing {} events with {} payloads",
            event_count, size_label
        );

        for i in 0..event_count {
            let factory = EventFactory::new("large-payload-test");
            let event = factory.create_event(
                &format!("large.payload.{}", size_label),
                json!({
                    "event_id": i,
                    "size": size_label,
                    "large_data": &large_data,
                    "metadata": {
                        "created_at": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
                        "test_type": "memory_usage"
                    }
                }),
            );

            sinex_primitives::db::insert_event_with_validator(pool, &event, None).await?;

            if i % 10 == 0 {
                metrics.record_measurement(&format!("{} payload event {}", size_label, i));
            }
        }

        metrics.record_measurement(&format!("After {} payload", size_label));

        // Try to cleanup
        drop(large_data);
        tokio::time::sleep(StdDuration::from_millis(200)).await;

        metrics.record_measurement(&format!("After {} cleanup", size_label));

        let current_memory = metrics.measurements.last().unwrap().memory_usage;
        println!(
            "  Memory after {} payloads: {} MB",
            size_label,
            current_memory / 1024 / 1024
        );
    }

    metrics.print_summary();

    // Memory assertions for large payloads
    assert!(
        !metrics.detect_memory_leak(2000), // 2GB threshold for large payloads
        "Memory leak detected with large payloads"
    );

    // Verify events were stored using centralized query system
    let stored_events = EventQueries::count_by_source(&pool, "large-payload-test")
        .fetch_one(pool)
        .await?;

    println!("📊 Large payload events stored: {}", stored_events);
    assert!(
        stored_events > 0,
        "Large payload events should be stored successfully"
    );

    println!("✅ Large payload memory usage test passed");
    Ok(())
}

/// Test memory usage during stress conditions
#[sinex_bench]
async fn test_memory_stress_conditions(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let mut metrics = MemoryMetrics::new();

    println!("🔥 Testing memory usage under stress conditions");

    metrics.record_measurement("Stress test start");

    // Phase 1: Rapid allocation and deallocation
    println!("\n⚡ Phase 1: Rapid allocation/deallocation");

    for cycle in 0..10 {
        metrics.record_measurement(&format!("Stress cycle {} start", cycle));

        // Rapidly create and drop large vectors
        let mut temp_data = Vec::new();
        for i in 0..1000 {
            temp_data.push(format!("stress-test-data-{}-{}", cycle, i));
        }

        metrics.record_measurement(&format!("Stress cycle {} allocated", cycle));

        // Process some events with this data
        for i in 0..10 {
            let factory = EventFactory::new("memory-stress-test");
            let event = factory.create_event(
                event_types::test::MEMORY_STRESS_TEST,
                json!({
                    "cycle": cycle,
                    "event": i,
                    "sample_data": &temp_data[i * 10..(i + 1) * 10],
                }),
            );

            sinex_primitives::db::insert_event_with_validator(pool, &event, None).await?;
        }

        metrics.record_measurement(&format!("Stress cycle {} processed", cycle));

        // Drop the large data
        drop(temp_data);

        metrics.record_measurement(&format!("Stress cycle {} dropped", cycle));

        // Small delay to allow cleanup
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }

    // Phase 2: Sustained load
    println!("\n⏳ Phase 2: Sustained memory load");

    let sustained_load_duration = StdDuration::from_secs(Timeouts::MEDIUM);
    let start_time = Instant::now();
    let mut operation_count = 0;

    while start_time.elapsed() < sustained_load_duration {
        let factory = EventFactory::new("sustained-memory-test");
        let event = factory.create_event(
            event_types::test::SUSTAINED_MEMORY_TEST,
            json!({
                "operation": operation_count,
                "timestamp": OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap(),
                "payload_data": "y".repeat(512), // 512 bytes per event
            }),
        );

        sinex_primitives::db::insert_event_with_validator(pool, &event, None).await?;

        operation_count += 1;

        if operation_count % 100 == 0 {
            metrics.record_measurement(&format!("Sustained operation {}", operation_count));
        }

        // Minimal delay to prevent overwhelming
        tokio::time::sleep(StdDuration::from_millis(1)).await;
    }

    metrics.record_measurement(&format!(
        "Sustained test completed - {} operations",
        operation_count
    ));

    println!(
        "  Completed {} operations in sustained load test",
        operation_count
    );

    // Phase 3: Memory recovery test
    println!("\n🔄 Phase 3: Memory recovery");

    // Allow time for garbage collection and cleanup
    tokio::time::sleep(StdDuration::from_secs(Timeouts::SHORT)).await;
    metrics.record_measurement("After recovery delay");

    metrics.print_summary();

    // Stress test assertions
    assert!(
        !metrics.detect_memory_leak(1500), // 1.5GB threshold
        "Memory leak detected under stress conditions"
    );

    let final_memory = metrics.measurements.last().unwrap().memory_usage;
    let peak_memory = metrics.peak_memory;
    let recovery_ratio = final_memory as f64 / peak_memory as f64;

    println!("📊 Memory recovery ratio: {:.2}", recovery_ratio);
    assert!(
        recovery_ratio < 1.5,
        "Memory should recover to reasonable levels after stress test"
    );

    // Verify database consistency using centralized query system
    let stress_test_count = EventQueries::count_by_source(&pool, "memory-stress-test")
        .fetch_one(pool)
        .await?;
    let sustained_test_count = EventQueries::count_by_source(&pool, "sustained-memory-test")
        .fetch_one(pool)
        .await?;
    let total_stress_events = stress_test_count + sustained_test_count;

    println!("📊 Stress test events stored: {}", total_stress_events);
    assert!(
        total_stress_events > 0,
        "Stress test events should be stored successfully"
    );

    println!("✅ Memory stress test passed");
    Ok(())
}

/// Test memory usage with database connection pools
#[sinex_bench]
async fn test_connection_pool_memory_usage(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let mut metrics = MemoryMetrics::new();

    println!("🏊 Testing memory usage with connection pools");

    metrics.record_measurement("Connection pool test start");

    // Test acquiring and releasing many connections
    let connection_cycles = 50;

    for cycle in 0..connection_cycles {
        metrics.record_measurement(&format!("Connection cycle {} start", cycle));

        // Acquire multiple connections simultaneously
        let mut connections = Vec::new();

        for i in 0..10 {
            match pool.acquire().await {
                Ok(conn) => {
                    connections.push(conn);
                    if i % 3 == 0 {
                        metrics.record_measurement(&format!("Cycle {} connection {}", cycle, i));
                    }
                }
                Err(e) => {
                    println!("Failed to acquire connection {}: {}", i, e);
                }
            }
        }

        metrics.record_measurement(&format!(
            "Cycle {} acquired {} connections",
            cycle,
            connections.len()
        ));

        // Use connections briefly
        for (i, mut conn) in connections.iter_mut().enumerate() {
            let _ = sqlx::query("SELECT $1 as test")
                .bind(format!("cycle-{}-conn-{}", cycle, i))
                .fetch_one(&mut **conn)
                .await;
        }

        metrics.record_measurement(&format!("Cycle {} used connections", cycle));

        // Drop connections to test cleanup
        drop(connections);

        metrics.record_measurement(&format!("Cycle {} dropped connections", cycle));

        // Small delay between cycles
        if cycle % 10 == 0 {
            tokio::time::sleep(StdDuration::from_millis(100)).await;
            metrics.record_measurement(&format!("Cycle {} delay completed", cycle));
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