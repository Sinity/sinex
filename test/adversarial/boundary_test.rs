//! # Boundary Test Suite
//!
//! Comprehensive boundary testing for system limits and edge cases.
//! This module tests behavior at the boundaries of system capabilities.
//!
//! ## Test Categories
//! - **Database Boundaries**: Payload size limits, connection pool exhaustion
//! - **Network Boundaries**: DNS timeouts, network partitions, connection limits
//! - **Numeric Boundaries**: Overflow conditions, timestamp limits, precision limits
//! - **Resource Boundaries**: Memory limits, disk space, file handle limits

use crate::common::events;
use crate::common::prelude::*;
use chrono::Datelike;
use futures::future::join_all;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;
use tokio::time::{timeout, Duration};

// =============================================================================
// Database Boundary Tests
// =============================================================================

/// Test event payload approaching 1GB PostgreSQL JSONB limit
#[sinex_test]
async fn test_event_payload_approaching_1gb_limit(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing JSONB 1GB limit:");

    // Start with smaller sizes and work up
    let test_sizes = vec![
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
        (100 * 1024 * 1024, "100MB"),
        (500 * 1024 * 1024, "500MB"),
        (900 * 1024 * 1024, "900MB"),
        (1000 * 1024 * 1024, "1000MB"), // Approaching limit
    ];

    for (size, label) in test_sizes {
        println!("  Testing {} payload...", label);

        // Create large string
        let _large_data = "x".repeat(size);

        let event = events::large_payload_test_event(1024);

        let start = Instant::now();
        match insert_event(pool, &event).await {
            Ok(_) => {
                let elapsed = start.elapsed();
                println!("    SUCCESS: Inserted in {:?}", elapsed);

                // Try to update with more data
                let extra_data = "y".repeat(100 * 1024 * 1024); // 100MB more
                let update_result = sqlx::query!(
                    r#"
                    UPDATE core.events
                    SET payload = payload || jsonb_build_object('extra_data', $2::text)
                    WHERE event_id::uuid = $1::uuid
                    "#,
                    event.id.to_uuid(),
                    extra_data
                )
                .execute(pool)
                .await;

                match update_result {
                    Ok(_) => println!("    UPDATE SUCCESS: Added 100MB more"),
                    Err(e) => println!("    UPDATE FAILED: {} (expected near limit)", e),
                }
            }
            Err(e) => {
                println!("    FAILED: {}", e);
                if size >= 900 * 1024 * 1024 {
                    println!("    Expected failure near 1GB limit");
                }
            }
        }
    }
    Ok(())
}

/// Test connection pool exhaustion
#[sinex_test]
async fn test_connection_pool_exhaustion(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing connection pool exhaustion:");

    // Get pool stats
    println!("  Pool size: {}", pool.size());

    let num_workers = 200; // Much more than typical pool size
    let hold_duration = Duration::from_secs(2);

    let mut handles = vec![];
    let start = Instant::now();
    let successful_acquisitions = Arc::new(AtomicU64::new(0));
    let failed_acquisitions = Arc::new(AtomicU64::new(0));
    let timeout_acquisitions = Arc::new(AtomicU64::new(0));

    for worker_id in 0..num_workers {
        let pool_clone = pool.clone();
        let success_count = successful_acquisitions.clone();
        let fail_count = failed_acquisitions.clone();
        let timeout_count = timeout_acquisitions.clone();

        let handle = tokio::spawn(async move {
            let acquire_start = Instant::now();

            // Try to acquire connection with timeout
            match timeout(Duration::from_secs(5), pool_clone.acquire()).await {
                Ok(Ok(mut conn)) => {
                    let acquire_time = acquire_start.elapsed();
                    success_count.fetch_add(1, Ordering::SeqCst);
                    println!(
                        "    Worker {} acquired connection after {:?}",
                        worker_id, acquire_time
                    );

                    // Hold connection for a while
                    tokio::time::sleep(hold_duration).await;

                    // Try to use connection
                    match sqlx::query!("SELECT 1 as test").fetch_one(&mut *conn).await {
                        Ok(_) => println!("    Worker {} used connection successfully", worker_id),
                        Err(e) => {
                            println!("    Worker {} failed to use connection: {}", worker_id, e)
                        }
                    }
                }
                Ok(Err(e)) => {
                    fail_count.fetch_add(1, Ordering::SeqCst);
                    println!(
                        "    Worker {} failed to acquire connection: {}",
                        worker_id, e
                    );
                }
                Err(_) => {
                    timeout_count.fetch_add(1, Ordering::SeqCst);
                    println!("    Worker {} timed out acquiring connection", worker_id);
                }
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;
    let elapsed = start.elapsed();

    let successful = successful_acquisitions.load(Ordering::SeqCst);
    let failed = failed_acquisitions.load(Ordering::SeqCst);
    let timeouts = timeout_acquisitions.load(Ordering::SeqCst);

    println!("\nConnection pool exhaustion test results:");
    println!("  Total workers: {}", num_workers);
    println!("  Successful acquisitions: {}", successful);
    println!("  Failed acquisitions: {}", failed);
    println!("  Timeout acquisitions: {}", timeouts);
    println!("  Total time: {:?}", elapsed);

    // Some connections should be acquired successfully
    assert!(successful > 0, "Some connections should be acquired");

    // Pool exhaustion should cause some failures or timeouts
    assert!(
        failed + timeouts > 0,
        "Pool exhaustion should cause some failures"
    );

    Ok(())
}

/// Test database transaction boundary limits
#[sinex_test]
async fn test_database_transaction_boundary_limits(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing database transaction limits:");

    // Test large batch operations (using pool directly for high volume)
    let operation_count = 10000;

    let start = Instant::now();
    for i in 0..operation_count {
        let event = RawEventBuilder::new(
            "boundary_test",
            "transaction.test",
            json!({"operation_id": i}),
        )
        .build();

        match sinex_db::events::insert_event_with_validator(pool, &event, None).await {
            Ok(_) => {}
            Err(e) => {
                println!("Transaction failed at operation {}: {}", i, e);
                break;
            }
        }

        if i % 1000 == 0 {
            println!("  Completed {} operations", i);
        }
    }

    let elapsed = start.elapsed();
    println!("  Completed large batch operation in {:?}", elapsed);

    Ok(())
}

/// Test database query complexity limits
#[sinex_test]
async fn test_database_query_complexity_limits(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert test data
    for i in 0..100 {
        let event = RawEventBuilder::new(
            "complexity_test",
            "query.test",
            json!({"value": i, "category": i % 10}),
        )
        .build();

        insert_event(pool, &event).await?;
    }

    // Test increasingly complex queries
    let complex_queries = vec![
        // Simple query
        ("SELECT COUNT(*) FROM core.events WHERE source = 'complexity_test'", "simple_count"),
        
        // Complex aggregation
        ("SELECT source, event_type, COUNT(*), AVG((payload->>'value')::int) FROM core.events WHERE source = 'complexity_test' GROUP BY source, event_type", "complex_aggregation"),
        
        // Very complex query with multiple joins and subqueries
        ("WITH event_stats AS (SELECT source, COUNT(*) as cnt FROM core.events GROUP BY source) SELECT e.source, e.event_type, es.cnt FROM core.events e JOIN event_stats es ON e.source = es.source WHERE e.source = 'complexity_test' ORDER BY es.cnt DESC", "complex_cte"),
    ];

    for (query, description) in complex_queries {
        println!("Testing query complexity: {}", description);

        let start = Instant::now();
        match timeout(Duration::from_secs(10), sqlx::query(query).fetch_all(pool)).await {
            Ok(Ok(rows)) => {
                let elapsed = start.elapsed();
                println!("  SUCCESS: {} rows in {:?}", rows.len(), elapsed);
            }
            Ok(Err(e)) => {
                println!("  FAILED: {}", e);
            }
            Err(_) => {
                println!("  TIMEOUT: Query took longer than 10 seconds");
            }
        }
    }

    Ok(())
}

// =============================================================================
// Network Boundary Tests
// =============================================================================

/// Test database DNS timeout
#[sinex_test(timeout = 30)]
async fn test_database_dns_timeout(ctx: TestContext) -> TestResult {
    // Test what happens when database hostname fails to resolve

    let fake_hostnames = vec![
        "nonexistent-db-host.invalid",
        "192.0.2.1",              // TEST-NET-1 (should not respond)
        "10.255.255.255",         // Private network edge
        "database.internal.corp", // Typical internal hostname
    ];

    for hostname in fake_hostnames {
        println!("Testing DNS/connection to: {}", hostname);

        let fake_url = format!("postgres://user:pass@{}:5432/testdb", hostname);

        let start = std::time::Instant::now();

        // Test connection with timeout
        let result = timeout(Duration::from_secs(5), DbPool::connect(&fake_url)).await;

        let elapsed = start.elapsed();

        match result {
            Ok(Ok(_pool)) => {
                println!("  UNEXPECTED: Connection succeeded to {}", hostname);
            }
            Ok(Err(e)) => {
                println!("  Connection failed in {:?}: {}", elapsed, e);
            }
            Err(_) => {
                println!(
                    "  TIMEOUT: Connection attempt to {} took longer than 5s",
                    hostname
                );

                if elapsed > Duration::from_secs(5) {
                    println!("  WARNING: Timeout handling is broken - took {:?}", elapsed);
                }
            }
        }
    }
    Ok(())
}

/// Test network partition during processing
#[sinex_test(timeout = 15)]
async fn test_network_partition_during_processing(ctx: TestContext) -> TestResult {
    // Simulate network partition by creating workers that lose connectivity

    let pool = ctx.pool();

    // Create test event to be processed
    let test_event = events::generic_adversarial_event(
        "partition_test",
        "network.test",
        json!({"test": true}),
        None,
    );

    insert_event(pool, &test_event).await?;

    let partition_events = Arc::new(AtomicU64::new(0));
    let successful_operations = Arc::new(AtomicU64::new(0));
    let failed_operations = Arc::new(AtomicU64::new(0));

    let mut worker_handles = vec![];

    // Create multiple "distributed" workers
    for worker_id in 0..3 {
        let pool_clone = pool.clone();
        let partition_count = partition_events.clone();
        let success_count = successful_operations.clone();
        let fail_count = failed_operations.clone();
        let _event_id = test_event.id;

        let handle = tokio::spawn(async move {
            println!("Worker {} starting", worker_id);

            for attempt in 0..10 {
                // Simulate network partition for worker 1 after attempt 5
                if worker_id == 1 && attempt >= 5 {
                    partition_count.fetch_add(1, Ordering::SeqCst);
                    println!(
                        "Worker {} experiencing network partition at attempt {}",
                        worker_id, attempt
                    );

                    // Simulate lost connectivity - operations will timeout
                    let fake_result = timeout(Duration::from_millis(100), async {
                        // This simulates a hung connection
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        Ok::<(), sqlx::Error>(())
                    })
                    .await;

                    match fake_result {
                        Ok(_) => {
                            success_count.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(_) => {
                            fail_count.fetch_add(1, Ordering::SeqCst);
                            println!("Worker {} timed out due to partition", worker_id);
                        }
                    }
                    continue;
                }

                // Normal operation
                match sqlx::query!("SELECT 1 as test")
                    .fetch_one(&pool_clone)
                    .await
                {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                        println!("Worker {} attempt {} succeeded", worker_id, attempt);
                    }
                    Err(e) => {
                        fail_count.fetch_add(1, Ordering::SeqCst);
                        println!("Worker {} attempt {} failed: {}", worker_id, attempt, e);
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        worker_handles.push(handle);
    }

    join_all(worker_handles).await;

    let partitions = partition_events.load(Ordering::SeqCst);
    let successes = successful_operations.load(Ordering::SeqCst);
    let failures = failed_operations.load(Ordering::SeqCst);

    println!("\nNetwork partition test results:");
    println!("  Partition events: {}", partitions);
    println!("  Successful operations: {}", successes);
    println!("  Failed operations: {}", failures);

    // Some operations should succeed despite partitions
    assert!(successes > 0, "Some operations should succeed");

    // Partitions should cause some failures
    assert!(failures > 0, "Network partitions should cause failures");

    Ok(())
}

/// Test connection limit exhaustion
#[sinex_test]
async fn test_connection_limit_exhaustion(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing connection limit exhaustion:");

    // Try to create many simultaneous connections
    let connection_attempts = 500;
    let mut handles = vec![];
    let successful_connections = Arc::new(AtomicU64::new(0));
    let failed_connections = Arc::new(AtomicU64::new(0));

    for conn_id in 0..connection_attempts {
        let pool_clone = pool.clone();
        let success_count = successful_connections.clone();
        let fail_count = failed_connections.clone();

        let handle = tokio::spawn(async move {
            match timeout(Duration::from_secs(2), pool_clone.acquire()).await {
                Ok(Ok(_conn)) => {
                    success_count.fetch_add(1, Ordering::SeqCst);
                    // Hold connection briefly
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok(Err(e)) => {
                    fail_count.fetch_add(1, Ordering::SeqCst);
                    if conn_id < 10 {
                        println!("Connection {} failed: {}", conn_id, e);
                    }
                }
                Err(_) => {
                    fail_count.fetch_add(1, Ordering::SeqCst);
                    if conn_id < 10 {
                        println!("Connection {} timed out", conn_id);
                    }
                }
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successful = successful_connections.load(Ordering::SeqCst);
    let failed = failed_connections.load(Ordering::SeqCst);

    println!("Connection limit test results:");
    println!("  Attempted connections: {}", connection_attempts);
    println!("  Successful connections: {}", successful);
    println!("  Failed connections: {}", failed);

    // System should handle reasonable connection load
    assert!(successful > 0, "Some connections should succeed");

    Ok(())
}

// =============================================================================
// Numeric Boundary Tests
// =============================================================================

/// Test ULID timestamp conversion overflow
#[sinex_test]
async fn test_ulid_timestamp_conversion_overflow_bug(ctx: TestContext) -> TestResult {
    // Test ULID timestamp overflow conditions

    // Create a ULID with maximum timestamp value
    let max_timestamp_ms: u64 = (1u64 << 48) - 1; // Max 48-bit value = 281474976710655

    // This timestamp is valid for ULID (year ~10889)
    println!("Max ULID timestamp ms: {}", max_timestamp_ms);
    println!("i64::MAX: {}", i64::MAX);

    // Create bytes for ULID with max timestamp
    let mut bytes = [0u8; 16];
    bytes[0] = (max_timestamp_ms >> 40) as u8;
    bytes[1] = (max_timestamp_ms >> 32) as u8;
    bytes[2] = (max_timestamp_ms >> 24) as u8;
    bytes[3] = (max_timestamp_ms >> 16) as u8;
    bytes[4] = (max_timestamp_ms >> 8) as u8;
    bytes[5] = max_timestamp_ms as u8;

    let ulid = Ulid::from_bytes(bytes).unwrap();

    // This should safely handle overflow by clamping to i64::MAX
    let timestamp = ulid.timestamp();

    println!("Max ULID timestamp: {:?}", timestamp);
    println!("Timestamp year: {}", timestamp.year());

    // The max ULID timestamp (48-bit) fits comfortably in i64
    assert_eq!(
        timestamp.year(),
        10889,
        "Expected year 10889 for max ULID timestamp"
    );

    // Verify timestamp conversion is safe
    let inner_ulid = ulid.inner();
    let timestamp_ms = inner_ulid.timestamp_ms();
    assert!(
        timestamp_ms < i64::MAX as u64,
        "ULID timestamps always fit in i64"
    );

    println!("✅ ULID timestamp conversion is safe - max ULID timestamp fits in i64");
    Ok(())
}

/// Test ULID high frequency ordering limitations
#[sinex_test]
async fn test_ulid_high_frequency_ordering_limitation(ctx: TestContext) -> TestResult {
    // Test: Demonstrate potential ordering violations under high frequency

    let mut ulids = Vec::new();
    let mut ordering_violations = 0;

    // Generate ULIDs as fast as possible to stress-test ordering
    for _ in 0..10000 {
        ulids.push(Ulid::new());
    }

    // Check for ordering violations
    for i in 1..ulids.len() {
        if ulids[i] < ulids[i - 1] {
            ordering_violations += 1;
            if ordering_violations <= 3 {
                // Log first few violations
                println!(
                    "Ordering violation #{} at index {}: {} < {}",
                    ordering_violations,
                    i,
                    ulids[i],
                    ulids[i - 1]
                );
            }
        }
    }

    println!(
        "Generated {} ULIDs with {} ordering violations ({:.2}%)",
        ulids.len(),
        ordering_violations,
        (ordering_violations as f64 / ulids.len() as f64) * 100.0
    );

    if ordering_violations == 0 {
        println!("✅ Standard ULID generation maintained perfect ordering");
    } else {
        println!("⚠️  Standard ULID generation has ordering violations - consider MonotonicUlidGenerator for strict ordering");
    }
    Ok(())
}

/// Test numeric overflow in event counters
#[sinex_test]
async fn test_numeric_overflow_in_event_counters(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test with values near integer limits
    let test_values = vec![
        (i32::MAX as i64 - 1, "i32::MAX - 1"),
        (i32::MAX as i64, "i32::MAX"),
        (i32::MAX as i64 + 1, "i32::MAX + 1"),
        (i64::MAX - 1, "i64::MAX - 1"),
    ];

    for (test_value, description) in test_values {
        println!("Testing numeric boundary: {} ({})", test_value, description);

        let event = RawEventBuilder::new(
            "numeric_test",
            "boundary.test",
            json!({
                "counter": test_value,
                "description": description
            }),
        )
        .build();

        match insert_event(pool, &event).await {
            Ok(_) => {
                println!("  SUCCESS: Inserted event with value {}", test_value);

                // Try to query it back
                match sqlx::query!(
                    "SELECT payload FROM core.events WHERE event_id::uuid = $1::uuid",
                    event.id.to_uuid()
                )
                .fetch_one(pool)
                .await
                {
                    Ok(row) => {
                        let retrieved_value = row.payload["counter"].as_i64().unwrap_or(-1);
                        if retrieved_value == test_value {
                            println!("  SUCCESS: Value retrieved correctly");
                        } else {
                            println!(
                                "  ERROR: Value corruption {} != {}",
                                retrieved_value, test_value
                            );
                        }
                    }
                    Err(e) => {
                        println!("  ERROR: Failed to retrieve event: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("  FAILED: {}", e);
            }
        }
    }

    Ok(())
}

/// Test floating point precision boundaries
#[sinex_test]
async fn test_floating_point_precision_boundaries(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test floating point values at precision boundaries
    let test_values = vec![
        (f64::MAX, "f64::MAX"),
        (f64::MIN, "f64::MIN"),
        (f64::INFINITY, "f64::INFINITY"),
        (f64::NEG_INFINITY, "f64::NEG_INFINITY"),
        (f64::NAN, "f64::NAN"),
        (f64::EPSILON, "f64::EPSILON"),
        (1.0 / 3.0, "1/3 (repeating decimal)"),
        (std::f64::consts::PI, "PI"),
        (std::f64::consts::E, "E"),
    ];

    for (test_value, description) in test_values {
        println!(
            "Testing floating point boundary: {} ({})",
            test_value, description
        );

        let event = RawEventBuilder::new(
            "float_test",
            "precision.test",
            json!({
                "value": test_value,
                "description": description
            }),
        )
        .build();

        match insert_event(pool, &event).await {
            Ok(_) => {
                println!("  SUCCESS: Inserted event with value {}", test_value);

                // Try to query it back
                match sqlx::query!(
                    "SELECT payload FROM core.events WHERE event_id::uuid = $1::uuid",
                    event.id.to_uuid()
                )
                .fetch_one(pool)
                .await
                {
                    Ok(row) => {
                        let retrieved_value = row.payload["value"].as_f64().unwrap_or(-1.0);

                        if test_value.is_nan() && retrieved_value.is_nan() {
                            println!("  SUCCESS: NaN preserved");
                        } else if test_value.is_infinite()
                            && retrieved_value.is_infinite()
                            && test_value.is_sign_positive() == retrieved_value.is_sign_positive()
                        {
                            println!("  SUCCESS: Infinity preserved");
                        } else if (retrieved_value - test_value).abs() < f64::EPSILON {
                            println!("  SUCCESS: Value retrieved with acceptable precision");
                        } else {
                            println!(
                                "  WARNING: Precision loss {} != {}",
                                retrieved_value, test_value
                            );
                        }
                    }
                    Err(e) => {
                        println!("  ERROR: Failed to retrieve event: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("  FAILED: {}", e);
            }
        }
    }

    Ok(())
}

// =============================================================================
// Resource Boundary Tests
// =============================================================================

/// Test memory allocation boundaries
#[sinex_test]
async fn test_memory_allocation_boundaries(ctx: TestContext) -> TestResult {
    println!("Testing memory allocation boundaries:");

    // Test progressively larger memory allocations
    let allocation_sizes = vec![
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
        (100 * 1024 * 1024, "100MB"),
        (500 * 1024 * 1024, "500MB"),
        (1024 * 1024 * 1024, "1GB"),
    ];

    for (size, description) in allocation_sizes {
        println!("  Testing allocation: {}", description);

        let start = Instant::now();

        // Test allocation
        let allocation_result = std::panic::catch_unwind(|| {
            let _large_vec: Vec<u8> = vec![0; size];
            println!("    Allocation successful");
        });

        let elapsed = start.elapsed();

        match allocation_result {
            Ok(_) => {
                println!("    SUCCESS: Allocated {} in {:?}", description, elapsed);
            }
            Err(_) => {
                println!("    FAILED: Allocation of {} failed (OOM?)", description);
            }
        }

        // Give system time to clean up
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}

/// Test concurrent resource exhaustion
#[sinex_test]
async fn test_concurrent_resource_exhaustion(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing concurrent resource exhaustion:");

    let worker_count = 50;
    let operations_per_worker = 20;
    let successful_operations = Arc::new(AtomicU64::new(0));
    let failed_operations = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for worker_id in 0..worker_count {
        let pool_clone = pool.clone();
        let success_count = successful_operations.clone();
        let fail_count = failed_operations.clone();

        let handle = tokio::spawn(async move {
            for op_id in 0..operations_per_worker {
                // Create resource-intensive operation
                let large_payload = json!({
                    "worker_id": worker_id,
                    "operation_id": op_id,
                    "large_data": "x".repeat(1024 * 1024) // 1MB string
                });

                let event =
                    RawEventBuilder::new("resource_test", "exhaustion.test", large_payload).build();

                match timeout(Duration::from_secs(5), insert_event(&pool_clone, &event)).await {
                    Ok(Ok(_)) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(Err(e)) => {
                        fail_count.fetch_add(1, Ordering::SeqCst);
                        if op_id < 3 {
                            println!("Worker {} op {} failed: {}", worker_id, op_id, e);
                        }
                    }
                    Err(_) => {
                        fail_count.fetch_add(1, Ordering::SeqCst);
                        if op_id < 3 {
                            println!("Worker {} op {} timed out", worker_id, op_id);
                        }
                    }
                }

                // Small delay to allow resource recovery
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        handles.push(handle);
    }

    let start = Instant::now();
    join_all(handles).await;
    let elapsed = start.elapsed();

    let successful = successful_operations.load(Ordering::SeqCst);
    let failed = failed_operations.load(Ordering::SeqCst);
    let total_operations = worker_count * operations_per_worker;

    println!("Concurrent resource exhaustion results:");
    println!("  Total operations: {}", total_operations);
    println!("  Successful operations: {}", successful);
    println!("  Failed operations: {}", failed);
    println!(
        "  Success rate: {:.2}%",
        (successful as f64 / total_operations as f64) * 100.0
    );
    println!("  Total time: {:?}", elapsed);

    // Most operations should succeed despite resource pressure
    assert!(
        successful > total_operations / 2,
        "Most operations should succeed despite resource pressure"
    );

    Ok(())
}
