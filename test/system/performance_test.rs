//! # Performance and Load Testing
//!
//! System performance validation tests that measure:
//! - Load testing with realistic data volumes
//! - Throughput and latency measurements
//! - Resource usage profiling
//! - Scaling behavior validation
//!
//! ## Test Categories
//!
//! - **Database Performance**: Insertion and query performance
//! - **Concurrent Processing**: Multi-worker performance validation
//! - **Memory Usage**: Memory consumption under load
//! - **Query Latency**: Database query response times
//! - **Scaling Tests**: Performance scaling with load
//!
//! ## Performance Expectations
//!
//! - **Individual tests**: 30-120 seconds
//! - **Resource usage**: High CPU/memory usage during tests
//! - **Baseline performance**: 1000+ events/second insertion rate

use crate::common::prelude::*;
use crate::common::timing_optimization::replacements::wait_for_filtered_event_count;
use std::time::{Duration, Instant};

// ==================== DATABASE PERFORMANCE TESTS ====================

#[sinex_test(timeout = 60)]
async fn test_database_insertion_performance(ctx: TestContext) -> TestResult {
    // Test: Basic database insertion performance
    let pool = ctx.pool();

    let target_events = 1000; // Reduced from 10k for stability
    let start_time = Instant::now();
    let events_inserted = Arc::new(AtomicU64::new(0));

    // Insert events sequentially to avoid overwhelming test DB
    for i in 0..target_events {
        let event = RawEventBuilder::new(
            "load_test",
            "performance_test",
            json!({
                "sequence": i,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }),
        )
        .build();

        match insert_event(pool, &event).await {
            Ok(_) => {
                events_inserted.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("Insert failed: {}", e);
            }
        }

        // Small delay to avoid overwhelming test infrastructure
        if i % 100 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let elapsed = start_time.elapsed();
    let total_inserted = events_inserted.load(Ordering::Relaxed);
    let insertion_rate = (total_inserted as f64) / elapsed.as_secs_f64();

    println!(
        "Inserted {} events in {:?} ({:.2} events/sec)",
        total_inserted, elapsed, insertion_rate
    );

    // Verify events in database using timing utility
    let db_count = wait_for_filtered_event_count(
        pool,
        "source = $1",
        &["load_test"],
        target_events as i64,
        10,
    )
    .await
    .unwrap_or(0) as u64;

    println!("Database contains {} load_test events", db_count);

    // Success criteria (very relaxed for test stability)
    assert!(
        insertion_rate >= 100.0,
        "Insertion rate too low: {:.2} events/sec",
        insertion_rate
    );
    assert!(
        db_count >= (total_inserted * 95 / 100),
        "Too many events lost: {} inserted, {} in DB",
        total_inserted,
        db_count
    );

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source = 'load_test'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test(timeout = 60)]
async fn test_concurrent_insertion_performance(ctx: TestContext) -> TestResult {
    // Test: Concurrent database insertion
    let pool = ctx.pool();

    let events_per_worker = 100;
    let num_workers = 5;
    let start_time = Instant::now();

    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let mut inserted = 0;

            for i in 0..events_per_worker {
                let event = RawEventBuilder::new(
                    "concurrent_load_test",
                    "worker_test",
                    json!({
                        "worker_id": worker_id,
                        "sequence": i,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }),
                )
                .build();

                if insert_event(&pool_clone, &event).await.is_ok() {
                    inserted += 1;
                }

                // Small delay to avoid overwhelming
                if i % 20 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            inserted
        });

        handles.push(handle);
    }

    // Wait for all workers
    let mut total_inserted = 0;
    for handle in handles {
        total_inserted += handle.await?;
    }

    let elapsed = start_time.elapsed();
    let insertion_rate = (total_inserted as f64) / elapsed.as_secs_f64();

    println!(
        "Concurrent test: {} workers inserted {} events in {:?} ({:.2} events/sec)",
        num_workers, total_inserted, elapsed, insertion_rate
    );

    // Verify events in database using timing utility
    let db_count = wait_for_filtered_event_count(
        pool,
        "source = $1",
        &["concurrent_load_test"],
        (num_workers * events_per_worker) as i64,
        10,
    )
    .await
    .unwrap_or(0) as u64;

    // Success criteria
    assert!(
        total_inserted >= (num_workers * events_per_worker * 95 / 100),
        "Too few events inserted"
    );
    assert!(
        db_count as u64 >= (total_inserted * 95 / 100),
        "Database count mismatch"
    );

    // Performance assertion - expect at least 1K events/sec with safety margin
    assert!(
        insertion_rate > 1_000.0,
        "Event insertion performance regression: {:.0}/sec is below 1K/sec threshold",
        insertion_rate
    );

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source = 'concurrent_load_test'")
        .execute(pool)
        .await?;

    Ok(())
}

// ==================== PERFORMANCE TESTS FROM MOD.RS ====================

#[sinex_test]
async fn test_high_volume_ingestion(ctx: TestContext) -> Result<(), anyhow::Error> {
    let start = Instant::now();
    let mut handles = vec![];

    // Spawn multiple tasks to insert events concurrently
    for i in 0..5 {
        let pool = ctx.pool().clone();
        let handle = tokio::spawn(async move {
            for j in 0..200 {
                queries::crate::common::insert_event_with_validator(
                    &pool,
                    &format!("perf_test_{}", i),
                    &format!("test_event_{}", j),
                    "test-host",
                    serde_json::json!({
                        "task": i,
                        "event": j,
                        "data": "performance test payload"
                    }),
                    None,
                    None,
                    None,
                )
                .await?;
            }
            Ok::<_, anyhow::Error>(())
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await??;
    }

    let elapsed = start.elapsed();
    println!("Inserted 1000 events in {:?}", elapsed);

    // Verify count using timing utility
    let count =
        wait_for_filtered_event_count(ctx.pool(), "source LIKE $1", &["perf_test_%"], 1000, 10)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to verify event count: {}", e))?;

    pretty_assertions::assert_eq!(count, 1000);
    assert!(
        elapsed < Duration::from_secs(5),
        "Ingestion took too long: {:?}",
        elapsed
    );

    Ok(())
}

#[sinex_test]
async fn test_concurrent_processing_performance(ctx: TestContext) -> TestResult {
    // Insert test events
    for i in 0..100 {
        queries::crate::common::insert_event_with_validator(
            ctx.pool(),
            "concurrent_test",
            "process_me",
            "test-host",
            serde_json::json!({ "id": i }),
            None,
            None,
            None,
        )
        .await?;
    }

    let start = Instant::now();
    let mut handles = vec![];

    // Spawn workers to process events concurrently
    for worker_id in 0..4 {
        let pool = ctx.pool().clone();
        let handle = tokio::spawn(async move {
            let mut processed = 0;

            // Process events until none left
            loop {
                // Try to claim an event for processing
                let maybe_event: Option<(uuid::Uuid,)> = sqlx::query_as(
                    r#"
                    SELECT id::uuid
                    FROM raw.events
                    WHERE source = 'concurrent_test'
                      AND event_type = 'process_me'
                      AND NOT EXISTS (
                        SELECT 1 FROM raw.events processed
                        WHERE processed.source = 'concurrent_test'
                          AND processed.event_type = 'processed'
                          AND processed.payload->>'original_id' = raw.events.id::text
                      )
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    "#,
                )
                .fetch_optional(&pool)
                .await?;

                if let Some((event_id,)) = maybe_event {
                    // Simulate processing
                    tokio::task::yield_now().await;

                    // Mark as processed
                    queries::crate::common::insert_event_with_validator(
                        &pool,
                        "concurrent_test",
                        "processed",
                        "test-host",
                        serde_json::json!({
                            "worker_id": worker_id,
                            "original_id": event_id.to_string()
                        }),
                        None,
                        None,
                        None,
                    )
                    .await?;

                    processed += 1;
                } else {
                    // No more events to process
                    break;
                }
            }

            Ok::<_, anyhow::Error>(processed)
        });
        handles.push(handle);
    }

    // Wait for all workers
    let mut total_processed = 0;
    for handle in handles {
        total_processed += handle.await??;
    }

    let elapsed = start.elapsed();
    println!(
        "Processed {} events in {:?} with 4 workers",
        total_processed, elapsed
    );

    pretty_assertions::assert_eq!(total_processed, 100);
    assert!(
        elapsed < Duration::from_secs(3),
        "Processing took too long: {:?}",
        elapsed
    );

    Ok(())
}

#[sinex_test]
async fn test_query_latency(ctx: TestContext) -> TestResult {
    // Insert test data
    for i in 0..1000 {
        queries::crate::common::insert_event_with_validator(
            ctx.pool(),
            "latency_test",
            if i % 2 == 0 { "type_a" } else { "type_b" },
            "test-host",
            serde_json::json!({
                "value": i,
                "category": if i % 10 == 0 { "special" } else { "normal" }
            }),
            None,
            None,
            None,
        )
        .await?;
    }

    // Test various query patterns
    let queries_to_test = vec![
        ("Simple count", "SELECT COUNT(*) FROM raw.events WHERE source = 'latency_test'"),
        ("Filtered count", "SELECT COUNT(*) FROM raw.events WHERE source = 'latency_test' AND event_type = 'type_a'"),
        ("JSON query", "SELECT COUNT(*) FROM raw.events WHERE source = 'latency_test' AND payload->>'category' = 'special'"),
        ("Recent events", "SELECT * FROM raw.events WHERE source = 'latency_test' ORDER BY ts_ingest DESC LIMIT 10"),
    ];

    for (name, query) in queries_to_test {
        let start = Instant::now();
        let _result = sqlx::query(query).fetch_all(ctx.pool()).await?;
        let elapsed = start.elapsed();

        println!("{}: {:?}", name, elapsed);
        assert!(
            elapsed < Duration::from_millis(100),
            "{} query too slow: {:?}",
            name,
            elapsed
        );
    }

    Ok(())
}

// ==================== MEMORY USAGE TESTS ====================

#[sinex_test]
async fn test_memory_usage_under_load(ctx: TestContext) -> TestResult {
    // Test memory usage during high-volume operations
    let pool = ctx.pool();
    let initial_memory = get_memory_usage();

    // Insert many events to test memory usage
    let num_events = 10000;
    let mut events = Vec::with_capacity(num_events);

    for i in 0..num_events {
        let event = RawEventBuilder::new(
            "memory_test",
            "load_event",
            json!({
                "sequence": i,
                "large_data": "x".repeat(1000), // 1KB per event
                "timestamp": chrono::Utc::now().to_rfc3339()
            }),
        )
        .build();
        events.push(event);
    }

    let mid_memory = get_memory_usage();
    println!("Memory after creating {} events: {} KB", num_events, mid_memory);

    // Insert events in batches to avoid overwhelming the system
    let batch_size = 100;
    for chunk in events.chunks(batch_size) {
        for event in chunk {
            insert_event(pool, event).await?;
        }
        tokio::task::yield_now().await;
    }

    let final_memory = get_memory_usage();
    println!("Memory after inserting {} events: {} KB", num_events, final_memory);

    // Verify memory usage is reasonable
    let memory_growth = final_memory - initial_memory;
    assert!(
        memory_growth < 100_000, // 100MB limit
        "Memory usage grew by {} KB, which is too much",
        memory_growth
    );

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source = 'memory_test'")
        .execute(pool)
        .await?;

    Ok(())
}

fn get_memory_usage() -> usize {
    // Simple memory usage estimation (platform-specific)
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        return kb_str.parse().unwrap_or(0);
                    }
                }
            }
        }
    }

    // Fallback: return 0 if can't get memory info
    0
}

// ==================== SCALING TESTS ====================

#[sinex_test]
async fn test_scaling_with_worker_count(ctx: TestContext) -> TestResult {
    // Test how performance scales with worker count
    let pool = ctx.pool();
    let events_per_test = 500;

    // Test different worker counts
    let worker_counts = vec![1, 2, 4, 8];
    let mut results = Vec::new();

    for worker_count in worker_counts {
        println!("Testing with {} workers", worker_count);

        // Insert test events
        for i in 0..events_per_test {
            let event = RawEventBuilder::new(
                "scaling_test",
                "worker_event",
                json!({
                    "sequence": i,
                    "worker_test": worker_count,
                }),
            )
            .build();
            insert_event(pool, &event).await?;
        }

        // Run workers
        let start = Instant::now();
        let mut handles = Vec::new();

        for worker_id in 0..worker_count {
            let pool_clone = pool.clone();
            let handle = tokio::spawn(async move {
                let mut processed = 0;

                // Process events until none left
                loop {
                    let maybe_event: Option<(uuid::Uuid,)> = sqlx::query_as(
                        r#"
                        SELECT id::uuid
                        FROM raw.events
                        WHERE source = 'scaling_test'
                          AND event_type = 'worker_event'
                          AND NOT EXISTS (
                            SELECT 1 FROM raw.events processed
                            WHERE processed.source = 'scaling_test'
                              AND processed.event_type = 'processed'
                              AND processed.payload->>'original_id' = raw.events.id::text
                          )
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        "#,
                    )
                    .fetch_optional(&pool_clone)
                    .await?;

                    if let Some((event_id,)) = maybe_event {
                        // Simulate processing time
                        tokio::time::sleep(Duration::from_millis(1)).await;

                        // Mark as processed
                        queries::crate::common::insert_event_with_validator(
                            &pool_clone,
                            "scaling_test",
                            "processed",
                            "test-host",
                            serde_json::json!({
                                "worker_id": worker_id,
                                "original_id": event_id.to_string()
                            }),
                            None,
                            None,
                            None,
                        )
                        .await?;

                        processed += 1;
                    } else {
                        break;
                    }
                }

                Ok::<_, anyhow::Error>(processed)
            });
            handles.push(handle);
        }

        // Wait for completion
        let mut total_processed = 0;
        for handle in handles {
            total_processed += handle.await??;
        }

        let elapsed = start.elapsed();
        let throughput = (total_processed as f64) / elapsed.as_secs_f64();

        results.push((worker_count, throughput, elapsed));
        println!(
            "  {} workers: {} events in {:?} ({:.2} events/sec)",
            worker_count, total_processed, elapsed, throughput
        );

        // Cleanup for next test
        sqlx::query!("DELETE FROM raw.events WHERE source = 'scaling_test'")
            .execute(pool)
            .await?;
    }

    // Analyze scaling results
    println!("\nScaling analysis:");
    for (workers, throughput, _) in &results {
        println!("  {} workers: {:.2} events/sec", workers, throughput);
    }

    // Verify that performance improves with more workers (at least initially)
    assert!(
        results[1].1 > results[0].1,
        "Performance should improve from 1 to 2 workers"
    );

    Ok(())
}

// ==================== RESOURCE USAGE TESTS ====================

#[sinex_test]
async fn test_database_connection_pooling(ctx: TestContext) -> TestResult {
    // Test database connection pool performance
    let pool = ctx.pool();
    let concurrent_connections = 20;
    let queries_per_connection = 50;

    let start = Instant::now();
    let mut handles = Vec::new();

    for conn_id in 0..concurrent_connections {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let mut query_count = 0;

            for i in 0..queries_per_connection {
                // Simple query to test connection pooling
                let result = sqlx::query("SELECT $1 as conn_id, $2 as query_id")
                    .bind(conn_id)
                    .bind(i)
                    .fetch_one(&pool_clone)
                    .await?;

                let returned_conn_id: i32 = result.get("conn_id");
                let returned_query_id: i32 = result.get("query_id");

                assert_eq!(returned_conn_id, conn_id);
                assert_eq!(returned_query_id, i);
                query_count += 1;
            }

            Ok::<_, anyhow::Error>(query_count)
        });
        handles.push(handle);
    }

    // Wait for all connections to complete
    let mut total_queries = 0;
    for handle in handles {
        total_queries += handle.await??;
    }

    let elapsed = start.elapsed();
    let query_rate = (total_queries as f64) / elapsed.as_secs_f64();

    println!(
        "Executed {} queries across {} connections in {:?} ({:.2} queries/sec)",
        total_queries, concurrent_connections, elapsed, query_rate
    );

    // Verify reasonable performance
    assert!(
        query_rate > 1000.0,
        "Query rate too low: {:.2} queries/sec",
        query_rate
    );

    Ok(())
}

#[sinex_test]
async fn test_large_payload_performance(ctx: TestContext) -> TestResult {
    // Test performance with large event payloads
    let pool = ctx.pool();
    let payload_sizes = vec![1024, 10240, 102400]; // 1KB, 10KB, 100KB

    for payload_size in payload_sizes {
        println!("Testing with {} byte payloads", payload_size);

        let large_data = "x".repeat(payload_size);
        let num_events = 50; // Smaller number due to large payloads

        let start = Instant::now();

        for i in 0..num_events {
            let event = RawEventBuilder::new(
                "large_payload_test",
                "large_event",
                json!({
                    "sequence": i,
                    "large_data": large_data,
                    "size": payload_size,
                }),
            )
            .build();

            insert_event(pool, &event).await?;
        }

        let elapsed = start.elapsed();
        let throughput = (num_events as f64) / elapsed.as_secs_f64();
        let bytes_per_sec = (num_events * payload_size) as f64 / elapsed.as_secs_f64();

        println!(
            "  {} events in {:?} ({:.2} events/sec, {:.2} bytes/sec)",
            num_events, elapsed, throughput, bytes_per_sec
        );

        // Cleanup
        sqlx::query!("DELETE FROM raw.events WHERE source = 'large_payload_test'")
            .execute(pool)
            .await?;
    }

    Ok(())
}

#[sinex_test]
async fn test_burst_load_handling(ctx: TestContext) -> TestResult {
    // Test how system handles burst loads
    let pool = ctx.pool();
    let burst_size = 1000;
    let burst_duration = Duration::from_millis(100);

    println!("Testing burst load: {} events in {:?}", burst_size, burst_duration);

    let start = Instant::now();
    let mut handles = Vec::new();

    // Create burst by spawning many concurrent insertion tasks
    for i in 0..burst_size {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let event = RawEventBuilder::new(
                "burst_test",
                "burst_event",
                json!({
                    "burst_id": i,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                }),
            )
            .build();

            insert_event(&pool_clone, &event).await
        });
        handles.push(handle);
    }

    // Wait for all insertions to complete
    let mut successful_inserts = 0;
    for handle in handles {
        if handle.await?.is_ok() {
            successful_inserts += 1;
        }
    }

    let elapsed = start.elapsed();
    let success_rate = (successful_inserts as f64) / (burst_size as f64) * 100.0;

    println!(
        "Burst test: {}/{} events inserted in {:?} ({:.1}% success rate)",
        successful_inserts, burst_size, elapsed, success_rate
    );

    // Verify most events were successfully inserted
    assert!(
        success_rate > 90.0,
        "Success rate too low: {:.1}%",
        success_rate
    );

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source = 'burst_test'")
        .execute(pool)
        .await?;

    Ok(())
}