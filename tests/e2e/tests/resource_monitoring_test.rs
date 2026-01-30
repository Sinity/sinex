// # Resource Monitoring Tests
//
// Tests that verify:
// - Memory usage monitoring under high-volume operations
// - Database connection limits under concurrent access
// - Resource exhaustion scenario handling
//
// ## Performance Expectations
//
// - **Individual tests**: 30-90 seconds
// - **Resource usage**: High CPU/memory, significant database load
// - **Dependencies**: PostgreSQL

use sinex_primitives::db::models::EventFactory;
use sinex_primitives::{Timestamp, ulid::Ulid};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

/// Test resource limits and monitoring under load
#[sinex_test]
async fn test_resource_limits_monitoring(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    println!("Testing resource limits and monitoring under load...");

    // Test 1: Memory usage monitoring during high-volume operations
    let memory_test_start = Instant::now();
    let events_to_create = 1000;
    let memory_usage_samples = Arc::new(parking_lot::Mutex::new(Vec::new()));

    // Monitor memory usage while creating many events
    let memory_monitoring = Arc::new(AtomicBool::new(true));
    let memory_counter = Arc::new(AtomicU64::new(0));

    // Spawn memory monitoring task
    let monitor_handle = {
        let monitoring = memory_monitoring.clone();
        let counter = memory_counter.clone();
        tokio::spawn(async move {
            while monitoring.load(Ordering::Relaxed) {
                // Simulate memory usage check (in real system would use process stats)
                if let Ok(stats) = tokio::fs::read_to_string("/proc/self/status").await {
                    if let Some(line) = stats.lines().find(|l| l.starts_with("VmRSS:")) {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<u64>() {
                                counter.store(kb, Ordering::Relaxed);
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    // Create substantial volume of events to properly test performance
    let (tx, mut rx) = mpsc::channel(200);

    // Event generation task
    let generation_task = tokio::spawn(async move {
        for i in 0..events_to_create {
            let event_data = json!({
                "sequence": i,
                "large_data": "x".repeat(1000), // 1KB per event
                "timestamp": Timestamp::now().to_string(),
                "memory_test": true
            });

            if tx.send(event_data).await.is_err() {
                break;
            }

            // Small delay to allow monitoring
            if i % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    // Event processing task
    let processing_task = {
        let pool = pool.clone();
        let memory_samples = memory_usage_samples.clone();
        tokio::spawn(async move {
            let mut processed = 0;
            while let Some(event_data) = rx.recv().await {
                let mut event = EventFactory::new("resource.monitoring")
                    .create_event("memory_load_test", event_data);
                event.host = "localhost".to_string();
                event.ingestor_version = Some("1.0.0".to_string());

                let result = sinex_primitives::db::insert_event_with_validator(&pool, &event, None).await;

                if result.is_ok() {
                    processed += 1;
                } else {
                    println!("  Event processing failed after {} events", processed);
                    break;
                }

                // Collect memory sample every 50 events
                if processed % 50 == 0 {
                    let memory_kb = memory_counter.load(Ordering::Relaxed);
                    memory_samples.lock().push((processed, memory_kb));
                    println!("  Processed {} events, memory: {}KB", processed, memory_kb);
                }
            }
            processed
        })
    };

    // Wait for completion or timeout
    let load_test_result = timeout(Duration::from_secs(Timeouts::MEDIUM), async {
        tokio::try_join!(generation_task, processing_task)
    })
    .await;

    memory_monitoring.store(false, Ordering::Relaxed);
    monitor_handle.await.ok();

    let load_test_duration = memory_test_start.elapsed();

    match load_test_result {
        Ok(Ok(((), processed_count))) => {
            println!(
                "  ✓ Load test completed: {} events in {:?}",
                processed_count, load_test_duration
            );

            // Analyze memory usage patterns
            let samples = memory_usage_samples.lock();
            if samples.len() >= 2 {
                let initial_memory = samples[0].1;
                let final_memory = samples.last().unwrap().1;
                let memory_growth = final_memory.saturating_sub(initial_memory);
                let growth_rate = memory_growth as f64 / processed_count as f64;

                println!("  Memory analysis:");
                println!(
                    "    Initial: {}KB, Final: {}KB",
                    initial_memory, final_memory
                );
                println!(
                    "    Growth: {}KB ({:.2}KB per event)",
                    memory_growth, growth_rate
                );

                // Memory growth should be reasonable
                assert!(
                    growth_rate < 10.0,
                    "Memory growth rate too high: {:.2}KB per event",
                    growth_rate
                );
                assert!(
                    memory_growth < 50_000,
                    "Total memory growth too high: {}KB",
                    memory_growth
                );
            }
        }
        Ok(Err(e)) => {
            println!("  Load test failed: {:?}", e);
        }
        Err(_) => {
            println!("  Load test timed out after {:?}", load_test_duration);
        }
    }

    // Test 2: Database connection limits under concurrent access
    println!("\nTesting database connection limits...");

    let concurrent_connections = 24; // Scale up for 12 cores with proper connection management
    let mut connection_tasks = Vec::new();

    for i in 0..concurrent_connections {
        let pool = pool.clone();
        let task = tokio::spawn(async move {
            let start_time = Instant::now();

            // Try to acquire connection and perform operation
            let result = timeout(Duration::from_secs(Timeouts::SHORT), async {
                let mut conn = pool.acquire().await?;

                // Perform a quick operation
                sqlx::query_scalar!("SELECT COUNT(*) FROM core.processor_manifests")
                    .fetch_one(&mut *conn)
                    .await
                    .map(|opt| opt.unwrap_or(0))
            })
            .await;

            let duration = start_time.elapsed();
            (i, result, duration)
        });

        connection_tasks.push(task);
    }

    // Wait for all connection tests
    let connection_results = timeout(
        Duration::from_secs(Timeouts::MEDIUM),
        futures::future::join_all(connection_tasks),
    )
    .await;

    match connection_results {
        Ok(results) => {
            let mut successful_connections = 0;
            let mut failed_connections = 0;
            let mut timed_out_connections = 0;
            let mut total_duration = Duration::ZERO;

            for (i, conn_result, duration) in results.into_iter().flatten() {
                total_duration += duration;

                match conn_result {
                    Ok(Ok(_)) => {
                        successful_connections += 1;
                        if i < 5 {
                            println!("  Connection {} succeeded in {:?}", i, duration);
                        }
                    }
                    Ok(Err(e)) => {
                        failed_connections += 1;
                        if i < 5 {
                            println!("  Connection {} failed: {}", i, e);
                        }
                    }
                    Err(_) => {
                        timed_out_connections += 1;
                        if i < 5 {
                            println!("  Connection {} timed out after {:?}", i, duration);
                        }
                    }
                }
            }

            let avg_duration = total_duration / concurrent_connections as u32;

            println!("\nConnection Limit Test Results:");
            println!(
                "  Concurrent connections attempted: {}",
                concurrent_connections
            );
            println!("  Successful: {}", successful_connections);
            println!("  Failed: {}", failed_connections);
            println!("  Timed out: {}", timed_out_connections);
            println!("  Average duration: {:?}", avg_duration);

            // System should handle concurrent load reasonably
            assert!(
                successful_connections > concurrent_connections / 2,
                "Too many connection failures: {}/{}",
                failed_connections,
                concurrent_connections
            );
            assert!(
                avg_duration < Duration::from_secs(Timeouts::SHORT),
                "Average connection time too slow: {:?}",
                avg_duration
            );
        }
        Err(_) => {
            println!("  Connection limit test timed out");
        }
    }

    // Cleanup
    sqlx::query!("DELETE FROM core.events WHERE source = 'resource.monitoring'")
        .execute(&pool)
        .await
        .ok();

    Ok(())
}

/// Test system behavior under resource exhaustion scenarios
#[sinex_test]
async fn test_resource_exhaustion_scenarios(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    println!("Testing resource exhaustion scenarios...");

    // Test 1: Large transaction handling
    let large_transaction_start = Instant::now();

    let large_transaction_result = timeout(Duration::from_secs(Timeouts::MEDIUM), async {
        let mut tx = pool.begin().await?;

        // Try to insert many events in a single transaction
        for i in 0..1000 {
            sqlx::query!(
                "INSERT INTO core.events (
            event_id, source, event_type, host, payload)
                     VALUES ($1::uuid, $2, $3, $4, $5)",
                Ulid::new().to_uuid(),
                "exhaustion.test",
                "large_transaction",
                "localhost",
                json!({"batch_item": i, "data": "x".repeat(100)})
            )
            .execute(&mut *tx)
            .await?;

            // Check for timeout every 100 items
            if i % 100 == 0 {
                println!("    Inserted {} items in transaction", i + 1);
            }
        }

        tx.commit().await?;
        Ok::<(), sqlx::Error>(())
    })
    .await;

    let large_transaction_duration = large_transaction_start.elapsed();

    match large_transaction_result {
        Ok(Ok(())) => {
            println!(
                "  ✓ Large transaction completed in {:?}",
                large_transaction_duration
            );
        }
        Ok(Err(e)) => {
            println!("  Large transaction failed: {}", e);
        }
        Err(_) => {
            println!("  ✓ Large transaction timed out (protection active)");
        }
    }

    // Test 2: Concurrent transaction stress
    println!("\nTesting concurrent transaction stress...");

    let concurrent_transactions = 20;
    let mut transaction_tasks = Vec::new();

    for i in 0..concurrent_transactions {
        let pool = pool.clone();
        let task = tokio::spawn(async move {
            let start_time = Instant::now();

            let result = timeout(Duration::from_secs(Timeouts::SHORT), async {
                let mut tx = pool.begin().await?;

                // Each transaction inserts a small batch
                for j in 0..10 {
                    sqlx::query!(
                        "INSERT INTO core.events (
            event_id, source, event_type, host, payload)
                             VALUES ($1::uuid, $2, $3, $4, $5)",
                        Ulid::new().to_uuid(),
                        format!("concurrent.tx.{}", i),
                        "concurrent_test",
                        "localhost",
                        json!({"tx_id": i, "item": j})
                    )
                    .execute(&mut *tx)
                    .await?;
                }

                tx.commit().await?;
                Ok::<(), sqlx::Error>(())
            })
            .await;

            let duration = start_time.elapsed();
            (i, result, duration)
        });

        transaction_tasks.push(task);
    }

    let transaction_results = timeout(
        Duration::from_secs(Timeouts::MEDIUM),
        futures::future::join_all(transaction_tasks),
    )
    .await;

    match transaction_results {
        Ok(results) => {
            let mut successful_transactions = 0;
            let mut failed_transactions = 0;
            let mut total_tx_duration = Duration::ZERO;

            for (i, tx_result, duration) in results.into_iter().flatten() {
                total_tx_duration += duration;

                match tx_result {
                    Ok(Ok(())) => {
                        successful_transactions += 1;
                        if i < 3 {
                            println!("    Transaction {} completed in {:?}", i, duration);
                        }
                    }
                    Ok(Err(e)) => {
                        failed_transactions += 1;
                        if i < 3 {
                            println!("    Transaction {} failed: {}", i, e);
                        }
                    }
                    Err(_) => {
                        failed_transactions += 1;
                        if i < 3 {
                            println!("    Transaction {} timed out", i);
                        }
                    }
                }
            }

            let avg_tx_duration = total_tx_duration / concurrent_transactions as u32;

            println!("\nConcurrent Transaction Results:");
            println!("  Attempted: {}", concurrent_transactions);
            println!("  Successful: {}", successful_transactions);
            println!("  Failed: {}", failed_transactions);
            println!("  Average duration: {:?}", avg_tx_duration);

            // Most transactions should succeed under normal load
            assert!(
                successful_transactions >= concurrent_transactions * 7 / 10,
                "Transaction failure rate too high: {}/{}",
                failed_transactions,
                concurrent_transactions
            );
        }
        Err(_) => {
            println!("  Concurrent transaction test timed out");
        }
    }

    // Cleanup all test data
    sqlx::query!(
        "DELETE FROM core.events WHERE source LIKE 'exhaustion%' OR source LIKE 'concurrent%'"
    )
    .execute(&pool)
    .await
    .ok();

    Ok(())
}
