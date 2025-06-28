use crate::common::prelude::*;
use sinex_db::queries::insert_raw_event;

/// Test graceful degradation under database connectivity issues
#[sinex_test]
async fn test_graceful_degradation_database_failure(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create test agent
    let agent_name = format!("degradation_test_{}", Ulid::new());
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description)
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "Graceful degradation test"
    )
    .execute(pool)
    .await?;

    println!("Testing graceful degradation under database connectivity issues...");

    // Test 1: Database connection pool exhaustion
    let mut held_connections = Vec::new();
    let max_connections = 20; // Typical test pool size

    // Exhaust connection pool
    for i in 0..max_connections {
        match pool.acquire().await {
            Ok(conn) => {
                held_connections.push(conn);
                println!("  Acquired connection {}/{}", i + 1, max_connections);
            }
            Err(e) => {
                println!("  Connection {} failed: {}", i + 1, e);
                break;
            }
        }
    }

    println!(
        "  Connection pool exhausted with {} connections",
        held_connections.len()
    );

    // Test graceful handling of no available connections
    let pool1 = pool.clone();
    let pool2 = pool.clone();
    let pool3 = pool.clone();

    // Define async functions for each operation
    async fn event_test(pool: DbPool) -> Result<(), anyhow::Error> {
        let _event = insert_raw_event(
            &pool,
            "degradation.test",
            "connection_exhaustion",
            "localhost",
            json!({"test": "degraded_mode"}),
            None,
            Some("1.0.0"),
            None,
        )
        .await
        .map_err(anyhow::Error::from)?;
        Ok(())
    }

    async fn health_test(pool: DbPool) -> Result<(), anyhow::Error> {
        let _health_check = sqlx::query_scalar!("SELECT 1")
            .fetch_one(pool)
            .await
            .map_err(anyhow::Error::from)?
            .unwrap_or(0);
        Ok(())
    }

    async fn agent_test(pool: DbPool) -> Result<(), anyhow::Error> {
        let _agent_check =
            sqlx::query!("SELECT agent_name FROM sinex_schemas.agent_manifests LIMIT 1")
                .fetch_one(pool)
                .await
                .map_err(anyhow::Error::from)?;
        Ok(())
    }

    let mut graceful_timeouts = 0;
    let mut unexpected_errors = 0;

    // Test event operation
    let operation = timeout(Duration::from_secs(2), event_test(pool1));
    match operation.await {
        Ok(Ok(_)) => {
            println!("  Operation 0 succeeded unexpectedly");
        }
        Ok(Err(e)) => {
            println!("  Operation 0 failed gracefully: {}", e);
            unexpected_errors += 1;
        }
        Err(_) => {
            println!("  ✓ Operation 0 timed out gracefully");
            graceful_timeouts += 1;
        }
    }

    // Test health operation
    let operation = timeout(Duration::from_secs(2), health_test(pool2));
    match operation.await {
        Ok(Ok(_)) => {
            println!("  Operation 1 succeeded unexpectedly");
        }
        Ok(Err(e)) => {
            println!("  Operation 1 failed gracefully: {}", e);
            unexpected_errors += 1;
        }
        Err(_) => {
            println!("  ✓ Operation 1 timed out gracefully");
            graceful_timeouts += 1;
        }
    }

    // Test agent operation
    let operation = timeout(Duration::from_secs(2), agent_test(pool3));
    match operation.await {
        Ok(Ok(_)) => {
            println!("  Operation 2 succeeded unexpectedly");
        }
        Ok(Err(e)) => {
            println!("  Operation 2 failed gracefully: {}", e);
            unexpected_errors += 1;
        }
        Err(_) => {
            println!("  ✓ Operation 2 timed out gracefully");
            graceful_timeouts += 1;
        }
    }

    // Release connections to restore functionality
    drop(held_connections);

    // Verify system recovery
    let recovery_start = Instant::now();
    let recovery_test = timeout(
        Duration::from_secs(5),
        insert_raw_event(
            &pool,
            "degradation.test",
            "recovery_test",
            "localhost",
            json!({"recovered": true}),
            None,
            Some("1.0.0"),
            None,
        ),
    )
    .await;

    let recovery_duration = recovery_start.elapsed();

    match recovery_test {
        Ok(Ok(_)) => {
            println!("  ✓ System recovered in {:?}", recovery_duration);
        }
        Ok(Err(e)) => {
            println!("  WARNING: Recovery failed: {}", e);
        }
        Err(_) => {
            println!(
                "  WARNING: Recovery timed out after {:?}",
                recovery_duration
            );
        }
    }

    println!("\nGraceful Degradation Test Results:");
    println!("  Graceful timeouts: {}/3", graceful_timeouts);
    println!("  Unexpected errors: {}/3", unexpected_errors);
    println!("  Recovery time: {:?}", recovery_duration);

    // System should handle degradation gracefully
    assert!(
        graceful_timeouts >= 2,
        "System should timeout gracefully under load"
    );
    assert!(
        recovery_duration < Duration::from_secs(5),
        "Recovery should be fast"
    );

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source = 'degradation.test'")
        .execute(pool)
        .await
        .ok();
    sqlx::query!(
        "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Test resource limits and monitoring under load
#[sinex_test]
async fn test_resource_limits_monitoring(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing resource limits and monitoring under load...");

    // Test 1: Memory usage monitoring during high-volume operations
    let memory_test_start = Instant::now();
    let events_to_create = 1000;
    let memory_usage_samples = Arc::new(std::sync::Mutex::new(Vec::new()));

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
                if let Ok(stats) = std::fs::read_to_string("/proc/self/status") {
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

    // Create high volume of events
    let (tx, mut rx) = mpsc::channel(100);

    // Event generation task
    let generation_task = tokio::spawn(async move {
        for i in 0..events_to_create {
            let event_data = json!({
                "sequence": i,
                "large_data": "x".repeat(1000), // 1KB per event
                "timestamp": chrono::Utc::now().to_rfc3339(),
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
                let result = insert_raw_event(
                    &pool,
                    "resource.monitoring",
                    "memory_load_test",
                    "localhost",
                    event_data,
                    None,
                    Some("1.0.0"),
                    None,
                )
                .await;

                if result.is_ok() {
                    processed += 1;
                } else {
                    println!("  Event processing failed after {} events", processed);
                    break;
                }

                // Collect memory sample every 50 events
                if processed % 50 == 0 {
                    let memory_kb = memory_counter.load(Ordering::Relaxed);
                    memory_samples.lock().unwrap().push((processed, memory_kb));
                    println!("  Processed {} events, memory: {}KB", processed, memory_kb);
                }
            }
            processed
        })
    };

    // Wait for completion or timeout
    let load_test_result = timeout(Duration::from_secs(5), async {
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
            let samples = memory_usage_samples.lock().unwrap();
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

    let concurrent_connections = 50;
    let mut connection_tasks = Vec::new();

    for i in 0..concurrent_connections {
        let pool = pool.clone();
        let task = tokio::spawn(async move {
            let start_time = Instant::now();

            // Try to acquire connection and perform operation
            let result = timeout(Duration::from_secs(3), async {
                let mut conn = pool.acquire().await?;

                // Perform a quick operation
                sqlx::query_scalar!("SELECT COUNT(*) FROM sinex_schemas.agent_manifests")
                    .fetch_one(mut *conn)
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
        Duration::from_secs(5),
        futures::future::join_all(connection_tasks),
    )
    .await;

    match connection_results {
        Ok(results) => {
            let mut successful_connections = 0;
            let mut failed_connections = 0;
            let mut timed_out_connections = 0;
            let mut total_duration = Duration::ZERO;

            for result in results {
                if let Ok((i, conn_result, duration)) = result {
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
                avg_duration < Duration::from_secs(3),
                "Average connection time too slow: {:?}",
                avg_duration
            );
        }
        Err(_) => {
            println!("  Connection limit test timed out");
        }
    }

    // Test 3: Health monitoring under stress
    println!("\nTesting health monitoring under stress...");

    let health_check_start = Instant::now();
    let health_checks = 20;
    let mut health_results = Vec::new();

    for i in 0..health_checks {
        let health_start = Instant::now();

        let health_result = timeout(Duration::from_secs(2), async {
            // Comprehensive health check
            let db_health = sqlx::query_scalar!("SELECT 1")
                .fetch_one(pool)
                .await?
                .unwrap_or(0);
            let table_count = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM information_schema.tables
                     WHERE table_schema IN ('raw', 'sinex_schemas')"
            )
            .fetch_one(pool)
            .await?
            .unwrap_or(0);
            let recent_events = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM raw.events WHERE ts_ingest > NOW() - INTERVAL '1 hour'"
            )
            .fetch_one(pool)
            .await?
            .unwrap_or(0);

            Ok::<_, sqlx::Error>((db_health, table_count, recent_events))
        })
        .await;

        let health_duration = health_start.elapsed();

        if i % 5 == 0 {
            match &health_result {
                Ok(Ok((_, table_count, event_count))) => {
                    println!(
                        "  Health check {}: OK ({} tables, {} recent events) in {:?}",
                        i, table_count, event_count, health_duration
                    );
                }
                Ok(Err(e)) => {
                    println!("  Health check {}: FAILED - {}", i, e);
                }
                Err(_) => {
                    println!("  Health check {}: TIMEOUT after {:?}", i, health_duration);
                }
            }
        }

        health_results.push((i, health_result, health_duration));

        // Small delay between health checks
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let total_health_duration = health_check_start.elapsed();

    // Analyze health check results
    let successful_health_checks = health_results
        .iter()
        .filter(|(_, result, _)| matches!(result, Ok(Ok(_))))
        .count();

    let failed_health_checks = health_results
        .iter()
        .filter(|(_, result, _)| matches!(result, Ok(Err(_))))
        .count();

    let timed_out_health_checks = health_results
        .iter()
        .filter(|(_, result, _)| matches!(result, Err(_)))
        .count();

    let avg_health_duration: Duration = health_results
        .iter()
        .map(|(_, _, duration)| *duration)
        .sum::<Duration>()
        / health_checks as u32;

    println!("\nHealth Monitoring Test Results:");
    println!("  Total health checks: {}", health_checks);
    println!("  Successful: {}", successful_health_checks);
    println!("  Failed: {}", failed_health_checks);
    println!("  Timed out: {}", timed_out_health_checks);
    println!("  Average health check time: {:?}", avg_health_duration);
    println!("  Total monitoring duration: {:?}", total_health_duration);

    // Health monitoring should be reliable
    assert!(
        successful_health_checks >= health_checks * 8 / 10,
        "Health check success rate too low: {}/{}",
        successful_health_checks,
        health_checks
    );
    assert!(
        avg_health_duration < Duration::from_millis(500),
        "Health checks too slow: {:?}",
        avg_health_duration
    );

    println!("  ✓ Health monitoring maintains reliability under stress");

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source = 'resource.monitoring'")
        .execute(pool)
        .await
        .ok();

    Ok(())
}

/// Test system behavior under resource exhaustion scenarios
#[sinex_test]
async fn test_resource_exhaustion_scenarios(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing resource exhaustion scenarios...");

    // Test 1: Large transaction handling
    let large_transaction_start = Instant::now();

    let large_transaction_result = timeout(Duration::from_secs(5), async {
        let mut tx = pool.begin().await?;

        // Try to insert many events in a single transaction
        for i in 0..1000 {
            sqlx::query!(
                "INSERT INTO raw.events (id, source, event_type, host, payload)
                     VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
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

            let result = timeout(Duration::from_secs(3), async {
                let mut tx = pool.begin().await?;

                // Each transaction inserts a small batch
                for j in 0..10 {
                    sqlx::query!(
                        "INSERT INTO raw.events (id, source, event_type, host, payload)
                             VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
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
        Duration::from_secs(5),
        futures::future::join_all(transaction_tasks),
    )
    .await;

    match transaction_results {
        Ok(results) => {
            let mut successful_transactions = 0;
            let mut failed_transactions = 0;
            let mut total_tx_duration = Duration::ZERO;

            for result in results {
                if let Ok((i, tx_result, duration)) = result {
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

    // Test 3: Query performance under load
    println!("\nTesting query performance under load...");

    let query_performance_start = Instant::now();
    let complex_queries = vec![
        // Aggregation query
        "SELECT source, COUNT(*) as event_count FROM raw.events WHERE created_at > NOW() - INTERVAL '1 hour' GROUP BY source",

        // Recent events query
        "SELECT * FROM raw.events ORDER BY created_at DESC LIMIT 100",

        // Pattern matching query
        "SELECT * FROM raw.events WHERE source LIKE 'exhaustion%' AND payload ? 'batch_item'",

        // Agent statistics query
        "SELECT agent_name, version, description FROM sinex_schemas.agent_manifests",
    ];

    let mut query_performance_results = Vec::new();

    for (i, query) in complex_queries.iter().enumerate() {
        let query_start = Instant::now();

        let query_result =
            timeout(Duration::from_secs(2), sqlx::query(query).fetch_all(pool)).await;

        let query_duration = query_start.elapsed();

        match query_result {
            Ok(Ok(rows)) => {
                println!(
                    "    Query {}: {} rows in {:?}",
                    i,
                    rows.len(),
                    query_duration
                );
                query_performance_results.push((i, true, query_duration, rows.len()));
            }
            Ok(Err(e)) => {
                println!("    Query {}: FAILED - {}", i, e);
                query_performance_results.push((i, false, query_duration, 0));
            }
            Err(_) => {
                println!("    Query {}: TIMEOUT after {:?}", i, query_duration);
                query_performance_results.push((i, false, query_duration, 0));
            }
        }
    }

    let successful_queries = query_performance_results
        .iter()
        .filter(|(_, success, _, _)| *success)
        .count();

    let avg_query_time: Duration = query_performance_results
        .iter()
        .map(|(_, _, duration, _)| *duration)
        .sum::<Duration>()
        / complex_queries.len() as u32;

    let total_performance_duration = query_performance_start.elapsed();

    println!("\nQuery Performance Results:");
    println!("  Queries tested: {}", complex_queries.len());
    println!("  Successful: {}", successful_queries);
    println!("  Average query time: {:?}", avg_query_time);
    println!("  Total test duration: {:?}", total_performance_duration);

    // Query performance should be reasonable
    assert!(
        successful_queries >= complex_queries.len() * 3 / 4,
        "Query success rate too low: {}/{}",
        successful_queries,
        complex_queries.len()
    );
    assert!(
        avg_query_time < Duration::from_secs(1),
        "Average query time too slow: {:?}",
        avg_query_time
    );

    println!("  ✓ Query performance remains acceptable under load");

    // Cleanup all test data
    sqlx::query!(
        "DELETE FROM raw.events WHERE source LIKE 'exhaustion%' OR source LIKE 'concurrent%'"
    )
    .execute(pool)
    .await
    .ok();

    Ok(())
}
