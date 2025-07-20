// # Database Performance Testing
//
// Comprehensive database performance tests that measure query performance,
// index effectiveness, connection pool behavior, and database scalability.
// These tests help identify database bottlenecks and optimization opportunities.

use crate::common::prelude::*;

use crate::common::prelude::*;
use crate::common::{events, generators};
use chrono::{Duration, Utc};
use serde_json::json;
use sinex_events::{EventFactory, services, event_types};
use sinex_ulid::Ulid;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::Mutex;

/// Database performance measurement utilities
struct DatabaseMetrics {
    query_times: HashMap<String, Vec<StdDuration>>,
    connection_times: Vec<StdDuration>,
    transaction_times: Vec<StdDuration>,
    error_counts: HashMap<String, usize>,
    success_counts: HashMap<String, usize>,
    start_time: Instant,
}

impl DatabaseMetrics {
    fn new() -> Self {
        Self {
            query_times: HashMap::new(),
            connection_times: Vec::new(),
            transaction_times: Vec::new(),
            error_counts: HashMap::new(),
            success_counts: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    fn record_query(&mut self, query_name: &str, duration: StdDuration, success: bool) {
        self.query_times
            .entry(query_name.to_string())
            .or_insert_with(Vec::new)
            .push(duration);

        if success {
            *self.success_counts.entry(query_name.to_string()).or_insert(0) += 1;
        } else {
            *self.error_counts.entry(query_name.to_string()).or_insert(0) += 1;
        }
    }

    fn record_connection(&mut self, duration: StdDuration) {
        self.connection_times.push(duration);
    }

    fn record_transaction(&mut self, duration: StdDuration) {
        self.transaction_times.push(duration);
    }

    fn average_query_time(&self, query_name: &str) -> StdDuration {
        if let Some(times) = self.query_times.get(query_name) {
            if !times.is_empty() {
                return times.iter().sum::<StdDuration>() / times.len() as u32;
            }
        }
        StdDuration::from_millis(0)
    }

    fn percentile_query_time(&self, query_name: &str, percentile: f64) -> StdDuration {
        if let Some(times) = self.query_times.get(query_name) {
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

    fn query_success_rate(&self, query_name: &str) -> f64 {
        let success = self.success_counts.get(query_name).unwrap_or(&0);
        let errors = self.error_counts.get(query_name).unwrap_or(&0);
        let total = success + errors;
        if total > 0 {
            *success as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }

    fn print_summary(&self) {
        println!("\n📊 Database Performance Summary:");
        println!("Total test duration: {:?}", self.start_time.elapsed());
        
        for query_name in self.query_times.keys() {
            println!("\n🔍 Query: {}", query_name);
            println!("  - Average latency: {:?}", self.average_query_time(query_name));
            println!("  - P50 latency: {:?}", self.percentile_query_time(query_name, 50.0));
            println!("  - P95 latency: {:?}", self.percentile_query_time(query_name, 95.0));
            println!("  - P99 latency: {:?}", self.percentile_query_time(query_name, 99.0));
            println!("  - Success rate: {:.2}%", self.query_success_rate(query_name));
            
            if let Some(times) = self.query_times.get(query_name) {
                println!("  - Total executions: {}", times.len());
                println!("  - Min latency: {:?}", times.iter().min().unwrap_or(&StdDuration::from_millis(0)));
                println!("  - Max latency: {:?}", times.iter().max().unwrap_or(&StdDuration::from_millis(0)));
            }
        }

        if !self.connection_times.is_empty() {
            let avg_conn_time = self.connection_times.iter().sum::<StdDuration>() / self.connection_times.len() as u32;
            println!("\n🔌 Connection Performance:");
            println!("  - Average connection time: {:?}", avg_conn_time);
            println!("  - Total connections: {}", self.connection_times.len());
        }

        if !self.transaction_times.is_empty() {
            let avg_tx_time = self.transaction_times.iter().sum::<StdDuration>() / self.transaction_times.len() as u32;
            println!("\n💳 Transaction Performance:");
            println!("  - Average transaction time: {:?}", avg_tx_time);
            println!("  - Total transactions: {}", self.transaction_times.len());
        }
    }
}

// =============================================================================
// Query Performance Tests
// =============================================================================

/// Test performance of different query patterns
#[sinex_test]
async fn test_query_performance_patterns(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let mut metrics = DatabaseMetrics::new();
    
    // Pre-populate with test data
    let event_count = 2000;
    let test_events = generators::test_events(event_count);
    
    println!("🔄 Populating database with {} test events for query performance testing", event_count);
    
    for event in &test_events {
        sinex_db::insert_event_with_validator(pool, event, None).await?;
    }
    
    // Define comprehensive query test suite
    let query_tests = vec![
        ("Primary Key Lookup", "SELECT * FROM core.events WHERE event_id = $1::uuid"),
        ("Source Index Scan", "SELECT * FROM core.events WHERE source = $1 LIMIT 100"),
        ("Event Type Filter", "SELECT * FROM core.events WHERE event_type = $1 LIMIT 100"),
        ("Time Range Query", "SELECT * FROM core.events WHERE ts_orig >= $1 AND ts_orig <= $2"),
        ("JSON Payload Contains", "SELECT * FROM core.events WHERE payload @> $1::jsonb"),
        ("JSON Payload Key Exists", "SELECT * FROM core.events WHERE payload ? $1"),
        ("JSON Path Query", "SELECT * FROM core.events WHERE payload #> '{test}' IS NOT NULL"),
        ("Complex Filter", "SELECT * FROM core.events WHERE source = $1 AND event_type = $2 AND ts_orig >= $3"),
        ("Count Aggregation", "SELECT source, COUNT(*) FROM core.events GROUP BY source"),
        ("Time Series Aggregation", "SELECT DATE_TRUNC('hour', ts_orig) as hour, COUNT(*) FROM core.events GROUP BY hour ORDER BY hour"),
        ("Recent Events", "SELECT * FROM core.events ORDER BY ts_orig DESC LIMIT 50"),
        ("Full Text Search", "SELECT * FROM core.events WHERE payload::text ILIKE $1"),
        ("Multi-Join Query", r#"
            SELECT e1.*, e2.source as related_source 
            FROM core.events e1 
            LEFT JOIN core.events e2 ON e1.source = e2.source 
            WHERE e1.event_type = $1 LIMIT 10
        "#),
    ];
    
    // Execute query performance tests
    for (test_name, query) in query_tests {
        println!("\n🔍 Testing query: {}", test_name);
        
        // Run each query multiple times for stable measurements
        for iteration in 0..50 {
            let operation_start = Instant::now();
            
            let result = match test_name {
                "Primary Key Lookup" => {
                    let test_id = test_events[iteration % test_events.len()].id.to_uuid();
                    sqlx::query(query).bind(test_id).fetch_all(&pool).await
                }
                "Source Index Scan" => {
                    let test_source = &test_events[iteration % test_events.len()].source;
                    sqlx::query(query).bind(test_source).fetch_all(&pool).await
                }
                "Event Type Filter" => {
                    let test_type = &test_events[iteration % test_events.len()].event_type;
                    sqlx::query(query).bind(test_type).fetch_all(&pool).await
                }
                "Time Range Query" => {
                    let end_time = Utc::now();
                    let start_time = end_time - Duration::hours(1);
                    sqlx::query(query).bind(start_time).bind(end_time).fetch_all(&pool).await
                }
                "JSON Payload Contains" => {
                    sqlx::query(query).bind(json!({"test": "value"})).fetch_all(&pool).await
                }
                "JSON Payload Key Exists" => {
                    sqlx::query(query).bind("test").fetch_all(&pool).await
                }
                "JSON Path Query" => {
                    sqlx::query(query).fetch_all(&pool).await
                }
                "Complex Filter" => {
                    let test_event = &test_events[iteration % test_events.len()];
                    let time_threshold = Utc::now() - Duration::hours(1);
                    sqlx::query(query)
                        .bind(&test_event.source)
                        .bind(&test_event.event_type)
                        .bind(time_threshold)
                        .fetch_all(&pool).await
                }
                "Count Aggregation" => {
                    sqlx::query(query).fetch_all(&pool).await
                }
                "Time Series Aggregation" => {
                    sqlx::query(query).fetch_all(&pool).await
                }
                "Recent Events" => {
                    sqlx::query(query).fetch_all(&pool).await
                }
                "Full Text Search" => {
                    sqlx::query(query).bind("%test%").fetch_all(&pool).await
                }
                "Multi-Join Query" => {
                    let test_type = &test_events[iteration % test_events.len()].event_type;
                    sqlx::query(query).bind(test_type).fetch_all(&pool).await
                }
                _ => unreachable!(),
            };
            
            let duration = operation_start.elapsed();
            let success = result.is_ok();
            
            metrics.record_query(test_name, duration, success);
            
            if !success {
                println!("  ❌ Query failed: {:?}", result.err());
            } else if iteration == 0 {
                let row_count = result.unwrap().len();
                println!("  📊 Query returned {} rows", row_count);
            }
        }
        
        // Print intermediate results
        println!("  ⏱️  Average latency: {:?}", metrics.average_query_time(test_name));
        println!("  📈 P95 latency: {:?}", metrics.percentile_query_time(test_name, 95.0));
    }
    
    metrics.print_summary();
    
    // Performance assertions
    assert!(metrics.average_query_time("Primary Key Lookup") < StdDuration::from_millis(5),
        "Primary key lookups should be < 5ms");
    assert!(metrics.average_query_time("Source Index Scan") < StdDuration::from_millis(50),
        "Index scans should be < 50ms");
    assert!(metrics.average_query_time("Time Range Query") < StdDuration::from_millis(100),
        "Time range queries should be < 100ms");
    
    // All queries should have high success rates
    for query_name in metrics.query_times.keys() {
        assert!(metrics.query_success_rate(query_name) > 95.0,
            "Query '{}' should have >95% success rate", query_name);
    }
    
    println!("✅ Query performance test passed");
    Ok(())
}

/// Test database performance under concurrent load
#[sinex_test]
async fn test_concurrent_database_performance(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    let concurrent_workers = 15;
    let operations_per_worker = 100;
    let shared_metrics = Arc::new(Mutex::new(DatabaseMetrics::new()));
    
    println!("🚀 Testing concurrent database performance:");
    println!("  - Workers: {}", concurrent_workers);
    println!("  - Operations per worker: {}", operations_per_worker);
    
    let worker_handles = (0..concurrent_workers)
        .map(|worker_id| {
            let pool_clone = pool.clone();
            let metrics = shared_metrics.clone();
            
            tokio::spawn(async move {
                for op_id in 0..operations_per_worker {
                    let operation_type = op_id % 5;
                    
                    match operation_type {
                        0..=2 => {
                            // Insert operations (60%)
                            let start = Instant::now();
                            let factory = EventFactory::new(&format!("concurrent-db-worker-{}", worker_id));
                            let event = factory.create_event(
                                event_types::test::CONCURRENT_DATABASE_TEST,
                                json!({
                                    "worker_id": worker_id,
                                    "operation_id": op_id,
                                    "timestamp": Utc::now().to_rfc3339()
                                })
                            );
                            
                            let result = sinex_db::insert_event_with_validator(&pool_clone, &event, None).await;
                            let duration = start.elapsed();
                            
                            let mut metrics_lock = metrics.lock().await;
                            metrics_lock.record_query("Concurrent Insert", duration, result.is_ok());
                        }
                        3 => {
                            // Query operations (20%)
                            let start = Instant::now();
                            let result = sqlx::query!(
                                "SELECT COUNT(*) as count FROM core.events WHERE source = $1",
                                format!("concurrent-db-worker-{}", worker_id)
                            ).fetch_one(&pool_clone).await;
                            let duration = start.elapsed();
                            
                            let mut metrics_lock = metrics.lock().await;
                            metrics_lock.record_query("Concurrent Query", duration, result.is_ok());
                        }
                        4 => {
                            // Complex query operations (20%)
                            let start = Instant::now();
                            let result = sqlx::query!(
                                "SELECT source, event_type, COUNT(*) as count FROM core.events WHERE source LIKE $1 GROUP BY source, event_type",
                                format!("concurrent-db-worker-{}%", worker_id)
                            ).fetch_all(&pool_clone).await;
                            let duration = start.elapsed();
                            
                            let mut metrics_lock = metrics.lock().await;
                            metrics_lock.record_query("Concurrent Complex Query", duration, result.is_ok());
                        }
                        _ => unreachable!(),
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    
    // Wait for all workers to complete
    futures::future::join_all(worker_handles).await;
    
    let final_metrics = shared_metrics.lock().await;
    final_metrics.print_summary();
    
    // Verify database consistency
    let total_inserted = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE source LIKE 'concurrent-db-worker-%'"
    ).fetch_one(&pool).await?;
    
    println!("🔍 Database consistency check: {} events inserted", 
             total_inserted.count.unwrap_or(0));
    
    // Performance assertions
    assert!(final_metrics.average_query_time("Concurrent Insert") < StdDuration::from_millis(100),
        "Concurrent inserts should be < 100ms");
    assert!(final_metrics.average_query_time("Concurrent Query") < StdDuration::from_millis(50),
        "Concurrent queries should be < 50ms");
    assert!(final_metrics.query_success_rate("Concurrent Insert") > 95.0,
        "Concurrent insert success rate should be > 95%");
    assert!(final_metrics.query_success_rate("Concurrent Query") > 95.0,
        "Concurrent query success rate should be > 95%");
    
    println!("✅ Concurrent database performance test passed");
    Ok(())
}

/// Test database connection pool performance
#[sinex_test]
async fn test_connection_pool_performance(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let mut metrics = DatabaseMetrics::new();
    
    println!("🏊 Testing connection pool performance");
    
    // Test connection acquisition under load
    let connection_tests = 100;
    
    for i in 0..connection_tests {
        let start = Instant::now();
        
        let conn_result = pool.acquire().await;
        let acquire_duration = start.elapsed();
        
        match conn_result {
            Ok(mut conn) => {
                metrics.record_connection(acquire_duration);
                
                // Perform a simple query to verify connection
                let query_start = Instant::now();
                let query_result = sqlx::query("SELECT 1 as test").fetch_one(&mut *conn).await;
                let query_duration = query_start.elapsed();
                
                metrics.record_query("Connection Test Query", query_duration, query_result.is_ok());
                
                if i % 20 == 0 {
                    println!("  🔗 Connection {} acquired in {:?}", i + 1, acquire_duration);
                }
            }
            Err(e) => {
                println!("  ❌ Connection acquisition failed: {}", e);
            }
        }
    }
    
    // Test concurrent connection acquisition
    println!("\n🔄 Testing concurrent connection acquisition");
    
    let concurrent_connections = 20;
    let connection_handles = (0..concurrent_connections)
        .map(|conn_id| {
            let pool_clone = pool.clone();
            let metrics = Arc::new(Mutex::new(DatabaseMetrics::new()));
            
            tokio::spawn(async move {
                let start = Instant::now();
                
                match pool_clone.acquire().await {
                    Ok(mut conn) => {
                        let acquire_duration = start.elapsed();
                        
                        // Hold connection briefly and perform operations
                        for op in 0..5 {
                            let query_start = Instant::now();
                            let result = sqlx::query("SELECT $1 as value")
                                .bind(format!("conn-{}-op-{}", conn_id, op))
                                .fetch_one(&mut *conn)
                                .await;
                            let query_duration = query_start.elapsed();
                            
                            let mut metrics_lock = metrics.lock().await;
                            metrics_lock.record_query("Concurrent Connection Query", query_duration, result.is_ok());
                        }
                        
                        let mut metrics_lock = metrics.lock().await;
                        metrics_lock.record_connection(acquire_duration);
                        
                        (true, metrics_lock.connection_times.clone(), metrics_lock.query_times.clone())
                    }
                    Err(e) => {
                        println!("  ❌ Concurrent connection {} failed: {}", conn_id, e);
                        (false, Vec::new(), HashMap::new())
                    }
                }
            })
        });
    
    // Collect results from concurrent connections
    let results = futures::future::join_all(connection_handles).await;
    
    let mut successful_connections = 0;
    let mut total_connection_times = Vec::new();
    
    for result in results {
        if let Ok((success, conn_times, _query_times)) = result {
            if success {
                successful_connections += 1;
                total_connection_times.extend(conn_times);
            }
        }
    }
    
    println!("  ✅ Successful concurrent connections: {}/{}", 
             successful_connections, concurrent_connections);
    
    if !total_connection_times.is_empty() {
        let avg_concurrent_conn_time = total_connection_times.iter().sum::<StdDuration>() 
            / total_connection_times.len() as u32;
        println!("  ⏱️  Average concurrent connection time: {:?}", avg_concurrent_conn_time);
    }
    
    metrics.print_summary();
    
    // Performance assertions
    let avg_connection_time = if !metrics.connection_times.is_empty() {
        metrics.connection_times.iter().sum::<StdDuration>() / metrics.connection_times.len() as u32
    } else {
        StdDuration::from_millis(0)
    };
    
    assert!(avg_connection_time < StdDuration::from_millis(50),
        "Average connection acquisition should be < 50ms");
    assert!(successful_connections as f64 / concurrent_connections as f64 > 0.95,
        "Concurrent connection success rate should be > 95%");
    assert!(metrics.query_success_rate("Connection Test Query") > 95.0,
        "Connection test queries should have > 95% success rate");
    
    println!("✅ Connection pool performance test passed");
    Ok(())
}

/// Test transaction performance and isolation
#[sinex_test]
async fn test_transaction_performance(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let mut metrics = DatabaseMetrics::new();
    
    println!("💳 Testing transaction performance");
    
    // Test simple transactions
    let transaction_count = 50;
    
    for i in 0..transaction_count {
        let tx_start = Instant::now();
        
        let mut tx = pool.begin().await?;
        
        // Perform multiple operations within transaction
        let factory = EventFactory::new("transaction-test");
        let event1 = factory.create_event(
            event_types::test::TRANSACTION_PERFORMANCE_TEST_1,
            json!({"transaction_id": i, "operation": 1})
        );
        
        let event2 = factory.create_event(
            event_types::test::TRANSACTION_PERFORMANCE_TEST_2,
            json!({"transaction_id": i, "operation": 2})
        );
        
        // Insert both events in the same transaction
        let insert1 = sinex_db::insert_event_with_validator(&mut tx, &event1, None).await;
        let insert2 = sinex_db::insert_event_with_validator(&mut tx, &event2, None).await;
        
        let commit_result = if insert1.is_ok() && insert2.is_ok() {
            tx.commit().await
        } else {
            tx.rollback().await
        };
        
        let tx_duration = tx_start.elapsed();
        metrics.record_transaction(tx_duration);
        
        let success = insert1.is_ok() && insert2.is_ok() && commit_result.is_ok();
        metrics.record_query("Transaction", tx_duration, success);
        
        if i % 10 == 0 {
            println!("  💳 Transaction {} completed in {:?}", i + 1, tx_duration);
        }
    }
    
    // Test concurrent transactions
    println!("\n🔄 Testing concurrent transactions");
    
    let concurrent_transactions = 10;
    let tx_handles = (0..concurrent_transactions)
        .map(|tx_id| {
            let pool_clone = pool.clone();
            
            tokio::spawn(async move {
                let tx_start = Instant::now();
                
                let mut tx = pool_clone.begin().await?;
                
                // Perform operations that might conflict
                for op in 0..3 {
                    let factory = EventFactory::new(&format!("concurrent-tx-{}", tx_id));
                    let event = factory.create_event(
                        event_types::test::CONCURRENT_TRANSACTION_TEST,
                        json!({
                            "transaction_id": tx_id,
                            "operation_id": op,
                            "timestamp": Utc::now().to_rfc3339()
                        })
                    );
                    
                    sinex_db::insert_event_with_validator(&mut tx, &event, None).await?;
                }
                
                tx.commit().await?;
                
                let tx_duration = tx_start.elapsed();
                Ok::<(usize, StdDuration), sqlx::Error>((tx_id, tx_duration))
            })
        });
    
    let tx_results = futures::future::join_all(tx_handles).await;
    let mut successful_transactions = 0;
    let mut concurrent_tx_times = Vec::new();
    
    for result in tx_results {
        match result {
            Ok(Ok((tx_id, duration))) => {
                successful_transactions += 1;
                concurrent_tx_times.push(duration);
                println!("  ✅ Concurrent transaction {} completed in {:?}", tx_id, duration);
            }
            Ok(Err(e)) => {
                println!("  ❌ Transaction failed: {}", e);
            }
            Err(e) => {
                println!("  ❌ Transaction task failed: {}", e);
            }
        }
    }
    
    println!("  📊 Successful concurrent transactions: {}/{}", 
             successful_transactions, concurrent_transactions);
    
    metrics.print_summary();
    
    // Verify database consistency
    let tx_event_count = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE source LIKE 'concurrent-tx-%' OR source = 'transaction-test'"
    ).fetch_one(&pool).await?;
    
    println!("🔍 Transaction consistency check: {} events inserted", 
             tx_event_count.count.unwrap_or(0));
    
    // Performance assertions
    let avg_tx_time = if !metrics.transaction_times.is_empty() {
        metrics.transaction_times.iter().sum::<StdDuration>() / metrics.transaction_times.len() as u32
    } else {
        StdDuration::from_millis(0)
    };
    
    assert!(avg_tx_time < StdDuration::from_millis(200),
        "Average transaction time should be < 200ms");
    assert!(successful_transactions as f64 / concurrent_transactions as f64 > 0.9,
        "Concurrent transaction success rate should be > 90%");
    assert!(metrics.query_success_rate("Transaction") > 95.0,
        "Transaction success rate should be > 95%");
    
    println!("✅ Transaction performance test passed");
    Ok(())
}
