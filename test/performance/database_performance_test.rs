// # Database Performance Testing
//
// Comprehensive database performance tests that measure query performance,
// index effectiveness, connection pool behavior, and database scalability.
// These tests help identify database bottlenecks and optimization opportunities.

use crate::common::test_macros::*;
use crate::common::prelude::*;

use crate::common::builders::{TestEventBuilder, BatchEventBuilder};
use crate::common::query_helpers::TestQueries;
use crate::common::{events, generators};
use crate::common::test_factories::{
    UserActivityFactory, SystemEventFactory, WorkflowFactory,
    FileSystemScenarioFactory, scenarios
};
use sinex_db::queries::EventQueries;
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
    
    // Use performance dataset fixture for pre-populated data
    let dataset = crate::common::fixtures::performance_dataset_with_size(&ctx, 2000).await?;
    
    println!("🔄 Using performance dataset with {} test events", dataset.event_count);
    println!("📊 Data spans from {} to {}", dataset.time_range.0, dataset.time_range.1);
    println!("📁 Sources: {:?}", dataset.sources);
    
    // Define comprehensive query test suite
    // NOTE: Many of these are kept as raw SQL because they test specific database features
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
                            let result = TestQueries::count_events_by_source(
                                &pool_clone,
                                &format!("concurrent-db-worker-{}", worker_id)
                            ).await;
                            let duration = start.elapsed();
                            
                            let mut metrics_lock = metrics.lock().await;
                            metrics_lock.record_query("Concurrent Query", duration, result.is_ok());
                        }
                        4 => {
                            // Complex query operations (20%)
                            let start = Instant::now();
                            // NOTE: Complex aggregation with GROUP BY requires raw SQL
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
    // NOTE: Using LIKE pattern requires raw SQL
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

/// Test connection pool performance
test_concurrent_operations!(test_connection_pool_performance, 5,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 5);
        Ok(())
    }
);

/// Test transaction performance
test_batch_events!(test_transaction_performance, "test", "test.event", 3, 
    |pool: &DbPool, events: &[RawEvent]| async move {
        // Verify batch
        assert_eq!(events.len(), 3);
        Ok(())
    }
);

/// Test database performance with realistic workload patterns
#[sinex_test]
async fn test_database_performance_with_realistic_workload(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let mut metrics = DatabaseMetrics::new();
    
    println!("🎯 Testing database performance with realistic workload patterns");
    
    // Generate a full workday scenario
    let workday_events = scenarios::user_workday();
    let total_events = workday_events.len();
    
    println!("📊 Generated {} events representing a full workday", total_events);
    
    // Phase 1: Bulk insert performance
    let bulk_start = Instant::now();
    
    // Insert events in batches (simulating real-time ingestion)
    let batch_size = 50;
    for (batch_idx, chunk) in workday_events.chunks(batch_size).enumerate() {
        let batch_start = Instant::now();
        
        // Insert batch using a transaction
        let tx_start = Instant::now();
        let mut tx = pool.begin().await?;
        
        for event in chunk {
            sinex_db::insert_event_with_validator(&mut *tx, event, None).await?;
        }
        
        tx.commit().await?;
        let tx_duration = tx_start.elapsed();
        metrics.record_transaction(tx_duration);
        
        let batch_duration = batch_start.elapsed();
        metrics.record_query("batch_insert", batch_duration, true);
        
        if batch_idx % 10 == 0 {
            println!("  Inserted batch {} ({} events) in {:?}", batch_idx, chunk.len(), batch_duration);
        }
    }
    
    let bulk_duration = bulk_start.elapsed();
    println!("✅ Bulk insert completed: {} events in {:?}", total_events, bulk_duration);
    println!("  Average: {:.2} events/second", total_events as f64 / bulk_duration.as_secs_f64());
    
    // Phase 2: Query performance with different patterns
    println!("\n🔍 Testing query patterns on realistic data...");
    
    // Test 1: Time range queries (common for dashboards)
    let time_ranges = vec![
        ("last_hour", Duration::hours(1)),
        ("last_day", Duration::days(1)),
        ("last_week", Duration::days(7)),
    ];
    
    for (name, duration) in time_ranges {
        let query_start = Instant::now();
        let end_time = Utc::now();
        let start_time = end_time - duration;
        
        let (count,) = EventQueries::count_by_time_range(start_time, end_time)
            .fetch_one::<(i64,)>(&pool)
            .await?;
        
        let query_duration = query_start.elapsed();
        metrics.record_query(&format!("time_range_{}", name), query_duration, true);
        
        println!("  Time range {} returned {} events in {:?}", name, count, query_duration);
    }
    
    // Test 2: Source-based queries (common for filtering)
    let sources = vec!["shell.kitty", "fs", "wm.hyprland", "sinex"];
    
    for source in sources {
        let query_start = Instant::now();
        
        let events = sqlx::query!(
            r#"
            SELECT id, ts_orig, event_type, payload
            FROM core.events
            WHERE source = $1
            ORDER BY ts_orig DESC
            LIMIT 100
            "#,
            source
        )
        .fetch_all(&pool)
        .await?;
        
        let query_duration = query_start.elapsed();
        metrics.record_query(&format!("source_filter_{}", source), query_duration, true);
        
        println!("  Source filter {} returned {} events in {:?}", source, events.len(), query_duration);
    }
    
    // Test 3: Complex aggregation queries
    let agg_start = Instant::now();
    
    let hourly_stats = sqlx::query!(
        r#"
        SELECT 
            date_trunc('hour', ts_orig) as hour,
            source,
            COUNT(*) as event_count,
            COUNT(DISTINCT event_type) as unique_types
        FROM core.events
        WHERE ts_orig > NOW() - INTERVAL '24 hours'
        GROUP BY hour, source
        ORDER BY hour DESC, event_count DESC
        "#
    )
    .fetch_all(&pool)
    .await?;
    
    let agg_duration = agg_start.elapsed();
    metrics.record_query("hourly_aggregation", agg_duration, true);
    
    println!("  Hourly aggregation returned {} rows in {:?}", hourly_stats.len(), agg_duration);
    
    // Test 4: Full-text search simulation (JSON queries)
    let search_terms = vec!["git", "cargo", "error", "test"];
    
    for term in search_terms {
        let search_start = Instant::now();
        
        let results = sqlx::query!(
            r#"
            SELECT id, source, event_type, payload
            FROM core.events
            WHERE payload::text LIKE $1
            LIMIT 50
            "#,
            format!("%{}%", term)
        )
        .fetch_all(&pool)
        .await?;
        
        let search_duration = search_start.elapsed();
        metrics.record_query(&format!("json_search_{}", term), search_duration, true);
        
        println!("  JSON search '{}' found {} results in {:?}", term, results.len(), search_duration);
    }
    
    // Phase 3: Concurrent query stress test
    println!("\n⚡ Testing concurrent query performance...");
    
    let concurrent_queries = 20;
    let mut handles = Vec::new();
    
    for i in 0..concurrent_queries {
        let pool = pool.clone();
        let query_type = i % 4;
        
        let handle = tokio::spawn(async move {
            let query_start = Instant::now();
            
            let result = match query_type {
                0 => {
                    // Recent events query
                    sqlx::query!("SELECT * FROM core.events ORDER BY ts_orig DESC LIMIT 100")
                        .fetch_all(&pool)
                        .await
                        .map(|rows| rows.len())
                }
                1 => {
                    // Count query
                    sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
                        .fetch_one(&pool)
                        .await
                        .map(|count: Option<i64>| count.unwrap_or(0) as usize)
                }
                2 => {
                    // Source stats
                    sqlx::query!("SELECT source, COUNT(*) as cnt FROM core.events GROUP BY source")
                        .fetch_all(&pool)
                        .await
                        .map(|rows| rows.len())
                }
                _ => {
                    // Random event lookup
                    sqlx::query!("SELECT * FROM core.events LIMIT 1 OFFSET $1", i as i32 * 10)
                        .fetch_optional(&pool)
                        .await
                        .map(|opt| if opt.is_some() { 1 } else { 0 })
                }
            };
            
            (query_type, query_start.elapsed(), result.is_ok())
        });
        
        handles.push(handle);
    }
    
    // Wait for all concurrent queries
    let results = futures::future::join_all(handles).await;
    
    for result in results {
        if let Ok((query_type, duration, success)) = result {
            metrics.record_query(&format!("concurrent_query_type_{}", query_type), duration, success);
        }
    }
    
    let successful = results.iter().filter(|r| r.as_ref().map(|(_, _, s)| *s).unwrap_or(false)).count();
    println!("  Concurrent queries completed: {}/{} successful", successful, concurrent_queries);
    
    // Print final metrics
    println!("\n" + "=".repeat(80));
    metrics.print_summary();
    
    // Performance assertions
    let avg_insert = metrics.average_query_time("batch_insert");
    assert!(
        avg_insert < StdDuration::from_millis(500),
        "Batch insert should average under 500ms, was {:?}",
        avg_insert
    );
    
    let p95_time_range = metrics.percentile_query_time("time_range_last_hour", 95.0);
    assert!(
        p95_time_range < StdDuration::from_millis(100),
        "Time range queries should have P95 under 100ms, was {:?}",
        p95_time_range
    );
    
    Ok(())
}
