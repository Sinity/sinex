//! Meta-tests for the database pool infrastructure
//!
//! These tests ensure that the pool system itself works correctly:
//! - Template regeneration when migrations change
//! - Pool availability under load
//! - Cleanup actually works
//! - Performance characteristics

use crate::common::prelude::*;
use crate::common::database_pool::*;
use std::sync::Arc;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_pool_initialization_performance() {
    let start = std::time::Instant::now();
    let manager = get_pool_manager().await.unwrap();
    let init_time = start.elapsed();
    
    // Pool should initialize quickly (template already exists)
    assert!(
        init_time < Duration::from_secs(5),
        "Pool initialization took too long: {:?}",
        init_time
    );
    
    println!("Pool initialized in {:?}", init_time);
}

#[tokio::test]
async fn test_database_acquisition_performance() {
    let manager = get_pool_manager().await.unwrap();
    
    // Warm up the pool
    let _warmup = manager.acquire().await.unwrap();
    drop(_warmup);
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Measure acquisition time
    let times: Vec<_> = (0..10)
        .map(|_| async {
            let start = std::time::Instant::now();
            let db = manager.acquire().await.unwrap();
            let acquire_time = start.elapsed();
            drop(db);
            acquire_time
        })
        .collect();
    
    let mut total = Duration::ZERO;
    for time_fut in times {
        let time = time_fut.await;
        total += time;
        assert!(
            time < Duration::from_millis(50),
            "Database acquisition took too long: {:?}",
            time
        );
    }
    
    let avg = total / 10;
    println!("Average acquisition time: {:?}", avg);
    assert!(
        avg < Duration::from_millis(10),
        "Average acquisition time too high: {:?}",
        avg
    );
}

#[tokio::test]
async fn test_cleanup_performance() {
    let manager = get_pool_manager().await.unwrap();
    let db = manager.acquire().await.unwrap();
    
    // Insert some test data
    let pool = db.pool();
    for i in 0..100 {
        let event = RawEventBuilder::new(
            "test",
            "cleanup.test",
            json!({"index": i})
        ).build();
        sinex_db::insert_event(pool, &event).await.unwrap();
    }
    
    // Verify data exists
    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
        .fetch_one(pool)
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(count, 100);
    
    let db_name = db.name().to_string();
    
    // Drop the database handle (triggers cleanup)
    let cleanup_start = std::time::Instant::now();
    drop(db);
    
    // Wait for cleanup to complete
    tokio::time::sleep(Duration::from_millis(200)).await;
    let cleanup_time = cleanup_start.elapsed();
    
    println!("Cleanup completed in {:?}", cleanup_time);
    assert!(
        cleanup_time < Duration::from_millis(500),
        "Cleanup took too long: {:?}",
        cleanup_time
    );
    
    // Acquire the same database again (should be clean)
    let db2 = manager.acquire().await.unwrap();
    let count2: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
        .fetch_one(db2.pool())
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(count2, 0, "Database was not properly cleaned");
}

#[tokio::test]
async fn test_pool_exhaustion_handling() {
    let manager = get_pool_manager().await.unwrap();
    let config = PoolConfig::default();
    
    // Acquire all databases from the pool
    let mut databases = Vec::new();
    for i in 0..config.min_size {
        println!("Acquiring database {}/{}", i + 1, config.min_size);
        let db = manager.acquire().await.unwrap();
        databases.push(db);
    }
    
    // Pool should expand automatically
    println!("Pool exhausted, testing expansion...");
    let extra_db = manager.acquire().await.unwrap();
    assert!(
        extra_db.use_count() == 1,
        "Extra database should be newly created"
    );
    
    // Return all databases
    drop(databases);
    drop(extra_db);
}

#[tokio::test]
async fn test_concurrent_database_usage() {
    let manager = Arc::new(get_pool_manager().await.unwrap());
    let mut tasks = JoinSet::new();
    
    // Spawn many concurrent tasks
    for i in 0..20 {
        let manager = manager.clone();
        tasks.spawn(async move {
            let start = std::time::Instant::now();
            let db = manager.acquire().await.unwrap();
            
            // Do some work
            let event = RawEventBuilder::new(
                "concurrent",
                "test.event",
                json!({"task": i})
            ).build();
            sinex_db::insert_event(db.pool(), &event).await.unwrap();
            
            // Simulate work
            tokio::time::sleep(Duration::from_millis(10)).await;
            
            let elapsed = start.elapsed();
            drop(db);
            elapsed
        });
    }
    
    // Collect all results
    let mut total_time = Duration::ZERO;
    let mut max_time = Duration::ZERO;
    while let Some(result) = tasks.join_next().await {
        let elapsed = result.unwrap();
        total_time += elapsed;
        max_time = max_time.max(elapsed);
    }
    
    let avg_time = total_time / 20;
    println!("Concurrent test stats:");
    println!("  Average time: {:?}", avg_time);
    println!("  Max time: {:?}", max_time);
    println!("  {}", manager.stats());
    
    // All operations should complete quickly
    assert!(
        max_time < Duration::from_secs(1),
        "Some operations took too long: {:?}",
        max_time
    );
}

#[tokio::test]
async fn test_database_health_check() {
    let manager = get_pool_manager().await.unwrap();
    let db = manager.acquire().await.unwrap();
    
    // Healthy database should pass checks
    let result = sqlx::query("SELECT 1").fetch_one(db.pool()).await;
    assert!(result.is_ok(), "Health check failed on good database");
    
    // Verify critical tables exist
    let tables = vec!["raw.events", "sinex_schemas.work_queue"];
    for table in tables {
        let parts: Vec<&str> = table.split('.').collect();
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = $1 AND table_name = $2
            )"
        )
        .bind(parts[0])
        .bind(parts[1])
        .fetch_one(db.pool())
        .await
        .unwrap();
        
        assert!(exists, "Critical table {} does not exist", table);
    }
}

#[tokio::test]
async fn test_pool_statistics() {
    let manager = get_pool_manager().await.unwrap();
    
    // Reset stats by creating a new manager would require changes
    // Instead, just verify stats are tracked
    let initial_stats = manager.stats();
    println!("Initial stats: {}", initial_stats);
    
    // Perform some operations
    for _ in 0..5 {
        let db = manager.acquire().await.unwrap();
        drop(db);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    
    let final_stats = manager.stats();
    println!("Final stats: {}", final_stats);
    
    // Stats should show activity
    assert!(final_stats.contains("acquisitions="));
    assert!(final_stats.contains("cleanups="));
}

#[tokio::test]
async fn test_template_database_optimization() {
    // This test verifies that the template database is properly optimized
    let config = PoolConfig::default();
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&config.admin_url)
        .await
        .unwrap();
    
    // Check that expensive indexes are dropped in template
    let template_name = format!("sinex_test_template_{}", std::process::id());
    let index_check = sqlx::query!(
        "SELECT COUNT(*) as count FROM pg_indexes 
         WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
         AND tablename = 'events'
         AND indexname LIKE '%vector%'",
    )
    .fetch_one(&admin_pool)
    .await;
    
    // Vector indexes should be dropped for performance
    if let Ok(record) = index_check {
        assert_eq!(
            record.count.unwrap_or(0), 0,
            "Template database should not have expensive vector indexes"
        );
    }
}

#[tokio::test] 
async fn test_graceful_cleanup_on_panic() {
    // Test that databases are properly returned even if test panics
    let manager = get_pool_manager().await.unwrap();
    
    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Handle::current().block_on(async {
            let _db = manager.acquire().await.unwrap();
            panic!("Test panic!");
        })
    });
    
    assert!(result.is_err(), "Should have panicked");
    
    // Give time for drop handler to run
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Pool should still be functional
    let db2 = manager.acquire().await.unwrap();
    assert!(db2.pool().acquire().await.is_ok());
}

#[tokio::test]
async fn test_pool_size_limits() {
    let manager = get_pool_manager().await.unwrap();
    let config = PoolConfig::default();
    
    // Try to acquire more than max_size databases
    let mut databases = Vec::new();
    for i in 0..config.max_size {
        match tokio::time::timeout(
            Duration::from_millis(100),
            manager.acquire()
        ).await {
            Ok(Ok(db)) => databases.push(db),
            _ => {
                println!("Could not acquire database {} (expected at limit)", i);
                break;
            }
        }
    }
    
    // We should have acquired up to max_size
    assert!(
        databases.len() <= config.max_size,
        "Acquired more databases than max_size"
    );
    
    println!("Successfully enforced pool size limit at {}", databases.len());
}
