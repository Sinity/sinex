//! Example tests showing the migration from old to new test infrastructure

use crate::common::prelude::*;

// OLD WAY - Using TestDatabase (300ms+ overhead per test)
#[tokio::test]
async fn old_way_test_database() -> TestResult {
    use crate::common::test_database::TestDatabase;
    
    let start = std::time::Instant::now();
    let test_db = TestDatabase::create("old_way_example").await?;
    let setup_time = start.elapsed();
    
    // Use the database
    let event = RawEventBuilder::new("test", "example", json!({"old": "way"})).build();
    queries::insert_event(&test_db.pool, &event).await?;
    
    println!("Old way setup time: {:?}", setup_time);
    assert!(setup_time > Duration::from_millis(100)); // Slow!
    
    Ok(())
}

// NEW WAY - Using universal pool with #[sinex_test] (5-20ms overhead)
#[sinex_test]
async fn new_way_pooled_database(ctx: TestContext) -> TestResult {
    let start = std::time::Instant::now();
    
    // Database is already available via ctx!
    let event = ctx.filesystem_event("/test/new_way.txt");
    ctx.insert_event(&event).await?;
    
    let total_time = start.elapsed();
    println!("New way total time: {:?}", total_time);
    assert!(total_time < Duration::from_millis(50)); // Fast!
    
    Ok(())
}

// Example: Simple test with database
#[sinex_test]
async fn test_event_insertion_simple(ctx: TestContext) -> TestResult {
    // Create and insert event
    let event = ctx.filesystem_event("/test/file.txt");
    ctx.insert_event(&event).await?;
    
    // Verify it was inserted
    let count = ctx.event_count().await?;
    assert_eq!(count, 1);
    
    Ok(())
}

// Example: Test with multiple operations
#[sinex_test]
async fn test_multiple_operations(ctx: TestContext) -> TestResult {
    // Insert multiple events
    for i in 0..10 {
        let event = ctx.terminal_event(&format!("command {}", i));
        ctx.insert_event(&event).await?;
    }
    
    // Wait for them to be processed
    ctx.wait_for_event_count(10).await?;
    
    // Query and verify
    let events = ctx.query_events().await?;
    assert_eq!(events.len(), 10);
    
    Ok(())
}

// Example: Test without TestContext (simple tests)
#[tokio::test]
async fn test_without_context() -> Result<(), Box<dyn std::error::Error>> {
    // For tests that don't need database, just use regular #[tokio::test]
    let result = 2 + 2;
    assert_eq!(result, 4);
    Ok(())
}

// Example: Direct pool usage (advanced)
#[tokio::test]
async fn test_direct_pool_usage() -> TestResult {
    use crate::common::database_pool;
    
    // Acquire database directly
    let db = database_pool::acquire_database().await?;
    
    // Use the pool
    let event = RawEventBuilder::new("direct", "test", json!({"example": true})).build();
    queries::insert_event(db.pool(), &event).await?;
    
    // Database automatically returned to pool on drop
    drop(db);
    
    Ok(())
}

// Example: Performance comparison
#[tokio::test] 
async fn test_performance_comparison() -> TestResult {
    use std::time::Instant;
    
    // Time old approach (if still available)
    let old_start = Instant::now();
    let old_db = crate::common::test_database::TestDatabase::create("perf_old").await?;
    let old_time = old_start.elapsed();
    drop(old_db);
    
    // Time new approach
    let new_start = Instant::now();
    let new_db = crate::common::database_pool::acquire_database().await?;
    let new_time = new_start.elapsed();
    drop(new_db);
    
    println!("Performance comparison:");
    println!("  Old approach: {:?}", old_time);
    println!("  New approach: {:?}", new_time);
    println!("  Speedup: {:.1}x", old_time.as_millis() as f64 / new_time.as_millis() as f64);
    
    // New approach should be much faster
    assert!(new_time < old_time / 10);
    
    Ok(())
}
