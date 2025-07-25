//! Integration tests for the test framework itself
//! 
//! These tests validate that the test framework infrastructure works correctly,
//! including TestContext isolation, database pool management, fixtures, and mocks.

use sinex_test_utils::prelude::*;
use sinex_error::CoreError;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

// Test Context State Management

#[sinex_test]
async fn test_context_provides_isolation(ctx: TestContext) -> TestResult<()> {
    // Create events in this context
    ctx.event()
        .source("test-isolation")
        .type_("marker")
        .field("context", ctx.test_name())
        .insert()
        .await?;
    
    // Create a second context
    let ctx2 = TestContext::with_name("other_test").await?;
    
    // Second context should not see first context's events
    let other_events = ctx2.events()
        .by_source("test-isolation")
        .count()
        .await?;
    assert_eq!(other_events, 0);
    
    // Original context should still see its event
    let our_events = ctx.events()
        .by_source("test-isolation")
        .count()
        .await?;
    assert_eq!(our_events, 1);
    
    Ok(())
}

#[sinex_test]
async fn test_context_tracks_event_count(ctx: TestContext) -> TestResult<()> {
    assert_eq!(ctx.test_event_count().await, 0);
    
    // Insert events and verify count
    for i in 1..=5 {
        ctx.event()
            .source("count-test")
            .type_("increment")
            .field("index", i)
            .insert()
            .await?;
        
        assert_eq!(ctx.test_event_count().await, i);
    }
    
    Ok(())
}

#[sinex_test]
async fn test_context_timing_measurement(ctx: TestContext) -> TestResult<()> {
    let start = ctx.elapsed();
    
    // Do some work
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    let end = ctx.elapsed();
    assert!(end > start);
    assert!(end.as_millis() >= 50);
    
    // Test measure helper
    let (result, duration) = ctx.measure(async {
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        Ok::<_, CoreError>("done")
    }).await?;
    
    assert_eq!(result?, "done");
    assert!(duration.as_millis() >= 25);
    
    Ok(())
}

// Database Pool Management

#[sinex_test]
async fn test_database_pool_provides_connection(ctx: TestContext) -> TestResult<()> {
    // Direct pool access should work
    let result: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(ctx.pool())
        .await?;
    assert_eq!(result, 1);
    
    Ok(())
}

#[sinex_test]
async fn test_concurrent_context_allocation(_ctx: TestContext) -> TestResult<()> {
    let success_count = Arc::new(AtomicU32::new(0));
    
    // Try to allocate multiple contexts concurrently
    let mut handles = vec![];
    for i in 0..5 {
        let counter = success_count.clone();
        let handle = tokio::spawn(async move {
            match TestContext::with_name(&format!("concurrent_{}", i)).await {
                Ok(ctx) => {
                    // Do some work
                    ctx.event()
                        .source("concurrent")
                        .type_("test")
                        .insert()
                        .await?;
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
                Err(e) => Err(e)
            }
        });
        handles.push(handle);
    }
    
    // Wait for all
    for handle in handles {
        let _ = handle.await;
    }
    
    assert!(success_count.load(Ordering::SeqCst) > 0);
    
    Ok(())
}

// Fixture Tests

#[sinex_test]
async fn test_fixture_lazy_loading(ctx: TestContext) -> TestResult<()> {
    let initial_count = ctx.test_event_count().await;
    
    // Getting scenarios handle shouldn't create events
    let scenarios = ctx.scenarios();
    assert_eq!(ctx.test_event_count().await, initial_count);
    
    // Actually accessing a fixture should create events
    let _fixture = scenarios.user_session().await?;
    assert!(ctx.test_event_count().await > initial_count);
    
    Ok(())
}

#[sinex_test]
async fn test_fixture_caching(ctx: TestContext) -> TestResult<()> {
    let scenarios = ctx.scenarios();
    
    // First access
    let fixture1 = scenarios.user_session().await?;
    let count_after_first = ctx.test_event_count().await;
    
    // Second access should return cached fixture
    let fixture2 = scenarios.user_session().await?;
    let count_after_second = ctx.test_event_count().await;
    
    // No new events should be created
    assert_eq!(count_after_first, count_after_second);
    
    // Should be same fixture data
    let data1 = fixture1.resource().await;
    let data2 = fixture2.resource().await;
    assert_eq!(
        data1.as_ref().unwrap().user_id,
        data2.as_ref().unwrap().user_id
    );
    
    Ok(())
}

// Mock Infrastructure Tests

#[sinex_test]
async fn test_mock_isolation(ctx: TestContext) -> TestResult<()> {
    let mocks = ctx.mocks();
    let fs = mocks.filesystem();
    
    // Create a file
    fs.create_file(std::path::Path::new("/test.txt"), b"content").await?;
    assert!(fs.exists(std::path::Path::new("/test.txt")).await);
    
    // Another context should have isolated mocks
    let ctx2 = TestContext::with_name("mock_test_2").await?;
    let fs2 = ctx2.mocks().filesystem();
    
    // Should not see the file
    assert!(!fs2.exists(std::path::Path::new("/test.txt")).await);
    
    Ok(())
}

#[sinex_test]
async fn test_mock_state_persistence(ctx: TestContext) -> TestResult<()> {
    let mocks = ctx.mocks();
    let redis = mocks.redis();
    let mut conn = redis.connect().await?;
    
    // Set values
    conn.set("key1", "value1").await?;
    conn.set("key2", "value2").await?;
    
    // Use the same connection to verify state persistence
    assert_eq!(conn.get::<String>("key1".to_string()).await?, Some("value1".to_string()));
    assert_eq!(conn.get::<String>("key2".to_string()).await?, Some("value2".to_string()));
    
    Ok(())
}

// Assertion Helpers

#[sinex_test]
async fn test_assertion_helpers(ctx: TestContext) -> TestResult<()> {
    // Test various assertion helpers
    ctx.assert("equality").eq(&5, &5)?;
    
    let vec = vec![1, 2, 3];
    ctx.assert("size").has_size(&vec, 3)?;
    ctx.assert("not empty").not_empty(&vec)?;
    
    let some_val = Some(42);
    ctx.assert("some").some(&some_val)?;
    
    let none_val: Option<i32> = None;
    ctx.assert("none").none(&none_val)?;
    
    Ok(())
}

// Event Builder Tests

#[sinex_test]
async fn test_event_builder_flexibility(ctx: TestContext) -> TestResult<()> {
    // Test that builder methods can be called in any order
    let event1 = ctx.event()
        .type_("test")
        .source("builder1")
        .field("a", 1)
        .insert()
        .await?;
    
    let event2 = ctx.event()
        .field("a", 1)
        .source("builder2")
        .type_("test")
        .insert()
        .await?;
    
    assert_eq!(event1.event_type, "test");
    assert_eq!(event2.event_type, "test");
    
    Ok(())
}

// Query Builder Tests

#[sinex_test]
async fn test_query_builder_chaining(ctx: TestContext) -> TestResult<()> {
    // Insert test data
    for i in 0..10 {
        ctx.event()
            .source("query-test")
            .type_(if i < 5 { "type.a" } else { "type.b" })
            .field("index", i)
            .insert()
            .await?;
    }
    
    // Test various query combinations
    let by_source = ctx.events()
        .by_source("query-test")
        .count()
        .await?;
    assert_eq!(by_source, 10);
    
    let by_type_a = ctx.events()
        .by_type("type.a")
        .count()
        .await?;
    assert_eq!(by_type_a, 5);
    
    let limited = ctx.events()
        .by_source("query-test")
        .limit(3)
        .fetch()
        .await?;
    assert_eq!(limited.len(), 3);
    
    Ok(())
}