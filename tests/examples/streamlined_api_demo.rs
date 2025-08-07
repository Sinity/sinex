//! Demonstration of the streamlined test infrastructure API
//!
//! This showcases how the consolidated TestContext provides a clean, unified API
//! for all test operations while maintaining access to all advanced functionality.

use sinex_test_utils::prelude::*;

#[sinex_test]
async fn demo_streamlined_event_creation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // DIRECT API - Using the new streamlined approach
    let event = ctx.create_test_event(
        "fs-watcher",
        "file.created",
        json!({
            "path": "/tmp/demo.txt",
            "size": 1024
        })
    ).await?;
    
    println!("Created event: {}", event.id);
    
    // ERROR TESTING - Test validation at EventSource level
    let result = std::panic::catch_unwind(|| {
        EventSource::new("")  // Invalid empty source
    });
    // Validation happens at type construction level
    
    // BATCH API - For performance testing, create multiple events
    let mut batch = Vec::new();
    for i in 0..5 {
        let event = ctx.create_test_event(
            "fs-watcher",
            "file.created",
            json!({
                "path": format!("/tmp/batch_{}.txt", i)
            })
        ).await?;
        batch.push(event);
    }
    
    assert_eq!(batch.len(), 5);
    
    Ok(())
}

#[sinex_test] 
async fn demo_streamlined_querying(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create some test data
    ctx.create_test_event("fs-watcher", "file.created", json!({"path": "/demo1.txt"})).await?;
    ctx.create_test_event("fs-watcher", "file.created", json!({"path": "/demo2.txt"})).await?;
    ctx.create_test_event("terminal", "command.executed", json!({"command": "ls", "exit_code": 0})).await?;
    
    // QUERY API - Using repository pattern
    let fs_events = ctx.pool.events()
        .by_source("fs-watcher")
        .limit(10)
        .fetch()
        .await?;
    
    assert_eq!(fs_events.len(), 2);
    
    // Simple query operations
    let total_count = ctx.pool.events().count().await?;
    assert_eq!(total_count, 3);
    
    Ok(())
}

#[sinex_test]
async fn demo_streamlined_fixtures(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // SCENARIO FIXTURES - Organized by purpose
    let session = ctx.scenarios()
        .user_session()
        .await?;
    
    println!("User session has {} events", session.event_ids.len());
    
    let checkpoints = ctx.scenarios()
        .populated_checkpoints()
        .await?;
        
    println!("Created {} checkpoints", checkpoints.checkpoint_ids.len());
    
    // PERFORMANCE FIXTURES
    let perf_data = ctx.performance()
        .large_dataset_with(1000)
        .await?;
        
    println!("Performance dataset has {} events", perf_data.event_count);
    
    // ERROR FIXTURES
    let errors = ctx.errors()
        .validation_failures()
        .await?;
        
    println!("Error scenarios: {} failed operations", errors.failed_operation_ids.len());
    
    Ok(())
}

#[sinex_test]
async fn demo_assertions_and_timing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create events
    ctx.create_test_event("fs-watcher", "file.created", json!({"path": "/test1.txt"})).await?;
    ctx.create_test_event("fs-watcher", "file.created", json!({"path": "/test2.txt"})).await?;
    
    // Built-in assertions
    ctx.assert_event_count(2).await?;
    
    // Wait for conditions
    ctx.wait_for_event_count(2).await?;
    
    // Query assertions
    let fs_count = ctx.pool.events().by_source("filesystem").count().await?;
    assert_eq!(fs_count, 2);
    
    Ok(())
}