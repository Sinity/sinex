//! Demonstration of the streamlined test infrastructure API
//!
//! This showcases how the consolidated TestContext provides a clean, unified API
//! for all test operations while maintaining access to all advanced functionality.

use crate::common::prelude::*;

#[sinex_test]
async fn demo_streamlined_event_creation(ctx: TestContext) -> TestResult {
    // FLUENT API - Most tests use this discoverable, type-safe approach
    let event = ctx.event()
        .filesystem()
        .path("/tmp/demo.txt")
        .size(1024)
        .created()
        .insert()
        .await?;
    
    println!("Created event: {}", event.id);
    
    // DIRECT API - For error testing, bypasses validation
    let _invalid_event = ctx.event()
        .source("")  // Invalid empty source
        .type_("")   // Invalid empty type
        .insert_direct()  // Bypasses validation
        .await
        .expect_err("Should fail due to empty fields");
    
    // BATCH API - For performance testing
    let batch = ctx.event()
        .filesystem()
        .path("/tmp/batch.txt")
        .created()
        .insert_batch(5)
        .await?;
    
    assert_eq!(batch.len(), 5);
    
    Ok(())
}

#[sinex_test] 
async fn demo_streamlined_querying(ctx: TestContext) -> TestResult {
    // Create some test data
    ctx.event().filesystem().path("/demo1.txt").created().insert().await?;
    ctx.event().filesystem().path("/demo2.txt").created().insert().await?;
    ctx.event().terminal().command("ls").success().insert().await?;
    
    // FLUENT QUERY API - Type-safe, discoverable
    let fs_events = ctx.events()
        .by_source("filesystem")
        .limit(10)
        .fetch()
        .await?;
    
    assert_eq!(fs_events.len(), 2);
    
    // Simple query operations
    let total_count = ctx.events().count().await?;
    assert_eq!(total_count, 3);
    
    Ok(())
}

#[sinex_test]
async fn demo_streamlined_fixtures(ctx: TestContext) -> TestResult {
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
async fn demo_assertions_and_timing(ctx: TestContext) -> TestResult {
    // Create events
    ctx.event().filesystem().path("/test1.txt").created().insert().await?;
    ctx.event().filesystem().path("/test2.txt").created().insert().await?;
    
    // Built-in assertions
    ctx.assert_event_count(2).await?;
    
    // Wait for conditions
    ctx.wait_for_event_count(2).await?;
    
    // Query assertions
    let fs_count = ctx.events().by_source("filesystem").count().await?;
    assert_eq!(fs_count, 2);
    
    Ok(())
}