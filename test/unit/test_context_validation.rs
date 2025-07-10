//! Test to validate TestContext implementation for Phase 0 Task 2

use crate::common::prelude::*;

#[sinex_test]
async fn test_context_basic_functionality(ctx: TestContext) -> TestResult {
    // Test 1: Event creation and insertion
    let event = ctx.events().filesystem().path("/test/file.txt").created().build();
    ctx.insert_event(&event).await?;
    
    // Test 2: Wait for event count
    ctx.wait_for_event_count(1).await?;
    
    // Test 3: Enhanced assertions
    ctx.assert_event_count(1).await?;
    ctx.assert_event_exists(event.id).await?;
    
    // Test 4: Event querying
    let events = ctx.query_events().await?;
    assert_eq!(events.len(), 1);
    
    // Test 5: Performance metrics
    let metrics = ctx.get_performance_metrics();
    assert_eq!(metrics.test_name, "test_context_basic_functionality");
    
    Ok(())
}

#[sinex_test]
async fn test_context_batch_operations(ctx: TestContext) -> TestResult {
    // Test batch event creation
    let events = ctx.create_event_batch("test_source", 5);
    assert_eq!(events.len(), 5);
    
    // Test batch insertion
    ctx.insert_events(&events).await?;
    
    // Test smart waiting
    ctx.wait_for_event_count(5).await?;
    
    // Test source-specific querying
    let source_events = ctx.query_events_by_source("test_source").await?;
    assert_eq!(source_events.len(), 5);
    
    Ok(())
}

#[sinex_test]
async fn test_context_event_builders(ctx: TestContext) -> TestResult {
    // Test filesystem event builder
    let fs_event = ctx.events().filesystem().path("/test/file.txt").created().build();
    ctx.insert_event(&fs_event).await?;
    
    // Test terminal event builder
    let term_event = ctx.events().terminal().command("ls -la").exit_code(0).build_completed();
    ctx.insert_event(&term_event).await?;
    
    // Test clipboard event builder
    let clip_event = ctx.events().clipboard().text("test content").build();
    ctx.insert_event(&clip_event).await?;
    
    // Test generic event builder
    let generic_event = ctx.events().generic("test", "generic.event")
        .payload(json!({"test": true}))
        .build();
    ctx.insert_event(&generic_event).await?;
    
    // Verify all events were inserted
    ctx.wait_for_event_count(4).await?;
    ctx.assert_event_count(4).await?;
    
    Ok(())
}

#[sinex_test]
async fn test_context_work_queue_operations(ctx: TestContext) -> TestResult {
    // Test work queue assertions
    ctx.assert_work_queue_empty().await?;
    
    // Test work queue waiting
    ctx.wait_for_work_queue_empty().await?;
    
    // Test specific work queue count
    ctx.wait_for_work_queue(0).await?;
    
    Ok(())
}

#[sinex_test]
async fn test_context_step_execution(ctx: TestContext) -> TestResult {
    // Test step execution with timing
    let result = ctx.run_step("test_step", || async {
        // Simulate some work
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        Ok("step completed")
    }).await?;
    
    assert_eq!(result, "step completed");
    
    Ok(())
}

#[sinex_test]
async fn test_context_comprehensive_workflow(ctx: TestContext) -> TestResult {
    // Test comprehensive workflow combining all features
    
    // Step 1: Create events with time distribution
    let start_time = chrono::Utc::now();
    let timed_events = ctx.create_time_distributed_batch(
        "workflow_test",
        3,
        start_time,
        std::time::Duration::from_millis(100),
    );
    
    // Step 2: Insert events and track IDs
    let mut ids = Vec::new();
    for event in timed_events {
        ctx.insert_event(&event).await?;
        ids.push(event.id);
    }
    
    // Step 3: Wait for all events to be processed
    ctx.wait_for_event_count(3).await?;
    
    // Step 4: Verify all events exist
    for id in ids {
        ctx.assert_event_exists(id).await?;
    }
    
    // Step 5: Create and insert events atomically
    let batch_ids = ctx.create_and_insert_events("batch_test", 2).await?;
    assert_eq!(batch_ids.len(), 2);
    
    // Step 6: Wait for final event count
    ctx.wait_for_event_count(5).await?;
    
    // Step 7: Verify final state
    ctx.assert_event_count(5).await?;
    
    // Step 8: Check performance metrics
    let metrics = ctx.get_performance_metrics();
    assert!(!metrics.test_name.is_empty());
    
    Ok(())
}