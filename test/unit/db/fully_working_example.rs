//! Fully working example of the new test infrastructure

use crate::common::prelude::*;
use crate::common::database_helpers;

/// Simplest possible test that actually works
#[tokio::test]
async fn test_without_macro() -> Result<(), Box<dyn std::error::Error>> {
    // Get shared pool
    let pool = database_helpers::get_shared_test_pool().await?;
    
    // Run a simple query
    let result = sqlx::query_scalar!("SELECT 1 + 1 as sum")
        .fetch_one(&pool)
        .await?;
    pretty_assertions::assert_eq!(result, Some(2));
    
    println!("✅ Basic test works without macro!");
    Ok(())
}

/// Test using the new macro - simplified version
#[sinex_test]
async fn test_with_new_macro(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // The macro should have injected TestContext with a pool
    println!("✅ Macro injected TestContext!");
    println!("✅ Test name: {}", ctx.test_name());
    
    // Simple database query
    let result = sqlx::query_scalar!("SELECT 2 + 2 as sum")
        .fetch_one(ctx.pool())
        .await?;
    pretty_assertions::assert_eq!(result, Some(4));
    
    println!("✅ Database query through TestContext works!");
    
    // Test event creation helpers
    let event = ctx.filesystem_event("/test/file.txt");
    println!("✅ Event builder works: {:?}", event.event_type);
    
    // Insert the event
    ctx.insert_event(&event).await?;
    println!("✅ Event insertion works!");
    
    // Verify it exists
    let count = ctx.event_count().await?;
    assert!(count >= 1);
    println!("✅ Event count: {}", count);
    
    Ok(())
}

/// Example of test that would use transactions (when properly implemented)
#[sinex_test]
async fn test_transaction_isolation(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // For now, this uses the shared pool approach
    // In the future, this would use actual transaction isolation
    
    let initial_count = ctx.event_count().await?;
    
    // Create some test events
    for i in 0..3 {
        let event = ctx.event_builder("test", "example")
            .payload(serde_json::json!({ "index": i }))
            .build();
        ctx.insert_event(&event).await?;
    }
    
    let new_count = ctx.event_count().await?;
    pretty_assertions::assert_eq!(new_count - initial_count, 3);
    
    println!("✅ Multiple event insertion works!");
    
    // Note: Without real transaction support, these events persist
    // With proper transaction support, they would be rolled back
    
    Ok(())
}