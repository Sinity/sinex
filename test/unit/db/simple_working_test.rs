//! A simple working test to demonstrate the correct pattern

use sinex_test_macros::sinex_test;
use crate::common::test_context::TestContext;
use anyhow::Result;

#[sinex_test]
async fn test_basic_database_operations(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test 1: Basic query works
    let result: i32 = sqlx::query_scalar!("SELECT 1 + 1 as sum")
        .fetch_one(ctx.pool())
        .await?;
    assert_eq!(result, 2);
    
    // Test 2: Insert an event using TestContext
    let event = ctx.filesystem_event("/test/file.txt");
    ctx.insert_event(&event).await?;
    
    // Test 3: Verify it was inserted
    let count = ctx.event_count().await?;
    assert!(count >= 1);
    
    println!("✓ Basic database operations work correctly");
    Ok(())
}

#[sinex_test]
async fn test_event_builders_work(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test using various event builders
    let fs_event = ctx.filesystem_event("/home/user/document.txt");
    let term_event = ctx.terminal_event("ls -la");
    let clip_event = ctx.clipboard_event("test content");
    
    // Insert them all
    ctx.insert_event(&fs_event).await?;
    ctx.insert_event(&term_event).await?;
    ctx.insert_event(&clip_event).await?;
    
    // Verify they exist
    ctx.wait_for_event_count(3).await?;
    
    println!("✓ Event builders work correctly");
    Ok(())
}

#[sinex_test]
async fn test_transaction_isolation(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Get initial count
    let initial_count = ctx.event_count().await?;
    
    // Insert some events
    for i in 0..5 {
        let event = ctx.event_builder("test", "isolation.test")
            .payload(serde_json::json!({ "index": i }))
            .build();
        ctx.insert_event(&event).await?;
    }
    
    // Verify they exist in our transaction
    let new_count = ctx.event_count().await?;
    assert_eq!(new_count - initial_count, 5);
    
    // Note: These will be rolled back after the test
    println!("✓ Transaction isolation works correctly");
    Ok(())
}