//! Minimal test to verify the procedural macro works

use sinex_test_macros::sinex_test;
use crate::common::test_context::TestContext;

#[sinex_test]
async fn test_macro_works(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    println!("✅ The procedural macro works!");
    println!("✅ TestContext was injected: {:?}", ctx.test_name());
    println!("✅ We have a database pool!");
    
    // Simple query to verify database works
    let result: i32 = sqlx::query_scalar!("SELECT 1 + 1 as sum")
        .fetch_one(ctx.pool())
        .await?;
    assert_eq!(result, 2);
    
    println!("✅ Database query works!");
    Ok(())
}