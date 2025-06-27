//! Minimal test to verify the procedural macro works

use crate::common::prelude::*;

#[sinex_test]
async fn test_macro_works(ctx: TestContext) -> TestResult {
    println!("✅ The procedural macro works!");
    println!("✅ TestContext was injected: {:?}", ctx.test_name());
    println!("✅ We have a database pool!");

    // Simple query to verify database works
    let result = sqlx::query_scalar!("SELECT 1 + 1 as sum")
        .fetch_one(ctx.pool())
        .await?;
    pretty_assertions::assert_eq!(result, Some(2));

    println!("✅ Database query works!");
    Ok(())
}
