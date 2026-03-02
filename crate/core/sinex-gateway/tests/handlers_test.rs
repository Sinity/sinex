//! Unit tests for gateway RPC handlers

use serde_json::json;
use sinex_gateway::handlers;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_handle_events_query_empty_params(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    // Query with no filters should return empty events on fresh DB
    let params = json!({});
    let result = handlers::handle_events_query(pool, params).await?;
    assert!(result.is_object(), "result should be a JSON object");
    assert_eq!(
        result.get("type").and_then(|v| v.as_str()),
        Some("events"),
        "empty query should return events variant"
    );
    Ok(())
}
