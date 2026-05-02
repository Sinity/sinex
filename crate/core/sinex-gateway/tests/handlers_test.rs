//! Unit tests for gateway RPC handlers

use serde_json::json;
use sinex_gateway::handlers;
use sinex_primitives::error::ErrorClass;
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

#[sinex_test]
async fn events_query_rejects_malformed_parameters(ctx: TestContext) -> TestResult<()> {
    let error = handlers::handle_events_query(ctx.pool(), json!({ "sources": "not-a-list" }))
        .await
        .expect_err("malformed events.query params must fail");

    assert_eq!(error.error_class(), ErrorClass::DataError);
    Ok(())
}

#[sinex_test]
async fn events_lineage_rejects_malformed_parameters(ctx: TestContext) -> TestResult<()> {
    let error = handlers::handle_events_lineage(ctx.pool(), json!({ "event_id": "not-a-uuid" }))
        .await
        .expect_err("malformed events.lineage params must fail");

    assert_eq!(error.error_class(), ErrorClass::DataError);
    Ok(())
}
