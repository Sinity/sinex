//! Unit tests for gateway RPC handlers

use serde_json::json;
use sinex_gateway::handlers;
use sinex_services::AnalyticsService;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_handle_activity_heatmap_defaults(ctx: TestContext) -> Result<()> {
    // Basic test with default parameters
    let service = AnalyticsService::new(ctx.pool().clone());
    let params = json!({});
    let result = handlers::handle_activity_heatmap(&service, params).await?;
    assert!(result.is_array());
    Ok(())
}
