//! Unit tests for gateway RPC handlers

use sinex_primitives::query::{EventQuery, EventQueryResult};
use sinexd::api::handlers;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_handle_events_query_empty_params(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    // Query with no filters should return empty events on fresh DB
    let result = handlers::handle_events_query(pool, EventQuery::default()).await?;
    match result {
        EventQueryResult::Events { events, .. } => {
            assert!(events.is_empty(), "fresh DB should return no events");
        }
        other => panic!("empty query should return events variant, got {other:?}"),
    }
    Ok(())
}
