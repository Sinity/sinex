//! Simple Test Suite
//!
//! Basic tests to verify the test infrastructure is working

use serde_json::json;
// Using shorter imports from sinex-core's re-exports
use sinex_core::{EventSource, Ulid};
use sinex_test_utils::prelude::*;

#[sinex_test]
fn test_ulid_generation() -> TestResult<()> {
    let ulid = Ulid::new();
    assert_eq!(ulid.to_string().len(), 26);
    Ok(())
}

#[sinex_test]
fn test_event_source_creation() -> TestResult<()> {
    let source = EventSource::from_static("test-source");
    assert_eq!(source.as_str(), "test-source");
    Ok(())
}

#[sinex_test]
async fn test_basic_database_connection(ctx: TestContext) -> TestResult<()> {
    // Just verify we can get a database connection
    let _pool = &ctx.pool;
    Ok(())
}

#[sinex_test]
async fn test_event_creation(ctx: TestContext) -> TestResult<()> {
    let event = ctx
        .publish_event("test", "test.event", json!({"value": 42}))
        .await?;

    assert_eq!(event.source.as_str(), "test");
    assert_eq!(event.event_type.as_str(), "test.event");

    Ok(())
}
