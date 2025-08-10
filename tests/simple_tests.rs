//! Simple Test Suite
//!
//! Basic tests to verify the test infrastructure is working

use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_core::db::models::RawEvent;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_core::types::{Id, Ulid};
use sinex_test_utils::prelude::*;

#[sinex_test]
fn test_ulid_generation() -> color_eyre::eyre::Result<()> {
    let ulid = Ulid::new();
    assert_eq!(ulid.to_string().len(), 26);
    Ok(())
}

#[sinex_test]
fn test_event_source_creation() -> color_eyre::eyre::Result<()> {
    let source = EventSource::from_static("test-source");
    assert_eq!(source.as_str(), "test-source");
    Ok(())
}

#[sinex_test]
async fn test_basic_database_connection(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Just verify we can get a database connection
    let _pool = &ctx.pool;
    Ok(())
}

#[sinex_test]
async fn test_event_creation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let event = ctx
        .create_test_event("test", "test.event", json!({"value": 42}))
        .await?;

    assert_eq!(event.source.as_str(), "test");
    assert_eq!(event.event_type.as_str(), "test.event");

    Ok(())
}
