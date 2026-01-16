//! Basic test to verify enhanced sinex_test macro works

use serde_json::{json, Value as JsonValue};
use sinex_core::db::models::event::Event;
use sinex_core::types::Id;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::WaitHelpers;

// Test that regular sinex_test still works
#[sinex_test]
async fn test_regular_works(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let event = publish_and_fetch(&ctx, "test", "basic.test", json!({ "shape": "circle" })).await?;

    assert_eq!(event.source.as_str(), "test");
    Ok(())
}

// Test that sinex_test with rstest cases works
#[sinex_test]
#[case("fs", "file.created")]
#[case("terminal", "command.executed")]
async fn test_rstest_integration(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let event = publish_and_fetch(&ctx, source, event_type, json!({})).await?;

    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);
    Ok(())
}

// Test that tracing works
#[sinex_test(trace = true)]
async fn test_tracing_enabled(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    tracing::info!("Test with tracing");
    ctx.capture_log("Test with tracing".into());

    publish_and_fetch(&ctx, "traced", "test.event", json!({ "trace": true })).await?;

    ctx.assert_logged("Test with tracing")?;
    Ok(())
}

async fn publish_and_fetch(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload: JsonValue,
) -> TestResult<Event<JsonValue>> {
    let event = Event::<JsonValue>::test_event(source, event_type, payload);
    let id = ctx.publish_test_event(&event).await?;
    WaitHelpers::wait_for_source_events(&ctx.pool, source, 1, 20).await?;
    let stored = ctx
        .pool
        .events()
        .get_by_id(&Id::<Event<JsonValue>>::from_ulid(id))
        .await?;
    Ok(stored)
}
