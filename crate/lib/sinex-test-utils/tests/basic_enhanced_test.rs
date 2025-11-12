//! Basic test to verify enhanced sinex_test macro works

use serde_json::json;
use sinex_test_utils::prelude::*;
use sinex_test_utils::TestResult;

// Test that regular sinex_test still works
#[sinex_test]
async fn test_regular_works(ctx: TestContext) -> TestResult<()> {
    let event = ctx
        .create_test_event("test", "basic.test", json!({}))
        .await?;

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
    let event = ctx.create_test_event(source, event_type, json!({})).await?;

    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);
    Ok(())
}

// Test that tracing works
#[sinex_test(trace = true)]
async fn test_tracing_enabled(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Test with tracing");
    ctx.capture_log("Test with tracing".into());

    ctx.create_test_event("traced", "test.event", json!({}))
        .await?;

    ctx.assert_logged("Test with tracing")?;
    Ok(())
}
