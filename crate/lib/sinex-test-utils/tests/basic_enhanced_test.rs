//! Basic test to verify enhanced sinex_test macro works

use sinex_test_utils::prelude::*;

// Test that regular sinex_test still works
#[sinex_test]
async fn test_regular_works(ctx: TestContext) -> Result<()> {
    let event = ctx
        .event()
        .source("test")
        .type_("basic.test")
        .insert()
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
) -> Result<()> {
    let event = ctx
        .event()
        .source(source)
        .type_(event_type)
        .insert()
        .await?;

    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);
    Ok(())
}

// Test that tracing works
#[sinex_test(trace = true)]
async fn test_tracing_enabled(ctx: TestContext) -> Result<()> {
    tracing::info!("Test with tracing");

    ctx.event()
        .source("traced")
        .type_("test.event")
        .insert()
        .await?;

    ctx.assert_logged("Test with tracing")?;
    Ok(())
}
