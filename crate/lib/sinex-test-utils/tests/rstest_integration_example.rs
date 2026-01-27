#![cfg(feature = "rstest-preview")]

//! rstest + TestContext example using `#[sinex_test]`.
//!
//! rstest drives the cases and `#[sinex_test]` wires up the Tokio runtime
//! plus a fresh `TestContext` for each case.

use rstest::rstest;
use serde_json::json;
use sinex_core::types::events::DynamicPayload;
use xtask::sandbox::{prelude::*, sinex_test, TestResult};

#[sinex_test]
#[rstest(
    source,
    event_type,
    case("fs", "file.created"),
    case("shell", "cmd.run"),
    case("service", "health.check")
)]
async fn test_event_creation_with_cases(
    ctx: TestContext,
    source: &str,
    event_type: &str,
) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let event = ctx
        .publish(DynamicPayload::new(
            source,
            event_type,
            json!({"rstest": true}),
        ))
        .await?;

    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);

    Ok(())
}

#[sinex_test]
#[rstest(
    name,
    size,
    expected_valid,
    case("tiny", 64usize, true),
    case("small", 1024usize, true)
)]
async fn test_payload_variations(
    ctx: TestContext,
    name: &str,
    size: usize,
    expected_valid: bool,
) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let payload = json!({
        "name": name,
        "data": "x".repeat(size),
        "size_kb": size / 1024,
    });

    let result = ctx
        .publish(DynamicPayload::new("test", "payload.test", payload))
        .await;

    if expected_valid {
        let event = result?;
        assert_eq!(event.payload["name"], json!(name));
        assert_eq!(event.payload["size_kb"], json!(size / 1024));
    } else {
        assert!(result.is_err());
    }

    Ok(())
}

#[sinex_test]
#[rstest(event_type, case("events.created"), case("fs.changed"))]
async fn test_with_fixture_and_cases(ctx: TestContext, event_type: &str) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let test_sources = vec!["fs", "shell", "service"];

    for source in &test_sources {
        ctx.publish(DynamicPayload::new(source, event_type, json!({})))
            .await?;
    }

    let counts = ctx.pool.events().count_by_type_all_time(None).await?;
    let count_for_type = counts
        .iter()
        .find(|c| c.event_type == event_type)
        .map(|c| c.count)
        .unwrap_or(0);

    assert_eq!(count_for_type, test_sources.len() as i64);

    Ok(())
}
