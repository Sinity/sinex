#![cfg(feature = "rstest-preview")]

//! rstest + TestContext example using `#[sinex_test]`.
//!
//! rstest drives the cases and `#[sinex_test]` wires up the Tokio runtime
//! plus a fresh `TestContext` for each case.

use color_eyre::eyre::eyre;
use rstest::rstest;
use serde_json::{json, Value as JsonValue};
use sinex_core::db::models::event::Event;
use sinex_core::types::Id;
use sinex_test_utils::timing_utils::WaitHelpers;
use sinex_test_utils::{prelude::*, sinex_test, TestResult};

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
    let event = publish_and_fetch(&ctx, source, event_type, json!({"rstest": true})).await?;

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

    let result = publish_and_fetch(&ctx, "test", "payload.test", payload.clone()).await;

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
        publish_and_fetch(&ctx, source, event_type, json!({})).await?;
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
        .get_by_id(Id::<Event<JsonValue>>::from_ulid(id))
        .await?
        .ok_or_else(|| eyre!("Event not found after publishing"))?;
    Ok(stored)
}
