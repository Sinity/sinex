#![cfg(feature = "rstest-preview")]

//! rstest + TestContext example using tokio runtime.
//!
//! This keeps the test harness simple: rstest drives the cases, tokio::test
//! supplies the runtime, and we allocate a fresh TestContext per case.

use rstest::rstest;
use sinex_test_utils::prelude::*;
use sinex_test_utils::TestResult;

#[rstest(
    source, event_type,
    case("fs", "file.created"),
    case("shell", "cmd.run"),
    case("service", "health.check"),
)]
#[tokio::test]
async fn test_event_creation_with_cases(
    source: &str,
    event_type: &str,
) -> TestResult<()> {
    let ctx = TestContext::new().await?;

    let event = ctx
        .create_test_event(source, event_type, json!({"rstest": true}))
        .await?;

    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);

    Ok(())
}

#[rstest(
    name, size, expected_valid,
    case("tiny", 64usize, true),
    case("small", 1024usize, true),
    case("too-big", 5_000_000usize, false),
)]
#[tokio::test]
async fn test_payload_variations(
    name: &str,
    size: usize,
    expected_valid: bool,
) -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let payload = json!({
        "name": name,
        "data": "x".repeat(size),
        "size_kb": size / 1024,
    });

    let result = ctx
        .create_test_event("test", "payload.test", payload.clone())
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

#[rstest(
    event_type,
    case("events.created"),
    case("fs.changed"),
)]
#[tokio::test]
async fn test_with_fixture_and_cases(event_type: &str) -> TestResult<()> {
    let ctx = TestContext::new().await?;
    let test_sources = vec!["fs", "shell", "service"];

    for source in &test_sources {
        ctx.create_test_event(*source, event_type, json!({})).await?;
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
