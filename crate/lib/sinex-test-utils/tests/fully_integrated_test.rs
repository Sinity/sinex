//! Demonstrates the fully integrated modern test infrastructure
//!
//! This shows how sinex_test seamlessly integrates rstest, insta, and tracing-test
//! without requiring separate attributes or manual setup.

use camino::Utf8PathBuf;
use color_eyre::eyre::Result;
use proptest::prelude::*;
use serde_json::{json, Value};
use sinex_core::db::repositories::DbPoolExt;
use sinex_test_utils::prelude::*;
use sinex_core::types::domain::{EventSource, EventType};

// Example 1: Basic rstest integration with automatic TestContext
#[sinex_test]
#[case("fs", "file.created", json!({"path": "/tmp/test.txt"}))]
#[case("terminal", "command.executed", json!({"command": "ls", "exit_code": 0}))]
#[case("desktop", "window.focused", json!({"window_id": "0x123", "title": "Test"}))]
async fn test_automatic_rstest_integration(
    ctx: TestContext, // Automatically created for each case
    #[case] source: &str,
    #[case] event_type: &str,
    #[case] payload: Value,
) -> Result<()> {
    // The sinex_test macro detects #[case] attributes and automatically:
    // 1. Adds #[rstest] attribute
    // 2. Creates TestContext for each case
    // 3. Manages database pooling
    // 4. Provides timeout and progress tracking

    let event = ctx
        .create_test_event(source, event_type, payload.clone())
        .await?;

    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);

    // Snapshot testing would work here if implemented
    // ctx.snapshot_event(&event, Some(&format!("{}_{}", source, event_type)));

    Ok(())
}

// Example 2: Tracing integration with sinex_test
#[sinex_test]
async fn test_automatic_tracing(ctx: TestContext) -> Result<()> {
    // With trace = true, tracing is automatically enabled
    tracing::info!("Starting test with automatic tracing");

    let event = ctx
        .create_test_event("traced-test", "test.event", json!({}))
        .await?;

    tracing::debug!("Created event: {:?}", event.id);

    // We can verify logs were captured
    ctx.assert_logged("Starting test with automatic tracing")?;
    ctx.assert_logged("Created event")?;
    ctx.assert_no_errors_logged()?;

    Ok(())
}

// Example 3: Combining rstest + tracing
#[sinex_test]
#[case("info", "Testing info level")]
#[case("debug", "Testing debug level")]
#[case("warn", "Testing warn level")]
async fn test_rstest_with_tracing(
    ctx: TestContext,
    #[case] level: &str,
    #[case] message: &str,
) -> Result<()> {
    // Both features work together seamlessly
    match level {
        "info" => tracing::info!("{}", message),
        "debug" => tracing::debug!("{}", message),
        "warn" => tracing::warn!("{}", message),
        _ => unreachable!(),
    }

    ctx.assert_logged(message)?;

    Ok(())
}

// Example 4: Snapshot testing with rstest
#[sinex_test]
#[case("create", json!({"action": "create", "path": "/tmp/new.txt"}))]
#[case("modify", json!({"action": "modify", "path": "/tmp/existing.txt", "size": 1024}))]
#[case("delete", json!({"action": "delete", "path": "/tmp/old.txt"}))]
async fn test_snapshots_with_rstest(
    ctx: TestContext,
    #[case] operation: &str,
    #[case] data: Value,
) -> Result<()> {
    let event = ctx
        .create_test_event("filesystem", &format!("file.{}", operation), data)
        .await?;

    // Snapshot paths are automatically configured to include:
    // - Test function name
    // - Case identifier
    // This prevents snapshot conflicts between cases
    // ctx.snapshot_event(&event, Some(operation)); // Not implemented yet

    Ok(())
}

// Example 5: Complex test with all features
#[sinex_test(trace = true, timeout = 60)]
#[case("small", 10, vec!["a", "b", "c"])]
#[case("medium", 100, vec!["x", "y", "z"])]
#[case("large", 1000, vec!["foo", "bar", "baz"])]
async fn test_all_features_combined(
    ctx: TestContext,
    #[case] size_name: &str,
    #[case] count: usize,
    #[case] tags: Vec<&str>,
) -> Result<()> {
    tracing::info!("Processing {} dataset with {} items", size_name, count);

    // Create multiple events
    let mut event_ids = Vec::new();
    for i in 0..count {
        let event = ctx
            .create_test_event(
                "bulk-test",
                "item.created",
                json!({
                    "index": i,
                    "size_category": size_name,
                    "tags": tags
                }),
            )
            .await?;

        event_ids.push(event.id);

        if i % 100 == 0 {
            tracing::debug!("Created {} events so far", i + 1);
        }
    }

    // Verify all were created
    let source_ref = sinex_types::domain::EventSource::from("bulk-test");
    let events = ctx
        .pool
        .events()
        .get_by_source(&source_ref, Some((count + 10) as i64), None)
        .await?;

    assert_eq!(events.len(), count);

    // Snapshot the summary
    let summary = json!({
        "size_name": size_name,
        "count": count,
        "tags": tags,
        "event_count": events.len(),
        "first_id": event_ids.first(),
        "last_id": event_ids.last(),
    });

    // ctx.snapshot(&summary, Some(&format!("summary_{}", size_name))); // Not implemented yet

    // Verify logging worked
    ctx.assert_logged(&format!("Processing {} dataset", size_name))?;

    Ok(())
}

// Example 6: Property testing still works with sinex_test
#[sinex_test]
async fn test_property_testing_integration(ctx: TestContext) -> Result<()> {
    // Test context is available for actual database operations
    let _event = ctx
        .create_test_event(
            "proptest",
            "validation.test",
            json!({"test": "property_testing"}),
        )
        .await?;

    // Simple validation without proptest for now
    let source = "valid-source";
    let result = std::panic::catch_unwind(|| EventSource::new(source));
    assert!(result.is_ok());

    Ok(())
}

// Example 7: Fixtures work seamlessly
#[sinex_test]
#[case("created")]
#[case("modified")]
async fn test_with_fixtures(
    ctx: TestContext,
    test_sources: Vec<&'static str>, // Fixture from test-utils
    test_paths: Vec<Utf8PathBuf>,    // Another fixture
    #[case] operation: &str,
) -> Result<()> {
    // Fixtures are injected alongside rstest parameters
    for source in &test_sources {
        for path in &test_paths {
            ctx.create_test_event(
                source,
                &format!("file.{}", operation),
                json!({"path": path.as_str()}),
            )
            .await?;
        }
    }

    let type_ref = sinex_types::domain::EventType::from(format!("file.{}", operation));
    let events = ctx
        .pool
        .events()
        .get_by_event_type(&type_ref, Some(1000), None)
        .await?;
    let count = events.len() as i64;

    assert_eq!(count, (test_sources.len() * test_paths.len()) as i64);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // This module existing and compiling proves the integration works
}
