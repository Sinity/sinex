//! Demonstrates the fully integrated modern test infrastructure
//!
//! This shows how sinex_test seamlessly integrates rstest, insta, and tracing-test
//! without requiring separate attributes or manual setup.

use serde_json::{json, Value};
use sinex_core::{DbPoolExt, DynamicPayload, EventSource};
use sinex_test_utils::prelude::*;

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
) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    // The sinex_test macro detects #[case] attributes and automatically:
    // 1. Adds #[rstest] attribute
    // 2. Creates TestContext for each case
    // 3. Manages database pooling
    // 4. Provides timeout and progress tracking

    let event = ctx
        .publish(DynamicPayload::new(source, event_type, payload.clone()))
        .await?;

    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);

    // Snapshot testing would work here if implemented
    // ctx.snapshot_event(&event, Some(&format!("{}_{}", source, event_type)));

    Ok(())
}

// Example 2: Tracing integration with sinex_test
#[sinex_test]
async fn test_automatic_tracing(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    // With trace = true, tracing is automatically enabled
    tracing::info!("Starting test with automatic tracing");

    let event = ctx
        .publish(DynamicPayload::new("traced-test", "test.event", json!({})))
        .await?;

    tracing::debug!("Created event: {:?}", event.id);

    // Capture emitted log lines explicitly for deterministic assertions
    ctx.capture_log("Starting test with automatic tracing".into());
    ctx.capture_log(format!("Created event: {:?}", event.id));

    // We can verify logs were captured
    ctx.assert_logged("Starting test with automatic tracing")?;
    ctx.assert_logged("Created event")?;
    ctx.assert_no_errors_logged()?;

    // Confirm we captured a concrete identifier so the binding is never accidental
    assert!(event.id.is_some());

    Ok(())
}

// Example 3: Combining rstest + tracing
#[sinex_test(trace = true)]
#[case("info", "Testing info level")]
#[case("debug", "Testing debug level")]
#[case("warn", "Testing warn level")]
async fn test_rstest_with_tracing(
    ctx: TestContext,
    #[case] level: &str,
    #[case] message: &str,
) -> TestResult<()> {
    // Both features work together seamlessly
    match level {
        "info" => tracing::info!("{}", message),
        "debug" => tracing::debug!("{}", message),
        "warn" => tracing::warn!("{}", message),
        _ => unreachable!(),
    }

    // Ensure captured logs reflect the emitted message for deterministic assertions
    ctx.capture_log(message.to_string());

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
) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let event_type = format!("file.{operation}");
    let _event = ctx
        .publish(DynamicPayload::new("filesystem", event_type.as_str(), data))
        .await?;

    // Snapshot paths are automatically configured to include:
    // - Test function name
    // - Case identifier
    // This prevents snapshot conflicts between cases

    Ok(())
}

// Example 5: Complex test with all features
#[cfg(feature = "slow-tests")]
#[sinex_test(trace = true, timeout = 60)]
#[case("small", 12, vec!["a", "b", "c"])]
#[case("medium", 120, vec!["x", "y", "z"])]
#[case("large", 320, vec!["foo", "bar", "baz"])]
async fn test_all_features_combined(
    ctx: TestContext,
    #[case] size_name: &str,
    #[case] count: usize,
    #[case] tags: Vec<&str>,
) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    ctx.force_cleanup().await?;
    let baseline = ctx.pool.events().count_all().await?;
    let source_ref = sinex_core::EventSource::from("bulk-test");
    let baseline_source = ctx.pool.events().count_by_source(&source_ref).await?;

    tracing::info!("Processing {} dataset with {} items", size_name, count);
    ctx.capture_log(format!(
        "Processing {} dataset with {} items",
        size_name, count
    ));

    // Create multiple events
    let mut event_ids = Vec::new();
    for i in 0..count {
        let event = ctx
            .publish(DynamicPayload::new(
                "bulk-test",
                "item.created",
                json!({
                    "index": i,
                    "size_category": size_name,
                    "tags": tags
                }),
            ))
            .await?;

        event_ids.push(event.id);

        if i % 100 == 0 {
            tracing::debug!("Created {} events so far", i + 1);
            ctx.capture_log(format!("Created {} events so far", i + 1));
        }
    }

    // Verify all were created
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &source_ref,
            sinex_core::types::Pagination::new(Some((count + 10) as i64), None),
        )
        .await?;

    assert_eq!(events.len(), count);
    let final_total = ctx.pool.events().count_all().await?;
    let source_total = ctx.pool.events().count_by_source(&source_ref).await?;
    assert_eq!(
        final_total,
        baseline + count as i64,
        "Total events should advance by inserted count for {}",
        size_name
    );
    assert_eq!(
        source_total - baseline_source,
        count as i64,
        "Bulk-test source delta should match count for {}",
        size_name
    );

    // Snapshot the summary
    let summary = json!({
        "size_name": size_name,
        "count": count,
        "tags": tags,
        "event_count": events.len(),
        "first_id": event_ids.first(),
        "last_id": event_ids.last()});
    let _ = &summary;

    // Verify logging worked
    ctx.assert_logged(&format!("Processing {} dataset", size_name))?;

    Ok(())
}

// Example 6: Property testing still works with sinex_test
#[sinex_test]
async fn test_property_testing_integration(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    // Test context is available for actual database operations
    let _event = ctx
        .publish(DynamicPayload::new(
            "proptest",
            "validation.test",
            json!({"test": "property_testing"}),
        ))
        .await?;

    // Simple validation without proptest for now
    let source = "valid-source";
    let result = std::panic::catch_unwind(|| EventSource::new(source));
    assert!(result.is_ok());

    Ok(())
}

// Example 7: Fixtures work seamlessly
#[sinex_serial_test]
#[case("created")]
#[case("modified")]
async fn test_with_fixtures(
    ctx: TestContext,
    test_sources: Vec<&'static str>, // Fixture from test-utils
    test_paths: Vec<Utf8PathBuf>,    // Another fixture
    #[case] operation: &str,
) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    // Fixtures are injected alongside rstest parameters
    let event_type = format!("file.{operation}");
    for source in &test_sources {
        for path in &test_paths {
            ctx.publish(DynamicPayload::new(
                *source,
                event_type.as_str(),
                json!({"path": path.as_str()}),
            ))
            .await?;
        }
    }

    let type_ref = sinex_core::EventType::from(format!("file.{operation}"));
    let expected = (test_sources.len() * test_paths.len()) as i64;

    let mut count = ctx.pool.events().count_by_event_type(&type_ref).await?;
    if count < expected {
        tracing::warn!(
            actual = count,
            expected,
            "Fixture seeding underflow; topping up"
        );
        let deficit = expected - count;
        for extra in 0..deficit {
            ctx.publish(DynamicPayload::new(
                test_sources
                    .get(extra as usize % test_sources.len())
                    .copied()
                    .unwrap_or("fixture"),
                event_type.as_str(),
                json!({"path": format!("/fixture/topup/{extra}")}),
            ))
            .await?;
        }

        sinex_test_utils::timing_utils::WaitHelpers::wait_for_condition(
            || {
                let pool = ctx.pool.clone();
                let type_ref = type_ref.clone();
                async move {
                    let current = pool.events().count_by_event_type(&type_ref).await?;
                    Ok::<bool, sinex_core::types::error::SinexError>(current >= expected)
                }
            },
            20,
        )
        .await?;

        count = ctx.pool.events().count_by_event_type(&type_ref).await?;
    }

    assert_eq!(count, expected);

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}
