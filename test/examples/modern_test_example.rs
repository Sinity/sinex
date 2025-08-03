//! Example demonstrating the modern test infrastructure in sinex-test-utils
//!
//! This file shows best practices for using rstest, insta, tracing-test, and similar-asserts
//! with the TestContext infrastructure.

use sinex_test_utils::prelude::*;

// ===== Basic rstest parameterized tests =====

#[rstest]
#[case("fs-watcher", "file.created", json!({"path": "/tmp/test.txt", "size": 1024}))]
#[case("terminal", "command.executed", json!({"command": "ls -la", "exit_code": 0}))]
#[case("desktop", "window.focused", json!({"window_id": "123", "title": "Editor"}))]
#[tokio::test]
async fn test_event_creation_parameterized(
    #[case] source: &str,
    #[case] event_type: &str,
    #[case] payload: Value,
) -> Result<()> {
    let ctx = TestContext::new().await?;
    
    let event = ctx.event()
        .source(source)
        .type_(event_type)
        .payload(payload.clone())
        .insert()
        .await?;
    
    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);
    assert_eq!(event.payload, payload);
    
    Ok(())
}

// ===== Using fixtures with rstest =====

#[rstest]
#[tokio::test]
async fn test_with_fixtures(
    test_sources: Vec<&'static str>,
    test_paths: Vec<Utf8PathBuf>,
) -> Result<()> {
    let ctx = TestContext::new().await?;
    
    // Create events for each source and path combination
    for source in &test_sources {
        for path in &test_paths {
            ctx.event()
                .source(*source)
                .type_("test.fixture")
                .field("path", path.as_str())
                .insert()
                .await?;
        }
    }
    
    // Verify all were created
    let count = ctx.events().count().await?;
    assert_eq!(count, (test_sources.len() * test_paths.len()) as i64);
    
    Ok(())
}

// ===== Snapshot testing with insta =====

#[sinex_test]
async fn test_snapshot_testing(ctx: TestContext) -> Result<()> {
    // Create a complex event
    let event = ctx.event()
        .filesystem()
        .path("/home/user/important.doc")
        .size(2048576)
        .permissions(0o644)
        .modified()
        .insert()
        .await?;
    
    // Snapshot the entire event with automatic redactions
    ctx.snapshot_event(&event, Some("filesystem_event"));
    
    // Snapshot just the payload
    ctx.snapshot(&event.payload, Some("filesystem_payload"));
    
    // JSON snapshot with custom redactions
    ctx.snapshot_json(
        &event,
        "custom_redacted",
        vec![
            (".host", "[redacted-host]"),
            (".ingestor_version", "[version]"),
        ],
    );
    
    // Debug snapshot for non-serializable types
    let complex_struct = (event.id, event.source.clone());
    ctx.snapshot_debug(&complex_struct, Some("complex_debug"));
    
    Ok(())
}

// ===== Tracing test for logging verification =====

#[sinex_test]
#[traced_test]
async fn test_with_tracing(ctx: TestContext) -> Result<()> {
    // Enable tracing for this test
    let _guard = ctx.with_tracing("debug");
    
    // Do some operations that generate logs
    tracing::info!("Starting test operations");
    
    let event = ctx.event()
        .source("tracing-test")
        .type_("test.logged")
        .insert()
        .await?;
    
    tracing::debug!("Created event with ID: {:?}", event.id);
    
    // Verify logs were captured
    ctx.assert_logged("Starting test operations")?;
    ctx.assert_logged("Created event")?;
    ctx.assert_no_errors_logged()?;
    
    // Get all captured logs
    let logs = ctx.captured_logs();
    assert!(logs.len() >= 2);
    
    Ok(())
}

// ===== Similar asserts for better diffs =====

#[sinex_test]
async fn test_similar_assertions(ctx: TestContext) -> Result<()> {
    let event1 = ctx.event()
        .source("test")
        .type_("similar.test")
        .field("data", vec![1, 2, 3, 4, 5])
        .build()?;
    
    let event2 = ctx.event()
        .source("test")
        .type_("similar.test")
        .field("data", vec![1, 2, 3, 4, 5])
        .build()?;
    
    // Use similar_asserts for better diff output
    ctx.assert_similar(&event1.payload, &event2.payload, "Payloads should match")?;
    
    // For JSON values specifically
    let json1 = json!({
        "name": "test",
        "items": [1, 2, 3],
        "nested": {
            "value": 42
        }
    });
    
    let json2 = json!({
        "name": "test",
        "items": [1, 2, 3],
        "nested": {
            "value": 42
        }
    });
    
    ctx.assert_json_similar(&json1, &json2, "JSON structures should match")?;
    
    Ok(())
}

// ===== Combining multiple modern features =====

#[rstest]
#[case("create", "file.created")]
#[case("modify", "file.modified")]
#[case("delete", "file.deleted")]
#[traced_test]
#[tokio::test]
async fn test_modern_infrastructure_combined(
    #[case] operation: &str,
    #[case] expected_type: &str,
) -> Result<()> {
    let ctx = TestContext::new().await?;
    let _guard = ctx.with_tracing("info");
    
    tracing::info!("Testing {} operation", operation);
    
    // Create event based on operation
    let event = match operation {
        "create" => ctx.event()
            .filesystem()
            .path("/tmp/test.txt")
            .created()
            .insert()
            .await?,
        "modify" => ctx.event()
            .filesystem()
            .path("/tmp/test.txt")
            .modified()
            .insert()
            .await?,
        "delete" => ctx.event()
            .filesystem()
            .path("/tmp/test.txt")
            .deleted()
            .insert()
            .await?,
        _ => unreachable!(),
    };
    
    // Verify event type
    assert_eq!(event.event_type.as_str(), expected_type);
    
    // Snapshot the event
    ctx.snapshot_event(&event, Some(&format!("{}_event", operation)));
    
    // Verify logging
    ctx.assert_logged(&format!("Testing {} operation", operation))?;
    
    Ok(())
}

// ===== Property testing with modern infrastructure =====

#[sinex_test]
async fn test_property_based_with_snapshots(ctx: TestContext) -> Result<()> {
    use proptest::prelude::*;
    
    // Generate test cases
    let test_cases = vec![
        ("short", "x".repeat(10)),
        ("medium", "x".repeat(100)),
        ("long", "x".repeat(1000)),
    ];
    
    for (name, content) in test_cases {
        let event = ctx.event()
            .source("property-test")
            .type_("test.content")
            .field("name", name)
            .field("content", &content)
            .field("length", content.len())
            .insert()
            .await?;
        
        // Snapshot each case
        ctx.snapshot_json(
            &event.payload,
            &format!("property_{}", name),
            vec![(".content", "[content]")], // Redact actual content
        );
    }
    
    Ok(())
}

// ===== Advanced fixture usage =====

#[rstest]
#[tokio::test]
async fn test_advanced_fixtures(
    #[future] test_context_with_tracing: TestContext,
) -> Result<()> {
    let ctx = test_context_with_tracing.await;
    
    // The context already has tracing enabled
    tracing::info!("This will be captured automatically");
    
    // Create some test data
    let dataset = ctx.fixtures().performance().large_dataset().await?;
    
    // Snapshot the dataset summary
    let summary = json!({
        "event_count": dataset.event_count,
        "source_distribution": dataset.source_distribution,
        "time_range": {
            "start": dataset.start_time.to_rfc3339(),
            "end": dataset.end_time.to_rfc3339(),
        }
    });
    
    ctx.snapshot(&summary, Some("dataset_summary"));
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // This ensures all examples compile and can run
    #[test]
    fn examples_compile() {
        // The examples above serve as both documentation and tests
    }
}