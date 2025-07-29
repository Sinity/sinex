//! Comprehensive test demonstrating all TestContext API methods
//!
//! This test serves as both documentation and validation of the complete
//! sinex-test-utils API surface. All operations go through TestContext.

use serde_json::json;
use sinex_test_utils::prelude::*;
use std::time::Duration;

#[sinex_test]
async fn test_complete_api_surface(ctx: TestContext) -> Result<()> {
    // ===== 1. Event Creation =====

    // Basic event creation with fluent builders
    let basic_event = ctx
        .event()
        .source("test-service")
        .type_("test.comprehensive")
        .field("test_name", "api_demo")
        .field("version", 1)
        .fields(vec![
            ("array", json!([1, 2, 3])),
            ("nested", json!({"key": "value"})),
        ])
        .timestamp(chrono::Utc::now())
        .insert()
        .await?;

    // Domain-specific builders
    let fs_event = ctx
        .event()
        .filesystem()
        .path("/test/file.txt")
        .size(1024)
        .permissions(0o644)
        .field("custom", "value") // .field() works on all builders
        .created()
        .insert()
        .await?;

    let term_event = ctx
        .event()
        .terminal()
        .command("echo 'Hello, World!'")
        .working_dir("/home/test")
        .exit_code(0)
        .duration_ms(50)
        .field("shell", "bash")
        .insert()
        .await?;

    let clipboard_event = ctx
        .event()
        .clipboard()
        .content("test content")
        .format("text/plain")
        .field("source_app", "test")
        .copied()
        .insert()
        .await?;

    let window_event = ctx
        .event()
        .window()
        .window_id("0x1234")
        .title("Test Window")
        .class("TestApp")
        .field("monitor", 0)
        .focused()
        .insert()
        .await?;

    let system_event = ctx
        .event()
        .system()
        .service("test-service")
        .unit_type("service")
        .field("pid", 12345)
        .started()
        .insert()
        .await?;

    let agent_event = ctx
        .event()
        .agent()
        .name("test-processor")
        .version("1.0.0")
        .uptime_seconds(3600)
        .events_processed(1000)
        .dlq_size(0)
        .field("custom_metric", 42)
        .heartbeat()
        .insert()
        .await?;

    // ===== 2. Event Querying =====

    // Basic queries
    let all_events = ctx.events().fetch().await?;
    let recent = ctx.events().limit(5).fetch().await?;
    let by_source = ctx.events().by_source("test-service").fetch().await?;
    let by_type = ctx.events().by_type("test.comprehensive").fetch().await?;
    let single = ctx.events().by_id(basic_event.id).fetch_one().await?;

    // Count queries
    let total_count = ctx.events().count().await?;
    let fs_count = ctx.events().by_source("fs").count().await?;

    // Checkpoint queries
    let checkpoint_count = ctx.checkpoints().count().await?;
    let processor_checkpoints = ctx
        .checkpoints()
        .by_processor("test-processor")
        .count()
        .await?;

    // ===== 3. Assertions =====

    // Basic assertions
    ctx.assert("event creation")
        .eq(&all_events.len(), &7)? // We created 7 events
        .that(total_count >= 7, "should have at least 7 events")?
        .not_empty(&all_events)?;

    // Event-specific assertions
    ctx.assert_event_count(7).await?;
    ctx.assert_event_exists(fs_event.id).await?;

    // Contextual assertions with rich error messages
    ctx.assert("filesystem event validation")
        .event_eq(&fs_event, &fs_event)? // Should match itself
        .that(
            fs_event.payload["path"] == "/test/file.txt",
            "path should match",
        )?
        .that(fs_event.payload["size"] == 1024, "size should be 1024")?;

    // Collection assertions
    ctx.assert("query results")
        .has_size(&by_source, 1)?
        .some(&single)?;

    // ===== 4. Schema Validation =====

    let schema_id = ctx
        .schema()
        .register(
            "test",
            "validated.event",
            json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "value": {"type": "number", "minimum": 0},
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["name", "value"]
            }),
        )
        .await?;

    // Create validated event
    let validated = ctx
        .validated_event(schema_id)
        .source("test")
        .type_("validated.event")
        .field("name", "test-item")
        .field("value", 42)
        .field("tags", json!(["important", "test"]))
        .insert()
        .await?;

    // Test schema validation
    ctx.schema().validate(&validated, schema_id).await?;
    ctx.schema().assert_valid(&validated, schema_id).await?;

    // Test invalid event
    let invalid = ctx
        .event()
        .source("test")
        .type_("validated.event")
        .field("name", "") // Empty string violates minLength
        .field("value", -1) // Negative violates minimum
        .build()?;

    ctx.schema().assert_invalid(&invalid, schema_id).await?;

    // ===== 5. Timing and Synchronization =====

    // Wait for conditions
    ctx.wait_for_event_count(7).await?;

    ctx.wait_for_condition(|| async {
        let count = ctx.events().by_source("fs").count().await?;
        Ok(count >= 1)
    })
    .await?;

    // Timing utilities
    let barrier = ctx.timing().barrier(2);
    let sync = ctx.timing().synchronizer(Duration::from_secs(1));
    let counter = ctx.timing().event_counter(5);

    // Measure operation time
    let (result, duration) = ctx
        .measure(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            "operation complete"
        })
        .await?;
    assert!(duration >= Duration::from_millis(10));
    assert_eq!(result, "operation complete");

    // ===== 6. Fixtures =====

    // Access fixtures through unified interface
    let session = ctx.fixtures().user_session().await?;
    assert!(!session.event_ids.is_empty());

    // Or use nested namespaces
    let checkpoints = ctx.fixtures().scenarios().populated_checkpoints().await?;
    let dataset = ctx.fixtures().performance().large_dataset().await?;
    let errors = ctx.fixtures().errors().validation_failures().await?;

    // ===== 7. Batch Operations =====

    // Create batch of events
    let batch = ctx.create_event_batch("batch-test", 3);
    assert_eq!(batch.len(), 3);

    for builder in batch {
        builder.insert().await?;
    }

    // Insert pre-built events
    let pre_built: Vec<RawEvent> = (0..3)
        .map(|i| {
            ctx.event()
                .source("pre-built")
                .type_("batch.test")
                .field("index", i)
                .build()
                .unwrap()
        })
        .collect();

    let inserted = ctx.insert_events(&pre_built).await?;
    assert_eq!(inserted.len(), 3);

    // Batch event insertion
    let batch_events = ctx
        .event()
        .source("batch")
        .type_("test.batch")
        .field("batch_name", "test")
        .insert_batch(5)
        .await?;
    assert_eq!(batch_events.len(), 5);

    // ===== 8. Concurrent Testing =====

    let results = ctx
        .run_concurrent(3, |ctx, i| async move {
            let event = ctx
                .event()
                .source("concurrent")
                .type_("worker.task")
                .field("worker_id", i)
                .insert()
                .await?;
            Ok(event.id)
        })
        .await?;

    assert_eq!(results.len(), 3);

    // ===== 9. Advanced Patterns =====

    // Build without inserting
    let built_only = ctx.event().source("built").type_("not.inserted").build()?;

    // Direct insertion (bypasses validation)
    let direct = ctx
        .event()
        .source("direct")
        .type_("bypass.validation")
        .insert_direct()
        .await?;

    // Test utilities
    let test_count = ctx.test_event_count().await;
    assert!(test_count > 0);

    let elapsed = ctx.elapsed();
    assert!(elapsed > Duration::from_millis(0));

    // Redis access (if needed)
    let _redis = ctx.redis().await?;
    // Use redis for test-specific caching, coordination, etc.

    // ===== 10. Error Testing =====

    // Test error assertions
    let error_result: Result<()> = Err(anyhow::anyhow!("test error: permission denied"));
    ctx.assert("error handling")
        .error_contains(&error_result, "permission denied")?;

    // Assert operations complete within timeout
    let fast_op = ctx
        .assert("timeout test")
        .completes_within(async { Ok(42) }, Duration::from_secs(1), "fast operation")
        .await?;
    assert_eq!(fast_op, 42);

    Ok(())
}

#[sinex_test]
async fn test_test_context_isolation(ctx: TestContext) -> Result<()> {
    // Each test gets its own isolated database
    ctx.assert_no_events().await?;

    // Create events in this context
    ctx.event()
        .source("isolation-test")
        .type_("marker")
        .insert()
        .await?;

    // Create a second context
    let ctx2 = TestContext::with_name("other_test").await?;

    // Second context should not see first context's events
    ctx2.assert_no_events().await?;

    // First context still sees its event
    ctx.assert_event_count(1).await?;

    Ok(())
}

#[sinex_test]
async fn test_data_driven_patterns(ctx: TestContext) -> Result<()> {
    // Since parameterized! is now internal, use regular loops
    let long_string = "x".repeat(1000);
    let test_cases = vec![
        ("empty", "", true),
        ("short", "a", true),
        ("normal", "hello world", true),
        ("unicode", "🌍 世界", true),
        ("very_long", long_string.as_str(), false), // Might fail validation
    ];

    for (name, content, should_succeed) in test_cases {
        let result = ctx
            .event()
            .source("data-driven")
            .type_("test.case")
            .field("name", name)
            .field("content", content)
            .insert()
            .await;

        if should_succeed {
            assert!(result.is_ok(), "Test case '{}' should succeed", name);
        } else {
            // Very long strings should still succeed - no validation prevents them
            assert!(
                result.is_ok(),
                "Test case '{}' should succeed (no validation)",
                name
            );
        }
    }

    Ok(())
}
