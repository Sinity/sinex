//! Integration Tests for Sinex
//!
//! This module contains comprehensive integration tests for the Sinex event system.
//! Tests cover database operations, event processing, schema validation, and
//! inter-component interactions using the current architecture:
//! - Repository pattern with `DbPoolExt`
//! - Generic `Id<T>` types
//! - test_event constructor for test events
//! - `#[sinex_test]` macro for async tests
//! - Modern test infrastructure (rstest, insta, tracing-test)

#[path = "integration/mod.rs"]
mod integration;

// Import test utilities with proper prelude for consistent testing
use serde_json::json;
use std::sync::Arc;
// Using shorter imports from sinex-core's re-exports
use sinex_core::{
    Blob, DbPoolExt, DynamicPayload, Event, EventSource, EventType, Id, JsonValue, Ulid,
};
use sinex_test_utils::constants::SOURCE_FIXTURE_REPO_PRIMARY;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::{Timeouts, WaitHelpers};

// =============================================================================
// BASIC DATABASE OPERATIONS - Core functionality tests
// =============================================================================

#[sinex_test]
async fn test_basic_event_insertion_and_retrieval(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _pipeline = ctx.pipeline().await?;

    // Test the fundamental event lifecycle: create -> insert -> retrieve
    let mut event = Event::test_event(
        "integration-test",
        "basic.test",
        json!({
            "test_value": 42,
            "description": "Basic integration test"
        }),
    );
    // Explicitly assume ID for verification
    event.id = Some(Id::new());

    ctx.publish_test_event(&event).await?;

    // Wait for ingestion
    // Wait for ingestion
    ctx.timing()
        .wait_for_source_events("integration-test", 1)
        .await?;

    // Verify event structure
    assert_eq!(event.source.as_str(), "integration-test");
    assert_eq!(event.event_type.as_str(), "basic.test");
    assert_eq!(event.payload["test_value"], json!(42));
    assert!(event.id.is_some());

    // Query back using repository pattern
    let retrieved_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("integration-test"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(retrieved_events.len(), 1);
    assert_eq!(retrieved_events[0].id, event.id);

    Ok(())
}

#[sinex_serial_test]
async fn test_batch_event_insertion(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let _pipeline = ctx.pipeline().await?;

    let source = format!("batch-test-{}", Ulid::new());
    // Test batch insertion performance and correctness
    let mut events = Vec::new();
    for i in 0..10 {
        let mut event = Event::test_event(
            &*source,
            "batch.item",
            json!({
                "index": i,
                "batch_id": "test-batch-001"
            }),
        );
        event.id = Some(Id::new());
        ctx.publish_test_event(&event).await?;
        events.push(event);
    }

    sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(&ctx.pool, &source, 10, 20)
        .await?;

    // Verify all events were inserted
    let retrieved = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(source.as_str()),
            sinex_core::types::Pagination::new(Some(20), None),
        )
        .await?;

    assert_eq!(retrieved.len(), 10);

    // Verify all events have unique IDs by comparing pairwise
    let ids: Vec<_> = retrieved.iter().filter_map(|e| e.id.clone()).collect();
    for (i, id1) in ids.iter().enumerate() {
        for id2 in ids.iter().skip(i + 1) {
            assert_ne!(id1, id2, "All event IDs should be unique");
        }
    }
    assert_eq!(ids.len(), 10);
    Ok(())
}

#[sinex_test]
async fn test_different_event_sources(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _pipeline = ctx.pipeline().await?;
    let test_cases = vec![
        (
            "filesystem",
            "file.created",
            json!({"path": "/test/file.txt"}),
        ),
        ("terminal", "command.executed", json!({"command": "ls -la"})),
        (
            "desktop",
            "window.focused",
            json!({"window_class": "firefox"}),
        ),
        ("system", "service.started", json!({"service": "nginx"})),
    ];

    for (source, event_type, payload) in test_cases {
        // Test various event source patterns
        let mut event = Event::test_event(source, event_type, payload.clone());
        event.id = Some(Id::new());
        ctx.publish_test_event(&event).await?;

        // Verify event can be queried by source (wait for it)
        ctx.timing().wait_for_source_events(source, 1).await?;

        assert_eq!(event.source.as_str(), source);
        assert_eq!(event.event_type.as_str(), event_type);
        assert_eq!(event.payload, payload);

        let source_events = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::new(source),
                sinex_core::types::Pagination::new(Some(10), None),
            )
            .await?;

        assert!(!source_events.is_empty());
        assert!(source_events.iter().any(|e| e.id == event.id));
    }

    Ok(())
}

// =============================================================================
// ULID AND ID SYSTEM TESTS - Generic ID verification
// =============================================================================

#[sinex_test]
async fn test_ulid_ordering_and_consistency(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    // Test ULID time-ordering properties
    let mut event_ids = Vec::new();

    for i in 0..5 {
        let mut event = Event::test_event("ulid-test", "ordering.test", json!({"sequence": i}));
        event.id = Some(Id::new());
        ctx.publish_test_event(&event).await?;

        event_ids.push(event.id.expect("Event should have an ID generated in test"));

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Verify ULIDs are in chronological order (compare string representations)
    for i in 1..event_ids.len() {
        assert!(
            event_ids[i].to_string() > event_ids[i - 1].to_string(),
            "ULIDs should be in chronological order"
        );
    }

    // Verify string representations are also ordered
    let ulid_strings: Vec<String> = event_ids.iter().map(|id| id.to_string()).collect();
    let mut sorted_strings = ulid_strings.clone();
    sorted_strings.sort();

    assert_eq!(
        ulid_strings, sorted_strings,
        "ULID strings should be naturally sorted"
    );

    // Ensure they arrived
    ctx.timing().wait_for_source_events("ulid-test", 5).await?;

    Ok(())
}

#[sinex_serial_test]
async fn test_generic_id_type_safety(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;

    // Test that generic IDs provide type safety
    let event_id = Id::<Event<JsonValue>>::new();
    let blob_id = Id::<Blob>::new();

    // IDs should be unique even across types
    assert_ne!(event_id.to_string(), blob_id.to_string());

    // Create an event with a specific ID
    let mut event = Event::test_event(
        EventSource::from_static("id-test"),
        EventType::from_static("id.safety.test"),
        json!({"event_id": event_id.to_string()}),
    );
    // Explicitly set ID
    event.id = Some(event_id.clone());

    // Insert (Publish) and verify
    ctx.publish_test_event(&event).await?;
    ctx.timing().wait_for_source_events("id-test", 1).await?;

    let retrieved_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("id-test"),
            sinex_core::types::Pagination::new(Some(1), None),
        )
        .await?;

    let retrieved = retrieved_events
        .into_iter()
        .next()
        .expect("Event should exist");

    assert_eq!(retrieved.payload["event_id"], json!(event_id.to_string()));
    assert_eq!(retrieved.id, Some(event_id));

    Ok(())
}

// =============================================================================
// REPOSITORY PATTERN TESTS - DbPoolExt functionality
// =============================================================================

#[sinex_serial_test]
async fn test_repository_pattern_functionality(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    // Test the repository pattern with various query operations

    // Insert test data
    let source_suffix = format!("{}-{}", SOURCE_FIXTURE_REPO_PRIMARY, Ulid::new());

    let mut e1 = Event::test_event(&*source_suffix, "type.a", json!({"category": "alpha"}));
    e1.id = Some(Id::new());
    ctx.publish_test_event(&e1).await?;

    let mut e2 = Event::test_event(&*source_suffix, "type.b", json!({"category": "beta"}));
    e2.id = Some(Id::new());
    ctx.publish_test_event(&e2).await?;

    let mut e3 = Event::test_event(&*source_suffix, "type.a", json!({"category": "gamma"}));
    e3.id = Some(Id::new());
    ctx.publish_test_event(&e3).await?;

    // Wait for ingestion
    ctx.timing()
        .wait_for_source_events(&source_suffix, 3)
        .await?;

    let repo = ctx.pool.events();

    // Test querying by source
    let by_source = repo
        .get_by_source(
            &EventSource::from(source_suffix.as_str()),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    assert_eq!(by_source.len(), 3);

    // Test querying by type
    let by_type = repo
        .get_by_event_type(
            &EventType::from_static("type.a"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    assert_eq!(by_type.len(), 2);

    // Test counting
    let count = repo
        .count_by_source(&EventSource::from(source_suffix.as_str()))
        .await?;
    assert_eq!(count, 3);

    Ok(())
}

#[sinex_serial_test]
async fn test_repository_pagination_and_limits(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    // Test pagination functionality

    // Insert 20 test events
    for i in 0..20 {
        let mut event = Event::test_event("pagination-test", "page.test", json!({"index": i}));
        event.id = Some(Id::new());
        ctx.publish_test_event(&event).await?;
    }

    let expected = 20usize;
    ctx.timing()
        .wait_for_source_events("pagination-test", expected)
        .await?;

    let repo = ctx.pool.events();

    // Test limit
    let limited = repo
        .get_by_source(
            &EventSource::from_static("pagination-test"),
            sinex_core::types::Pagination::new(Some(5), None),
        )
        .await?;
    assert_eq!(limited.len(), 5);

    // Test that all events can be retrieved
    let all_events = repo
        .get_by_source(
            &EventSource::from_static("pagination-test"),
            sinex_core::types::Pagination::new(Some(100), None),
        )
        .await?;
    assert_eq!(
        all_events.len(),
        20,
        "expected all inserted events to be returned"
    );

    Ok(())
}

// =============================================================================
// CONCURRENT OPERATIONS TESTS - Thread safety and isolation
// =============================================================================

#[sinex_serial_test]
async fn test_concurrent_event_insertion(ctx: TestContext) -> TestResult<()> {
    // Test concurrent insertions don't interfere with each other
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;

    // Share a single test context across concurrent tasks
    let ctx = Arc::new(ctx);
    let _pipeline = ctx.pipeline().await?;
    let mut handles = Vec::new();

    for i in 0..10 {
        let ctx_clone = Arc::clone(&ctx);
        let handle = tokio::spawn(async move {
            let mut event = Event::test_event(
                "concurrent-test",
                "concurrent.event",
                json!({
                    "task_id": i,
                    "timestamp": chrono::Utc::now().timestamp()
                }),
            );
            event.id = Some(Id::new());

            ctx_clone.publish_test_event(&event).await?;

            Ok::<_, color_eyre::eyre::Error>(event.id.expect("Event should have ID after creation"))
        });
        handles.push(handle);
    }

    // Collect results
    let mut event_ids = Vec::new();
    for handle in handles {
        let event_id = handle.await??;
        event_ids.push(event_id);
    }

    // Verify all insertions succeeded and IDs are unique
    assert_eq!(
        event_ids.len(),
        10,
        "Concurrent insertion should create 10 events"
    );
    let mut seen = std::collections::HashSet::new();
    for id in &event_ids {
        assert!(seen.insert(id.as_ulid()), "Event IDs should remain unique");
    }

    // Verify all events are in database
    WaitHelpers::wait_for_source_events(&ctx.pool, "concurrent-test", 10, 10).await?;
    let final_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("concurrent-test"),
            sinex_core::types::Pagination::new(Some(24), None),
        )
        .await?;
    assert_eq!(
        final_events.len(),
        10,
        "Expected 10 persisted concurrent-test events"
    );

    Ok(())
}

#[sinex_test]
async fn test_database_transaction_isolation(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;

    // Test that test contexts are properly isolated
    let test_id = uuid::Uuid::new_v4().to_string();

    // Create event in this context
    let mut event = Event::test_event(
        "isolation-test",
        "isolation.marker",
        json!({"test_id": test_id.clone()}),
    );
    event.id = Some(Id::new());
    ctx.publish_test_event(&event).await?;

    // Wait for ingestion
    ctx.timing()
        .wait_for_source_events("isolation-test", 1)
        .await?;

    // Create another context (should be isolated). If it happens to reuse the same
    // database, allocate a fresh one to avoid cross-contamination.
    let mut other_ctx = TestContext::new().await?.with_nats().shared().await?;
    if other_ctx.database_url() == ctx.database_url() {
        other_ctx = TestContext::new().await?.with_nats().shared().await?;
    }

    // Other context should not see our event
    let other_events = other_ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("isolation-test"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(other_events.len(), 0, "Test contexts should be isolated");

    // But our context should see it
    let our_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("isolation-test"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(our_events.len(), 1, "Should see our own events");
    Ok(())
}

// =============================================================================
// SCHEMA VALIDATION TESTS - JSON Schema integration
// =============================================================================

#[sinex_test]
async fn test_json_schema_validation_integration(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;

    ctx.publish(DynamicPayload::new(
        "schema-test",
        "validated.event",
        json!({
            "product_id": 12345,
            "quantity": 10,
            "tags": ["sale", "summer"]
        }),
    ))
    .await?;

    let retrieved = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("schema-test"),
            sinex_core::types::Pagination::new(Some(1), None),
        )
        .await?;

    assert_eq!(retrieved[0].payload["product_id"], json!(12345));
    assert_eq!(retrieved[0].payload["quantity"], json!(10));

    Ok(())
}

// =============================================================================
// PERFORMANCE AND STRESS TESTS - Load handling
// =============================================================================

#[sinex_test]
async fn test_large_payload_handling(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;

    let large_string = "a".repeat(1024 * 1024); // 1MB

    ctx.publish(DynamicPayload::new(
        "load-test",
        "large.payload",
        json!({
            "data": large_string,
            "meta": "metadata"
        }),
    ))
    .await?;

    let retrieved = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("load-test"),
            sinex_core::types::Pagination::new(Some(1), None),
        )
        .await?;

    assert_eq!(retrieved.len(), 1);
    let payload_str = retrieved[0].payload["data"]
        .as_str()
        .expect("data is string");
    assert_eq!(payload_str.len(), 1024 * 1024);

    Ok(())
}

#[sinex_serial_test]
async fn test_high_throughput_insertion(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;

    let source = format!("throughput-{}", Ulid::new());
    let mut events = Vec::new();

    // Prepare 1000 events
    for i in 0..1000 {
        let mut event = Event::test_event(&*source, "perf.test", json!({"i": i}));
        event.id = Some(Id::new());
        events.push(event);
    }

    let start = std::time::Instant::now();

    // Concurrent publish
    // Note: We don't use tokio::spawn here to avoid overhead of task creation dominating the test,
    // but synchronous publish might be slow. Let's use futures::join_all or similar if needed.
    // For now, sequential publish is fine as NATS publish is fast.
    for event in &events {
        ctx.publish_test_event(event).await?;
    }

    // Wait for all to be ingested
    // This includes the time for NATS->Ingestd->DB
    sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
        &ctx.pool, &source, 1000, 30,
    )
    .await?;

    let duration = start.elapsed();
    tracing::info!("Inserted 1000 events in {:?}", duration);

    // Should be able to process 1000 events quickly
    // Pipeline might be slower than raw DB insert, so we relax the constraint slightly if needed,
    // but QUICK (5s) is plenty for 1000 events.
    assert!(duration < std::time::Duration::from_secs(Timeouts::QUICK));

    // Verify count
    let count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::new(&source))
        .await?;
    assert_eq!(count, 1000);

    Ok(())
}

// =============================================================================
// ERROR HANDLING AND EDGE CASES - Robustness testing
// =============================================================================

#[sinex_test]
async fn test_error_propagation_and_recovery(ctx: TestContext) -> TestResult<()> {
    // Test error handling in various scenarios

    // Test invalid source (empty string)
    let invalid_result = ctx
        .publish(DynamicPayload::new(
            "", // Empty source should fail
            "error.test",
            json!({}),
        ))
        .await;

    assert!(invalid_result.is_err(), "Empty source should cause error");

    // Test that pool is still usable after error
    let valid_event = ctx
        .publish(DynamicPayload::new(
            "error-recovery-test",
            "recovery.test",
            json!({"recovery": true}),
        ))
        .await?;

    assert_eq!(valid_event.source.as_str(), "error-recovery-test");

    Ok(())
}

#[sinex_test]
async fn test_unicode_and_special_characters(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let special_strings = vec![
        "Hello World",
        "Hello World 🌍🚀",          // Emoji
        "こんにちは",                // Japanese
        "ñ",                         // Spanish
        "Müller",                    // German
        "NULL\0BYTE",                // Null byte (might be tricky in JSON/Postgres)
        "SQL'Injection",             // SQL quote
        "<script>alert(1)</script>", // XSS
    ];

    for s in special_strings {
        let inserted = ctx
            .publish(DynamicPayload::new(
                "unicode-test",
                "special.chars",
                json!({ "content": s }),
            ))
            .await?;

        let retrieved = ctx
            .pool
            .events()
            .get_by_id(inserted.id.clone().expect("event id"))
            .await?
            .expect("event should exist");

        let mut expected_content = json!(s);
        TestContext::sanitize_payload(&mut expected_content);

        assert_eq!(retrieved.payload["content"], expected_content);
    }

    Ok(())
}

// =============================================================================
// TIMING AND SYNCHRONIZATION TESTS - TestContext utilities
// =============================================================================

#[sinex_test]
async fn test_timing_utilities(ctx: TestContext) -> TestResult<()> {
    // Test timing measurement capabilities
    let start_time = ctx.elapsed();

    // Do some work
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let end_time = ctx.elapsed();
    assert!(end_time > start_time, "Time should advance");
    assert!(end_time.as_millis() >= 50, "Should measure at least 50ms");

    // Test measurement helper
    let (result, duration) = ctx
        .measure(async {
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
            Ok::<_, color_eyre::eyre::Error>("measured_result")
        })
        .await?;

    assert_eq!(result?, "measured_result");
    assert!(
        duration.as_millis() >= 25,
        "Duration should be at least 25ms"
    );

    Ok(())
}

#[sinex_test]
async fn test_assertion_helpers(ctx: TestContext) -> TestResult<()> {
    // Test enhanced assertion functionality

    // Create test data
    let events = vec![
        ctx.publish(DynamicPayload::new("assertion-test", "test.a", json!({})))
            .await?,
        ctx.publish(DynamicPayload::new("assertion-test", "test.b", json!({})))
            .await?,
        ctx.publish(DynamicPayload::new("assertion-test", "test.c", json!({})))
            .await?,
    ];

    // Test collection assertions
    ctx.assert("event collection validation")
        .not_empty(&events)?
        .has_size(&events, 3)?;
    ctx.assert_unique_event_ids(&events)?;

    // Test individual event assertions
    for event in &events {
        ctx.assert("event validation")
            .some(&event.id)?
            .eq(&event.source.as_str(), &"assertion-test")?;
    }

    // Test database count assertion
    let count = ctx.pool.events().count_all().await?;
    assert_eq!(count, 3);

    Ok(())
}

// =============================================================================
// MODERN TEST INFRASTRUCTURE INTEGRATION - rstest, insta, tracing
// =============================================================================

#[sinex_serial_test]
#[traced_test]
async fn test_rstest_integration_10(ctx: TestContext) -> TestResult<()> {
    let event_count = 10;
    // Test rstest parameterization with sinex_test
    tracing::info!("Testing with {} events", event_count);

    // Create specified number of events
    for i in 0..event_count {
        ctx.publish(DynamicPayload::new(
            "rstest-integration",
            "parameterized.test",
            json!({
                "index": i,
                "total": event_count
            }),
        ))
        .await?;
    }

    // Verify count
    let actual_count = ctx
        .pool
        .events()
        .count_by_source(&EventSource::from_static("rstest-integration"))
        .await?;

    assert_eq!(actual_count, event_count as i64);

    tracing::info!("Successfully inserted and verified {} events", event_count);

    Ok(())
}

#[sinex_test]
async fn test_insta_snapshots(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    // Test snapshot testing with insta
    let mut e1 = Event::test_event(
        "snapshot-test",
        "snapshot.a",
        json!({"value": 1, "name": "first"}),
    );
    e1.id = Some(Id::new());
    ctx.publish_test_event(&e1).await?;

    let mut e2 = Event::test_event(
        "snapshot-test",
        "snapshot.b",
        json!({"value": 2, "name": "second"}),
    );
    e2.id = Some(Id::new());
    ctx.publish_test_event(&e2).await?;

    // Wait for ingestion
    ctx.timing()
        .wait_for_source_events("snapshot-test", 2)
        .await?;

    let retrieved = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("snapshot-test"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    // Create snapshot of the results
    let snapshot_data = serde_json::json!({
        "event_count": retrieved.len(),
        "events": retrieved.iter().map(|e| {
            serde_json::json!({
                "source": e.source.as_str(),
                "event_type": e.event_type.as_str(),
                "payload": e.payload
            })
        }).collect::<Vec<_>>()
    });

    assert_json_snapshot!(snapshot_data, @r###"
    {
      "event_count": 2,
      "events": [
        {
          "event_type": "snapshot.b",
          "payload": {
            "name": "second",
            "value": 2
          },
          "source": "snapshot-test"
        },
        {
          "event_type": "snapshot.a",
          "payload": {
            "name": "first",
            "value": 1
          },
          "source": "snapshot-test"
        }
      ]
    }
    "###);

    Ok(())
}

// =============================================================================
// END-TO-END WORKFLOW TESTS - Complete system integration
// =============================================================================

#[sinex_test]
async fn test_complete_event_processing_workflow(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    // Test a complete end-to-end workflow

    // 1. Create initial event
    let mut e1 = Event::test_event(
        "workflow-test",
        "workflow.started",
        json!({
            "workflow_id": "wf-001",
            "step": 1
        }),
    );
    e1.id = Some(Id::new());
    ctx.publish_test_event(&e1).await?;

    // 2. Create processing events
    let mut e2 = Event::test_event(
        "workflow-test",
        "workflow.processing",
        json!({
            "workflow_id": "wf-001",
            "step": 2,
            "action": "validate_input"
        }),
    );
    e2.id = Some(Id::new());
    ctx.publish_test_event(&e2).await?;

    let mut e3 = Event::test_event(
        "workflow-test",
        "workflow.processing",
        json!({
            "workflow_id": "wf-001",
            "step": 3,
            "action": "transform_data"
        }),
    );
    e3.id = Some(Id::new());
    ctx.publish_test_event(&e3).await?;

    // 3. Create completion event
    let mut e4 = Event::test_event(
        "workflow-test",
        "workflow.completed",
        json!({
            "workflow_id": "wf-001",
            "step": 4,
            "result": "success",
            "duration_ms": 1250
        }),
    );
    e4.id = Some(Id::new());
    ctx.publish_test_event(&e4).await?;

    // Wait for all 4 events
    ctx.timing()
        .wait_for_source_events("workflow-test", 4)
        .await?;

    // 4. Verify complete workflow
    let workflow_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("workflow-test"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(workflow_events.len(), 4);

    // Results are returned newest-first; ensure ordering is monotonically non-increasing
    for i in 1..workflow_events.len() {
        let prev_ts = workflow_events[i - 1]
            .id
            .as_ref()
            .expect("event id present")
            .as_ulid()
            .timestamp();
        let curr_ts = workflow_events[i]
            .id
            .as_ref()
            .expect("event id present")
            .as_ulid()
            .timestamp();
        assert!(
            curr_ts <= prev_ts,
            "Events should be in reverse-chronological order"
        );
    }

    // Verify workflow stages
    let started = workflow_events
        .iter()
        .find(|e| e.event_type.as_str() == "workflow.started")
        .expect("Should have started event");
    let completed = workflow_events
        .iter()
        .find(|e| e.event_type.as_str() == "workflow.completed")
        .expect("Should have completed event");

    assert_eq!(started.payload["workflow_id"], json!("wf-001"));
    assert_eq!(completed.payload["result"], json!("success"));

    Ok(())
}
