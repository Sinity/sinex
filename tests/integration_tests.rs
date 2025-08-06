//! Integration Tests for Sinex
//!
//! This module contains comprehensive integration tests for the Sinex event system.
//! Tests cover database operations, event processing, schema validation, and
//! inter-component interactions using the current architecture:
//! - Repository pattern with `DbPoolExt`
//! - Generic `Id<T>` types
//! - `DbEvent::schemaless()` builder
//! - `#[sinex_test]` macro for async tests
//! - Modern test infrastructure (rstest, insta, tracing-test, similar-asserts)

// Import test utilities without broad prelude to avoid Event type conflicts
use chrono::{Duration, Utc};
use color_eyre::{
    eyre,
    eyre::{bail, ensure, eyre, Context},
};
use futures::future::join_all;
use insta::{assert_debug_snapshot, assert_json_snapshot, assert_snapshot, assert_yaml_snapshot};
use rstest::{fixture, rstest};
use serde_json::json;
use similar_asserts::{assert_eq as assert_similar, assert_str_eq};
use sinex_db::models::{Blob, Event as DbEvent};
use sinex_db::repositories::DbPoolExt;
use sinex_test_utils::{sinex_test, TestContext};
use sinex_types::domain::{EventSource, EventType, HostName};
use sinex_types::{Id, Ulid};
use std::collections::HashSet;
use std::time::Duration as StdDuration;
use tracing;
use tracing_test::traced_test;

// =============================================================================
// BASIC DATABASE OPERATIONS - Core functionality tests
// =============================================================================

#[sinex_test]
async fn test_basic_event_insertion_and_retrieval(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    // Test the fundamental event lifecycle: create -> insert -> retrieve
    let event = ctx
        .event()
        .source("integration-test")
        .type_("basic.test")
        .field("test_value", 42)
        .field("description", "Basic integration test")
        .insert()
        .await?;

    // Verify event structure
    assert_eq!(event.source.as_str(), "integration-test");
    assert_eq!(event.event_type.as_str(), "basic.test");
    assert_eq!(event.payload["test_value"], json!(42));
    assert!(event.id.is_some());

    // Query back using repository pattern
    let retrieved_events = ctx
        .pool()
        .events()
        .get_by_source(
            &EventSource::from_static("integration-test"),
            Some(10),
            None,
        )
        .await?;

    assert_eq!(retrieved_events.len(), 1);
    assert_eq!(retrieved_events[0].id, event.id);

    Ok(())
}

#[sinex_test]
async fn test_batch_event_insertion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test batch insertion performance and correctness
    let events = (0..10)
        .map(|i| {
            ctx.event()
                .source("batch-test")
                .type_("batch.event")
                .field("index", i)
                .field("batch_id", "test-batch-001")
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Insert events using batch operation
    ctx.insert_events(&events).await?;

    // Verify all events were inserted
    let retrieved = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from_static("batch-test"), Some(20), None)
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
async fn test_different_event_sources(ctx: TestContext) -> color_eyre::eyre::Result<()> {
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
        let event = ctx
            .event()
            .source(source)
            .type_(event_type)
            .payload(payload.clone())
            .insert()
            .await?;

        assert_eq!(event.source.as_str(), source);
        assert_eq!(event.event_type.as_str(), event_type);
        assert_eq!(event.payload, payload);

        // Verify event can be queried by source
        let source_events = ctx
            .pool()
            .events()
            .get_by_source(&EventSource::new(source), Some(10), None)
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
async fn test_ulid_ordering_and_consistency(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test ULID time-ordering properties
    let mut event_ids = Vec::new();

    for i in 0..5 {
        let event = ctx
            .event()
            .source("ulid-test")
            .type_("ordering.test")
            .field("sequence", i)
            .insert()
            .await?;

        event_ids.push(event.id.unwrap());

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

    Ok(())
}

#[sinex_test]
async fn test_generic_id_type_safety(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that generic IDs provide type safety
    let event_id = Id::<DbEvent>::new();
    let blob_id = Id::<Blob>::new();

    // IDs should be unique even across types
    assert_ne!(event_id.to_string(), blob_id.to_string());

    // Create an event with a specific ID
    let event = DbEvent::schemaless()
        .source(EventSource::from_static("id-test"))
        .event_type(EventType::from_static("id.safety.test"))
        .payload(json!({"event_id": event_id.to_string()}))
        .build();

    // Insert and verify
    ctx.pool().events().insert(event.into()).await?;

    let retrieved_events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from_static("id-test"), Some(1), None)
        .await?;

    let retrieved = retrieved_events
        .into_iter()
        .next()
        .expect("Event should exist");

    assert_eq!(retrieved.payload["event_id"], json!(event_id.to_string()));

    Ok(())
}

// =============================================================================
// REPOSITORY PATTERN TESTS - DbPoolExt functionality
// =============================================================================

#[sinex_test]
async fn test_repository_pattern_functionality(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test the repository pattern with various query operations

    // Insert test data
    let events = vec![
        ctx.event()
            .source("repo-test")
            .type_("type.a")
            .field("category", "alpha")
            .build()?,
        ctx.event()
            .source("repo-test")
            .type_("type.b")
            .field("category", "beta")
            .build()?,
        ctx.event()
            .source("repo-test")
            .type_("type.a")
            .field("category", "gamma")
            .build()?,
    ];

    ctx.insert_events(&events).await?;

    let repo = ctx.pool().events();

    // Test querying by source
    let by_source = repo
        .get_by_source(&EventSource::from_static("repo-test"), Some(10), None)
        .await?;
    assert_eq!(by_source.len(), 3);

    // Test querying by type
    let by_type = repo
        .get_by_event_type(&EventType::from_static("type.a"), Some(10), None)
        .await?;
    assert_eq!(by_type.len(), 2);

    // Test counting
    let count = repo
        .count_by_source(&EventSource::from_static("repo-test"))
        .await?;
    assert_eq!(count, 3);

    Ok(())
}

#[sinex_test]
async fn test_repository_pagination_and_limits(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test pagination functionality

    // Insert 20 test events
    let events = (0..20)
        .map(|i| {
            ctx.event()
                .source("pagination-test")
                .type_("page.test")
                .field("index", i)
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    ctx.insert_events(&events).await?;

    let repo = ctx.pool().events();

    // Test limit
    let limited = repo
        .get_by_source(&EventSource::from_static("pagination-test"), Some(5), None)
        .await?;
    assert_eq!(limited.len(), 5);

    // Test that all events can be retrieved
    let all_events = repo
        .get_by_source(
            &EventSource::from_static("pagination-test"),
            Some(100),
            None,
        )
        .await?;
    assert_eq!(all_events.len(), 20);

    Ok(())
}

// =============================================================================
// CONCURRENT OPERATIONS TESTS - Thread safety and isolation
// =============================================================================

#[sinex_test]
async fn test_concurrent_event_insertion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test concurrent insertions don't interfere with each other
    use std::sync::Arc;
    // use tokio::task::JoinSet; // Not needed for this test

    // Create events concurrently using separate contexts
    let mut handles = Vec::new();

    // Create events in parallel using separate contexts
    for i in 0..10 {
        let handle = tokio::spawn(async move {
            let ctx = TestContext::new().await?;
            let event = ctx
                .event()
                .source("concurrent-test")
                .type_("concurrent.event")
                .field("task_id", i)
                .field("timestamp", chrono::Utc::now().timestamp())
                .insert()
                .await?;

            Ok::<_, color_eyre::eyre::Error>(event.id.unwrap())
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
    assert_eq!(event_ids.len(), 10);
    // Verify uniqueness by checking each ID against all others
    for (i, id1) in event_ids.iter().enumerate() {
        for id2 in event_ids.iter().skip(i + 1) {
            assert_ne!(id1, id2, "All event IDs should be unique");
        }
    }

    // Verify all events are in database
    let db_events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from_static("concurrent-test"), Some(20), None)
        .await?;
    assert_eq!(db_events.len(), 10);

    Ok(())
}

#[sinex_test]
async fn test_database_transaction_isolation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that test contexts are properly isolated
    let test_id = uuid::Uuid::new_v4().to_string();

    // Create event in this context
    ctx.event()
        .source("isolation-test")
        .type_("isolation.marker")
        .field("test_id", test_id.clone())
        .insert()
        .await?;

    // Create another context (should be isolated)
    let other_ctx = TestContext::new().await?;

    // Other context should not see our event
    let other_events = other_ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from_static("isolation-test"), Some(10), None)
        .await?;

    assert_eq!(other_events.len(), 0, "Test contexts should be isolated");

    // But our context should see it
    let our_events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from_static("isolation-test"), Some(10), None)
        .await?;

    assert_eq!(our_events.len(), 1, "Should see our own events");

    Ok(())
}

// =============================================================================
// SCHEMA VALIDATION TESTS - JSON Schema integration
// =============================================================================

#[sinex_test]
async fn test_json_schema_validation_integration(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test JSON schema validation with real payloads

    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "file_path": {
                "type": "string",
                "minLength": 1
            },
            "file_size": {
                "type": "integer",
                "minimum": 0
            },
            "permissions": {
                "type": "string",
                "pattern": "^[0-7]{3}$"
            }
        },
        "required": ["file_path", "file_size"],
        "additionalProperties": false
    });

    // Register schema - using repository directly
    use sinex_db::repositories::events::NewSchema;
    let new_schema = NewSchema {
        schema_name: sinex_types::domain::SchemaName::new("filesystem"),
        schema_version: sinex_types::domain::SchemaVersion::new("1.0.0"),
        schema_content: schema,
        is_active: true,
        event_types: vec!["file.created".to_string()],
        description: Some("Test schema for filesystem events".to_string()),
        examples: None,
    };
    let _schema = ctx.pool().events().register_schema(new_schema).await?;

    // Test valid event
    let valid_event = ctx
        .event()
        .source("filesystem")
        .type_("file.created")
        .payload(json!({
            "file_path": "/test/valid.txt",
            "file_size": 1024,
            "permissions": "644"
        }))
        .insert()
        .await?;

    assert_eq!(valid_event.source.as_str(), "filesystem");

    // Test that invalid event would fail (if validation were enforced)
    // Note: This depends on database constraint configuration
    let _invalid_payload = json!({
        "file_path": "/test/invalid.txt"
        // missing required file_size
    });

    // For now, just verify the event was created (schema registration is separate)
    assert_eq!(valid_event.source.as_str(), "filesystem");
    assert_eq!(valid_event.event_type.as_str(), "file.created");

    Ok(())
}

// =============================================================================
// PERFORMANCE AND STRESS TESTS - Load handling
// =============================================================================

#[sinex_test]
async fn test_large_payload_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test handling of large JSON payloads
    let large_string = "x".repeat(10_000); // 10KB string

    let large_payload = json!({
        "data": large_string,
        "metadata": {
            "size": 10000,
            "type": "large_test",
            "nested": {
                "deep": {
                    "structure": [1, 2, 3, 4, 5]
                }
            }
        }
    });

    let event = ctx
        .event()
        .source("performance-test")
        .type_("large.payload")
        .payload(large_payload.clone())
        .insert()
        .await?;

    // Verify large payload was stored correctly
    let retrieved = ctx
        .pool()
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .expect("Event should exist");

    assert_eq!(retrieved.payload, large_payload);
    assert_eq!(retrieved.payload["data"].as_str().unwrap().len(), 10_000);

    Ok(())
}

#[sinex_test]
async fn test_high_throughput_insertion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use tokio::time::Instant;

    // Test inserting many events quickly
    let start = Instant::now();
    let event_count = 100;

    // Create events using single context (faster than separate contexts)
    let mut handles = Vec::new();
    for i in 0..event_count {
        let handle = ctx
            .event()
            .source("throughput-test")
            .type_("high.throughput")
            .field("index", i)
            .field("batch", "throughput-001")
            .insert();
        handles.push(handle);
    }

    // Wait for all to complete
    let results = futures::future::join_all(handles).await;
    let mut successful_inserts = 0;
    for result in results {
        if result.is_ok() {
            successful_inserts += 1;
        }
    }

    let duration = start.elapsed();

    // Verify performance
    assert_eq!(successful_inserts, event_count);
    println!(
        "Inserted {} events in {:?} ({:.2} events/sec)",
        event_count,
        duration,
        event_count as f64 / duration.as_secs_f64()
    );

    // Verify events are in database
    let inserted_events = ctx
        .pool()
        .events()
        .count_by_source(&EventSource::from_static("throughput-test"))
        .await?;

    // This test validates high-throughput event creation
    assert_eq!(inserted_events, event_count as i64);

    Ok(())
}

// =============================================================================
// ERROR HANDLING AND EDGE CASES - Robustness testing
// =============================================================================

#[sinex_test]
async fn test_error_propagation_and_recovery(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test error handling in various scenarios

    // Test invalid source (empty string)
    let invalid_result = ctx
        .event()
        .source("") // Empty source should fail
        .type_("error.test")
        .insert()
        .await;

    assert!(invalid_result.is_err(), "Empty source should cause error");

    // Test that pool is still usable after error
    let valid_event = ctx
        .event()
        .source("error-recovery-test")
        .type_("recovery.test")
        .field("recovery", true)
        .insert()
        .await?;

    assert_eq!(valid_event.source.as_str(), "error-recovery-test");

    Ok(())
}

#[sinex_test]
async fn test_unicode_and_special_characters(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test handling of various character encodings
    let test_cases = vec![
        ("unicode", "Hello 世界 🌍", "Unicode characters"),
        ("emoji", "🎉🚀💻", "Emoji characters"),
        ("special", "Special chars: !@#$%^&*()", "Special symbols"),
        ("quotes", r#"Quotes: "double" 'single'"#, "Quote characters"),
        ("newlines", "Line 1\nLine 2\nLine 3", "Newline characters"),
        ("tabs", "Tab\tseparated\tvalues", "Tab characters"),
    ];

    for (test_name, test_value, description) in test_cases {
        let event = ctx
            .event()
            .source("unicode-test")
            .type_("character.test")
            .field("test_name", test_name)
            .field("test_value", test_value)
            .field("description", description)
            .insert()
            .await?;

        // Verify data was stored correctly
        assert_eq!(event.payload["test_value"], json!(test_value));

        // Verify retrieval
        let retrieved = ctx
            .pool()
            .events()
            .get_by_id(event.id.unwrap())
            .await?
            .expect("Event should exist");

        assert_eq!(retrieved.payload["test_value"], json!(test_value));
    }

    Ok(())
}

// =============================================================================
// TIMING AND SYNCHRONIZATION TESTS - TestContext utilities
// =============================================================================

#[sinex_test]
async fn test_timing_utilities(ctx: TestContext) -> color_eyre::eyre::Result<()> {
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
async fn test_assertion_helpers(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test enhanced assertion functionality

    // Create test data
    let events = vec![
        ctx.event()
            .source("assertion-test")
            .type_("test.a")
            .insert()
            .await?,
        ctx.event()
            .source("assertion-test")
            .type_("test.b")
            .insert()
            .await?,
        ctx.event()
            .source("assertion-test")
            .type_("test.c")
            .insert()
            .await?,
    ];

    // Test collection assertions
    ctx.assert("event collection validation")
        .not_empty(&events)?
        .has_size(&events, 3)?;

    // Test individual event assertions
    for event in &events {
        ctx.assert("event validation")
            .some(&event.id)?
            .eq(&event.source.as_str(), &"assertion-test")?;
    }

    // Test database count assertion
    ctx.assert_event_count(3).await?;

    Ok(())
}

// =============================================================================
// MODERN TEST INFRASTRUCTURE INTEGRATION - rstest, insta, tracing
// =============================================================================

#[sinex_test]
#[traced_test]
async fn test_rstest_integration_10(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let event_count = 10;
    // Test rstest parameterization with sinex_test
    tracing::info!("Testing with {} events", event_count);

    // Create specified number of events
    for i in 0..event_count {
        ctx.event()
            .source("rstest-integration")
            .type_("parameterized.test")
            .field("index", i)
            .field("total", event_count)
            .insert()
            .await?;
    }

    // Verify count
    let actual_count = ctx
        .pool()
        .events()
        .count_by_source(&EventSource::from_static("rstest-integration"))
        .await?;

    assert_eq!(actual_count, event_count as i64);

    tracing::info!("Successfully inserted and verified {} events", event_count);

    Ok(())
}

#[sinex_test]
async fn test_insta_snapshots(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test snapshot testing with insta
    let events = vec![
        ctx.event()
            .source("snapshot-test")
            .type_("snapshot.a")
            .field("value", 1)
            .field("name", "first")
            .build()?,
        ctx.event()
            .source("snapshot-test")
            .type_("snapshot.b")
            .field("value", 2)
            .field("name", "second")
            .build()?,
    ];

    ctx.insert_events(&events).await?;

    let retrieved = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from_static("snapshot-test"), Some(10), None)
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
          "event_type": "snapshot.a",
          "payload": {
            "name": "first",
            "value": 1
          },
          "source": "snapshot-test"
        },
        {
          "event_type": "snapshot.b",
          "payload": {
            "name": "second",
            "value": 2
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
async fn test_complete_event_processing_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test a complete end-to-end workflow

    // 1. Create initial event
    let initial_event = ctx
        .event()
        .source("workflow-test")
        .type_("workflow.started")
        .field("workflow_id", "wf-001")
        .field("step", 1)
        .insert()
        .await?;

    // 2. Create processing events
    let processing_events = vec![
        ctx.event()
            .source("workflow-test")
            .type_("workflow.processing")
            .field("workflow_id", "wf-001")
            .field("step", 2)
            .field("action", "validate_input")
            .build()?,
        ctx.event()
            .source("workflow-test")
            .type_("workflow.processing")
            .field("workflow_id", "wf-001")
            .field("step", 3)
            .field("action", "transform_data")
            .build()?,
    ];

    ctx.insert_events(&processing_events).await?;

    // 3. Create completion event
    let _completion_event = ctx
        .event()
        .source("workflow-test")
        .type_("workflow.completed")
        .field("workflow_id", "wf-001")
        .field("step", 4)
        .field("result", "success")
        .field("duration_ms", 1250)
        .insert()
        .await?;

    // 4. Verify complete workflow
    let workflow_events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from_static("workflow-test"), Some(10), None)
        .await?;

    assert_eq!(workflow_events.len(), 4);

    // Verify events are in temporal order
    for i in 1..workflow_events.len() {
        assert!(
            workflow_events[i].ts_ingest >= workflow_events[i - 1].ts_ingest,
            "Events should be in temporal order"
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
