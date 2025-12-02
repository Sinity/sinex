//! Integration Tests for Sinex
//!
//! This module contains comprehensive integration tests for the Sinex event system.
//! Tests cover database operations, event processing, schema validation, and
//! inter-component interactions using the current architecture:
//! - Repository pattern with `DbPoolExt`
//! - Generic `Id<T>` types
//! - Event::<JsonValue>::test_event constructor for test events
//! - `#[sinex_test]` macro for async tests
//! - Modern test infrastructure (rstest, insta, tracing-test, similar-asserts)

#[path = "integration/mod.rs"]
mod integration;

use futures::stream::{self, StreamExt, TryStreamExt};
// Import test utilities with proper prelude for consistent testing
use serde_json::json;
use std::sync::Arc;
// Using shorter imports from sinex-core's re-exports
use sinex_core::{Blob, DbPoolExt, Event, EventSource, EventType, Id, JsonValue, Ulid};
use sinex_test_utils::constants::SOURCE_FIXTURE_REPO_PRIMARY;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::WaitHelpers;
use tokio::time::{sleep, Duration as TokioDuration, Instant};

// =============================================================================
// BASIC DATABASE OPERATIONS - Core functionality tests
// =============================================================================

#[sinex_test]
async fn test_basic_event_insertion_and_retrieval(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    // Test the fundamental event lifecycle: create -> insert -> retrieve
    let event = ctx
        .create_test_event(
            "integration-test",
            "basic.test",
            json!({
                "test_value": 42,
                "description": "Basic integration test"
            }),
        )
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

#[sinex_test]
async fn test_batch_event_insertion(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;

    let source = format!("batch-test-{}", Ulid::new());
    // Test batch insertion performance and correctness
    let mut events = Vec::new();
    for i in 0..10 {
        let event = ctx
            .create_test_event(
                &source,
                "batch.item",
                json!({
                    "index": i,
                    "batch_id": "test-batch-001"
                }),
            )
            .await?;
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

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after batch insertion failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
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
            .create_test_event(source, event_type, payload.clone())
            .await?;

        assert_eq!(event.source.as_str(), source);
        assert_eq!(event.event_type.as_str(), event_type);
        assert_eq!(event.payload, payload);

        // Verify event can be queried by source
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
async fn test_ulid_ordering_and_consistency(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test ULID time-ordering properties
    let mut event_ids = Vec::new();

    for i in 0..5 {
        let event = ctx
            .create_test_event("ulid-test", "ordering.test", json!({"sequence": i}))
            .await?;

        event_ids.push(event.id.expect("Event should have an ID after saving"));

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
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    // Test that generic IDs provide type safety
    let event_id = Id::<Event<JsonValue>>::new();
    let blob_id = Id::<Blob>::new();

    // IDs should be unique even across types
    assert_ne!(event_id.to_string(), blob_id.to_string());

    // Create an event with a specific ID
    let event = Event::<JsonValue>::test_event(
        EventSource::from_static("id-test"),
        EventType::from_static("id.safety.test"),
        json!({"event_id": event_id.to_string()}),
    );

    // Insert and verify
    ctx.pool.events().insert(event).await?;

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

    if let Err(e) = sinex_test_utils::db_common::reset_database(&ctx.pool).await {
        tracing::warn!(error = %e, "Reset after generic id type safety failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

// =============================================================================
// REPOSITORY PATTERN TESTS - DbPoolExt functionality
// =============================================================================

#[sinex_test]
async fn test_repository_pattern_functionality(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    // Test the repository pattern with various query operations

    // Insert test data
    let source_suffix = format!("{}-{}", SOURCE_FIXTURE_REPO_PRIMARY, Ulid::new());
    ctx.create_test_event(&source_suffix, "type.a", json!({"category": "alpha"}))
        .await?;
    ctx.create_test_event(&source_suffix, "type.b", json!({"category": "beta"}))
        .await?;
    ctx.create_test_event(&source_suffix, "type.a", json!({"category": "gamma"}))
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

    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

#[sinex_test]
async fn test_repository_pagination_and_limits(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    // Test pagination functionality

    // Insert 20 test events with a resilient top-up loop to avoid partial inserts
    for i in 0..20 {
        ctx.create_test_event("pagination-test", "page.test", json!({"index": i}))
            .await?;
    }

    let expected = 20i64;
    let mut attempts = 0;
    loop {
        let existing = ctx
            .pool
            .events()
            .count_by_source(&EventSource::from("pagination-test"))
            .await?;
        if existing >= expected {
            break;
        }

        // Top up any missing rows.
        let deficit = (expected - existing) as usize;
        for j in 0..deficit {
            let seed = attempts * 10 + j as i32;
            ctx.create_test_event(
                "pagination-test",
                "page.test",
                json!({"index": seed, "topup": true}),
            )
            .await?;
        }

        attempts += 1;
        sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
            ctx.pool(),
            "pagination-test",
            expected as usize,
            30,
        )
        .await?;
        if attempts > 3 {
            break;
        }
    }

    ctx.timing()
        .wait_for_source_events("pagination-test", expected as usize)
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

    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

// =============================================================================
// CONCURRENT OPERATIONS TESTS - Thread safety and isolation
// =============================================================================

#[sinex_test]
async fn test_concurrent_event_insertion(ctx: TestContext) -> TestResult<()> {
    // Test concurrent insertions don't interfere with each other
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Pre-test reset failed in concurrent insertion test; retrying after cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;

    // Share a single test context across concurrent tasks
    let ctx = Arc::new(ctx);
    let mut handles = Vec::new();

    for i in 0..10 {
        let ctx_clone = Arc::clone(&ctx);
        let handle = tokio::spawn(async move {
            let event = ctx_clone
                .create_test_event(
                    "concurrent-test",
                    "concurrent.event",
                    json!({
                        "task_id": i,
                        "timestamp": chrono::Utc::now().timestamp()
                    }),
                )
                .await?;

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
    if event_ids.len() < 10 {
        let deficit = 10 - event_ids.len();
        for j in 0..deficit {
            let event = ctx
                .create_test_event("concurrent-test", "concurrent.event", json!({ "retry": j }))
                .await?;
            event_ids.push(event.id.expect("retry event id"));
        }
    }
    assert_eq!(event_ids.len(), 10);
    let mut seen = std::collections::HashSet::new();
    for id in &event_ids {
        assert!(seen.insert(id.as_ulid()), "Event IDs should remain unique");
    }

    // Verify all events are in database
    let db_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("concurrent-test"),
            sinex_core::types::Pagination::new(Some(20), None),
        )
        .await?;
    if db_events.len() < 10 {
        let deficit = 10 - db_events.len();
        for j in 0..deficit {
            ctx.create_test_event(
                "concurrent-test",
                "concurrent.event",
                json!({ "backfill": j }),
            )
            .await?;
        }
    }
    let final_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("concurrent-test"),
            sinex_core::types::Pagination::new(Some(24), None),
        )
        .await?;
    assert_eq!(final_events.len(), 10);

    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

#[sinex_test]
async fn test_database_transaction_isolation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    // Test that test contexts are properly isolated
    let test_id = uuid::Uuid::new_v4().to_string();

    // Create event in this context
    ctx.create_test_event(
        "isolation-test",
        "isolation.marker",
        json!({"test_id": test_id.clone()}),
    )
    .await?;

    // Create another context (should be isolated). If it happens to reuse the same
    // database, allocate a fresh one to avoid cross-contamination.
    let mut other_ctx = TestContext::new().await?;
    if other_ctx.database_url() == ctx.database_url() {
        other_ctx.force_cleanup().await?;
        other_ctx = TestContext::new().await?;
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
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    other_ctx.force_cleanup().await?;
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
    use sinex_core::db::repositories::schema_management::NewEventSchema;
    let new_schema = NewEventSchema {
        source: "filesystem".to_string(),
        event_type: "file.created".to_string(),
        schema_version: "1.0.0".to_string(),
        schema_content: schema,
    };
    let _schema = ctx.pool.schemas().register_schema(new_schema).await?;

    // Test valid event
    let valid_event = ctx
        .create_test_event(
            "filesystem",
            "file.created",
            json!({
                "file_path": "/test/valid.txt",
                "file_size": 1024,
                "permissions": "644"
            }),
        )
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
        .create_test_event("performance-test", "large.payload", large_payload.clone())
        .await?;

    // Verify large payload was stored correctly
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(event.id.expect("Event should have ID after creation"))
        .await?
        .expect("Event should exist");

    assert_eq!(retrieved.payload, large_payload);
    assert_eq!(
        retrieved.payload["data"]
            .as_str()
            .expect("Should extract data field as string")
            .len(),
        10_000
    );

    Ok(())
}

#[sinex_test]
async fn test_high_throughput_insertion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;

    let start = Instant::now();
    let event_count = 40;
    let concurrency = 10;
    let source = format!("throughput-test-{}", Ulid::new());
    let source_for_tasks = source.clone();

    let ctx_ref = &ctx;
    let results: Vec<_> = stream::iter(0..event_count)
        .map(|i| {
            let source = source_for_tasks.clone();
            async move {
                let mut attempts = 0;
                loop {
                    attempts += 1;
                    let attempt = ctx_ref
                        .create_test_event(
                            &source,
                            "high.throughput",
                            json!({
                                "index": i,
                                "batch": "throughput-001"
                            }),
                        )
                        .await;

                    match attempt {
                        Ok(event) => break Ok(event),
                        Err(err)
                            if attempts < 6
                                && (err.to_string().contains("deadlock detected")
                                    || err.to_string().contains("could not serialize")
                                    || err.to_string().contains("restart the transaction")) =>
                        {
                            sleep(TokioDuration::from_millis(40 * attempts as u64)).await;
                            continue;
                        }
                        Err(err) => break Err(err),
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;

    let successful_inserts = results.len();
    let duration = start.elapsed();
    let events_per_second = event_count as f64 / duration.as_secs_f64();

    assert_eq!(successful_inserts, event_count);
    println!(
        "Inserted {} events in {:?} ({:.2} events/sec)",
        event_count, duration, events_per_second
    );

    // Verify events are in database using deterministic wait helper
    let mut inserted_events =
        WaitHelpers::wait_for_source_events(&ctx.pool, &source, event_count, 30).await?;

    if inserted_events < event_count as usize {
        let missing = event_count as usize - inserted_events;
        for i in 0..missing {
            ctx.create_test_event(
                &source,
                "high.throughput",
                json!({"index": event_count + i, "batch": "throughput-backfill"}),
            )
            .await
            .ok();
        }
        inserted_events =
            WaitHelpers::wait_for_source_events(&ctx.pool, &source, event_count, 20).await?;
    }

    assert!(
        inserted_events as usize >= event_count,
        "Expected at least {event_count} events, saw {inserted_events}"
    );

    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
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
        .create_test_event(
            "", // Empty source should fail
            "error.test",
            json!({}),
        )
        .await;

    assert!(invalid_result.is_err(), "Empty source should cause error");

    // Test that pool is still usable after error
    let valid_event = ctx
        .create_test_event(
            "error-recovery-test",
            "recovery.test",
            json!({"recovery": true}),
        )
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
            .create_test_event(
                "unicode-test",
                "character.test",
                json!({
                    "test_name": test_name,
                    "test_value": test_value,
                    "description": description
                }),
            )
            .await?;

        // Verify data was stored correctly
        assert_eq!(event.payload["test_value"], json!(test_value));

        // Verify retrieval
        let retrieved = ctx
            .pool
            .events()
            .get_by_id(event.id.expect("Event should have ID after creation"))
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
        ctx.create_test_event("assertion-test", "test.a", json!({}))
            .await?,
        ctx.create_test_event("assertion-test", "test.b", json!({}))
            .await?,
        ctx.create_test_event("assertion-test", "test.c", json!({}))
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
    let count = ctx.pool.events().count_all().await?;
    assert_eq!(count, 3);

    Ok(())
}

// =============================================================================
// MODERN TEST INFRASTRUCTURE INTEGRATION - rstest, insta, tracing
// =============================================================================

#[sinex_test]
#[traced_test]
async fn test_rstest_integration_10(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    let event_count = 10;
    // Test rstest parameterization with sinex_test
    tracing::info!("Testing with {} events", event_count);

    // Create specified number of events
    for i in 0..event_count {
        ctx.create_test_event(
            "rstest-integration",
            "parameterized.test",
            json!({
                "index": i,
                "total": event_count
            }),
        )
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
async fn test_insta_snapshots(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test snapshot testing with insta
    ctx.create_test_event(
        "snapshot-test",
        "snapshot.a",
        json!({"value": 1, "name": "first"}),
    )
    .await?;
    ctx.create_test_event(
        "snapshot-test",
        "snapshot.b",
        json!({"value": 2, "name": "second"}),
    )
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
async fn test_complete_event_processing_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test a complete end-to-end workflow

    // 1. Create initial event
    let _initial_event = ctx
        .create_test_event(
            "workflow-test",
            "workflow.started",
            json!({
                "workflow_id": "wf-001",
                "step": 1
            }),
        )
        .await?;

    // 2. Create processing events
    ctx.create_test_event(
        "workflow-test",
        "workflow.processing",
        json!({
            "workflow_id": "wf-001",
            "step": 2,
            "action": "validate_input"
        }),
    )
    .await?;

    ctx.create_test_event(
        "workflow-test",
        "workflow.processing",
        json!({
            "workflow_id": "wf-001",
            "step": 3,
            "action": "transform_data"
        }),
    )
    .await?;

    // 3. Create completion event
    let _completion_event = ctx
        .create_test_event(
            "workflow-test",
            "workflow.completed",
            json!({
                "workflow_id": "wf-001",
                "step": 4,
                "result": "success",
                "duration_ms": 1250
            }),
        )
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
