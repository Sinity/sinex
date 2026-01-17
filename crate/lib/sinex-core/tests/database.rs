//! Database Integration Tests
//!
//! Comprehensive integration tests for database functionality using modern infrastructure.
//! Tests cover:
//! - Basic database operations and transactions
//! - ULID primary key integration
//! - Event creation and querying
//! - Connection pool operations
//!
//! Uses #[sinex_test] for automatic transaction isolation and TestContext
//! for unified database access patterns.

use serde_json::json;
// Using shorter imports from sinex-core's re-exports
use sinex_core::{
    payloads::filesystem::{FileCreatedPayload, FileModifiedPayload},
    DbPoolExt, EventSource, Ulid,
};
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::WaitHelpers;
use std::time::Duration as StdDuration;

// =============================================================================
// BASIC DATABASE OPERATIONS
// =============================================================================

/// Test batch insertion of multiple events using modern patterns
#[sinex_serial_test]
async fn test_batch_event_insertion(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    ctx.ensure_clean().await?;
    let source = format!("fs-watcher-{}", Ulid::new());
    // Create test events using modern test utilities
    let mut inserted_events = Vec::new();
    let event_type = FileCreatedPayload::EVENT_TYPE.as_str().to_string();

    for i in 0..10 {
        let event = ctx
            .publish_json_event(
                &source,
                event_type.as_str(),
                json!({
                    "path": format!("/test/file_{}.txt", i),
                    "size": 1024 * (i + 1)
                }),
            )
            .await?;

        inserted_events.push(event);
    }

    // Verify all events were inserted.
    let expected = inserted_events.len();
    WaitHelpers::wait_for_source_events(&ctx.pool, &source, expected, 10).await?;
    let persisted = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(source.as_str()),
            sinex_core::types::Pagination::new(Some(64), None),
        )
        .await?;

    assert!(
        persisted.len() >= expected,
        "Expected at least {expected} events for source {source}, found {}",
        persisted.len()
    );

    let persisted_ids: Vec<_> = persisted
        .iter()
        .filter_map(|e| e.id.as_ref().cloned())
        .collect();
    for event in &inserted_events {
        if let Some(ref id) = event.id {
            assert!(
                persisted_ids.iter().any(|persisted| persisted == id),
                "Expected inserted event {id} to be present for source {source}"
            );
        }
    }

    Ok(())
}

/// Test querying events by source using modern patterns
#[sinex_serial_test]
async fn test_query_events_by_source(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    ctx.ensure_clean().await?;
    let fs_source = format!("fs-watcher-{}", Ulid::new());
    let terminal_source = format!("shell-{}", Ulid::new());

    // Create filesystem events
    let _fs_event1 = ctx
        .publish_json_event(
            &fs_source,
            FileCreatedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/file1.txt", "size": 1024}),
        )
        .await?;

    let _fs_event2 = ctx
        .publish_json_event(
            &fs_source,
            FileModifiedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/file2.txt", "size": 2048}),
        )
        .await?;

    let _term_event = ctx
        .publish_json_event(
            &terminal_source,
            "command.executed",
            json!({"command": "ls -la", "exit_status": 0, "kitty_window_id": "test", "kitty_tab_id": "test"}),
        )
        .await?;

    // Wait for both filesystem events to be visible before asserting.
    WaitHelpers::wait_for_source_events(ctx.pool(), &fs_source, 2, 20).await?;

    // Query filesystem events using direct repository access
    let filesystem_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(fs_source.as_str()),
            sinex_core::types::Pagination::new(Some(100), None),
        )
        .await?;
    assert!(filesystem_events.len() >= 2);

    for event in &filesystem_events {
        assert_eq!(event.source.as_str(), fs_source);
    }
    // Ensure terminal events did not leak into the filtered dataset.
    assert!(
        filesystem_events
            .iter()
            .all(|event| event.source.as_str() != terminal_source),
        "Terminal source events should not appear in filesystem source query"
    );

    Ok(())
}

/// Test ULID ordering in time-based queries
#[sinex_test]
#[traced_test]
async fn test_ulid_time_ordering(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    tracing::info!("Testing ULID time ordering");

    // Insert events with a small delay to ensure different timestamps
    let event1 = ctx
        .publish_json_event(
            FileCreatedPayload::SOURCE.as_str(),
            FileCreatedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/first.txt", "size": 100}),
        )
        .await?;
    let id1 = event1.id.unwrap();

    // Ensure different timestamp
    tokio::time::sleep(StdDuration::from_millis(1)).await;

    let event2 = ctx
        .publish_json_event(
            FileCreatedPayload::SOURCE.as_str(),
            FileCreatedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/second.txt", "size": 200}),
        )
        .await?;
    let id2 = event2.id.unwrap();

    // Verify ULIDs are in time order (later ULID should be larger)
    assert!(id2.to_string() > id1.to_string());

    tracing::debug!("ULID ordering verified: {} < {}", id1, id2);

    Ok(())
}

// =============================================================================
// ULID INTEGRATION TESTS
// =============================================================================

#[sinex_serial_test]
#[traced_test]
async fn test_ulid_ordering_in_database(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    tracing::info!("Testing ULID ordering in database queries");
    ctx.ensure_clean().await?;

    // Insert multiple events and collect their IDs
    let mut ulids = Vec::new();

    for i in 0..5 {
        let event = ctx
            .publish_json_event(
                FileCreatedPayload::SOURCE.as_str(),
                FileCreatedPayload::EVENT_TYPE.as_str(),
                json!({"path": format!("/test/file_{}.txt", i), "size": (i + 1) * 1024}),
            )
            .await?;
        ulids.push(event.id.unwrap());

        // Small delay to ensure ULID monotonic ordering
        tokio::time::sleep(StdDuration::from_millis(1)).await;
    }

    let expected_events = ulids.len();
    WaitHelpers::wait_for_source_events(
        ctx.pool(),
        FileCreatedPayload::SOURCE.as_str(),
        expected_events,
        10,
    )
    .await?;

    // Query filesystem events to verify they exist using direct repository access
    let filesystem_events = ctx
        .pool
        .events()
        .get_by_source(
            &FileCreatedPayload::SOURCE,
            sinex_core::types::Pagination::new(Some(100), None),
        )
        .await?;
    assert!(filesystem_events.len() >= 5);

    // Verify ULIDs are in chronological order by converting to strings
    for i in 1..ulids.len() {
        assert!(
            ulids[i].to_string() > ulids[i - 1].to_string(),
            "ULIDs should be in chronological order"
        );
    }

    tracing::debug!(
        "All {} ULIDs are in correct chronological order",
        ulids.len()
    );

    Ok(())
}

#[sinex_test]
async fn test_ulid_uuid_conversion_consistency() -> TestResult<()> {
    // Test that ULID <-> UUID conversion is consistent
    let original_ulid = Ulid::new();
    let uuid_form = original_ulid.to_uuid();
    let back_to_ulid = Ulid::from_uuid(uuid_form);

    assert_eq!(original_ulid, back_to_ulid);

    Ok(())
}

// =============================================================================
// BASIC CONCURRENCY TESTS
// =============================================================================

/// Test basic event creation functionality
#[sinex_serial_test]
#[traced_test]
async fn test_basic_event_creation_patterns(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    tracing::info!("Testing various event creation patterns");
    ctx.ensure_clean().await?;

    // Test simple event creation
    let simple_event = ctx
        .publish_json_event(
            "test-service",
            "simple.event",
            json!({"message": "Basic test event"}),
        )
        .await?;

    assert!(simple_event.id.is_some());
    assert_eq!(simple_event.source.as_str(), "test-service");
    assert_eq!(simple_event.event_type.as_str(), "simple.event");

    // Test event with complex payload
    let complex_event = ctx
        .publish_json_event(
            "test-service",
            "complex.event",
            json!({
                "metadata": {
                    "version": "1.0",
                    "tags": ["test", "integration"]
                },
                "data": {
                    "items": [1, 2, 3, 4, 5],
                    "nested": {
                        "value": true
                    }
                }
            }),
        )
        .await?;

    assert!(complex_event.id.is_some());
    assert_eq!(complex_event.payload["metadata"]["version"], json!("1.0"));

    tracing::info!("Both simple and complex event creation patterns work correctly");

    Ok(())
}

// =============================================================================
// EVENT VALIDATION TESTS
// =============================================================================

/// Test event creation with various payload types
#[sinex_serial_test]
async fn test_event_payload_validation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    ctx.ensure_clean().await?;
    // Test with different payload structures
    let simple_event = ctx
        .publish_json_event(
            "test-service",
            "simple.event",
            json!({"message": "hello world"}),
        )
        .await?;

    let complex_event = ctx
        .publish_json_event(
            "test-service",
            "complex.event",
            json!({
                "nested": {
                    "data": {
                        "values": [1, 2, 3, 4, 5]
                    }
                },
                "metadata": {
                    "version": "1.0",
                    "timestamp": "2025-01-01T00:00:00Z"
                }
            }),
        )
        .await?;

    // Verify events were created successfully
    assert!(simple_event.id.is_some());
    assert!(complex_event.id.is_some());

    // Verify payload structure
    assert_eq!(simple_event.payload["message"], json!("hello world"));
    assert_eq!(
        complex_event.payload["nested"]["data"]["values"],
        json!([1, 2, 3, 4, 5])
    );

    Ok(())
}
