//! Database Integration Tests
//!
//! Comprehensive integration tests for database functionality using the NATS pipeline.
//! Tests cover:
//! - Basic database operations and transactions
//! - `UUIDv7` primary key integration
//! - Event creation and querying
//! - Connection pool operations
//!
//! Uses #[`sinex_test`] for automatic transaction isolation and `TestContext`
//! for unified database access patterns. All events flow through `PipelineScope`
//! (NATS → ingestd → `PostgreSQL`) for realistic end-to-end validation.

use serde_json::json;
use sinex_db::{DbPoolExt, DynamicPayload};
use sinex_primitives::EventSource;
use sinex_primitives::Uuid;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{FileCreatedPayload, FileModifiedPayload};
use std::time::Duration as StdDuration;
use xtask::sandbox::prelude::*;

// =============================================================================
// BASIC DATABASE OPERATIONS
// =============================================================================

/// Test batch insertion of multiple events through the pipeline
#[sinex_test]
async fn test_batch_event_insertion(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let source = format!("fs-watcher-{}", Uuid::now_v7().to_string().to_lowercase());
    let mut inserted_events = Vec::new();
    let event_type = FileCreatedPayload::EVENT_TYPE.as_str().to_string();

    for i in 0..10 {
        let event = ctx
            .publish(DynamicPayload::new(
                source.as_str(),
                event_type.as_str(),
                json!({
                    "path": format!("/test/file_{}.txt", i),
                    "size": 1024 * (i + 1)
                }),
            ))
            .await?;

        inserted_events.push(event);
    }

    // Verify all events were inserted (each publish already waited for persistence)
    let expected = inserted_events.len();
    let persisted = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(source.as_str()),
            sinex_primitives::Pagination::new(Some(64), None),
        )
        .await?;

    assert!(
        persisted.len() >= expected,
        "Expected at least {expected} events for source {source}, found {}",
        persisted.len()
    );

    let persisted_ids: Vec<_> = persisted.iter().filter_map(|e| e.id).collect();
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

/// Test querying events by source through the pipeline
#[sinex_test]
async fn test_query_events_by_source(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    let fs_source = format!("fs-watcher-{}", Uuid::now_v7().to_string().to_lowercase());
    let terminal_source = format!("shell-{}", Uuid::now_v7().to_string().to_lowercase());

    // Create filesystem events
    let _fs_event1 = ctx
        .publish(DynamicPayload::new(
            fs_source.as_str(),
            FileCreatedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/file1.txt", "size": 1024}),
        ))
        .await?;

    let _fs_event2 = ctx
        .publish(DynamicPayload::new(
            fs_source.as_str(),
            FileModifiedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/file2.txt", "size": 2048}),
        ))
        .await?;

    let _term_event = ctx
        .publish(
            DynamicPayload::new(
                terminal_source.as_str(),
                "command.executed",
                json!({"command": "ls -la", "exit_status": 0, "kitty_window_id": "test", "kitty_tab_id": "test"}),
            ),
        )
        .await?;

    // Query filesystem events using direct repository access
    let filesystem_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(fs_source.as_str()),
            sinex_primitives::Pagination::new(Some(100), None),
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

/// Test UUIDv7 ordering in time-based queries
#[sinex_test]
#[traced_test]
async fn test_uuid_time_ordering(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing UUIDv7 time ordering");

    // Insert events with a small delay to ensure different timestamps
    let event1 = ctx
        .publish(DynamicPayload::new(
            FileCreatedPayload::SOURCE.as_str(),
            FileCreatedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/first.txt", "size": 100}),
        ))
        .await?;
    let id1 = event1.id.unwrap();

    // Ensure different timestamp
    tokio::time::sleep(StdDuration::from_millis(1)).await;

    let event2 = ctx
        .publish(DynamicPayload::new(
            FileCreatedPayload::SOURCE.as_str(),
            FileCreatedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/second.txt", "size": 200}),
        ))
        .await?;
    let id2 = event2.id.unwrap();

    // Verify UUIDv7 IDs are in time order (later UUIDv7 should be larger)
    assert!(id2.to_string() > id1.to_string());

    tracing::debug!("UUIDv7 ordering verified: {} < {}", id1, id2);

    Ok(())
}

// =============================================================================
// UUIDv7 INTEGRATION TESTS
// =============================================================================

#[sinex_test]
#[traced_test]
async fn test_uuid_ordering_in_database(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing UUIDv7 ordering in database queries");

    // Insert multiple events and collect their IDs
    let mut uuids = Vec::new();

    for i in 0..5 {
        let event = ctx
            .publish(DynamicPayload::new(
                FileCreatedPayload::SOURCE.as_str(),
                FileCreatedPayload::EVENT_TYPE.as_str(),
                json!({"path": format!("/test/file_{}.txt", i), "size": (i + 1) * 1024}),
            ))
            .await?;
        uuids.push(event.id.unwrap());

        // Small delay to ensure UUIDv7 monotonic ordering
        tokio::time::sleep(StdDuration::from_millis(1)).await;
    }

    // Query filesystem events to verify they exist using direct repository access
    let filesystem_events = ctx
        .pool
        .events()
        .get_by_source(
            &FileCreatedPayload::SOURCE,
            sinex_primitives::Pagination::new(Some(100), None),
        )
        .await?;
    assert!(filesystem_events.len() >= 5);

    // Verify UUIDv7 IDs are in chronological order by converting to strings
    for i in 1..uuids.len() {
        assert!(
            uuids[i].to_string() > uuids[i - 1].to_string(),
            "UUIDv7 IDs should be in chronological order"
        );
    }

    tracing::debug!(
        "All {} UUIDv7 IDs are in correct chronological order",
        uuids.len()
    );

    Ok(())
}

// =============================================================================
// BASIC CONCURRENCY TESTS
// =============================================================================

/// Test basic event creation functionality
#[sinex_test]
#[traced_test]
async fn test_basic_event_creation_patterns(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    tracing::info!("Testing various event creation patterns");

    // Test simple event creation
    let simple_event = ctx
        .publish(DynamicPayload::new(
            "test-service",
            "simple.event",
            json!({"message": "Basic test event"}),
        ))
        .await?;

    assert!(simple_event.id.is_some());
    assert_eq!(simple_event.source.as_str(), "test-service");
    assert_eq!(simple_event.event_type.as_str(), "simple.event");

    // Test event with complex payload
    let complex_event = ctx
        .publish(DynamicPayload::new(
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
        ))
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
#[sinex_test]
async fn test_event_payload_validation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

    // Test with different payload structures
    let simple_event = ctx
        .publish(DynamicPayload::new(
            "test-service",
            "simple.event",
            json!({"message": "hello world"}),
        ))
        .await?;

    let complex_event = ctx
        .publish(DynamicPayload::new(
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
        ))
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
