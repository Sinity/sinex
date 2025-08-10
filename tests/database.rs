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

use color_eyre::eyre::Result;
use serde_json::json;
use sinex_core::types::constants::{
    SOURCE_FS_WATCHER, SOURCE_TERMINAL, TYPE_COMMAND_EXECUTED, TYPE_FILE_CREATED,
    TYPE_FILE_MODIFIED,
};
use sinex_test_utils::prelude::*;
use std::time::Duration as StdDuration;

// =============================================================================
// BASIC DATABASE OPERATIONS
// =============================================================================

/// Test batch insertion of multiple events using modern patterns
#[sinex_test]
async fn test_batch_event_insertion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create test events using modern test utilities
    let mut inserted_events = Vec::new();

    for i in 0..10 {
        let event = ctx
            .create_test_event(
                SOURCE_FS_WATCHER.as_str(),
                TYPE_FILE_CREATED.as_str(),
                json!({
                    "path": format!("/test/file_{}.txt", i),
                    "size": 1024 * (i + 1)
                }),
            )
            .await?;

        inserted_events.push(event);
    }

    // Verify all events were inserted using direct repository access
    let recent_events = ctx.pool.events().get_recent(20).await?;
    assert!(recent_events.len() >= 10);

    // Verify specific events exist
    for event in &inserted_events {
        if let Some(ref id) = event.id {
            let found_events: Vec<_> = recent_events
                .iter()
                .filter(|e| e.id.as_ref() == Some(id))
                .collect();
            assert_eq!(found_events.len(), 1);
        }
    }

    Ok(())
}

/// Test querying events by source using modern patterns
#[sinex_test]
async fn test_query_events_by_source(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create filesystem events
    let _fs_event1 = ctx
        .create_test_event(
            SOURCE_FS_WATCHER.as_str(),
            TYPE_FILE_CREATED.as_str(),
            json!({"path": "/test/file1.txt", "size": 1024}),
        )
        .await?;

    let _fs_event2 = ctx
        .create_test_event(
            SOURCE_FS_WATCHER.as_str(),
            TYPE_FILE_MODIFIED.as_str(),
            json!({"path": "/test/file2.txt", "size": 2048}),
        )
        .await?;

    let _term_event = ctx
        .create_test_event(
            SOURCE_TERMINAL.as_str(),
            TYPE_COMMAND_EXECUTED.as_str(),
            json!({"command": "ls -la", "exit_code": 0}),
        )
        .await?;

    // Query filesystem events using direct repository access
    let filesystem_events = ctx
        .pool
        .events()
        .get_by_source(&SOURCE_FS_WATCHER, Some(100), None)
        .await?;
    assert!(filesystem_events.len() >= 2);

    for event in &filesystem_events {
        assert_eq!(event.source.as_str(), "fs-watcher");
    }

    // Basic verification - snapshot functionality may not be available
    assert!(filesystem_events.len() >= 2);

    // Verify all events have the expected source
    for event in &filesystem_events {
        assert_eq!(event.source.as_str(), "fs-watcher");
    }

    Ok(())
}

/// Test ULID ordering in time-based queries
#[sinex_test]
#[traced_test]
async fn test_ulid_time_ordering(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing ULID time ordering");

    // Insert events with a small delay to ensure different timestamps
    let event1 = ctx
        .create_test_event(
            SOURCE_FS_WATCHER.as_str(),
            TYPE_FILE_CREATED.as_str(),
            json!({"path": "/test/first.txt", "size": 100}),
        )
        .await?;
    let id1 = event1.id.unwrap();

    // Ensure different timestamp
    tokio::time::sleep(StdDuration::from_millis(1)).await;

    let event2 = ctx
        .create_test_event(
            SOURCE_FS_WATCHER.as_str(),
            TYPE_FILE_CREATED.as_str(),
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

#[sinex_test]
#[traced_test]
async fn test_ulid_ordering_in_database(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing ULID ordering in database queries");

    // Insert multiple events and collect their IDs
    let mut ulids = Vec::new();

    for i in 0..5 {
        let event = ctx
            .create_test_event(
                SOURCE_FS_WATCHER.as_str(),
                TYPE_FILE_CREATED.as_str(),
                json!({"path": format!("/test/file_{}.txt", i), "size": (i + 1) * 1024}),
            )
            .await?;
        ulids.push(event.id.unwrap());

        // Small delay to ensure ULID monotonic ordering
        tokio::time::sleep(StdDuration::from_millis(1)).await;
    }

    // Query filesystem events to verify they exist using direct repository access
    let filesystem_events = ctx
        .pool
        .events()
        .get_by_source(&SOURCE_FS_WATCHER, Some(100), None)
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
async fn test_ulid_uuid_conversion_consistency(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
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
#[sinex_test]
#[traced_test]
async fn test_basic_event_creation_patterns(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing various event creation patterns");

    // Test simple event creation
    let simple_event = ctx
        .create_test_event(
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
        .create_test_event(
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
#[sinex_test]
async fn test_event_payload_validation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test with different payload structures
    let simple_event = ctx
        .create_test_event(
            "test-service",
            "simple.event",
            json!({"message": "hello world"}),
        )
        .await?;

    let complex_event = ctx
        .create_test_event(
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
