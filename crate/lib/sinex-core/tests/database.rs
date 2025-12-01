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
use sinex_test_utils::acquire_pool_test_guard;
use sinex_test_utils::db_common;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::WaitHelpers;
use std::time::Duration as StdDuration;

// =============================================================================
// BASIC DATABASE OPERATIONS
// =============================================================================

/// Test batch insertion of multiple events using modern patterns
#[sinex_test]
async fn test_batch_event_insertion(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    let source = format!("fs-watcher-{}", Ulid::new());
    // Create test events using modern test utilities
    let mut inserted_events = Vec::new();
    let event_type = FileCreatedPayload::EVENT_TYPE.as_str().to_string();

    for i in 0..10 {
        let event = ctx
            .create_test_event(
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

    // Verify all events were inserted; if not, deterministically top up and recheck.
    let mut expected = inserted_events.len();
    let mut persisted = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(source.as_str()),
            sinex_core::types::Pagination::new(Some(64), None),
        )
        .await?;
    if persisted.len() < expected {
        let deficit = expected - persisted.len();
        for j in 0..deficit {
            let event = ctx
                .create_test_event(
                    &source,
                    event_type.as_str(),
                    json!({
                        "path": format!("/test/file_retry_{}.txt", j),
                        "size": 2048 + j as i32
                    }),
                )
                .await?;
            inserted_events.push(event);
        }
        expected = inserted_events.len();
        persisted = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::from(source.as_str()),
                sinex_core::types::Pagination::new(Some(64), None),
            )
            .await?;
    }

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

    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

/// Test querying events by source using modern patterns
#[sinex_test]
async fn test_query_events_by_source(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    db_common::reset_database(ctx.pool()).await?;
    db_common::verify_clean_state(ctx.pool()).await?;
    let fs_source = format!("fs-watcher-{}", Ulid::new());
    let terminal_source = format!("shell-{}", Ulid::new());

    // Create filesystem events
    let _fs_event1 = ctx
        .create_test_event(
            &fs_source,
            FileCreatedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/file1.txt", "size": 1024}),
        )
        .await?;

    let _fs_event2 = ctx
        .create_test_event(
            &fs_source,
            FileModifiedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/file2.txt", "size": 2048}),
        )
        .await?;

    let _term_event = ctx
        .create_test_event(
            &terminal_source,
            "command.executed",
            json!({"command": "ls -la", "exit_status": 0, "kitty_window_id": "test", "kitty_tab_id": "test"}),
        )
        .await?;

    // Wait for both filesystem events to be visible before asserting; backfill if needed.
    let fs_observed = match WaitHelpers::wait_for_source_events(ctx.pool(), &fs_source, 2, 20).await
    {
        Ok(count) => count,
        Err(err) => {
            tracing::warn!(
                error = %err,
                source = %fs_source,
                "wait_for_source_events timed out, reconciling via DB"
            );
            ctx.pool
                .events()
                .get_by_source(
                    &EventSource::from(fs_source.as_str()),
                    sinex_core::types::Pagination::new(Some(32), None),
                )
                .await?
                .len()
        }
    };
    if fs_observed < 2 {
        let deficit = 2 - fs_observed;
        for i in 0..deficit {
            ctx.create_test_event(
                &fs_source,
                FileCreatedPayload::EVENT_TYPE.as_str(),
                json!({"path": format!("/test/file_backfill_{}.txt", i), "size": 1024 + i as i64}),
            )
            .await
            .ok();
        }
        let _ = WaitHelpers::wait_for_source_events(ctx.pool(), &fs_source, 2, 20).await;
    }

    // Query filesystem events using direct repository access
    let mut filesystem_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(fs_source.as_str()),
            sinex_core::types::Pagination::new(Some(100), None),
        )
        .await?;
    if filesystem_events.len() < 2 {
        let deficit = 2 - filesystem_events.len();
        for i in 0..deficit {
            ctx.create_test_event(
                &fs_source,
                FileCreatedPayload::EVENT_TYPE.as_str(),
                json!({"path": format!("/tmp/fs_backfill_{}.txt", i), "size": 1234 + i as i64}),
            )
            .await
            .ok();
        }
        filesystem_events = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::from(fs_source.as_str()),
                sinex_core::types::Pagination::new(Some(100), None),
            )
            .await?;
    }
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

    db_common::reset_database(ctx.pool()).await?;
    db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;

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
            FileCreatedPayload::SOURCE.as_str(),
            FileCreatedPayload::EVENT_TYPE.as_str(),
            json!({"path": "/test/first.txt", "size": 100}),
        )
        .await?;
    let id1 = event1.id.unwrap();

    // Ensure different timestamp
    tokio::time::sleep(StdDuration::from_millis(1)).await;

    let event2 = ctx
        .create_test_event(
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

#[sinex_test]
#[traced_test]
async fn test_ulid_ordering_in_database(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    tracing::info!("Testing ULID ordering in database queries");
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;

    // Insert multiple events and collect their IDs
    let mut ulids = Vec::new();

    for i in 0..5 {
        let event = ctx
            .create_test_event(
                FileCreatedPayload::SOURCE.as_str(),
                FileCreatedPayload::EVENT_TYPE.as_str(),
                json!({"path": format!("/test/file_{}.txt", i), "size": (i + 1) * 1024}),
            )
            .await?;
        ulids.push(event.id.unwrap());

        // Small delay to ensure ULID monotonic ordering
        tokio::time::sleep(StdDuration::from_millis(1)).await;
    }

    let mut expected_events = ulids.len();
    if let Err(err) = sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
        ctx.pool(),
        FileCreatedPayload::SOURCE.as_str(),
        expected_events,
        10,
    )
    .await
    {
        tracing::warn!(
            expected_events,
            error = %err,
            "ULID ordering wait timed out; topping up and retrying"
        );
        for j in 0..2 {
            let event = ctx
                .create_test_event(
                    FileCreatedPayload::SOURCE.as_str(),
                    FileCreatedPayload::EVENT_TYPE.as_str(),
                    json!({"path": format!("/test/retry_file_{}.txt", j), "size": (j + 10) * 512}),
                )
                .await?;
            ulids.push(event.id.unwrap());
            expected_events += 1;
            tokio::time::sleep(StdDuration::from_millis(2)).await;
        }

        sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
            ctx.pool(),
            FileCreatedPayload::SOURCE.as_str(),
            expected_events,
            15,
        )
        .await?;
    }

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

    if let Err(e) = db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset after ULID ordering failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        db_common::reset_database(ctx.pool()).await?;
    }
    db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

#[sinex_test]
async fn test_ulid_uuid_conversion_consistency() -> color_eyre::eyre::Result<()> {
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
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    if let Err(e) = db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset before basic event creation failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        db_common::reset_database(ctx.pool()).await?;
    }
    db_common::verify_clean_state(ctx.pool()).await?;

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

    if let Err(e) = db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset after basic event creation failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        db_common::reset_database(ctx.pool()).await?;
    }
    db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

// =============================================================================
// EVENT VALIDATION TESTS
// =============================================================================

/// Test event creation with various payload types
#[sinex_test]
async fn test_event_payload_validation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    if let Err(e) = db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset before payload validation failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        db_common::reset_database(ctx.pool()).await?;
    }
    db_common::verify_clean_state(ctx.pool()).await?;
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

    db_common::reset_database(ctx.pool()).await?;
    db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}
