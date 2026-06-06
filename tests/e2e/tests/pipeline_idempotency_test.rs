//! Pipeline idempotency tests
//!
//! Tests for idempotent handling of duplicate event payloads and rapid republishing.
//! Verifies that the event pipeline correctly persists events with identical payloads
//! by assigning unique IDs, and handles rapid successive publishes from the same source.

use serde_json::json;
use sinex_primitives::DynamicPayload;
use xtask::sandbox::prelude::*;

/// Test that duplicate payloads (same source, type, JSON) are persisted with unique IDs.
///
/// Publishes 10 events with identical payloads and verifies all are persisted.
/// Each event gets a unique UUIDv7 even though content is identical.
#[sinex_test]
async fn test_pipeline_handles_duplicate_payloads(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let ctx = scope.ctx();

    let payload_json = json!({
        "message": "identical event",
        "sequence": 1,
        "data": {"nested": "value"}
    });

    let num_events = 10;
    let mut published_ids = Vec::new();

    // Publish 10 identical payloads
    for _ in 0..num_events {
        let id = scope
            .publish(DynamicPayload::new(
                "idempotency-test",
                "test.duplicate",
                payload_json.clone(),
            ))
            .await?;
        published_ids.push(id);
    }

    // Wait for all events to be persisted
    scope.wait_for_event_count(num_events).await?;

    // Verify that all published IDs are unique (each event got a unique UUIDv7)
    let unique_ids: std::collections::HashSet<_> = published_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        published_ids.len(),
        "all duplicate payloads should receive unique event IDs"
    );

    // Verify all 10 events were persisted in the database
    let count = ctx
        .pool
        .events()
        .count_by_source(&sinex_primitives::EventSource::from("idempotency-test"))
        .await?;
    assert_eq!(
        count, num_events as i64,
        "all 10 duplicate payload events should be persisted"
    );

    scope.shutdown().await?;
    Ok(())
}

/// Test that rapid successive publishes from the same source are all persisted.
///
/// Publishes 5 events of one type, then 5 more of a different type from the same source.
/// Verifies total count is 10 and both batches are present.
#[sinex_test]
async fn test_pipeline_handles_rapid_republish(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let ctx = scope.ctx();

    let source = "rapid-test";
    let mut all_ids = Vec::new();

    // Publish 5 events of type "batch.first"
    for i in 0..5 {
        let id = scope
            .publish(DynamicPayload::new(
                source,
                "batch.first",
                json!({"sequence": i, "batch": 1}),
            ))
            .await?;
        all_ids.push(id);
    }

    // Publish 5 events of type "batch.second" from the same source
    for i in 0..5 {
        let id = scope
            .publish(DynamicPayload::new(
                source,
                "batch.second",
                json!({"sequence": i, "batch": 2}),
            ))
            .await?;
        all_ids.push(id);
    }

    // Wait for all 10 events to be persisted
    scope.wait_for_event_count(10).await?;

    // Verify total event count by source is 10
    let total_count = ctx
        .pool
        .events()
        .count_by_source(&sinex_primitives::EventSource::from(source))
        .await?;
    assert_eq!(
        total_count, 10,
        "all 10 events (2 batches of 5) should be persisted"
    );

    // Verify all published IDs are unique
    let unique_ids: std::collections::HashSet<_> = all_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        all_ids.len(),
        "all events should receive unique IDs"
    );

    scope.shutdown().await?;
    Ok(())
}
