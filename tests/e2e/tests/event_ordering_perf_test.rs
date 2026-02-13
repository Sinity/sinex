//! Performance-oriented event ordering tests.

use serde_json::json;
use sinex_primitives::DynamicPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn perf_ulid_sequence_ordering_validation(ctx: TestContext) -> TestResult<()> {
    // Publish 100 events sequentially via publish_many(), verify strictly increasing ULIDs
    let payloads: Vec<_> = (0..100)
        .map(|i| DynamicPayload::new("sequence-test", "order.check", json!({"seq": i})))
        .collect();

    let events = ctx.publish_many(payloads).await?;

    // Verify all events have IDs
    for event in &events {
        assert!(event.id.is_some(), "Event should have a valid ID");
    }

    // Verify strictly increasing ULIDs
    for i in 1..events.len() {
        let prev_id = events[i - 1].id.unwrap();
        let curr_id = events[i].id.unwrap();
        assert!(
            prev_id.as_ulid() < curr_id.as_ulid(),
            "ULID sequence must be strictly increasing: {} < {}",
            prev_id.as_ulid(),
            curr_id.as_ulid()
        );
    }

    Ok(())
}

#[sinex_test]
async fn perf_concurrent_ulid_generation_ordering(ctx: TestContext) -> TestResult<()> {
    // Publish events from 5 different sources (20 each) via publish_many(),
    // verify within-source ordering is preserved (ULIDs increase per source)
    let mut all_payloads = Vec::new();
    for source_idx in 0..5 {
        let source = format!("source-{}", source_idx);
        for event_idx in 0..20 {
            all_payloads.push(DynamicPayload::new(
                source.as_str(),
                "concurrent.order",
                json!({"source_idx": source_idx, "event_idx": event_idx}),
            ));
        }
    }

    let events = ctx.publish_many(all_payloads).await?;

    // Group events by source
    let mut events_by_source: std::collections::HashMap<String, Vec<_>> =
        std::collections::HashMap::new();
    for event in &events {
        events_by_source
            .entry(event.source.as_str().to_string())
            .or_default()
            .push(event);
    }

    // Verify within-source ordering
    for (source, source_events) in &events_by_source {
        for i in 1..source_events.len() {
            let prev_id = source_events[i - 1].id.unwrap();
            let curr_id = source_events[i].id.unwrap();
            assert!(
                prev_id.as_ulid() < curr_id.as_ulid(),
                "Source {} ULID sequence must be strictly increasing: {} < {}",
                source,
                prev_id.as_ulid(),
                curr_id.as_ulid()
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn perf_database_ordering_consistency(ctx: TestContext) -> TestResult<()> {
    // Publish 3 separate batches of 30 events each, verify ULID ordering across batches

    let batch_1_payloads: Vec<_> = (0..30)
        .map(|i| DynamicPayload::new("batch-test", "batch.order", json!({"batch": 1, "idx": i})))
        .collect();
    let batch_1 = ctx.publish_many(batch_1_payloads).await?;

    let batch_2_payloads: Vec<_> = (0..30)
        .map(|i| DynamicPayload::new("batch-test", "batch.order", json!({"batch": 2, "idx": i})))
        .collect();
    let batch_2 = ctx.publish_many(batch_2_payloads).await?;

    let batch_3_payloads: Vec<_> = (0..30)
        .map(|i| DynamicPayload::new("batch-test", "batch.order", json!({"batch": 3, "idx": i})))
        .collect();
    let batch_3 = ctx.publish_many(batch_3_payloads).await?;

    // Verify all events have IDs
    for batch in [&batch_1, &batch_2, &batch_3] {
        for event in batch.iter() {
            assert!(event.id.is_some(), "Event should have a valid ID");
        }
    }

    // Verify batch ordering: collect ULIDs into owned Vecs
    let batch_1_ulids: Vec<_> = batch_1.iter().map(|e| e.id.unwrap()).collect();
    let batch_2_ulids: Vec<_> = batch_2.iter().map(|e| e.id.unwrap()).collect();
    let batch_3_ulids: Vec<_> = batch_3.iter().map(|e| e.id.unwrap()).collect();

    let max_b1 = batch_1_ulids.iter().map(|id| id.as_ulid()).max().unwrap();
    let min_b2 = batch_2_ulids.iter().map(|id| id.as_ulid()).min().unwrap();
    assert!(
        max_b1 < min_b2,
        "All batch 1 ULIDs should be < all batch 2 ULIDs: {} < {}",
        max_b1,
        min_b2
    );

    let max_b2 = batch_2_ulids.iter().map(|id| id.as_ulid()).max().unwrap();
    let min_b3 = batch_3_ulids.iter().map(|id| id.as_ulid()).min().unwrap();
    assert!(
        max_b2 < min_b3,
        "All batch 2 ULIDs should be < all batch 3 ULIDs: {} < {}",
        max_b2,
        min_b3
    );

    Ok(())
}
