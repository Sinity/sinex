//! Performance-oriented event ordering tests.

use serde_json::json;
use sinex_primitives::DynamicPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
#[ignore]
async fn perf_uuid_sequence_ordering_validation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    // Publish 100 events sequentially via publish_many(), verify strictly increasing UUIDv7 IDs
    let payloads: Vec<_> = (0..100)
        .map(|i| DynamicPayload::new("sequence-test", "order.check", json!({"seq": i})))
        .collect();

    let events = ctx.publish_many(payloads).await?;

    // Verify all events have IDs
    for event in &events {
        assert!(event.id.is_some(), "Event should have a valid ID");
    }

    // Verify strictly increasing UUIDv7 IDs
    for i in 1..events.len() {
        let prev_id = events[i - 1].id.unwrap();
        let curr_id = events[i].id.unwrap();
        assert!(
            prev_id.as_uuid() < curr_id.as_uuid(),
            "UUIDv7 sequence must be strictly increasing: {} < {}",
            prev_id.as_uuid(),
            curr_id.as_uuid()
        );
    }

    Ok(())
}

#[sinex_test]
#[ignore]
async fn perf_concurrent_uuid_generation_ordering(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    // Publish events from 5 different sources (20 each) via publish_many(),
    // verify within-source ordering is preserved (UUIDv7 IDs increase per source)
    let mut all_payloads = Vec::new();
    for source_idx in 0..5 {
        let source = format!("source-{source_idx}");
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
                prev_id.as_uuid() < curr_id.as_uuid(),
                "Source {} UUIDv7 sequence must be strictly increasing: {} < {}",
                source,
                prev_id.as_uuid(),
                curr_id.as_uuid()
            );
        }
    }

    Ok(())
}

#[sinex_test]
#[ignore]
async fn perf_database_ordering_consistency(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    // Publish 3 separate batches of 30 events each, verify UUIDv7 ordering across batches

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
        for event in batch {
            assert!(event.id.is_some(), "Event should have a valid ID");
        }
    }

    // Verify batch ordering: collect UUIDv7 IDs into owned Vecs
    let batch_1_uuids: Vec<_> = batch_1.iter().map(|e| e.id.unwrap()).collect();
    let batch_2_uuids: Vec<_> = batch_2.iter().map(|e| e.id.unwrap()).collect();
    let batch_3_uuids: Vec<_> = batch_3.iter().map(|e| e.id.unwrap()).collect();

    let max_b1 = batch_1_uuids
        .iter()
        .map(sinex_primitives::Id::as_uuid)
        .max()
        .unwrap();
    let min_b2 = batch_2_uuids
        .iter()
        .map(sinex_primitives::Id::as_uuid)
        .min()
        .unwrap();
    assert!(
        max_b1 < min_b2,
        "All batch 1 UUIDv7 IDs should be < all batch 2 UUIDv7 IDs: {max_b1} < {min_b2}"
    );

    let max_b2 = batch_2_uuids
        .iter()
        .map(sinex_primitives::Id::as_uuid)
        .max()
        .unwrap();
    let min_b3 = batch_3_uuids
        .iter()
        .map(sinex_primitives::Id::as_uuid)
        .min()
        .unwrap();
    assert!(
        max_b2 < min_b3,
        "All batch 2 UUIDv7 IDs should be < all batch 3 UUIDv7 IDs: {max_b2} < {min_b3}"
    );

    Ok(())
}
