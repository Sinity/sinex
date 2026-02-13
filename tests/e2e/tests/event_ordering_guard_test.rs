//! Event Ordering Guard Tests
//!
//! Tests to ensure that event ordering is preserved during ingestion,
//! even when events have timestamps that differ from ingestion order.

use serde_json::json;
use sinex_primitives::{DynamicPayload, EventSource, Pagination};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_pipeline_preserves_ingest_order_over_ts_orig(ctx: TestContext) -> TestResult<()> {
    // Publish 50 events sequentially, read them back, verify order matches publication order exactly

    let payloads: Vec<_> = (0..50)
        .map(|i| DynamicPayload::new("order-guard", "ingest.guard", json!({"idx": i})))
        .collect();

    let published_events = ctx.publish_many(payloads).await?;

    // Verify all events have IDs
    for event in &published_events {
        assert!(event.id.is_some(), "Event should have a valid ID");
    }

    // Retrieve events back from database by source
    let pool = ctx.pool();
    let source = EventSource::from("order-guard");
    let retrieved = pool
        .events()
        .get_by_source(&source, Pagination::new(Some(100), None))
        .await?;

    // Verify we got all events back
    assert_eq!(
        retrieved.len(),
        50,
        "Should retrieve exactly 50 events from database"
    );

    // Verify order matches publication order exactly by comparing ULIDs
    for (i, (published, retrieved_event)) in
        published_events.iter().zip(retrieved.iter()).enumerate()
    {
        let pub_id = published.id.unwrap();
        let ret_id = retrieved_event.id.unwrap();
        let pub_ulid = pub_id.as_ulid();
        let ret_ulid = ret_id.as_ulid();
        assert_eq!(
            pub_ulid, ret_ulid,
            "Event at index {} should match: published ULID {}, retrieved ULID {}",
            i, pub_ulid, ret_ulid
        );
    }

    Ok(())
}
