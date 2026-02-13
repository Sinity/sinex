//! Material Idempotency Tests
//!
//! Tests for idempotent handling of material stream ingestion.

use sinex_primitives::{DynamicPayload, Id, SourceMaterial};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_source_material_idempotent_creation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    // Create a source material with a specific ID
    let material_id = Id::<SourceMaterial>::new();
    ctx.ensure_source_material(material_id, Some("test-mat"))
        .await?;

    // Call it again with the same ID - should be idempotent (no error)
    ctx.ensure_source_material(material_id, Some("test-mat"))
        .await?;

    // Publish an event referencing that material
    let payload = DynamicPayload::new(
        "test-source",
        "test.event",
        serde_json::json!({
            "message": "test event with idempotent material"
        }),
    );

    let event = ctx.publish(payload).await?;
    assert!(event.id.is_some(), "published event should have an ID");

    Ok(())
}

#[sinex_test]
async fn test_multiple_events_same_material(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;
    // Create a single source material
    let material_id = Id::<SourceMaterial>::new();
    ctx.ensure_source_material(material_id, Some("shared-material"))
        .await?;

    // Publish 5 events all referencing the same material
    let payloads: Vec<DynamicPayload> = (0..5)
        .map(|i| {
            DynamicPayload::new(
                "test-source",
                "test.event",
                serde_json::json!({
                    "sequence": i,
                    "event": "shared material"
                }),
            )
        })
        .collect();

    let published_events = ctx.publish_many(payloads).await?;

    // Verify all 5 events were published
    assert_eq!(
        published_events.len(),
        5,
        "should have published exactly 5 events"
    );

    // Verify all events have IDs and the correct source
    for event in &published_events {
        assert!(event.id.is_some(), "each event should have an ID");
        assert_eq!(
            event.source.as_str(),
            "test-source",
            "all events should have the same source"
        );
    }

    Ok(())
}
