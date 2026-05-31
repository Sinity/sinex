//! Material Idempotency Tests
//!
//! Tests for idempotent handling of material stream ingestion.

use sinex_primitives::{DynamicPayload, Event, Id, Provenance, SourceMaterial, Uuid};
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
    assert_eq!(
        material_registry_count(&ctx, material_id).await?,
        1,
        "same source material id should create one registry row"
    );

    // Publish an event that actually references that material.
    let event = DynamicPayload::new(
        "test-source",
        "test.event",
        serde_json::json!({
            "message": "test event with idempotent material"
        }),
    )
    .from_material_at(material_id, 0)
    .build()?;

    let event_id = ctx.publish_prebuilt_event(&event).await?;
    let persisted = persisted_event(&ctx, event_id).await?;
    assert_material_provenance(&persisted, material_id, 0)?;
    assert_eq!(
        persisted.payload["message"],
        serde_json::json!("test event with idempotent material")
    );

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

    // Publish 5 events all referencing the same material with distinct anchors.
    let events: Vec<Event<serde_json::Value>> = (0..5)
        .map(|i| {
            DynamicPayload::new(
                "test-source",
                "test.event",
                serde_json::json!({
                    "sequence": i,
                    "event": "shared material"
                }),
            )
            .from_material_at(material_id, i)
            .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let published_ids = ctx.publish_prebuilt_events(&events).await?;

    assert_eq!(
        published_ids.len(),
        5,
        "should have published exactly 5 events"
    );
    assert_eq!(
        material_registry_count(&ctx, material_id).await?,
        1,
        "shared source material id should still have one registry row"
    );

    for (i, event_id) in published_ids.into_iter().enumerate() {
        let persisted = persisted_event(&ctx, event_id).await?;
        assert_eq!(
            persisted.source.as_str(),
            "test-source",
            "all events should have the same source"
        );
        assert_eq!(
            persisted.event_type.as_str(),
            "test.event",
            "all events should have the same event type"
        );
        assert_eq!(persisted.payload["sequence"], serde_json::json!(i));
        assert_eq!(
            persisted.payload["event"],
            serde_json::json!("shared material")
        );
        assert_material_provenance(&persisted, material_id, i as i64)?;
    }

    Ok(())
}

async fn material_registry_count(
    ctx: &TestContext,
    material_id: Id<SourceMaterial>,
) -> TestResult<i64> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM raw.source_material_registry WHERE id = $1::uuid",
    )
    .bind(material_id.to_uuid())
    .fetch_one(&ctx.pool)
    .await?;
    Ok(count)
}

async fn persisted_event(
    ctx: &TestContext,
    event_id: Uuid,
) -> TestResult<Event<serde_json::Value>> {
    let typed_id = Id::<Event<serde_json::Value>>::from_uuid(event_id);
    WaitHelpers::wait_for_event_id(&ctx.pool, typed_id, Timeouts::STANDARD).await?;
    ctx.pool
        .events()
        .get_by_id(typed_id)
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("published event {event_id} was not persisted"))
}

fn assert_material_provenance(
    event: &Event<serde_json::Value>,
    expected_material_id: Id<SourceMaterial>,
    expected_anchor_byte: i64,
) -> TestResult<()> {
    match event.provenance() {
        Provenance::Material {
            id, anchor_byte, ..
        } => {
            assert_eq!(*id, expected_material_id);
            assert_eq!(*anchor_byte, expected_anchor_byte);
        }
        other => {
            return Err(color_eyre::eyre::eyre!(
                "expected material provenance, got {other:?}"
            ));
        }
    }
    Ok(())
}
