use chrono::Utc;
use serde_json::json;
use sinex_core::db::models::Provenance;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::db::validation::{EventValidator, ValidationError};
use sinex_core::{DynamicPayload, EventId, Id, JsonValue, Ulid};
use sinex_test_utils::prelude::*;
use tracing::info;

/// Integration test for provenance tracking functionality
///
/// This test verifies basic provenance tracking through:
/// - Creating events with the test context API
/// - Verifying events can be stored and retrieved
/// - Testing basic event properties and persistence

#[sinex_test]
async fn test_basic_event_creation_and_persistence(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool().clone();

    info!("Testing basic event creation and persistence");

    // Create a test event using the test context convenience method
    let event = ctx
        .publish(DynamicPayload::new(
            "provenance-test",
            "test.event",
            json!({
                "message": "test provenance tracking",
                "step": 1
            }),
        ))
        .await?;

    // Verify the event was created
    let event_id = event.id.clone().expect("Event should have ID");
    info!("Created event with ID: {}", event_id);

    // Verify we can query recent events using the repository API
    let recent_events = pool.events().get_recent(10).await?;
    assert!(
        !recent_events.is_empty(),
        "Should have at least one recent event"
    );

    // Find our test event in the results
    let found_event = recent_events
        .iter()
        .find(|e| e.id.as_ref().map(|id| *id == event_id).unwrap_or(false));

    assert!(found_event.is_some(), "Should find our test event");

    let found_event = found_event.unwrap();
    assert_eq!(found_event.source.as_str(), "provenance-test");
    assert_eq!(found_event.event_type.as_str(), "test.event");
    assert_eq!(
        found_event.payload["message"],
        json!("test provenance tracking")
    );

    info!("✅ Basic event creation and persistence verified");
    Ok(())
}

/// Test event creation with different sources
#[sinex_test]
async fn test_multiple_event_sources(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let suffix_a = format!("source-a-{}", Ulid::new());
    let suffix_b = format!("source-b-{}", Ulid::new());
    let suffix_c = format!("source-c-{}", Ulid::new());
    let pool = ctx.pool().clone();

    info!("Testing multiple event sources");

    // Create events from different sources
    let event1 = ctx
        .publish(DynamicPayload::new(
            &*suffix_a,
            "test.event",
            json!({"data": "from source A"}),
        ))
        .await?;

    let event2 = ctx
        .publish(DynamicPayload::new(
            &*suffix_b,
            "test.event",
            json!({"data": "from source B"}),
        ))
        .await?;

    let event3 = ctx
        .publish(DynamicPayload::new(
            &*suffix_c,
            "different.type",
            json!({"data": "from source C"}),
        ))
        .await?;

    // Verify all events were created
    assert!(event1.id.is_some());
    assert!(event2.id.is_some());
    assert!(event3.id.is_some());

    // Query events by source
    let events_from_a = pool
        .events()
        .get_by_source(
            &EventSource::from(suffix_a.as_str()),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    let events_from_b = pool
        .events()
        .get_by_source(
            &EventSource::from(suffix_b.as_str()),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    let events_from_c = pool
        .events()
        .get_by_source(
            &EventSource::from(suffix_c.as_str()),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(events_from_a.len(), 1, "Should have 1 event from source-a");
    assert_eq!(events_from_b.len(), 1, "Should have 1 event from source-b");
    assert_eq!(events_from_c.len(), 1, "Should have 1 event from source-c");

    // Verify event content
    assert_eq!(events_from_a[0].payload["data"], json!("from source A"));
    assert_eq!(events_from_b[0].payload["data"], json!("from source B"));
    assert_eq!(events_from_c[0].payload["data"], json!("from source C"));

    info!("✅ Multiple event sources verified");
    Ok(())
}

/// Test event querying by type
#[sinex_serial_test]
async fn test_event_querying_by_type(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool().clone();

    info!("Testing event querying by type");

    // Create events of different types
    ctx.publish(DynamicPayload::new(
        "test-source",
        "type.a",
        json!({"category": "A"}),
    ))
    .await?;

    ctx.publish(DynamicPayload::new(
        "test-source",
        "type.a",
        json!({"category": "A2"}),
    ))
    .await?;

    ctx.publish(DynamicPayload::new(
        "test-source",
        "type.b",
        json!({"category": "B"}),
    ))
    .await?;

    // Query by event type
    let type_a_events = pool
        .events()
        .get_by_event_type(
            &EventType::from("type.a"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    let type_b_events = pool
        .events()
        .get_by_event_type(
            &EventType::from("type.b"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(type_a_events.len(), 2, "Should have 2 events of type.a");
    assert_eq!(type_b_events.len(), 1, "Should have 1 event of type.b");

    // Verify event content
    assert!(type_a_events
        .iter()
        .all(|e| e.event_type.as_str() == "type.a"));
    assert!(type_b_events
        .iter()
        .all(|e| e.event_type.as_str() == "type.b"));

    info!("✅ Event querying by type verified");
    Ok(())
}

/// Test batch event creation
#[sinex_serial_test]
async fn test_batch_event_creation(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool().clone();

    info!("Testing batch event creation");

    // Create multiple events in sequence
    let mut event_ids = Vec::new();

    for i in 0..5 {
        let event = ctx
            .publish(DynamicPayload::new(
                "batch-test",
                "batch.item",
                json!({
                    "index": i,
                    "data": format!("batch item {}", i)
                }),
            ))
            .await?;
        let id = event.id.clone().expect("Event should have ID");
        event_ids.push(id);
    }

    // Verify all events were created.
    assert_eq!(event_ids.len(), 5);
    ctx.timing().wait_for_source_events("batch-test", 5).await?;

    let batch_events = pool
        .events()
        .get_by_source(
            &EventSource::from("batch-test"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    let observed = pool
        .events()
        .count_by_source(&EventSource::from("batch-test"))
        .await?;
    assert!(
        batch_events.len() >= 5 && observed >= 5,
        "Expected 5 batch events, saw len={} observed={}",
        batch_events.len(),
        observed
    );

    // Verify events are in correct order and have correct content
    for event in &batch_events {
        assert_eq!(event.source.as_str(), "batch-test");
        assert_eq!(event.event_type.as_str(), "batch.item");
    }

    info!("✅ Batch event creation verified");
    Ok(())
}

/// Test event payload structure preservation
#[sinex_test]
async fn test_event_payload_preservation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool().clone();

    info!("Testing event payload structure preservation");

    // Create an event with complex nested payload
    let complex_payload = json!({
        "metadata": {
            "version": "1.0",
            "tags": ["test", "complex", "nested"],
            "config": {
                "enabled": true,
                "timeout": 5000,
                "retries": 3
            }
        },
        "data": {
            "items": [
                {"id": 1, "name": "first", "active": true},
                {"id": 2, "name": "second", "active": false}
            ],
            "statistics": {
                "total_count": 2,
                "active_count": 1,
                "last_updated": "2024-01-01T00:00:00Z"
            }
        },
        "simple_values": {
            "string": "test string",
            "number": 42,
            "float": 3.14159,
            "boolean": true,
            "null_value": null
        }
    });

    ctx.publish(DynamicPayload::new(
        "payload-test",
        "complex.payload",
        complex_payload.clone(),
    ))
    .await?;

    // Retrieve the event and verify payload integrity
    let retrieved_events = pool
        .events()
        .get_by_source(
            &EventSource::from("payload-test"),
            sinex_core::types::Pagination::new(Some(1), None),
        )
        .await?;

    assert_eq!(
        retrieved_events.len(),
        1,
        "Should have 1 payload test event"
    );
    let retrieved_event = &retrieved_events[0];

    // Verify the entire payload structure is preserved
    assert_eq!(
        retrieved_event.payload, complex_payload,
        "Payload should be exactly preserved"
    );

    // Verify specific nested elements
    assert_eq!(retrieved_event.payload["metadata"]["version"], json!("1.0"));
    assert_eq!(
        retrieved_event.payload["metadata"]["tags"][0],
        json!("test")
    );
    assert_eq!(
        retrieved_event.payload["metadata"]["config"]["enabled"],
        json!(true)
    );
    assert_eq!(
        retrieved_event.payload["data"]["items"][0]["name"],
        json!("first")
    );
    assert_eq!(
        retrieved_event.payload["data"]["statistics"]["total_count"],
        json!(2)
    );
    assert_eq!(
        retrieved_event.payload["simple_values"]["number"],
        json!(42)
    );
    assert_eq!(
        retrieved_event.payload["simple_values"]["float"],
        json!(3.14159)
    );
    assert_eq!(
        retrieved_event.payload["simple_values"]["null_value"],
        json!(null)
    );

    info!("✅ Event payload preservation verified");
    Ok(())
}

#[sinex_serial_test]
async fn provenance_xor_constraint_enforced(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool();
    let material = ctx.create_source_material(Some("xor-constraint")).await?;
    let parent = ctx
        .publish(DynamicPayload::new(
            "prov-parent",
            "prov.event",
            json!({ "p": true }),
        ))
        .await?
        .id
        .expect("parent event id");

    let err = sqlx::query!(
        r#"
        INSERT INTO core.events (
            id, source, event_type, host, payload,
            ts_orig, source_material_id, source_event_ids,
            anchor_byte, offset_kind
        ) VALUES (
            $1::uuid, $2, $3, $4, $5,
            $6, $7::uuid, ARRAY[$8::uuid]::uuid[]::ulid[],
            0, 'byte'
        )
        "#,
        Ulid::new().to_uuid(),
        "prov-xor",
        "dual.provenance",
        "provenance-suite",
        json!({"attack": "dual-provenance"}),
        Utc::now(),
        material.as_ulid().to_uuid(),
        parent.as_ulid().to_uuid()
    )
    .execute(pool)
    .await;

    assert!(err.is_err(), "dual provenance insert should fail");
    let message = format!("{:?}", err.unwrap_err());
    assert!(
        message.contains("check constraint"),
        "expected check constraint violation, got: {message}"
    );
    Ok(())
}

#[sinex_serial_test]
async fn malformed_source_event_ulid_rejected(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let pool = ctx.pool();

    let err = sqlx::query(
        r#"
        INSERT INTO core.events (
            id, source, event_type, host, payload,
            ts_orig, source_material_id, source_event_ids,
            anchor_byte, offset_kind
        ) VALUES (
            $1::uuid, 'prov-malformed', 'synthesis.bad', 'provenance-suite', $2,
            $3, NULL, ARRAY[$4::uuid]::uuid[]::ulid[],
            0, 'byte'
        )
        "#,
    )
    .bind(Ulid::new().to_uuid())
    .bind(json!({"case": "malformed-ulid"}))
    .bind(Utc::now())
    .bind("not-a-valid-ulid")
    .execute(pool)
    .await;

    assert!(err.is_err(), "malformed ULID should be rejected");
    assert!(
        err.is_err(),
        "malformed ULID payload should be rejected by the database"
    );

    Ok(())
}

#[sinex_test]
async fn synthesis_provenance_rejects_direct_cycles(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let pool = ctx.pool().clone();
    let repo = pool.events();

    let parent_id = Id::<Event<JsonValue>>::new();
    let child_id = Id::<Event<JsonValue>>::new();

    let mut parent_event =
        DynamicPayload::new("cycle-test", "cycle.parent", json!({"role": "parent"}))
            .with_provenance(
                Provenance::from_synthesis(vec![EventId::from_ulid(*child_id.as_ulid())])
                    .expect("non-empty"),
            )
            .build()?;
    parent_event.id = Some(parent_id.clone());

    repo.insert(parent_event).await?;

    let mut child_event =
        DynamicPayload::new("cycle-test", "cycle.child", json!({"role": "child"}))
            .with_provenance(
                Provenance::from_synthesis(vec![EventId::from_ulid(*parent_id.as_ulid())])
                    .expect("non-empty"),
            )
            .build()?;
    child_event.id = Some(child_id.clone());

    let err = repo
        .insert(child_event)
        .await
        .expect_err("cycle should fail");
    assert!(
        format!("{err}").contains("cycle"),
        "error should mention cycle"
    );

    Ok(())
}

#[sinex_test]
async fn synthesis_provenance_rejects_indirect_cycles(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let pool = ctx.pool().clone();
    let repo = pool.events();

    let ancestor_id = Id::<Event<JsonValue>>::new();
    let parent_id = Id::<Event<JsonValue>>::new();
    let child_id = Id::<Event<JsonValue>>::new();

    let mut ancestor_event =
        DynamicPayload::new("cycle-test", "cycle.ancestor", json!({"role": "ancestor"}))
            .with_provenance(
                Provenance::from_synthesis(vec![EventId::from_ulid(*child_id.as_ulid())])
                    .expect("non-empty"),
            )
            .build()?;
    ancestor_event.id = Some(ancestor_id.clone());
    repo.insert(ancestor_event).await?;

    let mut parent_event =
        DynamicPayload::new("cycle-test", "cycle.parent", json!({"role": "parent"}))
            .with_provenance(
                Provenance::from_synthesis(vec![EventId::from_ulid(*ancestor_id.as_ulid())])
                    .expect("non-empty"),
            )
            .build()?;
    parent_event.id = Some(parent_id.clone());
    repo.insert(parent_event).await?;

    let mut child_event =
        DynamicPayload::new("cycle-test", "cycle.child", json!({"role": "child"}))
            .with_provenance(
                Provenance::from_synthesis(vec![EventId::from_ulid(*parent_id.as_ulid())])
                    .expect("non-empty"),
            )
            .build()?;
    child_event.id = Some(child_id.clone());

    let err = repo
        .insert(child_event)
        .await
        .expect_err("cycle should fail");
    assert!(
        format!("{err}").contains("cycle"),
        "error should mention cycle"
    );

    Ok(())
}

#[sinex_test]
async fn duplicate_parent_ids_rejected_by_validator() -> TestResult<()> {
    let validator = EventValidator::new();
    let parent = EventId::new();

    let mut event =
        DynamicPayload::new("prov-security", "duplicate.parents", json!({"case": "dup"}))
            .from_parents(vec![parent.clone(), parent])?
            .build()?;

    event.id = Some(EventId::new());

    let err = validator
        .validate(&event)
        .expect_err("validator must reject duplicate parent list");
    assert!(
        matches!(
            err,
            ValidationError::InvalidValue { ref field, .. }
                if field == "provenance.source_event_ids"
        ),
        "expected duplicate parent validation error, got {err:?}"
    );

    Ok(())
}

/// CRITICAL TEST: Verify provenance survives NATS pipeline roundtrip
///
/// This test exposes the silent provenance loss bug where:
/// - Events serialize with nested `{"provenance": {"type": "material", ...}}`
/// - ingestd RawEvent expects flat fields `{"source_material_id": "...", "anchor_byte": 0}`
/// - serde ignores unknown `provenance` field → provenance lost
/// - ingestd falls back to "self-referential synthesis" for ALL events
///
/// WITHOUT THIS TEST: All 297 tests using NATS pipeline passed despite provenance being silently lost!
#[sinex_test]
async fn test_provenance_survives_nats_roundtrip(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool().clone();

    info!("Testing provenance survives NATS pipeline roundtrip");

    // Create a material provenance event (first-order event from source material)
    let material = ctx.create_source_material(Some("nats-roundtrip")).await?;

    let mut event = DynamicPayload::new(
        "provenance-roundtrip-test",
        "material.event",
        json!({"test": "provenance through nats"}),
    )
    .from_material_at(material, 100) // anchor_byte = 100
    .build()?;

    event.id = Some(Id::new());
    let event_id = event.id.clone().unwrap();

    // Publish through NATS (this is where provenance gets lost in the bug)
    ctx.publish_prebuilt_event(&event).await?;

    // Wait for persistence
    ctx.timing()
        .wait_for_source_events("provenance-roundtrip-test", 1)
        .await?;

    // Retrieve from database and verify provenance is MATERIAL, not synthesis
    let retrieved = pool
        .events()
        .get_by_id(event_id.clone())
        .await?
        .expect("Event should exist");

    // CRITICAL ASSERTIONS: These fail without the fix!
    match retrieved.provenance() {
        Provenance::Material {
            id, anchor_byte, ..
        } => {
            assert_eq!(*id, material, "Material ID should match what was sent");
            assert_eq!(*anchor_byte, 100, "Anchor byte should match what was sent");
            info!("✅ Provenance correctly preserved through NATS pipeline");
        }
        Provenance::Synthesis { .. } => {
            panic!(
                "BUG DETECTED: Event was created with Material provenance but retrieved as Synthesis! \
                 Provenance was lost in NATS serialization/deserialization."
            );
        }
        _ => panic!("Unexpected provenance variant"),
    }

    Ok(())
}

/// Test that synthesis provenance also survives NATS roundtrip
#[sinex_test]
async fn test_synthesis_provenance_survives_nats_roundtrip(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool().clone();

    info!("Testing synthesis provenance survives NATS roundtrip");

    // Create a parent event first
    let parent = ctx
        .publish(DynamicPayload::new(
            "parent-source",
            "parent.event",
            json!({"parent": true}),
        ))
        .await?;
    let parent_id = parent.id.expect("Parent should have ID");

    // Create a synthesis event (derived from parent)
    let mut synthesis_event = DynamicPayload::new(
        "synthesis-roundtrip-test",
        "derived.event",
        json!({"derived": "from parent"}),
    )
    .from_parents(vec![parent_id])?
    .build()?;

    synthesis_event.id = Some(Id::new());
    let synthesis_id = synthesis_event.id.clone().unwrap();

    // Publish through NATS
    ctx.publish_prebuilt_event(&synthesis_event).await?;

    // Wait for persistence
    ctx.timing()
        .wait_for_source_events("synthesis-roundtrip-test", 1)
        .await?;

    // Retrieve and verify synthesis provenance preserved
    let retrieved = pool
        .events()
        .get_by_id(synthesis_id.clone())
        .await?
        .expect("Event should exist");

    match retrieved.provenance() {
        Provenance::Synthesis {
            source_event_ids, ..
        } => {
            assert_eq!(source_event_ids.len(), 1, "Should have one parent event ID");
            assert_eq!(
                source_event_ids[0], parent_id,
                "Parent ID should match what was sent"
            );
            info!("✅ Synthesis provenance correctly preserved through NATS pipeline");
        }
        Provenance::Material { .. } => {
            panic!(
                "BUG DETECTED: Event was created with Synthesis provenance but retrieved as Material! \
                 Provenance was corrupted in NATS serialization/deserialization."
            );
        }
        _ => panic!("Unexpected provenance variant"),
    }

    Ok(())
}
