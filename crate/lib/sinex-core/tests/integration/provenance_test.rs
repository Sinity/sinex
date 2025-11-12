use chrono::Utc;
use serde_json::json;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::db::validation::{EventValidator, ValidationError};
use sinex_core::{Event, EventId, Ulid};
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
    let pool = ctx.pool().clone();

    info!("Testing basic event creation and persistence");

    // Create a test event using the test context convenience method
    let event = ctx
        .create_test_event(
            "provenance-test",
            "test.event",
            json!({
                "message": "test provenance tracking",
                "step": 1
            }),
        )
        .await?;

    // Verify the event was created
    let event_id = event.id.expect("Event should have ID");
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
    let pool = ctx.pool().clone();

    info!("Testing multiple event sources");

    // Create events from different sources
    let event1 = ctx
        .create_test_event("source-a", "test.event", json!({"data": "from source A"}))
        .await?;

    let event2 = ctx
        .create_test_event("source-b", "test.event", json!({"data": "from source B"}))
        .await?;

    let event3 = ctx
        .create_test_event(
            "source-c",
            "different.type",
            json!({"data": "from source C"}),
        )
        .await?;

    // Verify all events were created
    assert!(event1.id.is_some());
    assert!(event2.id.is_some());
    assert!(event3.id.is_some());

    // Query events by source
    let events_from_a = pool
        .events()
        .get_by_source(
            &EventSource::from("source-a"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    let events_from_b = pool
        .events()
        .get_by_source(
            &EventSource::from("source-b"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;
    let events_from_c = pool
        .events()
        .get_by_source(
            &EventSource::from("source-c"),
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
#[sinex_test]
async fn test_event_querying_by_type(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    info!("Testing event querying by type");

    // Create events of different types
    let _event1 = ctx
        .create_test_event("test-source", "type.a", json!({"category": "A"}))
        .await?;

    let _event2 = ctx
        .create_test_event("test-source", "type.a", json!({"category": "A2"}))
        .await?;

    let _event3 = ctx
        .create_test_event("test-source", "type.b", json!({"category": "B"}))
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
#[sinex_test]
async fn test_batch_event_creation(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    info!("Testing batch event creation");

    // Create multiple events in sequence
    let mut event_ids = Vec::new();

    for i in 0..5 {
        let event = ctx
            .create_test_event(
                "batch-test",
                "batch.item",
                json!({
                    "index": i,
                    "data": format!("batch item {}", i)
                }),
            )
            .await?;

        event_ids.push(event.id.expect("Event should have ID"));
    }

    // Verify all events were created
    assert_eq!(event_ids.len(), 5);

    // Query events by source to verify batch
    let batch_events = pool
        .events()
        .get_by_source(
            &EventSource::from("batch-test"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(batch_events.len(), 5, "Should have 5 batch events");

    // Verify events are in correct order and have correct content
    for event in &batch_events {
        assert_eq!(event.source.as_str(), "batch-test");
        assert_eq!(event.event_type.as_str(), "batch.item");
        // Note: The order might not be guaranteed, so we check that all indices exist
        let index = event.payload["index"].as_i64().unwrap() as usize;
        assert!(index < 5, "Index should be in range");
    }

    info!("✅ Batch event creation verified");
    Ok(())
}

/// Test event payload structure preservation
#[sinex_test]
async fn test_event_payload_preservation(ctx: TestContext) -> TestResult<()> {
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

    let _event = ctx
        .create_test_event("payload-test", "complex.payload", complex_payload.clone())
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

#[sinex_test]
async fn provenance_xor_constraint_enforced(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let material = ctx.create_source_material(Some("xor-constraint")).await?;
    let parent = ctx
        .create_test_event("prov-parent", "prov.event", json!({ "p": true }))
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

#[sinex_test]
async fn malformed_source_event_ulid_rejected(ctx: TestContext) -> TestResult<()> {
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
    let message = format!("{:?}", err.unwrap_err());
    assert!(
        message.contains("invalid input syntax for type uuid"),
        "expected UUID parse error, got: {message}"
    );

    Ok(())
}

#[sinex_test]
async fn duplicate_parent_ids_rejected_by_validator() -> color_eyre::eyre::Result<()> {
    let validator = EventValidator::new();
    let parent = EventId::new();

    let mut event = Event::dynamic("prov-security", "duplicate.parents", json!({"case": "dup"}))
        .from_parents(vec![parent.clone(), parent])
        .build();

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
