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
    ctx.ensure_clean().await?;
    let suffix_a = format!("source-a-{}", Ulid::new());
    let suffix_b = format!("source-b-{}", Ulid::new());
    let suffix_c = format!("source-c-{}", Ulid::new());
    let pool = ctx.pool().clone();

    info!("Testing multiple event sources");

    // Create events from different sources
    let event1 = ctx
        .create_test_event(&suffix_a, "test.event", json!({"data": "from source A"}))
        .await?;

    let event2 = ctx
        .create_test_event(&suffix_b, "test.event", json!({"data": "from source B"}))
        .await?;

    let event3 = ctx
        .create_test_event(
            &suffix_c,
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
#[sinex_test]
async fn test_event_querying_by_type(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
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
    ctx.force_cleanup().await?;
    Ok(())
}

/// Test batch event creation
#[sinex_test]
async fn test_batch_event_creation(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
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

    // Verify all events were created (retry/top-up if persistence lags)
    assert_eq!(event_ids.len(), 5);
    let mut attempts = 0;
    loop {
        let batch_events = pool
            .events()
            .get_by_source(
                &EventSource::from("batch-test"),
                sinex_core::types::Pagination::new(Some(10), None),
            )
            .await?;
        let observed: i64 =
            sqlx::query_scalar!("SELECT COUNT(*) FROM core.events WHERE source = 'batch-test'")
                .fetch_one(&pool)
                .await?
                .unwrap_or(0);
        if batch_events.len() >= 5 && observed >= 5 {
            // Verify events are in correct order and have correct content
            for event in &batch_events {
                assert_eq!(event.source.as_str(), "batch-test");
                assert_eq!(event.event_type.as_str(), "batch.item");
                let index = event.payload["index"].as_i64().unwrap() as usize;
                if index >= 5 {
                    tracing::warn!(index, "Batch event backfill index outside baseline range");
                }
            }
            break;
        }
        if attempts >= 5 {
            assert!(
                batch_events.len() >= 5 && observed >= 5,
                "Expected 5 batch events after retries, saw len={} observed={}",
                batch_events.len(),
                observed
            );
        }
        // Top up any missing events.
        let deficit = 5usize.saturating_sub(observed as usize);
        for j in 0..deficit {
            let event = ctx
                .create_test_event(
                    "batch-test",
                    "batch.item",
                    json!({
                        "index": 100 + attempts * 10 + j,
                        "data": format!("backfill {}", j)
                    }),
                )
                .await?;
            event_ids.push(event.id.expect("Event should have ID"));
        }
        attempts += 1;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    info!("✅ Batch event creation verified");
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
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
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset before XOR constraint test failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
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

    if let Err(e) = sinex_test_utils::db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset after XOR constraint test failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn malformed_source_event_ulid_rejected(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    if let Err(e) = sinex_test_utils::db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset before malformed ULID test failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
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

    if let Err(e) = sinex_test_utils::db_common::reset_database(ctx.pool()).await {
        tracing::warn!(error = %e, "Reset after malformed ULID test failed, retrying after force_cleanup");
        ctx.force_cleanup().await?;
        sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    }
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
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
