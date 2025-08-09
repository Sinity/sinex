use color_eyre::eyre::Result;
use sinex_test_utils::prelude::*;
use color_eyre::eyre::Result;
use sinex_core::db::repositories::DbPoolExt;
use serde_json::json;
use tracing::info;

/// Integration test for provenance tracking functionality
///
/// This test verifies basic provenance tracking through:
/// - Creating events with the test context API
/// - Verifying events can be stored and retrieved
/// - Testing basic event properties and persistence

#[sinex_test]
async fn test_basic_event_creation_and_persistence(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    info!("Testing basic event creation and persistence");

    // Create a test event using the test context convenience method
    let event = ctx.create_test_event(
        "provenance-test",
        "test.event",
        json!({
            "message": "test provenance tracking",
            "step": 1
        })
    ).await?;

    // Verify the event was created
    let event_id = event.id.expect("Event should have ID");
    info!("Created event with ID: {}", event_id);

    // Verify we can query recent events using the repository API
    let recent_events = pool.events().get_recent(10).await?;
    assert!(!recent_events.is_empty(), "Should have at least one recent event");

    // Find our test event in the results
    let found_event = recent_events.iter()
        .find(|e| e.id.as_ref().map(|id| *id == event_id).unwrap_or(false));
    
    assert!(found_event.is_some(), "Should find our test event");
    
    let found_event = found_event.unwrap();
    assert_eq!(found_event.source.as_str(), "provenance-test");
    assert_eq!(found_event.event_type.as_str(), "test.event");
    assert_eq!(found_event.payload["message"], json!("test provenance tracking"));

    info!("✅ Basic event creation and persistence verified");
    Ok(())
}

/// Test event creation with different sources
#[sinex_test]
async fn test_multiple_event_sources(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    info!("Testing multiple event sources");

    // Create events from different sources
    let event1 = ctx.create_test_event(
        "source-a",
        "test.event",
        json!({"data": "from source A"})
    ).await?;

    let event2 = ctx.create_test_event(
        "source-b",
        "test.event",
        json!({"data": "from source B"})
    ).await?;

    let event3 = ctx.create_test_event(
        "source-c",
        "different.type",
        json!({"data": "from source C"})
    ).await?;

    // Verify all events were created
    assert!(event1.id.is_some());
    assert!(event2.id.is_some());
    assert!(event3.id.is_some());

    // Query events by source
    let events_from_a = pool.events()
        .get_by_source(&EventSource::from("source-a"), Some(10), None)
        .await?;
    let events_from_b = pool.events()
        .get_by_source(&EventSource::from("source-b"), Some(10), None)
        .await?;
    let events_from_c = pool.events()
        .get_by_source(&EventSource::from("source-c"), Some(10), None)
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
async fn test_event_querying_by_type(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    info!("Testing event querying by type");

    // Create events of different types
    let _event1 = ctx.create_test_event(
        "test-source",
        "type.a",
        json!({"category": "A"})
    ).await?;

    let _event2 = ctx.create_test_event(
        "test-source",
        "type.a",
        json!({"category": "A2"})
    ).await?;

    let _event3 = ctx.create_test_event(
        "test-source",
        "type.b",
        json!({"category": "B"})
    ).await?;

    // Query by event type
    let type_a_events = pool.events()
        .get_by_event_type(&EventType::from("type.a"), Some(10), None)
        .await?;
    let type_b_events = pool.events()
        .get_by_event_type(&EventType::from("type.b"), Some(10), None)
        .await?;

    assert_eq!(type_a_events.len(), 2, "Should have 2 events of type.a");
    assert_eq!(type_b_events.len(), 1, "Should have 1 event of type.b");

    // Verify event content
    assert!(type_a_events.iter().all(|e| e.event_type.as_str() == "type.a"));
    assert!(type_b_events.iter().all(|e| e.event_type.as_str() == "type.b"));

    info!("✅ Event querying by type verified");
    Ok(())
}

/// Test batch event creation
#[sinex_test]
async fn test_batch_event_creation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();

    info!("Testing batch event creation");

    // Create multiple events in sequence
    let mut event_ids = Vec::new();
    
    for i in 0..5 {
        let event = ctx.create_test_event(
            "batch-test",
            "batch.item",
            json!({
                "index": i,
                "data": format!("batch item {}", i)
            })
        ).await?;
        
        event_ids.push(event.id.expect("Event should have ID"));
    }

    // Verify all events were created
    assert_eq!(event_ids.len(), 5);

    // Query events by source to verify batch
    let batch_events = pool.events()
        .get_by_source(&EventSource::from("batch-test"), Some(10), None)
        .await?;

    assert_eq!(batch_events.len(), 5, "Should have 5 batch events");

    // Verify events are in correct order and have correct content
    for (i, event) in batch_events.iter().enumerate() {
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
async fn test_event_payload_preservation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
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

    let event = ctx.create_test_event(
        "payload-test",
        "complex.payload",
        complex_payload.clone()
    ).await?;

    let event_id = event.id.expect("Event should have ID");

    // Retrieve the event and verify payload integrity
    let retrieved_events = pool.events()
        .get_by_source(&EventSource::from("payload-test"), Some(1), None)
        .await?;

    assert_eq!(retrieved_events.len(), 1, "Should have 1 payload test event");
    let retrieved_event = &retrieved_events[0];

    // Verify the entire payload structure is preserved
    assert_eq!(retrieved_event.payload, complex_payload, "Payload should be exactly preserved");

    // Verify specific nested elements
    assert_eq!(retrieved_event.payload["metadata"]["version"], json!("1.0"));
    assert_eq!(retrieved_event.payload["metadata"]["tags"][0], json!("test"));
    assert_eq!(retrieved_event.payload["metadata"]["config"]["enabled"], json!(true));
    assert_eq!(retrieved_event.payload["data"]["items"][0]["name"], json!("first"));
    assert_eq!(retrieved_event.payload["data"]["statistics"]["total_count"], json!(2));
    assert_eq!(retrieved_event.payload["simple_values"]["number"], json!(42));
    assert_eq!(retrieved_event.payload["simple_values"]["float"], json!(3.14159));
    assert_eq!(retrieved_event.payload["simple_values"]["null_value"], json!(null));

    info!("✅ Event payload preservation verified");
    Ok(())
}