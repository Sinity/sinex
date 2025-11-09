//! Type Safety Integration Tests
//!
//! Tests for type safety guarantees across the entire system:
//! - Generic Id<T> type safety and conversions  
//! - Event payload type safety and validation
//! - Domain string types (EventSource, EventType) safety
//! - Cross-component type safety integration
//! - Repository type safety guarantees

use serde_json::json;
use sinex_core::db::models::Event;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_core::types::{Id, Ulid};
use sinex_core::EventSearchFilters;
use sinex_test_utils::constants::{
    EVENT_SOURCE_REPO_PRIMARY, EVENT_TYPE_FIXTURE_QUERY_SAFETY, EVENT_TYPE_QUERY_SAFETY,
    SOURCE_FIXTURE_REPO_PRIMARY, SOURCE_FIXTURE_REPO_SECONDARY,
};
use sinex_test_utils::prelude::*;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestCheckpoint;

// =============================================================================
// GENERIC ID TYPE SAFETY TESTS
// =============================================================================

#[sinex_test]
async fn test_generic_id_type_isolation(ctx: TestContext) -> Result<()> {
    // Create IDs of different types
    let event_id = Id::<Event>::new();
    let checkpoint_id = Id::<TestCheckpoint>::new();

    // Verify they have different types at compile time
    // (the following would fail to compile if uncommented)
    // let _type_error: Id<Event> = checkpoint_id; // Compilation error
    // let _type_error: Id<TestCheckpoint> = event_id; // Compilation error

    // But they should both be unique
    assert_ne!(event_id.to_string(), checkpoint_id.to_string());

    // Both should convert to/from ULID correctly
    let event_ulid: Ulid = event_id.clone().into();
    let checkpoint_ulid: Ulid = checkpoint_id.clone().into();

    let recovered_event_id = Id::<Event>::from(event_ulid);
    let recovered_checkpoint_id = Id::<TestCheckpoint>::from(checkpoint_ulid);

    assert_eq!(event_id, recovered_event_id);
    assert_eq!(checkpoint_id, recovered_checkpoint_id);

    Ok(())
}

#[sinex_test]
async fn test_id_database_integration_type_safety(ctx: TestContext) -> Result<()> {
    // Create an event and verify its ID type is preserved through database operations
    let event = ctx
        .create_test_event(
            "type-safety-test",
            "id.type_safety",
            json!({ "test": "id_type_safety" }),
        )
        .await?;

    // Extract the ID (should be Id<Event>)
    let event_id = event.id.expect("Event should have ID");

    // Query by ID using repository pattern
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(event_id.clone())
        .await?
        .expect("Event should exist");

    // Verify ID types match
    assert_eq!(
        retrieved.id.expect("Retrieved event should have ID"),
        event_id
    );

    // Verify we can't accidentally use wrong ID type
    // The following would be a compilation error:
    // let _wrong_query = ctx.pool.checkpoints().get_by_id(event_id).await?;

    Ok(())
}

#[sinex_test]
async fn test_id_collection_type_safety(ctx: TestContext) -> Result<()> {
    // Create multiple events
    let mut event_ids = Vec::new();

    for i in 0..5 {
        let event = ctx
            .create_test_event(
                "collection-test",
                "id.collection_safety",
                json!({ "index": i }),
            )
            .await?;

        event_ids.push(event.id.expect("Event should have ID"));
    }

    // Verify all IDs are unique
    let id_set: HashSet<String> = event_ids.iter().map(|id| id.to_string()).collect();
    assert_eq!(id_set.len(), 5, "All IDs should be unique");

    // Verify we can use the IDs to query events
    for event_id in &event_ids {
        let retrieved = ctx
            .pool
            .events()
            .get_by_id(event_id.clone())
            .await?
            .expect("Event should exist");

        assert_eq!(
            retrieved.id.expect("Retrieved event should have ID"),
            event_id.clone()
        );
    }

    Ok(())
}

// =============================================================================
// DOMAIN STRING TYPE SAFETY TESTS
// =============================================================================

#[sinex_test]
async fn test_event_source_type_safety(ctx: TestContext) -> Result<()> {
    // Test EventSource construction and validation
    let static_source = EventSource::from_static("test-source");
    let dynamic_source = EventSource::new("dynamic-test-source");

    // Verify both work for event creation
    let event1 = ctx
        .create_test_event(
            static_source.as_str(),
            "source.type_safety",
            json!({ "source_type": "static" }),
        )
        .await?;

    let event2 = ctx
        .create_test_event(
            dynamic_source.as_str(),
            "source.type_safety",
            json!({ "source_type": "dynamic" }),
        )
        .await?;

    // Verify sources are preserved correctly
    assert_eq!(event1.source.as_str(), "test-source");
    assert_eq!(event2.source.as_str(), "dynamic-test-source");

    // Verify we can query by source
    let static_events = ctx
        .pool
        .events()
        .get_by_source(
            &static_source,
            sinex_core::types::Pagination::new(None, None),
        )
        .await?;

    let dynamic_events = ctx
        .pool
        .events()
        .get_by_source(
            &dynamic_source,
            sinex_core::types::Pagination::new(None, None),
        )
        .await?;

    assert_eq!(static_events.len(), 1);
    assert_eq!(dynamic_events.len(), 1);

    Ok(())
}

#[sinex_test]
async fn test_event_type_safety(ctx: TestContext) -> Result<()> {
    // Test EventType construction and validation
    let static_type = EventType::from_static("static.test");
    let dynamic_type = EventType::new("dynamic.test");

    // Create events with different type construction methods
    let event1 = ctx
        .create_test_event(
            "type-safety-source",
            static_type.as_str(),
            json!({ "type_construction": "static" }),
        )
        .await?;

    let event2 = ctx
        .create_test_event(
            "type-safety-source",
            dynamic_type.as_str(),
            json!({ "type_construction": "dynamic" }),
        )
        .await?;

    // Verify types are preserved
    assert_eq!(event1.event_type.as_str(), "static.test");
    assert_eq!(event2.event_type.as_str(), "dynamic.test");

    // Verify type-based queries work
    let static_events = ctx
        .pool
        .events()
        .get_by_event_type(&static_type, sinex_core::types::Pagination::new(None, None))
        .await?;

    let dynamic_events = ctx
        .pool
        .events()
        .get_by_event_type(
            &dynamic_type,
            sinex_core::types::Pagination::new(None, None),
        )
        .await?;

    assert_eq!(static_events.len(), 1);
    assert_eq!(dynamic_events.len(), 1);

    Ok(())
}

#[sinex_test]
async fn test_domain_string_const_support(ctx: TestContext) -> Result<()> {
    // Test compile-time constants for domain strings
    const TEST_SOURCE: EventSource = EventSource::from_static("const-source");
    const TEST_TYPE: EventType = EventType::from_static("const.type");

    // Use const values in runtime
    let event = ctx
        .create_test_event(
            TEST_SOURCE.as_str(),
            TEST_TYPE.as_str(),
            json!({ "const_test": true }),
        )
        .await?;

    // Verify const values work correctly
    assert_eq!(event.source.as_str(), "const-source");
    assert_eq!(event.event_type.as_str(), "const.type");

    Ok(())
}

// =============================================================================
// EVENT PAYLOAD TYPE SAFETY TESTS
// =============================================================================

#[sinex_test]
async fn test_payload_validation_type_safety(ctx: TestContext) -> Result<()> {
    // Test that payload validation preserves type safety
    let valid_payload = json!({
        "required_field": "value",
        "optional_field": 42,
        "array_field": [1, 2, 3]
    });

    let event = ctx
        .create_test_event("payload-test", "payload.validation", valid_payload.clone())
        .await?;

    // Verify payload structure is preserved
    assert_eq!(event.payload["required_field"], json!("value"));
    assert_eq!(event.payload["optional_field"], json!(42));
    assert_eq!(event.payload["array_field"], json!([1, 2, 3]));

    // Verify queried event has same payload
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .expect("Event should exist");

    assert_eq!(retrieved.payload, valid_payload);

    Ok(())
}

#[sinex_test]
async fn test_nested_payload_type_preservation(ctx: TestContext) -> Result<()> {
    // Test deeply nested payload structure preservation
    let complex_payload = json!({
        "metadata": {
            "version": "1.0.0",
            "timestamp": 1234567890,
            "tags": ["urgent", "filesystem", "monitoring"]
        },
        "data": {
            "filesystem": {
                "path": "/home/user/documents/file.txt",
                "size_bytes": 1024,
                "permissions": {
                    "owner": "rwx",
                    "group": "r-x",
                    "other": "r--"
                }
            },
            "checksums": {
                "md5": "d41d8cd98f00b204e9800998ecf8427e",
                "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            }
        }
    });

    let event = ctx
        .create_test_event(
            "complex-payload",
            "nested.type_safety",
            complex_payload.clone(),
        )
        .await?;

    // Verify complex structure is preserved
    assert_eq!(event.payload["metadata"]["version"], json!("1.0.0"));
    assert_eq!(
        event.payload["data"]["filesystem"]["size_bytes"],
        json!(1024)
    );
    assert_eq!(
        event.payload["data"]["checksums"]["md5"],
        json!("d41d8cd98f00b204e9800998ecf8427e")
    );

    // Test through database round-trip
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(event.id.unwrap())
        .await?
        .expect("Event should exist");

    assert_eq!(retrieved.payload, complex_payload);

    Ok(())
}

// =============================================================================
// REPOSITORY PATTERN TYPE SAFETY TESTS
// =============================================================================

#[sinex_test]
async fn test_repository_query_type_safety(ctx: TestContext) -> Result<()> {
    // Create test data
    let _events = vec![
        ctx.create_test_event(
            SOURCE_FIXTURE_REPO_PRIMARY,
            EVENT_TYPE_FIXTURE_QUERY_SAFETY,
            json!({"index": 1}),
        )
        .await?,
        ctx.create_test_event(
            SOURCE_FIXTURE_REPO_PRIMARY,
            EVENT_TYPE_FIXTURE_QUERY_SAFETY,
            json!({"index": 2}),
        )
        .await?,
        ctx.create_test_event(
            SOURCE_FIXTURE_REPO_SECONDARY,
            EVENT_TYPE_FIXTURE_QUERY_SAFETY,
            json!({"index": 3}),
        )
        .await?,
    ];

    // Test source-based queries return correct types
    let repo_source = EVENT_SOURCE_REPO_PRIMARY;
    let repo_events = ctx
        .pool
        .events()
        .get_by_source(&repo_source, sinex_core::types::Pagination::new(None, None))
        .await?;

    assert_eq!(repo_events.len(), 2);
    for event in &repo_events {
        assert_eq!(event.source.as_str(), SOURCE_FIXTURE_REPO_PRIMARY);
        assert!(event.id.is_some()); // All events should have IDs
    }

    // Test type-based queries
    let repo_event_type = EVENT_TYPE_QUERY_SAFETY;
    let safety_events = ctx
        .pool
        .events()
        .get_by_event_type(
            &repo_event_type,
            sinex_core::types::Pagination::new(None, None),
        )
        .await?;

    assert_eq!(safety_events.len(), 3);
    for event in &safety_events {
        assert_eq!(event.event_type.as_str(), EVENT_TYPE_FIXTURE_QUERY_SAFETY);
    }

    // Test combined queries
    let filters = EventSearchFilters {
        sources: vec![EVENT_SOURCE_REPO_PRIMARY],
        event_types: vec![EVENT_TYPE_QUERY_SAFETY],
        ..Default::default()
    };
    let specific_events = ctx.pool.events().search(filters).await?;

    assert_eq!(specific_events.len(), 2);
    assert!(specific_events
        .iter()
        .all(|row| row.source.as_str() == SOURCE_FIXTURE_REPO_PRIMARY));

    Ok(())
}

#[sinex_test]
async fn test_repository_id_query_type_safety(ctx: TestContext) -> Result<()> {
    // Create an event
    let event = ctx
        .create_test_event(
            "id-query-test",
            "id.query_safety",
            json!({ "test_data": "repository_id_safety" }),
        )
        .await?;

    let event_id = event.id.expect("Event should have ID");

    // Test ID-based query
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(event_id.clone())
        .await?
        .expect("Event should exist");

    // Verify types match exactly
    assert_eq!(
        retrieved.id.expect("Retrieved event should have ID"),
        event_id
    );
    assert_eq!(retrieved.source.as_str(), event.source.as_str());
    assert_eq!(retrieved.event_type.as_str(), event.event_type.as_str());
    assert_eq!(retrieved.payload, event.payload);

    Ok(())
}

// =============================================================================
// CROSS-COMPONENT TYPE SAFETY INTEGRATION TESTS
// =============================================================================

#[sinex_test]
async fn test_event_creation_pipeline_type_safety(ctx: TestContext) -> Result<()> {
    // Test that types are preserved through the entire event creation pipeline

    // 1. Domain type construction
    let source = EventSource::from_static("pipeline-test");
    let event_type = EventType::from_static("pipeline.type_safety");

    // 2. Event creation (test helper ensures provenance)
    let event = Event::test_event(
        source.clone(),
        event_type.clone(),
        json!({
            "pipeline_stage": "creation",
            "type_safety": true
        }),
    );

    // 3. Verify event structure
    assert_eq!(event.source, source);
    assert_eq!(event.event_type, event_type);
    // New events have no ID until inserted

    // 4. Insert into database
    let inserted = ctx.pool.events().insert(event.clone()).await?;
    let inserted_id = inserted
        .id
        .clone()
        .expect("Inserted event should have an ID");

    // 5. Verify ID type preservation happens in storage
    let inserted_ulid: Ulid = inserted_id.clone().into();
    assert_ne!(inserted_ulid.to_uuid(), uuid::Uuid::nil());

    // 6. Query back using repository pattern
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(inserted_id.clone())
        .await?
        .expect("Event should exist");

    // 7. Verify all types preserved through entire pipeline
    assert_eq!(retrieved.source, source);
    assert_eq!(retrieved.event_type, event_type);
    assert_eq!(retrieved.id.unwrap(), inserted_id);
    assert_eq!(retrieved.payload["pipeline_stage"], json!("creation"));

    Ok(())
}

#[sinex_test]
async fn test_concurrent_type_safety(ctx: TestContext) -> Result<()> {
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let ctx = Arc::new(ctx);
    let mut join_set = JoinSet::new();

    // Create events concurrently with different type combinations
    let test_cases = vec![
        ("concurrent-1", "type1.safety", json!({"worker": 1})),
        ("concurrent-2", "type2.safety", json!({"worker": 2})),
        ("concurrent-1", "type2.safety", json!({"worker": 3})),
        ("concurrent-2", "type1.safety", json!({"worker": 4})),
    ];

    for (source, event_type, payload) in test_cases {
        let ctx_clone = ctx.clone();
        join_set.spawn(async move {
            let event = ctx_clone
                .create_test_event(source, event_type, payload)
                .await?;
            Ok::<_, color_eyre::eyre::Error>((
                event.source.as_str().to_string(),
                event.event_type.as_str().to_string(),
                event.id.unwrap(),
            ))
        });
    }

    // Collect results
    let mut results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        let (source, event_type, id) = result??;
        results.push((source, event_type, id));
    }

    assert_eq!(results.len(), 4);

    // Verify all IDs are unique (type safety maintained under concurrency)
    let ids: HashSet<_> = results.iter().map(|(_, _, id)| id.to_string()).collect();
    assert_eq!(ids.len(), 4, "All IDs should be unique");

    // Verify we can query all events back with correct types
    for (source, event_type, id) in results {
        let retrieved = ctx
            .pool
            .events()
            .get_by_id(id.clone())
            .await?
            .expect("Event should exist");

        assert_eq!(retrieved.source.as_str(), source);
        assert_eq!(retrieved.event_type.as_str(), event_type);
        assert_eq!(retrieved.id.unwrap(), id);
    }

    Ok(())
}

// =============================================================================
// ULID TYPE SAFETY EDGE CASES
// =============================================================================

#[sinex_test]
async fn test_ulid_type_conversion_safety(ctx: TestContext) -> Result<()> {
    // Test edge cases in ULID type conversions

    // Create ULID directly
    let ulid = Ulid::new();

    // Convert to different ID types
    let event_id = Id::<Event>::from(ulid);
    let checkpoint_id = Id::<TestCheckpoint>::from(ulid);

    // Even though they came from the same ULID, they have different types
    assert_eq!(event_id.to_string(), checkpoint_id.to_string()); // Same string representation
                                                                 // But different types: assert_ne!(event_id, checkpoint_id); // Would not compile

    // Convert back to ULID
    let ulid_from_event: Ulid = event_id.into();
    let ulid_from_checkpoint: Ulid = checkpoint_id.into();

    // Should recover original ULID
    assert_eq!(ulid, ulid_from_event);
    assert_eq!(ulid, ulid_from_checkpoint);

    Ok(())
}

#[sinex_test]
async fn test_type_safety_boundary_conditions(ctx: TestContext) -> Result<()> {
    // Test type safety at system boundaries

    // Empty but valid domain strings
    let minimal_event = ctx
        .create_test_event(
            "a",       // Minimal valid source
            "b",       // Minimal valid type
            json!({}), // Minimal valid payload
        )
        .await?;

    assert_eq!(minimal_event.source.as_str(), "a");
    assert_eq!(minimal_event.event_type.as_str(), "b");
    assert_eq!(minimal_event.payload, json!({}));

    // Verify it round-trips through database
    let retrieved = ctx
        .pool
        .events()
        .get_by_id(minimal_event.id.unwrap())
        .await?
        .expect("Minimal event should exist");

    assert_eq!(retrieved.source.as_str(), "a");
    assert_eq!(retrieved.event_type.as_str(), "b");
    assert_eq!(retrieved.payload, json!({}));

    Ok(())
}
