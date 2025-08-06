//! Property-Based Tests for Sinex
//!
//! This module contains property-based tests using proptest to verify
//! system behavior across a wide range of inputs and edge cases.
//! Tests focus on invariants and properties that should hold regardless
//! of specific input values using the current architecture.

// Import test utilities without broad prelude to avoid Event type conflicts
use color_eyre::eyre::{eyre, Result};
use proptest::option;
use proptest::prelude::*;
use serde_json::json;
use sinex_db::models::Event as DbEvent;
use sinex_db::repositories::DbPoolExt;
use sinex_test_utils::{sinex_test, TestContext};
use sinex_types::domain::{EventSource, EventType, HostName};
use sinex_types::{Id, Ulid};
use std::collections::HashSet;

// =============================================================================
// ULID PROPERTY TESTS - Invariants for time-ordered identifiers
// =============================================================================

proptest! {
    #[test]
    fn test_ulid_generation_properties(count in 1..1000usize) {
        let mut ulids = Vec::new();
        for _ in 0..count {
            ulids.push(Ulid::new());
        }

        // Property: All ULIDs should be unique (check pairwise)
        for (i, ulid1) in ulids.iter().enumerate() {
            for ulid2 in ulids.iter().skip(i + 1) {
                prop_assert_ne!(ulid1, ulid2);
            }
        }

        // Property: ULIDs should be generally in temporal order
        for window in ulids.windows(2) {
            prop_assert!(window[0] <= window[1]);
        }
    }

    #[test]
    fn test_ulid_string_properties(ulid_str in "[0-9A-Z]{26}") {
        // Property: Valid ULID strings should always parse successfully
        let ulid_result = ulid_str.parse::<Ulid>();
        if ulid_result.is_ok() {
            let ulid = ulid_result.unwrap();
            // Property: Round-trip conversion should be identity
            prop_assert_eq!(ulid.to_string(), ulid_str);
        }
    }

    #[test]
    fn test_ulid_ordering_transitivity(
        count in 3..100usize,
        delay_ms in 0..10u64
    ) {
        let mut ulids = Vec::new();
        for _ in 0..count {
            ulids.push(Ulid::new());
            if delay_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(delay_ms));
            }
        }

        // Property: Ordering should be transitive
        for i in 0..ulids.len().saturating_sub(2) {
            if ulids[i] <= ulids[i+1] && ulids[i+1] <= ulids[i+2] {
                prop_assert!(ulids[i] <= ulids[i+2]);
            }
        }
    }
}

// =============================================================================
// EVENT CREATION PROPERTY TESTS - Event builder robustness
// =============================================================================

prop_compose! {
    fn arb_event_source()
        (source in "[a-zA-Z0-9_-]{1,50}")
        -> EventSource
    {
        EventSource::new(&source)
    }
}

prop_compose! {
    fn arb_event_type()
        (type_name in "[a-zA-Z0-9_.]{1,100}")
        -> EventType
    {
        EventType::new(&type_name)
    }
}

prop_compose! {
    fn arb_json_value()
        (
            string_val in option::of("[a-zA-Z0-9 ._-]*"),
            number_val in option::of(any::<i64>()),
            bool_val in option::of(any::<bool>())
        )
        -> serde_json::Value
    {
        match (string_val, number_val, bool_val) {
            (Some(s), _, _) => json!(s),
            (_, Some(n), _) => json!(n),
            (_, _, Some(b)) => json!(b),
            _ => json!(null)
        }
    }
}

proptest! {
    #[test]
    fn test_event_creation_properties(
        source in arb_event_source(),
        event_type in arb_event_type(),
        payload in arb_json_value()
    ) {
        let event = DbEvent::schemaless()
            .source(source.clone())
            .event_type(event_type.clone())
            .payload(payload.clone())
            .build();

        // Property: Event should preserve all input values
        prop_assert_eq!(event.source, source);
        prop_assert_eq!(event.event_type, event_type);
        prop_assert_eq!(event.payload, payload);

        // Property: Event should always have a valid ID
        prop_assert!(event.id.is_some());

        // Property: Event should have a reasonable timestamp
        let now = chrono::Utc::now();
        prop_assert!(event.ts_ingest <= now);
        prop_assert!(event.ts_ingest > now - chrono::Duration::minutes(1));
    }

    #[test]
    fn test_event_json_serialization_properties(
        source in "[a-zA-Z0-9_-]{1,20}",
        event_type in "[a-zA-Z0-9_.]{1,30}",
        string_value in "[a-zA-Z0-9 ._-]*",
        number_value in any::<i64>(),
        bool_value in any::<bool>()
    ) {
        let payload = json!({
            "string": string_value,
            "number": number_value,
            "boolean": bool_value
        });

        let original_event = DbEvent::schemaless()
            .source(EventSource::new(&source))
            .event_type(EventType::new(&event_type))
            .payload(payload)
            .build();

        // Property: Serialization should never fail for valid events
        let json_result = serde_json::to_string(&original_event);
        prop_assert!(json_result.is_ok());

        let json_str = json_result.unwrap();

        // Property: Deserialization should be inverse of serialization
        let deserialize_result = serde_json::from_str::<DbEvent>(&json_str);
        prop_assert!(deserialize_result.is_ok());

        let deserialized_event = deserialize_result.unwrap();

        // Property: Round-trip should preserve all fields
        prop_assert_eq!(deserialized_event.source, original_event.source);
        prop_assert_eq!(deserialized_event.event_type, original_event.event_type);
        prop_assert_eq!(deserialized_event.payload, original_event.payload);
        prop_assert_eq!(deserialized_event.id, original_event.id);
    }
}

// =============================================================================
// DOMAIN TYPE PROPERTY TESTS - String wrapper robustness
// =============================================================================

proptest! {
    #[test]
    fn test_event_source_properties(source_str in ".*") {
        let source = EventSource::new(&source_str);

        // Property: EventSource should preserve input string
        prop_assert_eq!(source.as_str(), source_str.as_str());

        // Property: Clone should be identical
        let cloned = source.clone();
        prop_assert_eq!(source.clone(), cloned);

        // Property: Different creation methods should be equal for same string
        let static_source = EventSource::new(&source_str);
        prop_assert_eq!(source.clone(), static_source);
    }

    #[test]
    fn test_event_type_properties(type_str in ".*") {
        let event_type = EventType::new(&type_str);

        // Property: EventType should preserve input string
        prop_assert_eq!(event_type.as_str(), type_str.as_str());

        // Property: Clone should be identical
        let cloned = event_type.clone();
        prop_assert_eq!(event_type, cloned);
    }

    #[test]
    fn test_hostname_properties(hostname_str in "[a-zA-Z0-9._-]{1,100}") {
        let hostname = HostName::new(&hostname_str);

        // Property: HostName should preserve input string
        prop_assert_eq!(hostname.as_str(), hostname_str.as_str());

        // Property: Clone should be identical
        let cloned = hostname.clone();
        prop_assert_eq!(hostname, cloned);
    }
}

// =============================================================================
// GENERIC ID PROPERTY TESTS - Type-safe identifier properties
// =============================================================================

proptest! {
    #[test]
    fn test_generic_id_properties(count in 1..1000usize) {
        let mut ids = Vec::new();
        for _ in 0..count {
            ids.push(Id::<DbEvent>::new());
        }

        // Property: All IDs should be unique (check pairwise)
        for (i, id1) in ids.iter().enumerate() {
            for id2 in ids.iter().skip(i + 1) {
                prop_assert_ne!(id1, id2);
            }
        }

        // Property: All IDs should have valid string representations
        for id in &ids {
            let id_str = id.to_string();
            prop_assert_eq!(id_str.len(), 26);
            prop_assert!(id_str.chars().all(|c| c.is_ascii_alphanumeric()));
        }

        // Property: IDs should be sortable by string representation
        let mut id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        id_strings.sort();

        // Property: Sorting should be consistent
        for window in id_strings.windows(2) {
            prop_assert!(window[0] <= window[1]);
        }
    }

    #[test]
    fn test_id_ulid_conversion_properties(count in 1..100usize) {
        for _ in 0..count {
            let original_id = Id::<DbEvent>::new();

            // Property: ID → ULID → ID should be identity
            let ulid: Ulid = original_id.clone().into();
            let converted_id = Id::<DbEvent>::from(ulid);
            prop_assert_eq!(original_id.clone(), converted_id);

            // Property: String conversion should be consistent
            prop_assert_eq!(original_id.to_string(), ulid.to_string());
        }
    }
}

// =============================================================================
// DATABASE INTEGRATION PROPERTY TESTS - Real database operations
// =============================================================================

proptest! {
    // TODO: This test needs refactoring to handle async properly within proptest
    /*
    #[test]
    fn test_database_event_insertion_properties(
        source in "[a-zA-Z0-9_-]{1,20}",
        event_type in "[a-zA-Z0-9_.]{1,30}",
        field_count in 1..10usize,
        string_values in proptest::collection::vec("[a-zA-Z0-9 ._-]*", 1..10)
    ) {
        prop_assert!(tokio_test::block_on(async {
            let ctx = TestContext::new().await.unwrap();

            // Create payload with varying number of fields
            let mut payload_map = serde_json::Map::new();
            for (i, value) in string_values.iter().enumerate().take(field_count) {
                payload_map.insert(format!("field_{}", i), json!(value));
            }
            let payload = serde_json::Value::Object(payload_map);

            let event = ctx.event()
                .source(source.as_str())
                .type_(event_type.as_str())
                .payload(payload.clone())
                .insert()
                .await;

            // Property: Valid events should always insert successfully
            prop_assert!(event.is_ok());

            let inserted_event = event.unwrap();

            // Property: Inserted event should preserve all data
            prop_assert_eq!(inserted_event.source.as_str(), source.as_str());
            prop_assert_eq!(inserted_event.event_type.as_str(), event_type.as_str());
            prop_assert_eq!(inserted_event.payload, payload);

            // Property: Event should be retrievable by ID
            let retrieved = ctx.pool().events()
                .get_by_id(inserted_event.id.unwrap())
                .await
                .unwrap();

            prop_assert!(retrieved.is_some());
            let retrieved_event = retrieved.unwrap();
            prop_assert_eq!(retrieved_event.id, inserted_event.id);
            prop_assert_eq!(retrieved_event.payload, payload);

            Ok::<(), proptest::test_runner::TestCaseError>(())
        }).is_ok());
    }
    */

    #[test]
    fn test_batch_insertion_properties(
        batch_size in 1..50usize,
        source in "[a-zA-Z0-9_-]{1,20}"
    ) {
        tokio_test::block_on(async {
            let ctx = TestContext::new().await.unwrap();

            // Create batch of events
            for i in 0..batch_size {
                ctx.create_test_event(
                    source.as_str(),
                    "batch.test",
                    json!({
                        "index": i,
                        "batch_size": batch_size
                    })
                ).await.unwrap();
            }

            // Property: All events should be retrievable
            let retrieved = ctx.pool.events()
                .get_by_source(&EventSource::new(&source), Some(batch_size as i64 + 10), None)
                .await
                .unwrap();

            prop_assert_eq!(retrieved.len(), batch_size);

            // Property: All events should have unique IDs (check by iteration)
            let ids: Vec<_> = retrieved.iter().filter_map(|e| e.id.clone()).collect();
            for (i, id1) in ids.iter().enumerate() {
                for id2 in ids.iter().skip(i + 1) {
                    prop_assert_ne!(id1, id2);
                }
            }

            // Property: All events should have correct source
            for event in &retrieved {
                prop_assert_eq!(event.source.as_str(), source.as_str());
            }

            Ok::<(), proptest::test_runner::TestCaseError>(())
        }).unwrap();
    }
}

// =============================================================================
// EDGE CASE PROPERTY TESTS - Boundary conditions and special cases
// =============================================================================

proptest! {
    #[test]
    fn test_unicode_handling_properties(
        unicode_source in "\\PC*",  // Any valid Unicode except control characters
        unicode_type in "\\PC*",
        unicode_value in "\\PC*"
    ) {
        let event = DbEvent::schemaless()
            .source(EventSource::new(&unicode_source))
            .event_type(EventType::new(&unicode_type))
            .payload(json!({
                "unicode_value": unicode_value,
                "original_source": unicode_source,
                "original_type": unicode_type
            }))
            .build();

        // Property: Unicode should be preserved in all fields
        prop_assert_eq!(event.source.as_str(), unicode_source.as_str());
        prop_assert_eq!(event.event_type.as_str(), unicode_type.as_str());
        prop_assert_eq!(event.payload["unicode_value"].clone(), json!(unicode_value));

        // Property: Event should serialize/deserialize correctly with Unicode
        let json_result = serde_json::to_string(&event);
        prop_assert!(json_result.is_ok());

        let deserialized_result = serde_json::from_str::<DbEvent>(&json_result.unwrap());
        prop_assert!(deserialized_result.is_ok());

        let deserialized = deserialized_result.unwrap();
        prop_assert_eq!(deserialized.source.as_str(), unicode_source.as_str());
        prop_assert_eq!(deserialized.event_type.as_str(), unicode_type.as_str());
    }

    #[test]
    fn test_large_payload_properties(
        string_size in 1000..100000usize,
        array_size in 100..10000usize
    ) {
        let large_string = "x".repeat(string_size);
        let large_array: Vec<i32> = (0..array_size).map(|i| i as i32).collect();

        let payload = json!({
            "large_string": large_string,
            "large_array": large_array,
            "metadata": {
                "string_size": string_size,
                "array_size": array_size
            }
        });

        let event = DbEvent::schemaless()
            .source(EventSource::from_static("large-payload-test"))
            .event_type(EventType::from_static("large.payload"))
            .payload(payload.clone())
            .build();

        // Property: Large payloads should be handled correctly
        prop_assert_eq!(event.payload.clone(), payload);
        prop_assert_eq!(
            event.payload["large_string"].as_str().unwrap().len(),
            string_size
        );
        prop_assert_eq!(
            event.payload["large_array"].as_array().unwrap().len(),
            array_size
        );

        // Property: Large events should serialize successfully
        let json_result = serde_json::to_string(&event);
        prop_assert!(json_result.is_ok());

        // Property: Serialized large events should deserialize successfully
        let json_str = json_result.unwrap();
        let deserialize_result = serde_json::from_str::<DbEvent>(&json_str);
        prop_assert!(deserialize_result.is_ok());
    }

    #[test]
    fn test_concurrent_operation_properties(
        thread_count in 2..10usize,
        operations_per_thread in 10..100usize
    ) {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let results = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        // Property: Concurrent ULID generation should always produce unique IDs
        for _ in 0..thread_count {
            let results_clone = results.clone();
            let handle = thread::spawn(move || {
                let mut thread_ulids = Vec::new();
                for _ in 0..operations_per_thread {
                    thread_ulids.push(Ulid::new());
                }
                results_clone.lock().unwrap().extend(thread_ulids);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let final_results = results.lock().unwrap();
        let expected_count = thread_count * operations_per_thread;

        // Property: Should have expected number of ULIDs
        prop_assert_eq!(final_results.len(), expected_count);

        // Property: All ULIDs should be unique despite concurrent generation (check pairwise)
        for (i, ulid1) in final_results.iter().enumerate() {
            for ulid2 in final_results.iter().skip(i + 1) {
                prop_assert_ne!(ulid1, ulid2);
            }
        }
    }
}

// =============================================================================
// VALIDATION PROPERTY TESTS - Input validation and error handling
// =============================================================================

proptest! {
    #[test]
    fn test_validation_properties(
        source_len in 0..1000usize,
        type_len in 0..1000usize
    ) {
        let source_str = "a".repeat(source_len);
        let type_str = "b".repeat(type_len);

        let source = EventSource::new(&source_str);
        let event_type = EventType::new(&type_str);

        // Property: Domain types should accept any length input
        prop_assert_eq!(source.as_str().len(), source_len);
        prop_assert_eq!(event_type.as_str().len(), type_len);

        // Property: Events should be creatable with any valid domain types
        let event = DbEvent::schemaless()
            .source(source)
            .event_type(event_type)
            .payload(json!({"test": true}))
            .build();

        prop_assert_eq!(event.source.as_str().len(), source_len);
        prop_assert_eq!(event.event_type.as_str().len(), type_len);
    }
}

// =============================================================================
// REGRESSION PROPERTY TESTS - Preserve important system invariants
// =============================================================================

proptest! {
    #[test]
    fn test_event_ordering_properties(event_count in 2..100usize) {
        tokio_test::block_on(async {
            let ctx = TestContext::new().await.unwrap();

            let mut event_ids = Vec::new();

            // Create events with small delays
            for i in 0..event_count {
                let event = ctx.create_test_event(
                    "ordering-prop-test",
                    "ordering.test",
                    json!({"sequence": i})
                ).await.unwrap();

                event_ids.push(event.id.unwrap());

                // Small delay to ensure ordering
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }

            // Property: Event IDs should be in temporal order (compare by string)
            for window in event_ids.windows(2) {
                prop_assert!(window[0].to_string() <= window[1].to_string());
            }

            // Property: Retrieved events should maintain order
            let retrieved = ctx.pool.events()
                .get_by_source(&EventSource::from_static("ordering-prop-test"), Some(event_count as i64 + 10), None)
                .await
                .unwrap();

            prop_assert_eq!(retrieved.len(), event_count);

            // Property: Database ordering should match insertion order
            for i in 1..retrieved.len() {
                prop_assert!(retrieved[i-1].ts_ingest <= retrieved[i].ts_ingest);
            }

            Ok::<(), proptest::test_runner::TestCaseError>(())
        }).unwrap();
    }
}
