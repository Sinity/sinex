//! Property-Based Tests for Sinex
//!
//! This module contains property-based tests using proptest to verify
//! system behavior across a wide range of inputs and edge cases.
//! Tests focus on invariants and properties that should hold regardless
//! of specific input values using the current architecture.

// Import test utilities without broad prelude to avoid Event type conflicts
use proptest::option;
use proptest::prelude::*;
use serde_json::json;
use std::collections::HashSet;
// Using shorter imports from sinex-core's re-exports
use sinex_core::{Event, EventSource, EventType, HostName, Id, JsonValue, Ulid};
use sinex_test_utils::{sinex_proptest, test_event};

// =============================================================================
// ULID PROPERTY TESTS - Invariants for time-ordered identifiers
// =============================================================================

sinex_proptest! {
    fn test_ulid_generation_properties(count: usize in 1..1000usize) -> TestResult<()> {
        let mut ulids = Vec::with_capacity(count);
        for _ in 0..count {
            ulids.push(Ulid::new());
        }

        let unique: HashSet<_> = ulids.iter().cloned().collect();
        prop_assert_eq!(unique.len(), ulids.len());

        if let (Some(first), Some(last)) = (ulids.first(), ulids.last()) {
            prop_assert!(first.timestamp() <= last.timestamp());
        }
        Ok(())
    }

    fn test_ulid_string_properties(
        bytes: [u8; 16] in prop::array::uniform16(any::<u8>())
    ) -> TestResult<()> {
        let ulid = Ulid::from_bytes(bytes).expect("Raw bytes should always form a ULID");
        let ulid_str = ulid.to_string();
        let parsed = ulid_str
            .parse::<Ulid>()
            .expect("String produced from ULID should parse");
        prop_assert_eq!(parsed, ulid);
        Ok(())
    }

    fn test_ulid_ordering_transitivity(
        count: usize in 3..100usize,
        delay_ms: u64 in 0..10u64
    ) -> TestResult<()> {
        let mut ulids = Vec::new();
        for _ in 0..count {
            ulids.push(Ulid::new());
            if delay_ms > 0 {
                for _ in 0..delay_ms {
                    std::thread::yield_now();
                }
            }
        }

        for i in 0..ulids.len().saturating_sub(2) {
            if ulids[i] <= ulids[i + 1] && ulids[i + 1] <= ulids[i + 2] {
                prop_assert!(ulids[i] <= ulids[i + 2]);
            }
        }
        Ok(())
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

sinex_proptest! {
    fn test_event_creation_properties(
        source: EventSource in arb_event_source(),
        event_type: EventType in arb_event_type(),
        payload: JsonValue in arb_json_value()
    ) -> TestResult<()> {
        let mut event = test_event(
            source.clone(),
            event_type.clone(),
            payload.clone(),
        );
        event.id = Some(Id::from_ulid(Ulid::new()));

        prop_assert_eq!(event.source, source);
        prop_assert_eq!(event.event_type, event_type);
        prop_assert_eq!(event.payload, payload);

        let now = chrono::Utc::now();
        let ts = event.id.as_ref().unwrap().as_ulid().timestamp();
        prop_assert!(ts <= now);
        prop_assert!(ts > now - chrono::Duration::minutes(1));
        Ok(())
    }

    fn test_event_json_serialization_properties(
        source: String in "[a-zA-Z0-9_-]{1,20}",
        event_type: String in "[a-zA-Z0-9_.]{1,30}",
        string_value: String in "[a-zA-Z0-9 ._-]*",
        number_value: i64 in any::<i64>(),
        bool_value: bool in any::<bool>()
    ) -> TestResult<()> {
        let payload = json!({
            "string": string_value,
            "number": number_value,
            "boolean": bool_value
        });

        let original_event = test_event(
            EventSource::new(&source),
            EventType::new(&event_type),
            payload,
        );

        let json_result = serde_json::to_string(&original_event);
        prop_assert!(json_result.is_ok());

        let json_str = json_result.expect("JSON serialization should succeed for valid event");
        let deserialize_result = serde_json::from_str::<Event<JsonValue>>(&json_str);
        prop_assert!(deserialize_result.is_ok());

        let deserialized_event =
            deserialize_result.expect("JSON deserialization should succeed for valid JSON");

        prop_assert_eq!(deserialized_event.source, original_event.source);
        prop_assert_eq!(deserialized_event.event_type, original_event.event_type);
        prop_assert_eq!(deserialized_event.payload, original_event.payload);
        prop_assert_eq!(deserialized_event.id, original_event.id);
        Ok(())
    }
}

// =============================================================================
// DOMAIN TYPE PROPERTY TESTS - String wrapper robustness
// =============================================================================

sinex_proptest! {
    fn test_event_source_properties(source_str: String in ".*") -> TestResult<()> {
        let source = EventSource::new(&source_str);
        prop_assert_eq!(source.as_str(), source_str.as_str());

        let cloned = source.clone();
        prop_assert_eq!(source.clone(), cloned);

        let static_source = EventSource::new(&source_str);
        prop_assert_eq!(source.clone(), static_source);
        Ok(())
    }

    fn test_event_type_properties(type_str: String in ".*") -> TestResult<()> {
        let event_type = EventType::new(&type_str);
        prop_assert_eq!(event_type.as_str(), type_str.as_str());

        let cloned = event_type.clone();
        prop_assert_eq!(event_type, cloned);
        Ok(())
    }

    fn test_hostname_properties(
        hostname_str: String in "[a-zA-Z0-9._-]{1,100}"
    ) -> TestResult<()> {
        let hostname = HostName::new(&hostname_str);
        prop_assert_eq!(hostname.as_str(), hostname_str.as_str());

        let cloned = hostname.clone();
        prop_assert_eq!(hostname, cloned);
        Ok(())
    }
}

// =============================================================================
// GENERIC ID PROPERTY TESTS - Type-safe identifier properties
// =============================================================================

sinex_proptest! {
    fn test_generic_id_properties(count: usize in 1..1000usize) -> TestResult<()> {
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(Id::<Event<JsonValue>>::new());
        }

        for (i, id1) in ids.iter().enumerate() {
            for id2 in ids.iter().skip(i + 1) {
                prop_assert_ne!(id1, id2);
            }
        }

        for id in &ids {
            let id_str = id.to_string();
            prop_assert_eq!(id_str.len(), 26);
            prop_assert!(id_str.chars().all(|c| c.is_ascii_alphanumeric()));
        }

        let mut id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        id_strings.sort();
        for window in id_strings.windows(2) {
            prop_assert!(window[0] <= window[1]);
        }
        Ok(())
    }

    fn test_id_ulid_conversion_properties(count: usize in 1..100usize) -> TestResult<()> {
        for _ in 0..count {
            let original_id = Id::<Event<JsonValue>>::new();
            let ulid: Ulid = original_id.clone().into();
            let converted_id = Id::<Event<JsonValue>>::from(ulid);
            prop_assert_eq!(original_id.clone(), converted_id);
            prop_assert_eq!(original_id.to_string(), ulid.to_string());
        }
        Ok(())
    }
}

// =============================================================================
// DATABASE INTEGRATION PROPERTY TESTS - Real database operations
// =============================================================================

// NOTE: Batch insertion property test removed due to proptest/async incompatibility.
// Async proptest requires special patterns that are not yet implemented in this test suite.
// Consider using integration_tests.rs for async batch insertion testing instead.

// =============================================================================
// EDGE CASE PROPERTY TESTS - Boundary conditions and special cases
// =============================================================================

sinex_proptest! {
    fn test_unicode_handling_properties(
        unicode_source: String in "\\PC*",
        unicode_type: String in "\\PC*",
        unicode_value: String in "\\PC*"
    ) -> TestResult<()> {
        let event = test_event(
            EventSource::new(&unicode_source),
            EventType::new(&unicode_type),
            json!({
                "unicode_value": unicode_value,
                "original_source": unicode_source,
                "original_type": unicode_type
            }),
        );

        prop_assert_eq!(event.source.as_str(), unicode_source.as_str());
        prop_assert_eq!(event.event_type.as_str(), unicode_type.as_str());
        prop_assert_eq!(event.payload["unicode_value"].clone(), json!(unicode_value));

        let json_result = serde_json::to_string(&event);
        prop_assert!(json_result.is_ok());

        let deserialized_result = serde_json::from_str::<Event<JsonValue>>(&json_result.unwrap());
        prop_assert!(deserialized_result.is_ok());

        let deserialized = deserialized_result.unwrap();
        prop_assert_eq!(deserialized.source.as_str(), unicode_source.as_str());
        prop_assert_eq!(deserialized.event_type.as_str(), unicode_type.as_str());
        Ok(())
    }

    fn test_large_payload_properties(
        string_size: usize in 1000..100000usize,
        array_size: usize in 100..10000usize
    ) -> TestResult<()> {
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

        let event = test_event(
            EventSource::from_static("large-payload-test"),
            EventType::from_static("large.payload"),
            payload.clone(),
        );

        prop_assert_eq!(event.payload.clone(), payload);
        prop_assert_eq!(
            event.payload["large_string"].as_str().unwrap().len(),
            string_size
        );
        prop_assert_eq!(
            event.payload["large_array"].as_array().unwrap().len(),
            array_size
        );

        let json_result = serde_json::to_string(&event);
        prop_assert!(json_result.is_ok());

        let json_str = json_result.unwrap();
        let deserialize_result = serde_json::from_str::<Event<JsonValue>>(&json_str);
        prop_assert!(deserialize_result.is_ok());
        Ok(())
    }

    fn test_concurrent_operation_properties(
        thread_count: usize in 2..10usize,
        operations_per_thread: usize in 10..100usize
    ) -> TestResult<()> {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let results = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for _ in 0..thread_count {
            let results_clone = results.clone();
            let handle = thread::spawn(move || {
                let mut thread_ulids = Vec::new();
                for _ in 0..operations_per_thread {
                    thread_ulids.push(Ulid::new());
                }
                let mut guard = results_clone.lock().expect("mutex poisoned");
                guard.extend(thread_ulids);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let final_results = results.lock().expect("mutex poisoned");
        let expected_count = thread_count * operations_per_thread;

        prop_assert_eq!(final_results.len(), expected_count);
        let unique: HashSet<_> = final_results.iter().cloned().collect();
        prop_assert_eq!(unique.len(), final_results.len());
        Ok(())
    }

    fn test_validation_properties(
        source_len: usize in 0..1000usize,
        type_len: usize in 0..1000usize
    ) -> TestResult<()> {
        let source_str = "a".repeat(source_len);
        let type_str = "b".repeat(type_len);

        let source = EventSource::new(&source_str);
        let event_type = EventType::new(&type_str);

        prop_assert_eq!(source.as_str().len(), source_len);
        prop_assert_eq!(event_type.as_str().len(), type_len);

        let event = test_event(
            source,
            event_type,
            json!({"test": true}),
        );

        prop_assert_eq!(event.source.as_str().len(), source_len);
        prop_assert_eq!(event.event_type.as_str().len(), type_len);
        Ok(())
    }
}

// REGRESSION PROPERTY TESTS - Preserve important system invariants
// =============================================================================

// NOTE: Event ordering property test removed due to proptest/async incompatibility.
// This test concept is better served by the dedicated integration test
// `test_ulid_ordering_integration` in integration_tests.rs.

// =============================================================================
// Include modernized event property tests
// =============================================================================

mod event_property;

// =============================================================================
// Include property test modules
// =============================================================================

#[path = "property"]
mod property_modules {
    pub mod event_model_fuzzing_test;
    pub mod event_validation_property_test;
    pub mod path_sanitization_property_test;
    pub mod schema_property_test;
    pub mod time_range_property_test;
    pub mod ulid_property_test;
}
