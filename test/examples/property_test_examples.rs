/// Example property tests demonstrating best practices for property testing in Sinex
///
/// This file shows how to effectively use the property test builders and macros
/// to create comprehensive property-based tests.

use crate::common::prelude::*;
use crate::common::property_builders::*;
use crate::common::property_test_macros::*;
use proptest::prelude::*;
use sinex_ulid::Ulid;
use sinex_db::RawEvent;
use std::collections::HashSet;

// Example 1: Using property builders for event testing
proptest! {
    #[test]
    fn event_builder_generates_valid_events(
        event in arbitrary_event()
    ) {
        // All generated events should be valid
        assert!(!event.source.is_empty(), "Event source should not be empty");
        assert!(!event.event_type.is_empty(), "Event type should not be empty");
        assert_ne!(event.id, Ulid::nil(), "Event ID should not be nil");
        
        // Payload should be valid JSON
        assert!(serde_json::to_string(&event.payload).is_ok(),
                "Event payload should be serializable");
    }
}

// Example 2: Testing event batches with relationships
proptest! {
    #[test]
    fn related_events_maintain_consistency(
        events in related_events_batch()
    ) {
        // Extract paths from all events
        let paths: HashSet<_> = events.iter()
            .filter_map(|e| e.payload.get("path")?.as_str())
            .collect();
        
        // Related events should all reference the same path
        assert_eq!(paths.len(), 1, "Related events should reference same path");
        
        // Events should be in chronological order
        for window in events.windows(2) {
            if let (Some(ts1), Some(ts2)) = (window[0].ts_orig, window[1].ts_orig) {
                assert!(ts1 <= ts2, "Events should be chronologically ordered");
            }
        }
    }
}

// Example 3: Using the sinex_proptest_sync macro
sinex_proptest_sync! {
    fn ulid_generation_properties(
        count in 1usize..100
    ) {
        let mut ulids = Vec::with_capacity(count);
        
        // Generate ULIDs
        for _ in 0..count {
            ulids.push(Ulid::new());
        }
        
        // Check uniqueness
        let unique: HashSet<_> = ulids.iter().cloned().collect();
        assert_eq!(ulids.len(), unique.len(), "All ULIDs should be unique");
        
        // Check ordering
        for i in 1..ulids.len() {
            assert!(ulids[i-1] <= ulids[i], "ULIDs should be monotonically increasing");
        }
    }
}

// Example 4: Using property_invariant macro
property_invariant! {
    name: event_serialization_roundtrip,
    given: (event: RawEvent),
    invariant: |event| {
        // Serialize to JSON
        let json = serde_json::to_string(&event).expect("Should serialize");
        
        // Deserialize back
        let restored: RawEvent = serde_json::from_str(&json).expect("Should deserialize");
        
        // Core fields should match
        assert_eq!(event.id, restored.id);
        assert_eq!(event.event_type, restored.event_type);
        assert_eq!(event.source, restored.source);
    }
}

// Example 5: Testing with specific event types
proptest! {
    #[test]
    fn filesystem_events_have_required_fields(
        event in filesystem_event()
    ) {
        // All filesystem events should have a path
        assert!(event.payload.get("path").is_some(),
                "Filesystem event should have path field");
        
        // Event type should be filesystem-related
        assert!(event.event_type.starts_with("file.") || event.event_type.starts_with("dir."),
                "Should be a filesystem event type");
        
        // Path should be absolute
        if let Some(path) = event.payload.get("path").and_then(|p| p.as_str()) {
            assert!(path.starts_with('/'), "Path should be absolute");
        }
    }
}

// Example 6: Testing automaton behavior with property tests
proptest! {
    #[test]
    fn heartbeat_events_track_progress(
        events in proptest::collection::vec(heartbeat_event(), 1..10)
    ) {
        // Heartbeat events should show increasing processed counts
        let mut automaton_progress: HashMap<String, Vec<u64>> = HashMap::new();
        
        for event in events {
            if let Some(automaton_name) = event.payload.get("automaton_name").and_then(|n| n.as_str()) {
                if let Some(processed) = event.payload.get("events_processed").and_then(|p| p.as_u64()) {
                    automaton_progress.entry(automaton_name.to_string())
                        .or_default()
                        .push(processed);
                }
            }
        }
        
        // Progress should be non-decreasing for each automaton
        for (name, counts) in automaton_progress {
            for window in counts.windows(2) {
                assert!(window[0] <= window[1],
                        "Automaton {} progress should not decrease", name);
            }
        }
    }
}

// Example 7: Using configured_proptest for performance-sensitive tests
configured_proptest! {
    #[cases(1000)]
    #[max_shrink_iters(10)]
    fn large_payload_handling(
        size in 100usize..10_000usize
    ) {
        // Generate event with large payload
        let large_data = "x".repeat(size);
        let event = TestEventBuilder::new()
            .source("test")
            .event_type("test.large_payload")
            .payload(json!({
                "data": large_data,
                "size": size
            }))
            .build();
        
        // Should be serializable
        let serialized = serde_json::to_string(&event);
        assert!(serialized.is_ok(), "Large payload should be serializable");
        
        // Size should be reasonable
        let json_size = serialized.unwrap().len();
        assert!(json_size < size * 2, "JSON overhead should be reasonable");
    }
}

// Example 8: Testing time-based properties
proptest! {
    #[test]
    fn time_ordered_events_maintain_causality(
        batch in time_ordered_batch()
    ) {
        // Events should be properly ordered
        for i in 1..batch.len() {
            let prev = &batch[i-1];
            let curr = &batch[i];
            
            if let (Some(ts1), Some(ts2)) = (prev.ts_orig, curr.ts_orig) {
                assert!(ts1 <= ts2, "Events should be time-ordered");
                
                // If same timestamp, IDs should still be ordered (ULID property)
                if ts1 == ts2 {
                    assert!(prev.id < curr.id, "Same-time events ordered by ULID");
                }
            }
        }
    }
}

// Example 9: Property suite for comprehensive event validation
property_suite! {
    name: event_validation_suite,
    given: arbitrary_event(),
    properties: {
        has_valid_ulid: |event| {
            assert_ne!(event.id, Ulid::nil());
            assert_eq!(event.id.to_string().len(), 26);
        },
        has_valid_source: |event| {
            assert!(!event.source.is_empty());
            assert!(event.source.chars().all(|c| c.is_ascii_graphic()));
        },
        has_valid_type: |event| {
            assert!(!event.event_type.is_empty());
            assert!(event.event_type.contains('.') || event.event_type == "test");
        },
        has_timestamp: |event| {
            assert!(event.ts_orig.is_some() || event.ts_ingest.is_some());
        },
        payload_is_object: |event| {
            assert!(event.payload.is_object() || event.payload.is_null());
        }
    }
}

// Example 10: Differential testing between implementations
#[test]
fn test_event_creation_methods() {
    use proptest::test_runner::TestRunner;
    use proptest::strategy::Strategy;
    
    let mut runner = TestRunner::default();
    
    // Compare different ways of creating events
    let strategy = (event_sources(), event_types(), event_payloads());
    
    runner.run(&strategy, |(source, event_type, payload)| {
        // Method 1: Using EventFactory
        let factory = sinex_events::EventFactory::new(source);
        let event1 = factory.create_event(&event_type, payload.clone());
        
        // Method 2: Using TestEventBuilder
        let event2 = TestEventBuilder::new()
            .source(source)
            .event_type(&event_type)
            .payload(payload)
            .build();
        
        // Both should produce valid events
        assert_eq!(event1.source, event2.source);
        assert_eq!(event1.event_type, event2.event_type);
        
        Ok(())
    }).unwrap();
}

// Example 11: Stateful property testing for work queues
#[cfg(feature = "stateful_tests")]
stateful_proptest! {
    name: work_queue_operations,
    state: std::collections::VecDeque<RawEvent>,
    operations: [
        enqueue(event: RawEvent) => {
            let old_len = state.len();
            state.push_back(event.clone());
            assert_eq!(state.len(), old_len + 1);
            assert_eq!(state.back(), Some(&event));
        },
        dequeue() => {
            let old_len = state.len();
            let front = state.pop_front();
            if old_len > 0 {
                assert!(front.is_some());
                assert_eq!(state.len(), old_len - 1);
            } else {
                assert!(front.is_none());
            }
        },
        peek() => {
            let front = state.front();
            let len = state.len();
            if len > 0 {
                assert!(front.is_some());
            } else {
                assert!(front.is_none());
            }
            // Peek shouldn't change state
            assert_eq!(state.len(), len);
        }
    ]
}

// Example 12: Regression test for specific failure case
regression_test! {
    name: specific_ulid_parsing_case,
    input: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    test: |ulid_str| {
        let parsed = Ulid::from_string(ulid_str);
        assert!(parsed.is_ok(), "Should parse valid ULID string");
        
        let ulid = parsed.unwrap();
        assert_eq!(ulid.to_string(), ulid_str, "Roundtrip should preserve string");
    }
}

#[cfg(test)]
mod advanced_examples {
    use super::*;
    
    // Example 13: Testing complex invariants
    proptest! {
        #[test]
        fn checkpoint_consistency_across_restarts(
            initial_checkpoint in arbitrary_checkpoint(),
            events in arbitrary_event_batch()
        ) {
            // Simulate processing with checkpoints
            let mut checkpoint = initial_checkpoint;
            let mut processed_ids = Vec::new();
            
            for event in events {
                processed_ids.push(event.id);
                
                // Update checkpoint based on type
                checkpoint = match checkpoint {
                    Checkpoint::None => Checkpoint::Database { event_id: event.id },
                    Checkpoint::Database { .. } => Checkpoint::Database { event_id: event.id },
                    other => other, // Keep other types unchanged
                };
            }
            
            // Verify checkpoint reflects processing
            match checkpoint {
                Checkpoint::Database { event_id } => {
                    assert!(processed_ids.is_empty() || processed_ids.contains(&event_id),
                            "Checkpoint should reference processed event");
                }
                _ => {} // Other checkpoint types have different semantics
            }
        }
    }
    
    // Example 14: Testing edge cases with adversarial inputs
    proptest! {
        #[test]
        fn handle_extreme_payloads(
            payload_type in prop_oneof![
                Just("empty"),
                Just("massive"),
                Just("deeply_nested"),
                Just("unicode_heavy"),
                Just("null_riddled")
            ]
        ) {
            let payload = match payload_type.as_str() {
                "empty" => json!({}),
                "massive" => json!({"data": "x".repeat(1_000_000)}),
                "deeply_nested" => create_deeply_nested_json(50),
                "unicode_heavy" => json!({"text": "🦀📊🔍" .repeat(1000)}),
                "null_riddled" => json!({"a": null, "b": [null, null], "c": {"d": null}}),
                _ => json!(null)
            };
            
            let event = TestEventBuilder::new()
                .source("test")
                .event_type("test.extreme")
                .payload(payload)
                .build();
            
            // Should handle without panic
            let _ = serde_json::to_string(&event);
            let _ = event.payload.to_string();
        }
    }
    
    fn create_deeply_nested_json(depth: usize) -> serde_json::Value {
        let mut current = json!("leaf");
        for i in 0..depth {
            current = json!({"level": i, "nested": current});
        }
        current
    }
}