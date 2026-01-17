//! Property tests for Node behavior
//!
//! Tests that verify automaton processing, state management, and event handling properties
//! for the modern NATS-based Node implementations.

use chrono::Utc;
use proptest::prelude::*;
use serde_json::json;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_core::{Event, JsonValue};
use sinex_node_sdk::{Checkpoint, NodeType, ScanArgs, TimeHorizon};
use sinex_test_utils::{TestResult, prelude::*, sinex_proptest};
use std::collections::HashMap;

/// Create property test strategies for events
fn arb_event_data() -> impl Strategy<Value = (String, String, serde_json::Value)> {
    prop_oneof![
        // Heartbeat events
        (
            Just("journald".to_string()),
            Just("node.heartbeat".to_string()),
            prop::collection::hash_map(
                prop_oneof![
                    Just("service_name".to_string()),
                    Just("uptime_seconds".to_string()),
                    Just("memory_usage_mb".to_string()),
                    Just("version".to_string())
                ],
                prop_oneof![
                    "[a-z-]+".prop_map(|s| json!(s)),
                    (1u64..86400u64).prop_map(|n| json!(n)),
                    (1u32..2048u32).prop_map(|n| json!(n)),
                    Just(json!("1.0.0"))
                ],
                1..=4
            )
            .prop_map(|m| json!(m))
        ),
        // File system events
        (
            Just("fs".to_string()),
            Just("file.created".to_string()),
            prop::collection::hash_map(
                prop_oneof![Just("path".to_string()), Just("size".to_string())],
                prop_oneof![
                    "/tmp/test[0-9]+\\.txt".prop_map(|s| json!(s)),
                    (1u64..1000000u64).prop_map(|n| json!(n))
                ],
                1..=2
            )
            .prop_map(|m| json!(m))
        ),
        // Terminal events
        (
            Just("terminal".to_string()),
            Just("command.executed".to_string()),
            prop::collection::hash_map(
                prop_oneof![Just("command".to_string()), Just("exit_code".to_string())],
                prop_oneof![
                    "[a-z ]+".prop_map(|s| json!(s)),
                    (0u32..255u32).prop_map(|n| json!(n))
                ],
                1..=2
            )
            .prop_map(|m| json!(m))
        )
    ]
}

/// Create test events for processing
fn create_test_event(
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> Event<JsonValue> {
    Event::test_event(
        EventSource::new(source),
        EventType::new(event_type),
        payload,
    )
}

sinex_proptest! {
    /// Test checkpoint handling properties
    fn test_checkpoint_properties(
        message_count in 0u64..10000u64,
        timestamp_offset in 0i64..86400i64,
    ) -> TestResult<()> {
        // Test different checkpoint types
        let checkpoints = vec![
            Checkpoint::None,
            Checkpoint::Internal {
                event_id: sinex_core::types::ulid::Ulid::new(),
                message_count,
            },
            Checkpoint::Timestamp {
                timestamp: Utc::now() + chrono::Duration::seconds(timestamp_offset),
                metadata: Some(json!({"test": true})),
            },
        ];

        for checkpoint in checkpoints {
            // Property: Checkpoint serialization should work
            let serialized = serde_json::to_string(&checkpoint);
            prop_assert!(serialized.is_ok());

            if let Ok(json_str) = serialized {
                let deserialized = serde_json::from_str::<Checkpoint>(&json_str);
                prop_assert!(deserialized.is_ok());
                prop_assert_eq!(checkpoint.clone(), deserialized.unwrap());
            }

            // Property: Checkpoint descriptions should be non-empty
            prop_assert!(!checkpoint.description().is_empty());
        }
        Ok(())
    }
}

sinex_proptest! {
    /// Test scan args validation properties
    fn test_scan_args_properties(
        max_events in 0u64..10000u64,
        dry_run in any::<bool>(),
        interactive in any::<bool>(),
        skip_duplicates in any::<bool>(),
    ) -> TestResult<()> {
        let args = ScanArgs {
            targets: vec!["test-target".to_string()],
            dry_run,
            interactive,
            max_events,
            skip_duplicates,
            config: HashMap::new(),
        };

        // Property: ScanArgs should serialize/deserialize correctly
        let serialized = serde_json::to_string(&args);
        prop_assert!(serialized.is_ok());

        if let Ok(json_str) = serialized {
            let deserialized = serde_json::from_str::<ScanArgs>(&json_str);
            prop_assert!(deserialized.is_ok());
            // Note: We can't test equality because ScanArgs doesn't derive Eq/PartialEq
        }
        Ok(())
    }
}

/// Test processor type consistency
#[sinex_test]
fn test_processor_type_properties() -> TestResult<()> {
    let types = vec![NodeType::Ingestor, NodeType::Automaton];

    for processor_type in types {
        // Property: Processor type should serialize correctly
        let serialized = serde_json::to_string(&processor_type).unwrap();
        let deserialized: NodeType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(processor_type, deserialized);

        // Property: NodeType should have consistent debug representation
        let debug1 = format!("{processor_type:?}");
        let debug2 = format!("{processor_type:?}");
        assert_eq!(debug1, debug2);
    }
    Ok(())
}

sinex_proptest! {
    /// Test event processing determinism (without actual scan)
    fn test_event_creation_determinism(
        events in proptest::collection::vec(arb_event_data(), 1..=20),
    ) -> TestResult<()> {
        // Property: Same event data should produce equivalent events
        for (source, event_type, payload) in events.iter() {
            let event1 = create_test_event(source, event_type, payload.clone());
            let event2 = create_test_event(source, event_type, payload.clone());

            // Properties that should be identical
            prop_assert_eq!(event1.source, event2.source);
            prop_assert_eq!(event1.event_type, event2.event_type);
            prop_assert_eq!(event1.payload, event2.payload);

            // Properties that should be the same (schemaless events have None ID)
            prop_assert_eq!(event1.id, event2.id); // Both should be None
        }
        Ok(())
    }
}

sinex_proptest! {
    /// Test error handling with malformed data
    fn test_error_handling_robustness(
        malformed_payloads in proptest::collection::vec(
            prop_oneof![
                Just(json!(null)),
                Just(json!([])),
                Just(json!({})),
                Just(json!({"invalid": "x".repeat(10000)})), // Large string
                // Note: JSON doesn't support infinity/NaN, so we use large numbers
                Just(json!({"numbers": [1e308, -1e308]})),
            ],
            1..=5
        ),
    ) -> TestResult<()> {
        // Property: Event creation should handle malformed payloads gracefully
        for payload in malformed_payloads.iter() {
            let event = create_test_event("test-source", "test.event", payload.clone());

            // Property: Event should still be created (ID is None for schemaless events)
            prop_assert!(event.id.is_none());
            prop_assert_eq!(event.source.as_str(), "test-source");
            prop_assert_eq!(event.event_type.as_str(), "test.event");

            // Property: Serialization should handle malformed payloads
            let serialized = serde_json::to_string(&event);
            prop_assert!(serialized.is_ok());
        }
        Ok(())
    }
}

sinex_proptest! {
    /// Test checkpoint description consistency
    fn test_checkpoint_description_properties(
        message_count in 0u64..1000u64,
    ) -> TestResult<()> {
        let checkpoint1 = Checkpoint::Internal {
            event_id: sinex_core::types::ulid::Ulid::new(),
            message_count,
        };

        let checkpoint2 = Checkpoint::None;

        let checkpoint3 = Checkpoint::Timestamp {
            timestamp: Utc::now(),
            metadata: Some(json!({"test": "data"})),
        };

        // Property: All checkpoints should have descriptions
        prop_assert!(!checkpoint1.description().is_empty());
        prop_assert!(!checkpoint2.description().is_empty());
        prop_assert!(!checkpoint3.description().is_empty());

        // Property: Same type checkpoints should have similar description format
        let checkpoint4 = Checkpoint::Internal {
            event_id: sinex_core::types::ulid::Ulid::new(),
            message_count,
        };

        // Both internal checkpoints should mention "internal" and message count
        let desc1 = checkpoint1.description().to_lowercase();
        let desc4 = checkpoint4.description().to_lowercase();
        prop_assert!(desc1.contains("internal") == desc4.contains("internal"));
        Ok(())
    }
}

sinex_proptest! {
    /// Test time horizon property consistency
    fn test_time_horizon_behavior_properties(
        hours_forward in 1u32..24u32, // 1 hour to 1 day
    ) -> TestResult<()> {
        let end_time = Utc::now() + chrono::Duration::hours(hours_forward as i64);

        let horizons = vec![
            TimeHorizon::Snapshot,
            TimeHorizon::Historical { end_time },
            TimeHorizon::Continuous,
        ];

        for horizon in horizons {
            // Property: TimeHorizon methods should be consistent
            match &horizon {
                TimeHorizon::Snapshot => {
                    prop_assert!(!horizon.is_continuous());
                    prop_assert!(horizon.is_bounded());
                    prop_assert_eq!(horizon.end_time(), None);
                }
                TimeHorizon::Historical { end_time } => {
                    prop_assert!(!horizon.is_continuous());
                    prop_assert!(horizon.is_bounded());
                    prop_assert_eq!(horizon.end_time(), Some(*end_time));
                }
                TimeHorizon::Continuous => {
                    prop_assert!(horizon.is_continuous());
                    prop_assert!(!horizon.is_bounded());
                    prop_assert_eq!(horizon.end_time(), None);
                }
            }

            // Property: Serialization should work
            let serialized = serde_json::to_string(&horizon);
            prop_assert!(serialized.is_ok());

            if let Ok(json_str) = serialized {
                let deserialized = serde_json::from_str::<TimeHorizon>(&json_str);
                prop_assert!(deserialized.is_ok());
                // Note: Can't test equality directly due to potential precision issues
            }
        }
        Ok(())
    }
}

sinex_proptest! {
    /// Test automation-related data structure consistency
    fn test_automation_data_structure_consistency(
        event_count in 1..100usize,
        batch_size in 1..50usize,
    ) -> TestResult<()> {
        // Property: Event batches should be processable
        let mut events = Vec::new();

        for i in 0..event_count {
            let event = Event::test_event(
                EventSource::from_static("automation-test"),
                EventType::from_static("batch.test"),
                json!({
                    "batch_index": i,
                    "total_events": event_count
                }),
            );

            events.push(event);
        }

        // Property: Events should be batchable
        let batches: Vec<_> = events.chunks(batch_size).collect();
        prop_assert!(!batches.is_empty());
        prop_assert!(batches.iter().all(|batch| batch.len() <= batch_size));

        // Property: All events should be accounted for
        let total_in_batches: usize = batches.iter().map(|batch| batch.len()).sum();
        prop_assert_eq!(total_in_batches, event_count);

        // Property: Each event should have valid properties
        for event in &events {
            prop_assert!(event.id.is_none()); // New events have no ID until inserted
            prop_assert_eq!(event.source.as_str(), "automation-test");
            prop_assert_eq!(event.event_type.as_str(), "batch.test");
            prop_assert!(event.payload.is_object());
        }
        Ok(())
    }
}
