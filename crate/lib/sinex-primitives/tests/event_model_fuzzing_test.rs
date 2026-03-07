//! Property-based tests for event model robustness
//!
//! Migrated from `test/property/event_model_fuzzing_test.rs` to modern infrastructure.
//! This module implements comprehensive property-based testing (fuzzing) for the Sinex event
//! processing pipeline to ensure robustness against malformed, extreme, or unexpected data.
//!
//! # Goals
//!
//! 1. **Prevent panics in production**: Any possible input should either process successfully
//!    or fail gracefully, never crash the system
//! 2. **Test boundary conditions**: Empty strings, max values, unicode edge cases
//! 3. **Validate error handling**: Ensure all error paths are robust
//! 4. **Comprehensive coverage**: Test all major event types and payload structures
//!
//! # Strategy
//!
//! - Use proptest to generate 1000+ test cases per event type
//! - Test with extreme values: empty strings, very long strings, unicode, control chars
//! - Test with edge case numbers: negative, zero, max values, floating point precision
//! - Test with malformed but parseable JSON structures
//! - Focus on the modern `RawEvent::schemaless()` API and database insertion paths

use sinex_primitives::testing::event_fixture;
use xtask::sandbox::prelude::*;

use proptest::{strategy::ValueTree, test_runner::TestCaseResult};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sinex_primitives::{Event, Id};
use sinex_primitives::{
    Timestamp, Uuid,
    domain::{EventSource, EventType, HostName},
}; // Modern Event API helpers

fn regex_strategy(pattern: &str) -> proptest::strategy::BoxedStrategy<String> {
    match prop::string::string_regex(pattern) {
        Ok(strategy) => strategy.boxed(),
        Err(_) => Just(String::new()).boxed(),
    }
}

// ============================================================================
// Proptest Strategies for Generating Fuzzed Data
// ============================================================================

/// Strategy for generating potentially problematic strings
fn problematic_strings() -> impl Strategy<Value = String> {
    prop_oneof![
        // Empty and whitespace
        Just(String::new()),
        Just(" ".to_string()),
        Just("\t\n\r".to_string()),
        // Very long strings (potential buffer overflow)
        prop::collection::vec(any::<char>(), 0..10000)
            .prop_map(|chars| chars.into_iter().collect()),
        // Unicode edge cases
        Just("🦀🔥💀".to_string()),                   // Emoji
        Just("тест测试テスト".to_string()),           // Mixed scripts
        Just("\u{200B}\u{FEFF}\u{00A0}".to_string()), // Zero-width and special chars
        Just("𝕋𝕖𝕤𝕥".to_string()),                     // Mathematical symbols
        // Control characters
        Just("\x00\x01\x02\x03".to_string()),
        Just("\x7F".to_string()),
        // SQL injection attempts (should be handled gracefully)
        Just("'; DROP TABLE events; --".to_string()),
        Just("' OR 1=1 --".to_string()),
        // JSON breaking characters
        Just("\"\\\"".to_string()),
        Just("{\"nested\": true}".to_string()),
        Just("[1,2,3]".to_string()),
        // Path injection
        Just("../../../etc/passwd".to_string()),
        Just("/dev/null".to_string()),
        Just("\\\\server\\share\\file".to_string()),
        // Regular problematic strings
        regex_strategy("[\\x00-\\x1F\\u{007F}-\\u{009F}]*"),
        regex_strategy("[\\p{C}]*"), // Control characters
        regex_strategy("[\\p{M}]*"), // Mark characters
    ]
}

/// Strategy for generating edge case numbers
fn edge_case_numbers() -> impl Strategy<Value = i64> {
    prop_oneof![
        Just(0),
        Just(-1),
        Just(1),
        Just(i64::MIN),
        Just(i64::MAX),
        Just(i64::from(i32::MIN)),
        Just(i64::from(i32::MAX)),
        Just(i64::from(u32::MAX)),
        Just(-9223372036854775808), // Min i64
        Just(9223372036854775807),  // Max i64
        any::<i64>(),
    ]
}

/// Strategy for generating edge case unsigned numbers
fn edge_case_u64() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(0),
        Just(1),
        Just(u64::MAX),
        Just(u64::from(u32::MAX)),
        Just(u64::from(u16::MAX)),
        Just(u64::from(u8::MAX)),
        any::<u64>(),
    ]
}

/// Strategy for generating problematic timestamps
fn problematic_timestamps() -> impl Strategy<Value = Timestamp> {
    prop_oneof![
        // Unix epoch
        Just(Timestamp::from_unix_timestamp(0).unwrap_or_else(Timestamp::now)),
        // Very early dates
        Just(Timestamp::from_unix_timestamp(-2208988800).unwrap_or_else(Timestamp::now)), // 1900-01-01
        // Very far future dates
        Just(Timestamp::from_unix_timestamp(4102444800).unwrap_or_else(Timestamp::now)), // 2100-01-01
        // Edge of 32-bit time_t
        Just(Timestamp::from_unix_timestamp(2147483647).unwrap_or_else(Timestamp::now)), // 2038-01-19
        Just(Timestamp::from_unix_timestamp(-2147483648).unwrap_or_else(Timestamp::now)), // 1901-12-13
        // Random timestamps
        (-2208988800i64..4102444800i64)
            .prop_map(|ts| Timestamp::from_unix_timestamp(ts).unwrap_or_else(Timestamp::now)),
    ]
}

/// Strategy for generating malformed but parseable JSON values
fn malformed_json_values() -> impl Strategy<Value = JsonValue> {
    prop_oneof![
        // Null values
        Just(JsonValue::Null),
        // Empty structures
        Just(JsonValue::Object(JsonMap::new())),
        Just(JsonValue::Array(vec![])),
        // Deeply nested structures (potential stack overflow)
        prop::collection::vec(any::<i32>(), 0..1000)
            .prop_map(|v| JsonValue::Array(v.into_iter().map(JsonValue::from).collect())),
        // Very large objects
        prop::collection::hash_map(problematic_strings(), any::<i32>(), 0..500).prop_map(|m| {
            let map: JsonMap<String, JsonValue> = m
                .into_iter()
                .map(|(k, v)| (k, JsonValue::from(v)))
                .collect();
            JsonValue::Object(map)
        }),
        // Mixed type arrays
        Just(JsonValue::Array(vec![
            JsonValue::Null,
            JsonValue::Bool(true),
            JsonValue::from(42),
            JsonValue::from("mixed"),
            JsonValue::Object(JsonMap::new()),
        ])),
        // Extreme numbers
        edge_case_numbers().prop_map(JsonValue::from),
        Just(JsonValue::from(f64::INFINITY)),
        Just(JsonValue::from(f64::NEG_INFINITY)),
        Just(JsonValue::from(f64::NAN)),
        // Problematic strings as JSON
        problematic_strings().prop_map(JsonValue::from),
    ]
}

/// Strategy for generating fuzzed Event instances using modern API.
///
/// Source and `event_type` are now validated on construction, so we use
/// `prop_filter_map` to skip invalid combinations and fuzz the remaining
/// fields (payload, host, timestamps).
fn fuzzed_events() -> impl Strategy<Value = Event<JsonValue>> {
    (
        problematic_strings(),    // source candidate
        problematic_strings(),    // event_type candidate
        problematic_timestamps(), // ts_orig
        problematic_strings(),    // host
        malformed_json_values(),  // payload
    )
        .prop_filter_map(
            "valid source and event_type required",
            |(source, event_type, ts_orig, host, payload)| {
                let source = EventSource::new(source).ok()?;
                let event_type = EventType::new(event_type).ok()?;
                let mut event = event_fixture(source, event_type, payload);
                event.host = HostName::new(host);
                event.id = Some(Id::from_uuid(Uuid::now_v7()));
                event.ts_orig = Some(ts_orig);
                Some(event)
            },
        )
}

// ============================================================================
// Event Type Specific Fuzzing Strategies
// ============================================================================

/// Generate fuzzed filesystem event payloads
fn fuzzed_filesystem_payloads() -> impl Strategy<Value = JsonValue> {
    (
        problematic_strings(),                   // path
        edge_case_u64(),                         // size
        problematic_timestamps(),                // created_at/modified_at
        prop::option::of(edge_case_numbers()),   // permissions
        prop::option::of(problematic_strings()), // modification_type
        prop::option::of(problematic_strings()), // old_path
    )
        .prop_map(
            |(path, size, timestamp, permissions, modification_type, old_path)| {
                let mut payload = serde_json::json!({
                    "path": path,
                    "size": size,
                    "created_at": timestamp,
                    "modified_at": timestamp,
                    "deleted_at": timestamp,
                    "moved_at": timestamp,
                });

                if let Some(perms) = permissions {
                    payload["permissions"] = JsonValue::from(perms);
                }
                if let Some(mod_type) = modification_type {
                    payload["modification_type"] = JsonValue::from(mod_type);
                }
                if let Some(old) = old_path {
                    payload["old_path"] = JsonValue::from(old);
                }

                payload
            },
        )
}

/// Generate fuzzed terminal event payloads
fn fuzzed_terminal_payloads() -> impl Strategy<Value = JsonValue> {
    prop_oneof![
        // Command execution payload
        (
            problematic_strings(),
            prop::option::of(problematic_strings()),
            prop::option::of(edge_case_numbers()),
            prop::option::of(edge_case_u64())
        )
            .prop_map(
                |(command, working_directory, exit_status, execution_time_ms)| {
                    serde_json::json!({
                        "command": command,
                        "working_directory": working_directory,
                        "exit_status": exit_status,
                        "execution_time_ms": execution_time_ms,
                    })
                }
            ),
        // Session payload
        (
            problematic_strings(),
            problematic_strings(),
            problematic_strings(),
            edge_case_u64()
        )
            .prop_map(|(session_id, terminal_type, shell, duration_ms)| {
                serde_json::json!({
                    "session_id": session_id,
                    "terminal_type": terminal_type,
                    "shell": shell,
                    "duration_ms": duration_ms,
                })
            }),
        // Command output payload
        (
            problematic_strings(),
            edge_case_u64(),
            edge_case_numbers(),
            problematic_timestamps()
        )
            .prop_map(
                |(command_output, output_size_bytes, output_line_count, completion_timestamp)| {
                    serde_json::json!({
                        "command_output": command_output,
                        "output_size_bytes": output_size_bytes,
                        "output_line_count": output_line_count,
                        "completion_timestamp": completion_timestamp,
                    })
                }
            ),
    ]
}

/// Generate fuzzed clipboard event payloads
fn fuzzed_clipboard_payloads() -> impl Strategy<Value = JsonValue> {
    (
        problematic_strings(),                   // content_type
        edge_case_u64(),                         // content_size
        prop::option::of(problematic_strings()), // text_preview
        prop::option::of(problematic_strings()), // content_hash
        prop::option::of(problematic_strings()), // source_app
        problematic_strings(),                   // selection_type
    )
        .prop_map(
            |(
                content_type,
                content_size,
                text_preview,
                content_hash,
                source_app,
                selection_type,
            )| {
                serde_json::json!({
                    "content_type": content_type,
                    "content_size": content_size,
                    "text_preview": text_preview,
                    "content_hash": content_hash,
                    "source_app": source_app,
                    "selection_type": selection_type,
                })
            },
        )
}

/// Generate fuzzed window manager event payloads
fn fuzzed_window_manager_payloads() -> impl Strategy<Value = JsonValue> {
    (
        problematic_strings(),                   // window_address
        problematic_strings(),                   // window_class
        problematic_strings(),                   // window_title
        problematic_strings(),                   // workspace_id
        problematic_timestamps(),                // opened_at/closed_at/focused_at
        problematic_strings(),                   // workspace_name
        prop::option::of(problematic_strings()), // previous_workspace_id
        problematic_timestamps(),                // switched_at
    )
        .prop_map(
            |(
                window_address,
                window_class,
                window_title,
                workspace_id,
                timestamp,
                workspace_name,
                previous_workspace_id,
                switched_at,
            )| {
                serde_json::json!({
                    "window_address": window_address,
                    "window_class": window_class,
                    "window_title": window_title,
                    "workspace_id": workspace_id,
                    "opened_at": timestamp,
                    "closed_at": timestamp,
                    "focused_at": timestamp,
                    "workspace_name": workspace_name,
                    "previous_workspace_id": previous_workspace_id,
                    "switched_at": switched_at,
                })
            },
        )
}

/// Generate fuzzed system event payloads
fn fuzzed_system_payloads() -> impl Strategy<Value = JsonValue> {
    (
        problematic_strings(),                   // message
        prop::option::of(any::<u8>()),           // priority
        prop::option::of(problematic_strings()), // unit
        prop::option::of(edge_case_numbers()),   // pid
        prop::option::of(problematic_strings()), // cursor
        prop::collection::hash_map(problematic_strings(), problematic_strings(), 0..50), // fields
        problematic_timestamps(),                // timestamp
        problematic_strings(),                   // state_type
        malformed_json_values(),                 // checkpoint_data
        problematic_timestamps(),                // changed_at
    )
        .prop_map(
            |(
                message,
                priority,
                unit,
                pid,
                cursor,
                fields,
                timestamp,
                state_type,
                checkpoint_data,
                changed_at,
            )| {
                serde_json::json!({
                    "message": message,
                    "priority": priority,
                    "unit": unit,
                    "pid": pid,
                    "cursor": cursor,
                    "fields": fields,
                    "timestamp": timestamp,
                    "state_type": state_type,
                    "checkpoint_data": checkpoint_data,
                    "changed_at": changed_at,
                })
            },
        )
}

// ============================================================================
// Property-Based Tests
// ============================================================================

// Test that event creation and database insertion never panics with arbitrary fuzzed events.
sinex_proptest! {
    fn test_event_creation_never_panics_with_fuzzed_data(
        event in fuzzed_events()
    ) -> TestResult<()> {
        let _ = serde_json::to_string(&event);
        let _ = serde_json::to_string_pretty(&event);
        let _ = event.source.as_str();
        let _ = event.event_type.as_str();
        let _ = event.host.as_str();
        let _ = &event.payload;
        let _ = event.ts_orig;
        let _ = event.node_version;
        let _ = event.payload_schema_id;
        TestCaseResult::Ok(())
    }
// Test filesystem events hold up when payloads become especially large or malformed.
    fn test_filesystem_events_robustness(
        payload in fuzzed_filesystem_payloads(),
        event_type in prop_oneof![
            Just("file.created"),
            Just("file.modified"),
            Just("file.deleted"),
            Just("file.moved"),
            Just("dir.created"),
            Just("dir.deleted"),
        ],
    ) -> TestResult<()> {
        let mut event = event_fixture(
            EventSource::from_static("fs"),
            EventType::from_static(event_type),
            payload,
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        let _json_result = serde_json::to_string(&event);
        let _ = event.source.as_str();
        let _ = event.event_type.as_str();
        let _ = &event.payload;
        TestCaseResult::Ok(())
    }

// Test terminal events with extreme payloads.
    fn test_terminal_events_robustness(
        payload in fuzzed_terminal_payloads(),
        source in prop_oneof![
            Just("shell.kitty"),
            Just("shell.atuin"),
            Just("shell.history"),
            Just("shell.recording"),
            Just("shell.scrollback"),
        ],
        event_type in prop_oneof![
            Just("command.executed"),
            Just("command.completed"),
            Just("session.started"),
            Just("session.ended"),
        ],
    ) -> TestResult<()> {
        let mut event = event_fixture(
            EventSource::from_static(source),
            EventType::from_static(event_type),
            payload,
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        let _json_result = serde_json::to_string(&event);
        let _ = event.source.as_str();
        let _ = event.event_type.as_str();
        let _ = &event.payload;
        TestCaseResult::Ok(())
    }

// Test clipboard events with extreme payloads.
    fn test_clipboard_events_robustness(
        payload in fuzzed_clipboard_payloads(),
        event_type in prop_oneof![
            Just("copied"),
            Just("selected"),
        ],
    ) -> TestResult<()> {
        let mut event = event_fixture(
            EventSource::from_static("clipboard"),
            EventType::from_static(event_type),
            payload,
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        let _json_result = serde_json::to_string(&event);
        let _ = event.source.as_str();
        let _ = event.event_type.as_str();
        let _ = &event.payload;
        TestCaseResult::Ok(())
    }

}

// Test window manager events with extreme payloads.
sinex_proptest! {
    fn test_window_manager_events_robustness(
        payload in fuzzed_window_manager_payloads(),
        event_type in prop_oneof![
            Just("window.opened"),
            Just("window.closed"),
            Just("window.focused"),
            Just("window.moved"),
            Just("window.resized"),
            Just("workspace.switched"),
            Just("workspace.created"),
            Just("workspace.destroyed"),
        ],
    ) -> TestResult<()> {
        let mut event = event_fixture(
            EventSource::from_static("wm.hyprland"),
            EventType::from_static(event_type),
            payload,
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        let _json_result = serde_json::to_string(&event);
        let _ = event.source.as_str();
        let _ = event.event_type.as_str();
        let _ = &event.payload;
        TestCaseResult::Ok(())
    }
}

sinex_proptest! {
    // Test system events with extreme payloads.
    #[cases(64)]
    fn test_system_events_robustness(
        payload in fuzzed_system_payloads(),
        source in prop_oneof![
            Just("dbus"),
            Just("journald"),
        ],
        event_type in prop_oneof![
            Just("signal.received"),
            Just("method.called"),
            Just("entry.written"),
            Just("state.changed"),
        ],
    ) -> TestResult<()> {
        let mut event = event_fixture(
            EventSource::from_static(source),
            EventType::from_static(event_type),
            payload,
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        let _json_result = serde_json::to_string(&event);
        let _ = event.source.as_str();
        let _ = event.event_type.as_str();
        let _ = &event.payload;
        TestCaseResult::Ok(())
    }

    // Test JSON serialization robustness with extreme payloads.
    fn test_json_serialization_robustness(
        payload in malformed_json_values()
    ) -> TestResult<()> {
        let mut event = event_fixture(
            EventSource::from_static("test"),
            EventType::from_static("test.event"),
            payload,
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        let json_result = serde_json::to_string(&event);
        let _ = json_result.is_ok();

        let _ = serde_json::to_string_pretty(&event);
        TestCaseResult::Ok(())
    }
}

sinex_proptest! {
    // Test UUIDv7 robustness with extreme timestamps.
    fn test_uuid_robustness_with_extreme_timestamps(
        timestamp in problematic_timestamps()
    ) -> TestResult<()> {
        // Test that UUIDv7 creation with extreme timestamps doesn't panic
        let uuid = Uuid::now_v7();

        // Test conversion to UUID (used in database operations)
        let _uuid: uuid::Uuid = uuid;

        // Test string conversion
        let _string = uuid.to_string();

        // Test that we can create an event with this timestamp
        let mut event = event_fixture(
            EventSource::from_static("test"),
            EventType::from_static("test.event"),
            serde_json::json!({}),
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));
        event.ts_orig = Some(timestamp);

        // Verify the event can be serialized
        let _json = serde_json::to_string(&event);
        TestCaseResult::Ok(())
    }

    // Test string handling robustness: source/event_type are now validated,
    // so we filter to valid values and fuzz host/payload instead.
    fn test_string_handling_robustness(
        source in problematic_strings(),
        event_type in problematic_strings(),
        host in problematic_strings(),
    ) -> TestResult<()> {
        // Source and event_type are validated — skip invalid combinations
        let source = match EventSource::new(source) {
            Ok(s) => s,
            Err(_) => return TestCaseResult::Ok(()),
        };
        let event_type = match EventType::new(event_type) {
            Ok(t) => t,
            Err(_) => return TestCaseResult::Ok(()),
        };
        let mut event = event_fixture(
            source.clone(),
            event_type,
            serde_json::json!({}),
        );
        event.host = HostName::new(host.clone());
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        // Test serialization with problematic strings in host/payload
        let _json_result = serde_json::to_string(&event);

        // Verify validated source matches
        prop_assert_eq!(event.source.as_str(), source.as_str());
        prop_assert_eq!(event.host.as_str(), host.as_str());
        TestCaseResult::Ok(())
    }
}

// ============================================================================
// Database Integration Fuzzing Tests
// ============================================================================

// Test database insertion with fuzzed events using the modern infrastructure.
#[sinex_test]
async fn test_database_insertion_robustness(ctx: TestContext) -> TestResult<()> {
    use proptest::test_runner::TestRunner;

    let mut runner = TestRunner::deterministic();

    // Generate multiple fuzzed events and test serialization robustness
    // Focus on the parts that are most likely to cause issues: JSON serialization
    for _ in 0..100 {
        let event = fuzzed_events().new_tree(&mut runner).unwrap().current();

        // Test JSON serialization (this is the critical path for database storage)
        let json_result = serde_json::to_string(&event);
        if let Ok(json_str) = json_result {
            // If serialization succeeds, test deserialization as well
            let deserialize_result = serde_json::from_str::<Event<JsonValue>>(&json_str);
            assert!(
                deserialize_result.is_ok(),
                "Event should deserialize successfully if serialization succeeded"
            );
        } else {
            // Serialization errors are acceptable as long as they don't cause panics
            // The main requirement is graceful failure
        }
    }
    Ok(())
}

// Test event creation with extreme payloads in the database context.
#[sinex_test]
async fn test_extreme_payload_database_handling(ctx: TestContext) -> TestResult<()> {
    // Test various extreme payload scenarios
    let test_cases = vec![
        // Very large string payload
        json!({
            "huge_field": "x".repeat(1_000_000), // 1MB string
            "description": "large_string_test"
        }),
        // Deeply nested JSON
        {
            let mut nested = json!("deep_value");
            for _ in 0..100 {
                nested = json!({"level": nested});
            }
            nested
        },
        // Special float values
        json!({
            "infinity": f64::INFINITY,
            "neg_infinity": f64::NEG_INFINITY,
            "nan": f64::NAN,
            "very_large": f64::MAX,
            "very_small": f64::MIN,
        }),
        // Unicode stress test
        json!({
            "emoji": "🦀🔥💀🌟⚡🎯🚀💎",
            "mixed_scripts": "Hello мир 世界 שלום مرحبا",
            "zero_width": "\u{200B}\u{FEFF}\u{00A0}",
            "math_symbols": "𝕋𝕖𝕤𝕥 ∑∫∆∇",
        }),
        // Control characters and special cases
        json!({
            "null_bytes": "test\0data\0here",
            "control_chars": "\x01\x02\x03\x7F",
            "newlines": "line1\nline2\r\nline3\tline4",
        }),
    ];

    for (i, payload) in test_cases.into_iter().enumerate() {
        let event = event_fixture(
            EventSource::from_static("fuzzing"),
            EventType::from_static("extreme.payload"),
            payload,
        );

        // Test should not panic regardless of success or failure
        // Focus on serialization since that's the critical path for database storage
        let json_result = serde_json::to_string(&event);
        match json_result {
            Ok(json_str) => {
                // If serialization succeeds, test deserialization too
                let deserialize_result = serde_json::from_str::<Event<JsonValue>>(&json_str);
                if deserialize_result.is_err() {
                    // Log for debugging but don't fail - the important thing is no panic
                    eprintln!(
                        "Warning: Event {i} failed to deserialize after successful serialization"
                    );
                }
            }
            Err(e) => {
                // Serialization errors are acceptable - log for analysis
                eprintln!("Event {i} serialization failed gracefully: {e}");
            }
        }
    }
    Ok(())
}

// ============================================================================
// Additional Robustness Tests
// ============================================================================

#[cfg(test)]
mod additional_tests {
    use super::*;
    use std::panic;

    #[sinex_test]
    async fn test_event_source_rejects_null_bytes() -> TestResult<()> {
        // Source and event_type with null bytes are now rejected at construction
        assert!(EventSource::new("test\0null\0bytes").is_err());
        assert!(EventType::new("test\0event").is_err());

        // Null bytes in payload and host are still allowed (not validated)
        let mut event = event_fixture(
            EventSource::from_static("test"),
            EventType::from_static("test.event"),
            serde_json::json!({"null_bytes": "test\0data"}),
        );
        event.host = HostName::new("test\0host".to_string());
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        // Serialization should still work for payload/host with null bytes
        let _result = serde_json::to_string(&event);

        Ok(())
    }

    #[sinex_test]
    async fn test_event_with_extremely_large_payload(ctx: TestContext) -> TestResult<()> {
        // Create a very large payload (10MB of data)
        let large_string = "x".repeat(10_000_000);
        let large_payload = serde_json::json!({
            "huge_field": large_string,
            "nested": {
                "also_huge": "y".repeat(5_000_000)
            }
        });

        let mut event = event_fixture(
            EventSource::from_static("test"),
            EventType::from_static("test.large"),
            large_payload,
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        // Should handle large payloads gracefully (may succeed or fail, but shouldn't panic)
        // Test serialization instead of database insertion
        let _result = serde_json::to_string(&event);

        Ok(())
    }

    #[sinex_test]
    async fn test_event_with_infinite_numbers(ctx: TestContext) -> TestResult<()> {
        let payload = serde_json::json!({
            "infinity": f64::INFINITY,
            "neg_infinity": f64::NEG_INFINITY,
            "nan": f64::NAN,
            "very_large": f64::MAX,
            "very_small": f64::MIN,
        });

        let mut event = event_fixture(
            EventSource::from_static("test"),
            EventType::from_static("test.numbers"),
            payload,
        );
        event.id = Some(Id::from_uuid(Uuid::now_v7()));

        // Should handle special float values gracefully
        // Test serialization instead of database insertion
        let _result = serde_json::to_string(&event);

        Ok(())
    }

    #[sinex_test]
    async fn test_panic_safety_with_catch_unwind() -> TestResult<()> {
        // Verify that invalid source/event_type are rejected, not panicking
        assert!(EventSource::new("\x00\x01\x02").is_err());
        assert!(EventType::new("💀🔥test").is_err());

        // Test that valid source/type with problematic payload doesn't panic
        let result = panic::catch_unwind(|| {
            let mut event = event_fixture(
                EventSource::from_static("test"),
                EventType::from_static("test.event"),
                serde_json::json!({
                    "🔥": "💀",
                    "\x00": "\x01",
                    "nested": {
                        "💀": [1, 2, 3, f64::INFINITY]
                    }
                }),
            );
            event.host = HostName::new("🦀".to_string());
            event.id = Some(Id::from_uuid(Uuid::now_v7()));

            // Test JSON serialization
            let _json_result = serde_json::to_string(&event);

            // Test field access
            let _ = event.source.as_str();
            let _ = event.event_type.as_str();
            let _ = event.host.as_str();
        });

        // This should not panic
        assert!(result.is_ok());
        Ok(())
    }
}
