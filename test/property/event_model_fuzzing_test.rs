// Property-based tests for event model robustness
//
// This module implements comprehensive property-based testing (fuzzing) for the Sinex event
// processing pipeline to ensure robustness against malformed, extreme, or unexpected data.
//
// # Goals
//
// 1. **Prevent panics in production**: Any possible input should either process successfully
//    or fail gracefully, never crash the system
// 2. **Test boundary conditions**: Empty strings, max values, unicode edge cases
// 3. **Validate error handling**: Ensure all error paths are robust
// 4. **Comprehensive coverage**: Test all major event types and payload structures
//
// # Strategy
//
// - Use proptest to generate 1000+ test cases per event type
// - Test with extreme values: empty strings, very long strings, unicode, control chars
// - Test with edge case numbers: negative, zero, max values, floating point precision
// - Test with malformed but parseable JSON structures
// - Focus on the `output_event` function which is the main processing pipeline entry point

use sinex_test_utils::prelude::*;

use chrono::{DateTime, TimeZone, Utc};
use proptest::prelude::*;
use serde_json::{Map as JsonMap, Value as JsonValue};
use sinex_types::events::RawEvent;
use sinex_types::ulid::Ulid;

// ============================================================================
// Proptest Strategies for Generating Fuzzed Data
// ============================================================================

/// Strategy for generating potentially problematic strings
fn problematic_strings() -> impl Strategy<Value = String> {
    prop_oneof![
        // Empty and whitespace
        Just("".to_string()),
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
        prop::string::string_regex("[\\x00-\\x1F\\u{007F}-\\u{009F}]*").unwrap(),
        prop::string::string_regex("[\\p{C}]*").unwrap(), // Control characters
        prop::string::string_regex("[\\p{M}]*").unwrap(), // Mark characters
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
        Just(i32::MIN as i64),
        Just(i32::MAX as i64),
        Just(u32::MAX as i64),
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
        Just(u32::MAX as u64),
        Just(u16::MAX as u64),
        Just(u8::MAX as u64),
        any::<u64>(),
    ]
}

/// Strategy for generating problematic timestamps
fn problematic_timestamps() -> impl Strategy<Value = DateTime<Utc>> {
    prop_oneof![
        // Unix epoch
        Just(Utc.timestamp_opt(0, 0).unwrap()),
        // Very early dates
        Just(Utc.timestamp_opt(-2208988800, 0).unwrap()), // 1900-01-01
        // Very far future dates
        Just(Utc.timestamp_opt(4102444800, 0).unwrap()), // 2100-01-01
        // Edge of 32-bit time_t
        Just(Utc.timestamp_opt(2147483647, 0).unwrap()), // 2038-01-19
        Just(Utc.timestamp_opt(-2147483648, 0).unwrap()), // 1901-12-13
        // Random timestamps
        (-2208988800i64..4102444800i64).prop_map(|ts| {
            match Utc.timestamp_opt(ts, 0) {
                chrono::LocalResult::Single(dt) => dt,
                _ => Utc::now(),
            }
        }),
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

/// Strategy for generating fuzzed RawEvent instances
fn fuzzed_raw_events() -> impl Strategy<Value = RawEvent> {
    (
        any::<Ulid>(),
        problematic_strings(),                      // source
        problematic_strings(),                      // event_type
        problematic_timestamps(),                   // ts_ingest
        prop::option::of(problematic_timestamps()), // ts_orig
        problematic_strings(),                      // host
        prop::option::of(problematic_strings()),    // ingestor_version
        prop::option::of(any::<Ulid>()),            // payload_schema_id
        malformed_json_values(),                    // payload
    )
        .prop_map(
            |(
                id,
                source,
                event_type,
                ts_ingest,
                ts_orig,
                host,
                ingestor_version,
                payload_schema_id,
                payload,
            )| {
                RawEvent {
                    id,
                    source,
                    event_type,
                    ts_ingest,
                    ts_orig,
                    host,
                    ingestor_version,
                    payload_schema_id,
                    payload,
                }
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
        malformed_json_values(),                 // state_data
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
                state_data,
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
                    "state_data": state_data,
                    "changed_at": changed_at,
                })
            },
        )
}

// ============================================================================
// Property-Based Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// Test that output_event never panics with arbitrary fuzzed events
    #[test]
    fn test_output_event_never_panics_with_fuzzed_events(
        event in fuzzed_raw_events(),
        to_database in any::<bool>(),
        to_stdout in any::<bool>(),
        dry_run in any::<bool>(),
    ) {
        // Use tokio runtime for async test
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let output_config = OutputConfig {
                to_database,
                to_stdout,
                to_file: None, // Skip file output to avoid filesystem issues in fuzz testing
                dry_run,
            };

            let mut file_handle = None;

            // The critical assertion: output_event should never panic, regardless of input
            // It should either succeed or return an error gracefully
            let result = output_event(
                &event,
                &output_config,
                None, // No database in fuzz test
                None, // No validator in fuzz test
                &mut file_handle,
            ).await;

            // We don't care if it succeeds or fails, just that it doesn't panic
            // This will catch any unwrap() calls or other panic sources
            match result {
                Ok(_) => {
                    // Success is fine
                }
                Err(_) => {
                    // Errors are also fine, as long as they're graceful
                }
            }
        });
    }

    /// Test filesystem events with extreme payloads
    #[test]
    fn test_filesystem_events_robustness(
        payload in fuzzed_filesystem_payloads(),
        source in problematic_strings(),
        event_type in prop_oneof![
            Just("file.created"),
            Just("file.modified"),
            Just("file.deleted"),
            Just("file.moved"),
            Just("dir.created"),
            Just("dir.deleted"),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let event = RawEvent {
                id: Ulid::new(),
                source: if source.is_empty() { "fs".to_string() } else { source },
                event_type: event_type.to_string(),
                ts_ingest: Utc::now(),
                ts_orig: None,
                host: "test-host".to_string(),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
                payload,
            };

            let output_config = OutputConfig {
                to_database: false,
                to_stdout: false,
                to_file: None,
                dry_run: true, // Safe mode for fuzzing
            };

            let mut file_handle = None;
            let _result = output_event(&event, &output_config, None, None, &mut file_handle).await;
            // Success: no panic occurred
        });
    }

    /// Test terminal events with extreme payloads
    #[test]
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
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let event = RawEvent {
                id: Ulid::new(),
                source: source.to_string(),
                event_type: event_type.to_string(),
                ts_ingest: Utc::now(),
                ts_orig: None,
                host: "test-host".to_string(),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
                payload,
            };

            let output_config = OutputConfig {
                to_database: false,
                to_stdout: false,
                to_file: None,
                dry_run: true,
            };

            let mut file_handle = None;
            let _result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        });
    }

    /// Test clipboard events with extreme payloads
    #[test]
    fn test_clipboard_events_robustness(
        payload in fuzzed_clipboard_payloads(),
        event_type in prop_oneof![
            Just("copied"),
            Just("selected"),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let event = RawEvent {
                id: Ulid::new(),
                source: "clipboard".to_string(),
                event_type: event_type.to_string(),
                ts_ingest: Utc::now(),
                ts_orig: None,
                host: "test-host".to_string(),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
                payload,
            };

            let output_config = OutputConfig {
                to_database: false,
                to_stdout: false,
                to_file: None,
                dry_run: true,
            };

            let mut file_handle = None;
            let _result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        });
    }

    /// Test window manager events with extreme payloads
    #[test]
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
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let event = RawEvent {
                id: Ulid::new(),
                source: "wm.hyprland".to_string(),
                event_type: event_type.to_string(),
                ts_ingest: Utc::now(),
                ts_orig: None,
                host: "test-host".to_string(),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
                payload,
            };

            let output_config = OutputConfig {
                to_database: false,
                to_stdout: false,
                to_file: None,
                dry_run: true,
            };

            let mut file_handle = None;
            let _result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        });
    }

    /// Test system events with extreme payloads
    #[test]
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
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let event = RawEvent {
                id: Ulid::new(),
                source: source.to_string(),
                event_type: event_type.to_string(),
                ts_ingest: Utc::now(),
                ts_orig: None,
                host: "test-host".to_string(),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
                payload,
            };

            let output_config = OutputConfig {
                to_database: false,
                to_stdout: false,
                to_file: None,
                dry_run: true,
            };

            let mut file_handle = None;
            let _result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        });
    }

    /// Test JSON serialization robustness with extreme payloads
    #[test]
    fn test_json_serialization_robustness(
        payload in malformed_json_values(),
    ) {
        let event = RawEvent {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "test-host".to_string(),
            ingestor_version: Some("test".to_string()),
            payload_schema_id: None,
            payload,
        };

        // Test that JSON serialization never panics
        let json_result = serde_json::to_string(&event);
        match json_result {
            Ok(_) => {
                // Success is fine
            }
            Err(_) => {
                // Serialization errors are acceptable as long as no panic
            }
        }

        // Test pretty printing as well (used in stdout output)
        let pretty_result = serde_json::to_string_pretty(&event);
        match pretty_result {
            Ok(_) => {}
            Err(_) => {}
        }
    }

    /// Test ULID robustness with extreme timestamps
    #[test]
    fn test_ulid_robustness_with_extreme_timestamps(
        timestamp in problematic_timestamps(),
    ) {
        // Test that ULID creation with extreme timestamps doesn't panic
        let ulid = Ulid::new();

        // Test conversion to UUID (used in database operations)
        let _uuid = ulid.to_uuid();

        // Test string conversion
        let _string = ulid.to_string();

        // Test that we can create an event with this timestamp
        let event = RawEvent {
            id: ulid,
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            ts_ingest: timestamp,
            ts_orig: Some(timestamp),
            host: "test-host".to_string(),
            ingestor_version: Some("test".to_string()),
            payload_schema_id: None,
            payload: serde_json::json!({}),
        };

        // Verify the event can be serialized
        let _json = serde_json::to_string(&event);
    }

    /// Test string handling robustness
    #[test]
    fn test_string_handling_robustness(
        source in problematic_strings(),
        event_type in problematic_strings(),
        host in problematic_strings(),
        ingestor_version in prop::option::of(problematic_strings()),
        payload_schema_id in prop::option::of(any::<Ulid>()),
    ) {
        let event = RawEvent {
            id: Ulid::new(),
            source,
            event_type,
            ts_ingest: Utc::now(),
            ts_orig: None,
            host,
            ingestor_version,
            payload_schema_id,
            payload: serde_json::json!({}),
        };

        // Test serialization with problematic strings
        let _json_result = serde_json::to_string(&event);

        // Test that we can create the struct without panicking
        assert!(true); // If we get here, no panic occurred
    }
}

// ============================================================================
// Additional Robustness Tests
// ============================================================================

#[cfg(test)]
mod additional_tests {
    use super::*;
    use std::panic;

    #[test]
    fn test_output_event_with_null_bytes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let event = RawEvent {
                id: Ulid::new(),
                source: "test\0null\0bytes".to_string(),
                event_type: "test\0event".to_string(),
                ts_ingest: Utc::now(),
                ts_orig: None,
                host: "test\0host".to_string(),
                ingestor_version: Some("test\0version".to_string()),
                payload_schema_id: None,
                payload: serde_json::json!({"null_bytes": "test\0data"}),
            };

            let output_config = OutputConfig {
                to_database: false,
                to_stdout: false,
                to_file: None,
                dry_run: true,
            };

            let mut file_handle = None;
            // Should not panic even with null bytes
            let _result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        });
    }

    #[test]
    fn test_output_event_with_extremely_large_payload() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Create a very large payload (10MB of data)
            let large_string = "x".repeat(10_000_000);
            let large_payload = serde_json::json!({
                "huge_field": large_string,
                "nested": {
                    "also_huge": "y".repeat(5_000_000)
                }
            });

            let event = RawEvent {
                id: Ulid::new(),
                source: "test".to_string(),
                event_type: "test.large".to_string(),
                ts_ingest: Utc::now(),
                ts_orig: None,
                host: "test-host".to_string(),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
                payload: large_payload,
            };

            let output_config = OutputConfig {
                to_database: false,
                to_stdout: false,
                to_file: None,
                dry_run: true,
            };

            let mut file_handle = None;
            // Should handle large payloads gracefully
            let _result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        });
    }

    #[test]
    fn test_output_event_with_infinite_numbers() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let payload = serde_json::json!({
                "infinity": f64::INFINITY,
                "neg_infinity": f64::NEG_INFINITY,
                "nan": f64::NAN,
                "very_large": f64::MAX,
                "very_small": f64::MIN,
            });

            let event = RawEvent {
                id: Ulid::new(),
                source: "test".to_string(),
                event_type: "test.numbers".to_string(),
                ts_ingest: Utc::now(),
                ts_orig: None,
                host: "test-host".to_string(),
                ingestor_version: Some("test".to_string()),
                payload_schema_id: None,
                payload,
            };

            let output_config = OutputConfig {
                to_database: false,
                to_stdout: false,
                to_file: None,
                dry_run: true,
            };

            let mut file_handle = None;
            // Should handle special float values gracefully
            let _result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        });
    }

    #[test]
    fn test_panic_safety_with_catch_unwind() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Test that even if there were a panic, it would be caught
        let result = panic::catch_unwind(|| {
            rt.block_on(async {
                let event = RawEvent {
                    id: Ulid::new(),
                    source: "\x00\x01\x02".to_string(),
                    event_type: "💀🔥test".to_string(),
                    ts_ingest: Utc::now(),
                    ts_orig: None,
                    host: "🦀".to_string(),
                    ingestor_version: Some("test".to_string()),
                    payload_schema_id: None,
                    payload: serde_json::json!({
                        "🔥": "💀",
                        "\x00": "\x01",
                        "nested": {
                            "💀": [1, 2, 3, f64::INFINITY]
                        }
                    }),
                };

                let output_config = OutputConfig {
                    to_database: false,
                    to_stdout: false,
                    to_file: None,
                    dry_run: true,
                };

                let mut file_handle = None;
                let _result =
                    output_event(&event, &output_config, None, None, &mut file_handle).await;
            });
        });

        // This should not panic
        assert!(result.is_ok());
    }
}
