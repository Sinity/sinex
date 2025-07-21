// Property test builders that integrate proptest with the test framework
//
// This module provides proptest strategies for generating test data that works
// seamlessly with our test builders, making property-based testing easier and
// more consistent throughout the codebase.

use crate::common::event_builders::*;
use crate::common::prelude::*;
use chrono::{DateTime, Utc};
use proptest::prelude::*;
use serde_json::Value;
use sinex_db::RawEvent;
use sinex_events::{event_types, sources, EventFactory};
use sinex_satellite_sdk::stream_processor::Checkpoint;
use sinex_ulid::Ulid;
use std::collections::HashMap;

/// Strategy for generating arbitrary valid events using TestEventBuilder
pub fn arbitrary_event() -> impl Strategy<Value = RawEvent> {
    (
        event_sources(),
        event_types(),
        event_payloads(),
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(source, event_type, payload, timestamp)| {
            let builder = TestEventBuilder::new().source(source).event_type(&event_type);

            let builder = if let Some(ts) = timestamp {
                builder.timestamp(ts)
            } else {
                builder
            };

            builder.payload(payload).build()
        })
}

/// Strategy for generating event batches with related events
pub fn arbitrary_event_batch() -> impl Strategy<Value = Vec<RawEvent>> {
    (1usize..=50, event_sources()).prop_flat_map(|(size, source)| {
        proptest::collection::vec(
            (
                Just(source),
                event_types(),
                event_payloads(),
                prop::option::of(valid_timestamps()),
            )
                .prop_map(move |(source, event_type, payload, timestamp)| {
                    let builder = TestEventBuilder::new()
                        .source(source)
                        .event_type(&event_type)
                        .payload(payload);

                    if let Some(ts) = timestamp {
                        builder.timestamp(ts).build()
                    } else {
                        builder.build()
                    }
                }),
            size,
        )
    })
}

/// Strategy for generating checkpoints with realistic data
pub fn arbitrary_checkpoint() -> impl Strategy<Value = Checkpoint> {
    prop_oneof![
        // No checkpoint
        Just(Checkpoint::None),
        // Stream checkpoint
        (
            "[0-9]+-[0-9]+",
            prop::option::of(ulids().prop_map(|u| u.to_string()))
        )
            .prop_map(|(message_id, event_id)| Checkpoint::Stream {
                message_id,
                event_id
            }),
        // Database checkpoint
        ulids().prop_map(|ulid| Checkpoint::Database { event_id: ulid }),
        // Timestamp checkpoint
        valid_timestamps().prop_map(|ts| Checkpoint::Timestamp { timestamp: ts }),
    ]
}

/// Strategy for generating ULID ranges for time-based queries
pub fn arbitrary_ulid_range() -> impl Strategy<Value = (Ulid, Ulid)> {
    (ulids(), ulids()).prop_map(|(a, b)| {
        // Ensure proper ordering
        if a < b {
            (a, b)
        } else {
            (b, a)
        }
    })
}

/// Strategy for generating time ranges for temporal queries
pub fn arbitrary_time_range() -> impl Strategy<Value = (DateTime<Utc>, DateTime<Utc>)> {
    (valid_timestamps(), 1u64..=86400u64).prop_map(|(start, duration_secs)| {
        let end = start + chrono::Duration::seconds(duration_secs as i64);
        (start, end)
    })
}

/// Strategy for generating filesystem events with proper structure
pub fn filesystem_event() -> impl Strategy<Value = RawEvent> {
    (
        prop_oneof![
            Just(event_types::filesystem::FILE_CREATED),
            Just(event_types::filesystem::FILE_MODIFIED),
            Just(event_types::filesystem::FILE_DELETED),
            Just(event_types::filesystem::FILE_RENAMED),
            Just(event_types::filesystem::DIR_CREATED),
            Just(event_types::filesystem::DIR_DELETED),
        ],
        file_paths(),
        0u64..=10_000_000u64, // file size
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(event_type, path, size, timestamp)| {
            let builder = FilesystemEventBuilder::new()
                .event_type(event_type)
                .path(&path)
                .size(size);

            if let Some(ts) = timestamp {
                builder.timestamp(ts).build()
            } else {
                builder.build()
            }
        })
}

/// Strategy for generating shell command events
pub fn shell_command_event() -> impl Strategy<Value = RawEvent> {
    (
        shell_commands(),
        0i32..=255i32,                   // exit code
        0u64..=60_000u64,                // duration in ms
        prop::option::of(file_paths()),  // working directory
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(command, exit_code, duration_ms, cwd, timestamp)| {
            let builder = ShellEventBuilder::new()
                .command(&command)
                .exit_code(exit_code)
                .duration_ms(duration_ms);

            let builder = if let Some(dir) = cwd {
                builder.working_directory(&dir)
            } else {
                builder
            };

            if let Some(ts) = timestamp {
                builder.timestamp(ts).build()
            } else {
                builder.build()
            }
        })
}

/// Strategy for generating window manager events
pub fn window_event() -> impl Strategy<Value = RawEvent> {
    (
        prop_oneof![
            Just(WindowManagerEventType::WindowOpened),
            Just(WindowManagerEventType::WindowClosed),
            Just(WindowManagerEventType::WindowFocused),
            Just(WindowManagerEventType::WorkspaceChanged),
        ],
        window_classes(),
        window_titles(),
        1u32..=10u32, // workspace id
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(event_type, class, title, workspace, timestamp)| {
            let builder = WindowManagerEventBuilder::new()
                .event_type(event_type)
                .window_class(&class)
                .window_title(&title)
                .workspace(workspace);

            if let Some(ts) = timestamp {
                builder.timestamp(ts).build()
            } else {
                builder.build()
            }
        })
}

/// Strategy for generating clipboard events
pub fn clipboard_event() -> impl Strategy<Value = RawEvent> {
    (
        prop_oneof![
            Just(ClipboardEventType::ContentCopied),
            Just(ClipboardEventType::ContentPasted),
        ],
        clipboard_content(),
        prop::option::of(mime_types()),
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(event_type, content, mime_type, timestamp)| {
            let builder = ClipboardEventBuilder::new()
                .event_type(event_type)
                .content(&content);

            let builder = if let Some(mime) = mime_type {
                builder.mime_type(&mime)
            } else {
                builder
            };

            if let Some(ts) = timestamp {
                builder.timestamp(ts).build()
            } else {
                builder.build()
            }
        })
}

/// Strategy for generating heartbeat events
pub fn heartbeat_event() -> impl Strategy<Value = RawEvent> {
    (
        automaton_names(),
        0u64..=1_000_000u64,  // events processed
        0u64..=86400u64,      // uptime seconds
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(name, processed, uptime, timestamp)| {
            let builder = HeartbeatEventBuilder::new()
                .automaton_name(&name)
                .events_processed(processed)
                .uptime_seconds(uptime);

            if let Some(ts) = timestamp {
                builder.timestamp(ts).build()
            } else {
                builder.build()
            }
        })
}

/// Strategy for generating invalid events with empty source
pub fn empty_source_event() -> impl Strategy<Value = RawEvent> {
    (event_types(), event_payloads()).prop_map(|(event_type, payload)| {
        let mut event = EventFactory::new("test").create_event(&event_type, payload);
        event.source = String::new(); // Make it invalid
        event
    })
}

/// Strategy for generating events with massive payloads
pub fn massive_payload_event() -> impl Strategy<Value = RawEvent> {
    (
        event_sources(),
        event_types(),
        1_000_000usize..=10_000_000usize, // payload size
    )
        .prop_map(|(source, event_type, size)| {
            let large_string = "x".repeat(size);
            let payload = json!({
                "massive_data": large_string,
                "size": size
            });
            EventFactory::new(source).create_event(&event_type, payload)
        })
}

/// Strategy for generating deeply nested events
pub fn deeply_nested_event() -> impl Strategy<Value = RawEvent> {
    (event_sources(), event_types(), 10usize..=100usize).prop_map(|(source, event_type, depth)| {
        let payload = create_nested_json(depth);
        EventFactory::new(source).create_event(&event_type, payload)
    })
}

/// Strategy for generating events with extreme timestamps
pub fn extreme_timestamp_event() -> impl Strategy<Value = RawEvent> {
    (
        event_sources(),
        event_types(),
        event_payloads(),
        prop_oneof![
            Just(DateTime::from_timestamp(0, 0).unwrap()),           // Unix epoch
            Just(DateTime::from_timestamp(i64::MAX / 1000, 0).unwrap()), // Far future
            Just(Utc::now() - chrono::Duration::days(365 * 50)),    // 50 years ago
            Just(Utc::now() + chrono::Duration::days(365 * 50)),    // 50 years future
        ],
    )
        .prop_map(|(source, event_type, payload, timestamp)| {
            let mut event = EventFactory::new(source).create_event(&event_type, payload);
            event.ts_orig = Some(timestamp);
            event
        })
}

/// Strategy for generating time-ordered event batches
pub fn time_ordered_batch() -> impl Strategy<Value = Vec<RawEvent>> {
    (
        5usize..=20usize,
        event_sources(),
        valid_timestamps(),
        1u64..=60u64, // interval seconds
    )
        .prop_flat_map(|(size, source, start_time, interval)| {
            (0..size)
                .map(|i| {
                    let timestamp = start_time + chrono::Duration::seconds((i as u64) * interval);
                    (
                        Just(source),
                        event_types(),
                        event_payloads(),
                        Just(timestamp),
                    )
                        .prop_map(|(source, event_type, payload, ts)| {
                            TestEventBuilder::new()
                                .source(source)
                                .event_type(&event_type)
                                .payload(payload)
                                .timestamp(ts)
                                .build()
                        })
                })
                .collect::<Vec<_>>()
                .prop_map(|strategies| {
                    // This is a bit tricky - we need to collect the strategies into a vec strategy
                    strategies
                })
        })
}

/// Strategy for generating realistic user activity batches
pub fn user_activity_batch() -> impl Strategy<Value = Vec<RawEvent>> {
    valid_timestamps().prop_flat_map(|start_time| {
        // Generate a sequence of user activities
        vec![
            // User starts work
            shell_command_event().prop_map(move |mut event| {
                event.ts_orig = Some(start_time);
                event
            }).boxed(),
            // Opens some files
            filesystem_event().prop_map(move |mut event| {
                event.ts_orig = Some(start_time + chrono::Duration::seconds(10));
                event
            }).boxed(),
            // Switches windows
            window_event().prop_map(move |mut event| {
                event.ts_orig = Some(start_time + chrono::Duration::seconds(30));
                event
            }).boxed(),
            // Copies some content
            clipboard_event().prop_map(move |mut event| {
                event.ts_orig = Some(start_time + chrono::Duration::seconds(45));
                event
            }).boxed(),
            // Runs more commands
            shell_command_event().prop_map(move |mut event| {
                event.ts_orig = Some(start_time + chrono::Duration::seconds(60));
                event
            }).boxed(),
        ]
    })
}

/// Strategy for generating related events (e.g., file operations on same file)
pub fn related_events_batch() -> impl Strategy<Value = Vec<RawEvent>> {
    file_paths().prop_flat_map(|path| {
        let base_time = Utc::now();
        vec![
            // File created
            Just(
                FilesystemEventBuilder::new()
                    .event_type(event_types::filesystem::FILE_CREATED)
                    .path(&path)
                    .timestamp(base_time)
                    .build()
            ),
            // File modified multiple times
            Just(
                FilesystemEventBuilder::new()
                    .event_type(event_types::filesystem::FILE_MODIFIED)
                    .path(&path)
                    .timestamp(base_time + chrono::Duration::seconds(5))
                    .build()
            ),
            Just(
                FilesystemEventBuilder::new()
                    .event_type(event_types::filesystem::FILE_MODIFIED)
                    .path(&path)
                    .timestamp(base_time + chrono::Duration::seconds(10))
                    .build()
            ),
            // File deleted
            Just(
                FilesystemEventBuilder::new()
                    .event_type(event_types::filesystem::FILE_DELETED)
                    .path(&path)
                    .timestamp(base_time + chrono::Duration::seconds(20))
                    .build()
            ),
        ]
    })
}

// Helper strategies for generating specific data types

fn event_sources() -> impl Strategy<Value = &'static str> {
    prop_oneof![
        Just(sources::FS),
        Just(sources::SHELL_KITTY),
        Just(sources::WM_HYPRLAND),
        Just(sources::CLIPBOARD),
        Just(sources::SINEX),
        Just("test"),
    ]
}

fn event_types() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(event_types::filesystem::FILE_CREATED.to_string()),
        Just(event_types::filesystem::FILE_MODIFIED.to_string()),
        Just(event_types::filesystem::FILE_DELETED.to_string()),
        Just(event_types::shell::COMMAND_EXECUTED.to_string()),
        Just(event_types::window_manager::WINDOW_OPENED.to_string()),
        Just(event_types::window_manager::WINDOW_CLOSED.to_string()),
        Just(event_types::clipboard::COPIED.to_string()),
        Just(event_types::sinex::AUTOMATON_HEARTBEAT.to_string()),
        Just("test.event".to_string()),
    ]
}

fn event_payloads() -> impl Strategy<Value = Value> {
    prop_oneof![
        // Small payloads
        Just(json!({"simple": "data"})),
        Just(json!({"number": 42})),
        // Medium payloads
        Just(json!({
            "type": "medium",
            "data": [1, 2, 3, 4, 5],
            "metadata": {"created": "2024-01-01"}
        })),
        // Larger payloads
        (0usize..=100).prop_map(|size| {
            json!({
                "array": (0..size).collect::<Vec<_>>(),
                "size": size
            })
        }),
        // Unicode payloads
        Just(json!({"unicode": "🦀 Rust 中文 العربية 🚀"})),
    ]
}

fn valid_timestamps() -> impl Strategy<Value = DateTime<Utc>> {
    // Generate timestamps between 2020 and 2030
    (1577836800i64..=1893456000i64).prop_map(|ts| DateTime::from_timestamp(ts, 0).unwrap())
}

fn ulids() -> impl Strategy<Value = Ulid> {
    any::<[u8; 16]>().prop_map(|bytes| Ulid::from_bytes(bytes).unwrap_or_else(|_| Ulid::new()))
}

fn file_paths() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("/home/user/document.txt".to_string()),
        Just("/tmp/cache/file.json".to_string()),
        Just("/var/log/system.log".to_string()),
        Just("/home/user/code/project/src/main.rs".to_string()),
        Just("/home/user/.config/app/settings.toml".to_string()),
        "[a-z]+/[a-z]+\\.[a-z]{2,4}".prop_map(|s| format!("/home/user/{}", s)),
    ]
}

fn shell_commands() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("ls -la".to_string()),
        Just("git status".to_string()),
        Just("cargo build".to_string()),
        Just("vim file.rs".to_string()),
        Just("cd /home/user".to_string()),
        Just("grep -r 'TODO' .".to_string()),
        Just("docker ps -a".to_string()),
        "[a-z]+ [a-z\\-]+".prop_map(|s| s),
    ]
}

fn window_classes() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("firefox".to_string()),
        Just("kitty".to_string()),
        Just("code".to_string()),
        Just("chromium".to_string()),
        Just("nautilus".to_string()),
        "[A-Z][a-z]+".prop_map(|s| s),
    ]
}

fn window_titles() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("Mozilla Firefox".to_string()),
        Just("Terminal - kitty".to_string()),
        Just("Visual Studio Code".to_string()),
        Just("~/Documents".to_string()),
        ".+ - .+".prop_map(|s| s),
    ]
}

fn clipboard_content() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("Hello, world!".to_string()),
        Just("https://example.com".to_string()),
        Just("user@example.com".to_string()),
        Just("fn main() { println!(\"Hello\"); }".to_string()),
        "[a-zA-Z0-9 ]{1,100}".prop_map(|s| s),
    ]
}

fn mime_types() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("text/plain".to_string()),
        Just("text/html".to_string()),
        Just("application/json".to_string()),
        Just("image/png".to_string()),
        Just("application/octet-stream".to_string()),
    ]
}

fn automaton_names() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("command-canonicalizer".to_string()),
        Just("health-aggregator".to_string()),
        Just("test-automaton".to_string()),
        "test-[a-z]+-automaton".prop_map(|s| s),
    ]
}

// Helper function to create nested JSON
fn create_nested_json(depth: usize) -> Value {
    let mut current = json!("leaf");
    for i in (0..depth).rev() {
        current = json!({
            "level": i,
            "nested": current,
            "data": format!("level_{}", i)
        });
    }
    current
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arbitrary_event_generation() {
        let mut runner = proptest::test_runner::TestRunner::default();
        let strategy = arbitrary_event();
        
        for _ in 0..10 {
            let event = strategy.new_tree(&mut runner).unwrap().current();
            assert!(!event.source.is_empty());
            assert!(!event.event_type.is_empty());
            assert!(event.id != Ulid::nil());
        }
    }

    #[test]
    fn test_event_batch_generation() {
        let mut runner = proptest::test_runner::TestRunner::default();
        let strategy = arbitrary_event_batch();
        
        let batch = strategy.new_tree(&mut runner).unwrap().current();
        assert!(!batch.is_empty());
        assert!(batch.len() <= 50);
        
        // All events in batch should have same source
        let first_source = &batch[0].source;
        assert!(batch.iter().all(|e| &e.source == first_source));
    }

    #[test]
    fn test_time_ordered_batch() {
        let mut runner = proptest::test_runner::TestRunner::default();
        let strategy = time_ordered_batch();
        
        for _ in 0..5 {
            let batch = strategy.new_tree(&mut runner).unwrap().current();
            
            // Verify events are in chronological order
            for window in batch.windows(2) {
                let (prev, curr) = (&window[0], &window[1]);
                if let (Some(prev_ts), Some(curr_ts)) = (prev.ts_orig, curr.ts_orig) {
                    assert!(prev_ts <= curr_ts);
                }
            }
        }
    }
}