//! Consolidated property testing helpers and strategies
//!
//! This module combines property test builders with proptest strategies and macros,
//! providing a comprehensive toolkit for property-based testing in the test suite.

use crate::common::builders::*;
use crate::common::prelude::*;
use chrono::{DateTime, Utc};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::Value;
use sinex_db::RawEvent;
use sinex_events::{event_types, sources, EventFactory};
use sinex_satellite_sdk::stream_processor::Checkpoint;
use sinex_ulid::Ulid;

// ===== Core Property Strategies =====

/// Strategy for generating ULIDs
pub fn ulids() -> impl Strategy<Value = Ulid> {
    any::<u128>().prop_map(|_| Ulid::new())
}

/// Strategy for generating arbitrary valid events using TestEventBuilder
pub fn arbitrary_event() -> impl Strategy<Value = RawEvent> {
    (
        event_sources(),
        event_types(),
        event_payloads(),
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(source, event_type, payload, timestamp)| {
            let mut builder = TestEventBuilder::new(&source, &event_type)
                .with_payload(payload);

            if let Some(ts) = timestamp {
                builder = builder.with_timestamp(ts);
            }

            builder.build()
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
                    let mut builder = TestEventBuilder::new(&source, &event_type)
                        .with_payload(payload);

                    if let Some(ts) = timestamp {
                        builder = builder.with_timestamp(ts);
                    }
                    
                    builder.build()
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
            prop::option::of(ulids())
        )
            .prop_map(|(message_id, event_id)| Checkpoint::Stream {
                message_id,
                event_id
            }),
        // Database checkpoint
        ulids().prop_map(|ulid| Checkpoint::Internal { event_id: ulid, message_count: 0 }),
        // Timestamp checkpoint
        valid_timestamps().prop_map(|ts| Checkpoint::Timestamp { timestamp: ts, metadata: None }),
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
        ],
        file_paths(),
        0u64..=10_000_000u64, // file size
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(event_type, path, size, timestamp)| {
            let factory = EventFactory::new(sources::FS);
            let payload = json!({
                "path": path,
                "size": size,
            });
            
            let mut event = factory.create_event(event_type, payload);
            
            if let Some(ts) = timestamp {
                event.ts_orig = Some(ts);
            }
            
            event
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
            let payload = json!({
                "command": command,
                "exit_code": exit_code,
                "duration_ms": duration_ms,
                "working_directory": cwd
            });
            
            let mut builder = TestEventBuilder::new(sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED)
                .with_payload(payload);
                
            if let Some(ts) = timestamp {
                builder = builder.with_timestamp(ts);
            }
            
            builder.build()
        })
}

/// Strategy for generating window manager events
pub fn window_event() -> impl Strategy<Value = RawEvent> {
    (
        prop_oneof![
            Just(event_types::window_manager::WINDOW_OPENED),
            Just(event_types::window_manager::WINDOW_CLOSED),  
            Just(event_types::window_manager::WINDOW_FOCUSED),
            Just(event_types::window_manager::WORKSPACE_CHANGED),
        ],
        window_classes(),
        window_titles(),
        1u32..=10u32, // workspace id
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(event_type, class, title, workspace, timestamp)| {
            let factory = EventFactory::new(sources::WM_HYPRLAND);
            let payload = json!({
                "window_class": class,
                "window_title": title,
                "workspace_id": workspace,
            });
            
            let mut event = factory.create_event(event_type, payload);
            
            if let Some(ts) = timestamp {
                event.ts_orig = Some(ts);
            }
            
            event
        })
}

/// Strategy for generating clipboard events
pub fn clipboard_event() -> impl Strategy<Value = RawEvent> {
    (
        prop_oneof![
            Just(event_types::clipboard::COPIED),
            Just(event_types::clipboard::COPIED), // Using COPIED for both since PASTED doesn't exist
        ],
        clipboard_content(),
        prop::option::of(valid_timestamps()),
    )
        .prop_map(|(event_type, content, timestamp)| {
            let factory = EventFactory::new(sources::CLIPBOARD);
            let payload = json!({
                "content": content,
                "content_type": "text/plain",
            });
            
            let mut event = factory.create_event(event_type, payload);
            
            if let Some(ts) = timestamp {
                event.ts_orig = Some(ts);
            }
            
            event
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
            let factory = EventFactory::new(sources::SINEX);
            let payload = json!({
                "automaton_name": name,
                "events_processed": processed,
                "uptime_seconds": uptime,
                "status": "running",
            });
            
            let mut event = factory.create_event(event_types::sinex::AUTOMATON_HEARTBEAT, payload);
            
            if let Some(ts) = timestamp {
                event.ts_orig = Some(ts);
            }
            
            event
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
                    let timestamp = start_time + chrono::Duration::seconds((i as i64) * (interval as i64));
                    (
                        Just(source),
                        event_types(),
                        event_payloads(),
                        Just(timestamp),
                    )
                        .prop_map(|(source, event_type, payload, ts)| {
                            TestEventBuilder::new(&source, &event_type)
                                .with_payload(payload)
                                .with_timestamp(ts)
                                .build()
                        })
                })
                .collect::<Vec<_>>()
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
        let factory = EventFactory::new(sources::FS);
        
        vec![
            // File created
            Just({
                let mut event = factory.create_event(
                    event_types::filesystem::FILE_CREATED,
                    json!({"path": &path, "size": 0})
                );
                event.ts_orig = Some(base_time);
                event
            }),
            // File modified multiple times
            Just({
                let mut event = factory.create_event(
                    event_types::filesystem::FILE_MODIFIED,
                    json!({"path": &path, "size": 100})
                );
                event.ts_orig = Some(base_time + chrono::Duration::seconds(5));
                event
            }),
            Just({
                let mut event = factory.create_event(
                    event_types::filesystem::FILE_MODIFIED,
                    json!({"path": &path, "size": 200})
                );
                event.ts_orig = Some(base_time + chrono::Duration::seconds(10));
                event
            }),
            // File deleted
            Just({
                let mut event = factory.create_event(
                    event_types::filesystem::FILE_DELETED,
                    json!({"path": &path})
                );
                event.ts_orig = Some(base_time + chrono::Duration::seconds(20));
                event
            }),
        ]
    })
}

// ===== Helper Strategies =====

pub fn event_sources() -> impl Strategy<Value = &'static str> {
    prop_oneof![
        Just(sources::FS),
        Just(sources::SHELL_KITTY),
        Just(sources::WM_HYPRLAND),
        Just(sources::CLIPBOARD),
        Just(sources::SINEX),
        Just("test"),
    ]
}

pub fn event_types() -> impl Strategy<Value = String> {
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

pub fn event_payloads() -> impl Strategy<Value = Value> {
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

pub fn valid_timestamps() -> impl Strategy<Value = DateTime<Utc>> {
    // Generate timestamps between 2020 and 2030
    (1577836800i64..=1893456000i64).prop_map(|ts| DateTime::from_timestamp(ts, 0).unwrap())
}

pub fn file_paths() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("/home/user/document.txt".to_string()),
        Just("/tmp/cache/file.json".to_string()),
        Just("/var/log/system.log".to_string()),
        Just("/home/user/code/project/src/main.rs".to_string()),
        Just("/home/user/.config/app/settings.toml".to_string()),
        "[a-z]+/[a-z]+\\.[a-z]{2,4}".prop_map(|s| format!("/home/user/{}", s)),
    ]
}

pub fn shell_commands() -> impl Strategy<Value = String> {
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

pub fn window_classes() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("firefox".to_string()),
        Just("kitty".to_string()),
        Just("code".to_string()),
        Just("chromium".to_string()),
        Just("nautilus".to_string()),
        "[A-Z][a-z]+".prop_map(|s| s),
    ]
}

pub fn window_titles() -> impl Strategy<Value = String> {
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

// ===== Enhanced Property Builders =====

/// Strategy for generating events with complex relationships
pub fn correlated_event_sequence() -> impl Strategy<Value = Vec<RawEvent>> {
    (1usize..=10, ulids()).prop_flat_map(|(count, parent_id)| {
        proptest::collection::vec(
            (Just(parent_id), 0usize..count, event_payloads()).prop_map(
                move |(parent, index, mut payload)| {
                    // Add correlation data
                    if let Value::Object(ref mut map) = payload {
                        map.insert("parent_id".to_string(), json!(parent.to_string()));
                        map.insert("sequence_index".to_string(), json!(index));
                        map.insert("correlation_id".to_string(), json!(format!("{}-{}", parent, index)));
                    }
                    
                    let mut event = EventFactory::new("correlated").create_event(
                        "sequence.event",
                        payload
                    );
                    
                    // Set source event IDs to show relationship
                    event.source_event_ids = Some(vec![parent]);
                    event
                }
            ),
            count
        )
    })
}

/// Strategy for generating events with realistic error scenarios
pub fn error_scenario_events() -> impl Strategy<Value = RawEvent> {
    prop_oneof![
        // Network timeout scenario
        Just(json!({
            "error": "NetworkTimeout",
            "details": {
                "host": "api.example.com",
                "timeout_ms": 30000,
                "retry_count": 3
            }
        })),
        // Permission denied scenario
        Just(json!({
            "error": "PermissionDenied",
            "details": {
                "path": "/etc/sensitive/config",
                "operation": "read",
                "user": "test_user"
            }
        })),
        // Resource exhausted scenario
        Just(json!({
            "error": "ResourceExhausted",
            "details": {
                "resource": "memory",
                "limit": "4GB",
                "requested": "8GB"
            }
        })),
        // Invalid input scenario
        Just(json!({
            "error": "InvalidInput",
            "details": {
                "field": "email",
                "value": "not-an-email",
                "expected": "valid email format"
            }
        }))
    ].prop_flat_map(|error_payload| {
        (event_sources(), Just(error_payload)).prop_map(|(source, payload)| {
            EventFactory::new(source).create_event("error.occurred", payload)
        })
    })
}

/// Strategy for generating events with realistic metadata patterns
pub fn metadata_rich_events() -> impl Strategy<Value = RawEvent> {
    (
        event_sources(),
        event_types(),
        event_payloads(),
        prop::option::of(ulids()),
        prop::option::of(0u64..1_000_000u64),
        prop::option::of(0u64..1_000_000u64),
    ).prop_map(|(source, event_type, mut payload, material_id, offset_start, offset_end)| {
        // Enrich payload with metadata
        if let Value::Object(ref mut map) = payload {
            map.insert("_metadata".to_string(), json!({
                "version": "1.0",
                "processor": "property_test",
                "environment": "test",
                "tags": ["test", "property", "automated"]
            }));
        }
        
        let mut event = EventFactory::new(source).create_event(&event_type, payload);
        
        // Add source material references
        if let Some(id) = material_id {
            event.source_material_id = Some(id);
            event.source_material_offset_start = offset_start.map(|o| o as i64);
            event.source_material_offset_end = offset_end.map(|o| o as i64);
        }
        
        event
    })
}

/// Strategy for generating events that test boundary conditions
pub fn boundary_condition_events() -> impl Strategy<Value = RawEvent> {
    prop_oneof![
        // Empty payload
        Just((json!({}), "boundary.empty")),
        // Single field payload
        Just((json!({"field": "value"}), "boundary.single")),
        // Maximum safe integer
        Just((json!({"number": i64::MAX}), "boundary.max_int")),
        // Minimum safe integer
        Just((json!({"number": i64::MIN}), "boundary.min_int")),
        // Unicode boundaries
        Just((json!({"text": "\u{0000}\u{10FFFF}"}), "boundary.unicode")),
        // Array boundaries
        Just((json!({"array": vec![0; 1000]}), "boundary.large_array")),
        // Nested object limit
        Just((create_nested_json(50), "boundary.deep_nesting"))
    ].prop_flat_map(|(payload, event_type)| {
        event_sources().prop_map(move |source| {
            EventFactory::new(source).create_event(event_type, payload.clone())
        })
    })
}

/// Strategy for generating concurrent operation scenarios
pub fn concurrent_operation_events() -> impl Strategy<Value = Vec<RawEvent>> {
    (2usize..=10, ulids()).prop_flat_map(|(worker_count, shared_resource)| {
        proptest::collection::vec(
            (0usize..worker_count, 0u64..1000u64).prop_map(move |(worker_id, operation_id)| {
                let payload = json!({
                    "worker_id": worker_id,
                    "operation_id": operation_id,
                    "resource_id": shared_resource.to_string(),
                    "operation": if operation_id % 2 == 0 { "read" } else { "write" },
                    "timestamp": Utc::now().timestamp_millis()
                });
                
                EventFactory::new("concurrent_test").create_event(
                    "operation.performed",
                    payload
                )
            }),
            worker_count * 10 // Multiple operations per worker
        )
    })
}

/// Strategy for generating events with realistic performance characteristics
pub fn performance_characteristic_events() -> impl Strategy<Value = RawEvent> {
    (
        event_sources(),
        prop_oneof![
            Just((1, "small")),        // 1KB
            Just((10, "medium")),      // 10KB
            Just((100, "large")),      // 100KB
            Just((1000, "xlarge"))     // 1MB
        ],
        0u64..10000u64, // Processing time in microseconds
        0u64..100u64,   // Queue depth
    ).prop_map(|(source, (size_kb, size_class), processing_time, queue_depth)| {
        let data_size = size_kb * 1024;
        let payload = json!({
            "performance_test": true,
            "size_class": size_class,
            "data": "x".repeat(data_size),
            "metrics": {
                "processing_time_us": processing_time,
                "queue_depth": queue_depth,
                "payload_size_bytes": data_size
            }
        });
        
        EventFactory::new(source).create_event("performance.test", payload)
    })
}

// ===== Property Testing Macros =====

/// Create a property test with database support
///
/// Usage:
/// ```
/// sinex_proptest! {
///     #[sinex_test]
///     async fn test_name(
///         event in arbitrary_event(),
///         count in 1usize..10
///     ) {
///         // Test body with database access
///         let pool = ctx.db_pool();
///         // ...
///     }
/// }
/// ```
#[macro_export]
macro_rules! sinex_proptest {
    (
        #[sinex_test]
        async fn $name:ident(
            $($param:ident in $strategy:expr),* $(,)?
        ) $body:block
    ) => {
        #[sinex_test]
        async fn $name(ctx: TestContext) {
            use proptest::prelude::*;
            
            let config = ProptestConfig::with_cases(100);
            let mut runner = TestRunner::new(config);
            
            let strategy = ($($strategy,)*);
            
            runner.run(&strategy, |($($param,)*)| {
                let test_future = async {
                    $body
                };
                
                // Run the async test
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(test_future);
                
                Ok(())
            }).unwrap();
        }
    };
}

/// Create a synchronous property test
///
/// Usage:
/// ```
/// sinex_proptest_sync! {
///     fn test_name(
///         ulid in ulids(),
///         size in 0usize..1000
///     ) {
///         // Synchronous test body
///         assert!(ulid != Ulid::nil());
///     }
/// }
/// ```
#[macro_export]
macro_rules! sinex_proptest_sync {
    (
        fn $name:ident(
            $($param:ident in $strategy:expr),* $(,)?
        ) $body:block
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            proptest!(|($($param in $strategy),*)| {
                $body
            });
        }
    };
}

/// Create a property test that generates test cases based on invariants
///
/// Usage:
/// ```
/// property_invariant! {
///     name: ulid_ordering,
///     given: (a: Ulid, b: Ulid),
///     invariant: |a, b| {
///         if a < b {
///             assert!(a.to_string() < b.to_string())
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! property_invariant {
    (
        name: $name:ident,
        given: ($($param:ident : $type:ty),* $(,)?),
        invariant: $check:expr $(,)?
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            proptest!(|($(
                $param: $type,
            )*)|{
                let check_fn = $check;
                check_fn($($param),*);
            });
        }
    };
}

/// Create a property test with custom configuration
///
/// Usage:
/// ```
/// configured_proptest! {
///     #[cases(1000)]
///     #[max_shrink_iters(50)]
///     fn test_name(
///         events in arbitrary_event_batch()
///     ) {
///         assert!(!events.is_empty());
///     }
/// }
/// ```
#[macro_export]
macro_rules! configured_proptest {
    (
        #[cases($cases:expr)]
        $(#[max_shrink_iters($shrink:expr)])?
        fn $name:ident(
            $($param:ident in $strategy:expr),* $(,)?
        ) $body:block
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            let mut config = ::proptest::test_runner::Config::with_cases($cases);
            $(config.max_shrink_iters = $shrink;)?
            
            let mut runner = ::proptest::test_runner::TestRunner::new(config);
            let strategy = ($($strategy,)*);
            
            runner.run(&strategy, |($($param,)*)| {
                $body
                Ok(())
            }).unwrap();
        }
    };
}

/// Create a stateful property test that maintains state across operations
///
/// Usage:
/// ```
/// stateful_proptest! {
///     name: queue_operations,
///     state: VecDeque<Event>,
///     operations: [
///         push_front(event: Event) => {
///             state.push_front(event);
///             assert!(state.len() > 0);
///         },
///         pop_back() => {
///             let old_len = state.len();
///             state.pop_back();
///             assert_eq!(state.len(), old_len.saturating_sub(1));
///         }
///     ]
/// }
/// ```
#[macro_export]
macro_rules! stateful_proptest {
    (
        name: $name:ident,
        state: $state_type:ty,
        operations: [
            $(
                $op_name:ident($($param:ident : $param_type:ty),* $(,)?) => $op_body:block
            ),* $(,)?
        ] $(,)?
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            #[derive(Debug, Clone)]
            enum Operation {
                $(
                    $op_name { $($param: $param_type),* },
                )*
            }
            
            fn arbitrary_operation() -> impl Strategy<Value = Operation> {
                prop_oneof![
                    $(
                        any::<($($param_type,)*)>().prop_map(|($($param,)*)| {
                            Operation::$op_name { $($param),* }
                        }),
                    )*
                ]
            }
            
            proptest!(|(ops in proptest::collection::vec(arbitrary_operation(), 0..100))| {
                let mut state: $state_type = Default::default();
                
                for op in ops {
                    match op {
                        $(
                            Operation::$op_name { $($param),* } => {
                                $op_body
                            }
                        )*
                    }
                }
            });
        }
    };
}

/// Create a property test that checks multiple related properties
///
/// Usage:
/// ```
/// property_suite! {
///     name: event_properties,
///     given: arbitrary_event(),
///     properties: {
///         has_valid_id: |event| {
///             assert_ne!(event.id, Ulid::nil());
///         },
///         has_source: |event| {
///             assert!(!event.source.is_empty());
///         },
///         has_type: |event| {
///             assert!(!event.event_type.is_empty());
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! property_suite {
    (
        name: $suite_name:ident,
        given: $strategy:expr,
        properties: {
            $(
                $prop_name:ident : $check:expr
            ),* $(,)?
        } $(,)?
    ) => {
        mod $suite_name {
            use super::*;
            use proptest::prelude::*;
            
            $(
                #[test]
                fn $prop_name() {
                    proptest!(|(value in $strategy)| {
                        let check_fn = $check;
                        check_fn(&value);
                    });
                }
            )*
        }
    };
}

/// Create a regression test from a failing property test case
///
/// Usage:
/// ```
/// regression_test! {
///     name: specific_ulid_case,
///     // This value caused a failure in property testing
///     input: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
///     test: |ulid| {
///         assert_eq!(ulid.to_string().len(), 26);
///     }
/// }
/// ```
#[macro_export]
macro_rules! regression_test {
    (
        name: $name:ident,
        input: $input:expr,
        test: $test:expr $(,)?
    ) => {
        #[test]
        fn $name() {
            let input = $input;
            let test_fn = $test;
            test_fn(input);
        }
    };
}

/// Create a property test that compares two implementations
///
/// Usage:
/// ```
/// differential_proptest! {
///     name: json_parsing,
///     input: arbitrary_json_string(),
///     implementations: {
///         serde: |s| serde_json::from_str::<Value>(s),
///         custom: |s| custom_json_parser::parse(s),
///     }
/// }
/// ```
#[macro_export]
macro_rules! differential_proptest {
    (
        name: $name:ident,
        input: $strategy:expr,
        implementations: {
            $impl1:ident : $fn1:expr,
            $impl2:ident : $fn2:expr $(,)?
        } $(,)?
    ) => {
        #[test]
        fn $name() {
            use proptest::prelude::*;
            
            proptest!(|(input in $strategy)| {
                let result1 = $fn1(&input);
                let result2 = $fn2(&input);
                
                match (result1, result2) {
                    (Ok(v1), Ok(v2)) => {
                        assert_eq!(v1, v2, 
                                   "Implementations {} and {} should produce same result",
                                   stringify!($impl1), stringify!($impl2));
                    }
                    (Err(_), Err(_)) => {
                        // Both failed - consistent
                    }
                    _ => {
                        panic!("Implementations {} and {} disagree on validity",
                               stringify!($impl1), stringify!($impl2));
                    }
                }
            });
        }
    };
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
    
    // Example usage of the macros
    
    sinex_proptest_sync! {
        fn example_ulid_property(
            ulid in ulids()
        ) {
            assert_ne!(ulid, Ulid::nil());
            assert_eq!(ulid.to_string().len(), 26);
        }
    }
    
    property_invariant! {
        name: example_invariant,
        given: (a: u32, b: u32),
        invariant: |a, b| {
            assert_eq!(a + b, b + a); // Commutative property
        }
    }
    
    configured_proptest! {
        #[cases(50)]
        fn example_configured(
            events in arbitrary_event_batch()
        ) {
            assert!(events.len() <= 50);
        }
    }
    
    property_suite! {
        name: example_suite,
        given: arbitrary_event(),
        properties: {
            has_id: |event: &RawEvent| {
                assert_ne!(event.id, Ulid::nil());
            },
            has_source: |event: &RawEvent| {
                assert!(!event.source.is_empty());
            }
        }
    }
}