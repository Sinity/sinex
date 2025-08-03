// Event Type System Tests
//
// Tests for the strongly-typed event system that replaced EventRegistry.
// This replaces the commented-out EventRegistry tests with equivalent
// functionality tests for the new architecture.

use sinex_test_utils::prelude::*;
use rstest::*;
use insta::assert_json_snapshot;
use tracing_test::traced_test;

use sinex_types::events::{
    event_types, sources, strongly_typed_events::*, EventEnvelope, EventFactory, RawEvent,
};
use std::collections::HashSet;

// =============================================================================
// EVENT TYPE CONSTANTS AND VALIDATION TESTS
// =============================================================================

/// Test that event type constants are properly defined and consistent
#[rstest]
#[case(sources::FS, "fs")]
#[case(sources::SHELL_KITTY, "shell.kitty")]
#[case(sources::SHELL_ATUIN, "shell.atuin")]
#[case(sources::SHELL_HISTORY, "shell.history")]
#[case(sources::SHELL_RECORDING, "shell.recording")]
#[case(sources::SHELL_SCROLLBACK, "shell.scrollback")]
#[case(sources::WM_HYPRLAND, "wm.hyprland")]
#[case(sources::CLIPBOARD, "clipboard")]
#[case(sources::DBUS, "dbus")]
#[case(sources::JOURNALD, "journald")]
#[case(sources::SINEX, "sinex")]
#[sinex_test]
async fn test_event_type_constants(
    _ctx: TestContext,
    #[case] source_const: &'static str,
    #[case] expected_value: &'static str,
) -> color_eyre::eyre::Result<()> {
    assert_eq!(source_const, expected_value);
    Ok(())
}

/// Test that sources follow consistent naming patterns
#[rstest]
#[case(sources::SHELL_KITTY, "shell.")]
#[case(sources::SHELL_ATUIN, "shell.")]
#[case(sources::SHELL_HISTORY, "shell.")]
#[case(sources::WM_HYPRLAND, "wm.")]
#[sinex_test]
async fn test_source_naming_patterns(
    _ctx: TestContext,
    #[case] source: &'static str,
    #[case] expected_prefix: &'static str,
) -> color_eyre::eyre::Result<()> {
    assert!(source.starts_with(expected_prefix), 
        "Source '{}' should start with '{}'", source, expected_prefix);
    Ok(())
}

/// Test event type validation through EventEnvelope variants
#[sinex_test]
async fn test_event_envelope_coverage(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that EventEnvelope covers all major event categories
    let event_factory = EventFactory::new("test");

    // Test filesystem events
    let file_event = event_factory
        .filesystem()
        .path("/test/file.txt")
        .created()
        .build();
    assert!(file_event.is_raw_event());
    assert_eq!(file_event.source, sources::FS);
    assert_eq!(file_event.event_type, "file.created");

    // Test terminal events
    let terminal_event = event_factory
        .terminal()
        .command("ls -la")
        .working_dir("/home/user")
        .build_executed();
    assert!(terminal_event.is_raw_event());
    assert_eq!(terminal_event.source, sources::SHELL_KITTY);
    assert_eq!(terminal_event.event_type, "command.executed");

    // Test clipboard events
    let clipboard_event = event_factory.clipboard().text("test content").build();
    assert!(clipboard_event.is_raw_event());
    assert_eq!(clipboard_event.source, sources::CLIPBOARD);
    assert_eq!(clipboard_event.event_type, "content.copied");

    // Test window manager events
    let wm_event = event_factory
        .window_manager()
        .window_title("Test Window")
        .window_class("test-app")
        .window_focused()
        .build();
    assert!(wm_event.is_raw_event());
    assert_eq!(wm_event.source, sources::WM_HYPRLAND);
    assert_eq!(wm_event.event_type, "window.focused");

    Ok(())
}

/// Test event type naming consistency and patterns
#[sinex_test]
async fn test_event_type_naming_patterns(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let event_factory = EventFactory::new("test");

    // Test filesystem event naming follows pattern: object.action
    let fs_events = vec![
        event_factory.filesystem().path("/test").created().build(),
        event_factory.filesystem().path("/test").modified().build(),
        event_factory.filesystem().path("/test").deleted().build(),
        event_factory
            .filesystem()
            .path("/new")
            .moved_from("/old")
            .build(),
        event_factory.filesystem().path("/test").created().build(),
        event_factory.filesystem().path("/test").deleted().build(),
    ];

    for event in fs_events {
        assert!(
            event.event_type.contains('.'),
            "Event type should contain dot separator: {}",
            event.event_type
        );

        // Should be object.action format
        let parts: Vec<&str> = event.event_type.split('.').collect();
        assert_eq!(
            parts.len(),
            2,
            "Event type should have exactly 2 parts: {}",
            event.event_type
        );

        let object = parts[0];
        let action = parts[1];
        assert!(
            ["file", "dir"].contains(&object),
            "Filesystem events should start with 'file' or 'dir': {}",
            event.event_type
        );
        assert!(
            ["created", "modified", "deleted", "moved"].contains(&action),
            "Filesystem actions should be valid: {}",
            event.event_type
        );
    }

    // Test terminal event naming patterns
    let terminal_events = vec![
        event_factory
            .terminal()
            .command("test")
            .working_dir("/")
            .build_executed(),
        event_factory
            .terminal()
            .command("test")
            .working_dir("/")
            .exit_code(0)
            .build_completed(),
        event_factory.terminal().command("bash").build_executed(),
        event_factory
            .terminal()
            .command("bash")
            .exit_code(0)
            .build_completed(),
    ];

    for event in terminal_events {
        let parts: Vec<&str> = event.event_type.split('.').collect();
        assert_eq!(
            parts.len(),
            2,
            "Terminal event should have 2 parts: {}",
            event.event_type
        );

        let object = parts[0];
        let action = parts[1];
        assert!(
            ["command", "session"].contains(&object),
            "Terminal events should start with 'command' or 'session': {}",
            event.event_type
        );
        assert!(
            ["executed", "completed", "started", "ended"].contains(&action),
            "Terminal actions should be valid: {}",
            event.event_type
        );
    }

    Ok(())
}

// =============================================================================
// SOURCE TO EVENT TYPE MAPPING TESTS
// =============================================================================

/// Test source to event type mapping consistency
#[rstest]
#[case::filesystem(
    "filesystem",
    sources::FS,
    vec![
        ("created", "/test/file1.txt"),
        ("modified", "/test/file2.txt"),
        ("deleted", "/test/file3.txt"),
        ("created", "/test/dir1"),
        ("deleted", "/test/dir2")
    ]
)]
#[case::terminal(
    "terminal",
    sources::SHELL_KITTY,
    vec![
        ("executed", "test-cmd"),
        ("completed", "test-cmd"),
        ("executed", "bash"),
        ("completed", "bash")
    ]
)]
#[case::clipboard(
    "clipboard",
    sources::CLIPBOARD,
    vec![
        ("text", "content1"),
        ("text", "content2")
    ]
)]
#[case::window_manager(
    "window_manager",
    sources::WM_HYPRLAND,
    vec![
        ("window", "TestWindow"),
        ("window", "AnotherWindow"),
        ("focused", "FocusedWindow"),
        ("workspace", "workspace1")
    ]
)]
#[sinex_test]
#[traced_test]
async fn test_source_event_type_mapping(
    ctx: TestContext,
    #[case] source_type: &str,
    #[case] expected_source: &'static str,
    #[case] test_cases: Vec<(&str, &str)>,
) -> color_eyre::eyre::Result<()> {
    let event_factory = EventFactory::new("test");
    let mut created_events = Vec::new();

    // Create events based on source type
    for (action, param) in test_cases {
        let event = match source_type {
            "filesystem" => match action {
                "created" => event_factory.filesystem().path(param).created().build(),
                "modified" => event_factory.filesystem().path(param).modified().build(),
                "deleted" => event_factory.filesystem().path(param).deleted().build(),
                _ => panic!("Unknown filesystem action: {}", action),
            },
            "terminal" => match action {
                "executed" => event_factory.terminal().command(param).working_dir("/").build_executed(),
                "completed" => event_factory.terminal().command(param).working_dir("/").exit_code(0).build_completed(),
                _ => panic!("Unknown terminal action: {}", action),
            },
            "clipboard" => event_factory.clipboard().text(param).build(),
            "window_manager" => match action {
                "window" => event_factory.window_manager().window_title(param).window_class("app").build(),
                "focused" => event_factory.window_manager().window_title(param).window_class("app").window_focused().build(),
                "workspace" => event_factory.window_manager().workspace_id(param).build(),
                _ => panic!("Unknown window manager action: {}", action),
            },
            _ => panic!("Unknown source type: {}", source_type),
        };
        
        created_events.push(event);
    }

    // Verify all events map to expected source
    for event in &created_events {
        assert_eq!(
            event.source,
            expected_source,
            "Event from {} should map to '{}' source",
            source_type,
            expected_source
        );
    }

    // Create snapshot of the source mapping results
    ctx.snapshot_json(&format!("source_mapping_{}", source_type), &serde_json::json!({
        "source_type": source_type,
        "expected_source": expected_source,
        "events": created_events.iter().map(|e| {
            serde_json::json!({
                "source": e.source,
                "event_type": e.event_type,
                "payload_keys": e.payload.as_object().map(|obj| obj.keys().collect::<Vec<_>>()).unwrap_or_default()
            })
        }).collect::<Vec<_>>()
    }));

    Ok(())
}

/// Test that source names are unique and don't conflict
#[rstest]
#[case(sources::FS)]
#[case(sources::SHELL_KITTY)]
#[case(sources::SHELL_ATUIN)]
#[case(sources::SHELL_HISTORY)]
#[case(sources::SHELL_RECORDING)]
#[case(sources::SHELL_SCROLLBACK)]
#[case(sources::WM_HYPRLAND)]
#[case(sources::CLIPBOARD)]
#[case(sources::DBUS)]
#[case(sources::JOURNALD)]
#[case(sources::SINEX)]
#[sinex_test]
#[traced_test]
async fn test_source_name_uniqueness(
    ctx: TestContext,
    #[case] source: &'static str,
) -> color_eyre::eyre::Result<()> {
    // Collect all sources for uniqueness verification
    let all_sources = vec![
        sources::FS,
        sources::SHELL_KITTY,
        sources::SHELL_ATUIN,
        sources::SHELL_HISTORY,
        sources::SHELL_RECORDING,
        sources::SHELL_SCROLLBACK,
        sources::WM_HYPRLAND,
        sources::CLIPBOARD,
        sources::DBUS,
        sources::JOURNALD,
        sources::SINEX,
    ];
    
    // Count occurrences of this source
    let count = all_sources.iter().filter(|&&s| s == source).count();
    assert_eq!(count, 1, "Source name '{}' should appear exactly once", source);
    
    // Validate source format
    assert!(!source.is_empty(), "Source name cannot be empty");
    assert!(!source.starts_with('.'), "Source name cannot start with dot");
    assert!(!source.ends_with('.'), "Source name cannot end with dot");
    
    // Create snapshot for this source validation
    ctx.snapshot_json(&format!("source_validation_{}", source.replace('.', "_")), &serde_json::json!({
        "source": source,
        "length": source.len(),
        "contains_dot": source.contains('.'),
        "dot_count": source.matches('.').count(),
        "is_unique": count == 1
    }));

    Ok(())
}

// =============================================================================
// EVENT ENUMERATION AND DISCOVERY TESTS
// =============================================================================

/// Test event type enumeration through EventEnvelope variants
#[rstest]
#[case::filesystem(
    "filesystem",
    sources::FS,
    vec![
        ("file", "created", "/test/file1.txt"),
        ("file", "modified", "/test/file2.txt"),
        ("file", "deleted", "/test/file3.txt"),
        ("dir", "created", "/test/dir1"),
        ("dir", "deleted", "/test/dir2")
    ],
    vec!["file.created", "file.modified", "file.deleted", "dir.created", "dir.deleted"]
)]
#[case::terminal(
    "terminal",
    sources::SHELL_KITTY,
    vec![
        ("command", "executed", "test-cmd"),
        ("command", "completed", "test-cmd"),
        ("command", "executed", "bash"),
        ("command", "completed", "bash")
    ],
    vec!["command.executed", "command.completed"]
)]
#[case::clipboard(
    "clipboard",
    sources::CLIPBOARD,
    vec![
        ("content", "copied", "content1"),
        ("content", "copied", "content2")
    ],
    vec!["content.copied"]
)]
#[case::window_manager(
    "window_manager",
    sources::WM_HYPRLAND,
    vec![
        ("window", "created", "TestWindow"),
        ("window", "created", "AnotherWindow"),
        ("window", "focused", "FocusedWindow"),
        ("workspace", "switched", "workspace1")
    ],
    vec!["window.created", "window.focused", "workspace.switched"]
)]
#[sinex_test]
#[traced_test]
async fn test_event_type_enumeration(
    ctx: TestContext,
    #[case] source_category: &str,
    #[case] expected_source: &'static str,
    #[case] event_specs: Vec<(&str, &str, &str)>,
    #[case] expected_event_types: Vec<&str>,
) -> color_eyre::eyre::Result<()> {
    let event_factory = EventFactory::new("test");
    let mut created_events = Vec::new();

    // Create events based on specifications
    for (object, action, param) in event_specs {
        let event = match source_category {
            "filesystem" => match (object, action) {
                ("file", "created") => event_factory.filesystem().path(param).created().build(),
                ("file", "modified") => event_factory.filesystem().path(param).modified().build(),
                ("file", "deleted") => event_factory.filesystem().path(param).deleted().build(),
                ("dir", "created") => event_factory.filesystem().path(param).created().build(),
                ("dir", "deleted") => event_factory.filesystem().path(param).deleted().build(),
                _ => panic!("Unknown filesystem event: {}.{}", object, action),
            },
            "terminal" => match (object, action) {
                ("command", "executed") => event_factory.terminal().command(param).working_dir("/").build_executed(),
                ("command", "completed") => event_factory.terminal().command(param).working_dir("/").exit_code(0).build_completed(),
                _ => panic!("Unknown terminal event: {}.{}", object, action),
            },
            "clipboard" => event_factory.clipboard().text(param).build(),
            "window_manager" => match (object, action) {
                ("window", "created") => event_factory.window_manager().window_title(param).window_class("app").build(),
                ("window", "focused") => event_factory.window_manager().window_title(param).window_class("app").window_focused().build(),
                ("workspace", "switched") => event_factory.window_manager().workspace_id(param).build(),
                _ => panic!("Unknown window manager event: {}.{}", object, action),
            },
            _ => panic!("Unknown source category: {}", source_category),
        };
        
        created_events.push(event);
    }

    // Verify all events map to expected source
    for event in &created_events {
        assert_eq!(
            event.source,
            expected_source,
            "Events from {} should map to '{}' source",
            source_category,
            expected_source
        );
    }

    // Verify events have expected types
    let actual_types: HashSet<String> = created_events.iter().map(|e| e.event_type.clone()).collect();
    for expected_type in &expected_event_types {
        assert!(
            actual_types.contains(*expected_type),
            "Should contain event type '{}' for {} category",
            expected_type,
            source_category
        );
    }

    // Create snapshot of enumeration results
    ctx.snapshot_json(&format!("event_enumeration_{}", source_category), &serde_json::json!({
        "source_category": source_category,
        "expected_source": expected_source,
        "expected_event_types": expected_event_types,
        "created_events": created_events.iter().map(|e| {
            serde_json::json!({
                "source": e.source,
                "event_type": e.event_type,
                "payload_structure": e.payload.as_object().map(|obj| {
                    obj.keys().collect::<Vec<_>>()
                }).unwrap_or_default()
            })
        }).collect::<Vec<_>>()
    }));

    Ok(())
}

// =============================================================================
// STRONGLY-TYPED EVENT SYSTEM TESTS
// =============================================================================

/// Test TypedRawEvent to RawEvent conversion
#[sinex_test]
async fn test_typed_raw_event_conversion(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use chrono::Utc;
    use sinex_types::events::strongly_typed_events::*;
    use sinex_types::ulid::Ulid;

    // Create a typed event
    let payload = FileCreatedPayload {
        path: "/test/file.txt".to_string(),
        size: 1024,
        permissions: Some(0o644),
        created_at: Utc::now(),
    };

    let typed_event = TypedRawEvent {
        id: Ulid::new(),
        source: sources::FS.to_string(),
        event_type: "file.created".to_string(),
        payload: payload.clone(),
        host: "test-host".to_string(),
        ingestor_version: "1.0.0".to_string(),
        ts_ingest: Utc::now(),
        ts_orig: Some(Utc::now()),
    };

    // Convert to JSON-based RawEvent
    let raw_event = typed_event.to_json_event();

    // Verify conversion
    assert_eq!(raw_event.source, sources::FS);
    assert_eq!(raw_event.event_type, "file.created");
    assert_eq!(raw_event.host, "test-host");
    assert_eq!(raw_event.ingestor_version, Some("1.0.0".to_string()));
    assert!(raw_event.is_raw_event());
    assert!(raw_event.source_event_ids.is_none());

    // Verify payload conversion
    let deserialized_payload: FileCreatedPayload = serde_json::from_value(raw_event.payload)?;
    assert_eq!(deserialized_payload.path, payload.path);
    assert_eq!(deserialized_payload.size, payload.size);
    assert_eq!(deserialized_payload.permissions, payload.permissions);
    assert_eq!(
        deserialized_payload.created_at.timestamp(),
        payload.created_at.timestamp()
    );

    Ok(())
}

/// Test EventEnvelope type safety
#[sinex_test]
async fn test_event_envelope_type_safety(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use chrono::Utc;
    use sinex_types::events::strongly_typed_events::*;
    use sinex_types::ulid::Ulid;

    // Create typed events and envelopes
    let file_payload = FileCreatedPayload {
        path: "/test/file.txt".to_string(),
        size: 1024,
        permissions: Some(0o644),
        created_at: Utc::now(),
    };

    let file_event = TypedRawEvent {
        id: Ulid::new(),
        source: sources::FS.to_string(),
        event_type: "file.created".to_string(),
        payload: file_payload,
        host: "test-host".to_string(),
        ingestor_version: "1.0.0".to_string(),
        ts_ingest: Utc::now(),
        ts_orig: Some(Utc::now()),
    };

    let command_payload = CommandExecutedPayload {
        command: "ls -la".to_string(),
        working_directory: Some("/home/user".to_string()),
        exit_status: Some(0),
        execution_time_ms: Some(150),
        shell_type: Some("bash".to_string()),
    };

    let command_event = TypedRawEvent {
        id: Ulid::new(),
        source: sources::SHELL_KITTY.to_string(),
        event_type: "command.executed".to_string(),
        payload: command_payload,
        host: "test-host".to_string(),
        ingestor_version: "1.0.0".to_string(),
        ts_ingest: Utc::now(),
        ts_orig: Some(Utc::now()),
    };

    // Test envelope variants
    let file_envelope = EventEnvelope::FileCreated(file_event);
    let command_envelope = EventEnvelope::CommandExecuted(command_event);

    // Test that envelopes maintain type safety
    match file_envelope {
        EventEnvelope::FileCreated(event) => {
            assert_eq!(event.source, sources::FS);
            assert_eq!(event.event_type, "file.created");
            assert_eq!(event.payload.path, "/test/file.txt");
        }
        _ => panic!("Expected FileCreated envelope"),
    }

    match command_envelope {
        EventEnvelope::CommandExecuted(event) => {
            assert_eq!(event.source, sources::SHELL_KITTY);
            assert_eq!(event.event_type, "command.executed");
            assert_eq!(event.payload.command, "ls -la");
        }
        _ => panic!("Expected CommandExecuted envelope"),
    }

    Ok(())
}

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

/// Test concurrent event creation and processing
#[sinex_test]
async fn test_concurrent_event_creation(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use std::sync::Arc;
    use tokio::task;

    let event_factory = Arc::new(EventFactory::new("test"));
    let mut handles = vec![];

    // Create multiple tasks that create events concurrently
    for i in 0..10 {
        let factory = Arc::clone(&event_factory);
        let handle = task::spawn(async move {
            let mut events = Vec::new();

            // Create various types of events
            events.push(
                factory
                    .filesystem()
                    .path(&format!("/test/file{}.txt", i))
                    .created()
                    .build(),
            );
            events.push(
                factory
                    .terminal()
                    .command(&format!("cmd{}", i))
                    .working_dir("/")
                    .build_executed(),
            );
            events.push(factory.clipboard().text(&format!("content{}", i)).build());
            events.push(
                factory
                    .window_manager()
                    .window_title(&format!("Window{}", i))
                    .window_class("app")
                    .window_focused()
                    .build(),
            );

            // Return events and task number
            Ok::<(Vec<RawEvent>, usize), color_eyre::eyre::Error>((events, i))
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(handles).await;

    // Verify all tasks completed successfully
    assert_eq!(results.len(), 10);
    let mut all_events = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        let (events, task_num) = result.unwrap()?;
        assert_eq!(task_num, i);
        assert_eq!(events.len(), 4);
        all_events.extend(events);
    }

    // Verify we have the expected number of events
    assert_eq!(all_events.len(), 40);

    // Verify event types are distributed correctly
    let fs_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.source == sources::FS)
        .collect();
    let terminal_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.source == sources::SHELL_KITTY)
        .collect();
    let clipboard_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.source == sources::CLIPBOARD)
        .collect();
    let wm_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.source == sources::WM_HYPRLAND)
        .collect();

    assert_eq!(fs_events.len(), 10);
    assert_eq!(terminal_events.len(), 10);
    assert_eq!(clipboard_events.len(), 10);
    assert_eq!(wm_events.len(), 10);

    Ok(())
}

/// Test that event IDs are unique even under concurrent creation
#[sinex_test]
async fn test_event_id_uniqueness_concurrent(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::task;

    let event_factory = Arc::new(EventFactory::new("test"));
    let mut handles = vec![];

    // Create many tasks that create events concurrently
    for i in 0..20 {
        let factory = Arc::clone(&event_factory);
        let handle = task::spawn(async move {
            let mut event_ids = Vec::new();

            // Create multiple events per task
            for j in 0..5 {
                let event = factory
                    .filesystem()
                    .path(&format!("/test/file{}_{}.txt", i, j))
                    .created()
                    .build();
                event_ids.push(event.id);
            }

            Ok::<Vec<sinex_ulid::Ulid>, color_eyre::eyre::Error>(event_ids)
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(handles).await;

    // Collect all event IDs
    let mut all_ids = HashSet::new();
    for result in results {
        let ids = result.unwrap()?;
        for id in ids {
            assert!(all_ids.insert(id), "Event ID {} is not unique", id);
        }
    }

    // Verify we have the expected number of unique IDs
    assert_eq!(all_ids.len(), 100); // 20 tasks * 5 events each

    Ok(())
}
