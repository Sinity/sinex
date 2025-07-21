// Event Type System Tests
//
// Tests for the strongly-typed event system that replaced EventRegistry.
// This replaces the commented-out EventRegistry tests with equivalent
// functionality tests for the new architecture.

use crate::common::prelude::*;

use sinex_events::{
    sources, EventFactory, RawEvent,
};
use std::collections::HashSet;

// =============================================================================
// EVENT TYPE CONSTANTS AND VALIDATION TESTS
// =============================================================================

/// Test that event type constants are properly defined and consistent
#[sinex_test]
async fn test_event_type_constants(_ctx: TestContext) -> TestResult {
    // Test that source constants exist and are consistent
    assert_eq!(sources::FS, "fs");
    assert_eq!(sources::SHELL_KITTY, "shell.kitty");
    assert_eq!(sources::SHELL_ATUIN, "shell.atuin");
    assert_eq!(sources::SHELL_HISTORY, "shell.history");
    assert_eq!(sources::SHELL_RECORDING, "shell.recording");
    assert_eq!(sources::SHELL_SCROLLBACK, "shell.scrollback");
    assert_eq!(sources::WM_HYPRLAND, "wm.hyprland");
    assert_eq!(sources::CLIPBOARD, "clipboard");
    assert_eq!(sources::DBUS, "dbus");
    assert_eq!(sources::JOURNALD, "journald");
    assert_eq!(sources::SINEX, "sinex");

    // Test that sources follow consistent naming patterns
    assert!(sources::SHELL_KITTY.starts_with("shell."));
    assert!(sources::SHELL_ATUIN.starts_with("shell."));
    assert!(sources::SHELL_HISTORY.starts_with("shell."));
    assert!(sources::WM_HYPRLAND.starts_with("wm."));

    Ok(())
}

/// Test event type validation through EventEnvelope variants
#[sinex_test]
async fn test_event_envelope_coverage(_ctx: TestContext) -> TestResult {
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
    let clipboard_event = event_factory
        .clipboard()
        .text("test content")
        .build();
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
async fn test_event_type_naming_patterns(_ctx: TestContext) -> TestResult {
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
        event_factory.terminal().command("bash").exit_code(0).build_completed(),
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
#[sinex_test]
async fn test_source_event_type_mapping(_ctx: TestContext) -> TestResult {
    let event_factory = EventFactory::new("test");

    // Test that all filesystem events map to 'fs' source
    let fs_events = vec![
        event_factory.filesystem().path("/test").created().build(),
        event_factory.filesystem().path("/test").modified().build(),
        event_factory.filesystem().path("/test").deleted().build(),
        event_factory.filesystem().path("/test").created().build(),
        event_factory.filesystem().path("/test").deleted().build(),
    ];

    for event in fs_events {
        assert_eq!(
            event.source,
            sources::FS,
            "Filesystem events should map to 'fs' source"
        );
    }

    // Test that all terminal events map to 'shell.kitty' source
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
        event_factory.terminal().command("bash").exit_code(0).build_completed(),
    ];

    for event in terminal_events {
        assert_eq!(
            event.source,
            sources::SHELL_KITTY,
            "Terminal events should map to 'shell.kitty' source"
        );
    }

    // Test that all clipboard events map to 'clipboard' source
    let clipboard_events = vec![
        event_factory.clipboard().text("test").build(),
        event_factory.clipboard().text("test").build(),
    ];

    for event in clipboard_events {
        assert_eq!(
            event.source,
            sources::CLIPBOARD,
            "Clipboard events should map to 'clipboard' source"
        );
    }

    // Test that all window manager events map to 'wm.hyprland' source
    let wm_events = vec![
        event_factory
            .window_manager()
            .window_title("Test")
            .window_class("app")
            .build(),
        event_factory
            .window_manager()
            .window_title("Test")
            .window_class("app")
            .build(),
        event_factory
            .window_manager()
            .window_title("Test")
            .window_class("app")
            .window_focused()
            .build(),
        event_factory
            .window_manager()
            .workspace_id("workspace1")
            .build(),
    ];

    for event in wm_events {
        assert_eq!(
            event.source,
            sources::WM_HYPRLAND,
            "Window manager events should map to 'wm.hyprland' source"
        );
    }

    Ok(())
}

/// Test that source names are unique and don't conflict
#[sinex_test]
async fn test_source_name_uniqueness(_ctx: TestContext) -> TestResult {
    let sources = vec![
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

    let mut seen_sources = HashSet::new();
    for source in sources {
        assert!(
            seen_sources.insert(source),
            "Source name '{}' is not unique",
            source
        );
    }

    Ok(())
}

// =============================================================================
// EVENT ENUMERATION AND DISCOVERY TESTS
// =============================================================================

/// Test event type enumeration through EventEnvelope variants
#[sinex_test]
async fn test_event_type_enumeration(_ctx: TestContext) -> TestResult {
    // Create events of different types and verify they can be enumerated
    let event_factory = EventFactory::new("test");

    let events = vec![
        event_factory.filesystem().path("/test").created().build(),
        event_factory.filesystem().path("/test").modified().build(),
        event_factory.filesystem().path("/test").deleted().build(),
        event_factory.filesystem().path("/test").created().build(),
        event_factory.filesystem().path("/test").deleted().build(),
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
        event_factory.terminal().command("bash").exit_code(0).build_completed(),
        event_factory.clipboard().text("test").build(),
        event_factory.clipboard().text("test").build(),
        event_factory
            .window_manager()
            .window_title("Test")
            .window_class("app")
            .build(),
        event_factory
            .window_manager()
            .window_title("Test")
            .window_class("app")
            .build(),
        event_factory
            .window_manager()
            .window_title("Test")
            .window_class("app")
            .window_focused()
            .build(),
        event_factory
            .window_manager()
            .workspace_id("workspace1")
            .build(),
    ];

    // Group events by category
    let mut fs_events = Vec::new();
    let mut terminal_events = Vec::new();
    let mut clipboard_events = Vec::new();
    let mut wm_events = Vec::new();

    for event in events {
        match event.source.as_str() {
            sources::FS => fs_events.push(event),
            sources::SHELL_KITTY => terminal_events.push(event),
            sources::CLIPBOARD => clipboard_events.push(event),
            sources::WM_HYPRLAND => wm_events.push(event),
            _ => panic!("Unexpected source: {}", event.source),
        }
    }

    // Verify we have events in each category
    assert!(!fs_events.is_empty(), "Should have filesystem events");
    assert!(!terminal_events.is_empty(), "Should have terminal events");
    assert!(!clipboard_events.is_empty(), "Should have clipboard events");
    assert!(!wm_events.is_empty(), "Should have window manager events");

    // Verify filesystem events have expected types
    let fs_types: HashSet<String> = fs_events.iter().map(|e| e.event_type.clone()).collect();
    assert!(fs_types.contains("file.created"));
    assert!(fs_types.contains("file.modified"));
    assert!(fs_types.contains("file.deleted"));
    assert!(fs_types.contains("dir.created"));
    assert!(fs_types.contains("dir.deleted"));

    // Verify terminal events have expected types
    let terminal_types: HashSet<String> = terminal_events
        .iter()
        .map(|e| e.event_type.clone())
        .collect();
    assert!(terminal_types.contains("command.executed"));
    assert!(terminal_types.contains("command.completed"));
    assert!(terminal_types.contains("session.started"));
    assert!(terminal_types.contains("session.ended"));

    Ok(())
}

// =============================================================================
// STRONGLY-TYPED EVENT SYSTEM TESTS
// =============================================================================

/// Test TypedRawEvent to RawEvent conversion
#[sinex_test]
async fn test_typed_raw_event_conversion(_ctx: TestContext) -> TestResult {
    use chrono::Utc;
    use sinex_events::strongly_typed_events::*;
    use sinex_ulid::Ulid;

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
    assert_eq!(deserialized_payload.created_at.timestamp(), payload.created_at.timestamp());

    Ok(())
}

/// Test EventEnvelope type safety
#[sinex_test]
async fn test_event_envelope_type_safety(_ctx: TestContext) -> TestResult {
    use chrono::Utc;
    use sinex_events::strongly_typed_events::*;
    use sinex_ulid::Ulid;

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
async fn test_concurrent_event_creation(_ctx: TestContext) -> TestResult {
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
            events.push(
                factory
                    .clipboard()
                    .text(&format!("content{}", i))
                    .build(),
            );
            events.push(
                factory
                    .window_manager()
                    .window_title(&format!("Window{}", i))
                    .window_class("app")
                    .window_focused()
                    .build(),
            );

            // Return events and task number
            Ok::<(Vec<RawEvent>, usize), anyhow::Error>((events, i))
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
async fn test_event_id_uniqueness_concurrent(_ctx: TestContext) -> TestResult {
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

            Ok::<Vec<sinex_ulid::Ulid>, anyhow::Error>(event_ids)
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
