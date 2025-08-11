//! Event Type System Tests
//!
//! Tests for the strongly-typed event system, validating event types,
//! sources, and the modern payload system.
//!
//! Migrated from test/unit/event_type_system_test.rs to use modern patterns:
//! - TestContext instead of custom fixtures
//! - Modern Event API with Event::from_payload()
//! - Direct repository access via ctx.pool.*()
//! - Modern payload types from sinex_core::types::events::payloads
//! - color_eyre for error handling

use sinex_core::db::models::RawEvent;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::types::events::payloads::{
    AtuinCommandExecutedPayload, ClipboardCopiedPayload, FileCreatedPayload, FileDeletedPayload,
    FileModifiedPayload, KittyCommandExecutedPayload,
};
use sinex_test_utils::prelude::*;
use std::collections::HashSet;

// =============================================================================
// EVENT SOURCE CONSTANTS AND VALIDATION TESTS
// =============================================================================

/// Test event source constants and consistent naming patterns
#[sinex_test]
async fn test_event_source_patterns(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let test_cases = vec![
        ("fs-watcher", "fs"),
        ("shell.kitty", "shell."),
        ("shell.atuin", "shell."),
        ("shell.history", "shell."),
        ("terminal.kitty", "terminal."),
        ("clipboard", "clipboard"),
        ("system", "system"),
    ];

    for (source_name, expected_prefix) in test_cases {
        if expected_prefix.ends_with('.') {
            assert!(
                source_name.starts_with(expected_prefix),
                "Source '{}' should start with '{}'",
                source_name,
                expected_prefix
            );
        } else {
            // For exact matches like "clipboard", "system"
            assert_eq!(source_name, expected_prefix);
        }
    }
    Ok(())
}

/// Test that event sources follow consistent naming patterns  
#[sinex_test]
async fn test_source_naming_conventions(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let test_sources = vec![
        "fs-watcher",
        "shell.kitty",
        "shell.atuin",
        "clipboard",
        "system",
    ];

    for source in test_sources {
        let source_type = EventSource::new(source);

        // Validate using the domain type validation
        source_type
            .validate()
            .map_err(|e| color_eyre::eyre::eyre!(e))?;

        // Additional specific checks
        assert!(!source.is_empty(), "Source cannot be empty");
        assert!(!source.starts_with('.'), "Source cannot start with dot");
        assert!(!source.ends_with('.'), "Source cannot end with dot");
        assert!(
            source
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '.' || c == '-' || c == '_'),
            "Source should only contain lowercase, dots, hyphens, underscores"
        );
    }

    Ok(())
}

// =============================================================================
// EVENT TYPE VALIDATION AND PATTERNS
// =============================================================================

/// Test event type naming patterns and validation
#[sinex_test]
async fn test_event_type_validation(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let test_cases = vec![
        ("file.created", true),
        ("file.modified", true),
        ("file.deleted", true),
        ("dir.created", true),
        ("command.executed", true),
        ("command.completed", true),
        ("window.focused", true),
        ("clipboard.copied", true),
        ("", false),              // Empty should fail
        (".invalid", false),      // Starting with dot should fail
        ("invalid.", false),      // Ending with dot should fail
        ("file..created", false), // Double dots should fail
        ("File.Created", false),  // Uppercase should fail
    ];

    for (event_type_str, should_be_valid) in test_cases {
        let event_type = EventType::new(event_type_str);
        let result = event_type.validate();

        if should_be_valid {
            assert!(
                result.is_ok(),
                "Event type '{}' should be valid, but got error: {:?}",
                event_type,
                result
            );
        } else {
            assert!(
                result.is_err(),
                "Event type '{}' should be invalid but passed validation",
                event_type
            );
        }
    }

    Ok(())
}

/// Test event type hierarchical structure (object.action pattern)
#[sinex_test]
async fn test_event_type_hierarchical_structure(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let test_cases = vec![
        ("file.created", "file", "created"),
        ("file.modified", "file", "modified"),
        ("file.deleted", "file", "deleted"),
        ("dir.created", "dir", "created"),
        ("command.executed", "command", "executed"),
        ("command.completed", "command", "completed"),
        ("window.focused", "window", "focused"),
        ("clipboard.copied", "clipboard", "copied"),
    ];

    for (event_type, expected_object, expected_action) in test_cases {
        let parts: Vec<&str> = event_type.split('.').collect();
        assert_eq!(
            parts.len(),
            2,
            "Event type should have exactly 2 parts: {}",
            event_type
        );

        assert_eq!(parts[0], expected_object, "Object part should match");
        assert_eq!(parts[1], expected_action, "Action part should match");
    }

    Ok(())
}

// =============================================================================
// MODERN PAYLOAD SYSTEM TESTS
// =============================================================================

/// Test filesystem payload creation and validation
#[sinex_test]
async fn test_filesystem_payload_system(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test FileCreatedPayload
    let file_payload = FileCreatedPayload::test_default("/test/file.txt")
        .with_size(1024)
        .with_permissions(0o644);

    let file_event = Event::from_payload(file_payload);
    ctx.pool.events().insert(file_event.clone().into()).await?;

    // Verify the event was stored correctly
    assert_eq!(file_event.source.as_str(), "fs-watcher");
    assert_eq!(file_event.event_type.as_str(), "file.created");
    assert_eq!(file_event.payload.size, 1024);
    assert_eq!(file_event.payload.path.as_str(), "/test/file.txt");

    // Test FileModifiedPayload
    let modified_payload = FileModifiedPayload::test_default("/test/modified.txt")
        .with_size(2048)
        .with_modification_type("content");

    let modified_event = Event::from_payload(modified_payload);
    ctx.pool
        .events()
        .insert(modified_event.clone().into())
        .await?;

    assert_eq!(modified_event.source.as_str(), "fs-watcher");
    assert_eq!(modified_event.event_type.as_str(), "file.modified");

    // Query events by source
    let fs_events = ctx
        .pool
        .events()
        .get_by_source(&EventSource::from_static("fs-watcher"), Some(10), None)
        .await?;

    assert_eq!(fs_events.len(), 2, "Should have 2 filesystem events");

    Ok(())
}

/// Test shell/terminal payload system
#[sinex_test]
async fn test_shell_payload_system(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test KittyCommandExecutedPayload
    let kitty_payload = KittyCommandExecutedPayload::test_default("ls -la")
        .with_working_directory("/home/user")
        .with_exit_status(0)
        .with_execution_time_ms(150)
        .with_shell_type("bash");

    let kitty_event = Event::from_payload(kitty_payload);
    ctx.pool.events().insert(kitty_event.clone().into()).await?;

    assert_eq!(kitty_event.source.as_str(), "shell.kitty");
    assert_eq!(kitty_event.event_type.as_str(), "command.executed");
    assert_eq!(kitty_event.payload.command.as_str(), "ls -la");

    // Test AtuinCommandExecutedPayload
    let atuin_payload = AtuinCommandExecutedPayload::test_default("git status", "/repo")
        .with_exit_code(0)
        .with_duration_ns(2000000)
        .with_hostname("dev-machine");

    let atuin_event = Event::from_payload(atuin_payload);
    ctx.pool.events().insert(atuin_event.clone().into()).await?;

    assert_eq!(atuin_event.source.as_str(), "shell.atuin");
    assert_eq!(atuin_event.event_type.as_str(), "command.executed");

    // Verify both shell events exist
    let shell_events = ctx.test_event_count().await;
    assert!(shell_events >= 2, "Should have at least 2 events");

    Ok(())
}

/// Test clipboard payload system
#[sinex_test]
async fn test_clipboard_payload_system(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test ClipboardCopiedPayload
    let clipboard_payload = ClipboardCopiedPayload::test_default("test-hash");

    let clipboard_event = Event::from_payload(clipboard_payload);
    ctx.pool
        .events()
        .insert(clipboard_event.clone().into())
        .await?;

    assert_eq!(clipboard_event.source.as_str(), "clipboard");
    assert_eq!(clipboard_event.event_type.as_str(), "clipboard.copied");

    Ok(())
}

// =============================================================================
// SOURCE TO EVENT TYPE MAPPING TESTS
// =============================================================================

/// Test that sources consistently map to appropriate event types
#[sinex_test]
async fn test_source_event_type_mapping(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let test_cases = vec![
        (
            "fs-watcher",
            vec![
                "file.created",
                "file.modified",
                "file.deleted",
                "dir.created",
                "dir.deleted",
            ],
        ),
        ("shell.kitty", vec!["command.executed", "command.completed"]),
        ("shell.atuin", vec!["command.executed", "command.completed"]),
        ("clipboard", vec!["clipboard.copied"]),
    ];

    for (source, expected_types) in test_cases {
        let mut created_events = Vec::new();

        // Create test events for each expected type
        for event_type in expected_types.iter() {
            let test_payload = match (source, *event_type) {
                ("fs-watcher", "file.created") => {
                    json!({"path": "/test/file.txt", "size": 1024, "created_at": "2024-01-01T00:00:00Z", "permissions": 644})
                }
                ("fs-watcher", "file.modified") => {
                    json!({"path": "/test/file.txt", "size": 1024, "modified_at": "2024-01-01T00:00:00Z", "modification_type": "content"})
                }
                ("fs-watcher", "file.deleted") => {
                    json!({"path": "/test/file.txt", "deleted_at": "2024-01-01T00:00:00Z"})
                }
                ("fs-watcher", "dir.created") => {
                    json!({"path": "/test/dir", "created_at": "2024-01-01T00:00:00Z"})
                }
                ("fs-watcher", "dir.deleted") => {
                    json!({"path": "/test/dir", "deleted_at": "2024-01-01T00:00:00Z"})
                }
                ("shell.kitty", "command.executed") => {
                    json!({"command": "test command", "kitty_window_id": "1", "kitty_tab_id": "1"})
                }
                ("shell.kitty", "command.completed") => {
                    json!({"command": "test command", "working_directory": "/", "exit_status": 0, "duration_ms": 100, "shell_pid": 1234, "kitty_window_id": "1", "kitty_tab_id": "1"})
                }
                ("shell.atuin", "command.executed") => {
                    json!({"command_string": "test", "cwd": "/", "exit_code": 0, "duration_ns": 1000000, "atuin_history_id": "test", "atuin_session_id": "test", "timestamp": 1704067200, "ts_start_orig": "2024-01-01T00:00:00Z", "ts_end_orig": "2024-01-01T00:00:00Z", "hostname": "test"})
                }
                ("shell.atuin", "command.completed") => {
                    json!({"command": "test", "working_directory": "/", "exit_status": 0, "duration_ms": 100, "hostname": "test", "username": "user", "shell": "bash", "atuin_id": "test", "session_id": "test"})
                }
                ("clipboard", "clipboard.copied") => {
                    json!({"operation": "copy", "content_type": "text/plain", "content_size": 12, "content_hash": "test-hash"})
                }
                _ => json!({"test": true}),
            };

            let event = ctx
                .create_test_event(source, event_type, test_payload)
                .await?;
            created_events.push(event);
        }

        // Verify all events have the expected source
        for event in &created_events {
            assert_eq!(
                event.source.as_str(),
                source,
                "Event should have source '{}'",
                source
            );
        }

        // Verify we created all expected event types
        let actual_types: HashSet<String> = created_events
            .iter()
            .map(|e| e.event_type.as_str().to_string())
            .collect();

        let expected_set: HashSet<String> = expected_types.iter().map(|s| s.to_string()).collect();

        assert_eq!(
            actual_types, expected_set,
            "Should create all expected event types for source '{}'",
            source
        );

        // Verify mapping was successful (no snapshot needed for simple validation)
        println!(
            "Successfully validated source '{}' with {} event types",
            source,
            expected_types.len()
        );
    }

    Ok(())
}

// =============================================================================
// CONCURRENT EVENT CREATION TESTS
// =============================================================================

/// Test concurrent event creation maintains type safety
#[sinex_test]
async fn test_concurrent_event_creation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use std::sync::Arc;
    use tokio::task;

    let ctx = Arc::new(ctx);
    let mut handles = vec![];

    // Create multiple tasks that create different types of events concurrently
    for i in 0..5 {
        let ctx_clone = Arc::clone(&ctx);
        let handle = task::spawn(async move {
            let mut events: Vec<RawEvent> = Vec::new();

            // Create filesystem event
            let fs_payload = FileCreatedPayload::test_default(&format!("/test/file{}.txt", i))
                .with_size((i as u64) * 1024);
            events.push(Event::from_payload(fs_payload).into());

            // Create shell event
            let shell_payload = KittyCommandExecutedPayload::test_default(&format!("cmd{}", i))
                .with_kitty_ids(&format!("win{}", i), &format!("tab{}", i));
            events.push(Event::from_payload(shell_payload).into());

            // Insert all events
            for event in &events {
                ctx_clone.pool.events().insert(event.clone()).await?;
            }

            Ok::<(Vec<RawEvent>, usize), color_eyre::eyre::Error>((events, i))
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(handles).await;

    // Verify all tasks completed successfully
    assert_eq!(results.len(), 5);
    let mut all_events = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        let (events, task_num) = result.unwrap()?;
        assert_eq!(task_num, i);
        assert_eq!(events.len(), 2, "Each task should create 2 events");
        all_events.extend(events);
    }

    // Verify we have the expected number of events
    assert_eq!(all_events.len(), 10, "Should have 10 total events");

    // Verify event types are distributed correctly
    let fs_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.source.as_str() == "fs-watcher")
        .collect();
    let shell_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.source.as_str() == "shell.kitty")
        .collect();

    assert_eq!(fs_events.len(), 5, "Should have 5 filesystem events");
    assert_eq!(shell_events.len(), 5, "Should have 5 shell events");

    Ok(())
}

/// Test that event IDs are unique even under concurrent creation
#[sinex_test]
async fn test_event_id_uniqueness_concurrent(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::task;

    let ctx = Arc::new(ctx);
    let mut handles = vec![];

    // Create many tasks that create events concurrently
    for i in 0..10 {
        let ctx_clone = Arc::clone(&ctx);
        let handle = task::spawn(async move {
            let mut event_ids = Vec::new();

            // Create multiple events per task
            for j in 0..3 {
                let payload =
                    FileCreatedPayload::test_default(&format!("/test/file{}_{}.txt", i, j));
                let event = Event::from_payload(payload);
                ctx_clone.pool.events().insert(event.clone().into()).await?;

                // Get the ID after insertion (should be set by repository)
                let inserted_events = ctx_clone
                    .pool
                    .events()
                    .get_by_source(&EventSource::from_static("fs-watcher"), Some(100), None)
                    .await?;

                // Find our event by checking the path in payload
                for inserted_event in inserted_events {
                    if inserted_event.payload["path"] == json!(format!("/test/file{}_{}.txt", i, j))
                    {
                        event_ids.push(inserted_event.id.expect("Inserted event should have ID"));
                        break;
                    }
                }
            }

            Ok::<Vec<Id<RawEvent>>, color_eyre::eyre::Error>(event_ids)
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(handles).await;

    // Collect all event IDs using ULID strings
    let mut all_ids = HashSet::new();
    for result in results {
        let ids = result.unwrap()?;
        for id in ids {
            let id_string = id.to_string();
            assert!(
                all_ids.insert(id_string.clone()),
                "Event ID {} should be unique",
                id_string
            );
        }
    }

    // Verify we have the expected number of unique IDs
    assert_eq!(
        all_ids.len(),
        30,
        "Should have 30 unique event IDs (10 tasks * 3 events each)"
    );

    Ok(())
}

// =============================================================================
// PAYLOAD VALIDATION TESTS
// =============================================================================

/// Test payload validation and schema adherence
#[sinex_test]
async fn test_payload_validation_system(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that valid payloads work correctly
    let valid_payload = FileCreatedPayload::test_default("/valid/path.txt")
        .with_size(1024)
        .with_permissions(0o644);

    let valid_event = Event::from_payload(valid_payload);
    ctx.pool.events().insert(valid_event.clone().into()).await?;

    // Verify the event was stored and has expected structure
    assert_eq!(valid_event.source.as_str(), "fs-watcher");
    assert_eq!(valid_event.event_type.as_str(), "file.created");

    // Verify payload structure matches expected schema - with typed payloads we access fields directly
    assert!(
        !valid_event.payload.path.is_empty(),
        "Path should not be empty"
    );
    assert!(valid_event.payload.size > 0, "Size should be positive");

    Ok(())
}

/// Test event type constants are consistent
#[sinex_test]
async fn test_event_type_constants_consistency(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that the EventPayload macro generates consistent event types
    let file_created = FileCreatedPayload::test_default("/test");
    let file_modified = FileModifiedPayload::test_default("/test");
    let file_deleted = FileDeletedPayload::test_default("/test");

    // Create events and check their types
    let created_event = Event::from_payload(file_created);
    let modified_event = Event::from_payload(file_modified);
    let deleted_event = Event::from_payload(file_deleted);

    // All should have same source
    assert_eq!(created_event.source.as_str(), "fs-watcher");
    assert_eq!(modified_event.source.as_str(), "fs-watcher");
    assert_eq!(deleted_event.source.as_str(), "fs-watcher");

    // But different event types
    assert_eq!(created_event.event_type.as_str(), "file.created");
    assert_eq!(modified_event.event_type.as_str(), "file.modified");
    assert_eq!(deleted_event.event_type.as_str(), "file.deleted");

    // Test shell events too
    let kitty_executed = KittyCommandExecutedPayload::test_default("test");
    let atuin_executed = AtuinCommandExecutedPayload::test_default("test", "/");

    let kitty_event = Event::from_payload(kitty_executed);
    let atuin_event = Event::from_payload(atuin_executed);

    // Different sources but same event type
    assert_eq!(kitty_event.source.as_str(), "shell.kitty");
    assert_eq!(atuin_event.source.as_str(), "shell.atuin");
    assert_eq!(kitty_event.event_type.as_str(), "command.executed");
    assert_eq!(atuin_event.event_type.as_str(), "command.executed");

    Ok(())
}
