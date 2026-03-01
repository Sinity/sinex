//! Event Type System Tests
//!
//! Tests for the strongly-typed event system, validating event types,
//! sources, and the modern payload system.
//!
//! Migrated from `test/unit/event_type_system_test.rs` to use modern patterns:
//! - `TestContext` instead of custom fixtures
//! - Modern Event API with `Event::from_payload()`
//! - Direct repository access via ctx.pool.*()
//! - Modern payload types from `sinex_primitives::events::payloads`
//! - `color_eyre` for error handling

use xtask::sandbox::prelude::*;

// Additional imports for specific payload types
use sinex_db::models::SourceMaterial;
use sinex_primitives::domain::{RecordedPath, ShellName};
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::enums::FileModificationType;
use sinex_primitives::events::payloads::{
    AtuinCommandExecutedPayload, ClipboardCopiedPayload, FileCreatedPayload, FileDeletedPayload,
    FileModifiedPayload, KittyCommandExecutedPayload,
};
use sinex_primitives::{DynamicPayload, Id, Provenance, Ulid, units::ExitCode};
use std::collections::HashSet;

async fn ensure_material(ctx: &TestContext, label: &str) -> TestResult<Id<SourceMaterial>> {
    let material_id = Id::<SourceMaterial>::from_ulid(Ulid::new());
    ctx.ensure_source_material(material_id, Some(label)).await?;
    Ok(material_id)
}

fn rp(path: impl AsRef<str>) -> RecordedPath {
    RecordedPath::from_observed(path.as_ref()).expect("test paths must be valid")
}

// =============================================================================
// EVENT SOURCE CONSTANTS AND VALIDATION TESTS
// =============================================================================

/// Test event source constants and consistent naming patterns
#[sinex_test]
async fn test_event_source_patterns() -> TestResult<()> {
    let sources = vec![
        FileCreatedPayload::SOURCE.to_string(),
        FileModifiedPayload::SOURCE.to_string(),
        FileDeletedPayload::SOURCE.to_string(),
        KittyCommandExecutedPayload::SOURCE.to_string(),
        AtuinCommandExecutedPayload::SOURCE.to_string(),
        ClipboardCopiedPayload::SOURCE.to_string(),
    ];

    for source in &sources {
        assert!(!source.is_empty(), "Event source should not be empty");
        assert!(
            source
                .chars()
                .all(|c| c.is_ascii_lowercase() || matches!(c, '.' | '-' | '_')),
            "Source {source} contains invalid characters"
        );
        assert!(
            !source.starts_with('.') && !source.ends_with('.'),
            "Source {source} should not start or end with a dot"
        );
        if source.contains('.') {
            assert!(
                source.split('.').all(|segment| !segment.is_empty()),
                "Dot-separated segments in {source} must be non-empty"
            );
        }
    }
    Ok(())
}

/// Test that event sources follow consistent naming patterns  
#[sinex_test]
async fn test_source_naming_conventions() -> TestResult<()> {
    let validated_sources = ["fs-watcher", "clipboard", "system"];
    for source in validated_sources {
        let source_type = EventSource::new(source);
        source_type
            .validate()
            .map_err(|e| color_eyre::eyre::eyre!(e))?;
    }

    let dot_sources = [
        "shell.kitty",
        "shell.atuin",
        "shell.history",
        "terminal.kitty",
    ];
    for source in dot_sources {
        let constant = EventSource::from_static(source);
        assert_eq!(constant.as_str(), source);
        // Dot-separated sources are valid (e.g., shell.kitty identifies shell+terminal combos)
        assert!(
            EventSource::new(source).validate().is_ok(),
            "Dot-separated source {source} should be valid"
        );
    }

    Ok(())
}

// =============================================================================
// EVENT TYPE VALIDATION AND PATTERNS
// =============================================================================

/// Test event type naming patterns and validation
#[sinex_test]
async fn test_event_type_validation() -> TestResult<()> {
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
                "Event type '{event_type}' should be valid, but got error: {result:?}"
            );
        } else {
            assert!(
                result.is_err(),
                "Event type '{event_type}' should be invalid but passed validation"
            );
        }
    }

    Ok(())
}

/// Test event type hierarchical structure (object.action pattern)
#[sinex_test]
async fn test_event_type_hierarchical_structure() -> TestResult<()> {
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
            "Event type should have exactly 2 parts: {event_type}"
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
async fn test_filesystem_payload_system(ctx: TestContext) -> TestResult<()> {
    // Test FileCreatedPayload
    let file_payload = FileCreatedPayload::test_default(rp("/test/file.txt"))
        .with_size(1024u64)
        .with_permissions(0o644u32);

    // Build a typed event and convert to JSON for storage
    let material_id = ensure_material(&ctx, "shell-kitty").await?;
    let prov = Provenance::from_material(material_id, 0, None, None);
    let file_event = file_payload.into_event(prov);
    ctx.pool
        .events()
        .insert(file_event.clone().to_json_event().unwrap())
        .await?;

    // Verify the event was stored correctly
    assert_eq!(file_event.source.as_str(), "fs-watcher");
    assert_eq!(file_event.event_type.as_str(), "file.created");
    assert_eq!(file_event.payload.size, 1024);
    assert_eq!(file_event.payload.path.as_str(), "/test/file.txt");

    // Test FileModifiedPayload
    let modified_payload = FileModifiedPayload::test_default(rp("/test/modified.txt"))
        .with_size(2048u64)
        .with_modification_type(FileModificationType::Content);

    let material_id = ensure_material(&ctx, "shell-atuin").await?;
    let prov = Provenance::from_material(material_id, 0, None, None);
    let modified_event = modified_payload.into_event(prov);
    ctx.pool
        .events()
        .insert(modified_event.clone().to_json_event().unwrap())
        .await?;

    assert_eq!(modified_event.source.as_str(), "fs-watcher");
    assert_eq!(modified_event.event_type.as_str(), "file.modified");

    // Query events by source
    let fs_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from_static("fs-watcher"),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(fs_events.len(), 2, "Should have 2 filesystem events");

    Ok(())
}

/// Test shell/terminal payload system
#[sinex_serial_test]
async fn test_shell_payload_system(ctx: TestContext) -> TestResult<()> {
    ctx.ensure_clean().await?;
    // Test KittyCommandExecutedPayload
    let kitty_payload = KittyCommandExecutedPayload::test_default("ls -la")
        .with_working_directory(Some(rp("/home/user")))
        .with_exit_status(Some(ExitCode::SUCCESS))
        .with_execution_time_ms(150)
        .with_shell_type(Some(ShellName::new("bash".to_string())));

    let material_id = ensure_material(&ctx, "clipboard").await?;
    let prov = Provenance::from_material(material_id, 0, None, None);
    let kitty_event = kitty_payload.into_event(prov);
    ctx.pool
        .events()
        .insert(kitty_event.clone().to_json_event().unwrap())
        .await?;

    assert_eq!(kitty_event.source.as_str(), "shell.kitty");
    assert_eq!(kitty_event.event_type.as_str(), "command.executed");
    assert_eq!(kitty_event.payload.command.as_str(), "ls -la");

    // Test AtuinCommandExecutedPayload
    let atuin_payload = AtuinCommandExecutedPayload::test_default("git status", rp("/repo"))
        .with_exit_code(0)
        .with_duration_ns(2000000)
        .with_hostname("dev-machine");

    let material_id = ensure_material(&ctx, "shell-atuin").await?;
    let prov = Provenance::from_material(material_id, 0, None, None);
    let atuin_event = atuin_payload.into_event(prov);
    ctx.pool
        .events()
        .insert(atuin_event.clone().to_json_event().unwrap())
        .await?;

    assert_eq!(atuin_event.source.as_str(), "shell.atuin");
    assert_eq!(atuin_event.event_type.as_str(), "command.executed");

    // Verify both shell events exist
    let shell_events = ctx.pool.events().count_all().await?;
    assert!(shell_events >= 2, "Should have at least 2 events");

    Ok(())
}

/// Test clipboard payload system
#[sinex_test]
async fn test_clipboard_payload_system(ctx: TestContext) -> TestResult<()> {
    // Test ClipboardCopiedPayload
    let clipboard_payload = ClipboardCopiedPayload::test_default("test-hash");

    let material_id = ensure_material(&ctx, "clipboard").await?;
    let prov = Provenance::from_material(material_id, 0, None, None);
    let clipboard_event = clipboard_payload.into_event(prov);
    ctx.pool
        .events()
        .insert(clipboard_event.clone().to_json_event().unwrap())
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
async fn test_source_event_type_mapping(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _scope = ctx.pipeline().await?;

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
        for &event_type in &expected_types {
            let test_payload = match (source, event_type) {
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
                .publish(DynamicPayload::new(source, event_type, test_payload))
                .await?;
            created_events.push(event);
        }

        // Verify all events have the expected source
        for event in &created_events {
            assert_eq!(
                event.source.as_str(),
                source,
                "Event should have source '{source}'"
            );
        }

        // Verify we created all expected event types
        let actual_types: HashSet<String> = created_events
            .iter()
            .map(|e| e.event_type.as_str().to_string())
            .collect();

        let expected_set: HashSet<String> = expected_types
            .iter()
            .map(std::string::ToString::to_string)
            .collect();

        assert_eq!(
            actual_types, expected_set,
            "Should create all expected event types for source '{source}'"
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
async fn test_concurrent_event_creation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    // Publish multiple typed events through the pipeline
    for i in 0..5 {
        // Filesystem event
        let fs_payload = FileCreatedPayload::test_default(rp(format!("/test/file{i}.txt")))
            .with_size((i as u64) * 1024);
        ctx.publish(fs_payload).await?;

        // Shell command event
        let shell_payload = KittyCommandExecutedPayload::test_default(format!("cmd{i}"))
            .with_kitty_ids(format!("win{i}"), format!("tab{i}"));
        ctx.publish(shell_payload).await?;
    }

    // Wait for all 10 events through the pipeline
    scope.wait_for_event_count(10).await?;

    // Verify distribution by source
    scope.wait_for_source_events("fs-watcher", 5).await?;
    scope.wait_for_source_events("shell.kitty", 5).await?;

    Ok(())
}

/// Test that event IDs are unique even under concurrent creation
#[sinex_test]
async fn test_event_id_uniqueness_concurrent(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    // Publish 30 events through the pipeline (10 batches of 3)
    for i in 0..10 {
        for j in 0..3 {
            let payload = FileCreatedPayload::test_default(rp(format!("/test/file{i}_{j}.txt")));
            ctx.publish(payload).await?;
        }
    }

    // Wait for all 30 events
    scope.wait_for_event_count(30).await?;

    // Verify all IDs are unique by querying from DB
    let events = ctx.pool().events().get_recent(50).await?;
    assert_eq!(events.len(), 30, "Should have 30 events in the DB");

    let mut all_ids = HashSet::new();
    for event in &events {
        let id = event.id.expect("persisted events must have an ID");
        assert!(
            all_ids.insert(id.to_string()),
            "Event ID {id} should be unique"
        );
    }

    assert_eq!(all_ids.len(), 30, "Should have 30 unique event IDs");

    Ok(())
}

// =============================================================================
// PAYLOAD VALIDATION TESTS
// =============================================================================

/// Test payload validation and schema adherence
#[sinex_test]
async fn test_payload_validation_system(ctx: TestContext) -> TestResult<()> {
    // Test that valid payloads work correctly
    let valid_payload = FileCreatedPayload::test_default(rp("/valid/path.txt"))
        .with_size(1024u64)
        .with_permissions(0o644u32);

    let material_id = ensure_material(&ctx, "payload-valid").await?;
    let prov = Provenance::from_material(material_id, 0, None, None);
    let valid_event = valid_payload.into_event(prov);
    ctx.pool
        .events()
        .insert(valid_event.clone().to_json_event().unwrap())
        .await?;

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
async fn test_event_type_constants_consistency(ctx: TestContext) -> TestResult<()> {
    // Test that the EventPayload macro generates consistent event types
    let file_created = FileCreatedPayload::test_default(rp("/test"));
    let file_modified = FileModifiedPayload::test_default(rp("/test"));
    let file_deleted = FileDeletedPayload::test_default(rp("/test"));

    // Create events and check their types
    let fs_material = ensure_material(&ctx, "event-constants-fs").await?;
    let prov = Provenance::from_material(fs_material, 0, None, None);
    // Use fluent API for typed payloads
    let created_event = file_created.into_event(prov.clone());
    let modified_event = file_modified.into_event(prov.clone());
    let deleted_event = file_deleted.into_event(prov.clone());

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
    let atuin_executed = AtuinCommandExecutedPayload::test_default("test", rp("/"));

    let shell_material = ensure_material(&ctx, "event-constants-shell").await?;
    let shell_prov = Provenance::from_material(shell_material, 0, None, None);
    // Use fluent API for typed payloads
    let kitty_event = kitty_executed.into_event(shell_prov.clone());
    let atuin_event = atuin_executed.into_event(shell_prov);

    // Different sources but same event type
    assert_eq!(kitty_event.source.as_str(), "shell.kitty");
    assert_eq!(atuin_event.source.as_str(), "shell.atuin");
    assert_eq!(kitty_event.event_type.as_str(), "command.executed");
    assert_eq!(atuin_event.event_type.as_str(), "command.executed");

    Ok(())
}
