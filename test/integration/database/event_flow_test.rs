//! Consolidated event flow tests replacing dozens of scattered insertion tests
//!
//! This file replaces similar tests found in:
//! - test/unit/db/database_operations_tests.rs
//! - test/integration/payload_boundary_test.rs
//! - test/system/end_to_end/complete_system_test.rs
//! - And 50+ other files with similar patterns

use crate::common::prelude::*;
use rstest::rstest;
use serde_json::json;

// Test data generators
fn filesystem_payload() -> JsonValue {
    json!({
        "path": "/test/file.txt",
        "size": 1024,
        "permissions": "0644"
    })
}

fn terminal_payload() -> JsonValue {
    json!({
        "command": "ls -la",
        "working_directory": "/home/user",
        "exit_status": 0
    })
}

fn clipboard_payload() -> JsonValue {
    json!({
        "content": "test clipboard content",
        "content_type": "text/plain",
        "size": 21
    })
}

fn window_manager_payload() -> JsonValue {
    json!({
        "window_id": 123,
        "workspace": "main",
        "title": "Test Window"
    })
}

fn system_payload() -> JsonValue {
    json!({
        "service": "test-service",
        "message": "test message",
        "priority": "info"
    })
}

fn large_payload() -> JsonValue {
    let large_content = "x".repeat(1024 * 1024); // 1MB
    json!({
        "large_content": large_content,
        "size": large_content.len(),
        "chunks": (0..100).map(|i| format!("chunk_{}", i)).collect::<Vec<_>>()
    })
}

/// Consolidated test for basic event insertion and retrieval
/// Replaces 50+ similar tests across the codebase
#[rstest]
#[case::filesystem("fs", "file.created", filesystem_payload())]
#[case::terminal_atuin("shell.atuin", "command.executed", terminal_payload())]
#[case::terminal_history("shell.history", "command.hist", terminal_payload())]
#[case::terminal_kitty("shell.kitty", "command.started", terminal_payload())]
#[case::clipboard("clipboard", "copied", clipboard_payload())]
#[case::window_manager("wm.hyprland", "window.focused", window_manager_payload())]
#[case::system_dbus("dbus", "signal.received", system_payload())]
#[case::system_journal("journald", "entry.written", system_payload())]
#[sinex_test]
async fn test_event_lifecycle(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
    #[case] payload: JsonValue,
) -> TestResult {
    // Create test event
    let event = RawEventBuilder::new(source, event_type, payload.clone())
        .with_host("test-host")
        .build();
    
    // Insert event
    let inserted_id = insert_event(ctx.pool(), &event).await?;
    assert_eq!(inserted_id, event.id);
    
    // Verify event exists
    let retrieved = get_event_by_id(ctx.pool(), event.id).await?;
    assert_eq!(retrieved.id, event.id);
    assert_eq!(retrieved.source, source);
    assert_eq!(retrieved.event_type, event_type);
    assert_eq!(retrieved.payload, payload);
    assert_eq!(retrieved.host, "test-host");
    
    // Verify event appears in queries
    let source_events = get_events_by_source(ctx.pool(), source, 10).await?;
    assert!(source_events.iter().any(|e| e.id == event.id));
    
    let type_events = get_events_by_type(ctx.pool(), event_type, 10).await?;
    assert!(type_events.iter().any(|e| e.id == event.id));
    
    Ok(())
}

/// Test event insertion with various payload sizes
/// Replaces payload boundary tests
#[rstest]
#[case::small("small", json!({"content": "x".repeat(1024)}))]
#[case::medium("medium", json!({"content": "x".repeat(1024 * 100)}))]
#[case::large("large", large_payload())]
#[sinex_test]
async fn test_payload_boundaries(
    ctx: TestContext,
    #[case] size_type: &str,
    #[case] payload: JsonValue,
) -> TestResult {
    let event = RawEventBuilder::new("test.boundary", "payload.test", payload.clone())
        .build();
    
    // Insert should succeed for all sizes
    let inserted_id = insert_event(ctx.pool(), &event).await?;
    assert_eq!(inserted_id, event.id);
    
    // Verify retrieval
    let retrieved = get_event_by_id(ctx.pool(), event.id).await?;
    assert_eq!(retrieved.payload, payload);
    
    Ok(())
}

/// Test batch event insertion
/// Replaces various batch insertion tests
#[rstest]
#[case::small_batch(10)]
#[case::medium_batch(100)]
#[case::large_batch(1000)]
#[sinex_test]
async fn test_batch_insertion(
    ctx: TestContext,
    #[case] event_count: usize,
) -> TestResult {
    let mut events = Vec::new();
    
    // Create batch of events
    for i in 0..event_count {
        let event = RawEventBuilder::new(
            "test.batch",
            "batch.test",
            json!({"batch_id": i, "data": format!("event_{}", i)}),
        ).build();
        events.push(event);
    }
    
    // Insert all events
    for event in &events {
        insert_event(ctx.pool(), event).await?;
    }
    
    // Verify all events were inserted
    let retrieved_events = get_events_by_source(ctx.pool(), "test.batch", event_count + 10).await?;
    assert_eq!(retrieved_events.len(), event_count);
    
    // Verify order preservation (ULIDs should be ordered)
    let mut event_ids: Vec<_> = retrieved_events.iter().map(|e| e.id).collect();
    event_ids.sort();
    let original_ids: Vec<_> = retrieved_events.iter().map(|e| e.id).collect();
    assert_eq!(event_ids, original_ids, "Events should be ULID-ordered");
    
    Ok(())
}

/// Test event queries by various criteria
/// Replaces query-specific tests
#[sinex_test]
async fn test_event_queries(ctx: TestContext) -> TestResult {
    // Insert diverse events
    let events = vec![
        RawEventBuilder::new("fs", "file.created", json!({"path": "/test1.txt"})).build(),
        RawEventBuilder::new("fs", "file.modified", json!({"path": "/test2.txt"})).build(),
        RawEventBuilder::new("shell.kitty", "command.executed", json!({"command": "ls"})).build(),
        RawEventBuilder::new("shell.kitty", "command.executed", json!({"command": "pwd"})).build(),
        RawEventBuilder::new("clipboard", "copied", json!({"content": "test"})).build(),
    ];
    
    for event in &events {
        insert_event(ctx.pool(), event).await?;
    }
    
    // Test source-based queries
    let fs_events = get_events_by_source(ctx.pool(), "fs", 10).await?;
    assert_eq!(fs_events.len(), 2);
    assert!(fs_events.iter().all(|e| e.source == "fs"));
    
    let shell_events = get_events_by_source(ctx.pool(), "shell.kitty", 10).await?;
    assert_eq!(shell_events.len(), 2);
    assert!(shell_events.iter().all(|e| e.source == "shell.kitty"));
    
    // Test type-based queries
    let command_events = get_events_by_type(ctx.pool(), "command.executed", 10).await?;
    assert_eq!(command_events.len(), 2);
    assert!(command_events.iter().all(|e| e.event_type == "command.executed"));
    
    let copy_events = get_events_by_type(ctx.pool(), "copied", 10).await?;
    assert_eq!(copy_events.len(), 1);
    assert_eq!(copy_events[0].event_type, "copied");
    
    Ok(())
}

/// Test event validation during insertion
/// Replaces validation-specific tests
#[rstest]
#[case::valid_event("fs", "file.created", json!({"path": "/valid.txt"}), true)]
#[case::invalid_source("", "file.created", json!({"path": "/test.txt"}), false)]
#[case::invalid_type("fs", "", json!({"path": "/test.txt"}), false)]
#[case::invalid_payload("fs", "file.created", json!(null), false)]
#[sinex_test]
async fn test_event_validation(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
    #[case] payload: JsonValue,
    #[case] should_succeed: bool,
) -> TestResult {
    let event = RawEventBuilder::new(source, event_type, payload).build();
    
    let result = insert_event(ctx.pool(), &event).await;
    
    if should_succeed {
        assert!(result.is_ok(), "Valid event should insert successfully");
    } else {
        assert!(result.is_err(), "Invalid event should fail insertion");
    }
    
    Ok(())
}

/// Test concurrent event insertion
/// Replaces concurrency-specific tests
#[sinex_test]
async fn test_concurrent_insertion(ctx: TestContext) -> TestResult {
    let concurrency_level = 10;
    let events_per_task = 50;
    
    let mut tasks = Vec::new();
    
    for task_id in 0..concurrency_level {
        let pool = ctx.pool().clone();
        let task = tokio::spawn(async move {
            let mut task_events = Vec::new();
            
            for i in 0..events_per_task {
                let event = RawEventBuilder::new(
                    "test.concurrent",
                    "concurrent.test",
                    json!({
                        "task_id": task_id,
                        "event_id": i,
                        "data": format!("task_{}_event_{}", task_id, i)
                    }),
                ).build();
                
                insert_event(&pool, &event).await?;
                task_events.push(event.id);
            }
            
            Ok::<Vec<sinex_ulid::Ulid>, Box<dyn std::error::Error>>(task_events)
        });
        
        tasks.push(task);
    }
    
    // Wait for all tasks to complete
    let mut all_event_ids = Vec::new();
    for task in tasks {
        let event_ids = task.await??;
        all_event_ids.extend(event_ids);
    }
    
    // Verify all events were inserted
    let total_expected = concurrency_level * events_per_task;
    assert_eq!(all_event_ids.len(), total_expected);
    
    // Verify uniqueness
    let mut unique_ids = all_event_ids.clone();
    unique_ids.sort();
    unique_ids.dedup();
    assert_eq!(unique_ids.len(), total_expected, "All event IDs should be unique");
    
    // Verify in database
    let db_events = get_events_by_source(ctx.pool(), "test.concurrent", total_expected + 10).await?;
    assert_eq!(db_events.len(), total_expected);
    
    Ok(())
}