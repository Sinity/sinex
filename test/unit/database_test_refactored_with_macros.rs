// Database Unit Tests - Refactored with Test Macros
//
// Consolidated database layer tests using test macros to reduce repetition

use crate::common::prelude::*;
use crate::common::event_builders::EventBuilder;
use serde_json::json;
use sinex_events::{EventFactory, event_types, sources};
use sinex_db::queries::EventQueries;
use sinex_db::validation::EventValidator;
use std::sync::Arc;

// Import test macros
use crate::test_event_insertion;
use crate::test_batch_events;
use crate::test_checkpoint_flow;
use crate::parameterized_test;
use crate::test_event_flow;
use crate::test_invalid_event;

// =============================================================================
// BASIC DATABASE OPERATIONS - Using Macros
// =============================================================================

// Simple event insertion tests
test_event_insertion!(
    test_basic_filesystem_event,
    sources::FS,
    event_types::filesystem::FILE_CREATED,
    json!({"path": "/test/simple_file.txt", "size": 1024})
);

test_event_insertion!(
    test_basic_terminal_event,
    sources::SHELL_KITTY,
    event_types::shell::COMMAND_EXECUTED,
    json!({"command": "ls", "exit_code": 0})
);

test_event_insertion!(
    test_basic_clipboard_event,
    sources::CLIPBOARD,
    event_types::clipboard::COPIED,
    json!({"content_type": "text", "content_size": 100})
);

// Invalid event tests using macros
test_invalid_event!(
    test_empty_source_fails,
    "",
    "test.event",
    json!({"data": "test"}),
    "empty source"
);

test_invalid_event!(
    test_empty_event_type_fails,
    "test_source",
    "",
    json!({"data": "test"}),
    "empty event_type"
);

test_invalid_event!(
    test_null_payload_fails,
    "test_source",
    "test.event",
    json!(null),
    "payload must be an object"
);

// Batch event tests
test_batch_events!(
    test_batch_filesystem_events,
    sources::FS,
    event_types::filesystem::FILE_MODIFIED,
    10,
    |pool, events| async move {
        // Verify all events are properly inserted
        for event in &events {
            assert_eq!(event.source, sources::FS);
            assert_eq!(event.event_type, event_types::filesystem::FILE_MODIFIED);
        }
        
        // Verify count
        let count = TestQueries::count_events_by_source(pool, sources::FS).await?;
        assert!(count >= 10);
        Ok(())
    }
);

// Parameterized tests for multiple event types
parameterized_test!(
    test_various_event_types,
    vec![
        ("Filesystem Created", (sources::FS, event_types::filesystem::FILE_CREATED, json!({"path": "/test/file1.txt"}))),
        ("Filesystem Modified", (sources::FS, event_types::filesystem::FILE_MODIFIED, json!({"path": "/test/file2.txt"}))),
        ("Shell Command", (sources::SHELL_KITTY, event_types::shell::COMMAND_EXECUTED, json!({"command": "echo test"}))),
        ("Clipboard Copied", (sources::CLIPBOARD, event_types::clipboard::COPIED, json!({"content_type": "text"}))),
        ("Window Focused", (sources::WM_HYPRLAND, event_types::window_manager::WINDOW_FOCUSED, json!({"window_id": 1}))),
    ],
    |pool, (source, event_type, payload)| async move {
        let event = TestEventBuilder::new(source, event_type)
            .with_payload(payload.clone())
            .insert(pool)
            .await?;
        
        // Verify the event was properly inserted
        let retrieved = TestQueries::get_event(pool, event.id).await?;
        assert_eq!(retrieved.source, source);
        assert_eq!(retrieved.event_type, event_type);
        assert_eq!(retrieved.payload, payload);
        
        // Additional validation
        assert!(retrieved.id.to_string().len() == 26, "ULID should be 26 characters");
        assert!(!retrieved.host.is_empty(), "Host should not be empty");
        
        Ok(())
    }
);

// =============================================================================
// COMPLEX TESTS - Still using direct implementation for flexibility
// =============================================================================

/// Test enhanced infrastructure features
#[sinex_test]
async fn test_enhanced_infrastructure(ctx: TestContext) -> TestResult {
    // Test that TestContext provides proper test name
    let test_name = ctx.test_name();
    assert!(!test_name.is_empty());

    // Simple database query - test basic connectivity
    let (count,) = EventQueries::count_all()
        .fetch_one::<(i64,)>(ctx.pool())
        .await?;
    // Just verify we can query the database
    assert!(count >= 0);

    // Test event creation helpers
    let event = ctx.filesystem_event("/test/file.txt");
    assert_eq!(event.event_type, "file.created");

    // Insert the event
    ctx.insert_event(&event).await?;

    // Verify it exists
    let (count,): (i64,) = EventQueries::count_all()
        .fetch_one(ctx.pool())
        .await?;
    assert!(count >= 1);

    Ok(())
}

/// Test transaction isolation pattern
#[sinex_test]
async fn test_transaction_isolation(ctx: TestContext) -> TestResult {
    let initial_count = ctx.event_count().await?;
    let events_to_insert = 3;

    // Create some test events
    for i in 0..events_to_insert {
        let event = ctx
            .event_builder("test", "example")
            .payload(serde_json::json!({ "index": i }))
            .build();
        ctx.insert_event(&event).await?;
    }

    let new_count = ctx.event_count().await?;
    pretty_assertions::assert_eq!(new_count - initial_count, events_to_insert);
    Ok(())
}

// =============================================================================
// QUERY OPERATIONS - Using Parameterized Tests
// =============================================================================

parameterized_test!(
    test_query_by_different_sources,
    vec![
        ("Filesystem", sources::FS, 5),
        ("Terminal", sources::SHELL_KITTY, 3),
        ("Clipboard", sources::CLIPBOARD, 2),
        ("Window Manager", sources::WM_HYPRLAND, 4),
    ],
    |pool, (source, expected_count)| async move {
        // Insert events for this source
        for i in 0..expected_count {
            TestEventBuilder::new(source, "test.event")
                .with_field("index", json!(i))
                .insert(pool)
                .await?;
        }
        
        // Query and verify
        let events = TestQueries::get_events_by_source(pool, source, None).await?;
        assert_eq!(events.len(), expected_count);
        
        // Verify all have correct source
        for event in &events {
            assert_eq!(event.source, source);
        }
        
        Ok(())
    }
);