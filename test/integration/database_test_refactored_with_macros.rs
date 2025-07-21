// Consolidated Database Integration Tests - Refactored with Test Macros
//
// This module demonstrates the use of test macros to reduce repetition
// and make tests more maintainable.

use crate::common::prelude::*;
use crate::common::{self, assertions, events, generators, schema_test_utils};
use crate::common::query_helpers::{TestQueries, CheckpointRecord};
use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder, BatchEventBuilder};
use chrono::{Duration, Utc};
use futures::future::join_all;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_events::{EventFactory, event_types};
use sinex_ulid::Ulid;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use uuid::Uuid;

// Import test macros
use crate::test_event_insertion;
use crate::test_batch_events;
use crate::test_checkpoint_flow;
use crate::parameterized_test;
use crate::test_event_flow;

// =============================================================================
// BASIC DATABASE OPERATIONS - Using Macros
// =============================================================================

// Simple event insertion tests using macros
test_event_insertion!(
    test_filesystem_event_basic,
    "fs",
    "file.created",
    json!({"path": "/test/file.txt", "size": 1024})
);

test_event_insertion!(
    test_shell_command_basic,
    "shell.kitty",
    "command.executed",
    json!({"command": "ls -la", "exit_code": 0})
);

test_event_insertion!(
    test_clipboard_copy_basic,
    "clipboard",
    "copied",
    json!({"content_type": "text", "size": 100})
);

// Batch insertion tests using macros
test_batch_events!(
    test_bulk_filesystem_events,
    "fs",
    "file.modified",
    25,
    |pool, events| async move {
        // Verify all events have the same source
        for event in events {
            assert_eq!(event.source, "fs");
            assert_eq!(event.event_type, "file.modified");
        }
        
        // Verify count
        let count = TestQueries::count_events_by_source(pool, "fs").await?;
        assert!(count >= 25);
        Ok(())
    }
);

test_batch_events!(
    test_bulk_window_events,
    "wm.hyprland",
    "window.opened",
    10,
    |pool, events| async move {
        // Verify events are time-ordered
        for i in 1..events.len() {
            assert!(events[i].id > events[i-1].id, "Events should be time-ordered");
        }
        Ok(())
    }
);

// =============================================================================
// CHECKPOINT TESTS - Using Macros
// =============================================================================

test_checkpoint_flow!(
    test_filesystem_scanner_checkpoint,
    "fs-scanner",
    0,
    1000
);

test_checkpoint_flow!(
    test_clipboard_monitor_checkpoint,
    "clipboard-monitor",
    100,
    250
);

test_checkpoint_flow!(
    test_window_tracker_checkpoint,
    "window-tracker",
    500,
    750
);

// =============================================================================
// EVENT FLOW TESTS - Using Macros
// =============================================================================

test_event_flow!(
    test_filesystem_to_scanner_flow,
    "fs",
    "file.created",
    "fs-scanner"
);

test_event_flow!(
    test_clipboard_to_monitor_flow,
    "clipboard",
    "copied",
    "clipboard-monitor"
);

test_event_flow!(
    test_shell_to_analyzer_flow,
    "shell.kitty",
    "command.executed",
    "command-analyzer"
);

// =============================================================================
// PARAMETERIZED TESTS - Using Macros
// =============================================================================

parameterized_test!(
    test_various_event_sources,
    vec![
        ("Filesystem", ("fs", "file.created", json!({"path": "/test.txt"}))),
        ("Shell", ("shell", "command.executed", json!({"cmd": "echo test"}))),
        ("Clipboard", ("clipboard", "copied", json!({"size": 100}))),
        ("Window Manager", ("wm", "window.focused", json!({"id": 1}))),
    ],
    |pool, (source, event_type, payload)| async move {
        let event = TestEventBuilder::new(source, event_type)
            .with_payload(payload.clone())
            .insert(pool)
            .await?;
        
        let retrieved = TestQueries::get_event(pool, event.id).await?;
        assert_eq!(retrieved.source, source);
        assert_eq!(retrieved.event_type, event_type);
        assert_eq!(retrieved.payload, payload);
        
        Ok(())
    }
);

parameterized_test!(
    test_checkpoint_states,
    vec![
        ("Initial", ("test-automaton-1", 0, None)),
        ("In Progress", ("test-automaton-2", 50, Some("event-50"))),
        ("Completed", ("test-automaton-3", 100, Some("event-100"))),
    ],
    |pool, (automaton, count, last_id)| async move {
        let mut builder = TestCheckpointBuilder::new(automaton)
            .with_processed_count(count);
        
        if let Some(id) = last_id {
            builder = builder.with_last_processed(id);
        }
        
        builder.insert(pool).await?;
        
        let checkpoint = TestQueries::get_checkpoint(pool, automaton)
            .await?
            .expect("Checkpoint should exist");
        
        assert_eq!(checkpoint.processed_count, count);
        assert_eq!(checkpoint.last_processed_id, last_id.map(String::from));
        
        Ok(())
    }
);

// =============================================================================
// COMPLEX TESTS - Still using direct builders for flexibility
// =============================================================================

/// Test invalid event insertion fails appropriately
#[sinex_test]
async fn test_invalid_event_insertion_fails(ctx: TestContext) -> TestResult {
    let invalid_event = events::invalid_event();
    assertions::assert_event_insertion_fails(ctx.pool(), &invalid_event).await?;
    Ok(())
}

/// Test ULID ordering in time-based queries
#[sinex_test(timeout = 35)]
async fn test_ulid_time_ordering(ctx: TestContext) -> TestResult {
    // Insert events with a small delay to ensure different timestamps
    let event1 = events::file_created_event("/test/first.txt");
    let id1 = assertions::assert_event_inserted(ctx.pool(), &event1).await?;

    tokio::task::yield_now().await;

    let event2 = events::file_created_event("/test/second.txt");
    let id2 = assertions::assert_event_inserted(ctx.pool(), &event2).await?;

    // Verify ULIDs are in time order (later ULID should be larger)
    assert!(id2 > id1);

    Ok(())
}

// The rest of the complex tests remain unchanged...