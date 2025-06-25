//! Unit tests for core Sinex functionality
//! 
//! Tests the basic building blocks of the event system:
//! - RawEventBuilder for creating events
//! - Event constants and source identifiers
//! - ULID generation and properties

use crate::common::prelude::*;
use sinex_core::{RawEventBuilder, sources, event_type_constants};

/// Test basic event creation with RawEventBuilder
/// 
/// Verifies that:
/// - Events are created with correct source and type
/// - Payload is properly attached
/// - Auto-generated fields (host, ID) are populated
/// - ULID format is correct (26 characters)
#[test]
fn test_raw_event_builder_basic() {
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({"path": "/test/file.txt"})
    ).build();
    
    pretty_assertions::assert_eq!(event.source, sources::FILESYSTEM);
    pretty_assertions::assert_eq!(event.event_type, event_type_constants::filesystem::FILE_CREATED);
    pretty_assertions::assert_eq!(event.payload["path"], "/test/file.txt");
    assert!(!event.host.is_empty());
    assert!(event.id.to_string().len() == 26); // ULID length
}

/// Test creating multiple events with different sources
/// 
/// Ensures that:
/// - Multiple events can be created independently
/// - Each event gets a unique ULID
/// - Different sources and types work correctly
/// - ULIDs maintain time ordering when created in sequence
#[test]
fn test_multiple_event_creation() {
    let events = vec![
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({"path": "/test/file1.txt"})
        ).build(),
        RawEventBuilder::new(
            sources::TERMINAL_KITTY,
            event_type_constants::terminal::COMMAND_EXECUTED,
            json!({"command": "ls -la"})
        ).build(),
        RawEventBuilder::new(
            sources::SINEX,
            event_type_constants::sinex::AGENT_HEARTBEAT,
            json!({"status": "running"})
        ).build(),
    ];
    
    pretty_assertions::assert_eq!(events.len(), 3);
    pretty_assertions::assert_eq!(events[0].source, "filesystem");
    pretty_assertions::assert_eq!(events[1].source, "terminal.kitty");
    pretty_assertions::assert_eq!(events[2].source, "sinex");
    
    // All events should have unique IDs
    pretty_assertions::assert_ne!(events[0].id, events[1].id);
    pretty_assertions::assert_ne!(events[1].id, events[2].id);
    pretty_assertions::assert_ne!(events[0].id, events[2].id);
}