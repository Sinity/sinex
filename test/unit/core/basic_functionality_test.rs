use sinex_core::{RawEventBuilder, sources, event_type_constants};
use serde_json::json;

#[test]
fn test_raw_event_builder_basic() {
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({"path": "/test/file.txt"})
    ).build();
    
    assert_eq!(event.source, sources::FILESYSTEM);
    assert_eq!(event.event_type, event_type_constants::filesystem::FILE_CREATED);
    assert_eq!(event.payload["path"], "/test/file.txt");
    assert!(!event.host.is_empty());
    assert!(event.id.to_string().len() == 26); // ULID length
}

// Removed trivial constant assertion tests - they just verified that "filesystem" == "filesystem"

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
    
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].source, "filesystem");
    assert_eq!(events[1].source, "terminal.kitty");
    assert_eq!(events[2].source, "sinex");
    
    // All events should have unique IDs
    assert_ne!(events[0].id, events[1].id);
    assert_ne!(events[1].id, events[2].id);
    assert_ne!(events[0].id, events[2].id);
}