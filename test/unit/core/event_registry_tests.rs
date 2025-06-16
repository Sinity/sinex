use sinex_core::{EventRegistry, EventType, create_registry};
use sinex_events::*;
use serde_json::json;

#[test]
fn test_event_registry_creation() {
    let registry = create_registry();
    
    // Verify registry is not empty
    assert!(!registry.get_all_event_types().is_empty());
    
    // Verify some expected event types are present
    assert!(registry.has_event_type("file.created"));
    assert!(registry.has_event_type("command.executed"));
    assert!(registry.has_event_type("window.focus"));
}

#[test]
fn test_event_registry_get_event_type() {
    let registry = create_registry();
    
    // Test getting existing event type
    let file_created = registry.get_event_type("file.created");
    assert!(file_created.is_some());
    
    let event_type = file_created.unwrap();
    assert_eq!(event_type.name(), "file.created");
    assert_eq!(event_type.source(), "filesystem");
    
    // Test getting non-existent event type
    let unknown = registry.get_event_type("nonexistent.event");
    assert!(unknown.is_none());
}

#[test]
fn test_event_registry_get_source_events() {
    let registry = create_registry();
    
    // Test filesystem source
    let fs_events = registry.get_events_for_source("filesystem");
    assert!(!fs_events.is_empty());
    assert!(fs_events.iter().any(|e| e.name() == "file.created"));
    assert!(fs_events.iter().any(|e| e.name() == "file.modified"));
    assert!(fs_events.iter().any(|e| e.name() == "file.deleted"));
    
    // Test terminal source
    let terminal_events = registry.get_events_for_source("terminal_kitty");
    assert!(!terminal_events.is_empty());
    assert!(terminal_events.iter().any(|e| e.name() == "command.executed"));
    
    // Test non-existent source
    let unknown_events = registry.get_events_for_source("unknown_source");
    assert!(unknown_events.is_empty());
}

#[test]
fn test_event_registry_all_sources() {
    let registry = create_registry();
    
    let sources = registry.get_all_sources();
    assert!(!sources.is_empty());
    
    // Verify expected sources are present
    assert!(sources.contains(&"filesystem".to_string()));
    assert!(sources.contains(&"terminal_kitty".to_string()));
    assert!(sources.contains(&"hyprland".to_string()));
    assert!(sources.contains(&"clipboard".to_string()));
}

#[test]
fn test_event_registry_event_type_properties() {
    let registry = create_registry();
    
    // Test file.created event type
    let file_created = registry.get_event_type("file.created").unwrap();
    assert_eq!(file_created.name(), "file.created");
    assert_eq!(file_created.source(), "filesystem");
    assert!(!file_created.description().is_empty());
    
    // Verify schema exists and is valid JSON
    let schema = file_created.schema();
    assert!(schema.is_object());
    
    // Test command.executed event type
    let command_executed = registry.get_event_type("command.executed").unwrap();
    assert_eq!(command_executed.name(), "command.executed");
    assert_eq!(command_executed.source(), "terminal_kitty");
    assert!(!command_executed.description().is_empty());
}

#[test]
fn test_event_registry_validate_payload() {
    let registry = create_registry();
    
    // Test valid file.created payload
    let file_created = registry.get_event_type("file.created").unwrap();
    let valid_payload = json!({
        "path": "/test/file.txt",
        "size": 1024,
        "permissions": "644",
        "created_time": "2024-01-01T12:00:00Z"
    });
    
    // Note: This test assumes validate_payload method exists
    // If not implemented yet, this will drive the implementation
    assert!(file_created.validate_payload(&valid_payload).is_ok());
    
    // Test invalid payload (missing required field)
    let invalid_payload = json!({
        "size": 1024
        // missing path
    });
    assert!(file_created.validate_payload(&invalid_payload).is_err());
}

#[test]
fn test_event_registry_immutability() {
    let registry1 = create_registry();
    let registry2 = create_registry();
    
    // Registries should have consistent content
    assert_eq!(
        registry1.get_all_event_types().len(),
        registry2.get_all_event_types().len()
    );
    
    // Test that getting the same event type returns equivalent data
    let event1 = registry1.get_event_type("file.created").unwrap();
    let event2 = registry2.get_event_type("file.created").unwrap();
    
    assert_eq!(event1.name(), event2.name());
    assert_eq!(event1.source(), event2.source());
    assert_eq!(event1.schema(), event2.schema());
}

#[test]
fn test_event_registry_schema_lookup() {
    let registry = create_registry();
    
    // Test schema lookup by event type name
    let schema = registry.get_schema_for_event("file.created");
    assert!(schema.is_some());
    
    let schema_obj = schema.unwrap();
    assert!(schema_obj.is_object());
    
    // Verify schema has expected structure
    if let Some(properties) = schema_obj.get("properties") {
        assert!(properties.is_object());
        assert!(properties.get("path").is_some());
    }
}

#[test]
fn test_event_registry_routing() {
    let registry = create_registry();
    
    // Test routing based on event type
    let routes = registry.get_interested_agents("file.created");
    
    // This test assumes some agents are interested in file.created
    // The actual implementation will determine the specific behavior
    assert!(routes.is_ok());
    
    // Test routing for unknown event type
    let unknown_routes = registry.get_interested_agents("unknown.event");
    assert!(unknown_routes.is_ok()); // Should return empty list, not error
}

#[test]
fn test_event_registry_concurrent_access() {
    use std::sync::Arc;
    use std::thread;
    
    let registry = Arc::new(create_registry());
    let mut handles = vec![];
    
    // Spawn multiple threads accessing registry
    for i in 0..10 {
        let registry_clone = Arc::clone(&registry);
        let handle = thread::spawn(move || {
            let event_type = if i % 2 == 0 { "file.created" } else { "command.executed" };
            let result = registry_clone.get_event_type(event_type);
            assert!(result.is_some());
            result.unwrap().name().to_string()
        });
        handles.push(handle);
    }
    
    // Wait for all threads and verify results
    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result == "file.created" || result == "command.executed");
    }
}

// Integration test with actual event types
#[test]
fn test_event_registry_with_real_events() {
    let registry = create_registry();
    
    // Test with actual event types from sinex-events
    let filesystem_events = [
        "file.created",
        "file.modified", 
        "file.deleted",
        "directory.created",
        "directory.deleted"
    ];
    
    for event_name in filesystem_events {
        let event_type = registry.get_event_type(event_name);
        if let Some(et) = event_type {
            assert_eq!(et.source(), "filesystem");
            assert!(!et.description().is_empty());
            assert!(et.schema().is_object());
        }
    }
    
    let terminal_events = [
        "command.executed",
        "session.started",
        "session.ended"
    ];
    
    for event_name in terminal_events {
        let event_type = registry.get_event_type(event_name);
        if let Some(et) = event_type {
            assert_eq!(et.source(), "terminal_kitty");
            assert!(!et.description().is_empty());
        }
    }
}