use crate::common::prelude::*;
use sinex_core::create_registry;

#[test]
fn test_event_registry_creation() {
    let registry = create_registry();
    
    // Verify registry is not empty
    assert!(!registry.event_types.is_empty());
    
    // Verify some expected event types are present
    assert!(registry.event_types.contains(&"file.created"));
    assert!(registry.event_types.contains(&"command.executed"));
    assert!(registry.event_types.contains(&"window.focused"));
}

#[test]
fn test_event_registry_get_event_type() {
    let registry = create_registry();
    
    // Test getting source for event type
    let source = registry.source_for_event("file.created");
    assert!(source.is_some());
    pretty_assertions::assert_eq!(source.unwrap(), "filesystem");
    
    // Test getting non-existent event type
    let unknown = registry.source_for_event("nonexistent.event");
    assert!(unknown.is_none());
}

#[test]
fn test_event_registry_get_source_events() {
    let registry = create_registry();
    
    // Test filesystem source
    let fs_events = registry.events_for_source("filesystem");
    assert!(!fs_events.is_empty());
    assert!(fs_events.contains(&"file.created"));
    assert!(fs_events.contains(&"file.modified"));
    assert!(fs_events.contains(&"file.deleted"));
    
    // Test terminal source
    let terminal_events = registry.events_for_source("terminal.kitty");
    assert!(!terminal_events.is_empty());
    assert!(terminal_events.contains(&"command.executed"));
    
    // Test non-existent source
    let unknown_events = registry.events_for_source("unknown_source");
    assert!(unknown_events.is_empty());
}

#[test]
fn test_event_registry_all_sources() {
    let registry = create_registry();
    
    // Get all unique sources from the event_to_source mapping
    let mut sources: Vec<&str> = registry.event_to_source
        .iter()
        .map(|(_, source)| *source)
        .collect();
    sources.sort();
    sources.dedup();
    
    assert!(!sources.is_empty());
    
    // Verify expected sources are present
    assert!(sources.contains(&"filesystem"));
    assert!(sources.contains(&"terminal.kitty"));
    assert!(sources.contains(&"window_manager.hyprland"));
    assert!(sources.contains(&"clipboard"));
}

#[test]
fn test_event_registry_event_type_properties() {
    let registry = create_registry();
    
    // Test file.created event type
    let source = registry.source_for_event("file.created");
    assert!(source.is_some());
    pretty_assertions::assert_eq!(source.unwrap(), "filesystem");
    
    // Test that we can get schema for event type
    let schema = registry.schema_for_event("file.created");
    assert!(schema.is_some());
    
    // Test command.executed event type
    let cmd_source = registry.source_for_event("command.executed");
    assert!(cmd_source.is_some());
    pretty_assertions::assert_eq!(cmd_source.unwrap(), "terminal.kitty");
}

#[test]
fn test_event_registry_validate_payload() {
    let registry = create_registry();
    
    // Test that schemas are available for validation
    let schema = registry.schema_for_event("file.created");
    assert!(schema.is_some());
    
    // We can't validate directly through the registry as it doesn't have validate methods
    // but we can verify schemas exist for known event types
    assert!(registry.schema_for_event("file.modified").is_some());
    assert!(registry.schema_for_event("file.deleted").is_some());
    assert!(registry.schema_for_event("command.executed").is_some());
}

#[test]
fn test_event_registry_immutability() {
    let registry1 = create_registry();
    let registry2 = create_registry();
    
    // Registries should have consistent content
    pretty_assertions::assert_eq!(
        registry1.event_types.len(),
        registry2.event_types.len()
    );
    
    // Test that sources are consistent
    let source1 = registry1.source_for_event("file.created");
    let source2 = registry2.source_for_event("file.created");
    pretty_assertions::assert_eq!(source1, source2);
}

#[test]
fn test_event_registry_schema_lookup() {
    let registry = create_registry();
    
    // Test schema lookup by event type name
    let schema = registry.schema_for_event("file.created");
    assert!(schema.is_some());
    
    // Schema generators exist for known event types
    assert!(registry.schema_generators.contains_key("file.created"));
}

#[test]
fn test_event_registry_event_source_mapping() {
    let registry = create_registry();
    
    // Test that events are properly mapped to sources
    let fs_events = registry.events_for_source("filesystem");
    let terminal_events = registry.events_for_source("terminal_kitty");
    
    // Verify filesystem events are mapped to filesystem source
    for event in &fs_events {
        let source = registry.source_for_event(event);
        pretty_assertions::assert_eq!(source, Some("filesystem"));
    }
    
    // Verify terminal events are mapped to terminal source
    for event in &terminal_events {
        let source = registry.source_for_event(event);
        pretty_assertions::assert_eq!(source, Some("terminal_kitty"));
    }
}

#[test]
fn test_event_registry_concurrent_access() {
    use std::thread;
    
    let registry = Arc::new(create_registry());
    let mut handles = vec![];
    
    // Spawn multiple threads accessing registry
    for i in 0..10 {
        let registry_clone = Arc::clone(&registry);
        let handle = thread::spawn(move || {
            let event_type = if i % 2 == 0 { "file.created" } else { "command.executed" };
            // Just check that we can look up sources concurrently
            let source = registry_clone.source_for_event(event_type);
            assert!(source.is_some());
            source.unwrap().to_string()
        });
        handles.push(handle);
    }
    
    // Wait for all threads and verify results
    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result == "filesystem" || result == "terminal.kitty");
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
        let source = registry.source_for_event(event_name);
        if source.is_some() {
            pretty_assertions::assert_eq!(source.unwrap(), "filesystem");
        }
    }
    
    let terminal_events = [
        "command.executed",
        "session.started",
        "session.ended"
    ];
    
    for event_name in terminal_events {
        let source = registry.source_for_event(event_name);
        if source.is_some() {
            pretty_assertions::assert_eq!(source.unwrap(), "terminal.kitty");
        }
    }
}