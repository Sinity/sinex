use crate::common::prelude::*;
use sinex_core::unified_collector::EventRegistryBuilder;

/// Test that demonstrates auto-registration pattern working
#[test]
fn test_auto_registration_pattern() -> TestResult {
    let builder = EventRegistryBuilder::new();
    
    // Before auto-registration, builder should be empty
    let empty_registry = builder.build();
    assert_eq!(empty_registry.event_types.len(), 0);
    
    // Create a new builder and use auto-registration
    let mut builder = EventRegistryBuilder::new();
    sinex_events_fs::register_events(&mut builder);
    let registry = builder.build();
    
    // After auto-registration, we should have filesystem events
    assert!(!registry.event_types.is_empty());
    
    // Verify specific filesystem events are registered
    assert!(registry.has_event("file.created"));
    assert!(registry.has_event("file.modified"));
    assert!(registry.has_event("file.deleted"));
    assert!(registry.has_event("file.moved"));
    assert!(registry.has_event("dir.created"));
    assert!(registry.has_event("dir.deleted"));
    
    // Verify source mappings
    assert_eq!(registry.source_for_event("file.created"), Some("fs"));
    assert_eq!(registry.source_for_event("dir.created"), Some("fs"));
    
    // Verify schema generation works
    let schema = registry.schema_for_event("file.created");
    assert!(schema.is_some());
    
    Ok(())
}

/// Test that auto-registration works with multiple crates
#[test]
fn test_multiple_crate_registration() -> TestResult {
    let mut builder = EventRegistryBuilder::new();
    
    // Register filesystem events
    sinex_events_fs::register_events(&mut builder);
    
    // TODO: When other crates implement register_events, add them here:
    // sinex_events_desktop::register_events(&mut builder);
    // sinex_events_terminal::register_events(&mut builder);
    // sinex_events_system::register_events(&mut builder);
    
    let registry = builder.build();
    
    // Filesystem events should be present
    assert!(registry.has_event("file.created"));
    
    // Verify all filesystem events are registered
    let fs_events = registry.events_for_source("fs");
    assert!(fs_events.len() >= 6); // At least 6 filesystem events
    
    Ok(())
}

/// Test that demonstrates deduplication if multiple sources emit same event
#[test]
fn test_deduplication_behavior() -> TestResult {
    let mut builder = EventRegistryBuilder::new();
    
    // Simulate registering the same event type from different sources
    builder.add_event_type(
        "test.event",
        "source1",
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<serde_json::Value>()
        }
    );
    
    builder.add_event_type(
        "test.event",
        "source2", 
        || {
            let gen = schemars::gen::SchemaGenerator::default();
            gen.into_root_schema_for::<serde_json::Value>()
        }
    );
    
    let registry = builder.build();
    
    // Event type should appear only once in the list
    let event_count = registry.event_types.iter().filter(|&&e| e == "test.event").count();
    assert_eq!(event_count, 1);
    
    // But both source mappings should be preserved
    let sources_for_event: Vec<_> = registry.event_to_source
        .iter()
        .filter(|(event, _)| *event == "test.event")
        .map(|(_, source)| *source)
        .collect();
    
    assert!(sources_for_event.contains(&"source1"));
    assert!(sources_for_event.contains(&"source2"));
    assert_eq!(sources_for_event.len(), 2);
    
    Ok(())
}

/// Test that demonstrates the value of auto-registration
#[test]
fn test_auto_registration_completeness() -> TestResult {
    // Create registry with auto-registration (this is now the production approach)
    let auto_registry = create_registry();
    
    // Print what auto-registration found for debugging
    println!("Auto-registered filesystem events:");
    for &event in auto_registry.event_types {
        if event.starts_with("file.") || event.starts_with("dir.") {
            println!("  - {}", event);
        }
    }
    
    // The key insight: auto-registration finds all events defined in the crates
    let auto_fs_events: Vec<_> = auto_registry.event_types.iter()
        .filter(|e| e.starts_with("file.") || e.starts_with("dir."))
        .collect();
    
    // Should have discovered multiple filesystem events automatically
    assert!(auto_fs_events.len() >= 6, 
        "Auto-registration should discover at least 6 filesystem events, found {}", 
        auto_fs_events.len());
    
    println!("Auto-registration found {} filesystem events - no manual maintenance needed!",
        auto_fs_events.len());
    
    // Auto-registered events should have schemas
    for &event in auto_registry.event_types {
        let auto_schema = auto_registry.schema_for_event(event);
        assert!(auto_schema.is_some(), 
            "Auto-registered event {} missing schema", event);
    }
    
    // Verify comprehensive coverage of event crates
    let all_sources: Vec<_> = auto_registry.all_sources();
    assert!(all_sources.contains(&"fs"), "Should include filesystem source");
    assert!(all_sources.contains(&"clipboard"), "Should include clipboard source");
    assert!(all_sources.contains(&"shell.kitty"), "Should include shell source");
    assert!(all_sources.contains(&"wm.hyprland"), "Should include window manager source");
    
    Ok(())
}