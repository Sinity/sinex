use crate::common::prelude::*;
// Using create_registry from prelude

#[sinex_test]
async fn test_event_registry_creation(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Verify registry is not empty
    assert!(
        !registry.event_types.is_empty(),
        "Registry should contain event types"
    );

    // Verify some expected event types are present
    assert!(
        registry.event_types.contains(&"file.created"),
        "Registry should contain file.created event type"
    );
    assert!(
        registry.event_types.contains(&"command.executed"),
        "Registry should contain command.executed event type"
    );
    assert!(
        registry.event_types.contains(&"window.focused"),
        "Registry should contain window.focused event type"
    );
    Ok(())
}

#[sinex_test]
async fn test_event_registry_get_event_type(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Test getting source for event type
    let source = registry.source_for_event("file.created");
    assert!(
        source.is_some(),
        "Should find source for file.created event"
    );
    pretty_assertions::assert_eq!(
        source.unwrap(),
        "fs",
        "file.created should map to filesystem source"
    );

    // Test getting non-existent event type
    let unknown = registry.source_for_event("nonexistent.event");
    assert!(
        unknown.is_none(),
        "Should not find source for nonexistent event type"
    );
    Ok(())
}

#[sinex_test]
async fn test_event_registry_get_source_events(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Test filesystem source
    let fs_events = registry.events_for_source("fs");
    assert!(
        !fs_events.is_empty(),
        "Filesystem source should have events"
    );
    assert!(
        fs_events.contains(&"file.created"),
        "Filesystem should contain file.created event"
    );
    assert!(
        fs_events.contains(&"file.modified"),
        "Filesystem should contain file.modified event"
    );
    assert!(
        fs_events.contains(&"file.deleted"),
        "Filesystem should contain file.deleted event"
    );

    // Test terminal source
    let shell_events = registry.events_for_source("shell.kitty");
    assert!(
        !shell_events.is_empty(),
        "Terminal source should have events"
    );
    assert!(
        shell_events.contains(&"command.executed"),
        "Terminal should contain command.executed event"
    );

    // Test non-existent source
    let unknown_events = registry.events_for_source("unknown_source");
    assert!(
        unknown_events.is_empty(),
        "Unknown source should have no events"
    );
    Ok(())
}

#[sinex_test]
async fn test_event_registry_all_sources(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Get all unique sources from the event_to_source mapping
    let mut sources: Vec<&str> = registry
        .event_to_source
        .iter()
        .map(|(_, source)| *source)
        .collect();
    sources.sort();
    sources.dedup();

    assert!(!sources.is_empty(), "Registry should contain sources");

    // Verify expected sources are present
    assert!(
        sources.contains(&"fs"),
        "Should contain filesystem source"
    );
    assert!(
        sources.contains(&"shell.kitty"),
        "Should contain shell.kitty source"
    );
    assert!(
        sources.contains(&"wm.hyprland"),
        "Should contain wm.hyprland source"
    );
    assert!(
        sources.contains(&"clipboard"),
        "Should contain clipboard source"
    );
    Ok(())
}

#[sinex_test]
async fn test_event_registry_event_type_properties(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Test file.created event type
    let source = registry.source_for_event("file.created");
    assert!(source.is_some(), "Should find source for file.created");
    pretty_assertions::assert_eq!(
        source.unwrap(),
        "fs",
        "file.created should map to filesystem"
    );

    // Test that we can get schema for event type
    let schema = registry.schema_for_event("file.created");
    assert!(schema.is_some(), "Should find schema for file.created");

    // Test command.executed event type
    let cmd_source = registry.source_for_event("command.executed");
    assert!(
        cmd_source.is_some(),
        "Should find source for command.executed"
    );
    pretty_assertions::assert_eq!(
        cmd_source.unwrap(),
        "shell.kitty",
        "command.executed should map to shell.kitty"
    );
    Ok(())
}

#[sinex_test]
async fn test_event_registry_validate_payload(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Test that schemas are available for validation
    let schema = registry.schema_for_event("file.created");
    assert!(schema.is_some(), "Should find schema for file.created");

    // We can't validate directly through the registry as it doesn't have validate methods
    // but we can verify schemas exist for known event types
    assert!(
        registry.schema_for_event("file.modified").is_some(),
        "Should find schema for file.modified"
    );
    assert!(
        registry.schema_for_event("file.deleted").is_some(),
        "Should find schema for file.deleted"
    );
    assert!(
        registry.schema_for_event("command.executed").is_some(),
        "Should find schema for command.executed"
    );
    Ok(())
}

#[sinex_test]
async fn test_event_registry_immutability(_ctx: TestContext) -> TestResult {
    let registry1 = create_registry();
    let registry2 = create_registry();

    // Registries should have consistent content
    pretty_assertions::assert_eq!(
        registry1.event_types.len(),
        registry2.event_types.len(),
        "Registry instances should have same number of event types"
    );

    // Test that sources are consistent
    let source1 = registry1.source_for_event("file.created");
    let source2 = registry2.source_for_event("file.created");
    pretty_assertions::assert_eq!(
        source1,
        source2,
        "Registry instances should return same source mappings"
    );
    Ok(())
}

#[sinex_test]
async fn test_event_registry_schema_lookup(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Test schema lookup by event type name
    let schema = registry.schema_for_event("file.created");
    assert!(schema.is_some(), "Should find schema for file.created");

    // Schema generators exist for known event types
    assert!(
        registry.schema_generators.contains_key("file.created"),
        "Should have schema generator for file.created"
    );
    Ok(())
}

#[sinex_test]
async fn test_event_registry_event_source_mapping(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Test that events are properly mapped to sources
    let fs_events = registry.events_for_source("fs");
    let shell_events = registry.events_for_source("shell.kitty");

    // Verify filesystem events are mapped to filesystem source
    for event in &fs_events {
        let source = registry.source_for_event(event);
        pretty_assertions::assert_eq!(
            source,
            Some("fs"),
            "Filesystem event {} should map to filesystem source",
            event
        );
    }

    // Verify terminal events are mapped to terminal source
    for event in &shell_events {
        let source = registry.source_for_event(event);
        pretty_assertions::assert_eq!(
            source,
            Some("shell.kitty"),
            "Terminal event {} should map to shell.kitty source",
            event
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_event_registry_concurrent_access(_ctx: TestContext) -> TestResult {
    use std::thread;

    let registry = Arc::new(create_registry());
    let mut handles = vec![];

    // Spawn multiple threads accessing registry
    for i in 0..10 {
        let registry_clone = Arc::clone(&registry);
        let handle = thread::spawn(move || {
            let event_type = if i % 2 == 0 {
                "file.created"
            } else {
                "command.executed"
            };
            // Just check that we can look up sources concurrently
            let source = registry_clone.source_for_event(event_type);
            assert!(
                source.is_some(),
                "Should find source for event type {}",
                event_type
            );
            source.unwrap().to_string()
        });
        handles.push(handle);
    }

    // Wait for all threads and verify results
    for handle in handles {
        let result = handle.join().unwrap();
        assert!(
            result == "fs" || result == "shell.kitty",
            "Source should be filesystem or shell.kitty, got: {}",
            result
        );
    }
    Ok(())
}

// Integration test with actual event types
#[sinex_test]
async fn test_event_registry_with_real_events(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Test with actual event types from sinex-events
    let filesystem_events = [
        "file.created",
        "file.modified",
        "file.deleted",
        "directory.created",
        "directory.deleted",
    ];

    for event_name in filesystem_events {
        let source = registry.source_for_event(event_name);
        if source.is_some() {
            pretty_assertions::assert_eq!(
                source.unwrap(),
                "fs",
                "Filesystem event {} should map to filesystem source",
                event_name
            );
        }
    }

    let shell_events = ["command.executed", "session.started", "session.ended"];

    for event_name in shell_events {
        let source = registry.source_for_event(event_name);
        if source.is_some() {
            pretty_assertions::assert_eq!(
                source.unwrap(),
                "shell.kitty",
                "Terminal event {} should map to shell.kitty source",
                event_name
            );
        }
    }
    Ok(())
}
