use serde_json::json;
use sinex_shared::{EventValidator, ValidationError, RawEventBuilder, sources, event_type_constants};

#[test]
fn test_filesystem_event_validation_success() {
    let validator = EventValidator::new();
    
    // Test all valid filesystem event types
    let test_cases = vec![
        (
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "path": "/home/user/test.txt",
                "size": 1024,
                "permissions": "644"
            }),
        ),
        (
            event_type_constants::filesystem::FILE_MODIFIED,
            json!({
                "path": "/home/user/test.txt",
                "old_size": 1024,
                "new_size": 2048,
                "modification_type": "content_change"
            }),
        ),
        (
            event_type_constants::filesystem::FILE_DELETED,
            json!({
                "path": "/home/user/test.txt",
                "was_directory": false
            }),
        ),
        (
            event_type_constants::filesystem::FILE_RENAMED,
            json!({
                "old_path": "/home/user/old.txt",
                "new_path": "/home/user/new.txt",
                "is_directory": false
            }),
        ),
    ];
    
    for (event_type, payload) in test_cases {
        let result = validator.validate(sources::FILESYSTEM, event_type, &payload);
        assert!(result.is_ok(), "Failed to validate {} with payload {:?}", event_type, payload);
    }
}

#[test]
fn test_filesystem_validation_failures() {
    let validator = EventValidator::new();
    
    // Missing required field
    let result = validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        &json!({"size": 1024}) // Missing "path"
    );
    assert!(matches!(result.unwrap_err(), ValidationError::MissingField { field } if field == "path"));
    
    // Wrong type for field
    let result = validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        &json!({
            "path": "/test.txt",
            "size": "not_a_number"
        })
    );
    assert!(matches!(result.unwrap_err(), ValidationError::InvalidType { field, .. } if field == "size"));
    
    // Invalid permissions format
    let result = validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        &json!({
            "path": "/test.txt",
            "size": 1024,
            "permissions": "999" // Invalid octal
        })
    );
    assert!(matches!(result.unwrap_err(), ValidationError::InvalidValue { field, .. } if field == "permissions"));
    
    // Empty path
    let result = validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        &json!({
            "path": "",
            "size": 1024
        })
    );
    assert!(matches!(result.unwrap_err(), ValidationError::InvalidValue { field, .. } if field == "path"));
}

#[test]
fn test_hyprland_event_validation() {
    let validator = EventValidator::new();
    
    // Valid window_focused
    assert!(validator.validate(
        sources::HYPRLAND,
        event_type_constants::hyprland::WINDOW_FOCUSED,
        &json!({
            "window": {"class": "firefox", "title": "Test"},
            "workspace": 2
        })
    ).is_ok());
    
    // Valid workspace_changed
    assert!(validator.validate(
        sources::HYPRLAND,
        event_type_constants::hyprland::WORKSPACE_CHANGED,
        &json!({
            "workspace": "special:scratchpad"
        })
    ).is_ok());
    
    // Missing window field
    let result = validator.validate(
        sources::HYPRLAND,
        event_type_constants::hyprland::WINDOW_FOCUSED,
        &json!({
            "workspace": 1
        })
    );
    assert!(matches!(result.unwrap_err(), ValidationError::MissingField { field } if field == "window"));
}

#[test]
fn test_terminal_event_validation() {
    let validator = EventValidator::new();
    
    // Valid command_executed
    assert!(validator.validate(
        sources::TERMINAL_KITTY,
        event_type_constants::terminal::COMMAND_EXECUTED,
        &json!({
            "command": "ls -la",
            "exit_code": 0,
            "duration": 0.5
        })
    ).is_ok());
    
    // Invalid exit_code type
    let result = validator.validate(
        sources::TERMINAL_KITTY,
        event_type_constants::terminal::COMMAND_EXECUTED,
        &json!({
            "command": "ls -la",
            "exit_code": "success" // Should be number
        })
    );
    assert!(matches!(result.unwrap_err(), ValidationError::InvalidType { field, .. } if field == "exit_code"));
}

#[test]
fn test_sinex_event_validation() {
    let validator = EventValidator::new();
    
    // Valid heartbeat
    assert!(validator.validate(
        sources::SINEX,
        event_type_constants::sinex::AGENT_HEARTBEAT,
        &json!({
            "agent_name": "test-agent",
            "status": "running",
            "version": "1.0.0",
            "uptime_seconds": 3600,
            "events_processed_session": 100,
            "dlq_size": 0
        })
    ).is_ok());
    
    // Invalid status type (should be string)
    let result = validator.validate(
        sources::SINEX,
        event_type_constants::sinex::AGENT_HEARTBEAT,
        &json!({
            "agent_name": "test-agent",
            "status": 123,  // Should be string
            "version": "1.0.0"
        })
    );
    assert!(matches!(result.unwrap_err(), ValidationError::InvalidType { field, .. } if field == "status"));
    
    // Valid error event
    assert!(validator.validate(
        sources::SINEX,
        event_type_constants::sinex::AGENT_ERROR,
        &json!({
            "agent_name": "test-agent",
            "error_message": "Connection failed",
            "severity": "high"
        })
    ).is_ok());
    
    // Invalid severity
    let result = validator.validate(
        sources::SINEX,
        event_type_constants::sinex::AGENT_ERROR,
        &json!({
            "agent_name": "test-agent",
            "error_message": "Connection failed",
            "severity": "extreme" // Not a valid level
        })
    );
    assert!(matches!(result.unwrap_err(), ValidationError::InvalidValue { field, .. } if field == "severity"));
}

#[test]
fn test_cross_source_validation_fails() {
    let validator = EventValidator::new();
    
    // Filesystem payload used for Hyprland event
    let result = validator.validate(
        sources::HYPRLAND,
        event_type_constants::hyprland::WINDOW_FOCUSED,
        &json!({
            "path": "/test.txt",
            "size": 1024
        })
    );
    assert!(result.is_err());
    
    // Hyprland payload used for filesystem event
    let result = validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        &json!({
            "window": "terminal",
            "workspace": 1
        })
    );
    assert!(result.is_err());
}

#[test]
fn test_unknown_event_types() {
    let validator = EventValidator::new();
    
    // Unknown source/type with valid object should pass
    assert!(validator.validate(
        "custom_source",
        "custom_event",
        &json!({
            "custom_field": "value"
        })
    ).is_ok());
    
    // Unknown source/type with non-object should fail
    let result = validator.validate(
        "custom_source",
        "custom_event",
        &json!("just a string")
    );
    assert!(matches!(result.unwrap_err(), ValidationError::InvalidType { .. }));
}

#[test]
fn test_edge_cases() {
    let validator = EventValidator::new();
    
    // Null values in payload
    assert!(validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        &json!({
            "path": "/test.txt",
            "size": 0, // Zero size is valid
            "metadata": null // Extra fields are allowed
        })
    ).is_ok());
    
    // Very long path
    let long_path = "/".to_string() + &"a".repeat(1000) + "/file.txt";
    assert!(validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        &json!({
            "path": long_path,
            "size": 1024
        })
    ).is_ok());
    
    // Unicode in paths
    assert!(validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        &json!({
            "path": "/home/user/文档/テスト.txt",
            "size": 1024
        })
    ).is_ok());
}

#[test]
fn test_partial_validation() {
    let validator = EventValidator::new();
    
    // FILE_MODIFIED with only modification_type (no size info)
    assert!(validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_MODIFIED,
        &json!({
            "path": "/test.txt",
            "modification_type": "metadata_change"
        })
    ).is_ok());
    
    // FILE_MODIFIED with only size info (no modification_type)
    assert!(validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_MODIFIED,
        &json!({
            "path": "/test.txt",
            "old_size": 1024,
            "new_size": 2048
        })
    ).is_ok());
    
    // FILE_MODIFIED with neither should fail
    let result = validator.validate(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_MODIFIED,
        &json!({
            "path": "/test.txt"
        })
    );
    assert!(result.is_err());
}

/// Test that RawEventBuilder creates valid events
#[test]
fn test_event_builder_creates_valid_events() {
    let validator = EventValidator::new();
    
    // Build various events and validate them
    let events = vec![
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "path": "/test.txt",
                "size": 1024
            })
        ).build(),
        
        RawEventBuilder::new(
            sources::HYPRLAND,
            event_type_constants::hyprland::WINDOW_FOCUSED,
            json!({
                "window": "firefox"
            })
        ).build(),
        
        RawEventBuilder::new(
            sources::SINEX,
            event_type_constants::sinex::AGENT_HEARTBEAT,
            json!({
                "agent_name": "test",
                "status": "running",
                "version": "1.0.0"
            })
        ).build(),
    ];
    
    for event in events {
        let result = validator.validate(&event.source, &event.event_type, &event.payload);
        assert!(result.is_ok(), "RawEventBuilder created invalid event: {:?}", result);
    }
}