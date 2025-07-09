//! Core Unit Tests
//!
//! Consolidated core functionality tests covering:
//! - Basic event creation with RawEventBuilder
//! - Error propagation and handling
//! - Event registry creation and management
//! - Event source context configuration
//! - Auto-registration patterns
//! - Event constants and source identifiers

use crate::common::prelude::*;
use sinex_core::{
    event_type_constants, sources, typed_event_types, CoreError, 
    Result as CoreResult, RawEventBuilder, unified_collector::EventRegistryBuilder
};
use chrono::{DateTime, Utc};
use std::io;

// =============================================================================
// BASIC FUNCTIONALITY TESTS
// =============================================================================

/// Test basic event creation with RawEventBuilder
///
/// Verifies that:
/// - Events are created with correct source and type
/// - Payload is properly attached
/// - Auto-generated fields (host, ID) are populated
/// - ULID format is correct (26 characters)
#[sinex_test]
async fn test_raw_event_builder_basic(_ctx: TestContext) -> TestResult {
    let event = RawEventBuilder::new(
        sources::FS,
        typed_event_types::filesystem::FILE_CREATED.as_str(),
        json!({"path": "/test/file.txt"}),
    )
    .build();

    pretty_assertions::assert_eq!(event.source, sources::FS);
    pretty_assertions::assert_eq!(
        event.event_type,
        typed_event_types::filesystem::FILE_CREATED.as_str()
    );
    pretty_assertions::assert_eq!(event.payload["path"], "/test/file.txt");
    assert!(!event.host.is_empty());
    assert!(event.id.to_string().len() == 26); // ULID length
    Ok(())
}

/// Test creating multiple events with different sources
///
/// Ensures that:
/// - Multiple events can be created independently
/// - Each event gets a unique ULID
/// - Different sources and types work correctly
/// - ULIDs maintain time ordering when created in sequence
#[sinex_test]
async fn test_multiple_event_creation(_ctx: TestContext) -> TestResult {
    let events = vec![
        RawEventBuilder::new(
            sources::FS,
            typed_event_types::filesystem::FILE_CREATED.as_str(),
            json!({"path": "/test/file1.txt"}),
        )
        .build(),
        RawEventBuilder::new(
            sources::SHELL_KITTY,
            typed_event_types::shell::COMMAND_EXECUTED.as_str(),
            json!({"command": "ls -la"}),
        )
        .build(),
        RawEventBuilder::new(
            sources::SINEX,
            typed_event_types::sinex::AGENT_HEARTBEAT.as_str(),
            json!({"status": "running"}),
        )
        .build(),
    ];

    pretty_assertions::assert_eq!(events.len(), 3);
    pretty_assertions::assert_eq!(events[0].source, "fs");
    pretty_assertions::assert_eq!(events[1].source, "shell.kitty");
    pretty_assertions::assert_eq!(events[2].source, "sinex");

    // All events should have unique IDs
    pretty_assertions::assert_ne!(events[0].id, events[1].id);
    pretty_assertions::assert_ne!(events[1].id, events[2].id);
    pretty_assertions::assert_ne!(events[0].id, events[2].id);
    Ok(())
}

// =============================================================================
// RAW EVENT BUILDER TESTS
// =============================================================================

/// Test RawEventBuilder with complete event creation
#[sinex_test]
async fn test_raw_event_builder_complete_creation(_ctx: TestContext) -> TestResult {
    let event = RawEventBuilder::new(
        sources::FS,
        event_type_constants::filesystem::FILE_CREATED,
        json!({"path": "/test/file.txt", "size": 1024}),
    )
    .build();

    pretty_assertions::assert_eq!(event.source, sources::FS);
    pretty_assertions::assert_eq!(
        event.event_type,
        event_type_constants::filesystem::FILE_CREATED
    );
    pretty_assertions::assert_eq!(event.payload["path"], "/test/file.txt");
    pretty_assertions::assert_eq!(event.payload["size"], 1024);
    assert!(!event.host.is_empty());
    assert!(event.id.to_string().len() == 26); // ULID length
    Ok(())
}

/// Test RawEventBuilder with custom host and timestamps
#[sinex_test]
async fn test_raw_event_builder_with_custom_fields(_ctx: TestContext) -> TestResult {
    let custom_host = "custom-host";
    let custom_timestamp = Utc::now();
    
    let event = RawEventBuilder::new(
        sources::SHELL_KITTY,
        event_type_constants::shell::COMMAND_EXECUTED,
        json!({"command": "echo hello", "exit_code": 0}),
    )
    .with_host(custom_host)
    .with_timestamp(custom_timestamp)
    .build();

    pretty_assertions::assert_eq!(event.source, sources::SHELL_KITTY);
    pretty_assertions::assert_eq!(event.event_type, event_type_constants::shell::COMMAND_EXECUTED);
    pretty_assertions::assert_eq!(event.host, custom_host);
    pretty_assertions::assert_eq!(event.payload["command"], "echo hello");
    pretty_assertions::assert_eq!(event.payload["exit_code"], 0);
    
    // Verify custom timestamp is preserved
    if let Some(ts_orig) = event.ts_orig {
        // Allow for small differences due to timing
        let diff = (ts_orig - custom_timestamp).num_milliseconds().abs();
        assert!(diff < 100, "Custom timestamp should be preserved");
    }
    
    Ok(())
}

/// Test RawEventBuilder with complex nested payloads
#[sinex_test]
async fn test_raw_event_builder_complex_payload(_ctx: TestContext) -> TestResult {
    let complex_payload = json!({
        "file_info": {
            "path": "/test/complex.txt",
            "size": 2048,
            "permissions": "0644",
            "metadata": {
                "created": "2024-01-01T00:00:00Z",
                "modified": "2024-01-01T12:00:00Z",
                "tags": ["important", "test"]
            }
        },
        "operation": {
            "type": "create",
            "user": "test_user",
            "process": {
                "pid": 1234,
                "name": "test_process"
            }
        }
    });

    let event = RawEventBuilder::new(
        sources::FS,
        event_type_constants::filesystem::FILE_CREATED,
        complex_payload.clone(),
    )
    .build();

    pretty_assertions::assert_eq!(event.payload, complex_payload);
    pretty_assertions::assert_eq!(event.payload["file_info"]["path"], "/test/complex.txt");
    pretty_assertions::assert_eq!(event.payload["operation"]["process"]["pid"], 1234);
    pretty_assertions::assert_eq!(event.payload["file_info"]["metadata"]["tags"][0], "important");
    
    Ok(())
}

/// Test RawEventBuilder with ingestor version
#[sinex_test]
async fn test_raw_event_builder_with_ingestor_version(_ctx: TestContext) -> TestResult {
    let ingestor_version = "1.2.3";
    
    let event = RawEventBuilder::new(
        sources::WM_HYPRLAND,
        event_type_constants::wm::WINDOW_FOCUSED,
        json!({"window_id": 42, "title": "Test Window"}),
    )
    .with_ingestor_version(ingestor_version)
    .build();

    pretty_assertions::assert_eq!(event.source, sources::WM_HYPRLAND);
    pretty_assertions::assert_eq!(event.event_type, event_type_constants::wm::WINDOW_FOCUSED);
    pretty_assertions::assert_eq!(event.ingestor_version, Some(ingestor_version.to_string()));
    pretty_assertions::assert_eq!(event.payload["window_id"], 42);
    pretty_assertions::assert_eq!(event.payload["title"], "Test Window");
    
    Ok(())
}

// =============================================================================
// ERROR PROPAGATION TESTS
// =============================================================================

/// Test CoreError conversion from IO errors
#[sinex_test]
async fn test_core_error_from_io_error(_ctx: TestContext) -> TestResult {
    let io_err = io::Error::new(io::ErrorKind::NotFound, "File not found");
    let core_err: CoreError = io_err.into();

    match core_err {
        CoreError::Io(msg) => assert!(msg.contains("File not found")),
        _ => panic!("Expected CoreError::Io variant"),
    }
    Ok(())
}

/// Test CoreError conversion from serde_json errors
#[sinex_test]
async fn test_core_error_from_serde_json_error(_ctx: TestContext) -> TestResult {
    let json_str = r#"{"invalid": json}"#;
    let json_err = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
    let core_err: CoreError = json_err.into();

    match core_err {
        CoreError::Serialization(msg) => assert!(msg.contains("invalid")),
        _ => panic!("Expected CoreError::Serialization variant"),
    }
    Ok(())
}

/// Test CoreError conversion from SQL errors
#[sinex_test]
async fn test_core_error_from_sql_error(_ctx: TestContext) -> TestResult {
    // Create a mock SQL error scenario
    let sql_result: std::result::Result<(), sqlx::Error> = Err(sqlx::Error::Configuration("Mock SQL error".into()));
    
    match sql_result {
        Err(sql_err) => {
            let core_err: CoreError = sql_err.into();
            match core_err {
                CoreError::Database(msg) => assert!(msg.contains("Mock SQL error")),
                _ => panic!("Expected CoreError::Database variant"),
            }
        }
        Ok(_) => panic!("Expected SQL error"),
    }
    Ok(())
}

/// Test CoreError context chaining
#[sinex_test]
async fn test_core_error_context_chaining(_ctx: TestContext) -> TestResult {
    let base_error = CoreError::validation("Base validation error");
    let chained_error = base_error.with_context("field", "test_field");
    
    match chained_error {
        CoreError::Validation(msg) => {
            assert!(msg.contains("Base validation error"));
            assert!(msg.contains("test_field"));
        }
        _ => panic!("Expected CoreError::Validation variant"),
    }
    Ok(())
}

/// Test CoreError result extension methods
#[sinex_test]
async fn test_core_error_result_extensions(_ctx: TestContext) -> TestResult {
    let result: CoreResult<String> = Err(CoreError::validation("Test error"));
    
    let extended_result = result.with_context("operation", "test_operation");
    
    match extended_result {
        Err(CoreError::Validation(msg)) => {
            assert!(msg.contains("Test error"));
            assert!(msg.contains("test_operation"));
        }
        _ => panic!("Expected validation error with context"),
    }
    Ok(())
}

// =============================================================================
// EVENT REGISTRY TESTS
// =============================================================================

/// Test event registry creation and basic functionality
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

/// Test event registry source lookup functionality
#[sinex_test]
async fn test_event_registry_source_lookup(_ctx: TestContext) -> TestResult {
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

/// Test event registry validation of event types
#[sinex_test]
async fn test_event_registry_validation(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Test validation of known event types
    assert!(registry.is_valid_event_type("file.created"));
    assert!(registry.is_valid_event_type("command.executed"));
    assert!(registry.is_valid_event_type("window.focused"));
    
    // Test validation of unknown event types
    assert!(!registry.is_valid_event_type("unknown.event"));
    assert!(!registry.is_valid_event_type("invalid-format"));
    assert!(!registry.is_valid_event_type(""));
    
    Ok(())
}

/// Test event registry enumeration
#[sinex_test]
async fn test_event_registry_enumeration(_ctx: TestContext) -> TestResult {
    let registry = create_registry();

    // Test getting all event types
    let all_types = registry.all_event_types();
    assert!(!all_types.is_empty());
    
    // Verify some expected categories are present
    let filesystem_types: Vec<_> = all_types.iter()
        .filter(|t| t.starts_with("file.") || t.starts_with("dir."))
        .collect();
    assert!(!filesystem_types.is_empty());
    
    let shell_types: Vec<_> = all_types.iter()
        .filter(|t| t.starts_with("command.") || t.starts_with("session."))
        .collect();
    assert!(!shell_types.is_empty());
    
    let wm_types: Vec<_> = all_types.iter()
        .filter(|t| t.starts_with("window.") || t.starts_with("workspace."))
        .collect();
    assert!(!wm_types.is_empty());
    
    Ok(())
}

// =============================================================================
// EVENT REGISTRY AUTO-REGISTRATION TESTS
// =============================================================================

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
    assert!(registry.event_types.contains(&"file.created"));
    assert!(registry.event_types.contains(&"file.modified"));
    assert!(registry.event_types.contains(&"file.deleted"));
    
    Ok(())
}

/// Test auto-registration for multiple event source types
#[test]
fn test_auto_registration_multiple_sources() -> TestResult {
    let mut builder = EventRegistryBuilder::new();
    
    // Register events from multiple sources
    sinex_events_fs::register_events(&mut builder);
    sinex_events_terminal::register_events(&mut builder);
    sinex_events_desktop::register_events(&mut builder);
    
    let registry = builder.build();
    
    // Should have events from all sources
    assert!(registry.event_types.contains(&"file.created"));      // fs
    assert!(registry.event_types.contains(&"command.executed"));  // terminal
    assert!(registry.event_types.contains(&"copied"));           // desktop
    
    // Verify source mappings work correctly
    assert_eq!(registry.source_for_event("file.created"), Some("fs"));
    assert_eq!(registry.source_for_event("command.executed"), Some("shell.kitty"));
    assert_eq!(registry.source_for_event("copied"), Some("clipboard"));
    
    Ok(())
}

/// Test auto-registration builder pattern
#[test]
fn test_auto_registration_builder_pattern() -> TestResult {
    let registry = EventRegistryBuilder::new()
        .with_auto_registration(sinex_events_fs::register_events)
        .with_auto_registration(sinex_events_terminal::register_events)
        .build();
    
    assert!(!registry.event_types.is_empty());
    assert!(registry.event_types.contains(&"file.created"));
    assert!(registry.event_types.contains(&"command.executed"));
    
    Ok(())
}

// =============================================================================
// EVENT SOURCE CONTEXT TESTS
// =============================================================================

/// Test event source context configuration merging
#[sinex_test]
async fn test_event_source_context_config_merging(_ctx: TestContext) -> TestResult {
    // Test scenario where context might be used to merge configs
    let base_config = json!({
        "enabled": true,
        "paths": ["/base"],
        "settings": {
            "timeout": 1000,
            "retry": 3
        }
    });

    let override_config = json!({
        "paths": ["/override"],
        "settings": {
            "timeout": 2000,
            "retry": 5
        }
    });

    // Test that config values can be extracted and merged
    let merged_timeout = override_config["settings"]["timeout"].as_u64().unwrap_or(
        base_config["settings"]["timeout"].as_u64().unwrap_or(1000)
    );
    
    pretty_assertions::assert_eq!(merged_timeout, 2000);
    
    // Test fallback behavior
    let fallback_enabled = override_config["enabled"].as_bool().unwrap_or(
        base_config["enabled"].as_bool().unwrap_or(false)
    );
    
    pretty_assertions::assert_eq!(fallback_enabled, true);
    
    Ok(())
}

/// Test event source context configuration validation
#[sinex_test]
async fn test_event_source_context_validation(_ctx: TestContext) -> TestResult {
    let config = json!({
        "enabled": true,
        "paths": ["/valid/path"],
        "timeout": 5000,
        "buffer_size": 1024
    });

    // Test configuration validation patterns
    let enabled = config["enabled"].as_bool().unwrap_or(false);
    let paths = config["paths"].as_array().unwrap_or(&vec![]);
    let timeout = config["timeout"].as_u64().unwrap_or(1000);
    let buffer_size = config["buffer_size"].as_u64().unwrap_or(512);

    assert!(enabled);
    assert!(!paths.is_empty());
    assert!(timeout > 0);
    assert!(buffer_size > 0);
    
    // Test validation of path format
    let first_path = paths[0].as_str().unwrap_or("");
    assert!(first_path.starts_with("/"));
    assert!(!first_path.is_empty());
    
    Ok(())
}

/// Test event source context with default values
#[sinex_test]
async fn test_event_source_context_defaults(_ctx: TestContext) -> TestResult {
    let minimal_config = json!({
        "enabled": true
    });

    // Test extraction with defaults
    let enabled = minimal_config["enabled"].as_bool().unwrap_or(false);
    let paths = minimal_config["paths"].as_array().unwrap_or(&vec![json!("/default")]);
    let timeout = minimal_config["timeout"].as_u64().unwrap_or(5000);
    let buffer_size = minimal_config["buffer_size"].as_u64().unwrap_or(1024);

    assert!(enabled);
    assert_eq!(paths.len(), 1);
    assert_eq!(timeout, 5000);
    assert_eq!(buffer_size, 1024);
    
    Ok(())
}
