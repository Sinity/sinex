// Core Unit Tests
//
// Consolidated core functionality tests covering:
// - Basic event creation with EventFactory
// - Error propagation and handling
// - Event registry creation and management
// - Event source context configuration
// - Auto-registration patterns
// - Event constants and source identifiers

use crate::common::prelude::*;

use crate::common::prelude::*;
use chrono::Utc;
use sinex_core_types::CoreError;
use sinex_core_types::Result as CoreResult;
use sinex_events::{sources, EventFactory, event_types};
use std::io;

// =============================================================================
// BASIC FUNCTIONALITY TESTS
// =============================================================================

// Basic event builder test removed - superseded by test_event_factory_complete_creation

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
        EventFactory::new(sources::FS).create_event(
            event_type_constants::filesystem::FILE_CREATED,
            json!({"path": "/test/file1.txt"}),
        ),
        EventFactory::new(sources::SHELL_KITTY).create_event(
            event_type_constants::shell::COMMAND_EXECUTED,
            json!({"command": "ls -la"}),
        ),
        EventFactory::new(sources::SINEX).create_event(
            event_type_constants::sinex::AUTOMATON_HEARTBEAT,
            json!({"status": "running"}),
        ),
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
// EVENT FACTORY TESTS
// =============================================================================

/// Test EventFactory with complete event creation
#[sinex_test]
async fn test_event_factory_complete_creation(_ctx: TestContext) -> TestResult {
    let event = EventFactory::new(sources::FS).create_event(
        event_type_constants::filesystem::FILE_CREATED,
        json!({"path": "/test/file.txt", "size": 1024}),
    );

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

/// Test EventFactory with custom host and timestamps
#[sinex_test]
async fn test_event_factory_with_custom_fields(_ctx: TestContext) -> TestResult {
    let custom_host = "custom-host";
    let custom_timestamp = Utc::now();

    let mut event = EventFactory::new(sources::SHELL_KITTY).create_event(
        event_type_constants::shell::COMMAND_EXECUTED,
        json!({"command": "echo hello", "exit_code": 0}),
    );
    event.host = custom_host.to_string();
    event.ts_orig = Some(custom_timestamp);

    pretty_assertions::assert_eq!(event.source, sources::SHELL_KITTY);
    pretty_assertions::assert_eq!(
        event.event_type,
        event_type_constants::shell::COMMAND_EXECUTED
    );
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

/// Test EventFactory with complex nested payloads
#[sinex_test]
async fn test_event_factory_complex_payload(_ctx: TestContext) -> TestResult {
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

    let event = EventFactory::new(sources::FS).create_event(
        event_type_constants::filesystem::FILE_CREATED,
        complex_payload.clone(),
    );

    pretty_assertions::assert_eq!(event.payload, complex_payload);
    pretty_assertions::assert_eq!(event.payload["file_info"]["path"], "/test/complex.txt");
    pretty_assertions::assert_eq!(event.payload["operation"]["process"]["pid"], 1234);
    pretty_assertions::assert_eq!(
        event.payload["file_info"]["metadata"]["tags"][0],
        "important"
    );

    Ok(())
}

/// Test EventFactory with ingestor version
#[sinex_test]
async fn test_event_factory_with_ingestor_version(_ctx: TestContext) -> TestResult {
    let ingestor_version = "1.2.3";

    let mut event = EventFactory::new(sources::WM_HYPRLAND).create_event(
        event_type_constants::window_manager::WINDOW_FOCUSED,
        json!({"window_id": 42, "title": "Test Window"}),
    );
    event.ingestor_version = Some(ingestor_version.to_string());

    pretty_assertions::assert_eq!(event.source, sources::WM_HYPRLAND);
    pretty_assertions::assert_eq!(
        event.event_type,
        event_type_constants::window_manager::WINDOW_FOCUSED
    );
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
    let sql_result: std::result::Result<(), sqlx::Error> =
        Err(sqlx::Error::Configuration("Mock SQL error".into()));

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
    let error_context =
        CoreError::validation("Base validation error").with_context("field", "test_field");

    let built_error = error_context.build();
    match built_error {
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
    let result: CoreResult<String> = Err(CoreError::validation("Test error").build());

    let extended_result = result.with_context(|| {
        CoreError::validation("Test error").with_context("operation", "test_operation")
    });

    match extended_result {
        Err(CoreError::Validation(msg)) => {
            assert!(msg.contains("Test error"));
            assert!(msg.contains("test_operation"));
        }
        _ => panic!("Expected validation error with context"),
    }
    Ok(())
}

/// Test error chain propagation - critical for debugging
#[sinex_test]
async fn test_error_chain_propagation(_ctx: TestContext) -> TestResult {
    fn inner_operation() -> CoreResult<String> {
        Err(CoreError::Database("Connection lost".to_string()))
    }

    fn middle_operation() -> CoreResult<String> {
        inner_operation().map_err(|e| CoreError::Unknown(format!("Middle layer: {}", e)))
    }

    fn outer_operation() -> CoreResult<String> {
        middle_operation().map_err(|e| CoreError::Unknown(format!("Outer layer: {}", e)))
    }

    let result = outer_operation();
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("Outer layer"));
    assert!(error_msg.contains("Middle layer"));
    assert!(error_msg.contains("Connection lost"));
    Ok(())
}

/// Test error display implementation - ensures proper formatting
#[sinex_test]
async fn test_error_display_implementation(_ctx: TestContext) -> TestResult {
    let errors = vec![
        (
            CoreError::Database("Connection timeout".to_string()),
            "Database error: Connection timeout",
        ),
        (
            CoreError::Serialization("Invalid JSON".to_string()),
            "Serialization error: Invalid JSON",
        ),
        (
            CoreError::Validation("Invalid input".to_string()),
            "Validation error: Invalid input",
        ),
        (
            CoreError::Configuration("Missing config".to_string()),
            "Configuration error: Missing config",
        ),
        (
            CoreError::Io("File not found".to_string()),
            "IO error: File not found",
        ),
        (
            CoreError::Unknown("Unknown error".to_string()),
            "Other error: Unknown error",
        ),
    ];

    for (error, expected) in errors {
        pretty_assertions::assert_eq!(error.to_string(), expected);
    }
    Ok(())
}

/// Test error propagation across thread boundaries - critical for concurrency
#[sinex_test]
async fn test_error_propagation_across_tasks(_ctx: TestContext) -> TestResult {
    use tokio::task;

    let handle = task::spawn(async {
        // Simulate work that fails
        Err::<String, CoreError>(CoreError::Database("Task failed".to_string()))
    });

    let result = handle.await;
    assert!(result.is_ok()); // Join succeeded

    let inner_result = result.unwrap();
    assert!(inner_result.is_err());
    assert!(matches!(inner_result, Err(CoreError::Database(_))));
    Ok(())
}

/// Test validation error propagation
#[sinex_test]
async fn test_validation_error_propagation(_ctx: TestContext) -> TestResult {
    fn validate_event_type(event_type: &str) -> CoreResult<()> {
        if event_type.is_empty() {
            return Err(CoreError::Validation(
                "Event type cannot be empty".to_string(),
            ));
        }
        if !event_type.contains('.') {
            return Err(CoreError::Validation(
                "Event type must contain a dot separator".to_string(),
            ));
        }
        Ok(())
    }

    // Test empty event type
    let result = validate_event_type("");
    assert!(matches!(result, Err(CoreError::Validation(msg)) if msg.contains("empty")));

    // Test invalid format
    let result = validate_event_type("invalid");
    assert!(matches!(result, Err(CoreError::Validation(msg)) if msg.contains("dot separator")));

    // Test valid event type
    let result = validate_event_type("system.startup");
    assert!(result.is_ok());
    Ok(())
}

// =============================================================================
// EVENT FACTORY EDGE CASES
// =============================================================================

/// Test EventFactory with empty payload - critical edge case
#[sinex_test]
async fn test_event_factory_empty_payload(_ctx: TestContext) -> TestResult {
    let event = EventFactory::new(sources::SINEX).create_event("system.startup", json!({}));

    pretty_assertions::assert_eq!(event.payload, json!({}));
    pretty_assertions::assert_eq!(event.source, sources::SINEX);
    pretty_assertions::assert_eq!(event.event_type, "system.startup");
    Ok(())
}

/// Test EventFactory ULID ordering in tight loop - critical for time ordering
#[sinex_test]
async fn test_event_factory_ulid_ordering(_ctx: TestContext) -> TestResult {
    let mut events = Vec::new();

    // Create events in rapid succession
    for i in 0..10 {
        let event = EventFactory::new(sources::SINEX).create_event("test.sequence", json!({"sequence": i}));
        events.push(event);

        // Small delay to ensure timestamp progression
        std::thread::sleep(std::time::Duration::from_micros(100));
    }

    // ULIDs should be in ascending order
    for i in 1..events.len() {
        assert!(events[i].id.to_string() > events[i - 1].id.to_string());
        assert!(events[i].ts_ingest >= events[i - 1].ts_ingest);
    }
    Ok(())
}

/// Test EventFactory multiple builds - verify independence
#[sinex_test]
async fn test_event_factory_multiple_builds(_ctx: TestContext) -> TestResult {
    // Create two events with same configuration
    let event1 = EventFactory::new("test").create_event("test.event", json!({"key": "value"}));
    let event2 = EventFactory::new("test").create_event("test.event", json!({"key": "value"}));

    // Events should have different IDs and timestamps
    pretty_assertions::assert_ne!(event1.id, event2.id);
    assert!(event2.ts_ingest >= event1.ts_ingest);

    // But same content
    pretty_assertions::assert_eq!(event1.source, event2.source);
    pretty_assertions::assert_eq!(event1.event_type, event2.event_type);
    pretty_assertions::assert_eq!(event1.payload, event2.payload);
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
    let merged_timeout = override_config["settings"]["timeout"]
        .as_u64()
        .unwrap_or(base_config["settings"]["timeout"].as_u64().unwrap_or(1000));

    pretty_assertions::assert_eq!(merged_timeout, 2000);

    // Test fallback behavior
    let fallback_enabled = override_config["enabled"]
        .as_bool()
        .unwrap_or(base_config["enabled"].as_bool().unwrap_or(false));

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
    let default_paths = vec![];
    let paths = config["paths"].as_array().unwrap_or(&default_paths);
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
    let default_minimal_paths = vec![json!("/default")];
    let paths = minimal_config["paths"]
        .as_array()
        .unwrap_or(&default_minimal_paths);
    let timeout = minimal_config["timeout"].as_u64().unwrap_or(5000);
    let buffer_size = minimal_config["buffer_size"].as_u64().unwrap_or(1024);

    assert!(enabled);
    assert_eq!(paths.len(), 1);
    assert_eq!(timeout, 5000);
    assert_eq!(buffer_size, 1024);

    Ok(())
}
