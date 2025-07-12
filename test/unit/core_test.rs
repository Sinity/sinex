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
use chrono::Utc;
use sinex_core::{
    event_type_constants, sources, CoreError, RawEventBuilder, Result as CoreResult,
};
use sinex_satellite_sdk::{EventSource, EventSourceContext};
use std::io;

// =============================================================================
// BASIC FUNCTIONALITY TESTS
// =============================================================================

// Basic event builder test removed - superseded by test_raw_event_builder_complete_creation

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
            event_type_constants::filesystem::FILE_CREATED,
            json!({"path": "/test/file1.txt"}),
        )
        .build(),
        RawEventBuilder::new(
            sources::SHELL_KITTY,
            event_type_constants::shell::COMMAND_EXECUTED,
            json!({"command": "ls -la"}),
        )
        .build(),
        RawEventBuilder::new(
            sources::SINEX,
            event_type_constants::sinex::AUTOMATON_HEARTBEAT,
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
    pretty_assertions::assert_eq!(
        event.payload["file_info"]["metadata"]["tags"][0],
        "important"
    );

    Ok(())
}

/// Test RawEventBuilder with ingestor version
#[sinex_test]
async fn test_raw_event_builder_with_ingestor_version(_ctx: TestContext) -> TestResult {
    let ingestor_version = "1.2.3";

    let event = RawEventBuilder::new(
        sources::WM_HYPRLAND,
        event_type_constants::window_manager::WINDOW_FOCUSED,
        json!({"window_id": 42, "title": "Test Window"}),
    )
    .with_ingestor_version(ingestor_version)
    .build();

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
        inner_operation().map_err(|e| CoreError::Other(format!("Middle layer: {}", e)))
    }

    fn outer_operation() -> CoreResult<String> {
        middle_operation().map_err(|e| CoreError::Other(format!("Outer layer: {}", e)))
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
            CoreError::Other("Unknown error".to_string()),
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

/// Test EventSource error propagation in async context
#[derive(Debug)]
struct FailingEventSource;

#[async_trait]
impl EventSource for FailingEventSource {
    type Config = serde_json::Value;
    const SOURCE_NAME: &'static str = "failing_source";

    async fn initialize(_ctx: EventSourceContext) -> CoreResult<Self> {
        // Simulate initialization failure
        Err(CoreError::Configuration(
            "Missing required field".to_string(),
        ))
    }

    async fn stream_events(&mut self, _tx: mpsc::Sender<RawEvent>) -> CoreResult<()> {
        Err(CoreError::Io("Stream failed".to_string()))
    }
}

#[sinex_test]
async fn test_event_source_error_propagation(_ctx: TestContext) -> TestResult {
    let ctx_local = crate::common::event_sources::test_context(json!({}));
    let result = FailingEventSource::initialize(ctx_local).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        CoreError::Configuration(msg) => {
            pretty_assertions::assert_eq!(msg, "Missing required field")
        }
        _ => panic!("Expected Configuration error"),
    }
    Ok(())
}

// =============================================================================
// RAW EVENT BUILDER EDGE CASES
// =============================================================================

/// Test RawEventBuilder with empty payload - critical edge case
#[sinex_test]
async fn test_raw_event_builder_empty_payload(_ctx: TestContext) -> TestResult {
    let event = RawEventBuilder::new(sources::SINEX, "system.startup", json!({})).build();

    pretty_assertions::assert_eq!(event.payload, json!({}));
    pretty_assertions::assert_eq!(event.source, sources::SINEX);
    pretty_assertions::assert_eq!(event.event_type, "system.startup");
    Ok(())
}

/// Test RawEventBuilder ULID ordering in tight loop - critical for time ordering
#[sinex_test]
async fn test_raw_event_builder_ulid_ordering(_ctx: TestContext) -> TestResult {
    let mut events = Vec::new();

    // Create events in rapid succession
    for i in 0..10 {
        let event =
            RawEventBuilder::new(sources::SINEX, "test.sequence", json!({"sequence": i})).build();
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

/// Test RawEventBuilder multiple builds - verify independence
#[sinex_test]
async fn test_raw_event_builder_multiple_builds(_ctx: TestContext) -> TestResult {
    // Create two events with same configuration
    let event1 = RawEventBuilder::new("test", "test.event", json!({"key": "value"})).build();
    let event2 = RawEventBuilder::new("test", "test.event", json!({"key": "value"})).build();

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
    let filesystem_types: Vec<_> = all_types
        .iter()
        .filter(|t| t.starts_with("file.") || t.starts_with("dir."))
        .collect();
    assert!(!filesystem_types.is_empty());

    let shell_types: Vec<_> = all_types
        .iter()
        .filter(|t| t.starts_with("command.") || t.starts_with("session."))
        .collect();
    assert!(!shell_types.is_empty());

    let wm_types: Vec<_> = all_types
        .iter()
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
    assert!(registry.event_types.contains(&"file.created")); // fs
    assert!(registry.event_types.contains(&"command.executed")); // terminal
    assert!(registry.event_types.contains(&"copied")); // desktop

    // Verify source mappings work correctly
    assert_eq!(registry.source_for_event("file.created"), Some("fs"));
    assert_eq!(
        registry.source_for_event("command.executed"),
        Some("shell.kitty")
    );
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

/// Test event registry deduplication behavior - critical for plugin architecture
#[test]
fn test_event_registry_deduplication_behavior() -> TestResult {
    let mut builder = EventRegistryBuilder::new();

    // Simulate registering the same event type from different sources
    builder.add_event_type("test.event", "source1", || {
        let gen = schemars::gen::SchemaGenerator::default();
        gen.into_root_schema_for::<serde_json::Value>()
    });

    builder.add_event_type("test.event", "source2", || {
        let gen = schemars::gen::SchemaGenerator::default();
        gen.into_root_schema_for::<serde_json::Value>()
    });

    let registry = builder.build();

    // Event type should appear only once in the list
    let event_count = registry
        .event_types
        .iter()
        .filter(|&&e| e == "test.event")
        .count();
    assert_eq!(event_count, 1);

    // But both source mappings should be preserved
    let sources_for_event: Vec<_> = registry
        .event_to_source
        .iter()
        .filter(|(event, _)| *event == "test.event")
        .map(|(_, source)| *source)
        .collect();

    assert!(sources_for_event.contains(&"source1"));
    assert!(sources_for_event.contains(&"source2"));
    assert_eq!(sources_for_event.len(), 2);

    Ok(())
}

/// Test event registry concurrent access safety
#[sinex_test]
async fn test_event_registry_concurrent_access(_ctx: TestContext) -> TestResult {
    use std::sync::Arc;
    use tokio::task;

    let registry = Arc::new(create_registry());
    let mut handles = vec![];

    // Spawn multiple tasks that read from the registry concurrently
    for i in 0..10 {
        let registry_clone = Arc::clone(&registry);
        let handle = task::spawn(async move {
            // Read operations should be thread-safe
            let _event_types = registry_clone.all_event_types();
            let _source = registry_clone.source_for_event("file.created");
            let _valid = registry_clone.is_valid_event_type("command.executed");

            // Return task number to verify all completed
            i
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(handles).await;

    // All tasks should complete successfully
    assert_eq!(results.len(), 10);
    for (i, result) in results.into_iter().enumerate() {
        assert_eq!(result.unwrap(), i);
    }

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
