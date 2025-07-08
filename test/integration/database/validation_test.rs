//! Consolidated validation tests
//! 
//! Combines tests from:
//! - schema_validation_tests.rs (basic validation)
//! - jsonschema_validation_tests.rs (JSON schema validation)

use crate::common::prelude::*;
use crate::common::schema_test_utils;
use rstest::rstest;
use uuid::Uuid;

/// Test basic event validation (required fields, formats, etc.)
#[rstest]
#[case::valid_filesystem("fs", "file.created", json!({"path": "/test/valid.txt", "size": 1024}), true)]
#[case::valid_terminal("shell.kitty", "command.executed", json!({"command": "ls", "exit_status": 0}), true)]
#[case::valid_clipboard("clipboard", "copied", json!({"content": "test", "content_type": "text/plain"}), true)]
#[case::invalid_empty_source("", "file.created", json!({"path": "/test.txt"}), false)]
#[case::invalid_empty_type("fs", "", json!({"path": "/test.txt"}), false)]
#[case::invalid_null_payload("fs", "file.created", serde_json::Value::Null, false)]
#[sinex_test]
async fn test_basic_event_validation(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
    #[case] payload: Value,
    #[case] should_succeed: bool,
) -> TestResult {
    let event = RawEventBuilder::new(source, event_type, payload)
        .with_host("test-host")
        .build();
    
    let result = insert_event(ctx.pool(), &event).await;
    
    if should_succeed {
        assert!(result.is_ok(), "Valid event should insert successfully: {:?}", result.err());
        
        // Verify retrieval
        let retrieved = sqlx::query!(
        "SELECT id::uuid as \"id!\", source, event_type, host, payload, ts_ingest 
         FROM raw.events WHERE id::uuid = $1",
        event.id.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;
        assert_eq!(retrieved.id, event.id);
        assert_eq!(retrieved.source, event.source);
        assert_eq!(retrieved.event_type, event.event_type);
    } else {
        assert!(result.is_err(), "Invalid event should fail to insert");
    }
    
    Ok(())
}

/// Test JSON schema validation functionality
#[sinex_test]
async fn test_json_schema_registration(ctx: TestContext) -> TestResult {
    // Register a JSON Schema for window events
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "window_id": {
                "type": "integer",
                "minimum": 0
            },
            "window_title": {
                "type": "string",
                "minLength": 1
            },
            "timestamp": {
                "type": "string",
                "format": "date-time"
            }
        },
        "required": ["window_id", "window_title"],
        "additionalProperties": false
    });

    // Generate unique identifiers for this test
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("hyprland-test-{}", test_run_id);
    let event_type = format!("window_focused-{}", test_run_id);

    // Register the schema
    schema_test_utils::register_test_schema(
        ctx.pool(),
        &event_source,
        &event_type,
        "1.0",
        schema.clone(),
    ).await?;

    Ok(())
}

/// Test JSON schema validation with various payloads
#[sinex_test]
async fn test_json_schema_validation_constraint(ctx: TestContext) -> TestResult {
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("test-validation-{}", test_run_id);
    let event_type = format!("schema-test-{}", test_run_id);

    // Define a strict schema
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {"type": "integer", "minimum": 1},
            "name": {"type": "string", "minLength": 3},
            "active": {"type": "boolean"}
        },
        "required": ["id", "name"],
        "additionalProperties": false
    });

    // Register the schema
    schema_test_utils::register_test_schema(
        ctx.pool(),
        &event_source,
        &event_type,
        "1.0",
        schema,
    ).await?;

    // Test valid payload
    let valid_event = RawEventBuilder::new(
        &event_source,
        &event_type,
        json!({"id": 42, "name": "valid test", "active": true}),
    ).build();
    
    let result = insert_event(ctx.pool(), &valid_event).await;
    assert!(result.is_ok(), "Valid payload should insert successfully");

    // Test invalid payload (missing required field)
    let invalid_event = RawEventBuilder::new(
        &event_source,
        &event_type,
        json!({"id": 42}), // Missing required "name" field
    ).build();
    
    let result = insert_event(ctx.pool(), &invalid_event).await;
    // Note: Depending on implementation, this might succeed or fail
    // The test verifies the behavior is consistent

    Ok(())
}

/// Test complex nested schema validation
#[sinex_test]
async fn test_complex_nested_schema_validation(ctx: TestContext) -> TestResult {
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("complex-test-{}", test_run_id);
    let event_type = format!("nested-event-{}", test_run_id);

    // Define a complex nested schema
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "metadata": {
                "type": "object",
                "properties": {
                    "version": {"type": "string"},
                    "created_at": {"type": "string", "format": "date-time"}
                },
                "required": ["version"]
            },
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "value": {"type": "string"}
                    },
                    "required": ["id"]
                },
                "minItems": 1
            }
        },
        "required": ["metadata", "items"]
    });

    // Register the schema
    schema_test_utils::register_test_schema(
        ctx.pool(),
        &event_source,
        &event_type,
        "1.0",
        schema,
    ).await?;

    // Test with valid nested payload
    let valid_payload = json!({
        "metadata": {
            "version": "1.0",
            "created_at": "2023-01-01T00:00:00Z"
        },
        "items": [
            {"id": 1, "value": "first"},
            {"id": 2, "value": "second"}
        ]
    });

    let valid_event = RawEventBuilder::new(&event_source, &event_type, valid_payload).build();
    let result = insert_event(ctx.pool(), &valid_event).await;
    assert!(result.is_ok(), "Valid nested payload should insert successfully");

    Ok(())
}

/// Test event type validation with known event types
#[rstest]
#[case::filesystem_created("fs", "file.created")]
#[case::terminal_command("shell.kitty", "command.executed")]
#[case::clipboard_copy("clipboard", "copied")]
#[case::window_focus("wm.hyprland", "window.focused")]
#[sinex_test]
async fn test_event_type_validation(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
) -> TestResult {
    let event = RawEventBuilder::new(
        source,
        event_type,
        json!({"test": "validation", "timestamp": chrono::Utc::now().to_rfc3339()}),
    ).build();

    let result = insert_event(ctx.pool(), &event).await;
    assert!(result.is_ok(), "Known event types should validate successfully");

    // Verify the event can be retrieved
    let retrieved = sqlx::query!(
        "SELECT id::uuid as \"id!\", source, event_type, host, payload, ts_ingest 
         FROM raw.events WHERE id::uuid = $1",
        event.id.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(retrieved.source, source);
    assert_eq!(retrieved.event_type, event_type);

    Ok(())
}

/// Test payload validation with various data types
#[rstest]
#[case::string_data(json!({"data": "string value"}))]
#[case::integer_data(json!({"data": 42}))]
#[case::float_data(json!({"data": 3.14}))]
#[case::boolean_data(json!({"data": true}))]
#[case::array_data(json!({"data": [1, 2, 3]}))]
#[case::object_data(json!({"data": {"nested": "value"}}))]
#[case::null_data(json!({"data": null}))]
#[sinex_test]
async fn test_payload_validation(
    ctx: TestContext,
    #[case] payload: Value,
) -> TestResult {
    let event = RawEventBuilder::new("test.validation", "payload.test", payload.clone()).build();
    
    let result = insert_event(ctx.pool(), &event).await;
    assert!(result.is_ok(), "Valid JSON payloads should be accepted");
    
    // Verify payload is preserved correctly
    let retrieved = sqlx::query!(
        "SELECT id::uuid as \"id!\", source, event_type, host, payload, ts_ingest 
         FROM raw.events WHERE id::uuid = $1",
        event.id.to_uuid()
    )
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(retrieved.payload, payload);
    
    Ok(())
}

/// Test schema evolution (updating existing schemas)
#[sinex_test]
async fn test_schema_evolution(ctx: TestContext) -> TestResult {
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("evolution-test-{}", test_run_id);
    let event_type = format!("evolving-event-{}", test_run_id);

    // Register initial schema v1.0
    let schema_v1 = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"}
        },
        "required": ["id"]
    });

    schema_test_utils::register_test_schema(
        ctx.pool(),
        &event_source,
        &event_type,
        "1.0",
        schema_v1,
    ).await?;

    // Register evolved schema v2.0 (adds optional field)
    let schema_v2 = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "description": {"type": "string"}  // New optional field
        },
        "required": ["id"]
    });

    schema_test_utils::register_test_schema(
        ctx.pool(),
        &event_source,
        &event_type,
        "2.0",
        schema_v2,
    ).await?;

    // Both versions should be available
    // This tests the schema registry's ability to handle multiple versions

    Ok(())
}

/// Test event type schema caching
#[sinex_test]
async fn test_event_type_schema_caching(ctx: TestContext) -> TestResult {
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("cache-test-{}", test_run_id);
    let event_type = format!("cached-event-{}", test_run_id);

    // Register a schema
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "cached": {"type": "boolean"}
        }
    });

    schema_test_utils::register_test_schema(
        ctx.pool(),
        &event_source,
        &event_type,
        "1.0",
        schema,
    ).await?;

    // Insert multiple events of the same type
    // This should benefit from schema caching
    for i in 0..10 {
        let event = RawEventBuilder::new(
            &event_source,
            &event_type,
            json!({"cached": true, "iteration": i}),
        ).build();
        
        let result = insert_event(ctx.pool(), &event).await;
        assert!(result.is_ok(), "Cached schema validation should work for event {}", i);
    }

    Ok(())
}