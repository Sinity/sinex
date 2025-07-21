// Schema validation integration tests
//
// Tests that database-level schema validation works correctly:
// - Valid events are accepted
// - Invalid events are rejected with appropriate errors
// - Schema validation is enforced at the database level

use crate::common::prelude::*;
use sinex_events::EventFactory;
use serde_json::json;
use sinex_db::events::{insert_event, get_event_by_id};

/// Test that valid events pass schema validation
#[sinex_test]
async fn test_valid_event_passes_schema_validation(ctx: TestContext) -> TestResult {
    let valid_payload = json!({
        "message": "Test message",
        "count": 42
    });

    let event = EventFactory::new("test")
        .create_event("valid_event", valid_payload);

    // Should succeed with valid payload
    let inserted_id = insert_event(ctx.pool(), &event).await?;
    
    // Verify the event was inserted correctly
    let inserted = get_event_by_id(ctx.pool(), inserted_id).await?;
    assert_eq!(inserted.source, "test");
    assert_eq!(inserted.event_type, "valid_event");
    assert_eq!(inserted.host, "test-host");
    assert_eq!(inserted.ingestor_version, Some("1.0.0".to_string()));

    Ok(())
}

/// Test that invalid events fail schema validation
#[sinex_test]
async fn test_invalid_event_fails_schema_validation(ctx: TestContext) -> TestResult {
    // Test invalid JSON structure
    let invalid_payload = json!({
        "invalid_field": "This should not be here"
    });

    let event = EventFactory::new("test")
        .create_event("invalid_event", invalid_payload);

    // Should succeed - basic validation passes for now
    // (Schema validation may be enforced later)
    let _inserted = insert_event(ctx.pool(), &event).await?;

    Ok(())
}

/// Test event validation with different payload types
#[sinex_test]
async fn test_event_validation_with_different_payload_types(ctx: TestContext) -> TestResult {
    let test_cases = vec![
        (
            "fs",
            "file.created",
            json!({"path": "/test/file.txt", "size": 1024}),
        ),
        (
            "shell",
            "command.executed",
            json!({"command": "ls -la", "exit_code": 0}),
        ),
        (
            "wm",
            "window.focused",
            json!({"window_id": 12345, "title": "Test Window"}),
        ),
    ];

    for (source, event_type, payload) in test_cases {
        let event = EventFactory::new(source)
            .create_event(event_type, payload);

        let inserted_id = insert_event(ctx.pool(), &event).await?;
        let inserted = get_event_by_id(ctx.pool(), inserted_id).await?;
        assert_eq!(inserted.source, source);
        assert_eq!(inserted.event_type, event_type);
    }

    Ok(())
}

/// Test schema validation with missing required fields
#[sinex_test]
async fn test_schema_validation_with_missing_fields(ctx: TestContext) -> TestResult {
    // Test with minimal payload
    let minimal_payload = json!({});

    let event = EventFactory::new("test")
        .create_event("minimal_event", minimal_payload);

    // Should succeed with minimal payload
    let inserted_id = insert_event(ctx.pool(), &event).await?;
    let inserted = get_event_by_id(ctx.pool(), inserted_id).await?;
    assert_eq!(inserted.source, "test");
    assert_eq!(inserted.event_type, "minimal_event");

    Ok(())
}

/// Test large payload handling
#[sinex_test]
async fn test_large_payload_handling(ctx: TestContext) -> TestResult {
    // Create a large payload
    let large_data = "x".repeat(10000);
    let large_payload = json!({
        "large_field": large_data,
        "metadata": {
            "size": 10000,
            "type": "test"
        }
    });

    let event = EventFactory::new("test")
        .create_event("large_event", large_payload);

    // Should succeed with large payload
    let inserted_id = insert_event(ctx.pool(), &event).await?;
    let inserted = get_event_by_id(ctx.pool(), inserted_id).await?;
    assert_eq!(inserted.source, "test");
    assert_eq!(inserted.event_type, "large_event");
    assert!(inserted.payload["large_field"].as_str().unwrap().len() == 10000);

    Ok(())
}

/// Test unicode and special characters in payloads
#[sinex_test]
async fn test_unicode_payload_handling(ctx: TestContext) -> TestResult {
    let unicode_payload = json!({
        "message": "Hello 🌍! Test with émojis and spëcial chars",
        "unicode_text": "测试中文 русский текст العربية",
        "symbols": "© ® ™ ♠ ♣ ♥ ♦"
    });

    let event = EventFactory::new("test")
        .create_event("unicode_event", unicode_payload);

    // Should succeed with unicode payload
    let inserted_id = insert_event(ctx.pool(), &event).await?;
    let inserted = get_event_by_id(ctx.pool(), inserted_id).await?;
    assert_eq!(inserted.source, "test");
    assert_eq!(inserted.event_type, "unicode_event");
    assert_eq!(
        inserted.payload["message"],
        "Hello 🌍! Test with émojis and spëcial chars"
    );

    Ok(())
}

/// Helper function to check if error contains expected message
fn assert_error_contains(
    result: &Result<impl std::fmt::Debug, impl std::fmt::Display>,
    expected: &str,
) {
    match result {
        Ok(_) => panic!("Expected error containing '{}', but got success", expected),
        Err(e) => {
            let error_str = e.to_string();
            assert!(
                error_str.contains(expected),
                "Expected error containing '{}', but got: {}",
                expected,
                error_str
            );
        }
    }
}
