use crate::common::prelude::*;
use sinex_core::{RawEventBuilder, sources, event_type_constants};

/// Test that validation prevents malformed events from being inserted
#[sinex_test]
async fn test_validation_prevents_malformed_events(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test 1: Valid event should work
    let valid_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/valid.txt",
            "size": 1024,
            "permissions": "644"
        })
    ).build();

    // This should succeed
    let result = queries::insert_event(ctx.pool(), &valid_event).await;
    assert!(result.is_ok(), "Valid event should be inserted successfully");

    // Test 2: Invalid event should fail (empty source)
    let invalid_event = RawEventBuilder::new(
        "", // Invalid empty source
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/invalid.txt",
            "size": 1024
        })
    ).build();

    // This should fail
    let result = queries::insert_event(ctx.pool(), &invalid_event).await;
    assert!(result.is_err(), "Invalid event with empty source should fail");

    Ok(())
}

/// Test event type validation
#[sinex_test]
async fn test_event_type_validation(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Valid event type should work
    let valid_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_MODIFIED,
        json!({
            "path": "/test/modified.txt",
            "size": 2048
        })
    ).build();

    let result = queries::insert_event(ctx.pool(), &valid_event).await;
    assert!(result.is_ok(), "Valid event type should be accepted");

    // Test with agent event type
    let agent_event = RawEventBuilder::new(
        sources::SINEX,
        event_type_constants::sinex::AGENT_HEARTBEAT,
        json!({
            "agent_name": "test-agent",
            "status": "running",
            "uptime_seconds": 3600
        })
    ).build();

    let result = queries::insert_event(ctx.pool(), &agent_event).await;
    assert!(result.is_ok(), "Valid agent event should be accepted");

    Ok(())
}

/// Test payload validation
#[sinex_test]
async fn test_payload_validation(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Valid JSON payload should work
    let valid_event = RawEventBuilder::new(
        sources::TERMINAL_KITTY,
        event_type_constants::terminal::COMMAND_EXECUTED,
        json!({
            "command": "ls -la",
            "exit_code": 0,
            "duration_ms": 150
        })
    ).build();

    let result = queries::insert_event(ctx.pool(), &valid_event).await;
    assert!(result.is_ok(), "Valid JSON payload should be accepted");

    // Complex nested payload should also work
    let complex_event = RawEventBuilder::new(
        sources::HYPRLAND,
        "workspace.changed",
        json!({
            "old_workspace": {
                "id": 1,
                "name": "workspace1"
            },
            "new_workspace": {
                "id": 2,
                "name": "workspace2"
            },
            "timestamp": "2025-01-01T00:00:00Z"
        })
    ).build();

    let result = queries::insert_event(ctx.pool(), &complex_event).await;
    assert!(result.is_ok(), "Complex nested payload should be accepted");

    Ok(())
}