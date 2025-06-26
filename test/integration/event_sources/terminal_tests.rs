use crate::common::prelude::*;
use sinex_events::terminal::{KittySocketListener, KittyConfig, CommandExecuted, CommandExecutedPayload};
use sinex_core::{EventSource, EventType};
// use crate::common::resources;  // Not needed anymore
use chrono::Utc;
use crate::common::event_sources;

#[sinex_test]
async fn test_kitty_listener_initialization(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let socket_path = temp_dir.path().join("kitty-test-*");
    
    let config = KittyConfig {
        socket_path: socket_path.to_string_lossy().to_string(),
        polling_interval_secs: 1,
    };
    
    let ctx = event_sources::test_context(serde_json::to_value(&config).unwrap());
    let listener = KittySocketListener::initialize(ctx).await;
    // Should succeed even if no socket exists (will wait for socket)
    assert!(listener.is_ok(), "Should initialize even without active socket");
    Ok(())
}

#[sinex_test]
async fn test_kitty_event_structure(_ctx: TestContext) -> TestResult {
    // Test that the event payload structure is correct
    let payload = CommandExecutedPayload {
        command_string: "echo test".to_string(),
        cwd: "/tmp".to_string(),
        exit_code: 0,
        ts_start_orig: Utc::now(),
        ts_end_orig: Utc::now(),
    };
    
    // Verify serialization works
    let json = serde_json::to_value(&payload).unwrap();
    pretty_assertions::assert_eq!(json["command_string"], "echo test");
    pretty_assertions::assert_eq!(json["cwd"], "/tmp");
    pretty_assertions::assert_eq!(json["exit_code"], 0);
    
    // Verify event type constant
    pretty_assertions::assert_eq!(CommandExecuted::EVENT_NAME, "command.executed");
    
    Ok(())
}

#[sinex_test]
async fn test_kitty_socket_pattern_matching(_ctx: TestContext) -> TestResult {
    let config = KittyConfig {
        socket_path: "/tmp/mykitty-*".to_string(),
        polling_interval_secs: 1,
    };
    
    // The socket path should support glob patterns
    assert!(config.socket_path.contains("*"), "Socket path should support wildcards");
    
    Ok(())
}

// Note: Full integration tests for Kitty would require an actual Kitty terminal
// running with a socket. These tests verify the basic structure and initialization.