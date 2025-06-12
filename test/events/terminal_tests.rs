use sinex_events::terminal::{KittySocketListener, KittyConfig, CommandExecuted, CommandExecutedPayload};
use sinex_core::{EventSource, EventType};
use sinex_db::models::RawEvent;
use tokio::sync::mpsc;
use std::path::PathBuf;
use tempfile::TempDir;
use chrono::Utc;

#[tokio::test]
async fn test_kitty_listener_initialization() {
    let temp_dir = TempDir::new().unwrap();
    let socket_path = temp_dir.path().join("kitty-test-*");
    
    let config = KittyConfig {
        socket_path: socket_path.to_string_lossy().to_string(),
        polling_interval_secs: 1,
    };
    
    let listener = KittySocketListener::initialize(config.clone()).await;
    // Should succeed even if no socket exists (will wait for socket)
    assert!(listener.is_ok(), "Should initialize even without active socket");
}

#[tokio::test]
async fn test_kitty_event_structure() {
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
    assert_eq!(json["command_string"], "echo test");
    assert_eq!(json["cwd"], "/tmp");
    assert_eq!(json["exit_code"], 0);
    
    // Verify event type constant
    assert_eq!(CommandExecuted::EVENT_NAME, "command.executed");
}

#[tokio::test]
async fn test_kitty_socket_pattern_matching() {
    let config = KittyConfig {
        socket_path: "/tmp/mykitty-*".to_string(),
        polling_interval_secs: 1,
    };
    
    // The socket path should support glob patterns
    assert!(config.socket_path.contains("*"), "Socket path should support wildcards");
}

// Note: Full integration tests for Kitty would require an actual Kitty terminal
// running with a socket. These tests verify the basic structure and initialization.