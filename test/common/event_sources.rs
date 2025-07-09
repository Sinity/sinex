//! Event source utilities for testing
//! 
//! This module provides utilities for creating and managing event sources in tests.

use crate::common::prelude::*;

/// Create a test filesystem event source configuration
pub fn test_filesystem_config() -> serde_json::Value {
    json!({
        "enabled": true,
        "paths": ["/tmp/test"],
        "recursive": true
    })
}

/// Create a test terminal event source configuration
pub fn test_terminal_config() -> serde_json::Value {
    json!({
        "enabled": true,
        "capture_commands": true
    })
}

/// Create a test clipboard event source configuration  
pub fn test_clipboard_config() -> serde_json::Value {
    json!({
        "enabled": true,
        "poll_interval_ms": 100
    })
}