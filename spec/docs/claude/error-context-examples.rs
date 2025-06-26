// Example usage of the structured error context builders
// This file demonstrates how to replace existing string-based errors
// with structured error contexts for better debugging and monitoring.

use sinex_core::{CoreError, ErrorContext, ResultExt};
use sinex_ulid::Ulid;
use chrono::Utc;
use std::path::Path;

// ===== BEFORE: String-based error patterns =====

fn old_database_error_pattern() -> Result<(), CoreError> {
    let query = "SELECT * FROM events";
    let context = "get_recent_events";
    
    // OLD: String formatting loses structure
    Err(CoreError::Database(format!("{}: Query timeout", context)))
}

fn old_config_error_pattern() -> Result<(), CoreError> {
    let config_type = "EventSourceConfig";
    
    // OLD: Generic formatting 
    Err(CoreError::Configuration(format!("Failed to parse config: {}", "invalid JSON")))
}

fn old_file_error_pattern() -> Result<(), CoreError> {
    let path = "/var/log/sinex.log";
    let operation = "open";
    
    // OLD: Path information embedded in string
    Err(CoreError::Other(format!("Failed to {} file {:?}: {}", operation, path, "Permission denied")))
}

// ===== AFTER: Structured error context builders =====

fn new_database_error_pattern() -> Result<(), CoreError> {
    let query = "SELECT * FROM events";
    let context = "get_recent_events";
    
    // NEW: Structured error with queryable context
    Err(CoreError::database("Query timeout")
        .with_operation(context)
        .with_context("query", query)
        .with_context("timeout_seconds", "30")
        .build())
}

fn new_config_error_pattern() -> Result<(), CoreError> {
    let config_type = "EventSourceConfig";
    
    // NEW: Structured error with typed context
    Err(CoreError::configuration("Config parsing failed")
        .with_context("config_type", config_type)
        .with_context("parser", "serde_json")
        .with_source("invalid JSON")
        .build())
}

fn new_file_error_pattern() -> Result<(), CoreError> {
    let path = Path::new("/var/log/sinex.log");
    
    // NEW: Structured error with path context
    Err(CoreError::io_error(path)
        .with_operation("open")
        .with_context("mode", "read")
        .with_source("Permission denied")
        .build())
}

// ===== Advanced usage patterns =====

fn event_processing_error() -> Result<(), CoreError> {
    let event_id = Ulid::new();
    let timestamp = Utc::now();
    
    // Rich event context for debugging
    Err(CoreError::processing_failed()
        .with_event_id(event_id)
        .with_timestamp(timestamp)
        .with_operation("validate_event")
        .with_context("event_source", "filesystem")
        .with_context("event_type", "file.created")
        .with_field("file_path", "/home/user/document.txt")
        .with_source("Schema validation failed")
        .build())
}

fn chained_error_example() -> Result<(), CoreError> {
    // Demonstrate error source chaining
    Err(CoreError::database("Connection failed")
        .with_context("host", "localhost")
        .with_context("port", "5432")
        .with_operation("connect")
        .with_source("Connection timeout after 30s")
        .with_source("Network unreachable")
        .build())
}

// ===== Helper functions for common patterns =====

/// Helper for database query errors
fn db_query_error(operation: &str, query: &str, err: impl std::fmt::Display) -> CoreError {
    CoreError::database("Query failed")
        .with_operation(operation)
        .with_context("query", query)
        .with_source(err)
        .build()
}

/// Helper for file system operation errors  
fn fs_error(operation: &str, path: &Path, err: impl std::fmt::Display) -> CoreError {
    CoreError::io_error(path)
        .with_operation(operation)
        .with_source(err)
        .build()
}

/// Helper for command execution errors
fn command_error(command: &str, stderr: &str) -> CoreError {
    CoreError::processing_failed()
        .with_operation("execute_command")
        .with_context("command", command)
        .with_context("stderr", stderr)
        .build()
}

/// Helper for configuration validation errors
fn config_error(config_type: &str, field: &str, reason: &str) -> CoreError {
    CoreError::configuration("Invalid configuration")
        .with_context("config_type", config_type)
        .with_field(field, reason)
        .build()
}

// ===== ResultExt trait usage =====

fn result_extension_example() -> sinex_core::Result<()> {
    // Using the ResultExt trait for easy context addition
    std::fs::read_to_string("/nonexistent/file")
        .context("Failed to read configuration file")?;
        
    Ok(())
}

fn result_extension_with_context() -> sinex_core::Result<()> {
    // Using with_context for structured error creation
    std::fs::read_to_string("/nonexistent/file")
        .with_context(|| {
            CoreError::io_error("/nonexistent/file")
                .with_operation("read_config")
                .with_context("config_type", "collector")
        })?;
        
    Ok(())
}

// ===== Migration examples from actual codebase =====

// From crate/sinex-events/src/terminal.rs
fn migrate_terminal_error() -> Result<(), CoreError> {
    // BEFORE:
    // .map_err(|e| sinex_core::CoreError::Other(format!("Failed to list Kitty windows: {}", e)))?
    
    // AFTER:
    Err(CoreError::processing_failed()
        .with_operation("list_kitty_windows")
        .with_context("command", "kitty @ ls")
        .with_source("Command execution failed")
        .build())
}

// From crate/sinex-events/src/filesystem.rs  
fn migrate_filesystem_error() -> Result<(), CoreError> {
    let path = Path::new("/home/user/watched");
    
    // BEFORE:
    // .map_err(|e| sinex_core::CoreError::Other(format!("Failed to watch path: {}", e)))?
    
    // AFTER:
    Err(CoreError::io_error(path)
        .with_operation("watch_directory")
        .with_context("watch_mode", "recursive")
        .with_source("notify watcher creation failed")
        .build())
}

// From crate/sinex-events/src/window_manager.rs
fn migrate_window_manager_error() -> Result<(), CoreError> {
    // BEFORE:
    // .map_err(|e| sinex_core::CoreError::Other(format!("Failed to parse hyprctl output: {}", e)))?
    
    // AFTER:
    Err(CoreError::serialization("Failed to parse hyprctl output")
        .with_operation("parse_hyprctl_json")
        .with_context("command", "hyprctl clients -j")
        .with_context("expected_format", "JSON array")
        .with_source("serde_json parse error")
        .build())
}

// ===== Error information extraction =====

fn demonstrate_error_info() {
    let error_context = CoreError::database("Connection failed")
        .with_context("host", "localhost")
        .with_context("port", "5432")
        .with_operation("connect")
        .with_source("Connection timeout");
    
    // Extract structured error information for logging/monitoring
    let error_info = error_context.to_error_info();
    
    // This provides structured data:
    // {
    //   "error_type": "Database",
    //   "message": "Connection failed", 
    //   "context": {
    //     "host": "localhost",
    //     "port": "5432",
    //     "operation": "connect"
    //   },
    //   "source_chain": ["Connection timeout"]
    // }
    
    // Can be serialized as JSON for logging
    let json = serde_json::to_string_pretty(&error_info).unwrap();
    println!("Structured error: {}", json);
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_structured_error_patterns() {
        // Test that new patterns provide better error information
        let error = new_database_error_pattern().unwrap_err();
        let error_str = error.to_string();
        
        assert!(error_str.contains("Query timeout"));
        assert!(error_str.contains("operation: get_recent_events"));
        assert!(error_str.contains("query: SELECT * FROM events"));
    }
    
    #[test]
    fn test_helper_functions() {
        let error = db_query_error("insert_event", "INSERT INTO events", "constraint violation");
        let error_str = error.to_string();
        
        assert!(error_str.contains("Query failed"));
        assert!(error_str.contains("operation: insert_event"));
        assert!(error_str.contains("constraint violation"));
    }
    
    #[test]
    fn test_backwards_compatibility() {
        // Old-style errors still work
        let old_error = CoreError::Database("Simple error".to_string());
        assert_eq!(old_error.to_string(), "Database error: Simple error");
        
        // New-style errors provide more information
        let new_error = CoreError::database("Simple error")
            .with_context("host", "localhost")
            .build();
        assert!(new_error.to_string().contains("host: localhost"));
    }
}