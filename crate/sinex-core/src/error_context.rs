use crate::{CoreError, Timestamp};
use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::Path;

/// Builder for creating rich error contexts
#[derive(Debug)]
pub struct ErrorContext {
    error_type: ErrorType,
    message: String,
    context: HashMap<String, String>,
    source_chain: Vec<String>,
    stack_trace: Option<String>,
}

#[derive(Debug)]
enum ErrorType {
    Database,
    Serialization,
    Validation,
    Configuration,
    Io,
    Other,
}

impl ErrorContext {
    /// Create a new error context
    pub fn new(error_type: CoreError) -> Self {
        let (err_type, message) = match error_type {
            CoreError::Database(msg) => (ErrorType::Database, msg),
            CoreError::Serialization(msg) => (ErrorType::Serialization, msg),
            CoreError::Validation(msg) => (ErrorType::Validation, msg),
            CoreError::Configuration(msg) => (ErrorType::Configuration, msg),
            CoreError::Io(msg) => (ErrorType::Io, msg),
            CoreError::Other(msg) => (ErrorType::Other, msg),
        };

        Self {
            error_type: err_type,
            message,
            context: HashMap::new(),
            source_chain: Vec::new(),
            stack_trace: capture_stack_trace(),
        }
    }

    /// Add contextual information
    pub fn with_context(mut self, key: &str, value: impl Display) -> Self {
        self.context.insert(key.to_string(), value.to_string());
        self
    }

    /// Add source error information
    pub fn with_source(mut self, source: impl Display) -> Self {
        self.source_chain.push(source.to_string());
        self
    }

    /// Add event ID context
    pub fn with_event_id(self, id: Ulid) -> Self {
        self.with_context("event_id", id)
    }

    /// Add timestamp context
    pub fn with_timestamp(self, ts: Timestamp) -> Self {
        self.with_context("timestamp", ts.to_rfc3339())
    }

    /// Add file path context
    pub fn with_path(self, path: impl AsRef<Path>) -> Self {
        self.with_context("path", path.as_ref().display())
    }

    /// Add operation context
    pub fn with_operation(self, operation: &str) -> Self {
        self.with_context("operation", operation)
    }

    /// Add field context
    pub fn with_field(self, field: &str, value: impl Display) -> Self {
        self.with_context(&format!("field_{}", field), value)
    }

    /// Build the final CoreError with all context
    pub fn build(self) -> CoreError {
        let mut final_message = self.message.clone();

        // Add context if any
        if !self.context.is_empty() {
            final_message.push_str(" (");
            let mut parts = Vec::new();
            for (key, value) in &self.context {
                parts.push(format!("{}: {}", key, value));
            }
            final_message.push_str(&parts.join(", "));
            final_message.push(')');
        }

        // Add source chain if any
        if !self.source_chain.is_empty() {
            final_message.push_str("\nCaused by:");
            for (i, source) in self.source_chain.iter().enumerate() {
                final_message.push_str(&format!("\n  {}: {}", i + 1, source));
            }
        }

        match self.error_type {
            ErrorType::Database => CoreError::Database(final_message),
            ErrorType::Serialization => CoreError::Serialization(final_message),
            ErrorType::Validation => CoreError::Validation(final_message),
            ErrorType::Configuration => CoreError::Configuration(final_message),
            ErrorType::Io => CoreError::Io(final_message),
            ErrorType::Other => CoreError::Other(final_message),
        }
    }

    /// Convert to structured error info for logging
    pub fn to_error_info(&self) -> ErrorInfo {
        ErrorInfo {
            error_type: format!("{:?}", self.error_type),
            message: self.message.clone(),
            context: self.context.clone(),
            source_chain: self.source_chain.clone(),
            stack_trace: self.stack_trace.clone(),
        }
    }
}

/// Structured error information for logging/serialization
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub error_type: String,
    pub message: String,
    pub context: HashMap<String, String>,
    pub source_chain: Vec<String>,
    pub stack_trace: Option<String>,
}

impl CoreError {
    /// Create a database error with context builder
    pub fn database(msg: impl Display) -> ErrorContext {
        ErrorContext::new(CoreError::Database(msg.to_string()))
    }

    /// Create a validation error with context builder
    pub fn validation(msg: impl Display) -> ErrorContext {
        ErrorContext::new(CoreError::Validation(msg.to_string()))
    }

    /// Create a configuration error with context builder
    pub fn configuration(msg: impl Display) -> ErrorContext {
        ErrorContext::new(CoreError::Configuration(msg.to_string()))
    }

    /// Create a serialization error with context builder
    pub fn serialization(msg: impl Display) -> ErrorContext {
        ErrorContext::new(CoreError::Serialization(msg.to_string()))
    }

    /// Create an IO error with context builder
    pub fn io_error(path: impl AsRef<Path>) -> ErrorContext {
        ErrorContext::new(CoreError::Io(format!(
            "IO error for path: {}",
            path.as_ref().display()
        )))
        .with_path(path)
    }

    /// Create a generic processing failed error
    pub fn processing_failed() -> ErrorContext {
        ErrorContext::new(CoreError::Other("Processing failed".to_string()))
    }

    /// Extract context from an existing error (for chaining)
    pub fn context(&self) -> ErrorContext {
        ErrorContext::new(self.clone())
    }

    /// Check if error has specific context key
    pub fn has_context_key(&self, key: &str) -> bool {
        self.to_string().contains(&format!("{}: ", key))
    }
}

// Helper for capturing stack traces
fn capture_stack_trace() -> Option<String> {
    // Stack trace capture could be implemented here if needed
    // For now, return None to avoid overhead
    None
}

/// Extension trait for Result types to add context
pub trait ResultExt<T> {
    /// Add context to an error
    fn context(self, msg: &str) -> crate::Result<T>;

    /// Add context with a key-value pair
    fn with_context<F>(self, f: F) -> crate::Result<T>
    where
        F: FnOnce() -> ErrorContext;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: Into<CoreError>,
{
    fn context(self, msg: &str) -> crate::Result<T> {
        self.map_err(|e| {
            let core_err: CoreError = e.into();
            core_err.context().with_context("context", msg).build()
        })
    }

    fn with_context<F>(self, f: F) -> crate::Result<T>
    where
        F: FnOnce() -> ErrorContext,
    {
        self.map_err(|_| f().build())
    }
}

// Integration with anyhow - removed to avoid conflict with blanket impl

// Implement Clone for CoreError to support error chaining
impl Clone for CoreError {
    fn clone(&self) -> Self {
        match self {
            CoreError::Database(msg) => CoreError::Database(msg.clone()),
            CoreError::Serialization(msg) => CoreError::Serialization(msg.clone()),
            CoreError::Validation(msg) => CoreError::Validation(msg.clone()),
            CoreError::Configuration(msg) => CoreError::Configuration(msg.clone()),
            CoreError::Io(msg) => CoreError::Io(msg.clone()),
            CoreError::Other(msg) => CoreError::Other(msg.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_error_context_builder() {
        let error = CoreError::database("Connection failed")
            .with_context("host", "localhost")
            .with_context("port", 5432)
            .build();

        let error_str = error.to_string();
        assert!(error_str.contains("Connection failed"));
        assert!(error_str.contains("host: localhost"));
        assert!(error_str.contains("port: 5432"));
    }

    #[test]
    fn test_error_with_source_chain() {
        let error = CoreError::processing_failed()
            .with_source("Database connection timed out")
            .with_source("Network unreachable")
            .build();

        let error_str = error.to_string();
        assert!(error_str.contains("Processing failed"));
        assert!(error_str.contains("Database connection timed out"));
        assert!(error_str.contains("Network unreachable"));
    }

    #[test]
    fn test_error_with_event_context() {
        let event_id = Ulid::new();
        let timestamp = Utc::now();

        let error = CoreError::validation("Invalid event payload")
            .with_event_id(event_id)
            .with_timestamp(timestamp)
            .build();

        let error_str = error.to_string();
        assert!(error_str.contains("Invalid event payload"));
        assert!(error_str.contains(&event_id.to_string()));
    }

    #[test]
    fn test_io_error_with_path() {
        let error = CoreError::io_error("/var/log/sinex.log")
            .with_operation("write")
            .build();

        let error_str = error.to_string();
        assert!(error_str.contains("/var/log/sinex.log"));
        assert!(error_str.contains("operation: write"));
    }

    #[test]
    fn test_error_info_serialization() {
        let error_context = CoreError::configuration("Missing required field")
            .with_field("database_url", "not set")
            .with_context("config_file", "/etc/sinex/config.toml");

        let error_info = error_context.to_error_info();

        assert_eq!(error_info.message, "Missing required field");
        assert_eq!(
            error_info.context.get("field_database_url"),
            Some(&"not set".to_string())
        );
        assert_eq!(
            error_info.context.get("config_file"),
            Some(&"/etc/sinex/config.toml".to_string())
        );
    }

    #[test]
    fn test_result_extension() {
        fn failing_operation() -> Result<(), std::io::Error> {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found",
            ))
        }

        let result: crate::Result<()> =
            failing_operation().context("Failed to read configuration file");

        assert!(result.is_err());
        let error_str = result.unwrap_err().to_string();
        assert!(error_str.contains("Failed to read configuration file"));
    }

    #[test]
    fn test_error_context_chaining() {
        // Create an initial error
        let initial_error = CoreError::Database("Primary key violation".to_string());
        
        // Chain additional context
        let chained_error = initial_error.context()
            .with_context("table", "raw_events")
            .with_context("operation", "INSERT")
            .with_source("UNIQUE constraint failed")
            .with_source("Transaction aborted")
            .build();

        let error_str = chained_error.to_string();
        assert!(error_str.contains("Primary key violation"));
        assert!(error_str.contains("table: raw_events"));
        assert!(error_str.contains("operation: INSERT"));
        assert!(error_str.contains("UNIQUE constraint failed"));
        assert!(error_str.contains("Transaction aborted"));
        assert!(error_str.contains("Caused by:"));
    }

    #[test]
    fn test_all_error_types_creation() {
        // Test each error type factory method
        let db_error = CoreError::database("DB connection lost").build();
        assert!(db_error.to_string().contains("DB connection lost"));
        assert!(matches!(db_error, CoreError::Database(_)));

        let validation_error = CoreError::validation("Invalid format").build();
        assert!(validation_error.to_string().contains("Invalid format"));
        assert!(matches!(validation_error, CoreError::Validation(_)));

        let config_error = CoreError::configuration("Missing config").build();
        assert!(config_error.to_string().contains("Missing config"));
        assert!(matches!(config_error, CoreError::Configuration(_)));

        let serial_error = CoreError::serialization("JSON parse error").build();
        assert!(serial_error.to_string().contains("JSON parse error"));
        assert!(matches!(serial_error, CoreError::Serialization(_)));

        let io_error = CoreError::io_error("/tmp/test.txt").build();
        assert!(io_error.to_string().contains("/tmp/test.txt"));
        assert!(matches!(io_error, CoreError::Io(_)));

        let process_error = CoreError::processing_failed().build();
        assert!(process_error.to_string().contains("Processing failed"));
        assert!(matches!(process_error, CoreError::Other(_)));
    }

    #[test]
    fn test_context_key_detection() {
        let error = CoreError::database("Connection failed")
            .with_context("host", "localhost")
            .with_context("port", 5432)
            .build();

        assert!(error.has_context_key("host"));
        assert!(error.has_context_key("port"));
        assert!(!error.has_context_key("database"));
        assert!(!error.has_context_key("nonexistent"));
    }

    #[test]
    fn test_complex_context_building() {
        let event_id = Ulid::new();
        let timestamp = Utc::now();
        use std::path::Path;

        let error = CoreError::validation("Schema validation failed")
            .with_event_id(event_id)
            .with_timestamp(timestamp)
            .with_path(Path::new("/data/events/invalid.json"))
            .with_operation("validate_event_payload")
            .with_field("event_type", "unknown")
            .with_field("source", "invalid_source")
            .with_source("Schema mismatch: expected object, got array")
            .with_source("Field 'timestamp' is required but missing")
            .build();

        let error_str = error.to_string();
        
        // Check all context is included
        assert!(error_str.contains(&event_id.to_string()));
        assert!(error_str.contains("/data/events/invalid.json"));
        assert!(error_str.contains("operation: validate_event_payload"));
        assert!(error_str.contains("field_event_type: unknown"));
        assert!(error_str.contains("field_source: invalid_source"));
        assert!(error_str.contains("Schema mismatch"));
        assert!(error_str.contains("Field 'timestamp' is required"));
        assert!(error_str.contains("Caused by:"));
    }

    #[test]
    fn test_empty_context_and_sources() {
        // Test error with no additional context
        let error = CoreError::database("Simple error").build();
        let error_str = error.to_string();
        
        assert_eq!(error_str, "Database error: Simple error");
        assert!(!error_str.contains("("));
        assert!(!error_str.contains("Caused by:"));
    }

    #[test]
    fn test_context_only_no_sources() {
        let error = CoreError::validation("Validation error")
            .with_context("field", "username")
            .with_context("value", "too_short")
            .build();

        let error_str = error.to_string();
        assert!(error_str.contains("Validation error"));
        assert!(error_str.contains("field: username"));
        assert!(error_str.contains("value: too_short"));
        assert!(!error_str.contains("Caused by:"));
    }

    #[test]
    fn test_sources_only_no_context() {
        let error = CoreError::io_error("/dev/null")
            .with_source("Permission denied")
            .with_source("File system read-only")
            .build();

        let error_str = error.to_string();
        assert!(error_str.contains("IO error"));
        assert!(error_str.contains("/dev/null"));
        assert!(error_str.contains("Caused by:"));
        assert!(error_str.contains("1: Permission denied"));
        assert!(error_str.contains("2: File system read-only"));
    }

    #[test]
    fn test_error_info_complete_structure() {
        let error_context = CoreError::configuration("Database URL not found")
            .with_context("config_file", "/etc/sinex/config.toml")
            .with_context("section", "database")
            .with_source("Environment variable DATABASE_URL not set")
            .with_source("Config file missing database section");

        let error_info = error_context.to_error_info();

        // Verify all fields are populated correctly
        assert_eq!(error_info.error_type, "Configuration");
        assert_eq!(error_info.message, "Database URL not found");
        
        assert_eq!(error_info.context.len(), 2);
        assert_eq!(error_info.context.get("config_file"), Some(&"/etc/sinex/config.toml".to_string()));
        assert_eq!(error_info.context.get("section"), Some(&"database".to_string()));
        
        assert_eq!(error_info.source_chain.len(), 2);
        assert_eq!(error_info.source_chain[0], "Environment variable DATABASE_URL not set");
        assert_eq!(error_info.source_chain[1], "Config file missing database section");
        
        // Stack trace should be None in this implementation
        assert!(error_info.stack_trace.is_none());
    }

    #[test]
    fn test_result_ext_with_context_closure() {
        fn failing_db_operation() -> Result<String, sqlx::Error> {
            Err(sqlx::Error::RowNotFound)
        }

        let result: crate::Result<String> = failing_db_operation()
            .with_context(|| {
                CoreError::database("Failed to fetch user")
                    .with_context("operation", "get_user_by_id")
                    .with_context("user_id", 12345)
            });

        assert!(result.is_err());
        let error_str = result.unwrap_err().to_string();
        assert!(error_str.contains("Failed to fetch user"));
        assert!(error_str.contains("operation: get_user_by_id"));
        assert!(error_str.contains("user_id: 12345"));
    }

    #[test]
    fn test_path_display_formatting() {
        use std::path::PathBuf;
        
        let path = PathBuf::from("/home/user/documents/file.txt");
        let error = CoreError::io_error(&path)
            .with_operation("read")
            .build();

        let error_str = error.to_string();
        assert!(error_str.contains("/home/user/documents/file.txt"));
        assert!(error_str.contains("operation: read"));
        assert!(error_str.contains("path: /home/user/documents/file.txt"));
    }

    #[test]
    fn test_ulid_and_timestamp_formatting() {
        let event_id = Ulid::new();
        let timestamp = Utc::now();
        
        let error = CoreError::processing_failed()
            .with_event_id(event_id)
            .with_timestamp(timestamp)
            .build();

        let error_str = error.to_string();
        assert!(error_str.contains(&event_id.to_string()));
        assert!(error_str.contains(&timestamp.to_rfc3339()));
        assert!(error_str.contains("event_id:"));
        assert!(error_str.contains("timestamp:"));
    }

    #[test]
    fn test_field_context_formatting() {
        let error = CoreError::validation("Multiple field errors")
            .with_field("username", "")
            .with_field("email", "invalid@")
            .with_field("age", -5)
            .build();

        let error_str = error.to_string();
        assert!(error_str.contains("field_username: "));
        assert!(error_str.contains("field_email: invalid@"));
        assert!(error_str.contains("field_age: -5"));
    }
}
