//! # sinex-error
//!
//! Comprehensive error handling for the Sinex ecosystem.
//!
//! This crate provides a unified error type ([`SinexError`]) that is used throughout
//! the Sinex system. It offers rich error context, categorization, serialization,
//! and seamless integration with both standard Rust error handling and the `anyhow` crate.
//!
//! ## Features
//!
//! - **Rich Context**: Attach key-value pairs and source errors to provide detailed diagnostics
//! - **Categorization**: Errors are categorized by type (database, validation, network, etc.)
//! - **Serialization**: Full serde support for API responses and logging
//! - **Status Codes**: Automatic HTTP status code mapping for web services
//! - **Retryability**: Built-in classification of retryable vs permanent errors
//! - **Performance**: Zero-allocation error creation for common cases
//! - **Integration**: Seamless conversion from common error types (io, serde, sqlx, etc.)
//!
//! ## Examples
//!
//! ### Basic Usage
//!
//! ```rust
//! use sinex_types::error::{SinexError, Result};
//!
//! fn validate_email(email: &str) -> Result<()> {
//!     if !email.contains('@') {
//!         return Err(SinexError::validation("Invalid email format")
//!             .wrap_err_with("email", email)
//!             .wrap_err_with("reason", "missing @ symbol"));
//!     }
//!     Ok(())
//! }
//! ```
//!
//! ### With Source Chain
//!
//! ```rust
//! use sinex_types::error::SinexError;
//!
//! let error = SinexError::service("Request processing failed")
//!     .with_source("Database connection lost")
//!     .with_source("Network timeout after 30s")
//!     .wrap_err_with("request_id", "abc-123")
//!     .wrap_err_with("retry_count", 3);
//!
//! // Error display includes full context and source chain
//! println!("{}", error);
//! ```
//!
//! ### Error Categorization
//!
//! ```rust
//! use sinex_types::error::SinexError;
//!
//! let network_error = SinexError::network("Connection refused");
//! assert!(network_error.is_retryable());
//! assert_eq!(network_error.status_code(), 500);
//!
//! let validation_error = SinexError::validation("Invalid input");
//! assert!(validation_error.is_client_error());
//! assert_eq!(validation_error.status_code(), 400);
//! ```
//!
//! ### Integration with anyhow
//!
//! ```rust
//! use sinex_types::error::SinexError;
//! use color_eyre::eyre::Result;
//!
//! fn process_data() -> Result<String> {
//!     // SinexError automatically converts to color_eyre::eyre::Error
//!     Err(SinexError::not_found("Data not found"))?
//! }
//! ```

// Re-export the with_context macro for error context enrichment
#[cfg(feature = "macros")]
pub use sinex_macros::with_context;

use displaydoc::Display;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Core error type for the Sinex system.
///
/// This enum represents all possible error conditions in the Sinex ecosystem.
/// Each variant contains an [`ErrorDetails`] struct that holds the error message,
/// optional context as key-value pairs, and optional source errors.
///
/// # Serialization
///
/// Errors are serialized with a `type` field containing the variant name and
/// a `details` field containing the error details. This format is suitable for
/// API responses and structured logging.
///
/// # Examples
///
/// ```rust
/// use sinex_types::error::SinexError;
///
/// // Simple error
/// let err = SinexError::database("Connection failed");
///
/// // Error with context
/// let err = SinexError::database("Query failed")
///     .wrap_err_with("table", "users")
///     .wrap_err_with("query_time_ms", 1500);
///
/// // Error with source chain
/// let err = SinexError::service("Processing failed")
///     .with_source("Database unavailable")
///     .with_source("Connection timeout");
/// ```
#[derive(Error, Display, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "details")]
pub enum SinexError {
    /// Database error: {0}
    Database(ErrorDetails),

    /// Validation error: {0}
    Validation(ErrorDetails),

    /// Service error: {0}
    Service(ErrorDetails),

    /// IO error: {0}
    Io(ErrorDetails),

    /// Configuration error: {0}
    Configuration(ErrorDetails),

    /// Serialization error: {0}
    Serialization(ErrorDetails),

    /// Parse error: {0}
    Parse(ErrorDetails),

    /// Not found: {0}
    NotFound(ErrorDetails),

    /// Already exists: {0}
    AlreadyExists(ErrorDetails),

    /// Invalid state: {0}
    InvalidState(ErrorDetails),

    /// Permission denied: {0}
    PermissionDenied(ErrorDetails),

    /// Network error: {0}
    Network(ErrorDetails),

    /// Channel send error: {0}
    ChannelSend(ErrorDetails),

    /// Channel receive error: {0}
    ChannelReceive(ErrorDetails),

    /// Timeout: {0}
    Timeout(ErrorDetails),

    /// Operation cancelled: {0}
    Cancelled(ErrorDetails),

    /// Max retries exceeded: {0}
    MaxRetriesExceeded(ErrorDetails),

    /// Resource exhausted: {0}
    ResourceExhausted(ErrorDetails),

    /// Unknown error: {0}
    Unknown(ErrorDetails),
}

/// Detailed error information including message, context, and sources.
///
/// This struct holds the actual error details for each [`SinexError`] variant.
/// It supports:
/// - A primary error message
/// - Key-value context pairs for debugging (preserved in insertion order)
/// - A chain of source errors that led to this error
///
/// # Serialization
///
/// Empty context and sources are omitted from serialization to reduce payload size.
/// The `default` attribute ensures deserialization works correctly when these fields
/// are missing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetails {
    /// The primary error message
    message: String,
    /// Additional context as key-value pairs (insertion order preserved)
    #[serde(skip_serializing_if = "IndexMap::is_empty", default)]
    context: IndexMap<String, String>,
    /// Chain of source errors that led to this error
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    sources: Vec<String>,
}

impl ErrorDetails {
    /// Creates a new `ErrorDetails` with the given message.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use sinex_types::error::ErrorDetails;
    /// let details = ErrorDetails::new("Connection failed");
    /// ```
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            context: IndexMap::new(),
            sources: Vec::new(),
        }
    }

    /// Adds a context key-value pair to the error details.
    ///
    /// Context is preserved in insertion order and displayed when the error
    /// is formatted. This method consumes and returns `self` for chaining.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use sinex_types::error::ErrorDetails;
    /// let details = ErrorDetails::new("Query failed")
    ///     .wrap_err_with("table", "users")
    ///     .wrap_err_with("rows_affected", 0);
    /// ```
    pub fn with_context(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.context.insert(key.into(), value.to_string());
        self
    }

    /// Adds a source error to the error chain.
    ///
    /// Sources are displayed in order when the error is formatted, showing
    /// the chain of errors that led to this one. This method consumes and
    /// returns `self` for chaining.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use sinex_types::error::ErrorDetails;
    /// let details = ErrorDetails::new("Service unavailable")
    ///     .with_source("Database connection failed")
    ///     .with_source("Network timeout");
    /// ```
    pub fn with_source(mut self, source: impl ToString) -> Self {
        self.sources.push(source.to_string());
        self
    }
}

impl fmt::Display for ErrorDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;

        if !self.context.is_empty() {
            write!(f, " (")?;
            for (i, (k, v)) in self.context.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}: {}", k, v)?;
            }
            write!(f, ")")?;
        }

        if !self.sources.is_empty() {
            write!(f, "\nCaused by:")?;
            for (i, source) in self.sources.iter().enumerate() {
                write!(f, "\n  {}: {}", i + 1, source)?;
            }
        }

        Ok(())
    }
}

impl SinexError {
    // Direct constructors that return SinexError

    /// Creates a new database error.
    ///
    /// Use this for errors related to database operations, connections,
    /// queries, or schema issues.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sinex_types::error::SinexError;
    ///
    /// let err = SinexError::database("Connection pool exhausted");
    /// let err = SinexError::database("Query timeout after 30s")
    ///     .wrap_err_with("query", "SELECT * FROM users")
    ///     .wrap_err_with("timeout_ms", 30000);
    /// ```
    pub fn database(msg: impl Into<String>) -> Self {
        SinexError::Database(ErrorDetails::new(msg))
    }

    /// Creates a new validation error.
    ///
    /// Use this for input validation failures, schema validation errors,
    /// or business rule violations.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sinex_types::error::SinexError;
    ///
    /// let err = SinexError::validation("Email format invalid")
    ///     .wrap_err_with("field", "email")
    ///     .wrap_err_with("value", "not-an-email");
    /// ```
    pub fn validation(msg: impl Into<String>) -> Self {
        SinexError::Validation(ErrorDetails::new(msg))
    }

    pub fn service(msg: impl Into<String>) -> Self {
        SinexError::Service(ErrorDetails::new(msg))
    }

    pub fn io(msg: impl Into<String>) -> Self {
        SinexError::Io(ErrorDetails::new(msg))
    }

    pub fn configuration(msg: impl Into<String>) -> Self {
        SinexError::Configuration(ErrorDetails::new(msg))
    }

    pub fn serialization(msg: impl Into<String>) -> Self {
        SinexError::Serialization(ErrorDetails::new(msg))
    }

    pub fn parse(msg: impl Into<String>) -> Self {
        SinexError::Parse(ErrorDetails::new(msg))
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        SinexError::NotFound(ErrorDetails::new(msg))
    }

    pub fn already_exists(msg: impl Into<String>) -> Self {
        SinexError::AlreadyExists(ErrorDetails::new(msg))
    }

    pub fn invalid_state(msg: impl Into<String>) -> Self {
        SinexError::InvalidState(ErrorDetails::new(msg))
    }

    pub fn permission_denied(msg: impl Into<String>) -> Self {
        SinexError::PermissionDenied(ErrorDetails::new(msg))
    }

    pub fn network(msg: impl Into<String>) -> Self {
        SinexError::Network(ErrorDetails::new(msg))
    }

    pub fn channel_send(msg: impl Into<String>) -> Self {
        SinexError::ChannelSend(ErrorDetails::new(msg))
    }

    pub fn channel_receive(msg: impl Into<String>) -> Self {
        SinexError::ChannelReceive(ErrorDetails::new(msg))
    }

    pub fn timeout(msg: impl Into<String>) -> Self {
        SinexError::Timeout(ErrorDetails::new(msg))
    }

    pub fn cancelled(msg: impl Into<String>) -> Self {
        SinexError::Cancelled(ErrorDetails::new(msg))
    }

    pub fn max_retries_exceeded(msg: impl Into<String>) -> Self {
        SinexError::MaxRetriesExceeded(ErrorDetails::new(msg))
    }

    pub fn resource_exhausted(msg: impl Into<String>) -> Self {
        SinexError::ResourceExhausted(ErrorDetails::new(msg))
    }

    pub fn unknown(msg: impl Into<String>) -> Self {
        SinexError::Unknown(ErrorDetails::new(msg))
    }

    // Builder-style context methods
    pub fn with_context(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        use SinexError::*;
        let details = match &mut self {
            Database(d)
            | Validation(d)
            | Service(d)
            | Io(d)
            | Configuration(d)
            | Serialization(d)
            | Parse(d)
            | NotFound(d)
            | AlreadyExists(d)
            | InvalidState(d)
            | PermissionDenied(d)
            | Network(d)
            | ChannelSend(d)
            | ChannelReceive(d)
            | Timeout(d)
            | Cancelled(d)
            | MaxRetriesExceeded(d)
            | ResourceExhausted(d)
            | Unknown(d) => d,
        };
        details.context.insert(key.into(), value.to_string());
        self
    }

    pub fn with_source(mut self, source: impl ToString) -> Self {
        use SinexError::*;
        let details = match &mut self {
            Database(d)
            | Validation(d)
            | Service(d)
            | Io(d)
            | Configuration(d)
            | Serialization(d)
            | Parse(d)
            | NotFound(d)
            | AlreadyExists(d)
            | InvalidState(d)
            | PermissionDenied(d)
            | Network(d)
            | ChannelSend(d)
            | ChannelReceive(d)
            | Timeout(d)
            | Cancelled(d)
            | MaxRetriesExceeded(d)
            | ResourceExhausted(d)
            | Unknown(d) => d,
        };
        details.sources.push(source.to_string());
        self
    }

    pub fn with_operation(self, operation: impl ToString) -> Self {
        self.with_context("operation", operation)
    }

    // Common context helpers
    pub fn with_path(self, path: impl AsRef<camino::Utf8Path>) -> Self {
        self.with_context("path", path.as_ref().as_str())
    }

    pub fn with_duration(self, duration: std::time::Duration) -> Self {
        self.with_context("duration_ms", duration.as_millis())
    }

    pub fn with_count(self, name: &str, count: usize) -> Self {
        self.with_context(name, count)
    }

    pub fn with_id(self, id_type: &str, id: impl ToString) -> Self {
        self.with_context(id_type, id)
    }

    // Helper methods for error categorization

    /// Determines if this error is retryable.
    ///
    /// Retryable errors are typically transient failures that may succeed
    /// if retried. This includes network issues, timeouts, and temporary
    /// service or database unavailability.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sinex_types::error::SinexError;
    ///
    /// assert!(SinexError::timeout("Request timed out").is_retryable());
    /// assert!(SinexError::network("Connection refused").is_retryable());
    /// assert!(!SinexError::validation("Invalid input").is_retryable());
    /// ```
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            SinexError::Timeout(_)
                | SinexError::Network(_)
                | SinexError::Database(_)
                | SinexError::Service(_)
        )
    }

    /// Determines if this error is caused by client behavior.
    ///
    /// Client errors are caused by invalid input, missing resources,
    /// or insufficient permissions. These errors should not be retried
    /// without changing the request.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sinex_types::error::SinexError;
    ///
    /// assert!(SinexError::validation("Invalid email").is_client_error());
    /// assert!(SinexError::not_found("User not found").is_client_error());
    /// assert!(!SinexError::database("Connection failed").is_client_error());
    /// ```
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            SinexError::Validation(_)
                | SinexError::NotFound(_)
                | SinexError::AlreadyExists(_)
                | SinexError::Parse(_)
                | SinexError::PermissionDenied(_)
        )
    }

    /// Determines if this error represents a permanent failure.
    ///
    /// Permanent errors indicate conditions that won't be resolved by
    /// retrying, such as configuration issues, permission denials, or
    /// exhausted retry attempts.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sinex_types::error::SinexError;
    ///
    /// assert!(SinexError::permission_denied("Access denied").is_permanent());
    /// assert!(SinexError::configuration("Invalid config").is_permanent());
    /// assert!(!SinexError::timeout("Request timed out").is_permanent());
    /// ```
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            SinexError::MaxRetriesExceeded(_)
                | SinexError::PermissionDenied(_)
                | SinexError::Configuration(_)
                | SinexError::InvalidState(_)
        )
    }

    /// Returns the appropriate HTTP status code for this error.
    ///
    /// This mapping follows standard HTTP conventions:
    /// - 400: Bad Request (validation, parse errors)
    /// - 403: Forbidden (permission denied)
    /// - 404: Not Found
    /// - 408: Request Timeout
    /// - 409: Conflict (already exists)
    /// - 429: Too Many Requests (resource exhausted)
    /// - 500: Internal Server Error (all others)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sinex_types::error::SinexError;
    ///
    /// assert_eq!(SinexError::validation("Bad input").status_code(), 400);
    /// assert_eq!(SinexError::not_found("Missing").status_code(), 404);
    /// assert_eq!(SinexError::database("Failed").status_code(), 500);
    /// ```
    pub fn status_code(&self) -> u16 {
        match self {
            SinexError::Validation(_) | SinexError::Parse(_) => 400,
            SinexError::PermissionDenied(_) => 403,
            SinexError::NotFound(_) => 404,
            SinexError::Timeout(_) => 408,
            SinexError::AlreadyExists(_) => 409,
            SinexError::ResourceExhausted(_) => 429,
            _ => 500,
        }
    }

    // Get structured context for telemetry
    pub fn context_map(&self) -> &IndexMap<String, String> {
        use SinexError::*;
        let details = match self {
            Database(d)
            | Validation(d)
            | Service(d)
            | Io(d)
            | Configuration(d)
            | Serialization(d)
            | Parse(d)
            | NotFound(d)
            | AlreadyExists(d)
            | InvalidState(d)
            | PermissionDenied(d)
            | Network(d)
            | ChannelSend(d)
            | ChannelReceive(d)
            | Timeout(d)
            | Cancelled(d)
            | MaxRetriesExceeded(d)
            | ResourceExhausted(d)
            | Unknown(d) => d,
        };
        &details.context
    }

    // Get the error message without context
    pub fn message(&self) -> &str {
        use SinexError::*;
        let details = match self {
            Database(d)
            | Validation(d)
            | Service(d)
            | Io(d)
            | Configuration(d)
            | Serialization(d)
            | Parse(d)
            | NotFound(d)
            | AlreadyExists(d)
            | InvalidState(d)
            | PermissionDenied(d)
            | Network(d)
            | ChannelSend(d)
            | ChannelReceive(d)
            | Timeout(d)
            | Cancelled(d)
            | MaxRetriesExceeded(d)
            | ResourceExhausted(d)
            | Unknown(d) => d,
        };
        &details.message
    }

    // Get the error sources
    pub fn sources(&self) -> &[String] {
        use SinexError::*;
        let details = match self {
            Database(d)
            | Validation(d)
            | Service(d)
            | Io(d)
            | Configuration(d)
            | Serialization(d)
            | Parse(d)
            | NotFound(d)
            | AlreadyExists(d)
            | InvalidState(d)
            | PermissionDenied(d)
            | Network(d)
            | ChannelSend(d)
            | ChannelReceive(d)
            | Timeout(d)
            | Cancelled(d)
            | MaxRetriesExceeded(d)
            | ResourceExhausted(d)
            | Unknown(d) => d,
        };
        &details.sources
    }

    // Get error variant as string for telemetry
    pub fn variant_name(&self) -> &'static str {
        match self {
            SinexError::Database(_) => "Database",
            SinexError::Validation(_) => "Validation",
            SinexError::Service(_) => "Service",
            SinexError::Io(_) => "Io",
            SinexError::Configuration(_) => "Configuration",
            SinexError::Serialization(_) => "Serialization",
            SinexError::Parse(_) => "Parse",
            SinexError::NotFound(_) => "NotFound",
            SinexError::AlreadyExists(_) => "AlreadyExists",
            SinexError::InvalidState(_) => "InvalidState",
            SinexError::PermissionDenied(_) => "PermissionDenied",
            SinexError::Network(_) => "Network",
            SinexError::ChannelSend(_) => "ChannelSend",
            SinexError::ChannelReceive(_) => "ChannelReceive",
            SinexError::Timeout(_) => "Timeout",
            SinexError::Cancelled(_) => "Cancelled",
            SinexError::MaxRetriesExceeded(_) => "MaxRetriesExceeded",
            SinexError::ResourceExhausted(_) => "ResourceExhausted",
            SinexError::Unknown(_) => "Unknown",
        }
    }
}

// Conversions from common error types
impl From<std::io::Error> for SinexError {
    fn from(e: std::io::Error) -> Self {
        SinexError::Io(ErrorDetails::new(e.to_string()))
    }
}

impl From<serde_json::Error> for SinexError {
    fn from(e: serde_json::Error) -> Self {
        SinexError::Serialization(ErrorDetails::new(e.to_string()))
    }
}

impl From<sqlx::Error> for SinexError {
    fn from(e: sqlx::Error) -> Self {
        SinexError::Database(ErrorDetails::new(e.to_string()))
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for SinexError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        SinexError::ChannelSend(ErrorDetails::new(err.to_string()))
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for SinexError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        SinexError::ChannelReceive(ErrorDetails::new(err.to_string()))
    }
}

// Note: SinexError automatically converts to color_eyre::eyre::Error via std::error::Error trait

// Re-export anyhow utilities
pub use color_eyre::eyre::{bail, ensure, Context as AnyhowContext, ContextCompat};

pub type Result<T> = std::result::Result<T, SinexError>;

/// Enhanced JSON deserialization with path-aware error reporting
///
/// This function deserializes JSON data and provides detailed error messages
/// showing exactly where in the JSON structure the error occurred.
pub fn deserialize_with_path<T: serde::de::DeserializeOwned>(json_str: &str) -> Result<T> {
    let jd = &mut serde_json::Deserializer::from_str(json_str);

    serde_path_to_error::deserialize(jd).map_err(|err| {
        let path = err.path().to_string();
        SinexError::serialization(format!(
            "JSON deserialization failed at path '{}': {}",
            path,
            err.inner()
        ))
        .with_context("json_path", path)
        .with_context("error_type", format!("{:?}", err.inner().classify()))
    })
}

/// Extension trait for enriching `Result` types with context.
///
/// This trait provides methods similar to `anyhow::Context` but for `SinexError`.
/// It allows adding context to errors in a chain of operations.
///
/// # Examples
///
/// ```rust
/// use sinex_types::error::{Result, ResultExt, SinexError};
/// use std::fs;
///
/// fn read_config() -> Result<String> {
///     fs::read_to_string("/etc/config.toml")
///         .wrap_err("Failed to read config file")?;
///     Ok("config".to_string())
/// }
///
/// fn process_data() -> Result<()> {
///     let data = fetch_data()
///         .wrap_err_with(|| SinexError::service("Data processing failed")
///             .wrap_err_with("service", "data-processor")
///             .wrap_err_with("retry_count", 3))?;
///     Ok(())
/// }
/// # fn fetch_data() -> Result<String> { Ok("data".to_string()) }
/// ```
pub trait ResultExt<T> {
    /// Adds a simple text context to the error.
    ///
    /// The context is added as a "context" key in the error's context map.
    fn context(self, msg: &str) -> Result<T>;

    /// Adds a custom error with full context.
    ///
    /// The closure is only called if the result is an error, allowing
    /// lazy construction of complex error context.
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> SinexError;
}

impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: Into<SinexError>,
{
    fn context(self, msg: &str) -> Result<T> {
        self.map_err(|e| {
            let err: SinexError = e.into();
            err.with_context("context", msg)
        })
    }

    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> SinexError,
    {
        self.map_err(|_| f())
    }
}

#[cfg(test)]
mod tests {
    use super::SinexError;
    use sinex_test_utils::prelude::*;

    #[sinex_test]
    fn test_error_display_with_displaydoc() -> color_eyre::eyre::Result<()> {
        let error = SinexError::database("Connection failed");
        assert_eq!(error.to_string(), "Database error: Connection failed");

        let error = SinexError::validation("Invalid input");
        assert_eq!(error.to_string(), "Validation error: Invalid input");

        Ok(())
    }

    #[sinex_test]
    fn test_error_with_context() {
        let error = SinexError::database("Connection failed")
            .with_context("host", "localhost")
            .with_context("port", 5432);

        let error_str = error.to_string();
        assert!(error_str.contains("Connection failed"));
        assert!(error_str.contains("host: localhost"));
        assert!(error_str.contains("port: 5432"));
    }

    #[sinex_test]
    fn test_error_with_source_chain() {
        let error = SinexError::service("Processing failed")
            .with_source("Database connection timed out")
            .with_source("Network unreachable");

        let error_str = error.to_string();
        assert!(error_str.contains("Processing failed"));
        assert!(error_str.contains("Database connection timed out"));
        assert!(error_str.contains("Network unreachable"));
    }

    #[sinex_test]
    fn test_error_categorization() {
        assert!(SinexError::timeout("test").is_retryable());
        assert!(SinexError::network("test").is_retryable());
        assert!(!SinexError::validation("test").is_retryable());

        assert!(SinexError::validation("test").is_client_error());
        assert!(SinexError::not_found("test").is_client_error());
        assert!(!SinexError::database("test").is_client_error());

        assert!(SinexError::max_retries_exceeded("test").is_permanent());
        assert!(SinexError::permission_denied("test").is_permanent());
        assert!(!SinexError::timeout("test").is_permanent());
    }

    #[sinex_test]
    fn test_status_codes() {
        assert_eq!(SinexError::validation("test").status_code(), 400);
        assert_eq!(SinexError::not_found("test").status_code(), 404);
        assert_eq!(SinexError::permission_denied("test").status_code(), 403);
        assert_eq!(SinexError::timeout("test").status_code(), 408);
        assert_eq!(SinexError::already_exists("test").status_code(), 409);
        assert_eq!(SinexError::resource_exhausted("test").status_code(), 429);
        assert_eq!(SinexError::database("test").status_code(), 500);
    }

    #[sinex_test]
    fn test_error_serialization() {
        let error = SinexError::database("Connection failed")
            .with_context("host", "localhost")
            .with_context("port", 5432);

        let json = serde_json::to_string(&error).unwrap();
        assert!(json.contains("Database"));
        assert!(json.contains("Connection failed"));

        let deserialized: SinexError = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.to_string(), error.to_string());
    }

    #[sinex_test]
    fn test_anyhow_integration() {
        fn returns_anyhow() -> color_eyre::eyre::Result<()> {
            Err(SinexError::database("test"))?
        }

        let result = returns_anyhow();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Database error"));
    }

    #[sinex_test]
    fn test_from_implementations() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let sinex_err: SinexError = io_err.into();
        assert!(matches!(sinex_err, SinexError::Io(_)));

        let json_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let sinex_err: SinexError = json_err.into();
        assert!(matches!(sinex_err, SinexError::Serialization(_)));
    }

    #[sinex_test]
    fn test_context_preservation() {
        let error = SinexError::database("Connection failed")
            .with_context("attempt", 3)
            .with_context("retry_after", "5s");

        let context = error.context_map();
        assert_eq!(context.get("attempt"), Some(&"3".to_string()));
        assert_eq!(context.get("retry_after"), Some(&"5s".to_string()));
    }

    #[sinex_test]
    fn test_ordered_context() {
        let error = SinexError::validation("Invalid input")
            .with_context("field", "email")
            .with_context("value", "not-an-email")
            .with_context("reason", "missing @ symbol");

        let error_str = error.to_string();
        // IndexMap preserves insertion order
        assert!(error_str.contains("field: email, value: not-an-email, reason: missing @ symbol"));
    }

    #[sinex_test]
    fn test_convenience_methods() {
        use camino::Utf8Path;
        use std::time::Duration;

        let error = SinexError::io("File operation failed")
            .with_path(Utf8Path::new("/tmp/test.txt"))
            .with_duration(Duration::from_millis(1500))
            .with_count("retry_count", 3)
            .with_id("request_id", "abc123");

        let context = error.context_map();
        assert_eq!(context.get("path"), Some(&"/tmp/test.txt".to_string()));
        assert_eq!(context.get("duration_ms"), Some(&"1500".to_string()));
        assert_eq!(context.get("retry_count"), Some(&"3".to_string()));
        assert_eq!(context.get("request_id"), Some(&"abc123".to_string()));
    }

    #[sinex_test]
    fn test_accessor_methods() {
        let error = SinexError::database("Query failed")
            .with_context("table", "users")
            .with_source("Connection timeout");

        assert_eq!(error.message(), "Query failed");
        assert_eq!(error.variant_name(), "Database");
        assert_eq!(error.sources(), &["Connection timeout"]);
        assert_eq!(error.context_map().get("table"), Some(&"users".to_string()));
    }

    #[sinex_test]
    fn test_all_error_variants() {
        // Test each error variant constructor
        let errors = vec![
            (SinexError::database("db"), "Database"),
            (SinexError::validation("val"), "Validation"),
            (SinexError::service("svc"), "Service"),
            (SinexError::io("io"), "Io"),
            (SinexError::configuration("cfg"), "Configuration"),
            (SinexError::serialization("ser"), "Serialization"),
            (SinexError::parse("parse"), "Parse"),
            (SinexError::not_found("nf"), "NotFound"),
            (SinexError::already_exists("ae"), "AlreadyExists"),
            (SinexError::invalid_state("is"), "InvalidState"),
            (SinexError::permission_denied("pd"), "PermissionDenied"),
            (SinexError::network("net"), "Network"),
            (SinexError::channel_send("cs"), "ChannelSend"),
            (SinexError::channel_receive("cr"), "ChannelReceive"),
            (SinexError::timeout("to"), "Timeout"),
            (SinexError::cancelled("can"), "Cancelled"),
            (
                SinexError::max_retries_exceeded("mre"),
                "MaxRetriesExceeded",
            ),
            (SinexError::resource_exhausted("re"), "ResourceExhausted"),
            (SinexError::unknown("unk"), "Unknown"),
        ];

        for (error, expected_variant) in errors {
            assert_eq!(error.variant_name(), expected_variant);
        }
    }

    #[sinex_test]
    fn test_error_details_display() {
        let details = crate::error::ErrorDetails::new("Base error")
            .with_context("key1", "value1")
            .with_context("key2", "value2")
            .with_source("Source 1")
            .with_source("Source 2");

        let display = format!("{}", details);
        assert!(display.contains("Base error"));
        assert!(display.contains("key1: value1"));
        assert!(display.contains("key2: value2"));
        assert!(display.contains("Caused by:"));
        assert!(display.contains("1: Source 1"));
        assert!(display.contains("2: Source 2"));
    }

    #[sinex_test]
    fn test_error_chain_building() {
        let error = SinexError::service("Service unavailable")
            .with_source("Database connection failed")
            .with_source("Network unreachable")
            .with_source("DNS resolution failed");

        assert_eq!(error.sources().len(), 3);
        assert_eq!(error.sources()[0], "Database connection failed");
        assert_eq!(error.sources()[1], "Network unreachable");
        assert_eq!(error.sources()[2], "DNS resolution failed");
    }

    #[sinex_test]
    fn test_result_ext_trait() {
        fn failing_operation() -> std::result::Result<(), std::io::Error> {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "test"))
        }

        use crate::error::ResultExt;
        let result: crate::Result<()> = ResultExt::context(failing_operation(), "Operation failed");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SinexError::Io(_)));
        assert_eq!(
            err.context_map().get("context"),
            Some(&"Operation failed".to_string())
        );
    }

    #[sinex_test]
    fn test_result_ext_with_context() {
        fn failing_operation() -> std::result::Result<(), std::io::Error> {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "test"))
        }

        use crate::error::ResultExt;
        let result: crate::Result<()> = ResultExt::with_context(failing_operation(), || {
            SinexError::service("Custom error").with_context("component", "test-component")
        });

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SinexError::Service(_)));
        // SinexError doesn't have a message() method, use to_string()
        assert!(err.to_string().contains("Custom error"));
        assert_eq!(
            err.context_map().get("component"),
            Some(&"test-component".to_string())
        );
    }

    #[sinex_test]
    fn test_serialization_roundtrip() {
        let original = SinexError::database("Connection failed")
            .with_context("host", "localhost")
            .with_context("port", 5432)
            .with_source("Network timeout")
            .with_source("DNS failed");

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: SinexError = serde_json::from_str(&json).unwrap();

        assert_eq!(original.message(), deserialized.message());
        assert_eq!(original.variant_name(), deserialized.variant_name());
        assert_eq!(original.sources(), deserialized.sources());
        assert_eq!(original.context_map(), deserialized.context_map());
    }

    #[sinex_test]
    fn test_empty_context_serialization() {
        let error = SinexError::validation("Simple error");
        let json = serde_json::to_string(&error).unwrap();

        // Ensure empty context and sources are not serialized
        assert!(!json.contains("context"));
        assert!(!json.contains("sources"));

        // Ensure it deserializes correctly
        let deserialized: SinexError = serde_json::from_str(&json).unwrap();
        assert!(deserialized.context_map().is_empty());
        assert!(deserialized.sources().is_empty());
    }

    #[sinex_test]
    fn test_with_operation_helper() {
        let error = SinexError::database("Query failed").with_operation("user.find_by_id");

        assert_eq!(
            error.context_map().get("operation"),
            Some(&"user.find_by_id".to_string())
        );
    }

    #[sinex_test]
    fn test_error_conversion_chain() {
        // Test that errors can be converted and preserve information
        let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Access denied");
        let sinex_error: SinexError = io_error.into();

        assert!(matches!(sinex_error, SinexError::Io(_)));
        assert!(sinex_error.message().contains("Access denied"));
    }

    #[tokio::test]
    async fn test_channel_error_conversions() {
        use tokio::sync::mpsc;
        use tokio::sync::oneshot;

        // Test mpsc SendError conversion
        let (tx, rx) = mpsc::channel::<i32>(1);
        drop(rx); // Close the receiver
        let send_result = tx.send(42).await;
        if let Err(e) = send_result {
            let sinex_err: SinexError = e.into();
            assert!(matches!(sinex_err, SinexError::ChannelSend(_)));
        }

        // Test oneshot RecvError conversion
        let (tx, _rx) = oneshot::channel::<i32>();
        drop(tx); // Drop the sender
                  // We can't easily test the actual error without async context,
                  // but we can test the type conversion compiles
        fn test_conversion(err: oneshot::error::RecvError) -> SinexError {
            err.into()
        }
    }

    #[sinex_test]
    fn test_error_equality_after_cloning() {
        let error = SinexError::validation("Test error")
            .with_context("field", "email")
            .with_source("Invalid format");

        let cloned = error.clone();

        assert_eq!(error.message(), cloned.message());
        assert_eq!(error.variant_name(), cloned.variant_name());
        assert_eq!(error.context_map(), cloned.context_map());
        assert_eq!(error.sources(), cloned.sources());
    }

    #[sinex_test]
    fn test_complex_context_values() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert("key", "value");

        let error = SinexError::service("Processing failed")
            .with_context("json", serde_json::json!({"nested": {"value": 42}}))
            .with_context("array", format!("{:?}", vec![1, 2, 3]))
            .with_context("map", format!("{:?}", map));

        let context = error.context_map();
        assert!(context.get("json").unwrap().contains("nested"));
        assert_eq!(context.get("array").unwrap(), "[1, 2, 3]");
        assert!(context.get("map").unwrap().contains("key"));
    }

    #[sinex_test]
    fn test_indexmap_preserves_order() {
        let error = SinexError::validation("Test")
            .with_context("a", "1")
            .with_context("b", "2")
            .with_context("c", "3")
            .with_context("d", "4");

        let keys: Vec<_> = error.context_map().keys().collect();
        assert_eq!(keys, vec!["a", "b", "c", "d"]);
    }

    #[sinex_test]
    fn test_edge_cases() {
        // Empty message
        let error = SinexError::unknown("");
        assert_eq!(error.message(), "");

        // Very long message
        let long_msg = "x".repeat(10000);
        let error = SinexError::unknown(&long_msg);
        assert_eq!(error.message().len(), 10000);

        // Unicode in context
        let error = SinexError::parse("Failed")
            .with_context("emoji", "🦀")
            .with_context("chinese", "你好")
            .with_context("arabic", "مرحبا");

        assert_eq!(error.context_map().get("emoji"), Some(&"🦀".to_string()));
        assert_eq!(
            error.context_map().get("chinese"),
            Some(&"你好".to_string())
        );
        assert_eq!(
            error.context_map().get("arabic"),
            Some(&"مرحبا".to_string())
        );
    }
}
