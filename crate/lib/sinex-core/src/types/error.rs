#![doc = include_str!("../../docs/error.md")]

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
/// use crate::error::SinexError;
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
    /// # use crate::error::ErrorDetails;
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
    /// # use crate::error::ErrorDetails;
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
    /// # use crate::error::ErrorDetails;
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
                write!(f, "{k}: {v}")?;
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
    /// use crate::error::SinexError;
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
    /// use crate::error::SinexError;
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
    /// use crate::error::SinexError;
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
    /// use crate::error::SinexError;
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
    /// use crate::error::SinexError;
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
    /// use crate::error::SinexError;
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
/// use crate::error::{Result, ResultExt, SinexError};
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
