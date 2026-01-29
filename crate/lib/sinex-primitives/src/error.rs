use displaydoc::Display;
use indexmap::IndexMap;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;
use thiserror::Error;

/// Core error type for the Sinex system.
///
/// This enum represents all possible error conditions in the Sinex ecosystem.
/// Each variant contains an [`ErrorDetails`] struct that holds the error message,
/// optional context as key-value pairs, and optional source errors.
///
/// **Guideline:** Prefer using specific variants of `SinexError` (e.g., `SinexError::database`)
/// over generic `color_eyre::eyre::eyre!` in library code. This ensures errors can be
/// programmatically handled and categorized.
#[derive(Error, Display, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "details")]
#[non_exhaustive]
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
    /// KV Store error: {0}
    Kv(ErrorDetails),
    /// Automaton error: {0}
    Automaton(ErrorDetails),
    /// Checkpoint error: {0}
    Checkpoint(ErrorDetails),
    /// Lifecycle error: {0}
    Lifecycle(ErrorDetails),
    /// Processing error: {0}
    Processing(ErrorDetails),
    /// NATS/Messaging error: {0}
    #[cfg(feature = "nats")]
    Nats(ErrorDetails),
    /// NATS Ack Failed: {0}
    #[cfg(feature = "nats")]
    NatsAckFailed(ErrorDetails),
    /// Database Persistence Failed: {0}
    DbPersistenceFailed(ErrorDetails),
    /// NATS publish operation failed: {0}
    NatsPublish(ErrorDetails),
    /// NATS subscribe operation failed: {0}
    NatsSubscribe(ErrorDetails),
    /// Blob storage operation failed: {0}
    BlobStorage(ErrorDetails),
    /// Coordination operation failed: {0}
    Coordination(ErrorDetails),
}

/// Detailed error information including message, context, and sources.
#[derive(Debug, Clone, Deserialize)]
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
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            context: IndexMap::new(),
            sources: Vec::new(),
        }
    }

    pub fn with_context(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.context.insert(key.into(), value.to_string());
        self
    }

    pub fn with_source(mut self, source: impl ToString) -> Self {
        self.sources.push(source.to_string());
        self
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn context_map(&self) -> &IndexMap<String, String> {
        &self.context
    }

    pub fn sources(&self) -> &[String] {
        &self.sources
    }
}

impl Serialize for ErrorDetails {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ErrorDetails", 3)?;

        state.serialize_field("message", &self.message)?;

        // Sanitize context keys
        const SAFE_KEYS: &[&str] = &[
            "table_name",
            "field_name",
            "operation",
            "resource_type",
            "status_code",
            "retry_count",
            "duration_ms",
            "count",
            "id_type",
            "path",
            "json_path",
            "error_type",
        ];

        let safe_context: IndexMap<_, _> = self
            .context
            .iter()
            .filter(|(k, _)| SAFE_KEYS.contains(&k.as_str()))
            .collect();

        if !safe_context.is_empty() {
            state.serialize_field("context", &safe_context)?;
        }

        if !self.sources.is_empty() {
            state.serialize_field("sources", &self.sources)?;
        }

        state.end()
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
    pub fn database(msg: impl std::fmt::Display) -> Self {
        SinexError::Database(ErrorDetails::new(msg.to_string()))
    }
    pub fn validation(msg: impl std::fmt::Display) -> Self {
        SinexError::Validation(ErrorDetails::new(msg.to_string()))
    }
    pub fn service(msg: impl std::fmt::Display) -> Self {
        SinexError::Service(ErrorDetails::new(msg.to_string()))
    }
    pub fn io(msg: impl std::fmt::Display) -> Self {
        SinexError::Io(ErrorDetails::new(msg.to_string()))
    }
    pub fn configuration(msg: impl std::fmt::Display) -> Self {
        SinexError::Configuration(ErrorDetails::new(msg.to_string()))
    }
    pub fn serialization(msg: impl std::fmt::Display) -> Self {
        SinexError::Serialization(ErrorDetails::new(msg.to_string()))
    }
    pub fn parse(msg: impl std::fmt::Display) -> Self {
        SinexError::Parse(ErrorDetails::new(msg.to_string()))
    }
    pub fn not_found(msg: impl std::fmt::Display) -> Self {
        SinexError::NotFound(ErrorDetails::new(msg.to_string()))
    }
    pub fn already_exists(msg: impl std::fmt::Display) -> Self {
        SinexError::AlreadyExists(ErrorDetails::new(msg.to_string()))
    }
    pub fn invalid_state(msg: impl std::fmt::Display) -> Self {
        SinexError::InvalidState(ErrorDetails::new(msg.to_string()))
    }
    pub fn permission_denied(msg: impl std::fmt::Display) -> Self {
        SinexError::PermissionDenied(ErrorDetails::new(msg.to_string()))
    }
    pub fn network(msg: impl std::fmt::Display) -> Self {
        SinexError::Network(ErrorDetails::new(msg.to_string()))
    }
    pub fn channel_send(msg: impl std::fmt::Display) -> Self {
        SinexError::ChannelSend(ErrorDetails::new(msg.to_string()))
    }
    pub fn channel_receive(msg: impl std::fmt::Display) -> Self {
        SinexError::ChannelReceive(ErrorDetails::new(msg.to_string()))
    }
    pub fn timeout(msg: impl std::fmt::Display) -> Self {
        SinexError::Timeout(ErrorDetails::new(msg.to_string()))
    }
    pub fn cancelled(msg: impl std::fmt::Display) -> Self {
        SinexError::Cancelled(ErrorDetails::new(msg.to_string()))
    }
    pub fn max_retries_exceeded(msg: impl std::fmt::Display) -> Self {
        SinexError::MaxRetriesExceeded(ErrorDetails::new(msg.to_string()))
    }
    pub fn resource_exhausted(msg: impl std::fmt::Display) -> Self {
        SinexError::ResourceExhausted(ErrorDetails::new(msg.to_string()))
    }
    pub fn unknown(msg: impl std::fmt::Display) -> Self {
        SinexError::Unknown(ErrorDetails::new(msg.to_string()))
    }
    pub fn kv(msg: impl std::fmt::Display) -> Self {
        SinexError::Kv(ErrorDetails::new(msg.to_string()))
    }
    pub fn automaton(msg: impl std::fmt::Display) -> Self {
        SinexError::Automaton(ErrorDetails::new(msg.to_string()))
    }
    pub fn checkpoint(msg: impl std::fmt::Display) -> Self {
        SinexError::Checkpoint(ErrorDetails::new(msg.to_string()))
    }
    pub fn lifecycle(msg: impl std::fmt::Display) -> Self {
        SinexError::Lifecycle(ErrorDetails::new(msg.to_string()))
    }
    pub fn processing(msg: impl std::fmt::Display) -> Self {
        SinexError::Processing(ErrorDetails::new(msg.to_string()))
    }

    pub fn messaging(msg: impl std::fmt::Display) -> Self {
        #[cfg(feature = "nats")]
        {
            SinexError::Nats(ErrorDetails::new(msg.to_string()))
        }
        #[cfg(not(feature = "nats"))]
        {
            SinexError::Network(ErrorDetails::new(msg.to_string()))
        }
    }

    pub fn general(msg: impl std::fmt::Display) -> Self {
        SinexError::Unknown(ErrorDetails::new(msg.to_string()))
    }

    #[cfg(feature = "nats")]
    pub fn nats(msg: impl Into<String>) -> Self {
        SinexError::Nats(ErrorDetails::new(msg))
    }

    #[cfg(feature = "nats")]
    pub fn nats_ack_failed(msg: impl Into<String>) -> Self {
        SinexError::NatsAckFailed(ErrorDetails::new(msg))
    }

    pub fn db_persistence_failed(msg: impl std::fmt::Display) -> Self {
        SinexError::DbPersistenceFailed(ErrorDetails::new(msg.to_string()))
    }

    pub fn nats_publish(msg: impl std::fmt::Display) -> Self {
        SinexError::NatsPublish(ErrorDetails::new(msg.to_string()))
    }

    pub fn nats_subscribe(msg: impl std::fmt::Display) -> Self {
        SinexError::NatsSubscribe(ErrorDetails::new(msg.to_string()))
    }

    pub fn blob_storage(msg: impl std::fmt::Display) -> Self {
        SinexError::BlobStorage(ErrorDetails::new(msg.to_string()))
    }

    pub fn coordination(msg: impl std::fmt::Display) -> Self {
        SinexError::Coordination(ErrorDetails::new(msg.to_string()))
    }

    pub fn with_context(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        use SinexError::*;
        let details = match &mut self {
            Database(d)
            | DbPersistenceFailed(d)
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
            | Unknown(d)
            | Kv(d)
            | Automaton(d)
            | Checkpoint(d)
            | Lifecycle(d)
            | Processing(d)
            | NatsPublish(d)
            | NatsSubscribe(d)
            | BlobStorage(d)
            | Coordination(d) => d,
            #[cfg(feature = "nats")]
            Nats(d) | NatsAckFailed(d) => d,
        };
        details.context.insert(key.into(), value.to_string());
        self
    }

    pub fn with_source(mut self, source: impl ToString) -> Self {
        use SinexError::*;
        let details = match &mut self {
            Database(d)
            | DbPersistenceFailed(d)
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
            | Unknown(d)
            | Kv(d)
            | Automaton(d)
            | Checkpoint(d)
            | Lifecycle(d)
            | Processing(d)
            | NatsPublish(d)
            | NatsSubscribe(d)
            | BlobStorage(d)
            | Coordination(d) => d,
            #[cfg(feature = "nats")]
            Nats(d) | NatsAckFailed(d) => d,
        };
        details.sources.push(source.to_string());
        self
    }

    /// Captures the full error chain from a standard error trait object.
    pub fn with_std_error(mut self, err: &(dyn std::error::Error + 'static)) -> Self {
        self = self.with_source(err); // Adds the error itself as a source description
        let mut current = err.source();
        while let Some(cause) = current {
            self = self.with_source(cause);
            current = cause.source();
        }
        self
    }

    pub fn with_operation(self, operation: impl Into<String>) -> Self {
        self.with_context("operation", operation.into())
    }

    pub fn with_path(self, path: impl ToString) -> Self {
        self.with_context("path", path.to_string())
    }

    pub fn with_id(self, key: impl Into<String>, id: impl ToString) -> Self {
        self.with_context(key, id.to_string())
    }

    pub fn with_count(self, count: usize) -> Self {
        self.with_context("count", count)
    }

    pub fn with_duration(self, duration: std::time::Duration) -> Self {
        self.with_context("duration_ms", duration.as_millis())
    }

    fn details(&self) -> &ErrorDetails {
        use SinexError::*;
        match self {
            Database(d)
            | DbPersistenceFailed(d)
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
            | Unknown(d)
            | Kv(d)
            | Automaton(d)
            | Checkpoint(d)
            | Lifecycle(d)
            | Processing(d)
            | NatsPublish(d)
            | NatsSubscribe(d)
            | BlobStorage(d)
            | Coordination(d) => d,
            #[cfg(feature = "nats")]
            Nats(d) | NatsAckFailed(d) => d,
        }
    }

    pub fn message(&self) -> &str {
        self.details().message()
    }

    pub fn context_map(&self) -> &IndexMap<String, String> {
        self.details().context_map()
    }

    pub fn sources(&self) -> &[String] {
        self.details().sources()
    }

    pub fn variant_name(&self) -> &'static str {
        use SinexError::*;
        match self {
            Database(_) => "Database",
            DbPersistenceFailed(_) => "DbPersistenceFailed",
            Validation(_) => "Validation",
            Service(_) => "Service",
            Io(_) => "Io",
            Configuration(_) => "Configuration",
            Serialization(_) => "Serialization",
            Parse(_) => "Parse",
            NotFound(_) => "NotFound",
            AlreadyExists(_) => "AlreadyExists",
            InvalidState(_) => "InvalidState",
            PermissionDenied(_) => "PermissionDenied",
            Network(_) => "Network",
            ChannelSend(_) => "ChannelSend",
            ChannelReceive(_) => "ChannelReceive",
            Timeout(_) => "Timeout",
            Cancelled(_) => "Cancelled",
            MaxRetriesExceeded(_) => "MaxRetriesExceeded",
            ResourceExhausted(_) => "ResourceExhausted",
            Unknown(_) => "Unknown",
            Kv(_) => "Kv",
            Automaton(_) => "Automaton",
            Checkpoint(_) => "Checkpoint",
            Lifecycle(_) => "Lifecycle",
            Processing(_) => "Processing",
            NatsPublish(_) => "NatsPublish",
            NatsSubscribe(_) => "NatsSubscribe",
            BlobStorage(_) => "BlobStorage",
            Coordination(_) => "Coordination",
            #[cfg(feature = "nats")]
            Nats(_) => "Nats",
            #[cfg(feature = "nats")]
            NatsAckFailed(_) => "NatsAckFailed",
        }
    }

    // Helper methods for error categorization (used in tests)
    pub fn is_retryable(&self) -> bool {
        use SinexError::*;
        matches!(
            self,
            Network(_) | Timeout(_) | ChannelSend(_) | ChannelReceive(_) | Unknown(_)
        )
    }

    pub fn is_client_error(&self) -> bool {
        use SinexError::*;
        matches!(
            self,
            Validation(_) | NotFound(_) | AlreadyExists(_) | PermissionDenied(_)
        )
    }

    pub fn is_permanent(&self) -> bool {
        use SinexError::*;
        matches!(
            self,
            MaxRetriesExceeded(_) | PermissionDenied(_) | Configuration(_)
        )
    }

    pub fn status_code(&self) -> u16 {
        use SinexError::*;
        match self {
            Validation(_) => 400,
            NotFound(_) => 404,
            PermissionDenied(_) => 403,
            Timeout(_) => 408,
            AlreadyExists(_) => 409,
            ResourceExhausted(_) => 429,
            _ => 500,
        }
    }
}

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

#[cfg(feature = "sqlx")]
impl From<sqlx::Error> for SinexError {
    fn from(e: sqlx::Error) -> Self {
        SinexError::Database(ErrorDetails::new(e.to_string())).with_std_error(&e)
    }
}

#[cfg(feature = "nats")]
impl<T> From<async_nats::error::Error<T>> for SinexError
where
    T: std::clone::Clone + std::fmt::Debug + std::fmt::Display + std::cmp::PartialEq,
{
    fn from(e: async_nats::error::Error<T>) -> Self {
        SinexError::nats(format!("{}", e))
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

pub type Result<T> = std::result::Result<T, SinexError>;

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

pub trait ResultExt<T> {
    /// Adds a simple text context to the error.
    fn context(self, msg: &str) -> Result<T>;

    /// Adds a custom error with full context.
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
    use super::*;

    #[test]
    fn test_serialization_filtering() {
        let err = ErrorDetails::new("test")
            .with_context("table_name", "users") // Safe
            .with_context("secret_info", "hidden"); // Unsafe

        let json = serde_json::to_value(&err).unwrap();
        let context = json.get("context").unwrap();

        assert!(context.get("table_name").is_some());
        assert!(context.get("secret_info").is_none());
    }
}
