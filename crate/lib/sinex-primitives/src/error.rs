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
    #[cfg(feature = "nats")]
    NatsPublish(ErrorDetails),
    /// NATS subscribe operation failed: {0}
    #[cfg(feature = "nats")]
    NatsSubscribe(ErrorDetails),
    /// Blob storage operation failed: {0}
    BlobStorage(ErrorDetails),
    /// Coordination operation failed: {0}
    Coordination(ErrorDetails),
}

/// Classification of a `SinexError` by its inherent semantics.
///
/// Every error variant maps to one class. This is the default mapping — callers
/// should use [`SinexError::error_class`] rather than matching variants directly.
/// For contextual overrides (e.g. `ChannelSend` during shutdown is not fatal),
/// see `FailurePolicy` in the `settlement` module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorClass {
    /// This event is invalid — DLQ it and continue.
    /// Schema validation failures, malformed payloads, domain rule violations.
    DataError,
    /// A transient infrastructure hiccup — retry with finite backoff.
    /// Network timeouts, brief NATS disconnections, CAS races on first attempt.
    TransientInfra,
    /// The node or runtime is permanently broken — halt, do NOT retry.
    /// Checkpoint CAS failure (stale revision), lifecycle state corruption,
    /// output channel closed, invalid runtime configuration, permission denied.
    NodeFatal,
    /// Transport is degraded — pause consumption, circuit-break, alert.
    /// DLQ stream unavailable, confirmation stream unavailable,
    /// JetStream publish failing beyond retry budget.
    TransportDegraded,
}

impl ErrorClass {
    #[must_use]
    pub fn is_fatal(self) -> bool {
        matches!(self, Self::NodeFatal | Self::TransportDegraded)
    }

    #[must_use]
    pub fn is_data_error(self) -> bool {
        matches!(self, Self::DataError)
    }
}

impl SinexError {
    /// Default error classification. Callers may override with `FailurePolicy`
    /// for context-specific settlement (e.g. `ChannelSend` during normal
    /// shutdown is not fatal).
    #[must_use]
    pub fn error_class(&self) -> ErrorClass {
        match self {
            SinexError::Checkpoint(_) => ErrorClass::NodeFatal,
            SinexError::Lifecycle(_) => ErrorClass::NodeFatal,
            SinexError::Configuration(_) => ErrorClass::NodeFatal,
            SinexError::PermissionDenied(_) => ErrorClass::NodeFatal,
            SinexError::ChannelSend(_) => ErrorClass::NodeFatal,
            SinexError::Validation(_) => ErrorClass::DataError,
            SinexError::Parse(_) => ErrorClass::DataError,
            SinexError::Serialization(_) => ErrorClass::DataError,
            SinexError::Processing(_) => ErrorClass::TransientInfra,
            SinexError::Automaton(_) => ErrorClass::DataError,
            SinexError::NotFound(_) => ErrorClass::DataError,
            SinexError::AlreadyExists(_) => ErrorClass::DataError,
            SinexError::InvalidState(_) => ErrorClass::DataError,
            SinexError::Network(_) => ErrorClass::TransientInfra,
            SinexError::Timeout(_) => ErrorClass::TransientInfra,
            SinexError::Cancelled(_) => ErrorClass::TransientInfra,
            SinexError::MaxRetriesExceeded(_) => ErrorClass::TransientInfra,
            SinexError::Io(_) => ErrorClass::TransientInfra,
            SinexError::Database(_) => ErrorClass::TransientInfra,
            SinexError::DbPersistenceFailed(_) => ErrorClass::TransientInfra,
            SinexError::Service(_) => ErrorClass::TransientInfra,
            SinexError::ResourceExhausted(_) => ErrorClass::TransientInfra,
            SinexError::Kv(_) => ErrorClass::TransientInfra,
            SinexError::ChannelReceive(_) => ErrorClass::TransientInfra,
            SinexError::Unknown(_) => ErrorClass::TransientInfra,
            #[cfg(feature = "nats")]
            SinexError::Nats(_) => ErrorClass::TransientInfra,
            #[cfg(feature = "nats")]
            SinexError::NatsAckFailed(_) => ErrorClass::TransientInfra,
            #[cfg(feature = "nats")]
            SinexError::NatsPublish(_) => ErrorClass::TransientInfra,
            #[cfg(feature = "nats")]
            SinexError::NatsSubscribe(_) => ErrorClass::TransientInfra,
            SinexError::BlobStorage(_) => ErrorClass::TransientInfra,
            SinexError::Coordination(_) => ErrorClass::TransientInfra,
        }
    }
}

/// Detailed error information including message, context, and sources.
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
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            context: IndexMap::new(),
            sources: Vec::new(),
        }
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Builder API: allows callers to pass owning strings without explicit borrow"
    )]
    pub fn with_context(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.context.insert(key.into(), value.to_string());
        self
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Builder API: allows callers to pass owning strings without explicit borrow"
    )]
    pub fn with_source(mut self, source: impl ToString) -> Self {
        self.sources.push(source.to_string());
        self
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub fn context_map(&self) -> &IndexMap<String, String> {
        &self.context
    }

    #[must_use]
    pub fn sources(&self) -> &[String] {
        &self.sources
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

/// Generates `details()`, `details_mut()`, and `variant_name()` for `SinexError`.
///
/// This is the **single source of truth** for the variant list used in match arms.
/// When adding a new variant to the enum, add it here too — all three methods are
/// generated automatically from this one list.
///
/// `variant_name()` uses `stringify!()`, eliminating the typo risk of hand-written
/// string literals. `details()` and `details_mut()` are generated from the same
/// list, eliminating the sync burden between them.
macro_rules! sinex_error_accessors {
    (
        variants: [$($v:ident),+ $(,)?],
        nats_variants: [$($nv:ident),+ $(,)?] $(,)?
    ) => {
        fn details_mut(&mut self) -> &mut ErrorDetails {
            match self {
                $(SinexError::$v(d))|+ => d,
                #[cfg(feature = "nats")]
                $(SinexError::$nv(d))|+ => d,
            }
        }

        fn details(&self) -> &ErrorDetails {
            match self {
                $(SinexError::$v(d))|+ => d,
                #[cfg(feature = "nats")]
                $(SinexError::$nv(d))|+ => d,
            }
        }

        /// Returns the variant name as a static string.
        ///
        /// Generated via `stringify!()` from the macro — guaranteed to match
        /// the actual variant identifier, eliminating typo risk.
        #[must_use]
        pub fn variant_name(&self) -> &'static str {
            match self {
                $(SinexError::$v(_) => stringify!($v),)+
                $(#[cfg(feature = "nats")] SinexError::$nv(_) => stringify!($nv),)+
            }
        }
    };
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

    #[cfg(feature = "nats")]
    pub fn nats_publish(msg: impl std::fmt::Display) -> Self {
        SinexError::NatsPublish(ErrorDetails::new(msg.to_string()))
    }

    #[cfg(feature = "nats")]
    pub fn nats_subscribe(msg: impl std::fmt::Display) -> Self {
        SinexError::NatsSubscribe(ErrorDetails::new(msg.to_string()))
    }

    pub fn blob_storage(msg: impl std::fmt::Display) -> Self {
        SinexError::BlobStorage(ErrorDetails::new(msg.to_string()))
    }

    pub fn coordination(msg: impl std::fmt::Display) -> Self {
        SinexError::Coordination(ErrorDetails::new(msg.to_string()))
    }

    // Generated via `sinex_error_accessors!` — see macro definition above for
    // details. Single source of truth for all variant-list match arms.
    sinex_error_accessors! {
        variants: [
            Database, Validation, Service, Io, Configuration, Serialization,
            Parse, NotFound, AlreadyExists, InvalidState, PermissionDenied,
            Network, ChannelSend, ChannelReceive, Timeout, Cancelled,
            MaxRetriesExceeded, ResourceExhausted, Unknown, Kv, Automaton,
            Checkpoint, Lifecycle, Processing, DbPersistenceFailed,
            BlobStorage, Coordination,
        ],
        nats_variants: [Nats, NatsAckFailed, NatsPublish, NatsSubscribe],
    }

    #[must_use]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Builder API: allows callers to pass owning strings without explicit borrow"
    )]
    pub fn with_context(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.details_mut()
            .context
            .insert(key.into(), value.to_string());
        self
    }

    #[must_use]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Builder API: allows callers to pass owning strings without explicit borrow"
    )]
    pub fn with_source(mut self, source: impl ToString) -> Self {
        self.details_mut().sources.push(source.to_string());
        self
    }

    /// Captures the full error chain from a standard error trait object.
    #[must_use]
    pub fn with_std_error(mut self, err: &(dyn std::error::Error + 'static)) -> Self {
        self = self.with_source(err); // Adds the error itself as a source description
        let mut current = err.source();
        while let Some(cause) = current {
            self = self.with_source(cause);
            current = cause.source();
        }
        self
    }

    #[must_use]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Builder API: allows callers to pass owning types without explicit borrow"
    )]
    pub fn with_operation(self, operation: impl Into<String>) -> Self {
        self.with_context("operation", operation.into())
    }

    #[must_use]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Builder API: allows callers to pass owning strings without explicit borrow"
    )]
    pub fn with_path(self, path: impl ToString) -> Self {
        self.with_context("path", path.to_string())
    }

    #[must_use]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Builder API: allows callers to pass owning types without explicit borrow"
    )]
    pub fn with_id(self, key: impl Into<String>, id: impl ToString) -> Self {
        self.with_context(key, id.to_string())
    }

    #[must_use]
    pub fn with_count(self, count: usize) -> Self {
        self.with_context("count", count)
    }

    #[must_use]
    pub fn with_duration(self, duration: std::time::Duration) -> Self {
        self.with_context("duration_ms", duration.as_millis())
    }

    #[must_use]
    pub fn message(&self) -> &str {
        self.details().message()
    }

    #[must_use]
    pub fn context_map(&self) -> &IndexMap<String, String> {
        self.details().context_map()
    }

    #[must_use]
    pub fn sources(&self) -> &[String] {
        self.details().sources()
    }

    // Helper methods for error categorization (used in tests)
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        use SinexError::{ChannelReceive, ChannelSend, Network, Timeout, Unknown};
        matches!(
            self,
            Network(_) | Timeout(_) | ChannelSend(_) | ChannelReceive(_) | Unknown(_)
        )
    }

    #[must_use]
    pub fn is_client_error(&self) -> bool {
        use SinexError::{AlreadyExists, NotFound, PermissionDenied, Validation};
        matches!(
            self,
            Validation(_) | NotFound(_) | AlreadyExists(_) | PermissionDenied(_)
        )
    }

    #[must_use]
    pub fn is_permanent(&self) -> bool {
        use SinexError::{Configuration, MaxRetriesExceeded, PermissionDenied};
        matches!(
            self,
            MaxRetriesExceeded(_) | PermissionDenied(_) | Configuration(_)
        )
    }

    /// Returns a message safe for client consumption.
    ///
    /// For client-facing variants (Validation, `NotFound`, `AlreadyExists`, `InvalidState`,
    /// `PermissionDenied`, Parse), this returns the primary error message verbatim — these
    /// messages are authored at call sites specifically to be user-readable and must not
    /// contain implementation details. All other variants return a generic category string
    /// that reveals no infrastructure topology, SQL, paths, or internal state.
    ///
    /// Use this method exclusively when constructing error responses for external callers
    /// (API responses, CLI output). Internal code should use `Display` for full fidelity.
    #[must_use]
    pub fn client_message(&self) -> &str {
        use SinexError::{
            AlreadyExists, Automaton, BlobStorage, Cancelled, ChannelReceive, ChannelSend,
            Checkpoint, Configuration, Coordination, Database, DbPersistenceFailed, InvalidState,
            Io, Kv, Lifecycle, MaxRetriesExceeded, Network, NotFound, Parse, PermissionDenied,
            Processing, ResourceExhausted, Serialization, Service, Timeout, Unknown, Validation,
        };
        #[cfg(feature = "nats")]
        use SinexError::{Nats, NatsAckFailed, NatsPublish, NatsSubscribe};
        match self {
            // Client-authored messages — safe to surface verbatim
            Validation(d) | NotFound(d) | AlreadyExists(d) | InvalidState(d)
            | PermissionDenied(d) | Parse(d) => d.message(),
            // Server-internal errors — generic strings only
            Database(_) | DbPersistenceFailed(_) => "A database error occurred",
            Network(_) => "A connectivity error occurred",
            Timeout(_) => "The operation timed out",
            ResourceExhausted(_) => "Server resource limit reached",
            Service(_) => "An internal service error occurred",
            Io(_) => "An internal server error occurred",
            Configuration(_) => "A server configuration error occurred",
            Serialization(_) => "An internal serialization error occurred",
            Cancelled(_) => "The operation was cancelled",
            MaxRetriesExceeded(_) => "The operation failed after too many retries",
            ChannelSend(_) | ChannelReceive(_) => "An internal communication error occurred",
            Kv(_) | Automaton(_) | Checkpoint(_) | Lifecycle(_) | Processing(_) => {
                "An internal processing error occurred"
            }
            BlobStorage(_) => "A storage error occurred",
            Coordination(_) => "A coordination error occurred",
            Unknown(_) => "An unknown error occurred",
            #[cfg(feature = "nats")]
            Nats(_) | NatsAckFailed(_) | NatsPublish(_) | NatsSubscribe(_) => {
                "A messaging error occurred"
            }
        }
    }

    #[must_use]
    pub fn status_code(&self) -> u16 {
        use SinexError::{
            AlreadyExists, NotFound, PermissionDenied, ResourceExhausted, Timeout, Validation,
        };
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
        SinexError::Io(ErrorDetails::new(e.to_string())).with_std_error(&e)
    }
}

impl From<serde_json::Error> for SinexError {
    fn from(e: serde_json::Error) -> Self {
        SinexError::Serialization(ErrorDetails::new(e.to_string())).with_std_error(&e)
    }
}

#[cfg(feature = "sqlx")]
fn classify_sqlx_error(error: &sqlx::Error, message: impl Into<String>) -> SinexError {
    use sqlx::error::ErrorKind;

    let message = message.into();
    let mut sinex_error = match error {
        sqlx::Error::RowNotFound => SinexError::not_found(message),
        sqlx::Error::PoolTimedOut => {
            SinexError::timeout(message).with_context("timeout_reason", "pool_exhausted")
        }
        sqlx::Error::Database(db_err) => {
            let mut err = match db_err.kind() {
                ErrorKind::UniqueViolation => SinexError::already_exists(message),
                ErrorKind::ForeignKeyViolation
                | ErrorKind::NotNullViolation
                | ErrorKind::CheckViolation => SinexError::validation(message),
                ErrorKind::Other | _ => SinexError::database(message),
            };

            if let Some(code) = db_err.code() {
                err = err.with_context("sqlstate", code.as_ref());
            }
            if let Some(constraint) = db_err.constraint() {
                err = err.with_context("constraint", constraint);
            }
            if let Some(table) = db_err.table() {
                err = err.with_context("table", table);
            }

            err.with_context("database_error_kind", format!("{:?}", db_err.kind()))
        }
        _ => SinexError::database(message),
    };

    sinex_error = sinex_error.with_std_error(error);
    sinex_error
}

#[cfg(feature = "sqlx")]
impl From<sqlx::Error> for SinexError {
    fn from(e: sqlx::Error) -> Self {
        classify_sqlx_error(&e, e.to_string())
    }
}

/// Build a `SinexError::Nats` enriched with the kind name, kind value, and
/// the underlying source-error chain. The bare `From` impl below uses this so
/// every NATS-bound failure path inherits the same diagnostics shape — the
/// pattern mirrors `classify_sqlx_error` for SQLx errors.
///
/// The captured shape is intentionally string-only (no `with_std_error`) so
/// the helper does not require a `'static` bound on `T`. Every NATS error
/// kind in `async_nats` is itself `'static` in practice, but stringifying
/// keeps the helper usable in generic code paths that don't promise it.
#[cfg(feature = "nats")]
fn classify_nats_error<T>(error: &async_nats::error::Error<T>) -> SinexError
where
    T: std::clone::Clone + std::fmt::Debug + std::fmt::Display + std::cmp::PartialEq,
{
    use std::error::Error as _;

    let kind_type = std::any::type_name::<T>().rsplit("::").next().unwrap_or("");
    let mut sinex_error = SinexError::nats(format!("{error}"))
        .with_context("nats_kind_type", kind_type)
        .with_context("nats_kind", format!("{:?}", error.kind()));
    if let Some(source) = error.source() {
        sinex_error = sinex_error.with_context("nats_source", source.to_string());
    }
    sinex_error
}

#[cfg(feature = "nats")]
impl<T> From<async_nats::error::Error<T>> for SinexError
where
    T: std::clone::Clone + std::fmt::Debug + std::fmt::Display + std::cmp::PartialEq,
{
    fn from(e: async_nats::error::Error<T>) -> Self {
        classify_nats_error(&e)
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for SinexError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        // `SendError<T>` is a unit-variant signal: the receiver was dropped.
        // The `T` payload is owned by the error and would require `Debug`
        // to render — capture only the type name so generic call sites
        // don't pull in extra bounds.
        let payload_type = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("unknown");
        SinexError::ChannelSend(ErrorDetails::new(err.to_string()))
            .with_context("channel_direction", "send")
            .with_context("channel_state", "receiver_dropped")
            .with_context("payload_type", payload_type)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for SinexError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        SinexError::ChannelReceive(ErrorDetails::new(err.to_string()))
            .with_context("channel_direction", "recv")
            .with_context("channel_state", "sender_dropped")
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
    /// Adds a "context" key with the given message to the error, preserving the
    /// original error variant.
    fn context(self, msg: &str) -> Result<T>;

    /// Adds a key-value context pair to the error, preserving the original variant.
    ///
    /// Unlike the previous closure-based design, this preserves the error variant
    /// and simply adds structured context. Use `.map_err()` directly if you need
    /// to replace the error entirely.
    fn with_context(self, key: impl Into<String>, value: impl ToString) -> Result<T>;
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

    fn with_context(self, key: impl Into<String>, value: impl ToString) -> Result<T> {
        self.map_err(|e| {
            let err: SinexError = e.into();
            err.with_context(key, value)
        })
    }
}
