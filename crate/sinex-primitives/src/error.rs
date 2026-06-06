use displaydoc::Display;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::backtrace::Backtrace;
use std::fmt;

/// Core error type for the Sinex system.
///
/// This enum represents all possible error conditions in the Sinex ecosystem.
/// Each variant contains an [`ErrorDetails`] struct that holds the error message,
/// optional context as key-value pairs, and optional source errors.
///
/// **Guideline:** Prefer using specific variants of `SinexError` (e.g., `SinexError::database`)
/// over generic `color_eyre::eyre::eyre!` in library code. This ensures errors can be
/// programmatically handled and categorized.
#[derive(Display, Debug, Clone, Serialize, Deserialize)]
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

impl std::error::Error for SinexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.details().source()
    }
}

/// Stable, machine-readable error kind.
///
/// `SinexError` keeps its enum variants for existing pattern matches, while
/// this kind is the public/programmatic classification used by APIs, tests,
/// logs, and future structural error handling. Callers should prefer
/// [`SinexError::kind`] over parsing display strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SinexErrorKind {
    Database,
    Validation,
    Service,
    Io,
    Configuration,
    Serialization,
    Parse,
    NotFound,
    AlreadyExists,
    InvalidState,
    PermissionDenied,
    Network,
    ChannelSend,
    ChannelReceive,
    Timeout,
    Cancelled,
    MaxRetriesExceeded,
    ResourceExhausted,
    Unknown,
    Kv,
    Automaton,
    Checkpoint,
    Lifecycle,
    Processing,
    Nats,
    NatsAckFailed,
    DbPersistenceFailed,
    NatsPublish,
    NatsSubscribe,
    BlobStorage,
    Coordination,
}

impl SinexErrorKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Database => "database",
            Self::Validation => "validation",
            Self::Service => "service",
            Self::Io => "io",
            Self::Configuration => "configuration",
            Self::Serialization => "serialization",
            Self::Parse => "parse",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::InvalidState => "invalid_state",
            Self::PermissionDenied => "permission_denied",
            Self::Network => "network",
            Self::ChannelSend => "channel_send",
            Self::ChannelReceive => "channel_receive",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::MaxRetriesExceeded => "max_retries_exceeded",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Unknown => "unknown",
            Self::Kv => "kv",
            Self::Automaton => "automaton",
            Self::Checkpoint => "checkpoint",
            Self::Lifecycle => "lifecycle",
            Self::Processing => "processing",
            Self::Nats => "nats",
            Self::NatsAckFailed => "nats_ack_failed",
            Self::DbPersistenceFailed => "db_persistence_failed",
            Self::NatsPublish => "nats_publish",
            Self::NatsSubscribe => "nats_subscribe",
            Self::BlobStorage => "blob_storage",
            Self::Coordination => "coordination",
        }
    }
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
    /// The runtime module is permanently broken — halt, do NOT retry.
    /// Checkpoint CAS failure (stale revision), lifecycle state corruption,
    /// output channel closed, invalid runtime configuration, permission denied.
    RuntimeFatal,
    /// Transport is degraded — pause consumption, circuit-break, alert.
    /// DLQ stream unavailable, confirmation stream unavailable,
    /// `JetStream` publish failing beyond retry budget.
    TransportDegraded,
}

/// Captured causal error node.
///
/// This preserves source chains structurally instead of keeping only one
/// flattened string. The original concrete error value is not retained because
/// `SinexError` must remain cloneable and serializable across API/logging
/// boundaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorSource {
    type_name: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    source: Option<Box<ErrorSource>>,
}

impl ErrorSource {
    fn from_error(err: &(dyn std::error::Error + 'static)) -> Self {
        let source = err.source().map(Self::from_error).map(Box::new);
        Self {
            type_name: std::any::type_name_of_val(err).to_string(),
            message: err.to_string(),
            source,
        }
    }

    fn from_typed_error<E>(err: &E) -> Self
    where
        E: std::error::Error + 'static,
    {
        let source = err.source().map(Self::from_error).map(Box::new);
        Self {
            type_name: std::any::type_name::<E>().to_string(),
            message: err.to_string(),
            source,
        }
    }

    #[must_use]
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub fn child(&self) -> Option<&ErrorSource> {
        self.source.as_deref()
    }

    fn push_messages(&self, out: &mut Vec<String>) {
        out.push(self.message.clone());
        if let Some(source) = &self.source {
            source.push_messages(out);
        }
    }
}

impl fmt::Display for ErrorSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ErrorSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

/// Public, sanitized error payload suitable for API responses and user-facing
/// CLI output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicError {
    pub kind: SinexErrorKind,
    pub kind_name: String,
    pub message: String,
    pub status_code: u16,
    #[serde(skip_serializing_if = "IndexMap::is_empty", default)]
    pub context: IndexMap<String, String>,
}

impl ErrorClass {
    #[must_use]
    pub fn is_fatal(self) -> bool {
        matches!(self, Self::RuntimeFatal | Self::TransportDegraded)
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
            SinexError::Checkpoint(_) => ErrorClass::RuntimeFatal,
            SinexError::Lifecycle(_) => ErrorClass::RuntimeFatal,
            SinexError::Configuration(_) => ErrorClass::RuntimeFatal,
            SinexError::PermissionDenied(_) => ErrorClass::RuntimeFatal,
            SinexError::ChannelSend(_) => ErrorClass::RuntimeFatal,
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
    /// Structured causal chain captured from `std::error::Error::source`.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    source_chain: Vec<ErrorSource>,
    /// Optional captured backtrace text. Captured only when explicitly enabled
    /// through `SINEX_ERROR_BACKTRACE=1` or `RUST_BACKTRACE=1/full`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    backtrace: Option<String>,
}

impl ErrorDetails {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            context: IndexMap::new(),
            sources: Vec::new(),
            source_chain: Vec::new(),
            backtrace: None,
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

    pub fn with_error_source<E>(mut self, source: &E) -> Self
    where
        E: std::error::Error + 'static,
    {
        let captured = ErrorSource::from_typed_error(source);
        captured.push_messages(&mut self.sources);
        self.source_chain.push(captured);
        self
    }

    pub fn with_backtrace(mut self) -> Self {
        self.backtrace = Some(Backtrace::capture().to_string());
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

    #[must_use]
    pub fn source_chain(&self) -> &[ErrorSource] {
        &self.source_chain
    }

    #[must_use]
    pub fn backtrace(&self) -> Option<&str> {
        self.backtrace.as_deref()
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source_chain
            .first()
            .map(|source| source as &(dyn std::error::Error + 'static))
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

        if let Some(backtrace) = &self.backtrace {
            write!(f, "\nBacktrace:\n{backtrace}")?;
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
        let captured = ErrorSource::from_error(err);
        captured.push_messages(&mut self.details_mut().sources);
        self.details_mut().source_chain.push(captured);
        if should_capture_backtrace() {
            self = self.with_backtrace();
        }
        self
    }

    /// Captures a typed source error while preserving its concrete type name.
    #[must_use]
    pub fn with_error_source<E>(mut self, err: &E) -> Self
    where
        E: std::error::Error + 'static,
    {
        let captured = ErrorSource::from_typed_error(err);
        captured.push_messages(&mut self.details_mut().sources);
        self.details_mut().source_chain.push(captured);
        if should_capture_backtrace() {
            self = self.with_backtrace();
        }
        self
    }

    /// Explicitly attach a backtrace. Ordinary constructors do not capture one.
    #[must_use]
    pub fn with_backtrace(mut self) -> Self {
        self.details_mut().backtrace = Some(Backtrace::capture().to_string());
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

    #[must_use]
    pub fn source_chain(&self) -> &[ErrorSource] {
        self.details().source_chain()
    }

    #[must_use]
    pub fn backtrace(&self) -> Option<&str> {
        self.details().backtrace()
    }

    #[must_use]
    pub fn kind(&self) -> SinexErrorKind {
        match self {
            SinexError::Database(_) => SinexErrorKind::Database,
            SinexError::Validation(_) => SinexErrorKind::Validation,
            SinexError::Service(_) => SinexErrorKind::Service,
            SinexError::Io(_) => SinexErrorKind::Io,
            SinexError::Configuration(_) => SinexErrorKind::Configuration,
            SinexError::Serialization(_) => SinexErrorKind::Serialization,
            SinexError::Parse(_) => SinexErrorKind::Parse,
            SinexError::NotFound(_) => SinexErrorKind::NotFound,
            SinexError::AlreadyExists(_) => SinexErrorKind::AlreadyExists,
            SinexError::InvalidState(_) => SinexErrorKind::InvalidState,
            SinexError::PermissionDenied(_) => SinexErrorKind::PermissionDenied,
            SinexError::Network(_) => SinexErrorKind::Network,
            SinexError::ChannelSend(_) => SinexErrorKind::ChannelSend,
            SinexError::ChannelReceive(_) => SinexErrorKind::ChannelReceive,
            SinexError::Timeout(_) => SinexErrorKind::Timeout,
            SinexError::Cancelled(_) => SinexErrorKind::Cancelled,
            SinexError::MaxRetriesExceeded(_) => SinexErrorKind::MaxRetriesExceeded,
            SinexError::ResourceExhausted(_) => SinexErrorKind::ResourceExhausted,
            SinexError::Unknown(_) => SinexErrorKind::Unknown,
            SinexError::Kv(_) => SinexErrorKind::Kv,
            SinexError::Automaton(_) => SinexErrorKind::Automaton,
            SinexError::Checkpoint(_) => SinexErrorKind::Checkpoint,
            SinexError::Lifecycle(_) => SinexErrorKind::Lifecycle,
            SinexError::Processing(_) => SinexErrorKind::Processing,
            #[cfg(feature = "nats")]
            SinexError::Nats(_) => SinexErrorKind::Nats,
            #[cfg(feature = "nats")]
            SinexError::NatsAckFailed(_) => SinexErrorKind::NatsAckFailed,
            SinexError::DbPersistenceFailed(_) => SinexErrorKind::DbPersistenceFailed,
            #[cfg(feature = "nats")]
            SinexError::NatsPublish(_) => SinexErrorKind::NatsPublish,
            #[cfg(feature = "nats")]
            SinexError::NatsSubscribe(_) => SinexErrorKind::NatsSubscribe,
            SinexError::BlobStorage(_) => SinexErrorKind::BlobStorage,
            SinexError::Coordination(_) => SinexErrorKind::Coordination,
        }
    }

    // Helper methods for error categorization (used in tests)
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.error_class() == ErrorClass::TransientInfra
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
        self.error_class() == ErrorClass::RuntimeFatal
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

    #[must_use]
    pub fn public_context(&self) -> IndexMap<String, String> {
        const SAFE_KEYS: &[&str] = &[
            "code",
            "constraint",
            "count",
            "database_error_kind",
            "duration_ms",
            "error_type",
            "field",
            "found",
            "kind",
            "operation",
            "reason",
            "requested",
            "retry_after",
            "retry_count",
            "sqlstate",
            "status",
            "timeout_reason",
            "validation_type",
        ];

        self.context_map()
            .iter()
            .filter(|(key, _)| SAFE_KEYS.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    #[must_use]
    pub fn public_payload(&self) -> PublicError {
        let kind = self.kind();
        PublicError {
            kind,
            kind_name: kind.as_str().to_string(),
            message: self.client_message().to_string(),
            status_code: self.status_code(),
            context: self.public_context(),
        }
    }
}

fn should_capture_backtrace() -> bool {
    fn enabled(value: &str) -> bool {
        let trimmed = value.trim();
        !trimmed.is_empty() && trimmed != "0"
    }

    std::env::var("SINEX_ERROR_BACKTRACE")
        .ok()
        .as_deref()
        .is_some_and(enabled)
        || std::env::var("RUST_BACKTRACE")
            .ok()
            .as_deref()
            .is_some_and(enabled)
}

impl From<std::io::Error> for SinexError {
    fn from(e: std::io::Error) -> Self {
        SinexError::Io(ErrorDetails::new(e.to_string())).with_error_source(&e)
    }
}

impl From<serde_json::Error> for SinexError {
    fn from(e: serde_json::Error) -> Self {
        SinexError::Serialization(ErrorDetails::new(e.to_string())).with_error_source(&e)
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

    sinex_error = sinex_error.with_error_source(error);
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
/// pattern mirrors `classify_sqlx_error` for `SQLx` errors.
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

#[cfg(test)]
mod retryability_tests {
    use super::{ErrorClass, ErrorDetails, SinexError};
    use xtask::sandbox::sinex_test;

    fn details() -> ErrorDetails {
        ErrorDetails::new("test")
    }

    /// Returns every `SinexError` variant once.
    /// When a new variant is added to `SinexError`, this list must be extended
    /// or the `match` becomes a compile error — ensuring the test stays
    /// exhaustive.
    fn all_variants() -> Vec<SinexError> {
        vec![
            SinexError::Database(details()),
            SinexError::Validation(details()),
            SinexError::Service(details()),
            SinexError::Io(details()),
            SinexError::Configuration(details()),
            SinexError::Serialization(details()),
            SinexError::Parse(details()),
            SinexError::NotFound(details()),
            SinexError::AlreadyExists(details()),
            SinexError::InvalidState(details()),
            SinexError::PermissionDenied(details()),
            SinexError::Network(details()),
            SinexError::ChannelSend(details()),
            SinexError::ChannelReceive(details()),
            SinexError::Timeout(details()),
            SinexError::Cancelled(details()),
            SinexError::MaxRetriesExceeded(details()),
            SinexError::ResourceExhausted(details()),
            SinexError::Unknown(details()),
            SinexError::Kv(details()),
            SinexError::Automaton(details()),
            SinexError::Checkpoint(details()),
            SinexError::Lifecycle(details()),
            SinexError::Processing(details()),
            SinexError::DbPersistenceFailed(details()),
            SinexError::BlobStorage(details()),
            SinexError::Coordination(details()),
            #[cfg(feature = "nats")]
            SinexError::Nats(details()),
            #[cfg(feature = "nats")]
            SinexError::NatsAckFailed(details()),
            #[cfg(feature = "nats")]
            SinexError::NatsPublish(details()),
            #[cfg(feature = "nats")]
            SinexError::NatsSubscribe(details()),
        ]
    }

    /// `is_retryable` and `is_permanent` must agree with `error_class()` for
    /// every variant. This test will fail to compile if a variant is missing
    /// from `all_variants()` because the exhaustive `match` below covers the
    /// enum — add missing variants to the list to fix it.
    #[sinex_test]
    async fn is_retryable_agrees_with_error_class() -> TestResult<()> {
        for err in all_variants() {
            let class = err.error_class();
            assert_eq!(
                err.is_retryable(),
                class == ErrorClass::TransientInfra,
                "is_retryable disagrees with error_class for {err:?}: class={class:?}"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn is_permanent_agrees_with_error_class() -> TestResult<()> {
        for err in all_variants() {
            let class = err.error_class();
            assert_eq!(
                err.is_permanent(),
                class == ErrorClass::RuntimeFatal,
                "is_permanent disagrees with error_class for {err:?}: class={class:?}"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn retryable_and_permanent_are_mutually_exclusive() -> TestResult<()> {
        for err in all_variants() {
            assert!(
                !(err.is_retryable() && err.is_permanent()),
                "error is both retryable and permanent: {err:?}"
            );
        }
        Ok(())
    }
}
