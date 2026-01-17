#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]

//! Shared runtime for Sinex nodes (ingestors and automata).

// Macro re-exports removed; prefer explicit imports from `sinex-macros` if needed.
pub mod acquisition_manager;
pub mod annex;
pub mod automaton_base;
pub mod automaton_event_handler;
pub mod checkpoint;
pub mod config;
pub mod confirmation_handler;
pub mod coordination;
pub mod dlq_retry;
pub mod error_helpers;
pub mod event_processor;
pub mod examples;
pub mod heartbeat;
pub mod ingestion_helpers;
pub mod jetstream_consumer;
pub mod lifecycle;
pub mod nats_publisher;
#[cfg(feature = "preflight")]
pub mod preflight;
pub mod prelude;
pub mod replay;
pub mod runtime;
pub mod schema_validator;
pub mod shutdown;
pub mod simple_processor;
pub mod stage_as_you_go;
pub mod stream_processor {
    pub use crate::runtime::stream::*;
}
pub mod version;

pub use acquisition_manager::{
    AcquisitionManager, AppendStreamAcquirer, RotationPolicy, SourceMaterialHandle,
};
pub use automaton_base::{
    ActivityEntry, AutomatonFields, AutomatonStats, ChannelConfirmedEventHandler,
    IngestionHistoryEntry, DEFAULT_CHANNEL_CAPACITY, DEFAULT_MAX_HISTORY_ENTRIES,
};
pub use automaton_event_handler::AutomatonEventHandler;
pub use checkpoint::{CheckpointManager, CheckpointState};
pub use config::{AutomatonConfig, EventSourceConfig, NodeConfig};
pub use confirmation_handler::{
    ConfirmationBuffer, ConfirmedEventHandler, EventConfirmation, ProcessingModel,
    ProvisionalEvent, ProvisionalEventHandler,
};
pub use coordination::{HandoffRequest, InstanceMode, NodeCoordination};
pub use dlq_retry::{DlqRetryConfig, DlqRetryHandler, DlqStats};
pub use heartbeat::{HeartbeatCounterHandle, HeartbeatEmitter, HeartbeatLogSink, HeartbeatMetrics};
pub use jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig};

pub use lifecycle::{LifecycleManager, ServiceStatus};
pub use nats_publisher::NatsPublisher;
pub use replay::{
    MetricsSnapshot, ProgressTracker, ReplayController, ReplayFilters, ReplayMetrics, ReplayMode,
    ReplayProgress, ReplayResult, ReplayService, ReplayStats,
};
pub use shutdown::{default_checkpoint_path, ShutdownConfig, ShutdownHandler, ShutdownSignal};
pub use simple_processor::{
    ErrorAction, PersistedState, SimpleProcessor, SimpleProcessorConfig, SimpleProcessorError,
    SimpleProcessorNode,
};
pub use stream_processor::{
    Checkpoint, EventSender, EventStream, Node, NodeCapabilities, NodeType, ScanArgs,
    ScanEstimate, ScanReport, StreamProcessorRunner, TimeHorizon,
};
pub use version::{NodeInstance, NodeVersion};

// Re-export commonly used annex types

// Re-export preflight utilities
pub use annex::{AnnexConfig, AnnexKey, BlobManager, BlobMetadata, GitAnnex};
#[cfg(feature = "preflight")]
pub use preflight::{run_preflight_checks, verify_service_dependencies, VerificationStatus};

/// Version information for node components
#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub git_revision: String,
    pub binary_hash: String,
    pub component_version: String,
}

impl VersionInfo {
    /// Create version info for the current component
    pub fn current(component_name: &str) -> Self {
        let version = option_env!("SATELLITE_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"));
        let git_revision = option_env!("SATELLITE_COMMIT_HASH")
            .or_else(|| option_env!("GIT_HASH"))
            .unwrap_or("unknown");
        let mut binary_hash = option_env!("SATELLITE_BINARY_HASH")
            .or_else(|| option_env!("BINARY_HASH"))
            .or_else(|| option_env!("GIT_HASH"))
            .unwrap_or("unknown");
        if binary_hash == "unknown" {
            binary_hash = git_revision;
        }

        Self {
            git_revision: git_revision.to_string(),
            binary_hash: binary_hash.to_string(),
            component_version: format!("{}-v{}", component_name, version),
        }
    }
}

#[cfg(test)]
mod version_info_tests {
    use super::VersionInfo;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    async fn version_info_has_build_stamp() -> color_eyre::eyre::Result<()> {
        let info = VersionInfo::current("build-stamp-check");
        assert!(!info.git_revision.is_empty());
        assert!(!info.binary_hash.is_empty());

        if !cfg!(debug_assertions) {
            assert_ne!(info.git_revision, "unknown");
            assert_ne!(info.binary_hash, "unknown");
        }

        Ok(())
    }
}

/// Common CLI arguments for node services.
///
/// This structure provides standardized command-line arguments that all
/// node services can use. It includes common parameters for NATS
/// communication, batching, and operational modes.
///
/// # Examples
///
/// ```rust
/// use clap::Parser;
/// use sinex_node_sdk::NodeArgs;
///
/// // Parse from command line
/// let args = NodeArgs::parse();
///
/// // Use in service configuration
/// let config = NodeConfig {
///     service_name: args.service_name.clone(),
///     nats_url: args.nats_url.clone(),
///     dry_run: args.dry_run,
///     // ... other fields
/// };
/// ```
#[derive(clap::Parser, Debug, Clone)]
pub struct NodeArgs {
    /// NATS server URL for event ingestion
    #[arg(long, env = "SINEX_NATS_URL", default_value = "nats://localhost:4222")]
    pub nats_url: String,

    /// Service name for identification
    #[arg(long, default_value = "node")]
    pub service_name: String,

    /// Event batch size.
    ///
    /// Number of events to accumulate before submitting a batch to ingestd.
    /// Higher values improve throughput but increase latency and memory usage.
    #[arg(long, default_value = "100")]
    pub batch_size: usize,

    /// Batch timeout in seconds
    #[arg(long, default_value = "5")]
    pub batch_timeout: u64,

    /// Working directory for temporary files
    #[arg(long)]
    pub work_dir: Option<std::path::PathBuf>,

    /// Enable dry run mode (no actual event ingestion).
    ///
    /// When enabled, the service will process events but not submit them
    /// to ingestd. Useful for testing and debugging processing logic.
    #[arg(long)]
    pub dry_run: bool,
}

// Re-export commonly used types from dependencies
pub use sinex_core::types::error::SinexError;
pub use sinex_core::types::ulid::Ulid;
// Just use the actual Event type from sinex_core directly - no confusing aliases!

/// Result type for node operations
pub type NodeResult<T> = std::result::Result<T, NodeError>;

/// Common error types for node services.
///
/// This enum provides a unified error handling system for all node services.
/// Error types are categorized by their source and expected handling:
///
/// # Error Categories
///
/// ## Configuration Errors
/// - `Config`: Invalid configuration values, missing required fields
/// - **Recovery**: Fix configuration and restart service
/// - **Typical causes**: Missing environment variables, invalid file paths, malformed TOML
///
/// ## Communication Errors
/// - `Grpc`: gRPC communication failures with ingestd
/// - `GrpcTransport`: Lower-level transport issues (connection refused, timeout)
/// - **Recovery**: Retry with backoff, check service health
/// - **Typical causes**: ingestd not running, socket permission issues, network problems
///
/// ## Data Storage Errors
/// - `Redis`: Redis connection or operation failures
/// - `Database`: PostgreSQL connection or query failures
/// - **Recovery**: Retry with backoff, implement circuit breaker
/// - **Typical causes**: Service unavailable, connection pool exhausted, query timeouts
///
/// ## Processing Errors
/// - `Processing`: Recoverable processing failures (bad input, temporary resource issues)
/// - `Automaton`: Automaton-specific processing failures
/// - **Recovery**: Skip/retry individual items, log for investigation
/// - **Typical causes**: Malformed events, resource exhaustion, business rule violations
///
/// ## System Errors
/// - `Serialization`: JSON/TOML serialization failures
/// - `Io`: Filesystem and general I/O failures
/// - `General`: Catch-all for unexpected errors
/// - **Recovery**: Varies by context, often requires manual intervention
///
/// ## Lifecycle Errors
/// - `Checkpoint`: Checkpoint loading/saving failures
/// - `Lifecycle`: Service startup/shutdown failures
/// - **Recovery**: Restart service, investigate system state
///
/// # Error Handling Patterns
///
/// ```rust
/// use sinex_node_sdk::{NodeError, NodeResult};
///
/// // Recoverable processing error
/// fn process_event(event: &Event<JsonValue>) -> NodeResult<()> {
///     if event.payload.is_null() {
///         return Err(NodeError::Processing(
///             "Event payload cannot be null".to_string()
///         ));
///     }
///     // ... process event
///     Ok(())
/// }
///
/// // Non-recoverable configuration error
/// fn validate_config(config: &Config) -> NodeResult<()> {
///     if config.service_name.is_empty() {
///         return Err(NodeError::Config(
///             config::ConfigError::MissingField("service_name".to_string())
///         ));
///     }
///     Ok(())
/// }
/// ```
#[derive(thiserror::Error, Debug)]
pub enum NodeError {
    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("Configuration parsing error: {0}")]
    Configuration(String),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("General error: {0}")]
    General(#[from] color_eyre::eyre::Error),

    #[error("Processing error: {0}")]
    Processing(String),

    #[error("Automaton error: {0}")]
    Automaton(String),

    #[error("Checkpoint error: {0}")]
    Checkpoint(String),

    #[error("Lifecycle error: {0}")]
    Lifecycle(String),

    #[error("Operation cancelled: {0}")]
    OperationCancelled(String),

    #[error("Validation error: {0}")]
    Validation(String),
}

impl From<NodeError> for sinex_core::error::SinexError {
    fn from(e: NodeError) -> Self {
        use sinex_core::error::SinexError;
        match e {
            NodeError::Config(_) => SinexError::configuration(e.to_string()),
            NodeError::Configuration(_) => SinexError::configuration(e.to_string()),
            NodeError::Database(_) => SinexError::database(e.to_string()),
            NodeError::Serialization(_) => SinexError::serialization(e.to_string()),
            NodeError::Io(_) => SinexError::io(e.to_string()),
            NodeError::General(_) => SinexError::unknown(e.to_string()),
            NodeError::Processing(_) => SinexError::processing(e.to_string()),
            NodeError::Automaton(_) => SinexError::automaton(e.to_string()),
            NodeError::Checkpoint(_) => SinexError::checkpoint(e.to_string()),
            NodeError::Lifecycle(_) => SinexError::lifecycle(e.to_string()),
            NodeError::OperationCancelled(_) => SinexError::cancelled(e.to_string()),
            NodeError::Validation(_) => SinexError::validation(e.to_string()),
        }
    }
}

impl From<sinex_core::error::SinexError> for NodeError {
    fn from(e: sinex_core::error::SinexError) -> Self {
        use sinex_core::error::SinexError;
        match e {
            SinexError::Configuration(_) => NodeError::Configuration(e.to_string()),
            SinexError::Database(_) => NodeError::Database(sqlx::Error::Protocol(e.to_string())),
            SinexError::Serialization(_) => NodeError::Processing(e.to_string()),
            SinexError::Io(_) => NodeError::Io(std::io::Error::other(e.to_string())),
            SinexError::Unknown(_) => NodeError::Processing(e.to_string()),
            SinexError::Validation(_) => NodeError::Validation(e.to_string()),
            SinexError::Processing(_) => NodeError::Processing(e.to_string()),
            SinexError::Automaton(_) => NodeError::Automaton(e.to_string()),
            SinexError::Checkpoint(_) => NodeError::Checkpoint(e.to_string()),
            SinexError::Lifecycle(_) => NodeError::Lifecycle(e.to_string()),
            SinexError::Cancelled(_) => NodeError::OperationCancelled(e.to_string()),
            _ => NodeError::Processing(e.to_string()),
        }
    }
}
