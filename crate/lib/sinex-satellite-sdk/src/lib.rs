//! Sinex Satellite SDK
//!
//! Shared library for building Sinex satellite services (event sources and automata).
//!
//! This crate provides:
//! - Common traits and interfaces
//! - gRPC client for communicating with sinex-ingestd
//! - NATS JetStream client for message bus communication (replacing Redis Streams)
//! - Redis Streams client (deprecated, for backwards compatibility)
//! - Configuration management
//! - Lifecycle management and graceful shutdown
//! - State persistence and checkpointing
//! - Historical replay capabilities
//!
//! ## Core Architecture: Deep Symmetry
//!
//! All satellites implement the [`StatefulStreamProcessor`] trait, achieving "deep symmetry"
//! between ingestors and automata. This unified interface enables consistent behavior
//! across all data capture and processing mechanisms.
//!
//! ## Satellite Constellation Architecture
//!
//! Sinex uses a satellite constellation pattern where independent services communicate via
//! gRPC and Redis Streams. Each satellite implements `StatefulStreamProcessor` with a
//! unified interface for consistent behavior across all data capture and processing mechanisms.
//!
//! ```text
//! ┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
//! │   External World    │────▶│    Satellites       │────▶│   Data Substrate    │
//! │                     │     │                     │     │                     │
//! │ • Files             │     │ • fs-watcher        │     │ • core.events       │
//! │ • Terminal          │     │ • terminal          │     │ • source_material   │
//! │ • Desktop           │     │ • desktop           │     │ • knowledge_graph   │
//! │ • System            │     │ • system            │     │ • checkpoints       │
//! └─────────────────────┘     └──────────┬──────────┘     └──────────┬──────────┘
//!                                        │                            │
//!                             ┌──────────▼──────────┐                 │
//!                             │   Redis Streams     │                 │
//!                             │                     │                 │
//!                             │ • Unified hotlog    │◀────────────────┘
//!                             │ • Consumer groups   │
//!                             │ • Event filtering   │
//!                             └──────────┬──────────┘
//!                                        │
//!                             ┌──────────▼──────────┐
//!                             │     Automata        │
//!                             │                     │
//!                             │ • canonicalizer     │
//!                             │ • health-aggregator │
//!                             │ • synthesis engines │
//!                             └─────────────────────┘
//! ```
//!
//! ### Satellite Roles
//!
//! Each satellite can serve one or more roles:
//! - **Ingestor Role**: Capture external data, create raw events
//! - **Automaton Role**: Process events, create synthesis events
//! - **Actuator Role**: Act on instructional events (planned)
//!
//! ### Key Implementation Patterns
//!
//! #### Event Symmetry (Active Inference)
//! Same event types serve as both observations and instructions:
//! ```json
//! // Observation (what happened)
//! {
//!     "source": "ingestor.hyprland",
//!     "event_type": "desktop.workspace.switched",
//!     "payload": { "workspace_id": 3 }
//! }
//!
//! // Instruction (what should happen)
//! {
//!     "source": "user.cli",
//!     "event_type": "desktop.workspace.switched",
//!     "payload": { "workspace_id": 3 }
//! }
//! ```
//!
//! ## Three-Phase Startup Pattern
//!
//! All satellites follow a consistent startup sequence that ensures complete data capture:
//!
//! ### Phase 1: Snapshot
//! - Captures current state of the external system
//! - Uses `TimeHorizon::Snapshot` for instantaneous data capture
//! - Example: Filesystem satellite scans existing files
//!
//! ### Phase 2: Gap-filling (Historical)
//! - Processes events that occurred while offline
//! - Uses `TimeHorizon::Historical { end_time }` for bounded scanning
//! - Only runs if processor supports historical scanning
//! - Ensures no events are lost during service restarts
//!
//! ### Phase 3: Continuous Processing
//! - Real-time event monitoring and streaming
//! - Uses `TimeHorizon::Continuous` for unbounded operation
//! - Continues indefinitely until shutdown
//!
//! ## Archive and Replace Pattern
//!
//! The system never loses data but allows evolution of interpretations:
//! - Original interpretations archived with full audit trail
//! - New interpretations created with updated logic
//! - Complete provenance chain maintained via `source_event_ids`

// Re-export satellite derive macros and utilities
#[cfg(feature = "macros")]
pub use sinex_macros::{
    auto_satellite_metrics, stream_processor, EventHandler, PayloadExtractor, SatelliteConfig,
    SatelliteProcessor,
};

// pub mod automaton; // REMOVED - use StatefulStreamProcessor instead
pub mod annex;
pub mod checkpoint;
pub mod cli;
pub mod config;
pub mod coordination;
pub mod event_processor;
pub mod examples;
pub mod figment_config;
pub mod grpc_client;
pub mod heartbeat;
pub mod lifecycle;
pub mod nats;
pub mod nats_client;
pub mod nats_stream_consumer;
pub mod preflight;
pub mod processor_runner;
pub mod replay;
pub mod stage_as_you_go;
pub mod stream_processor;
pub mod version;

pub use crate::nats_stream_consumer::{
    BatchProcessingResult as NatsBatchProcessingResult,
    EventBatchProcessor as NatsEventBatchProcessor, EventFilter as NatsEventFilter,
    NatsConsumerConfig, NatsStreamConsumer,
};
pub use checkpoint::{CheckpointManager, CheckpointState};
pub use cli::{
    parse_checkpoint, parse_time_horizon, CoverageAnalysis, ExplorationProvider, ExportFormat,
    IngestionHistoryEntry, MissingItem, ProcessorCli, ProcessorCliRunner, ProcessorCommand,
    SourceState,
};
pub use config::{AutomatonConfig, EventSourceConfig, SatelliteConfig};
pub use grpc_client::IngestClient;
pub use heartbeat::{HeartbeatCounterHandle, HeartbeatEmitter, HeartbeatMetrics};
pub use lifecycle::{LifecycleManager, ServiceStatus};
pub use processor_runner::{ProcessorMode, ProcessorRunner, ProcessorRunnerConfig};
pub use replay::ReplayMode;
pub use stream_processor::{
    Checkpoint, EventSender, EventStream, ProcessorCapabilities, ProcessorType, ScanArgs,
    ScanEstimate, ScanReport, StatefulStreamProcessor, StreamProcessorContext,
    StreamProcessorRunner, TimeHorizon,
};

/// Version information for satellite components
#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub git_revision: String,
    pub binary_hash: String,
    pub component_version: String,
}

impl VersionInfo {
    /// Create version info for the current component
    pub fn current(component_name: &str) -> Self {
        Self {
            git_revision: "dev-unknown".to_string(), // Simplified for testing
            binary_hash: format!("hash-{}", component_name), // Simplified for now
            component_version: format!("{}-v1.0.0", component_name),
        }
    }
}

/// Common CLI arguments for satellite services.
///
/// This structure provides standardized command-line arguments that all
/// satellite services can use. It includes common parameters for gRPC
/// communication, batching, and operational modes.
///
/// # Examples
///
/// ```rust
/// use clap::Parser;
/// use sinex_satellite_sdk::SatelliteArgs;
///
/// // Parse from command line
/// let args = SatelliteArgs::parse();
///
/// // Use in service configuration
/// let config = SatelliteConfig {
///     service_name: args.service_name.clone(),
///     ingest_socket_path: args.ingest_socket_path.clone(),
///     dry_run: args.dry_run,
///     // ... other fields
/// };
/// ```
#[derive(clap::Parser, Debug, Clone)]
pub struct SatelliteArgs {
    /// Socket path for ingestd communication.
    ///
    /// Unix Domain Socket path where ingestd listens for gRPC connections.
    /// This socket is used by ingestors to submit events for processing.
    #[arg(long, default_value = "/tmp/sinex-ingestd.sock")]
    pub ingest_socket_path: String,

    /// Service name for identification
    #[arg(long, default_value = "satellite")]
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

// Re-export generated gRPC types
pub mod proto {
    tonic::include_proto!("sinex.ingest");
}

// Re-export commonly used types from dependencies
pub use sinex_core::db::models::RawEvent;
pub use sinex_core::types::error::SinexError;
pub use sinex_core::types::ulid::Ulid;

/// Result type for satellite operations
pub type SatelliteResult<T> = std::result::Result<T, SatelliteError>;

/// Common error types for satellite services.
///
/// This enum provides a unified error handling system for all satellite services.
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
/// use sinex_satellite_sdk::{SatelliteError, SatelliteResult};
///
/// // Recoverable processing error
/// fn process_event(event: &RawEvent) -> SatelliteResult<()> {
///     if event.payload.is_null() {
///         return Err(SatelliteError::Processing(
///             "Event payload cannot be null".to_string()
///         ));
///     }
///     // ... process event
///     Ok(())
/// }
///
/// // Non-recoverable configuration error
/// fn validate_config(config: &Config) -> SatelliteResult<()> {
///     if config.service_name.is_empty() {
///         return Err(SatelliteError::Config(
///             config::ConfigError::MissingField("service_name".to_string())
///         ));
///     }
///     Ok(())
/// }
/// ```
#[derive(thiserror::Error, Debug)]
pub enum SatelliteError {
    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("Configuration parsing error: {0}")]
    Configuration(String),

    #[error("gRPC communication error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("gRPC transport error: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),

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
}

impl From<SatelliteError> for sinex_core::error::SinexError {
    fn from(e: SatelliteError) -> Self {
        match e {
            SatelliteError::Config(_) => {
                sinex_core::error::SinexError::configuration(e.to_string())
            }
            SatelliteError::Configuration(_) => {
                sinex_core::error::SinexError::configuration(e.to_string())
            }
            SatelliteError::Grpc(_) => sinex_core::error::SinexError::unknown(e.to_string()),
            SatelliteError::GrpcTransport(_) => {
                sinex_core::error::SinexError::unknown(e.to_string())
            }
            SatelliteError::Database(_) => sinex_core::error::SinexError::database(e.to_string()),
            SatelliteError::Serialization(_) => {
                sinex_core::error::SinexError::serialization(e.to_string())
            }
            SatelliteError::Io(_) => sinex_core::error::SinexError::io(e.to_string()),
            SatelliteError::General(_) => sinex_core::error::SinexError::unknown(e.to_string()),
            SatelliteError::Processing(_) => sinex_core::error::SinexError::unknown(e.to_string()),
            SatelliteError::Automaton(_) => sinex_core::error::SinexError::unknown(e.to_string()),
            SatelliteError::Checkpoint(_) => sinex_core::error::SinexError::unknown(e.to_string()),
            SatelliteError::Lifecycle(_) => sinex_core::error::SinexError::unknown(e.to_string()),
        }
    }
}

impl From<sinex_core::error::SinexError> for SatelliteError {
    fn from(e: sinex_core::error::SinexError) -> Self {
        match e {
            sinex_core::error::SinexError::Configuration(_) => {
                SatelliteError::Processing(e.to_string())
            }
            sinex_core::error::SinexError::Database(_) => SatelliteError::Processing(e.to_string()),
            sinex_core::error::SinexError::Serialization(_) => {
                SatelliteError::Processing(e.to_string())
            }
            sinex_core::error::SinexError::Io(_) => SatelliteError::Processing(e.to_string()),
            sinex_core::error::SinexError::Unknown(_) => SatelliteError::Processing(e.to_string()),
            sinex_core::error::SinexError::Validation(_) => {
                SatelliteError::Processing(e.to_string())
            }
            _ => SatelliteError::Processing(e.to_string()),
        }
    }
}
