#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]
#![doc = include_str!("../docs/coordination.md")]
#![doc = include_str!("../docs/stage_as_you_go.md")]
#![doc = include_str!("../docs/stream_runtime.md")]

//! # Sinex Node SDK
//!
//! The Sinex Node SDK provides the core abstractions and runtime for building nodes in the Sinex ecosystem.
//! It implements a **Unified Node Architecture**, where both **Ingestors** (data capturers) and
//! **Automata** (data processors) are unified as stateful stream processors.
//!
//! ## Core Concepts
//!
//! ### Unified Node Interface
//! All nodes implement the [`Node`] trait, which provides a single interface for point-in-time snapshots,
//! historical gap-filling, and continuous real-time processing.
//!
//! ### Gen2 Patterns
//! The SDK provides high-level traits like [`SimpleNode`] and [`SimpleIngestor`] that automate:
//! - **State Persistence**: Automatic checkpointing to NATS KV and local backup files.
//! - **Hot Reload**: Fast state restoration from local files during development rebuilds.
//! - **Graceful Lifecycle**: Cooperative shutdown patterns via [`WatcherHandle`] and `CancellationToken`.
//! - **Health Monitoring**: Automatic error-rate tracking and status emission.
//!
//! ### Data Integrity & Provenance
//! - **Single-Writer Pattern**: Nodes submit provisional events to NATS; `sinex-ingestd` ensures durable database persistence.
//! - **Dual-Hash Verification**: Large files managed by the [`annex`] subsystem are verified using both BLAKE3 and SHA256.
//! - **Lineage Tracking**: Automatic synthesis provenance links derived events to their source.
//!
//! ### Distributed Coordination
//! High-level primitives for:
//! - **Leadership Election**: Ensuring singleton execution of stateful automata.
//! - **Graceful Handoff**: Zero-downtime version upgrades.
//! - **Work Tracking**: Ensuring in-flight operations complete before shutdown.
//!
//! # Clock Skew Considerations (Issue 7)
//!
//! Event ordering relies on ULID timestamps. Clock skew between nodes can cause:
//! - Out-of-order event processing
//! - Checkpoint confusion (newer events appear older)
//! - False timeout detections in confirmation handler
//!
//! ## Mitigations
//! - Use NTP/chrony for time synchronization across all nodes
//! - Prefer DB-generated ULIDs where possible (via `DEFAULT gen_ulid()`)
//! - Monitor clock skew via confirmation handler warnings (see `confirmation_handler.rs`)
//! - Set conservative confirmation timeouts (>5 seconds)
//! - For critical ordering, use database sequences instead of client-side ULIDs

// Macro re-exports removed; prefer explicit imports from `sinex-macros` if needed.
#[cfg(feature = "messaging")]
pub mod acquisition_manager;
#[cfg(feature = "db")]
pub mod annex;
#[cfg(feature = "messaging")]
pub mod automaton_base;
#[cfg(feature = "messaging")]
pub mod automaton_event_handler;
#[cfg(feature = "messaging")]
pub mod checkpoint;
pub mod config;
pub mod confirmation_handler;
#[cfg(feature = "messaging")]
pub mod coordination;
pub mod diagnostics;
#[cfg(feature = "messaging")]
pub mod dlq_retry;

#[cfg(feature = "messaging")]
pub mod error_helpers;
#[cfg(feature = "messaging")]
pub mod event_node;
#[cfg(feature = "messaging")]
pub mod examples;
#[cfg(feature = "messaging")]
pub mod exploration;
#[cfg(feature = "messaging")]
pub mod health_reporter;
#[cfg(feature = "messaging")]
pub mod heartbeat;
pub mod ingestion_helpers;
#[cfg(feature = "messaging")]
pub mod jetstream_consumer;
#[cfg(feature = "messaging")]
pub mod lifecycle;
#[cfg(feature = "messaging")]
pub mod nats_publisher;
#[cfg(feature = "preflight")]
pub mod preflight;
pub mod prelude;
#[cfg(all(feature = "db", feature = "messaging"))]
pub mod replay;
#[cfg(feature = "messaging")]
pub mod runtime;
#[cfg(feature = "messaging")]
pub mod schema_validator;
#[cfg(feature = "messaging")]
pub mod self_observation;
pub mod shutdown;
#[cfg(feature = "messaging")]
pub mod simple_ingestor;
#[cfg(feature = "messaging")]
pub mod simple_node;
#[cfg(feature = "messaging")]
pub mod stage_as_you_go;
#[cfg(feature = "messaging")]
pub mod stream_processor {
    pub use crate::runtime::stream::*;
}
pub mod version;
#[cfg(feature = "messaging")]
pub mod watcher_handle;

#[cfg(feature = "messaging")]
pub use acquisition_manager::{
    AcquisitionManager, AppendStreamAcquirer, RotationPolicy, SourceMaterialHandle,
};
#[cfg(feature = "messaging")]
pub use automaton_base::{
    ActivityEntry, AutomatonFields, AutomatonStats, ChannelConfirmedEventHandler,
    IngestionHistoryEntry, DEFAULT_CHANNEL_CAPACITY, DEFAULT_MAX_HISTORY_ENTRIES,
};
#[cfg(feature = "messaging")]
pub use automaton_event_handler::AutomatonEventHandler;
#[cfg(feature = "messaging")]
pub use checkpoint::{
    cleanup_stale_checkpoints, spawn_checkpoint_cleanup_task, CheckpointCleanupConfig,
    CheckpointCleanupResult, CheckpointManager, CheckpointState,
};
pub use config::{AutomatonConfig, EventSourceConfig, NodeConfig};
pub use confirmation_handler::{
    ConfirmationBuffer, ConfirmedEventHandler, EventConfirmation, ProcessingModel,
    ProvisionalEvent, ProvisionalEventHandler, DEFAULT_MAX_PENDING_EVENTS,
};
#[cfg(feature = "messaging")]
pub use coordination::{HandoffRequest, InstanceMode, NodeCoordination};
#[cfg(feature = "messaging")]
pub use dlq_retry::{DlqRetryConfig, DlqRetryHandler, DlqStats};
#[cfg(feature = "messaging")]
pub use exploration::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, MissingItem, SourceState,
};
#[cfg(feature = "messaging")]
pub use health_reporter::{HealthMetrics, HealthReporter, HealthThresholds};
#[cfg(feature = "messaging")]
pub use heartbeat::{HeartbeatCounterHandle, HeartbeatEmitter, HeartbeatLogSink, HeartbeatMetrics};
#[cfg(feature = "messaging")]
pub use jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig};

#[cfg(feature = "messaging")]
pub use event_node::{spawn_event_processor, EventBatcher, EventBatcherConfig, EventTransport};
#[cfg(feature = "messaging")]
pub use lifecycle::{LifecycleManager, ServiceStatus};
#[cfg(feature = "messaging")]
pub use nats_publisher::NatsPublisher;
#[cfg(all(feature = "db", feature = "messaging"))]
pub use replay::{
    MetricsSnapshot, ProgressTracker, ReplayController, ReplayFilters, ReplayMetrics, ReplayMode,
    ReplayProgress, ReplayResult, ReplayService, ReplayStats,
};
#[cfg(feature = "messaging")]
pub use self_observation::{
    SelfObservationError, SelfObservationTask, SelfObserver, SelfObserverConfig,
};
pub use shutdown::{default_checkpoint_path, ShutdownConfig, ShutdownHandler, ShutdownSignal};
#[cfg(feature = "messaging")]
pub use simple_ingestor::{IngestorState, SimpleIngestor, SimpleIngestorWrapper};
#[cfg(feature = "messaging")]
pub use simple_node::{
    ErrorAction, PersistedState, SimpleNode, SimpleNodeConfig, SimpleNodeError, SimpleNodeWrapper,
};
#[cfg(feature = "messaging")]
pub use stream_processor::{
    Checkpoint, EventSender, EventStream, Node, NodeCapabilities, NodeRunner, NodeType,
    RunnerLifecycle, ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
};
pub use version::{NodeInstance, NodeVersion};
#[cfg(feature = "messaging")]
pub use watcher_handle::{WatcherHandle, WatcherHealth, WatcherState};

// Re-export commonly used annex types

// Re-export preflight utilities
#[cfg(feature = "db")]
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
    ///
    /// Uses shadow-rs build constants for git revision and version information.
    pub fn current(component_name: &str) -> Self {
        use version::{node_commit_hash, node_version};

        let version = node_version()
            .map_or_else(|_| env!("CARGO_PKG_VERSION").to_string(), |v| v.to_string());
        let git_revision = node_commit_hash();
        // For binary_hash, use commit hash as a proxy (same as git revision)
        let binary_hash = git_revision.clone();

        Self {
            git_revision,
            binary_hash,
            component_version: format!("{}-v{}", component_name, version),
        }
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
#[cfg(feature = "messaging")]
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
pub use sinex_primitives::error::{ErrorDetails, SinexError};
pub use sinex_primitives::temporal::Timestamp;
pub use sinex_primitives::Ulid;

/// Result type for node operations
pub type NodeResult<T> = std::result::Result<T, SinexError>;

#[cfg(test)]
mod version_info_tests {
    use super::VersionInfo;
    use xtask::sandbox::sinex_test;

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
