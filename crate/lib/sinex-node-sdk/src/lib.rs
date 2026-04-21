#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../docs/coordination.md")]
#![doc = include_str!("../docs/stage_as_you_go.md")]
#![doc = include_str!("../docs/stream_runtime.md")]

//! # Sinex Node SDK
//!
//! The Sinex Node SDK provides the core abstractions and runtime for building
//! ingestors and derived nodes in the Sinex ecosystem.
//!
//! ## Core Concepts
//!
//! ### Shared Runtime Surface
//! The low-level [`Node`] trait and runtime support point-in-time snapshots,
//! historical catch-up, and continuous real-time processing.
//!
//! ### High-Level Node Traits
//! The SDK provides higher-level traits like [`TransducerNode`],
//! [`WindowedNode`], [`ScopeReconcilerNode`], and [`IngestorNode`] that automate:
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
//! # Clock Skew Considerations
//!
//! Event ordering relies on `UUIDv7` timestamps. Clock skew between nodes can cause:
//! - Out-of-order event processing
//! - Checkpoint confusion (newer events appear older)
//! - False timeout detections in confirmation handler
//!
//! ## Mitigations
//! - Use NTP/chrony for time synchronization across all nodes
//! - Prefer DB-generated `UUIDv7` IDs where possible (via `DEFAULT uuidv7()`)
//! - Monitor clock skew via confirmation handler warnings (see `confirmation_handler.rs`)
//! - Set conservative confirmation timeouts (>5 seconds)
//! - For critical ordering, use database sequences instead of client-side `UUIDv7` IDs

#[cfg(feature = "messaging")]
pub mod acquisition_manager;
#[cfg(feature = "db")]
pub mod annex;
pub mod api_poller;
#[cfg(feature = "messaging")]
pub mod automaton_base;
pub mod batch_importer;
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
pub mod derived_node;
#[cfg(feature = "messaging")]
pub mod error_helpers;
#[cfg(feature = "messaging")]
pub mod event_node;
#[cfg(feature = "messaging")]
pub mod examples;
#[cfg(feature = "messaging")]
pub mod exploration;
pub mod file_tailer;
#[cfg(feature = "messaging")]
pub mod health_reporter;
#[cfg(feature = "messaging")]
pub mod heartbeat;
pub mod ingestion_helpers;
#[cfg(feature = "messaging")]
pub mod ingestor_node;
pub mod input_shapes;
#[cfg(feature = "messaging")]
pub mod jetstream_consumer;
#[cfg(feature = "messaging")]
pub mod nats_publisher;
#[cfg(all(feature = "db", feature = "messaging"))]
pub mod node_cli;
#[cfg(feature = "preflight")]
pub mod preflight;
pub mod prelude;
pub mod processing;
#[cfg(feature = "messaging")]
pub mod runtime;
#[cfg(feature = "messaging")]
pub mod schema_validator;
#[cfg(feature = "messaging")]
pub mod self_observation;
pub mod shutdown;
pub mod source_material;
pub mod sqlite_source;
#[cfg(feature = "messaging")]
pub mod stage_as_you_go;
#[cfg(feature = "messaging")]
pub mod systemd_notify;
pub mod version;
#[cfg(feature = "messaging")]
pub mod watcher_handle;

#[cfg(feature = "messaging")]
pub use acquisition_manager::{
    AcquisitionManager, AppendStreamAcquirer, RotationPolicy, SourceMaterialHandle,
    SourceRecordAnchor,
};
#[cfg(feature = "messaging")]
pub use automaton_base::{ActivityEntry, IngestionHistoryEntry};
pub use batch_importer::{
    BatchImporterState, DiscoveredFile, ImportFileChangeKind, ImportedFileFingerprint,
    ImportedFileState, ScanError, read_file_content, read_file_lines, scan_for_new_files,
};
#[cfg(feature = "messaging")]
pub use checkpoint::{
    CheckpointCleanupConfig, CheckpointCleanupResult, CheckpointManager, CheckpointState,
    cleanup_stale_checkpoints, spawn_checkpoint_cleanup_task,
};
pub use config::{AutomatonConfig, EventSourceConfig, NodeConfig};
pub use confirmation_handler::{
    ConfirmationBuffer, ConfirmedEventHandler, DEFAULT_MAX_PENDING_EVENTS, EventConfirmation,
    ProcessingModel, ProvisionalEvent, ProvisionalEventHandler,
};
#[cfg(feature = "messaging")]
pub use coordination::{HandoffRequest, InstanceMode, NodeCoordination};
#[cfg(feature = "messaging")]
pub use derived_node::{
    DerivedAggregationMeta, DerivedNodeAdapter, DerivedNodeConfig, DerivedOutput,
    DerivedScopeInvalidation, DerivedTriggerContext, INVALIDATION_SUBJECT, InputProvenanceFilter,
    ScopeReconcilerNode, ScopeReconcilerNodeAdapter, TransducerNode, TransducerNodeAdapter,
    WindowedNode, WindowedNodeAdapter,
};
#[cfg(feature = "messaging")]
pub use dlq_retry::{DlqRetryConfig, DlqRetryHandler, DlqRetryResult, DlqStats};
#[cfg(feature = "messaging")]
pub use event_node::{EventBatcher, EventBatcherConfig, EventTransport, spawn_event_batcher};
#[cfg(feature = "messaging")]
pub use exploration::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, MissingItem, SourceState,
};
pub use file_tailer::{
    AppendOnlyFileChange, AppendOnlyFilePollResult, AppendOnlyFileState, TailError, poll_utf8_lines,
};
#[cfg(feature = "messaging")]
pub use health_reporter::{HealthMetrics, HealthReporter, HealthThresholds};
#[cfg(feature = "messaging")]
pub use heartbeat::{HeartbeatCounterHandle, HeartbeatEmitter, HeartbeatLogSink, HeartbeatMetrics};
#[cfg(feature = "messaging")]
pub use ingestor_node::{IngestorNode, IngestorNodeAdapter, IngestorState};
pub use input_shapes::{
    SqliteSourceCheckpointState, checkpointed_sqlite_history_lenient,
    checkpointed_sqlite_history_strict, checkpointed_sqlite_source_lenient,
    checkpointed_sqlite_source_strict, discover_importable_files_at_root,
    poll_append_only_utf8_source,
};
#[cfg(feature = "messaging")]
pub use jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig};
#[cfg(feature = "messaging")]
pub use nats_publisher::NatsPublisher;
#[cfg(all(feature = "db", feature = "messaging"))]
pub use node_cli::{NodeCli, NodeCliRunner, NodeCommand, parse_checkpoint, parse_time_horizon};
pub use processing::{ErrorAction, NodeLogicError};
#[cfg(feature = "messaging")]
pub use runtime::stream::{
    Checkpoint, EventSender, EventStream, MaterialReplayContext, Node, NodeCapabilities,
    NodeRunner, NodeScanAck, NodeScanCommand, NodeScanProgress, NodeType, ReplayScopeFilters,
    ResolvedReplayMaterial, RunnerLifecycle, ScanArgs, ScanEstimate, ScanReport, TimeHorizon,
};
#[cfg(feature = "messaging")]
pub use self_observation::{
    SelfObservationError, SelfObservationTask, SelfObserver, SelfObserverConfig,
};
#[cfg(feature = "messaging")]
pub use shutdown::wait_for_shutdown_signal;
pub use shutdown::{ShutdownConfig, default_checkpoint_path};
#[cfg(feature = "messaging")]
pub use source_material::{stage_material, stage_material_from_file};
pub use sqlite_source::{
    SqliteHistoryImportError, SqliteHistoryImportReport, SqliteHistoryRowOutcome,
    SqliteHistoryWarningDisposition, SqliteTableCheckError, ensure_sqlite_with_tables,
    import_sqlite_history_lenient, import_sqlite_history_strict, max_row_id_for_query,
    read_rows_after, read_rows_with_params,
};
#[cfg(feature = "messaging")]
pub use systemd_notify::{notify_ready, notify_stopping, spawn_watchdog, stop_watchdog};
pub use version::{NodeInstance, NodeVersion};
#[cfg(feature = "messaging")]
pub use watcher_handle::{WatcherHandle, WatcherHealth, WatcherState};

// Re-export commonly used annex types

// Re-export preflight utilities
#[cfg(feature = "db")]
pub use annex::{AnnexConfig, AnnexKey, BlobManager, BlobMetadata, GitAnnex};
#[cfg(feature = "preflight")]
pub use preflight::{VerificationStatus, verify_service_dependencies};

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
    #[must_use]
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
            component_version: format!("{component_name}-v{version}"),
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
pub use uuid::Uuid;

/// Result type for node operations
pub type NodeResult<T> = std::result::Result<T, SinexError>;
