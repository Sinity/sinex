//! # Sinex Runtime Support
//!
//! This module provides the current authoring and runtime surface for Sinex
//! source contracts, automata, transport, checkpointing, content storage, and
//! source-material staging. The `runtime` path is historical and should not be
//! read as a separate crate or deployment boundary.
//!
//! # Clock Skew Considerations
//!
//! Event ordering relies on `UUIDv7` timestamps. Clock skew between capture
//! and processing tasks can cause:
//! - Out-of-order event processing
//! - Checkpoint confusion (newer events appear older)
//! - False timeout detections in confirmation handler
//!
//! ## Mitigations
//! - Use NTP/chrony for time synchronization on capture hosts
//! - Use deterministic ID helpers for source-occurrence and derived-event IDs; DB defaults remain for DB-owned rows
//! - Monitor clock skew via confirmation handler warnings (see `confirmation_handler.rs`)
//! - Set conservative confirmation timeouts (>5 seconds)
//! - For critical ordering, use database sequences instead of client-side `UUIDv7` IDs

pub mod acquisition_manager;
pub mod automaton_base;
pub mod batch_importer;
pub mod checkpoint;
pub mod config;
pub mod confirmation_handler;
pub mod content_store;
pub mod coordination;
pub mod diagnostics {
    pub mod regression;
}
pub mod dlq_retry;

pub mod automaton;
pub mod error_helpers;
pub mod event_transport;
pub mod examples;
pub mod exploration;
pub mod file_tailer;
pub mod health_reporter;
pub mod heartbeat;
pub mod hyprland;
pub mod ingestion_helpers;
pub mod input_shapes;
pub mod jetstream_consumer;
pub mod material;
pub mod nats_publisher;
pub mod parser;
pub mod preflight;
pub mod prelude;
pub mod pressure;
pub mod processing;
pub mod runtime_cli;
pub mod schema_validator;
pub mod self_observation;
pub mod service_runtime;
pub mod shutdown;
pub mod source_driver;
pub mod source_material;
pub mod sqlite_source;
pub mod stage_as_you_go;
pub mod stream;
pub mod supervised_watcher;
pub mod systemd_notify;
pub mod tags;
pub mod version;
pub mod watcher_handle;

pub use acquisition_manager::{
    AcquisitionManager, AppendStreamAcquirer, RotationPolicy, SOURCE_MATERIAL_BEGIN_SUBJECT,
    SOURCE_MATERIAL_END_SUBJECT, SOURCE_MATERIAL_FRAMES_SUBJECT,
    SOURCE_MATERIAL_SLICE_SUBJECT_PREFIX, SOURCE_MATERIAL_STREAM, SourceMaterialHandle,
    SourceRecordAnchor, source_material_slice_subject,
};
pub use automaton::{
    AutomatonAdapterConfig, AutomatonContext, AutomatonRuntime, DerivedAggregationMeta,
    DerivedOutput, DerivedScopeInvalidation, INVALIDATION_SUBJECT, InputProvenanceFilter,
    MultiOutputTransducer, MultiOutputTransducerAdapter, MultiOutputTransducerWrapper,
    ScopeReconciler, ScopeReconcilerAdapter, ScopeReconcilerWrapper, Transducer, TransducerAdapter,
    TransducerWrapper, Windowed, WindowedAdapter, WindowedWrapper,
};
pub use automaton_base::{ActivityEntry, IngestionHistoryEntry};
pub use batch_importer::{
    BatchImporterState, DiscoveredFile, ImportFileChangeKind, ImportedFileFingerprint,
    ImportedFileState, ScanError, read_file_content, read_file_lines, scan_for_new_files,
};
pub use checkpoint::{
    CheckpointCleanupConfig, CheckpointCleanupResult, CheckpointManager, CheckpointState,
    cleanup_stale_checkpoints, spawn_checkpoint_cleanup_task,
};
pub use config::{
    AutomatonConfig, EventSourceConfig, MaterialMetadataPolicy, PathClassRule, RuntimeConfig,
};
pub use confirmation_handler::{
    ConfirmationBuffer, ConfirmedEventHandler, EventConfirmation, ProcessingModel,
    ProvisionalEvent, ProvisionalEventHandler,
};
pub use coordination::{HandoffRequest, InstanceMode, RuntimeCoordination};
pub use dlq_retry::{DlqRetryConfig, DlqRetryHandler, DlqRetryResult, DlqStats};
pub use event_transport::{EventBatcher, EventBatcherConfig, EventTransport, spawn_event_batcher};
pub use exploration::{ExplorationProvider, ExportFormat, SourceState};
pub use file_tailer::{
    AppendOnlyFileChange, AppendOnlyFileLine, AppendOnlyFilePollResult, AppendOnlyFileState,
    TailError, poll_utf8_lines,
};
pub use health_reporter::{EmitTracker, HealthMetrics, HealthReporter, HealthThresholds};
pub use heartbeat::{HeartbeatCounterHandle, HeartbeatEmitter, HeartbeatLogSink, HeartbeatMetrics};
pub use hyprland::{
    HyprlandCommandSocketProbe, HyprlandCommandSocketResponse, dispatch_hyprland_workspace_command,
    probe_hyprland_command_socket, resolve_hyprland_command_socket_path,
};
pub use input_shapes::{
    SqliteSnapshotCheckpointState, SqliteSourceCheckpointState, discover_importable_files_at_root,
};
pub use jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig};
pub use material::{
    ObservationMaterializer, RetryableMaterialCapture, StreamMaterialContext,
    TransientErrorPredicate,
};
pub use nats_publisher::NatsPublisher;
pub use pressure::PressureMonitor;
pub use processing::AutomatonLogicError;
pub use runtime_cli::{
    RuntimeCli, RuntimeCliRunner, RuntimeCommand, parse_checkpoint, parse_time_horizon,
};
pub use self_observation::{
    SelfObservationError, SelfObservationTask, SelfObserver, SelfObserverConfig,
};
pub use shutdown::wait_for_os_shutdown_signal;
pub use shutdown::wait_for_shutdown_signal;
pub use shutdown::wait_for_shutdown_signal_bool;
pub use shutdown::{ShutdownConfig, default_checkpoint_path};
pub use source_driver::{SourceDriver, SourceDriverRuntime, SourceDriverState};
pub use source_material::{
    stage_material, stage_material_from_file, stage_material_from_file_bounded,
};
pub use sqlite_source::{
    SqliteSnapshotCapture, SqliteSnapshotError, SqliteSnapshotEvidenceReport,
    SqliteSnapshotMetadata, SqliteSnapshotPolicy, SqliteSnapshotState, SqliteSnapshotTrigger,
    SqliteTableCheckError, capture_sqlite_snapshot, ensure_sqlite_with_tables,
    max_row_id_for_query, read_rows_after, read_rows_with_params,
};
pub use stream::{
    Checkpoint, ContinuousStart, EventSender, EventStream, MaterialReplayContext, ModuleKind,
    ReplayScopeFilters, ResolvedReplayMaterial, RunnerLifecycle, RuntimeCapabilities,
    RuntimeModule, RuntimeRunner, ScanArgs, ScanEstimate, ScanReport, SourceScanAck,
    SourceScanCommand, SourceScanProgress, TimeHorizon,
};
pub use supervised_watcher::{
    SupervisedWatcherConfig, spawn_supervised_watcher, spawn_watcher_with_panic_catch,
};
pub use systemd_notify::{notify_ready, notify_stopping, spawn_watchdog, stop_watchdog};
pub use version::{RuntimeInstance, RuntimeVersion};
pub use watcher_handle::{WatcherHandle, WatcherHealth, WatcherState};

// Re-export preflight utilities
pub use content_store::{
    BlobMetadata, ContentBackend, ContentStoreConfig, ContentStoreKey, ContentStoreManager,
    MaterialContentStore,
};
pub use preflight::{VerificationStatus, verify_service_dependencies};

// ApiCursor adapter — paginated REST import support (#1746).
pub use parser::{
    ApiClient, ApiFetchError, ApiFetchPage, ApiCursorAdapter, ApiCursorConfig, ApiCursorPosition,
    RetryPolicy,
};

// IncrementalDump adapter — periodic full-export superset dumps (#1774).
pub use parser::{
    DumpLoader, IncrementalDumpAdapter, IncrementalDumpConfig, IncrementalDumpCursor,
    IncrementalDumpError, IncrementalDumpPosition,
};

// Re-export commonly used types from dependencies
pub use sinex_primitives::error::{ErrorDetails, SinexError};
pub use sinex_primitives::temporal::Timestamp;
pub use uuid::Uuid;

/// Result type for runtime operations
pub type RuntimeResult<T> = std::result::Result<T, SinexError>;
