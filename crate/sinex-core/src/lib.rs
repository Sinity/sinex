pub mod channel_enhancements;
pub mod channel_helpers;
pub mod chunking;
pub mod config_extractors;
pub mod config_helpers;
pub mod constants;
pub mod directory_manager;
pub mod error_context;
// event_builders module moved to sinex-events crate
pub mod event_pipeline;
pub mod file_watcher;
pub mod heartbeat;
pub mod json_helpers;
pub mod retry_helpers;
pub mod sqlite_helpers;
pub mod timestamp_helpers;
// strongly_typed_events module moved to sinex-events crate
pub mod validation;
pub mod validation_chains;
pub mod wait_helpers;

pub use channel_enhancements::{
    create_enhanced_event_sender, BatchSendResult, ChannelDiagnostics, ChannelHealthReport,
    DiagnosticsReport, EnhancedEventSender, PerformanceMetrics, PerformanceTracker,
};
pub use channel_helpers::{
    BackpressureManager, // MonitoredEventSender, monitored_channel temporarily removed due to RawEvent move
    ChannelMonitor,
    ChannelReceiverExt,
    ChannelSenderExt,
    ChannelStats,
};
pub use chunking::{ChunkInfo, ChunkingConfig, ChunkingService, ContentChunk};
pub use config_extractors::{parse_duration, ConfigExtractor, ConfigValidator};
pub use config_helpers::{
    CollectorConfig, ConfigExtraction, ConfigFactory, ConfigMerger, DatabaseConfig,
    ObservabilityConfig, SourcesConfig,
};
pub use constants::{buffers, filesystem, limits, retry, timeouts};
pub use directory_manager::{DirectoryConfig, DirectoryManager};
pub use error_context::{ErrorContext, ErrorInfo, ResultExt};
pub use sinex_events::{
    ClipboardEventBuilder, EventFactory, FilesystemEventBuilder, SystemEventBuilder,
    TerminalEventBuilder, WindowManagerEventBuilder,
};
pub use sinex_macros::with_context;
// Re-export strongly typed events from sinex-events crate
pub use sinex_events::{
    typed_event_channel, ClipboardCopiedPayload, ClipboardSelectedPayload, CommandCompletedPayload,
    CommandExecutedPayload, DirCreatedPayload, DirDeletedPayload, EnforcedTypedEventSource,
    EventEnvelope, FileCreatedPayload, FileDeletedPayload, FileModifiedPayload, FileMovedPayload,
    JournalEntryPayload, ProcessHeartbeatPayload, ProcessShutdownPayload, ProcessStartedPayload,
    SessionEndedPayload, SessionStartedPayload, SystemStatePayload,
    TypedEventBuilder, TypedEventError, TypedEventPipelineAdapter, TypedEventReceiver,
    TypedEventResult, TypedEventSender, TypedRawEvent, TypedToJsonAdapter, WindowClosedPayload,
    WindowFocusedPayload, WindowOpenedPayload, WorkspaceSwitchedPayload,
};
// Note: TypedFilesystemEventBuilder, TypedTerminalEventBuilder, TypedClipboardEventBuilder from strongly_typed_events
// have different signatures than the ones in unified_event_source.rs - they are separate builders for different purposes
// Note: TypedSourceAdapter was removed from strongly_typed_events to avoid circular dependency
pub use event_pipeline::{
    DistributionStage, EnrichmentStage, EventPipeline, EventTiming, PipelineConfig,
    PipelineMetrics, PipelineStage, StageMetrics, StageResult, StageTimeouts, StagedEvent,
    StorageStage, ValidationStage,
};
pub use file_watcher::{
    FileChangeEvent, FileChangeKind, FileWatcher, FileWatcherBuilder, FileWatcherConfig,
};
pub use heartbeat::{
    determine_health_status, HealthStatus, MetricsProvider, ProcessHeartbeatEmitter, SystemHealth,
};
pub use json_helpers::{
    extract_field, parse_json, parse_json_file, parse_json_value, to_json_value,
};
pub use retry_helpers::{
    retry_async, retry_simple, retry_with_predicate, RetryBuilder, RetryConfig,
};
pub use sqlite_helpers::{
    QueryResultExt, SqliteConnection, SqliteQueryBuilder, SqliteStatementExt,
};
pub use timestamp_helpers::{
    parse_flexible_timestamp, timestamp_micros_to_datetime, timestamp_millis_to_datetime,
    timestamp_nanos_to_datetime, timestamp_to_datetime, timestamp_with_nanos_to_datetime,
};
pub use validation::{contains_shell_metacharacters, validate_path_within_root};
pub use validation_chains::{JsonType, MultiValidator, ValidationChain};
pub use wait_helpers::{
    wait_for_agent_status, wait_for_condition, wait_for_condition_or_timeout,
    wait_for_database_ready, wait_for_database_ready_with_timeout, wait_for_event_count,
    wait_for_work_queue_count, wait_for_work_queue_empty, wait_for_work_queue_status_count,
    wait_for_worker_status, BackoffHelper,
};

// Re-export constants modules - note: modules are defined later in this file

// Common type aliases for event handling (defined after RawEvent struct)

// Common type aliases for time handling
pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type OptionalTimestamp = Option<chrono::DateTime<chrono::Utc>>;

// Common type aliases for data handling
pub type JsonValue = serde_json::Value;
pub type ConfigValue = toml::Value;

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ===== Database type aliases =====

pub type DbPool = sqlx::PgPool;
pub type DbPoolRef<'a> = &'a sqlx::PgPool;

// ===== Error types (from error.rs) =====

/// Core error types used throughout the system
#[derive(Error, Debug)]
pub enum CoreError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Other error: {0}")]
    Other(String),
}

/// Validation error type
#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Field validation failed: {field} - {message}")]
    Field { field: String, message: String },

    #[error("Schema validation failed: {0}")]
    Schema(String),

    #[error("Business rule validation failed: {0}")]
    BusinessRule(String),

    #[error("Invalid value for field {field}: {message}")]
    InvalidValue { field: String, message: String },

    #[error("Invalid type for field {field}: expected {expected}, got {actual}")]
    InvalidType {
        field: String,
        expected: String,
        actual: String,
    },

    #[error("Schema validation error: {0}")]
    SchemaValidation(String),

    #[error("Missing required field: {field}")]
    MissingField { field: String },
}

impl From<std::io::Error> for CoreError {
    fn from(e: std::io::Error) -> Self {
        CoreError::Io(e.to_string())
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(e: serde_json::Error) -> Self {
        CoreError::Serialization(e.to_string())
    }
}

impl From<sqlx::Error> for CoreError {
    fn from(e: sqlx::Error) -> Self {
        CoreError::Database(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, CoreError>;

// ===== Core data structures (moved to sinex-events) =====

// RawEvent and RawEventBuilder are now defined in sinex-events
pub use sinex_events::{RawEvent, RawEventBuilder};

// ===== Common type aliases for event handling =====

/// Event sender type alias (using RawEvent from sinex-events)
pub type EventSender = tokio::sync::mpsc::Sender<RawEvent>;
/// Event receiver type alias (using RawEvent from sinex-events)
pub type EventReceiver = tokio::sync::mpsc::Receiver<RawEvent>;

// ===== Common types and constants (from types.rs) =====

/// Common event sources
pub mod sources {
    pub const SINEX: &str = "sinex";
    pub const FS: &str = "fs";
    pub const SHELL_KITTY: &str = "shell.kitty";
    pub const SHELL_ATUIN: &str = "shell.atuin";
    pub const SHELL_HISTORY: &str = "shell.history";
    pub const SHELL_BASH_HISTFILE: &str = "shell.bash_histfile";
    pub const SHELL_ZSH_HISTFILE: &str = "shell.zsh_histfile";  
    pub const SHELL_FISH_HISTORY: &str = "shell.fish_history";
    pub const SHELL_RECORDING: &str = "shell.recording";
    pub const SHELL_ASCIINEMA: &str = "shell.asciinema";
    pub const SHELL_SCROLLBACK: &str = "shell.scrollback";
    pub const WM_HYPRLAND: &str = "wm.hyprland";
    pub const CLIPBOARD: &str = "clipboard";
    pub const DBUS: &str = "dbus";
    pub const JOURNALD: &str = "journald";
    pub const UDEV: &str = "udev";
    pub const SYSTEMD: &str = "systemd";
}

/// Common event type constants
pub mod event_type_constants {
    pub mod sinex {
        pub const AUTOMATON_STARTUP: &str = "automaton.startup";
        pub const AUTOMATON_SHUTDOWN: &str = "automaton.shutdown";
        pub const AUTOMATON_HEARTBEAT: &str = "automaton.heartbeat";
        pub const AUTOMATON_ERROR: &str = "automaton.error";
        pub const AUTOMATON_DLQ_EVENT_WRITTEN: &str = "automaton.dlq_event_written";
    }

    pub mod process {
        pub const PROCESS_STARTED: &str = "process.started";
        pub const PROCESS_HEARTBEAT: &str = "process.heartbeat";
        pub const PROCESS_SHUTDOWN: &str = "process.shutdown";
    }

    pub mod filesystem {
        pub const FILE_CREATED: &str = "file.created";
        pub const FILE_MODIFIED: &str = "file.modified";
        pub const FILE_DELETED: &str = "file.deleted";
        pub const FILE_MOVED: &str = "file.moved";
        pub const DIR_CREATED: &str = "dir.created";
        pub const DIR_DELETED: &str = "dir.deleted";
    }

    pub mod shell {
        pub const COMMAND_EXECUTED: &str = "command.executed";
        pub const COMMAND_COMPLETED: &str = "command.completed";
        pub const COMMAND_FAILED: &str = "command.failed";
        pub const SESSION_STARTED: &str = "session.started";
        pub const SESSION_ENDED: &str = "session.ended";
        pub const COMMAND_IMPORTED: &str = "command.imported";
        pub const RECORDING_STARTED: &str = "recording.started";
        pub const RECORDING_ENDED: &str = "recording.ended";
        pub const COMMAND_OUTPUT: &str = "command.output";
        pub const SCROLLBACK_FULL: &str = "scrollback.full";
        pub const TAB_CREATED: &str = "tab.created";
        pub const TAB_FOCUSED: &str = "tab.focused";
        pub const TAB_CLOSED: &str = "tab.closed";
        pub const PROCESS_CHANGED: &str = "process.changed";
        pub const CONFIG_CHANGED: &str = "config.changed";
    }

    pub mod window_manager {
        pub const WINDOW_OPENED: &str = "window.opened";
        pub const WINDOW_CLOSED: &str = "window.closed";
        pub const WINDOW_FOCUSED: &str = "window.focused";
        pub const WINDOW_MOVED: &str = "window.moved";
        pub const WINDOW_RESIZED: &str = "window.resized";
        pub const WORKSPACE_SWITCHED: &str = "workspace.switched";
        pub const WORKSPACE_CREATED: &str = "workspace.created";
        pub const WORKSPACE_DESTROYED: &str = "workspace.destroyed";
        pub const DISPLAY_CONNECTED: &str = "display.connected";
        pub const DISPLAY_DISCONNECTED: &str = "display.disconnected";
        pub const MONITOR_FOCUSED: &str = "monitor.focused";
        pub const STATE_CAPTURED: &str = "state.captured";
    }

    pub mod clipboard {
        pub const COPIED: &str = "clipboard.copied";
        pub const SELECTED: &str = "clipboard.selected";
    }

    pub mod dbus {
        pub const SIGNAL_RECEIVED: &str = "signal.received";
        pub const METHOD_CALLED: &str = "method.called";
        pub const NOTIFICATION_SENT: &str = "notification.sent";
        pub const DEVICE_CONNECTED: &str = "device.connected";
        pub const DEVICE_DISCONNECTED: &str = "device.disconnected";
        pub const MEDIA_STATE_CHANGED: &str = "media.state_changed";
        pub const POWER_STATE_CHANGED: &str = "power.state_changed";
        pub const NETWORK_STATE_CHANGED: &str = "network.state_changed";
        pub const BLUETOOTH_DEVICE_CHANGED: &str = "bluetooth.device_changed";
        pub const MOUNT_CHANGED: &str = "mount.changed";
        pub const SESSION_STATE_CHANGED: &str = "session.state_changed";
        pub const SECURITY_AUTHORIZATION: &str = "security.authorization";
        pub const SCREENSAVER_STATE_CHANGED: &str = "screensaver.state_changed";
    }

    pub mod journald {
        pub const ENTRY_WRITTEN: &str = "entry.written";
        pub const SYNC_COMPLETED: &str = "sync.completed";
    }
}

/// Agent status for heartbeats
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AgentStatus {
    Starting,
    Running,
    Stopping,
    Error,
}

/// Error severity levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ErrorSeverity {
    Warning,
    Error,
    Critical,
}
