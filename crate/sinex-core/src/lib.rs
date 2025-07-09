pub mod channel_helpers;
pub mod channel_enhancements;
pub mod chunking;
pub mod config_extractors;
pub mod config_helpers;
pub mod constants;
pub mod directory_manager;
pub mod error_context;
pub mod event;
pub mod event_builders;
pub mod event_registry_macro;
pub mod event_source_base;
pub mod event_source_context;
pub mod file_watcher;
pub mod heartbeat;
pub mod json_helpers;
pub mod retry_helpers;
pub mod sqlite_helpers;
pub mod timestamp_helpers;
pub mod unified_collector;
pub mod unified_event_source;
pub mod strongly_typed_events;
pub mod validation;
pub mod validation_chains;
pub mod wait_helpers;

pub use channel_helpers::{
    BackpressureManager, // MonitoredEventSender, monitored_channel temporarily removed due to RawEvent move
    ChannelMonitor,
    ChannelReceiverExt,
    ChannelSenderExt,
    ChannelStats,
};
pub use channel_enhancements::{
    EnhancedEventSender, PerformanceTracker, PerformanceMetrics, 
    BatchSendResult, ChannelHealthReport, ChannelDiagnostics, 
    DiagnosticsReport, create_enhanced_event_sender
};
pub use chunking::{
    ChunkingConfig, ChunkingService, ContentChunk, ChunkInfo,
};
pub use config_extractors::{parse_duration, ConfigExtractor, ConfigValidator};
pub use config_helpers::{
    ConfigFactory, ConfigExtraction, ConfigMerger, 
    DatabaseConfig, CollectorConfig, ObservabilityConfig, SourcesConfig
};
pub use constants::{timeouts, limits, buffers, retry, filesystem};
pub use directory_manager::{DirectoryManager, DirectoryConfig};
pub use error_context::{ErrorContext, ErrorInfo, ResultExt};
pub use event_builders::{EventFactory, FilesystemEventBuilder, TerminalEventBuilder, ClipboardEventBuilder, WindowManagerEventBuilder, SystemEventBuilder};
pub use event_source_base::EventSourceBase;
pub use event_source_context::EventSourceContext;
pub use file_watcher::{
    FileWatcher, FileWatcherBuilder, FileWatcherConfig, FileChangeEvent, FileChangeKind,
};
pub use heartbeat::{
    ComponentHeartbeat, HealthStatus, HeartbeatEmitter, MetricsProvider, SystemHealth,
};
pub use json_helpers::{
    parse_json, parse_json_file, parse_json_value, extract_field, to_json_value,
};
pub use retry_helpers::{
    retry_async, retry_simple, retry_with_predicate, RetryConfig, RetryBuilder,
};
pub use sqlite_helpers::{
    SqliteConnection, SqliteStatementExt, SqliteQueryBuilder, QueryResultExt,
};
pub use timestamp_helpers::{
    timestamp_to_datetime, timestamp_with_nanos_to_datetime, timestamp_millis_to_datetime,
    timestamp_micros_to_datetime, timestamp_nanos_to_datetime, parse_flexible_timestamp,
};
pub use unified_collector::{EventOutput, EventSource, EventType};
pub use unified_event_source::{UnifiedEventSource, TypedFilesystemEventBuilder, TypedTerminalEventBuilder, TypedClipboardEventBuilder};
pub use strongly_typed_events::{
    EventEnvelope, TypedRawEvent, TypedEventBuilder, TypedEventSender, TypedEventReceiver, typed_event_channel,
    FileCreatedPayload, FileModifiedPayload, FileDeletedPayload, FileMovedPayload, DirCreatedPayload, DirDeletedPayload,
    CommandExecutedPayload, CommandCompletedPayload, SessionStartedPayload, SessionEndedPayload,
    ClipboardCopiedPayload, ClipboardSelectedPayload, WindowOpenedPayload, WindowClosedPayload,
    WindowFocusedPayload, WorkspaceSwitchedPayload, JournalEntryPayload, SystemStatePayload
};
pub use validation_chains::{JsonType, MultiValidator, ValidationChain};
pub use validation::{validate_path_within_root, contains_shell_metacharacters};
pub use wait_helpers::{
    wait_for_database_ready, wait_for_database_ready_with_timeout, wait_for_event_count,
    wait_for_worker_status, wait_for_work_queue_count, wait_for_work_queue_status_count,
    wait_for_work_queue_empty, wait_for_agent_status, wait_for_condition, 
    wait_for_condition_or_timeout, BackoffHelper
};

// Common type aliases for event handling (defined after RawEvent struct)

// Common type aliases for time handling
pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type OptionalTimestamp = Option<chrono::DateTime<chrono::Utc>>;

// Common type aliases for data handling
pub type JsonValue = serde_json::Value;
pub type ConfigValue = toml::Value;

use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
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

// ===== Core data structures =====

/// Raw event structure
///
/// This is the canonical event structure used throughout the system.
/// NOTE: This struct uses ULID directly. When using with SQLX queries,
/// use type overrides like: `id::uuid as "id: _"` for proper type inference
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RawEvent {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: Timestamp,
    pub ts_orig: OptionalTimestamp,
    pub host: String,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub payload: JsonValue,
}

impl RawEvent {
    /// Extract ingestion timestamp from ULID (convenience method)
    pub fn ts_ingest_from_ulid(&self) -> Timestamp {
        self.id.timestamp()
    }
}

/// Builder for creating RawEvent instances
pub struct RawEventBuilder {
    source: String,
    event_type: String,
    payload: JsonValue,
    ts_orig: OptionalTimestamp,
    host: Option<String>,
    ingestor_version: Option<String>,
    payload_schema_id: Option<Ulid>,
}

impl RawEventBuilder {
    pub fn new(
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: JsonValue,
    ) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: None,
            host: None,
            ingestor_version: None,
            payload_schema_id: None,
        }
    }

    pub fn with_orig_timestamp(mut self, ts: Timestamp) -> Self {
        self.ts_orig = Some(ts);
        self
    }

    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn with_ingestor_version(mut self, version: impl Into<String>) -> Self {
        self.ingestor_version = Some(version.into());
        self
    }

    pub fn with_payload_schema_id(mut self, id: Ulid) -> Self {
        self.payload_schema_id = Some(id);
        self
    }

    pub fn build(self) -> RawEvent {
        let id = Ulid::new();
        let hostname = self
            .host
            .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().to_string());

        RawEvent {
            id,
            source: self.source,
            event_type: self.event_type,
            ts_ingest: chrono::Utc::now(),
            ts_orig: self.ts_orig,
            host: hostname,
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id,
            payload: self.payload,
        }
    }
}

// ===== Common type aliases for event handling =====

/// Event sender type alias (now that RawEvent is defined)
pub type EventSender = tokio::sync::mpsc::Sender<RawEvent>;
/// Event receiver type alias (now that RawEvent is defined)
pub type EventReceiver = tokio::sync::mpsc::Receiver<RawEvent>;

// ===== Common types and constants (from types.rs) =====

/// Common event sources
pub mod sources {
    pub const SINEX: &str = "sinex";
    pub const FS: &str = "fs";
    pub const SHELL_KITTY: &str = "shell.kitty";
    pub const SHELL_ATUIN: &str = "shell.atuin";
    pub const SHELL_HISTORY: &str = "shell.history";
    pub const SHELL_RECORDING: &str = "shell.recording";
    pub const SHELL_SCROLLBACK: &str = "shell.scrollback";
    pub const WM_HYPRLAND: &str = "wm.hyprland";
    pub const CLIPBOARD: &str = "clipboard";
    pub const DBUS: &str = "dbus";
    pub const JOURNALD: &str = "journald";
    
}

/// Common event type constants
pub mod event_type_constants {
    pub mod sinex {
        pub const AGENT_STARTUP: &str = "agent.startup";
        pub const AGENT_SHUTDOWN: &str = "agent.shutdown";
        pub const AGENT_HEARTBEAT: &str = "agent.heartbeat";
        pub const AGENT_ERROR: &str = "agent.error";
        pub const AGENT_DLQ_EVENT_WRITTEN: &str = "agent.dlq_event_written";
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
