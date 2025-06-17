pub mod event;
pub mod event_source_context;
pub mod heartbeat;
pub mod unified_collector;
pub mod validation;

pub use event::{RawEvent, RawEventBuilder};
pub use event_source_context::EventSourceContext;
pub use heartbeat::{ComponentHeartbeat, HealthStatus, HeartbeatEmitter, SystemHealth};
pub use unified_collector::{EventType, EventSource, EventRegistry, EventOutput, create_registry};

use serde::{Deserialize, Serialize};
use thiserror::Error;

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

pub type Result<T> = std::result::Result<T, CoreError>;
pub type Error = CoreError;

// ===== Common types and constants (from types.rs) =====

/// Common event sources
pub mod sources {
    pub const SINEX: &str = "sinex";
    pub const FILESYSTEM: &str = "filesystem";
    pub const TERMINAL_KITTY: &str = "terminal.kitty";
    pub const HYPRLAND: &str = "hyprland";
    pub const WINDOW_MANAGER_HYPRLAND: &str = "window_manager.hyprland";
    pub const ATUIN_DB_READER: &str = "ingestor.atuin_db_reader";
    pub const CLIPBOARD: &str = "clipboard";
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
        pub const FILE_RENAMED: &str = "file.renamed";
    }

    pub mod terminal {
        pub const COMMAND_EXECUTED: &str = "command.executed";
    }
    
    pub mod window_manager {
        // Window events
        pub const WINDOW_FOCUSED: &str = "window.focused";
        pub const WINDOW_OPENED: &str = "window.opened";
        pub const WINDOW_CLOSED: &str = "window.closed";
        pub const WINDOW_MOVED: &str = "window.moved";
        pub const WINDOW_TITLE_CHANGED: &str = "window.title_changed";
        pub const WINDOW_URGENT: &str = "window.urgent";
        
        // Workspace events
        pub const WORKSPACE_CHANGED: &str = "workspace.changed";
        pub const WORKSPACE_CREATED: &str = "workspace.created";
        pub const WORKSPACE_DESTROYED: &str = "workspace.destroyed";
        
        // Monitor events
        pub const MONITOR_FOCUSED: &str = "monitor.focused";
        pub const MONITOR_ADDED: &str = "monitor.added";
        pub const MONITOR_REMOVED: &str = "monitor.removed";
        
        // State dumps (periodic)
        pub const STATE_SNAPSHOT: &str = "state.snapshot";
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