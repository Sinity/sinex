use serde::{Deserialize, Serialize};

/// Common event sources
pub mod sources {
    pub const SINEX: &str = "sinex";
    pub const FILESYSTEM: &str = "filesystem";
    pub const TERMINAL_KITTY: &str = "terminal.kitty";
    pub const HYPRLAND: &str = "hyprland";
    pub const WINDOW_MANAGER_HYPRLAND: &str = "window_manager.hyprland";
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
        pub const WINDOW_FOCUSED: &str = "window.focused";
        pub const WORKSPACE_CHANGED: &str = "workspace.changed";
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