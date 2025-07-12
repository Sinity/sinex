//! Core Event Types and Builders
//!
//! This crate provides the fundamental event types and builders used throughout
//! the Sinex system, extracted from sinex-core for focused responsibility.

pub mod event_builders;
pub mod raw_event;
pub mod strongly_typed_events;

// Re-export core event types
pub use raw_event::{JsonValue, OptionalTimestamp, RawEvent, RawEventBuilder, Timestamp};

// Re-export event builders
pub use event_builders::{
    ClipboardContentType, ClipboardEventBuilder, EventFactory, FileOperation,
    FilesystemEventBuilder, SystemEventBuilder, TerminalEventBuilder, WindowManagerEventBuilder,
    WindowManagerEventType,
};

// Re-export strongly typed events
pub use strongly_typed_events::{
    typed_event_channel, AtuinEntryPayload, ClipboardCopiedPayload, ClipboardSelectedPayload,
    CommandCompletedPayload, CommandExecutedPayload, CommandImportedPayload, DirCreatedPayload,
    DirDeletedPayload, EnforcedTypedEventSource, EventEnvelope, FileCreatedPayload,
    FileDeletedPayload, FileModifiedPayload, FileMovedPayload, JournalEntryPayload,
    ProcessHeartbeatPayload, ProcessShutdownPayload, ProcessStartedPayload, ScanCompletedPayload, ScanStartedPayload,
    SensorActivatedPayload, SensorDeactivatedPayload, SessionEndedPayload, SessionStartedPayload,
    SystemStatePayload, TypedClipboardEventBuilder, TypedEventBuilder, TypedEventError,
    TypedEventPipelineAdapter, TypedEventReceiver, TypedEventResult, TypedEventSender,
    TypedFilesystemEventBuilder, TypedRawEvent, TypedTerminalEventBuilder, TypedToJsonAdapter,
    WindowClosedPayload, WindowFocusedPayload, WindowOpenedPayload, WorkspaceSwitchedPayload,
};

// Common type aliases
pub type EventSender = tokio::sync::mpsc::Sender<RawEvent>;
pub type EventReceiver = tokio::sync::mpsc::Receiver<RawEvent>;

// Event constants
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

pub mod event_types {
    pub mod sinex {
        // Automaton events
        pub const AUTOMATON_STARTUP: &str = "automaton.startup";
        pub const AUTOMATON_SHUTDOWN: &str = "automaton.shutdown";
        pub const AUTOMATON_HEARTBEAT: &str = "automaton.heartbeat";
        pub const AUTOMATON_ERROR: &str = "automaton.error";
        pub const AUTOMATON_DLQ_EVENT_WRITTEN: &str = "automaton.dlq_event_written";
        
        // Scanner events
        pub const SCAN_STARTED: &str = "scan.started";
        pub const SCAN_COMPLETED: &str = "scan.completed";
        
        // Process events
        pub const PROCESS_STARTED: &str = "process.started";
        pub const PROCESS_HEARTBEAT: &str = "process.heartbeat";
        pub const PROCESS_SHUTDOWN: &str = "process.shutdown";
        
        // Sensor events
        pub const SENSOR_ACTIVATED: &str = "sensor.activated";
        pub const SENSOR_DEACTIVATED: &str = "sensor.deactivated";
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
        pub const ENTRY_IMPORTED: &str = "entry.imported";
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

// Agent status and error types
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AutomatonStatus {
    Starting,
    Running,
    Stopping,
    Error,
}

// Legacy alias for compatibility
pub type AgentStatus = AutomatonStatus;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ErrorSeverity {
    Warning,
    Error,
    Critical,
}
