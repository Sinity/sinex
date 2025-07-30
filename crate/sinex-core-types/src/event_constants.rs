//! Typed constants for EventSource and EventType
//!
//! This module provides strongly-typed constants that replace string literals
//! throughout the codebase. All event sources and types should be defined here
//! and used via these typed constants.

use crate::domain::{EventSource, EventType};
use lazy_static::lazy_static;

/// Event sources as typed constants
pub mod sources {
    use super::*;

    lazy_static! {
        // Core system sources
        pub static ref SINEX: EventSource = EventSource::new("sinex");
        pub static ref FS: EventSource = EventSource::new("fs");

        // File system watcher
        pub static ref FS_WATCHER: EventSource = EventSource::new("fs-watcher");

        // Shell integration sources
        pub static ref SHELL_KITTY: EventSource = EventSource::new("shell.kitty");
        pub static ref SHELL_ATUIN: EventSource = EventSource::new("shell.atuin");
        pub static ref SHELL_HISTORY: EventSource = EventSource::new("shell.history");
        pub static ref SHELL_BASH_HISTFILE: EventSource = EventSource::new("shell.bash_histfile");
        pub static ref SHELL_ZSH_HISTFILE: EventSource = EventSource::new("shell.zsh_histfile");
        pub static ref SHELL_FISH_HISTORY: EventSource = EventSource::new("shell.fish_history");
        pub static ref SHELL_RECORDING: EventSource = EventSource::new("shell.recording");
        pub static ref SHELL_ASCIINEMA: EventSource = EventSource::new("shell.asciinema");
        pub static ref SHELL_SCROLLBACK: EventSource = EventSource::new("shell.scrollback");

        // Terminal sources
        pub static ref TERMINAL: EventSource = EventSource::new("terminal");
        pub static ref TERMINAL_KITTY: EventSource = EventSource::new("terminal.kitty");

        // Desktop environment sources
        pub static ref DESKTOP: EventSource = EventSource::new("desktop");
        pub static ref WM_HYPRLAND: EventSource = EventSource::new("wm.hyprland");
        pub static ref CLIPBOARD: EventSource = EventSource::new("clipboard");

        // System sources
        pub static ref SYSTEM: EventSource = EventSource::new("system");
        pub static ref DBUS: EventSource = EventSource::new("dbus");
        pub static ref JOURNALD: EventSource = EventSource::new("journald");
        pub static ref UDEV: EventSource = EventSource::new("udev");
        pub static ref SYSTEMD: EventSource = EventSource::new("systemd");

        // Service sources
        pub static ref HEALTH_AGGREGATOR: EventSource = EventSource::new("health-aggregator");
        pub static ref BLOB_STORAGE: EventSource = EventSource::new("blob_storage");

        // Test sources
        pub static ref TEST: EventSource = EventSource::new("test");
    }
}

/// Event types as typed constants
pub mod types {
    use super::*;

    /// Filesystem event types
    pub mod filesystem {
        use super::*;

        lazy_static! {
            // File operations
            pub static ref FILE_CREATED: EventType = EventType::new("file.created");
            pub static ref FILE_MODIFIED: EventType = EventType::new("file.modified");
            pub static ref FILE_DELETED: EventType = EventType::new("file.deleted");
            pub static ref FILE_MOVED: EventType = EventType::new("file.moved");
            pub static ref FILE_RENAMED: EventType = EventType::new("file.renamed");

            // Directory operations
            pub static ref DIR_CREATED: EventType = EventType::new("dir.created");
            pub static ref DIR_DELETED: EventType = EventType::new("dir.deleted");
        }
    }

    /// Shell and terminal event types
    pub mod shell {
        use super::*;

        lazy_static! {
            // Command events
            pub static ref COMMAND_EXECUTED: EventType = EventType::new("command.executed");
            pub static ref COMMAND_COMPLETED: EventType = EventType::new("command.completed");
            pub static ref COMMAND_FAILED: EventType = EventType::new("command.failed");
            pub static ref COMMAND_IMPORTED: EventType = EventType::new("command.imported");
            pub static ref COMMAND_OUTPUT: EventType = EventType::new("command.output");

            // Session events
            pub static ref SESSION_STARTED: EventType = EventType::new("session.started");
            pub static ref SESSION_ENDED: EventType = EventType::new("session.ended");

            // Recording events
            pub static ref RECORDING_STARTED: EventType = EventType::new("recording.started");
            pub static ref RECORDING_ENDED: EventType = EventType::new("recording.ended");

            // Tab events
            pub static ref TAB_CREATED: EventType = EventType::new("tab.created");
            pub static ref TAB_FOCUSED: EventType = EventType::new("tab.focused");
            pub static ref TAB_CLOSED: EventType = EventType::new("tab.closed");

            // Process and config events
            pub static ref PROCESS_CHANGED: EventType = EventType::new("process.changed");
            pub static ref CONFIG_CHANGED: EventType = EventType::new("config.changed");

            // Terminal content events
            pub static ref SCROLLBACK_FULL: EventType = EventType::new("scrollback.full");
            pub static ref ENTRY_IMPORTED: EventType = EventType::new("entry.imported");
        }
    }

    /// Window manager event types
    pub mod window {
        use super::*;

        lazy_static! {
            // Window events
            pub static ref WINDOW_OPENED: EventType = EventType::new("window.opened");
            pub static ref WINDOW_CLOSED: EventType = EventType::new("window.closed");
            pub static ref WINDOW_FOCUSED: EventType = EventType::new("window.focused");
            pub static ref WINDOW_MOVED: EventType = EventType::new("window.moved");
            pub static ref WINDOW_RESIZED: EventType = EventType::new("window.resized");
            pub static ref WINDOW_CREATED: EventType = EventType::new("window.created");

            // Workspace events
            pub static ref WORKSPACE_SWITCHED: EventType = EventType::new("workspace.switched");
            pub static ref WORKSPACE_CREATED: EventType = EventType::new("workspace.created");
            pub static ref WORKSPACE_DESTROYED: EventType = EventType::new("workspace.destroyed");
            pub static ref WORKSPACE_CHANGED: EventType = EventType::new("workspace.changed");

            // Display events
            pub static ref DISPLAY_CONNECTED: EventType = EventType::new("display.connected");
            pub static ref DISPLAY_DISCONNECTED: EventType = EventType::new("display.disconnected");
            pub static ref MONITOR_FOCUSED: EventType = EventType::new("monitor.focused");

            // State events
            pub static ref STATE_CAPTURED: EventType = EventType::new("state.captured");
        }
    }

    /// Clipboard event types
    pub mod clipboard {
        use super::*;

        lazy_static! {
            pub static ref COPIED: EventType = EventType::new("clipboard.copied");
            pub static ref SELECTED: EventType = EventType::new("clipboard.selected");
        }
    }

    /// System event types
    pub mod system {
        use super::*;

        lazy_static! {
            // Sinex internal events
            pub static ref AUTOMATON_STARTUP: EventType = EventType::new("automaton.startup");
            pub static ref AUTOMATON_SHUTDOWN: EventType = EventType::new("automaton.shutdown");
            pub static ref AUTOMATON_HEARTBEAT: EventType = EventType::new("automaton.heartbeat");
            pub static ref AUTOMATON_ERROR: EventType = EventType::new("automaton.error");

            // Scanner events
            pub static ref SCAN_STARTED: EventType = EventType::new("scan.started");
            pub static ref SCAN_COMPLETED: EventType = EventType::new("scan.completed");

            // Process events
            pub static ref PROCESS_STARTED: EventType = EventType::new("process.started");
            pub static ref PROCESS_HEARTBEAT: EventType = EventType::new("process.heartbeat");
            pub static ref PROCESS_SHUTDOWN: EventType = EventType::new("process.shutdown");

            // Health monitoring
            pub static ref SYSTEM_HEALTH_SUMMARY: EventType = EventType::new("system.health.summary");
        }
    }

    /// D-Bus event types
    pub mod dbus {
        use super::*;

        lazy_static! {
            // Core D-Bus events
            pub static ref SIGNAL_RECEIVED: EventType = EventType::new("signal.received");
            pub static ref METHOD_CALLED: EventType = EventType::new("method.called");
            pub static ref NOTIFICATION_SENT: EventType = EventType::new("notification.sent");

            // Device events
            pub static ref DEVICE_CONNECTED: EventType = EventType::new("device.connected");
            pub static ref DEVICE_DISCONNECTED: EventType = EventType::new("device.disconnected");
            pub static ref DEVICE_CHANGED: EventType = EventType::new("device.changed");

            // State change events
            pub static ref MEDIA_STATE_CHANGED: EventType = EventType::new("media.state_changed");
            pub static ref POWER_STATE_CHANGED: EventType = EventType::new("power.state_changed");
            pub static ref NETWORK_STATE_CHANGED: EventType = EventType::new("network.state_changed");
            pub static ref BLUETOOTH_DEVICE_CHANGED: EventType = EventType::new("bluetooth.device_changed");
            pub static ref SESSION_STATE_CHANGED: EventType = EventType::new("session.state_changed");
            pub static ref SCREENSAVER_STATE_CHANGED: EventType = EventType::new("screensaver.state_changed");
        }
    }

    /// Systemd event types
    pub mod systemd {
        use super::*;

        lazy_static! {
            pub static ref UNIT_STARTED: EventType = EventType::new("unit.started");
            pub static ref UNIT_STOPPED: EventType = EventType::new("unit.stopped");
            pub static ref UNIT_CHANGED: EventType = EventType::new("unit.changed");
            pub static ref UNIT_STATE_CHANGED: EventType = EventType::new("unit.state_changed");
        }
    }
}

/// Helper functions for creating typed values from strings (for migration)
pub mod helpers {
    use super::*;

    /// Parse an event source string into a typed EventSource
    /// This validates the source and returns an error for invalid sources
    pub fn parse_event_source(source: &str) -> Result<EventSource, String> {
        let es = EventSource::new(source);
        es.validate()?;
        Ok(es)
    }

    /// Parse an event type string into a typed EventType
    /// This validates the type and returns an error for invalid types
    pub fn parse_event_type(event_type: &str) -> Result<EventType, String> {
        let et = EventType::new(event_type);
        et.validate()?;
        Ok(et)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_source_constants() {
        assert_eq!(sources::FS_WATCHER.as_str(), "fs-watcher");
        assert_eq!(sources::TERMINAL.as_str(), "terminal");
        assert_eq!(sources::DESKTOP.as_str(), "desktop");
        assert_eq!(sources::SHELL_KITTY.as_str(), "shell.kitty");
    }

    #[test]
    fn test_event_type_constants() {
        assert_eq!(types::filesystem::FILE_CREATED.as_str(), "file.created");
        assert_eq!(types::shell::COMMAND_EXECUTED.as_str(), "command.executed");
        assert_eq!(types::window::WINDOW_FOCUSED.as_str(), "window.focused");
    }

    #[test]
    fn test_type_safety() {
        // These are different types and cannot be mixed
        let source = sources::FS_WATCHER.clone();
        let event_type = types::filesystem::FILE_CREATED.clone();

        // Can't assign one to the other (would fail to compile if uncommented)
        // let _wrong: EventSource = event_type;
        // let _wrong: EventType = source;

        // But we can use them properly
        assert!(source.validate().is_ok());
        assert!(event_type.validate().is_ok());
    }

    #[test]
    fn test_helpers() {
        // Valid parsing
        assert!(helpers::parse_event_source("fs-watcher").is_ok());
        assert!(helpers::parse_event_type("file.created").is_ok());

        // Invalid parsing
        assert!(helpers::parse_event_source("").is_err());
        assert!(helpers::parse_event_source("Invalid Source").is_err());
        assert!(helpers::parse_event_type("").is_err());
        assert!(helpers::parse_event_type("Invalid.Type.").is_err());
    }
}
