//! Shell and terminal event payloads
//!
//! Note: Payloads are source-specific. A command from Kitty is different
//! from a command from Atuin, even if they have similar fields.

use crate::domain::{CommandText, HostName, SanitizedPath, ShellName};
use crate::events::enums::{ScanType, TerminalType};
use crate::units::{ExitCode, Nanoseconds, ProcessId};
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::collections::HashMap;

// Kitty shell integration payloads
define_event_payload! {
    /// Kitty command executed event emitted by shell integration.
    pub struct KittyCommandExecutedPayload {
        command: CommandText,
        working_directory: Option<SanitizedPath>,
        exit_status: Option<ExitCode>,
        execution_time_ms: Option<u64>,
        shell_type: Option<ShellName>,
        kitty_window_id: String,
        kitty_tab_id: String,
    } => ("shell.kitty", "command.executed");
}

define_event_payload! {
    /// Kitty command completion event.
    pub struct KittyCommandCompletedPayload {
        command: CommandText,
        working_directory: SanitizedPath,
        exit_status: ExitCode,
        duration_ms: u64,
        shell_pid: ProcessId,
        kitty_window_id: String,
        kitty_tab_id: String,
        output_lines: Option<u32>,
        error_output: Option<String>,
    } => ("shell.kitty", "command.completed");
}

define_event_payload! {
    /// Kitty terminal session start.
    pub struct KittySessionStartedPayload {
        window_id: String,
        tab_id: String,
        shell_type: ShellName,
        working_directory: SanitizedPath,
        env_vars: Option<HashMap<String, String>>,
    } => ("terminal.kitty", "session.started");
}

define_event_payload! {
    /// Kitty terminal session end event.
    pub struct KittySessionEndedPayload {
        window_id: String,
        tab_id: String,
        duration_seconds: u64,
        exit_code: Option<ExitCode>,
    } => ("terminal.kitty", "session.ended");
}

// Atuin history payloads

define_event_payload! {
    /// Atuin command execution captured from history ingestion.
    pub struct AtuinCommandExecutedPayload {
        command_string: CommandText,
        cwd: SanitizedPath,
        exit_code: ExitCode,
        duration_ns: Nanoseconds,
        atuin_history_id: String,
        atuin_session_id: String,
        timestamp: i64,
        ts_start_orig: Timestamp,
        ts_end_orig: Timestamp,
        hostname: HostName,
        terminal_session_ulid: Option<String>,
    } => ("shell.atuin", "command.executed");
}

// Test helpers for external tests
#[cfg(any(test, feature = "testing"))]
impl KittyCommandExecutedPayload {
    pub fn test_default(command: impl Into<String>) -> Self {
        Self {
            command: command.into().into(),
            working_directory: None,
            exit_status: Some(ExitCode::SUCCESS),
            execution_time_ms: Some(1),
            shell_type: None,
            kitty_window_id: "test".to_string(),
            kitty_tab_id: "test".to_string(),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl AtuinCommandExecutedPayload {
    pub fn test_default(command: impl Into<String>, cwd: impl Into<SanitizedPath>) -> Self {
        Self {
            command_string: command.into().into(),
            cwd: cwd.into(),
            exit_code: ExitCode::SUCCESS,
            duration_ns: Nanoseconds::from_nanos(1),
            atuin_history_id: "h1".to_string(),
            atuin_session_id: "s1".to_string(),
            timestamp: 0,
            ts_start_orig: crate::temporal::now(),
            ts_end_orig: crate::temporal::now(),
            hostname: HostName::new("test-host".to_string()),
            terminal_session_ulid: None,
        }
    }
}

define_event_payload! {
    /// Atuin command completion payload.
    pub struct AtuinCommandCompletedPayload {
        command: String,
        working_directory: String,
        exit_status: ExitCode,
        duration_ms: u64,
        hostname: String,
        username: String,
        shell: String,
        atuin_id: String,
        session_id: String,
    } => ("shell.atuin", "command.completed");
}

// Generic shell history import payloads

define_event_payload! {
    /// Shell history command imported event.
    pub struct HistoryCommandImportedPayload {
        command: String,
        timestamp: Option<Timestamp>,
        shell_type: String,
        source_file: String,
        line_number: Option<u32>,
    } => ("shell.history", "command.imported");
}

// Atuin imported entry (from CSV/DB import)

define_event_payload! {
    /// Atuin entry imported from external source.
    pub struct AtuinEntryPayload {
        id: String,
        command: String,
        timestamp: Timestamp,
        duration_ms: u64,
        exit_code: ExitCode,
        directory: String,
        session: String,
        hostname: String,
    } => ("atuin", "entry.imported");
}

// Command imported from shell history

define_event_payload! {
    /// Generic shell command import event.
    pub struct CommandImportedPayload {
        command: String,
        timestamp: Timestamp,
        source_file: String,
        line_number: Option<u32>,
        shell_type: String,
    } => ("shell", "command.imported");
}

// Bash-specific history

define_event_payload! {
    /// Entry parsed from a Bash history file.
    pub struct BashHistoryEntryPayload {
        command: String,
        timestamp: Option<Timestamp>,
        histfile_path: String,
        line_number: u32,
    } => ("shell.bash_histfile", "entry.imported");
}

// Real-time shell history file monitoring

define_event_payload! {
    /// Historical command streamed from Bash histfile tailing.
    pub struct BashHistoricalCommandPayload {
        command_string: String,
        source_file: String,
    } => ("shell.bash_histfile", "command.historical");
}

define_event_payload! {
    /// Historical command streamed from Zsh history monitoring.
    pub struct ZshHistoricalCommandPayload {
        command_string: String,
        source_file: String,
    } => ("shell.zsh_histfile", "command.historical");
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.fish_history", event_type = "command.historical")]
pub struct FishHistoricalCommandPayload {
    pub command_string: String,
    pub source_file: String,
}

// Terminal monitoring events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "terminal", event_type = "shell.terminal_monitoring_started")]
pub struct TerminalMonitoringStartedPayload {
    pub enabled_sources: HashMap<String, bool>,
    pub start_time: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "terminal", event_type = "shell.command_historical")]
pub struct TerminalCommandHistoricalPayload {
    pub source: String,
    pub db_path: Option<std::path::PathBuf>,
    pub file_path: Option<std::path::PathBuf>,
    pub scan_type: ScanType,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "terminal", event_type = "shell.history_historical")]
pub struct TerminalHistoryHistoricalPayload {
    pub source: String,
    pub file_path: std::path::PathBuf,
    pub scan_type: ScanType,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "terminal", event_type = "shell.terminal_snapshot")]
pub struct TerminalSnapshotPayload {
    pub active_watchers: usize,
    pub enabled_sources: HashMap<String, bool>,
    pub snapshot_time: Timestamp,
}

// Kitty terminal-specific events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.kitty", event_type = "process.changed")]
pub struct KittyProcessChangedPayload {
    pub kitty_window_id: String,
    pub kitty_tab_id: String,
    pub previous_process: Option<serde_json::Value>,
    pub current_process: serde_json::Value,
    pub change_timestamp: Timestamp,
    pub working_directory: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.kitty", event_type = "tab.focused")]
pub struct KittyTabFocusedPayload {
    pub kitty_tab_id: String,
    pub kitty_window_id: String,
    pub tab_title: String,
    pub tab_index: usize,
    pub previous_tab_id: Option<String>,
    pub focus_timestamp: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.kitty", event_type = "content.streamed")]
pub struct KittyContentStreamedPayload {
    pub kitty_window_id: String,
    pub new_lines: Vec<String>,
    pub line_start_offset: usize,
    pub capture_timestamp: Timestamp,
}

// Canonical command payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "canonical.terminal", event_type = "command.canonical")]
pub struct CanonicalCommandPayload {
    pub command: String,
    pub working_directory: String,
    pub exit_code: ExitCode,
    pub duration_ms: u64,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub user: String,
    pub session_id: String,
    pub environment_hash: String,
    pub source_events: Vec<String>,
    pub enrichment_history: Vec<serde_json::Value>,
}

// Scrollback capture payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.scrollback", event_type = "shell.output_captured")]
pub struct ShellOutputCapturedPayload {
    pub window_id: String,
    pub terminal_type: TerminalType,
    pub cwd: String,
    pub window_title: String,
    pub scrollback_text: Option<String>,
    pub scrollback_chunks: Option<Vec<String>>,
    pub git_annex_path: Option<String>,
    pub git_annex_key: Option<String>,
    pub scrollback_lines: usize,
    pub scrollback_size_bytes: usize,
    pub is_chunked: bool,
    pub chunk_count: Option<usize>,
    pub includes_screen: bool,
    pub has_ansi_codes: bool,
    pub timestamp: Timestamp,
}

impl KittyCommandExecutedPayload {
    /// Builder-style method for window and tab IDs
    pub fn with_kitty_ids(
        mut self,
        window_id: impl Into<String>,
        tab_id: impl Into<String>,
    ) -> Self {
        self.kitty_window_id = window_id.into();
        self.kitty_tab_id = tab_id.into();
        self
    }
}

impl AtuinCommandExecutedPayload {
    /// Builder-style method for atuin IDs
    pub fn with_atuin_ids(
        mut self,
        history_id: impl Into<String>,
        session_id: impl Into<String>,
    ) -> Self {
        self.atuin_history_id = history_id.into();
        self.atuin_session_id = session_id.into();
        self
    }
}

impl KittySessionStartedPayload {
    /// Builder-style method for window and tab IDs
    pub fn with_kitty_ids(
        mut self,
        window_id: impl Into<String>,
        tab_id: impl Into<String>,
    ) -> Self {
        self.window_id = window_id.into();
        self.tab_id = tab_id.into();
        self
    }
}

impl HistoryCommandImportedPayload {}

// Asciinema recording payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.asciinema", event_type = "shell.session_started")]
pub struct AsciinemaSessionStartedPayload {
    pub session_id: String,
    pub terminal_type: TerminalType,
    pub terminal_id: String,
    pub cwd: String,
    pub command: Option<String>,
    pub environment: serde_json::Value,
    pub dimensions: serde_json::Value,
    pub start_time: Timestamp,
    pub recording_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.asciinema", event_type = "shell.session_ended")]
pub struct AsciinemaSessionEndedPayload {
    pub session_id: String,
    pub terminal_type: TerminalType,
    pub terminal_id: String,
    pub end_time: Timestamp,
    pub duration_seconds: f64,
    pub event_count: usize,
    pub recording_file: String,
    pub file_size_bytes: Option<u64>,
    pub git_annex_path: Option<serde_json::Value>,
    pub git_annex_key: Option<serde_json::Value>,
}
