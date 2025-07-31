//! Shell and terminal event payloads
//!
//! Note: Payloads are source-specific. A command from Kitty is different
//! from a command from Atuin, even if they have similar fields.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::collections::HashMap;

// Kitty shell integration payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.kitty", event_type = "command.executed")]
pub struct KittyCommandExecutedPayload {
    pub command: String,
    pub working_directory: Option<String>,
    pub exit_status: Option<i32>,
    pub execution_time_ms: Option<u64>,
    pub shell_type: Option<String>,
    pub kitty_window_id: String,
    pub kitty_tab_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.kitty", event_type = "command.completed")]
pub struct KittyCommandCompletedPayload {
    pub command: String,
    pub working_directory: String,
    pub exit_status: i32,
    pub duration_ms: u64,
    pub shell_pid: u32,
    pub kitty_window_id: String,
    pub kitty_tab_id: String,
    pub output_lines: Option<u32>,
    pub error_output: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "terminal.kitty", event_type = "session.started")]
pub struct KittySessionStartedPayload {
    pub window_id: String,
    pub tab_id: String,
    pub shell_type: String,
    pub working_directory: String,
    pub env_vars: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "terminal.kitty", event_type = "session.ended")]
pub struct KittySessionEndedPayload {
    pub window_id: String,
    pub tab_id: String,
    pub duration_seconds: u64,
    pub exit_code: Option<i32>,
}

// Atuin history payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.atuin", event_type = "command.executed")]
pub struct AtuinCommandExecutedPayload {
    pub command_string: String,
    pub cwd: String,
    pub exit_code: i32,
    pub duration_ns: i64,
    pub atuin_history_id: String,
    pub atuin_session_id: String,
    pub timestamp: i64,
    pub ts_start_orig: DateTime<Utc>,
    pub ts_end_orig: DateTime<Utc>,
    pub hostname: String,
    pub terminal_session_ulid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.atuin", event_type = "command.completed")]
pub struct AtuinCommandCompletedPayload {
    pub command: String,
    pub working_directory: String,
    pub exit_status: i32,
    pub duration_ms: u64,
    pub hostname: String,
    pub username: String,
    pub shell: String,
    pub atuin_id: String,
    pub session_id: String,
}

// Generic shell history import payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.history", event_type = "command.imported")]
pub struct HistoryCommandImportedPayload {
    pub command: String,
    pub timestamp: Option<DateTime<Utc>>,
    pub shell_type: String,
    pub source_file: String,
    pub line_number: Option<u32>,
}

// Atuin imported entry (from CSV/DB import)

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "atuin", event_type = "entry.imported")]
pub struct AtuinEntryPayload {
    pub id: String,
    pub command: String,
    pub timestamp: DateTime<Utc>,
    pub duration_ms: u64,
    pub exit_code: i32,
    pub directory: String,
    pub session: String,
    pub hostname: String,
}

// Command imported from shell history

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell", event_type = "command.imported")]
pub struct CommandImportedPayload {
    pub command: String,
    pub timestamp: DateTime<Utc>,
    pub source_file: String,
    pub line_number: Option<u64>,
    pub shell_type: String,
}

// Bash-specific history

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.bash_histfile", event_type = "entry.imported")]
pub struct BashHistoryEntryPayload {
    pub command: String,
    pub timestamp: Option<DateTime<Utc>>,
    pub histfile_path: String,
    pub line_number: u32,
}

// Real-time shell history file monitoring

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.bash_histfile", event_type = "command.historical")]
pub struct BashHistoricalCommandPayload {
    pub command_string: String,
    pub source_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.zsh_histfile", event_type = "command.historical")]
pub struct ZshHistoricalCommandPayload {
    pub command_string: String,
    pub source_file: String,
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
    pub start_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "terminal", event_type = "shell.command_historical")]
pub struct TerminalCommandHistoricalPayload {
    pub source: String,
    pub db_path: Option<std::path::PathBuf>,
    pub file_path: Option<std::path::PathBuf>,
    pub scan_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "terminal", event_type = "shell.history_historical")]
pub struct TerminalHistoryHistoricalPayload {
    pub source: String,
    pub file_path: std::path::PathBuf,
    pub scan_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "terminal", event_type = "shell.terminal_snapshot")]
pub struct TerminalSnapshotPayload {
    pub active_watchers: usize,
    pub enabled_sources: HashMap<String, bool>,
    pub snapshot_time: DateTime<Utc>,
}

// Kitty terminal-specific events

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.kitty", event_type = "process.changed")]
pub struct KittyProcessChangedPayload {
    pub kitty_window_id: String,
    pub kitty_tab_id: String,
    pub previous_process: Option<serde_json::Value>,
    pub current_process: serde_json::Value,
    pub change_timestamp: String,
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
    pub focus_timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.kitty", event_type = "content.streamed")]
pub struct KittyContentStreamedPayload {
    pub kitty_window_id: String,
    pub new_lines: Vec<String>,
    pub line_start_offset: usize,
    pub capture_timestamp: String,
}

// Canonical command payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "canonical.terminal", event_type = "command.canonical")]
pub struct CanonicalCommandPayload {
    pub command: String,
    pub working_directory: String,
    pub exit_code: i32,
    pub duration_ms: u64,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
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
    pub terminal_type: String,
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
    pub timestamp: String,
}

// Asciinema recording payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.asciinema", event_type = "shell.session_started")]
pub struct AsciinemaSessionStartedPayload {
    pub session_id: String,
    pub terminal_type: String,
    pub terminal_id: String,
    pub cwd: String,
    pub command: Option<String>,
    pub environment: serde_json::Value,
    pub dimensions: serde_json::Value,
    pub start_time: String,
    pub recording_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.asciinema", event_type = "shell.session_ended")]
pub struct AsciinemaSessionEndedPayload {
    pub session_id: String,
    pub terminal_type: String,
    pub terminal_id: String,
    pub end_time: String,
    pub duration_seconds: f64,
    pub event_count: usize,
    pub recording_file: String,
    pub file_size_bytes: Option<u64>,
    pub git_annex_path: Option<serde_json::Value>,
    pub git_annex_key: Option<serde_json::Value>,
}
