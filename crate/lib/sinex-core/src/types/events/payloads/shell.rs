//! Shell and terminal event payloads
//!
//! Note: Payloads are source-specific. A command from Kitty is different
//! from a command from Atuin, even if they have similar fields.

use crate::types::domain::{CommandText, HostName, SanitizedPath, ShellName};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;
use std::collections::HashMap;

// Kitty shell integration payloads

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.kitty", event_type = "command.executed")]
pub struct KittyCommandExecutedPayload {
    pub command: CommandText,
    pub working_directory: Option<SanitizedPath>,
    pub exit_status: Option<i32>,
    pub execution_time_ms: Option<u64>,
    pub shell_type: Option<ShellName>,
    pub kitty_window_id: String,
    pub kitty_tab_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "shell.kitty", event_type = "command.completed")]
pub struct KittyCommandCompletedPayload {
    pub command: CommandText,
    pub working_directory: SanitizedPath,
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
    pub shell_type: ShellName,
    pub working_directory: SanitizedPath,
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
    pub command_string: CommandText,
    pub cwd: SanitizedPath,
    pub exit_code: i32,
    pub duration_ns: i64,
    pub atuin_history_id: String,
    pub atuin_session_id: String,
    pub timestamp: i64,
    pub ts_start_orig: DateTime<Utc>,
    pub ts_end_orig: DateTime<Utc>,
    pub hostname: HostName,
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

impl KittyCommandExecutedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(command: impl Into<String>) -> Self {
        Self {
            command: CommandText::from(command.into()),
            working_directory: None,
            exit_status: None,
            execution_time_ms: None,
            shell_type: None,
            kitty_window_id: "1".to_string(),
            kitty_tab_id: "1".to_string(),
        }
    }

    /// Builder-style method for working directory
    pub fn with_working_directory(mut self, dir: impl Into<String>) -> Self {
        self.working_directory = Some(SanitizedPath::from(dir.into()));
        self
    }

    /// Builder-style method for exit status
    pub fn with_exit_status(mut self, status: i32) -> Self {
        self.exit_status = Some(status);
        self
    }

    /// Builder-style method for execution time
    pub fn with_execution_time_ms(mut self, time_ms: u64) -> Self {
        self.execution_time_ms = Some(time_ms);
        self
    }

    /// Builder-style method for shell type
    pub fn with_shell_type(mut self, shell: impl Into<String>) -> Self {
        self.shell_type = Some(ShellName::from(shell.into()));
        self
    }

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
    /// Create a test payload with sensible defaults
    pub fn test_default(command_string: impl Into<String>, cwd: impl Into<String>) -> Self {
        use chrono::Utc;
        let now = Utc::now();
        Self {
            command_string: CommandText::from(command_string.into()),
            cwd: SanitizedPath::from(cwd.into()),
            exit_code: 0,
            duration_ns: 1000000, // 1ms in nanoseconds
            atuin_history_id: "test-history-id".to_string(),
            atuin_session_id: "test-session-id".to_string(),
            timestamp: now.timestamp(),
            ts_start_orig: now,
            ts_end_orig: now,
            hostname: HostName::from("test-hostname".to_string()),
            terminal_session_ulid: None,
        }
    }

    /// Builder-style method for exit code
    pub fn with_exit_code(mut self, code: i32) -> Self {
        self.exit_code = code;
        self
    }

    /// Builder-style method for duration
    pub fn with_duration_ns(mut self, duration: i64) -> Self {
        self.duration_ns = duration;
        self
    }

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

    /// Builder-style method for hostname
    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = HostName::from(hostname.into());
        self
    }

    /// Builder-style method for terminal session ULID
    pub fn with_terminal_session_ulid(mut self, ulid: impl Into<String>) -> Self {
        self.terminal_session_ulid = Some(ulid.into());
        self
    }
}

impl CanonicalCommandPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(command: impl Into<String>, working_directory: impl Into<String>) -> Self {
        use chrono::Utc;
        let now = Utc::now();
        Self {
            command: command.into(),
            working_directory: working_directory.into(),
            exit_code: 0,
            duration_ms: 100,
            start_time: now,
            end_time: now,
            user: "test-user".to_string(),
            session_id: "test-session".to_string(),
            environment_hash: "test-env-hash".to_string(),
            source_events: vec![],
            enrichment_history: vec![],
        }
    }

    /// Builder-style method for exit code
    pub fn with_exit_code(mut self, code: i32) -> Self {
        self.exit_code = code;
        self
    }

    /// Builder-style method for duration
    pub fn with_duration_ms(mut self, duration: u64) -> Self {
        self.duration_ms = duration;
        self
    }

    /// Builder-style method for user
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    /// Builder-style method for session ID
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = session_id.into();
        self
    }

    /// Builder-style method for source events
    pub fn with_source_events(mut self, events: Vec<String>) -> Self {
        self.source_events = events;
        self
    }

    /// Builder-style method for enrichment history
    pub fn with_enrichment_history(mut self, history: Vec<serde_json::Value>) -> Self {
        self.enrichment_history = history;
        self
    }
}

impl KittySessionStartedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default() -> Self {
        Self {
            window_id: "1".to_string(),
            tab_id: "1".to_string(),
            shell_type: ShellName::from("bash".to_string()),
            working_directory: SanitizedPath::from("/tmp".to_string()),
            env_vars: None,
        }
    }

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

    /// Builder-style method for shell type
    pub fn with_shell_type(mut self, shell: impl Into<String>) -> Self {
        self.shell_type = ShellName::from(shell.into());
        self
    }

    /// Builder-style method for working directory
    pub fn with_working_directory(mut self, dir: impl Into<String>) -> Self {
        self.working_directory = SanitizedPath::from(dir.into());
        self
    }

    /// Builder-style method for environment variables
    pub fn with_env_vars(mut self, env_vars: HashMap<String, String>) -> Self {
        self.env_vars = Some(env_vars);
        self
    }
}

impl HistoryCommandImportedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default(
        command: impl Into<String>,
        shell_type: impl Into<String>,
        source_file: impl Into<String>,
    ) -> Self {
        Self {
            command: command.into(),
            timestamp: None,
            shell_type: shell_type.into(),
            source_file: source_file.into(),
            line_number: None,
        }
    }

    /// Builder-style method for timestamp
    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Builder-style method for line number
    pub fn with_line_number(mut self, line: u32) -> Self {
        self.line_number = Some(line);
        self
    }
}

impl TerminalMonitoringStartedPayload {
    /// Create a test payload with sensible defaults
    pub fn test_default() -> Self {
        Self {
            enabled_sources: HashMap::new(),
            start_time: Utc::now(),
        }
    }

    /// Builder-style method for enabled sources
    pub fn with_enabled_sources(mut self, sources: HashMap<String, bool>) -> Self {
        self.enabled_sources = sources;
        self
    }

    /// Builder-style method for start time
    pub fn with_start_time(mut self, time: DateTime<Utc>) -> Self {
        self.start_time = time;
        self
    }
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
