//! Shell Events Module
//!
//! This module provides event sources for shell command tracking, history management,
//! and command execution monitoring across different shell environments.

pub mod atuin;
pub mod shell_history;
pub mod command_execution;

// Re-export shell event types and payloads
pub use atuin::{AtuinCommandExecuted, AtuinCommandExecutedPayload, AtuinHistoryImporter};
pub use shell_history::{ShellHistoryCommand, ShellHistoryCommandPayload, ShellHistoryMonitor};
pub use command_execution::{
    CommandExecuted, CommandExecutedPayload, CommandCompleted, CommandCompletedPayload
};

use sinex_core::register_events;

// Register all shell event types using the macro
register_events! {
    // Command execution (rich metadata from Atuin)
    "command.executed" => (shell.atuin, AtuinCommandExecutedPayload),
    
    // Command execution (discovered from history files)
    "command.imported" => (shell.history, ShellHistoryCommandPayload),
    
    // Generic command execution events
    "command.completed" => (shell.generic, CommandCompletedPayload),
}

/// Common shell command metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellCommandInfo {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub shell_type: Option<String>,
    pub session_id: Option<String>,
    pub pid: Option<u32>,
    pub exit_code: Option<i32>,
    pub execution_time_ms: Option<u64>,
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl ShellCommandInfo {
    /// Parse a command line into command and arguments
    pub fn parse_command_line(command_line: &str) -> Result<(String, Vec<String>), shell_words::ParseError> {
        let words = shell_words::split(command_line)?;
        if words.is_empty() {
            Ok((String::new(), Vec::new()))
        } else {
            Ok((words[0].clone(), words[1..].to_vec()))
        }
    }
    
    /// Get the full command line as a single string
    pub fn full_command(&self) -> String {
        if self.args.is_empty() {
            self.command.clone()
        } else {
            format!("{} {}", self.command, self.args.join(" "))
        }
    }
    
    /// Check if this is a long-running command (> 1 second)
    pub fn is_long_running(&self) -> bool {
        self.execution_time_ms.map(|t| t > 1000).unwrap_or(false)
    }
}

/// Configuration for shell event sources
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellConfig {
    /// Enable Atuin history integration
    pub enable_atuin: bool,
    
    /// Enable shell history file monitoring
    pub enable_history_files: bool,
    
    /// Shell history file paths to monitor
    pub history_paths: Vec<String>,
    
    /// Minimum command length to capture
    pub min_command_length: usize,
    
    /// Commands to ignore (prefixes)
    pub ignore_commands: Vec<String>,
    
    /// Maximum execution time to track (in milliseconds)
    pub max_execution_time_ms: u64,
}

impl Default for ShellConfig {
    fn default() -> Self {
        Self {
            enable_atuin: true,
            enable_history_files: true,
            history_paths: vec![
                "~/.bash_history".to_string(),
                "~/.zsh_history".to_string(),
                "~/.history".to_string(),
            ],
            min_command_length: 2,
            ignore_commands: vec![
                "ls".to_string(),
                "cd".to_string(),
                "pwd".to_string(),
                "echo".to_string(),
                "clear".to_string(),
                "exit".to_string(),
            ],
            max_execution_time_ms: 3600000, // 1 hour
        }
    }
}