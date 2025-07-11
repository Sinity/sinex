//! Generic Command Execution Events
//!
//! This module provides generic command execution event types that can be used
//! by various shell integration components.

use serde::{Deserialize, Serialize};
use sinex_core::Timestamp;

use crate::ShellCommandInfo;

// ============================================================================
// Event Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CommandExecutedPayload {
    pub command_line: String,
    pub working_directory: Option<String>,
    pub shell_type: Option<String>,
    pub session_id: Option<String>,
    pub pid: Option<u32>,
    pub start_time: Timestamp,
    pub shell_command_info: ShellCommandInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CommandCompletedPayload {
    pub command_line: String,
    pub working_directory: Option<String>,
    pub shell_type: Option<String>,
    pub session_id: Option<String>,
    pub pid: Option<u32>,
    pub exit_code: Option<i32>,
    pub execution_time_ms: Option<u64>,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub shell_command_info: ShellCommandInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_output: Option<String>,
}

// ============================================================================
// Event Types
// ============================================================================

pub struct CommandExecuted;
pub struct CommandCompleted;

// ============================================================================
// Helper Functions
// ============================================================================

impl CommandExecutedPayload {
    pub fn new(command_line: String, shell_command_info: ShellCommandInfo) -> Self {
        Self {
            command_line,
            working_directory: shell_command_info.working_directory.clone(),
            shell_type: shell_command_info.shell_type.clone(),
            session_id: shell_command_info.session_id.clone(),
            pid: shell_command_info.pid,
            start_time: shell_command_info.start_time,
            shell_command_info,
        }
    }
}

impl CommandCompletedPayload {
    pub fn new(
        command_line: String,
        mut shell_command_info: ShellCommandInfo,
        output_preview: Option<String>,
        error_output: Option<String>,
    ) -> Self {
        let end_time = shell_command_info.end_time.unwrap_or_else(chrono::Utc::now);

        // Update shell command info with completion data
        if shell_command_info.end_time.is_none() {
            shell_command_info.end_time = Some(end_time);
        }

        // Calculate execution time if not already set
        if shell_command_info.execution_time_ms.is_none() {
            let duration = end_time.signed_duration_since(shell_command_info.start_time);
            shell_command_info.execution_time_ms = Some(duration.num_milliseconds() as u64);
        }

        Self {
            command_line,
            working_directory: shell_command_info.working_directory.clone(),
            shell_type: shell_command_info.shell_type.clone(),
            session_id: shell_command_info.session_id.clone(),
            pid: shell_command_info.pid,
            exit_code: shell_command_info.exit_code,
            execution_time_ms: shell_command_info.execution_time_ms,
            start_time: shell_command_info.start_time,
            end_time,
            shell_command_info,
            output_preview,
            error_output,
        }
    }

    /// Check if the command was successful (exit code 0)
    pub fn was_successful(&self) -> bool {
        self.exit_code.map(|code| code == 0).unwrap_or(false)
    }

    /// Get a summary description of the command execution
    pub fn execution_summary(&self) -> String {
        let status = if self.was_successful() {
            "succeeded"
        } else {
            "failed"
        };

        let duration = if let Some(ms) = self.execution_time_ms {
            if ms < 1000 {
                format!(" in {}ms", ms)
            } else {
                format!(" in {:.1}s", ms as f64 / 1000.0)
            }
        } else {
            String::new()
        };

        format!("Command {}{}", status, duration)
    }
}
