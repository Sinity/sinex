//! Logs command - view devenv process logs

use anyhow::{Context, Result};
use std::path::Path;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Logs command configuration
pub struct LogsCommand {
    pub process: String,
    pub lines: usize,
    pub follow: bool,
}

impl XtaskCommand for LogsCommand {
    fn name(&self) -> &str {
        "logs"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // devenv logs are typically in .devenv/state/*/logs
        let devenv_state = Path::new(".devenv").join("state");

        // Find the process log file
        let log_path = devenv_state.join(&self.process).join("process.log");

        if !log_path.exists() {
            // Try alternative locations
            let alt_path = devenv_state.join(format!("{}.log", self.process));
            if alt_path.exists() {
                return view_log(&alt_path, self.lines, self.follow, ctx);
            }

            // Try journalctl as fallback
            ctx.heading(&format!("logs: {}", self.process));

            let mut cmd = Command::new("journalctl");
            cmd.args(["--user", "-u", &format!("devenv-up-{}", self.process)]);
            cmd.arg("-n").arg(self.lines.to_string());

            if self.follow {
                cmd.arg("-f");
            }

            let status = cmd.status().context("journalctl failed to spawn")?;

            if !status.success() {
                return Ok(CommandResult::failure(crate::output::StructuredError::new(
                    "LOG_VIEW_FAILED",
                    format!("Failed to view logs for process '{}'", self.process),
                ))
                .with_duration(ctx.elapsed()));
            }

            return Ok(CommandResult::success()
                .with_detail("viewed logs via journalctl")
                .with_duration(ctx.elapsed()));
        }

        view_log(&log_path, self.lines, self.follow, ctx)
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("devenv".to_string()),
            timeout: None,
            modifies_state: false,
            track_in_history: false,
        }
    }
}

/// View a log file using tail.
fn view_log(
    path: &Path,
    lines: usize,
    follow: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading(&format!("logs: {}", path.display()));

    let mut cmd = Command::new("tail");
    cmd.arg("-n").arg(lines.to_string());

    if follow {
        cmd.arg("-f");
    }

    cmd.arg(path);

    let status = cmd.status().context("tail failed to spawn")?;

    if !status.success() {
        return Ok(CommandResult::failure(crate::output::StructuredError::new(
            "LOG_VIEW_FAILED",
            format!("tail failed with status {}", status),
        ))
        .with_duration(ctx.elapsed()));
    }

    Ok(CommandResult::success()
        .with_detail(format!("viewed {} lines from {}", lines, path.display()))
        .with_duration(ctx.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_name() {
        let cmd = LogsCommand {
            process: "nats".to_string(),
            lines: 50,
            follow: false,
        };
        assert_eq!(cmd.name(), "logs");
    }

    #[test]
    fn test_command_metadata() {
        let cmd = LogsCommand {
            process: "nats".to_string(),
            lines: 50,
            follow: false,
        };
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("devenv".to_string()));
        assert!(!metadata.modifies_state);
        assert!(!metadata.track_in_history);
    }

    #[test]
    fn test_default_lines() {
        let cmd = LogsCommand {
            process: "nats".to_string(),
            lines: 50,
            follow: false,
        };
        assert_eq!(cmd.lines, 50);
    }

    #[test]
    fn test_follow_flag() {
        let cmd = LogsCommand {
            process: "nats".to_string(),
            lines: 50,
            follow: true,
        };
        assert!(cmd.follow);
    }

    #[test]
    fn test_process_name() {
        let cmd = LogsCommand {
            process: "ingestd".to_string(),
            lines: 100,
            follow: false,
        };
        assert_eq!(cmd.process, "ingestd");
    }
}
