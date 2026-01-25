//! Up command - start devenv processes

use anyhow::Result;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Up command configuration
pub struct UpCommand {
    pub all: bool,
    pub processes: Vec<String>,
}

impl XtaskCommand for UpCommand {
    fn name(&self) -> &str {
        "up"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let default_processes = vec!["nats", "ingestd", "gateway"];

        let procs: Vec<&str> = if self.all {
            vec![
                "nats",
                "ingestd",
                "gateway",
                "fs-ingestor",
                "terminal-ingestor",
                "desktop-ingestor",
                "system-ingestor",
                "analytics-automaton",
                "pkm-automaton",
            ]
        } else if self.processes.is_empty() {
            default_processes
        } else {
            self.processes.iter().map(|s| s.as_str()).collect()
        };

        ctx.heading("devenv up");

        let mut cmd = Command::new("devenv");
        cmd.arg("up");
        cmd.args(&procs);

        if ctx.is_human() {
            println!("Starting: {}", procs.join(", "));
        }

        let status = cmd.status()?;
        if !status.success() {
            return Ok(CommandResult::failure(crate::output::StructuredError::new(
                "DEVENV_UP_FAILED",
                format!("devenv up failed with status {}", status),
            ))
            .with_duration(ctx.elapsed()));
        }

        Ok(CommandResult::success()
            .with_detail(format!("started {} process(es)", procs.len()))
            .with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("devenv".to_string()),
            timeout: Some(std::time::Duration::from_secs(60)),
            modifies_state: true,
            track_in_history: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputWriter;

    #[test]
    fn test_command_name() {
        let cmd = UpCommand {
            all: false,
            processes: vec![],
        };
        assert_eq!(cmd.name(), "up");
    }

    #[test]
    fn test_command_metadata() {
        let cmd = UpCommand {
            all: false,
            processes: vec![],
        };
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("devenv".to_string()));
        assert!(metadata.modifies_state);
    }

    #[test]
    fn test_default_processes() {
        let cmd = UpCommand {
            all: false,
            processes: vec![],
        };
        // Default processes should be used when none specified
        assert!(cmd.processes.is_empty());
        assert!(!cmd.all);
    }

    #[test]
    fn test_custom_processes() {
        let cmd = UpCommand {
            all: false,
            processes: vec!["nats".to_string(), "ingestd".to_string()],
        };
        assert_eq!(cmd.processes.len(), 2);
    }

    #[test]
    fn test_all_flag() {
        let cmd = UpCommand {
            all: true,
            processes: vec![],
        };
        assert!(cmd.all);
    }
}
