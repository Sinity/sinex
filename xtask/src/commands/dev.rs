//! Developer utilities - TLS fixtures, test helpers, etc.

use anyhow::{bail, Context, Result};
use std::path::Path;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Developer utilities command variants
#[derive(Debug, Clone)]
pub enum DevSubcommand {
    TlsFixtures { output: String },
}

/// Developer utilities command
pub struct DevCommand {
    pub subcommand: DevSubcommand,
}

impl XtaskCommand for DevCommand {
    fn name(&self) -> &str {
        "dev"
    }

    fn execute(&self, _ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            DevSubcommand::TlsFixtures { output } => execute_tls_fixtures(output),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::utility()
    }
}

/// Generate TLS fixtures for secure NATS tests
fn execute_tls_fixtures(output: &str) -> Result<CommandResult> {
    let script = Path::new("scripts").join("generate_tls_fixtures.sh");
    if !script.exists() {
        bail!("TLS fixture script missing at {}", script.to_string_lossy());
    }

    let status = Command::new(&script)
        .arg(output)
        .status()
        .with_context(|| format!("failed to run {}", script.display()))?;

    if !status.success() {
        bail!("{} exited with {}", script.display(), status);
    }

    Ok(CommandResult::success().with_detail(format!("TLS fixtures generated in {output}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dev_command_name() {
        let cmd = DevCommand {
            subcommand: DevSubcommand::TlsFixtures {
                output: "tests/fixtures/tls".to_string(),
            },
        };

        assert_eq!(cmd.name(), "dev");
    }

    #[test]
    fn test_dev_command_metadata() {
        let cmd = DevCommand {
            subcommand: DevSubcommand::TlsFixtures {
                output: "tests/fixtures/tls".to_string(),
            },
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("utility".to_string()));
        assert!(
            !metadata.modifies_state,
            "dev utilities should not modify state"
        );
        assert!(
            !metadata.track_in_history,
            "dev utilities should not be tracked"
        );
    }

    #[test]
    fn test_tls_fixtures_subcommand_clone() {
        let cmd1 = DevSubcommand::TlsFixtures {
            output: "tests/fixtures/tls".to_string(),
        };
        let cmd2 = cmd1.clone();

        let (
            DevSubcommand::TlsFixtures {
                output: output1, ..
            },
            DevSubcommand::TlsFixtures {
                output: output2, ..
            },
        ) = (&cmd1, &cmd2);
        assert_eq!(output1, output2);
    }
}
