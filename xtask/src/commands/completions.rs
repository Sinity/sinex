//! Completions command - generate shell completions for xtask

use clap::{Command, ValueEnum};
use clap_complete::{generate, shells};
use color_eyre::eyre::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Shell type for completions generation
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Shell {
    /// Bash shell
    Bash,
    /// Zsh shell
    Zsh,
    /// Fish shell
    Fish,
    /// `PowerShell`
    PowerShell,
}

/// Completions command configuration
pub struct CompletionsCommand {
    pub shell: Shell,
}

impl CompletionsCommand {
    /// Generate completions for the given CLI command.
    ///
    /// This is the main entry point for completion generation, called from main.rs
    /// with the actual Cli command structure.
    pub fn generate_completions(shell: Shell, mut cmd: Command) -> Result<()> {
        let name = cmd.get_name().to_string();

        // Generate completions based on selected shell
        match shell {
            Shell::Bash => generate(shells::Bash, &mut cmd, name, &mut std::io::stdout()),
            Shell::Zsh => generate(shells::Zsh, &mut cmd, name, &mut std::io::stdout()),
            Shell::Fish => generate(shells::Fish, &mut cmd, name, &mut std::io::stdout()),
            Shell::PowerShell => {
                generate(shells::PowerShell, &mut cmd, name, &mut std::io::stdout());
            }
        }

        Ok(())
    }
}

impl XtaskCommand for CompletionsCommand {
    fn name(&self) -> &'static str {
        "completions"
    }

    async fn execute(&self, _ctx: &CommandContext) -> Result<CommandResult> {
        // Note: The actual completions generation is handled by the dispatcher in main.rs
        // which has access to the Cli command structure. This execute method serves as
        // a marker for the XtaskCommand trait implementation.
        // Use CompletionsCommand::generate_completions() instead.
        Ok(CommandResult::success().with_message("Completions generated successfully"))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::utility()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_completions_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = CompletionsCommand { shell: Shell::Bash };
        assert_eq!(cmd.name(), "completions");
        Ok(())
    }

    #[sinex_test]
    async fn test_completions_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = CompletionsCommand { shell: Shell::Zsh };
        let metadata = cmd.metadata();

        assert_eq!(metadata.category, Some("utility".to_string()));
        assert!(!metadata.track_in_history);
        assert!(!metadata.modifies_state);
        Ok(())
    }

    #[sinex_test]
    async fn test_all_shell_variants() -> ::xtask::sandbox::TestResult<()> {
        for shell in [Shell::Bash, Shell::Zsh, Shell::Fish, Shell::PowerShell] {
            let cmd = CompletionsCommand { shell };
            assert_eq!(cmd.name(), "completions");
        }
        Ok(())
    }
}
