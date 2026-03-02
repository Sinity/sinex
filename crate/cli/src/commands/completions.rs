use clap::{Args, Command, ValueEnum};
use clap_complete::{Shell as ClapShell, generate};
use color_eyre::Result;
use std::io;

/// Generate shell completions
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Generate bash completions
    sinexctl completions bash > ~/.local/share/bash-completion/completions/sinexctl

    # Generate zsh completions
    sinexctl completions zsh > ~/.zfunc/_sinexctl

    # Generate fish completions
    sinexctl completions fish > ~/.config/fish/completions/sinexctl.fish

    # Source directly (bash)
    source <(sinexctl completions bash)
")]
pub struct CompletionsCommand {
    /// Shell to generate completions for
    #[arg(value_enum)]
    pub shell: Shell,
}

/// Supported shells for completion generation
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Shell {
    /// Bash shell
    Bash,
    /// Zsh shell
    Zsh,
    /// Fish shell
    Fish,
    /// PowerShell
    #[value(name = "powershell")]
    PowerShell,
    /// Elvish shell
    Elvish,
}

impl CompletionsCommand {
    /// Execute the completions command with the given CLI command
    pub fn execute(&self, cmd: &mut Command) -> Result<()> {
        let shell = match self.shell {
            Shell::Bash => ClapShell::Bash,
            Shell::Zsh => ClapShell::Zsh,
            Shell::Fish => ClapShell::Fish,
            Shell::PowerShell => ClapShell::PowerShell,
            Shell::Elvish => ClapShell::Elvish,
        };

        generate(shell, cmd, cmd.get_name().to_string(), &mut io::stdout());
        Ok(())
    }
}
