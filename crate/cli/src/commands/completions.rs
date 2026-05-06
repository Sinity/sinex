use clap::{Args, Command, ValueEnum};
use clap_complete::{Shell as ClapShell, generate};
use color_eyre::Result;
use sinex_primitives::events::schema_registry::get_all_payloads;
use std::collections::BTreeSet;
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
    /// `PowerShell`
    #[value(name = "powershell")]
    PowerShell,
    /// Elvish shell
    Elvish,
    /// List known event sources for shell completion scripts
    #[value(name = "list-sources", hide = true)]
    ListSources,
    /// List known event types for shell completion scripts
    #[value(name = "list-event-types", hide = true)]
    ListEventTypes,
}

impl CompletionsCommand {
    /// Execute the completions command with the given CLI command
    pub fn execute(&self, cmd: &mut Command) -> Result<()> {
        match self.shell {
            Shell::ListSources => {
                for source in completion_sources() {
                    println!("{source}");
                }
                return Ok(());
            }
            Shell::ListEventTypes => {
                for event_type in completion_event_types() {
                    println!("{event_type}");
                }
                return Ok(());
            }
            Shell::Bash | Shell::Zsh | Shell::Fish | Shell::PowerShell | Shell::Elvish => {}
        }

        let shell = match self.shell {
            Shell::Bash => ClapShell::Bash,
            Shell::Zsh => ClapShell::Zsh,
            Shell::Fish => ClapShell::Fish,
            Shell::PowerShell => ClapShell::PowerShell,
            Shell::Elvish => ClapShell::Elvish,
            Shell::ListSources | Shell::ListEventTypes => unreachable!("handled above"),
        };

        if matches!(self.shell, Shell::Zsh) {
            let mut buf = Vec::new();
            generate(shell, cmd, cmd.get_name().to_string(), &mut buf);
            let raw = String::from_utf8_lossy(&buf);
            print!("{}", postprocess_zsh(&raw));
        } else {
            generate(shell, cmd, cmd.get_name().to_string(), &mut io::stdout());
        }

        Ok(())
    }
}

fn completion_sources() -> Vec<&'static str> {
    get_all_payloads()
        .map(|payload| payload.source)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn completion_event_types() -> Vec<&'static str> {
    get_all_payloads()
        .map(|payload| payload.event_type)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn postprocess_zsh(script: &str) -> String {
    script
        .replace(
            ":SOURCE:_default",
            ":SOURCE:($(sinexctl completions list-sources 2>/dev/null))",
        )
        .replace(
            ":EVENT_TYPE:_default",
            ":EVENT_TYPE:($(sinexctl completions list-event-types 2>/dev/null))",
        )
        .replace(
            ":TYPE:_default",
            ":TYPE:($(sinexctl completions list-event-types 2>/dev/null))",
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn completion_sources_are_derived_from_payload_inventory() -> TestResult<()> {
        let sources = completion_sources();

        assert!(sources.contains(&"fs-watcher"));
        assert!(sources.contains(&"wm.hyprland"));
        assert_eq!(
            sources,
            sources
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>(),
            "sources should be unique and sorted for stable shell output"
        );
        Ok(())
    }

    #[sinex_test]
    async fn completion_event_types_are_derived_from_payload_inventory() -> TestResult<()> {
        let event_types = completion_event_types();

        assert!(event_types.contains(&"file.created"));
        assert!(event_types.contains(&"window.focused"));
        assert_eq!(
            event_types,
            event_types
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>(),
            "event types should be unique and sorted for stable shell output"
        );
        Ok(())
    }

    #[sinex_test]
    async fn zsh_postprocessor_injects_dynamic_source_and_event_type_lists() -> TestResult<()> {
        let input = "':SOURCE:_default' '*--event-type=[x]:EVENT_TYPE:_default' ':TYPE:_default'";
        let output = postprocess_zsh(input);

        assert!(output.contains("sinexctl completions list-sources"));
        assert!(output.contains("sinexctl completions list-event-types"));
        assert!(!output.contains(":SOURCE:_default"));
        assert!(!output.contains(":EVENT_TYPE:_default"));
        assert!(!output.contains(":TYPE:_default"));
        Ok(())
    }
}
