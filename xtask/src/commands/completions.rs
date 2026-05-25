//! Completions command - generate shell completions for xtask

use clap_complete::{generate, shells};
use color_eyre::eyre::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::cargo_command;

/// Completions subcommand variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum CompletionsSubcommand {
    /// Generate Bash completions
    Bash,
    /// Generate Zsh completions (with dynamic package/target values)
    Zsh,
    /// Generate Fish completions
    Fish,
    /// Generate PowerShell completions
    PowerShell,
    /// List workspace package names (for shell completion scripts)
    #[command(hide = true)]
    ListPackages,
    /// List run target names (for shell completion scripts)
    #[command(hide = true)]
    ListRunTargets,
}

/// Completions command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct CompletionsCommand {
    #[command(subcommand)]
    pub subcommand: CompletionsSubcommand,
}

/// Workspace packages from cargo metadata (fast, no graph traversal needed)
fn list_workspace_packages() -> Result<Vec<String>> {
    let out = cargo_command()
        .args(["metadata", "--format-version=1", "--no-deps", "--quiet"])
        .output();

    match out {
        Ok(output) => workspace_packages_from_metadata_output(&output),
        Err(error) => Err(color_eyre::eyre::eyre!(
            "failed to invoke cargo metadata: {error}"
        )),
    }
}

fn workspace_packages_from_metadata_output(output: &std::process::Output) -> Result<Vec<String>> {
    use color_eyre::eyre::{Context, eyre};

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return match output.status.code() {
            Some(code) if stderr.is_empty() => {
                Err(eyre!("cargo metadata failed with exit code {code}"))
            }
            Some(code) => Err(eyre!(
                "cargo metadata failed with exit code {code}: {stderr}"
            )),
            None if stderr.is_empty() => Err(eyre!("cargo metadata terminated by signal")),
            None => Err(eyre!("cargo metadata terminated by signal: {stderr}")),
        };
    }

    let meta: serde_json::Value = serde_json::from_slice(&output.stdout)
        .context("failed to parse cargo metadata JSON for completions")?;

    let packages = meta["packages"]
        .as_array()
        .ok_or_else(|| eyre!("cargo metadata JSON omitted packages array"))?;

    let mut names = Vec::with_capacity(packages.len());
    for (index, package) in packages.iter().enumerate() {
        let name = package["name"]
            .as_str()
            .ok_or_else(|| eyre!("cargo metadata package entry {index} omitted string name"))?;
        names.push(name.to_string());
    }
    names.sort();
    Ok(names)
}

/// Post-process a generated zsh completion script to inject dynamic package and run-target
/// completions.
///
/// clap_complete generates `:PACKAGES:_default` for Vec<String> args with short `-p`. We
/// replace that with a dynamic call to `xtask completions list-packages` so the shell
/// queries the actual workspace at tab-complete time.
///
/// Similarly, the run `node` subcommand's `<NAME>` argument gets wired to
/// `xtask completions list-run-targets`.
fn postprocess_zsh(script: &str) -> String {
    // Replace `:PACKAGES:_default` with a call to xtask for the real package list.
    // This covers -p / --package in check, test, build, fix.
    let script = script.replace(
        ":PACKAGES:_default",
        ":PACKAGES:($(xtask completions list-packages 2>/dev/null))",
    );

    // The run node NAME arg shows as :NAME:_default in the generated completions.
    // Replace it with dynamic run-target completion.
    script.replace(
        "':NAME:_default'",
        "':NAME:($(xtask completions list-run-targets 2>/dev/null))'",
    )
}

fn ensure_completion_bin_names(cmd: &mut clap::Command, parent_bin_name: &str) {
    for subcommand in cmd.get_subcommands_mut() {
        let bin_name = format!("{parent_bin_name} {}", subcommand.get_name());
        subcommand.set_bin_name(bin_name.clone());
        ensure_completion_bin_names(subcommand, &bin_name);
    }
}

fn prepare_completion_command(command: clap::Command) -> clap::Command {
    command
        .disable_help_subcommand(true)
        .mut_subcommands(prepare_completion_command)
}

fn shell_words<'a>(words: impl IntoIterator<Item = &'a str>) -> String {
    words.into_iter().collect::<Vec<_>>().join(" ")
}

fn command_options(cmd: &clap::Command) -> Vec<String> {
    let mut options = Vec::new();
    for arg in cmd.get_opts().filter(|arg| !arg.is_hide_set()) {
        if let Some(short) = arg.get_short() {
            options.push(format!("-{short}"));
        }
        if let Some(long) = arg.get_long() {
            options.push(format!("--{long}"));
        }
    }
    options.sort();
    options.dedup();
    options
}

fn generate_basic_bash(cmd: &clap::Command) {
    let commands = cmd
        .get_subcommands()
        .filter(|subcommand| !subcommand.is_hide_set())
        .map(clap::Command::get_name)
        .collect::<Vec<_>>();
    let global_options = command_options(cmd);
    let completions_subcommands = cmd
        .find_subcommand("completions")
        .map(|subcommand| {
            subcommand
                .get_subcommands()
                .filter(|nested| !nested.is_hide_set())
                .map(clap::Command::get_name)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    println!(
        r#"_xtask() {{
    local cur command
    COMPREPLY=()
    cur="${{COMP_WORDS[COMP_CWORD]}}"
    command="${{COMP_WORDS[1]}}"

    if [[ $COMP_CWORD -eq 1 ]]; then
        COMPREPLY=( $(compgen -W "{commands} {global_options}" -- "$cur") )
        return 0
    fi

    case "$command" in
        completions)
            COMPREPLY=( $(compgen -W "{completion_shells}" -- "$cur") )
            return 0
            ;;
    esac

    COMPREPLY=( $(compgen -W "{global_options}" -- "$cur") )
}}
complete -F _xtask xtask"#,
        commands = shell_words(commands),
        global_options = global_options.join(" "),
        completion_shells = shell_words(completions_subcommands),
    );
}

impl CompletionsCommand {
    /// Generate completions for the given CLI command.
    pub fn generate_for(subcommand: &CompletionsSubcommand) -> Result<()> {
        use clap::CommandFactory;
        let mut cmd = prepare_completion_command(crate::Cli::command()).bin_name("xtask");
        cmd.build();
        ensure_completion_bin_names(&mut cmd, "xtask");
        let name = cmd.get_name().to_string();

        match subcommand {
            CompletionsSubcommand::Bash => {
                generate_basic_bash(&cmd);
            }
            CompletionsSubcommand::Zsh => {
                let mut buf = Vec::new();
                generate(shells::Zsh, &mut cmd, &name, &mut buf);
                let raw = String::from_utf8_lossy(&buf);
                print!("{}", postprocess_zsh(&raw));
            }
            CompletionsSubcommand::Fish => {
                generate(shells::Fish, &mut cmd, name, &mut std::io::stdout());
            }
            CompletionsSubcommand::PowerShell => {
                generate(shells::PowerShell, &mut cmd, name, &mut std::io::stdout());
            }
            CompletionsSubcommand::ListPackages => {
                for pkg in list_workspace_packages()? {
                    println!("{pkg}");
                }
            }
            CompletionsSubcommand::ListRunTargets => {
                for target in crate::commands::run::list_run_targets() {
                    println!("{target}");
                }
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
        Self::generate_for(&self.subcommand)?;
        // List commands and completion scripts write directly to stdout — suppress the
        // JSON wrapper so the output is clean for shell subshell consumption.
        Ok(CommandResult::success().with_silent())
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::utility()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use std::os::unix::process::ExitStatusExt;

    #[sinex_test]
    async fn test_completions_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = CompletionsCommand {
            subcommand: CompletionsSubcommand::Bash,
        };
        assert_eq!(cmd.name(), "completions");
        Ok(())
    }

    #[sinex_test]
    async fn test_completions_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = CompletionsCommand {
            subcommand: CompletionsSubcommand::Zsh,
        };
        let metadata = cmd.metadata();

        assert_eq!(metadata.category, Some("utility"));
        assert!(!metadata.track_in_history);
        assert!(!metadata.modifies_state);
        Ok(())
    }

    #[sinex_test]
    async fn test_all_subcommand_variants() -> ::xtask::sandbox::TestResult<()> {
        for sub in [
            CompletionsSubcommand::Bash,
            CompletionsSubcommand::Zsh,
            CompletionsSubcommand::Fish,
            CompletionsSubcommand::PowerShell,
            CompletionsSubcommand::ListPackages,
            CompletionsSubcommand::ListRunTargets,
        ] {
            let cmd = CompletionsCommand { subcommand: sub };
            assert_eq!(cmd.name(), "completions");
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_list_run_targets_non_empty() -> ::xtask::sandbox::TestResult<()> {
        let targets = crate::commands::run::list_run_targets();
        assert!(!targets.is_empty(), "run targets should not be empty");
        assert!(targets.contains(&"ingestd".to_string()));
        assert!(targets.contains(&"core".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_postprocess_zsh_packages() -> ::xtask::sandbox::TestResult<()> {
        let input = "':PACKAGES:_default'";
        let output = postprocess_zsh(input);
        assert!(
            output.contains("xtask completions list-packages"),
            "zsh post-processor should inject dynamic package completion"
        );
        assert!(
            !output.contains(":PACKAGES:_default"),
            "zsh post-processor should remove static fallback"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_workspace_packages_from_metadata_output_reports_invalid_json()
    -> ::xtask::sandbox::TestResult<()> {
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: br#"{"packages":"nope"}"#.to_vec(),
            stderr: Vec::new(),
        };

        let error = workspace_packages_from_metadata_output(&output)
            .expect_err("invalid cargo metadata JSON should surface");
        assert!(error.to_string().contains("packages array"));
        Ok(())
    }

    #[sinex_test]
    async fn test_workspace_packages_from_metadata_output_reports_failed_status()
    -> ::xtask::sandbox::TestResult<()> {
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(2 << 8),
            stdout: Vec::new(),
            stderr: b"metadata boom".to_vec(),
        };

        let error = workspace_packages_from_metadata_output(&output)
            .expect_err("cargo metadata failure should surface");
        assert!(error.to_string().contains("exit code 2"));
        assert!(error.to_string().contains("metadata boom"));
        Ok(())
    }

    #[sinex_test]
    async fn test_workspace_packages_from_metadata_output_reports_missing_package_name()
    -> ::xtask::sandbox::TestResult<()> {
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: br#"{"packages":[{"version":"0.1.0"}]}"#.to_vec(),
            stderr: Vec::new(),
        };

        let error = workspace_packages_from_metadata_output(&output)
            .expect_err("metadata entries without names should surface");
        assert!(error.to_string().contains("package entry 0"));
        assert!(error.to_string().contains("name"));
        Ok(())
    }
}
