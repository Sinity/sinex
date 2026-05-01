use clap::CommandFactory;
use serde::Serialize;

/// Serializable command-argument metadata derived from clap introspection.
#[derive(Debug, Clone, Serialize)]
pub struct ArgInfo {
    pub name: String,
    pub short: Option<char>,
    pub long: Option<String>,
    pub help: Option<String>,
    pub required: bool,
    pub global: bool,
    pub possible_values: Vec<String>,
    pub takes_value: bool,
}

/// Serializable command metadata derived from clap introspection.
#[derive(Debug, Clone, Serialize)]
pub struct CommandInfo {
    pub name: String,
    pub about: Option<String>,
    pub subcommands: Vec<CommandInfo>,
    pub args: Vec<ArgInfo>,
}

// Commands excluded from the public discovery surface and generated docs.
const HIDDEN_COMMANDS: &[&str] = &["ci", "completions", "help"];

/// Collect the public xtask command tree from clap introspection.
#[must_use]
pub fn collect_command_catalog() -> Vec<CommandInfo> {
    let cli = crate::Cli::command();
    extract_commands(&cli)
}

/// Collect global xtask flags from clap introspection.
#[must_use]
pub fn collect_global_args() -> Vec<ArgInfo> {
    let cli = crate::Cli::command();
    cli.get_arguments().map(extract_arg).collect()
}

#[must_use]
pub fn find_command<'a>(commands: &'a [CommandInfo], path: &str) -> Option<&'a CommandInfo> {
    let mut parts = path.split_whitespace();
    let first = parts.next()?;
    let mut current = commands.iter().find(|command| command.name == first)?;
    for part in parts {
        current = current
            .subcommands
            .iter()
            .find(|command| command.name == part)?;
    }
    Some(current)
}

fn extract_commands(cmd: &clap::Command) -> Vec<CommandInfo> {
    cmd.get_subcommands()
        .filter(|sub| !HIDDEN_COMMANDS.contains(&sub.get_name()))
        .map(|sub| CommandInfo {
            name: sub.get_name().to_string(),
            about: sub.get_about().map(ToString::to_string),
            subcommands: extract_commands(sub),
            args: sub.get_arguments().map(extract_arg).collect(),
        })
        .collect()
}

fn extract_arg(arg: &clap::Arg) -> ArgInfo {
    ArgInfo {
        name: arg.get_id().to_string(),
        short: arg.get_short(),
        long: arg.get_long().map(String::from),
        help: arg.get_help().map(ToString::to_string),
        required: arg.is_required_set(),
        global: arg.is_global_set(),
        possible_values: arg
            .get_possible_values()
            .iter()
            .map(|value| value.get_name().to_string())
            .collect(),
        takes_value: matches!(
            arg.get_action(),
            clap::ArgAction::Set | clap::ArgAction::Append
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::prelude::*;

    #[sinex_test(
        scenario = "xtask.command-catalog-core-surface.v1",
        category = "command_contract",
        lane = "fast",
        cost_tier = "fast",
        tags = "command_contract,catalog,xtask",
        fixtures = "clap_command_tree",
        subjects = "xtask_command:check,xtask_command:test,xtask_command:docs,issue:485",
        claims = "claim:xtask.command_catalog_introspection",
        reproducer = "xtask test -p xtask --scenario-tag command_contract"
    )]
    async fn command_catalog_exposes_core_public_surface(_ctx: TestContext) -> TestResult<()> {
        let commands = collect_command_catalog();

        assert!(
            commands.len() >= 15,
            "public command catalog unexpectedly shrank to {} entries",
            commands.len()
        );
        for command in ["check", "test", "build", "status", "docs", "schema"] {
            assert!(
                find_command(&commands, command).is_some(),
                "missing public xtask command `{command}`"
            );
        }
        assert!(
            find_command(&commands, "schema strict-diff").is_some(),
            "strict schema drift check must stay discoverable"
        );
        assert!(
            find_command(&commands, "docs proof-catalog").is_some(),
            "proof catalog command must stay discoverable"
        );

        let global_args = collect_global_args();
        for arg in ["json", "list_commands", "bg"] {
            assert!(
                global_args.iter().any(|candidate| candidate.name == arg),
                "missing global xtask arg `{arg}`"
            );
        }
        Ok(())
    }
}
