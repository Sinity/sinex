use std::process::Command;

use clap::{Args, Subcommand};
use color_eyre::eyre::{Result, bail, eyre};
use serde::Serialize;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Rust-analyzer refactor/search helpers.
#[derive(Debug, Clone, Args)]
pub struct RaCommand {
    #[command(subcommand)]
    pub subcommand: RaSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum RaSubcommand {
    /// Run rust-analyzer structured search.
    Search {
        /// Structured search pattern, e.g. `$a.foo($b)`.
        pattern: Vec<String>,
    },
    /// Run rust-analyzer structured search replace.
    Ssr {
        /// Structured search-replace rule, e.g. `$a.foo($b) ==>> bar($a, $b)`.
        rule: Vec<String>,
        /// Apply edits. Without this flag, only print the command that would run.
        #[arg(long)]
        apply: bool,
    },
    /// Run rust-analyzer batch diagnostics.
    Diagnostics {
        /// Minimum severity accepted by rust-analyzer.
        #[arg(long, default_value = "error")]
        severity: String,
        /// Do not run build scripts or load OUT_DIR values.
        #[arg(long)]
        disable_build_scripts: bool,
        /// Do not expand proc macros.
        #[arg(long)]
        disable_proc_macros: bool,
    },
}

#[derive(Debug, Serialize)]
struct RaRunOutput {
    command: Vec<String>,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    applied: bool,
}

impl XtaskCommand for RaCommand {
    fn name(&self) -> &'static str {
        "ra"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            RaSubcommand::Search { pattern } => run_ra(ctx, build_search_args(pattern)?, false),
            RaSubcommand::Ssr { rule, apply } => {
                let args = build_ssr_args(rule)?;
                if !apply {
                    if ctx.is_human() {
                        println!("dry-run: {}", render_command(&args));
                        println!("Pass --apply to run rust-analyzer ssr and modify files.");
                    }
                    return Ok(CommandResult::success()
                        .with_message("rust-analyzer ssr dry-run")
                        .with_detail(render_command(&args))
                        .with_data(serde_json::json!({
                            "command": command_vec(&args),
                            "applied": false,
                        }))
                        .with_duration(ctx.elapsed()));
                }
                run_ra(ctx, args, true)
            }
            RaSubcommand::Diagnostics {
                severity,
                disable_build_scripts,
                disable_proc_macros,
            } => run_ra(
                ctx,
                build_diagnostics_args(severity, *disable_build_scripts, *disable_proc_macros),
                false,
            ),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        match &self.subcommand {
            RaSubcommand::Ssr { apply: true, .. } => CommandMetadata::fix(),
            RaSubcommand::Diagnostics { .. } => CommandMetadata::diagnostics(),
            RaSubcommand::Search { .. } | RaSubcommand::Ssr { apply: false, .. } => {
                CommandMetadata::analysis()
            }
        }
    }
}

fn build_search_args(pattern: &[String]) -> Result<Vec<String>> {
    if pattern.is_empty() {
        bail!("xtask ra search requires a structured search pattern");
    }
    let mut args = vec!["search".to_string()];
    args.extend(pattern.iter().cloned());
    Ok(args)
}

fn build_ssr_args(rule: &[String]) -> Result<Vec<String>> {
    if rule.is_empty() {
        bail!("xtask ra ssr requires a structured search-replace rule");
    }
    let mut args = vec!["ssr".to_string()];
    args.extend(rule.iter().cloned());
    Ok(args)
}

fn build_diagnostics_args(
    severity: &str,
    disable_build_scripts: bool,
    disable_proc_macros: bool,
) -> Vec<String> {
    let mut args = vec![
        "diagnostics".to_string(),
        crate::config::workspace_root().display().to_string(),
        "--severity".to_string(),
        severity.to_string(),
    ];
    if disable_build_scripts {
        args.push("--disable-build-scripts".to_string());
    }
    if disable_proc_macros {
        args.push("--disable-proc-macros".to_string());
    }
    args
}

fn run_ra(ctx: &CommandContext, args: Vec<String>, applied: bool) -> Result<CommandResult> {
    if ctx.is_human() {
        println!("running {}", render_command(&args));
    }

    let output = Command::new("rust-analyzer")
        .args(&args)
        .output()
        .map_err(|error| eyre!("failed to run rust-analyzer: {error}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if ctx.is_human() {
        print!("{stdout}");
        eprint!("{stderr}");
    }

    let data = RaRunOutput {
        command: command_vec(&args),
        exit_code: output.status.code(),
        stdout,
        stderr,
        applied,
    };
    let result = if output.status.success() {
        CommandResult::success()
    } else {
        CommandResult::partial()
    };

    Ok(result
        .with_message(if output.status.success() {
            "rust-analyzer completed"
        } else {
            "rust-analyzer reported failures"
        })
        .with_detail(render_command(&args))
        .with_data(serde_json::to_value(data)?)
        .with_duration(ctx.elapsed()))
}

fn command_vec(args: &[String]) -> Vec<String> {
    std::iter::once("rust-analyzer".to_string())
        .chain(args.iter().cloned())
        .collect()
}

fn render_command(args: &[String]) -> String {
    command_vec(args)
        .into_iter()
        .map(|arg| shell_quote(&arg))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(arg: &str) -> String {
    if arg
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ':' | '='))
    {
        return arg.to_string();
    }
    format!("'{}'", arg.replace('\'', r#"'\''"#))
}
