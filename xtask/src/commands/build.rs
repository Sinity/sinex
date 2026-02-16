//! Build command - compile workspace packages with diagnostics capture.
//!
//! Compiler diagnostics (warnings and errors) are captured and stored in the
//! history database for later analysis via `cargo xtask history diagnostics`.

use crate::affected;
use crate::cargo_diagnostics::{parse_cargo_json_output, DiagnosticSummary};
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::preflight;
use anyhow::Result;
use std::process::{Command, Stdio};

#[derive(Debug, Clone, clap::Args)]
pub struct BuildCommand {
    /// Packages to build (default: all)
    #[arg(short, long)]
    pub package: Vec<String>,
    /// Build in release mode
    #[arg(short, long)]
    pub release: bool,
    /// Only build affected packages (DEFAULT - use --all to build all)
    #[arg(short = 'A', long, default_value_t = true, action = clap::ArgAction::Set)]
    pub affected: bool,
    /// Build ALL packages (disables --affected default)
    #[arg(short, long)]
    pub all: bool,
}

impl BuildCommand {
    /// Run cargo build with JSON output and parse diagnostics
    fn run_cargo_build(&self, args: &[&str]) -> Result<DiagnosticSummary> {
        let mut cmd_args = vec!["build", "--message-format=json"];
        cmd_args.extend(args);

        let output = Command::new("cargo")
            .args(&cmd_args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_cargo_json_output(&stdout, output.status.success())
    }
}

#[async_trait::async_trait]
impl XtaskCommand for BuildCommand {
    fn name(&self) -> &'static str {
        "build"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution
        if ctx.is_background() {
            let mut args = Vec::new();
            for p in &self.package {
                args.push("-p".to_string());
                args.push(p.clone());
            }
            if self.release {
                args.push("--release".to_string());
            }
            if self.all {
                args.push("--all".to_string());
            } else if !self.affected {
                args.push("--affected=false".to_string());
            }

            return crate::coordinator::coordinate_and_spawn("build", &args, ctx);
        }

        // Ensure infrastructure is ready (DB needed for sqlx compile-time checks)
        preflight::ensure_ready(ctx)?;

        // Record fingerprint+scope for coordinator freshness detection.
        {
            let mut scope_args = Vec::new();
            for p in &self.package {
                scope_args.push("-p".to_string());
                scope_args.push(p.clone());
            }
            if self.release {
                scope_args.push("--release".to_string());
            }
            if self.all {
                scope_args.push("--all".to_string());
            }
            ctx.record_coordination_fingerprint("build", &scope_args);
        }

        let mut args: Vec<String> = Vec::new();

        if self.release {
            args.push("--release".to_string());
        }

        let mut packages = self.package.clone();

        // --affected is default ON, --all disables it
        if self.affected && !self.all {
            let affected = affected::affected_packages()?;
            if affected.is_empty() {
                if ctx.is_human() {
                    println!("No packages affected by current changes.");
                }
                return Ok(CommandResult::success());
            }
            packages.extend(affected);
        }

        if packages.is_empty() {
            args.push("--workspace".to_string());
        } else {
            for p in &packages {
                args.push("-p".to_string());
                args.push(p.clone());
            }
        }

        if ctx.is_human() {
            println!("Building packages...");
        }

        let args_refs: Vec<&str> = args.iter().map(std::string::String::as_str).collect();
        let summary = self.run_cargo_build(&args_refs)?;

        // Show rendered output for humans
        if ctx.is_human() {
            for diag in &summary.diagnostics {
                if let Some(rendered) = &diag.rendered {
                    eprint!("{rendered}");
                }
            }
        }

        // Record diagnostics to history database
        if let Err(e) = ctx.record_diagnostics(&summary.diagnostics) {
            if ctx.is_human() {
                eprintln!("Warning: failed to record diagnostics: {e}");
            }
        }

        let mut result = CommandResult::success();

        // Add summary info
        if summary.errors > 0 {
            result = result.with_warning(format!(
                "build: {} error(s), {} warning(s)",
                summary.errors, summary.warnings
            ));
        } else if summary.warnings > 0 {
            result = result.with_warning(format!("build: {} warning(s)", summary.warnings));
        }

        // Add diagnostic data
        result = result.with_data(serde_json::json!({
            "errors": summary.errors,
            "warnings": summary.warnings,
            "diagnostics_recorded": ctx.invocation_id().is_some()
        }));

        if summary.success {
            result = result.with_detail("build passed");
        } else {
            result = result.with_detail("build failed");
        }

        Ok(result.with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
