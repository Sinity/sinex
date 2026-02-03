//! Build command - compile workspace packages with diagnostics capture.
//!
//! Compiler diagnostics (warnings and errors) are captured and stored in the
//! history database for later analysis via `cargo xtask history diagnostics`.

use crate::affected;
use crate::cargo_diagnostics::DiagnosticSummary;
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

/// Parse cargo's JSON output format (duplicated from `cargo_diagnostics` to keep build.rs self-contained)
fn parse_cargo_json_output(output: &str, success: bool) -> Result<DiagnosticSummary> {
    let mut diagnostics = Vec::new();
    let mut errors = 0;
    let mut warnings = 0;

    for line in output.lines() {
        if line.trim().is_empty() {
            continue;
        }

        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            if json.get("reason").and_then(|r| r.as_str()) == Some("compiler-message") {
                if let Some(message) = json.get("message") {
                    if let Some(diag) = parse_diagnostic_message(message) {
                        match diag.level.as_str() {
                            "error" => errors += 1,
                            "warning" => warnings += 1,
                            _ => {}
                        }
                        diagnostics.push(diag);
                    }
                }
            }
        }
    }

    Ok(DiagnosticSummary {
        errors,
        warnings,
        diagnostics,
        success,
    })
}

fn parse_diagnostic_message(
    msg: &serde_json::Value,
) -> Option<crate::cargo_diagnostics::CompilerDiagnostic> {
    let level = msg.get("level")?.as_str()?;
    let message = msg.get("message")?.as_str()?;

    if level == "note" || level == "help" {
        return None;
    }

    let code = msg
        .get("code")
        .and_then(|c| c.get("code"))
        .and_then(|c| c.as_str())
        .map(std::string::ToString::to_string);

    let rendered = msg
        .get("rendered")
        .and_then(|r| r.as_str())
        .map(std::string::ToString::to_string);

    let (file_path, line, column) = if let Some(spans) = msg.get("spans").and_then(|s| s.as_array())
    {
        spans
            .iter()
            .find(|s| s.get("is_primary").and_then(serde_json::Value::as_bool) == Some(true))
            .map_or((None, None, None), |span| {
                (
                    span.get("file_name")
                        .and_then(|f| f.as_str())
                        .map(std::string::ToString::to_string),
                    span.get("line_start")
                        .and_then(serde_json::Value::as_u64)
                        .map(|l| l as u32),
                    span.get("column_start")
                        .and_then(serde_json::Value::as_u64)
                        .map(|c| c as u32),
                )
            })
    } else {
        (None, None, None)
    };

    let suggestion = msg
        .get("children")
        .and_then(|c| c.as_array())
        .and_then(|children| {
            children
                .iter()
                .find(|child| child.get("level").and_then(|l| l.as_str()) == Some("help"))
                .and_then(|help| help.get("message").and_then(|m| m.as_str()))
                .map(std::string::ToString::to_string)
        });

    Some(crate::cargo_diagnostics::CompilerDiagnostic {
        level: level.to_string(),
        code,
        message: message.to_string(),
        file_path,
        line,
        column,
        rendered,
        suggestion,
    })
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
            if self.affected && !self.all {
                args.push("--affected".to_string());
            }
            if self.all {
                args.push("--all".to_string());
            }
            return ctx.spawn_background("build", &args).await;
        }

        // Ensure infrastructure is ready (DB needed for sqlx compile-time checks)
        preflight::ensure_ready(ctx)?;

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
