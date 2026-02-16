//! Check command - fast correctness checks (fmt check + cargo check)
//!
//! This command runs fmt, cargo check, clippy, and forbidden pattern scans.
//! Compiler diagnostics are captured and stored in the history database for
//! later analysis via `cargo xtask history diagnostics`.

use anyhow::Result;

use crate::cargo_diagnostics::{run_cargo_check, run_cargo_clippy, DiagnosticSummary};
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::preflight;
use crate::process::ProcessBuilder;
use crate::resources;

/// Check command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct CheckCommand {
    /// Skip formatting check
    #[arg(long)]
    pub skip_fmt: bool,
    /// Run clippy lints (default: true)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub lint: bool,
    /// Run forbidden pattern scan (default: true)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub forbidden: bool,
    /// Also run slow lints
    #[arg(long)]
    pub heavy: bool,
    /// Only check affected packages (DEFAULT - use --all to check all)
    #[arg(short = 'A', long, default_value_t = true, action = clap::ArgAction::Set)]
    pub affected: bool,
    /// Check ALL packages (disables --affected default)
    #[arg(short = 'a', long)]
    pub all: bool,
    /// Check specific package(s) only
    #[arg(short = 'p', long = "package")]
    pub packages: Vec<String>,
    /// Skip test compilation check (faster, but may miss test errors)
    #[arg(long)]
    pub skip_tests: bool,
    /// Show breakdown of warning counts by lint code (top 10)
    #[arg(long)]
    pub lint_breakdown: bool,
    /// Show breakdown of warning counts by file path (top 20)
    #[arg(long)]
    pub by_file: bool,
}

impl CheckCommand {
    /// Build cargo args based on package scope
    fn build_package_args(&self, include_tests: bool) -> Result<Vec<String>> {
        let mut args = vec!["--all-features".to_string()];

        // Include tests by default (unless skip_tests is set)
        if include_tests && !self.skip_tests {
            args.push("--tests".to_string());
            args.push("--benches".to_string());
            args.push("--examples".to_string());
        }

        if !self.packages.is_empty() {
            for p in &self.packages {
                args.push("-p".to_string());
                args.push(p.clone());
            }
        } else if self.affected && !self.all {
            // --affected is default ON, --all disables it
            let affected_pkgs = crate::affected::affected_packages()?;
            if affected_pkgs.is_empty() {
                eprintln!("  ℹ No affected packages detected — checking full workspace");
                args.push("--workspace".to_string());
            } else {
                for p in affected_pkgs {
                    args.push("-p".to_string());
                    args.push(p);
                }
            }
        } else {
            args.push("--workspace".to_string());
        }

        Ok(args)
    }

    /// Record diagnostics to history and add to result
    fn process_diagnostics(
        &self,
        ctx: &CommandContext,
        summary: &DiagnosticSummary,
        result: &mut CommandResult,
        step_name: &str,
    ) {
        // Record diagnostics to history database
        if let Err(e) = ctx.record_diagnostics(&summary.diagnostics) {
            if ctx.is_human() {
                eprintln!("Warning: failed to record diagnostics: {e}");
            }
        }

        // Add summary to result
        if summary.errors > 0 {
            result.warnings.push(format!(
                "{}: {} error(s), {} warning(s)",
                step_name, summary.errors, summary.warnings
            ));
        } else if summary.warnings > 0 {
            result
                .warnings
                .push(format!("{}: {} warning(s)", step_name, summary.warnings));
        }
    }
}

#[async_trait::async_trait]
impl XtaskCommand for CheckCommand {
    fn name(&self) -> &'static str {
        "check"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution
        if ctx.is_background() {
            let mut args = Vec::new();
            if self.skip_fmt {
                args.push("--skip-fmt".to_string());
            }
            if !self.lint {
                args.push("--lint=false".to_string());
            }
            if !self.forbidden {
                args.push("--forbidden=false".to_string());
            }
            if self.heavy {
                args.push("--heavy".to_string());
            }
            if self.all {
                args.push("--all".to_string());
            } else if !self.affected {
                args.push("--affected=false".to_string());
            }
            if self.skip_tests {
                args.push("--skip-tests".to_string());
            }
            if self.lint_breakdown {
                args.push("--lint-breakdown".to_string());
            }
            if self.by_file {
                args.push("--by-file".to_string());
            }
            for p in &self.packages {
                args.push("-p".to_string());
                args.push(p.clone());
            }

            // Coordinate with other concurrent check invocations
            if crate::coordinator::JobCoordinator::should_coordinate("check", &args) {
                if let Ok(coordinator) = crate::coordinator::JobCoordinator::new() {
                    if let Ok(result) = coordinator.request("check", &args, false) {
                        return Ok(coordination_to_result(&result, ctx));
                    }
                }
            }

            return ctx.spawn_background("check", &args).await;
        }

        // Ensure infrastructure is ready (DB needed for sqlx compile-time checks)
        preflight::ensure_ready(ctx)?;

        // Resource warning before heavy operation
        if ctx.is_human() {
            if let Ok(status) = resources::ResourceStatus::capture() {
                if let Some(warning) = status.warning(resources::thresholds::CARGO_CHECK_GB) {
                    eprintln!("  ⚠ {warning}");
                }
            }
        }

        let mut result = CommandResult::success();
        let package_args = self.build_package_args(true)?;

        // 1. Formatting
        if !self.skip_fmt {
            if ctx.is_human() {
                println!("Checking formatting...");
            }
            ProcessBuilder::cargo()
                .args(["fmt", "--all", "--", "--check"])
                .with_description("cargo fmt --check")
                .inherit_output()
                .run_ok()?;
            result = result.with_detail("fmt check passed");
        }

        // 2. Cargo Check (with diagnostics capture)
        if ctx.is_human() {
            println!("Checking compilation...");
        }

        let check_args: Vec<&str> = package_args
            .iter()
            .map(std::string::String::as_str)
            .collect();
        let check_summary = run_cargo_check(&check_args)?;

        // Show rendered output for humans
        if ctx.is_human() {
            for diag in &check_summary.diagnostics {
                if let Some(rendered) = &diag.rendered {
                    eprint!("{rendered}");
                }
            }
        }

        self.process_diagnostics(ctx, &check_summary, &mut result, "cargo check");

        if !check_summary.success {
            return Ok(result.with_detail("cargo check failed"));
        }
        result = result.with_detail("cargo check passed");

        // 3. Clippy (with diagnostics capture)
        if self.lint {
            if ctx.is_human() {
                println!("Running clippy...");
            }

            // Include tests in clippy unless skip_tests is set
            let clippy_args: Vec<&str> = package_args
                .iter()
                .map(std::string::String::as_str)
                .collect();
            let clippy_summary = run_cargo_clippy(&clippy_args)?;

            // Show rendered output for humans
            if ctx.is_human() {
                for diag in &clippy_summary.diagnostics {
                    if let Some(rendered) = &diag.rendered {
                        eprint!("{rendered}");
                    }
                }
            }

            self.process_diagnostics(ctx, &clippy_summary, &mut result, "clippy");

            // Show lint breakdown if requested or if there are many warnings
            if self.lint_breakdown || clippy_summary.warnings > 50 {
                let top_lints = clippy_summary.top_lints(10);
                if !top_lints.is_empty() {
                    if ctx.is_human() {
                        println!("\n📊 Top clippy warnings by lint:");
                        for lint in &top_lints {
                            println!("  {:>4}  {}", lint.count, lint.code);
                        }
                        println!();
                    }
                    // Add to JSON data
                    result = result.with_data(serde_json::json!({
                        "lint_breakdown": top_lints
                    }));
                }
            }

            // Show file breakdown if requested
            if self.by_file {
                let top_files = clippy_summary.top_files(20);
                if !top_files.is_empty() {
                    if ctx.is_human() {
                        println!("📁 Top files by warning count:");
                        for file in &top_files {
                            println!("  {:>4}  {}", file.count, file.path);
                        }
                        println!();
                    }
                    // Add to JSON data
                    result = result.with_data(serde_json::json!({
                        "file_breakdown": top_files
                    }));
                }
            }

            if !clippy_summary.success {
                return Ok(result.with_detail("clippy failed"));
            }
            result = result.with_detail("clippy passed");
        }

        // 4. Forbidden patterns
        if self.forbidden {
            if ctx.is_human() {
                println!("Scanning for forbidden patterns...");
            }
            crate::commands::lint_forbidden::LintForbiddenCommand
                .execute(ctx)
                .await?;
            result = result.with_detail("forbidden pattern scan passed");
        }

        // Add diagnostic counts to result data
        let diagnostics_data = serde_json::json!({
            "diagnostics_recorded": ctx.invocation_id().is_some()
        });
        result = result.with_data(diagnostics_data);

        Ok(result.with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

/// Convert a coordination result to a command result for the --bg path.
pub(crate) fn coordination_to_result(
    result: &crate::coordinator::CoordinationResult,
    ctx: &CommandContext,
) -> CommandResult {
    use crate::coordinator::CoordinationResult as CR;
    match result {
        CR::Fresh {
            job_id,
            status,
            duration_secs,
        } => {
            if ctx.is_human() {
                println!("✅ Fresh: last check already validated this code state (job {job_id}, {status} in {duration_secs:.1}s)");
            }
            CommandResult::success()
                .with_message(format!("Fresh result from job {job_id}"))
                .with_data(serde_json::json!({
                    "action": "fresh",
                    "job_id": job_id,
                    "cached_status": status,
                    "cached_duration_secs": duration_secs,
                }))
        }
        CR::Attached { job_id } => {
            if ctx.is_human() {
                println!("🔗 Attached: identical check already running (job {job_id})");
                println!("   Monitor: cargo xtask jobs status {job_id}");
            }
            CommandResult::success()
                .with_message(format!("Attached to running job {job_id}"))
                .with_data(serde_json::json!({
                    "action": "attached",
                    "job_id": job_id,
                    "hint": format!("Monitor with: cargo xtask jobs status {job_id}"),
                }))
        }
        CR::Superseded {
            old_job_id,
            new_job_id,
        } => {
            if ctx.is_human() {
                println!("♻ Superseded: cancelled stale job {old_job_id}, starting fresh job {new_job_id}");
            }
            CommandResult::success()
                .with_message(format!("Superseded job {old_job_id} with {new_job_id}"))
                .with_data(serde_json::json!({
                    "action": "superseded",
                    "old_job_id": old_job_id,
                    "new_job_id": new_job_id,
                }))
        }
        CR::Queued { current_job_id } => {
            if ctx.is_human() {
                println!("⏳ Queued: waiting for job {current_job_id} to complete");
            }
            CommandResult::success()
                .with_message(format!("Queued behind job {current_job_id}"))
                .with_data(serde_json::json!({
                    "action": "queued",
                    "current_job_id": current_job_id,
                }))
        }
        CR::Started { job_id } => {
            // This shouldn't normally be returned in the --bg path since
            // we proceed to spawn_background after, but handle it for completeness
            CommandResult::success()
                .with_message(format!("Started job {job_id}"))
                .with_data(serde_json::json!({
                    "action": "started",
                    "job_id": job_id,
                }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_command_metadata() {
        let cmd = CheckCommand {
            skip_fmt: false,
            lint: true,
            forbidden: true,
            heavy: false,
            affected: false,
            all: false,
            packages: vec![],
            skip_tests: false,
            lint_breakdown: false,
            by_file: false,
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("check".to_string()));
        assert!(metadata.timeout.is_some());
    }

    #[test]
    fn test_check_command_name() {
        let cmd = CheckCommand {
            skip_fmt: true,
            lint: false,
            forbidden: false,
            heavy: false,
            affected: false,
            all: false,
            packages: vec![],
            skip_tests: false,
            lint_breakdown: false,
            by_file: false,
        };

        assert_eq!(cmd.name(), "check");
    }
}
