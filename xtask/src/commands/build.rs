//! Build command - compile workspace packages with diagnostics capture.
//!
//! Compiler diagnostics (warnings and errors) are captured and stored in the
//! history database for later analysis via `xtask history diagnostics`.

use crate::affected;
use crate::cargo_diagnostics::{estimate_package_count, run_cargo_build_streaming};
use crate::command::{CommandContext, CommandMetadata, CommandResult, WorkloadScope, XtaskCommand};
use crate::preflight;
use color_eyre::eyre::Result;

/// Build workspace packages while capturing compiler diagnostics.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct BuildCommand {
    /// Packages to build (default: all)
    #[arg(short = 'p', long = "package")]
    pub packages: Vec<String>,
    /// Build in release mode
    #[arg(short, long)]
    pub release: bool,
    /// Build ALL packages (disables affected mode default)
    #[arg(short, long)]
    pub all: bool,

    /// Print what would happen without building
    #[arg(long)]
    pub dry_run: bool,
}

impl XtaskCommand for BuildCommand {
    fn name(&self) -> &'static str {
        "build"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution
        if ctx.is_background() {
            let mut args = Vec::new();
            for p in &self.packages {
                args.push("-p".to_string());
                args.push(p.clone());
            }
            if self.release {
                args.push("--release".to_string());
            }
            if self.all {
                args.push("--all".to_string());
            }

            return crate::coordinator::coordinate_and_spawn("build", &args, ctx);
        }

        // Guard: same deadlock as xtask test — cargo target/ lock is held by nextest for the
        // entire run. Detect via NEXTEST_RUN_ID and fail immediately instead of hanging.
        if std::env::var("NEXTEST_RUN_ID").is_ok() {
            return Err(color_eyre::eyre::eyre!(
                "Cannot run `xtask build` foreground inside an active nextest run — \
                 the cargo target/ lock would deadlock.\n\
                 Use `xtask build --bg ...` to spawn in background instead."
            ));
        }

        // Ensure infrastructure is ready (DB needed for sqlx compile-time checks)
        let stage = ctx.start_stage("preflight");
        let ready = preflight::ensure_ready(ctx);
        ctx.finish_stage(stage, ready.is_ok());
        ready?;

        // Record fingerprint+scope for coordinator freshness detection.
        {
            let mut scope_args = Vec::new();
            for p in &self.packages {
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

        let mut packages = self.packages.clone();
        let workload_scope;

        // Affected mode is default ON, --all disables it
        if !self.all {
            let stage = ctx.start_stage("affected");
            let affected = affected::affected_packages();
            ctx.finish_stage(stage, affected.is_ok());
            let mut affected = affected?;
            if affected.is_empty() {
                if ctx.is_human() {
                    println!("No changes detected. Building ALL packages.");
                }
                // Fall through to build all (packages is empty -> --workspace)
                workload_scope = if packages.is_empty() {
                    WorkloadScope::Workspace
                } else {
                    packages.sort();
                    WorkloadScope::Packages(packages.clone())
                };
            } else {
                affected.sort();
                packages.extend(affected);
                packages.sort();
                packages.dedup();
                workload_scope = if self.packages.is_empty() {
                    WorkloadScope::Affected(packages.clone())
                } else {
                    WorkloadScope::Packages(packages.clone())
                };
            }
        } else if packages.is_empty() {
            workload_scope = WorkloadScope::Workspace;
        } else {
            packages.sort();
            packages.dedup();
            workload_scope = WorkloadScope::Packages(packages.clone());
        }

        if packages.is_empty() {
            args.push("--workspace".to_string());
        } else {
            for p in &packages {
                args.push("-p".to_string());
                args.push(p.clone());
            }
        }

        let mut workload_args = Vec::new();
        if self.release {
            workload_args.push("--release".to_string());
        }
        workload_args.push(workload_scope.encode_marker());
        ctx.record_invocation_args(&workload_args);

        if ctx.is_human() {
            println!("Building packages (args: {args:?})...");
        }

        if self.dry_run {
            return Ok(
                CommandResult::success().with_detail("dry-run passed (would build packages)")
            );
        }

        let args_refs: Vec<&str> = args.iter().map(std::string::String::as_str).collect();

        // Estimate package count for determinate progress (fast, no rustc invocation).
        let pkg_total = estimate_package_count(&args_refs);

        let stage = ctx.start_stage("build");
        if pkg_total > 0 {
            ctx.report_progress_full(
                "build",
                Some(0.0),
                Some(0),
                Some(pkg_total as i64),
                "determinate",
                Some("packages"),
                None,
                "rough",
                Some(&format!("0/{pkg_total} packages (0%)")),
            );
        }
        let summary = run_cargo_build_streaming(&args_refs, |n| {
            if pkg_total > 0 {
                let pct = (n as f64 / pkg_total as f64 * 100.0).min(100.0);
                ctx.report_progress_full(
                    "build",
                    Some(pct),
                    Some(n as i64),
                    Some(pkg_total as i64),
                    "determinate",
                    Some("packages"),
                    None,
                    "rough",
                    Some(&format!("{n}/{pkg_total} packages ({pct:.0}%)")),
                );
            }
        })?;
        ctx.finish_stage(stage, summary.success);

        // Show rendered output for humans
        if ctx.is_human() {
            for diag in &summary.diagnostics {
                if let Some(rendered) = &diag.rendered {
                    eprint!("{rendered}");
                }
            }
        }

        // Record diagnostics to history database
        if let Err(e) = ctx.record_diagnostics(&summary.diagnostics)
            && ctx.is_human()
        {
            eprintln!("Warning: failed to record diagnostics: {e}");
        }

        // Add diagnostic data (include affected packages if used)
        let mut data = serde_json::json!({
            "errors": summary.errors,
            "warnings": summary.warnings,
            "diagnostics_recorded": ctx.invocation_id().is_some()
        });
        if !packages.is_empty() {
            data["packages"] = serde_json::json!(packages);
        }

        if !summary.success {
            let mut failure = CommandResult::failure(crate::output::StructuredError {
                code: "BUILD_FAILED".to_string(),
                message: format!(
                    "build failed: {} error(s), {} warning(s)",
                    summary.errors, summary.warnings
                ),
                location: Some("build".to_string()),
                suggestion: Some("Run `xtask check` to inspect diagnostics".to_string()),
            })
            .with_detail("build failed");
            failure.data = Some(data);
            return Ok(failure.with_duration(ctx.elapsed()));
        }

        let mut result = CommandResult::success();
        if summary.warnings > 0 {
            result = result.with_warning(format!("build: {} warning(s)", summary.warnings));
        }
        result = result.with_data(data).with_detail("build passed");

        Ok(result.with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
