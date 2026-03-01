//! Check command — compilation, linting, and pattern verification.
//!
//! Pipeline: [fmt] → [clippy | cargo check] → [forbidden patterns].
//! Defaults to compile-only (cargo check, ~3s warm). Use additive flags to escalate:
//!   --lint      run clippy (~20s warm, subsumes cargo check)
//!   --fmt       run cargo fmt --check (~1s extra)
//!   --forbidden run forbidden pattern scan (~1s extra)
//!   --full      shorthand for --fmt --lint --forbidden (~25s warm)
//!
//! Compiler diagnostics are captured and stored in the history database for
//! later analysis via `xtask history diagnostics`.

use color_eyre::eyre::Result;

use crate::cargo_diagnostics::{DiagnosticSummary, run_cargo_check, run_cargo_clippy};
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::preflight;
use crate::process::ProcessBuilder;
use crate::resources;

/// Check command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct CheckCommand {
    /// Run clippy lints (slower, ~20s warm — subsumes cargo check)
    #[arg(long)]
    pub lint: bool,
    /// Run cargo fmt --check
    #[arg(long)]
    pub fmt: bool,
    /// Run forbidden pattern scan
    #[arg(long)]
    pub forbidden: bool,
    /// Full pipeline: fmt + clippy + forbidden (~25s warm)
    #[arg(long)]
    pub full: bool,
    /// Auto-fix formatting if fmt check fails (safe, always reversible)
    #[arg(long)]
    pub fix_fmt: bool,
    /// Auto-fix fmt + clippy suggestions, then run full check (equivalent to: xtask fix && xtask check --full)
    #[arg(long)]
    pub fix: bool,
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
    /// Resolve composite flags into individual flags (mutates self).
    fn resolve_flags(&mut self) {
        // --fix implies --fix-fmt and full pipeline
        if self.fix {
            self.fix_fmt = true;
            self.full = true;
        }
        if self.full {
            self.lint = true;
            self.fmt = true;
            self.forbidden = true;
        }
    }

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

    /// Record diagnostics and compiled packages to history, add summary to result
    fn process_diagnostics(
        &self,
        ctx: &CommandContext,
        summary: &DiagnosticSummary,
        result: &mut CommandResult,
        step_name: &str,
    ) {
        // Record diagnostics to history database
        if let Err(e) = ctx.record_diagnostics(&summary.diagnostics)
            && ctx.is_human()
        {
            eprintln!("Warning: failed to record diagnostics: {e}");
        }

        // Record which packages were compiled (for package-scoped supersession)
        if let Err(e) = ctx.record_compiled_packages(&summary.compiled_packages)
            && ctx.is_human()
        {
            eprintln!("Warning: failed to record compiled packages: {e}");
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
        // Resolve --full before anything else
        let mut this = self.clone();
        this.resolve_flags();

        // Handle background execution
        if ctx.is_background() {
            let mut args = Vec::new();
            if this.lint {
                args.push("--lint".to_string());
            }
            if this.fmt {
                args.push("--fmt".to_string());
            }
            if this.forbidden {
                args.push("--forbidden".to_string());
            }
            if this.full {
                args.push("--full".to_string());
            }
            if this.fix_fmt {
                args.push("--fix-fmt".to_string());
            }
            if this.fix {
                args.push("--fix".to_string());
            }
            if this.heavy {
                args.push("--heavy".to_string());
            }
            if this.all {
                args.push("--all".to_string());
            } else if !this.affected {
                args.push("--affected=false".to_string());
            }
            if this.skip_tests {
                args.push("--skip-tests".to_string());
            }
            if this.lint_breakdown {
                args.push("--lint-breakdown".to_string());
            }
            if this.by_file {
                args.push("--by-file".to_string());
            }
            for p in &this.packages {
                args.push("-p".to_string());
                args.push(p.clone());
            }

            return crate::coordinator::coordinate_and_spawn("check", &args, ctx);
        }

        // Ensure infrastructure is ready (DB needed for sqlx compile-time checks)
        preflight::ensure_ready(ctx)?;

        // Record fingerprint+scope for coordinator freshness detection.
        // Check scope includes -p/--all flags so narrow checks don't
        // satisfy broader scopes.
        {
            let mut scope_args = Vec::new();
            for p in &this.packages {
                scope_args.push("-p".to_string());
                scope_args.push(p.clone());
            }
            if this.all {
                scope_args.push("--all".to_string());
            }
            ctx.record_coordination_fingerprint("check", &scope_args);
        }

        // Resource warning before heavy operation
        if ctx.is_human()
            && let Ok(status) = resources::ResourceStatus::capture()
            && let Some(warning) = status.warning(resources::thresholds::CARGO_CHECK_GB)
        {
            eprintln!("  ⚠ {warning}");
        }

        let mut result = CommandResult::success();

        // --fix: apply all automatic fixes first, then run the full check pipeline.
        // Equivalent to: xtask fix && xtask check --full
        if this.fix {
            if ctx.is_human() {
                println!("Applying automatic fixes before full check...");
            }
            let fix_cmd = crate::commands::fix::FixCommand::default();
            fix_cmd.execute(ctx).await?;
            // fix flag is consumed; fix_fmt is still set for the fmt stage below
            this.fix = false;
        }

        let package_args = this.build_package_args(true)?;

        // 1. Formatting (optional, off by default)
        if this.fmt {
            if ctx.is_human() {
                println!("Checking formatting...");
            }
            let stage = ctx.start_stage("fmt");
            let fmt_result = ProcessBuilder::cargo()
                .args(["fmt", "--all", "--", "--check"])
                .with_description("cargo fmt --check")
                .inherit_output()
                .run_ok();

            let final_result = if fmt_result.is_err() && this.fix_fmt {
                // Auto-correct formatting and re-check
                if ctx.is_human() {
                    eprintln!("  ✗ fmt failed — auto-correcting...");
                }
                ProcessBuilder::cargo()
                    .args(["fmt", "--all"])
                    .with_description("cargo fmt --fix")
                    .inherit_output()
                    .run_ok()?;
                let re_result = ProcessBuilder::cargo()
                    .args(["fmt", "--all", "--", "--check"])
                    .with_description("cargo fmt --check (after fix)")
                    .inherit_output()
                    .run_ok();
                if re_result.is_ok() && ctx.is_human() {
                    eprintln!("  ✓ fmt (auto-corrected)");
                }
                re_result
            } else {
                fmt_result
            };

            ctx.finish_stage(stage, final_result.is_ok());
            final_result?;
            result = result.with_detail("fmt check passed");
        }

        // 2. Compilation + Linting
        //
        // Clippy subsumes cargo check — it runs the full compiler before applying
        // lint rules. Running both wastes the entire compilation step on cold builds
        // (60-120s). So when lint=true, we skip standalone cargo check and go straight
        // to clippy. When lint=false (the default), cargo check is the only compilation
        // verification (~3s warm vs ~20s for clippy).
        let package_arg_refs: Vec<&str> = package_args
            .iter()
            .map(std::string::String::as_str)
            .collect();

        if this.lint {
            let stage = ctx.start_stage("clippy");
            if ctx.is_human() {
                println!("Running clippy (includes compilation check)...");
            }

            let clippy_summary = run_cargo_clippy(&package_arg_refs)?;
            let success = clippy_summary.success;

            // Show rendered output for humans
            if ctx.is_human() {
                for diag in &clippy_summary.diagnostics {
                    if let Some(rendered) = &diag.rendered {
                        eprint!("{rendered}");
                    }
                }
            }

            this.process_diagnostics(ctx, &clippy_summary, &mut result, "clippy");
            ctx.finish_stage(stage, success);

            // Show lint breakdown if requested or if there are many warnings
            if this.lint_breakdown || clippy_summary.warnings > 50 {
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
            if this.by_file {
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
        } else {
            // Default: run standalone cargo check (~3s warm)
            let stage = ctx.start_stage("compile");
            if ctx.is_human() {
                println!("Checking compilation...");
            }

            let check_summary = run_cargo_check(&package_arg_refs)?;
            let success = check_summary.success;

            if ctx.is_human() {
                for diag in &check_summary.diagnostics {
                    if let Some(rendered) = &diag.rendered {
                        eprint!("{rendered}");
                    }
                }
            }

            this.process_diagnostics(ctx, &check_summary, &mut result, "cargo check");
            ctx.finish_stage(stage, success);

            if !check_summary.success {
                return Ok(result.with_detail("cargo check failed"));
            }
            result = result.with_detail("cargo check passed");
        }

        // 3. Forbidden patterns (optional, off by default)
        if this.forbidden {
            let stage = ctx.start_stage("forbidden");
            if ctx.is_human() {
                println!("Scanning for forbidden patterns...");
            }
            let forbidden_result = crate::commands::lint_forbidden::LintForbiddenCommand
                .execute(ctx)
                .await;
            ctx.finish_stage(stage, forbidden_result.is_ok());
            forbidden_result?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    fn make_cmd(lint: bool, fmt: bool, forbidden: bool, full: bool) -> CheckCommand {
        CheckCommand {
            lint,
            fmt,
            forbidden,
            full,
            fix_fmt: false,
            fix: false,
            heavy: false,
            affected: false,
            all: false,
            packages: vec![],
            skip_tests: false,
            lint_breakdown: false,
            by_file: false,
        }
    }

    #[sinex_test]
    fn test_check_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(false, false, false, false);
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("check".to_string()));
        assert!(metadata.timeout.is_some());
        Ok(())
    }

    #[sinex_test]
    fn test_check_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(false, false, false, false);
        assert_eq!(cmd.name(), "check");
        Ok(())
    }

    #[sinex_test]
    fn test_full_flag_resolves() -> ::xtask::sandbox::TestResult<()> {
        let mut cmd = make_cmd(false, false, false, true);
        cmd.resolve_flags();
        assert!(cmd.lint);
        assert!(cmd.fmt);
        assert!(cmd.forbidden);
        Ok(())
    }

    #[sinex_test]
    fn test_fix_flag_implies_full_and_fix_fmt() -> ::xtask::sandbox::TestResult<()> {
        let mut cmd = CheckCommand {
            fix: true,
            ..make_cmd(false, false, false, false)
        };
        cmd.resolve_flags();
        assert!(cmd.fix_fmt, "--fix should imply --fix-fmt");
        assert!(cmd.lint, "--fix should imply --full → --lint");
        assert!(cmd.fmt, "--fix should imply --full → --fmt");
        assert!(cmd.forbidden, "--fix should imply --full → --forbidden");
        Ok(())
    }

    #[sinex_test]
    fn test_defaults_are_compile_only() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(false, false, false, false);
        assert!(!cmd.lint);
        assert!(!cmd.fmt);
        assert!(!cmd.forbidden);
        assert!(!cmd.full);
        Ok(())
    }
}
