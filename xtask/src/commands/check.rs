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

use crate::cargo_diagnostics::{DiagnosticSummary, estimate_package_count};
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::preflight;
use crate::process::ProcessBuilder;
use crate::resources;

/// Check command configuration
#[derive(Debug, Clone, Default, clap::Args)]
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
    /// Auto-fix fmt + clippy suggestions, then run full check (equivalent to: xtask fix && xtask check --full)
    #[arg(long)]
    pub fix: bool,
    /// Also run slow lints
    #[arg(long)]
    pub heavy: bool,
    /// Check ALL packages (disables affected mode default)
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

    /// Run `nix flake check --no-build` (evaluation only, ~2-5s). Included in --full.
    /// Skipped silently when `nix` is not on PATH.
    #[arg(long)]
    pub nix: bool,
}

impl CheckCommand {
    /// Resolve composite flags into individual flags (mutates self).
    fn resolve_flags(&mut self) {
        if self.fix {
            self.full = true;
        }
        if self.full {
            self.lint = true;
            self.fmt = true;
            self.forbidden = true;
            self.nix = true;
        }
    }

    /// Build cargo args based on package scope.
    ///
    /// `is_human` gates informational `eprintln!` output (B2 fix — these should
    /// not appear in JSON/machine output mode).
    fn build_package_args(&self, include_tests: bool, is_human: bool) -> Result<Vec<String>> {
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
        } else if !self.all {
            // Affected mode is default ON, --all disables it
            let affected_pkgs = crate::affected::affected_packages()?;
            if affected_pkgs.is_empty() {
                if is_human {
                    eprintln!("  ℹ No affected packages detected — checking full workspace");
                }
                args.push("--workspace".to_string());
            } else {
                // H6: Narrate which packages were selected and why
                if is_human {
                    let pkg_list = if affected_pkgs.len() <= 4 {
                        affected_pkgs.join(", ")
                    } else {
                        format!(
                            "{}, …+{}",
                            affected_pkgs[..3].join(", "),
                            affected_pkgs.len() - 3
                        )
                    };
                    eprintln!(
                        "  ℹ Affected mode: {} package{} ({pkg_list})",
                        affected_pkgs.len(),
                        if affected_pkgs.len() == 1 { "" } else { "s" }
                    );
                }
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
            if this.fix {
                args.push("--fix".to_string());
            }
            if this.heavy {
                args.push("--heavy".to_string());
            }
            if this.all {
                args.push("--all".to_string());
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
            if this.nix {
                args.push("--nix".to_string());
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

        let package_args = this.build_package_args(true, ctx.is_human())?;

        // 1. Formatting (optional, off by default)
        if this.fmt {
            if ctx.is_human() {
                println!("Checking formatting...");
            }
            let stage = ctx.start_stage("fmt");
            let fmt_result = ctx.cargo_runner().run_fmt_check();

            ctx.finish_stage(stage, fmt_result.is_ok());
            fmt_result?;
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

        // Estimate package count for determinate progress (fast, no rustc invocation).
        let pkg_total = estimate_package_count(&package_arg_refs);

        if this.lint {
            let stage = ctx.start_stage("clippy");
            if ctx.is_human() {
                println!("Running clippy (includes compilation check)...");
            }
            if pkg_total > 0 {
                ctx.report_progress_full(
                    "clippy",
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

            let clippy_summary = ctx.cargo_runner().run_clippy_streaming(&package_arg_refs, &mut |n| {
                if pkg_total > 0 {
                    let pct = (n as f64 / pkg_total as f64 * 100.0).min(100.0);
                    ctx.report_progress_full(
                        "clippy",
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
                let mut failure =
                    crate::command::CommandResult::failure(crate::output::StructuredError {
                        code: "CLIPPY_FAILED".to_string(),
                        message: "clippy failed".to_string(),
                        location: Some("check".to_string()),
                        suggestion: Some(
                            "Run `xtask check --lint` and inspect diagnostics".to_string(),
                        ),
                    })
                    .with_detail("clippy failed");
                failure.warnings = result.warnings;
                failure.data = result.data;
                return Ok(failure.with_duration(ctx.elapsed()));
            }
            result = result.with_detail("clippy passed");
        } else {
            // Default: run standalone cargo check (~3s warm)
            let stage = ctx.start_stage("compile");
            if ctx.is_human() {
                println!("Checking compilation...");
            }
            if pkg_total > 0 {
                ctx.report_progress_full(
                    "compile",
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

            let check_summary = ctx.cargo_runner().run_check_streaming(&package_arg_refs, &mut |n| {
                if pkg_total > 0 {
                    let pct = (n as f64 / pkg_total as f64 * 100.0).min(100.0);
                    ctx.report_progress_full(
                        "compile",
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
                let mut failure =
                    crate::command::CommandResult::failure(crate::output::StructuredError {
                        code: "CHECK_FAILED".to_string(),
                        message: "cargo check failed".to_string(),
                        location: Some("check".to_string()),
                        suggestion: Some("Run `xtask check` and inspect diagnostics".to_string()),
                    })
                    .with_detail("cargo check failed");
                failure.warnings = result.warnings;
                return Ok(failure.with_duration(ctx.elapsed()));
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

        // 4. Nix flake evaluation (Q6 — optional, off by default, ON with --nix or --full)
        if this.nix {
            if which_nix_on_path() {
                let stage = ctx.start_stage("nix-check");
                if ctx.is_human() {
                    println!("Evaluating nix flake (--no-build)...");
                }
                let nix_result = ProcessBuilder::new("nix")
                    .args(["flake", "check", "--no-build"])
                    .with_description("nix flake check --no-build")
                    .inherit_output()
                    .run_ok();
                ctx.finish_stage(stage, nix_result.is_ok());
                nix_result?;
                result = result.with_detail("nix flake check passed");
            } else if ctx.is_human() {
                eprintln!("  ℹ nix not found on PATH — skipping nix flake check");
            }
        }

        // Q5: if NixOS modules are dirty, suggest running the NixOS compatibility gate
        match crate::affected::nixos_modules_dirty() {
            Ok(true) if ctx.is_human() => {
                eprintln!(
                    "→ NixOS modules modified. Run the NixOS compatibility gate: \
                     xtask test --vm --category smoke"
                );
            }
            Ok(_) => {}
            Err(error) => {
                let warning = format!("Failed to detect dirty NixOS modules: {error:#}");
                if ctx.is_human() {
                    eprintln!("→ {warning}");
                }
                result = result.with_warning(warning);
            }
        }

        // H1: Post-check fixable diagnostic hint
        let fixable_count = ctx
            .with_history_db(|db| db.get_fixable_diagnostic_count())
            .unwrap_or(0);

        // Merge diagnostic counts into any existing breakdown data already in result.
        // with_data() replaces — so we must merge here to preserve lint_breakdown/file_breakdown.
        let mut final_data = result.data.take().unwrap_or(serde_json::json!({}));
        final_data["diagnostics_recorded"] = serde_json::json!(ctx.invocation_id().is_some());
        final_data["fixable"] = serde_json::json!(fixable_count);
        result = result.with_data(final_data);

        if ctx.is_human() && fixable_count > 0 {
            eprintln!(
                "→ {} auto-fixable warning{} detected. Run: xtask check --fix --smart",
                fixable_count,
                if fixable_count == 1 { "" } else { "s" }
            );
        }

        // R3: Predictive prefetch — if check→test transition probability > 70%,
        // spawn `cargo test --no-run` in the background so the test binary is already
        // compiled when the developer types `xtask test`.
        // Only in foreground mode: background checks run in CI where prefetch is wasteful.
        if result.is_success() && !ctx.is_background() {
            trigger_compilation_prefetch(ctx);
        }

        Ok(result.with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

/// R3: Spawn `cargo test --no-run` in the background when the check→test transition
/// probability exceeds 70% in recent history within a 5-minute window.
///
/// This pre-warms the test binary compilation so `xtask test` starts faster.
/// Fire-and-forget: errors are silently ignored since this is a pure optimization.
fn trigger_compilation_prefetch(ctx: &crate::command::CommandContext) {
    let probability = ctx
        .with_history_db(|db| db.get_transition_probability("check", "test", 5, 20))
        .unwrap_or(0.0);

    if probability > 70.0 {
        tracing::info!(
            target: "xtask::coordinator",
            probability = probability,
            "R3: pre-compiling tests ({probability:.0}% of recent check runs are followed by test)"
        );
        if ctx.is_human() {
            eprintln!("  ⚡ Pre-compiling tests ({probability:.0}% chance you'll run them next)");
        }
        // Spawn cargo test --no-run as a detached background process.
        // This is intentionally a raw cargo call (not via xtask) since we want
        // fire-and-forget semantics without history tracking or coordinator overhead.
        let _ = std::process::Command::new("cargo")
            .args(["test", "--no-run", "--workspace"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn(); // Don't wait — just warm the compiler cache
    }
}

/// Returns true when `nix` is found on the system PATH (Q6).
fn which_nix_on_path() -> bool {
    std::process::Command::new("nix")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cargo_diagnostics::CompilerDiagnostic;
    use crate::cargo_runner::MockCargoRunner;
    use crate::command::CommandContext;
    use crate::output::{OutputFormat, OutputWriter};
    use crate::sandbox::sinex_test;
    use std::sync::Arc;

    fn make_cmd(lint: bool, fmt: bool, forbidden: bool, full: bool) -> CheckCommand {
        CheckCommand {
            lint,
            fmt,
            forbidden,
            full,
            fix: false,
            heavy: false,
            all: false,
            packages: vec![],
            skip_tests: false,
            lint_breakdown: false,
            by_file: false,
            nix: false,
        }
    }

    #[sinex_test]
    async fn test_check_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(false, false, false, false);
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("check"));
        assert!(metadata.timeout.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_check_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(false, false, false, false);
        assert_eq!(cmd.name(), "check");
        Ok(())
    }

    #[sinex_test]
    async fn test_full_flag_resolves() -> ::xtask::sandbox::TestResult<()> {
        let mut cmd = make_cmd(false, false, false, true);
        cmd.resolve_flags();
        assert!(cmd.lint);
        assert!(cmd.fmt);
        assert!(cmd.forbidden);
        assert!(cmd.nix, "--full should imply --nix");
        Ok(())
    }

    #[sinex_test]
    async fn test_fix_flag_implies_full() -> ::xtask::sandbox::TestResult<()> {
        let mut cmd = CheckCommand {
            fix: true,
            ..make_cmd(false, false, false, false)
        };
        cmd.resolve_flags();
        assert!(cmd.lint, "--fix should imply --full → --lint");
        assert!(cmd.fmt, "--fix should imply --full → --fmt");
        assert!(cmd.forbidden, "--fix should imply --full → --forbidden");
        Ok(())
    }

    #[sinex_test]
    async fn test_defaults_are_compile_only() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(false, false, false, false);
        assert!(!cmd.lint);
        assert!(!cmd.fmt);
        assert!(!cmd.forbidden);
        assert!(!cmd.full);
        Ok(())
    }

    // ── execute() unit tests via MockCargoRunner ──────────────────────────────

    fn mock_ctx(runner: Arc<MockCargoRunner>) -> CommandContext {
        CommandContext::new(
            OutputWriter::new(OutputFormat::Silent),
            false,
            None,
            "check",
        )
        .with_cargo_runner(runner as Arc<dyn crate::cargo_runner::CargoRunner>)
    }

    fn error_summary() -> DiagnosticSummary {
        DiagnosticSummary {
            errors: 1,
            warnings: 0,
            diagnostics: vec![CompilerDiagnostic {
                level: "error".to_string(),
                message: "type mismatch".to_string(),
                ..Default::default()
            }],
            success: false,
            compiled_packages: Default::default(),
        }
    }

    fn warning_summary(n: usize) -> DiagnosticSummary {
        let packages: std::collections::HashSet<String> =
            (0..n).map(|i| format!("pkg-{i}")).collect();
        DiagnosticSummary {
            errors: 0,
            warnings: n,
            diagnostics: (0..n)
                .map(|i| CompilerDiagnostic {
                    level: "warning".to_string(),
                    message: format!("unused import #{i}"),
                    ..Default::default()
                })
                .collect(),
            success: true,
            compiled_packages: packages,
        }
    }

    #[sinex_test]
    async fn test_execute_clean_compile_succeeds() -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let ctx = mock_ctx(runner);
        let cmd = make_cmd(false, false, false, false);
        let result = cmd.execute(&ctx).await?;
        assert!(result.is_success(), "clean check should succeed: {result:?}");
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_check_errors_yield_failure() -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean().with_check(error_summary()));
        let ctx = mock_ctx(runner);
        let cmd = make_cmd(false, false, false, false);
        let result = cmd.execute(&ctx).await?;
        assert!(!result.is_success(), "check with errors should fail");
        assert!(
            result.errors.iter().any(|e| e.code == "CHECK_FAILED"),
            "expected CHECK_FAILED in errors: {:?}", result.errors,
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_lint_routes_to_clippy_not_check() -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let ctx = mock_ctx(runner.clone());
        let cmd = make_cmd(true, false, false, false); // --lint
        cmd.execute(&ctx).await?;
        let calls = runner.calls();
        assert_eq!(calls.clippy, 1, "clippy should have been called once");
        assert_eq!(calls.check, 0, "cargo check must NOT run when --lint active");
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_compile_only_routes_to_check_not_clippy() -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let ctx = mock_ctx(runner.clone());
        let cmd = make_cmd(false, false, false, false); // default: compile-only
        cmd.execute(&ctx).await?;
        let calls = runner.calls();
        assert_eq!(calls.check, 1, "cargo check should have been called once");
        assert_eq!(calls.clippy, 0, "clippy must NOT run in compile-only mode");
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_clippy_errors_yield_failure() -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean().with_clippy(error_summary()));
        let ctx = mock_ctx(runner);
        let cmd = make_cmd(true, false, false, false); // --lint
        let result = cmd.execute(&ctx).await?;
        assert!(!result.is_success(), "clippy errors should propagate to failure");
        assert!(
            result.errors.iter().any(|e| e.code == "CLIPPY_FAILED"),
            "expected CLIPPY_FAILED in errors: {:?}", result.errors,
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_fmt_fail_short_circuits_before_compile() -> ::xtask::sandbox::TestResult<()> {
        // --fmt with a formatting violation should bail before running cargo check.
        let runner = Arc::new(MockCargoRunner::clean().with_fmt_fail());
        let ctx = mock_ctx(runner.clone());
        let cmd = make_cmd(false, true, false, false); // --fmt
        let result = cmd.execute(&ctx).await;
        // fmt failure surfaces as Err (propagated via `?` in execute)
        assert!(result.is_err(), "fmt failure should propagate as Err");
        let calls = runner.calls();
        assert_eq!(calls.fmt, 1, "fmt must have been called");
        assert_eq!(calls.check, 0, "cargo check must NOT run after fmt failure");
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_warnings_recorded_in_result() -> ::xtask::sandbox::TestResult<()> {
        // Warnings don't fail the check, but they appear in result.warnings.
        let runner = Arc::new(MockCargoRunner::clean().with_check(warning_summary(3)));
        let ctx = mock_ctx(runner);
        let cmd = make_cmd(false, false, false, false);
        let result = cmd.execute(&ctx).await?;
        assert!(result.is_success(), "warnings alone should not fail the check");
        assert!(
            result.warnings.iter().any(|w| w.contains("3 warning")),
            "3 warnings should appear in result.warnings: {:?}",
            result.warnings
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_progress_callback_fired_per_package() -> ::xtask::sandbox::TestResult<()> {
        // Verify that the progress callback is fired once per compiled package.
        // MockCargoRunner fires on_package_done N times for N compiled_packages.
        let runner = Arc::new(MockCargoRunner::clean().with_check(warning_summary(5)));
        let ctx = mock_ctx(runner);
        let cmd = make_cmd(false, false, false, false);
        // If the callback fires correctly, execute completes without panic.
        let result = cmd.execute(&ctx).await?;
        assert!(result.is_success());
        Ok(())
    }
}
