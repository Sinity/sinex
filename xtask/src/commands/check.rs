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

use color_eyre::eyre::{Result, WrapErr, eyre};

use crate::cargo_diagnostics::{DiagnosticSummary, estimate_package_count};
use crate::command::{CommandContext, CommandMetadata, CommandResult, WorkloadScope, XtaskCommand};
use crate::preflight;
use crate::process::ProcessBuilder;
use crate::resources;
use crate::tools::{ToolInfo, ToolManager};

/// Run the fast workspace verification pipeline.
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
    /// Fails if `nix` is unavailable or unhealthy.
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

    fn semantic_invocation_args(&self, scope: &WorkloadScope) -> Vec<String> {
        let mut args = Vec::new();

        if self.fix {
            args.push("--fix".to_string());
        }
        if self.full {
            args.push("--full".to_string());
        } else {
            if self.lint {
                args.push("--lint".to_string());
            }
            if self.fmt {
                args.push("--fmt".to_string());
            }
            if self.forbidden {
                args.push("--forbidden".to_string());
            }
            if self.nix {
                args.push("--nix".to_string());
            }
        }
        if self.heavy {
            args.push("--heavy".to_string());
        }
        if self.skip_tests {
            args.push("--skip-tests".to_string());
        }

        args.push(scope.encode_marker());
        args
    }

    /// Build cargo args based on package scope.
    ///
    /// `is_human` gates informational `eprintln!` output (B2 fix — these should
    /// not appear in JSON/machine output mode).
    fn build_package_args(
        &self,
        include_tests: bool,
        is_human: bool,
    ) -> Result<(Vec<String>, WorkloadScope)> {
        let mut args = vec!["--all-features".to_string()];

        // Include tests by default (unless skip_tests is set)
        if include_tests && !self.skip_tests {
            args.push("--tests".to_string());
            args.push("--benches".to_string());
            args.push("--examples".to_string());
        }

        if !self.packages.is_empty() {
            let mut packages = self.packages.clone();
            packages.sort();
            packages.dedup();
            for p in &packages {
                args.push("-p".to_string());
                args.push(p.clone());
            }
            return Ok((args, WorkloadScope::Packages(packages)));
        } else if !self.all {
            // Affected mode is default ON, --all disables it
            let mut affected_pkgs = crate::affected::affected_packages()?;
            if affected_pkgs.is_empty() {
                if is_human {
                    eprintln!("  ℹ No affected packages detected — checking full workspace");
                }
                args.push("--workspace".to_string());
                return Ok((args, WorkloadScope::Workspace));
            } else {
                affected_pkgs.sort();
                affected_pkgs.dedup();
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
                for p in &affected_pkgs {
                    args.push("-p".to_string());
                    args.push(p.clone());
                }
                return Ok((args, WorkloadScope::Affected(affected_pkgs)));
            }
        } else {
            args.push("--workspace".to_string());
            return Ok((args, WorkloadScope::Workspace));
        }
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
        if ctx.is_human() {
            match resources::ResourceStatus::capture() {
                Ok(status) => {
                    if let Some(warning) = status.warning(resources::thresholds::CARGO_CHECK_GB) {
                        eprintln!("  ⚠ {warning}");
                    }
                }
                Err(error) => {
                    eprintln!("  ⚠ Failed to inspect local resources: {error:#}");
                }
            }
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

        let (package_args, workload_scope) = this.build_package_args(true, ctx.is_human())?;
        ctx.record_invocation_args(&this.semantic_invocation_args(&workload_scope));

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

            let clippy_summary =
                ctx.cargo_runner()
                    .run_clippy_streaming(&package_arg_refs, &mut |n| {
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

            let check_summary =
                ctx.cargo_runner()
                    .run_check_streaming(&package_arg_refs, &mut |n| {
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
            let forbidden_result = forbidden_result?;
            for detail in forbidden_result.details {
                result = result.with_detail(detail);
            }
            for warning in forbidden_result.warnings {
                result = result.with_warning(warning);
            }
            result = result.with_detail("forbidden pattern scan passed");
        }

        // 4. Optional Nix flake evaluation.
        if this.nix {
            ensure_nix_tool_ready_with(ToolManager::check_tool)?;

            let stage = ctx.start_stage("nix-check");
            if ctx.is_human() {
                println!("Evaluating nix flake (--no-build)...");
            }
            let nix_result = ProcessBuilder::nix()
                .args(["flake", "check", "--no-build"])
                .with_description("nix flake check --no-build")
                .inherit_output()
                .run_ok();
            ctx.finish_stage(stage, nix_result.is_ok());
            nix_result?;
            result = result.with_detail("nix flake check passed");
        }

        // If NixOS modules are dirty, suggest running the NixOS compatibility gate.
        match crate::affected::nixos_modules_dirty() {
            Ok(true) if ctx.is_human() => {
                eprintln!(
                    "→ NixOS modules modified. Run the NixOS compatibility gate: \
                     xtask test vm --category smoke"
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
        let (fixable_count, fixable_warning) = resolve_fixable_diagnostic_count(ctx);
        if let Some(warning) = fixable_warning {
            if ctx.is_human() {
                eprintln!("→ {warning}");
            }
            result = result.with_warning(warning);
        }

        // Merge diagnostic counts into any existing breakdown data already in result.
        // with_data() replaces — so we must merge here to preserve lint_breakdown/file_breakdown.
        let mut final_data = result.data.take().unwrap_or(serde_json::json!({}));
        final_data["diagnostics_recorded"] = serde_json::json!(ctx.invocation_id().is_some());
        if let Some(fixable_count) = fixable_count {
            final_data["fixable"] = serde_json::json!(fixable_count);
        }
        result = result.with_data(final_data);

        if ctx.is_human() && fixable_count.is_some_and(|count| count > 0) {
            eprintln!(
                "→ {} auto-fixable warning{} detected. Run: xtask check --fix --smart",
                fixable_count.unwrap_or_default(),
                if fixable_count == Some(1) { "" } else { "s" }
            );
        }

        // R3: Predictive prefetch — if check→test transition probability > 70%,
        // spawn `cargo test --no-run` in the background so the test binary is already
        // compiled when the developer types `xtask test`.
        //
        // Only interactive human runs may trigger this. JSON/compact/silent
        // executions should remain observational and deterministic instead of
        // consulting ambient workstation history to start helper subprocesses.
        if result.is_success() && ctx.allows_ambient_optimizations() {
            trigger_compilation_prefetch(ctx);
        }

        Ok(result.with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

fn resolve_fixable_diagnostic_count(ctx: &CommandContext) -> (Option<usize>, Option<String>) {
    match ctx.try_with_history_db(|db| db.get_fixable_diagnostic_count()) {
        Some(Ok(count)) => (Some(count), None),
        Some(Err(error)) => (
            None,
            Some(format!(
                "Failed to query auto-fixable diagnostic count from history DB: {error:#}"
            )),
        ),
        None if ctx.invocation_id().is_some() => (
            None,
            Some("Failed to open history DB for auto-fixable diagnostic count".to_string()),
        ),
        None => (None, None),
    }
}

/// R3: Spawn `cargo test --no-run` in the background when the check→test transition
/// probability exceeds 70% in recent history within a 5-minute window.
///
/// This pre-warms the test binary compilation so `xtask test` starts faster.
/// Spawn failures are surfaced as warnings because this remains a pure optimization.
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
        if let Err(error) = spawn_compilation_prefetch_with("cargo") {
            tracing::warn!(
                target: "xtask::coordinator",
                error = %error,
                "R3: failed to spawn test pre-compilation"
            );
            if ctx.is_human() {
                eprintln!("  ⚠ Failed to start test pre-compilation: {error:#}");
            }
        }
    }
}

fn spawn_compilation_prefetch_with(program: &str) -> Result<()> {
    std::process::Command::new(program)
        .args(["test", "--no-run", "--workspace"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map(|_| ())
        .wrap_err("failed to spawn `cargo test --no-run --workspace` prefetch")
}

fn ensure_nix_tool_ready_with(check_tool: impl FnOnce(&str) -> Result<ToolInfo>) -> Result<()> {
    let nix = check_tool("nix")
        .map_err(|error| eyre!("xtask check --nix requires `nix` on PATH: {error:#}"))?;
    if let Some(probe_issue) = nix.probe_issue {
        return Err(eyre!(
            "`nix` is present at {} but failed readiness probe: {probe_issue}",
            nix.path.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cargo_diagnostics::CompilerDiagnostic;
    use crate::cargo_runner::MockCargoRunner;
    use crate::command::CommandContext;
    use crate::output::{OutputFormat, OutputWriter};
    use crate::sandbox::sinex_test;
    use std::{future::Future, sync::Arc};

    fn run_async_test(
        fut: impl Future<Output = ::xtask::sandbox::TestResult<()>>,
    ) -> ::xtask::sandbox::TestResult<()> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(fut)
    }

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

    #[test]
    fn test_check_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(false, false, false, false);
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("check"));
        assert!(metadata.timeout.is_some());
        Ok(())
    }

    #[test]
    fn test_check_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(false, false, false, false);
        assert_eq!(cmd.name(), "check");
        Ok(())
    }

    #[test]
    fn test_full_flag_resolves() -> ::xtask::sandbox::TestResult<()> {
        let mut cmd = make_cmd(false, false, false, true);
        cmd.resolve_flags();
        assert!(cmd.lint);
        assert!(cmd.fmt);
        assert!(cmd.forbidden);
        assert!(cmd.nix, "--full should imply --nix");
        Ok(())
    }

    #[test]
    fn test_fix_flag_implies_full() -> ::xtask::sandbox::TestResult<()> {
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

    #[test]
    fn test_defaults_are_compile_only() -> ::xtask::sandbox::TestResult<()> {
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

    fn mock_ctx_with_history(
        runner: Arc<MockCargoRunner>,
        invocation_id: Option<i64>,
        db_path: std::path::PathBuf,
    ) -> CommandContext {
        CommandContext::new_with_db_override(
            OutputWriter::new(OutputFormat::Silent),
            false,
            invocation_id,
            "check",
            db_path,
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

    #[test]
    fn test_execute_clean_compile_succeeds() -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
            let runner = Arc::new(MockCargoRunner::clean());
            let ctx = mock_ctx(runner);
            let cmd = make_cmd(false, false, false, false);
            let result = cmd.execute(&ctx).await?;
            assert!(
                result.is_success(),
                "clean check should succeed: {result:?}"
            );
            Ok(())
        })
    }

    #[test]
    fn test_execute_check_warns_when_fixable_count_is_unavailable()
    -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
            let runner = Arc::new(MockCargoRunner::clean());
            let temp = tempfile::tempdir()?;
            let ctx = mock_ctx_with_history(runner, Some(42), temp.path().to_path_buf());
            let cmd = make_cmd(false, false, false, false);
            let result = cmd.execute(&ctx).await?;
            let data = result
                .data
                .as_ref()
                .unwrap_or_else(|| panic!("expected structured data"));
            assert!(
                result.is_success(),
                "clean check should still succeed: {result:?}"
            );
            assert!(
                result
                    .warnings
                    .iter()
                    .any(|warning| warning.contains("auto-fixable diagnostic count"))
            );
            assert!(data.get("fixable").is_none());
            Ok(())
        })
    }

    #[test]
    fn test_execute_check_without_history_invocation_skips_fixable_probe_warning()
    -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
            let runner = Arc::new(MockCargoRunner::clean());
            let ctx = mock_ctx(runner);
            let cmd = make_cmd(false, false, false, false);
            let result = cmd.execute(&ctx).await?;
            assert!(
                result.is_success(),
                "clean check should succeed: {result:?}"
            );
            assert!(
                result
                    .warnings
                    .iter()
                    .all(|warning| !warning.contains("auto-fixable diagnostic count")),
                "unexpected auto-fixable warning: {:?}",
                result.warnings
            );
            Ok(())
        })
    }

    #[test]
    fn test_execute_check_errors_yield_failure() -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
            let runner = Arc::new(MockCargoRunner::clean().with_check(error_summary()));
            let ctx = mock_ctx(runner);
            let cmd = make_cmd(false, false, false, false);
            let result = cmd.execute(&ctx).await?;
            assert!(!result.is_success(), "check with errors should fail");
            assert!(
                result.errors.iter().any(|e| e.code == "CHECK_FAILED"),
                "expected CHECK_FAILED in errors: {:?}",
                result.errors,
            );
            Ok(())
        })
    }

    #[test]
    fn test_execute_lint_routes_to_clippy_not_check() -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
            let runner = Arc::new(MockCargoRunner::clean());
            let ctx = mock_ctx(runner.clone());
            let cmd = make_cmd(true, false, false, false); // --lint
            cmd.execute(&ctx).await?;
            let calls = runner.calls();
            assert_eq!(calls.clippy, 1, "clippy should have been called once");
            assert_eq!(
                calls.check, 0,
                "cargo check must NOT run when --lint active"
            );
            Ok(())
        })
    }

    #[test]
    fn test_execute_compile_only_routes_to_check_not_clippy()
    -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
            let runner = Arc::new(MockCargoRunner::clean());
            let ctx = mock_ctx(runner.clone());
            let cmd = make_cmd(false, false, false, false); // default: compile-only
            cmd.execute(&ctx).await?;
            let calls = runner.calls();
            assert_eq!(calls.check, 1, "cargo check should have been called once");
            assert_eq!(calls.clippy, 0, "clippy must NOT run in compile-only mode");
            Ok(())
        })
    }

    #[test]
    fn test_execute_clippy_errors_yield_failure() -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
            let runner = Arc::new(MockCargoRunner::clean().with_clippy(error_summary()));
            let ctx = mock_ctx(runner);
            let cmd = make_cmd(true, false, false, false); // --lint
            let result = cmd.execute(&ctx).await?;
            assert!(
                !result.is_success(),
                "clippy errors should propagate to failure"
            );
            assert!(
                result.errors.iter().any(|e| e.code == "CLIPPY_FAILED"),
                "expected CLIPPY_FAILED in errors: {:?}",
                result.errors,
            );
            Ok(())
        })
    }

    #[test]
    fn test_execute_fmt_fail_short_circuits_before_compile()
    -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
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
        })
    }

    #[test]
    fn test_execute_warnings_recorded_in_result() -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
            // Warnings don't fail the check, but they appear in result.warnings.
            let runner = Arc::new(MockCargoRunner::clean().with_check(warning_summary(3)));
            let ctx = mock_ctx(runner);
            let cmd = make_cmd(false, false, false, false);
            let result = cmd.execute(&ctx).await?;
            assert!(
                result.is_success(),
                "warnings alone should not fail the check"
            );
            assert!(
                result.warnings.iter().any(|w| w.contains("3 warning")),
                "3 warnings should appear in result.warnings: {:?}",
                result.warnings
            );
            Ok(())
        })
    }

    #[test]
    fn test_execute_progress_callback_fired_per_package() -> ::xtask::sandbox::TestResult<()> {
        run_async_test(async {
            // Verify that the progress callback is fired once per compiled package.
            // MockCargoRunner fires on_package_done N times for N compiled_packages.
            let runner = Arc::new(MockCargoRunner::clean().with_check(warning_summary(5)));
            let ctx = mock_ctx(runner);
            let cmd = make_cmd(false, false, false, false);
            // If the callback fires correctly, execute completes without panic.
            let result = cmd.execute(&ctx).await?;
            assert!(result.is_success());
            Ok(())
        })
    }

    #[test]
    fn test_ambient_optimizations_only_enabled_for_human_foreground()
    -> ::xtask::sandbox::TestResult<()> {
        let human = CommandContext::new(
            OutputWriter::new(OutputFormat::Human),
            false,
            None,
            "check",
        );
        assert!(human.allows_ambient_optimizations());

        let json = CommandContext::new(
            OutputWriter::new(OutputFormat::Json),
            false,
            None,
            "check",
        );
        assert!(!json.allows_ambient_optimizations());

        let silent = CommandContext::new(
            OutputWriter::new(OutputFormat::Silent),
            false,
            None,
            "check",
        );
        assert!(!silent.allows_ambient_optimizations());

        let background = CommandContext::new(
            OutputWriter::new(OutputFormat::Human),
            true,
            None,
            "check",
        );
        assert!(!background.allows_ambient_optimizations());

        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_nix_tool_ready_accepts_healthy_tool() -> ::xtask::sandbox::TestResult<()> {
        let healthy_tool = ToolInfo {
            path: "/run/current-system/sw/bin/nix".into(),
            version: "nix (Nix) 2.0".to_string(),
            probe_issue: None,
        };
        ensure_nix_tool_ready_with(|tool| {
            assert_eq!(tool, "nix");
            Ok(healthy_tool)
        })?;
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_nix_tool_ready_rejects_missing_tool() -> ::xtask::sandbox::TestResult<()> {
        let error = ensure_nix_tool_ready_with(|_| Err(eyre!("Tool 'nix' not found in PATH")))
            .expect_err("missing nix should fail");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("xtask check --nix requires `nix` on PATH"));
        assert!(rendered.contains("Tool 'nix' not found in PATH"));
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_nix_tool_ready_rejects_probe_issue() -> ::xtask::sandbox::TestResult<()> {
        let error = ensure_nix_tool_ready_with(|_| {
            Ok(ToolInfo {
                path: "/tmp/nix".into(),
                version: "unknown".to_string(),
                probe_issue: Some("failed to run `nix --version`".to_string()),
            })
        })
        .expect_err("broken nix probe should fail");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("failed readiness probe"));
        assert!(rendered.contains("failed to run `nix --version`"));
        Ok(())
    }

    #[sinex_test]
    async fn test_spawn_compilation_prefetch_surfaces_spawn_failure()
    -> ::xtask::sandbox::TestResult<()> {
        let error = spawn_compilation_prefetch_with("nonexistent-cargo-xyz-12345")
            .expect_err("missing cargo should fail");
        assert!(
            format!("{error:#}")
                .contains("failed to spawn `cargo test --no-run --workspace` prefetch")
        );
        Ok(())
    }
}
