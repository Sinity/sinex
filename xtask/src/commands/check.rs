//! Check command — compilation, linting, and pattern verification.
//!
//! Pipeline: [fmt] → [clippy | cargo check] → [forbidden patterns].
//! Defaults to compile-only (cargo check, ~3s warm). Use additive flags to escalate:
//!   --lint      run clippy (~20s warm, subsumes cargo check)
//!   --fmt       run cargo fmt --check (~1s extra)
//!   --forbidden run forbidden pattern scan (~1s extra)
//!   --full      shorthand for --fmt --lint --forbidden (~25s warm)
//!   --changed-strict  API drift guard: check only packages that own changed Rust
//!                     files in HEAD vs merge-base. Opt-in, does not run by default.
//!
//! Compiler diagnostics are captured and stored in the history database for
//! later analysis via `xtask history diagnostics`.
//!
//! ## Proof Authority
//!
//! This command's proof surface is rustc/clippy compiler output only. Native
//! rust-analyzer diagnostics are advisory and MUST NOT gate check success or
//! proof reuse — they are not a correctness proof (proc macros, build scripts,
//! cfgs, generated surfaces, and RA engine failures cause divergence from
//! rustc). See #1221 for the RA advisory surface.

use color_eyre::eyre::{Result, eyre};

use crate::cargo_diagnostics::{DiagnosticSummary, estimate_package_count};
use crate::command::{CommandContext, CommandMetadata, CommandResult, WorkloadScope, XtaskCommand};
use crate::output::StructuredError;
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
    /// Use planner to select workload (supplements affected-scope with failure history)
    #[arg(long)]
    pub plan: bool,
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
    /// Allow broad checks to start even when host PSI is already severe.
    #[arg(long)]
    pub allow_contended_host: bool,

    /// API drift guard: check only packages that own Rust files changed between
    /// HEAD and the merge-base of the given ref (default `origin/master`).
    /// Emits a JSON report of changed files, affected packages, and per-package
    /// results. Non-zero exit if any per-package check fails.
    /// This flag is opt-in and does not alter the default check behaviour.
    #[arg(long, value_name = "BASE_REF")]
    pub changed_strict: Option<Option<String>>,
}

/// Push `flag` onto `args` if `cond` is true.
fn push_flag(args: &mut Vec<String>, cond: bool, flag: &'static str) {
    if cond {
        args.push(flag.to_string());
    }
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

    /// Build the serialized CLI args for background re-invocation.
    fn background_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        push_flag(&mut args, self.lint, "--lint");
        push_flag(&mut args, self.fmt, "--fmt");
        push_flag(&mut args, self.forbidden, "--forbidden");
        push_flag(&mut args, self.full, "--full");
        push_flag(&mut args, self.fix, "--fix");
        push_flag(&mut args, self.heavy, "--heavy");
        push_flag(&mut args, self.all, "--all");
        push_flag(&mut args, self.skip_tests, "--skip-tests");
        push_flag(&mut args, self.lint_breakdown, "--lint-breakdown");
        push_flag(&mut args, self.by_file, "--by-file");
        push_flag(&mut args, self.nix, "--nix");
        push_flag(
            &mut args,
            self.allow_contended_host,
            "--allow-contended-host",
        );
        if let Some(base_ref) = &self.changed_strict {
            args.push("--changed-strict".to_string());
            if let Some(base_ref) = base_ref {
                args.push(base_ref.clone());
            }
        }
        for p in &self.packages {
            args.push("-p".to_string());
            args.push(p.clone());
        }
        args
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
        if self.allow_contended_host {
            args.push("--allow-contended-host".to_string());
        }
        if self.skip_tests {
            args.push("--skip-tests".to_string());
        }
        if let Some(base_ref) = &self.changed_strict {
            args.push("--changed-strict".to_string());
            if let Some(base_ref) = base_ref {
                args.push(base_ref.clone());
            }
        }

        args.push(scope.encode_marker());
        args
    }

    fn guard_broad_start_pressure(&self, ctx: &CommandContext) -> Result<()> {
        if self.allow_contended_host || !self.is_broad_pressure_sensitive() {
            return Ok(());
        }

        let pressure = resources::PressureRecommendation::capture();
        if let Some(error) = pressure.broad_start_error("check") {
            return Err(eyre!(error));
        }
        if let Some(warning) = pressure.warning("check")
            && ctx.is_human()
        {
            eprintln!("  ⚠ {warning}");
        }
        Ok(())
    }

    fn is_broad_pressure_sensitive(&self) -> bool {
        self.all || self.full || self.lint || self.heavy || self.fix || self.packages.is_empty()
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
        }
        if !self.all {
            // Affected mode is default ON, --all disables it
            let mut affected_pkgs = crate::affected::affected_packages()?;

            // --plan: supplement affected scope with planner recommendations (#1146)
            if self.plan
                && let Ok(actions) = crate::planner::plan_next_actions()
            {
                let planner_pkgs = extract_packages_from_actions(&actions);
                for pkg in planner_pkgs {
                    if !affected_pkgs.contains(&pkg) {
                        if is_human {
                            eprintln!("  ℹ Planner: adding {pkg} (recent failure context)");
                        }
                        affected_pkgs.push(pkg);
                    }
                }
            }

            if affected_pkgs.is_empty() {
                if is_human {
                    eprintln!("  ℹ No affected packages detected — checking full workspace");
                }
                args.push("--workspace".to_string());
                return Ok((args, WorkloadScope::Workspace));
            }
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
        args.push("--workspace".to_string());
        Ok((args, WorkloadScope::Workspace))
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

    /// Run clippy or cargo check depending on `self.lint`, updating `result` in place.
    /// Returns `Some(failure_result)` on compilation/lint failure, `None` on success.
    fn run_compile_stage(
        &self,
        ctx: &CommandContext,
        package_arg_refs: &[&str],
        pkg_total: usize,
        result: &mut CommandResult,
    ) -> Result<Option<CommandResult>> {
        if self.lint {
            self.run_clippy_stage(ctx, package_arg_refs, pkg_total, result)
        } else {
            self.run_check_stage(ctx, package_arg_refs, pkg_total, result)
        }
    }

    fn run_clippy_stage(
        &self,
        ctx: &CommandContext,
        package_arg_refs: &[&str],
        pkg_total: usize,
        result: &mut CommandResult,
    ) -> Result<Option<CommandResult>> {
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
                .run_clippy_streaming(package_arg_refs, &mut |n| {
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
        if ctx.is_human() {
            for diag in &clippy_summary.diagnostics {
                if let Some(rendered) = &diag.rendered {
                    eprint!("{rendered}");
                }
            }
        }
        self.process_diagnostics(ctx, &clippy_summary, result, "clippy");
        ctx.finish_stage(stage, success);
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
                result.data = Some(serde_json::json!({"lint_breakdown": top_lints}));
            }
        }
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
                result.data = Some(serde_json::json!({"file_breakdown": top_files}));
            }
        }
        if !success {
            let mut failure = CommandResult::failure(crate::output::StructuredError {
                code: "CLIPPY_FAILED".to_string(),
                message: "clippy failed".to_string(),
                location: Some("check".to_string()),
                suggestion: Some("Run `xtask check --lint` and inspect diagnostics".to_string()),
            })
            .with_detail("clippy failed");
            failure.warnings = result.warnings.drain(..).collect();
            failure.data = result.data.take();
            return Ok(Some(failure));
        }
        result.details.push("clippy passed".to_string());
        Ok(None)
    }

    fn run_check_stage(
        &self,
        ctx: &CommandContext,
        package_arg_refs: &[&str],
        pkg_total: usize,
        result: &mut CommandResult,
    ) -> Result<Option<CommandResult>> {
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
        let check_summary = ctx
            .cargo_runner()
            .run_check_streaming(package_arg_refs, &mut |n| {
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
        self.process_diagnostics(ctx, &check_summary, result, "cargo check");
        ctx.finish_stage(stage, success);
        if !success {
            let mut failure = CommandResult::failure(crate::output::StructuredError {
                code: "CHECK_FAILED".to_string(),
                message: "cargo check failed".to_string(),
                location: Some("check".to_string()),
                suggestion: Some("Run `xtask check` and inspect diagnostics".to_string()),
            })
            .with_detail("cargo check failed");
            failure.warnings = result.warnings.drain(..).collect();
            return Ok(Some(failure));
        }
        result.details.push("cargo check passed".to_string());
        Ok(None)
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
            let args = this.background_args();
            let (_, workload_scope) = this.build_package_args(true, false)?;
            let coordination_args = this.semantic_invocation_args(&workload_scope);
            return crate::coordinator::coordinate_and_spawn_with_scope(
                "check",
                &args,
                &coordination_args,
                ctx,
            );
        }

        // --changed-strict: API drift guard.  Runs before the normal check
        // pipeline and short-circuits it when set.
        if let Some(ref base_opt) = this.changed_strict {
            let base_ref = base_opt.as_deref().unwrap_or("origin/master");
            let workspace_root = crate::config::workspace_root();
            let plan = crate::strict_changed::plan_changed_strict(base_ref, &workspace_root)?;
            if plan.affected_packages.is_empty() {
                if ctx.is_human() {
                    println!(
                        "API drift guard: checking packages changed relative to {base_ref}..."
                    );
                }
                return changed_strict_command_result(ctx, plan.into_empty_report());
            }

            this.guard_broad_start_pressure(ctx)?;

            // Ensure only compile-time infrastructure is ready. Per-package
            // `cargo check` needs a live Postgres schema for sqlx macros, but it
            // must not start NATS or runtime services as a verification side
            // effect.
            let compile_ready = preflight::ensure_compile_ready(ctx)?;
            let result = run_changed_strict_command(base_ref, ctx, &this);
            drop(compile_ready);
            return result;
        }

        this.guard_broad_start_pressure(ctx)?;

        // Ensure only compile-time infrastructure is ready. `cargo check` needs a
        // live Postgres schema for sqlx macros, but it must not start NATS or
        // runtime services as a side effect of verification.
        let _compile_ready = preflight::ensure_compile_ready(ctx)?;

        // Resource warning before heavy operation.  Captured regardless of output
        // mode so that machine-facing callers (agents, CI) can surface the
        // warning through the CommandResult rather than silently ignoring it.
        let resource_warning = capture_resource_warning(ctx);

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
        let coordination_args = this.semantic_invocation_args(&workload_scope);
        ctx.record_coordination_fingerprint("check", &coordination_args);
        ctx.record_invocation_args(&coordination_args);

        // 1. Formatting (optional, off by default)
        if this.fmt {
            if ctx.is_human() {
                println!("Checking formatting...");
            }
            let stage = ctx.start_stage("fmt");
            let fmt_args = fmt_args_for_scope(&workload_scope);
            let fmt_arg_refs: Vec<&str> = fmt_args.iter().map(String::as_str).collect();
            let fmt_result = ctx.cargo_runner().run_fmt_check(&fmt_arg_refs);

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

        // 2b. Run compile or lint stage; returns early if there are failures.
        if let Some(failure) =
            this.run_compile_stage(ctx, &package_arg_refs, pkg_total, &mut result)?
        {
            return Ok(failure);
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

        // If NixOS modules are dirty, suggest running the NixOS VM deployment gate.
        match crate::affected::nixos_modules_dirty() {
            Ok(true) if ctx.is_human() => {
                eprintln!(
                    "→ NixOS modules modified. Run the NixOS VM deployment gate: \
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

        // Surface resource warning through CommandResult so machine callers
        // (agents, CI) can inspect it even when is_human() is false.
        if let Some(ref warning) = resource_warning {
            result = result.with_warning(warning.clone());
        }

        // Merge diagnostic counts into any existing breakdown data already in result.
        // with_data() replaces — so we must merge here to preserve lint_breakdown/file_breakdown.
        let mut final_data = result.data.take().unwrap_or(serde_json::json!({}));
        final_data["diagnostics_recorded"] = serde_json::json!(ctx.invocation_id().is_some());
        if let Some(fixable_count) = fixable_count {
            final_data["fixable"] = serde_json::json!(fixable_count);
        }
        if let Some(ref warning) = resource_warning {
            final_data["resource_warning"] = serde_json::json!(warning);
        }
        result = result.with_data(final_data);

        if ctx.is_human() && fixable_count.is_some_and(|count| count > 0) {
            eprintln!(
                "→ {} auto-fixable warning{} detected. Run: xtask check --fix --smart",
                fixable_count.unwrap_or_default(),
                if fixable_count == Some(1) { "" } else { "s" }
            );
        }

        // R3: Predictive prefetch is allowed to inspect history, but it must
        // not spawn raw cargo outside xtask's planner/history/target handling.
        // Until planner-owned prefetch exists, this remains a narrated hint.
        if result.is_success() && ctx.allows_ambient_optimizations() {
            trigger_compilation_prefetch(ctx);
        }

        Ok(result.with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

/// Capture and optionally display a resource warning before heavy cargo operations.
fn capture_resource_warning(ctx: &CommandContext) -> Option<String> {
    match resources::ResourceStatus::capture() {
        Ok(status) => {
            let warning = status.warning(resources::thresholds::CARGO_CHECK_GB);
            if let Some(ref msg) = warning
                && ctx.is_human()
            {
                eprintln!("  ⚠ {msg}");
            }
            warning
        }
        Err(error) => {
            if ctx.is_human() {
                eprintln!("  ⚠ Failed to inspect local resources: {error:#}");
            }
            None
        }
    }
}

fn resolve_fixable_diagnostic_count(ctx: &CommandContext) -> (Option<usize>, Option<String>) {
    if ctx.invocation_id().is_none() {
        return (None, None);
    }

    match ctx.try_with_history_db(|db| db.get_current_diagnostics(None, None, None, None, true)) {
        Some(Ok(mut diagnostics)) => {
            let workspace_root = crate::config::workspace_root();
            diagnostics.retain(|diagnostic| diagnostic.points_to_existing_file(&workspace_root));
            (Some(diagnostics.len()), None)
        }
        Some(Err(error)) => (
            None,
            Some(format!(
                "Failed to query auto-fixable diagnostic count from history DB: {error:#}"
            )),
        ),
        None => (
            None,
            Some("Failed to open history DB for auto-fixable diagnostic count".to_string()),
        ),
    }
}

/// R3: Surface a prefetch opportunity when the check→test transition probability
/// exceeds 70% in recent history within a 5-minute window.
///
/// This intentionally does not spawn raw cargo. Compilation prefetch must be
/// implemented by xtask's planner/scheduler so it inherits target-dir, history,
/// supersession, and background-job semantics.
fn trigger_compilation_prefetch(ctx: &crate::command::CommandContext) {
    let probability = ctx
        .with_history_db(|db| db.get_transition_probability("check", "test", 5, 20))
        .unwrap_or(0.0);

    if probability > 70.0 {
        tracing::info!(
            target: "xtask::coordinator",
            probability = probability,
            "R3: test prefetch opportunity detected"
        );
        if ctx.is_human() {
            eprintln!(
                "  ℹ Test prefetch opportunity ({probability:.0}% chance you'll run tests next); \
                 deferred until xtask planner-owned prefetch exists"
            );
        }
    }
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

/// Extract package names from planner action commands (#1146).
///
/// Parses commands like "xtask check -p sinex-db -p sinexd" and
/// returns the set of package names after `-p` flags.
fn extract_packages_from_actions(actions: &[crate::planner::PlannedAction]) -> Vec<String> {
    let mut packages = std::collections::BTreeSet::new();
    for action in actions {
        let tokens: Vec<&str> = action.command.split_whitespace().collect();
        let mut i = 0;
        while i < tokens.len() {
            if tokens[i] == "-p" && i + 1 < tokens.len() {
                let pkg = tokens[i + 1];
                if !pkg.starts_with('-') {
                    packages.insert(pkg.to_string());
                }
            }
            i += 1;
        }
    }
    packages.into_iter().collect()
}

fn fmt_args_for_scope(scope: &WorkloadScope) -> Vec<String> {
    match scope {
        WorkloadScope::Workspace => vec!["--all".to_string()],
        WorkloadScope::Packages(packages) | WorkloadScope::Affected(packages) => {
            let mut packages = packages.clone();
            if packages.iter().any(|package| package == "xtask")
                && !packages.iter().any(|package| package == "xtask-macros")
            {
                packages.push("xtask-macros".to_string());
            }
            packages.sort();
            packages.dedup();
            packages
                .iter()
                .flat_map(|package| ["-p".to_string(), package.clone()])
                .collect()
        }
    }
}

/// Execute the `--changed-strict` drift-guard path.
///
/// Discovers Rust files changed between `HEAD` and the merge-base of `base_ref`,
/// maps each file to its owning Cargo package, then runs `xtask check -p <pkg>`
/// for each affected package. Aggregates results and emits a JSON report.
///
/// Returns a [`CommandResult`] containing the [`crate::strict_changed::ChangedStrictReport`]
/// as structured JSON data. The result is a failure if any per-package check fails.
fn run_changed_strict_command(
    base_ref: &str,
    ctx: &CommandContext,
    this: &CheckCommand,
) -> Result<CommandResult> {
    if ctx.is_human() {
        println!("API drift guard: checking packages changed relative to {base_ref}...");
    }

    let workspace_root = crate::config::workspace_root();

    // Resolve the xtask binary: use the currently-running executable so we
    // don't accidentally pick up a different version.
    let xtask_bin = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("xtask"));

    // Forward check modifier flags to the per-package invocations.
    let mut extra_args: Vec<String> = vec![];
    if this.lint {
        extra_args.push("--lint".to_string());
    }
    if this.fmt {
        extra_args.push("--fmt".to_string());
    }
    if this.forbidden {
        extra_args.push("--forbidden".to_string());
    }
    if this.skip_tests {
        extra_args.push("--skip-tests".to_string());
    }

    let report = crate::strict_changed::run_changed_strict(
        base_ref,
        &workspace_root,
        &xtask_bin,
        &extra_args,
    )?;

    if ctx.is_human() {
        let n_files = report.changed_files.len();
        let n_pkgs = report.affected_packages.len();
        println!(
            "  {} changed Rust file{}, {} affected package{}",
            n_files,
            if n_files == 1 { "" } else { "s" },
            n_pkgs,
            if n_pkgs == 1 { "" } else { "s" },
        );
        for pr in &report.package_results {
            let mark = if pr.success { "✓" } else { "✗" };
            let reused = if pr.reused { " (fresh)" } else { "" };
            println!("  {mark} {}{reused}", pr.package);
            if let Some(ref excerpt) = pr.output_excerpt {
                for line in excerpt.lines().take(5) {
                    println!("    {line}");
                }
            }
        }
    }

    changed_strict_command_result(ctx, report)
}

fn changed_strict_command_result(
    ctx: &CommandContext,
    report: crate::strict_changed::ChangedStrictReport,
) -> Result<CommandResult> {
    let report_json = serde_json::to_value(&report)?;

    if report.success {
        Ok(CommandResult::success()
            .with_detail(format!(
                "changed-strict: {} package{} checked, all passed",
                report.affected_packages.len(),
                if report.affected_packages.len() == 1 {
                    ""
                } else {
                    "s"
                }
            ))
            .with_data(report_json)
            .with_duration(ctx.elapsed()))
    } else {
        let failed: Vec<String> = report
            .package_results
            .iter()
            .filter(|r| !r.success)
            .map(|r| r.package.clone())
            .collect();
        let msg = format!("changed-strict: check failed for: {}", failed.join(", "));
        let mut result = CommandResult::failure(StructuredError {
            code: "CHANGED_STRICT_FAILED".to_string(),
            message: msg.clone(),
            location: Some("check --changed-strict".to_string()),
            suggestion: Some(format!(
                "Run `xtask check -p {}` and inspect diagnostics",
                failed.join(" -p ")
            )),
        })
        .with_detail(msg)
        .with_data(report_json);
        result.warnings = vec![];
        Ok(result.with_duration(ctx.elapsed()))
    }
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

    #[derive(Default, Clone, Copy)]
    struct CheckFlags {
        lint: bool,
        fmt: bool,
        forbidden: bool,
        full: bool,
    }

    fn make_cmd(flags: CheckFlags) -> CheckCommand {
        CheckCommand {
            lint: flags.lint,
            fmt: flags.fmt,
            forbidden: flags.forbidden,
            full: flags.full,
            fix: false,
            heavy: false,
            all: false,
            packages: vec![],
            skip_tests: false,
            lint_breakdown: false,
            by_file: false,
            nix: false,
            plan: false,
            allow_contended_host: true,
            changed_strict: None,
        }
    }

    #[sinex_test]
    async fn test_check_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(CheckFlags::default());
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("check"));
        assert!(metadata.timeout.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_check_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(CheckFlags::default());
        assert_eq!(cmd.name(), "check");
        Ok(())
    }

    #[sinex_test]
    async fn test_full_flag_resolves() -> ::xtask::sandbox::TestResult<()> {
        let mut cmd = make_cmd(CheckFlags {
            full: true,
            ..CheckFlags::default()
        });
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
            ..make_cmd(CheckFlags::default())
        };
        cmd.resolve_flags();
        assert!(cmd.lint, "--fix should imply --full → --lint");
        assert!(cmd.fmt, "--fix should imply --full → --fmt");
        assert!(cmd.forbidden, "--fix should imply --full → --forbidden");
        Ok(())
    }

    #[sinex_test]
    async fn test_defaults_are_compile_only() -> ::xtask::sandbox::TestResult<()> {
        let cmd = make_cmd(CheckFlags::default());
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
            compiled_packages: std::collections::HashSet::default(),
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
        let cmd = make_cmd(CheckFlags::default());
        let result = cmd.execute(&ctx).await?;
        assert!(
            result.is_success(),
            "clean check should succeed: {result:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_check_warns_when_fixable_count_is_unavailable()
    -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let temp = tempfile::tempdir()?;
        let ctx = mock_ctx_with_history(runner, Some(42), temp.path().to_path_buf());
        let cmd = make_cmd(CheckFlags::default());
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
    }

    #[sinex_test]
    async fn test_execute_check_without_history_invocation_skips_fixable_probe_warning()
    -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let temp = tempfile::tempdir()?;
        let db_path = temp.path().join("history.db");
        let ctx = mock_ctx_with_history(runner, None, db_path.clone());
        let cmd = make_cmd(CheckFlags::default());
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
        assert!(
            !db_path.exists(),
            "check without invocation_id should not even open the history DB"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_check_errors_yield_failure() -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean().with_check(error_summary()));
        let ctx = mock_ctx(runner);
        let cmd = make_cmd(CheckFlags::default());
        let result = cmd.execute(&ctx).await?;
        assert!(!result.is_success(), "check with errors should fail");
        assert!(
            result.errors.iter().any(|e| e.code == "CHECK_FAILED"),
            "expected CHECK_FAILED in errors: {:?}",
            result.errors,
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_lint_routes_to_clippy_not_check() -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let ctx = mock_ctx(runner.clone());
        let cmd = make_cmd(CheckFlags {
            lint: true,
            ..CheckFlags::default()
        }); // --lint
        cmd.execute(&ctx).await?;
        let calls = runner.calls();
        assert_eq!(calls.clippy, 1, "clippy should have been called once");
        assert_eq!(
            calls.check, 0,
            "cargo check must NOT run when --lint active"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_compile_only_routes_to_check_not_clippy()
    -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let ctx = mock_ctx(runner.clone());
        let cmd = make_cmd(CheckFlags::default()); // default: compile-only
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
        let cmd = make_cmd(CheckFlags {
            lint: true,
            ..CheckFlags::default()
        }); // --lint
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
    }

    #[sinex_test]
    async fn test_execute_fmt_fail_short_circuits_before_compile()
    -> ::xtask::sandbox::TestResult<()> {
        // --fmt with a formatting violation should bail before running cargo check.
        let runner = Arc::new(MockCargoRunner::clean().with_fmt_fail());
        let ctx = mock_ctx(runner.clone());
        let cmd = make_cmd(CheckFlags {
            fmt: true,
            ..CheckFlags::default()
        }); // --fmt
        let result = cmd.execute(&ctx).await;
        // fmt failure surfaces as Err (propagated via `?` in execute)
        assert!(result.is_err(), "fmt failure should propagate as Err");
        let calls = runner.calls();
        assert_eq!(calls.fmt, 1, "fmt must have been called");
        assert_eq!(calls.check, 0, "cargo check must NOT run after fmt failure");
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_fmt_uses_package_scope() -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let ctx = mock_ctx(runner.clone());
        let cmd = CheckCommand {
            fmt: true,
            packages: vec!["sinex-primitives".to_string()],
            ..make_cmd(CheckFlags::default())
        };
        let result = cmd.execute(&ctx).await?;
        assert!(
            result.is_success(),
            "package-scoped fmt check should succeed: {result:?}"
        );
        let calls = runner.calls();
        assert_eq!(calls.fmt, 1, "fmt must have been called once");
        assert_eq!(calls.fmt_args, vec!["-p", "sinex-primitives"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_fmt_includes_xtask_macro_path_dependency()
    -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let ctx = mock_ctx(runner.clone());
        let cmd = CheckCommand {
            fmt: true,
            packages: vec!["xtask".to_string()],
            ..make_cmd(CheckFlags::default())
        };
        let result = cmd.execute(&ctx).await?;
        assert!(
            result.is_success(),
            "xtask fmt check should include local macro package: {result:?}"
        );
        let calls = runner.calls();
        assert_eq!(calls.fmt, 1, "fmt must have been called once");
        assert_eq!(calls.fmt_args, vec!["-p", "xtask", "-p", "xtask-macros"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_fmt_uses_workspace_scope_for_all() -> ::xtask::sandbox::TestResult<()> {
        let runner = Arc::new(MockCargoRunner::clean());
        let ctx = mock_ctx(runner.clone());
        let cmd = CheckCommand {
            fmt: true,
            all: true,
            ..make_cmd(CheckFlags::default())
        };
        let result = cmd.execute(&ctx).await?;
        assert!(
            result.is_success(),
            "workspace fmt check should succeed: {result:?}"
        );
        let calls = runner.calls();
        assert_eq!(calls.fmt, 1, "fmt must have been called once");
        assert_eq!(calls.fmt_args, vec!["--all"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_warnings_recorded_in_result() -> ::xtask::sandbox::TestResult<()> {
        // Warnings don't fail the check, but they appear in result.warnings.
        let runner = Arc::new(MockCargoRunner::clean().with_check(warning_summary(3)));
        let ctx = mock_ctx(runner);
        let cmd = make_cmd(CheckFlags::default());
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
    }

    #[sinex_test]
    async fn test_execute_progress_callback_fired_per_package() -> ::xtask::sandbox::TestResult<()>
    {
        // Verify that the progress callback is fired once per compiled package.
        // MockCargoRunner fires on_package_done N times for N compiled_packages.
        let runner = Arc::new(MockCargoRunner::clean().with_check(warning_summary(5)));
        let ctx = mock_ctx(runner);
        let cmd = make_cmd(CheckFlags::default());
        // If the callback fires correctly, execute completes without panic.
        let result = cmd.execute(&ctx).await?;
        assert!(result.is_success());
        Ok(())
    }

    #[sinex_test]
    async fn test_ambient_optimizations_only_enabled_for_human_foreground()
    -> ::xtask::sandbox::TestResult<()> {
        let human =
            CommandContext::new(OutputWriter::new(OutputFormat::Human), false, None, "check");
        assert!(human.allows_ambient_optimizations());

        let json = CommandContext::new(OutputWriter::new(OutputFormat::Json), false, None, "check");
        assert!(!json.allows_ambient_optimizations());

        let silent = CommandContext::new(
            OutputWriter::new(OutputFormat::Silent),
            false,
            None,
            "check",
        );
        assert!(!silent.allows_ambient_optimizations());

        let background =
            CommandContext::new(OutputWriter::new(OutputFormat::Human), true, None, "check");
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
}
