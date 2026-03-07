use crate::cargo_diagnostics::run_cargo_clippy;
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::graph::WorkspaceGraph;
use crate::history::HistoryDb;
use crate::preflight;
use crate::process::ProcessBuilder;
use color_eyre::eyre::{Result, eyre};

#[derive(Debug, Clone, Default, clap::Args)]
pub struct FixCommand {
    /// Packages to fix (default: all workspace packages)
    #[arg(short = 'p', long = "package")]
    pub packages: Vec<String>,

    /// Fix ALL packages (disables affected mode default)
    #[arg(short = 'a', long)]
    pub all: bool,

    /// Thorough mode: iterate packages individually for maximum fix coverage.
    /// Slower but catches more fixes since clippy --fix only applies to freshly compiled code.
    #[arg(short, long)]
    pub thorough: bool,

    /// Smart mode: only fix packages that have stored MachineApplicable diagnostics.
    /// Falls back to normal fix if no diagnostic data is available.
    #[arg(long)]
    pub smart: bool,
}

impl XtaskCommand for FixCommand {
    fn name(&self) -> &'static str {
        "fix"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution via coordinator (same as check/build)
        if ctx.is_background() {
            let mut args = Vec::new();
            for p in &self.packages {
                args.push("-p".to_string());
                args.push(p.clone());
            }
            if self.all {
                args.push("--all".to_string());
            }
            if self.thorough {
                args.push("--thorough".to_string());
            }
            if self.smart {
                args.push("--smart".to_string());
            }
            return crate::coordinator::coordinate_and_spawn("fix", &args, ctx);
        }

        // Guard: cargo fmt / cargo fix / clippy --fix all invoke cargo and need the target/ lock.
        // Running inside nextest would deadlock. Detect via NEXTEST_RUN_ID and fail immediately.
        if std::env::var("NEXTEST_RUN_ID").is_ok() {
            return Err(color_eyre::eyre::eyre!(
                "Cannot run `xtask fix` foreground inside an active nextest run — \
                 cargo fmt / cargo fix need the cargo target/ lock which nextest holds.\n\
                 Use `xtask fix --bg ...` to spawn in background instead."
            ));
        }

        // Ensure DB/NATS/schema are ready — cargo fix and clippy --fix compile against sqlx
        // which requires the database to be available for compile-time query verification.
        let preflight_stage = ctx.start_stage("preflight");
        let ready = preflight::ensure_ready(ctx);
        ctx.finish_stage(preflight_stage, ready.is_ok());
        ready?;

        // Determine which packages to fix
        let packages = if self.smart {
            self.resolve_smart_packages(ctx)?
        } else {
            self.resolve_packages()?
        };

        // H2/H3: Capture pre-fix diagnostic snapshot
        let pre_fix = ctx.with_history_db(|db| db.get_current_diagnostic_counts());

        // H3: Advisory if there are current errors (fix won't resolve compile errors)
        if ctx.is_human() {
            if let Some(ref counts) = pre_fix {
                if counts.errors > 0 {
                    eprintln!(
                        "→ {} compile error{} in history — fix cannot resolve compile errors. Run `xtask check` first.",
                        counts.errors,
                        if counts.errors == 1 { "" } else { "s" }
                    );
                }
            }
        }

        // H2: Record pre-fix snapshot in the invocation record
        if let (Some(counts), Some(inv_id)) = (pre_fix.as_ref(), ctx.invocation_id()) {
            let _ = ctx.with_history_db(|db| {
                db.record_fix_session_snapshot(
                    inv_id,
                    counts.errors as i64,
                    counts.warnings as i64,
                    counts.fixable as i64,
                )
            });
        }

        if ctx.is_human() {
            if packages.is_empty() {
                println!("Applying automatic fixes to entire workspace...");
            } else {
                println!(
                    "Applying automatic fixes to {} package(s)...",
                    packages.len()
                );
            }
        }

        // Run formatting first (always workspace-wide)
        let fmt_stage = ctx.start_stage("fmt");
        self.run_fmt(&packages, ctx.is_human())?;
        ctx.finish_stage(fmt_stage, true);

        if self.thorough && packages.is_empty() {
            // Thorough mode: iterate through all packages for maximum fix coverage
            let thorough_stage = ctx.start_stage("thorough");
            self.run_thorough_fixes(ctx)?;
            ctx.finish_stage(thorough_stage, true);
        } else {
            // Normal mode: single pass (fast but may miss some fixes)
            let cargo_fix_stage = ctx.start_stage("cargo_fix");
            self.run_cargo_fix(&packages, ctx.is_human())?;
            ctx.finish_stage(cargo_fix_stage, true);

            let clippy_fix_stage = ctx.start_stage("clippy_fix");
            self.run_clippy_fix(ctx, &packages)?;
            ctx.finish_stage(clippy_fix_stage, true);
        }

        // H2: Post-fix summary with before/after context
        let mut result = CommandResult::success().with_detail("fixes applied");

        if ctx.is_human() {
            if let Some(counts) = pre_fix {
                if counts.warnings > 0 || counts.errors > 0 {
                    eprintln!(
                        "Before: {} error{}, {} warning{} ({} auto-fixable). Fixes applied.",
                        counts.errors,
                        if counts.errors == 1 { "" } else { "s" },
                        counts.warnings,
                        if counts.warnings == 1 { "" } else { "s" },
                        counts.fixable
                    );
                    eprintln!("→ Verify with: xtask check");
                }
            }
        } else if let Some(counts) = &pre_fix {
            // JSON mode: include pre_fix snapshot in result data
            result = result.with_data(serde_json::json!({
                "pre_fix": {
                    "errors": counts.errors,
                    "warnings": counts.warnings,
                    "fixable": counts.fixable,
                }
            }));
        }

        Ok(result.with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::fix()
    }
}

impl FixCommand {
    /// Resolve which packages to fix based on flags
    fn resolve_packages(&self) -> Result<Vec<String>> {
        if !self.packages.is_empty() {
            return Ok(self.packages.clone());
        }

        if !self.all {
            let affected_pkgs = crate::affected::affected_packages()?;
            if !affected_pkgs.is_empty() {
                return Ok(affected_pkgs);
            }
            // If clean, fall through to all packages.
        }

        Ok(vec![])
    }

    /// Resolve packages with fixable diagnostics from history DB.
    /// Falls back to normal resolve_packages() if no data available.
    fn resolve_smart_packages(&self, ctx: &CommandContext) -> Result<Vec<String>> {
        let cfg = config();
        let db = if let Ok(db) = HistoryDb::open(&cfg.history_db_path()) {
            db
        } else {
            if ctx.is_human() {
                println!("No diagnostic history available, falling back to normal fix...");
            }
            return self.resolve_packages();
        };

        let fixable = db.get_current_diagnostics(None, None, None, None, true)?;

        if fixable.is_empty() {
            if ctx.is_human() {
                println!("No fixable diagnostics found in history, falling back to normal fix...");
            }
            return self.resolve_packages();
        }

        // Extract unique package names from fixable diagnostics
        let mut packages: Vec<String> = fixable
            .iter()
            .filter_map(|d| d.package.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        packages.sort();

        if ctx.is_human() {
            println!(
                "Smart fix: {} fixable diagnostic(s) in {} package(s): {}",
                fixable.len(),
                packages.len(),
                packages.join(", ")
            );
        }

        Ok(packages)
    }

    /// Get all workspace package names for thorough iteration
    fn all_workspace_packages() -> Result<Vec<String>> {
        let graph = WorkspaceGraph::new()?;
        Ok(graph
            .workspace_packages()
            .into_iter()
            .map(|p| p.name().to_string())
            .collect())
    }

    /// Run cargo fmt
    fn run_fmt(&self, packages: &[String], is_human: bool) -> Result<()> {
        if is_human {
            println!("Running cargo fmt...");
        }
        let mut fmt = ProcessBuilder::cargo().arg("fmt");
        for p in packages {
            fmt = fmt.arg("-p").arg(p);
        }
        fmt.run_ok()
    }

    /// Run cargo fix (Rust compiler fixes)
    fn run_cargo_fix(&self, packages: &[String], is_human: bool) -> Result<()> {
        if is_human {
            println!("Running cargo fix...");
        }
        let mut fix = ProcessBuilder::cargo()
            .arg("fix")
            .arg("--allow-dirty")
            .arg("--allow-staged")
            .arg("--all-targets")
            .inherit_output();
        for p in packages {
            fix = fix.arg("-p").arg(p);
        }
        fix.run_ok()
    }

    /// Run clippy --fix, capturing JSON output so diagnostics are recorded to the history DB.
    ///
    /// Uses `--message-format=json` with `--fix` in a single pass: cargo applies fixes while
    /// emitting JSON describing what it found. The pre-fix diagnostic state is recorded.
    fn run_clippy_fix(&self, ctx: &CommandContext, packages: &[String]) -> Result<()> {
        if ctx.is_human() {
            println!("Running clippy --fix...");
        }

        // Build arg list. Package flags must be owned before borrowing as &str.
        let mut args = vec!["--fix", "--allow-dirty", "--allow-staged", "--all-targets"];
        let pkg_pairs: Vec<[String; 2]> = packages
            .iter()
            .map(|p| ["-p".to_string(), p.clone()])
            .collect();
        for pair in &pkg_pairs {
            args.push(pair[0].as_str());
            args.push(pair[1].as_str());
        }
        // Explicit -W flags needed because workspace lints alone don't trigger --fix
        args.extend_from_slice(&["--", "-W", "clippy::all", "-W", "clippy::pedantic"]);

        let summary = run_cargo_clippy(&args)?;

        if let Err(e) = ctx.record_diagnostics(&summary.diagnostics)
            && ctx.is_human()
        {
            eprintln!("Warning: failed to record fix diagnostics: {e}");
        }

        if summary.success {
            Ok(())
        } else {
            Err(eyre!("clippy --fix failed"))
        }
    }

    /// Thorough mode: iterate packages individually for maximum fix coverage.
    /// Clippy --fix only applies fixes for warnings emitted during compilation.
    /// Cached builds don't re-emit warnings, so per-package iteration catches more.
    fn run_thorough_fixes(&self, ctx: &CommandContext) -> Result<()> {
        let packages = Self::all_workspace_packages()?;

        if ctx.is_human() {
            println!(
                "Thorough mode: iterating {} packages individually...",
                packages.len()
            );
        }

        // First pass: cargo fix on all
        self.run_cargo_fix(&[], ctx.is_human())?;

        // Second pass: clippy --fix per package, capturing diagnostics into the history DB
        for (i, pkg) in packages.iter().enumerate() {
            if ctx.is_human() {
                println!("[{}/{}] Fixing {}...", i + 1, packages.len(), pkg);
            }

            let args = vec![
                "--fix",
                "--allow-dirty",
                "--allow-staged",
                "--all-targets",
                "-p",
                pkg.as_str(),
                "--",
                "-W",
                "clippy::all",
                "-W",
                "clippy::pedantic",
            ];

            match run_cargo_clippy(&args) {
                Ok(summary) => {
                    if let Err(e) = ctx.record_diagnostics(&summary.diagnostics)
                        && ctx.is_human()
                    {
                        eprintln!("  Warning: failed to record diagnostics for {pkg}: {e}");
                    }
                }
                Err(e) => {
                    if ctx.is_human() {
                        eprintln!("  Warning: {pkg} had errors: {e}");
                    }
                }
            }
        }

        // Final format pass
        if ctx.is_human() {
            println!("Final formatting pass...");
        }
        ProcessBuilder::cargo().arg("fmt").run_ok()?;

        Ok(())
    }
}
