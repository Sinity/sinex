use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::graph::WorkspaceGraph;
use crate::history::HistoryDb;
use crate::process::ProcessBuilder;
use color_eyre::eyre::Result;

#[derive(Debug, Clone, Default, clap::Args)]
pub struct FixCommand {
    /// Packages to fix (default: all workspace packages)
    #[arg(short, long)]
    pub package: Vec<String>,

    /// Only fix affected packages (DEFAULT - use --all to fix all)
    #[arg(short = 'A', long, default_value_t = true, action = clap::ArgAction::Set)]
    pub affected: bool,

    /// Fix ALL packages (disables --affected default)
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

#[async_trait::async_trait]
impl XtaskCommand for FixCommand {
    fn name(&self) -> &'static str {
        "fix"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution
        if ctx.is_background() {
            let mut args = Vec::new();
            for p in &self.package {
                args.push("-p".to_string());
                args.push(p.clone());
            }
            if self.affected {
                args.push("--affected".to_string());
            }
            if self.thorough {
                args.push("--thorough".to_string());
            }
            if self.smart {
                args.push("--smart".to_string());
            }
            return ctx.spawn_background("fix", &args);
        }

        // Determine which packages to fix
        let packages = if self.smart {
            self.resolve_smart_packages(ctx)?
        } else {
            self.resolve_packages()?
        };

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
        self.run_fmt(&packages)?;
        ctx.finish_stage(fmt_stage, true);

        if self.thorough && packages.is_empty() {
            // Thorough mode: iterate through all packages for maximum fix coverage
            let thorough_stage = ctx.start_stage("thorough");
            self.run_thorough_fixes(ctx)?;
            ctx.finish_stage(thorough_stage, true);
        } else {
            // Normal mode: single pass (fast but may miss some fixes)
            let cargo_fix_stage = ctx.start_stage("cargo_fix");
            self.run_cargo_fix(&packages)?;
            ctx.finish_stage(cargo_fix_stage, true);

            let clippy_fix_stage = ctx.start_stage("clippy_fix");
            self.run_clippy_fix(&packages)?;
            ctx.finish_stage(clippy_fix_stage, true);
        }

        Ok(CommandResult::success()
            .with_detail("fixes applied")
            .with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

impl FixCommand {
    /// Resolve which packages to fix based on flags
    fn resolve_packages(&self) -> Result<Vec<String>> {
        if !self.package.is_empty() {
            return Ok(self.package.clone());
        }

        if self.affected && !self.all {
            let affected_pkgs = crate::affected::affected_packages()?;
            if !affected_pkgs.is_empty() {
                // If affected packages found, return them.
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
        let db = match HistoryDb::open(&cfg.history_db_path()) {
            Ok(db) => db,
            Err(_) => {
                if ctx.is_human() {
                    println!("No diagnostic history available, falling back to normal fix...");
                }
                return self.resolve_packages();
            }
        };

        let _ = db.ensure_diagnostic_columns();
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
    fn run_fmt(&self, packages: &[String]) -> Result<()> {
        println!("Running cargo fmt...");
        let mut fmt = ProcessBuilder::cargo().arg("fmt");
        for p in packages {
            fmt = fmt.arg("-p").arg(p);
        }
        fmt.run_ok()
    }

    /// Run cargo fix (Rust compiler fixes)
    fn run_cargo_fix(&self, packages: &[String]) -> Result<()> {
        println!("Running cargo fix...");
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

    /// Run clippy --fix
    fn run_clippy_fix(&self, packages: &[String]) -> Result<()> {
        println!("Running clippy --fix...");
        let mut clippy = ProcessBuilder::cargo()
            .arg("clippy")
            .arg("--fix")
            .arg("--allow-dirty")
            .arg("--allow-staged")
            .arg("--all-targets");

        for p in packages {
            clippy = clippy.arg("-p").arg(p);
        }

        // Explicit -W flags needed because workspace lints alone don't trigger --fix
        clippy = clippy
            .arg("--")
            .arg("-W")
            .arg("clippy::all")
            .arg("-W")
            .arg("clippy::pedantic")
            .inherit_output();

        clippy.run_ok()
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
        self.run_cargo_fix(&[])?;

        // Second pass: clippy --fix per package
        for (i, pkg) in packages.iter().enumerate() {
            if ctx.is_human() {
                println!("[{}/{}] Fixing {}...", i + 1, packages.len(), pkg);
            }
            // Run clippy fix for this package
            let result = ProcessBuilder::cargo()
                .arg("clippy")
                .arg("--fix")
                .arg("--allow-dirty")
                .arg("--allow-staged")
                .arg("--all-targets")
                .arg("-p")
                .arg(pkg)
                .arg("--")
                .arg("-W")
                .arg("clippy::all")
                .arg("-W")
                .arg("clippy::pedantic")
                .inherit_output()
                .run_ok();

            // Continue on error (some packages may have issues)
            if let Err(e) = result
                && ctx.is_human()
            {
                eprintln!("  Warning: {pkg} had errors: {e}");
            }
        }

        // Final format pass
        println!("Final formatting pass...");
        ProcessBuilder::cargo().arg("fmt").run_ok()?;

        Ok(())
    }
}
