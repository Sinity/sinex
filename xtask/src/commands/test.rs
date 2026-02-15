//! Test command - run nextest with profiles and options
//!
//! Provides a rich TUI experience while capturing detailed test execution data
//! (timing, output, system resources) into the history database.
//!
//! This module has been refactored to delegate core logic to `crate::nextest`.

use anyhow::Result;

use crate::affected;
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::HistoryDb;
use crate::nextest::runner::TestRunner;
use crate::process::ProcessBuilder;

// UI & System monitoring
use console::style;

/// Test command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct TestCommand {
    /// Use debug profile (single-threaded, extended timeout)
    #[arg(long)]
    pub debug: bool,

    /// Stop on first failure (default: false, run all tests)
    #[arg(long)]
    pub fail_fast: bool,

    /// Number of threads (default: 24, debug: 1)
    #[arg(short, long)]
    pub threads: Option<usize>,

    /// Test retries (nextest)
    #[arg(short, long)]
    pub retries: Option<usize>,

    /// Test timeout (nextest)
    #[arg(long)]
    pub timeout: Option<String>,

    /// Run only on affected packages (DEFAULT - use --all to run all)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub affected: bool,

    /// Prime database before testing
    #[arg(long)]
    pub prime: bool,

    /// List tests instead of running
    #[arg(long, short)]
    pub list: bool,

    /// Filter tests by name pattern (nextest -E filter)
    #[arg(long, short = 'E')]
    pub filter: Option<String>,

    /// Run tests for specific package(s)
    #[arg(long, short = 'p')]
    pub package: Option<Vec<String>>,

    /// Print what would happen
    #[arg(long)]
    pub dry_run: bool,

    /// Skip automatic infrastructure setup (preflight is ON by default)
    #[arg(long)]
    pub skip_preflight: bool,

    /// Include tests marked `#[ignore]`
    #[arg(long)]
    pub include_ignored: bool,

    /// Run fuzz tests
    #[arg(long)]
    pub fuzz: bool,

    /// Run mutation tests
    #[arg(long)]
    pub mutants: bool,

    /// Run heavy/ignored tests
    #[arg(long)]
    pub heavy: bool,

    /// Run ALL packages (disables --affected default)
    #[arg(short, long)]
    pub all: bool,

    // --- Bench/Coverage flags (legacy or specific modes) ---
    /// Run benchmarks (delegate to bench command)
    #[arg(long)]
    pub bench: bool,

    /// Run coverage (delegate to coverage command)
    #[arg(long)]
    pub coverage: bool,

    /// Arguments passed to the test binary (not supported by nextest directly, usually)
    #[arg(last = true)]
    pub args: Vec<String>,
}

#[async_trait::async_trait]
impl XtaskCommand for TestCommand {
    fn name(&self) -> &'static str {
        "test"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution (like build/check/fix)
        if ctx.is_background() {
            let mut args = Vec::new();
            if self.debug {
                args.push("--debug".to_string());
            }
            if self.fail_fast {
                args.push("--fail-fast".to_string());
            }
            if self.all {
                args.push("--all".to_string());
            }
            if self.heavy {
                args.push("--heavy".to_string());
            }
            if self.include_ignored {
                args.push("--include-ignored".to_string());
            }
            if self.skip_preflight {
                args.push("--skip-preflight".to_string());
            }
            if self.prime {
                args.push("--prime".to_string());
            }
            if self.fuzz {
                args.push("--fuzz".to_string());
            }
            if self.mutants {
                args.push("--mutants".to_string());
            }
            if self.coverage {
                args.push("--coverage".to_string());
            }
            if self.bench {
                args.push("--bench".to_string());
            }
            if let Some(ref f) = self.filter {
                args.push("-E".to_string());
                args.push(f.clone());
            }
            if let Some(ref pkgs) = self.package {
                for p in pkgs {
                    args.push("-p".to_string());
                    args.push(p.clone());
                }
            }
            if let Some(threads) = self.threads {
                args.push(format!("--threads={threads}"));
            }
            if let Some(retries) = self.retries {
                args.push(format!("--retries={retries}"));
            }
            if let Some(ref timeout) = self.timeout {
                args.push(format!("--timeout={timeout}"));
            }
            if !self.args.is_empty() {
                args.push("--".to_string());
                args.extend(self.args.clone());
            }
            return ctx.spawn_background("test", &args).await;
        }

        // Handle --bench flag - delegate to bench infrastructure
        if self.bench {
            // ... (keep existing bench delegation if needed, or remove if unused)
            use crate::bench::{self, BenchConfig};

            let config = BenchConfig {
                mode: crate::bench::BenchMode::Sweeps,
                profile: "fast".to_string(),
                runs: 3,
                threads: vec![12, 24],
                baseline: None,
                regression_threshold_pct: 10.0,
                history_db: None,
                history_trend_limit: 5,
                report_md: false,
                report_html: false,
                git_tag: false,
                dry_run: false,
                gha: false,
                bisect_good: None,
                bisect_bad: None,
                stress_limit: 100,
                soak_duration: 3600,
                output: None,
                verbose: false,
                refine_top_threads: 3,
                refine_threshold_pct: 10.0,
                refine_sweep_runs: 1,
                target: "workspace".to_string(),
                continue_on_fail: false,
                fail_fast: false,
            };
            return bench::run(config).map(|()| CommandResult::success());
        }

        // Handle --coverage flag
        if self.coverage {
            let subcommand = crate::commands::coverage::CoverageSubcommand::Html {
                output: "target/coverage".to_string(),
                open: true,
                package: None,
            };
            return crate::commands::coverage::CoverageCommand { subcommand }
                .execute(ctx)
                .await;
        }

        // Handle --fuzz flag
        if self.fuzz {
            return crate::commands::fuzz::FuzzCommand {
                subcommand: crate::commands::fuzz::FuzzSubcommand::List,
            }
            .execute(ctx)
            .await;
        }

        // Handle --mutants flag
        if self.mutants {
            return crate::commands::mutants::MutantsCommand {
                package: None,
                file: None,
                timeout: 300,
                jobs: 1,
                args: vec![],
            }
            .execute(ctx)
            .await;
        }

        // Check disk space
        if !check_disk_space_gb(2) {
            eprintln!(
                "{} Low disk space (<2GB). Tests might fail.",
                style("WARNING:").red().bold()
            );
        }

        // Preflight is default ON unless explicitly disabled
        if !self.skip_preflight {
            crate::preflight::ensure_ready(ctx)?;
        }

        // Determine profile
        // Available profiles in .config/nextest.toml:
        //   default = 24 threads, fail-fast=false (good for CI/batch runs)
        //   debug   = 1 thread, 300s slow-timeout (good for investigating tests)
        let profile = if self.debug { "debug" } else { "default" };
        let use_fail_fast = self.fail_fast;

        // Compute affected packages (default ON, --all disables it)
        // --all flag takes precedence over --affected default
        let use_affected = self.affected && !self.all;
        let affected_filter = if use_affected {
            let packages = affected::affected_packages()?;
            if packages.is_empty() {
                if ctx.is_human() {
                    println!("No packages affected by current changes.");
                }
                return Ok(CommandResult::success().with_duration(ctx.elapsed()));
            }

            let filter = affected::build_nextest_filter(&packages);
            if ctx.is_human() {
                println!("{}", affected::affected_summary(&packages));
            }
            Some(filter)
        } else {
            None
        };

        // List: show tests only
        if self.list {
            // For brevity, skipping full list impl here for now,
            // but could delegate to `nextest list` via ProcessBuilder
            // simplified:
            let mut cmd = ProcessBuilder::cargo().args(["nextest", "list", "--workspace"]);
            if let Some(f) = &affected_filter {
                cmd = cmd.args(["-E", f]);
            }
            cmd.run_ok()?;
            return Ok(CommandResult::success().with_detail("tests listed"));
        }

        // Dry-run
        if self.dry_run {
            // Similar to list
            return Ok(CommandResult::success().with_detail("dry-run passed"));
        }

        // Prime database pool
        if self.prime {
            ProcessBuilder::cargo()
                .args(["run", "-p", "sinex-test-utils", "--bin", "db_prime"])
                .with_description("prime test pool")
                .run_ok()?;
        }

        // --- PREPARE EXECUTION via Runner ---

        let mut runner = TestRunner::new(ctx, profile);

        if use_fail_fast {
            runner.add_arg("--fail-fast");
        } else {
            runner.add_arg("--no-fail-fast");
        }

        if let Some(threads) = self.threads {
            runner.add_arg(format!("--test-threads={threads}"));
        }
        if let Some(retries) = self.retries {
            runner.add_arg(format!("--retries={retries}"));
        }
        if let Some(ref timeout) = self.timeout {
            runner.add_arg(format!("--timeout={timeout}"));
        }

        // Filters
        if let Some(ref filter) = affected_filter {
            runner.add_arg("-E");
            runner.add_arg(filter);
        }
        if let Some(ref filter) = self.filter {
            runner.add_arg("-E");
            runner.add_arg(filter);
        }
        if let Some(ref packages) = self.package {
            for pkg in packages {
                runner.add_arg("-p");
                runner.add_arg(pkg);
            }
        }

        if self.include_ignored || self.heavy {
            // Use --run-ignored=all to run both regular and ignored tests
            // Note: --ignored alone would run ONLY ignored tests
            // Note: --all only affects package selection (all vs affected), not ignored tests
            runner.add_arg("--run-ignored=all");
        }

        // Pass through args to test binary
        for arg in &self.args {
            runner.add_arg(arg);
        }

        // History DB: open and use context invocation ID if available
        let history_db = open_history_db().ok();
        let invocation_id = ctx.invocation_id();

        let history_ctx = match (history_db.as_ref(), invocation_id) {
            (Some(db), Some(id)) => Some((db, id)),
            _ => None,
        };

        // Execute!
        let stats = runner.execute(history_ctx)?;

        if stats.failed > 0 {
            // Query per-test failure details from history DB for structured output
            let failures = history_ctx
                .and_then(|(db, _)| db.get_failing_tests_with_output(50).ok())
                .unwrap_or_default();

            Ok(CommandResult::failure(crate::output::StructuredError {
                code: "TEST_REGS".to_string(),
                message: format!("{} tests failed", stats.failed),
                location: Some("test".to_string()),
                suggestion: Some(
                    "Run with --debug for single-threaded output and longer timeouts".to_string(),
                ),
            })
            .with_data(serde_json::json!({
                "passed": stats.passed,
                "failed": stats.failed,
                "ignored": stats.ignored,
                "failures": failures,
            }))
            .with_duration(ctx.elapsed()))
        } else {
            Ok(CommandResult::success()
                .with_message(format!(
                    "Passed: {}, Ignored: {}",
                    stats.passed, stats.ignored
                ))
                .with_duration(ctx.elapsed()))
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::test()
    }
}

/// Check if sufficient disk space is available on current directory's filesystem
fn check_disk_space_gb(min_gb: u64) -> bool {
    #[cfg(unix)]
    {
        use nix::sys::statvfs::statvfs;
        if let Ok(stat) = statvfs(".") {
            let available_bytes = stat.blocks_available() * stat.fragment_size();
            let available_gb = available_bytes / (1024 * 1024 * 1024);
            return available_gb >= min_gb;
        }
    }
    true // Assume OK on non-Unix or if check fails
}

/// Open the history database
fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}
