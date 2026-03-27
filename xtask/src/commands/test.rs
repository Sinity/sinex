//! Test command - run nextest with profiles and options
//!
//! Provides a rich TUI experience while capturing detailed test execution data
//! (timing, output, system resources) into the history database.
//!
//! Specialized test modes (bench, fuzz, coverage, mutants, vm) are subcommands,
//! not flags. Bare `xtask test` runs the default nextest path.

use color_eyre::eyre::Result;
use std::path::PathBuf;

use crate::affected;
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::nextest::runner::TestRunner;
use crate::process::ProcessBuilder;

// UI & System monitoring
use console::style;

fn failing_test_details_issue(ctx: &CommandContext, error: Option<&color_eyre::Report>) -> String {
    match error {
        Some(error) => format!(
            "Failed to read failing-test details from history DB at {}: {error}",
            ctx.history_db_path().display()
        ),
        None => format!(
            "History DB unavailable at {} while reading failing-test details",
            ctx.history_db_path().display()
        ),
    }
}

fn flaky_test_probe_issue(ctx: &CommandContext, error: Option<&color_eyre::Report>) -> String {
    match error {
        Some(error) => format!(
            "Failed to read flaky-test history from DB at {}: {error}",
            ctx.history_db_path().display()
        ),
        None => format!(
            "History DB unavailable at {} while reading flaky-test history",
            ctx.history_db_path().display()
        ),
    }
}

fn load_failing_test_details(
    ctx: &CommandContext,
    limit: usize,
) -> (Vec<crate::history::FailingTest>, Option<String>) {
    match ctx.try_with_history_db(|db| db.get_failing_tests_with_output(limit)) {
        Some(Ok(failures)) => (failures, None),
        Some(Err(error)) => (Vec::new(), Some(failing_test_details_issue(ctx, Some(&error)))),
        None => (Vec::new(), Some(failing_test_details_issue(ctx, None))),
    }
}

fn load_flaky_tests(ctx: &CommandContext, limit: usize) -> (Vec<(String, String, i64)>, Option<String>) {
    match ctx.try_with_history_db(|db| db.get_flaky_tests(limit)) {
        Some(Ok(flaky)) => (flaky, None),
        Some(Err(error)) => (Vec::new(), Some(flaky_test_probe_issue(ctx, Some(&error)))),
        None => (Vec::new(), Some(flaky_test_probe_issue(ctx, None))),
    }
}

/// Test command configuration
///
/// Bare `xtask test` runs nextest (the common case). Specialized workflows
/// are subcommands: `test bench`, `test fuzz`, `test coverage`, `test mutants`, `test vm`.
#[derive(Debug, Clone, Default, clap::Args)]
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
    #[arg(long = "package", short = 'p')]
    pub packages: Vec<String>,

    /// Print what would happen
    #[arg(long)]
    pub dry_run: bool,

    /// Skip automatic infrastructure setup (preflight is ON by default)
    #[arg(long)]
    pub skip_preflight: bool,

    /// Include tests marked `#[ignore]`
    #[arg(long)]
    pub include_ignored: bool,

    /// Run heavy/ignored tests
    #[arg(long)]
    pub heavy: bool,

    /// Run ALL packages (disables affected mode default)
    #[arg(short, long)]
    pub all: bool,

    /// Update insta snapshots (sets INSTA_UPDATE=always).
    /// Replaces the manual `INSTA_UPDATE=always cargo nextest run ...` pattern.
    #[arg(long)]
    pub update_snapshots: bool,

    /// Arguments passed to the test binary (not supported by nextest directly, usually)
    #[arg(last = true)]
    pub args: Vec<String>,

    /// Specialized test mode
    #[command(subcommand)]
    pub subcommand: Option<TestSubcommand>,
}

/// Specialized test modes — each is a distinct workflow with its own flags.
#[derive(Debug, Clone, clap::Subcommand)]
pub enum TestSubcommand {
    /// Run benchmarks with optional contract enforcement
    ///
    /// Sweep, refine, bisect, stress, or soak modes. Use --contracts to enforce
    /// perf budgets from config/verify/perf-contracts.toml.
    Bench(BenchArgs),

    /// Run fuzz tests (requires cargo-fuzz)
    ///
    /// Discovers fuzz targets under crate/*/fuzz/ and runs them with libfuzzer.
    Fuzz(FuzzArgs),

    /// Run code coverage analysis (requires cargo-llvm-cov)
    Coverage(CoverageArgs),

    /// Run mutation testing (requires cargo-mutants)
    Mutants(MutantsArgs),

    /// Run NixOS VM tests
    ///
    /// `xtask test vm --category smoke` is the fast NixOS compatibility gate (~5-10min).
    Vm(VmArgs),
}

/// Benchmark arguments and perf-contract/report workflow
#[derive(Debug, Clone, clap::Args)]
pub struct BenchArgs {
    /// Benchmark mode
    #[arg(long, default_value = "sweeps")]
    pub mode: crate::bench::BenchMode,

    /// Nextest profile to use
    #[arg(long, default_value = "fast")]
    pub profile: String,

    /// Number of runs per configuration
    #[arg(long, default_value_t = 3)]
    pub runs: u32,

    /// Thread counts to test (comma-separated)
    #[arg(long, value_delimiter = ',', default_values_t = vec![12, 24])]
    pub threads: Vec<u32>,

    /// Target package(s) or "workspace"
    #[arg(long, default_value = "workspace")]
    pub target: String,

    /// Enforce perf contracts from config/verify/perf-contracts.toml
    #[arg(long)]
    pub contracts: bool,

    /// Contract file path (default: config/verify/perf-contracts.toml)
    #[arg(long)]
    pub contracts_file: Option<PathBuf>,

    /// Print summary from a stored perf report JSON
    #[arg(long)]
    pub report: Option<PathBuf>,

    /// Compare two perf reports: --compare <current> <previous>
    #[arg(long, num_args = 2)]
    pub compare: Option<Vec<PathBuf>>,

    /// Output directory for artifacts
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// History DB path for benchmark series
    #[arg(long)]
    pub history_db: Option<PathBuf>,

    /// Dry run (compile only, no test execution)
    #[arg(long)]
    pub dry_run: bool,

    /// Verbose output
    #[arg(long)]
    pub verbose: bool,
}

/// Fuzz test arguments
#[derive(Debug, Clone, clap::Args)]
pub struct FuzzArgs {
    /// Specific target to run (format: crate::target_name)
    pub target: Option<String>,

    /// Maximum fuzzing time in seconds (default: 60)
    #[arg(long, default_value_t = 60)]
    pub max_time: u64,

    /// Number of parallel fuzzing jobs
    #[arg(long)]
    pub jobs: Option<usize>,

    /// List available fuzz targets instead of running
    #[arg(long)]
    pub list: bool,
}

/// Coverage arguments
#[derive(Debug, Clone, clap::Args)]
pub struct CoverageArgs {
    /// Output directory
    #[arg(long, default_value = "target/coverage")]
    pub output: String,

    /// Open HTML report in browser
    #[arg(long)]
    pub open: bool,

    /// Specific package
    #[arg(short, long)]
    pub package: Option<String>,

    /// Generate HTML report (default)
    #[arg(long)]
    pub html: bool,

    /// Enforce minimum coverage threshold
    #[arg(long)]
    pub enforce: Option<f64>,
}

/// Mutation testing arguments
#[derive(Debug, Clone, clap::Args)]
pub struct MutantsArgs {
    /// Specific package
    #[arg(short, long)]
    pub package: Option<String>,

    /// Specific file to mutate
    #[arg(short, long)]
    pub file: Option<String>,

    /// Timeout per mutant in seconds
    #[arg(long, default_value_t = 300)]
    pub timeout: u64,

    /// Number of parallel jobs
    #[arg(short, long, default_value_t = 1)]
    pub jobs: usize,
}

/// VM test arguments
#[derive(Debug, Clone, clap::Args)]
pub struct VmArgs {
    /// Test category: smoke, integration, performance, chaos, all
    #[arg(long)]
    pub category: Option<String>,

    /// Run VM tests in parallel
    #[arg(long)]
    pub parallel: bool,

    /// Additional test arguments
    #[arg(last = true)]
    pub args: Vec<String>,
}

impl XtaskCommand for TestCommand {
    fn name(&self) -> &'static str {
        "test"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution (like build/check/fix)
        if ctx.is_background() {
            let mut args = Vec::new();

            // Serialize subcommand first (if any)
            match &self.subcommand {
                Some(TestSubcommand::Bench(bench)) => {
                    args.push("bench".to_string());
                    args.push(format!("--mode={}", bench.mode));
                    args.push(format!("--profile={}", bench.profile));
                    args.push(format!("--runs={}", bench.runs));
                    let threads_str: Vec<String> =
                        bench.threads.iter().map(|t| t.to_string()).collect();
                    args.push(format!("--threads={}", threads_str.join(",")));
                    args.push(format!("--target={}", bench.target));
                    if bench.contracts {
                        args.push("--contracts".to_string());
                    }
                    if let Some(ref f) = bench.contracts_file {
                        args.push(format!("--contracts-file={}", f.display()));
                    }
                    if let Some(ref r) = bench.report {
                        args.push(format!("--report={}", r.display()));
                    }
                    if let Some(ref c) = bench.compare {
                        args.push(format!("--compare={}", c[0].display()));
                        args.push(c[1].display().to_string());
                    }
                    if let Some(ref o) = bench.output {
                        args.push(format!("--output={}", o.display()));
                    }
                    if let Some(ref h) = bench.history_db {
                        args.push(format!("--history-db={}", h.display()));
                    }
                    if bench.dry_run {
                        args.push("--dry-run".to_string());
                    }
                    if bench.verbose {
                        args.push("--verbose".to_string());
                    }
                }
                Some(TestSubcommand::Fuzz(fuzz)) => {
                    args.push("fuzz".to_string());
                    if let Some(ref t) = fuzz.target {
                        args.push(t.clone());
                    }
                    args.push(format!("--max-time={}", fuzz.max_time));
                    if let Some(j) = fuzz.jobs {
                        args.push(format!("--jobs={j}"));
                    }
                    if fuzz.list {
                        args.push("--list".to_string());
                    }
                }
                Some(TestSubcommand::Coverage(cov)) => {
                    args.push("coverage".to_string());
                    args.push(format!("--output={}", cov.output));
                    if cov.open {
                        args.push("--open".to_string());
                    }
                    if let Some(ref p) = cov.package {
                        args.push(format!("--package={p}"));
                    }
                    if cov.html {
                        args.push("--html".to_string());
                    }
                    if let Some(e) = cov.enforce {
                        args.push(format!("--enforce={e}"));
                    }
                }
                Some(TestSubcommand::Mutants(m)) => {
                    args.push("mutants".to_string());
                    if let Some(ref p) = m.package {
                        args.push(format!("--package={p}"));
                    }
                    if let Some(ref f) = m.file {
                        args.push(format!("--file={f}"));
                    }
                    args.push(format!("--timeout={}", m.timeout));
                    args.push(format!("--jobs={}", m.jobs));
                }
                Some(TestSubcommand::Vm(vm)) => {
                    args.push("vm".to_string());
                    if let Some(ref c) = vm.category {
                        args.push(format!("--category={c}"));
                    }
                    if vm.parallel {
                        args.push("--parallel".to_string());
                    }
                    if !vm.args.is_empty() {
                        args.push("--".to_string());
                        args.extend(vm.args.clone());
                    }
                }
                None => {
                    // Default nextest mode — serialize nextest-specific flags
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
                    if self.list {
                        args.push("--list".to_string());
                    }
                    if self.skip_preflight {
                        args.push("--skip-preflight".to_string());
                    }
                    if self.prime {
                        args.push("--prime".to_string());
                    }
                    if self.dry_run {
                        args.push("--dry-run".to_string());
                    }
                    if self.update_snapshots {
                        args.push("--update-snapshots".to_string());
                    }
                    if let Some(ref f) = self.filter {
                        args.push("-E".to_string());
                        args.push(f.clone());
                    }
                    for p in &self.packages {
                        args.push("-p".to_string());
                        args.push(p.clone());
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
                }
            }

            return crate::coordinator::coordinate_and_spawn("test", &args, ctx);
        }

        // Dispatch to subcommand handler if present
        if let Some(ref sub) = self.subcommand {
            return match sub {
                TestSubcommand::Bench(bench) => execute_bench(bench, ctx),
                TestSubcommand::Fuzz(fuzz) => execute_fuzz(fuzz, ctx).await,
                TestSubcommand::Coverage(cov) => execute_coverage(cov, ctx).await,
                TestSubcommand::Mutants(m) => execute_mutants(m, ctx).await,
                TestSubcommand::Vm(vm) => execute_vm(vm, ctx).await,
            };
        }

        if self.dry_run {
            return Ok(CommandResult::success().with_detail("dry-run passed"));
        }

        // Record fingerprint+scope for coordinator freshness detection.
        {
            let mut scope_args = Vec::new();
            for p in &self.packages {
                scope_args.push("-p".to_string());
                scope_args.push(p.clone());
            }
            if let Some(ref f) = self.filter {
                scope_args.push("-E".to_string());
                scope_args.push(f.clone());
            }
            if self.heavy {
                scope_args.push("--heavy".to_string());
            }
            if self.include_ignored {
                scope_args.push("--include-ignored".to_string());
            }
            if self.all {
                scope_args.push("--all".to_string());
            }
            ctx.record_coordination_fingerprint("test", &scope_args);
        }

        let low_disk_space = !check_disk_space_gb(2);
        let low_disk_space_warning = "Low disk space (<2GB). Tests might fail.";

        // Check disk space (human warning plus structured warning on final result)
        if ctx.is_human() && low_disk_space {
            eprintln!(
                "{} Low disk space (<2GB). Tests might fail.",
                style("WARNING:").red().bold()
            );
        }

        // Guard: running xtask test foreground inside nextest causes a cargo target/ lock
        // deadlock. nextest holds the lock for its entire run; a nested `cargo nextest` waits
        // forever. Detect via NEXTEST_RUN_ID (set by nextest in all children) and bail.
        // Note: special lanes (bench, coverage, mutants) have their own guards above.
        // --fuzz is safe (directory listing only, no cargo subprocess).
        if std::env::var("NEXTEST_RUN_ID").is_ok() {
            return Err(color_eyre::eyre::eyre!(
                "Cannot run `xtask test` foreground inside an active nextest run — \
                 the cargo target/ lock would deadlock.\n\
                 Use `xtask test --bg ...` to spawn in background instead:\n\
                 \n  xtask test --bg [your flags]\n\
                 \n  Then: xtask jobs wait <ID>"
            ));
        }

        // Preflight is default ON unless explicitly disabled
        if !self.skip_preflight {
            let stage = ctx.start_stage("preflight");
            let ready = crate::preflight::ensure_ready(ctx);
            ctx.finish_stage(stage, ready.is_ok());
            ready?;
        }

        // Determine profile
        // Available profiles in .config/nextest.toml:
        //   default = 24 threads, fail-fast=false (good for CI/batch runs)
        //   debug   = 1 thread, 300s slow-timeout (good for investigating tests)
        let profile = if self.debug { "debug" } else { "default" };
        let use_fail_fast = self.fail_fast;

        // Affected mode is default ON, --all disables it
        let use_affected = !self.all && self.packages.is_empty();
        let affected_filter = if use_affected {
            let stage = ctx.start_stage("affected");
            let packages = affected::affected_packages();
            ctx.finish_stage(stage, packages.is_ok());
            let packages = packages?;
            if packages.is_empty() {
                // Smart default: If no changes detected (clean repo), run EVERYTHING
                // instead of running nothing.
                if ctx.is_human() {
                    println!("No changes detected. Running ALL tests.");
                }
                None
            } else {
                let filter = affected::build_nextest_filter(&packages);
                if ctx.is_human() {
                    println!("{}", affected::affected_summary(&packages));
                }
                Some(filter)
            }
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

        // Prime database pool — pre-provision all slots upfront
        if self.prime {
            println!("{}", style("Priming test database pool...").cyan());
            crate::sandbox::db::pool::prime_pool().await?;
            println!("{}", style("Test pool primed successfully").green());
        }

        // --- PREPARE EXECUTION via Runner ---

        let test_stage = ctx.start_stage("test");
        let mut runner = TestRunner::new(ctx, profile);

        if self.update_snapshots {
            runner.add_env("INSTA_UPDATE", "always");
        }

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
        // When -p is specified, skip the affected filter — -p already constrains
        // the package scope and the affected filter is redundant.
        // When both affected and user filters exist, AND them into a single -E
        // expression, because nextest ORs multiple -E args (which would make
        // the narrower filter a no-op).
        if self.packages.is_empty() {
            match (affected_filter.as_ref(), self.filter.as_ref()) {
                (Some(affected), Some(user)) => {
                    // AND them: run only tests matching BOTH filters.
                    runner.add_arg("-E");
                    runner.add_arg(format!("({affected}) & ({user})"));
                }
                (Some(filter), None) | (None, Some(filter)) => {
                    runner.add_arg("-E");
                    runner.add_arg(filter);
                }
                (None, None) => {}
            }
        } else {
            for pkg in &self.packages {
                runner.add_arg("-p");
                runner.add_arg(pkg);
            }
            // Only the user filter applies when -p is specified.
            if let Some(ref filter) = self.filter {
                runner.add_arg("-E");
                runner.add_arg(filter);
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

        // Execute! Use the cached HistoryDb from CommandContext instead of
        // opening a second connection.
        let stats = match ctx.try_with_history_db(|db| {
            let invocation_id = ctx.invocation_id().unwrap_or(0);
            runner.execute(Some((db, invocation_id)))
        }) {
            Some(result) => result?,
            None => runner.execute(None)?,
        };

        ctx.finish_stage(test_stage, stats.failed == 0);

        if stats.failed > 0 {
            // Query per-test failure details from history DB for structured output
            let (failures, failure_details_issue) = load_failing_test_details(ctx, 50);

            // H4: Inline failure table for human mode (capped at 5)
            if ctx.is_human() && !failures.is_empty() {
                let shown = failures.len().min(5);
                eprintln!("\nFailed tests:");
                for failure in failures.iter().take(shown) {
                    eprintln!("  ✗ {} ({})", failure.test_name, failure.package);
                }
                if failures.len() > 5 {
                    eprintln!("  … and {} more", failures.len() - 5);
                }
                eprintln!();
            }
            if ctx.is_human() && let Some(issue) = &failure_details_issue {
                eprintln!("⚠  {issue}");
                eprintln!();
            }

            let mut result = CommandResult::failure(crate::output::StructuredError {
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
                "failure_details_issue": failure_details_issue.clone(),
            }))
            .with_duration(ctx.elapsed());
            if low_disk_space {
                result = result.with_warning(low_disk_space_warning);
            }
            if let Some(issue) = failure_details_issue {
                result = result.with_warning(issue);
            }
            Ok(result)
        } else {
            // H7: Surface flaky tests after a clean run
            let (flaky, flaky_issue) = load_flaky_tests(ctx, 5);
            if ctx.is_human() && !flaky.is_empty() {
                eprintln!(
                    "\n⚠  {} test{} passed on retry (flaky):",
                    flaky.len(),
                    if flaky.len() == 1 { "" } else { "s" }
                );
                for (name, pkg, _inv_id) in flaky.iter().take(3) {
                    eprintln!("   {name}  ({pkg})");
                }
                if flaky.len() > 3 {
                    eprintln!(
                        "   …and {} more. Run: xtask history tests flaky",
                        flaky.len() - 3
                    );
                } else {
                    eprintln!("   Run: xtask history tests flaky");
                }
                eprintln!();
            }
            if ctx.is_human() && let Some(issue) = &flaky_issue {
                eprintln!("⚠  {issue}");
                eprintln!();
            }

            let mut result = CommandResult::success()
                .with_message(format!(
                    "Passed: {}, Ignored: {}",
                    stats.passed, stats.ignored
                ))
                .with_duration(ctx.elapsed());
            if low_disk_space {
                result = result.with_warning(low_disk_space_warning);
            }
            if let Some(issue) = flaky_issue {
                result = result.with_warning(issue);
            }
            Ok(result)
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::test()
    }
}

#[cfg(test)]
mod tests {
    // Inline because these helpers are private and are exercised more directly here
    // than through a full nextest command harness.
    use super::*;
    use crate::command::CommandContext;
    use crate::history::HistoryDb;
    use crate::output::{OutputFormat, OutputWriter};
    use crate::sandbox::sinex_test;

    fn test_context(db_path: std::path::PathBuf) -> CommandContext {
        CommandContext::new_with_db_override(
            OutputWriter::new(OutputFormat::Silent),
            false,
            None,
            "test",
            db_path,
        )
    }

    #[sinex_test]
    async fn test_load_failing_test_details_surfaces_history_query_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let _db = HistoryDb::open(&db_path)?;
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("DROP TABLE test_results", [])?;

        let (_failures, issue) = load_failing_test_details(&test_context(db_path.clone()), 50);
        let issue = issue.expect("query failure should surface");
        assert!(issue.contains("Failed to read failing-test details"));
        assert!(issue.contains(&db_path.display().to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_load_flaky_tests_surfaces_history_query_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let _db = HistoryDb::open(&db_path)?;
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("DROP TABLE test_results", [])?;

        let (_flaky, issue) = load_flaky_tests(&test_context(db_path.clone()), 5);
        let issue = issue.expect("query failure should surface");
        assert!(issue.contains("Failed to read flaky-test history"));
        assert!(issue.contains(&db_path.display().to_string()));
        Ok(())
    }
}

// ─── Subcommand handlers ───────────────────────────────────────────────────

fn execute_bench(bench: &BenchArgs, ctx: &CommandContext) -> Result<CommandResult> {
    // Handle --report (read and print existing report)
    if let Some(ref report_path) = bench.report {
        return crate::commands::verify::execute_report(Some(report_path.clone()), ctx);
    }

    // Handle --compare (diff two reports)
    if let Some(ref paths) = bench.compare {
        return crate::commands::verify::execute_compare(&paths[0], &paths[1], ctx);
    }

    // Guard: bench invokes `cargo nextest run` which needs target/ lock
    if std::env::var("NEXTEST_RUN_ID").is_ok() {
        return Err(color_eyre::eyre::eyre!(
            "Cannot run `xtask test bench` inside an active nextest run — \
             cargo target/ lock would deadlock.\n\
             Use `xtask test --bg bench` instead."
        ));
    }

    if bench.contracts {
        // Contract enforcement mode for stored perf budgets.
        return crate::commands::verify::execute_perf(
            crate::commands::verify::PerfArgs {
                profile: bench.profile.clone(),
                runs: bench.runs,
                threads: bench.threads.clone(),
                target: bench.target.clone(),
                contracts: bench.contracts_file.clone(),
                output_dir: bench.output.clone(),
                history_db: bench.history_db.clone(),
            },
            ctx,
        );
    }

    // Standard bench mode
    use crate::bench::{self, BenchConfig};

    let config = BenchConfig {
        mode: bench.mode,
        profile: bench.profile.clone(),
        runs: bench.runs,
        threads: bench.threads.clone(),
        baseline: None,
        regression_threshold_pct: 10.0,
        history_db: bench.history_db.clone(),
        history_trend_limit: 5,
        report_md: false,
        report_html: false,
        git_tag: false,
        dry_run: bench.dry_run,
        gha: false,
        bisect_good: None,
        bisect_bad: None,
        stress_limit: 100,
        soak_duration: 3600,
        output: bench.output.clone(),
        verbose: bench.verbose,
        refine_top_threads: 3,
        refine_threshold_pct: 10.0,
        refine_sweep_runs: 1,
        target: bench.target.clone(),
        continue_on_fail: false,
        fail_fast: false,
    };
    bench::run(config).map(|()| CommandResult::success())
}

async fn execute_fuzz(fuzz: &FuzzArgs, ctx: &CommandContext) -> Result<CommandResult> {
    // List mode
    if fuzz.list || fuzz.target.is_none() {
        let list_result = crate::commands::fuzz::FuzzCommand {
            subcommand: crate::commands::fuzz::FuzzSubcommand::List,
        }
        .execute(ctx)
        .await?;
        let target_count = list_result
            .data
            .as_ref()
            .and_then(|data| data.get("target_count"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);

        if fuzz.list || target_count == 0 {
            if target_count == 0 {
                return Ok(CommandResult::failure(crate::output::StructuredError {
                    code: "FUZZ_NO_TARGETS".to_string(),
                    message: "No fuzz targets found".to_string(),
                    location: Some("test fuzz".to_string()),
                    suggestion: Some(
                        "Add fuzz targets under crate/*/fuzz/ and rerun `xtask test fuzz`."
                            .to_string(),
                    ),
                })
                .with_duration(ctx.elapsed()));
            }
            return Ok(list_result);
        }
    }

    // Run specific target
    if let Some(ref target) = fuzz.target {
        return crate::commands::fuzz::FuzzCommand {
            subcommand: crate::commands::fuzz::FuzzSubcommand::Run {
                target: target.clone(),
                max_time: fuzz.max_time,
                jobs: fuzz.jobs,
            },
        }
        .execute(ctx)
        .await;
    }

    Ok(CommandResult::success()
        .with_message("No fuzz target specified")
        .with_duration(ctx.elapsed()))
}

async fn execute_coverage(cov: &CoverageArgs, ctx: &CommandContext) -> Result<CommandResult> {
    // Guard: coverage invokes `cargo llvm-cov` which needs target/ lock
    if std::env::var("NEXTEST_RUN_ID").is_ok() {
        return Err(color_eyre::eyre::eyre!(
            "Cannot run `xtask test coverage` inside an active nextest run — \
             cargo target/ lock would deadlock.\n\
             Use `xtask test --bg coverage` instead."
        ));
    }

    let subcommand = if let Some(threshold) = cov.enforce {
        crate::commands::coverage::CoverageSubcommand::Enforce {
            threshold,
            package: cov.package.clone(),
            html: cov.html,
            output: cov.output.clone(),
        }
    } else {
        crate::commands::coverage::CoverageSubcommand::Html {
            output: cov.output.clone(),
            open: cov.open,
            package: cov.package.clone(),
        }
    };

    crate::commands::coverage::CoverageCommand { subcommand }
        .execute(ctx)
        .await
}

async fn execute_mutants(m: &MutantsArgs, _ctx: &CommandContext) -> Result<CommandResult> {
    use color_eyre::eyre::eyre;

    // Guard: mutants invokes cargo-mutants which needs target/ lock
    if std::env::var("NEXTEST_RUN_ID").is_ok() {
        return Err(eyre!(
            "Cannot run `xtask test mutants` inside an active nextest run — \
             cargo target/ lock would deadlock.\n\
             Use `xtask test --bg mutants` instead."
        ));
    }

    if !ProcessBuilder::new("cargo-mutants")
        .arg("--version")
        .run_success()?
    {
        return Err(eyre!(
            "cargo-mutants not found in PATH. Add it to this repo's devshell/flake."
        ));
    }

    let mut builder = ProcessBuilder::new("cargo-mutants");
    builder = builder
        .arg("--timeout")
        .arg(format!("{}", m.timeout))
        .arg("--jobs")
        .arg(format!("{}", m.jobs));

    if let Some(pkg) = &m.package {
        builder = builder.arg("--package").arg(pkg);
    }
    if let Some(f) = &m.file {
        builder = builder.arg("--file").arg(f);
    }

    let description = match (&m.package, &m.file) {
        (Some(pkg), _) => format!("cargo-mutants --package {pkg}"),
        (None, Some(f)) => format!("cargo-mutants --file {f}"),
        (None, None) => "cargo-mutants (full workspace)".to_string(),
    };

    builder
        .with_description(&description)
        .inherit_output()
        .run()?;

    Ok(CommandResult::success()
        .with_message("Mutation testing completed successfully")
        .with_detail(format!("Timeout per mutant: {}s", m.timeout))
        .with_detail(format!("Parallel jobs: {}", m.jobs)))
}

async fn execute_vm(vm: &VmArgs, ctx: &CommandContext) -> Result<CommandResult> {
    let vm_cmd = crate::commands::vm::VmCommand {
        subcommand: crate::commands::vm::VmSubcommand::Test {
            category: vm.category.clone(),
            parallel: vm.parallel,
            timeout: crate::commands::vm::DEFAULT_TIMEOUT_SECS,
            keep_failed: false,
            list: false,
            validate: false,
            tests: vm.args.clone(),
        },
    };
    vm_cmd.execute(ctx).await
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
