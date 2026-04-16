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
use crate::command::{CommandContext, CommandMetadata, CommandResult, WorkloadScope, XtaskCommand};
use crate::nextest::runner::TestRunner;
use crate::process::ProcessBuilder;

// UI & System monitoring
use console::style;

const HEAVY_TEST_THREAD_CAP: usize = 4;

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

fn test_analysis_issue(ctx: &CommandContext, error: Option<&color_eyre::Report>) -> String {
    match error {
        Some(error) => format!(
            "Failed to analyze current test run from history DB at {}: {error}",
            ctx.history_db_path().display()
        ),
        None => format!(
            "History DB unavailable at {} while analyzing the current test run",
            ctx.history_db_path().display()
        ),
    }
}

fn load_current_test_analysis(ctx: &CommandContext) -> (Option<serde_json::Value>, Option<String>) {
    let Some(invocation_id) = ctx.invocation_id() else {
        return (
            None,
            Some("Current test invocation ID unavailable for analysis".to_string()),
        );
    };

    match ctx.try_with_history_db(|db| db.analyze_test_run(invocation_id)) {
        Some(Ok(Some(analysis))) => match serde_json::to_value(&analysis) {
            Ok(value) => (Some(value), None),
            Err(error) => (
                None,
                Some(format!(
                    "Failed to serialize test analysis for invocation {invocation_id}: {error}"
                )),
            ),
        },
        Some(Ok(None)) => (
            None,
            Some(format!(
                "No stored test analysis rows found for invocation {invocation_id}"
            )),
        ),
        Some(Err(error)) => (None, Some(test_analysis_issue(ctx, Some(&error)))),
        None => (None, Some(test_analysis_issue(ctx, None))),
    }
}

fn load_failing_test_details(
    ctx: &CommandContext,
    limit: usize,
) -> (Vec<crate::history::FailingTest>, Option<String>) {
    let Some(invocation_id) = ctx.invocation_id() else {
        return (
            Vec::new(),
            Some("Current test invocation ID unavailable".to_string()),
        );
    };

    match ctx.try_with_history_db(|db| db.get_failing_tests_with_output(invocation_id, limit)) {
        Some(Ok(failures)) => (failures, None),
        Some(Err(error)) => (
            Vec::new(),
            Some(failing_test_details_issue(ctx, Some(&error))),
        ),
        None => (Vec::new(), Some(failing_test_details_issue(ctx, None))),
    }
}

fn load_flaky_tests(
    ctx: &CommandContext,
    limit: usize,
) -> (Vec<(String, String, i64)>, Option<String>) {
    match ctx.try_with_history_db(|db| db.get_flaky_tests(limit)) {
        Some(Ok(flaky)) => (flaky, None),
        Some(Err(error)) => (Vec::new(), Some(flaky_test_probe_issue(ctx, Some(&error)))),
        None => (Vec::new(), Some(flaky_test_probe_issue(ctx, None))),
    }
}

fn nextest_history<'a>(
    ctx: &CommandContext,
    db: &'a crate::history::HistoryDb,
) -> Option<(&'a crate::history::HistoryDb, i64)> {
    ctx.invocation_id().map(|invocation_id| (db, invocation_id))
}

/// Run the repo's primary nextest-backed test workflows.
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

    /// Number of threads (default: profile default; heavy defaults to <=4, debug: 1)
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

    /// Update insta snapshots (sets `INSTA_UPDATE=always`).
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
    /// perf budgets from xtask/config/perf-contracts.toml.
    Bench(BenchArgs),

    /// Run fuzz tests (requires cargo-fuzz)
    ///
    /// Discovers fuzz targets under crate/*/fuzz/ and runs them with libfuzzer.
    Fuzz(FuzzArgs),

    /// Run code coverage analysis (requires cargo-llvm-cov)
    Coverage(CoverageArgs),

    /// Run mutation testing (requires cargo-mutants)
    Mutants(MutantsArgs),

    /// Run exported NixOS VM flake checks
    ///
    /// `xtask test vm --category smoke` is the fast exported NixOS compatibility gate (~5-10min).
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

    /// Enforce perf contracts from xtask/config/perf-contracts.toml
    #[arg(long)]
    pub contracts: bool,

    /// Contract file path (default: xtask/config/perf-contracts.toml)
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
    #[arg(long, default_value = ".sinex/coverage")]
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

    /// Timeout per test in seconds
    #[arg(long, default_value_t = crate::commands::vm::DEFAULT_TIMEOUT_SECS)]
    pub timeout: u64,

    /// Keep failed VM derivations for inspection
    #[arg(long)]
    pub keep_failed: bool,

    /// List exported VM checks instead of running them
    #[arg(long, short)]
    pub list: bool,

    /// Validate VM scenario files without running them
    #[arg(long)]
    pub validate: bool,

    /// Specific exported VM checks to run
    #[arg(last = true)]
    pub args: Vec<String>,
}

impl TestCommand {
    fn effective_threads(&self) -> Option<usize> {
        self.threads.or_else(|| {
            if self.heavy && !self.debug {
                let cpu_count = std::thread::available_parallelism()
                    .map_or(HEAVY_TEST_THREAD_CAP, std::num::NonZeroUsize::get);
                Some(default_heavy_test_threads(cpu_count))
            } else {
                None
            }
        })
    }

    fn semantic_invocation_args(&self, scope: &WorkloadScope) -> Vec<String> {
        let mut args = Vec::new();

        if self.debug {
            args.push("--debug".to_string());
        }
        if self.fail_fast {
            args.push("--fail-fast".to_string());
        }
        if self.heavy {
            args.push("--heavy".to_string());
        }
        if self.include_ignored {
            args.push("--include-ignored".to_string());
        }
        if self.update_snapshots {
            args.push("--update-snapshots".to_string());
        }
        if let Some(ref filter) = self.filter {
            args.push(format!("--filter={filter}"));
        }
        if let Some(threads) = self.effective_threads() {
            args.push(format!("--threads={threads}"));
        }
        if let Some(retries) = self.retries {
            args.push(format!("--retries={retries}"));
        }
        if let Some(ref timeout) = self.timeout {
            args.push(format!("--timeout={timeout}"));
        }

        args.push(scope.encode_marker());
        args
    }

    fn resolve_execution_plan(&self, ctx: Option<&CommandContext>) -> Result<NextestExecutionPlan> {
        let explicit_packages = normalize_packages(&self.packages);
        let inferred_packages = if !self.all {
            if let Some(filter) = &self.filter {
                let inferred_result = if let Some(ctx) = ctx {
                    let stage = ctx.start_stage("scope-inference");
                    let inferred = affected::infer_packages_for_test_filter(filter);
                    ctx.finish_stage(stage, inferred.is_ok());
                    inferred
                } else {
                    affected::infer_packages_for_test_filter(filter)
                };
                let inferred = normalize_packages(&inferred_result?);
                if let Some(ctx) = ctx
                    && ctx.is_human()
                    && !inferred.is_empty()
                {
                    println!(
                        "Inferred package scope from filter: {}",
                        inferred.join(", ")
                    );
                }
                inferred
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let affected_packages =
            if !self.all && explicit_packages.is_empty() && inferred_packages.is_empty() {
                let affected_result = if let Some(ctx) = ctx {
                    let stage = ctx.start_stage("affected");
                    let packages = affected::affected_packages();
                    ctx.finish_stage(stage, packages.is_ok());
                    packages
                } else {
                    affected::affected_packages()
                };
                let affected_packages = normalize_packages(&affected_result?);
                if !affected_packages.is_empty() {
                    if let Some(ctx) = ctx
                        && ctx.is_human()
                    {
                        println!("{}", affected::affected_summary(&affected_packages));
                    }
                    Some(affected_packages)
                } else {
                    if let Some(ctx) = ctx
                        && ctx.is_human()
                    {
                        println!("No changes detected. Running ALL tests.");
                    }
                    None
                }
            } else {
                None
            };

        Ok(resolve_nextest_execution_plan(
            &explicit_packages,
            inferred_packages,
            affected_packages,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NextestExecutionPlan {
    runner_packages: Vec<String>,
    workload_scope: WorkloadScope,
}

fn normalize_packages(packages: &[String]) -> Vec<String> {
    let mut packages = packages.to_vec();
    packages.sort();
    packages.dedup();
    packages
}

fn default_heavy_test_threads(cpu_count: usize) -> usize {
    cpu_count.clamp(1, HEAVY_TEST_THREAD_CAP)
}

fn resolve_nextest_execution_plan(
    explicit_packages: &[String],
    inferred_packages: Vec<String>,
    affected_packages: Option<Vec<String>>,
) -> NextestExecutionPlan {
    let explicit_packages = normalize_packages(explicit_packages);
    if !explicit_packages.is_empty() {
        return NextestExecutionPlan {
            runner_packages: explicit_packages.clone(),
            workload_scope: WorkloadScope::Packages(explicit_packages),
        };
    }

    let inferred_packages = normalize_packages(&inferred_packages);
    if !inferred_packages.is_empty() {
        return NextestExecutionPlan {
            runner_packages: inferred_packages.clone(),
            workload_scope: WorkloadScope::Packages(inferred_packages),
        };
    }

    if let Some(affected_packages) = affected_packages {
        let affected_packages = normalize_packages(&affected_packages);
        if !affected_packages.is_empty() {
            return NextestExecutionPlan {
                runner_packages: affected_packages.clone(),
                workload_scope: WorkloadScope::Affected(affected_packages),
            };
        }
    }

    NextestExecutionPlan {
        runner_packages: Vec::new(),
        workload_scope: WorkloadScope::Workspace,
    }
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
                        bench.threads.iter().map(ToString::to_string).collect();
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
                    if vm.timeout != crate::commands::vm::DEFAULT_TIMEOUT_SECS {
                        args.push(format!("--timeout={}", vm.timeout));
                    }
                    if vm.keep_failed {
                        args.push("--keep-failed".to_string());
                    }
                    if vm.list {
                        args.push("--list".to_string());
                    }
                    if vm.validate {
                        args.push("--validate".to_string());
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

                    let execution_plan = self.resolve_execution_plan(None)?;
                    let coordination_args =
                        self.semantic_invocation_args(&execution_plan.workload_scope);
                    return crate::coordinator::coordinate_and_spawn_with_scope(
                        "test",
                        &args,
                        &coordination_args,
                        ctx,
                    );
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
                TestSubcommand::Mutants(m) => execute_mutants(m, ctx),
                TestSubcommand::Vm(vm) => execute_vm(vm, ctx).await,
            };
        }

        if self.dry_run {
            return Ok(CommandResult::success().with_detail("dry-run passed"));
        }

        let disk_space_status = check_disk_space_gb(2);
        let low_disk_space_warning = "Low disk space (<2GB). Tests might fail.";
        let disk_space_probe_warning = match &disk_space_status {
            DiskSpaceStatus::Unknown { issue } => Some(format!(
                "Failed to inspect available disk space before running tests: {issue}"
            )),
            _ => None,
        };

        // Check disk space (human warning plus structured warning on final result)
        if ctx.is_human() {
            match &disk_space_status {
                DiskSpaceStatus::Low { .. } => {
                    eprintln!(
                        "{} Low disk space (<2GB). Tests might fail.",
                        style("WARNING:").red().bold()
                    );
                }
                DiskSpaceStatus::Unknown { issue } => {
                    eprintln!(
                        "{} Failed to inspect available disk space before running tests: {issue}",
                        style("WARNING:").red().bold()
                    );
                }
                DiskSpaceStatus::Sufficient { .. } => {}
            }
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
        let execution_plan = self.resolve_execution_plan(Some(ctx))?;
        let workload_scope = execution_plan.workload_scope.clone();
        let coordination_args = self.semantic_invocation_args(&workload_scope);
        ctx.record_coordination_fingerprint("test", &coordination_args);
        ctx.record_invocation_args(&coordination_args);

        // List: show tests only
        if self.list {
            let mut cmd = ProcessBuilder::cargo().args(["nextest", "list"]);
            if execution_plan.runner_packages.is_empty() {
                cmd = cmd.arg("--workspace");
            } else {
                for package in &execution_plan.runner_packages {
                    cmd = cmd.args(["-p", package]);
                }
            }
            if let Some(filter) = &self.filter {
                cmd = cmd.args(["-E", filter]);
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

        if let Some(threads) = self.effective_threads() {
            runner.add_arg(format!("--test-threads={threads}"));
        }
        if let Some(retries) = self.retries {
            runner.add_arg(format!("--retries={retries}"));
        }
        if let Some(ref timeout) = self.timeout {
            runner.add_arg(format!("--timeout={timeout}"));
        }

        for package in &execution_plan.runner_packages {
            runner.add_arg("-p");
            runner.add_arg(package);
        }
        if let Some(ref filter) = self.filter {
            runner.add_arg("-E");
            runner.add_arg(filter);
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
        let stats = match ctx.try_with_history_db(|db| runner.execute(nextest_history(ctx, db))) {
            Some(result) => result?,
            None => runner.execute(None)?,
        };

        ctx.finish_stage(test_stage, stats.failed == 0);

        if stats.failed > 0 {
            // Query per-test failure details from history DB for structured output
            let (failures, failure_details_issue) = load_failing_test_details(ctx, 50);
            let (analysis, analysis_issue) = load_current_test_analysis(ctx);

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
            if ctx.is_human()
                && let Some(issue) = &failure_details_issue
            {
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
                "invocation_id": ctx.invocation_id(),
                "passed": stats.passed,
                "failed": stats.failed,
                "ignored": stats.ignored,
                "failures": failures,
                "failure_details_issue": failure_details_issue.clone(),
                "analysis": analysis,
                "analysis_issue": analysis_issue.clone(),
            }))
            .with_detail(format!(
                "Inspect with: xtask history tests analyze --invocation {}",
                ctx.invocation_id().unwrap_or_default()
            ))
            .with_duration(ctx.elapsed());
            if matches!(disk_space_status, DiskSpaceStatus::Low { .. }) {
                result = result.with_warning(low_disk_space_warning);
            }
            if let Some(warning) = &disk_space_probe_warning {
                result = result.with_warning(warning.clone());
            }
            if let Some(issue) = failure_details_issue {
                result = result.with_warning(issue);
            }
            if let Some(issue) = analysis_issue {
                result = result.with_warning(issue);
            }
            Ok(result)
        } else {
            // H7: Surface flaky tests after a clean run
            let (flaky, flaky_issue) = load_flaky_tests(ctx, 5);
            let (analysis, analysis_issue) = load_current_test_analysis(ctx);
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
            if ctx.is_human()
                && let Some(issue) = &flaky_issue
            {
                eprintln!("⚠  {issue}");
                eprintln!();
            }

            let mut result = CommandResult::success()
                .with_message(format!(
                    "Passed: {}, Ignored: {}",
                    stats.passed, stats.ignored
                ))
                .with_data(serde_json::json!({
                    "invocation_id": ctx.invocation_id(),
                    "passed": stats.passed,
                    "failed": stats.failed,
                    "ignored": stats.ignored,
                    "flaky": flaky,
                    "flaky_issue": flaky_issue.clone(),
                    "analysis": analysis,
                    "analysis_issue": analysis_issue.clone(),
                }))
                .with_detail(format!(
                    "Inspect with: xtask history tests analyze --invocation {}",
                    ctx.invocation_id().unwrap_or_default()
                ))
                .with_duration(ctx.elapsed());
            if matches!(disk_space_status, DiskSpaceStatus::Low { .. }) {
                result = result.with_warning(low_disk_space_warning);
            }
            if let Some(warning) = &disk_space_probe_warning {
                result = result.with_warning(warning.clone());
            }
            if let Some(issue) = flaky_issue {
                result = result.with_warning(issue);
            }
            if let Some(issue) = analysis_issue {
                result = result.with_warning(issue);
            }
            Ok(result)
        }
    }

    fn metadata(&self) -> CommandMetadata {
        let mut metadata = CommandMetadata::test();
        if matches!(self.subcommand, Some(TestSubcommand::Vm(_))) {
            metadata.timeout = None;
        }
        metadata
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
        test_context_with_invocation(db_path, None)
    }

    fn test_context_with_invocation(
        db_path: std::path::PathBuf,
        invocation_id: Option<i64>,
    ) -> CommandContext {
        CommandContext::new_with_db_override(
            OutputWriter::new(OutputFormat::Silent),
            false,
            invocation_id,
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

        let (_failures, issue) =
            load_failing_test_details(&test_context_with_invocation(db_path.clone(), Some(1)), 50);
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

    #[sinex_test]
    async fn test_nextest_history_skips_recording_without_invocation()
    -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = HistoryDb::open(&db_path)?;
        let ctx = test_context(db_path);

        assert!(super::nextest_history(&ctx, &db).is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_nextest_history_preserves_real_invocation_id() -> ::xtask::sandbox::TestResult<()>
    {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = HistoryDb::open(&db_path)?;
        let ctx = CommandContext::new_with_db_override(
            OutputWriter::new(OutputFormat::Silent),
            false,
            Some(42),
            "test",
            db_path,
        );

        let (_db, invocation_id) =
            super::nextest_history(&ctx, &db).expect("history should keep the real invocation id");
        assert_eq!(invocation_id, 42);
        Ok(())
    }

    #[sinex_test]
    async fn test_vm_subcommand_disables_outer_command_timeout() -> ::xtask::sandbox::TestResult<()>
    {
        let command = TestCommand {
            subcommand: Some(TestSubcommand::Vm(VmArgs {
                category: Some("smoke".to_string()),
                parallel: false,
                timeout: crate::commands::vm::DEFAULT_TIMEOUT_SECS,
                keep_failed: false,
                list: false,
                validate: false,
                args: Vec::new(),
            })),
            ..Default::default()
        };

        let metadata = command.metadata();
        assert_eq!(metadata.category, Some("test"));
        assert!(metadata.timeout.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_prefers_explicit_packages()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &["sinex-db".into(), "xtask".into()],
            vec!["sinex-services".into()],
            Some(vec!["sinex-e2e-tests".into()]),
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-db".into(), "xtask".into()],
                workload_scope: WorkloadScope::Packages(vec!["sinex-db".into(), "xtask".into()]),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_prefers_inferred_packages_over_affected_scope()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &[],
            vec!["sinex-services".into()],
            Some(vec!["xtask".into(), "sinex-db".into(), "xtask".into()]),
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-services".into()],
                workload_scope: WorkloadScope::Packages(vec!["sinex-services".into()]),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_falls_back_to_affected_when_no_inference()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &[],
            Vec::new(),
            Some(vec!["xtask".into(), "sinex-db".into(), "xtask".into()]),
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-db".into(), "xtask".into()],
                workload_scope: WorkloadScope::Affected(vec!["sinex-db".into(), "xtask".into()]),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_falls_back_to_inferred_packages()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &[],
            vec!["sinex-e2e-tests".into(), "sinex-e2e-tests".into()],
            None,
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-e2e-tests".into()],
                workload_scope: WorkloadScope::Packages(vec!["sinex-e2e-tests".into()]),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_default_heavy_test_threads_caps_parallelism() -> ::xtask::sandbox::TestResult<()>
    {
        assert_eq!(default_heavy_test_threads(1), 1);
        assert_eq!(default_heavy_test_threads(2), 2);
        assert_eq!(default_heavy_test_threads(4), 4);
        assert_eq!(default_heavy_test_threads(24), 4);
        Ok(())
    }

    #[sinex_test]
    async fn test_effective_threads_prefers_explicit_override() -> ::xtask::sandbox::TestResult<()>
    {
        let command = TestCommand {
            heavy: true,
            threads: Some(9),
            ..Default::default()
        };

        assert_eq!(command.effective_threads(), Some(9));
        Ok(())
    }

    #[sinex_test]
    async fn test_semantic_invocation_args_include_heavy_thread_cap()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            heavy: true,
            ..Default::default()
        };

        let args = command.semantic_invocation_args(&WorkloadScope::Workspace);
        assert!(args.contains(&"--heavy".to_string()));
        assert!(args.contains(&"--threads=4".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_load_current_test_analysis_surfaces_current_invocation_summary()
    -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = HistoryDb::open(&db_path)?;
        let invocation_id = db.start_invocation("test", None, None, None)?;
        db.finish_invocation(
            invocation_id,
            crate::history::InvocationStatus::Success,
            Some(0),
            1.0,
        )?;
        db.store_test_results(
            invocation_id,
            &[crate::history::TestResult {
                test_name: "test_alpha".into(),
                package: "pkg-a".into(),
                status: crate::history::TestStatus::Pass,
                duration_secs: Some(0.25),
                attempt: 1,
                output: None,
            }],
        )?;

        let ctx = test_context_with_invocation(db_path, Some(invocation_id));
        let (analysis, issue) = super::load_current_test_analysis(&ctx);

        assert!(issue.is_none());
        let analysis = analysis.expect("analysis should be available");
        assert_eq!(analysis["invocation_id"], invocation_id);
        assert_eq!(analysis["total_passed"], 1);
        assert_eq!(analysis["total_failed"], 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_load_current_test_analysis_requires_invocation_id()
    -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let _db = HistoryDb::open(&db_path)?;
        let ctx = test_context(db_path);

        let (analysis, issue) = super::load_current_test_analysis(&ctx);
        assert!(analysis.is_none());
        assert_eq!(
            issue.as_deref(),
            Some("Current test invocation ID unavailable for analysis")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_fuzz_target_count_accepts_valid_count() -> ::xtask::sandbox::TestResult<()>
    {
        let result = CommandResult::success().with_data(serde_json::json!({
            "target_count": 3u64
        }));

        assert_eq!(super::parse_fuzz_target_count(&result)?, 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_fuzz_target_count_rejects_missing_count() -> ::xtask::sandbox::TestResult<()>
    {
        let result = CommandResult::success().with_data(serde_json::json!({
            "items": []
        }));

        let error =
            super::parse_fuzz_target_count(&result).expect_err("missing target count must surface");
        assert!(format!("{error:#}").contains("missing target_count"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_fuzz_target_count_rejects_non_numeric_count()
    -> ::xtask::sandbox::TestResult<()> {
        let result = CommandResult::success().with_data(serde_json::json!({
            "target_count": "three"
        }));

        let error = super::parse_fuzz_target_count(&result)
            .expect_err("non-numeric target count must surface");
        assert!(format!("{error:#}").contains("invalid target_count"));
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_disk_space_probe_reports_low_space() -> ::xtask::sandbox::TestResult<()>
    {
        let status = super::classify_disk_space_probe_result(Ok(1), 2);
        assert!(matches!(
            status,
            DiskSpaceStatus::Low {
                available_gb: 1,
                min_gb: 2
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_disk_space_probe_reports_sufficient_space()
    -> ::xtask::sandbox::TestResult<()> {
        let status = super::classify_disk_space_probe_result(Ok(4), 2);
        assert!(matches!(
            status,
            DiskSpaceStatus::Sufficient {
                available_gb: 4,
                min_gb: 2
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_disk_space_probe_surfaces_probe_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let status = super::classify_disk_space_probe_result(Err("statvfs failed".to_string()), 2);
        let DiskSpaceStatus::Unknown { issue } = status else {
            panic!("expected unknown disk-space status");
        };
        assert!(issue.contains("statvfs failed"));
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
        let target_count = parse_fuzz_target_count(&list_result)?;

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

fn parse_fuzz_target_count(result: &CommandResult) -> Result<u64> {
    let data = result
        .data
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("fuzz list result is missing structured data"))?;
    let target_count = data
        .get("target_count")
        .ok_or_else(|| color_eyre::eyre::eyre!("fuzz list result is missing target_count"))?;
    target_count
        .as_u64()
        .ok_or_else(|| color_eyre::eyre::eyre!("fuzz list result has invalid target_count"))
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

fn execute_mutants(m: &MutantsArgs, _ctx: &CommandContext) -> Result<CommandResult> {
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
            timeout: vm.timeout,
            keep_failed: vm.keep_failed,
            list: vm.list,
            validate: vm.validate,
            tests: vm.args.clone(),
        },
    };
    vm_cmd.execute(ctx).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DiskSpaceStatus {
    Sufficient { available_gb: u64, min_gb: u64 },
    Low { available_gb: u64, min_gb: u64 },
    Unknown { issue: String },
}

fn classify_disk_space_probe_result(
    available_gb: std::result::Result<u64, String>,
    min_gb: u64,
) -> DiskSpaceStatus {
    match available_gb {
        Ok(available_gb) if available_gb >= min_gb => DiskSpaceStatus::Sufficient {
            available_gb,
            min_gb,
        },
        Ok(available_gb) => DiskSpaceStatus::Low {
            available_gb,
            min_gb,
        },
        Err(issue) => DiskSpaceStatus::Unknown { issue },
    }
}

/// Check if sufficient disk space is available on current directory's filesystem.
/// Probe failures remain explicit instead of being treated as healthy.
fn check_disk_space_gb(min_gb: u64) -> DiskSpaceStatus {
    #[cfg(unix)]
    {
        use nix::sys::statvfs::statvfs;
        classify_disk_space_probe_result(
            statvfs(".")
                .map(|stat| {
                    let available_bytes = stat.blocks_available() * stat.fragment_size();
                    available_bytes / (1024 * 1024 * 1024)
                })
                .map_err(|error| error.to_string()),
            min_gb,
        )
    }
    #[cfg(not(unix))]
    {
        classify_disk_space_probe_result(
            Err("disk-space probing is unavailable on this platform".to_string()),
            min_gb,
        )
    }
}
