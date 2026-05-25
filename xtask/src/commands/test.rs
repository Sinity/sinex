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
use modes::{
    DiskSpaceStatus, check_disk_space_gb, execute_bench, execute_coverage, execute_fuzz,
    execute_mutants, execute_vm,
};
use plan::{
    HEAVY_TEST_THREAD_CAP, NextestExecutionPlan, default_heavy_test_threads, normalize_packages,
    prepare_runtime_binaries_for_plan, resolve_nextest_execution_plan,
    runtime_binary_requirements_for_target, test_database_required_for_plan,
};

// UI & System monitoring
use console::style;

mod modes;
mod plan;

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

    /// Exclude workspace package(s) from --all/workspace test runs.
    #[arg(long = "exclude", value_name = "PACKAGE")]
    pub exclude_packages: Vec<String>,

    /// Run tests from specific test binary target(s) (nextest --test)
    #[arg(long = "test", value_name = "TEST_BINARY")]
    pub test_binaries: Vec<String>,

    /// Run only library unit tests (nextest --lib)
    #[arg(long)]
    pub lib: bool,

    /// Print what would happen
    #[arg(long)]
    pub dry_run: bool,

    /// Skip automatic infrastructure setup (preflight is ON by default)
    #[arg(long)]
    pub skip_preflight: bool,

    /// Run DB-backed tests inside a fresh throwaway Postgres cluster.
    #[arg(long)]
    pub ephemeral_postgres: bool,

    /// Disable SINEX_TEST_POSTGRES=ephemeral auto-wrapping for this invocation.
    #[arg(long)]
    pub no_ephemeral_postgres: bool,

    /// Include tests marked `#[ignore]`
    #[arg(long)]
    pub include_ignored: bool,

    /// Run heavy/ignored tests
    #[arg(long)]
    pub heavy: bool,

    /// Run ALL packages (disables affected mode default)
    #[arg(short, long)]
    pub all: bool,

    /// Allow broad tests to start even when host PSI is already severe.
    #[arg(long)]
    pub allow_contended_host: bool,

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

    /// Test database pool sizes to sweep (comma-separated). Enables ephemeral Postgres mode.
    #[arg(long, value_delimiter = ',')]
    pub db_pool_sizes: Vec<u32>,

    /// Use the system-impact preset for measured test concurrency calibration.
    #[arg(long)]
    pub system_impact: bool,

    /// Include aggressive over-subscription points in the system-impact preset.
    #[arg(long)]
    pub system_impact_extended: bool,

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

    /// Continue running benchmark scenarios after a scenario fails
    #[arg(long)]
    pub continue_on_fail: bool,

    /// Allow DB benchmark runs while other heavy workloads or high IO pressure are active.
    #[arg(long)]
    pub allow_contended_host: bool,

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
    fn uses_automatic_impact(&self) -> bool {
        self.subcommand.is_none()
            && !self.all
            && self.packages.is_empty()
            && self.exclude_packages.is_empty()
            && self.test_binaries.is_empty()
            && !self.lib
            && self.filter.is_none()
            && self.args.is_empty()
            && !self.list
            && !self.dry_run
            && !self.heavy
            && !self.include_ignored
            && !self.update_snapshots
            && !self.prime
    }

    fn guard_broad_start_pressure(
        &self,
        ctx: &CommandContext,
        execution_plan: &NextestExecutionPlan,
        effective_filter: Option<&str>,
        effective_test_binaries: &[String],
    ) -> Result<()> {
        if self.allow_contended_host || self.list || self.dry_run {
            return Ok(());
        }

        let pressure = crate::resources::PressureRecommendation::capture();
        if let Some(error) = pressure.start_error("test execution") {
            return Err(color_eyre::eyre::eyre!(error));
        }
        if self.is_broad_pressure_sensitive(
            execution_plan,
            effective_filter,
            effective_test_binaries,
        ) && let Some(warning) = pressure.warning("test")
            && ctx.is_human()
        {
            eprintln!("  ⚠ {warning}");
        }
        Ok(())
    }

    fn is_broad_pressure_sensitive(
        &self,
        execution_plan: &NextestExecutionPlan,
        effective_filter: Option<&str>,
        effective_test_binaries: &[String],
    ) -> bool {
        if self.list || self.dry_run {
            return false;
        }
        if self.all || self.heavy || self.include_ignored || self.update_snapshots {
            return true;
        }
        if self.threads.is_some_and(|threads| threads >= 12) {
            return true;
        }
        effective_filter.is_none()
            && effective_test_binaries.is_empty()
            && self.test_binaries.is_empty()
            && self.packages.is_empty()
            && execution_plan.runner_packages.len() != 1
    }

    fn effective_test_binaries(&self, filter: Option<&str>) -> Result<Vec<String>> {
        if !self.test_binaries.is_empty() {
            return Ok(normalize_packages(&self.test_binaries));
        }

        let Some(filter) = filter else {
            return Ok(Vec::new());
        };

        affected::infer_test_binaries_for_test_filter(filter)
    }

    fn effective_lib_target(&self, filter: Option<&str>, test_binaries: &[String]) -> Result<bool> {
        if self.lib {
            return Ok(true);
        }
        if !self.test_binaries.is_empty() || !test_binaries.is_empty() {
            return Ok(false);
        }
        let Some(filter) = filter else {
            return Ok(false);
        };

        affected::infer_lib_target_for_test_filter(filter)
    }

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

    fn narrow_test_db_pool_size(
        &self,
        execution_plan: &NextestExecutionPlan,
        effective_filter: Option<&str>,
        effective_test_binaries: &[String],
        effective_lib_target: bool,
    ) -> Option<usize> {
        if self.prime
            || self.all
            || self.heavy
            || self.include_ignored
            || self.update_snapshots
            || std::env::var_os("SINEX_TEST_DB_POOL_SIZE").is_some()
        {
            return None;
        }
        if execution_plan.runner_packages.len() != 1 {
            return None;
        }
        if !effective_lib_target && effective_test_binaries.is_empty() {
            return None;
        }

        let test_terms = effective_filter.and_then(affected::simple_test_name_term_count)?;
        let requested_threads = self.effective_threads().unwrap_or(test_terms);
        let concurrent_tests = test_terms.min(requested_threads.max(1)).max(1);
        Some((concurrent_tests * 2).clamp(2, 16))
    }

    fn semantic_invocation_args(
        &self,
        scope: &WorkloadScope,
        filter: Option<&str>,
        test_binaries: &[String],
        lib_target: bool,
    ) -> Vec<String> {
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
        if self.allow_contended_host {
            args.push("--allow-contended-host".to_string());
        }
        if self.update_snapshots {
            args.push("--update-snapshots".to_string());
        }
        if let Some(filter) = filter {
            args.push(format!("--filter={filter}"));
        }
        for test_binary in test_binaries {
            args.push(format!("--test={test_binary}"));
        }
        if lib_target {
            args.push("--lib".to_string());
        }
        for package in normalize_packages(&self.exclude_packages) {
            args.push(format!("--exclude={package}"));
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

    fn nextest_invocation_args(&self, force_skip_preflight: bool) -> Vec<String> {
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
        if self.list {
            args.push("--list".to_string());
        }
        if self.skip_preflight || force_skip_preflight {
            args.push("--skip-preflight".to_string());
        }
        if self.ephemeral_postgres {
            args.push("--ephemeral-postgres".to_string());
        }
        if self.no_ephemeral_postgres {
            args.push("--no-ephemeral-postgres".to_string());
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
        if self.allow_contended_host {
            args.push("--allow-contended-host".to_string());
        }
        if let Some(ref f) = self.filter {
            args.push("-E".to_string());
            args.push(f.clone());
        }
        for p in &self.packages {
            args.push("-p".to_string());
            args.push(p.clone());
        }
        for p in &self.exclude_packages {
            args.push("--exclude".to_string());
            args.push(p.clone());
        }
        for test_binary in &self.test_binaries {
            args.push("--test".to_string());
            args.push(test_binary.clone());
        }
        if self.lib {
            args.push("--lib".to_string());
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
        args
    }

    fn test_postgres_auto_requested(&self) -> bool {
        std::env::var("SINEX_TEST_POSTGRES").is_ok_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "ephemeral" | "tmpfs" | "auto"
            )
        })
    }

    fn should_use_ephemeral_postgres(
        &self,
        ctx: &CommandContext,
    ) -> Result<Option<NextestExecutionPlan>> {
        if self.no_ephemeral_postgres
            || self.skip_preflight
            || self.dry_run
            || self.list
            || self.subcommand.is_some()
            || std::env::var("SINEX_EPHEMERAL_POSTGRES_ACTIVE").is_ok()
            || std::env::var("NEXTEST_RUN_ID").is_ok()
        {
            return Ok(None);
        }

        let auto_requested = self.test_postgres_auto_requested();
        let requested = self.ephemeral_postgres || auto_requested;
        if !requested {
            return Ok(None);
        }

        let effective_filter = self.filter.clone();
        let effective_test_binaries = self.effective_test_binaries(effective_filter.as_deref())?;
        let effective_lib_target =
            self.effective_lib_target(effective_filter.as_deref(), &effective_test_binaries)?;
        let execution_plan =
            self.resolve_execution_plan(Some(ctx), effective_filter.as_deref(), None)?;

        if self.should_skip_auto_ephemeral_postgres_for_exact_target(
            auto_requested,
            &execution_plan,
            effective_filter.as_deref(),
            &effective_test_binaries,
            effective_lib_target,
        ) {
            return Ok(None);
        }

        if self.ephemeral_postgres || test_database_required_for_plan(&execution_plan) {
            Ok(Some(execution_plan))
        } else {
            Ok(None)
        }
    }

    fn should_skip_auto_ephemeral_postgres_for_exact_target(
        &self,
        auto_requested: bool,
        execution_plan: &NextestExecutionPlan,
        effective_filter: Option<&str>,
        effective_test_binaries: &[String],
        effective_lib_target: bool,
    ) -> bool {
        auto_requested
            && !self.ephemeral_postgres
            && self.is_exact_targeted_test(
                execution_plan,
                effective_filter,
                effective_test_binaries,
                effective_lib_target,
            )
    }

    fn is_exact_targeted_test(
        &self,
        execution_plan: &NextestExecutionPlan,
        effective_filter: Option<&str>,
        effective_test_binaries: &[String],
        effective_lib_target: bool,
    ) -> bool {
        if execution_plan.runner_packages.len() != 1 {
            return false;
        }
        if !effective_lib_target && effective_test_binaries.is_empty() {
            return false;
        }
        effective_filter
            .and_then(affected::simple_test_name_term_count)
            .is_some()
    }

    async fn execute_with_ephemeral_postgres(
        &self,
        ctx: &CommandContext,
        execution_plan: &NextestExecutionPlan,
    ) -> Result<CommandResult> {
        let base_dir = std::env::var_os("SINEX_TEST_PGDATA_DIR")
            .map(PathBuf::from)
            .or_else(|| crate::config::workspace_tmpfs_dir("sinex-ci-pgdata", 1024.0))
            .unwrap_or_else(|| PathBuf::from(".sinex/ci-pgdata"));
        let data_dir = base_dir.join("current");
        let socket_dir = base_dir.join("run");
        let port = 5433;

        if ctx.is_human() {
            println!(
                "Using throwaway Postgres for DB-backed tests: {}",
                data_dir.display()
            );
        }

        let mut nested_test_args = self.nextest_invocation_args(true);
        nested_test_args.retain(|arg| arg != "--ephemeral-postgres");
        nested_test_args.push("--no-ephemeral-postgres".to_string());
        nested_test_args.push("--allow-contended-host".to_string());

        let xtask_exe = std::env::current_exe()
            .map_err(|e| color_eyre::eyre::eyre!("failed to resolve current xtask binary: {e}"))?;
        let xtask_program = xtask_exe.to_string_lossy().into_owned();

        let mut args = vec![
            "ci".to_string(),
            "postgres".to_string(),
            "--schema".to_string(),
            format!("--port={port}"),
            format!("--data-dir={}", data_dir.display()),
            format!("--socket-dir={}", socket_dir.display()),
            "--".to_string(),
            xtask_program.clone(),
            "test".to_string(),
        ];
        args.extend(nested_test_args);

        let result = ProcessBuilder::new(&xtask_program)
            .args(&args)
            .inherit_output()
            .without_timeout()
            .run();

        match result {
            Ok(_) => Ok(CommandResult::success()
                .with_message("tests passed with ephemeral Postgres")
                .with_detail(format!(
                    "scope={}",
                    execution_plan.workload_scope.encode_marker()
                ))
                .with_detail(format!("pgdata={}", data_dir.display()))
                .with_duration(ctx.elapsed())),
            Err(error) => Err(error),
        }
    }

    fn resolve_execution_plan(
        &self,
        ctx: Option<&CommandContext>,
        filter: Option<&str>,
        impact_packages: Option<&[String]>,
    ) -> Result<NextestExecutionPlan> {
        let explicit_packages = normalize_packages(&self.packages);
        let requested_excludes = normalize_packages(&self.exclude_packages);
        let inferred_packages = if self.all {
            Vec::new()
        } else if let Some(filter) = filter {
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
        };

        let affected_packages = if let Some(impact_packages) = impact_packages {
            let impact_packages = normalize_packages(impact_packages);
            if impact_packages.is_empty() {
                None
            } else {
                if let Some(ctx) = ctx
                    && ctx.is_human()
                {
                    println!(
                        "Impact-selected package scope: {}",
                        impact_packages.join(", ")
                    );
                }
                Some(impact_packages)
            }
        } else if !self.all && explicit_packages.is_empty() && inferred_packages.is_empty() {
            let affected_result = if let Some(ctx) = ctx {
                let stage = ctx.start_stage("affected");
                let packages = affected::affected_packages();
                ctx.finish_stage(stage, packages.is_ok());
                packages
            } else {
                affected::affected_packages()
            };
            let affected_packages = normalize_packages(&affected_result?);
            if affected_packages.is_empty() {
                if let Some(ctx) = ctx
                    && ctx.is_human()
                {
                    println!("No changes detected. Running ALL tests.");
                }
                None
            } else {
                if let Some(ctx) = ctx
                    && ctx.is_human()
                {
                    println!("{}", affected::affected_summary(&affected_packages));
                }
                Some(affected_packages)
            }
        } else {
            None
        };

        let execution_plan = resolve_nextest_execution_plan(
            &explicit_packages,
            inferred_packages,
            affected_packages,
            &self.exclude_packages,
        );

        if !requested_excludes.is_empty()
            && !matches!(execution_plan.workload_scope, WorkloadScope::Workspace)
        {
            return Err(color_eyre::eyre::eyre!(
                "`xtask test --exclude` is only valid for --all/workspace test runs; \
                 use explicit -p package selection for scoped test runs"
            ));
        }

        Ok(execution_plan)
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
                    if !bench.db_pool_sizes.is_empty() {
                        let pool_sizes = bench
                            .db_pool_sizes
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(",");
                        args.push(format!("--db-pool-sizes={pool_sizes}"));
                    }
                    if bench.system_impact {
                        args.push("--system-impact".to_string());
                    }
                    if bench.system_impact_extended {
                        args.push("--system-impact-extended".to_string());
                    }
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
                    if bench.continue_on_fail {
                        args.push("--continue-on-fail".to_string());
                    }
                    if bench.allow_contended_host {
                        args.push("--allow-contended-host".to_string());
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

                    let coordination_args = crate::commands::vm::vm_test_coordination_args(
                        vm.category.as_deref(),
                        vm.timeout,
                        vm.keep_failed,
                        vm.validate,
                        &vm.args,
                    );
                    return crate::coordinator::coordinate_and_spawn_with_scope(
                        "test",
                        &args,
                        &coordination_args,
                        ctx,
                    );
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
                    if self.ephemeral_postgres {
                        args.push("--ephemeral-postgres".to_string());
                    }
                    if self.no_ephemeral_postgres {
                        args.push("--no-ephemeral-postgres".to_string());
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
                    if self.allow_contended_host {
                        args.push("--allow-contended-host".to_string());
                    }
                    if let Some(ref f) = self.filter {
                        args.push("-E".to_string());
                        args.push(f.clone());
                    }
                    for p in &self.packages {
                        args.push("-p".to_string());
                        args.push(p.clone());
                    }
                    for p in &self.exclude_packages {
                        args.push("--exclude".to_string());
                        args.push(p.clone());
                    }
                    for test_binary in &self.test_binaries {
                        args.push("--test".to_string());
                        args.push(test_binary.clone());
                    }
                    if self.lib {
                        args.push("--lib".to_string());
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

                    let execution_plan =
                        self.resolve_execution_plan(None, self.filter.as_deref(), None)?;
                    let effective_test_binaries =
                        self.effective_test_binaries(self.filter.as_deref())?;
                    let effective_lib_target = self
                        .effective_lib_target(self.filter.as_deref(), &effective_test_binaries)?;
                    let coordination_args = self.semantic_invocation_args(
                        &execution_plan.workload_scope,
                        self.filter.as_deref(),
                        &effective_test_binaries,
                        effective_lib_target,
                    );
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

        if let Some(execution_plan) = self.should_use_ephemeral_postgres(ctx)? {
            return self
                .execute_with_ephemeral_postgres(ctx, &execution_plan)
                .await;
        }

        if self.dry_run {
            let effective_filter = self.filter.clone();
            let effective_test_binaries =
                self.effective_test_binaries(effective_filter.as_deref())?;
            let effective_lib_target =
                self.effective_lib_target(effective_filter.as_deref(), &effective_test_binaries)?;
            let execution_plan =
                self.resolve_execution_plan(Some(ctx), effective_filter.as_deref(), None)?;
            let workload_scope = execution_plan.workload_scope.clone();
            let coordination_args = self.semantic_invocation_args(
                &workload_scope,
                effective_filter.as_deref(),
                &effective_test_binaries,
                effective_lib_target,
            );
            ctx.record_invocation_args(&coordination_args);
            let runtime_binary_requirements = runtime_binary_requirements_for_target(
                &execution_plan,
                effective_lib_target,
                &effective_test_binaries,
                effective_filter.as_deref(),
            );
            let db_pool_size = self.narrow_test_db_pool_size(
                &execution_plan,
                effective_filter.as_deref(),
                &effective_test_binaries,
                effective_lib_target,
            );
            let proof_kind = crate::coordinator::proof_kind("test", &coordination_args);
            let input_fingerprint =
                crate::coordinator::current_scoped_tree_fingerprint("test", &coordination_args)?;
            let scope_key = crate::coordinator::compute_scope_key("test", &coordination_args);
            let reusable = proof_kind == "test.nextest.exact";
            let reusable_proof = if reusable {
                ctx.try_with_history_db_query(|db| {
                    db.get_successful_reusable_test_proof_unit(
                        &proof_kind,
                        &input_fingerprint,
                        &scope_key,
                    )
                })
                .and_then(std::result::Result::ok)
                .flatten()
            } else {
                None
            };

            if ctx.is_human() {
                println!("Dry run: nextest plan resolved");
                println!("  scope: {}", workload_scope.encode_marker());
                if !execution_plan.runner_packages.is_empty() {
                    println!("  packages: {}", execution_plan.runner_packages.join(", "));
                }
                if !execution_plan.excluded_packages.is_empty() {
                    println!(
                        "  excluded: {}",
                        execution_plan.excluded_packages.join(", ")
                    );
                }
                if !effective_test_binaries.is_empty() {
                    println!("  test binaries: {}", effective_test_binaries.join(", "));
                }
                println!("  lib target: {effective_lib_target}");
                if let Some(filter) = &effective_filter {
                    println!("  filter: {filter}");
                }
                if runtime_binary_requirements.is_empty() {
                    println!("  runtime binaries: none");
                } else {
                    println!(
                        "  runtime binaries: {}",
                        runtime_binary_requirements
                            .iter()
                            .map(|requirement| {
                                format!("{}:{}", requirement.package, requirement.binary)
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                println!(
                    "  db pool size override: {}",
                    db_pool_size.map_or_else(|| "default".to_string(), |value| value.to_string())
                );
                let reuse_state = if let Some(proof) = &reusable_proof {
                    format!("hit invocation {}", proof.invocation_id)
                } else if reusable {
                    "eligible, no exact proof yet".to_string()
                } else {
                    "disabled for runtime or mutating test shape".to_string()
                };
                println!("  reuse eligibility: {reuse_state}");
            }
            return Ok(CommandResult::success()
                .with_message("test dry-run passed")
                .with_detail(format!("scope={}", workload_scope.encode_marker()))
                .with_data(serde_json::json!({
                    "scope": workload_scope.encode_marker(),
                    "runner_packages": execution_plan.runner_packages,
                    "excluded_packages": execution_plan.excluded_packages,
                    "test_binaries": effective_test_binaries,
                    "lib": effective_lib_target,
                    "filter": effective_filter,
                    "runtime_binary_requirements": runtime_binary_requirements,
                    "db_pool_size": db_pool_size,
                    "reuse": {
                        "eligible": reusable,
                        "proof_kind": proof_kind,
                        "input_fingerprint": input_fingerprint,
                        "scope_key": scope_key,
                        "hit": reusable_proof,
                        "reason": if reusable {
                            "nextest proof can be reused when the exact manifest and input fingerprint match"
                        } else {
                            "runtime, ignored, snapshot, or listing test shapes are never reused"
                        },
                    },
                }))
                .with_duration(ctx.elapsed()));
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

        let impact_plan = if self.uses_automatic_impact() {
            Some(
                match ctx.try_with_history_db_query(|db| {
                    crate::impact::plan_default_test_impact_with_history(Some(db))
                }) {
                    Some(result) => result?,
                    None => crate::impact::plan_default_test_impact()?,
                },
            )
        } else {
            None
        };
        if let Some(plan) = &impact_plan {
            if let Some(result) = ctx
                .try_with_history_db(|db| db.record_impact_plan(ctx.invocation_id(), "auto", plan))
                && let Err(error) = result
            {
                tracing::warn!(target: "xtask::test", error = %error, "failed to record impact plan");
            }
            if ctx.is_human() {
                println!(
                    "Impact planner: {} changed file(s), {} affected package(s), {} impacted test(s)",
                    plan.changed.len(),
                    plan.affected_packages.len(),
                    plan.impacted_tests.len()
                );
                if let Some(filter) = &plan.impact_filter {
                    println!("  impact filter: {filter}");
                }
                for risk in &plan.accepted_risks {
                    println!("  accepted risk: {risk}");
                }
            }
            if plan.can_reuse_exact_proof() {
                let proof_args = crate::impact::exact_proof_args_for_plan(plan);
                let (proof_kind, input_fingerprint, scope_key) =
                    crate::impact::exact_test_proof_key(&proof_args)?;
                let reusable_proof = ctx
                    .try_with_history_db_query(|db| {
                        db.get_successful_reusable_test_proof_unit(
                            &proof_kind,
                            &input_fingerprint,
                            &scope_key,
                        )
                    })
                    .and_then(std::result::Result::ok)
                    .flatten();
                if let Some(proof) = reusable_proof {
                    if ctx.is_human() {
                        println!(
                            "Skipping tests: exact reusable proof from invocation {}",
                            proof.invocation_id
                        );
                    }
                    return Ok(CommandResult::success()
                        .with_message("tests skipped by exact impact proof")
                        .with_detail(format!("reused_invocation={}", proof.invocation_id))
                        .with_duration(ctx.elapsed())
                        .with_data(serde_json::json!({
                            "impact_plan": plan,
                            "reused_proof": proof,
                        })));
                }
            }
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

        // Affected mode is default ON, --all disables it.
        let impact_packages = impact_plan
            .as_ref()
            .and_then(crate::impact::packages_for_plan);
        let effective_filter = impact_plan
            .as_ref()
            .and_then(|plan| plan.impact_filter.clone())
            .or_else(|| self.filter.clone());
        let effective_test_binaries = self.effective_test_binaries(effective_filter.as_deref())?;
        let effective_lib_target =
            self.effective_lib_target(effective_filter.as_deref(), &effective_test_binaries)?;
        let execution_plan = self.resolve_execution_plan(
            Some(ctx),
            effective_filter.as_deref(),
            impact_packages.as_deref(),
        )?;
        self.guard_broad_start_pressure(
            ctx,
            &execution_plan,
            effective_filter.as_deref(),
            &effective_test_binaries,
        )?;
        let workload_scope = execution_plan.workload_scope.clone();
        let coordination_args = self.semantic_invocation_args(
            &workload_scope,
            effective_filter.as_deref(),
            &effective_test_binaries,
            effective_lib_target,
        );
        ctx.record_coordination_fingerprint("test", &coordination_args);
        ctx.record_invocation_args(&coordination_args);

        // List: show tests only
        if self.list {
            let mut cmd = ProcessBuilder::cargo().args(["nextest", "list"]);
            if execution_plan.runner_packages.is_empty() {
                cmd = cmd.arg("--workspace");
                for package in &execution_plan.excluded_packages {
                    cmd = cmd.args(["--exclude", package]);
                }
            } else {
                for package in &execution_plan.runner_packages {
                    cmd = cmd.args(["-p", package]);
                }
            }
            for test_binary in &effective_test_binaries {
                cmd = cmd.args(["--test", test_binary]);
            }
            if effective_lib_target {
                cmd = cmd.arg("--lib");
            }
            if let Some(filter) = &effective_filter {
                cmd = cmd.args(["-E", filter]);
            }
            cmd.run_ok()?;
            return Ok(CommandResult::success().with_detail("tests listed"));
        }

        let runtime_binary_requirements = runtime_binary_requirements_for_target(
            &execution_plan,
            effective_lib_target,
            &effective_test_binaries,
            effective_filter.as_deref(),
        );
        let runtime_binary_reports = if runtime_binary_requirements.is_empty() {
            Vec::new()
        } else {
            let runtime_stage = ctx.start_stage("runtime-binaries");
            let result = prepare_runtime_binaries_for_plan(ctx, &runtime_binary_requirements);
            ctx.finish_stage(runtime_stage, result.is_ok());
            result?
        };

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
        if let Some(pool_size) = self.narrow_test_db_pool_size(
            &execution_plan,
            effective_filter.as_deref(),
            &effective_test_binaries,
            effective_lib_target,
        ) {
            runner.add_env("SINEX_TEST_DB_POOL_SIZE", pool_size.to_string());
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
        for package in &execution_plan.excluded_packages {
            runner.add_arg("--exclude");
            runner.add_arg(package);
        }
        for test_binary in &effective_test_binaries {
            runner.add_arg("--test");
            runner.add_arg(test_binary);
        }
        if effective_lib_target {
            runner.add_arg("--lib");
        }
        if let Some(ref filter) = effective_filter {
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
                "impact_plan": impact_plan.clone(),
                "runtime_binaries": runtime_binary_reports.clone(),
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
            let proof_kind = crate::coordinator::proof_kind("test", &coordination_args);
            let test_proof_unit = if let Some(invocation_id) = ctx.invocation_id() {
                let input_fingerprint =
                    crate::coordinator::current_scoped_tree_fingerprint("test", &coordination_args)
                        .ok();
                let scope_key = Some(crate::coordinator::compute_scope_key(
                    "test",
                    &coordination_args,
                ));
                match (input_fingerprint, scope_key) {
                    (Some(input_fingerprint), Some(scope_key)) => {
                        let reusable = proof_kind == "test.nextest.exact";
                        let manifest = serde_json::json!({
                            "scope": workload_scope.encode_marker(),
                            "runner_packages": execution_plan.runner_packages,
                            "excluded_packages": execution_plan.excluded_packages,
                            "test_binaries": effective_test_binaries,
                            "lib": effective_lib_target,
                            "filter": effective_filter,
                            "runtime_binary_requirements": runtime_binary_requirements,
                            "db_pool_size": self.narrow_test_db_pool_size(
                                &execution_plan,
                                effective_filter.as_deref(),
                                &effective_test_binaries,
                                effective_lib_target,
                            ),
                            "passed": stats.passed,
                            "ignored": stats.ignored,
                            "impact_plan": impact_plan.clone(),
                        });
                        match serde_json::to_string(&manifest)
                            .map_err(color_eyre::eyre::Report::from)
                            .and_then(|manifest_json| {
                                if let Some(result) = ctx.try_with_history_db(|db| {
                                    db.record_test_proof_unit(
                                        invocation_id,
                                        &proof_kind,
                                        &scope_key,
                                        &input_fingerprint,
                                        &manifest_json,
                                        reusable,
                                    )
                                }) {
                                    result?;
                                }
                                Ok(())
                            }) {
                            Ok(()) => Some(serde_json::json!({
                                "proof_kind": proof_kind,
                                "scope_key": scope_key,
                                "input_fingerprint": input_fingerprint,
                                "reusable": reusable,
                            })),
                            Err(error) => {
                                tracing::warn!(
                                    target: "xtask::test",
                                    invocation_id,
                                    error = %error,
                                    "failed to record test proof unit"
                                );
                                None
                            }
                        }
                    }
                    _ => None,
                }
            } else {
                None
            };
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
                    "impact_plan": impact_plan.clone(),
                    "runtime_binaries": runtime_binary_reports.clone(),
                    "test_proof_unit": test_proof_unit,
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

        let args = command.semantic_invocation_args(&WorkloadScope::Workspace, None, &[], false);
        assert!(args.contains(&"--heavy".to_string()));

        // The thread cap is min(available_parallelism, HEAVY_TEST_THREAD_CAP).
        // Asserting exactly "--threads=4" is brittle on machines with fewer than 4
        // logical CPUs.  Instead verify a thread arg is present and within range.
        let thread_arg = args.iter().find(|a| a.starts_with("--threads="));
        assert!(
            thread_arg.is_some(),
            "heavy invocation must include a --threads=N arg, got: {args:?}"
        );
        let n: usize = thread_arg
            .unwrap()
            .strip_prefix("--threads=")
            .unwrap()
            .parse()
            .expect("--threads= value must be numeric");
        assert!(
            (1..=HEAVY_TEST_THREAD_CAP).contains(&n),
            "--threads={n} is outside the expected range 1..={HEAVY_TEST_THREAD_CAP}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_semantic_invocation_args_include_nextest_test_targets()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand::default();

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["sinex-e2e-tests".to_string()]),
            None,
            &["large_payload_test".to_string()],
            false,
        );

        assert!(
            args.contains(&"--test=large_payload_test".to_string()),
            "test binary selector should be part of the coordination identity: {args:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_semantic_invocation_args_include_lib_target() -> ::xtask::sandbox::TestResult<()>
    {
        let command = TestCommand::default();

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["sinex-node-sdk".to_string()]),
            None,
            &[],
            true,
        );

        assert!(
            args.contains(&"--lib".to_string()),
            "library target selector should be part of the coordination identity: {args:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_narrow_test_db_pool_size_scales_with_exact_filter()
    -> ::xtask::sandbox::TestResult<()> {
        let mut _guard = crate::sandbox::prelude::EnvGuard::new();
        _guard.clear("SINEX_TEST_DB_POOL_SIZE");
        let command = TestCommand::default();
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-source-worker".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-source-worker".to_string()]),
        };

        assert_eq!(
            command.narrow_test_db_pool_size(&plan, Some("test(one) | test(two)"), &[], true,),
            Some(4)
        );
        assert_eq!(
            command.narrow_test_db_pool_size(&plan, Some("test(one)"), &[], true),
            Some(2)
        );

        assert_eq!(
            command.narrow_test_db_pool_size(&plan, Some("test(one)"), &[], false),
            None,
            "package-wide filtered runs should keep the normal pool unless target narrowed"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_narrow_test_db_pool_size_skips_broad_or_configured_runs()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-source-worker".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-source-worker".to_string()]),
        };
        let broad = TestCommand {
            all: true,
            ..Default::default()
        };
        assert_eq!(
            broad.narrow_test_db_pool_size(&plan, Some("test(one)"), &[], true),
            None
        );

        let _guard = crate::sandbox::prelude::EnvGuard::set_single("SINEX_TEST_DB_POOL_SIZE", "48");
        assert_eq!(
            TestCommand::default().narrow_test_db_pool_size(&plan, Some("test(one)"), &[], true,),
            None
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_postgres_auto_skips_exact_targeted_runs() -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            packages: vec!["sinex-node-sdk".to_string()],
            filter: Some(
                "test(sqlite_harness_records_snapshot_success_and_failure_evidence)".to_string(),
            ),
            ..Default::default()
        };
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-node-sdk".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-node-sdk".to_string()]),
        };

        assert!(
            command.should_skip_auto_ephemeral_postgres_for_exact_target(
                true,
                &plan,
                command.filter.as_deref(),
                &[],
                true,
            ),
            "auto ephemeral Postgres should not wrap exact targeted lib tests"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_explicit_ephemeral_postgres_keeps_exact_targeted_runs()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            ephemeral_postgres: true,
            packages: vec!["sinex-node-sdk".to_string()],
            filter: Some(
                "test(sqlite_harness_records_snapshot_success_and_failure_evidence)".to_string(),
            ),
            ..Default::default()
        };
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-node-sdk".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-node-sdk".to_string()]),
        };

        assert!(
            !command.should_skip_auto_ephemeral_postgres_for_exact_target(
                true,
                &plan,
                command.filter.as_deref(),
                &[],
                true,
            ),
            "explicit --ephemeral-postgres must still force the wrapper"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_semantic_invocation_args_include_package_excludes()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            exclude_packages: vec!["sinex-e2e-tests".to_string()],
            ..Default::default()
        };

        let args = command.semantic_invocation_args(&WorkloadScope::Workspace, None, &[], false);
        assert!(
            args.contains(&"--exclude=sinex-e2e-tests".to_string()),
            "package excludes must be part of coordination identity: {args:?}"
        );
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
}
