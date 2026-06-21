//! Test command - run nextest with profiles and options
//!
//! Provides a rich TUI experience while capturing detailed test execution data
//! (timing, output, system resources) into the history database.
//!
//! Specialized test modes (bench, fuzz, coverage, mutants, vm) are subcommands,
//! not flags. Bare `xtask test` runs the default nextest path.

use color_eyre::eyre::Result;
use serde::Serialize;
use std::path::PathBuf;

use crate::affected;
use crate::command::{CommandContext, CommandMetadata, CommandResult, WorkloadScope, XtaskCommand};
use crate::nextest::runner::TestRunner;
use modes::{
    DiskSpaceStatus, check_disk_space_gb, execute_bench, execute_coverage, execute_fuzz,
    execute_mutants, execute_vm,
};
use plan::{
    HEAVY_TEST_THREAD_CAP, NextestExecutionPlan, default_heavy_test_threads, normalize_packages,
    prepare_runtime_binaries_for_plan, resolve_nextest_execution_plan,
    runtime_binary_requirements_for_target,
};

// UI & System monitoring
use console::style;

mod modes;
mod plan;

#[derive(Debug, Clone, Serialize)]
struct ReusedImpactPackageProof {
    package: String,
    invocation_id: i64,
    proof_kind: String,
    scope_key: String,
}

/// Proof coverage state for a single package scope in test planning.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProofCoverageState {
    /// An exact reusable proof exists in the history DB for this scope.
    Covered,
    /// No proof exists but the scope is eligible for reuse.
    Missing,
    /// A proof existed but its fingerprint or scope no longer matches.
    Stale,
    /// The scope shape cannot be reused (runtime, listing, mutating, heavy, etc.).
    Ineligible,
}

#[derive(Debug, Clone, Serialize)]
struct PackageProofCoverage {
    package: String,
    state: ProofCoverageState,
    #[serde(skip_serializing_if = "Option::is_none")]
    proof_invocation_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestPreflightMode {
    Skipped,
    CompileOnly,
    RuntimeStack,
}

fn preflight_mode_for_test_plan(
    skip_preflight: bool,
    prime_pool: bool,
    runtime_binary_requirements: &[plan::RuntimeBinaryRequirement],
) -> TestPreflightMode {
    if skip_preflight {
        return TestPreflightMode::Skipped;
    }
    if prime_pool || !runtime_binary_requirements.is_empty() {
        return TestPreflightMode::RuntimeStack;
    }
    TestPreflightMode::CompileOnly
}

/// Push `flag` onto `args` if `cond` is true.
fn push_flag(args: &mut Vec<String>, cond: bool, flag: &'static str) {
    if cond {
        args.push(flag.to_string());
    }
}

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

    /// Cargo features to enable for the selected test packages.
    #[arg(long = "features", value_delimiter = ',')]
    pub cargo_features: Vec<String>,

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

    /// Impact planner mode for bare `xtask test`.
    #[arg(long, value_enum, default_value_t = crate::impact::ImpactMode::Balanced)]
    pub impact_mode: crate::impact::ImpactMode,

    /// Bypass exact proof reuse for this invocation.
    #[arg(long)]
    pub no_reuse: bool,

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

    /// Test database pool sizes to sweep (comma-separated).
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
    /// Background path: serialize args for the chosen subcommand (or the default
    /// nextest run) and hand off to the coordinator. Extracted from `execute` to
    /// keep it within the cognitive-complexity budget.
    fn execute_background(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let mut args = Vec::new();

        // Serialize subcommand first (if any)
        match &self.subcommand {
            Some(TestSubcommand::Bench(bench)) => {
                args = bench_background_args(bench);
            }
            Some(TestSubcommand::Fuzz(fuzz)) => {
                args = fuzz_background_args(fuzz);
            }
            Some(TestSubcommand::Coverage(cov)) => {
                args = coverage_background_args(cov);
            }
            Some(TestSubcommand::Mutants(m)) => {
                args = mutants_background_args(m);
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
                args = self.nextest_invocation_args(false);

                let execution_plan =
                    self.resolve_execution_plan(None, self.filter.as_deref(), None)?;
                let effective_test_binaries =
                    self.effective_test_binaries(self.filter.as_deref())?;
                let effective_lib_target =
                    self.effective_lib_target(self.filter.as_deref(), &effective_test_binaries)?;
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

        crate::coordinator::coordinate_and_spawn("test", &args, ctx)
    }

    /// `--dry-run` path: resolve the execution plan, print/emit it, and return
    /// without running tests. Extracted from `execute` to keep it within the
    /// cognitive-complexity budget.
    fn execute_dry_run(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let effective_filter = self.filter.clone();
        let effective_test_binaries = self.effective_test_binaries(effective_filter.as_deref())?;
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
        let reusable = self.can_consume_exact_test_proof() && proof_kind == "test.nextest.exact";
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
            print_dry_run_plan(
                self,
                ctx,
                &workload_scope,
                &execution_plan,
                &effective_test_binaries,
                effective_lib_target,
                &effective_filter,
                &runtime_binary_requirements,
                db_pool_size,
                reusable,
                &reusable_proof,
            );
        }
        // Build coverage array for JSON output
        let proof_coverage: Vec<PackageProofCoverage> = if execution_plan.runner_packages.is_empty()
        {
            Vec::new()
        } else {
            self.classify_package_proof_coverage(ctx, &execution_plan.runner_packages)
        };
        Ok(CommandResult::success()
            .with_message("test dry-run passed")
            .with_detail(format!("scope={}", workload_scope.encode_marker()))
            .with_data(serde_json::json!({
                "scope": workload_scope.encode_marker(),
                "runner_packages": execution_plan.runner_packages,
                "excluded_packages": execution_plan.excluded_packages,
                "test_binaries": effective_test_binaries,
                "lib": effective_lib_target,
                "features": normalize_packages(&self.cargo_features),
                "filter": effective_filter,
                "runtime_binary_requirements": runtime_binary_requirements,
                "db_pool_size": db_pool_size,
                "proof_coverage": proof_coverage,
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
            .with_duration(ctx.elapsed()))
    }

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
            && !matches!(self.impact_mode, crate::impact::ImpactMode::Off)
    }

    fn can_consume_exact_test_proof(&self) -> bool {
        !self.no_reuse && !self.list && !self.prime
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

        let inferred_binary_packages =
            affected::infer_test_binary_packages_for_test_filter(filter)?;
        if !self.packages.is_empty() {
            let selected_packages = normalize_packages(&self.packages);
            if inferred_binary_packages.is_empty()
                || inferred_binary_packages
                    .iter()
                    .any(|(package, _binary)| !selected_packages.contains(package))
            {
                return Ok(Vec::new());
            }
        }

        let mut binaries: Vec<String> = inferred_binary_packages
            .into_iter()
            .map(|(_package, binary)| binary)
            .collect();
        binaries.sort();
        binaries.dedup();
        Ok(binaries)
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

    pub(crate) fn freshness_explain_args(
        &self,
        ctx: Option<&CommandContext>,
    ) -> Result<Vec<String>> {
        let effective_filter = self.filter.clone();
        let effective_test_binaries = self.effective_test_binaries(effective_filter.as_deref())?;
        let effective_lib_target =
            self.effective_lib_target(effective_filter.as_deref(), &effective_test_binaries)?;
        let execution_plan = self.resolve_execution_plan(ctx, effective_filter.as_deref(), None)?;

        Ok(self.semantic_invocation_args(
            &execution_plan.workload_scope,
            effective_filter.as_deref(),
            &effective_test_binaries,
            effective_lib_target,
        ))
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
        if self.heavy {
            args.push("--heavy".to_string());
        }
        if self.include_ignored {
            args.push("--include-ignored".to_string());
        }
        if self.all {
            args.push("--all".to_string());
        }
        for feature in normalize_packages(&self.cargo_features) {
            args.push(format!("--features={feature}"));
        }
        if self.no_reuse {
            args.push("--no-reuse".to_string());
        }
        if let Some(pool_size) = std::env::var_os("SINEX_TEST_DB_POOL_SIZE") {
            args.push(format!(
                "--db-pool-size-env={}",
                pool_size.to_string_lossy()
            ));
        }
        let execution_plan = NextestExecutionPlan {
            runner_packages: match scope {
                WorkloadScope::Workspace => Vec::new(),
                WorkloadScope::Packages(packages) | WorkloadScope::Affected(packages) => {
                    packages.clone()
                }
            },
            excluded_packages: Vec::new(),
            workload_scope: scope.clone(),
        };
        for requirement in runtime_binary_requirements_for_target(
            &execution_plan,
            lib_target,
            test_binaries,
            filter,
        ) {
            args.push(format!(
                "--runtime-binary={}:{}",
                requirement.package, requirement.binary
            ));
        }
        if self.prime {
            args.push("--prime".to_string());
        }
        if !matches!(self.impact_mode, crate::impact::ImpactMode::Balanced) {
            args.push(format!("--impact-mode={}", self.impact_mode.as_str()));
        }
        args.push(format!(
            "--impact-planner-version={}",
            crate::impact::IMPACT_PLANNER_VERSION
        ));
        args.push(format!(
            "--impact-coverage-schema={}",
            crate::impact::IMPACT_COVERAGE_SCHEMA_VERSION
        ));
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
        for arg in &self.args {
            args.push(format!("--test-arg={arg}"));
        }

        args.push(scope.encode_marker());
        args
    }

    fn exact_package_proof_args(&self, package: &str) -> Vec<String> {
        self.semantic_invocation_args(
            &WorkloadScope::Packages(vec![package.to_string()]),
            None,
            &[],
            false,
        )
    }

    fn subtract_reusable_impact_package_proofs(
        &self,
        ctx: &CommandContext,
        packages: &[String],
    ) -> Result<(Vec<String>, Vec<ReusedImpactPackageProof>)> {
        let mut remaining = Vec::new();
        let mut reused = Vec::new();
        for package in packages {
            let proof_args = self.exact_package_proof_args(package);
            let proof_kind = crate::coordinator::proof_kind("test", &proof_args);
            if proof_kind != "test.nextest.exact" {
                remaining.push(package.clone());
                continue;
            }
            let input_fingerprint =
                crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
            let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
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
                reused.push(ReusedImpactPackageProof {
                    package: package.clone(),
                    invocation_id: proof.invocation_id,
                    proof_kind,
                    scope_key,
                });
            } else {
                remaining.push(package.clone());
            }
        }
        Ok((remaining, reused))
    }

    /// Classify proof coverage for each package scope.
    ///
    /// Returns a `PackageProofCoverage` per package with one of:
    /// - `Covered`: exact reusable proof exists for this scope
    /// - `Missing`: eligible but no proof found
    /// - `Ineligible`: scope shape cannot be reused
    fn classify_package_proof_coverage(
        &self,
        ctx: &CommandContext,
        packages: &[String],
    ) -> Vec<PackageProofCoverage> {
        packages
            .iter()
            .map(|package| {
                if !self.can_consume_exact_test_proof() {
                    return PackageProofCoverage {
                        package: package.clone(),
                        state: ProofCoverageState::Ineligible,
                        proof_invocation_id: None,
                    };
                }
                let proof_args = self.exact_package_proof_args(package);
                let proof_kind = crate::coordinator::proof_kind("test", &proof_args);
                if proof_kind != "test.nextest.exact" {
                    return PackageProofCoverage {
                        package: package.clone(),
                        state: ProofCoverageState::Ineligible,
                        proof_invocation_id: None,
                    };
                }
                let input_fingerprint =
                    crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args);
                let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
                match input_fingerprint {
                    Ok(fingerprint) => {
                        let reusable_proof = ctx
                            .try_with_history_db_query(|db| {
                                db.get_successful_reusable_test_proof_unit(
                                    &proof_kind,
                                    &fingerprint,
                                    &scope_key,
                                )
                            })
                            .and_then(std::result::Result::ok)
                            .flatten();
                        if let Some(proof) = reusable_proof {
                            PackageProofCoverage {
                                package: package.clone(),
                                state: ProofCoverageState::Covered,
                                proof_invocation_id: Some(proof.invocation_id),
                            }
                        } else {
                            // No exact match — check for stale proof (same scope,
                            // different fingerprint from a prior run).
                            let stale_proof = ctx
                                .try_with_history_db_query(|db| {
                                    db.get_any_successful_test_proof_for_scope(
                                        &proof_kind,
                                        &scope_key,
                                    )
                                })
                                .and_then(std::result::Result::ok)
                                .flatten();
                            if let Some(proof) = stale_proof {
                                PackageProofCoverage {
                                    package: package.clone(),
                                    state: ProofCoverageState::Stale,
                                    proof_invocation_id: Some(proof.invocation_id),
                                }
                            } else {
                                PackageProofCoverage {
                                    package: package.clone(),
                                    state: ProofCoverageState::Missing,
                                    proof_invocation_id: None,
                                }
                            }
                        }
                    }
                    Err(_) => PackageProofCoverage {
                        package: package.clone(),
                        state: ProofCoverageState::Ineligible,
                        proof_invocation_id: None,
                    },
                }
            })
            .collect()
    }

    fn nextest_invocation_args(&self, force_skip_preflight: bool) -> Vec<String> {
        let mut args = Vec::new();
        push_flag(&mut args, self.debug, "--debug");
        push_flag(&mut args, self.fail_fast, "--fail-fast");
        push_flag(&mut args, self.all, "--all");
        push_flag(&mut args, self.heavy, "--heavy");
        push_flag(&mut args, self.include_ignored, "--include-ignored");
        push_flag(&mut args, self.list, "--list");
        push_flag(
            &mut args,
            self.skip_preflight || force_skip_preflight,
            "--skip-preflight",
        );
        push_flag(&mut args, self.prime, "--prime");
        push_flag(&mut args, self.dry_run, "--dry-run");
        push_flag(&mut args, self.update_snapshots, "--update-snapshots");
        push_flag(
            &mut args,
            self.allow_contended_host,
            "--allow-contended-host",
        );
        push_flag(&mut args, self.no_reuse, "--no-reuse");
        push_flag(&mut args, self.lib, "--lib");
        for feature in normalize_packages(&self.cargo_features) {
            args.push("--features".to_string());
            args.push(feature);
        }
        if !matches!(self.impact_mode, crate::impact::ImpactMode::Balanced) {
            args.push(format!("--impact-mode={}", self.impact_mode.as_str()));
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

    /// Load the impact plan if automatic impact is enabled.
    fn load_impact_plan(&self, ctx: &CommandContext) -> Result<Option<crate::impact::ImpactPlan>> {
        if !self.uses_automatic_impact() {
            return Ok(None);
        }
        let plan = match ctx.try_with_history_db_query(|db| {
            crate::impact::plan_default_test_impact_with_history_and_mode(
                Some(db),
                self.impact_mode,
            )
        }) {
            Some(result) => result?,
            None => crate::impact::plan_default_test_impact_with_history_and_mode(
                None,
                self.impact_mode,
            )?,
        };
        if let Some(result) = ctx.try_with_history_db(|db| {
            db.record_impact_plan(ctx.invocation_id(), self.impact_mode.as_str(), &plan)
        }) && let Err(error) = result
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
        Ok(Some(plan))
    }

    /// Check if an exact proof can be reused, returning early if so.
    fn check_exact_impact_proof(
        &self,
        ctx: &CommandContext,
        impact_plan: Option<&crate::impact::ImpactPlan>,
    ) -> Result<Option<CommandResult>> {
        let Some(plan) = impact_plan else {
            return Ok(None);
        };
        if !self.can_consume_exact_test_proof() || !plan.can_reuse_exact_proof() {
            return Ok(None);
        }
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
            return Ok(Some(
                CommandResult::success()
                    .with_message("tests skipped by exact impact proof")
                    .with_detail(format!("reused_invocation={}", proof.invocation_id))
                    .with_duration(ctx.elapsed())
                    .with_data(serde_json::json!({
                        "impact_plan": plan,
                        "reused_proof": proof,
                    })),
            ));
        }
        Ok(None)
    }
}

/// Run `cargo nextest list` with the resolved execution plan scope.
fn execute_nextest_list(
    execution_plan: &plan::NextestExecutionPlan,
    effective_test_binaries: &[String],
    effective_lib_target: bool,
    cargo_features: &[String],
    effective_filter: Option<&str>,
) -> Result<CommandResult> {
    let mut cmd = crate::process::ProcessBuilder::cargo().args(["nextest", "list"]);
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
    for test_binary in effective_test_binaries {
        cmd = cmd.args(["--test", test_binary]);
    }
    if effective_lib_target {
        cmd = cmd.arg("--lib");
    }
    for feature in cargo_features {
        cmd = cmd.args(["--features", feature]);
    }
    if let Some(filter) = effective_filter {
        cmd = cmd.args(["-E", filter]);
    }
    cmd.run_ok()?;
    Ok(CommandResult::success().with_detail("tests listed"))
}

/// Print the human-readable dry-run plan summary to stdout.
#[allow(clippy::too_many_arguments)]
fn print_dry_run_plan(
    this: &TestCommand,
    ctx: &CommandContext,
    workload_scope: &WorkloadScope,
    execution_plan: &NextestExecutionPlan,
    effective_test_binaries: &[String],
    effective_lib_target: bool,
    effective_filter: &Option<String>,
    runtime_binary_requirements: &[plan::RuntimeBinaryRequirement],
    db_pool_size: Option<usize>,
    reusable: bool,
    reusable_proof: &Option<crate::history::TestProofUnit>,
) {
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
    let features = normalize_packages(&this.cargo_features);
    if !features.is_empty() {
        println!("  features: {}", features.join(", "));
    }
    println!("  lib target: {effective_lib_target}");
    if let Some(filter) = effective_filter {
        println!("  filter: {filter}");
    }
    if runtime_binary_requirements.is_empty() {
        println!("  runtime binaries: none");
    } else {
        println!(
            "  runtime binaries: {}",
            runtime_binary_requirements
                .iter()
                .map(|r| format!("{}:{}", r.package, r.binary))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!(
        "  db pool size override: {}",
        db_pool_size.map_or_else(|| "default".to_string(), |v| v.to_string())
    );
    let reuse_state = if this.no_reuse {
        "disabled by --no-reuse".to_string()
    } else if this.list {
        "disabled for --list".to_string()
    } else if let Some(proof) = reusable_proof {
        format!("hit invocation {}", proof.invocation_id)
    } else if reusable {
        "eligible, no exact proof yet".to_string()
    } else {
        "disabled for runtime or mutating test shape".to_string()
    };
    println!("  reuse eligibility: {reuse_state}");

    if !execution_plan.runner_packages.is_empty() {
        let coverage = this.classify_package_proof_coverage(ctx, &execution_plan.runner_packages);
        print_proof_coverage(&coverage);
    }
}

/// Print the proof coverage classification summary.
fn print_proof_coverage(coverage: &[PackageProofCoverage]) {
    let covered: Vec<_> = coverage
        .iter()
        .filter(|c| c.state == ProofCoverageState::Covered)
        .collect();
    let missing: Vec<_> = coverage
        .iter()
        .filter(|c| c.state == ProofCoverageState::Missing)
        .collect();
    let ineligible: Vec<_> = coverage
        .iter()
        .filter(|c| c.state == ProofCoverageState::Ineligible)
        .collect();
    let stale: Vec<_> = coverage
        .iter()
        .filter(|c| c.state == ProofCoverageState::Stale)
        .collect();
    let fmt_with_id = |items: &[&PackageProofCoverage]| {
        items
            .iter()
            .map(|c| format!("{}@{}", c.package, c.proof_invocation_id.unwrap_or(0)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    if !covered.is_empty() {
        println!("  proof covered: {}", fmt_with_id(&covered));
    }
    if !stale.is_empty() {
        println!("  proof stale: {}", fmt_with_id(&stale));
    }
    if !missing.is_empty() {
        println!(
            "  proof missing: {}",
            missing
                .iter()
                .map(|c| c.package.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if !ineligible.is_empty() {
        println!(
            "  proof ineligible: {}",
            ineligible
                .iter()
                .map(|c| c.package.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}

/// Build serialized background CLI args for a bench subcommand.
fn bench_background_args(bench: &BenchArgs) -> Vec<String> {
    let mut args = vec![
        "bench".to_string(),
        format!("--mode={}", bench.mode),
        format!("--profile={}", bench.profile),
        format!("--runs={}", bench.runs),
        {
            let threads_str: Vec<String> = bench.threads.iter().map(ToString::to_string).collect();
            format!("--threads={}", threads_str.join(","))
        },
    ];
    if !bench.db_pool_sizes.is_empty() {
        let pool_sizes = bench
            .db_pool_sizes
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        args.push(format!("--db-pool-sizes={pool_sizes}"));
    }
    push_flag(&mut args, bench.system_impact, "--system-impact");
    push_flag(
        &mut args,
        bench.system_impact_extended,
        "--system-impact-extended",
    );
    args.push(format!("--target={}", bench.target));
    push_flag(&mut args, bench.contracts, "--contracts");
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
    push_flag(&mut args, bench.dry_run, "--dry-run");
    push_flag(&mut args, bench.continue_on_fail, "--continue-on-fail");
    push_flag(
        &mut args,
        bench.allow_contended_host,
        "--allow-contended-host",
    );
    push_flag(&mut args, bench.verbose, "--verbose");
    args
}

/// Build serialized background CLI args for a fuzz subcommand.
fn fuzz_background_args(fuzz: &FuzzArgs) -> Vec<String> {
    let mut args = vec!["fuzz".to_string()];
    if let Some(ref t) = fuzz.target {
        args.push(t.clone());
    }
    args.push(format!("--max-time={}", fuzz.max_time));
    if let Some(j) = fuzz.jobs {
        args.push(format!("--jobs={j}"));
    }
    push_flag(&mut args, fuzz.list, "--list");
    args
}

/// Build serialized background CLI args for a coverage subcommand.
fn coverage_background_args(cov: &CoverageArgs) -> Vec<String> {
    let mut args = vec!["coverage".to_string(), format!("--output={}", cov.output)];
    push_flag(&mut args, cov.open, "--open");
    if let Some(ref p) = cov.package {
        args.push(format!("--package={p}"));
    }
    push_flag(&mut args, cov.html, "--html");
    if let Some(e) = cov.enforce {
        args.push(format!("--enforce={e}"));
    }
    args
}

/// Build serialized background CLI args for a mutants subcommand.
fn mutants_background_args(m: &MutantsArgs) -> Vec<String> {
    let mut args = vec!["mutants".to_string()];
    if let Some(ref p) = m.package {
        args.push(format!("--package={p}"));
    }
    if let Some(ref f) = m.file {
        args.push(format!("--file={f}"));
    }
    args.push(format!("--timeout={}", m.timeout));
    args.push(format!("--jobs={}", m.jobs));
    args
}

impl XtaskCommand for TestCommand {
    fn name(&self) -> &'static str {
        "test"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution (like build/check/fix)
        if ctx.is_background() {
            return self.execute_background(ctx);
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
            return self.execute_dry_run(ctx);
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

        let impact_plan = self.load_impact_plan(ctx)?;
        if let Some(early_return) = self.check_exact_impact_proof(ctx, impact_plan.as_ref())? {
            return Ok(early_return);
        }

        let raw_impact_packages = impact_plan
            .as_ref()
            .and_then(crate::impact::packages_for_plan);
        let effective_filter = impact_plan
            .as_ref()
            .and_then(|plan| plan.impact_filter.clone())
            .or_else(|| self.filter.clone());

        // Determine the package set to test and subtract reusable proofs before
        // preflight. A reusable proof or package-proof hit should not start
        // Postgres/NATS only to discover that no tests need to run.
        let (impact_packages, reused_package_proofs) = {
            let packages_for_subtraction: Option<Vec<String>> =
                if let Some(packages) = raw_impact_packages.as_ref() {
                    Some(packages.clone())
                } else if !self.packages.is_empty() && effective_filter.is_none() {
                    Some(normalize_packages(&self.packages))
                } else {
                    None
                };

            if self.can_consume_exact_test_proof()
                && effective_filter.is_none()
                && let Some(packages) = packages_for_subtraction
            {
                let (remaining, reused) =
                    self.subtract_reusable_impact_package_proofs(ctx, &packages)?;
                if ctx.is_human() && !reused.is_empty() {
                    println!(
                        "Reusing {} package proof{}: {}",
                        reused.len(),
                        if reused.len() == 1 { "" } else { "s" },
                        reused
                            .iter()
                            .map(|proof| format!("{}@{}", proof.package, proof.invocation_id))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                if remaining.is_empty() && !reused.is_empty() {
                    return Ok(CommandResult::success()
                        .with_message("tests skipped by package proofs")
                        .with_detail(format!("reused_packages={}", reused.len()))
                        .with_duration(ctx.elapsed())
                        .with_data(serde_json::json!({
                            "impact_plan": impact_plan.clone(),
                            "reused_package_proofs": reused,
                        })));
                }
                (Some(remaining), reused)
            } else {
                (raw_impact_packages, Vec::new())
            }
        };
        let effective_test_binaries = self.effective_test_binaries(effective_filter.as_deref())?;
        let effective_lib_target =
            self.effective_lib_target(effective_filter.as_deref(), &effective_test_binaries)?;
        let execution_plan = self.resolve_execution_plan(
            Some(ctx),
            effective_filter.as_deref(),
            impact_packages.as_deref(),
        )?;
        let workload_scope = execution_plan.workload_scope.clone();
        let coordination_args = self.semantic_invocation_args(
            &workload_scope,
            effective_filter.as_deref(),
            &effective_test_binaries,
            effective_lib_target,
        );
        ctx.record_invocation_args(&coordination_args);

        // List is an introspection command, not a proof-producing or
        // proof-consuming test run. It also does not need runtime preflight.
        if self.list {
            return execute_nextest_list(
                &execution_plan,
                &effective_test_binaries,
                effective_lib_target,
                &normalize_packages(&self.cargo_features),
                effective_filter.as_deref(),
            );
        }

        ctx.record_coordination_fingerprint("test", &coordination_args);
        if self.can_consume_exact_test_proof() {
            let proof_kind = crate::coordinator::proof_kind("test", &coordination_args);
            if proof_kind == "test.nextest.exact" {
                let input_fingerprint = crate::coordinator::current_scoped_tree_fingerprint(
                    "test",
                    &coordination_args,
                )?;
                let scope_key = crate::coordinator::compute_scope_key("test", &coordination_args);
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
                        .with_message("tests skipped by exact proof")
                        .with_detail(format!("reused_invocation={}", proof.invocation_id))
                        .with_duration(ctx.elapsed())
                        .with_data(serde_json::json!({
                            "impact_plan": impact_plan.clone(),
                            "reused_proof": proof,
                            "reused_package_proofs": reused_package_proofs,
                        })));
                }
            }
        }

        // Determine profile
        // Available profiles in .config/nextest.toml:
        //   default = 24 threads, fail-fast=false (good for CI/batch runs)
        //   debug   = 1 thread, 300s slow-timeout (good for investigating tests)
        let profile = if self.debug { "debug" } else { "default" };
        let use_fail_fast = self.fail_fast;

        let runtime_binary_requirements = runtime_binary_requirements_for_target(
            &execution_plan,
            effective_lib_target,
            &effective_test_binaries,
            effective_filter.as_deref(),
        );

        // Preflight is default ON unless explicitly disabled. Runtime-independent
        // nextest plans still need compile-time sqlx readiness, but they should
        // not auto-start NATS, TLS, or contract deployment just to compile and
        // execute local unit/DTO tests.
        let mut _compile_ready_guard: Option<crate::preflight::CompileReadyGuard> = None;
        match preflight_mode_for_test_plan(
            self.skip_preflight,
            self.prime,
            &runtime_binary_requirements,
        ) {
            TestPreflightMode::Skipped => {}
            TestPreflightMode::CompileOnly => {
                let stage = ctx.start_stage("preflight");
                let ready = crate::preflight::ensure_compile_ready(ctx);
                ctx.finish_stage(stage, ready.is_ok());
                _compile_ready_guard = Some(ready?);
            }
            TestPreflightMode::RuntimeStack => {
                let stage = ctx.start_stage("preflight");
                let ready = crate::preflight::ensure_ready(ctx);
                ctx.finish_stage(stage, ready.is_ok());
                ready?;
            }
        }

        self.guard_broad_start_pressure(
            ctx,
            &execution_plan,
            effective_filter.as_deref(),
            &effective_test_binaries,
        )?;

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
        for feature in normalize_packages(&self.cargo_features) {
            runner.add_arg("--features");
            runner.add_arg(feature);
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
                "reused_package_proofs": reused_package_proofs,
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
                        let reusable = self.can_consume_exact_test_proof()
                            && proof_kind == "test.nextest.exact";
                        let manifest = serde_json::json!({
                            "scope": workload_scope.encode_marker(),
                            "runner_packages": execution_plan.runner_packages,
                            "excluded_packages": execution_plan.excluded_packages,
                            "test_binaries": effective_test_binaries,
                            "lib": effective_lib_target,
                            "features": normalize_packages(&self.cargo_features),
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
                            "reused_package_proofs": reused_package_proofs.clone(),
                        });
                        match serde_json::to_string(&manifest)
                            .map_err(color_eyre::eyre::Report::from)
                            .and_then(|manifest_json| {
                                let filter_for_proof = effective_filter.clone();
                                if let Some(result) = ctx.try_with_history_db(|db| {
                                    let r = db.record_test_proof_unit(
                                        invocation_id,
                                        &proof_kind,
                                        &scope_key,
                                        &input_fingerprint,
                                        &manifest_json,
                                        reusable,
                                    );
                                    // Store test filter for per-test-name evidence (#1393 Phase 3).
                                    if r.is_ok()
                                        && let Some(ref filter) = filter_for_proof
                                    {
                                        let _ = db.set_test_proof_filter(
                                            invocation_id,
                                            &proof_kind,
                                            &scope_key,
                                            &input_fingerprint,
                                            filter,
                                        );
                                    }
                                    r
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
                    "reused_package_proofs": reused_package_proofs,
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
    async fn test_semantic_invocation_args_include_cargo_features()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            cargo_features: vec!["extra-feature".to_string()],
            ..Default::default()
        };

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["xtask".to_string()]),
            Some("test(some_case)"),
            &[],
            true,
        );

        assert!(args.contains(&"--features=extra-feature".to_string()));
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
    async fn test_explicit_package_scope_preserves_matching_test_binary_inference()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            packages: vec!["sinexd".to_string()],
            filter: Some("test(weechat_descriptor_registered)".to_string()),
            ..Default::default()
        };

        let binaries = command.effective_test_binaries(command.filter.as_deref())?;

        assert_eq!(
            binaries,
            vec!["registry_dispatch_test".to_string()],
            "explicit matching package scope should still infer the exact integration-test binary"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_explicit_package_scope_rejects_cross_package_test_binary_inference()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            packages: vec!["xtask".to_string()],
            filter: Some("test(mcp_catalog_exactly_covers_live_tools)".to_string()),
            ..Default::default()
        };

        let binaries = command.effective_test_binaries(command.filter.as_deref())?;

        assert!(
            binaries.is_empty(),
            "explicit package scope must not infer integration-test binaries from other packages: {binaries:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_semantic_invocation_args_include_lib_target() -> ::xtask::sandbox::TestResult<()>
    {
        let command = TestCommand::default();

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["sinexd".to_string()]),
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
    async fn test_semantic_invocation_args_include_all_scope() -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            all: true,
            ..Default::default()
        };

        let args = command.semantic_invocation_args(&WorkloadScope::Workspace, None, &[], false);

        assert!(
            args.contains(&"--all".to_string()),
            "--all must be part of the proof identity: {args:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_semantic_invocation_args_include_configured_db_pool_size()
    -> ::xtask::sandbox::TestResult<()> {
        let _guard = crate::sandbox::prelude::EnvGuard::set_single("SINEX_TEST_DB_POOL_SIZE", "48");
        let command = TestCommand::default();

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["sinexd".to_string()]),
            Some("test(one)"),
            &[],
            true,
        );

        assert!(
            args.contains(&"--db-pool-size-env=48".to_string()),
            "configured DB pool size must be part of the proof identity: {args:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_semantic_invocation_args_include_runtime_binary_requirements()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand::default();

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["sinex-db".to_string()]),
            None,
            &[],
            false,
        );

        assert!(
            args.contains(&"--runtime-binary=sinexd:sinexd".to_string()),
            "runtime binary requirements must be part of proof identity: {args:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_semantic_invocation_args_ignore_success_irrelevant_scheduling_flags()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            fail_fast: true,
            allow_contended_host: true,
            ..Default::default()
        };

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["xtask".to_string()]),
            Some("test(example)"),
            &[],
            true,
        );

        assert!(
            !args.contains(&"--fail-fast".to_string()),
            "--fail-fast affects failure scheduling, not successful proof identity: {args:?}"
        );
        assert!(
            !args.contains(&"--allow-contended-host".to_string()),
            "host-pressure override affects command admission, not proof identity: {args:?}"
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
            runner_packages: vec!["sinexd".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinexd".to_string()]),
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
            runner_packages: vec!["sinexd".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinexd".to_string()]),
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
    async fn test_exact_package_proof_args_match_reusable_package_scope()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand::default();

        let args = command.exact_package_proof_args("xtask");

        assert_eq!(
            crate::coordinator::proof_kind("test", &args),
            "test.nextest.exact"
        );
        assert!(
            args.contains(&"--scope=packages:xtask".to_string()),
            "package proof args should use the same scope marker as executed package tests: {args:?}"
        );
        assert!(
            !args.iter().any(|arg| arg.starts_with("--filter=")),
            "package proof subtraction must not claim coverage of a filtered test plan: {args:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_subtract_reusable_impact_package_proofs_keeps_unproven_packages()
    -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = HistoryDb::open(&db_path)?;
        let command = TestCommand::default();
        let proof_args = command.exact_package_proof_args("xtask");
        let input_fingerprint =
            crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
        let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
        let invocation_id = db.start_invocation("test", None, None, None)?;
        db.record_test_proof_unit(
            invocation_id,
            "test.nextest.exact",
            &scope_key,
            &input_fingerprint,
            r#"{"scope":"packages:xtask"}"#,
            true,
        )?;
        db.finish_invocation(
            invocation_id,
            crate::history::InvocationStatus::Success,
            Some(0),
            0.1,
        )?;
        drop(db);

        let ctx = test_context(db_path);
        let (remaining, reused) = command.subtract_reusable_impact_package_proofs(
            &ctx,
            &["xtask".to_string(), "sinex-primitives".to_string()],
        )?;

        assert_eq!(remaining, vec!["sinex-primitives".to_string()]);
        assert_eq!(reused.len(), 1);
        assert_eq!(reused[0].package, "xtask");
        assert_eq!(reused[0].invocation_id, invocation_id);
        assert_eq!(reused[0].proof_kind, "test.nextest.exact");
        assert_eq!(reused[0].scope_key, scope_key);
        Ok(())
    }

    #[sinex_test]
    async fn test_explicit_package_proof_subtraction_keeps_unproven()
    -> ::xtask::sandbox::TestResult<()> {
        // When explicit -p lists two packages and only one has a reusable proof,
        // subtract_reusable_impact_package_proofs keeps the unproven package.
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = HistoryDb::open(&db_path)?;
        let command = TestCommand::default();
        let proof_args = command.exact_package_proof_args("xtask");
        let input_fingerprint =
            crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
        let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
        let invocation_id = db.start_invocation("test", None, None, None)?;
        db.record_test_proof_unit(
            invocation_id,
            "test.nextest.exact",
            &scope_key,
            &input_fingerprint,
            r#"{"scope":"packages:xtask"}"#,
            true,
        )?;
        db.finish_invocation(
            invocation_id,
            crate::history::InvocationStatus::Success,
            Some(0),
            0.1,
        )?;
        drop(db);

        // Test with explicit -p via the subtraction method directly.
        // The command doesn't need packages set — the method receives them.
        let ctx = test_context(db_path);
        let (remaining, reused) = command.subtract_reusable_impact_package_proofs(
            &ctx,
            &["xtask".to_string(), "xtask-macros".to_string()],
        )?;
        assert_eq!(remaining, vec!["xtask-macros".to_string()]);
        assert_eq!(reused.len(), 1);
        assert_eq!(reused[0].package, "xtask");
        Ok(())
    }

    #[sinex_test]
    async fn test_subtract_explicit_package_proof_all_reusable() -> ::xtask::sandbox::TestResult<()>
    {
        // Verify that when all explicit -p packages have reusable proofs,
        // execution is skipped without running nextest.
        let mut _env = crate::sandbox::prelude::EnvGuard::new();
        _env.clear("NEXTEST_RUN_ID");
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = HistoryDb::open(&db_path)?;
        let command = TestCommand {
            packages: vec!["xtask".to_string()],
            skip_preflight: true,
            ..Default::default()
        };
        let proof_args = command.exact_package_proof_args("xtask");
        let input_fingerprint =
            crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
        let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
        let invocation_id = db.start_invocation("test", None, None, None)?;
        db.record_test_proof_unit(
            invocation_id,
            "test.nextest.exact",
            &scope_key,
            &input_fingerprint,
            r#"{"scope":"packages:xtask"}"#,
            true,
        )?;
        db.finish_invocation(
            invocation_id,
            crate::history::InvocationStatus::Success,
            Some(0),
            0.1,
        )?;
        drop(db);

        let ctx = test_context_with_invocation(db_path, Some(invocation_id));
        let result = command.execute(&ctx).await?;
        assert_eq!(result.status, crate::output::Status::Success);
        assert_eq!(
            result.message.as_deref(),
            Some("tests skipped by package proofs")
        );
        let reused: Vec<i64> = result
            .data
            .as_ref()
            .and_then(|data| data["reused_package_proofs"].as_array())
            .map(|proofs| {
                proofs
                    .iter()
                    .filter_map(|p| p["invocation_id"].as_i64())
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(reused, vec![invocation_id]);
        Ok(())
    }

    #[sinex_test]
    async fn test_execute_reuses_exact_test_proof_before_nextest()
    -> ::xtask::sandbox::TestResult<()> {
        let mut _env = crate::sandbox::prelude::EnvGuard::new();
        _env.clear("NEXTEST_RUN_ID");
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = HistoryDb::open(&db_path)?;
        let command = TestCommand {
            packages: vec!["xtask".to_string()],
            skip_preflight: true,
            ..Default::default()
        };
        let proof_args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["xtask".to_string()]),
            None,
            &[],
            false,
        );
        let input_fingerprint =
            crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
        let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
        let invocation_id = db.start_invocation("test", None, None, None)?;
        db.record_test_proof_unit(
            invocation_id,
            "test.nextest.exact",
            &scope_key,
            &input_fingerprint,
            r#"{"scope":"packages:xtask"}"#,
            true,
        )?;
        db.finish_invocation(
            invocation_id,
            crate::history::InvocationStatus::Success,
            Some(0),
            0.1,
        )?;
        drop(db);

        let ctx = test_context(db_path);
        let result = command.execute(&ctx).await?;

        assert_eq!(result.status, crate::output::Status::Success);
        // Explicit -p packages now go through package-level subtraction first,
        // so the skip message reflects that path.
        assert!(result.message.as_deref().is_some_and(|msg| {
            msg == "tests skipped by exact proof" || msg == "tests skipped by package proofs"
        }));
        // Proof data may be in reused_proof (exact path) or reused_package_proofs (package path).
        let reused_invocation: Option<i64> = result.data.as_ref().and_then(|data| {
            data["reused_proof"]["invocation_id"].as_i64().or_else(|| {
                data["reused_package_proofs"]
                    .as_array()
                    .and_then(|proofs| proofs.first())
                    .and_then(|p| p["invocation_id"].as_i64())
            })
        });
        assert_eq!(reused_invocation, Some(invocation_id));
        Ok(())
    }

    #[sinex_test]
    async fn test_no_reuse_changes_test_proof_kind_to_plan() -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            no_reuse: true,
            ..Default::default()
        };

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["xtask".to_string()]),
            None,
            &[],
            false,
        );

        assert!(args.contains(&"--no-reuse".to_string()));
        assert_eq!(
            crate::coordinator::proof_kind("test", &args),
            "test.nextest.plan"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_list_disables_exact_test_proof_reuse() -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            list: true,
            packages: vec!["xtask".to_string()],
            ..Default::default()
        };

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["xtask".to_string()]),
            None,
            &[],
            false,
        );

        assert_eq!(
            crate::coordinator::proof_kind("test", &args),
            "test.nextest.exact"
        );
        assert!(!command.can_consume_exact_test_proof());
        Ok(())
    }

    #[sinex_test]
    async fn test_prime_disables_exact_test_proof_reuse() -> ::xtask::sandbox::TestResult<()> {
        for (flag, command) in [(
            "--prime",
            TestCommand {
                prime: true,
                packages: vec!["xtask".to_string()],
                ..Default::default()
            },
        )] {
            let args = command.semantic_invocation_args(
                &WorkloadScope::Packages(vec!["xtask".to_string()]),
                None,
                &[],
                false,
            );

            assert!(args.contains(&flag.to_string()), "{flag} missing: {args:?}");
            assert_eq!(
                crate::coordinator::proof_kind("test", &args),
                "test.nextest.plan",
                "{flag} must not produce an exact reusable proof key"
            );
            assert!(
                !command.can_consume_exact_test_proof(),
                "{flag} must bypass direct exact proof consumption"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_preflight_mode_uses_compile_only_for_runtime_independent_tests()
    -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(
            super::preflight_mode_for_test_plan(false, false, &[]),
            super::TestPreflightMode::CompileOnly
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_preflight_mode_uses_runtime_stack_for_runtime_test_requirements()
    -> ::xtask::sandbox::TestResult<()> {
        let requirements = [plan::RuntimeBinaryRequirement {
            package: "sinexd",
            binary: "sinexd",
        }];

        assert_eq!(
            super::preflight_mode_for_test_plan(false, false, &requirements),
            super::TestPreflightMode::RuntimeStack
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_preflight_mode_uses_runtime_stack_for_pool_priming()
    -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(
            super::preflight_mode_for_test_plan(false, true, &[]),
            super::TestPreflightMode::RuntimeStack
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_preflight_mode_honors_explicit_skip() -> ::xtask::sandbox::TestResult<()> {
        let requirements = [plan::RuntimeBinaryRequirement {
            package: "sinexd",
            binary: "sinexd",
        }];

        assert_eq!(
            super::preflight_mode_for_test_plan(true, true, &requirements),
            super::TestPreflightMode::Skipped
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_semantic_invocation_args_include_test_binary_args()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            packages: vec!["xtask".to_string()],
            args: vec!["--exact".to_string(), "case-name".to_string()],
            ..Default::default()
        };

        let args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["xtask".to_string()]),
            None,
            &[],
            false,
        );

        assert!(
            args.contains(&"--test-arg=--exact".to_string()),
            "test binary args should be part of the proof identity: {args:?}"
        );
        assert!(
            args.contains(&"--test-arg=case-name".to_string()),
            "test binary args should be part of the proof identity: {args:?}"
        );
        assert_eq!(
            crate::coordinator::proof_kind("test", &args),
            "test.nextest.exact"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_nextest_invocation_args_include_reuse_and_impact_flags()
    -> ::xtask::sandbox::TestResult<()> {
        let command = TestCommand {
            no_reuse: true,
            impact_mode: crate::impact::ImpactMode::Aggressive,
            packages: vec!["xtask".to_string()],
            filter: Some("test(freshness_explain)".to_string()),
            cargo_features: vec!["extra-feature".to_string()],
            ..Default::default()
        };

        let args = command.nextest_invocation_args(false);

        assert!(args.contains(&"--no-reuse".to_string()));
        assert!(args.contains(&"--impact-mode=aggressive".to_string()));
        assert_eq!(
            args.windows(2)
                .find(|window| window[0] == "-p")
                .map(|window| window[1].as_str()),
            Some("xtask")
        );
        assert_eq!(
            args.windows(2)
                .find(|window| window[0] == "-E")
                .map(|window| window[1].as_str()),
            Some("test(freshness_explain)")
        );
        assert_eq!(
            args.windows(2)
                .find(|window| window[0] == "--features")
                .map(|window| window[1].as_str()),
            Some("extra-feature")
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

    #[sinex_test]
    async fn test_classify_package_proof_coverage_covered() -> ::xtask::sandbox::TestResult<()> {
        let mut _env = crate::sandbox::prelude::EnvGuard::new();
        _env.clear("NEXTEST_RUN_ID");
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = HistoryDb::open(&db_path)?;
        let command = TestCommand {
            packages: vec!["xtask".to_string()],
            skip_preflight: true,
            ..Default::default()
        };
        let proof_args = command.semantic_invocation_args(
            &WorkloadScope::Packages(vec!["xtask".to_string()]),
            None,
            &[],
            false,
        );
        let input_fingerprint =
            crate::coordinator::current_scoped_tree_fingerprint("test", &proof_args)?;
        let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
        let invocation_id = db.start_invocation("test", None, None, None)?;
        db.record_test_proof_unit(
            invocation_id,
            "test.nextest.exact",
            &scope_key,
            &input_fingerprint,
            r#"{"scope":"packages:xtask"}"#,
            true,
        )?;
        db.finish_invocation(
            invocation_id,
            crate::history::InvocationStatus::Success,
            Some(0),
            0.1,
        )?;
        drop(db);

        let ctx = test_context(db_path);
        let coverage = command.classify_package_proof_coverage(&ctx, &["xtask".to_string()]);
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, super::ProofCoverageState::Covered);
        assert_eq!(coverage[0].proof_invocation_id, Some(invocation_id));
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_package_proof_coverage_missing() -> ::xtask::sandbox::TestResult<()> {
        let mut _env = crate::sandbox::prelude::EnvGuard::new();
        _env.clear("NEXTEST_RUN_ID");
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let _db = HistoryDb::open(&db_path)?;
        let command = TestCommand {
            packages: vec!["xtask".to_string()],
            skip_preflight: true,
            ..Default::default()
        };
        let ctx = test_context(db_path);
        let coverage = command.classify_package_proof_coverage(&ctx, &["xtask".to_string()]);
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, super::ProofCoverageState::Missing);
        assert!(coverage[0].proof_invocation_id.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_package_proof_coverage_ineligible_no_reuse()
    -> ::xtask::sandbox::TestResult<()> {
        let mut _env = crate::sandbox::prelude::EnvGuard::new();
        _env.clear("NEXTEST_RUN_ID");
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let _db = HistoryDb::open(&db_path)?;
        let command = TestCommand {
            packages: vec!["xtask".to_string()],
            skip_preflight: true,
            no_reuse: true,
            ..Default::default()
        };
        let ctx = test_context(db_path);
        let coverage = command.classify_package_proof_coverage(&ctx, &["xtask".to_string()]);
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, super::ProofCoverageState::Ineligible);
        assert!(coverage[0].proof_invocation_id.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_package_proof_coverage_empty_packages()
    -> ::xtask::sandbox::TestResult<()> {
        let mut _env = crate::sandbox::prelude::EnvGuard::new();
        _env.clear("NEXTEST_RUN_ID");
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let _db = HistoryDb::open(&db_path)?;
        let command = TestCommand {
            skip_preflight: true,
            ..Default::default()
        };
        let ctx = test_context(db_path);
        let coverage = command.classify_package_proof_coverage(&ctx, &[]);
        assert!(coverage.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_package_proof_coverage_stale() -> ::xtask::sandbox::TestResult<()> {
        // A proof exists for the scope but with a different (old) fingerprint
        // than the current tree — it should be classified as Stale.
        let mut _env = crate::sandbox::prelude::EnvGuard::new();
        _env.clear("NEXTEST_RUN_ID");
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.db");
        let db = HistoryDb::open(&db_path)?;
        let command = TestCommand {
            packages: vec!["xtask".to_string()],
            skip_preflight: true,
            ..Default::default()
        };
        // Record a proof with a deliberately different (stale) fingerprint
        let proof_args = command.exact_package_proof_args("xtask");
        let scope_key = crate::coordinator::compute_scope_key("test", &proof_args);
        let stale_fingerprint = "0000000000000000000000000000000000000000000000000000000000000000";
        let invocation_id = db.start_invocation("test", None, None, None)?;
        db.record_test_proof_unit(
            invocation_id,
            "test.nextest.exact",
            &scope_key,
            stale_fingerprint,
            r#"{"scope":"packages:xtask"}"#,
            true,
        )?;
        db.finish_invocation(
            invocation_id,
            crate::history::InvocationStatus::Success,
            Some(0),
            0.1,
        )?;
        drop(db);

        let ctx = test_context(db_path);
        let coverage = command.classify_package_proof_coverage(&ctx, &["xtask".to_string()]);
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, super::ProofCoverageState::Stale);
        assert_eq!(coverage[0].proof_invocation_id, Some(invocation_id));
        Ok(())
    }
}
