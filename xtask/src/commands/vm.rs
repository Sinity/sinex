//! VM commands - NixOS VM management for integration testing.
//!
//! This module provides commands for managing NixOS VMs used in
//! end-to-end testing of the sinex infrastructure.
//!
//! ## NixOS Compatibility Gate
//!
//! VM tests ARE the NixOS compatibility enforcement mechanism.
//! They import real NixOS modules and exercise actual deployment paths.
//!
//! Current exported flake checks:
//!   `xtask test vm --category smoke`       # basic-flow coverage
//!   `xtask test vm --category integration` # module/runtime compatibility coverage

use color_eyre::eyre::{Result, WrapErr, bail};
use console::style;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config;
use crate::history::TestStatus;

// ─────────────────────────────────────────────────────────────────────────────
// Test Catalogue
// ─────────────────────────────────────────────────────────────────────────────

const SMOKE_TESTS: &[&str] = &["basic", "replay-smoke"];
const INTEGRATION_TESTS: &[&str] = &[
    "preflight",
    "maintenance",
    "node-matrix",
    "multi-source",
    "failure-recovery",
    "kitty-eventsource",
    "mtls-enforcement",
    "sinexctl-e2e",
    // Environmental hostility tests
    "hostile-host",
    "migration-stress",
];
const PERFORMANCE_TESTS: &[&str] = &["performance", "production-scale"];
const CHAOS_TESTS: &[&str] = &[
    "chaos-network-partition",
    "chaos-process-restart",
    "chaos-clock-skew",
    "xtask-concurrency",
];

/// Default timeout per test in seconds (15 minutes).
pub const DEFAULT_TIMEOUT_SECS: u64 = 900;
/// Extended timeout for closure-heavy deployment/runtime scenarios: 60 minutes.
const EXTENDED_TIMEOUT_SECS: u64 = 3600;

/// Tests that require the extended timeout.
const EXTENDED_TIMEOUT_TESTS: &[&str] = &[
    "basic",
    "maintenance",
    "node-matrix",
    "multi-source",
    "performance",
    "production-scale",
    "migration-stress",
    "chaos-network-partition",
    "chaos-process-restart",
    "chaos-clock-skew",
    "xtask-concurrency",
];

fn all_tests() -> Vec<&'static str> {
    let mut tests: Vec<&'static str> = Vec::new();
    tests.extend_from_slice(SMOKE_TESTS);
    tests.extend_from_slice(INTEGRATION_TESTS);
    tests.extend_from_slice(PERFORMANCE_TESTS);
    tests.extend_from_slice(CHAOS_TESTS);
    tests.sort_unstable();
    tests.dedup();
    tests
}

// ─────────────────────────────────────────────────────────────────────────────
// Command Definitions
// ─────────────────────────────────────────────────────────────────────────────

/// VM management command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum VmSubcommand {
    /// Run exported NixOS VM flake checks
    Test {
        /// Test category: smoke, integration, performance, chaos, all
        #[arg(long, short)]
        category: Option<String>,
        /// Timeout per test in seconds (default: 900, closure-heavy scenarios: 3600)
        #[arg(long, short, default_value = "900")]
        timeout: u64,
        /// Keep VM state after test failure for debugging
        #[arg(long, short)]
        keep_failed: bool,
        /// List available tests
        #[arg(long, short)]
        list: bool,
        /// Validate VM test infrastructure (nix syntax check)
        #[arg(long)]
        validate: bool,
        /// Specific test names to run
        tests: Vec<String>,
    },
    /// Start an interactive VM
    Start {
        /// VM preset name. Interactive preset wiring is not implemented yet.
        preset: String,
        /// Keep state between runs
        #[arg(long)]
        persistent: bool,
        /// Start from a snapshot
        #[arg(long)]
        snapshot: Option<String>,
    },
    /// SSH into a running VM
    Ssh,
    /// Stop a running VM
    Stop,
    /// Manage VM snapshots
    Snapshot {
        #[command(subcommand)]
        cmd: VmSnapshotSubcommand,
    },
}

/// VM snapshot subcommands
#[derive(Debug, Clone, clap::Subcommand)]
pub enum VmSnapshotSubcommand {
    /// Create a named snapshot
    Create { name: String },
    /// Restore from a snapshot
    Restore { name: String },
    /// List available snapshots
    List,
}

/// VM command
#[derive(Debug, Clone, clap::Args)]
pub struct VmCommand {
    #[command(subcommand)]
    pub subcommand: VmSubcommand,
}

// ─────────────────────────────────────────────────────────────────────────────
// XtaskCommand Implementation
// ─────────────────────────────────────────────────────────────────────────────

impl XtaskCommand for VmCommand {
    fn name(&self) -> &'static str {
        "vm"
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("infrastructure"),
            timeout: None, // VMs can run indefinitely
            modifies_state: true,
            track_in_history: true,
            history_access: crate::command::HistoryAccessMode::ReadWrite,
        }
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        if ctx.is_background()
            && let Some((spawn_args, coordination_args)) = self.background_coordination_plan()
        {
            return crate::coordinator::coordinate_and_spawn_with_scope(
                "vm",
                &spawn_args,
                &coordination_args,
                ctx,
            );
        }

        match &self.subcommand {
            VmSubcommand::Test {
                category,
                timeout,
                keep_failed,
                list,
                validate,
                tests,
            } => {
                execute_test(
                    category.as_deref(),
                    *timeout,
                    *keep_failed,
                    *list,
                    *validate,
                    tests,
                    ctx,
                )
                .await
            }
            VmSubcommand::Start {
                preset,
                persistent,
                snapshot,
            } => execute_start(preset, *persistent, snapshot.as_deref(), ctx),
            VmSubcommand::Ssh => execute_ssh(ctx),
            VmSubcommand::Stop => execute_stop(ctx),
            VmSubcommand::Snapshot { cmd } => Ok(execute_snapshot(cmd, ctx)),
        }
    }
}

pub(crate) fn vm_test_coordination_args(
    category: Option<&str>,
    timeout: u64,
    keep_failed: bool,
    validate: bool,
    tests: &[String],
) -> Vec<String> {
    let selection = if validate {
        "all-scenarios".to_string()
    } else if !tests.is_empty() {
        let mut sorted = tests.to_vec();
        sorted.sort();
        format!("tests:{}", sorted.join(","))
    } else if let Some(category) = category {
        format!("category:{category}")
    } else {
        "category:smoke".to_string()
    };

    if validate {
        vec![format!("--scope=vm:validate:{selection}")]
    } else {
        vec![format!(
            "--scope=vm:run:{selection}:timeout={timeout}:keep_failed={}",
            u8::from(keep_failed)
        )]
    }
}

impl VmCommand {
    fn background_coordination_plan(&self) -> Option<(Vec<String>, Vec<String>)> {
        match &self.subcommand {
            VmSubcommand::Test {
                category,
                timeout,
                keep_failed,
                list,
                validate,
                tests,
            } => {
                if *list {
                    return None;
                }

                let mut spawn_args = vec!["test".to_string()];
                if *validate {
                    spawn_args.push("--validate".to_string());
                } else {
                    if let Some(category) = category {
                        spawn_args.push("--category".to_string());
                        spawn_args.push(category.clone());
                    }
                    if *timeout != DEFAULT_TIMEOUT_SECS {
                        spawn_args.push(format!("--timeout={timeout}"));
                    }
                    if *keep_failed {
                        spawn_args.push("--keep-failed".to_string());
                    }
                    if !tests.is_empty() {
                        spawn_args.push("--".to_string());
                        spawn_args.extend(tests.iter().cloned());
                    }
                }

                let coordination_args = vm_test_coordination_args(
                    category.as_deref(),
                    *timeout,
                    *keep_failed,
                    *validate,
                    tests,
                );

                Some((spawn_args, coordination_args))
            }
            VmSubcommand::Start { .. }
            | VmSubcommand::Ssh
            | VmSubcommand::Stop
            | VmSubcommand::Snapshot { .. } => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// VM Test Execution
// ─────────────────────────────────────────────────────────────────────────────

/// Result of running a single VM test.
#[derive(Debug)]
struct VmTestResult {
    name: String,
    passed: bool,
    duration_secs: f64,
    output: String,
    timed_out: bool,
}

async fn collect_vm_stream_output<R>(
    reader: Option<BufReader<R>>,
    stream_name: &str,
) -> Result<String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let Some(reader) = reader else {
        return Ok(String::new());
    };
    let mut lines = reader.lines();
    let mut out = String::new();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                out.push_str(&line);
                out.push('\n');
            }
            Ok(None) => return Ok(out),
            Err(error) => bail!("failed to read VM {stream_name} output: {error}"),
        }
    }
}

fn append_stream_task_output(
    combined_output: &mut String,
    stream_name: &str,
    output: std::result::Result<Result<String>, tokio::task::JoinError>,
) {
    match output {
        Ok(Ok(output)) => combined_output.push_str(&output),
        Ok(Err(error)) => combined_output.push_str(&format!("{error:#}\n")),
        Err(error) => combined_output.push_str(&format!(
            "Failed to collect VM {stream_name} output: {error}\n"
        )),
    }
}

fn detect_nix_system(workspace_root: &Path) -> Result<String> {
    if let Ok(system) = std::env::var("NIX_SYSTEM")
        && !system.trim().is_empty()
    {
        return Ok(system);
    }

    let output = Command::new("nix")
        .args([
            "eval",
            "--impure",
            "--raw",
            "--expr",
            "builtins.currentSystem",
        ])
        .current_dir(workspace_root)
        .output()
        .wrap_err("Failed to detect the current Nix system")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Failed to detect the current Nix system: {stderr}");
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn available_vm_tests(workspace_root: &Path, system: &str) -> Result<Vec<String>> {
    let attr_path = format!(".#checks.{system}");
    let output = Command::new("nix")
        .args([
            "eval",
            &attr_path,
            "--apply",
            "builtins.attrNames",
            "--json",
        ])
        .current_dir(workspace_root)
        .output()
        .wrap_err("Failed to enumerate exported VM checks")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Failed to enumerate exported VM checks: {stderr}");
    }

    let exported: Vec<String> = serde_json::from_slice(&output.stdout)
        .wrap_err("Failed to parse exported VM check list")?;
    Ok(exported
        .into_iter()
        .filter_map(|name| name.strip_prefix("sinex-vm-").map(str::to_string))
        .collect())
}

#[cfg(unix)]
fn configure_process_group_leader(command: &mut tokio::process::Command) {
    crate::process::configure_managed_child_tokio(command);
}

#[cfg(not(unix))]
fn configure_process_group_leader(_command: &mut tokio::process::Command) {}

async fn terminate_vm_test_process_tree(child: &mut tokio::process::Child) -> Result<()> {
    #[cfg(unix)]
    {
        crate::process::terminate_tokio_child_process_group(child, "vm test", "vm test timeout")?;
        child
            .wait()
            .await
            .wrap_err("failed to reap timed-out VM test process")?;
        Ok(())
    }

    #[cfg(not(unix))]
    {
        child
            .kill()
            .await
            .wrap_err("failed to kill timed-out VM test process")?;
        child
            .wait()
            .await
            .wrap_err("failed to reap timed-out VM test process")?;
        Ok(())
    }
}

/// Run a single NixOS VM test via `nix build`.
///
/// Builds the canonical flake check output `.#checks.<system>.sinex-vm-{name}`.
/// Captures all output for history DB recording.
async fn run_single_vm_test(
    name: &str,
    timeout_secs: u64,
    keep_failed: bool,
    workspace_root: &std::path::Path,
    system: &str,
) -> VmTestResult {
    let start = Instant::now();

    let effective_timeout = if EXTENDED_TIMEOUT_TESTS.contains(&name) {
        timeout_secs.max(EXTENDED_TIMEOUT_SECS)
    } else {
        timeout_secs
    };

    let build_target = format!(".#checks.{system}.sinex-vm-{name}");

    let mut combined_output = String::new();
    let mut passed = false;
    let mut timed_out = false;

    let mut cmd = tokio::process::Command::new("nix");
    cmd.args(["build", &build_target, "-L"]);
    if keep_failed {
        cmd.arg("--keep-failed");
    }
    cmd.current_dir(workspace_root);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.kill_on_drop(true);
    configure_process_group_leader(&mut cmd);

    let child_result = cmd.spawn();
    let mut child = match child_result {
        Ok(c) => c,
        Err(e) => {
            combined_output.push_str(&format!("Failed to spawn nix build {build_target}: {e}\n"));
            return VmTestResult {
                name: name.to_string(),
                passed,
                duration_secs: start.elapsed().as_secs_f64(),
                output: combined_output,
                timed_out,
            };
        }
    };
    crate::process::register_tokio_child_process_group(&child, &format!("vm test {name}"));

    let stdout = child.stdout.take().map(BufReader::new);
    let stderr = child.stderr.take().map(BufReader::new);

    let stdout_task = tokio::spawn(async move { collect_vm_stream_output(stdout, "stdout").await });
    let stderr_task = tokio::spawn(async move { collect_vm_stream_output(stderr, "stderr").await });

    let timeout_duration = std::time::Duration::from_secs(effective_timeout);
    let wait_result = tokio::time::timeout(timeout_duration, child.wait()).await;

    match wait_result {
        Ok(Ok(status)) => {
            if status.success() {
                passed = true;
            }
        }
        Ok(Err(e)) => {
            combined_output.push_str(&format!("Process wait error: {e}\n"));
        }
        Err(_elapsed) => {
            timed_out = true;
            if let Err(error) = terminate_vm_test_process_tree(&mut child).await {
                combined_output.push_str(&format!("Timed-out VM test cleanup failed: {error:#}\n"));
            }
            combined_output.push_str(&format!(
                "Test {name} timed out after {effective_timeout}s\n"
            ));
        }
    }

    let (stdout_out, stderr_out) = tokio::join!(stdout_task, stderr_task);
    append_stream_task_output(&mut combined_output, "stdout", stdout_out);
    append_stream_task_output(&mut combined_output, "stderr", stderr_out);

    VmTestResult {
        name: name.to_string(),
        passed,
        duration_secs: start.elapsed().as_secs_f64(),
        output: combined_output,
        timed_out,
    }
}

/// Execute `xtask test vm`.
#[allow(
    clippy::too_many_arguments,
    clippy::fn_params_excessive_bools,
    reason = "CLI argument passthrough: each bool flag maps directly to a user-visible --flag"
)]
async fn execute_test(
    category: Option<&str>,
    timeout_secs: u64,
    keep_failed: bool,
    list: bool,
    validate: bool,
    explicit_tests: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let workspace_root = config::workspace_root();
    if !list {
        let coordination_args = vm_test_coordination_args(
            category,
            timeout_secs,
            keep_failed,
            validate,
            explicit_tests,
        );
        ctx.record_coordination_fingerprint("test", &coordination_args);
        ctx.record_invocation_args(&coordination_args);
    }

    // `--validate` checks the raw VM scenario surface directly and does not need
    // flake-export discovery or system-specific attr enumeration first.
    if validate {
        if category.is_some() || keep_failed || !explicit_tests.is_empty() {
            bail!(
                "`xtask test vm --validate` validates the full VM scenario surface and does not accept category selection, explicit tests, or --keep-failed"
            );
        }
        return execute_validate(ctx);
    }

    let discover_stage = ctx.start_stage("vm-checks-discover");
    let resolved_environment = (|| -> Result<(String, Vec<String>)> {
        let system = detect_nix_system(&workspace_root)?;
        let available_tests = available_vm_tests(&workspace_root, &system)?;
        Ok((system, available_tests))
    })();
    ctx.finish_stage(discover_stage, resolved_environment.is_ok());
    let (system, available_tests) = resolved_environment?;

    if list {
        let categories = [
            ("smoke", SMOKE_TESTS),
            ("integration", INTEGRATION_TESTS),
            ("performance", PERFORMANCE_TESTS),
            ("chaos", CHAOS_TESTS),
        ];

        println!("\n{}", style("Exported VM checks:").bold());
        println!("  System: {system}");
        for (name, tests) in categories {
            let exported: Vec<&str> = tests
                .iter()
                .copied()
                .filter(|test| available_tests.iter().any(|available| available == test))
                .collect();
            println!("\n  {}", style(name).dim());
            if exported.is_empty() {
                println!("    (no exported checks)");
            } else {
                for test in exported {
                    println!("    - {test}");
                }
            }
        }
        println!();
        println!("  Exported checks come from tests/e2e/nixos-vm/default.nix via flake `checks`.");
        println!("  Add or remove scenarios there to keep the runner surface coherent.");
        println!();
        return Ok(CommandResult::success().with_message("listed exported VM checks"));
    }

    // Resolve tests to run
    let tests_to_run: Vec<&str> = if !explicit_tests.is_empty() {
        let available: Vec<&str> = available_tests.iter().map(String::as_str).collect();
        for t in explicit_tests {
            if !available.contains(&t.as_str()) {
                bail!("VM test '{t}' is not exported by this flake's checks for system {system}.");
            }
        }
        explicit_tests.iter().map(String::as_str).collect()
    } else {
        let catalogue = match category {
            Some("smoke") => SMOKE_TESTS.to_vec(),
            Some("integration") => INTEGRATION_TESTS.to_vec(),
            Some("performance") => PERFORMANCE_TESTS.to_vec(),
            Some("chaos") => CHAOS_TESTS.to_vec(),
            Some("all") => all_tests(),
            Some(unknown) => bail!(
                "Unknown category: '{unknown}'\nValid: smoke, integration, performance, chaos, all"
            ),
            None => SMOKE_TESTS.to_vec(), // default: smoke
        };
        catalogue
            .into_iter()
            .filter(|name| available_tests.iter().any(|available| available == name))
            .collect()
    };

    if tests_to_run.is_empty() {
        println!("No tests to run (category may be empty, e.g. chaos).");
        return Ok(CommandResult::success().with_message("no tests to run"));
    }

    if ctx.is_human() {
        println!("\n{}", style("NixOS VM Tests").bold());
        println!("  Tests : {}", tests_to_run.join(", "));
        println!("  Timeout: {timeout_secs}s per test");
        println!();
    }

    let suite_start = Instant::now();
    let test_stage = ctx.start_stage("vm-test");

    // Run tests
    let results = run_sequential(
        &tests_to_run,
        timeout_secs,
        keep_failed,
        &workspace_root,
        &system,
        ctx,
    )
    .await;

    let suite_duration = suite_start.elapsed().as_secs_f64();
    let passed: Vec<&VmTestResult> = results.iter().filter(|r| r.passed).collect();
    let failed: Vec<&VmTestResult> = results.iter().filter(|r| !r.passed).collect();
    ctx.finish_stage(test_stage, failed.is_empty());

    // Print summary
    println!("\n{}", style("VM Test Summary").bold());
    for r in &results {
        let status = if r.timed_out {
            style("TIMEOUT").yellow().to_string()
        } else if r.passed {
            style("PASS").green().to_string()
        } else {
            style("FAIL").red().to_string()
        };
        println!("  [{status}] {} ({:.1}s)", r.name, r.duration_secs);
    }
    println!();
    println!(
        "  {}/{} passed in {suite_duration:.1}s",
        passed.len(),
        results.len()
    );
    println!();

    // Record per-scenario results to the existing `xtask test` invocation.
    if let Some(inv_id) = ctx.invocation_id() {
        for r in &results {
            let status = if r.timed_out {
                "timeout"
            } else if r.passed {
                TestStatus::Pass.as_str()
            } else {
                TestStatus::Fail.as_str()
            };
            let output = if r.output.is_empty() {
                None
            } else {
                Some(r.output.as_str())
            };
            ctx.with_history_db(|db| {
                db.record_test_result(inv_id, &r.name, "vm", status, r.duration_secs, output, "vm")
            });
        }
    }

    if failed.is_empty() {
        Ok(CommandResult::success()
            .with_message(format!(
                "{}/{} VM tests passed",
                passed.len(),
                results.len()
            ))
            .with_duration(ctx.elapsed()))
    } else {
        let names: Vec<&str> = failed.iter().map(|r| r.name.as_str()).collect();
        bail!(
            "{}/{} VM tests failed: {}",
            failed.len(),
            results.len(),
            names.join(", ")
        )
    }
}

/// Run tests sequentially, printing progress.
async fn run_sequential(
    tests: &[&str],
    timeout_secs: u64,
    keep_failed: bool,
    workspace_root: &std::path::Path,
    system: &str,
    ctx: &CommandContext,
) -> Vec<VmTestResult> {
    let mut results = Vec::with_capacity(tests.len());
    let total = tests.len() as i64;
    ctx.report_progress("vm-test", None, Some(0.0), Some(0), Some(total));

    for (index, &name) in tests.iter().enumerate() {
        let completed = index as i64;
        let current_pct = if total == 0 {
            100.0
        } else {
            completed as f64 / total as f64 * 100.0
        };
        ctx.report_progress(
            "vm-test",
            Some(name),
            Some(current_pct),
            Some(completed),
            Some(total),
        );
        print!("  Running {name}… ");
        let scenario_stage = ctx.start_stage(&format!("vm-test:{name}"));
        let r = run_single_vm_test(name, timeout_secs, keep_failed, workspace_root, system).await;
        ctx.finish_stage(scenario_stage, r.passed);
        if r.passed {
            println!("{} ({:.1}s)", style("PASS").green(), r.duration_secs);
        } else if r.timed_out {
            println!("{} ({:.1}s)", style("TIMEOUT").yellow(), r.duration_secs);
        } else {
            println!("{} ({:.1}s)", style("FAIL").red(), r.duration_secs);
        }
        results.push(r);
        let completed = results.len() as i64;
        let pct = if total == 0 {
            100.0
        } else {
            completed as f64 / total as f64 * 100.0
        };
        ctx.report_progress(
            "vm-test",
            Some(name),
            Some(pct),
            Some(completed),
            Some(total),
        );
    }
    results
}

const VM_VALIDATION_DUMMY_PACKAGE_EXPR: &str =
    r#"(import <nixpkgs> {}).runCommand "dummy" {} "mkdir -p $out""#;

fn run_vm_validation_probe(
    target: &Path,
    workspace_root: &Path,
) -> std::io::Result<std::process::Output> {
    Command::new("nix-instantiate")
        .arg(target)
        .args([
            "--arg",
            "pkgs",
            "import <nixpkgs> {}",
            "--arg",
            "sinex-ingestd",
            VM_VALIDATION_DUMMY_PACKAGE_EXPR,
            "--arg",
            "sinex-gateway",
            VM_VALIDATION_DUMMY_PACKAGE_EXPR,
            "--arg",
            "sinex",
            VM_VALIDATION_DUMMY_PACKAGE_EXPR,
            "--arg",
            "sinexCli",
            VM_VALIDATION_DUMMY_PACKAGE_EXPR,
            "--arg",
            "pg_jsonschema",
            VM_VALIDATION_DUMMY_PACKAGE_EXPR,
            "--arg",
            "xtask",
            VM_VALIDATION_DUMMY_PACKAGE_EXPR,
            "--arg",
            "sinexVmTestSuite",
            VM_VALIDATION_DUMMY_PACKAGE_EXPR,
        ])
        .current_dir(workspace_root)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
}

fn first_stderr_detail(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("nix-instantiate failed")
        .to_owned()
}

fn validate_vm_test_file(file: &Path, workspace_root: &Path, ctx: &CommandContext) -> (bool, bool) {
    match run_vm_validation_probe(file, workspace_root) {
        Ok(output) if output.status.success() => {
            if ctx.is_human() {
                println!(
                    "  {} OK: {}",
                    style("✓").green(),
                    display_vm_test_label(file)
                );
            }
            (true, false)
        }
        Ok(output) => {
            if ctx.is_human() {
                println!(
                    "  {} Syntax error: {} ({})",
                    style("✗").red(),
                    file.display(),
                    first_stderr_detail(&output)
                );
            }
            (false, false)
        }
        Err(error) => {
            if ctx.is_human() {
                println!(
                    "  {} Probe failure: {} ({error})",
                    style("⚠").yellow(),
                    file.display()
                );
            }
            (false, true)
        }
    }
}

/// Validate nix syntax of VM test scenario files.
fn execute_validate(ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("vm validate");

    let workspace_root = config::workspace_root();
    let discover_stage = ctx.start_stage("vm-scenarios-discover");
    let test_files = discover_vm_test_files(&workspace_root);
    ctx.finish_stage(discover_stage, test_files.is_ok());
    let test_files = test_files?;

    let mut valid = 0usize;
    let mut missing = 0usize;
    let mut failed = 0usize;
    let mut probe_failures = 0usize;
    let total = test_files.len() as i64;
    let validate_stage = ctx.start_stage("vm-validate");
    ctx.report_progress("vm-validate", None, Some(0.0), Some(0), Some(total));

    let mut existing_files = Vec::with_capacity(test_files.len());
    for file in &test_files {
        let label = display_vm_test_label(file);
        if file.exists() {
            existing_files.push(file.as_path());
            continue;
        }
        missing += 1;
        if ctx.is_human() {
            println!("  {} Missing: {}", style("⚠").yellow(), file.display());
        }
        let completed = (valid + missing + failed + probe_failures) as i64;
        let current_pct = if total == 0 {
            100.0
        } else {
            completed as f64 / total as f64 * 100.0
        };
        ctx.report_progress(
            "vm-validate",
            Some(&label),
            Some(current_pct),
            Some(completed),
            Some(total),
        );
    }

    let batch_stage = ctx.start_stage("vm-validate-batch");
    let catalog_file = workspace_root.join("tests/e2e/nixos-vm/default.nix");
    let batch_result = if existing_files.is_empty() {
        None
    } else {
        ctx.report_progress(
            "vm-validate",
            Some("all-scenarios"),
            Some(missing as f64 / total.max(1) as f64 * 100.0),
            Some(missing as i64),
            Some(total),
        );
        Some(run_vm_validation_probe(&catalog_file, &workspace_root))
    };

    let needs_diagnosis = match batch_result {
        None => {
            ctx.finish_stage(batch_stage, true);
            false
        }
        Some(Ok(output)) if output.status.success() => {
            ctx.finish_stage(batch_stage, true);
            valid += existing_files.len();
            if ctx.is_human() {
                println!(
                    "  {} Batch OK: {} existing VM scenarios",
                    style("✓").green(),
                    existing_files.len()
                );
            }
            ctx.report_progress(
                "vm-validate",
                Some("all-scenarios"),
                Some(100.0),
                Some(total),
                Some(total),
            );
            false
        }
        Some(Ok(output)) => {
            ctx.finish_stage(batch_stage, false);
            if ctx.is_human() {
                println!(
                    "  {} Batch validation failed; isolating scenario failures ({})",
                    style("⚠").yellow(),
                    first_stderr_detail(&output)
                );
            }
            true
        }
        Some(Err(error)) => {
            ctx.finish_stage(batch_stage, false);
            if ctx.is_human() {
                println!(
                    "  {} Batch validation probe failed; isolating scenario failures ({error})",
                    style("⚠").yellow()
                );
            }
            true
        }
    };

    if needs_diagnosis {
        let diagnose_stage = ctx.start_stage("vm-validate-diagnose");
        for (index, file) in existing_files.iter().enumerate() {
            let checked = (missing + index) as i64;
            let current_pct = if total == 0 {
                100.0
            } else {
                checked as f64 / total as f64 * 100.0
            };
            let label = display_vm_test_label(file);
            ctx.report_progress(
                "vm-validate",
                Some(&label),
                Some(current_pct),
                Some(checked),
                Some(total),
            );

            let (file_valid, probe_failed) = validate_vm_test_file(file, &workspace_root, ctx);
            if file_valid {
                valid += 1;
            } else if probe_failed {
                probe_failures += 1;
            } else {
                failed += 1;
            }

            let completed = (missing + index + 1) as i64;
            let pct = if total == 0 {
                100.0
            } else {
                completed as f64 / total as f64 * 100.0
            };
            ctx.report_progress(
                "vm-validate",
                Some(&label),
                Some(pct),
                Some(completed),
                Some(total),
            );
        }
        ctx.finish_stage(diagnose_stage, failed == 0 && probe_failures == 0);
    }

    let validate_ok = failed == 0 && probe_failures == 0;
    ctx.finish_stage(validate_stage, validate_ok);

    if ctx.is_human() {
        println!();
        println!(
            "  {valid} valid, {missing} missing, {failed} failed, {probe_failures} probe failures",
        );
        if failed == 0 && probe_failures == 0 {
            println!("  VM test infrastructure is ready.");
        }
    }

    if !validate_ok {
        bail!(
            "{failed} test file(s) have syntax errors; {probe_failures} validation probe(s) failed"
        );
    }

    Ok(CommandResult::success()
        .with_message(format!("validated {valid} files ({missing} missing)")))
}

fn display_vm_test_label(file: &Path) -> String {
    file.file_name().map_or_else(
        || file.display().to_string(),
        |name| name.to_string_lossy().into_owned(),
    )
}

fn discover_vm_test_files(workspace_root: &Path) -> Result<Vec<PathBuf>> {
    let scenarios_dir = workspace_root.join("tests/e2e/nixos-vm/test-scenarios");
    let mut test_files =
        vec![workspace_root.join("tests/e2e/nixos-vm/preflight_deployment_test.nix")];

    let mut discovered = Vec::new();
    for entry in std::fs::read_dir(&scenarios_dir).wrap_err_with(|| {
        format!(
            "failed to read VM scenarios directory {}",
            scenarios_dir.display()
        )
    })? {
        let entry = entry.wrap_err_with(|| {
            format!(
                "failed to enumerate an entry in VM scenarios directory {}",
                scenarios_dir.display()
            )
        })?;
        if entry.path().extension().and_then(|s| s.to_str()) == Some("nix") {
            discovered.push(entry.path());
        }
    }

    discovered.sort();
    test_files.extend(discovered);
    Ok(test_files)
}

// ─────────────────────────────────────────────────────────────────────────────
// VM Lifecycle Commands (unchanged from original)
// ─────────────────────────────────────────────────────────────────────────────

fn execute_start(
    preset: &str,
    persistent: bool,
    snapshot: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("vm start");

    let valid_presets = ["minimal", "standard", "full"];
    if !valid_presets.contains(&preset) {
        bail!(
            "Invalid preset '{}'. Valid options: {}",
            preset,
            valid_presets.join(", ")
        );
    }

    if ctx.is_human() {
        println!("Starting VM...");
        println!("  Preset: {preset}");
        println!("  Persistent: {persistent}");
        if let Some(snap) = snapshot {
            println!("  Snapshot: {snap}");
        }
        println!();
    }

    let _ = persistent;
    let _ = snapshot;

    bail!(
        "Interactive VM presets are not exported from the flake yet. Finish this by wiring runnable `config.system.build.vm` outputs for presets {} and mapping them here; today the public VM surface is still the exported flake-check suite behind `xtask test vm`.",
        valid_presets.join(", ")
    )
}

fn execute_ssh(ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("vm ssh");

    let ssh_port = std::env::var("SINEX_VM_SSH_PORT").unwrap_or_else(|_| "2222".to_string());

    if ctx.is_human() {
        println!("Connecting to VM via SSH on port {ssh_port}...");
    }

    let mut command = Command::new("ssh");
    command.args([
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-p",
        &ssh_port,
        "root@localhost",
    ]);
    let status = crate::process::run_managed_foreground_std_command(&mut command, "vm ssh")
        .context("Failed to connect via SSH")?;

    if crate::process::status_indicates_clean_interactive_shutdown(&status) {
        Ok(CommandResult::success().with_message("SSH session ended"))
    } else {
        bail!("SSH connection failed")
    }
}

fn execute_stop(ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("vm stop");

    if ctx.is_human() {
        println!("Stopping VM...");
    }

    let output = Command::new("pgrep")
        .args(["-f", "qemu.*sinex"])
        .output()
        .context("Failed to find VM process")?;

    if output.stdout.is_empty() {
        return Ok(CommandResult::success().with_message("No VM running"));
    }

    let pids: Vec<&str> = std::str::from_utf8(&output.stdout)?
        .trim()
        .lines()
        .collect();

    for pid in &pids {
        if ctx.is_human() {
            println!("Sending SIGTERM to PID {pid}...");
        }
        let pid = pid
            .parse::<u32>()
            .with_context(|| format!("invalid VM pid reported by pgrep: {pid}"))?;
        crate::process::terminate_process_group_by_leader_pid(pid, "vm stop", "manual VM stop")
            .with_context(|| format!("Failed to stop VM process group rooted at {pid}"))?;
    }

    Ok(CommandResult::success().with_message(format!("Stopped {} VM process(es)", pids.len())))
}

fn execute_snapshot(cmd: &VmSnapshotSubcommand, ctx: &CommandContext) -> CommandResult {
    match cmd {
        VmSnapshotSubcommand::Create { name } => {
            ctx.heading("vm snapshot create");
            if ctx.is_human() {
                println!("Creating VM snapshot '{name}'...");
                println!();
                println!("Note: VM snapshots require QEMU monitor access.");
                println!("Use Ctrl+A C in the VM console, then: savevm {name}");
            }
            CommandResult::success()
                .with_message(format!("Snapshot '{name}' created (manual step required)"))
        }
        VmSnapshotSubcommand::Restore { name } => {
            ctx.heading("vm snapshot restore");
            if ctx.is_human() {
                println!("To restore snapshot '{name}':");
                println!("  xtask vm start --snapshot {name}");
            }
            CommandResult::success()
                .with_message(format!("Use 'vm start --snapshot {name}' to restore"))
        }
        VmSnapshotSubcommand::List => {
            ctx.heading("vm snapshot list");
            let state_dir = config::workspace_root().join(".vm-state");
            if !state_dir.exists() {
                return CommandResult::success()
                    .with_message("No VM state directory found (no snapshots)");
            }
            if ctx.is_human() {
                println!("VM state directory: {}", state_dir.display());
                println!();
                println!("Note: Snapshot listing requires QEMU monitor access.");
                println!("Use Ctrl+A C in the VM console, then: info snapshots");
            }
            CommandResult::success().with_message("VM snapshot info (manual step required)")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandContext;
    use crate::output::{OutputFormat, OutputWriter};
    use crate::sandbox::sinex_test;

    fn silent_ctx() -> CommandContext {
        CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, None, "vm")
    }

    #[sinex_test]
    async fn test_background_coordination_plan_for_validate_normalizes_to_single_lane()
    -> ::xtask::sandbox::TestResult<()> {
        let command = VmCommand {
            subcommand: VmSubcommand::Test {
                category: Some("integration".to_string()),
                timeout: DEFAULT_TIMEOUT_SECS,
                keep_failed: false,
                list: false,
                validate: true,
                tests: vec!["node-matrix".to_string()],
            },
        };

        let (spawn_args, coordination_args) = command
            .background_coordination_plan()
            .expect("validate should coordinate in background");

        assert_eq!(
            spawn_args,
            vec!["test".to_string(), "--validate".to_string()]
        );
        assert_eq!(
            coordination_args,
            vec!["--scope=vm:validate:all-scenarios".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_background_coordination_plan_for_run_tracks_selected_tests()
    -> ::xtask::sandbox::TestResult<()> {
        let command = VmCommand {
            subcommand: VmSubcommand::Test {
                category: None,
                timeout: 1337,
                keep_failed: true,
                list: false,
                validate: false,
                tests: vec!["replay-smoke".to_string(), "basic".to_string()],
            },
        };

        let (spawn_args, coordination_args) = command
            .background_coordination_plan()
            .expect("vm runs should coordinate in background");

        assert_eq!(
            spawn_args,
            vec![
                "test".to_string(),
                "--timeout=1337".to_string(),
                "--keep-failed".to_string(),
                "--".to_string(),
                "replay-smoke".to_string(),
                "basic".to_string(),
            ]
        );
        assert_eq!(
            coordination_args,
            vec!["--scope=vm:run:tests:basic,replay-smoke:timeout=1337:keep_failed=1".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_validate_rejects_run_only_flags() -> ::xtask::sandbox::TestResult<()> {
        let error = execute_test(
            Some("smoke"),
            DEFAULT_TIMEOUT_SECS,
            true,
            false,
            true,
            &["basic".to_string()],
            &silent_ctx(),
        )
        .await
        .expect_err("validate should reject run-only selection flags");

        let message = format!("{error:#}");
        assert!(message.contains("does not accept category selection"));
        Ok(())
    }

    #[sinex_test]
    async fn test_discover_vm_test_files_reports_scenarios_dir_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let scenarios_dir = temp.path().join("tests/e2e/nixos-vm/test-scenarios");
        std::fs::create_dir_all(scenarios_dir.parent().unwrap())?;
        std::fs::write(&scenarios_dir, "not a directory")?;

        let error = discover_vm_test_files(temp.path()).unwrap_err();
        assert!(format!("{error:#}").contains("failed to read VM scenarios directory"));
        Ok(())
    }

    #[sinex_test]
    async fn test_discover_vm_test_files_includes_preflight_and_sorted_scenarios()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let vm_root = temp.path().join("tests/e2e/nixos-vm");
        let scenarios_dir = vm_root.join("test-scenarios");
        std::fs::create_dir_all(&scenarios_dir)?;
        std::fs::write(vm_root.join("preflight_deployment_test.nix"), "")?;
        std::fs::write(scenarios_dir.join("b-test.nix"), "")?;
        std::fs::write(scenarios_dir.join("a-test.nix"), "")?;
        std::fs::write(scenarios_dir.join("notes.txt"), "")?;

        let files = discover_vm_test_files(temp.path())?;
        let labels: Vec<_> = files
            .iter()
            .map(|path| {
                path.strip_prefix(temp.path())
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();

        assert_eq!(
            labels,
            vec![
                "tests/e2e/nixos-vm/preflight_deployment_test.nix",
                "tests/e2e/nixos-vm/test-scenarios/a-test.nix",
                "tests/e2e/nixos-vm/test-scenarios/b-test.nix",
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_display_vm_test_label_falls_back_to_full_path() -> ::xtask::sandbox::TestResult<()>
    {
        let root = Path::new("/");
        assert_eq!(display_vm_test_label(root), root.display().to_string());
        Ok(())
    }

    #[sinex_test]
    async fn test_append_stream_task_output_surfaces_join_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let mut combined_output = String::new();
        let handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            Result::<String>::Ok(String::from("unreachable"))
        });
        handle.abort();

        append_stream_task_output(&mut combined_output, "stdout", handle.await);

        assert!(combined_output.contains("Failed to collect VM stdout output"));
        assert!(combined_output.contains("cancelled"));
        Ok(())
    }

    #[sinex_test]
    async fn test_collect_vm_stream_output_collects_utf8_lines() -> ::xtask::sandbox::TestResult<()>
    {
        use tokio::io::AsyncWriteExt;

        let (reader, mut writer) = tokio::io::duplex(64);
        writer.write_all(b"alpha\nbeta\n").await?;
        drop(writer);

        let output = collect_vm_stream_output(Some(BufReader::new(reader)), "stdout").await?;
        assert_eq!(output, "alpha\nbeta\n");
        Ok(())
    }

    #[sinex_test]
    async fn test_collect_vm_stream_output_surfaces_invalid_utf8()
    -> ::xtask::sandbox::TestResult<()> {
        use tokio::io::AsyncWriteExt;

        let (reader, mut writer) = tokio::io::duplex(64);
        writer.write_all(&[0xff, b'\n']).await?;
        drop(writer);

        let error = collect_vm_stream_output(Some(BufReader::new(reader)), "stderr")
            .await
            .expect_err("invalid utf8 should surface");
        let message = format!("{error:#}");
        assert!(message.contains("failed to read VM stderr output"));
        assert!(message.contains("valid UTF-8"));
        Ok(())
    }

    #[sinex_test]
    async fn test_append_stream_task_output_surfaces_stream_errors()
    -> ::xtask::sandbox::TestResult<()> {
        let mut combined_output = String::new();
        append_stream_task_output(
            &mut combined_output,
            "stderr",
            Ok(Err(color_eyre::eyre::eyre!("stream exploded"))),
        );

        assert!(combined_output.contains("stream exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn test_terminate_vm_test_process_tree_kills_child_process_group()
    -> ::xtask::sandbox::TestResult<()> {
        use std::os::unix::process::ExitStatusExt;

        let mut command = tokio::process::Command::new("sh");
        command.args(["-c", "sleep 30 & echo $!; wait"]);
        command.stdout(Stdio::piped());
        command.stderr(Stdio::null());
        configure_process_group_leader(&mut command);

        let mut child = command.spawn()?;
        let stdout = child.stdout.take().expect("stdout should be piped");
        let mut lines = BufReader::new(stdout).lines();
        let sleep_pid = lines
            .next_line()
            .await?
            .expect("shell should print background child pid")
            .parse::<i32>()?;

        terminate_vm_test_process_tree(&mut child).await?;

        assert!(
            child.try_wait()?.is_some(),
            "terminated VM helper child should be reaped"
        );
        assert_ne!(
            unsafe { libc::kill(sleep_pid, 0) },
            0,
            "background process in the VM helper group should be gone"
        );

        let status = child.wait().await?;
        assert!(
            status.signal().is_some() || !status.success(),
            "terminated child should not report clean success"
        );
        Ok(())
    }
}
