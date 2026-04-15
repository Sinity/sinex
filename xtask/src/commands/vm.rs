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

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use console::style;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config;
use crate::history::InvocationStatus;
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
/// Extended timeout for slow tests (maintenance, performance): 30 minutes.
const EXTENDED_TIMEOUT_SECS: u64 = 1800;

/// Tests that require the extended timeout.
const EXTENDED_TIMEOUT_TESTS: &[&str] = &[
    "maintenance",
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
        /// Run tests in parallel
        #[arg(long)]
        parallel: bool,
        /// Timeout per test in seconds (default: 900, maintenance/performance: 1800)
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
        match &self.subcommand {
            VmSubcommand::Test {
                category,
                parallel,
                timeout,
                keep_failed,
                list,
                validate,
                tests,
            } => {
                execute_test(
                    category.as_deref(),
                    *parallel,
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
    if let Ok(system) = std::env::var("NIX_SYSTEM") {
        if !system.trim().is_empty() {
            return Ok(system);
        }
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
        .filter_map(|name| {
            name.strip_prefix("sinex-vm-")
                .map(|short| short.to_string())
        })
        .collect())
}

#[cfg(unix)]
fn configure_process_group_leader(command: &mut tokio::process::Command) {
    // SAFETY: `setpgid(0, 0)` is async-signal-safe per POSIX and runs in the child
    // between fork and exec so the spawned process becomes the leader of its own group.
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group_leader(_command: &mut tokio::process::Command) {}

async fn terminate_vm_test_process_tree(child: &mut tokio::process::Child) -> Result<()> {
    #[cfg(unix)]
    {
        let pid = nix::unistd::Pid::from_raw(
            child
                .id()
                .ok_or_else(|| eyre!("failed to terminate timed-out VM build: child PID missing"))?
                as i32,
        );

        match nix::sys::signal::killpg(pid, nix::sys::signal::Signal::SIGTERM) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(error) => {
                return Err(eyre!(
                    "failed to send SIGTERM to timed-out VM test process group: {error}"
                ));
            }
        }

        let grace_deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        loop {
            if child
                .try_wait()
                .wrap_err("failed to poll timed-out VM test after SIGTERM")?
                .is_some()
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= grace_deadline {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        match nix::sys::signal::killpg(pid, nix::sys::signal::Signal::SIGKILL) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(error) => {
                return Err(eyre!(
                    "failed to send SIGKILL to timed-out VM test process group: {error}"
                ));
            }
        }

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
#[allow(clippy::too_many_arguments)]
async fn execute_test(
    category: Option<&str>,
    parallel: bool,
    timeout_secs: u64,
    keep_failed: bool,
    list: bool,
    validate: bool,
    explicit_tests: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // --list: show available tests and exit
    let workspace_root = config::workspace_root();
    let system = detect_nix_system(&workspace_root)?;
    let available_tests = available_vm_tests(&workspace_root, &system)?;

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

    // --validate: check nix syntax of test scenario files
    if validate {
        return execute_validate(ctx);
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
        println!("  Parallel: {parallel}");
        println!();
    }

    // Open history DB for recording.
    let invocation_id = ctx.with_history_db(|db| {
        let args = serde_json::json!({
            "category": category,
            "parallel": parallel,
            "tests": tests_to_run,
        });
        db.start_invocation("test", Some("vm"), None, Some(&args.to_string()))
    });

    let suite_start = Instant::now();

    // Run tests
    let results: Vec<VmTestResult> = if parallel && tests_to_run.len() > 1 {
        run_parallel(
            &tests_to_run,
            timeout_secs,
            keep_failed,
            &workspace_root,
            &system,
        )
        .await
    } else {
        run_sequential(
            &tests_to_run,
            timeout_secs,
            keep_failed,
            &workspace_root,
            &system,
        )
        .await
    };

    let suite_duration = suite_start.elapsed().as_secs_f64();
    let passed: Vec<&VmTestResult> = results.iter().filter(|r| r.passed).collect();
    let failed: Vec<&VmTestResult> = results.iter().filter(|r| !r.passed).collect();

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

    // Record results to history DB.
    if let Some(inv_id) = invocation_id {
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
        let final_status = if failed.is_empty() {
            InvocationStatus::Success
        } else {
            InvocationStatus::Failed
        };
        let exit_code = if failed.is_empty() { 0 } else { 1 };
        ctx.with_history_db(|db| {
            db.finish_invocation(inv_id, final_status, Some(exit_code), suite_duration)
        });
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
) -> Vec<VmTestResult> {
    let mut results = Vec::with_capacity(tests.len());
    for &name in tests {
        print!("  Running {name}… ");
        let r = run_single_vm_test(name, timeout_secs, keep_failed, workspace_root, system).await;
        if r.passed {
            println!("{} ({:.1}s)", style("PASS").green(), r.duration_secs);
        } else if r.timed_out {
            println!("{} ({:.1}s)", style("TIMEOUT").yellow(), r.duration_secs);
        } else {
            println!("{} ({:.1}s)", style("FAIL").red(), r.duration_secs);
        }
        results.push(r);
    }
    results
}

/// Run tests in parallel using tokio tasks.
async fn run_parallel(
    tests: &[&str],
    timeout_secs: u64,
    keep_failed: bool,
    workspace_root: &std::path::Path,
    system: &str,
) -> Vec<VmTestResult> {
    let workspace_root = workspace_root.to_path_buf();
    let mut handles = Vec::with_capacity(tests.len());

    for &name in tests {
        let name = name.to_string();
        let wr = workspace_root.clone();
        let system = system.to_string();
        let handle = tokio::spawn(async move {
            run_single_vm_test(&name, timeout_secs, keep_failed, &wr, &system).await
        });
        handles.push(handle);
    }

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(r) => results.push(r),
            Err(e) => results.push(VmTestResult {
                name: "unknown".to_string(),
                passed: false,
                duration_secs: 0.0,
                output: format!("Task panicked: {e}"),
                timed_out: false,
            }),
        }
    }
    results
}

/// Validate nix syntax of VM test scenario files.
fn execute_validate(ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("vm validate");

    let workspace_root = config::workspace_root();
    let test_files = discover_vm_test_files(&workspace_root)?;

    let dummy_pkg = r#"(import <nixpkgs> {}).runCommand "dummy" {} "mkdir -p $out""#;

    let mut valid = 0usize;
    let mut missing = 0usize;
    let mut failed = 0usize;
    let mut probe_failures = 0usize;

    for file in &test_files {
        if !file.exists() {
            if ctx.is_human() {
                println!("  {} Missing: {}", style("⚠").yellow(), file.display());
            }
            missing += 1;
            continue;
        }

        let output = Command::new("nix-instantiate")
            .arg(file)
            .args([
                "--arg",
                "pkgs",
                "import <nixpkgs> {}",
                "--arg",
                "lib",
                "(import <nixpkgs> {}).lib",
                "--arg",
                "sinex-ingestd",
                dummy_pkg,
                "--arg",
                "sinex-gateway",
                dummy_pkg,
                "--arg",
                "sinex",
                dummy_pkg,
                "--arg",
                "sinexCli",
                dummy_pkg,
                "--arg",
                "pg_jsonschema",
                dummy_pkg,
                "--arg",
                "xtask",
                dummy_pkg,
                "--arg",
                "sinexVmTestSuite",
                dummy_pkg,
            ])
            .current_dir(&workspace_root)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output();

        match output {
            Ok(output) if output.status.success() => {
                if ctx.is_human() {
                    println!(
                        "  {} OK: {}",
                        style("✓").green(),
                        display_vm_test_label(file)
                    );
                }
                valid += 1;
            }
            Ok(output) => {
                if ctx.is_human() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    let detail = stderr
                        .lines()
                        .map(str::trim)
                        .find(|line| !line.is_empty())
                        .unwrap_or("nix-instantiate failed");
                    println!(
                        "  {} Syntax error: {} ({detail})",
                        style("✗").red(),
                        file.display()
                    );
                }
                failed += 1;
            }
            Err(error) => {
                if ctx.is_human() {
                    println!(
                        "  {} Probe failure: {} ({error})",
                        style("⚠").yellow(),
                        file.display()
                    );
                }
                probe_failures += 1;
            }
        }
    }

    if ctx.is_human() {
        println!();
        println!(
            "  {} valid, {} missing, {} failed, {} probe failures",
            valid, missing, failed, probe_failures
        );
        if failed == 0 && probe_failures == 0 {
            println!("  VM test infrastructure is ready.");
        }
    }

    if failed > 0 || probe_failures > 0 {
        bail!(
            "{failed} test file(s) have syntax errors; {probe_failures} validation probe(s) failed"
        );
    }

    Ok(CommandResult::success()
        .with_message(format!("validated {valid} files ({missing} missing)")))
}

fn display_vm_test_label(file: &Path) -> String {
    file.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| file.display().to_string())
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

    let status = Command::new("ssh")
        .args([
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-p",
            &ssh_port,
            "root@localhost",
        ])
        .status()
        .context("Failed to connect via SSH")?;

    if status.success() {
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
        Command::new("kill")
            .args(["-TERM", pid])
            .status()
            .context("Failed to stop VM")?;
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
    use crate::sandbox::sinex_test;

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
