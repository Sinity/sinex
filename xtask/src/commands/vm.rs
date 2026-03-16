//! VM commands - NixOS VM management for integration testing.
//!
//! This module provides commands for managing NixOS VMs used in
//! end-to-end testing of the sinex infrastructure.
//!
//! ## NixOS Compatibility Gate (Q4)
//!
//! VM tests ARE the NixOS compatibility enforcement mechanism.
//! They import real NixOS modules and exercise actual deployment paths.
//!
//! Fast compatibility gate (5-10min):
//!   `xtask test --vm --category smoke`
//!
//! Full suite (integration + performance, 30-90min):
//!   `xtask test --vm --category all`

use color_eyre::eyre::{Result, WrapErr, bail};
use console::style;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config;
use crate::history::TestStatus;
use crate::history::InvocationStatus;

// ─────────────────────────────────────────────────────────────────────────────
// Test Catalogue
// ─────────────────────────────────────────────────────────────────────────────

const SMOKE_TESTS: &[&str] = &["basic"];
const INTEGRATION_TESTS: &[&str] = &[
    "preflight",
    "maintenance",
    "satellite-matrix",
    "multi-source",
    "failure-recovery",
];
const PERFORMANCE_TESTS: &[&str] = &["performance"];
/// Chaos tests are intentionally empty — pending the new failure-injection harness.
const CHAOS_TESTS: &[&str] = &[];

/// Default timeout per test in seconds (15 minutes).
pub const DEFAULT_TIMEOUT_SECS: u64 = 900;
/// Extended timeout for slow tests (maintenance, performance): 30 minutes.
const EXTENDED_TIMEOUT_SECS: u64 = 1800;

/// Tests that require the extended timeout.
const EXTENDED_TIMEOUT_TESTS: &[&str] = &["maintenance", "performance"];

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
    /// Run NixOS VM tests natively (replaces run-vm-tests.sh) (Q2)
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
        /// VM preset: minimal, standard, full
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
// VM Test Execution (Q2: native Rust, no bash script)
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

/// Run a single NixOS VM test via `nix build`.
///
/// Tries `.#sinex-vm-{name}` first, falls back to `.#checks.x86_64-linux.sinex-vm-{name}`.
/// Captures all output for history DB recording (Q3).
async fn run_single_vm_test(
    name: &str,
    timeout_secs: u64,
    keep_failed: bool,
    workspace_root: &std::path::Path,
) -> VmTestResult {
    let start = Instant::now();

    let effective_timeout = if EXTENDED_TIMEOUT_TESTS.contains(&name) {
        timeout_secs.max(EXTENDED_TIMEOUT_SECS)
    } else {
        timeout_secs
    };

    let build_targets = [
        format!(".#sinex-vm-{name}"),
        format!(".#checks.x86_64-linux.sinex-vm-{name}"),
    ];

    let mut combined_output = String::new();
    let mut passed = false;
    let mut timed_out = false;

    for target in &build_targets {
        let mut cmd = tokio::process::Command::new("nix");
        cmd.args(["build", target, "-L"]);
        if keep_failed {
            cmd.arg("--keep-failed");
        }
        cmd.current_dir(workspace_root);
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let child_result = cmd.spawn();
        let mut child = match child_result {
            Ok(c) => c,
            Err(e) => {
                combined_output.push_str(&format!("Failed to spawn nix build {target}: {e}\n"));
                continue;
            }
        };

        // Stream output while collecting it
        let stdout = child.stdout.take().map(BufReader::new);
        let stderr = child.stderr.take().map(BufReader::new);

        let stdout_task = tokio::spawn(async move {
            let mut out = String::new();
            if let Some(reader) = stdout {
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    out.push_str(&line);
                    out.push('\n');
                }
            }
            out
        });
        let stderr_task = tokio::spawn(async move {
            let mut out = String::new();
            if let Some(reader) = stderr {
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    out.push_str(&line);
                    out.push('\n');
                }
            }
            out
        });

        let timeout_duration = std::time::Duration::from_secs(effective_timeout);
        let wait_result = tokio::time::timeout(timeout_duration, child.wait()).await;

        let (stdout_out, stderr_out) = tokio::join!(stdout_task, stderr_task);
        combined_output.push_str(&stdout_out.unwrap_or_default());
        combined_output.push_str(&stderr_out.unwrap_or_default());

        match wait_result {
            Ok(Ok(status)) => {
                if status.success() {
                    passed = true;
                    break;
                }
                // Non-zero exit: try the fallback target
            }
            Ok(Err(e)) => {
                combined_output.push_str(&format!("Process wait error: {e}\n"));
            }
            Err(_elapsed) => {
                timed_out = true;
                combined_output.push_str(&format!(
                    "Test {name} timed out after {effective_timeout}s\n"
                ));
                break;
            }
        }
    }

    VmTestResult {
        name: name.to_string(),
        passed,
        duration_secs: start.elapsed().as_secs_f64(),
        output: combined_output,
        timed_out,
    }
}

/// Execute `xtask infra vm test` — the native Rust test runner (Q2).
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
    if list {
        println!("\n{}", style("Available VM tests:").bold());
        println!(
            "\n  {} (smoke)",
            style("Smoke (fast NixOS compatibility gate):").dim()
        );
        for t in SMOKE_TESTS {
            println!("    - {t}");
        }
        println!("\n  {}", style("Integration:").dim());
        for t in INTEGRATION_TESTS {
            println!("    - {t}");
        }
        println!("\n  {}", style("Performance:").dim());
        for t in PERFORMANCE_TESTS {
            println!("    - {t}");
        }
        println!("\n  {}", style("Chaos:").dim());
        if CHAOS_TESTS.is_empty() {
            println!("    (pending failure-injection harness)");
        } else {
            for t in CHAOS_TESTS {
                println!("    - {t}");
            }
        }
        println!();
        return Ok(CommandResult::success().with_message("listed VM tests"));
    }

    // --validate: check nix syntax of test scenario files
    if validate {
        return execute_validate(ctx);
    }

    let workspace_root = config::workspace_root();

    // Resolve tests to run
    let tests_to_run: Vec<&str> = if !explicit_tests.is_empty() {
        let all = all_tests();
        for t in explicit_tests {
            if !all.contains(&t.as_str()) {
                bail!(
                    "Unknown VM test: '{t}'\nRun `xtask infra vm test --list` to see available tests."
                );
            }
        }
        explicit_tests.iter().map(String::as_str).collect()
    } else {
        match category {
            Some("smoke") => SMOKE_TESTS.to_vec(),
            Some("integration") => INTEGRATION_TESTS.to_vec(),
            Some("performance") => PERFORMANCE_TESTS.to_vec(),
            Some("chaos") => CHAOS_TESTS.to_vec(),
            Some("all") => all_tests(),
            Some(unknown) => bail!(
                "Unknown category: '{unknown}'\nValid: smoke, integration, performance, chaos, all"
            ),
            None => SMOKE_TESTS.to_vec(), // default: smoke
        }
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

    // Open history DB for recording (Q3)
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
        run_parallel(&tests_to_run, timeout_secs, keep_failed, &workspace_root).await
    } else {
        run_sequential(&tests_to_run, timeout_secs, keep_failed, &workspace_root).await
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

    // Record results to history DB (Q3)
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
        ctx.with_history_db(|db| db.finish_invocation(inv_id, final_status, Some(exit_code), suite_duration));
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
) -> Vec<VmTestResult> {
    let mut results = Vec::with_capacity(tests.len());
    for &name in tests {
        print!("  Running {name}… ");
        let r = run_single_vm_test(name, timeout_secs, keep_failed, workspace_root).await;
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
) -> Vec<VmTestResult> {
    let workspace_root = workspace_root.to_path_buf();
    let mut handles = Vec::with_capacity(tests.len());

    for &name in tests {
        let name = name.to_string();
        let wr = workspace_root.clone();
        let handle =
            tokio::spawn(
                async move { run_single_vm_test(&name, timeout_secs, keep_failed, &wr).await },
            );
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
    let scenarios_dir = workspace_root.join("tests/e2e/nixos-vm/test-scenarios");

    let test_files = [
        workspace_root.join("tests/e2e/nixos-vm/test-scenarios/basic-flow.nix"),
        workspace_root.join("tests/e2e/nixos-vm/preflight_deployment_test.nix"),
        workspace_root.join("tests/e2e/nixos-vm/test-scenarios/maintenance.nix"),
        workspace_root.join("tests/e2e/nixos-vm/test-scenarios/satellite-matrix.nix"),
        workspace_root.join("tests/e2e/nixos-vm/test-scenarios/multi-source.nix"),
        workspace_root.join("tests/e2e/nixos-vm/test-scenarios/performance.nix"),
    ];

    let dummy_pkg = r#"(import <nixpkgs> {}).runCommand "dummy" {} "mkdir -p $out""#;

    let mut valid = 0usize;
    let mut missing = 0usize;
    let mut failed = 0usize;

    for file in &test_files {
        if !file.exists() {
            if ctx.is_human() {
                println!("  {} Missing: {}", style("⚠").yellow(), file.display());
            }
            missing += 1;
            continue;
        }

        let status = Command::new("nix-instantiate")
            .args([
                file.to_str().unwrap_or_default(),
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
            ])
            .current_dir(&workspace_root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        match status {
            Ok(s) if s.success() => {
                if ctx.is_human() {
                    println!(
                        "  {} OK: {}",
                        style("✓").green(),
                        file.file_name().unwrap_or_default().to_string_lossy()
                    );
                }
                valid += 1;
            }
            _ => {
                if ctx.is_human() {
                    println!("  {} Syntax error: {}", style("✗").red(), file.display());
                }
                failed += 1;
            }
        }
    }

    drop(scenarios_dir); // suppress unused warning

    if ctx.is_human() {
        println!();
        println!("  {} valid, {} missing, {} failed", valid, missing, failed);
        if failed == 0 {
            println!("  VM test infrastructure is ready.");
        }
    }

    if failed > 0 {
        bail!("{failed} test file(s) have syntax errors");
    }

    Ok(CommandResult::success()
        .with_message(format!("validated {valid} files ({missing} missing)")))
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

    let workspace_root = config::workspace_root();

    if ctx.is_human() {
        println!("Starting VM...");
        println!("  Preset: {preset}");
        println!("  Persistent: {persistent}");
        if let Some(snap) = snapshot {
            println!("  Snapshot: {snap}");
        }
        println!();
    }

    let flake_output = format!(".#sinex-vm-{preset}");

    if ctx.is_human() {
        println!("Building VM: nix build {flake_output}");
    }

    let build_status = Command::new("nix")
        .args(["build", &flake_output, "--no-link", "--print-out-paths"])
        .current_dir(&workspace_root)
        .stdout(Stdio::piped())
        .status()
        .context("Failed to build VM")?;

    if !build_status.success() {
        bail!("Failed to build VM with preset: {preset}");
    }

    let output = Command::new("nix")
        .args(["build", &flake_output, "--no-link", "--print-out-paths"])
        .current_dir(&workspace_root)
        .output()
        .context("Failed to get VM path")?;

    let vm_path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let run_script = PathBuf::from(&vm_path).join("bin/run-sinex-vm");

    if !run_script.exists() {
        let alt_script = PathBuf::from(&vm_path).join("bin").join("run-nixos-vm");
        if alt_script.exists() {
            return run_vm(&alt_script, persistent, snapshot, ctx);
        }
        bail!(
            "VM run script not found at {} or alternate locations",
            run_script.display()
        );
    }

    run_vm(&run_script, persistent, snapshot, ctx)
}

fn run_vm(
    script: &PathBuf,
    persistent: bool,
    snapshot: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut cmd = Command::new(script);

    if persistent {
        let state_dir = config::workspace_root().join(".vm-state");
        std::fs::create_dir_all(&state_dir)?;
        cmd.env(
            "QEMU_OPTS",
            format!("-drive file={}/vm.qcow2,if=virtio", state_dir.display()),
        );
    }

    if let Some(snap) = snapshot {
        cmd.arg("-loadvm").arg(snap);
    }

    if ctx.is_human() {
        println!("Starting VM (press Ctrl+A X to exit)...");
    }

    let status = cmd.status().context("Failed to start VM")?;

    if status.success() {
        Ok(CommandResult::success().with_message("VM exited normally"))
    } else {
        bail!("VM exited with error: {:?}", status.code())
    }
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
