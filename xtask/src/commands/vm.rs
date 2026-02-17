//! VM commands - NixOS VM management for integration testing.
//!
//! This module provides commands for managing NixOS VMs used in
//! end-to-end testing of the sinex infrastructure.

use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config;

// ─────────────────────────────────────────────────────────────────────────────
// Command Definitions
// ─────────────────────────────────────────────────────────────────────────────

/// VM management command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum VmSubcommand {
    /// Run VM tests (wraps run-vm-tests.sh)
    Test {
        /// Test category: smoke, integration, performance, chaos
        #[arg(long, short)]
        category: Option<String>,
        /// Run tests in parallel
        #[arg(long)]
        parallel: bool,
        /// Timeout per test in seconds
        timeout: u64,
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

#[async_trait::async_trait]
impl XtaskCommand for VmCommand {
    fn name(&self) -> &'static str {
        "vm"
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("infrastructure".to_string()),
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
                tests,
            } => execute_test(category.as_deref(), *parallel, *timeout, tests, ctx),
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
// Command Implementations
// ─────────────────────────────────────────────────────────────────────────────

fn execute_test(
    category: Option<&str>,
    parallel: bool,
    timeout: u64,
    tests: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("vm test");

    let workspace_root = config::workspace_root();
    let script = workspace_root.join("tests/e2e/nixos-vm/run-vm-tests.sh");

    if !script.exists() {
        bail!(
            "VM test script not found at: {}\nMake sure you're running from the workspace root.",
            script.display()
        );
    }

    let mut cmd = Command::new(&script);

    if let Some(cat) = category {
        cmd.args(["-c", cat]);
    }

    if parallel {
        cmd.arg("--parallel");
    }

    cmd.args(["--timeout", &timeout.to_string()]);

    if !tests.is_empty() {
        cmd.arg("--").args(tests);
    }

    if ctx.is_human() {
        println!("Running VM tests...");
        if let Some(cat) = category {
            println!("  Category: {cat}");
        }
        println!("  Parallel: {parallel}");
        println!("  Timeout: {timeout}s");
        if !tests.is_empty() {
            println!("  Tests: {}", tests.join(", "));
        }
        println!();
    }

    let status = cmd.status().context("Failed to run VM test script")?;

    if status.success() {
        Ok(CommandResult::success().with_message("VM tests passed"))
    } else {
        bail!("VM tests failed with exit code: {:?}", status.code())
    }
}

fn execute_start(
    preset: &str,
    persistent: bool,
    snapshot: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("vm start");

    // Validate preset
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

    // Build the VM using nix
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

    // Get the built VM path
    let output = Command::new("nix")
        .args(["build", &flake_output, "--no-link", "--print-out-paths"])
        .current_dir(&workspace_root)
        .output()
        .context("Failed to get VM path")?;

    let vm_path = String::from_utf8_lossy(&output.stdout).trim().to_string();

    // Determine the run script location
    let run_script = PathBuf::from(&vm_path).join("bin/run-sinex-vm");

    if !run_script.exists() {
        // Try alternate location
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

    // Set up state directory if persistent
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

    // Default SSH port for NixOS VM testing
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

    // Try to find QEMU process
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
