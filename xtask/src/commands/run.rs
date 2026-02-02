//! Run command - Binary lifecycle management
//!
//! Provides unified command to run sinex binaries with:
//! - Process spawning with instance ID tracking
//! - `--watch` mode for development with seamless handoff
//! - `--bg` support via jobs system
//! - `--tether` mode for connecting to production NATS
//! - Bundle shortcuts (stack, all-nodes)

use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::jobs::JobManager;
use crate::preflight;

/// Known binary targets and their package names
static BINARIES: &[(&str, &str, &str)] = &[
    // (name, package, binary name)
    ("ingestd", "sinex-ingestd", "sinex-ingestd"),
    ("gateway", "sinex-gateway", "sinex-gateway"),
    // Ingestors
    ("fs-ingestor", "sinex-fs-ingestor", "sinex-fs-ingestor"),
    (
        "terminal-ingestor",
        "sinex-terminal-ingestor",
        "sinex-terminal-ingestor",
    ),
    (
        "desktop-ingestor",
        "sinex-desktop-ingestor",
        "sinex-desktop-ingestor",
    ),
    (
        "system-ingestor",
        "sinex-system-ingestor",
        "sinex-system-ingestor",
    ),
    (
        "document-ingestor",
        "sinex-document-ingestor",
        "sinex-document-ingestor",
    ),
    // Automatons
    (
        "analytics-automaton",
        "sinex-analytics-automaton",
        "sinex-analytics-automaton",
    ),
    (
        "search-automaton",
        "sinex-search-automaton",
        "sinex-search-automaton",
    ),
    (
        "pkm-automaton",
        "sinex-pkm-automaton",
        "sinex-pkm-automaton",
    ),
    (
        "content-automaton",
        "sinex-content-automaton",
        "sinex-content-automaton",
    ),
    (
        "health-automaton",
        "sinex-health-automaton",
        "sinex-health-automaton",
    ),
    // Processors
    (
        "terminal-canonicalizer",
        "sinex-terminal-command-canonicalizer",
        "sinex-terminal-command-canonicalizer",
    ),
];

/// Run subcommand variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum RunSubcommand {
    /// Run sinex-ingestd
    Ingestd {
        /// Instance ID for multi-instance coordination
        #[arg(long)]
        instance_id: Option<String>,
    },
    /// Run sinex-gateway
    Gateway {
        /// Instance ID for multi-instance coordination
        #[arg(long)]
        instance_id: Option<String>,
    },
    /// Run a specific node by name
    Node {
        /// Node name (e.g., fs-ingestor, analytics-automaton)
        name: String,
        /// Instance ID for multi-instance coordination
        #[arg(long)]
        instance_id: Option<String>,
    },
    /// Run core services bundle (ingestd + gateway)
    Stack {
        /// Instance ID prefix
        #[arg(long)]
        instance_id: Option<String>,
    },
    /// Run all ingestors
    AllIngestors {
        /// Instance ID prefix
        #[arg(long)]
        instance_id: Option<String>,
    },
    /// Run all automatons
    AllAutomatons {
        /// Instance ID prefix
        #[arg(long)]
        instance_id: Option<String>,
    },
    /// List available binaries
    List,
    /// Connect to a remote environment via The Tether
    ///
    /// The Tether creates a shadow consumer on the target environment's gateway
    /// and streams events to stdout. Shadow consumers use fan-out delivery and
    /// don't affect production consumers.
    ///
    /// # Environment Variables
    ///
    /// - SINEX_GATEWAY_URL or SINEX_{TARGET}_GATEWAY_URL: Gateway RPC URL
    /// - SINEX_RPC_TOKEN or SINEX_{TARGET}_RPC_TOKEN: RPC auth token (required)
    /// - SINEX_TETHER_NATS_URL or SINEX_{TARGET}_NATS_URL: Production NATS URL
    /// - SINEX_TETHER_NATS_*: NATS TLS config (CA_CERT, CLIENT_CERT, CLIENT_KEY, CREDS)
    ///
    /// Note: Requires the `sandbox` feature to be enabled.
    #[cfg(feature = "sandbox")]
    Tether {
        /// Target environment (e.g., "prod", "staging")
        target: String,

        /// Subject filter for events (default: events.>)
        #[arg(long, default_value = "events.>")]
        filter: String,

        /// Start from the beginning of the stream
        #[arg(long)]
        from_beginning: bool,

        /// Start from a specific sequence number
        #[arg(long)]
        from_sequence: Option<u64>,
    },
}

/// Run command for binary lifecycle management
#[derive(Debug, Clone, clap::Args)]
pub struct RunCommand {
    #[command(subcommand)]
    pub subcommand: RunSubcommand,

    /// Watch mode: rebuild and restart on source changes
    #[arg(long, global = true)]
    pub watch: bool,

    /// Run in background (returns job ID immediately)
    #[arg(long, global = true)]
    pub bg: bool,

    /// Build in release mode
    #[arg(long, global = true)]
    pub release: bool,
}

/// Result of running a binary
#[derive(Debug, Serialize)]
#[allow(dead_code)] // Used for future JSON output
struct RunResult {
    binary: String,
    pid: Option<u32>,
    instance_id: Option<String>,
    status: String,
}

impl XtaskCommand for RunCommand {
    fn name(&self) -> &'static str {
        "run"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            RunSubcommand::List => execute_list(ctx),
            RunSubcommand::Ingestd { instance_id } => {
                self.run_binary("ingestd", instance_id.clone(), ctx)
            }
            RunSubcommand::Gateway { instance_id } => {
                self.run_binary("gateway", instance_id.clone(), ctx)
            }
            RunSubcommand::Node { name, instance_id } => {
                self.run_binary(name, instance_id.clone(), ctx)
            }
            RunSubcommand::Stack { instance_id } => {
                self.run_bundle(&["ingestd", "gateway"], instance_id.clone(), ctx)
            }
            RunSubcommand::AllIngestors { instance_id } => {
                let ingestors: Vec<&str> = BINARIES
                    .iter()
                    .filter(|(name, _, _)| name.contains("ingestor"))
                    .map(|(name, _, _)| *name)
                    .collect();
                self.run_bundle(&ingestors, instance_id.clone(), ctx)
            }
            RunSubcommand::AllAutomatons { instance_id } => {
                let automatons: Vec<&str> = BINARIES
                    .iter()
                    .filter(|(name, _, _)| name.contains("automaton"))
                    .map(|(name, _, _)| *name)
                    .collect();
                self.run_bundle(&automatons, instance_id.clone(), ctx)
            }
            #[cfg(feature = "sandbox")]
            RunSubcommand::Tether {
                target,
                filter,
                from_beginning,
                from_sequence,
            } => execute_tether(ctx, target, filter, *from_beginning, *from_sequence),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

impl RunCommand {
    fn run_binary(
        &self,
        name: &str,
        instance_id: Option<String>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        // Find binary info
        let (_, package, binary) =
            BINARIES
                .iter()
                .find(|(n, _, _)| *n == name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                    "Unknown binary '{name}'. Use 'cargo xtask run list' to see available binaries."
                )
                })?;

        // Ensure infrastructure is ready (binaries need DB + NATS)
        preflight::ensure_ready(ctx)?;

        let instance_id = instance_id.unwrap_or_else(|| format!("{}-{}", name, std::process::id()));

        if self.bg {
            return self.run_background(package, binary, &instance_id, ctx);
        }

        if self.watch {
            return self.run_watch(package, binary, &instance_id, ctx);
        }

        // Direct run
        self.run_direct(package, binary, &instance_id, ctx)
    }

    fn run_bundle(
        &self,
        binaries: &[&str],
        instance_prefix: Option<String>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        // Ensure infrastructure is ready (binaries need DB + NATS)
        preflight::ensure_ready(ctx)?;

        if self.bg {
            // Background mode: spawn all as separate jobs
            let cfg = config();
            let manager = JobManager::new(cfg.jobs_dir())?;
            let mut job_ids = Vec::new();

            for name in binaries {
                let (_, package, _binary) = BINARIES
                    .iter()
                    .find(|(n, _, _)| n == name)
                    .ok_or_else(|| anyhow::anyhow!("Unknown binary: {name}"))?;

                let instance_id = instance_prefix.as_ref().map_or_else(
                    || format!("{}-{}", name, std::process::id()),
                    |p| format!("{p}-{name}"),
                );

                let mut args = vec!["run".to_string(), "-p".to_string(), package.to_string()];

                if self.release {
                    args.push("--release".to_string());
                }

                args.extend(["--".to_string(), format!("--instance-id={instance_id}")]);

                let job = manager.spawn("cargo", &args)?;
                job_ids.push(job.id);
            }

            return Ok(CommandResult::success()
                .with_message(format!("Started {} binaries in background", binaries.len()))
                .with_data(serde_json::json!({
                    "binaries": binaries,
                    "job_ids": job_ids,
                })));
        }

        // Foreground bundle: spawn all, wait for any exit
        if ctx.is_human() {
            println!("Starting {} binaries...", binaries.len());
        }

        let mut children: HashMap<String, Child> = HashMap::new();

        // Build all first
        for name in binaries {
            let (_, package, _) = BINARIES
                .iter()
                .find(|(n, _, _)| n == name)
                .ok_or_else(|| anyhow::anyhow!("Unknown binary: {name}"))?;

            if ctx.is_human() {
                println!("Building {name}...");
            }

            let mut build_cmd = Command::new("cargo");
            build_cmd.arg("build").arg("-p").arg(package);
            if self.release {
                build_cmd.arg("--release");
            }

            let status = build_cmd.status()?;
            if !status.success() {
                bail!("Failed to build {name}");
            }
        }

        // Start all
        for name in binaries {
            let (_, _package, binary) = BINARIES
                .iter()
                .find(|(n, _, _)| n == name)
                .ok_or_else(|| anyhow::anyhow!("Unknown binary: {name}"))?;

            let instance_id = instance_prefix.as_ref().map_or_else(
                || format!("{}-{}", name, std::process::id()),
                |p| format!("{p}-{name}"),
            );

            let target_dir = if self.release { "release" } else { "debug" };
            let binary_path = PathBuf::from(format!("target/{target_dir}/{binary}"));

            if ctx.is_human() {
                println!("Starting {name} (instance: {instance_id})...");
            }

            let child = Command::new(&binary_path)
                .arg(format!("--instance-id={instance_id}"))
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .with_context(|| format!("Failed to spawn {name}"))?;

            children.insert(name.to_string(), child);
        }

        if ctx.is_human() {
            println!(
                "\n{} binaries running. Press Ctrl+C to stop.\n",
                children.len()
            );
        }

        // Wait for any child to exit, then stop all
        let mut exited_name: Option<String> = None;
        loop {
            for (name, child) in &mut children {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        if ctx.is_human() {
                            println!("{name} exited with status: {status}");
                        }
                        exited_name = Some(name.clone());
                        break;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        if ctx.is_human() {
                            eprintln!("Error checking {name}: {e}");
                        }
                    }
                }
            }
            if exited_name.is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }

        // Kill remaining children
        if ctx.is_human() {
            println!("\nShutting down remaining processes...");
        }
        for (name, mut child) in children {
            if Some(&name) != exited_name.as_ref() {
                if let Err(e) = child.kill() {
                    if ctx.is_human() {
                        eprintln!("Warning: couldn't kill {name}: {e}");
                    }
                }
                let _ = child.wait();
            }
        }

        Ok(CommandResult::success()
            .with_message(format!(
                "Bundle stopped (triggered by {})",
                exited_name.unwrap_or_else(|| "unknown".to_string())
            ))
            .with_duration(ctx.elapsed()))
    }

    fn run_direct(
        &self,
        package: &str,
        _binary: &str,
        instance_id: &str,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        if ctx.is_human() {
            println!("Building {package}...");
        }

        let mut args = vec!["run".to_string(), "-p".to_string(), package.to_string()];

        if self.release {
            args.push("--release".to_string());
        }

        args.extend(["--".to_string(), format!("--instance-id={instance_id}")]);

        let status = Command::new("cargo")
            .args(&args)
            .status()
            .with_context(|| format!("Failed to run {package}"))?;

        if status.success() {
            Ok(CommandResult::success()
                .with_message(format!("{package} exited successfully"))
                .with_duration(ctx.elapsed()))
        } else {
            Ok(CommandResult::failure(crate::output::StructuredError {
                code: "RUN_FAILED".to_string(),
                message: format!("{package} exited with error"),
                location: None,
                suggestion: None,
            })
            .with_duration(ctx.elapsed()))
        }
    }

    fn run_background(
        &self,
        package: &str,
        _binary: &str,
        instance_id: &str,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        let cfg = config();
        let manager = JobManager::new(cfg.jobs_dir())?;

        let mut args = vec!["run".to_string(), "-p".to_string(), package.to_string()];

        if self.release {
            args.push("--release".to_string());
        }

        args.extend(["--".to_string(), format!("--instance-id={instance_id}")]);

        let job = manager.spawn("cargo", &args)?;

        Ok(CommandResult::success()
            .with_message(format!("Backgrounded {} as job {}", package, job.id))
            .with_data(serde_json::json!({
                "job_id": job.id,
                "package": package,
                "instance_id": instance_id,
            }))
            .with_duration(ctx.elapsed()))
    }

    fn run_watch(
        &self,
        package: &str,
        binary: &str,
        instance_id: &str,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        if ctx.is_human() {
            println!("Watch mode: {package} (instance: {instance_id})");
            println!("Press Ctrl+C to stop.\n");
        }

        // Use cargo-watch if available
        let watch_check = Command::new("which")
            .arg("cargo-watch")
            .output()
            .ok()
            .filter(|o| o.status.success());

        if watch_check.is_some() {
            // Use cargo-watch
            let args = vec![
                "watch".to_string(),
                "-x".to_string(),
                format!(
                    "run -p {} {} -- --instance-id={}",
                    package,
                    if self.release { "--release" } else { "" },
                    instance_id
                ),
            ];

            let status = Command::new("cargo")
                .args(&args)
                .status()
                .context("cargo-watch failed")?;

            if status.success() {
                return Ok(CommandResult::success()
                    .with_message("Watch mode ended")
                    .with_duration(ctx.elapsed()));
            }
        }

        // Fallback: simple run without watch (cargo-watch not available)
        if ctx.is_human() {
            println!("cargo-watch not found. Running without watch mode...");
            println!("Install with: cargo install cargo-watch");
        }

        // Just do a direct run
        self.run_direct(package, binary, instance_id, ctx)
    }
}

fn execute_list(ctx: &CommandContext) -> Result<CommandResult> {
    let mut binaries: Vec<serde_json::Value> = Vec::new();

    if ctx.is_human() {
        println!("Available binaries:\n");
        println!("Core Services:");
        for (name, package, _) in BINARIES.iter().take(2) {
            println!("  {name:<25} ({package})");
        }

        println!("\nIngestors:");
        for (name, package, _) in BINARIES.iter().filter(|(n, _, _)| n.contains("ingestor")) {
            println!("  {name:<25} ({package})");
        }

        println!("\nAutomatons:");
        for (name, package, _) in BINARIES.iter().filter(|(n, _, _)| n.contains("automaton")) {
            println!("  {name:<25} ({package})");
        }

        println!("\nProcessors:");
        for (name, package, _) in BINARIES
            .iter()
            .filter(|(n, _, _)| n.contains("canonicalizer"))
        {
            println!("  {name:<25} ({package})");
        }

        println!("\nBundles:");
        println!("  {:<25} ingestd + gateway", "stack");
        println!("  {:<25} all *-ingestor binaries", "all-ingestors");
        println!("  {:<25} all *-automaton binaries", "all-automatons");

        #[cfg(feature = "sandbox")]
        {
            println!("\nSpecial:");
            println!(
                "  {:<25} Connect to remote NATS via The Tether",
                "tether <target>"
            );
        }
    }

    for (name, package, binary) in BINARIES {
        binaries.push(serde_json::json!({
            "name": name,
            "package": package,
            "binary": binary,
        }));
    }

    #[cfg(feature = "sandbox")]
    let special = vec!["tether"];
    #[cfg(not(feature = "sandbox"))]
    let special: Vec<&str> = vec![];

    Ok(CommandResult::success()
        .with_data(serde_json::json!({
            "binaries": binaries,
            "bundles": ["stack", "all-ingestors", "all-automatons"],
            "special": special
        }))
        .with_duration(ctx.elapsed()))
}

/// Execute the tether command
#[cfg(feature = "sandbox")]
fn execute_tether(
    ctx: &CommandContext,
    target: &str,
    filter: &str,
    from_beginning: bool,
    from_sequence: Option<u64>,
) -> Result<CommandResult> {
    use crate::sandbox::tether::{TetherConfig, TetherSession};

    // Build a runtime for the tether
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        // Build config from environment, then override with CLI args
        let mut config = TetherConfig::from_env(target)?;
        config.subject_filter = if filter.is_empty() {
            None
        } else {
            Some(filter.to_string())
        };
        config.from_beginning = from_beginning;
        // Note: from_sequence not yet supported in TetherConfig
        let _ = from_sequence;

        if ctx.is_human() {
            println!("Connecting to {} via The Tether...", target);
            println!("  Gateway: {}", config.gateway_url);
            if let Some(ref f) = config.subject_filter {
                println!("  Filter: {}", f);
            }
            if from_beginning {
                println!("  Starting from: beginning of stream");
            } else {
                println!("  Starting from: new events only");
            }
            println!();
        }

        // Start the session
        let mut session = TetherSession::start(config).await?;

        if ctx.is_human() {
            if let Some(info) = session.consumer_info() {
                println!(
                    "Connected! Consumer: {}, Stream: {}",
                    info.consumer_name, info.stream_name
                );
            }
        }

        // TODO: Implement actual event streaming
        // The streaming functionality is not yet complete.
        // For now, just verify connection and clean up.
        if ctx.is_human() {
            println!("\nNote: Event streaming not yet implemented. Connection verified.");
        }

        // Cleanup
        session.cleanup().await;

        Ok(CommandResult::success()
            .with_message("Tether connection verified (streaming not yet implemented)")
            .with_data(serde_json::json!({
                "target": target,
                "status": "connection_verified",
            }))
            .with_duration(ctx.elapsed()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_lookup() {
        // All binaries should be findable
        for (name, package, _) in BINARIES {
            let found = BINARIES.iter().find(|(n, _, _)| n == name);
            assert!(found.is_some(), "Binary {name} not found");
            assert_eq!(found.unwrap().1, *package);
        }
    }

    #[test]
    fn test_ingestor_filter() {
        let ingestors: Vec<_> = BINARIES
            .iter()
            .filter(|(name, _, _)| name.contains("ingestor"))
            .collect();
        assert!(!ingestors.is_empty());
        for (name, _, _) in ingestors {
            assert!(name.contains("ingestor"));
        }
    }

    #[test]
    fn test_automaton_filter() {
        let automatons: Vec<_> = BINARIES
            .iter()
            .filter(|(name, _, _)| name.contains("automaton"))
            .collect();
        assert!(!automatons.is_empty());
        for (name, _, _) in automatons {
            assert!(name.contains("automaton"));
        }
    }
}
