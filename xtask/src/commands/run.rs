//! Run command - Binary lifecycle management
//!
//! Provides unified command to run sinex binaries with:
//! - Process spawning with instance ID tracking
//! - `--watch` mode for development with seamless handoff
//! - `--bg` support via jobs system
//! - `--tether` mode for connecting to production NATS
//! - Bundle shortcuts (core, all-nodes)
//! - `--logs` mode: interleaved color-coded output from all bundle processes

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use console::style;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::jobs::JobManager;
use crate::orchestrator::{DevOrchestrator, RunArgs};
use crate::preflight;

/// Check if a package/name refers to a node (ingestor, automaton, or canonicalizer).
/// Nodes support --instance-id flag; core services (ingestd, gateway) don't.
fn is_node_package(name: &str) -> bool {
    name.contains("ingestor") || name.contains("automaton") || name.contains("canonicalizer")
}

/// Build a deterministic instance ID from a binary name and optional prefix.
fn make_instance_id(name: &str, prefix: Option<&str>) -> String {
    prefix.map_or_else(
        || format!("{}-{}", name, std::process::id()),
        |p| format!("{p}-{name}"),
    )
}

/// Append `--instance-id` (for nodes) or `rpc-server` (for gateway) to cargo args.
fn append_binary_extra_args(args: &mut Vec<String>, package: &str, instance_id: &str) {
    if is_node_package(package) {
        args.extend(["--".to_string(), format!("--instance-id={instance_id}")]);
    } else if package.contains("gateway") {
        args.extend(["--".to_string(), "rpc-server".to_string()]);
    }
}

/// P2: Spawn async tasks that prefix each process's stdout/stderr lines with its name.
///
/// Colors cycle through a fixed palette so each process has a distinct color.
/// Uses `tokio::task::spawn` (detached) — tasks terminate when their streams close
/// (i.e. when the child exits), keeping the parent loop unblocked.
fn spawn_log_prefixers(streams: Vec<(String, Option<ChildStdout>, Option<ChildStderr>)>) {
    // Color cycle: cyan, yellow, magenta, blue, green (wraps for >5 processes)
    let colors: &[fn(&str) -> console::StyledObject<String>] = &[
        |s| style(s.to_string()).cyan(),
        |s| style(s.to_string()).yellow(),
        |s| style(s.to_string()).magenta(),
        |s| style(s.to_string()).blue(),
        |s| style(s.to_string()).green(),
    ];

    for (idx, (name, stdout, stderr)) in streams.into_iter().enumerate() {
        let color = colors[idx % colors.len()];
        let prefix_colored = color(&format!("[{name}]")).to_string();

        if let Some(stdout) = stdout {
            let prefix = prefix_colored.clone();
            tokio::task::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    println!("{prefix} {line}");
                }
            });
        }

        if let Some(stderr) = stderr {
            let prefix = prefix_colored.clone();
            tokio::task::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("{prefix} {line}");
                }
            });
        }
    }
}

/// Poll children until one exits, returning its name.
///
/// Returns `None` after 8 hours (D6 fix) — callers treat None as "kill everything",
/// so a timeout causes a clean shutdown rather than an infinite poll.
///
/// X5: Signal propagation note — for foreground children spawned with `kill_on_drop(true)`
/// (X2 fix), Ctrl+C reaches the children directly via the terminal process group (SIGINT
/// is broadcast to all processes sharing the controlling terminal). This function does
/// not need to intercept SIGINT to forward it. A tokio::signal handler would add full
/// programmatic control but is deferred as low-priority since terminal delivery is correct
/// for the common interactive case.
async fn wait_for_any_child_exit(
    children: &mut HashMap<String, Child>,
    ctx: &CommandContext,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8 * 60 * 60);
    loop {
        if tokio::time::Instant::now() >= deadline {
            if ctx.is_human() {
                eprintln!("[run] 8-hour timeout reached — shutting down");
            }
            return None;
        }
        for (name, child) in children.iter_mut() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if ctx.is_human() {
                        println!("{name} exited with status: {status}");
                    }
                    return Some(name.clone());
                }
                Ok(None) => {}
                Err(e) => {
                    if ctx.is_human() {
                        eprintln!("Error checking {name}: {e}");
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

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
    Core {
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
    /// - `SINEX_GATEWAY_URL` or SINEX_{TARGET}_`GATEWAY_URL`: Gateway RPC URL
    /// - `SINEX_RPC_TOKEN` or SINEX_{TARGET}_`RPC_TOKEN`: RPC auth token (required)
    /// - `SINEX_TETHER_NATS_URL` or SINEX_{TARGET}_`NATS_URL`: Production NATS URL
    /// - `SINEX_TETHER_NATS`_*: NATS TLS config (`CA_CERT`, `CLIENT_CERT`, `CLIENT_KEY`, CREDS)
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

    /// Build in release mode
    #[arg(long, global = true)]
    pub release: bool,

    /// Print command without executing
    #[arg(long, global = true)]
    pub dry_run: bool,

    /// Interleave color-coded logs from all bundle processes on stdout (P2)
    ///
    /// Each process's stdout/stderr is prefixed with `[name] ` in a distinct color.
    /// Implies foreground mode; incompatible with --bg.
    #[arg(long, global = true)]
    pub logs: bool,

    /// Show periodic runtime metrics overlay (heartbeat, lag, batch latency)
    #[arg(long, global = true)]
    pub metrics: bool,
}

/// Result of running a binary
#[derive(Debug, Serialize)]
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

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Guard: xtask run invokes `cargo build` before starting binaries, which needs the
        // cargo target/ lock. If nextest is running, that lock is held and we'd deadlock.
        if std::env::var("NEXTEST_RUN_ID").is_ok() {
            return Err(color_eyre::eyre::eyre!(
                "Cannot run `xtask run` inside an active nextest run — \
                 cargo build needs the cargo target/ lock which nextest holds.\n\
                 Use `xtask run --bg ...` to spawn in background instead."
            ));
        }

        match &self.subcommand {
            RunSubcommand::List => Ok(execute_list(ctx)),
            RunSubcommand::Ingestd { instance_id } => {
                self.run_binary("ingestd", instance_id.clone(), ctx).await
            }
            RunSubcommand::Gateway { instance_id } => {
                self.run_binary("gateway", instance_id.clone(), ctx).await
            }
            RunSubcommand::Node { name, instance_id } => {
                self.run_binary(name, instance_id.clone(), ctx).await
            }
            RunSubcommand::Core { instance_id } => {
                self.run_bundle(&["ingestd", "gateway"], instance_id.clone(), ctx)
                    .await
            }
            RunSubcommand::AllIngestors { instance_id } => {
                let ingestors: Vec<&str> = BINARIES
                    .iter()
                    .filter(|(name, _, _)| name.contains("ingestor"))
                    .map(|(name, _, _)| *name)
                    .collect();
                self.run_bundle(&ingestors, instance_id.clone(), ctx).await
            }
            RunSubcommand::AllAutomatons { instance_id } => {
                let automatons: Vec<&str> = BINARIES
                    .iter()
                    .filter(|(name, _, _)| name.contains("automaton"))
                    .map(|(name, _, _)| *name)
                    .collect();
                self.run_bundle(&automatons, instance_id.clone(), ctx).await
            }
            RunSubcommand::Tether {
                target,
                filter,
                from_beginning,
                from_sequence,
            } => execute_tether(ctx, target, filter, *from_beginning, *from_sequence).await,
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

impl RunCommand {
    async fn run_binary(
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
                    eyre!(
                        "Unknown binary '{name}'. Use 'xtask run list' to see available binaries."
                    )
                })?;

        // Ensure infrastructure is ready (binaries need DB + NATS)
        preflight::ensure_ready(ctx)?;

        let instance_id = instance_id.unwrap_or_else(|| format!("{}-{}", name, std::process::id()));

        if ctx.is_background() {
            return self.run_background(package, binary, &instance_id, ctx);
        }

        if self.dry_run {
            println!("Would run: {name} (package: {package}, instance: {instance_id})");
            if self.watch {
                println!("  (with --watch)");
            }
            return Ok(CommandResult::success().with_detail("dry-run passed"));
        }

        if self.watch {
            return self.run_watch(package, binary, &instance_id, ctx).await;
        }

        // Direct run
        self.run_direct(package, binary, &instance_id, ctx).await
    }

    async fn run_bundle(
        &self,
        binaries: &[&str],
        instance_prefix: Option<String>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        // Ensure infrastructure is ready (binaries need DB + NATS)
        if !self.dry_run {
            preflight::ensure_ready(ctx)?;
        }

        if self.dry_run {
            println!("Would run bundle: {binaries:?}");
            if ctx.is_background() {
                println!("  (background mode via JobManager)");
            }
            return Ok(CommandResult::success().with_detail("dry-run passed"));
        }

        if ctx.is_background() {
            return self.run_bundle_background(binaries, instance_prefix.as_deref(), ctx);
        }

        self.run_bundle_foreground(binaries, instance_prefix.as_deref(), ctx)
            .await
    }

    fn run_bundle_background(
        &self,
        binaries: &[&str],
        instance_prefix: Option<&str>,
        _ctx: &CommandContext,
    ) -> Result<CommandResult> {
        let cfg = config();
        let manager = JobManager::new(cfg.jobs_dir())?;
        let mut job_ids = Vec::new();

        for name in binaries {
            let (_, package, _binary) = BINARIES
                .iter()
                .find(|(n, _, _)| n == name)
                .ok_or_else(|| eyre!("Unknown binary: {name}"))?;

            let instance_id = make_instance_id(name, instance_prefix);
            let mut args = vec!["run".to_string(), "-p".to_string(), package.to_string()];
            if self.release {
                args.push("--release".to_string());
            }
            append_binary_extra_args(&mut args, package, &instance_id);

            let job = manager.spawn("cargo", &args)?;
            job_ids.push(job.id);
        }

        Ok(CommandResult::success()
            .with_message(format!("Started {} binaries in background", binaries.len()))
            .with_data(serde_json::json!({
                "binaries": binaries,
                "job_ids": job_ids,
            })))
    }

    async fn run_bundle_foreground(
        &self,
        binaries: &[&str],
        instance_prefix: Option<&str>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        if ctx.is_human() {
            println!("Starting {} binaries...", binaries.len());
        }

        // Build all packages in a single cargo invocation for parallelism
        {
            let mut build_cmd = Command::new("cargo");
            build_cmd.arg("build");
            for name in binaries {
                let (_, package, _) = BINARIES
                    .iter()
                    .find(|(n, _, _)| n == name)
                    .ok_or_else(|| eyre!("Unknown binary: {name}"))?;
                build_cmd.arg("-p").arg(package);
            }
            if self.release {
                build_cmd.arg("--release");
            }

            if ctx.is_human() {
                let names: Vec<_> = binaries.to_vec();
                println!("Building {}...", names.join(", "));
            }

            let status = build_cmd.status().await?;
            if !status.success() {
                bail!("Failed to build binaries");
            }
        }

        // Start all
        let mut children: HashMap<String, Child> = HashMap::new();
        // Collected (stdout, stderr) for log-mode prefixed streaming
        let mut log_streams: Vec<(String, Option<ChildStdout>, Option<ChildStderr>)> = Vec::new();

        for name in binaries {
            let (_, _package, binary) = BINARIES
                .iter()
                .find(|(n, _, _)| n == name)
                .ok_or_else(|| eyre!("Unknown binary: {name}"))?;

            let instance_id = make_instance_id(name, instance_prefix);
            let target_dir = if self.release { "release" } else { "debug" };
            let binary_path = PathBuf::from(format!("target/{target_dir}/{binary}"));

            if ctx.is_human() {
                println!("Starting {name} (instance: {instance_id})...");
            }

            let mut cmd = Command::new(&binary_path);
            if is_node_package(name) {
                cmd.arg(format!("--instance-id={instance_id}"));
            } else if *name == "gateway" {
                cmd.arg("rpc-server");
            }

            let (stdout_io, stderr_io) = if self.logs {
                (Stdio::piped(), Stdio::piped())
            } else {
                (Stdio::inherit(), Stdio::inherit())
            };

            let mut child = cmd
                .stdout(stdout_io)
                .stderr(stderr_io)
                .kill_on_drop(true)
                .spawn()
                .with_context(|| format!("Failed to spawn {name}"))?;

            if self.logs {
                log_streams.push((name.to_string(), child.stdout.take(), child.stderr.take()));
            }

            children.insert(name.to_string(), child);
        }

        if ctx.is_human() {
            println!(
                "\n{} binaries running. Press Ctrl+C to stop.\n",
                children.len()
            );
        }

        // Spawn prefixed log-streaming tasks (P2)
        if self.logs && !log_streams.is_empty() {
            spawn_log_prefixers(log_streams);
        }

        // Spawn metrics overlay task (B5)
        if self.metrics {
            let db_url = crate::config::config().database_url.clone();
            tokio::spawn(async move {
                if let Some(url) = db_url {
                    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
                    loop {
                        interval.tick().await;
                        let metrics = crate::runtime_metrics::query_runtime_metrics(&url).await;
                        eprintln!("[metrics] {}", style(metrics.summary_fragment()).dim());
                    }
                }
            });
        }

        let exited_name = wait_for_any_child_exit(&mut children, ctx).await;

        // Kill remaining children
        if ctx.is_human() {
            println!("\nShutting down remaining processes...");
        }
        for (name, child) in &mut children {
            if Some(&name.clone()) != exited_name.as_ref() {
                if let Err(e) = child.kill().await
                    && ctx.is_human()
                {
                    eprintln!("Warning: couldn't kill {name}: {e}");
                }
                let _ = child.wait().await;
            }
        }

        Ok(CommandResult::success()
            .with_message(format!(
                "Bundle stopped (triggered by {})",
                exited_name.unwrap_or_else(|| "Ctrl+C".to_string())
            ))
            .with_duration(ctx.elapsed()))
    }

    async fn run_direct(
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

        // Only pass --instance-id to nodes (ingestors, automatons, canonicalizers)
        // Core services (ingestd, gateway) don't support this flag
        if is_node_package(package) {
            args.extend(["--".to_string(), format!("--instance-id={instance_id}")]);
        } else if package == "sinex-gateway" {
            // Gateway requires a subcommand - default to rpc-server
            args.extend(["--".to_string(), "rpc-server".to_string()]);
        }

        let status = Command::new("cargo")
            .args(&args)
            .status()
            .await
            .with_context(|| format!("Failed to run {package}"))?;

        let run_result = RunResult {
            binary: package.to_string(),
            pid: None,
            instance_id: Some(instance_id.to_string()),
            status: if status.success() {
                "success".to_string()
            } else {
                "failed".to_string()
            },
        };

        if status.success() {
            Ok(CommandResult::success()
                .with_message(format!("{package} exited successfully"))
                .with_data(serde_json::to_value(&run_result)?)
                .with_duration(ctx.elapsed()))
        } else {
            Ok(CommandResult::failure(crate::output::StructuredError {
                code: "RUN_FAILED".to_string(),
                message: format!("{package} exited with error"),
                location: Some("run".to_string()),
                suggestion: Some("Check logs with: xtask infra logs".to_string()),
            })
            .with_data(serde_json::to_value(&run_result)?)
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

        // Only pass --instance-id to nodes (ingestors, automatons, canonicalizers)
        // Core services (ingestd, gateway) don't support this flag
        if is_node_package(package) {
            args.extend(["--".to_string(), format!("--instance-id={instance_id}")]);
        } else if package == "sinex-gateway" {
            // Gateway requires a subcommand - default to rpc-server
            args.extend(["--".to_string(), "rpc-server".to_string()]);
        }

        let job = manager.spawn("cargo", &args)?;

        Ok(CommandResult::success()
            .with_message(format!("Backgrounded {package} as job {}", job.id))
            .with_data(serde_json::json!({
                "job_id": job.id,
                "package": package,
                "instance_id": instance_id,
            }))
            .with_duration(ctx.elapsed()))
    }

    async fn run_watch(
        &self,
        package: &str,
        _binary: &str,
        instance_id: &str,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        if ctx.is_human() {
            println!("Watch mode: {package} (instance: {instance_id})");
            println!("Press Ctrl+C to stop.\n");
        }

        let workspace_root = crate::config::workspace_root();
        let workspace_utf8 = camino::Utf8PathBuf::from_path_buf(workspace_root.to_path_buf())
            .map_err(|p| eyre!("workspace root is not valid UTF-8: {}", p.display()))?;

        // Build extra args for this binary type
        let mut extra_args = Vec::new();
        append_binary_extra_args(&mut extra_args, package, instance_id);

        let args = RunArgs {
            binary: package.to_string(),
            release: self.release,
            no_watch: false,
            tether: None,
            checkpoint: None,
            args: extra_args,
            env_vars: vec![],
        };

        let mut orchestrator = DevOrchestrator::new(args, workspace_utf8);
        orchestrator.run().await?;

        Ok(CommandResult::success()
            .with_message("Watch mode ended")
            .with_duration(ctx.elapsed()))
    }
}

fn execute_list(ctx: &CommandContext) -> CommandResult {
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
        println!("  {:<25} ingestd + gateway", "core");
        println!("  {:<25} all *-ingestor binaries", "all-ingestors");
        println!("  {:<25} all *-automaton binaries", "all-automatons");

        println!("\nSpecial:");
        println!(
            "  {:<25} Connect to remote NATS via The Tether",
            "tether <target>"
        );
    }

    for (name, package, binary) in BINARIES {
        binaries.push(serde_json::json!({
            "name": name,
            "package": package,
            "binary": binary,
        }));
    }

    CommandResult::success()
        .with_data(serde_json::json!({
            "binaries": binaries,
            "bundles": ["core", "all-ingestors", "all-automatons"],
            "special": ["tether"]
        }))
        .with_duration(ctx.elapsed())
}

/// Execute the tether command
async fn execute_tether(
    ctx: &CommandContext,
    target: &str,
    filter: &str,
    from_beginning: bool,
    from_sequence: Option<u64>,
) -> Result<CommandResult> {
    use crate::sandbox::tether::{TetherConfig, TetherSession};

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
        println!("Connecting to {target} via The Tether...");
        println!("  Gateway: {}", config.gateway_url);
        if let Some(ref f) = config.subject_filter {
            println!("  Filter: {f}");
        }
        if from_beginning {
            println!("  Starting from: beginning of stream");
        } else {
            println!("  Starting from: new events only");
        }
        println!();
    }

    // Start the session
    let session = TetherSession::start(config).await?;

    if ctx.is_human() {
        if let Some(info) = session.consumer_info() {
            println!(
                "Connected! Consumer: {}, Stream: {}",
                info.consumer_name, info.stream_name
            );
        }
        println!(
            "{}",
            console::style("Streaming events... (Press Ctrl+C to stop)").dim()
        );
        println!();
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let mut session_clone = session;

    // Spawn streaming task
    let stream_handle = tokio::spawn(async move {
        if let Err(e) = session_clone.stream_events(tx).await {
            eprintln!("[tether] Streaming error: {e}");
        }
        session_clone
    });

    // Print events as they arrive
    let mut count = 0;
    while let Some(event) = rx.recv().await {
        count += 1;
        if ctx.is_human() {
            println!(
                "[{}] {} {}",
                console::style(event.sequence).cyan(),
                console::style(&event.subject).green(),
                serde_json::to_string(&event.payload).unwrap_or_default()
            );
        } else {
            println!("{}", serde_json::to_string(&event)?);
        }
    }

    // Cleanup and collect stats
    let mut session = stream_handle.await?;
    let stats = session.stats();
    session.cleanup().await;

    Ok(CommandResult::success()
        .with_message(format!("Tether session closed. Received {count} events."))
        .with_data(serde_json::json!({
            "target": target,
            "events_received": stats.events_received(),
            "events_forwarded": stats.events_forwarded(),
            "errors": stats.errors(),
        }))
        .with_duration(ctx.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_binary_lookup() -> ::xtask::sandbox::TestResult<()> {
        // All binaries should be findable
        for (name, package, _) in BINARIES {
            let found = BINARIES.iter().find(|(n, _, _)| n == name);
            assert!(found.is_some(), "Binary {name} not found");
            assert_eq!(found.unwrap().1, *package);
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_ingestor_filter() -> ::xtask::sandbox::TestResult<()> {
        let ingestors: Vec<_> = BINARIES
            .iter()
            .filter(|(name, _, _)| name.contains("ingestor"))
            .collect();
        assert!(!ingestors.is_empty());
        for (name, _, _) in ingestors {
            assert!(name.contains("ingestor"));
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_automaton_filter() -> ::xtask::sandbox::TestResult<()> {
        let automatons: Vec<_> = BINARIES
            .iter()
            .filter(|(name, _, _)| name.contains("automaton"))
            .collect();
        assert!(!automatons.is_empty());
        for (name, _, _) in automatons {
            assert!(name.contains("automaton"));
        }
        Ok(())
    }
}
