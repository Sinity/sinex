//! Run command - Binary lifecycle management
//!
//! Provides unified command to run sinex binaries with:
//! - Process spawning with instance ID tracking
//! - `--watch` mode for development with seamless handoff
//! - `--bg` support via jobs system
//! - `--tether` mode for connecting to production NATS
//! - Bundle shortcuts (core, all-sources, all-automatons)
//! - `--logs` mode: interleaved color-coded output from all bundle processes

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use console::style;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::infra::stack::StackConfig;
use crate::jobs::JobManager;
use crate::orchestrator::{DevOrchestrator, RunArgs};
use crate::preflight;
use crate::process::{
    ProcessBuilder, configure_managed_child_tokio, register_tokio_child_process_group,
    terminate_tokio_child_process_group,
};

fn unix_timestamp_secs(now: std::time::SystemTime, context: &str) -> Result<u64> {
    now.duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .with_context(|| format!("{context}: system clock is before the unix epoch"))
}

fn unix_timestamp_micros(now: std::time::SystemTime, context: &str) -> Result<u128> {
    now.duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .with_context(|| format!("{context}: system clock is before the unix epoch"))
}

/// Build a deterministic instance ID from a binary name and optional prefix.
fn make_instance_id(name: &str, prefix: Option<&str>) -> String {
    prefix.map_or_else(
        || format!("{}-{}", name, std::process::id()),
        |p| format!("{p}-{name}"),
    )
}

const DEV_SOURCE_BINDINGS_PATH: &str = ".agent/dev/dev-source-bindings.json";

#[derive(Debug, Clone, Deserialize)]
struct DevSourceBindingsManifest {
    #[serde(default)]
    bindings: Vec<DevSourceBinding>,
}

#[derive(Debug, Clone, Deserialize)]
struct DevSourceBinding {
    source_id: String,
    #[serde(default = "default_source_binding_instance_idx")]
    instance_idx: u32,
    #[serde(default)]
    service_name: Option<String>,
    #[serde(default)]
    runtime_config: Option<serde_json::Value>,
    #[serde(default)]
    extra_args: Vec<String>,
    #[serde(default)]
    extra_env: HashMap<String, String>,
}

const DEFAULT_EXCLUDED_ALL_SOURCE_BINDINGS: &[&str] = &["system.journald"];

fn default_source_binding_instance_idx() -> u32 {
    1
}

fn default_source_binding_service_name(binding: &DevSourceBinding) -> String {
    binding.service_name.clone().unwrap_or_else(|| {
        format!(
            "source-driver-{}-{}",
            binding.source_id, binding.instance_idx
        )
    })
}

fn is_default_excluded_all_source_binding(source_id: &str) -> bool {
    DEFAULT_EXCLUDED_ALL_SOURCE_BINDINGS.contains(&source_id)
}

fn load_dev_source_binding(source_id: &str) -> Option<DevSourceBinding> {
    if cfg!(test) {
        return None;
    }

    let explicit_path = std::env::var("SINEX_SOURCE_BINDINGS_PATH").ok();
    let path = explicit_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| crate::config::workspace_root().join(DEV_SOURCE_BINDINGS_PATH));
    let bytes = std::fs::read(path).ok()?;
    let manifest: DevSourceBindingsManifest = serde_json::from_slice(&bytes).ok()?;
    manifest
        .bindings
        .into_iter()
        .find(|binding| binding.source_id == source_id)
}

fn load_dev_source_bindings_manifest() -> Option<DevSourceBindingsManifest> {
    let explicit_path = std::env::var("SINEX_SOURCE_BINDINGS_PATH").ok();
    let path = explicit_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| crate::config::workspace_root().join(DEV_SOURCE_BINDINGS_PATH));
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn default_all_source_bindings_from_manifest(
    manifest: DevSourceBindingsManifest,
) -> Vec<DevSourceBinding> {
    manifest
        .bindings
        .into_iter()
        .filter(|binding| !is_default_excluded_all_source_binding(&binding.source_id))
        .collect()
}

fn default_dev_source_bindings_path() -> PathBuf {
    crate::config::workspace_root().join(DEV_SOURCE_BINDINGS_PATH)
}

fn source_bindings_env_override() -> Option<(String, String)> {
    if std::env::var("SINEX_SOURCE_BINDINGS_PATH")
        .ok()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return None;
    }

    let path = default_dev_source_bindings_path();
    path.exists().then(|| {
        (
            "SINEX_SOURCE_BINDINGS_PATH".to_string(),
            path.display().to_string(),
        )
    })
}

fn append_core_source_bindings_env(env: &mut Vec<(String, String)>) {
    if env
        .iter()
        .any(|(key, value)| key == "SINEX_SOURCE_BINDINGS_PATH" && !value.trim().is_empty())
    {
        return;
    }

    if let Some(binding_env) = source_bindings_env_override() {
        env.push(binding_env);
    }
}

fn source_binding_runtime_args(binding: &DevSourceBinding, run_identity: &str) -> Vec<String> {
    let mut args = vec![
        "scan-source-driver".to_string(),
        "--source".to_string(),
        binding.source_id.clone(),
        "--service-name".to_string(),
        run_identity.to_string(),
    ];
    append_source_binding_args(&mut args, binding.clone());
    args
}

fn append_source_binding_args(args: &mut Vec<String>, binding: DevSourceBinding) {
    if let Some(config) = binding.runtime_config
        && !config.is_null()
    {
        args.push("--runtime-config".to_string());
        args.push(config.to_string());
    }

    for extra_arg in binding.extra_args {
        args.push("--extra-arg".to_string());
        args.push(extra_arg);
    }

    let mut extra_env: Vec<(String, String)> = binding.extra_env.into_iter().collect();
    extra_env.sort_by(|left, right| left.0.cmp(&right.0));
    for (key, value) in extra_env {
        args.push("--extra-env".to_string());
        args.push(format!("{key}={value}"));
    }
}

fn append_dev_source_binding_args(args: &mut Vec<String>, source_id: &str) {
    let Some(binding) = load_dev_source_binding(source_id) else {
        return;
    };
    append_source_binding_args(args, binding);
}

/// Build the runtime CLI arguments for the unified `sinexd` binary.
///
/// Post-collapse, every short name (`sinexd`, automatons, source contracts)
/// resolves to the same `sinexd` binary. Source short names dispatch through
/// `sinexd scan-source-driver --source <id>`; everything else falls through to
/// the default `serve` subcommand which runs the full supervisor.
fn runtime_cli_args(_package: &str, run_identity: &str, source: Option<&str>) -> Vec<String> {
    source.map_or_else(Vec::new, |id| {
        let mut args = vec![
            "scan-source-driver".to_string(),
            "--source".to_string(),
            id.to_string(),
            "--service-name".to_string(),
            run_identity.to_string(),
        ];
        append_dev_source_binding_args(&mut args, id);
        args
    })
}

/// Append source runtime args after the cargo `--` separator when needed.
fn append_binary_extra_args(
    args: &mut Vec<String>,
    package: &str,
    run_identity: &str,
    automaton: Option<&str>,
) {
    let extra_args = runtime_cli_args(package, run_identity, automaton);
    if !extra_args.is_empty() {
        args.push("--".to_string());
        args.extend(extra_args);
    }
}

fn target_binary_path(release: bool, binary: &str) -> PathBuf {
    let target_dir = if release { "release" } else { "debug" };
    crate::orchestrator::get_target_dir(&crate::config::workspace_root())
        .join(target_dir)
        .join(binary)
}

fn local_run_failure_suggestion(dev_journal_path: Option<&Path>) -> String {
    dev_journal_path.map_or_else(
        || "Inspect the process output above".to_string(),
        |path| {
            format!(
                "Inspect the process output above or the dev journal at {}",
                path.display()
            )
        },
    )
}

/// Developer observability shim — writes pseudo-journald NDJSON to a log file.
///
/// `sinexd system.journald source` consumes `journalctl --output=json` (one JSON object per
/// line, each with `_SYSTEMD_UNIT`, `MESSAGE`, `_PID`, `_BOOT_ID`,
/// `__REALTIME_TIMESTAMP`, `SYSLOG_IDENTIFIER`). This struct writes equivalent entries
/// so the source's journald-monitoring loop works end-to-end in dev environments
/// without systemd.
///
/// Clones share the same underlying sender — safe to distribute across stream tasks.
#[derive(Clone)]
struct DevJournal {
    writer: std::sync::Arc<std::sync::Mutex<std::io::BufWriter<std::fs::File>>>,
    boot_id: String,
}

impl DevJournal {
    fn new(path: &std::path::Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open dev journal at {}", path.display()))?;
        let boot_ts = unix_timestamp_secs(
            std::time::SystemTime::now(),
            "failed to derive dev journal boot timestamp",
        )?;
        let boot_id = format!("dev-{boot_ts}");

        Ok(Self {
            writer: std::sync::Arc::new(std::sync::Mutex::new(std::io::BufWriter::new(file))),
            boot_id,
        })
    }

    fn write_entry(&self, unit: &str, pid: u32, message: &str) {
        let ts_us = match unix_timestamp_micros(
            std::time::SystemTime::now(),
            "failed to derive dev journal entry timestamp",
        ) {
            Ok(ts_us) => ts_us,
            Err(error) => {
                eprintln!("[run] {error:#}");
                return;
            }
        };
        // journald --output=json format consumed by unified_journal_watcher.rs
        let entry = serde_json::json!({
            "_SYSTEMD_UNIT": format!("{unit}.service"),
            "MESSAGE": message,
            "_PID": pid.to_string(),
            "_BOOT_ID": &self.boot_id,
            "__REALTIME_TIMESTAMP": ts_us.to_string(),
            "SYSLOG_IDENTIFIER": unit,
        });
        use std::io::Write;
        let line = entry.to_string();
        let Ok(mut writer) = self.writer.lock() else {
            eprintln!("[run] failed to lock dev journal writer for {unit} (pid {pid})");
            return;
        };
        if let Err(error) = writer
            .write_all(line.as_bytes())
            .and_then(|()| writer.write_all(b"\n"))
            .and_then(|()| writer.flush())
        {
            eprintln!("[run] failed to write dev journal entry for {unit} (pid {pid}): {error}");
        }
    }
}

/// Spawn async tasks to stream process output to the terminal (optionally with
/// colored name prefix) and/or write journald-format entries to a `DevJournal`.
///
/// Each `(name, stdout, stderr, pid)` entry gets up to two detached tasks.
/// Tasks terminate naturally when child streams close (process exit).
fn spawn_output_handlers(
    streams: Vec<(String, Option<ChildStdout>, Option<ChildStderr>, u32)>,
    show_prefix: bool,
    journal: &Option<DevJournal>,
) {
    // Color cycle: cyan, yellow, magenta, blue, green (wraps for >5 processes)
    let colors: &[fn(&str) -> console::StyledObject<String>] = &[
        |s| style(s.to_string()).cyan(),
        |s| style(s.to_string()).yellow(),
        |s| style(s.to_string()).magenta(),
        |s| style(s.to_string()).blue(),
        |s| style(s.to_string()).green(),
    ];

    for (idx, (name, stdout, stderr, pid)) in streams.into_iter().enumerate() {
        let color = colors[idx % colors.len()];
        let prefix_colored = color(&format!("[{name}]")).to_string();

        if let Some(stdout) = stdout {
            let prefix = prefix_colored.clone();
            let name_clone = name.clone();
            let journal_clone = journal.clone();
            tokio::task::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => {
                            if show_prefix {
                                println!("{prefix} {line}");
                            } else {
                                println!("{line}");
                            }
                            if let Some(ref j) = journal_clone {
                                j.write_entry(&name_clone, pid, &line);
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            let message =
                                format!("stdout stream read failed for {name_clone}: {error}");
                            eprintln!("[run] {message}");
                            if let Some(ref j) = journal_clone {
                                j.write_entry(&name_clone, pid, &message);
                            }
                            break;
                        }
                    }
                }
            });
        }

        if let Some(stderr) = stderr {
            let prefix = prefix_colored.clone();
            let name_clone = name.clone();
            let journal_clone = journal.clone();
            tokio::task::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => {
                            if show_prefix {
                                eprintln!("{prefix} {line}");
                            } else {
                                eprintln!("{line}");
                            }
                            if let Some(ref j) = journal_clone {
                                j.write_entry(&name_clone, pid, &line);
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            let message =
                                format!("stderr stream read failed for {name_clone}: {error}");
                            eprintln!("[run] {message}");
                            if let Some(ref j) = journal_clone {
                                j.write_entry(&name_clone, pid, &message);
                            }
                            break;
                        }
                    }
                }
            });
        }
    }
}

fn require_spawned_pid(pid: Option<u32>, binary: &str) -> Result<u32> {
    pid.ok_or_else(|| eyre!("spawned process for {binary} did not expose a PID"))
}

/// Poll children until one exits, returning its name.
///
/// Returns `None` after 8 hours (D6 fix) — callers treat None as "kill everything",
/// so a timeout causes a clean shutdown rather than an infinite poll.
///
/// X5: Signal propagation note — foreground helpers become managed child process
/// groups with a parent-death kill signal. Ctrl+C therefore does not rely on the
/// terminal forwarding SIGINT into each child process group: if xtask exits, the
/// child groups are torn down by the managed-process contract.
async fn wait_for_any_child_exit(
    children: &mut HashMap<String, Child>,
    ctx: &CommandContext,
) -> Option<String> {
    use futures::stream::{FuturesUnordered, StreamExt};

    // Event-driven: each child.wait() wakes when its SIGCHLD arrives.
    // FuturesUnordered yields the first to complete; no polling needed.
    let mut waiters: FuturesUnordered<_> = children
        .iter_mut()
        .map(|(name, child)| {
            let name = name.clone();
            Box::pin(async move {
                let status = child.wait().await;
                (name, status)
            })
        })
        .collect();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_hours(8);
    tokio::select! {
        result = waiters.next() => match result {
            Some((name, Ok(status))) => {
                if ctx.is_human() {
                    println!("{name} exited with status: {status}");
                }
                Some(name)
            }
            Some((name, Err(e))) => {
                if ctx.is_human() {
                    eprintln!("Error waiting on {name}: {e}");
                }
                Some(name)
            }
            None => None, // empty children map
        },
        () = tokio::time::sleep_until(deadline) => {
            if ctx.is_human() {
                eprintln!("[run] 8-hour timeout reached — shutting down");
            }
            None
        }
    }
}

async fn stop_bundle_child(name: &str, child: &mut Child) -> Result<()> {
    if child
        .try_wait()
        .with_context(|| format!("failed to poll {name} before bundle shutdown"))?
        .is_some()
    {
        return Ok(());
    }

    terminate_tokio_child_process_group(child, name, "bundle shutdown").with_context(|| {
        format!("failed to terminate {name} process group during bundle shutdown")
    })?;

    child
        .wait()
        .await
        .with_context(|| format!("failed to wait for {name} during bundle shutdown"))?;
    Ok(())
}

/// Known binary targets and their package names.
///
/// Tuple layout: `(short_name, package, binary_name, source_id)`.
///
/// Post-sinexd-collapse: previously separate binaries folded into the unified
/// `sinexd` daemon. Dev targets use current source/automaton labels and all
/// build/run sinexd.
///
/// - `sinexd`: launch sinexd's supervisor (default `serve` subcommand). The
///   supervisor brings up the event engine, the API, and every enabled
///   source/automaton in one process.
/// - Source short names (e.g. `fs-source`): dispatch through
///   `sinexd scan-source-driver --source <id>` for one-off scan-mode
///   runs against a single source.
/// - Automaton short names: also resolve to the supervisor (`serve`) since
///   individual automatons are no longer separately runnable.
static BINARIES: &[(&str, &str, &str, Option<&str>)] = &[
    // Core supervisor entry points (serve the whole daemon)
    ("sinexd", "sinexd", "sinexd", None),
    // Source one-off scans (sinexd scan-source-driver --source <id>)
    ("fs-source", "sinexd", "sinexd", Some("fs")),
    (
        "terminal-source",
        "sinexd",
        "sinexd",
        Some("terminal.zsh-history"),
    ),
    (
        "desktop-source",
        "sinexd",
        "sinexd",
        Some("desktop.activitywatch"),
    ),
    ("system-source", "sinexd", "sinexd", Some("system.journald")),
    // Automatons — no per-automaton dispatch in the new layout; running any
    // of these brings up the full supervisor.
    ("analytics-automaton", "sinexd", "sinexd", None),
    ("health-automaton", "sinexd", "sinexd", None),
    ("session-detector", "sinexd", "sinexd", None),
    ("hourly-summarizer", "sinexd", "sinexd", None),
    ("daily-summarizer", "sinexd", "sinexd", None),
    ("terminal-canonicalizer", "sinexd", "sinexd", None),
];

const CORE_TARGETS: &[&str] = &["sinexd"];
const SOURCE_TARGETS: &[&str] = &[
    "fs-source",
    "terminal-source",
    "desktop-source",
    "system-source",
];
const AUTOMATON_TARGETS: &[&str] = &[
    "analytics-automaton",
    "health-automaton",
    "session-detector",
    "hourly-summarizer",
    "daily-summarizer",
    "terminal-canonicalizer",
];

fn lookup_binary(
    name: &str,
) -> Option<&'static (
    &'static str,
    &'static str,
    &'static str,
    Option<&'static str>,
)> {
    BINARIES
        .iter()
        .find(|(candidate, _, _, _)| *candidate == name)
}

pub(crate) fn list_run_targets() -> Vec<String> {
    let mut targets: Vec<String> = BINARIES
        .iter()
        .map(|(name, _, _, _)| (*name).to_string())
        .collect();
    targets.extend(
        ["core", "all-sources", "all-automatons"]
            .into_iter()
            .map(str::to_string),
    );
    targets.sort_unstable();
    targets
}

/// Run subcommand variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum RunSubcommand {
    /// Run a specific runtime module target by name
    #[command(name = "module")]
    RuntimeModule {
        /// RuntimeModule target name (e.g., fs-source, analytics-automaton)
        name: String,
        /// Instance ID for multi-instance coordination
        #[arg(long)]
        instance_id: Option<String>,
    },
    /// Run core services bundle (event engine + API)
    Core {
        /// Instance ID prefix
        #[arg(long)]
        instance_id: Option<String>,
    },
    /// Run all source scan targets
    AllSources {
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
    /// - `SINEX_API_URL` or SINEX_{TARGET}_`GATEWAY_URL`: Gateway RPC URL
    /// - `SINEX_API_TOKEN` or SINEX_{TARGET}_`RPC_TOKEN`: RPC auth token (required)
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

    /// Write pseudo-journald NDJSON to .sinex/state/dev-journal.log
    ///
    /// Wraps each log line in a journald JSON envelope (`_SYSTEMD_UNIT`, `MESSAGE`,
    /// `_PID`, `__REALTIME_TIMESTAMP`) so `sinexd system.journald source` can monitor locally-
    /// running sinex processes without systemd. Implies stdout/stderr capture.
    #[arg(long, global = true)]
    pub dev_journal: bool,
}

/// Result of running a binary
#[derive(Debug, Serialize)]
struct RunResult {
    binary: String,
    pid: Option<u32>,
    instance_id: Option<String>,
    status: String,
}

#[derive(Debug, Serialize)]
struct LocalRuntimeCoordinates {
    mode: &'static str,
    checkout_root: String,
    dev_state_dir: String,
    logs_dir: String,
    database_url: String,
    nats_url: String,
    api_url: Option<String>,
    jobs_dir: String,
}

impl LocalRuntimeCoordinates {
    fn gather() -> Result<Self> {
        let stack = StackConfig::for_current_checkout()?;
        let cfg = config();
        Ok(Self {
            mode: "dev-local-explicit",
            checkout_root: crate::config::workspace_root().display().to_string(),
            dev_state_dir: stack.state_dir.display().to_string(),
            logs_dir: stack.logs_dir().display().to_string(),
            database_url: cfg
                .database_url
                .clone()
                .unwrap_or_else(|| stack.database_url()),
            nats_url: cfg.nats_url.clone().unwrap_or_else(|| stack.nats_url()),
            api_url: cfg.gateway_url.clone(),
            jobs_dir: cfg.jobs_dir().display().to_string(),
        })
    }

    fn print_human(&self) {
        println!("Local runtime:");
        println!("  mode:        {}", self.mode);
        println!("  checkout:    {}", self.checkout_root);
        println!("  dev-state:   {}", self.dev_state_dir);
        println!("  logs:        {}", self.logs_dir);
        println!("  database:    {}", self.database_url);
        println!("  nats:        {}", self.nats_url);
        println!(
            "  api:         {}",
            self.api_url.as_deref().unwrap_or("not configured")
        );
        println!("  jobs:        {}", self.jobs_dir);
        println!("  inspect:     xtask infra status");
    }
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

        self.validate_flag_compatibility(ctx)?;

        match &self.subcommand {
            RunSubcommand::List => Ok(execute_list(ctx)),
            RunSubcommand::RuntimeModule { name, instance_id } => {
                self.run_binary(name, instance_id.clone(), ctx).await
            }
            RunSubcommand::Core { instance_id } => {
                self.run_bundle(CORE_TARGETS, instance_id.clone(), ctx)
                    .await
            }
            RunSubcommand::AllSources { instance_id } => {
                self.run_source_bindings_bundle(instance_id.clone(), ctx)
                    .await
            }
            RunSubcommand::AllAutomatons { instance_id } => {
                self.run_bundle(AUTOMATON_TARGETS, instance_id.clone(), ctx)
                    .await
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
        CommandMetadata {
            timeout: None,
            ..CommandMetadata::build()
        }
    }
}

impl RunCommand {
    fn ensure_ready_staged(&self, ctx: &CommandContext) -> Result<()> {
        let stage = ctx.start_stage("preflight");
        let result = preflight::ensure_ready(ctx);
        ctx.finish_stage(stage, result.is_ok());
        result
    }

    fn runs_single_binary(&self) -> bool {
        matches!(self.subcommand, RunSubcommand::RuntimeModule { .. })
    }

    fn runs_bundle(&self) -> bool {
        matches!(
            self.subcommand,
            RunSubcommand::Core { .. }
                | RunSubcommand::AllSources { .. }
                | RunSubcommand::AllAutomatons { .. }
        )
    }

    fn runs_local_processes(&self) -> bool {
        self.runs_single_binary() || self.runs_bundle()
    }

    fn validate_flag_compatibility(&self, ctx: &CommandContext) -> Result<()> {
        if self.watch && !self.runs_single_binary() {
            bail!("--watch only supports single local module targets");
        }

        if self.watch && ctx.is_background() {
            bail!("--watch is incompatible with --bg");
        }

        if (self.logs || self.dev_journal) && !self.runs_local_processes() {
            bail!("--logs and --dev-journal only support local binary or bundle runs");
        }

        if (self.logs || self.dev_journal) && ctx.is_background() {
            bail!("--logs and --dev-journal are incompatible with --bg");
        }

        if (self.logs || self.dev_journal) && self.watch {
            bail!("--logs and --dev-journal are incompatible with --watch");
        }

        if self.metrics && !self.runs_local_processes() {
            bail!("--metrics only supports local binary or bundle runs");
        }

        if self.metrics && ctx.is_background() {
            bail!("--metrics is incompatible with --bg");
        }

        if let RunSubcommand::Tether {
            from_beginning: true,
            from_sequence: Some(_),
            ..
        } = &self.subcommand
        {
            bail!("--from-beginning and --from-sequence are mutually exclusive");
        }

        Ok(())
    }

    fn maybe_spawn_metrics_overlay(&self, ctx: &CommandContext) {
        if !self.metrics {
            return;
        }

        let db_url = crate::config::config().database_url.clone();
        if db_url.is_none() {
            if ctx.is_human() {
                eprintln!("[metrics] DATABASE_URL not set; runtime overlay disabled");
            }
            return;
        }

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

    fn local_run_env_vars(&self) -> Vec<(String, String)> {
        crate::preflight::local_runtime_env_overrides()
    }

    fn core_bundle_env_vars(&self) -> Vec<(String, String)> {
        let mut env = self.local_run_env_vars();
        append_core_source_bindings_env(&mut env);
        env
    }

    fn local_runtime_coordinates(&self) -> Result<LocalRuntimeCoordinates> {
        LocalRuntimeCoordinates::gather()
    }

    fn print_local_runtime_coordinates(&self, ctx: &CommandContext) -> Result<()> {
        if ctx.is_human() {
            self.local_runtime_coordinates()?.print_human();
        }
        Ok(())
    }

    fn build_cargo_run_args(
        &self,
        package: &str,
        instance_id: &str,
        automaton: Option<&str>,
    ) -> Vec<String> {
        let mut args = vec!["run".to_string(), "-p".to_string(), package.to_string()];
        if self.release {
            args.push("--release".to_string());
        }
        append_binary_extra_args(&mut args, package, instance_id, automaton);
        args
    }

    async fn build_packages(&self, packages: &[&str], ctx: &CommandContext) -> Result<()> {
        let stage = ctx.start_stage("build");
        let mut build_cmd = ProcessBuilder::cargo()
            .arg("build")
            .current_dir(crate::config::workspace_root())
            .inherit_output()
            .with_description(format!("building packages: {}", packages.join(", ")));
        for package in packages {
            build_cmd = build_cmd.arg("-p").arg(*package);
        }
        if self.release {
            build_cmd = build_cmd.arg("--release");
        }

        if ctx.is_human() {
            println!("Building {}...", packages.join(", "));
        }

        let status = build_cmd
            .run_tokio_status()
            .await
            .with_context(|| format!("Failed to build packages: {}", packages.join(", ")));
        ctx.finish_stage(
            stage,
            status.as_ref().is_ok_and(std::process::ExitStatus::success),
        );
        let status = status?;
        if !status.success() {
            bail!("Failed to build packages: {}", packages.join(", "));
        }
        Ok(())
    }

    async fn run_binary(
        &self,
        name: &str,
        instance_id: Option<String>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        // Find binary info
        let (_, package, binary, automaton) = BINARIES
            .iter()
            .find(|(n, _, _, _)| *n == name)
            .ok_or_else(|| {
                eyre!("Unknown binary '{name}'. Use 'xtask run list' to see available binaries.")
            })?;

        // Ensure infrastructure is ready (binaries need DB + NATS)
        preflight::ensure_ready(ctx)?;

        let instance_id = instance_id.unwrap_or_else(|| format!("{}-{}", name, std::process::id()));

        if self.dry_run {
            let runtime = self.local_runtime_coordinates()?;
            println!("Would run: {name} (package: {package}, instance: {instance_id})");
            if self.watch {
                println!("  (with --watch)");
            }
            if ctx.is_human() {
                runtime.print_human();
            }
            return Ok(CommandResult::success()
                .with_detail("dry-run passed")
                .with_data(serde_json::json!({
                    "target": name,
                    "package": package,
                    "instance_id": instance_id,
                    "runtime": runtime,
                })));
        }

        self.print_local_runtime_coordinates(ctx)?;

        if ctx.is_background() {
            return self
                .run_background(package, binary, &instance_id, *automaton, ctx)
                .await;
        }

        if self.watch {
            return self
                .run_watch(package, binary, &instance_id, *automaton, ctx)
                .await;
        }

        // Direct run
        self.run_direct(package, binary, &instance_id, *automaton, ctx)
            .await
    }

    async fn run_bundle(
        &self,
        binaries: &[&str],
        instance_prefix: Option<String>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        // Ensure infrastructure is ready (binaries need DB + NATS)
        if !self.dry_run {
            self.ensure_ready_staged(ctx)?;
        }

        if self.dry_run {
            let runtime = self.local_runtime_coordinates()?;
            println!("Would run bundle: {binaries:?}");
            if ctx.is_background() {
                println!("  (background mode via JobManager)");
            }
            if ctx.is_human() {
                runtime.print_human();
            }
            return Ok(CommandResult::success()
                .with_detail("dry-run passed")
                .with_data(serde_json::json!({
                    "binaries": binaries,
                    "runtime": runtime,
                })));
        }

        self.print_local_runtime_coordinates(ctx)?;

        if ctx.is_background() {
            return self
                .run_bundle_background(binaries, instance_prefix.as_deref(), ctx)
                .await;
        }

        self.run_bundle_foreground(binaries, instance_prefix.as_deref(), ctx)
            .await
    }

    async fn run_source_bindings_bundle(
        &self,
        instance_prefix: Option<String>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        let manifest = load_dev_source_bindings_manifest().ok_or_else(|| {
            eyre!("No dev source bindings manifest found. Run `xtask infra dev-bindings` first.")
        })?;
        let excluded: Vec<String> = manifest
            .bindings
            .iter()
            .filter(|binding| is_default_excluded_all_source_binding(&binding.source_id))
            .map(|binding| binding.source_id.clone())
            .collect();
        let bindings = default_all_source_bindings_from_manifest(manifest);
        if bindings.is_empty() {
            bail!("dev source bindings manifest has no runnable bindings after default exclusions");
        }

        if !self.dry_run {
            self.ensure_ready_staged(ctx)?;
        }

        let runtime = self.local_runtime_coordinates()?;
        if self.dry_run {
            let sources: Vec<&str> = bindings
                .iter()
                .map(|binding| binding.source_id.as_str())
                .collect();
            if ctx.is_human() {
                println!("Would run source bindings: {sources:?}");
                if !excluded.is_empty() {
                    println!("Default-excluded source bindings: {excluded:?}");
                }
                runtime.print_human();
            }
            return Ok(CommandResult::success()
                .with_detail("dry-run passed")
                .with_data(serde_json::json!({
                    "sources": sources,
                    "excluded_sources": excluded,
                    "runtime": runtime,
                })));
        }

        self.print_local_runtime_coordinates(ctx)?;

        if ctx.is_background() {
            return self
                .run_source_bindings_background(
                    &bindings,
                    &excluded,
                    instance_prefix.as_deref(),
                    runtime,
                    ctx,
                )
                .await;
        }

        bail!(
            "foreground all-sources is not supported for manifest-driven source bindings yet; use `xtask run all-sources --bg`"
        )
    }

    async fn run_source_bindings_background(
        &self,
        bindings: &[DevSourceBinding],
        excluded: &[String],
        instance_prefix: Option<&str>,
        runtime: LocalRuntimeCoordinates,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        let cfg = config();
        let manager = JobManager::new(cfg.jobs_dir())?;
        let runtime_env = self.core_bundle_env_vars();
        self.build_packages(&["sinexd"], ctx).await?;

        let binary_command = target_binary_path(self.release, "sinexd")
            .to_string_lossy()
            .into_owned();
        let mut job_ids = Vec::with_capacity(bindings.len());
        let mut sources = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let default_service_name = default_source_binding_service_name(binding);
            let run_identity = instance_prefix.map_or(default_service_name.clone(), |prefix| {
                format!("{prefix}-{default_service_name}")
            });
            let args = source_binding_runtime_args(binding, &run_identity);
            let job =
                manager.spawn_with_env_without_watchdog(&binary_command, &args, &runtime_env)?;
            job_ids.push(job.id);
            sources.push(binding.source_id.clone());
        }

        Ok(CommandResult::success()
            .with_message(format!(
                "Started {} source bindings in background",
                bindings.len()
            ))
            .with_data(serde_json::json!({
                "sources": sources,
                "excluded_sources": excluded,
                "job_ids": job_ids,
                "runtime": runtime,
            })))
    }

    async fn run_bundle_background(
        &self,
        binaries: &[&str],
        instance_prefix: Option<&str>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        let cfg = config();
        let manager = JobManager::new(cfg.jobs_dir())?;
        let mut job_ids = Vec::new();
        let runtime_env = if binaries == CORE_TARGETS {
            self.core_bundle_env_vars()
        } else {
            self.local_run_env_vars()
        };
        let packages: Vec<&str> = binaries
            .iter()
            .map(|name| {
                BINARIES
                    .iter()
                    .find(|(n, _, _, _)| n == name)
                    .map(|(_, package, _, _)| *package)
                    .ok_or_else(|| eyre!("Unknown binary: {name}"))
            })
            .collect::<Result<Vec<_>>>()?;

        self.build_packages(&packages, ctx).await?;

        let runtime = self.local_runtime_coordinates()?;
        for name in binaries {
            let (_, package, binary, automaton) = BINARIES
                .iter()
                .find(|(n, _, _, _)| n == name)
                .ok_or_else(|| eyre!("Unknown binary: {name}"))?;

            let instance_id = make_instance_id(name, instance_prefix);
            let binary_command = target_binary_path(self.release, binary)
                .to_string_lossy()
                .into_owned();
            let args = runtime_cli_args(package, &instance_id, *automaton);

            let job =
                manager.spawn_with_env_without_watchdog(&binary_command, &args, &runtime_env)?;
            job_ids.push(job.id);
        }

        Ok(CommandResult::success()
            .with_message(format!("Started {} binaries in background", binaries.len()))
            .with_data(serde_json::json!({
                "binaries": binaries,
                "job_ids": job_ids,
                "runtime": runtime,
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

        let packages: Vec<&str> = binaries
            .iter()
            .map(|name| {
                BINARIES
                    .iter()
                    .find(|(n, _, _, _)| n == name)
                    .map(|(_, package, _, _)| *package)
                    .ok_or_else(|| eyre!("Unknown binary: {name}"))
            })
            .collect::<Result<Vec<_>>>()?;
        self.build_packages(&packages, ctx).await?;

        // Start all
        let mut children: HashMap<String, Child> = HashMap::new();
        // Collected (name, stdout, stderr, pid) for output handling
        let mut log_streams: Vec<(String, Option<ChildStdout>, Option<ChildStderr>, u32)> =
            Vec::new();
        // Pipe stdout/stderr when --logs (prefix display) or --dev-journal (journal write)
        let pipe_output = self.logs || self.dev_journal;
        let runtime_env = if binaries == CORE_TARGETS {
            self.core_bundle_env_vars()
        } else {
            self.local_run_env_vars()
        };

        for name in binaries {
            let (_, package, binary, automaton) = BINARIES
                .iter()
                .find(|(n, _, _, _)| n == name)
                .ok_or_else(|| eyre!("Unknown binary: {name}"))?;

            let instance_id = make_instance_id(name, instance_prefix);
            let binary_path = target_binary_path(self.release, binary);

            if ctx.is_human() {
                println!("Starting {name} (instance: {instance_id})...");
            }

            let mut cmd = Command::new(&binary_path);
            configure_managed_child_tokio(&mut cmd);
            cmd.args(runtime_cli_args(package, &instance_id, *automaton));

            let (stdout_io, stderr_io) = if pipe_output {
                (Stdio::piped(), Stdio::piped())
            } else {
                (Stdio::inherit(), Stdio::inherit())
            };

            let mut child = cmd
                .envs(runtime_env.iter().cloned())
                .stdout(stdout_io)
                .stderr(stderr_io)
                .kill_on_drop(true)
                .spawn()
                .with_context(|| format!("Failed to spawn {name}"))?;
            register_tokio_child_process_group(&child, name);

            if pipe_output {
                let pid = require_spawned_pid(child.id(), name)?;
                log_streams.push((
                    name.to_string(),
                    child.stdout.take(),
                    child.stderr.take(),
                    pid,
                ));
            }

            children.insert(name.to_string(), child);
        }

        if ctx.is_human() {
            println!(
                "\n{} binaries running. Press Ctrl+C to stop.\n",
                children.len()
            );
        }

        // Spawn output handler tasks: prefix display (--logs) and/or journal writes (--dev-journal)
        if pipe_output && !log_streams.is_empty() {
            let journal = if self.dev_journal {
                let journal_path = config().state_dir.join("dev-journal.log");
                if ctx.is_human() {
                    println!(
                        "Dev journal: {} (sinexd system.journald source will pick this up)",
                        journal_path.display()
                    );
                }
                Some(DevJournal::new(&journal_path)?)
            } else {
                None
            };
            spawn_output_handlers(log_streams, self.logs, &journal);
        }

        self.maybe_spawn_metrics_overlay(ctx);

        let run_stage = ctx.start_stage("bundle-run");
        let exited_name = wait_for_any_child_exit(&mut children, ctx).await;

        // Kill remaining children
        if ctx.is_human() {
            println!("\nShutting down remaining processes...");
        }
        let mut shutdown_failures = Vec::new();
        for (name, child) in &mut children {
            if Some(name) != exited_name.as_ref()
                && let Err(error) = stop_bundle_child(name, child).await
            {
                if ctx.is_human() {
                    eprintln!("Error stopping {name}: {error:#}");
                }
                shutdown_failures.push(format!("{name}: {error:#}"));
            }
        }
        if !shutdown_failures.is_empty() {
            ctx.finish_stage(run_stage, false);
            bail!(
                "failed to stop remaining bundle processes:\n{}",
                shutdown_failures.join("\n")
            );
        }
        ctx.finish_stage(run_stage, true);

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
        binary: &str,
        instance_id: &str,
        automaton: Option<&str>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        if ctx.is_human() {
            println!("Building {package}...");
        }

        // When --dev-journal is active, build first then spawn the binary directly
        // so we can pipe stdout/stderr through the journal shim.
        if self.dev_journal || self.logs {
            return self
                .run_direct_piped(package, binary, instance_id, automaton, ctx)
                .await;
        }

        let args = self.build_cargo_run_args(package, instance_id, automaton);

        self.maybe_spawn_metrics_overlay(ctx);
        let runtime_env = self.local_run_env_vars();

        let run_stage = ctx.start_stage("run");
        let status = ProcessBuilder::cargo()
            .args(&args)
            .envs(runtime_env)
            .current_dir(crate::config::workspace_root())
            .inherit_output()
            .without_timeout()
            .with_description(format!("running {package}"))
            .run_tokio_status()
            .await
            .with_context(|| format!("Failed to run {package}"));
        ctx.finish_stage(
            run_stage,
            status.as_ref().is_ok_and(std::process::ExitStatus::success),
        );
        let status = status?;

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
                suggestion: Some(local_run_failure_suggestion(None)),
            })
            .with_data(serde_json::to_value(&run_result)?)
            .with_duration(ctx.elapsed()))
        }
    }

    /// Build then spawn a single binary with piped I/O for `--logs` / `--dev-journal`.
    async fn run_direct_piped(
        &self,
        package: &str,
        binary: &str,
        instance_id: &str,
        automaton: Option<&str>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        // Step 1: build
        let build_stage = ctx.start_stage("build");
        let mut build = ProcessBuilder::cargo()
            .arg("build")
            .arg("-p")
            .arg(package)
            .current_dir(crate::config::workspace_root())
            .inherit_output()
            .with_description(format!("building {package}"));
        if self.release {
            build = build.arg("--release");
        }
        let build_status = build
            .run_tokio_status()
            .await
            .with_context(|| format!("Failed to build {package}"));
        ctx.finish_stage(
            build_stage,
            build_status
                .as_ref()
                .is_ok_and(std::process::ExitStatus::success),
        );
        let build_status = build_status?;
        if !build_status.success() {
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "BUILD_FAILED".to_string(),
                message: format!("{package} failed to build"),
                location: Some("run".to_string()),
                suggestion: None,
            }));
        }

        // Step 2: spawn binary directly
        let binary_path = target_binary_path(self.release, binary);

        let mut cmd = Command::new(&binary_path);
        configure_managed_child_tokio(&mut cmd);
        cmd.args(runtime_cli_args(package, instance_id, automaton));
        cmd.envs(self.local_run_env_vars());

        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("Failed to spawn {binary}"))?;

        let pid = require_spawned_pid(child.id(), binary)?;

        // Derive a short name for display/journal from the binary name
        let short_name = BINARIES
            .iter()
            .find(|(_, pkg, _, _)| *pkg == package)
            .map_or(binary, |(n, _, _, _)| *n);
        register_tokio_child_process_group(&child, short_name);

        let journal_path = self
            .dev_journal
            .then(|| config().state_dir.join("dev-journal.log"));
        let journal = if let Some(journal_path) = journal_path.as_ref() {
            if ctx.is_human() {
                println!(
                    "Dev journal: {} (sinexd system.journald source will pick this up)",
                    journal_path.display()
                );
            }
            Some(DevJournal::new(journal_path)?)
        } else {
            None
        };

        spawn_output_handlers(
            vec![(
                short_name.to_string(),
                child.stdout.take(),
                child.stderr.take(),
                pid,
            )],
            self.logs,
            &journal,
        );

        self.maybe_spawn_metrics_overlay(ctx);

        if ctx.is_human() {
            println!("{short_name} running (pid {pid}). Press Ctrl+C to stop.");
        }

        let run_stage = ctx.start_stage("run");
        let exit_status = child.wait().await;
        ctx.finish_stage(
            run_stage,
            exit_status
                .as_ref()
                .is_ok_and(std::process::ExitStatus::success),
        );
        let exit_status = exit_status?;

        let run_result = RunResult {
            binary: package.to_string(),
            pid: Some(pid),
            instance_id: Some(instance_id.to_string()),
            status: if exit_status.success() {
                "success".to_string()
            } else {
                "failed".to_string()
            },
        };

        if exit_status.success() {
            Ok(CommandResult::success()
                .with_message(format!("{package} exited successfully"))
                .with_data(serde_json::to_value(&run_result)?)
                .with_duration(ctx.elapsed()))
        } else {
            Ok(CommandResult::failure(crate::output::StructuredError {
                code: "RUN_FAILED".to_string(),
                message: format!("{package} exited with error"),
                location: Some("run".to_string()),
                suggestion: Some(local_run_failure_suggestion(journal_path.as_deref())),
            })
            .with_data(serde_json::to_value(&run_result)?)
            .with_duration(ctx.elapsed()))
        }
    }

    async fn run_background(
        &self,
        package: &str,
        binary: &str,
        instance_id: &str,
        automaton: Option<&str>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        let cfg = config();
        let manager = JobManager::new(cfg.jobs_dir())?;
        self.build_packages(&[package], ctx).await?;

        let binary_command = target_binary_path(self.release, binary)
            .to_string_lossy()
            .into_owned();
        let args = runtime_cli_args(package, instance_id, automaton);
        let runtime_env = self.local_run_env_vars();
        let runtime = self.local_runtime_coordinates()?;

        let job = manager.spawn_with_env_without_watchdog(&binary_command, &args, &runtime_env)?;

        Ok(CommandResult::success()
            .with_message(format!("Backgrounded {package} as job {}", job.id))
            .with_data(serde_json::json!({
                "job_id": job.id,
                "package": package,
                "instance_id": instance_id,
                "runtime": runtime,
            }))
            .with_duration(ctx.elapsed()))
    }

    async fn run_watch(
        &self,
        package: &str,
        _binary: &str,
        instance_id: &str,
        automaton: Option<&str>,
        ctx: &CommandContext,
    ) -> Result<CommandResult> {
        if ctx.is_human() {
            println!("Watch mode: {package} (instance: {instance_id})");
            println!("Press Ctrl+C to stop.\n");
        }

        let workspace_root = crate::config::workspace_root();
        let workspace_utf8 = camino::Utf8PathBuf::from_path_buf(workspace_root.clone())
            .map_err(|p| eyre!("workspace root is not valid UTF-8: {}", p.display()))?;

        // Build extra args for this binary type
        let mut extra_args = Vec::new();
        append_binary_extra_args(&mut extra_args, package, instance_id, automaton);

        let args = RunArgs {
            binary: package.to_string(),
            release: self.release,
            no_watch: false,
            tether: None,
            checkpoint: None,
            args: extra_args,
            env_vars: self.local_run_env_vars(),
        };

        let mut orchestrator = DevOrchestrator::new(args, workspace_utf8);
        self.maybe_spawn_metrics_overlay(ctx);
        let watch_stage = ctx.start_stage("watch");
        let result = orchestrator.run().await;
        ctx.finish_stage(watch_stage, result.is_ok());
        result?;

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
        for name in CORE_TARGETS {
            let (_, package, _, _) = lookup_binary(name).expect("core target must exist");
            println!("  {name:<25} ({package})");
        }

        println!("\nSources:");
        for name in SOURCE_TARGETS {
            let (_, package, _, _) = lookup_binary(name).expect("source target must exist");
            println!("  {name:<25} ({package})");
        }

        println!("\nAutomatons:");
        for name in AUTOMATON_TARGETS {
            let (_, package, _, _) = lookup_binary(name).expect("automaton target must exist");
            println!("  {name:<25} ({package})");
        }

        println!("\nBundles:");
        println!("  {:<25} {}", "core", CORE_TARGETS.join(", "));
        println!(
            "  {:<25} dev source bindings manifest (default excludes: {})",
            "all-sources",
            DEFAULT_EXCLUDED_ALL_SOURCE_BINDINGS.join(", ")
        );
        println!(
            "  {:<25} {}",
            "all-automatons",
            AUTOMATON_TARGETS.join(", ")
        );

        println!("\nSpecial:");
        println!(
            "  {:<25} Connect to remote NATS via The Tether",
            "tether <target>"
        );
        println!(
            "  {:<25} Managed oneshot scan surface (use systemd / NixOS, not xtask run)",
            "document-scan"
        );
    }

    for (name, package, binary, automaton) in BINARIES {
        binaries.push(serde_json::json!({
            "name": name,
            "package": package,
            "binary": binary,
            "automaton": automaton,
        }));
    }

    CommandResult::success()
        .with_data(serde_json::json!({
            "binaries": binaries,
            "bundles": ["core", "all-sources", "all-automatons"],
            "special": ["tether", "document-scan"]
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
    config.from_sequence = from_sequence;

    if ctx.is_human() {
        println!("Connecting to {target} via The Tether...");
        println!("  Gateway: {}", config.gateway_url);
        if let Some(ref f) = config.subject_filter {
            println!("  Filter: {f}");
        }
        if let Some(sequence) = config.from_sequence {
            println!("  Starting from: stream sequence {sequence}");
        } else if from_beginning {
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
                event.payload
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
#[path = "run_test.rs"]
mod tests;
