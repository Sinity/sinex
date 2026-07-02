//! Infra command - infrastructure management.

use clap::Subcommand;
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config as xtask_config;
use crate::history::JobLifecycleStatus;
use crate::infra::flake_stage::stage_checkout_for_flake;
use crate::infra::stack::{self, AllCheckoutsStatus, StackConfig, StackStatus};
use crate::infra::state::CheckoutState;
use crate::jobs::JobManager;
use crate::runtime_target::{
    checkout_dev_gateway_url, checkout_runtime_target, checkout_runtime_target_path,
    checkout_runtime_target_token_file,
};

/// Infra command - manages the isolated development environment.
pub struct InfraCommand {
    pub subcommand: InfraSubcommand,
}

#[derive(Subcommand)]
pub enum InfraSubcommand {
    /// Start the infrastructure
    Start {
        /// Start all processes
        #[arg(long)]
        all: bool,
        /// Specific processes to start
        processes: Vec<String>,
    },
    /// Stop the infrastructure
    Stop {
        /// Stop/clean every checkout-local dev-state root under /var/cache/sinex/$USER
        #[arg(long)]
        all_checkouts: bool,
        /// Only remove stale/malformed lock and PID files; do not stop live processes
        #[arg(long)]
        stale_only: bool,
        /// Print planned actions without stopping processes or removing files
        #[arg(long)]
        dry_run: bool,
        /// Specific processes to stop
        processes: Vec<String>,
    },
    /// Run the explicit local devshell/runtime lifecycle smoke
    Smoke {
        /// Print the smoke plan and current coordinates without starting or stopping services
        #[arg(long)]
        dry_run: bool,
        /// Stop current-checkout infra before the smoke if it is already running
        #[arg(long)]
        reset_first: bool,
        /// Skip the explicit infra start/stop phase and only verify read-only probes
        #[arg(long)]
        skip_start: bool,
        /// Start a managed local sinexd job, observe it in infra status, and cancel it
        #[arg(long)]
        run_core: bool,
    },
    /// Show infrastructure status
    Status {
        /// Watch mode
        #[arg(long, short)]
        watch: bool,
        /// Show every checkout-local dev-state root under /var/cache/sinex/$USER
        #[arg(long)]
        all_checkouts: bool,
    },
    /// View logs
    Logs {
        /// Process name
        #[arg(value_name = "PROCESS", default_value = "all")]
        process: String,
        /// Lines to show
        #[arg(long, short, default_value_t = 50)]
        lines: usize,
        /// Follow output
        #[arg(long, short)]
        follow: bool,
    },
    /// Apply the declarative schema to a database
    SchemaApply {
        /// Target database URL. Falls back to DATABASE_URL, then the current checkout stack.
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
    },
    /// Generate gateway TLS certificates using rcgen
    TlsInitGateway {
        /// Output directory for generated files
        #[arg(long, default_value = "/var/lib/sinex/tls")]
        output_dir: PathBuf,
        /// Subject alternative name to include. Repeat for multiple SANs.
        #[arg(long = "san", value_name = "SAN")]
        san: Vec<String>,
        /// Common name for the generated certificate authority
        #[arg(long, default_value = "Sinex Gateway CA")]
        ca_name: String,
        /// Certificate validity in days
        #[arg(long, default_value_t = crate::tls::DEFAULT_DEV_CERT_VALIDITY_DAYS)]
        validity_days: u32,
        /// Overwrite an existing certificate set
        #[arg(long)]
        force: bool,
    },
    /// Manage VM integration
    Vm {
        #[command(subcommand)]
        cmd: crate::commands::vm::VmSubcommand,
    },
    /// Stage a flake-safe checkout copy for local Nix builds and deploys
    FlakeStage {
        /// Output directory for the staged checkout. Defaults to a unique /tmp path.
        #[arg(long)]
        output_dir: Option<PathBuf>,
        /// Replace an existing output directory instead of failing.
        #[arg(long)]
        force: bool,
    },
    /// Generate the dogfood dev-loop source-bindings manifest
    DevBindings {
        /// Output manifest path. Defaults to .agent/dev/dev-source-bindings.json.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Print the manifest JSON to stdout instead of writing a file.
        #[arg(long, conflicts_with = "check")]
        stdout: bool,
        /// Exit non-zero if the output file differs from the generated manifest.
        #[arg(long)]
        check: bool,
        /// Root to watch and scan for git/fs sources. Defaults to the workspace root.
        #[arg(long)]
        watch_root: Option<PathBuf>,
        /// Include only the named source id. Repeat for multiple sources.
        #[arg(
            long = "source",
            value_name = "SOURCE_ID",
            conflicts_with = "exclude_source"
        )]
        source: Vec<String>,
        /// Exclude the named source id from the generated manifest. Repeat for multiple sources.
        #[arg(long = "exclude-source", value_name = "SOURCE_ID")]
        exclude_source: Vec<String>,
    },
    /// Write the checkout-local runtime target descriptor for sinexctl/MCP clients
    RuntimeTarget {
        /// Output descriptor path. Defaults to .sinex/state/runtime-target.json.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Print the descriptor JSON to stdout instead of writing a file.
        #[arg(long)]
        stdout: bool,
    },
}

impl XtaskCommand for InfraCommand {
    fn name(&self) -> &'static str {
        "infra"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            InfraSubcommand::Start { all, processes } => {
                let config = StackConfig::for_current_checkout()?;
                execute_start(&config, *all, processes, ctx)
            }
            InfraSubcommand::Stop {
                all_checkouts,
                stale_only,
                dry_run,
                processes,
            } => {
                let config = StackConfig::for_current_checkout()?;
                execute_stop(
                    config,
                    *all_checkouts,
                    *stale_only,
                    *dry_run,
                    processes,
                    ctx,
                )
            }
            InfraSubcommand::Smoke {
                dry_run,
                reset_first,
                skip_start,
                run_core,
            } => {
                let config = StackConfig::for_current_checkout()?;
                execute_smoke(&config, *dry_run, *reset_first, *skip_start, *run_core, ctx)
            }
            InfraSubcommand::Status {
                watch,
                all_checkouts,
            } => {
                let config = StackConfig::for_current_checkout()?;
                execute_status(&config, *watch, *all_checkouts, ctx).await
            }
            InfraSubcommand::Logs {
                process,
                lines,
                follow,
            } => {
                let config = StackConfig::for_current_checkout()?;
                execute_logs(&config, process, *lines, *follow, ctx)
            }
            InfraSubcommand::SchemaApply { database_url } => {
                execute_schema_apply(database_url.as_deref(), ctx)
            }
            InfraSubcommand::TlsInitGateway {
                output_dir,
                san,
                ca_name,
                validity_days,
                force,
            } => execute_tls_init_gateway(output_dir, san, ca_name, *validity_days, *force, ctx),
            InfraSubcommand::Vm { cmd } => {
                let vm_cmd = crate::commands::vm::VmCommand {
                    subcommand: cmd.clone(),
                };
                vm_cmd.execute(ctx).await
            }
            InfraSubcommand::FlakeStage { output_dir, force } => {
                execute_flake_stage(output_dir.as_deref(), *force, ctx)
            }
            InfraSubcommand::DevBindings {
                output,
                stdout,
                check,
                watch_root,
                source,
                exclude_source,
            } => execute_dev_bindings(
                output.as_deref(),
                *stdout,
                *check,
                watch_root.as_deref(),
                source,
                exclude_source,
                ctx,
            ),
            InfraSubcommand::RuntimeTarget { output, stdout } => {
                execute_runtime_target(output.as_deref(), *stdout, ctx)
            }
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

fn resolve_database_url(database_url: Option<&str>) -> Result<String> {
    if let Some(database_url) = database_url {
        return Ok(database_url.to_owned());
    }

    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        return Ok(database_url);
    }

    Ok(StackConfig::for_current_checkout()?.database_url())
}

// ─────────────────────────────────────────────────────────────────────────────
// Implementations
// ─────────────────────────────────────────────────────────────────────────────

fn execute_start(
    config: &StackConfig,
    all: bool,
    processes: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra start");

    // Validate process names before starting anything
    for p in processes {
        if p != "postgres" && p != "nats" {
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "UNKNOWN_PROCESS".to_string(),
                message: format!("unknown process: {p}"),
                location: Some("infra::start".to_string()),
                suggestion: Some("valid processes: postgres, nats".to_string()),
            }));
        }
    }

    let start_pg = all || processes.is_empty() || processes.iter().any(|p| p == "postgres");
    let start_nats = all || processes.is_empty() || processes.iter().any(|p| p == "nats");

    // Check lock
    let checkout_state = CheckoutState::for_current_checkout()?;
    if let Some(lock_info) = checkout_state.is_locked_by_other()? {
        let pid = lock_info.pid;
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "INFRA_LOCKED".to_string(),
            message: format!("Infra locked by {pid}"),
            location: Some("infra::start".to_string()),
            suggestion: Some(format!("Stop running instance: kill {pid}")),
        }));
    }

    let _lock = checkout_state.acquire_lock(Some("infra".into()))?;
    std::mem::forget(_lock);

    stack::ensure_directories(config)?;

    let verbose = ctx.is_human();

    // Parallelize independent Postgres and NATS startup.
    // NATS has zero dependency on Postgres — run them concurrently.
    std::thread::scope(|s| -> Result<()> {
        // Spawn NATS startup in background thread
        let nats_handle = if start_nats {
            Some(s.spawn(|| -> Result<()> {
                stack::nats_generate_config(config, verbose)?;
                stack::nats_start(config, verbose)
            }))
        } else {
            None
        };

        // Postgres chain runs in the foreground (critical path)
        if start_pg {
            if config.annex.enable {
                stack::annex_init(config, verbose)?;
            }

            stack::pg_init(config, verbose)?;
            stack::pg_start(config, verbose)?;
            stack::pg_setup_database(config, verbose)?;

            // Skip schema apply when declarative sources haven't changed since last apply
            if crate::preflight::schema_changed_since_last_apply() {
                stack::pg_apply_schema(config, verbose)?;
                crate::preflight::record_schema_applied();
            }
        }

        // Collect NATS result
        if let Some(handle) = nats_handle {
            let nats_result = handle
                .join()
                .map_err(|_| eyre!("NATS startup thread panicked"))?;
            nats_result?;
        }
        Ok(())
    })?;

    let pg_port = config.postgres.port;
    let nats_port = config.nats.port;
    let mut result = CommandResult::success().with_message("Infra started");
    if start_pg {
        result = result.with_detail(format!("Postgres on port {pg_port}"));
    }
    if start_nats {
        result = result.with_detail(format!("NATS on port {nats_port}"));
    }
    Ok(result)
}

fn execute_schema_apply(database_url: Option<&str>, ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("infra schema-apply");

    let database_url = resolve_database_url(database_url)?;
    stack::apply_schema_for_database_url(&database_url, ctx.is_human())?;

    Ok(CommandResult::success().with_message("Schema applied"))
}

fn execute_tls_init_gateway(
    output_dir: &Path,
    san: &[String],
    ca_name: &str,
    validity_days: u32,
    force: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra tls-init-gateway");

    let san = if san.is_empty() {
        vec!["localhost".to_string(), "127.0.0.1".to_string()]
    } else {
        san.to_vec()
    };

    let data = crate::tls::generate_dev_certs(&crate::tls::CertConfig {
        output_dir: output_dir.to_path_buf(),
        san: san.clone(),
        ca_name: ca_name.to_string(),
        validity_days,
        force,
    })?;

    let mut result = CommandResult::success()
        .with_message("Gateway TLS initialized")
        .with_data(data)
        .with_detail(format!("Output directory: {}", output_dir.display()));
    for san_entry in san {
        result = result.with_detail(format!("SAN: {san_entry}"));
    }
    Ok(result)
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeTargetWriteResult {
    descriptor_path: Option<PathBuf>,
    token_file: PathBuf,
    gateway_url: String,
}

fn execute_runtime_target(
    output: Option<&Path>,
    stdout: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra runtime-target");

    let token_file = checkout_runtime_target_token_file();
    if let Some(parent) = token_file.parent() {
        std::fs::create_dir_all(parent)
            .wrap_err_with(|| format!("create token directory {}", parent.display()))?;
    }
    let token = crate::preflight::default_dev_rpc_token();
    std::fs::write(&token_file, format!("{token}\n"))
        .wrap_err_with(|| format!("write dev API token {}", token_file.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o600))
            .wrap_err_with(|| format!("chmod dev API token {}", token_file.display()))?;
    }

    let target = checkout_runtime_target(xtask_config())?;
    let json = serde_json::to_string_pretty(&target)?;
    let descriptor_path = if stdout {
        println!("{json}");
        None
    } else {
        let path = output
            .map(Path::to_path_buf)
            .unwrap_or_else(checkout_runtime_target_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .wrap_err_with(|| format!("create descriptor directory {}", parent.display()))?;
        }
        std::fs::write(&path, format!("{json}\n"))
            .wrap_err_with(|| format!("write runtime target {}", path.display()))?;
        Some(path)
    };

    let data = RuntimeTargetWriteResult {
        descriptor_path: descriptor_path.clone(),
        token_file,
        gateway_url: target
            .gateway
            .base_url
            .clone()
            .unwrap_or_else(|| checkout_dev_gateway_url().to_string()),
    };
    let mut result = CommandResult::success()
        .with_message("Runtime target descriptor ready")
        .with_data(json!(data));
    if let Some(path) = descriptor_path {
        result = result.with_detail(format!(
            "Use: sinexctl --runtime-target {} <command>",
            path.display()
        ));
    }
    Ok(result)
}

#[derive(Debug, Clone, Serialize)]
struct FlakeStageResult {
    staged_root: String,
    flake_uri: String,
    copied_dirs: usize,
    copied_files: usize,
    copied_symlinks: usize,
    excluded_count: usize,
    unsupported_count: usize,
    excluded_paths: Vec<String>,
    unsupported_paths: Vec<String>,
}

fn execute_flake_stage(
    output_dir: Option<&Path>,
    force: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra flake-stage");

    let report = stage_checkout_for_flake(&crate::config::workspace_root(), output_dir, force)?;
    let result = FlakeStageResult {
        staged_root: report.staged_root.clone(),
        flake_uri: report.flake_uri.clone(),
        copied_dirs: report.copied_dirs,
        copied_files: report.copied_files,
        copied_symlinks: report.copied_symlinks,
        excluded_count: report.excluded_paths.len(),
        unsupported_count: report.unsupported_paths.len(),
        excluded_paths: report.excluded_paths.clone(),
        unsupported_paths: report.unsupported_paths.clone(),
    };

    let mut command_result = CommandResult::success()
        .with_message("Flake-safe checkout staged")
        .with_detail(format!("Stage root: {}", report.staged_root))
        .with_detail(format!("Flake URI: {}", report.flake_uri))
        .with_detail(format!(
            "Copied {} directories, {} files, {} symlinks",
            report.copied_dirs, report.copied_files, report.copied_symlinks
        ))
        .with_detail(format!(
            "Excluded {} paths and skipped {} unsupported entries",
            report.excluded_paths.len(),
            report.unsupported_paths.len()
        ))
        .with_data(serde_json::to_value(result)?)
        .with_duration(ctx.elapsed());

    if !report.unsupported_paths.is_empty() {
        command_result = command_result.with_warning(format!(
            "Skipped unsupported filesystem entries: {}",
            report.unsupported_paths.join(", ")
        ));
    }

    Ok(command_result)
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DevSourceBindingsManifest {
    #[serde(rename = "_comment")]
    comment: String,
    bindings: Vec<DevSourceBinding>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DevSourceBinding {
    source_id: String,
    instance_idx: u32,
    service_name: String,
    runtime_config: Value,
    extra_args: Vec<String>,
    extra_env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DevBindingsResult {
    output: Option<String>,
    env: String,
    binding_count: usize,
    sources: Vec<String>,
    manifest: DevSourceBindingsManifest,
}

fn default_dev_bindings_output_path() -> PathBuf {
    crate::config::workspace_root()
        .join(".agent")
        .join("dev")
        .join("dev-source-bindings.json")
}

fn dev_source_binding(
    source_id: &str,
    instance_idx: u32,
    runtime_config: Value,
) -> DevSourceBinding {
    DevSourceBinding {
        source_id: source_id.to_string(),
        instance_idx,
        service_name: format!("source-driver-{source_id}-{instance_idx}"),
        runtime_config,
        extra_args: Vec::new(),
        extra_env: BTreeMap::new(),
    }
}

struct BrowserSqliteDevSource {
    path: PathBuf,
    query: &'static str,
    table: &'static str,
}

impl BrowserSqliteDevSource {
    fn qutebrowser_native(home: &Path) -> Self {
        Self {
            path: home.join(".local/share/qutebrowser/history.sqlite"),
            query: "SELECT rowid, * FROM History",
            table: "History",
        }
    }

    fn qutebrowser_webengine(home: &Path) -> Self {
        Self::chromium(home.join(".local/share/qutebrowser/webengine/History"))
    }

    fn chrome_workspace(home: &Path) -> Self {
        Self::chromium(home.join(".config/chrome-ws/Default/History"))
    }

    fn chromium(path: PathBuf) -> Self {
        Self {
            path,
            query: "SELECT visits.id AS rowid, urls.url AS url, urls.title AS title, \
                    visits.visit_time AS visit_time, \
                    visits.external_referrer_url AS external_referrer_url, \
                    visits.transition AS transition, \
                    visits.visit_duration AS visit_duration \
                    FROM visits JOIN urls ON visits.url = urls.id",
            table: "visits",
        }
    }
}

fn generate_dev_source_bindings_manifest(watch_root: &Path) -> DevSourceBindingsManifest {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/"));
    generate_dev_source_bindings_manifest_for_home_and_exports(
        watch_root,
        &home,
        default_browser_history_dump_path().as_deref(),
        default_raindrop_bookmarks_export_path().as_deref(),
    )
}

#[cfg(test)]
fn generate_dev_source_bindings_manifest_for_home(
    watch_root: &Path,
    home: &Path,
) -> DevSourceBindingsManifest {
    generate_dev_source_bindings_manifest_for_home_and_exports(watch_root, home, None, None)
}

fn default_browser_history_dump_path() -> Option<PathBuf> {
    let path = PathBuf::from("/realm/data/captures/webhistory/gestalt/derived/full_history.ndjson");
    path.exists().then_some(path)
}

fn default_raindrop_bookmarks_export_path() -> Option<PathBuf> {
    let path = PathBuf::from("/realm/data/exports/raindrop/processed/bookmarks.csv");
    path.exists().then_some(path)
}

fn generate_dev_source_bindings_manifest_for_home_and_exports(
    watch_root: &Path,
    home: &Path,
    browser_history_dump: Option<&Path>,
    raindrop_bookmarks_export: Option<&Path>,
) -> DevSourceBindingsManifest {
    let zsh_history = home.join(".zsh_history");
    let atuin_history = home.join(".local/share/atuin/history.db");
    let browser_sqlite_sources = [
        BrowserSqliteDevSource::qutebrowser_native(home),
        BrowserSqliteDevSource::qutebrowser_webengine(home),
        BrowserSqliteDevSource::chrome_workspace(home),
    ];
    let watch_root = watch_root.to_string_lossy().to_string();

    let mut bindings = Vec::new();
    if zsh_history.exists() {
        bindings.push(dev_source_binding(
            "terminal.zsh-history",
            1,
            json!({
                "path": zsh_history,
                "skip_empty": true,
            }),
        ));
    }
    bindings.push(dev_source_binding(
        "terminal.atuin-history",
        1,
        json!({
            "path": atuin_history,
            "query": "history",
            "table": "history",
            "immutable": false,
            "read_only": false,
        }),
    ));
    for (idx, browser_source) in browser_sqlite_sources
        .into_iter()
        .filter(|source| source.path.exists())
        .enumerate()
    {
        bindings.push(dev_source_binding(
            "browser.history",
            (idx + 1) as u32,
            json!({
                "primary": {
                    "path": browser_source.path,
                    "query": browser_source.query,
                    "table": browser_source.table,
                    // qutebrowser keeps history.sqlite in WAL mode with a live
                    // writer; Chrome/Chromium does the same. SQLite may need
                    // to recover/open WAL sidecars even for SELECT-only
                    // readers, so mirror the NixOS source binding's WAL-safe
                    // mode here.
                    "read_only": false,
                    "immutable": false
                },
                "secondary": {
                    "path": browser_history_dump.unwrap_or_else(|| Path::new("")),
                    "skip_empty": true
                },
                "interleaved": false
            }),
        ));
    }
    if let Some(raindrop_bookmarks_export) = raindrop_bookmarks_export {
        bindings.push(dev_source_binding(
            "raindrop-bookmarks",
            1,
            json!({
                "path": raindrop_bookmarks_export,
                "source_identifier": "raindrop-bookmarks",
            }),
        ));
    }
    bindings.push(dev_source_binding(
        "git-commit-history",
        1,
        json!({
            "path": watch_root,
            "continuous_poll_interval_secs": 30,
        }),
    ));
    bindings.push(dev_source_binding(
        "fs",
        1,
        json!({
            "watch_paths": [watch_root],
            "recursive": true,
            "ignored_directory_names": [
                "target",
                ".git",
                ".sinex",
                ".direnv",
                ".claude",
                "node_modules",
                "result",
            ],
            "ignored_file_suffixes": [
                "-wal",
                "-shm",
                "-journal",
                ".tmp",
                ".swp",
                ".swo",
                "~",
                ".lock",
                ".o",
                ".d",
                ".rmeta",
            ],
            "ignored_file_substrings": [
                ".tmp.",
                ".swp",
                ".swx",
                ".goutputstream-",
            ],
            "max_capture_bytes": 1048576,
        }),
    ));
    bindings.push(dev_source_binding(
        "system.journald",
        1,
        json!({
            "units": [],
            "start_at_now_without_cursor": true,
        }),
    ));

    DevSourceBindingsManifest {
        comment: "Generated by `xtask infra dev-bindings`. Point SINEX_SOURCE_BINDINGS_PATH at this file before `xtask run core` to run the fast dogfood dev loop with real terminal/git/fs/journald/browser sources when their local materials exist.".to_string(),
        bindings,
    }
}

fn filter_dev_source_bindings_manifest(
    mut manifest: DevSourceBindingsManifest,
    include_sources: &[String],
    exclude_sources: &[String],
) -> Result<DevSourceBindingsManifest> {
    if include_sources.is_empty() && exclude_sources.is_empty() {
        return Ok(manifest);
    }

    let available = manifest
        .bindings
        .iter()
        .map(|binding| binding.source_id.as_str())
        .collect::<BTreeSet<_>>();
    validate_dev_binding_filter("source", include_sources, &available)?;
    validate_dev_binding_filter("exclude-source", exclude_sources, &available)?;

    if !include_sources.is_empty() {
        let include = include_sources
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        manifest
            .bindings
            .retain(|binding| include.contains(binding.source_id.as_str()));
    }
    if !exclude_sources.is_empty() {
        let exclude = exclude_sources
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        manifest
            .bindings
            .retain(|binding| !exclude.contains(binding.source_id.as_str()));
    }

    Ok(manifest)
}

fn validate_dev_binding_filter(
    flag: &str,
    requested: &[String],
    available: &BTreeSet<&str>,
) -> Result<()> {
    let unknown = requested
        .iter()
        .filter(|source| !available.contains(source.as_str()))
        .map(String::as_str)
        .collect::<Vec<_>>();
    if unknown.is_empty() {
        return Ok(());
    }

    let available = available.iter().copied().collect::<Vec<_>>().join(", ");
    Err(eyre!(
        "unknown --{flag} value(s): {}; available dev sources: {}",
        unknown.join(", "),
        available
    ))
}

fn execute_dev_bindings(
    output: Option<&Path>,
    stdout: bool,
    check: bool,
    watch_root: Option<&Path>,
    include_sources: &[String],
    exclude_sources: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra dev-bindings");

    let output = output
        .map(Path::to_path_buf)
        .unwrap_or_else(default_dev_bindings_output_path);
    let watch_root = watch_root
        .map(Path::to_path_buf)
        .unwrap_or_else(crate::config::workspace_root);
    let manifest = filter_dev_source_bindings_manifest(
        generate_dev_source_bindings_manifest(&watch_root),
        include_sources,
        exclude_sources,
    )?;
    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    let sources = manifest
        .bindings
        .iter()
        .map(|binding| binding.source_id.clone())
        .collect::<Vec<_>>();

    if stdout {
        println!("{manifest_json}");
        return Ok(CommandResult::success()
            .with_message("Dev source-bindings manifest generated")
            .with_silent()
            .with_duration(ctx.elapsed()));
    }

    if check {
        let existing = std::fs::read_to_string(&output).with_context(|| {
            format!("read dev source-bindings manifest at {}", output.display())
        })?;
        if existing.trim_end() != manifest_json {
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "DEV_BINDINGS_STALE".to_string(),
                message: format!("{} is not up to date", output.display()),
                location: Some("infra::dev-bindings".to_string()),
                suggestion: Some(format!(
                    "run `xtask infra dev-bindings --output {}`",
                    output.display()
                )),
            }));
        }
        return Ok(CommandResult::success()
            .with_message("Dev source-bindings manifest is up to date")
            .with_detail(format!("Output: {}", output.display()))
            .with_data(serde_json::to_value(DevBindingsResult {
                output: Some(output.display().to_string()),
                env: format!("SINEX_SOURCE_BINDINGS_PATH={}", output.display()),
                binding_count: manifest.bindings.len(),
                sources,
                manifest,
            })?)
            .with_duration(ctx.elapsed()));
    }

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dev source-bindings dir {}", parent.display()))?;
    }
    std::fs::write(&output, format!("{manifest_json}\n"))
        .with_context(|| format!("write dev source-bindings manifest at {}", output.display()))?;

    Ok(CommandResult::success()
        .with_message("Dev source-bindings manifest written")
        .with_detail(format!("Output: {}", output.display()))
        .with_detail(format!(
            "Run with: SINEX_SOURCE_BINDINGS_PATH={} xtask run core",
            output.display()
        ))
        .with_data(serde_json::to_value(DevBindingsResult {
            output: Some(output.display().to_string()),
            env: format!("SINEX_SOURCE_BINDINGS_PATH={}", output.display()),
            binding_count: manifest.bindings.len(),
            sources,
            manifest,
        })?)
        .with_duration(ctx.elapsed()))
}

fn execute_stop(
    config: StackConfig,
    all_checkouts: bool,
    stale_only: bool,
    dry_run: bool,
    processes: &[String],
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra stop");

    for process in processes {
        if process != "postgres" && process != "nats" {
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "UNKNOWN_PROCESS".to_string(),
                message: format!("unknown process: {process}"),
                location: Some("infra::stop".to_string()),
                suggestion: Some("valid processes: postgres, nats".to_string()),
            }));
        }
    }

    if all_checkouts {
        if !processes.is_empty() {
            bail!("infra stop --all-checkouts does not accept process names");
        }
        return execute_all_checkouts_stop(stale_only, dry_run, ctx);
    }
    if stale_only {
        bail!("infra stop --stale-only requires --all-checkouts");
    }
    if dry_run {
        bail!("infra stop --dry-run requires --all-checkouts");
    }

    let stop_pg = processes.is_empty() || processes.iter().any(|p| p == "postgres");
    let stop_nats = processes.is_empty() || processes.iter().any(|p| p == "nats");

    if stop_nats {
        stack::nats_stop(&config, ctx.is_human())?;
    }
    if stop_pg {
        stack::pg_stop(&config, ctx.is_human())?;
    }

    let status = StackStatus::gather(&config);
    if !status.postgres.running && !status.nats.running {
        let checkout_state = CheckoutState::for_current_checkout()?;
        checkout_state.release_lock()?;
    }

    let message = if processes.is_empty() {
        "Infra stopped".to_string()
    } else {
        format!("Infra stopped: {}", processes.join(", "))
    };
    Ok(CommandResult::success().with_message(message))
}

fn execute_all_checkouts_stop(
    stale_only: bool,
    dry_run: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let base_dir = CheckoutState::default_inventory_base_dir();
    let roots = CheckoutState::inventory_roots_under(&base_dir)?;
    let cleanup = stack::AllCheckoutsCleanup::run(base_dir, roots, dry_run, stale_only)?;

    if ctx.is_human() {
        println!("sinex-dev infra cleanup: all checkouts");
        println!("────────────────────────────────────────");
        println!(
            "Checkouts: {}  actions: {}  skipped: {}",
            cleanup.totals.checkouts, cleanup.totals.actions, cleanup.totals.skipped
        );
        println!(
            "Stopped:   postgres={} nats={} sinexd={}",
            cleanup.totals.stopped_postgres,
            cleanup.totals.stopped_nats,
            cleanup.totals.stopped_sinexd
        );
        println!(
            "Removed:   files={}{}{}",
            cleanup.totals.removed_files,
            if cleanup.stale_only {
                " (stale-only)"
            } else {
                ""
            },
            if cleanup.dry_run { " (dry-run)" } else { "" }
        );
        for checkout in &cleanup.checkouts {
            if checkout.actions.is_empty() && checkout.skipped.is_empty() {
                continue;
            }
            println!();
            println!("{}", checkout.cache_root.display());
            if let Some(path) = &checkout.checkout_path {
                println!("  checkout: {}", path.display());
            }
            for action in &checkout.actions {
                println!(
                    "  action:   {:?} {}{}",
                    action.action,
                    action.target.display(),
                    if action.dry_run { " (dry-run)" } else { "" }
                );
            }
            for skipped in &checkout.skipped {
                println!("  skipped:  {skipped}");
            }
        }
    }

    let mut result = CommandResult::success()
        .with_message(if dry_run {
            "All-checkout infra cleanup dry-run complete"
        } else {
            "All-checkout infra cleanup complete"
        })
        .with_data(serde_json::to_value(&cleanup)?);
    for warning in &cleanup.warnings {
        result = result.with_warning(warning.clone());
    }
    Ok(result)
}

#[derive(Debug, Serialize)]
struct InfraSmokeReport {
    checkout_root: String,
    dev_state_dir: String,
    database_url: String,
    nats_url: String,
    dry_run: bool,
    reset_first: bool,
    skip_start: bool,
    run_core: bool,
    steps: Vec<InfraSmokeStep>,
    baseline: InfraSmokeSnapshot,
    final_state: InfraSmokeSnapshot,
    all_checkouts: AllCheckoutsStatus,
    service_mode_decision: InfraServiceModeDecision,
}

#[derive(Debug, Clone, Serialize)]
struct InfraSmokeStep {
    name: String,
    command: Vec<String>,
    status: String,
    detail: String,
}

#[derive(Debug, Clone, Serialize)]
struct InfraSmokeSnapshot {
    postgres: String,
    nats: String,
    sinexd: String,
    rss_bytes: u64,
    state_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
struct InfraServiceModeDecision {
    selected_default: &'static str,
    reason: &'static str,
    shared_service_status: &'static str,
    hybrid_status: &'static str,
    correctness_notes: Vec<&'static str>,
}

fn execute_smoke(
    config: &StackConfig,
    dry_run: bool,
    reset_first: bool,
    skip_start: bool,
    run_core: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("infra smoke");

    let mut steps = Vec::new();
    let baseline = smoke_snapshot(config);
    let mut current = StackStatus::gather(config);

    if reset_first {
        steps.push(InfraSmokeStep {
            name: "reset current checkout infra".to_string(),
            command: vec!["xtask".into(), "infra".into(), "stop".into()],
            status: if dry_run { "planned" } else { "ran" }.to_string(),
            detail: "current-checkout Postgres/NATS are stopped before the smoke".to_string(),
        });
        if !dry_run {
            stack::nats_stop(config, ctx.is_human())?;
            stack::pg_stop(config, ctx.is_human())?;
            CheckoutState::for_current_checkout()?.release_lock()?;
            current = StackStatus::gather(config);
        }
    }

    if services_running(&current) {
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "INFRA_SMOKE_NOT_STOPPED".to_string(),
            message: "current checkout infra is already running".to_string(),
            location: Some("infra::smoke".to_string()),
            suggestion: Some(
                "rerun with --reset-first, or stop current-checkout infra before the smoke"
                    .to_string(),
            ),
        })
        .with_data(serde_json::to_value(InfraSmokeReport {
            checkout_root: crate::config::workspace_root().display().to_string(),
            dev_state_dir: config.state_dir.display().to_string(),
            database_url: config.database_url(),
            nats_url: config.nats_url(),
            dry_run,
            reset_first,
            skip_start,
            run_core,
            steps,
            baseline,
            final_state: smoke_snapshot(config),
            all_checkouts: all_checkouts_status()?,
            service_mode_decision: service_mode_decision(),
        })?));
    }

    for (name, args) in read_only_smoke_commands() {
        let mut command = vec!["xtask".to_string()];
        command.extend(args.iter().map(ToString::to_string));
        steps.push(if dry_run {
            InfraSmokeStep {
                name: name.to_string(),
                command,
                status: "planned".to_string(),
                detail: "read-only probe should not start Postgres, NATS, or sinexd".to_string(),
            }
        } else {
            match run_xtask_probe(name, &args) {
                Ok(step) => step,
                Err(err) => {
                    stop_current_checkout_infra(config, ctx.is_human())?;
                    return Err(err);
                }
            }
        });

        let after_probe = StackStatus::gather(config);
        if services_running(&after_probe) {
            if !dry_run {
                stop_current_checkout_infra(config, ctx.is_human())?;
            }
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "INFRA_SMOKE_READ_ONLY_STARTED_SERVICE".to_string(),
                message: format!("read-only probe {name} started local infra"),
                location: Some("infra::smoke".to_string()),
                suggestion: Some(
                    "inspect the wrapper/command classification for this probe".to_string(),
                ),
            })
            .with_data(serde_json::to_value(InfraSmokeReport {
                checkout_root: crate::config::workspace_root().display().to_string(),
                dev_state_dir: config.state_dir.display().to_string(),
                database_url: config.database_url(),
                nats_url: config.nats_url(),
                dry_run,
                reset_first,
                skip_start,
                run_core,
                steps,
                baseline,
                final_state: smoke_snapshot(config),
                all_checkouts: all_checkouts_status()?,
                service_mode_decision: service_mode_decision(),
            })?));
        }
    }

    if !skip_start {
        steps.push(InfraSmokeStep {
            name: "explicit infra start".to_string(),
            command: vec!["xtask".into(), "infra".into(), "start".into()],
            status: if dry_run { "planned" } else { "ran" }.to_string(),
            detail: "Postgres/NATS may start only at this explicit phase".to_string(),
        });
        if !dry_run {
            if let Err(err) = execute_start(config, true, &[], ctx) {
                stop_current_checkout_infra(config, ctx.is_human())?;
                return Err(err);
            }
            let running = StackStatus::gather(config);
            if !running.postgres.running || !running.nats.running {
                stop_current_checkout_infra(config, ctx.is_human())?;
                return Ok(CommandResult::failure(crate::output::StructuredError {
                    code: "INFRA_SMOKE_START_FAILED".to_string(),
                    message: "explicit infra start did not bring up Postgres and NATS".to_string(),
                    location: Some("infra::smoke".to_string()),
                    suggestion: Some(
                        "inspect xtask infra status/logs for the current checkout".to_string(),
                    ),
                }));
            }
        }

        let run_dry_step = if dry_run {
            InfraSmokeStep {
                name: "local sinexd dry-run".to_string(),
                command: vec![
                    "xtask".into(),
                    "run".into(),
                    "core".into(),
                    "--dry-run".into(),
                ],
                status: "planned".to_string(),
                detail: "prints checkout-local DB/NATS/API coordinates without starting sinexd"
                    .to_string(),
            }
        } else {
            match run_xtask_probe("local sinexd dry-run", &["run", "core", "--dry-run"]) {
                Ok(step) => step,
                Err(err) => {
                    stop_current_checkout_infra(config, ctx.is_human())?;
                    return Err(err);
                }
            }
        };
        steps.push(run_dry_step);

        let after_dry_run = StackStatus::gather(config);
        if after_dry_run.sinexd.running {
            stop_current_checkout_infra(config, ctx.is_human())?;
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "INFRA_SMOKE_DRY_RUN_STARTED_SINEXD".to_string(),
                message: "xtask run core --dry-run started a local sinexd process".to_string(),
                location: Some("infra::smoke".to_string()),
                suggestion: Some("dry-run must remain an explicit non-runtime probe".to_string()),
            }));
        }

        if run_core {
            let runtime_step = if dry_run {
                InfraSmokeStep {
                    name: "managed local sinexd runtime".to_string(),
                    command: vec!["xtask".into(), "--bg".into(), "run".into(), "core".into()],
                    status: "planned".to_string(),
                    detail:
                        "would start a managed background sinexd job, observe it, and cancel it"
                            .to_string(),
                }
            } else {
                match run_managed_core_smoke(config) {
                    Ok(step) => step,
                    Err(err) => {
                        stop_current_checkout_infra(config, ctx.is_human())?;
                        return Err(err);
                    }
                }
            };
            steps.push(runtime_step);
        }

        steps.push(InfraSmokeStep {
            name: "explicit infra stop".to_string(),
            command: vec!["xtask".into(), "infra".into(), "stop".into()],
            status: if dry_run { "planned" } else { "ran" }.to_string(),
            detail: "current-checkout Postgres/NATS are stopped at the end of the smoke"
                .to_string(),
        });
        if !dry_run {
            stop_current_checkout_infra(config, ctx.is_human())?;
        }
    }

    let all_checkouts = all_checkouts_status()?;
    let final_state = smoke_snapshot(config);
    let report = InfraSmokeReport {
        checkout_root: crate::config::workspace_root().display().to_string(),
        dev_state_dir: config.state_dir.display().to_string(),
        database_url: config.database_url(),
        nats_url: config.nats_url(),
        dry_run,
        reset_first,
        skip_start,
        run_core,
        steps,
        baseline,
        final_state,
        all_checkouts,
        service_mode_decision: service_mode_decision(),
    };

    if ctx.is_human() {
        println!("Checkout:  {}", report.checkout_root);
        println!("Dev-state: {}", report.dev_state_dir);
        println!("Database:  {}", report.database_url);
        println!("NATS:      {}", report.nats_url);
        println!();
        println!("Smoke steps:");
        for step in &report.steps {
            println!("  {:<34} {:<8} {}", step.name, step.status, step.detail);
        }
        println!();
        println!(
            "Final: pg={} nats={} sinexd={} rss={}",
            report.final_state.postgres,
            report.final_state.nats,
            report.final_state.sinexd,
            format_bytes(report.final_state.rss_bytes)
        );
        println!(
            "All checkouts: {} roots, {} RSS, {} state",
            report.all_checkouts.totals.checkout_count,
            format_bytes(report.all_checkouts.totals.rss_bytes),
            format_bytes(report.all_checkouts.totals.state_bytes)
        );
    }

    let mut result = CommandResult::success()
        .with_message(if dry_run {
            "Infra smoke dry-run complete"
        } else {
            "Infra smoke complete"
        })
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed());
    if dry_run {
        result = result.with_warning("dry-run did not start or stop services".to_string());
    }
    Ok(result)
}

fn read_only_smoke_commands() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        ("current infra status", vec!["infra", "status"]),
        (
            "all-checkout infra status",
            vec!["infra", "status", "--all-checkouts"],
        ),
        ("run target list", vec!["run", "list"]),
        ("run core dry-run", vec!["run", "core", "--dry-run"]),
    ]
}

fn run_managed_core_smoke(config: &StackConfig) -> Result<InfraSmokeStep> {
    let manager = JobManager::new(xtask_config().jobs_dir())?;
    let current_exe = std::env::current_exe().wrap_err("failed to resolve current xtask binary")?;
    let args = vec![
        "--fg".to_string(),
        "--format".to_string(),
        "json".to_string(),
        "run".to_string(),
        "core".to_string(),
    ];
    let env_vars = crate::preflight::local_runtime_env_overrides();
    let job = manager.spawn_with_env(&current_exe.to_string_lossy(), &args, &env_vars)?;
    let job_id = job.id;

    match wait_for_sinexd_observed(config, Duration::from_secs(300)) {
        Ok(status) => {
            let cancel_result = manager.cancel(job_id);
            let stopped = wait_for_sinexd_stopped(config, Duration::from_secs(10));
            match (cancel_result, stopped) {
                (Ok(true), Ok(())) => Ok(InfraSmokeStep {
                    name: "managed local sinexd runtime".to_string(),
                    command: vec!["xtask".into(), "--bg".into(), "run".into(), "core".into()],
                    status: "passed".to_string(),
                    detail: format!(
                        "job {job_id} observed pids {:?} rss={}, then cancelled cleanly",
                        status.sinexd.pids,
                        format_bytes(status.sinexd.rss_bytes)
                    ),
                }),
                (Ok(false), _) => bail!(
                    "managed local sinexd smoke observed job {job_id}, but jobs cancel reported it was not running"
                ),
                (Err(error), _) => Err(error)
                    .with_context(|| format!("failed to cancel managed sinexd smoke job {job_id}")),
                (_, Err(error)) => Err(error).with_context(|| {
                    format!("managed sinexd smoke job {job_id} did not disappear after cancel")
                }),
            }
        }
        Err(error) => {
            let _ = manager.cancel(job_id);
            let status = manager
                .get(job_id)
                .ok()
                .flatten()
                .map(|job| job.job_status)
                .unwrap_or(JobLifecycleStatus::Orphaned);
            Err(error).with_context(|| {
                format!("managed sinexd smoke job {job_id} was not observable; last job status was {status:?}")
            })
        }
    }
}

fn wait_for_sinexd_observed(config: &StackConfig, timeout: Duration) -> Result<StackStatus> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let status = StackStatus::gather(config);
        if status.sinexd.running {
            return Ok(status);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    bail!(
        "timed out after {}s waiting for dev-local sinexd to appear in infra status",
        timeout.as_secs()
    )
}

fn wait_for_sinexd_stopped(config: &StackConfig, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let status = StackStatus::gather(config);
        if !status.sinexd.running {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    bail!(
        "timed out after {}s waiting for dev-local sinexd to stop",
        timeout.as_secs()
    )
}

fn run_xtask_probe(name: &str, args: &[&str]) -> Result<InfraSmokeStep> {
    let current_exe = std::env::current_exe().wrap_err("failed to resolve current xtask binary")?;
    let output = Command::new(&current_exe)
        .arg("--format")
        .arg("json")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .wrap_err_with(|| format!("failed to run smoke probe {name}"))?;
    let command = std::iter::once("xtask".to_string())
        .chain(args.iter().map(|arg| (*arg).to_string()))
        .collect();
    let status = if output.status.success() {
        "passed"
    } else {
        "failed"
    }
    .to_string();
    let detail = if output.status.success() {
        "probe completed without starting unexpected services".to_string()
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        format!(
            "exit status {}; stderr={}; stdout={}",
            output.status,
            stderr.trim(),
            stdout.trim()
        )
    };
    if !output.status.success() {
        bail!("infra smoke probe {name} failed: {detail}");
    }
    Ok(InfraSmokeStep {
        name: name.to_string(),
        command,
        status,
        detail,
    })
}

fn stop_current_checkout_infra(config: &StackConfig, verbose: bool) -> Result<()> {
    stack::nats_stop(config, verbose)?;
    stack::pg_stop(config, verbose)?;
    CheckoutState::for_current_checkout()?.release_lock()?;
    Ok(())
}

fn all_checkouts_status() -> Result<AllCheckoutsStatus> {
    let base_dir = CheckoutState::default_inventory_base_dir();
    let roots = CheckoutState::inventory_roots_under(&base_dir)?;
    Ok(AllCheckoutsStatus::gather(base_dir, roots))
}

fn smoke_snapshot(config: &StackConfig) -> InfraSmokeSnapshot {
    let status = StackStatus::gather(config);
    let state_bytes = status.data_sizes.postgres_bytes
        + status.data_sizes.nats_bytes
        + status.data_sizes.annex_bytes;
    InfraSmokeSnapshot {
        postgres: format_service_state(&status.postgres).to_string(),
        nats: format_service_state(&status.nats).to_string(),
        sinexd: if status.sinexd.running {
            "running".to_string()
        } else {
            "stopped".to_string()
        },
        rss_bytes: status.postgres.rss_bytes.unwrap_or(0)
            + status.nats.rss_bytes.unwrap_or(0)
            + status.sinexd.rss_bytes,
        state_bytes,
    }
}

fn services_running(status: &StackStatus) -> bool {
    status.postgres.running || status.nats.running || status.sinexd.running
}

fn service_mode_decision() -> InfraServiceModeDecision {
    InfraServiceModeDecision {
        selected_default: "per-checkout isolated",
        reason: "SQLx validation, schema drift, destructive tests, and JetStream state remain branch-sensitive; current cleanup makes the cost visible and stoppable instead of sharing mutable state by default.",
        shared_service_status: "not default; requires a proven namespace/database cleanup design before use",
        hybrid_status: "allowed as future optimization only when compile-only and destructive/runtime paths are separated by command metadata",
        correctness_notes: vec![
            "Postgres uses checkout-local Unix sockets and databases so schema apply and test cleanup cannot cross branches.",
            "NATS uses checkout-derived ports and JetStream storage so subjects, streams, durable consumers, and DLQ state stay isolated.",
            "All-checkout status/stop expose the RAM/process cost and stale state without touching system-managed services.",
        ],
    }
}

async fn execute_status(
    config: &StackConfig,
    watch: bool,
    all_checkouts: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if all_checkouts {
        if watch {
            bail!("infra status --all-checkouts does not support --watch");
        }
        return execute_all_checkouts_status(ctx);
    }

    loop {
        if watch {
            print!("\x1B[2J\x1B[H");
        }

        let status = StackStatus::gather(config);

        if ctx.is_human() {
            println!("sinex-dev infra status");
            println!("────────────────────────────────────────");
            println!("Checkout:    {}", status.checkout_root.display());
            println!("Dev-state:   {}", status.dev_state_dir.display());
            println!("Logs:        {}", status.logs_dir.display());
            println!(
                "PostgreSQL:  {}{} (unix socket, port: {}, rss: {})",
                format_service_state(&status.postgres),
                format_pid(status.postgres.pid),
                status.postgres.port,
                format_optional_bytes(status.postgres.rss_bytes),
            );
            println!(
                "NATS:        {}{} (port: {}, rss: {})",
                format_service_state(&status.nats),
                format_pid(status.nats.pid),
                status.nats.port,
                format_optional_bytes(status.nats.rss_bytes),
            );
            println!(
                "sinexd:      {} (rss: {})",
                format_runtime_state(&status.sinexd),
                format_bytes(status.sinexd.rss_bytes)
            );
            if let Some(issue) = &status.sinexd.issue {
                println!("             {issue}");
            }
            println!(
                "Git-annex:   {}",
                if status.annex.initialized {
                    "initialized"
                } else {
                    "not initialized"
                }
            );
            println!(
                "Data sizes:  pg={} nats={} annex={}",
                format_bytes(status.data_sizes.postgres_bytes),
                format_bytes(status.data_sizes.nats_bytes),
                format_bytes(status.data_sizes.annex_bytes),
            );
            for issue in &status.data_size_issues {
                println!("             {issue}");
            }
            if let Some(issue) = &status.snapshot_issue {
                println!("Snapshots:   unavailable");
                println!("             {issue}");
            } else {
                println!("Snapshots:   {}", status.snapshots.len());
            }
        }

        if !watch {
            let mut result = CommandResult::success().with_data(serde_json::to_value(&status)?);
            for issue in &status.data_size_issues {
                result = result.with_warning(issue.clone());
            }
            if let Some(issue) = &status.snapshot_issue {
                result = result.with_warning(issue.clone());
            }
            if let Some(issue) = &status.sinexd.issue {
                result = result.with_warning(issue.clone());
            }
            return Ok(result);
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

fn execute_all_checkouts_status(ctx: &CommandContext) -> Result<CommandResult> {
    let base_dir = CheckoutState::default_inventory_base_dir();
    let roots = CheckoutState::inventory_roots_under(&base_dir)?;
    let status = AllCheckoutsStatus::gather(base_dir, roots);

    if ctx.is_human() {
        println!("sinex-dev infra status: all checkouts");
        println!("────────────────────────────────────────");
        println!(
            "Checkouts: {}  RSS: {}  state: {}",
            status.totals.checkout_count,
            format_bytes(status.totals.rss_bytes),
            format_bytes(status.totals.state_bytes)
        );
        println!(
            "Running:   postgres={} nats={} sinexd={}",
            status.totals.running_postgres,
            status.totals.running_nats,
            status.totals.running_sinexd
        );
        println!(
            "Stale PIDs: postgres={} nats={}",
            status.totals.stale_postgres_pid_files, status.totals.stale_nats_pid_files
        );
        for checkout in &status.checkouts {
            println!();
            println!("{}", checkout.cache_root.display());
            println!("  dev-state: {}", checkout.dev_state_dir.display());
            match &checkout.checkout_path {
                Some(path) => println!(
                    "  checkout:  {} ({})",
                    path.display(),
                    if checkout.checkout_path_exists == Some(true) {
                        "exists"
                    } else {
                        "missing"
                    }
                ),
                None => println!("  checkout:  unknown"),
            }
            println!(
                "  lock:      {}{}",
                format_lock_state(checkout.lock.state),
                checkout
                    .lock
                    .pid
                    .map(|pid| format!(" pid={pid}"))
                    .unwrap_or_default()
            );
            if let Some(issue) = &checkout.lock.issue {
                println!("             {issue}");
            }
            println!(
                "  postgres:  {}{} rss={}",
                format_service_state(&checkout.postgres),
                format_pid(checkout.postgres.pid),
                format_optional_bytes(checkout.postgres.rss_bytes)
            );
            println!(
                "  nats:      {}{} port={} rss={}",
                format_service_state(&checkout.nats),
                format_pid(checkout.nats.pid),
                checkout.nats.port,
                format_optional_bytes(checkout.nats.rss_bytes)
            );
            println!(
                "  sinexd:    {} rss={}",
                if checkout.sinexd.running {
                    format!("running pids={:?}", checkout.sinexd.pids)
                } else {
                    "stopped".to_string()
                },
                format_bytes(checkout.sinexd.rss_bytes)
            );
            if let Some(issue) = &checkout.sinexd.issue {
                println!("             {issue}");
            }
            println!(
                "  sizes:     pg={} nats={} annex={} logs={} total={}",
                format_bytes(checkout.data_sizes.postgres_bytes),
                format_bytes(checkout.data_sizes.nats_bytes),
                format_bytes(checkout.data_sizes.annex_bytes),
                format_bytes(checkout.logs_bytes),
                format_bytes(checkout.total_state_bytes)
            );
            for command in &checkout.remediation {
                println!("  remedy:    {command}");
            }
            for issue in &checkout.data_size_issues {
                println!("  issue:     {issue}");
            }
        }
    }

    let mut result = CommandResult::success().with_data(serde_json::to_value(&status)?);
    for issue in &status.issues {
        result = result.with_warning(issue.clone());
    }
    Ok(result)
}

fn format_service_state(status: &stack::ServiceStatus) -> &'static str {
    match status.pid_state {
        stack::ServicePidState::Missing => "stopped",
        stack::ServicePidState::Running => "running",
        stack::ServicePidState::Stale => "stale-pid",
        stack::ServicePidState::Malformed => "malformed-pid",
    }
}

fn format_lock_state(state: stack::LockState) -> &'static str {
    match state {
        stack::LockState::Missing => "missing",
        stack::LockState::Live => "live",
        stack::LockState::Stale => "stale",
        stack::LockState::Malformed => "malformed",
    }
}

fn format_pid(pid: Option<u32>) -> String {
    pid.map(|pid| format!(" pid={pid}")).unwrap_or_default()
}

fn format_runtime_state(status: &stack::RuntimeProcessStatus) -> String {
    if status.running {
        format!("running pids={:?}", status.pids)
    } else {
        "stopped".to_string()
    }
}

fn format_optional_bytes(bytes: Option<u64>) -> String {
    bytes.map(format_bytes).unwrap_or_else(|| "-".to_string())
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = UNITS[0];
    for next_unit in &UNITS[1..] {
        if value < 1024.0 {
            break;
        }
        value /= 1024.0;
        unit = next_unit;
    }
    if unit == "B" {
        format!("{bytes}B")
    } else {
        format!("{value:.1}{unit}")
    }
}

fn execute_logs(
    config: &StackConfig,
    process: &str,
    lines: usize,
    follow: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let log_file = match process {
        "postgres" => "postgres.log",
        "nats" | "nats-server" => "nats.log",
        _ => {
            // Try generic process log location if orchestrator uses it
            if Path::new(".sinex/state")
                .join(process)
                .join("process.log")
                .exists()
            {
                "process.log"
            } else {
                bail!("Unknown process: {process}");
            }
        }
    };

    let log_path = if log_file == "postgres.log" || log_file == "nats.log" {
        config.logs_dir().join(log_file)
    } else {
        // Fallback logic
        PathBuf::from(format!(".sinex/state/{process}/process.log"))
    };

    if !log_path.exists() {
        bail!("Log file not found: {}", log_path.display());
    }

    ctx.heading(&format!("logs: {process}"));

    let mut cmd = Command::new("tail");
    cmd.arg("-n").arg(lines.to_string());
    if follow {
        cmd.arg("-f");
    }
    cmd.arg(&log_path);

    let status = crate::process::run_managed_foreground_std_command(&mut cmd, "infra logs")
        .context("tail failed")?;
    if !crate::process::status_indicates_clean_interactive_shutdown(&status) {
        bail!("tail failed");
    }

    Ok(CommandResult::success())
}

#[cfg(test)]
#[path = "infra_test.rs"]
mod tests;
