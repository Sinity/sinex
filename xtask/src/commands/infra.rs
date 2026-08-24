//! Infra command - infrastructure management.

use clap::Subcommand;
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config as xtask_config;
use crate::infra::flake_stage::stage_checkout_for_flake;
use crate::infra::stack::{self, StackConfig};
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
    /// Run AgentCTL lease-owned development Postgres and NATS in the foreground
    LeaseServices,
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
            InfraSubcommand::LeaseServices => execute_lease_services(ctx),
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

const LEASE_POSTGRES_PORT_RANGE: std::ops::RangeInclusive<u16> = 45432..=45559;
const LEASE_NATS_PORT_RANGE: std::ops::RangeInclusive<u16> = 44308..=44435;

fn lease_port(name: &str, range: &std::ops::RangeInclusive<u16>) -> Result<u16> {
    let value = std::env::var(name).wrap_err_with(|| {
        format!("{name} is injected only by the AgentCTL dev_services operation")
    })?;
    lease_port_value(&value, range).wrap_err_with(|| format!("{name} must be a port"))
}

fn lease_port_value(value: &str, range: &std::ops::RangeInclusive<u16>) -> Result<u16> {
    let port = value.parse::<u16>()?;
    if !range.contains(&port) {
        bail!("port must be within the declared AgentCTL lease range {range:?}, got {port}");
    }
    Ok(port)
}

fn execute_lease_services(ctx: &CommandContext) -> Result<CommandResult> {
    ctx.heading("infra lease-services");
    let postgres_port = lease_port("SINEX_DEV_POSTGRES_PORT", &LEASE_POSTGRES_PORT_RANGE)?;
    let nats_port = lease_port("SINEX_DEV_NATS_PORT", &LEASE_NATS_PORT_RANGE)?;
    let config = StackConfig::for_current_checkout()?;
    if config.postgres.port != postgres_port || config.nats.port != nats_port {
        bail!("lease coordinates changed while preparing development services");
    }

    stack::ensure_directories(&config)?;
    stack::annex_init(&config, ctx.is_human())?;
    stack::nats_generate_config(&config, ctx.is_human())?;
    stack::pg_init(&config, ctx.is_human())?;
    stack::pg_start(&config, ctx.is_human())?;
    stack::pg_setup_database(&config, ctx.is_human())?;
    stack::pg_apply_schema(&config, ctx.is_human())?;
    stack::nats_start(&config, ctx.is_human())?;

    println!("lease services ready: postgres=127.0.0.1:{postgres_port} nats=127.0.0.1:{nats_port}");
    println!(
        "AgentCTL owns this foreground job and its systemd cgroup; cancellation releases both leases."
    );
    loop {
        std::thread::park();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Implementations
// ─────────────────────────────────────────────────────────────────────────────

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
    let path = PathBuf::from("/realm/data/accounts/raindrop/processed/bookmarks.csv");
    path.exists().then_some(path)
}

fn default_activitywatch_db_path(home: &Path) -> Option<PathBuf> {
    let path = home.join(".local/share/activitywatch/aw-server-rust/sqlite.db");
    path.exists().then_some(path)
}

fn default_hyprland_event_socket_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("SINEX_HYPRLAND_EVENT_SOCKET")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| {
            path.metadata()
                .is_ok_and(|metadata| metadata.file_type().is_socket())
        })
    {
        return Some(path);
    }

    let runtime_dir = std::env::var_os("SINEX_HYPRLAND_RUNTIME_DIR")
        .or_else(|| std::env::var_os("XDG_RUNTIME_DIR"))
        .filter(|value| !value.is_empty())?;
    let signature = std::env::var_os("SINEX_HYPRLAND_INSTANCE_SIGNATURE")
        .or_else(|| std::env::var_os("HYPRLAND_INSTANCE_SIGNATURE"))
        .filter(|value| !value.is_empty())?;
    let path = PathBuf::from(runtime_dir)
        .join("hypr")
        .join(signature)
        .join(".socket2.sock");
    path.metadata()
        .is_ok_and(|metadata| metadata.file_type().is_socket())
        .then_some(path)
}

fn generate_dev_source_bindings_manifest_for_home_and_exports(
    watch_root: &Path,
    home: &Path,
    browser_history_dump: Option<&Path>,
    raindrop_bookmarks_export: Option<&Path>,
) -> DevSourceBindingsManifest {
    let zsh_history = home.join(".zsh_history");
    let atuin_history = home.join(".local/share/atuin/history.db");
    let activitywatch_db = default_activitywatch_db_path(home);
    let hyprland_event_socket = default_hyprland_event_socket_path();
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
        let owns_shared_dump = idx == 0 && browser_history_dump.is_some();
        let secondary_path = if owns_shared_dump {
            browser_history_dump.unwrap_or_else(|| Path::new(""))
        } else {
            Path::new("")
        };
        let mut runtime_config = json!({
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
                "path": secondary_path,
                "skip_empty": true
            },
            "interleaved": false
        });
        if owns_shared_dump {
            runtime_config["checkpoint_identity"] = json!("browser.history");
            runtime_config["control_identity"] = json!("browser.history");
        }
        bindings.push(dev_source_binding(
            "browser.history",
            (idx + 1) as u32,
            runtime_config,
        ));
    }
    if let Some(activitywatch_db) = activitywatch_db {
        bindings.push(dev_source_binding(
            "desktop.activitywatch",
            1,
            json!({
                "path": activitywatch_db,
                "read_only": false,
                "immutable": false,
            }),
        ));
    }
    if let Some(hyprland_event_socket) = hyprland_event_socket {
        bindings.push(dev_source_binding(
            "desktop.window-manager",
            1,
            json!({
                "socket_path": hyprland_event_socket,
                "reconnect_on_eof": true,
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
            "control_identity": "fs-watcher",
            "watch_paths": [watch_root],
            "recursive": true,
            "ignored_directory_names": [
                "target",
                ".git",
                ".sinex",
                ".beads",
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
        comment: "Generated by `xtask infra dev-bindings`. Point SINEX_SOURCE_BINDINGS_PATH at this file, then run `agentctl job start sinex run_all_sources` to start the fast dogfood dev loop with real terminal/git/fs/journald/browser/desktop source bindings when their local materials exist.".to_string(),
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
            "Run with: SINEX_SOURCE_BINDINGS_PATH={} agentctl job start sinex run_all_sources",
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
