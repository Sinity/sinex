//! `sinexd` — the Sinex local daemon.
//!
//! Single binary hosting the event engine, the operator API, the enabled
//! automata, and the configured source bindings. The
//! default subcommand (`serve`, also the no-subcommand path) starts the
//! supervisor; auxiliary subcommands run one-off scans against a single
//! source (used by oneshot units like the document snapshot scan).

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use clap::{Parser, Subcommand};
use sinexd::api::config::GatewayConfig;
use sinexd::api::rpc_server;
use sinexd::api::service_container::ServiceContainer;
use sinexd::event_engine::EventEngineConfig;
use sinexd::runtime::service_runtime::{TracingFormat, install_tracing, spawn_shutdown_task};
use sinexd::sources::bindings::{self as source_bindings, SourceBinding};
use sinexd::supervisor::Supervisor;
use std::collections::HashMap;

#[derive(Parser, Debug)]
#[command(name = "sinexd", about = "Sinex local daemon", version)]
struct Cli {
    #[arg(long, env = "DATABASE_URL", global = true)]
    database_url: Option<String>,

    #[arg(
        long,
        env = "SINEX_NATS_URL",
        default_value = "nats://localhost:4222",
        global = true
    )]
    nats_url: String,

    #[arg(long, env = "SINEX_NATS_REQUIRE_TLS", global = true)]
    nats_require_tls: bool,

    #[arg(
        long,
        env = "SINEX_EVENT_ENGINE_POOL_SIZE",
        default_value = "16",
        global = true
    )]
    pool_size: u32,

    #[arg(long, env = "RUST_LOG", default_value = "info", global = true)]
    log_level: String,

    #[arg(long, env = "SINEX_LOG_FORMAT", default_value = "text", global = true)]
    log_format: String,

    #[arg(long, env = "SINEX_NAMESPACE", global = true)]
    namespace: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the long-lived supervisor (default).
    Serve,

    /// Run only the operator API / RPC gateway (no event engine, automata, or
    /// source bindings).
    ///
    /// API-only entrypoint used by sandbox fixtures and manual diagnostics.
    /// Used by the sandbox `TestCoreStack` fixture to run the gateway as a
    /// standalone TLS subprocess on a known port.
    RpcServer {
        /// TCP listen address in `host:port` form (overrides config / env).
        #[arg(long)]
        tcp_listen: Option<String>,

        /// Allowed CORS origins (comma-separated). If unset, only localhost
        /// origins are permitted.
        #[arg(long)]
        cors_origins: Option<String>,
    },

    /// Run a single source to completion against the given subcommand.
    ///
    /// Runs a source through the `sinexd scan-source-driver` entrypoint
    /// like the document snapshot scan. Reuses the source-binding manifest
    /// shape so operator-facing tooling matches the supervisor's catalog.
    ScanSourceDriver {
        /// Source id (must match a registered descriptor).
        #[arg(long)]
        source: String,

        /// Runtime label reported by heartbeats. Defaults to the source id
        /// when absent.
        #[arg(long)]
        service_name: Option<String>,

        /// JSON object passed verbatim as `--runtime-config`.
        #[arg(long)]
        runtime_config: Option<String>,

        /// Extra CLI arguments inserted before the runtime subcommand
        /// (e.g. `scan --until snapshot`).
        ///
        /// `allow_hyphen_values` is required because forwarded runtime flags are
        /// themselves hyphen-prefixed (`--until`, `--targets`); without it clap
        /// rejects `--extra-arg --until` as an unknown top-level flag.
        #[arg(
            long = "extra-arg",
            action = clap::ArgAction::Append,
            allow_hyphen_values = true
        )]
        extra_args: Vec<String>,

        /// Extra environment variables to set in the source host process
        /// (repeatable, format `KEY=VAL`). Used to reproduce operator-side
        /// issues that need session-specific env like `DISPLAY` or
        /// `XAUTHORITY` for desktop.clipboard.
        #[arg(long = "extra-env", value_parser = parse_kv, action = clap::ArgAction::Append)]
        extra_env: Vec<(String, String)>,
    },

    /// Export the typed source catalog (contracts + bindings + resource limits)
    /// to the committed JSON artifact consumed by the NixOS deployment layer.
    ///
    /// This is the Rust→Nix generation seam (#1727): the link-time source
    /// inventory is the authoring source of truth. With `--check`, the artifact
    /// is verified rather than written and a stale artifact exits non-zero
    /// (the drift gate).
    ExportSourceCatalog {
        /// Output path (defaults to the committed artifact path, relative to CWD).
        #[arg(long)]
        output: Option<String>,

        /// Verify only; do not write. Exits non-zero if the artifact is stale.
        #[arg(long)]
        check: bool,
    },

    /// Export the static source/privacy coverage matrix.
    ///
    /// The matrix joins compiled source contracts, runtime bindings, parser
    /// manifests, and declarative field metadata where available. With
    /// `--check`, the committed artifact is verified rather than written.
    ExportPrivacyCoverageMatrix {
        /// Output path (defaults to the committed artifact path, relative to CWD).
        #[arg(long)]
        output: Option<String>,

        /// Verify only; do not write. Exits non-zero if the artifact is stale.
        #[arg(long)]
        check: bool,
    },

    /// Emit the SourcePackage / mode completeness report (#1792).
    ///
    /// The report is generated from compiled source contracts, runtime
    /// bindings, parser/source factories, payload schemas, and the current
    /// catalog/privacy projections. It is review output, not a hand-maintained
    /// proof ledger.
    ExportPackageCompleteness {
        /// Output path. If omitted, writes JSON to stdout.
        #[arg(long)]
        output: Option<String>,

        /// Restrict the report to one package id from the completeness map.
        #[arg(long, alias = "package")]
        package_id: Option<String>,

        /// Restrict the report to one package-local mode id.
        #[arg(long, alias = "mode")]
        mode_id: Option<String>,

        /// Fail when any accepted mode has blocking missing requirements.
        #[arg(long)]
        strict: bool,
    },

    /// Emit a reviewed Rust source/package skeleton for one package mode (#1737).
    ///
    /// The skeleton is generated from the #1792 completeness report so it starts
    /// from compiled SourceContract, SourceRuntimeBinding, EventContract,
    /// AdmissionPolicy, catalog, and privacy-coverage evidence.
    ExportSourceSkeleton {
        /// Package id from the package-completeness report.
        #[arg(long, alias = "package")]
        package_id: String,

        /// Mode id from the package-completeness report.
        #[arg(long, alias = "mode")]
        mode_id: String,

        /// Output path. If omitted, writes Rust skeleton text to stdout.
        #[arg(long)]
        output: Option<String>,
    },
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("expected KEY=VAL, got {s:?}"))?;
    if k.is_empty() {
        return Err("KEY must not be empty".into());
    }
    Ok((k.into(), v.into()))
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let mut cli = Cli::parse();

    let tracing_format = match cli.log_format.as_str() {
        "text" => TracingFormat::Text,
        "json" => TracingFormat::Json,
        other => {
            color_eyre::eyre::bail!(
                "invalid --log-format value: '{other}'. Expected 'text' or 'json'"
            );
        }
    };
    install_tracing(tracing_format, &cli.log_level)?;

    // Install the process-global rustls crypto provider before any subsystem
    // builds a reqwest/rustls client (gateway clients, the event-engine privacy
    // recognizer HTTP client, etc.). reqwest is compiled with
    // `rustls-no-provider`, so the first client built without this panics.
    rpc_server::ensure_rustls_crypto_provider()?;

    let command = cli.command.take().unwrap_or(Command::Serve);
    match command {
        Command::Serve => serve(&cli).await,
        Command::RpcServer {
            tcp_listen,
            cors_origins,
        } => rpc_server_serve(&cli, tcp_listen, cors_origins).await,
        Command::ScanSourceDriver {
            source,
            service_name,
            runtime_config,
            extra_args,
            extra_env,
        } => scan_source(source, service_name, runtime_config, extra_args, extra_env).await,
        Command::ExportSourceCatalog { output, check } => export_source_catalog(output, check),
        Command::ExportPrivacyCoverageMatrix { output, check } => {
            export_privacy_coverage_matrix(output, check)
        }
        Command::ExportPackageCompleteness {
            output,
            package_id,
            mode_id,
            strict,
        } => export_package_completeness(output, package_id.as_deref(), mode_id.as_deref(), strict),
        Command::ExportSourceSkeleton {
            package_id,
            mode_id,
            output,
        } => export_source_skeleton(&package_id, &mode_id, output),
    }
}

fn export_source_catalog(output: Option<String>, check: bool) -> color_eyre::Result<()> {
    use sinexd::sources::catalog_export::{CATALOG_ARTIFACT_PATH, export_catalog};

    let path = output.unwrap_or_else(|| CATALOG_ARTIFACT_PATH.to_string());
    let changed = export_catalog(std::path::Path::new(&path), check)?;

    if check {
        if changed {
            color_eyre::eyre::bail!(
                "source catalog artifact {path} is stale; run `sinexd export-source-catalog` to regenerate"
            );
        }
        println!("source catalog up to date: {path}");
    } else if changed {
        println!("source catalog written: {path}");
    } else {
        println!("source catalog already up to date: {path}");
    }
    Ok(())
}

fn export_privacy_coverage_matrix(output: Option<String>, check: bool) -> color_eyre::Result<()> {
    use sinexd::sources::privacy_coverage::{
        PRIVACY_COVERAGE_ARTIFACT_PATH, export_privacy_coverage_matrix,
    };

    let path = output.unwrap_or_else(|| PRIVACY_COVERAGE_ARTIFACT_PATH.to_string());
    let changed = export_privacy_coverage_matrix(std::path::Path::new(&path), check)?;

    if check {
        if changed {
            color_eyre::eyre::bail!(
                "privacy coverage matrix artifact {path} is stale; run `sinexd export-privacy-coverage-matrix` to regenerate"
            );
        }
        println!("privacy coverage matrix up to date: {path}");
    } else if changed {
        println!("privacy coverage matrix written: {path}");
    } else {
        println!("privacy coverage matrix already up to date: {path}");
    }
    Ok(())
}

fn export_package_completeness(
    output: Option<String>,
    package_id: Option<&str>,
    mode_id: Option<&str>,
    strict: bool,
) -> color_eyre::Result<()> {
    use sinexd::sources::package_completeness::{
        render_filtered_package_completeness_report, render_package_completeness_report,
    };

    let rendered = if package_id.is_some() || mode_id.is_some() {
        render_filtered_package_completeness_report(package_id, mode_id)?
    } else {
        render_package_completeness_report()?
    };
    let report: serde_json::Value = serde_json::from_str(&rendered)?;
    let blocking_missing = report["summary"]["blocking_missing_count"]
        .as_u64()
        .unwrap_or(0);

    if let Some(path) = output {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(&path, rendered)?;
        println!("package completeness report written: {path}");
    } else {
        print!("{rendered}");
    }

    if strict && blocking_missing > 0 {
        color_eyre::eyre::bail!(
            "package completeness strict gate found {blocking_missing} blocking missing requirement(s)"
        );
    }

    Ok(())
}

fn export_source_skeleton(
    package_id: &str,
    mode_id: &str,
    output: Option<String>,
) -> color_eyre::Result<()> {
    use sinexd::sources::source_skeleton::render_source_skeleton;

    let rendered = render_source_skeleton(package_id, mode_id)?;
    if let Some(path) = output {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(&path, rendered)?;
        println!("source skeleton written: {path}");
    } else {
        print!("{rendered}");
    }

    Ok(())
}

async fn serve(cli: &Cli) -> color_eyre::Result<()> {
    let event_engine_config = EventEngineConfig::from_args(
        cli.database_url.clone(),
        cli.nats_url.clone(),
        cli.nats_require_tls,
        cli.pool_size,
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        cli.namespace.clone(),
    )?;

    if schema_apply_on_startup_from_env() {
        tracing::info!("applying database schema before starting sinexd modules");
        sinex_db::apply_schema_for_url(&event_engine_config.database_url).await?;
    }

    event_engine_config.validate().await?;

    let api_config = match cli.database_url.as_ref() {
        Some(url) => GatewayConfig::load_with_database_url(url.clone()),
        None => GatewayConfig::load(),
    }?;

    // The API is enabled by default. Engine-only deployments (e.g. the sandbox
    // event_engine fixture, which runs the gateway as a separate TLS subprocess)
    // opt out via `SINEX_API_ENABLED=false` so the supervisor does not try to
    // bind the TLS-required gateway and tear the daemon down.
    let supervisor = Supervisor {
        event_engine_enabled: true,
        api_enabled: api_enabled_from_env(),
    };
    supervisor.run(event_engine_config, api_config).await?;

    Ok(())
}

/// Read the `SINEX_API_ENABLED` toggle (default `true`).
fn api_enabled_from_env() -> bool {
    match std::env::var("SINEX_API_ENABLED") {
        Ok(value) => !matches!(value.trim(), "0" | "false" | "no" | "off"),
        Err(_) => true,
    }
}

/// Read the `SINEX_SCHEMA_APPLY_ON_STARTUP` toggle (default `false`).
fn schema_apply_on_startup_from_env() -> bool {
    match std::env::var("SINEX_SCHEMA_APPLY_ON_STARTUP") {
        Ok(value) => matches!(value.trim(), "1" | "true" | "yes" | "on"),
        Err(_) => false,
    }
}

/// Run only the operator API / RPC gateway as a standalone process.
async fn rpc_server_serve(
    cli: &Cli,
    tcp_listen: Option<String>,
    cors_origins: Option<String>,
) -> color_eyre::Result<()> {
    let config = match cli.database_url.as_ref() {
        Some(url) => GatewayConfig::load_with_database_url(url.clone()),
        None => GatewayConfig::load(),
    }?
    .with_cli_overrides(None, tcp_listen, cors_origins);

    let services = ServiceContainer::new(&config).await?;
    let shutdown_rx = spawn_shutdown_task("sinexd-rpc-server");
    rpc_server::run(&config, services, shutdown_rx).await?;

    Ok(())
}

async fn scan_source(
    source: String,
    service_name: Option<String>,
    runtime_config: Option<String>,
    extra_args: Vec<String>,
    extra_env: Vec<(String, String)>,
) -> color_eyre::Result<()> {
    let runtime_config_value = match runtime_config {
        Some(s) if !s.trim().is_empty() => Some(serde_json::from_str(&s)?),
        _ => None,
    };

    let extra_env: HashMap<String, String> = extra_env.into_iter().collect();

    let binding = SourceBinding {
        source_id: source,
        instance_idx: 1,
        service_name,
        runtime_config: runtime_config_value,
        extra_args,
        extra_env,
    };

    source_bindings::run_binding(binding).await?;
    Ok(())
}

#[cfg(test)]
#[path = "main_test.rs"]
mod tests;
