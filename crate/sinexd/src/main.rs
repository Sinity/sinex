//! `sinexd` — the Sinex local daemon.
//!
//! Single binary hosting the event engine, the operator API, the enabled
//! derived-node automata, and the configured source-unit bindings. The
//! default subcommand (`serve`, also the no-subcommand path) starts the
//! supervisor; auxiliary subcommands run one-off scans against a single
//! source unit (used by oneshot units like the document snapshot scan).

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
use sinexd::node_sdk::service_runtime::{TracingFormat, install_tracing, spawn_shutdown_task};
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
        default_value = "50",
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

    /// Run a single source unit to completion against the given subcommand.
    ///
    /// Runs a source unit through the `sinexd scan-source-unit` entrypoint
    /// like the document snapshot scan. Reuses the source-binding manifest
    /// shape so operator-facing tooling matches the supervisor's catalog.
    ScanSourceUnit {
        /// Source-unit id (must match a registered descriptor).
        #[arg(long)]
        source_unit: String,

        /// Service name reported by systemd / heartbeats. Defaults to
        /// `sinex-source-unit-<id>` when absent.
        #[arg(long)]
        service_name: Option<String>,

        /// JSON object passed verbatim as `--node-config`.
        #[arg(long)]
        node_config: Option<String>,

        /// Extra CLI arguments inserted before the SDK subcommand
        /// (e.g. `scan --until snapshot`).
        ///
        /// `allow_hyphen_values` is required because forwarded SDK flags are
        /// themselves hyphen-prefixed (`--until`, `--targets`); without it clap
        /// rejects `--extra-arg --until` as an unknown top-level flag.
        #[arg(
            long = "extra-arg",
            action = clap::ArgAction::Append,
            allow_hyphen_values = true
        )]
        extra_args: Vec<String>,

        /// Extra environment variables to set in the source-unit host process
        /// (repeatable, format `KEY=VAL`). Used to reproduce operator-side
        /// issues that need session-specific env like `DISPLAY` or
        /// `XAUTHORITY` for desktop.clipboard.
        #[arg(long = "extra-env", value_parser = parse_kv, action = clap::ArgAction::Append)]
        extra_env: Vec<(String, String)>,
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
        Command::ScanSourceUnit {
            source_unit,
            service_name,
            node_config,
            extra_args,
            extra_env,
        } => {
            scan_source_unit(
                source_unit,
                service_name,
                node_config,
                extra_args,
                extra_env,
            )
            .await
        }
    }
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

async fn scan_source_unit(
    source_unit: String,
    service_name: Option<String>,
    node_config: Option<String>,
    extra_args: Vec<String>,
    extra_env: Vec<(String, String)>,
) -> color_eyre::Result<()> {
    let node_config_value = match node_config {
        Some(s) if !s.trim().is_empty() => Some(serde_json::from_str(&s)?),
        _ => None,
    };

    let extra_env: HashMap<String, String> = extra_env.into_iter().collect();

    let binding = SourceBinding {
        source_unit_id: source_unit,
        instance_idx: 1,
        service_name,
        node_config: node_config_value,
        extra_args,
        extra_env,
    };

    source_bindings::run_binding(binding).await?;
    Ok(())
}
