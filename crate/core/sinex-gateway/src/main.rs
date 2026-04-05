//! Sinex Gateway - Unified API gateway for CLI and browser extension
//!
//! This binary provides two modes:
//! - RPC Server: JSON-RPC over TLS for CLI
//! - Native Messaging: stdin/stdout protocol for browser extensions

mod build {
    include!(concat!(env!("OUT_DIR"), "/shadow.rs"));
}

use clap::{Parser, Subcommand, ValueEnum};
use color_eyre::eyre::{Result, eyre};
use std::io;
use tracing::{error, info, warn};

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_gateway::config::GatewayConfig;
use sinex_gateway::service_container::ServiceContainer;
use sinex_gateway::{native_messaging, rpc_server};
use sinex_primitives::strict_env_filter_source;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum LogFormat {
    /// Human-readable text output (default)
    Text,
    /// Structured JSON output for machine parsing
    Json,
}

#[derive(Parser)]
#[command(name = "sinex-gateway")]
#[command(about = "Unified API gateway for Sinex")]
#[command(version = build::CLAP_LONG_VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log output format
    #[arg(long, default_value = "text", global = true)]
    log_format: LogFormat,

    /// Enable tokio-console subscriber for async debugging.
    /// Requires compilation with `--features tokio-console` and
    /// `RUSTFLAGS="--cfg tokio_unstable"`.
    #[cfg(feature = "tokio-console")]
    #[arg(long, global = true)]
    tokio_console: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Start RPC server for CLI communication
    RpcServer {
        /// TCP listen address in host:port form
        #[arg(long)]
        tcp_listen: Option<String>,

        /// Database URL
        #[arg(long)]
        database_url: Option<String>,

        /// Allowed CORS origins (comma-separated). If not set, only localhost is allowed.
        #[arg(long)]
        cors_origins: Option<String>,
    },

    /// Start native messaging mode for browser extension
    NativeMessaging {
        /// Database URL
        #[arg(long)]
        database_url: Option<String>,
    },
}

fn setup_tracing(format: LogFormat, tokio_console: bool) -> Result<()> {
    if tokio_console {
        #[cfg(feature = "tokio-console")]
        {
            console_subscriber::init();
            return Ok(());
        }
        #[cfg(not(feature = "tokio-console"))]
        {
            return Err(color_eyre::eyre::eyre!(
                "--tokio-console requires compilation with --features tokio-console"
            ));
        }
    }

    let env_filter = load_env_filter("sinex_gateway=info")?;

    match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_writer(std::io::stderr)
            .with_env_filter(env_filter)
            .with_target(true)
            .with_thread_ids(true)
            .try_init()
            .map_err(|e| color_eyre::eyre::eyre!("Failed to initialize tracing: {e}")),
        LogFormat::Text => tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(env_filter)
            .with_target(true)
            .with_thread_ids(true)
            .try_init()
            .map_err(|e| color_eyre::eyre::eyre!("Failed to initialize tracing: {e}")),
    }
}

fn load_gateway_config(database_url: Option<String>) -> Result<GatewayConfig> {
    match database_url {
        Some(database_url) => GatewayConfig::load_with_database_url(database_url)
            .map_err(|error| eyre!("Failed to load gateway config").wrap_err(error)),
        None => GatewayConfig::load()
            .map_err(|error| eyre!("Failed to load gateway config").wrap_err(error)),
    }
}

fn load_env_filter(default_filter: &str) -> Result<tracing_subscriber::EnvFilter> {
    let raw = strict_env_filter_source(default_filter)?;
    tracing_subscriber::EnvFilter::try_new(&raw).map_err(|error| {
        eyre!(
            "Invalid {} directive `{raw}`: {error}",
            tracing_subscriber::EnvFilter::DEFAULT_ENV
        )
    })
}

async fn wait_for_shutdown_signal() -> io::Result<&'static str> {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await?;
        Ok("SIGINT (Ctrl+C)")
    };

    #[cfg(unix)]
    let terminate = async {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        sigterm.recv().await;
        Ok("SIGTERM")
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<io::Result<&'static str>>();

    tokio::select! {
        result = ctrl_c => result,
        result = terminate => result,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    human_panic::setup_panic!();
    color_eyre::install()?;

    let cli = Cli::parse();

    #[cfg(feature = "tokio-console")]
    let tokio_console = cli.tokio_console;
    #[cfg(not(feature = "tokio-console"))]
    let tokio_console = false;

    setup_tracing(cli.log_format, tokio_console)?;

    // Issue 128: Set up graceful shutdown signal handling
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let shutdown_task = {
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            match wait_for_shutdown_signal().await {
                Ok(signal_name) => {
                    info!(
                        signal = signal_name,
                        "Received shutdown signal, initiating graceful shutdown"
                    );
                }
                Err(error) => {
                    error!(error = %error, "Failed to listen for gateway shutdown signal");
                }
            }

            if shutdown_tx.send(true).is_err() {
                warn!("Gateway shutdown receiver was already dropped before signal delivery");
            }
        })
    };

    match cli.command {
        Commands::RpcServer {
            tcp_listen,
            database_url,
            cors_origins,
        } => {
            // CLI args override the loaded config before the runtime starts.
            let config = load_gateway_config(database_url)?.with_cli_overrides(
                None,
                tcp_listen,
                cors_origins,
            );

            info!("Starting RPC server on {}", config.tcp_listen);

            // Initialize service container
            let services = ServiceContainer::new(&config).await.map_err(|e| {
                color_eyre::eyre::eyre!("Failed to initialize services").wrap_err(e)
            })?;

            // Start RPC server with shutdown signal
            let result = rpc_server::run(&config, services, shutdown_rx)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("RPC server failed").wrap_err(e));

            // Clean up shutdown task
            shutdown_task.abort();
            result?;
        }

        Commands::NativeMessaging { database_url } => {
            let config = load_gateway_config(database_url)?;

            info!("Starting native messaging mode");

            // Initialize service container
            let services = ServiceContainer::new(&config).await.map_err(|e| {
                color_eyre::eyre::eyre!("Failed to initialize services").wrap_err(e)
            })?;

            // Start native messaging loop with shutdown signal
            let result = native_messaging::run(services, &config, shutdown_rx)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Native messaging failed").wrap_err(e));

            // Clean up shutdown task
            shutdown_task.abort();
            result?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use xtask::sandbox::prelude::*;

    use xtask::sandbox::EnvGuard;

    #[sinex_serial_test]
    async fn load_env_filter_defaults_when_rust_log_is_missing() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("RUST_LOG");

        load_env_filter("sinex_gateway=info")?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn load_env_filter_rejects_invalid_rust_log_directive() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("RUST_LOG", "sinex_gateway=wat");

        let error = load_env_filter("sinex_gateway=info")
            .expect_err("invalid directives must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("sinex_gateway=wat"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn load_env_filter_rejects_non_utf8_rust_log() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("RUST_LOG", OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]));

        let error = load_env_filter("sinex_gateway=info")
            .expect_err("non-UTF8 RUST_LOG must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("UTF-8"));
        Ok(())
    }

    #[sinex_serial_test]
    async fn load_gateway_config_uses_cli_database_url_without_env() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("DATABASE_URL");

        let config = load_gateway_config(Some("postgresql://gateway-cli/sinex".to_string()))?;

        assert_eq!(config.database_url, "postgresql://gateway-cli/sinex");
        Ok(())
    }

    #[sinex_serial_test]
    async fn load_gateway_config_cli_database_url_overrides_malformed_env() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("DATABASE_URL", "not-a-database-url");

        let config = load_gateway_config(Some("postgresql://gateway-cli/sinex".to_string()))?;

        assert_eq!(config.database_url, "postgresql://gateway-cli/sinex");
        Ok(())
    }
}
