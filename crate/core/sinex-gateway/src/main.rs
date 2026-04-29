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
use tracing::info;

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_gateway::config::GatewayConfig;
use sinex_gateway::service_container::ServiceContainer;
use sinex_gateway::{native_messaging, rpc_server};
use sinex_node_sdk::service_runtime::{self, TracingFormat};

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
            return Err(eyre!(
                "--tokio-console requires compilation with --features tokio-console"
            ));
        }
    }

    let format = match format {
        LogFormat::Json => TracingFormat::Json,
        LogFormat::Text => TracingFormat::Text,
    };
    service_runtime::install_tracing(format, "sinex_gateway=info")
}

fn load_gateway_config(database_url: Option<String>) -> Result<GatewayConfig> {
    match database_url {
        Some(database_url) => GatewayConfig::load_with_database_url(database_url)
            .map_err(|error| eyre!("Failed to load gateway config").wrap_err(error)),
        None => GatewayConfig::load()
            .map_err(|error| eyre!("Failed to load gateway config").wrap_err(error)),
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

    let shutdown_rx = service_runtime::spawn_shutdown_task("gateway");

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

        service_runtime::load_env_filter("sinex_gateway=info")?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn load_env_filter_rejects_invalid_rust_log_directive() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("RUST_LOG", "sinex_gateway=wat");

        let error = service_runtime::load_env_filter("sinex_gateway=info")
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

        let error = service_runtime::load_env_filter("sinex_gateway=info")
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
