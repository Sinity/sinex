mod build {
    include!(concat!(env!("OUT_DIR"), "/shadow.rs"));
}

use clap::{Parser, ValueEnum};
use color_eyre::eyre::Result;
use sinex_ingestd::{IngestService, IngestdConfig};
use sinex_node_sdk::service_runtime::{self, TracingFormat};
use tracing::{error, info};

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(
    author,
    version = build::CLAP_LONG_VERSION,
    about = "Sinex ingestion daemon - central hub for event ingestion"
)]
struct Args {
    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// NATS URL for message bus
    #[arg(long, env = "SINEX_NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,
    /// Require TLS for NATS connections (enforces tls:// or wss://)
    #[arg(long, env = "SINEX_NATS_REQUIRE_TLS")]
    nats_require_tls: bool,

    /// Database connection pool size
    #[arg(long, env = "SINEX_INGESTD_POOL_SIZE", default_value = "50")]
    pool_size: u32,

    /// `JetStream` pull batch max messages
    #[arg(long, env = "SINEX_INGESTD_CONSUMER_FETCH_MAX_MESSAGES")]
    consumer_fetch_max_messages: Option<usize>,

    /// `JetStream` pull batch timeout in milliseconds
    #[arg(long, env = "SINEX_INGESTD_CONSUMER_FETCH_TIMEOUT_MS")]
    consumer_fetch_timeout_ms: Option<u64>,

    /// `JetStream` `max_ack_pending` for the main consumer
    #[arg(long, env = "SINEX_INGESTD_CONSUMER_MAX_ACK_PENDING")]
    consumer_max_ack_pending: Option<i64>,

    /// `JetStream` `max_ack_pending` for the material slices consumer
    #[arg(long, env = "SINEX_INGESTD_MATERIAL_SLICES_MAX_ACK_PENDING")]
    material_slices_max_ack_pending: Option<i64>,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Log output format
    #[arg(long, default_value = "text")]
    log_format: LogFormat,

    /// Enable tokio-console subscriber for async debugging.
    /// Requires compilation with `--features tokio-console` and
    /// `RUSTFLAGS="--cfg tokio_unstable"`.
    #[cfg(feature = "tokio-console")]
    #[arg(long)]
    tokio_console: bool,

    /// Enable dry-run mode (log events but don't persist)
    #[arg(long)]
    dry_run: bool,

    /// Validate configuration and exit
    #[arg(long)]
    validate_config: bool,

    /// Path to the content-store root for material storage
    #[arg(long, env = "SINEX_CONTENT_STORE_PATH")]
    content_store_path: Option<String>,

    /// Directory used to persist assembler state between restarts
    #[arg(long, env = "SINEX_ASSEMBLER_STATE_DIR")]
    assembler_state_dir: Option<String>,

    /// NATS namespace for subject/stream isolation (used by test infrastructure)
    #[arg(long, env = "SINEX_NAMESPACE")]
    namespace: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    human_panic::setup_panic!();
    color_eyre::install()?;
    let args = Args::parse();

    #[cfg(feature = "tokio-console")]
    let tokio_console = args.tokio_console;
    #[cfg(not(feature = "tokio-console"))]
    let tokio_console = false;

    setup_tracing(args.log_format, tokio_console, &args.log_level)?;

    info!("Starting Sinex Ingestion Daemon");

    let config = load_runtime_config(&args).await?;

    if args.validate_config {
        info!("Configuration is valid");
        return Ok(());
    }

    info!(?config, "Configuration loaded");

    // Create and run the service
    let mut service = IngestService::new(config).await?;

    // Set up graceful shutdown
    let shutdown_signal = async {
        match sinex_node_sdk::wait_for_os_shutdown_signal().await {
            Ok(signal_name) => {
                info!(signal = signal_name, "Received shutdown signal");
            }
            Err(err) => {
                error!("Failed to listen for shutdown signal: {}", err);
            }
        }
    };

    // Run the service
    tokio::select! {
        result = service.run() => {
            match result {
                Ok(()) => info!("Service completed successfully"),
                Err(e) => {
                    error!("Service failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
        () = shutdown_signal => {
            info!("Shutting down gracefully...");
            if let Err(e) = service.shutdown().await {
                error!("Error during shutdown: {}", e);
            }
        }
    }

    info!("Sinex Ingestion Daemon stopped");
    Ok(())
}

async fn load_runtime_config(args: &Args) -> Result<IngestdConfig> {
    let config = IngestdConfig::from_args(
        args.database_url.clone(),
        args.nats_url.clone(),
        args.nats_require_tls,
        args.pool_size,
        args.consumer_fetch_max_messages,
        args.consumer_fetch_timeout_ms,
        args.consumer_max_ack_pending,
        args.material_slices_max_ack_pending,
        args.dry_run,
        args.content_store_path.clone(),
        args.assembler_state_dir.clone(),
        args.namespace.clone(),
    )?;

    config.validate().await?;
    Ok(config)
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum LogFormat {
    /// Human-readable text output (default)
    Text,
    /// Structured JSON output for machine parsing
    Json,
}

fn setup_tracing(format: LogFormat, tokio_console: bool, default_filter: &str) -> Result<()> {
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
    service_runtime::install_tracing(format, default_filter)
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

        service_runtime::load_env_filter("sinex_ingestd=info")?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn load_env_filter_rejects_invalid_rust_log_directive() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("RUST_LOG", "sinex_ingestd=wat");

        let error = service_runtime::load_env_filter("sinex_ingestd=info")
            .expect_err("invalid directives must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("sinex_ingestd=wat"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn load_env_filter_rejects_non_utf8_rust_log() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("RUST_LOG", OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]));

        let error = service_runtime::load_env_filter("sinex_ingestd=info")
            .expect_err("non-UTF8 RUST_LOG must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("UTF-8"));
        Ok(())
    }

    fn test_args() -> Args {
        Args {
            database_url: Some("postgresql://localhost/test".to_string()),
            nats_url: "nats://localhost:4222".to_string(),
            nats_require_tls: false,
            pool_size: 16,
            consumer_fetch_max_messages: None,
            consumer_fetch_timeout_ms: None,
            consumer_max_ack_pending: None,
            material_slices_max_ack_pending: None,
            log_level: "info".to_string(),
            log_format: LogFormat::Text,
            #[cfg(feature = "tokio-console")]
            tokio_console: false,
            dry_run: false,
            validate_config: false,
            content_store_path: None,
            assembler_state_dir: None,
            namespace: None,
        }
    }

    #[sinex_serial_test]
    async fn load_runtime_config_rejects_invalid_database_url_before_service_start()
    -> TestResult<()> {
        let mut args = test_args();
        args.database_url = Some("mysql://localhost/test".to_string());

        let error = load_runtime_config(&args)
            .await
            .expect_err("normal startup must validate the loaded config");
        let message = error.to_string();

        assert!(message.contains("Validation failed"));
        assert!(message.contains("PostgreSQL"));
        Ok(())
    }
}
