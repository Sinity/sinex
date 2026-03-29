mod build {
    include!(concat!(env!("OUT_DIR"), "/shadow.rs"));
}

use clap::{Parser, ValueEnum};
use color_eyre::eyre::{Result, eyre};
use sinex_ingestd::{IngestService, IngestdConfig};
use std::io;
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

    /// Path to the git-annex repository for material storage
    #[arg(long, env = "SINEX_ANNEX_PATH")]
    annex_path: Option<String>,

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

    // Load configuration from environment and command line arguments
    let config = IngestdConfig::from_args(
        args.database_url,
        args.nats_url,
        args.nats_require_tls,
        args.pool_size,
        args.consumer_fetch_max_messages,
        args.consumer_fetch_timeout_ms,
        args.consumer_max_ack_pending,
        args.material_slices_max_ack_pending,
        args.dry_run,
        args.annex_path,
        args.assembler_state_dir,
        args.namespace,
    )?;

    if args.validate_config {
        config.validate_and_exit().await;
    }

    info!(?config, "Configuration loaded");

    // Create and run the service
    let mut service = IngestService::new(config).await?;

    // Set up graceful shutdown
    let shutdown_signal = async {
        if let Err(err) = wait_for_shutdown_signal().await {
            error!("Failed to listen for shutdown signal: {}", err);
        } else {
            info!("Received shutdown signal");
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum LogFormat {
    /// Human-readable text output (default)
    Text,
    /// Structured JSON output for machine parsing
    Json,
}

fn load_env_filter(default_filter: &str) -> Result<tracing_subscriber::EnvFilter> {
    let Some(raw) = std::env::var_os(tracing_subscriber::EnvFilter::DEFAULT_ENV) else {
        return Ok(tracing_subscriber::EnvFilter::new(default_filter));
    };

    let raw = raw.into_string().map_err(|_| {
        eyre!(
            "{} is not valid UTF-8",
            tracing_subscriber::EnvFilter::DEFAULT_ENV
        )
    })?;

    tracing_subscriber::EnvFilter::try_new(&raw).map_err(|error| {
        eyre!(
            "Invalid {} directive `{raw}`: {error}",
            tracing_subscriber::EnvFilter::DEFAULT_ENV
        )
    })
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
            return Err(color_eyre::eyre::eyre!(
                "--tokio-console requires compilation with --features tokio-console"
            ));
        }
    }

    let env_filter = load_env_filter(default_filter)?;

    match format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_writer(io::stderr)
                .with_env_filter(env_filter)
                .with_target(true)
                .with_thread_ids(true)
                .init();
        }
        LogFormat::Text => {
            tracing_subscriber::fmt()
                .with_writer(io::stderr)
                .with_env_filter(env_filter)
                .with_target(true)
                .with_thread_ids(true)
                .init();
        }
    }

    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    tokio::select! {
        _ = sigterm.recv() => Ok(()),
        _ = sigint.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use xtask::sandbox::prelude::*;

    struct EnvGuard {
        saved: Vec<(String, Option<std::ffi::OsString>)>,
    }

    impl EnvGuard {
        fn new(keys: &[&str]) -> Self {
            Self {
                saved: keys
                    .iter()
                    .map(|key| ((*key).to_string(), std::env::var_os(key)))
                    .collect(),
            }
        }

        fn set(&mut self, key: &str, value: impl AsRef<std::ffi::OsStr>) {
            unsafe { std::env::set_var(key, value) };
        }

        fn remove(&mut self, key: &str) {
            unsafe { std::env::remove_var(key) };
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..) {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
    }

    #[sinex_serial_test]
    async fn load_env_filter_defaults_when_rust_log_is_missing() -> TestResult<()> {
        let mut env = EnvGuard::new(&["RUST_LOG"]);
        env.remove("RUST_LOG");

        load_env_filter("sinex_ingestd=info")?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn load_env_filter_rejects_invalid_rust_log_directive() -> TestResult<()> {
        let mut env = EnvGuard::new(&["RUST_LOG"]);
        env.set("RUST_LOG", "sinex_ingestd=wat");

        let error =
            load_env_filter("sinex_ingestd=info").expect_err("invalid directives must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("sinex_ingestd=wat"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn load_env_filter_rejects_non_utf8_rust_log() -> TestResult<()> {
        let mut env = EnvGuard::new(&["RUST_LOG"]);
        env.set("RUST_LOG", OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]));

        let error =
            load_env_filter("sinex_ingestd=info").expect_err("non-UTF8 RUST_LOG must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("UTF-8"));
        Ok(())
    }
}
