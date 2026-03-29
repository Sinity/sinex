use clap::{CommandFactory, Parser, Subcommand};
use color_eyre::eyre::eyre;
use sinexctl::client::{ClientConfig, GatewayClient};
use sinexctl::commands::{
    AuditCommand, CompletionsCommand, ConfigCommands, ContextCommand, CoreCommands, DemoCommand,
    DlqCommands, ErrorsCommand, GatewayCommands, GitOpsCommands, LifecycleCommands, NodeCommands,
    OpsCommands, QueryCommand, RecentCommand, ReplayCommands, ReportCommands, StatusCommand,
    TelemetryCommands, TraceCommand, TuiCommand, WatchCommand,
};
use sinexctl::model::OutputFormat;
use sinexctl::{Config, default_rpc_url};

/// Sinex control CLI
#[derive(Debug, Parser)]
#[command(name = "sinexctl", about = "Sinex control CLI", version)]
struct Cli {
    /// Gateway RPC URL
    #[arg(
        long,
        env = "SINEX_RPC_URL",
        default_value = "https://127.0.0.1:9999",
        global = true
    )]
    rpc_url: String,

    /// Authentication token
    #[arg(long, env = "SINEX_RPC_TOKEN", global = true)]
    token: Option<String>,

    /// Token file path
    #[arg(long, global = true)]
    token_file: Option<String>,

    /// Root CA certificate path
    #[arg(long, global = true)]
    ca_cert: Option<String>,

    /// Client certificate path (for mTLS)
    #[arg(long, global = true)]
    client_cert: Option<String>,

    /// Client private key path (for mTLS)
    #[arg(long, global = true)]
    client_key: Option<String>,

    /// Accept invalid certificates (dev only!)
    #[arg(long, global = true)]
    insecure: bool,

    /// Request timeout in seconds
    #[arg(long, default_value = "30", global = true)]
    timeout: u64,

    /// Output format (can be overridden per command)
    #[arg(long, short = 'f', value_enum, default_value = "table", global = true)]
    format: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

fn load_env_filter(default_filter: &str) -> color_eyre::eyre::Result<tracing_subscriber::EnvFilter> {
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

#[derive(Debug, Subcommand)]
enum Commands {
    /// Gateway operations
    Gateway {
        #[command(subcommand)]
        cmd: GatewayCommands,
    },

    /// Core system operations
    Core {
        #[command(subcommand)]
        cmd: CoreCommands,
    },

    /// Node operations
    Node {
        #[command(subcommand)]
        cmd: NodeCommands,
    },

    /// Replay operations
    Replay {
        #[command(subcommand)]
        cmd: ReplayCommands,
    },

    /// Dead letter queue operations
    Dlq {
        #[command(subcommand)]
        cmd: DlqCommands,
    },

    /// Query/search events
    Query(QueryCommand),

    /// Trace event provenance chain
    Trace(TraceCommand),

    /// Operations log commands
    Ops {
        #[command(subcommand)]
        cmd: OpsCommands,
    },

    /// Get audit trail for an operation
    Audit(AuditCommand),

    /// Launch interactive TUI dashboard
    Tui(TuiCommand),

    /// Configuration management
    Config {
        #[command(subcommand)]
        cmd: ConfigCommands,
    },

    /// Seed database with deterministic fake events for testing/demos
    Demo(DemoCommand),

    /// Data lifecycle management (archive, restore, tombstone)
    Lifecycle {
        #[command(subcommand)]
        cmd: LifecycleCommands,
    },

    /// `GitOps` schema source management
    GitOps {
        #[command(subcommand)]
        cmd: GitOpsCommands,
    },

    /// Telemetry data from event-time activity views and operator read models
    Telemetry {
        #[command(subcommand)]
        cmd: TelemetryCommands,
    },

    /// Daily activity reports (today, yesterday)
    Report {
        #[command(subcommand)]
        cmd: ReportCommands,
    },

    // ===== Shortcut Commands =====
    /// Quick system status check
    Status(StatusCommand),

    /// Show recent events (last hour by default)
    Recent(RecentCommand),

    /// Show recent errors only
    Errors(ErrorsCommand),

    /// Watch events in real-time
    Watch(WatchCommand),

    /// Show activity context for session resumption ("what was I doing?")
    Context(ContextCommand),

    /// Generate shell completions
    Completions(CompletionsCommand),
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // Initialize error handling
    color_eyre::install()?;

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(load_env_filter("warn")?)
        .init();

    // Parse CLI arguments
    let cli = Cli::parse();

    // Handle config commands early (they don't need a gateway client)
    if let Commands::Config { cmd } = cli.command {
        return cmd.execute().await;
    }

    // Handle completions command early (doesn't need a gateway client)
    if let Commands::Completions(cmd) = cli.command {
        let mut clap_cmd = Cli::command();
        return cmd.execute(&mut clap_cmd);
    }

    // Handle demo command early (connects directly to DB, no gateway needed)
    if let Commands::Demo(cmd) = cli.command {
        return cmd.execute().await;
    }

    // Load effective config:
    // defaults -> runtime env overrides -> local user preferences
    let mut config = Config::load().unwrap_or_else(|e| {
        tracing::debug!("Failed to load sinexctl preferences: {}, using defaults", e);
        Config::default()
    });

    // Override with explicit CLI args.
    let rpc_url_override = if cli.rpc_url == default_rpc_url() {
        None
    } else {
        Some(cli.rpc_url.clone())
    };

    config.merge_cli_args(
        rpc_url_override,
        cli.token,
        cli.token_file,
        cli.ca_cert,
        cli.client_cert,
        cli.client_key,
        cli.insecure,
        Some(cli.timeout),
        Some(cli.format),
    );

    // Convert to ClientConfig and create gateway client
    let client_config = ClientConfig::from(&config);
    let client = GatewayClient::new(client_config)?;

    // Execute command (use merged config's format for commands that need it)
    let format = config.default_format;
    match cli.command {
        Commands::Gateway { cmd } => cmd.execute(&client, format).await?,
        Commands::Core { cmd } => cmd.execute(&client, format).await?,
        Commands::Node { cmd } => cmd.execute(&client).await?,
        Commands::Replay { cmd } => cmd.execute(&client).await?,
        Commands::Dlq { cmd } => cmd.execute(&client).await?,
        Commands::Query(cmd) => cmd.execute(&client).await?,
        Commands::Trace(cmd) => cmd.execute(&client).await?,
        Commands::Ops { cmd } => cmd.execute(&client).await?,
        Commands::Audit(cmd) => cmd.execute(&client).await?,
        Commands::Tui(cmd) => cmd.execute(&client).await?,
        Commands::Config { .. } => unreachable!("Config command handled above"),
        Commands::Demo(_) => unreachable!("Demo command handled above"),
        Commands::Lifecycle { cmd } => cmd.execute(&client).await?,
        Commands::GitOps { cmd } => cmd.execute(&client, format).await?,
        Commands::Telemetry { cmd } => cmd.execute(&client).await?,
        Commands::Report { cmd } => cmd.execute(&client).await?,
        Commands::Status(cmd) => cmd.execute(&client).await?,
        Commands::Recent(cmd) => cmd.execute(&client).await?,
        Commands::Errors(cmd) => cmd.execute(&client).await?,
        Commands::Watch(cmd) => cmd.execute(&client).await?,
        Commands::Context(cmd) => cmd.execute(&client).await?,
        Commands::Completions(_) => unreachable!("Completions command handled above"),
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

        load_env_filter("warn")?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn load_env_filter_rejects_invalid_rust_log_directive() -> TestResult<()> {
        let mut env = EnvGuard::new(&["RUST_LOG"]);
        env.set("RUST_LOG", "sinexctl=wat");

        let error = load_env_filter("warn").expect_err("invalid directives must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("sinexctl=wat"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn load_env_filter_rejects_non_utf8_rust_log() -> TestResult<()> {
        let mut env = EnvGuard::new(&["RUST_LOG"]);
        env.set("RUST_LOG", OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]));

        let error = load_env_filter("warn").expect_err("non-UTF8 RUST_LOG must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("UTF-8"));
        Ok(())
    }
}
