use clap::{CommandFactory, Parser, Subcommand};
use sinexctl::client::{ClientConfig, GatewayClient};
use sinexctl::commands::{
    AuditCommand, CompletionsCommand, ConfigCommands, CoreCommands, DbCommands, DlqCommands,
    ErrorsCommand, GatewayCommands, GitOpsCommands, LifecycleCommands, NodeCommands, OpsCommands,
    QueryCommand, RecentCommand, ReplayCommands, StatusCommand, TuiCommand, WatchCommand,
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

    /// Direct database access (bypasses gateway)
    Db {
        #[command(subcommand)]
        cmd: DbCommands,
    },

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

    // ===== Shortcut Commands =====
    /// Quick system status check
    Status(StatusCommand),

    /// Show recent events (last hour by default)
    Recent(RecentCommand),

    /// Show recent errors only
    Errors(ErrorsCommand),

    /// Watch events in real-time
    Watch(WatchCommand),

    /// Generate shell completions
    Completions(CompletionsCommand),
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // Initialize error handling
    color_eyre::install()?;

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
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

    // Handle db commands early (doesn't need a gateway client)
    if let Commands::Db { cmd } = cli.command {
        return cmd.execute().await;
    }

    // Load layered config (defaults < config file < env vars)
    let mut config = Config::load().unwrap_or_else(|e| {
        tracing::debug!("Failed to load config file: {}, using defaults", e);
        Config::default()
    });

    // Override with explicit CLI args (only if they differ from defaults)
    // This allows config file values to take effect unless explicitly overridden
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
        Commands::Ops { cmd } => cmd.execute(&client).await?,
        Commands::Audit(cmd) => cmd.execute(&client).await?,
        Commands::Tui(cmd) => cmd.execute(&client).await?,
        Commands::Config { .. } => unreachable!("Config command handled above"),
        Commands::Db { .. } => unreachable!("Db command handled above"),
        Commands::Lifecycle { cmd } => cmd.execute(&client).await?,
        Commands::GitOps { cmd } => cmd.execute(&client, format).await?,
        Commands::Status(cmd) => cmd.execute(&client).await?,
        Commands::Recent(cmd) => cmd.execute(&client).await?,
        Commands::Errors(cmd) => cmd.execute(&client).await?,
        Commands::Watch(cmd) => cmd.execute(&client).await?,
        Commands::Completions(_) => unreachable!("Completions command handled above"),
    }

    Ok(())
}
