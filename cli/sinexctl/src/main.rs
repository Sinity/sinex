use clap::{Parser, Subcommand};
use sinex_cli::client::{ClientConfig, GatewayClient};
use sinex_cli::commands::{CoreCommands, DlqCommands, GatewayCommands, NodeCommands, ReplayCommands};
use sinex_cli::model::OutputFormat;

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

    // Create client config
    let config = ClientConfig {
        url: cli.rpc_url,
        token: cli.token,
        token_file: cli.token_file,
        ca_cert: cli.ca_cert,
        client_cert: cli.client_cert,
        client_key: cli.client_key,
        insecure: cli.insecure,
        timeout: cli.timeout,
    };

    // Create gateway client
    let client = GatewayClient::new(config)?;

    // Execute command
    match cli.command {
        Commands::Gateway { cmd } => cmd.execute(&client, cli.format).await?,
        Commands::Core { cmd } => cmd.execute(&client, cli.format).await?,
        Commands::Node { cmd } => cmd.execute(&client).await?,
        Commands::Replay { cmd } => cmd.execute(&client).await?,
        Commands::Dlq { cmd } => cmd.execute(&client).await?,
    }

    Ok(())
}
