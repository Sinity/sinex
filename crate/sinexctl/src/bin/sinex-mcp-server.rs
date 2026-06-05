use clap::Parser;
use sinexctl::client::{ClientConfig, GatewayClient};
use sinexctl::{Config, mcp};
use sinexd::runtime::service_runtime;
use std::path::PathBuf;

/// Read-only Sinex MCP server over stdio.
#[derive(Debug, Parser)]
#[command(name = "sinex-mcp-server", about = "Read-only Sinex MCP stdio server")]
struct Args {
    /// Gateway RPC URL.
    #[arg(long, env = "SINEX_API_URL")]
    rpc_url: Option<String>,

    /// Authentication token.
    #[arg(long, env = "SINEX_API_TOKEN")]
    token: Option<String>,

    /// Token file path.
    #[arg(long)]
    token_file: Option<String>,

    /// Root CA certificate path.
    #[arg(long)]
    ca_cert: Option<String>,

    /// Client certificate path for mTLS.
    #[arg(long)]
    client_cert: Option<String>,

    /// Client private key path for mTLS.
    #[arg(long)]
    client_key: Option<String>,

    /// Accept invalid certificates.
    #[arg(long)]
    insecure: bool,

    /// Request timeout in seconds.
    #[arg(long, default_value = "30")]
    timeout: u64,

    /// Runtime target descriptor to load for gateway/auth/TLS settings.
    #[arg(long, env = "SINEX_RUNTIME_TARGET_CONFIG")]
    runtime_target: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(service_runtime::load_env_filter("warn")?)
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let mut config = Config::load().unwrap_or_default();
    if let Some(path) = args
        .runtime_target
        .filter(|path| !path.as_os_str().is_empty())
    {
        config.apply_runtime_target(sinex_primitives::RuntimeTargetDescriptor::load_from_path(
            path,
        )?);
    }

    config.merge_cli_args(
        args.rpc_url,
        args.token,
        args.token_file,
        args.ca_cert,
        args.client_cert,
        args.client_key,
        args.insecure,
        Some(args.timeout),
        None,
    );

    let client = GatewayClient::new(ClientConfig::from(&config))?;
    mcp::run_stdio(client).await
}
