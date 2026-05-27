//! `sinexd` — the Sinex local daemon.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use clap::Parser;
use sinex_node_sdk::service_runtime::{TracingFormat, install_tracing};
use sinexd::api::config::GatewayConfig;
use sinexd::event_engine::IngestdConfig;
use sinexd::supervisor::Supervisor;

#[derive(Parser, Debug)]
#[command(name = "sinexd", about = "Sinex local daemon", version)]
struct Cli {
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    #[arg(long, env = "SINEX_NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    #[arg(long, env = "SINEX_NATS_REQUIRE_TLS")]
    nats_require_tls: bool,

    #[arg(long, env = "SINEX_EVENT_ENGINE_POOL_SIZE", default_value = "50")]
    pool_size: u32,

    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    install_tracing(TracingFormat::Text, &cli.log_level)?;

    let event_engine_config = IngestdConfig::from_args(
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
        None,
    )?;

    let api_config = match cli.database_url.as_ref() {
        Some(url) => GatewayConfig::load_with_database_url(url.clone()),
        None => GatewayConfig::load(),
    }?;

    Supervisor::new()
        .run(event_engine_config, api_config)
        .await?;

    Ok(())
}
