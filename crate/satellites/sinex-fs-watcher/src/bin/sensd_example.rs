//! Example binary showing fs-watcher with sensd integration

use color_eyre::eyre::Result;
use sinex_fs_watcher::{run_with_sensd, SensdIntegrationConfig};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt().with_env_filter("debug").init();

    info!("Starting fs-watcher with sensd integration example");

    // Create configuration
    let config = SensdIntegrationConfig {
        database_url: std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string()),
        sensd_grpc_endpoint: "http://localhost:50051".to_string(),
        batch_size: 100,
        processing_interval_ms: 1000,
    };

    // Run with sensd integration
    run_with_sensd(config).await?;

    Ok(())
}
