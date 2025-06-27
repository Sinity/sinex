// Standalone metrics server for testing queue metrics
use anyhow::Result;
use sinex_db::create_pool;
use sinex_worker::start_queue_metrics_server;
use std::env;
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Get database URL
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    // Get port from environment or use default
    let port: u16 = env::var("METRICS_PORT")
        .unwrap_or_else(|_| "9090".to_string())
        .parse()
        .unwrap_or(9090);

    // Get update interval from environment or use default (10 seconds)
    let update_interval: u64 = env::var("UPDATE_INTERVAL_SECS")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);

    info!("Starting metrics server...");
    info!("Database URL: {}", database_url);
    info!("Metrics port: {}", port);
    info!("Update interval: {} seconds", update_interval);

    // Create database pool
    let pool = create_pool(&database_url).await?;

    info!("Database pool created successfully");

    // Start the enhanced metrics server
    start_queue_metrics_server(pool, port, update_interval).await?;

    Ok(())
}
