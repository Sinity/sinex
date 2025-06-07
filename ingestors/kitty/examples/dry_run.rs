use kitty_ingestor::config::KittyConfig;
use sinex_shared::{EventSink, LogSink};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Create config
    let config = KittyConfig {
        socket_path: "/tmp/kitty-*".to_string(),
        polling_interval_secs: 5,
        heartbeat_interval_secs: 60,
        max_retries: 3,
        retry_delay_secs: 5,
    };

    // Create log sink for dry-run output
    let event_sink = Arc::new(LogSink::new("KITTY-DRY-RUN"));

    // Create and run listener
    let listener = kitty_ingestor::kitty_listener::KittyListener::new(config, event_sink)?;
    
    println!("Starting Kitty listener in dry-run mode...");
    println!("Events will be logged to stdout instead of database");
    println!("Press Ctrl+C to stop");
    
    listener.start().await?;
    
    Ok(())
}