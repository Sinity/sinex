//! sensd - Universal acquisition daemon for Sinex
//!
//! This daemon manages all source material acquisition, maintaining
//! the temporal ledger and providing MaterialSliceStream interfaces
//! to ingestors.

use color_eyre::eyre::Result;
use sinex_sensd::{config::SensdConfig, service::SensdService};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize error handling
    color_eyre::install()?;

    // Initialize tracing
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting sensd - Universal acquisition daemon");

    // Load configuration
    let config = SensdConfig::from_env()?;
    info!("Configuration loaded: {:?}", config);

    // Create and run service
    let service = SensdService::new(config).await?;

    // Set up shutdown handler
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C handler");
        info!("Shutdown signal received");
    };

    // Run service until shutdown
    tokio::select! {
        result = service.run() => {
            if let Err(e) = result {
                error!("Service error: {}", e);
                return Err(e);
            }
        }
        _ = shutdown => {
            info!("Shutting down gracefully");
        }
    }

    Ok(())
}
