//! Desktop Satellite Main Binary

use clap::Parser;
use sinex_desktop_satellite::{DesktopConfig, DesktopSatellite};
use sinex_satellite_sdk::{EventSourceRunner, IngestClient, SatelliteArgs, SatelliteResult};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(flatten)]
    satellite: SatelliteArgs,

    /// Enable clipboard monitoring
    #[arg(long, default_value = "true")]
    clipboard_enabled: bool,

    /// Enable window manager monitoring
    #[arg(long, default_value = "true")]
    window_manager_enabled: bool,

    /// Window manager type
    #[arg(long, default_value = "hyprland")]
    window_manager_type: String,

    /// Clipboard polling interval in seconds
    #[arg(long, default_value = "2")]
    clipboard_poll_interval_secs: u64,
}

fn create_config_from_args(args: &Args) -> DesktopConfig {
    DesktopConfig {
        clipboard_enabled: args.clipboard_enabled,
        window_manager_enabled: args.window_manager_enabled,
        window_manager_type: args.window_manager_type.clone(),
        clipboard_poll_interval_secs: args.clipboard_poll_interval_secs,
    }
}

#[tokio::main]
async fn main() -> SatelliteResult<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();

    info!(
        "Starting Desktop Satellite (clipboard: {}, wm: {} {})",
        args.clipboard_enabled, args.window_manager_enabled, args.window_manager_type
    );

    // Run the satellite
    match run_satellite().await {
        Ok(()) => {
            info!("Desktop satellite completed successfully");
            Ok(())
        }
        Err(e) => {
            error!("Desktop satellite failed: {}", e);
            Err(e)
        }
    }
}

async fn run_satellite() -> SatelliteResult<()> {
    let args = Args::parse();
    
    // Create configuration from args
    let config = create_config_from_args(&args);

    // Set config environment variable for satellite to pick up
    std::env::set_var("SINEX_DESKTOP_CONFIG", serde_json::to_string(&config)?);

    // Create ingest client
    let ingest_client = IngestClient::new(&args.satellite.ingest_socket_path).await?;

    // Create and configure desktop satellite
    let desktop_satellite = DesktopSatellite::with_config(config);

    // Create and run the satellite
    let mut runner = EventSourceRunner::new(desktop_satellite, ingest_client);
    
    // Create config map for initialization
    let mut config_map = std::collections::HashMap::new();
    config_map.insert(
        "clipboard_enabled".to_string(),
        serde_json::Value::Bool(args.clipboard_enabled),
    );
    config_map.insert(
        "window_manager_enabled".to_string(),
        serde_json::Value::Bool(args.window_manager_enabled),
    );
    config_map.insert(
        "window_manager_type".to_string(),
        serde_json::Value::String(args.window_manager_type),
    );
    config_map.insert(
        "clipboard_poll_interval_secs".to_string(),
        serde_json::Value::Number(serde_json::Number::from(args.clipboard_poll_interval_secs)),
    );

    // Set working directory
    let work_dir = args.satellite.work_dir.unwrap_or_else(|| {
        dirs::cache_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
            .join("sinex-desktop-satellite")
    });

    // Initialize runner
    runner
        .initialize(
            args.satellite.service_name,
            config_map,
            args.satellite.batch_size,
            args.satellite.batch_timeout,
            work_dir,
            args.satellite.dry_run,
        )
        .await?;

    // Run the satellite
    runner.run().await?;

    Ok(())
}