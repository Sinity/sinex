//! System Satellite Main Binary

use clap::Parser;
use sinex_satellite_sdk::{EventSourceRunner, IngestClient, SatelliteArgs, SatelliteResult};
use sinex_system_satellite::{SystemConfig, SystemSatellite, SystemdConfig, DbusConfig, JournalConfig};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(flatten)]
    satellite: SatelliteArgs,

    /// Enable D-Bus monitoring
    #[arg(long, default_value = "true")]
    dbus_enabled: bool,

    /// Enable systemd journal monitoring
    #[arg(long, default_value = "true")]
    journal_enabled: bool,
    
    /// Enable udev hardware monitoring
    #[arg(long, default_value = "true")]
    udev_enabled: bool,
    
    /// Enable systemd unit monitoring
    #[arg(long, default_value = "true")]
    systemd_enabled: bool,

    /// D-Bus buses to monitor ("session", "system", or "both")
    #[arg(long, default_value = "both")]
    dbus_buses: String,

    /// Journal follow timeout in seconds
    #[arg(long, default_value = "5")]
    journal_timeout_secs: u64,
}

fn create_config_from_args(args: &Args) -> SystemConfig {
    SystemConfig {
        dbus_enabled: args.dbus_enabled,
        journal_enabled: args.journal_enabled,
        udev_enabled: args.udev_enabled,
        systemd_enabled: args.systemd_enabled,
        dbus_buses: args.dbus_buses.clone(),
        journal_timeout_secs: args.journal_timeout_secs,
        systemd_config: SystemdConfig::default(),
        dbus_config: DbusConfig::default(),
        journal_config: JournalConfig::default(),
    }
}

#[tokio::main]
async fn main() -> SatelliteResult<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();

    info!(
        "Starting System Satellite (dbus: {} {}, journal: {}, udev: {}, systemd: {})",
        args.dbus_enabled, args.dbus_buses, args.journal_enabled, args.udev_enabled, args.systemd_enabled
    );

    // Run the satellite
    match run_satellite().await {
        Ok(()) => {
            info!("System satellite completed successfully");
            Ok(())
        }
        Err(e) => {
            error!("System satellite failed: {}", e);
            Err(e)
        }
    }
}

async fn run_satellite() -> SatelliteResult<()> {
    let args = Args::parse();
    
    // Create configuration from args
    let config = create_config_from_args(&args);

    // Set config environment variable for satellite to pick up
    std::env::set_var("SINEX_SYSTEM_CONFIG", serde_json::to_string(&config)?);

    // Create ingest client
    let ingest_client = IngestClient::new(&args.satellite.ingest_socket_path).await?;

    // Create and configure system satellite
    let system_satellite = SystemSatellite::with_config(config);

    // Create and run the satellite
    let mut runner = EventSourceRunner::new(system_satellite, ingest_client);
    
    // Create config map for initialization
    let mut config_map = std::collections::HashMap::new();
    config_map.insert(
        "dbus_enabled".to_string(),
        serde_json::Value::Bool(args.dbus_enabled),
    );
    config_map.insert(
        "journal_enabled".to_string(),
        serde_json::Value::Bool(args.journal_enabled),
    );
    config_map.insert(
        "dbus_buses".to_string(),
        serde_json::Value::String(args.dbus_buses),
    );
    config_map.insert(
        "journal_timeout_secs".to_string(),
        serde_json::Value::Number(serde_json::Number::from(args.journal_timeout_secs)),
    );

    // Set working directory
    let work_dir = args.satellite.work_dir.unwrap_or_else(|| {
        dirs::cache_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
            .join("sinex-system-satellite")
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