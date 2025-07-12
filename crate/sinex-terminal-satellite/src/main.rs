use clap::Parser;
use sinex_terminal_satellite::TerminalSatellite;
use sinex_satellite_sdk::{
    event_source::EventSourceRunner,
    grpc_client::IngestClient,
    config::EventSourceConfig,
    satellite_main,
    SatelliteResult,
};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about = "Sinex unified terminal satellite")]
struct Args {
    /// Configuration file path
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Path to Unix Domain Socket for ingestd communication
    #[arg(long, default_value = "/run/sinex/ingest.sock")]
    ingest_socket: String,

    /// Enable Atuin shell history monitoring
    #[arg(long)]
    enable_atuin: bool,

    /// Enable shell history file monitoring
    #[arg(long)]
    enable_history: bool,

    /// Enable Kitty terminal integration
    #[arg(long)]
    enable_kitty: bool,

    /// Enable terminal recording monitoring
    #[arg(long)]
    enable_recording: bool,

    /// Enable scrollback capture
    #[arg(long)]
    enable_scrollback: bool,

    /// Atuin database path
    #[arg(long)]
    atuin_db_path: Option<PathBuf>,

    /// Shell history files (comma-separated)
    #[arg(long)]
    history_files: Option<String>,

    /// Polling interval in seconds
    #[arg(long, default_value = "5")]
    polling_interval: u64,

    /// Batch size for event submission
    #[arg(long, default_value = "100")]
    batch_size: usize,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Enable dry-run mode
    #[arg(long)]
    dry_run: bool,
}

async fn run_satellite() -> SatelliteResult<()> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(&args.log_level)
        .init();

    info!("Starting Sinex Unified Terminal Satellite");

    // Load configuration
    let config = if let Some(config_path) = args.config {
        EventSourceConfig::load_from_file(&config_path)?
    } else {
        create_config_from_args(&args)?
    };

    // Create gRPC client for ingestd communication
    let ingest_client = IngestClient::new(&config.base.ingest_socket_path).await?;

    // Create terminal satellite
    let terminal_satellite = TerminalSatellite::new();

    // Create and initialize runner
    let mut runner = EventSourceRunner::new(terminal_satellite, ingest_client);
    
    runner.initialize(
        config.base.service_name.clone(),
        config.source_config.clone(),
        config.batch_size,
        config.batch_timeout_secs,
        config.base.work_dir.clone(),
        config.base.dry_run,
    ).await?;

    // Run the satellite
    runner.run().await?;

    info!("Terminal satellite stopped");
    Ok(())
}

fn create_config_from_args(args: &Args) -> SatelliteResult<EventSourceConfig> {
    use sinex_satellite_sdk::config::SatelliteConfig;
    
    let base_config = SatelliteConfig {
        service_name: "sinex-terminal-satellite".to_string(),
        log_level: args.log_level.clone(),
        ingest_socket_path: args.ingest_socket.clone(),
        redis_url: "redis://localhost:6379".to_string(),
        database_url: None,
        database_pool_size: 10,
        work_dir: PathBuf::from("/tmp/sinex-terminal"),
        dry_run: args.dry_run,
        replay: None,
    };

    // Create source configuration
    let mut source_config = HashMap::new();
    
    // Enable/disable sources based on args
    let mut enabled_sources = HashMap::new();
    enabled_sources.insert("atuin".to_string(), args.enable_atuin);
    enabled_sources.insert("history".to_string(), args.enable_history);
    enabled_sources.insert("kitty".to_string(), args.enable_kitty);
    enabled_sources.insert("recording".to_string(), args.enable_recording);
    enabled_sources.insert("scrollback".to_string(), args.enable_scrollback);
    
    source_config.insert("enabled_sources".to_string(), serde_json::to_value(enabled_sources)?);
    
    // Add specific configurations
    if let Some(atuin_path) = &args.atuin_db_path {
        source_config.insert("atuin_db_path".to_string(), serde_json::to_value(atuin_path)?);
    }
    
    if let Some(history_files_str) = &args.history_files {
        let history_files: Vec<PathBuf> = history_files_str
            .split(',')
            .map(|s| PathBuf::from(s.trim()))
            .collect();
        source_config.insert("history_files".to_string(), serde_json::to_value(history_files)?);
    }
    
    source_config.insert("polling_interval_secs".to_string(), serde_json::to_value(args.polling_interval)?);
    source_config.insert("batch_size".to_string(), serde_json::to_value(args.batch_size)?);

    Ok(EventSourceConfig {
        base: base_config,
        batch_size: args.batch_size,
        batch_timeout_secs: 5,
        source_config,
    })
}

// Use the satellite_main macro for proper lifecycle management
satellite_main!("sinex-terminal-satellite", run_satellite());