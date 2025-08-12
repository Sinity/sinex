//! CLI for fs-watcher with sensd integration option

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;
use sinex_core::SanitizedPath;
use sinex_fs_watcher::{
    FilesystemProcessor, SensdIntegrationConfig, run_with_sensd,
};
use std::str::FromStr;
use tracing::info;

/// Extension trait for SensdIntegrationConfig construction
trait SensdIntegrationConfigExt {
    fn from_args(
        database_url: Option<String>,
        grpc_endpoint: String,
        batch_size: usize,
        processing_interval_ms: u64,
    ) -> Self;
}

impl SensdIntegrationConfigExt for SensdIntegrationConfig {
    fn from_args(
        database_url: Option<String>,
        grpc_endpoint: String,
        batch_size: usize,
        processing_interval_ms: u64,
    ) -> Self {
        let mut config = SensdIntegrationConfig::default();
        
        if let Some(db_url) = database_url {
            config.database_url = db_url;
        }
        
        config.sensd_grpc_endpoint = grpc_endpoint;
        config.batch_size = batch_size;
        config.processing_interval_ms = processing_interval_ms;
        
        config
    }
}

/// Validate and parse filesystem path for watching
pub fn validate_watch_path(s: &str) -> Result<SanitizedPath, String> {
    if s.is_empty() {
        return Err("Watch path cannot be empty".to_string());
    }
    SanitizedPath::from_str(s)
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run with traditional direct filesystem monitoring
    Direct {
        /// Paths to watch
        #[arg(value_name = "PATH", value_parser = validate_watch_path)]
        paths: Vec<SanitizedPath>,
        
        /// Debounce delay in milliseconds
        #[arg(long, default_value_t = 100)]
        debounce_ms: u64,
    },
    
    /// Run with sensd integration for MaterialSliceStream
    Sensd {
        /// Database URL for sensd tables
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
        
        /// gRPC endpoint for sensd service
        #[arg(long, default_value = "http://localhost:50051")]
        grpc_endpoint: String,
        
        /// Batch size for processing slices
        #[arg(long, default_value_t = 100)]
        batch_size: usize,
        
        /// Processing interval in milliseconds
        #[arg(long, default_value_t = 1000)]
        processing_interval_ms: u64,
    },
}

pub async fn run() -> Result<()> {
    let args = Args::parse();
    
    match args.command {
        Commands::Direct { paths, debounce_ms } => {
            run_direct_mode(paths, debounce_ms).await
        }
        Commands::Sensd {
            database_url,
            grpc_endpoint,
            batch_size,
            processing_interval_ms,
        } => {
            run_sensd_mode(database_url, grpc_endpoint, batch_size, processing_interval_ms).await
        }
    }
}

/// Run in direct filesystem watching mode
async fn run_direct_mode(paths: Vec<SanitizedPath>, debounce_ms: u64) -> Result<()> {
    info!("Running fs-watcher in direct mode");
    info!("Watching paths: {:?}", paths);
    info!("Debounce: {}ms", debounce_ms);
    
    // Run traditional fs-watcher
    // This would use the existing FilesystemProcessor
    Err(color_eyre::eyre::eyre!("Direct mode not supported"))
}

/// Run with sensd integration mode
async fn run_sensd_mode(
    database_url: Option<String>,
    grpc_endpoint: String,
    batch_size: usize,
    processing_interval_ms: u64,
) -> Result<()> {
    info!("Running fs-watcher with sensd integration");
    
    let config = SensdIntegrationConfig::from_args(
        database_url,
        grpc_endpoint,
        batch_size,
        processing_interval_ms,
    );
    
    info!("Configuration: {:?}", config);
    
    // Run with sensd integration
    run_with_sensd(config).await
}