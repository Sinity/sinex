use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{error, info};

use crate::{
    create_agent_manifest, event_types, sources, DatabaseConfig, DatabaseService, ManifestManager,
    EventSink, DatabaseSink, LogSink, FileSink,
};

/// Common CLI arguments for all ingestors
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct CommonCli<T: Subcommand> {
    /// Path to configuration file
    #[arg(short, long, env = "SINEX_CONFIG")]
    pub config: Option<PathBuf>,

    /// Override database URL from config
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Override log level from config
    #[arg(long, env = "RUST_LOG")]
    pub log_level: Option<String>,
    
    /// Run in dry-run mode (log events instead of inserting to database)
    #[arg(long)]
    pub dry_run: bool,
    
    /// Write events to file (implies dry-run)
    #[arg(long)]
    pub output_file: Option<PathBuf>,

    /// Command to run
    #[command(subcommand)]
    pub command: Option<T>,
}

/// Common commands that all ingestors should support
#[derive(Subcommand, Debug, Clone)]
pub enum CommonCommands {
    /// Run the ingestor (default)
    Run,
    /// Check database connectivity
    Check,
    /// Show current configuration
    Config,
    /// Generate example configuration file
    GenerateConfig {
        /// Output file path (stdout if not specified)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

/// Trait for ingestor-specific configuration
pub trait IngestorConfig: Serialize + DeserializeOwned + Default + Clone {
    /// Load configuration from default locations
    fn load() -> Result<Self>;
    
    /// Load configuration from specific file
    fn load_from_file(path: &Path) -> Result<Self>;
    
    /// Get database URL
    fn database_url(&self) -> &str;
    
    /// Set database URL
    fn set_database_url(&mut self, url: String);
    
    /// Get database max connections
    fn database_max_connections(&self) -> u32;
    
    /// Get database connection timeout in seconds
    fn database_connection_timeout_secs(&self) -> u64;
    
    /// Get logging level
    fn log_level(&self) -> &str;
    
    /// Set logging level
    fn set_log_level(&mut self, level: String);
}

/// Base trait for ingestors
#[async_trait]
pub trait Ingestor: Sized {
    /// The ingestor's configuration type
    type Config: IngestorConfig;
    
    /// The ingestor's CLI command type (can be CommonCommands or extended)
    type Commands: Subcommand + Into<CommonCommands> + Clone;
    
    /// Get the ingestor name
    fn name() -> &'static str;
    
    /// Get the ingestor description
    fn description() -> &'static str;
    
    /// Get the event types this ingestor produces
    fn produces_events() -> HashMap<String, Vec<String>>;
    
    /// Initialize the ingestor with config and event sink
    async fn new(config: Self::Config, event_sink: Arc<dyn EventSink>) -> Result<Self>;
    
    /// Run the main ingestor logic
    async fn run(&mut self) -> Result<()>;
    
    /// Handle custom commands (if any)
    async fn handle_custom_command(&self, _command: &Self::Commands) -> Result<()> {
        Ok(())
    }
}

/// Main application framework for ingestors
pub struct IngestorApp<I: Ingestor> {
    config: I::Config,
    db: Option<Arc<DatabaseService>>,
    event_sink: Option<Arc<dyn EventSink>>,
    dry_run: bool,
    _phantom: std::marker::PhantomData<I>,
}

impl<I: Ingestor> IngestorApp<I> {
    /// Parse CLI and run the application
    pub async fn run_from_cli() -> Result<()> {
        let cli = CommonCli::<I::Commands>::parse();
        
        // Load configuration
        let mut config = if let Some(config_path) = &cli.config {
            info!("Loading configuration from: {}", config_path.display());
            I::Config::load_from_file(config_path)?
        } else {
            I::Config::load()?
        };

        // Override with CLI arguments
        if let Some(ref database_url) = cli.database_url {
            config.set_database_url(database_url.clone());
        }
        if let Some(ref log_level) = cli.log_level {
            config.set_log_level(log_level.clone());
        }

        // Initialize logging
        init_logging(config.log_level());

        // Create app instance  
        let app = Self::new(config, cli.dry_run, cli.output_file).await?;
        
        // Handle command - default to Run if not specified
        if let Some(command) = cli.command {
            app.handle_command(command).await
        } else {
            // Default to run command
            app.run_ingestor().await
        }
    }

    async fn new(config: I::Config, dry_run: bool, output_file: Option<PathBuf>) -> Result<Self> {
        // Only initialize database for non-dry-run mode
        let needs_db = !dry_run && output_file.is_none();
        
        let db = if needs_db {
            let db_config = DatabaseConfig {
                url: config.database_url().to_string(),
                max_connections: config.database_max_connections(),
                min_connections: 2,
                acquire_timeout: std::time::Duration::from_secs(
                    config.database_connection_timeout_secs()
                ),
                idle_timeout: std::time::Duration::from_secs(600),
            };
            
            Some(Arc::new(DatabaseService::new(db_config).await?))
        } else {
            None
        };
        
        // Create event sink based on CLI options
        let event_sink = if let Some(ref file_path) = output_file {
            info!("Writing events to file: {}", file_path.display());
            Some(Arc::new(FileSink::new(file_path.clone()).await?) as Arc<dyn EventSink>)
        } else if dry_run {
            info!("Running in dry-run mode - events will be logged");
            Some(Arc::new(LogSink::new(I::name())) as Arc<dyn EventSink>)
        } else if let Some(db) = &db {
            Some(Arc::new(DatabaseSink::new(Arc::clone(db))) as Arc<dyn EventSink>)
        } else {
            None
        };

        Ok(Self {
            config,
            db,
            event_sink,
            dry_run: dry_run || output_file.is_some(),
            _phantom: std::marker::PhantomData,
        })
    }

    async fn handle_command(&self, command: I::Commands) -> Result<()> {
        match command.clone().into() {
            CommonCommands::Run => self.run_ingestor().await,
            CommonCommands::Check => self.check_database().await,
            CommonCommands::Config => self.show_config().await,
            CommonCommands::GenerateConfig { output } => {
                self.generate_config(output.as_ref()).await
            }
        }
    }

    async fn run_ingestor(&self) -> Result<()> {
        info!("Starting {} ingestor", I::name());
        
        if self.dry_run {
            info!("Running in DRY-RUN mode - no database operations will be performed");
        }

        // Register agent manifest (skip in dry-run mode)
        if !self.dry_run {
            self.register_manifest().await?;
        }

        // Create and run the ingestor
        let event_sink = self.event_sink.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Event sink not initialized"))?;
        let mut ingestor = I::new(self.config.clone(), Arc::clone(event_sink)).await?;
        ingestor.run().await
    }

    async fn register_manifest(&self) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| anyhow::anyhow!("Database not initialized"))?;
        let manifest_manager = ManifestManager::new(db.pool().clone());
        
        let mut produces = I::produces_events();
        
        // Add common sinex events
        produces.insert(
            sources::SINEX.to_string(),
            vec![
                event_types::event_types::sinex::AGENT_HEARTBEAT.to_string(),
                event_types::event_types::sinex::AGENT_ERROR.to_string(),
                event_types::event_types::sinex::AGENT_DLQ_EVENT_WRITTEN.to_string(),
            ],
        );

        let manifest = create_agent_manifest(
            I::name(),
            I::description(),
            env!("CARGO_PKG_VERSION"),
            produces,
        );
        
        manifest_manager.register_agent(&manifest).await?;
        Ok(())
    }

    async fn check_database(&self) -> Result<()> {
        info!("Checking database connectivity...");
        
        let db = self.db.as_ref().ok_or_else(|| anyhow::anyhow!("Database not initialized"))?;
        match db.health_check().await {
            Ok(()) => {
                info!("✅ Database connection successful");
                println!("Database connection successful");
                Ok(())
            }
            Err(e) => {
                error!("❌ Database connection failed: {}", e);
                eprintln!("Database connection failed: {}", e);
                Err(e.into())
            }
        }
    }

    async fn show_config(&self) -> Result<()> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        println!("{}", config_json);
        Ok(())
    }

    async fn generate_config(&self, output: Option<&PathBuf>) -> Result<()> {
        let default_config = I::Config::default();
        let config_str = toml::to_string_pretty(&default_config)?;
        
        if let Some(path) = output {
            std::fs::write(path, config_str)
                .context("Failed to write configuration file")?;
            info!("Generated configuration file: {}", path.display());
            println!("Generated configuration file: {}", path.display());
        } else {
            println!("{}", config_str);
        }
        
        Ok(())
    }
}

/// Initialize logging with the given level
pub fn init_logging(level: &str) {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// Standard main function for ingestors
#[macro_export]
macro_rules! ingestor_main {
    ($ingestor:ty) => {
        #[tokio::main]
        async fn main() {
            use sinex_shared::ingestor_framework::IngestorApp;
            
            let exit_code = match IngestorApp::<$ingestor>::run_from_cli().await {
                Ok(()) => 0,
                Err(e) => {
                    tracing::error!("Application failed: {}", e);
                    eprintln!("Error: {}", e);
                    
                    // Print error chain
                    let mut current = e.source();
                    while let Some(err) = current {
                        eprintln!("  Caused by: {}", err);
                        current = err.source();
                    }
                    
                    1
                }
            };

            std::process::exit(exit_code);
        }
    };
}