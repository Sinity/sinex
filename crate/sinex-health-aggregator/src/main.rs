use clap::Parser;
use sinex_db::create_pool;
use sinex_satellite_sdk::{
    automaton::{HotlogAutomaton, HotlogAutomatonRunner},
    config::AutomatonConfig,
    grpc_client::IngestClient,
    redis_client::RedisStreamClient,
    satellite_main, SatelliteResult,
};
use std::path::PathBuf;
use tracing::info;

mod automaton;
use automaton::HealthAggregatorAutomaton;

#[derive(Parser, Debug)]
#[command(author, version, about = "Sinex health aggregator automaton")]
struct Args {

    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Redis URL for message bus
    #[arg(
        long,
        env = "SINEX_REDIS_URL",
        default_value = "redis://localhost:6379"
    )]
    redis_url: String,

    /// Consumer group name
    #[arg(long, default_value = "health-aggregators")]
    consumer_group: String,

    /// Consumer name (defaults to hostname-pid)
    #[arg(long)]
    consumer_name: Option<String>,

    /// Ingest socket path
    #[arg(long, default_value = "/run/sinex/ingest.sock")]
    ingest_socket_path: String,

    /// Processing batch size
    #[arg(long, default_value = "50")]
    batch_size: usize,

    /// Checkpoint interval in seconds
    #[arg(long, default_value = "30")]
    checkpoint_interval: u64,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Enable dry-run mode
    #[arg(long)]
    dry_run: bool,
}

async fn run_automaton() -> SatelliteResult<()> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(&args.log_level)
        .init();

    info!("Starting Sinex Health Aggregator Automaton");

    // Load configuration from environment and command line arguments
    let config = create_config_from_args(&args)?;

    // Create database pool
    let database_url = config.base.database_url.as_ref().ok_or_else(|| {
        sinex_satellite_sdk::SatelliteError::Config(
            sinex_satellite_sdk::config::ConfigError::MissingField(
                "Database URL is required".to_string(),
            ),
        )
    })?;
    let db_pool = create_pool(database_url).await?;

    // Create Redis client
    let redis_client = RedisStreamClient::new(&config.base.redis_url)?;

    // Create ingest client
    let ingest_client = IngestClient::new(&config.base.ingest_socket_path).await?;

    // Create automaton
    let automaton = HealthAggregatorAutomaton::new();

    // Create and initialize runner
    let mut runner = HotlogAutomatonRunner::new(automaton);

    // Get event filters from automaton (need to temporarily create it)
    let temp_automaton = HealthAggregatorAutomaton::new();
    let event_filters = temp_automaton.event_filters();

    runner
        .initialize(
            config.base.service_name.clone(),
            config.consumer_group.clone(),
            config.consumer_name.clone(),
            event_filters,
            config.automaton_config.clone(),
            db_pool,
            redis_client,
            ingest_client,
            config.base.work_dir.clone(),
            config.base.dry_run,
        )
        .await?;

    // Run the automaton
    runner.run().await?;

    info!("Health aggregator automaton stopped");
    Ok(())
}

fn create_config_from_args(args: &Args) -> SatelliteResult<AutomatonConfig> {
    use sinex_satellite_sdk::config::SatelliteConfig;
    use std::collections::HashMap;

    let database_url = args
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    let consumer_name = args
        .consumer_name
        .clone()
        .unwrap_or_else(AutomatonConfig::default_consumer_name);

    let base_config = SatelliteConfig {
        service_name: "sinex-health-aggregator".to_string(),
        log_level: args.log_level.clone(),
        ingest_socket_path: args.ingest_socket_path.clone(),
        redis_url: args.redis_url.clone(),
        database_url: Some(database_url),
        database_pool_size: 10,
        work_dir: PathBuf::from("/tmp/sinex/health-aggregator"),
        dry_run: args.dry_run,
        replay: None,
    };

    Ok(AutomatonConfig {
        base: base_config,
        consumer_group: args.consumer_group.clone(),
        consumer_name,
        topics: vec![], // Not used in hotlog architecture
        processing_batch_size: args.batch_size,
        checkpoint_interval_secs: args.checkpoint_interval,
        automaton_config: HashMap::new(),
    })
}

// Use the satellite_main macro for proper lifecycle management
satellite_main!("sinex-health-aggregator", run_automaton());
