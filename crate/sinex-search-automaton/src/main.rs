//! Search Service Automaton Binary
//!
//! Standalone binary that runs the search service as an automaton

use anyhow::Result;
use clap::Parser;
use sinex_db::create_pool;
use sinex_satellite_sdk::{EventFilter, HotlogAutomatonRunner, IngestClient, RedisStreamClient};
use sinex_search_automaton::SearchServiceAutomaton;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "sinex-search-automaton")]
#[command(about = "Search service automaton")]
struct Cli {
    /// Configuration file path
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Service name
    #[arg(long, default_value = "search")]
    service_name: String,

    /// Host identifier
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Working directory
    #[arg(long, default_value = "/tmp/sinex-search")]
    work_dir: PathBuf,

    /// Dry run mode
    #[arg(long)]
    dry_run: bool,

    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Redis URL
    #[arg(long, env = "REDIS_URL", default_value = "redis://localhost:6379")]
    redis_url: String,

    /// gRPC ingest service socket path
    #[arg(
        long,
        env = "INGEST_SOCKET_PATH",
        default_value = "/tmp/sinex-ingestd.sock"
    )]
    ingest_socket_path: String,

    /// Consumer group name
    #[arg(long, default_value = "search-service-group")]
    consumer_group: String,

    /// Consumer name
    #[arg(long, default_value = "search-consumer")]
    consumer_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sinex_search_automaton=info".into()),
        )
        .init();

    let cli = Cli::parse();

    // Create search automaton
    let search_automaton = SearchServiceAutomaton::new();

    // Configure and run the automaton
    let mut runner = HotlogAutomatonRunner::new(search_automaton);

    // Initialize database connection pool
    let db_pool = create_pool(
        cli.database_url
            .as_deref()
            .unwrap_or("postgresql:///sinex_dev?host=/run/postgresql"),
    )
    .await?;

    // Initialize Redis client
    let redis_client = RedisStreamClient::new(&cli.redis_url)?;

    // Initialize ingest client
    let ingest_client = IngestClient::new(&cli.ingest_socket_path).await?;

    // Set up event filters for search RPC requests
    let event_filters = vec![EventFilter::new(
        Some("rpc.search".to_string()),
        Some("request".to_string()),
    )];

    // Initialize the runner
    runner
        .initialize(
            cli.service_name.clone(),
            cli.consumer_group,
            cli.consumer_name,
            event_filters,
            HashMap::new(), // No additional config for now
            db_pool,
            redis_client,
            ingest_client,
            cli.work_dir,
            cli.dry_run,
        )
        .await?;

    // Run the automaton
    runner.run().await?;

    Ok(())
}
