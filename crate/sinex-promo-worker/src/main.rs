use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use sinex_db::{
    models::{AgentHeartbeat, WorkQueueItem, RawEvent},
    queries::{insert_raw_event, update_agent_heartbeat, upsert_agent_manifest},
};
use sinex_promo_worker::{create_work_entries, get_active_manifests, EventScanner, WorkRouter, ScannerConfig};
use sinex_worker::{start_metrics_server, worker::Worker, EventProcessor};
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::{signal, task, time::sleep};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Agent name to process events for (optional, if not provided runs promotion scanner)
    #[arg(long, env = "AGENT_NAME")]
    agent_name: Option<String>,

    /// Worker ID (defaults to hostname-pid)
    #[arg(long, env = "WORKER_ID")]
    worker_id: Option<String>,

    /// Metrics port
    #[arg(long, env = "METRICS_PORT", default_value = "9090")]
    metrics_port: u16,

    /// Batch size for processing
    #[arg(long, env = "BATCH_SIZE", default_value = "10")]
    batch_size: i32,

    /// Poll interval in seconds
    #[arg(long, env = "POLL_INTERVAL", default_value = "1")]
    poll_interval: u64,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,
    
    /// Run as promotion scanner instead of worker
    #[arg(long, default_value = "false")]
    scanner_mode: bool,
    
    /// Scanner batch size
    #[arg(long, env = "SCANNER_BATCH_SIZE", default_value = "1000")]
    scanner_batch_size: usize,
}

/// Example processor that logs events
/// 
/// This is a reference implementation showing how to build a processor.
/// In production, you would:
/// 1. Parse the event payload according to its schema
/// 2. Transform/enrich the data as needed  
/// 3. Insert into domain-specific tables
/// 4. Generate derived events if needed
struct ExampleProcessor {
    agent_name: String,
    batch_size: i32,
    poll_interval: u64,
    events_processed: Arc<AtomicU64>,
}

#[async_trait]
impl EventProcessor for ExampleProcessor {
    async fn process_event(&self, pool: &PgPool, item: &WorkQueueItem) -> Result<()> {
        // Fetch the raw event - need to handle ULID conversion manually
        let record = sqlx::query!(
            r#"
            SELECT 
                id::uuid as "id!", 
                source as "source!", 
                event_type as "event_type!", 
                ts_ingest as "ts_ingest!",
                ts_orig,
                host as "host!", 
                ingestor_version, 
                payload_schema_id::uuid as "payload_schema_id", 
                payload as "payload!"
            FROM raw.events 
            WHERE id = $1::uuid::ulid
            "#,
            uuid::Uuid::from(item.raw_event_id)
        )
        .fetch_one(pool)
        .await?;
        
        let event = RawEvent {
            id: record.id.into(),
            source: record.source,
            event_type: record.event_type,
            ts_ingest: record.ts_ingest,
            ts_orig: record.ts_orig,
            host: record.host,
            ingestor_version: record.ingestor_version,
            payload_schema_id: record.payload_schema_id.map(Into::into),
            payload: record.payload,
        };

        info!(
            agent = %self.agent_name,
            event_id = %event.id,
            source = %event.source,
            event_type = %event.event_type,
            "Processing event"
        );

        // Example: Just log the event payload
        info!(
            agent = %self.agent_name,
            payload = %event.payload,
            "Event payload"
        );

        // In a real implementation, you would:
        // 1. Parse the payload according to its schema
        // 2. Transform/enrich the data
        // 3. Insert into domain-specific tables
        // 4. Generate derived events if needed
        
        // Track processed events
        self.events_processed.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    fn agent_name(&self) -> &str {
        &self.agent_name
    }

    fn batch_size(&self) -> i32 {
        self.batch_size
    }

    fn poll_interval_secs(&self) -> u64 {
        self.poll_interval
    }
}

async fn register_agent(pool: &PgPool, agent_name: &str) -> Result<()> {
    let version = env!("CARGO_PKG_VERSION");
    
    // Register the agent
    upsert_agent_manifest(
        pool,
        agent_name,
        version,
        "running",
        "promoter",
        Some("Example promotion worker that logs events"),
        Some(serde_json::json!({
            "sinex.agent.heartbeat": [{"type": "heartbeat"}]
        })),
        Some(serde_json::json!({
            "raw.events_feed_all": [{"note": "Subscribes to all events for demo purposes"}]
        })),
    )
    .await?;

    info!(agent_name = %agent_name, version = %version, "Agent registered");
    Ok(())
}

async fn emit_heartbeat(
    pool: PgPool, 
    agent_name: String,
    events_processed: Arc<AtomicU64>,
    start_time: Instant,
) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    
    loop {
        interval.tick().await;
        
        let uptime = start_time.elapsed().as_secs();
        let events_count = events_processed.load(Ordering::Relaxed);
        
        let heartbeat = AgentHeartbeat {
            agent_name: agent_name.clone(),
            status: "running".to_string(),
            uptime_seconds: uptime,
            events_processed_session: events_count,
            dlq_size: 0, // Still TODO: Would need DLQ manager integration
            version: env!("CARGO_PKG_VERSION").to_string(),
        };

        match insert_raw_event(
            &pool,
            "sinex.agent.heartbeat",
            "heartbeat",
            &gethostname::gethostname().to_string_lossy(),
            serde_json::to_value(&heartbeat)?,
            Some(chrono::Utc::now()),
            Some(env!("CARGO_PKG_VERSION")),
            None,
        )
        .await
        {
            Ok(_) => {
                let _ = update_agent_heartbeat(&pool, &agent_name).await;
            }
            Err(e) => {
                warn!(error = %e, "Failed to emit heartbeat");
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Extract log level before args is moved
    let log_level = args.log_level.clone();

    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting sinex-promo-worker");

    // Create database pool
    let database_url = args.database_url.clone();
    let pool = sinex_db::create_pool(&database_url).await?;
    
    // Run in scanner mode or worker mode
    if args.scanner_mode || args.agent_name.is_none() {
        run_scanner_mode(pool, args).await
    } else {
        let agent_name = args.agent_name.clone().unwrap();
        // Register the agent
        register_agent(&pool, &agent_name).await?;
        run_worker_mode(pool, agent_name, args).await
    }
}

/// Run as a scanner that creates work queue entries
async fn run_scanner_mode(pool: PgPool, args: Args) -> Result<()> {
    info!("Running in scanner mode");
    
    // Create scanner with configuration
    let config = ScannerConfig {
        batch_size: args.scanner_batch_size,
        initial_lookback: chrono::Duration::hours(24),
        process_historical: false,
    };
    let mut scanner = EventScanner::new(config);
    
    // Start heartbeat emission task
    let heartbeat_pool = pool.clone();
    task::spawn(async move {
        use sinex_core::HeartbeatEmitter;
        let emitter = HeartbeatEmitter::new(heartbeat_pool, "promo-worker-scanner".to_string(), 45);
        emitter.run().await;
    });
    info!("Started heartbeat emission for promo-worker-scanner");
    
    // Start metrics server
    let metrics_handle = task::spawn(async move {
        if let Err(e) = start_metrics_server(args.metrics_port).await {
            error!(error = %e, "Metrics server failed");
        }
    });
    
    // Main scanner loop
    loop {
        match scan_and_promote(&pool, &mut scanner).await {
            Ok(count) => {
                if count == 0 {
                    // No new events, sleep before next scan
                    sleep(Duration::from_secs(args.poll_interval)).await;
                }
            }
            Err(e) => {
                error!(error = %e, "Scanner error, retrying in 5s");
                sleep(Duration::from_secs(5)).await;
            }
        }
        
        // Check for shutdown signal
        if tokio::select! {
            _ = signal::ctrl_c() => true,
            else => false,
        } {
            info!("Shutdown signal received");
            metrics_handle.abort();
            break;
        }
    }
    
    Ok(())
}

/// Scan for new events and create work entries
async fn scan_and_promote(pool: &PgPool, scanner: &mut EventScanner) -> Result<usize> {
    // Get active agent manifests
    let manifests = get_active_manifests(pool).await?;
    let router = WorkRouter::from_manifests(manifests);
    
    // Scan for new events
    let events = scanner.scan_new_events(pool).await?;
    
    if events.is_empty() {
        return Ok(0);
    }
    
    // Create work entries
    let count = create_work_entries(pool, events, &router).await?;
    
    Ok(count)
}

/// Run as a worker processing work queue entries
async fn run_worker_mode(pool: PgPool, agent_name: String, args: Args) -> Result<()> {
    info!(agent = %agent_name, "Running in worker mode");

    // Create shared state for tracking
    let events_processed = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    // Start legacy heartbeat task  
    let heartbeat_pool = pool.clone();
    let heartbeat_agent_name = agent_name.clone();
    let heartbeat_events = events_processed.clone();
    task::spawn(async move {
        if let Err(e) = emit_heartbeat(
            heartbeat_pool, 
            heartbeat_agent_name,
            heartbeat_events,
            start_time,
        ).await {
            error!(error = %e, "Heartbeat task failed");
        }
    });

    // Start new component heartbeat emission task
    let new_heartbeat_pool = pool.clone();
    task::spawn(async move {
        use sinex_core::HeartbeatEmitter;
        let emitter = HeartbeatEmitter::new(new_heartbeat_pool, "promo-worker".to_string(), 45);
        emitter.run().await;
    });
    info!("Started component heartbeat emission for promo-worker");

    // Start metrics server
    let metrics_handle = task::spawn(async move {
        if let Err(e) = start_metrics_server(args.metrics_port).await {
            error!(error = %e, "Metrics server failed");
        }
    });

    // Create processor
    let processor = Arc::new(ExampleProcessor {
        agent_name: agent_name.clone(),
        batch_size: args.batch_size,
        poll_interval: args.poll_interval,
        events_processed: events_processed.clone(),
    });

    // Create worker
    let worker_id = args.worker_id.unwrap_or_else(|| {
        format!(
            "{}-{}",
            gethostname::gethostname().to_string_lossy(),
            std::process::id()
        )
    });
    
    let worker = Worker::new(pool, processor, worker_id);

    // Run worker until shutdown signal
    let worker_handle = task::spawn(async move {
        if let Err(e) = worker.run().await {
            error!(error = %e, "Worker failed");
        }
    });

    // Wait for shutdown signal
    signal::ctrl_c().await?;
    info!("Shutdown signal received");

    // Cancel tasks (they should handle cancellation gracefully)
    worker_handle.abort();
    metrics_handle.abort();

    Ok(())
}
