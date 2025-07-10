use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use sinex_db::{
    models::WorkQueueItem, agent::upsert_agent_manifest, DbPool, DbPoolRef, JsonValue,
};
use sinex_automaton::{
    create_work_entries, get_active_manifests, EventScanner, ScannerConfig, WorkRouter,
};
use sinex_worker::{start_metrics_server, worker::Worker, EventProcessor};
use sinex_services::{
    AnalyticsService, ContentService, PkmService, SearchService,
};
use sinex_annex::BlobManager;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::path::PathBuf;
use std::time::Duration;
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

    /// Annex repository path for blob storage
    #[arg(long, env = "SINEX_ANNEX_PATH", default_value = "/tmp/sinex-annex")]
    annex_path: PathBuf,
}

/// Service container holding all service instances
struct ServiceContainer {
    analytics: Arc<AnalyticsService>,
    content: Arc<ContentService>,
    pkm: Arc<PkmService>,
    search: Arc<SearchService>,
}

impl ServiceContainer {
    async fn new(pool: DbPool, annex_path: PathBuf) -> Result<Self> {
        // Create blob manager for content service
        let annex_config = sinex_annex::AnnexConfig {
            repo_path: annex_path,
            num_copies: None,
            large_files: None,
        };
        let blob_manager = Arc::new(
            BlobManager::new(annex_config, pool.clone())?
        );
        
        // Initialize all services
        Ok(Self {
            analytics: Arc::new(AnalyticsService::new(pool.clone())),
            content: Arc::new(ContentService::new(pool.clone(), blob_manager)),
            pkm: Arc::new(PkmService::new(pool.clone())),
            search: Arc::new(SearchService::new(pool)),
        })
    }
}

/// Event processor that routes events to the appropriate service methods
struct ServiceBasedProcessor {
    agent_name: String,
    batch_size: i32,
    poll_interval: u64,
    events_processed: Arc<AtomicU64>,
    services: Arc<ServiceContainer>,
}

#[async_trait]
impl EventProcessor for ServiceBasedProcessor {
    async fn process_event(&self, pool: DbPoolRef<'_>, item: &WorkQueueItem) -> Result<()> {
        // Get the event
        let event = sinex_db::events::get_event_by_id(pool, item.raw_event_id).await?;

        info!(
            agent = %self.agent_name,
            event_id = %event.id,
            source = %event.source,
            event_type = %event.event_type,
            "Processing event via service layer"
        );

        // Route events based on source and type to appropriate service methods
        match (event.source.as_str(), event.event_type.as_str()) {
            // Filesystem events might trigger content analysis
            ("fs", "file.created") | ("fs", "file.modified") => {
                info!("Filesystem event detected - would trigger content analysis");
                // In the future, this might:
                // - Extract text from documents
                // - Generate thumbnails for images
                // - Index content for search
            }
            
            // Shell events might trigger command analysis
            ("shell.kitty", "command.executed") => {
                info!("Command execution detected - would analyze command patterns");
                // In the future, this might:
                // - Extract command patterns
                // - Build command frequency statistics
                // - Detect workflow patterns
            }
            
            // Clipboard events might trigger entity extraction
            ("clipboard", "copied") => {
                info!("Clipboard event detected - would extract entities");
                // In the future, this might:
                // - Extract URLs, emails, code snippets
                // - Create knowledge graph entities
                // - Link to related events
            }
            
            // Default: just log that we processed it
            _ => {
                info!(
                    "Event processed: {} :: {}",
                    event.source,
                    event.event_type
                );
            }
        }

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

async fn register_agent(pool: DbPoolRef<'_>, agent_name: &str) -> Result<()> {
    let version = env!("CARGO_PKG_VERSION");

    // Register the agent
    upsert_agent_manifest(
        pool,
        agent_name,
        version,
        Some("Event automation worker that routes events to service layer"),
        "automation",
        serde_json::json!({
            "uses_services": true,
            "service_routing": true
        }),
        serde_json::json!({
            "sinex.agent.heartbeat": [{"type": "heartbeat"}]
        }),
        serde_json::json!({
            "raw.events_feed_all": [{"note": "Subscribes to all events for routing to services"}]
        }),
        serde_json::json!({})
    )
    .await?;

    info!(agent_name = %agent_name, version = %version, "Agent registered");
    Ok(())
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

    info!("Starting sinex-automaton");

    // Create database pool
    let database_url = args.database_url.clone();
    let pool = sinex_db::create_pool(&database_url).await?;

    // Create service container
    let services = Arc::new(ServiceContainer::new(pool.clone(), args.annex_path.clone()).await?);

    // Run in scanner mode or worker mode
    if args.scanner_mode || args.agent_name.is_none() {
        run_scanner_mode(pool, args).await
    } else {
        let agent_name = args.agent_name.clone().unwrap();
        // Register the agent
        register_agent(&pool, &agent_name).await?;
        run_worker_mode(pool, agent_name, args, services).await
    }
}

/// Run as a scanner that creates work queue entries
async fn run_scanner_mode(pool: DbPool, args: Args) -> Result<()> {
    info!("Running in scanner mode");

    // Set up graceful shutdown
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    // Create scanner with configuration
    let config = ScannerConfig {
        batch_size: args.scanner_batch_size,
        initial_lookback: chrono::Duration::hours(24),
        process_historical: false,
    };
    let mut scanner = EventScanner::new(config);

    // Start heartbeat emission task
    let heartbeat_pool = pool.clone();
    let heartbeat_shutdown = shutdown.clone();
    let heartbeat_handle = task::spawn(async move {
        use sinex_core::HeartbeatEmitter;
        let emitter = HeartbeatEmitter::new(heartbeat_pool, "automaton-scanner".to_string(), 45);

        // Run heartbeat until shutdown
        tokio::select! {
            _ = emitter.run() => {
                warn!("Heartbeat emitter stopped unexpectedly");
            }
            _ = async {
                while !heartbeat_shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            } => {
                info!("Heartbeat emitter shutting down gracefully");
            }
        }
    });
    info!("Started heartbeat emission for automaton-scanner");

    // Start metrics server
    let metrics_handle = task::spawn(async move {
        if let Err(e) = start_metrics_server(args.metrics_port).await {
            error!(error = %e, "Metrics server failed");
        }
    });

    // Notify systemd that we're ready
    match sd_notify::notify(true, &[sd_notify::NotifyState::Ready]) {
        Ok(_) => info!("Notified systemd: ready"),
        Err(e) => info!("Running without systemd integration: {}", e),
    }

    // Set up signal handlers
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    // Main scanner loop
    let mut shutdown_requested = false;
    loop {
        // Check for shutdown signals
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
                shutdown_requested = true;
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, initiating graceful shutdown");
                shutdown_requested = true;
            }
            result = scan_and_promote(&pool, &mut scanner) => {
                match result {
                    Ok(count) => {
                        if count == 0 {
                            // No new events, sleep before next scan
                            tokio::select! {
                                _ = sleep(Duration::from_secs(args.poll_interval)) => {},
                                _ = signal::ctrl_c() => {
                                    info!("Received SIGINT during sleep");
                                    shutdown_requested = true;
                                }
                                _ = sigterm.recv() => {
                                    info!("Received SIGTERM during sleep");
                                    shutdown_requested = true;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Scanner error, retrying in 5s");
                        let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Status("Scanner error, retrying")]);

                        tokio::select! {
                            _ = sleep(Duration::from_secs(5)) => {},
                            _ = signal::ctrl_c() => {
                                info!("Received SIGINT during error retry");
                                shutdown_requested = true;
                            }
                            _ = sigterm.recv() => {
                                info!("Received SIGTERM during error retry");
                                shutdown_requested = true;
                            }
                        }
                    }
                }
            }
        }

        if shutdown_requested {
            break;
        }
    }

    // Notify systemd we're stopping
    let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Stopping]);

    // Signal shutdown to all tasks
    shutdown_clone.store(true, Ordering::Relaxed);

    // Wait for tasks to complete
    info!("Waiting for tasks to complete...");

    // Wait for heartbeat task
    match tokio::time::timeout(tokio::time::Duration::from_secs(5), heartbeat_handle).await {
        Ok(Ok(_)) => info!("Heartbeat task completed"),
        Ok(Err(e)) => warn!("Heartbeat task failed: {}", e),
        Err(_) => warn!("Heartbeat task timed out"),
    }

    // Abort metrics server
    metrics_handle.abort();

    info!("Scanner shutdown complete");
    Ok(())
}

/// Scan for new events and create work entries
async fn scan_and_promote(pool: DbPoolRef<'_>, scanner: &mut EventScanner) -> Result<usize> {
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

/// Metrics provider that tracks events processed
struct WorkerMetrics {
    events_processed: Arc<AtomicU64>,
    start_time: std::time::Instant,
}

impl sinex_core::MetricsProvider for WorkerMetrics {
    fn get_events_processed_last_minute(&self) -> u32 {
        // Simplified - returns total events processed
        // In a real implementation, you'd track events per minute
        self.events_processed.load(Ordering::Relaxed) as u32
    }

    fn get_errors_last_hour(&self) -> u32 {
        // No error tracking yet
        0
    }

    fn get_last_error_message(&self) -> Option<String> {
        None
    }

    fn get_custom_metrics(&self) -> JsonValue {
        serde_json::json!({
            "total_events_processed": self.events_processed.load(Ordering::Relaxed),
            "uptime_seconds": self.start_time.elapsed().as_secs()
        })
    }
}

/// Run as a worker processing work queue entries
async fn run_worker_mode(
    pool: DbPool, 
    agent_name: String, 
    args: Args,
    services: Arc<ServiceContainer>
) -> Result<()> {
    info!(agent = %agent_name, "Running in worker mode with service layer");

    // Set up graceful shutdown
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    // Create shared state for tracking events processed
    let events_processed = Arc::new(AtomicU64::new(0));
    let start_time = std::time::Instant::now();

    // Create metrics provider
    let metrics = WorkerMetrics {
        events_processed: events_processed.clone(),
        start_time,
    };

    // Start unified component heartbeat emission task with metrics
    let heartbeat_pool = pool.clone();
    let heartbeat_agent_name = agent_name.clone();
    let heartbeat_shutdown = shutdown.clone();
    let heartbeat_handle = task::spawn(async move {
        use sinex_core::HeartbeatEmitter;
        let emitter = HeartbeatEmitter::with_metrics_provider(
            heartbeat_pool,
            heartbeat_agent_name,
            45,
            metrics,
        );

        // Run heartbeat until shutdown
        tokio::select! {
            _ = emitter.run() => {
                warn!("Heartbeat emitter stopped unexpectedly");
            }
            _ = async {
                while !heartbeat_shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            } => {
                info!("Heartbeat emitter shutting down gracefully");
            }
        }
    });
    info!(agent = %agent_name, "Started component heartbeat emission with metrics");

    // Start metrics server
    let metrics_handle = task::spawn(async move {
        if let Err(e) = start_metrics_server(args.metrics_port).await {
            error!(error = %e, "Metrics server failed");
        }
    });

    // Create processor with service layer
    let processor = Arc::new(ServiceBasedProcessor {
        agent_name: agent_name.clone(),
        batch_size: args.batch_size,
        poll_interval: args.poll_interval,
        events_processed: events_processed.clone(),
        services,
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

    // Notify systemd that we're ready
    match sd_notify::notify(true, &[sd_notify::NotifyState::Ready]) {
        Ok(_) => info!("Notified systemd: ready"),
        Err(e) => info!("Running without systemd integration: {}", e),
    }

    // Set up signal handlers
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    // Run worker until shutdown signal
    let worker_handle = task::spawn(async move {
        if let Err(e) = worker.run().await {
            error!(error = %e, "Worker failed");
            let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Status("Worker failed")]);
        }
    });

    // Wait for shutdown signal
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received SIGINT (Ctrl+C), initiating graceful shutdown");
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM, initiating graceful shutdown");
        }
    }

    // Notify systemd we're stopping
    let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Stopping]);

    // Signal shutdown to all tasks
    shutdown_clone.store(true, Ordering::Relaxed);

    // Cancel worker task
    worker_handle.abort();

    // Wait for tasks to complete
    info!("Waiting for tasks to complete...");

    // Wait for heartbeat task
    match tokio::time::timeout(tokio::time::Duration::from_secs(5), heartbeat_handle).await {
        Ok(Ok(_)) => info!("Heartbeat task completed"),
        Ok(Err(e)) => warn!("Heartbeat task failed: {}", e),
        Err(_) => warn!("Heartbeat task timed out"),
    }

    // Abort metrics server
    metrics_handle.abort();

    info!("Worker shutdown complete");
    Ok(())
}
