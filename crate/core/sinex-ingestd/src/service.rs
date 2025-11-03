#![doc = include_str!("../doc/service.md")]

//! Main ingestion service implementation.

// Local crate imports
use crate::{
    config::IngestdConfig, validator::EventValidator, IngestdResult, JetStreamTopology, SinexError,
};

// External crates
use async_nats::{jetstream, Client as NatsClient};
use sinex_core::environment as sinex_environment;
use sinex_satellite_sdk::annex::{AnnexConfig, GitAnnex};
use sqlx::PgPool;

// Standard library and common crates
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::SystemTime,
};
use tokio::{
    sync::RwLock,
    time::{interval, Duration},
};
use tracing::{error, info, warn};

// Shared ingestor version as a compile-time constant
const INGESTOR_VERSION: &str = "0.4.2";

/// Helper function to create a shutdown signal future
async fn shutdown_signal(shutdown_flag: &Arc<AtomicBool>) {
    loop {
        if shutdown_flag.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Main ingestion service
pub struct IngestService {
    config: IngestdConfig,
    db_pool: Option<PgPool>,
    nats_client: Option<NatsClient>,
    jetstream: Option<jetstream::Context>,
    validator: Arc<RwLock<EventValidator>>,
    stats: Arc<IngestStats>,
    shutdown_flag: Arc<AtomicBool>,
}

impl IngestService {
    /// Create a new ingestion service
    pub async fn new(config: IngestdConfig) -> IngestdResult<Self> {
        info!("Initializing ingestion service");

        // Initialize database pool
        let db_pool = if config.dry_run {
            None
        } else {
            let pool = config
                .get_db_options()
                .connect(&config.database_url)
                .await?;
            Some(pool)
        };

        // Initialize NATS client and JetStream
        let (nats_client, jetstream) = if config.dry_run {
            (None, None)
        } else {
            let client = async_nats::connect(&config.nats_url).await.map_err(|e| {
                SinexError::network(format!("Failed to connect to NATS: {e}"))
                    .with_operation("service.connect_nats")
                    .with_context("nats_url", config.nats_url.clone())
            })?;
            let js = jetstream::new(client.clone());

            // Create or get the events stream (subjects namespaced by environment)
            let env = sinex_environment();
            let stream_config = jetstream::stream::Config {
                name: config.nats_stream_name.clone(),
                subjects: vec![env.nats_subject("events.>")],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 10_000_000,
                max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
                storage: jetstream::stream::StorageType::File,
                num_replicas: 1,
                ..Default::default()
            };

            js.get_or_create_stream(stream_config).await.map_err(|e| {
                SinexError::network(format!("Failed to create/get stream: {e}"))
                    .with_operation("service.bootstrap_stream")
                    .with_context("stream", config.nats_stream_name.clone())
            })?;

            info!(
                "Connected to NATS JetStream stream: {}",
                config.nats_stream_name
            );

            (Some(client), Some(js))
        };

        // Initialize event validator
        let validator = if let Some(ref pool) = db_pool {
            // First, synchronize schemas from codebase to database
            if !config.dry_run && !config.skip_schema_sync {
                match crate::schema_sync::synchronize_schemas(pool).await {
                    Ok(sync_result) => {
                        info!(
                            discovered = sync_result.discovered,
                            created = sync_result.created,
                            updated = sync_result.updated,
                            unchanged = sync_result.unchanged,
                            "Schema synchronization completed"
                        );
                    }
                    Err(e) => {
                        error!("Failed to synchronize schemas: {}", e);
                        // Continue anyway - we can still use existing schemas
                    }
                }
            }

            EventValidator::load_schemas_from_db(pool, config.validate_schemas).await?
        } else {
            EventValidator::new(false)
        };

        // Initialize telemetry (we'll set up the channel after service is created)

        let service = Self {
            config: config.clone(),
            db_pool,
            nats_client,
            jetstream,
            validator: Arc::new(RwLock::new(validator)),
            stats: Arc::new(IngestStats::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        };

        info!("Ingestion service initialized successfully");
        Ok(service)
    }

    /// Run the ingestion service
    pub async fn run(&mut self) -> IngestdResult<()> {
        info!("Starting ingestion service");

        // Start background tasks
        let stats = self.stats.clone();
        let shutdown_flag = self.shutdown_flag.clone();

        // Start JetStream consumer task
        if let Some(ref nats_client) = self.nats_client {
            if let Some(ref pool) = self.db_pool {
                self.start_jetstream_consumer_task(nats_client.clone(), pool.clone())
                    .await;
            }
        }

        // Start MaterialAssembler task
        if let Some(ref nats_client) = self.nats_client {
            if let Some(ref pool) = self.db_pool {
                self.start_material_assembler_task(nats_client.clone(), pool.clone())
                    .await;
            }
        }

        // Stats logging task with panic recovery
        let stats_handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        stats.log_stats();
                    }
                    _ = shutdown_signal(&shutdown_flag) => {
                        break;
                    }
                }
            }
        });

        // Monitor the stats task for panics
        tokio::spawn(async move {
            if let Err(e) = stats_handle.await {
                error!("Stats logging task panicked: {:?}", e);
            }
        });

        // Schema reload task
        if let Some(ref pool) = self.db_pool {
            self.start_schema_reload_task(pool.clone()).await;
        }

        // Notify systemd that we're ready
        if let Err(e) = sd_notify::notify(true, &[sd_notify::NotifyState::Ready]) {
            warn!("Failed to notify systemd ready state: {}", e);
        }

        // Wait for shutdown signal
        shutdown_signal(&self.shutdown_flag).await;

        info!("Ingestion service stopped");
        Ok(())
    }

    /// Start the JetStream consumer task
    async fn start_jetstream_consumer_task(&self, nats_client: NatsClient, pool: PgPool) {
        let shutdown_flag = self.shutdown_flag.clone();
        let validator = self.validator.clone();
        let env = sinex_environment();
        let topology = JetStreamTopology::new(
            &env,
            self.config.nats_stream_name.clone(),
            self.config.nats_consumer_name.clone(),
        );

        tokio::spawn(async move {
            let consumer = crate::JetStreamConsumer::new(
                nats_client,
                pool.clone(),
                validator.clone(),
                topology,
            );

            tokio::select! {
                result = consumer.run() => {
                    match result {
                        Ok(()) => info!("JetStream consumer completed"),
                        Err(e) => error!("JetStream consumer failed: {}", e),
                    }
                }
                _ = shutdown_signal(&shutdown_flag) => {
                    info!("JetStream consumer shutting down");
                }
            }
        });
    }

    /// Start the MaterialAssembler task
    async fn start_material_assembler_task(&self, nats_client: NatsClient, pool: PgPool) {
        let shutdown_flag = self.shutdown_flag.clone();
        let annex_repo_path = self.config.annex_repo_path.clone();
        let assembler_state_dir = self.config.assembler_state_dir.clone();

        tokio::spawn(async move {
            let annex_config = AnnexConfig {
                repo_path: annex_repo_path.clone(),
                num_copies: None,
                large_files: None,
            };

            let git_annex = match GitAnnex::new(annex_config) {
                Ok(annex) => Arc::new(annex),
                Err(e) => {
                    error!(
                        path = %annex_repo_path,
                        "Failed to initialize git-annex repository: {}",
                        e
                    );
                    return;
                }
            };

            let state_dir: PathBuf = assembler_state_dir.into();

            let assembler =
                match crate::MaterialAssembler::new(nats_client, pool, git_annex, state_dir) {
                    Ok(assembler) => assembler,
                    Err(e) => {
                        error!("Failed to create MaterialAssembler: {}", e);
                        return;
                    }
                };

            tokio::select! {
                result = assembler.run() => {
                    match result {
                        Ok(()) => info!("MaterialAssembler completed"),
                        Err(e) => error!("MaterialAssembler failed: {}", e),
                    }
                }
                _ = shutdown_signal(&shutdown_flag) => {
                    info!("MaterialAssembler shutting down");
                }
            }
        });
    }

    /// Start schema reload task
    async fn start_schema_reload_task(&self, pool: PgPool) {
        let validator = self.validator.clone();
        let shutdown_flag = self.shutdown_flag.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(300)); // Reload every 5 minutes

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut validator_guard = validator.write().await;
                        if let Err(e) = validator_guard.reload_schemas(&pool).await {
                            warn!("Failed to reload schemas: {}", e);
                        }
                    }
                    _ = shutdown_signal(&shutdown_flag) => {
                        break;
                    }
                }
            }
        });
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> IngestdResult<()> {
        info!("Initiating graceful shutdown");

        self.shutdown_flag.store(true, Ordering::Relaxed);

        // Close database connections
        if let Some(pool) = &self.db_pool {
            pool.close().await;
        }

        info!("Graceful shutdown completed");
        Ok(())
    }
}

impl Clone for IngestService {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            db_pool: self.db_pool.clone(),
            nats_client: self.nats_client.clone(),
            jetstream: self.jetstream.clone(),
            validator: self.validator.clone(),
            stats: self.stats.clone(),
            shutdown_flag: self.shutdown_flag.clone(),
        }
    }
}

/// Statistics for the ingestion service
#[derive(Debug)]
struct IngestStats {
    events_received: AtomicU64,
    events_processed: AtomicU64,
    batches_processed: AtomicU64,
    validation_errors: AtomicU64,
    db_errors: AtomicU64,
    nats_errors: AtomicU64,
    start_time: SystemTime,
}

impl IngestStats {
    fn new() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            events_processed: AtomicU64::new(0),
            batches_processed: AtomicU64::new(0),
            validation_errors: AtomicU64::new(0),
            db_errors: AtomicU64::new(0),
            nats_errors: AtomicU64::new(0),
            start_time: SystemTime::now(),
        }
    }

    fn log_stats(&self) {
        let uptime = self.start_time.elapsed().unwrap_or_default().as_secs();
        let events_received = self.events_received.load(Ordering::Relaxed);
        let events_processed = self.events_processed.load(Ordering::Relaxed);
        let batches_processed = self.batches_processed.load(Ordering::Relaxed);
        let validation_errors = self.validation_errors.load(Ordering::Relaxed);
        let db_errors = self.db_errors.load(Ordering::Relaxed);
        let nats_errors = self.nats_errors.load(Ordering::Relaxed);

        let events_per_sec = if uptime > 0 {
            events_processed as f64 / uptime as f64
        } else {
            0.0
        };

        info!(
            uptime_secs = uptime,
            events_received = events_received,
            events_processed = events_processed,
            batches_processed = batches_processed,
            validation_errors = validation_errors,
            db_errors = db_errors,
            nats_errors = nats_errors,
            events_per_sec = format!("{:.2}", events_per_sec),
            "Ingestion service statistics"
        );
    }
}
