#![doc = include_str!("../docs/service.md")]

//! Main ingestion service implementation.

// Local crate imports
use crate::{
    config::IngestdConfig, validator::EventValidator, IngestdResult, JetStreamTopology, SinexError,
};
use sinex_core::JsonValue;

// External crates
use async_nats::{jetstream, Client as NatsClient};
use serde::Serialize;
use sinex_core::db::advisory_lock::AdvisoryLock;
use sinex_core::environment as sinex_environment;
use sinex_core::types::utils::ResourceGuard;
use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
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
    sync::Mutex,
    sync::RwLock,
    task::JoinHandle,
    time::{interval, Duration},
};
use tracing::{error, info, warn};

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
    task_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
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
            let client = config.nats.connect().await.map_err(|e| {
                SinexError::network(format!("Failed to connect to NATS: {e}"))
                    .with_operation("service.connect_nats")
                    .with_context("nats_url", config.nats.url.clone())
            })?;
            let js = jetstream::new(client.clone());

            (Some(client), Some(js))
        };

        // Initialize event validator
        let validator = if let Some(ref pool) = db_pool {
            // Ensure only one instance performs migration/schema sync at a time.
            let _migration_lock = try_acquire_migration_lock(pool).await?;

            // First, synchronize schemas from codebase to database
            if !config.dry_run && !config.skip_schema_sync {
                let sync_result = crate::schema_sync::synchronize_schemas(pool)
                    .await
                    .map_err(|e| {
                        SinexError::service(format!("Failed to synchronize schemas: {e}"))
                            .with_operation("service.schema_sync")
                    })?;

                info!(
                    discovered = sync_result.discovered,
                    created = sync_result.created,
                    updated = sync_result.updated,
                    unchanged = sync_result.unchanged,
                    "Schema synchronization completed"
                );
            }

            EventValidator::load_schemas_from_db(pool, config.validate_schemas).await?
        } else {
            EventValidator::new(false)
        };

        if let Some(ref nats_client) = nats_client {
            if let Some(ref pool) = db_pool {
                if let Err(e) = Self::broadcast_active_schemas(&validator, nats_client, pool).await
                {
                    warn!("Failed to broadcast schemas: {}", e);
                }
            }
        }

        // Initialize telemetry (we'll set up the channel after service is created)

        let service = Self {
            config: config.clone(),
            db_pool,
            nats_client,
            jetstream,
            validator: Arc::new(RwLock::new(validator)),
            stats: Arc::new(IngestStats::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            task_handles: Arc::new(Mutex::new(Vec::new())),
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
                let handle = self
                    .start_jetstream_consumer_task(nats_client.clone(), pool.clone())
                    .await;
                self.track_task(handle).await;
            }
        }

        // Start MaterialAssembler task
        if let Some(ref nats_client) = self.nats_client {
            if let Some(ref pool) = self.db_pool {
                let handle = self
                    .start_material_assembler_task(nats_client.clone(), pool.clone())
                    .await;
                self.track_task(handle).await;
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
        self.track_task(stats_handle).await;

        // Schema reload task
        if let Some(ref pool) = self.db_pool {
            let handle = self
                .start_schema_reload_task(pool.clone(), self.nats_client.clone())
                .await;
            self.track_task(handle).await;
        }

        // Notify systemd that we're ready
        if let Err(e) = sd_notify::notify(true, &[sd_notify::NotifyState::Ready]) {
            warn!("Failed to notify systemd ready state: {}", e);
        }

        // Wait for shutdown signal
        shutdown_signal(&self.shutdown_flag).await;

        // Ensure background tasks have a chance to shut down before closing resources.
        self.wait_for_tasks(Duration::from_secs(5)).await;

        info!("Ingestion service stopped");
        Ok(())
    }

    /// Start the JetStream consumer task
    async fn start_jetstream_consumer_task(
        &self,
        nats_client: NatsClient,
        pool: PgPool,
    ) -> JoinHandle<()> {
        let shutdown_flag = self.shutdown_flag.clone();
        let validator = self.validator.clone();
        let env = sinex_environment();
        let topology = JetStreamTopology::new(
            &env,
            self.config.nats_stream_name.clone(),
            self.config.nats_consumer_name.clone(),
            self.config.nats_namespace.as_deref(),
        );

        let fetch_timeout = self.config.consumer_fetch_timeout_ms.as_duration();
        let fetch_max = self.config.consumer_fetch_max_messages.max(1);
        let max_ack_pending = self.config.consumer_max_ack_pending;

        tokio::spawn(async move {
            let consumer = crate::JetStreamConsumer::new(
                nats_client,
                pool.clone(),
                validator.clone(),
                topology,
            )
            .with_batch_fetch_config(fetch_max, fetch_timeout)
            .with_max_ack_pending(max_ack_pending);

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
        })
    }

    /// Start the MaterialAssembler task
    async fn start_material_assembler_task(
        &self,
        nats_client: NatsClient,
        pool: PgPool,
    ) -> JoinHandle<()> {
        let shutdown_flag = self.shutdown_flag.clone();
        let annex_repo_path = self.config.annex_repo_path.clone();
        let assembler_state_dir = self.config.assembler_state_dir.clone();
        let namespace = self.config.nats_namespace.clone();
        let slices_max_ack_pending = self.config.material_slices_max_ack_pending;

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

            let assembler = match crate::MaterialAssembler::new(
                nats_client,
                pool,
                git_annex,
                state_dir,
                namespace.clone(),
                slices_max_ack_pending,
            ) {
                Ok(assembler) => assembler,
                Err(e) => {
                    error!("Failed to create MaterialAssembler: {}", e);
                    return;
                }
            };

            let result = assembler.run_with_shutdown(shutdown_flag.clone()).await;
            if shutdown_flag.load(Ordering::Relaxed) {
                info!("MaterialAssembler shutting down");
            }
            match result {
                Ok(()) => info!("MaterialAssembler completed"),
                Err(e) => error!("MaterialAssembler failed: {}", e),
            }
        })
    }

    /// Start schema reload task
    async fn start_schema_reload_task(
        &self,
        pool: PgPool,
        nats_client: Option<NatsClient>,
    ) -> JoinHandle<()> {
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
                        } else if let Some(nc) = &nats_client {
                            if let Err(e) = Self::broadcast_active_schemas(&validator_guard, nc, &pool).await {
                                warn!("Failed to broadcast active schemas: {}", e);
                            }
                        }
                    }
                    _ = shutdown_signal(&shutdown_flag) => {
                        break;
                    }
                }
            }
        })
    }

    async fn track_task(&self, handle: JoinHandle<()>) {
        let mut handles = self.task_handles.lock().await;
        handles.push(handle);
    }

    async fn wait_for_tasks(&self, timeout: Duration) {
        let mut handles = self.task_handles.lock().await;
        for mut handle in handles.drain(..) {
            let timeout_sleep = tokio::time::sleep(timeout);
            tokio::pin!(timeout_sleep);

            tokio::select! {
                result = &mut handle => {
                    if let Err(join_err) = result {
                        error!("Background task panicked: {:?}", join_err);
                    }
                }
                _ = &mut timeout_sleep => {
                    warn!("Background task did not shutdown in time; aborting");
                    handle.abort();
                    if let Err(join_err) = handle.await {
                        if !join_err.is_cancelled() {
                            error!("Background task failed after abort: {:?}", join_err);
                        }
                    }
                }
            }
        }
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> IngestdResult<()> {
        info!("Initiating graceful shutdown");

        self.shutdown_flag.store(true, Ordering::Relaxed);

        // Let background tasks observe the flag and finish before tearing down shared state.
        self.wait_for_tasks(Duration::from_secs(5)).await;

        // Close database connections
        if let Some(pool) = &self.db_pool {
            info!("Closing ingestd database pool");
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
            task_handles: self.task_handles.clone(),
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

const MIGRATION_LOCK_KEY: &str = "ingestd:migrations";

pub async fn try_acquire_migration_lock(
    pool: &PgPool,
) -> IngestdResult<ResourceGuard<AdvisoryLock>> {
    match AdvisoryLock::try_acquire(pool, MIGRATION_LOCK_KEY).await {
        Ok(Some(guard)) => Ok(guard),
        Ok(None) => Err(SinexError::service(
            "Another ingestd instance is already applying migrations",
        )
        .with_operation("service.migration_lock")),
        Err(err) => Err(
            SinexError::service(format!("Failed to acquire migration lock: {err}"))
                .with_operation("service.migration_lock"),
        ),
    }
}

#[derive(Serialize)]
struct SchemaBroadcastEntry {
    name: String,
    version: String,
    schema_id: String,
}

impl IngestService {
    async fn broadcast_active_schemas(
        validator: &EventValidator,
        nats_client: &NatsClient,
        pool: &PgPool,
    ) -> IngestdResult<()> {
        let env = sinex_environment();
        let subject = env.nats_subject("system.schemas.active");
        let js = jetstream::new(nats_client.clone());

        let entries: Vec<SchemaBroadcastEntry> = validator
            .get_available_schemas()
            .into_iter()
            .map(|s| SchemaBroadcastEntry {
                name: s.name,
                version: (*s.version).clone(),
                schema_id: s.schema_id.to_string(),
            })
            .collect();

        // Store full schemas in NATS KV for node-side validation
        if let Err(e) = Self::store_schemas_in_kv(&entries, pool, &js).await {
            warn!("Failed to store schemas in KV: {}", e);
        }

        // Broadcast metadata for cache invalidation signal
        js.publish(subject, serde_json::to_vec(&entries)?.into())
            .await
            .map_err(|e| SinexError::network(format!("Failed to publish schema broadcast: {e}")))?
            .await
            .map_err(|e| SinexError::network(format!("Failed to confirm schema broadcast: {e}")))?;

        info!(
            count = entries.len(),
            "Broadcasted active schemas snapshot to JetStream"
        );

        Ok(())
    }

    /// Store full schema JSON in NATS KV for node validation
    async fn store_schemas_in_kv(
        entries: &[SchemaBroadcastEntry],
        pool: &PgPool,
        js: &jetstream::Context,
    ) -> IngestdResult<()> {
        use sinex_core::Ulid;

        // Create or get KV bucket
        let kv_config = jetstream::kv::Config {
            bucket: "KV_sinex_schemas".to_string(),
            history: 5,
            ..Default::default()
        };

        let kv = match js.create_key_value(kv_config).await {
            Ok(store) => store,
            Err(_) => js
                .get_key_value("KV_sinex_schemas")
                .await
                .map_err(|e| SinexError::kv(format!("Failed to get schema KV bucket: {e}")))?,
        };

        // For each schema entry, fetch full JSON from DB and store in KV
        for entry in entries {
            let schema_id = entry
                .schema_id
                .parse::<Ulid>()
                .map_err(|e| SinexError::validation(format!("Invalid schema ID: {e}")))?;

            // Fetch full schema from database using regular query to avoid ulid/uuid casting issues
            let schema_json: Option<JsonValue> = sqlx::query_scalar(
                r#"
                SELECT schema_content
                FROM sinex_schemas.event_payload_schemas
                WHERE id::uuid = $1 AND is_active = true
                "#,
            )
            .bind(schema_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| SinexError::database(format!("Failed to fetch schema: {e}")))?;

            if let Some(json) = schema_json {
                let key = format!("schema:{}", entry.schema_id);
                let payload = serde_json::to_vec(&json).map_err(|e| {
                    SinexError::serialization(format!("Failed to serialize schema: {e}"))
                })?;

                kv.put(&key, payload.into())
                    .await
                    .map_err(|e| SinexError::kv(format!("Failed to store schema in KV: {e}")))?;
            }
        }

        info!(
            count = entries.len(),
            "Stored full schemas in NATS KV for node validation"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_service() -> IngestService {
        IngestService {
            config: IngestdConfig::builder().build(),
            db_pool: None,
            nats_client: None,
            jetstream: None,
            validator: Arc::new(RwLock::new(EventValidator::new(false))),
            stats: Arc::new(IngestStats::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            task_handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[tokio::test]
    async fn wait_for_tasks_aborts_hung_tasks_before_shutdown() {
        struct CancelFlag(Arc<AtomicBool>);

        impl Drop for CancelFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let service = test_service();
        let cancelled = Arc::new(AtomicBool::new(false));

        let handle_cancelled = cancelled.clone();
        let handle = tokio::spawn(async move {
            let _guard = CancelFlag(handle_cancelled);
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        service.task_handles.lock().await.push(handle);

        service.wait_for_tasks(Duration::from_millis(10)).await;

        assert!(cancelled.load(Ordering::SeqCst));
    }
}
