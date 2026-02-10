#![doc = include_str!("../docs/service.md")]

//! Main ingestion service implementation.

// Local crate imports
use crate::{
    config::IngestdConfig, material_ready_set::MaterialReadySet, validator::EventValidator,
    IngestdResult, JetStreamTopology, SinexError,
};
use sinex_primitives::error::ResultExt;

// External crates
use async_nats::{jetstream, Client as NatsClient};
use serde::Serialize;
use sinex_db::advisory_lock::AdvisoryLock;
use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
use sinex_node_sdk::{SelfObserver, SelfObserverConfig};
use sinex_primitives::environment as sinex_environment;
use sinex_primitives::utils::ResourceGuard;
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
use tracing::{debug, error, info, warn};

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
    observer: Arc<SelfObserver>,
    shutdown_flag: Arc<AtomicBool>,
    task_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl IngestService {
    /// Create a new ingestion service
    pub async fn new(config: IngestdConfig) -> IngestdResult<Self> {
        info!("Initializing ingestion service");

        let db_pool = Self::init_db_pool(&config).await?;
        let (nats_client, jetstream) = Self::init_nats(&config).await?;
        let validator = Self::init_validator(&config, db_pool.as_ref()).await?;

        if let (Some(nats), Some(pool)) = (&nats_client, &db_pool) {
            if let Err(e) = Self::broadcast_active_schemas(&validator, nats, pool).await {
                warn!("Failed to broadcast schemas: {}", e);
            }
        }

        let observer = Self::init_observer(&nats_client);

        let service = Self {
            config: config.clone(),
            db_pool,
            nats_client,
            jetstream,
            validator: Arc::new(RwLock::new(validator)),
            stats: Arc::new(IngestStats::new()),
            observer: Arc::new(observer),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            task_handles: Arc::new(Mutex::new(Vec::new())),
        };

        info!("Ingestion service initialized successfully");
        Ok(service)
    }

    async fn init_db_pool(config: &IngestdConfig) -> IngestdResult<Option<PgPool>> {
        if config.dry_run {
            return Ok(None);
        }

        let pool = config
            .get_db_options()
            .connect(&config.database_url)
            .await
            .map_err(|e| {
                SinexError::database(format!(
                    "Failed to connect to database at {}: {e}",
                    config.database_url
                ))
            })?;
        Ok(Some(pool))
    }

    async fn init_nats(
        config: &IngestdConfig,
    ) -> IngestdResult<(Option<NatsClient>, Option<jetstream::Context>)> {
        if config.dry_run {
            return Ok((None, None));
        }

        let client = config.nats.connect().await.map_err(|e| {
            SinexError::network(format!(
                "Failed to connect to NATS at {}: {e}",
                config.nats.url
            ))
            .with_operation("service.connect_nats")
            .with_context("nats_url", config.nats.url.clone())
        })?;
        let js = jetstream::new(client.clone());

        Ok((Some(client), Some(js)))
    }

    async fn init_validator(
        config: &IngestdConfig,
        pool: Option<&PgPool>,
    ) -> IngestdResult<EventValidator> {
        if let Some(pool) = pool {
            let _lock = try_acquire_migration_lock(pool).await?;

            if !config.dry_run && !config.skip_schema_sync {
                Self::sync_schemas(pool).await?;
            }

            if config.strict_validation {
                EventValidator::load_schemas_from_db_strict(pool, config.validate_schemas).await
            } else {
                EventValidator::load_schemas_from_db(pool, config.validate_schemas).await
            }
        } else if config.strict_validation {
            Ok(EventValidator::new_strict(false))
        } else {
            Ok(EventValidator::new(false))
        }
    }

    async fn sync_schemas(pool: &PgPool) -> IngestdResult<()> {
        let sync_result = crate::schema_sync::synchronize_schemas(pool)
            .await
            .context("Failed to synchronize event schemas from codebase to database")
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
        Ok(())
    }

    fn init_observer(nats_client: &Option<NatsClient>) -> SelfObserver {
        match nats_client {
            Some(nats) => {
                let config = SelfObserverConfig::from_env("sinex-ingestd");
                SelfObserver::new(nats.clone(), config)
            }
            None => SelfObserver::disabled(),
        }
    }

    /// Run the ingestion service
    pub async fn run(&mut self) -> IngestdResult<()> {
        info!("Starting ingestion service");

        // Create shared MaterialReadySet for cross-consumer coordination.
        // This prevents FK violations when events arrive before their material's BEGIN
        // message is processed (separate NATS streams, no cross-stream ordering).
        let ready_set = MaterialReadySet::new();
        if let Some(pool) = &self.db_pool {
            if let Err(e) = ready_set.seed_from_db(pool).await {
                warn!("Failed to seed MaterialReadySet from database: {}", e);
                // Non-fatal: events will be deferred until materials are registered
            }
        }

        // Start JetStream and MaterialAssembler tasks (critical - failure stops service)
        let js_handle = match (&self.nats_client, &self.db_pool) {
            (Some(nats), Some(pool)) => Some(
                self.start_jetstream_consumer_task(nats.clone(), pool.clone(), ready_set.clone())
                    .await,
            ),
            _ => None,
        };

        let ma_handle = match (&self.nats_client, &self.db_pool) {
            (Some(nats), Some(pool)) => Some(
                self.start_material_assembler_task(nats.clone(), pool.clone(), ready_set.clone())
                    .await,
            ),
            _ => None,
        };

        // Start background tasks
        self.start_stats_logging_task().await;

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

        // Monitor critical tasks - exit on first failure or shutdown signal
        let monitor_result = self.monitor_runtime(js_handle, ma_handle).await;

        // Ensure background tasks have a chance to shut down before closing resources.
        self.wait_for_tasks(Duration::from_secs(5)).await;

        info!("Ingestion service stopped");
        monitor_result
    }

    /// Start stats logging task with self-observation emission
    async fn start_stats_logging_task(&self) {
        let stats = self.stats.clone();
        let observer = self.observer.clone();
        let shutdown_flag = self.shutdown_flag.clone();

        let stats_handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_mins(1));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        stats.log_stats();

                        // Emit metrics via self-observation
                        if observer.is_enabled() {
                            let events_processed = stats.events_processed.load(Ordering::Relaxed);
                            let events_received = stats.events_received.load(Ordering::Relaxed);
                            let validation_errors = stats.validation_errors.load(Ordering::Relaxed);
                            let db_errors = stats.db_errors.load(Ordering::Relaxed);

                            if let Err(e) = observer.emit_node_processing_stats(
                                "ingestd",
                                events_processed,
                                events_received.saturating_sub(events_processed),
                                None,
                                0,
                                validation_errors + db_errors,
                            ).await {
                                warn!("Failed to emit self-observation metrics: {}", e);
                            }
                        }
                    }
                    () = shutdown_signal(&shutdown_flag) => {
                        break;
                    }
                }
            }
        });
        self.track_task(stats_handle).await;
    }

    /// Monitor critical tasks - exit on first failure or shutdown signal
    async fn monitor_runtime(
        &self,
        js_handle: Option<JoinHandle<IngestdResult<()>>>,
        ma_handle: Option<JoinHandle<IngestdResult<()>>>,
    ) -> IngestdResult<()> {
        let shutdown_flag = self.shutdown_flag.clone();

        tokio::select! {
            // JetStream consumer exited
            result = async {
                match js_handle {
                    Some(handle) => handle.await,
                    None => std::future::pending().await,
                }
            } => {
                Self::handle_task_result("JetStream consumer", result, &shutdown_flag)
            }

            // MaterialAssembler exited
            result = async {
                match ma_handle {
                    Some(handle) => handle.await,
                    None => std::future::pending().await,
                }
            } => {
                Self::handle_task_result("MaterialAssembler", result, &shutdown_flag)
            }

            // Normal shutdown signal
            () = shutdown_signal(&shutdown_flag) => {
                info!("Received shutdown signal");
                Ok(())
            }
        }
    }

    fn handle_task_result(
        name: &str,
        result: Result<IngestdResult<()>, tokio::task::JoinError>,
        shutdown_flag: &Arc<AtomicBool>,
    ) -> IngestdResult<()> {
        match result {
            Ok(res) => Self::handle_join_success(name, res, shutdown_flag),
            Err(e) => Self::handle_join_error(name, e, shutdown_flag),
        }
    }

    fn handle_join_success(
        name: &str,
        result: IngestdResult<()>,
        shutdown_flag: &Arc<AtomicBool>,
    ) -> IngestdResult<()> {
        match result {
            Ok(()) if shutdown_flag.load(Ordering::Relaxed) => {
                info!("{name} completed during shutdown");
                Ok(())
            }
            Ok(()) => {
                error!("{name} exited unexpectedly without error");
                shutdown_flag.store(true, Ordering::Relaxed);
                Err(SinexError::service(format!("{name} exited unexpectedly")))
            }
            Err(e) => {
                error!(error = %e, "{name} failed");
                shutdown_flag.store(true, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    fn handle_join_error(
        name: &str,
        err: tokio::task::JoinError,
        shutdown_flag: &Arc<AtomicBool>,
    ) -> IngestdResult<()> {
        error!(error = ?err, "{name} panicked");
        shutdown_flag.store(true, Ordering::Relaxed);
        Err(SinexError::service(format!("{name} panicked: {err}")))
    }

    /// Start the `JetStream` consumer task
    async fn start_jetstream_consumer_task(
        &self,
        nats_client: NatsClient,
        pool: PgPool,
        ready_set: MaterialReadySet,
    ) -> JoinHandle<IngestdResult<()>> {
        let shutdown_flag = self.shutdown_flag.clone();
        let validator = self.validator.clone();
        let observer = self.observer.clone();
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
            .with_max_ack_pending(max_ack_pending)
            .with_ready_set(ready_set)
            .with_observer(observer);

            tokio::select! {
                result = consumer.run() => {
                    match result {
                        Ok(()) => {
                            info!("JetStream consumer completed normally");
                            Ok(())
                        }
                        Err(e) => {
                            error!(error = %e, "JetStream consumer failed");
                            Err(e)
                        }
                    }
                }
                () = shutdown_signal(&shutdown_flag) => {
                    info!("JetStream consumer shutting down");
                    Ok(())
                }
            }
        })
    }

    /// Start the `MaterialAssembler` task
    async fn start_material_assembler_task(
        &self,
        nats_client: NatsClient,
        pool: PgPool,
        ready_set: MaterialReadySet,
    ) -> JoinHandle<IngestdResult<()>> {
        let shutdown_flag = self.shutdown_flag.clone();
        let observer = self.observer.clone();
        let annex_repo_path = self.config.annex_repo_path.clone();
        let assembler_state_dir = self.config.assembler_state_dir.clone();
        let namespace = self.config.nats_namespace.clone();
        let slices_max_ack_pending = self.config.material_slices_max_ack_pending;
        let max_concurrent_assemblies = self.config.max_concurrent_assemblies;

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
                        error = %e,
                        "Failed to initialize git-annex repository"
                    );
                    return Err(SinexError::service(format!(
                        "Failed to initialize git-annex at {annex_repo_path}: {e}"
                    )));
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
                max_concurrent_assemblies,
                ready_set,
            ) {
                Ok(assembler) => assembler.with_observer(observer),
                Err(e) => {
                    error!(error = %e, "Failed to create MaterialAssembler");
                    return Err(e);
                }
            };

            let result = assembler.run_with_shutdown(shutdown_flag.clone()).await;
            if shutdown_flag.load(Ordering::Relaxed) {
                info!("MaterialAssembler shutting down normally");
                Ok(())
            } else {
                match result {
                    Ok(()) => {
                        info!("MaterialAssembler completed normally");
                        Ok(())
                    }
                    Err(e) => {
                        error!(error = %e, "MaterialAssembler failed");
                        Err(e)
                    }
                }
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
            let mut interval = interval(Duration::from_mins(5)); // Reload every 5 minutes

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
                    () = shutdown_signal(&shutdown_flag) => {
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
        let mut handles = {
            let mut guard = self.task_handles.lock().await;
            std::mem::take(&mut *guard)
        };

        if handles.is_empty() {
            return;
        }

        info!(
            "Waiting for {} background tasks to finish...",
            handles.len()
        );

        let wait_task = async {
            for (i, handle) in handles.iter_mut().enumerate() {
                if let Err(e) = handle.await {
                    if let Ok(panic) = e.try_into_panic() {
                        let msg = match panic.downcast_ref::<&'static str>() {
                            Some(s) => *s,
                            None => match panic.downcast_ref::<String>() {
                                Some(s) => s.as_str(),
                                None => "Unknown panic",
                            },
                        };
                        error!("Background task {} panicked: {}", i, msg);
                    } else {
                        debug!("Background task {} was cancelled or failed", i);
                    }
                }
            }
        };

        if tokio::time::timeout(timeout, wait_task).await.is_err() {
            warn!(
                "Timed out waiting for background tasks after {:?}, aborting {} remaining",
                timeout,
                handles.len()
            );
            for handle in &handles {
                handle.abort();
            }
            // Await aborted handles so their destructors run before we return.
            for handle in handles {
                let _ = handle.await;
            }
        } else {
            info!("All background tasks finished");
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
            observer: self.observer.clone(),
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
    match AdvisoryLock::try_acquire(pool, MIGRATION_LOCK_KEY)
        .await
        .context("Failed to acquire advisory lock for schema migrations")
    {
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

        // Broadcast metadata for cache invalidation signal using core NATS pub/sub.
        // This is fire-and-forget since it's just a notification - no durability needed.
        nats_client
            .publish(subject.clone(), serde_json::to_vec(&entries)?.into())
            .await
            .map_err(|e| SinexError::network(format!("Failed to publish schema broadcast: {e}")))?;

        info!(
            count = entries.len(),
            "Broadcasted active schemas snapshot to NATS"
        );

        Ok(())
    }

    /// Store full schema JSON in NATS KV for node validation
    async fn store_schemas_in_kv(
        entries: &[SchemaBroadcastEntry],
        pool: &PgPool,
        js: &jetstream::Context,
    ) -> IngestdResult<()> {
        use sinex_db::repositories::DbPoolExt;
        use sinex_primitives::Ulid;

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

        // Parse schema IDs and fetch in bulk via centralized repository
        let schema_ids: Vec<Ulid> = entries
            .iter()
            .filter_map(|entry| entry.schema_id.parse::<Ulid>().ok())
            .collect();

        let schemas = pool
            .schema_cache()
            .get_schemas_by_ids(&schema_ids)
            .await
            .context("Failed to fetch schema content for KV storage")
            .map_err(|e| SinexError::database(format!("Failed to fetch schemas: {e}")))?;

        // Store each schema in KV
        for schema in schemas {
            let key = format!("schema-{}", schema.id);
            let payload = serde_json::to_vec(&schema.schema_content).map_err(|e| {
                SinexError::serialization(format!("Failed to serialize schema: {e}"))
            })?;

            kv.put(&key, payload.into())
                .await
                .context(&{
                    format!(
                        "Failed to store schema {}.{} ({}) in NATS KV bucket",
                        schema.source, schema.event_type, schema.id
                    )
                })
                .map_err(|e| SinexError::kv(format!("Failed to store schema in KV: {e}")))?;
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
    use xtask::sandbox::sinex_test;

    fn test_service() -> IngestService {
        IngestService {
            config: IngestdConfig::builder().build(),
            db_pool: None,
            nats_client: None,
            jetstream: None,
            validator: Arc::new(RwLock::new(EventValidator::new(false))),
            stats: Arc::new(IngestStats::new()),
            observer: Arc::new(SelfObserver::disabled()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            task_handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[sinex_test]
    async fn wait_for_tasks_aborts_hung_tasks_before_shutdown() -> xtask::sandbox::TestResult<()> {
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
        Ok(())
    }
}
