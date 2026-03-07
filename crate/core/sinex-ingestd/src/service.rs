#![doc = include_str!("../docs/service.md")]

//! Main ingestion service implementation.

// Local crate imports
use crate::{
    IngestdResult, JetStreamTopology, SinexError, config::IngestdConfig,
    material_ready_set::MaterialReadySet, validator::EventValidator,
};
// External crates
use async_nats::{Client as NatsClient, jetstream};
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
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::Mutex,
    sync::RwLock,
    task::JoinHandle,
    time::{Duration, interval},
};
use tracing::{debug, error, info, warn};

/// Awaits the shutdown signal reactively (no polling).
///
/// Returns immediately if the flag is already set, otherwise waits for
/// `shutdown_notify.notify_waiters()` to be called during graceful shutdown.
async fn shutdown_signal(
    shutdown_flag: &Arc<AtomicBool>,
    shutdown_notify: &Arc<tokio::sync::Notify>,
) {
    if shutdown_flag.load(Ordering::Relaxed) {
        return;
    }
    shutdown_notify.notified().await;
}

/// Main ingestion service
pub struct IngestService {
    config: IngestdConfig,
    db_pool: Option<PgPool>,
    nats_client: Option<NatsClient>,
    jetstream: Option<jetstream::Context>,
    validator: Arc<RwLock<EventValidator>>,
    observer: Arc<SelfObserver>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<tokio::sync::Notify>,
    task_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Clone for IngestService {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            db_pool: self.db_pool.clone(),
            nats_client: self.nats_client.clone(),
            jetstream: self.jetstream.clone(),
            validator: self.validator.clone(),
            observer: self.observer.clone(),
            shutdown_flag: self.shutdown_flag.clone(),
            shutdown_notify: self.shutdown_notify.clone(),
            task_handles: self.task_handles.clone(),
        }
    }
}

impl IngestService {
    /// Create a new ingestion service
    pub async fn new(config: IngestdConfig) -> IngestdResult<Self> {
        info!("Initializing ingestion service");

        let db_pool = Self::init_db_pool(&config).await?;
        let (nats_client, jetstream) = Self::init_nats(&config).await?;
        let validator = Self::init_validator(&config, db_pool.as_ref()).await?;

        if let (Some(nats), Some(pool)) = (&nats_client, &db_pool)
            && let Err(e) = Self::broadcast_active_schemas(&validator, nats, pool).await
        {
            warn!("Failed to broadcast schemas: {}", e);
        }

        let observer = Self::init_observer(&nats_client);

        let service = Self {
            config: config.clone(),
            db_pool,
            nats_client,
            jetstream,
            validator: Arc::new(RwLock::new(validator)),
            observer: Arc::new(observer),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
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
                SinexError::database(format!("Failed to connect to database: {e}"))
                    .with_operation("service.init_db_pool")
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
            .map_err(|e| {
                SinexError::service("Failed to synchronize schemas")
                    .with_operation("service.schema_sync")
                    .with_source(e)
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
        //
        // In test mode (namespace set), skip MaterialReadySet: tests pre-register source
        // materials directly in the database via ensure_source_material() before publishing
        // events. The MaterialReadySet only knows about materials that existed at startup
        // or arrived via the MaterialAssembler NATS stream, so test-inserted materials
        // would cause all events to be NAK'd and never persisted.
        let ready_set = if self.config.nats_namespace.is_none() {
            let set = MaterialReadySet::new();
            if let Some(pool) = &self.db_pool
                && let Err(e) = set.seed_from_db(pool).await
            {
                warn!("Failed to seed MaterialReadySet from database: {}", e);
                // Non-fatal: events will be deferred until materials are registered
            }
            Some(set)
        } else {
            info!(
                "MaterialReadySet disabled (test namespace mode — materials pre-registered in DB)"
            );
            None
        };

        // Start JetStream and MaterialAssembler tasks (critical - failure stops service)
        let (js_handle, js_ready_rx) = match (&self.nats_client, &self.db_pool) {
            (Some(nats), Some(pool)) => {
                let (h, rx) = self
                    .start_jetstream_consumer_task(nats.clone(), pool.clone(), ready_set.clone())
                    .await;
                (Some(h), Some(rx))
            }
            _ => (None, None),
        };

        let (ma_handle, ma_ready_rx) = match (&self.nats_client, &self.db_pool) {
            (Some(nats), Some(pool)) => {
                let (h, rx) = self
                    .start_material_assembler_task(nats.clone(), pool.clone(), ready_set.clone())
                    .await;
                (Some(h), Some(rx))
            }
            _ => (None, None),
        };

        if let Some(ref pool) = self.db_pool {
            let handle = self
                .start_schema_reload_task(pool.clone(), self.nats_client.clone())
                .await;
            self.track_task(handle).await;
        }

        // Start GitOps sync service if enabled
        if self.config.gitops_enabled {
            if let Some(ref pool) = self.db_pool {
                let handle = self.start_gitops_sync_task(pool.clone()).await;
                self.track_task(handle).await;
                info!("GitOps schema sync service started");
            } else {
                warn!("GitOps sync enabled but no database pool available");
            }
        }

        // Wait for both critical tasks to signal they're past their setup phase
        // (streams bound, WAL restored) before telling systemd we're ready.
        // Use a 30s timeout so a hung startup doesn't prevent systemd from detecting failure.
        let ready_timeout = Duration::from_secs(30);
        if let Some(rx) = js_ready_rx {
            match tokio::time::timeout(ready_timeout, rx).await {
                Ok(Ok(())) => info!("JetStream consumer ready"),
                // Sender dropped without sending — setup task failed before reaching the ready point.
                // monitor_runtime will observe the task exit and report the actual error.
                Ok(Err(_)) => {
                    warn!("JetStream consumer setup failed (ready channel closed without signal)");
                }
                Err(_) => warn!(
                    "JetStream consumer did not signal ready within {ready_timeout:?}; proceeding anyway"
                ),
            }
        }
        if let Some(rx) = ma_ready_rx {
            match tokio::time::timeout(ready_timeout, rx).await {
                Ok(Ok(())) => info!("MaterialAssembler ready"),
                Ok(Err(_)) => {
                    warn!("MaterialAssembler setup failed (ready channel closed without signal)");
                }
                Err(_) => warn!(
                    "MaterialAssembler did not signal ready within {ready_timeout:?}; proceeding anyway"
                ),
            }
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

    /// Monitor critical tasks - exit on first failure or shutdown signal
    async fn monitor_runtime(
        &self,
        js_handle: Option<JoinHandle<IngestdResult<()>>>,
        ma_handle: Option<JoinHandle<IngestdResult<()>>>,
    ) -> IngestdResult<()> {
        let shutdown_flag = self.shutdown_flag.clone();
        let shutdown_notify = self.shutdown_notify.clone();

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
            () = shutdown_signal(&shutdown_flag, &shutdown_notify) => {
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

    /// Start the `JetStream` consumer task, returning both the handle and a readiness receiver.
    ///
    /// The receiver fires after the durable `JetStream` consumer has been created and the pull
    /// loop is about to start. Await it before emitting `sd_notify(READY)`.
    async fn start_jetstream_consumer_task(
        &self,
        nats_client: NatsClient,
        pool: PgPool,
        ready_set: Option<MaterialReadySet>,
    ) -> (
        JoinHandle<IngestdResult<()>>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let shutdown_flag = self.shutdown_flag.clone();
        let shutdown_notify = self.shutdown_notify.clone();
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
        let stats_log_interval = Duration::from_secs(self.config.stats_log_interval_secs);

        let database_url = self.config.database_url.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut consumer = crate::JetStreamConsumer::new(
                nats_client,
                pool.clone(),
                validator.clone(),
                topology,
            )
            .with_database_url(database_url)
            .with_batch_fetch_config(fetch_max, fetch_timeout)
            .with_max_ack_pending(max_ack_pending)
            .with_stats_log_interval(stats_log_interval)
            .with_observer(observer);

            if let Some(set) = ready_set {
                consumer = consumer.with_ready_set(set);
            }

            tokio::select! {
                result = consumer.run_with_ready_signal(Some(ready_tx)) => {
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
                () = shutdown_signal(&shutdown_flag, &shutdown_notify) => {
                    info!("JetStream consumer shutting down");
                    Ok(())
                }
            }
        });
        (handle, ready_rx)
    }

    /// Start the `MaterialAssembler` task, returning the handle and a readiness receiver.
    ///
    /// The receiver fires after stream bootstrap and WAL restore complete, just before
    /// the consumer sub-tasks start. Await it before emitting `sd_notify(READY)`.
    async fn start_material_assembler_task(
        &self,
        nats_client: NatsClient,
        pool: PgPool,
        ready_set: Option<MaterialReadySet>,
    ) -> (
        JoinHandle<IngestdResult<()>>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let shutdown_flag = self.shutdown_flag.clone();
        let observer = self.observer.clone();
        let annex_repo_path = self.config.annex_repo_path.clone();
        let assembler_state_dir = self.config.assembler_state_dir.clone();
        let namespace = self.config.nats_namespace.clone();
        let slices_max_ack_pending = self.config.material_slices_max_ack_pending;
        let max_concurrent_assemblies = self.config.max_concurrent_assemblies;
        let max_buffered_slices = self.config.max_buffered_slices;
        let slice_timeout_secs = self.config.slice_timeout_secs;
        let orphan_threshold_secs = self.config.orphan_threshold_secs;
        let disk_threshold_percent = self.config.disk_threshold_percent;

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
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
                max_buffered_slices,
                slice_timeout_secs,
                orphan_threshold_secs,
                disk_threshold_percent,
            ) {
                Ok(assembler) => assembler.with_observer(observer),
                Err(e) => {
                    error!(error = %e, "Failed to create MaterialAssembler");
                    return Err(e);
                }
            };

            let result = assembler
                .run_with_shutdown_and_ready(shutdown_flag.clone(), Some(ready_tx))
                .await;
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
        });
        (handle, ready_rx)
    }

    /// Start schema reload task
    async fn start_schema_reload_task(
        &self,
        pool: PgPool,
        nats_client: Option<NatsClient>,
    ) -> JoinHandle<()> {
        let validator = self.validator.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let reload_interval = Duration::from_secs(self.config.schema_reload_interval_secs);

        tokio::spawn(async move {
            let mut interval = interval(reload_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Load fresh schemas outside the write lock (does DB I/O with only a
                        // read lock held to snapshot validation_enabled). This keeps the write
                        // lock window to microseconds rather than the full DB round-trip.
                        let load_result = {
                            let read_guard = validator.read().await;
                            read_guard.load_fresh_schemas(&pool).await
                        };
                        match load_result {
                            Err(e) => warn!("Failed to reload schemas: {}", e),
                            Ok(new_inner) => {
                                // Brief write lock: swap only, no I/O
                                let mut write_guard = validator.write().await;
                                write_guard.swap_inner(new_inner);
                                drop(write_guard);

                                if let Some(nc) = &nats_client {
                                    let read_guard = validator.read().await;
                                    if let Err(e) = Self::broadcast_active_schemas(&read_guard, nc, &pool).await {
                                        warn!("Failed to broadcast active schemas: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    () = shutdown_signal(&shutdown_flag, &shutdown_notify) => {
                        break;
                    }
                }
            }
        })
    }

    /// Start the `GitOps` schema sync background task
    async fn start_gitops_sync_task(&self, pool: PgPool) -> JoinHandle<()> {
        let shutdown_flag = self.shutdown_flag.clone();
        let work_dir = self.config.gitops_work_dir.clone().into_std_path_buf();

        tokio::spawn(async move {
            let service = crate::gitops::GitOpsSyncService::new(pool, work_dir, shutdown_flag);
            service.run().await;
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
        // Wake all tasks that are waiting in shutdown_signal() reactively.
        self.shutdown_notify.notify_waiters();

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

const MIGRATION_LOCK_KEY: &str = "ingestd.migrations";
const SCHEMA_KV_BUCKET_NAME: &str = "sinex_schemas";

pub async fn try_acquire_migration_lock(
    pool: &PgPool,
) -> IngestdResult<ResourceGuard<AdvisoryLock>> {
    match AdvisoryLock::try_acquire(pool, MIGRATION_LOCK_KEY).await {
        Ok(Some(guard)) => Ok(guard),
        Ok(None) => Err(SinexError::service(
            "Another ingestd instance is already applying migrations",
        )
        .with_operation("service.migration_lock")),
        Err(err) => Err(SinexError::service("Failed to acquire migration lock")
            .with_operation("service.migration_lock")
            .with_source(err)),
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
            .map_err(|e| {
                SinexError::network("Failed to publish schema broadcast").with_source(e)
            })?;

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
        use uuid::Uuid;

        // KV bucket name is namespaced by environment (dev/prod) to prevent cross-environment
        // schema pollution when multiple environments share the same NATS cluster.
        // NATS KV bucket names cannot contain dots, so we use underscores.
        let env = sinex_environment();
        let bucket = format!("{}_{SCHEMA_KV_BUCKET_NAME}", env.name());

        let kv_config = jetstream::kv::Config {
            bucket: bucket.clone(),
            history: 5,
            ..Default::default()
        };

        let kv = match js.create_key_value(kv_config).await {
            Ok(store) => store,
            Err(_) => js
                .get_key_value(&bucket)
                .await
                .map_err(|e| SinexError::kv("Failed to get schema KV bucket").with_source(e))?,
        };

        // Parse schema IDs and fetch in bulk via centralized repository
        let schema_ids: Vec<Uuid> = entries
            .iter()
            .filter_map(|entry| entry.schema_id.parse::<Uuid>().ok())
            .collect();

        let schemas = pool
            .schema_cache()
            .get_schemas_by_ids(&schema_ids)
            .await
            .map_err(|e| SinexError::database("Failed to fetch schemas").with_source(e))?;

        // Store each schema in KV
        for schema in schemas {
            let key = format!("schema-{}", schema.id);
            let payload = serde_json::to_vec(&schema.schema_content).map_err(|e| {
                SinexError::serialization(format!("Failed to serialize schema: {e}"))
            })?;

            kv.put(&key, payload.into()).await.map_err(|e| {
                SinexError::kv("Failed to store schema in KV")
                    .with_context("schema_source", &schema.source)
                    .with_context("schema_event_type", &schema.event_type)
                    .with_context("schema_id", schema.id)
                    .with_source(e)
            })?;
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
            observer: Arc::new(SelfObserver::disabled()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
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
