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
use sinex_db::DbPoolExt;
use sinex_db::advisory_lock::AdvisoryLock;
use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
use sinex_node_sdk::heartbeat::HeartbeatEmitter;
use sinex_node_sdk::systemd_notify;
use sinex_node_sdk::{SelfObserver, SelfObserverConfig};
use sinex_primitives::domain::{NodeName, NodeType};
use sinex_primitives::environment as sinex_environment;
use sinex_primitives::nats::create_or_open_kv_store;
use sinex_primitives::utils::ResourceGuard;
use sqlx::PgPool;

// Standard library and common crates
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::Mutex,
    sync::RwLock,
    sync::oneshot,
    task::{JoinHandle, JoinSet},
    time::{Duration, interval},
};
use tracing::{debug, error, info, warn};

/// Awaits the shutdown signal reactively (no polling).
///
/// Arms the `Notify` waiter before reading the flag so a shutdown that lands
/// between subscription and the flag read cannot be lost.
async fn shutdown_signal(
    shutdown_flag: &Arc<AtomicBool>,
    shutdown_notify: &Arc<tokio::sync::Notify>,
) {
    loop {
        let notified = shutdown_notify.notified();
        if shutdown_flag.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

fn trigger_shutdown(shutdown_flag: &Arc<AtomicBool>, shutdown_notify: &Arc<tokio::sync::Notify>) {
    if !shutdown_flag.swap(true, Ordering::AcqRel) {
        shutdown_notify.notify_waiters();
    }
}

fn log_node_manifest_write_failure(
    operation: &'static str,
    node_name: &NodeName,
    error: &SinexError,
) {
    warn!(
        operation,
        node = %node_name,
        version = env!("CARGO_PKG_VERSION"),
        error = %error,
        "Failed to persist ingestd node manifest state"
    );
}

async fn await_ready_signal(
    component: &'static str,
    ready_timeout: Duration,
    ready_rx: oneshot::Receiver<()>,
) -> IngestdResult<()> {
    match tokio::time::timeout(ready_timeout, ready_rx).await {
        Ok(Ok(())) => {
            info!(component, "Startup component reached ready state");
            Ok(())
        }
        Ok(Err(_)) => Err(SinexError::service(format!(
            "{component} setup failed before signaling ready"
        ))
        .with_operation("service.await_ready_signal")
        .with_context("component", component)
        .with_context("timeout_secs", ready_timeout.as_secs().to_string())),
        Err(_) => Err(SinexError::service(format!(
            "{component} did not signal ready within {ready_timeout:?}"
        ))
        .with_operation("service.await_ready_signal")
        .with_context("component", component)
        .with_context("timeout_secs", ready_timeout.as_secs().to_string())),
    }
}

fn attach_startup_cleanup_error(
    startup_error: SinexError,
    cleanup_error: &SinexError,
) -> SinexError {
    startup_error.with_context("shutdown_cleanup_error", cleanup_error.to_string())
}

fn attach_background_shutdown_error(error: SinexError, cleanup_error: &SinexError) -> SinexError {
    error.with_context("background_shutdown_error", cleanup_error.to_string())
}

/// Shared helper for task shutdown errors across critical, material, and background tasks.
pub(crate) fn task_shutdown_error(
    category: &str,
    name: &str,
    error: impl std::fmt::Display,
) -> SinexError {
    SinexError::service(format!(
        "{category} task join failed during shutdown: {name}"
    ))
    .with_context("task", name.to_string())
    .with_context("category", category.to_string())
    .with_source(error.to_string())
}

fn cleanup_task_timeout(count: usize, timeout: Duration) -> SinexError {
    SinexError::timeout(format!(
        "timed out waiting for {count} critical tasks during shutdown"
    ))
    .with_duration(timeout)
    .with_count(count)
}

fn background_task_timeout(count: usize, timeout: Duration) -> SinexError {
    SinexError::timeout(format!(
        "timed out waiting for {count} background tasks during shutdown"
    ))
    .with_duration(timeout)
    .with_count(count)
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
    /// Heartbeat counter handle — set during `start()`, passed to `JetStreamConsumer`
    heartbeat_counter_handle: Option<sinex_node_sdk::heartbeat::HeartbeatCounterHandle>,
}

type CriticalTaskOutcome = (
    &'static str,
    Result<IngestdResult<()>, tokio::task::JoinError>,
);

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
            heartbeat_counter_handle: self.heartbeat_counter_handle.clone(),
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
            heartbeat_counter_handle: None,
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
        // message is processed (separate NATS streams, no cross-stream ordering) while
        // still allowing externally-registered materials to be discovered via DB fallback.
        let ready_set = if self.db_pool.is_some() {
            let set = MaterialReadySet::new();
            if let Some(pool) = &self.db_pool
                && let Err(e) = set.seed_from_db(pool).await
            {
                warn!("Failed to seed MaterialReadySet from database: {}", e);
                // Non-fatal: events will be deferred until materials are registered
            }
            Some(set)
        } else {
            None
        };

        if let Some(set) = ready_set.clone() {
            let handle = self.start_material_ready_set_maintenance_task(set).await;
            self.track_task(handle).await;
        }

        // Start JetStream and MaterialAssembler tasks (critical - failure stops service)
        let (mut js_handle, mut js_ready_rx) = match (&self.nats_client, &self.db_pool) {
            (Some(nats), Some(pool)) => {
                let (h, rx) = self
                    .start_jetstream_consumer_task(nats.clone(), pool.clone(), ready_set.clone())
                    .await;
                (Some(h), Some(rx))
            }
            _ => (None, None),
        };

        let (mut ma_handle, mut ma_ready_rx) = match (&self.nats_client, &self.db_pool) {
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

        // Register ingestd in node_manifests and start periodic heartbeat
        if let Some(ref pool) = self.db_pool {
            let node_name = NodeName::new("sinex-ingestd");
            // Register or update node manifest (idempotent via ON CONFLICT)
            match pool
                .state()
                .register_node(
                    &node_name,
                    NodeType::Service,
                    env!("CARGO_PKG_VERSION"),
                    Some("Ingestion daemon - central hub for event ingestion"),
                )
                .await
            {
                Ok(_) => info!("Registered ingestd in node_manifests"),
                Err(e) => {
                    // May fail if already registered (unique constraint) - update heartbeat instead
                    debug!("Node registration failed (may already exist): {e}");
                    if let Err(update_error) = pool
                        .state()
                        .update_node_heartbeat_for_version(&node_name, env!("CARGO_PKG_VERSION"))
                        .await
                    {
                        warn!(
                            node = %node_name,
                            version = env!("CARGO_PKG_VERSION"),
                            register_error = %e,
                            heartbeat_error = %update_error,
                            "Failed to recover ingestd node manifest registration by updating heartbeat"
                        );
                    }
                }
            }

            // Emit health-aware heartbeats on a fixed cadence.
            // Tracks error window for Healthy/Degraded/Failed status determination.
            // Counter handle is passed to JetStreamConsumer so batch counts feed health status.
            let emitter = HeartbeatEmitter::new(
                "sinex-ingestd".to_string(),
                sinex_primitives::Seconds::from_secs(60),
            )
            .with_node_name(node_name.clone())
            .with_db_pool(pool.clone());
            self.heartbeat_counter_handle = Some(emitter.get_counter_handle());

            let shutdown_flag = self.shutdown_flag.clone();
            let shutdown_notify = self.shutdown_notify.clone();
            let heartbeat_pool = pool.clone();
            let handle = tokio::spawn(async move {
                tokio::select! {
                    () = emitter.start_periodic_heartbeat(None) => {}
                    () = shutdown_signal(&shutdown_flag, &shutdown_notify) => {
                        let node_name = NodeName::new("sinex-ingestd");
                        if let Err(error) = heartbeat_pool
                            .state()
                            .mark_node_inactive_for_version(&node_name, env!("CARGO_PKG_VERSION"))
                            .await
                        {
                            log_node_manifest_write_failure("mark_node_inactive", &node_name, &error);
                        }
                    }
                }
            });
            self.track_task(handle).await;
        }

        // Wait for both critical tasks to signal they're past their setup phase
        // (streams bound, WAL restored) before telling systemd we're ready.
        // Use a 30s timeout so a hung startup doesn't prevent systemd from detecting failure.
        let ready_timeout = Duration::from_secs(30);
        if let Some(rx) = js_ready_rx.take()
            && let Err(error) = await_ready_signal("JetStream consumer", ready_timeout, rx).await
        {
            return self
                .finish_startup_failure(error, js_handle.take(), ma_handle.take())
                .await;
        }
        if let Some(rx) = ma_ready_rx.take()
            && let Err(error) = await_ready_signal("MaterialAssembler", ready_timeout, rx).await
        {
            return self
                .finish_startup_failure(error, js_handle.take(), ma_handle.take())
                .await;
        }

        systemd_notify::notify_ready("sinex-ingestd");
        let watchdog_handle = systemd_notify::spawn_watchdog("sinex-ingestd");

        // Monitor critical tasks - exit on first failure or shutdown signal
        let monitor_result = self
            .monitor_runtime(js_handle.take(), ma_handle.take())
            .await;
        systemd_notify::stop_watchdog(watchdog_handle, "sinex-ingestd").await;
        systemd_notify::notify_stopping("sinex-ingestd");

        // Ensure background tasks have a chance to shut down before closing resources.
        let shutdown_result = self.wait_for_tasks(Duration::from_secs(5)).await;
        match (monitor_result, shutdown_result) {
            (Ok(()), Ok(())) => {
                info!("Ingestion service stopped");
                Ok(())
            }
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(cleanup_error)) => {
                error!(
                    runtime_error = %error,
                    cleanup_error = %cleanup_error,
                    "Runtime shutdown surfaced an additional background task failure"
                );
                Err(attach_background_shutdown_error(error, &cleanup_error))
            }
        }
    }

    async fn finish_startup_failure(
        &self,
        startup_error: SinexError,
        js_handle: Option<JoinHandle<IngestdResult<()>>>,
        ma_handle: Option<JoinHandle<IngestdResult<()>>>,
    ) -> IngestdResult<()> {
        error!(error = %startup_error, "Critical ingestd component failed during startup");
        trigger_shutdown(&self.shutdown_flag, &self.shutdown_notify);

        let mut startup_error = match self.monitor_runtime(js_handle, ma_handle).await {
            Ok(()) => startup_error,
            Err(cleanup_error) => {
                error!(
                    startup_error = %startup_error,
                    cleanup_error = %cleanup_error,
                    "Startup failure cleanup surfaced an additional critical task failure"
                );
                attach_startup_cleanup_error(startup_error, &cleanup_error)
            }
        };

        if let Err(cleanup_error) = self.wait_for_tasks(Duration::from_secs(5)).await {
            error!(
                startup_error = %startup_error,
                cleanup_error = %cleanup_error,
                "Startup failure cleanup surfaced an additional background task failure"
            );
            startup_error = attach_background_shutdown_error(startup_error, &cleanup_error);
        }
        Err(startup_error)
    }

    /// Monitor critical tasks - exit on first failure or shutdown signal
    async fn monitor_runtime(
        &self,
        js_handle: Option<JoinHandle<IngestdResult<()>>>,
        ma_handle: Option<JoinHandle<IngestdResult<()>>>,
    ) -> IngestdResult<()> {
        let shutdown_flag = self.shutdown_flag.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let mut critical_tasks = JoinSet::new();
        Self::track_critical_task(&mut critical_tasks, "JetStream consumer", js_handle);
        Self::track_critical_task(&mut critical_tasks, "MaterialAssembler", ma_handle);

        let result = tokio::select! {
            maybe_task = critical_tasks.join_next(), if !critical_tasks.is_empty() => {
                match maybe_task {
                    Some(Ok((name, result))) => {
                        Self::handle_task_result(name, result, &shutdown_flag, &shutdown_notify)
                    }
                    Some(Err(err)) => {
                        trigger_shutdown(&shutdown_flag, &shutdown_notify);
                        Err(SinexError::service(format!("Critical task monitor panicked: {err}")))
                    }
                    None => Ok(()),
                }
            }

            // Normal shutdown signal
            () = shutdown_signal(&shutdown_flag, &shutdown_notify) => {
                info!("Received shutdown signal");
                Ok(())
            }
        };

        let cleanup_error =
            Self::wait_for_critical_tasks(&mut critical_tasks, Duration::from_secs(5)).await;
        match (result, cleanup_error) {
            (Ok(()), Some(error)) => Err(error),
            (Err(error), _) => Err(error),
            (Ok(()), None) => Ok(()),
        }
    }

    fn track_critical_task(
        tasks: &mut JoinSet<CriticalTaskOutcome>,
        name: &'static str,
        handle: Option<JoinHandle<IngestdResult<()>>>,
    ) {
        if let Some(handle) = handle {
            tasks.spawn(async move { (name, handle.await) });
        }
    }

    async fn wait_for_critical_tasks(
        tasks: &mut JoinSet<CriticalTaskOutcome>,
        timeout: Duration,
    ) -> Option<SinexError> {
        if tasks.is_empty() {
            return None;
        }

        info!("Waiting for {} critical tasks to finish...", tasks.len());

        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);
        let mut cleanup_error = None;

        loop {
            tokio::select! {
                maybe = tasks.join_next(), if !tasks.is_empty() => {
                    match maybe {
                        Some(Ok((name, Ok(Ok(()))))) => {
                            debug!(task = name, "Critical task stopped cleanly");
                        }
                        Some(Ok((name, Ok(Err(error))))) => {
                            warn!(task = name, error = %error, "Critical task exited with error during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(task_shutdown_error("critical", name, &error));
                            }
                        }
                        Some(Ok((name, Err(error)))) => {
                            warn!(task = name, error = ?error, "Critical task join failed during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(task_shutdown_error("critical", name, &error));
                            }
                        }
                        Some(Err(error)) => {
                            warn!(error = ?error, "Critical task monitor join failed during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(task_shutdown_error("critical", "monitor", &error));
                            }
                        }
                        None => break,
                    }
                    if tasks.is_empty() {
                        break;
                    }
                }
                () = &mut deadline => {
                    let remaining = tasks.len();
                    warn!(
                        "Timed out waiting for {} critical tasks after {:?}, aborting remaining work",
                        remaining,
                        timeout
                    );
                    tasks.abort_all();
                    while let Some(result) = tasks.join_next().await {
                        if let Err(error) = result {
                            debug!(error = ?error, "Critical task aborted during shutdown cleanup");
                        }
                    }
                    if cleanup_error.is_none() {
                        cleanup_error = Some(cleanup_task_timeout(remaining, timeout));
                    }
                    break;
                }
            }
        }

        info!("Critical task cleanup complete");
        cleanup_error
    }

    fn handle_task_result(
        name: &str,
        result: Result<IngestdResult<()>, tokio::task::JoinError>,
        shutdown_flag: &Arc<AtomicBool>,
        shutdown_notify: &Arc<tokio::sync::Notify>,
    ) -> IngestdResult<()> {
        match result {
            Ok(res) => Self::handle_join_success(name, res, shutdown_flag, shutdown_notify),
            Err(e) => Self::handle_join_error(name, e, shutdown_flag, shutdown_notify),
        }
    }

    fn handle_join_success(
        name: &str,
        result: IngestdResult<()>,
        shutdown_flag: &Arc<AtomicBool>,
        shutdown_notify: &Arc<tokio::sync::Notify>,
    ) -> IngestdResult<()> {
        match result {
            Ok(()) if shutdown_flag.load(Ordering::Acquire) => {
                info!("{name} completed during shutdown");
                Ok(())
            }
            Ok(()) => {
                error!("{name} exited unexpectedly without error");
                trigger_shutdown(shutdown_flag, shutdown_notify);
                Err(SinexError::service(format!("{name} exited unexpectedly")))
            }
            Err(e) => {
                error!(error = %e, "{name} failed");
                trigger_shutdown(shutdown_flag, shutdown_notify);
                Err(e)
            }
        }
    }

    fn handle_join_error(
        name: &str,
        err: tokio::task::JoinError,
        shutdown_flag: &Arc<AtomicBool>,
        shutdown_notify: &Arc<tokio::sync::Notify>,
    ) -> IngestdResult<()> {
        error!(error = ?err, "{name} panicked");
        trigger_shutdown(shutdown_flag, shutdown_notify);
        Err(SinexError::service(format!("{name} panicked: {err}")))
    }

    fn handle_material_assembler_result(
        result: IngestdResult<()>,
        shutdown_flag: &Arc<AtomicBool>,
    ) -> IngestdResult<()> {
        match result {
            Ok(()) if shutdown_flag.load(Ordering::Acquire) => {
                info!("MaterialAssembler shutting down normally");
                Ok(())
            }
            Ok(()) => {
                info!("MaterialAssembler completed normally");
                Ok(())
            }
            Err(error) => {
                error!(error = %error, "MaterialAssembler failed");
                Err(error)
            }
        }
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

        let heartbeat_handle = self.heartbeat_counter_handle.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut consumer = crate::JetStreamConsumer::new(
                nats_client,
                pool.clone(),
                validator.clone(),
                topology,
            )
            .with_batch_fetch_config(fetch_max, fetch_timeout)
            .with_max_ack_pending(max_ack_pending)
            .with_stats_log_interval(stats_log_interval)
            .with_observer(observer);

            if let Some(hb) = heartbeat_handle {
                consumer = consumer.with_heartbeat_handle(hb);
            }
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
        let shutdown_notify = self.shutdown_notify.clone();
        let observer = self.observer.clone();
        let annex_repo_path = self.config.annex_repo_path.clone();
        let assembler_state_dir = self.config.assembler_state_dir.clone();
        let namespace = self.config.nats_namespace.clone();
        let slices_max_ack_pending = self.config.material_slices_max_ack_pending;
        let max_buffered_slices = self.config.max_buffered_slices;
        let max_material_size_bytes = self.config.max_material_size_bytes.as_u64();
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
                ready_set,
                max_buffered_slices,
                max_material_size_bytes,
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
                .run_with_shutdown_signal_and_ready(
                    shutdown_flag.clone(),
                    shutdown_notify,
                    Some(ready_tx),
                )
                .await;
            Self::handle_material_assembler_result(result, &shutdown_flag)
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
        let heartbeat_handle = self.heartbeat_counter_handle.clone();

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
                            Err(e) => {
                                warn!("Failed to reload schemas: {}", e);
                                if let Some(ref hb) = heartbeat_handle {
                                    hb.record_error(&format!("schema reload failed: {e}"));
                                }
                            }
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

    async fn start_material_ready_set_maintenance_task(
        &self,
        ready_set: MaterialReadySet,
    ) -> JoinHandle<()> {
        let shutdown_flag = self.shutdown_flag.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let interval_duration = ready_set.maintenance_interval();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let removed = ready_set.purge_stale();
                        if removed > 0 {
                            debug!(
                                removed,
                                retained = ready_set.len(),
                                "Evicted stale materials from MaterialReadySet background maintenance"
                            );
                        }
                    }
                    () = shutdown_signal(&shutdown_flag, &shutdown_notify) => {
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

    fn collapse_background_shutdown_errors(mut errors: Vec<SinexError>) -> IngestdResult<()> {
        if errors.is_empty() {
            return Ok(());
        }

        let mut error = errors.remove(0);
        for (index, extra) in errors.into_iter().enumerate() {
            error = error.with_context(
                format!("additional_shutdown_error_{}", index + 1),
                extra.to_string(),
            );
        }
        Err(error)
    }

    fn log_aborted_task_shutdown_result(
        index: usize,
        result: Result<(), tokio::task::JoinError>,
    ) -> Option<SinexError> {
        match result {
            Ok(()) => {
                debug!(
                    task_index = index,
                    "Background task finished before forced shutdown"
                );
                None
            }
            Err(error) if error.is_cancelled() => {
                debug!(
                    task_index = index,
                    "Background task cancelled during forced shutdown"
                );
                None
            }
            Err(error) => {
                error!(
                    task_index = index,
                    error = %error,
                    "Background task exited unexpectedly during forced shutdown"
                );
                Some(task_shutdown_error(
                    "background",
                    &index.to_string(),
                    &error,
                ))
            }
        }
    }

    async fn wait_for_tasks(&self, timeout: Duration) -> IngestdResult<()> {
        let mut handles = {
            let mut guard = self.task_handles.lock().await;
            std::mem::take(&mut *guard)
        };

        if handles.is_empty() {
            return Ok(());
        }

        info!(
            "Waiting for {} background tasks to finish...",
            handles.len()
        );

        let wait_task = async {
            let mut shutdown_errors = Vec::new();
            for (i, handle) in handles.iter_mut().enumerate() {
                if let Err(error) = handle.await {
                    if error.is_panic() {
                        error!(
                            task_index = i,
                            error = %error,
                            "Background task panicked during shutdown"
                        );
                    } else {
                        debug!(task_index = i, error = %error, "Background task exited during shutdown");
                    }
                    shutdown_errors.push(task_shutdown_error("background", &i.to_string(), &error));
                }
            }
            shutdown_errors
        };

        if let Ok(shutdown_errors) = tokio::time::timeout(timeout, wait_task).await {
            info!("All background tasks finished");
            Self::collapse_background_shutdown_errors(shutdown_errors)
        } else {
            warn!(
                "Timed out waiting for background tasks after {:?}, aborting {} remaining",
                timeout,
                handles.len()
            );
            for handle in &handles {
                handle.abort();
            }
            let mut shutdown_errors = vec![background_task_timeout(handles.len(), timeout)];
            // Await aborted handles so their destructors run before we return.
            for (index, handle) in handles.into_iter().enumerate() {
                if let Some(error) = Self::log_aborted_task_shutdown_result(index, handle.await) {
                    shutdown_errors.push(error);
                }
            }
            Self::collapse_background_shutdown_errors(shutdown_errors)
        }
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> IngestdResult<()> {
        info!("Initiating graceful shutdown");

        trigger_shutdown(&self.shutdown_flag, &self.shutdown_notify);

        // Let background tasks observe the flag and finish before tearing down shared state.
        self.wait_for_tasks(Duration::from_secs(5)).await?;

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
    fn parse_schema_broadcast_entries(
        entries: &[SchemaBroadcastEntry],
    ) -> IngestdResult<Vec<(uuid::Uuid, &SchemaBroadcastEntry)>> {
        entries
            .iter()
            .map(|entry| {
                let schema_id = entry.schema_id.parse::<uuid::Uuid>().map_err(|error| {
                    SinexError::invalid_state("schema broadcast contains invalid schema_id")
                        .with_context("schema_name", &entry.name)
                        .with_context("schema_version", &entry.version)
                        .with_context("schema_id", &entry.schema_id)
                        .with_std_error(&error)
                })?;
                Ok((schema_id, entry))
            })
            .collect()
    }

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
        Self::store_schemas_in_kv(&entries, pool, &js).await?;

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

        let kv = create_or_open_kv_store(js, kv_config).await?;

        // Parse schema IDs and fetch in bulk via centralized repository
        let parsed_entries = Self::parse_schema_broadcast_entries(entries)?;
        let schema_ids: Vec<uuid::Uuid> = parsed_entries
            .iter()
            .map(|(schema_id, _entry)| *schema_id)
            .collect();

        let schemas = pool
            .schema_cache()
            .get_schemas_by_ids(&schema_ids)
            .await
            .map_err(|e| SinexError::database("Failed to fetch schemas").with_source(e))?;

        let resolved_ids: HashSet<uuid::Uuid> = schemas.iter().map(|schema| schema.id).collect();
        if let Some((missing_schema_id, entry)) = parsed_entries
            .iter()
            .find(|(schema_id, _entry)| !resolved_ids.contains(schema_id))
        {
            return Err(SinexError::invalid_state(
                "schema broadcast references schema missing from repository",
            )
            .with_context("schema_name", &entry.name)
            .with_context("schema_version", &entry.version)
            .with_context("schema_id", missing_schema_id.to_string()));
        }

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
    use sinex_primitives::Uuid;
    use xtask::sandbox::prelude::*;
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
            heartbeat_counter_handle: None,
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

        let error = service
            .wait_for_tasks(Duration::from_millis(10))
            .await
            .expect_err("hung background tasks must fail shutdown honestly");

        assert!(cancelled.load(Ordering::SeqCst));
        assert!(
            error
                .to_string()
                .contains("timed out waiting for 1 background tasks")
        );
        Ok(())
    }

    #[sinex_test]
    async fn log_aborted_task_shutdown_result_accepts_clean_exit() -> xtask::sandbox::TestResult<()>
    {
        let handle = tokio::spawn(async {});
        let error = IngestService::log_aborted_task_shutdown_result(0, handle.await);
        assert!(error.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn log_aborted_task_shutdown_result_accepts_cancelled_task()
    -> xtask::sandbox::TestResult<()> {
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        handle.abort();
        let error = IngestService::log_aborted_task_shutdown_result(1, handle.await);
        assert!(error.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn log_aborted_task_shutdown_result_rejects_panicked_task()
    -> xtask::sandbox::TestResult<()> {
        let handle = tokio::spawn(async {
            panic!("ingestd background task panic");
        });
        let error = IngestService::log_aborted_task_shutdown_result(2, handle.await)
            .expect("panicked background task must stay visible");
        assert!(
            error
                .to_string()
                .contains("background task join failed during shutdown")
        );
        Ok(())
    }

    #[sinex_test]
    async fn wait_for_tasks_rejects_panicked_background_task() -> xtask::sandbox::TestResult<()> {
        let service = test_service();
        service.task_handles.lock().await.push(tokio::spawn(async {
            panic!("background task exploded");
        }));

        let error = service
            .wait_for_tasks(Duration::from_secs(1))
            .await
            .expect_err("panicked background task must fail shutdown");

        assert!(
            error
                .to_string()
                .contains("background task join failed during shutdown")
        );
        Ok(())
    }

    #[sinex_test]
    async fn shutdown_surfaces_background_task_failures() -> xtask::sandbox::TestResult<()> {
        let mut service = test_service();
        service.task_handles.lock().await.push(tokio::spawn(async {
            panic!("shutdown background task panic");
        }));

        let error = service
            .shutdown()
            .await
            .expect_err("shutdown must fail when background tasks panic");

        assert!(
            error
                .to_string()
                .contains("background task join failed during shutdown")
        );
        Ok(())
    }

    #[sinex_test]
    async fn task_failure_notifies_shutdown_waiters() -> xtask::sandbox::TestResult<()> {
        let service = test_service();

        let error = IngestService::handle_join_success(
            "JetStream consumer",
            Err(SinexError::service("boom")),
            &service.shutdown_flag,
            &service.shutdown_notify,
        )
        .expect_err("task failure should bubble up");
        assert!(error.to_string().contains("boom"));

        tokio::time::timeout(
            Duration::from_millis(10),
            shutdown_signal(&service.shutdown_flag, &service.shutdown_notify),
        )
        .await
        .expect("shutdown waiters should wake immediately");

        Ok(())
    }

    #[sinex_test]
    async fn unexpected_task_exit_notifies_shutdown_waiters() -> xtask::sandbox::TestResult<()> {
        let service = test_service();

        let error = IngestService::handle_join_success(
            "MaterialAssembler",
            Ok(()),
            &service.shutdown_flag,
            &service.shutdown_notify,
        )
        .expect_err("unexpected exit should bubble up");
        assert!(error.to_string().contains("exited unexpectedly"));

        tokio::time::timeout(
            Duration::from_millis(10),
            shutdown_signal(&service.shutdown_flag, &service.shutdown_notify),
        )
        .await
        .expect("shutdown waiters should wake immediately");

        Ok(())
    }

    #[sinex_test]
    async fn prior_shutdown_signal_wakes_late_waiters_immediately() -> xtask::sandbox::TestResult<()>
    {
        let service = test_service();
        trigger_shutdown(&service.shutdown_flag, &service.shutdown_notify);
        trigger_shutdown(&service.shutdown_flag, &service.shutdown_notify);

        tokio::time::timeout(
            Duration::from_millis(10),
            shutdown_signal(&service.shutdown_flag, &service.shutdown_notify),
        )
        .await
        .expect("late shutdown waiters should observe an already-triggered shutdown");

        Ok(())
    }

    #[sinex_test]
    async fn await_ready_signal_accepts_ready_component() -> xtask::sandbox::TestResult<()> {
        let (tx, rx) = oneshot::channel();
        tx.send(())
            .expect("sending ready signal should succeed in the test");

        await_ready_signal("JetStream consumer", Duration::from_millis(10), rx).await?;
        Ok(())
    }

    #[sinex_test]
    async fn await_ready_signal_rejects_dropped_sender() -> xtask::sandbox::TestResult<()> {
        let (tx, rx) = oneshot::channel::<()>();
        drop(tx);

        let error = await_ready_signal("MaterialAssembler", Duration::from_millis(10), rx)
            .await
            .expect_err("dropped ready sender must fail honestly");

        let message = error.to_string();
        assert!(message.contains("setup failed"));
        assert!(message.contains("MaterialAssembler"));
        Ok(())
    }

    #[sinex_test]
    async fn await_ready_signal_rejects_timeout() -> xtask::sandbox::TestResult<()> {
        let (_tx, rx) = oneshot::channel::<()>();

        let error = await_ready_signal("JetStream consumer", Duration::from_millis(10), rx)
            .await
            .expect_err("timed out ready signal must fail honestly");

        let message = error.to_string();
        assert!(message.contains("did not signal ready"));
        assert!(message.contains("JetStream consumer"));
        Ok(())
    }

    #[sinex_test]
    async fn handle_material_assembler_result_preserves_errors_during_shutdown()
    -> xtask::sandbox::TestResult<()> {
        let shutdown_flag = Arc::new(AtomicBool::new(true));

        let error = IngestService::handle_material_assembler_result(
            Err(SinexError::service("material bootstrap failed")),
            &shutdown_flag,
        )
        .expect_err("material assembler errors must not be masked by shutdown");

        assert!(error.to_string().contains("material bootstrap failed"));
        Ok(())
    }

    #[sinex_test]
    async fn handle_material_assembler_result_allows_clean_shutdown()
    -> xtask::sandbox::TestResult<()> {
        let shutdown_flag = Arc::new(AtomicBool::new(true));

        IngestService::handle_material_assembler_result(Ok(()), &shutdown_flag)?;
        Ok(())
    }

    #[sinex_test]
    async fn monitor_runtime_waits_for_remaining_critical_tasks_after_failure()
    -> xtask::sandbox::TestResult<()> {
        let service = test_service();
        let sibling_finished = Arc::new(AtomicBool::new(false));

        let failing = tokio::spawn(async { Err(SinexError::service("boom")) });
        let sibling_flag = Arc::clone(&sibling_finished);
        let shutdown_flag = Arc::clone(&service.shutdown_flag);
        let shutdown_notify = Arc::clone(&service.shutdown_notify);
        let sibling = tokio::spawn(async move {
            shutdown_signal(&shutdown_flag, &shutdown_notify).await;
            sibling_flag.store(true, Ordering::SeqCst);
            Ok(())
        });

        let error = service
            .monitor_runtime(Some(failing), Some(sibling))
            .await
            .expect_err("unexpected failure should bubble up");

        assert!(error.to_string().contains("boom"));
        assert!(
            sibling_finished.load(Ordering::SeqCst),
            "monitor_runtime should await the sibling critical task after shutdown"
        );
        Ok(())
    }

    #[sinex_test]
    async fn finish_startup_failure_preserves_cleanup_error_context()
    -> xtask::sandbox::TestResult<()> {
        let service = test_service();
        let sibling_finished = Arc::new(AtomicBool::new(false));

        let failing = tokio::spawn(async { Err(SinexError::service("cleanup boom")) });
        let sibling_flag = Arc::clone(&sibling_finished);
        let shutdown_flag = Arc::clone(&service.shutdown_flag);
        let shutdown_notify = Arc::clone(&service.shutdown_notify);
        let sibling = tokio::spawn(async move {
            shutdown_signal(&shutdown_flag, &shutdown_notify).await;
            sibling_flag.store(true, Ordering::SeqCst);
            Ok(())
        });

        let error = service
            .finish_startup_failure(
                SinexError::service("startup failed"),
                Some(failing),
                Some(sibling),
            )
            .await
            .expect_err("startup failure should remain an error");

        assert!(error.to_string().contains("startup failed"));
        let cleanup_context = error
            .context_map()
            .get("shutdown_cleanup_error")
            .expect("cleanup failure should be preserved in startup error context");
        assert!(cleanup_context.contains("JetStream consumer"));
        assert!(cleanup_context.contains("cleanup boom"));
        assert!(
            sibling_finished.load(Ordering::SeqCst),
            "startup cleanup should still await sibling critical tasks"
        );
        Ok(())
    }

    #[sinex_test]
    async fn finish_startup_failure_preserves_background_task_error_context()
    -> xtask::sandbox::TestResult<()> {
        let service = test_service();
        service.task_handles.lock().await.push(tokio::spawn(async {
            panic!("startup cleanup background panic");
        }));

        let error = service
            .finish_startup_failure(SinexError::service("startup failed"), None, None)
            .await
            .expect_err("startup failure should remain an error");

        assert!(error.to_string().contains("startup failed"));
        let cleanup_context = error
            .context_map()
            .get("background_shutdown_error")
            .expect("background cleanup failure should stay attached");
        assert!(cleanup_context.contains("background task join failed during shutdown"));
        Ok(())
    }

    #[sinex_test]
    async fn material_ready_set_maintenance_purges_idle_entries() -> xtask::sandbox::TestResult<()>
    {
        let mut service = test_service();
        let ready_set =
            MaterialReadySet::with_policy_for_tests(Duration::from_millis(10), u64::MAX);
        let material_id = Uuid::now_v7();
        ready_set.mark_ready(material_id);

        tokio::time::sleep(Duration::from_millis(15)).await;

        let handle = service
            .start_material_ready_set_maintenance_task(ready_set.clone())
            .await;
        service.task_handles.lock().await.push(handle);

        WaitHelpers::wait_for_condition(
            || {
                let ready_set = ready_set.clone();
                async move { Ok::<bool, SinexError>(ready_set.is_empty()) }
            },
            Timeouts::SHORT,
        )
        .await?;

        service.shutdown().await?;
        Ok(())
    }

    #[sinex_test]
    async fn material_ready_set_maintenance_stops_promptly_on_shutdown()
    -> xtask::sandbox::TestResult<()> {
        let mut service = test_service();
        let ready_set = MaterialReadySet::with_policy_for_tests(Duration::from_mins(1), u64::MAX);
        let handle = service
            .start_material_ready_set_maintenance_task(ready_set)
            .await;
        service.task_handles.lock().await.push(handle);

        tokio::time::timeout(Duration::from_millis(200), service.shutdown())
            .await
            .expect("maintenance task should observe shutdown without waiting for its interval")?;
        Ok(())
    }

    #[sinex_test]
    async fn store_schemas_in_kv_rejects_invalid_schema_ids(
        ctx: TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let js = ctx.jetstream().await?;
        let entries = vec![SchemaBroadcastEntry {
            name: "test.schema".to_string(),
            version: "1.0.0".to_string(),
            schema_id: "not-a-uuid".to_string(),
        }];

        let error = IngestService::store_schemas_in_kv(&entries, ctx.pool(), &js)
            .await
            .expect_err("invalid schema ids must fail honestly");
        let message = error.to_string();
        assert!(message.contains("invalid schema_id"));
        assert!(message.contains("test.schema"));
        Ok(())
    }

    #[sinex_test]
    async fn store_schemas_in_kv_rejects_missing_repository_rows(
        ctx: TestContext,
    ) -> xtask::sandbox::TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let js = ctx.jetstream().await?;
        let missing_schema_id = uuid::Uuid::now_v7().to_string();
        let entries = vec![SchemaBroadcastEntry {
            name: "test.schema".to_string(),
            version: "1.0.0".to_string(),
            schema_id: missing_schema_id.clone(),
        }];

        let error = IngestService::store_schemas_in_kv(&entries, ctx.pool(), &js)
            .await
            .expect_err("missing schema rows must fail honestly");
        let message = error.to_string();
        assert!(message.contains("missing from repository"));
        assert!(message.contains(&missing_schema_id));
        Ok(())
    }

    #[sinex_test]
    async fn log_node_manifest_write_failure_accepts_processing_errors()
    -> xtask::sandbox::TestResult<()> {
        let node_name = NodeName::new("sinex-ingestd");
        let error = SinexError::processing("node manifest update exploded");
        log_node_manifest_write_failure("mark_node_inactive", &node_name, &error);
        Ok(())
    }
}
