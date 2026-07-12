//! `initialize_with_transport` for `RuntimeRunner`.
//!
//! The module initialization sequence: lifecycle gate, transport wiring,
//! checkpoint manager bootstrap, schema/checkpoint listeners, leader election
//! preparation, DB-backed registration, and runtime state assembly.

use super::{
    Arc, CheckpointManager, DEFAULT_EVENT_CHANNEL_SIZE, ErasedInitContext, Event,
    EventBatcherConfig, EventEmitter, EventTransport, HashMap, JsonValue, ModuleKind, ModuleState,
    PgPool, ProcessingModel, RunnerLifecycle, RuntimeHandles, RuntimeResult, RuntimeRunner,
    ServiceInfo, SinexError, Utf8PathBuf, create_checkpoint_kv, info, maybe_start_schema_listener,
    mpsc, spawn_event_batcher, watch,
};
use sinex_primitives::domain::ServiceName;

impl RuntimeRunner {
    /// Initialize the module with a specific transport.
    pub async fn initialize_with_transport(
        &mut self,
        service_name: impl Into<ServiceName>,
        raw_config: HashMap<String, serde_json::Value>,
        #[cfg(feature = "db")] db_pool: Option<PgPool>,
        transport: EventTransport,
        work_dir: std::path::PathBuf,
        dry_run: bool,
    ) -> RuntimeResult<()> {
        // Re-entrancy guard: only allow initialization from Created state
        match self.lifecycle {
            RunnerLifecycle::Created => {}
            RunnerLifecycle::Initializing => {
                return Err(SinexError::lifecycle(
                    "RuntimeModule is already being initialized (concurrent initialize call detected)"
                        .to_string(),
                ));
            }
            RunnerLifecycle::Initialized
            | RunnerLifecycle::Running
            | RunnerLifecycle::ShutdownFailed
            | RunnerLifecycle::ShutDown => {
                return Err(SinexError::lifecycle(format!(
                    "Cannot initialize module: runner is in '{}' state (expected 'Created')",
                    self.lifecycle,
                )));
            }
        }
        self.lifecycle = RunnerLifecycle::Initializing;
        let service_name: ServiceName = service_name.into();

        // DATABASE_URL is optional - modules that need it will call
        // require_db_pool() which provides a clear error message.

        // Create bounded event channel
        let (event_sender_raw, event_receiver) =
            mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);

        // Create shutdown channels
        let (batcher_shutdown_sender, batcher_shutdown_receiver) = tokio::sync::oneshot::channel();
        self.event_batcher_shutdown = Some(batcher_shutdown_sender);

        // Get hostname
        let host = sinex_primitives::events::builder::get_hostname();
        let instance_id = Self::build_instance_id(host.as_str());
        let version = crate::runtime::version::runtime_version().map_or_else(
            |_| env!("CARGO_PKG_VERSION").to_string(),
            |value| value.to_string(),
        );
        let transport_for_context = transport.clone();
        let transport_clone_for_runner = transport.clone();

        let kv_store = create_checkpoint_kv(&transport).await?;

        #[cfg(feature = "messaging")]
        let (schema_cache, schema_validator, schema_listener_shutdown, schema_listener_handle) =
            maybe_start_schema_listener(&transport).await?;
        #[cfg(not(feature = "messaging"))]
        let (schema_cache, schema_validator, schema_listener_shutdown, schema_listener_handle) = (
            Option::<Arc<crate::runtime::stream::SchemaBroadcastCache>>::None,
            Option::<()>::None,
            Option::<watch::Sender<bool>>::None,
            Option::<tokio::task::JoinHandle<()>>::None,
        );
        self.schema_listener_shutdown = schema_listener_shutdown;
        self.schema_listener_handle = schema_listener_handle;

        // Start checkpoint cleanup background task if enabled
        // Start checkpoint cleanup background task if enabled
        let cleanup_enabled = {
            #[cfg(feature = "messaging")]
            {
                crate::runtime::checkpoint::CheckpointCleanupConfig::from_env().enabled
            }
            #[cfg(not(feature = "messaging"))]
            {
                false
            }
        };

        if cleanup_enabled {
            #[cfg(feature = "messaging")]
            {
                let cleanup_config =
                    crate::runtime::checkpoint::CheckpointCleanupConfig::from_env();
                let kv_for_cleanup = kv_store.clone();
                let (cleanup_shutdown_tx, cleanup_shutdown_rx) = watch::channel(false);
                let cleanup_handle = crate::runtime::checkpoint::spawn_checkpoint_cleanup_task(
                    kv_for_cleanup,
                    cleanup_config,
                    cleanup_shutdown_rx,
                );
                self.checkpoint_cleanup_shutdown = Some(cleanup_shutdown_tx);
                self.checkpoint_cleanup_handle = Some(cleanup_handle);
                tracing::info!("Checkpoint cleanup task started");
            }
        }

        // Initialize checkpoint manager with KV. Respect explicit consumer_group
        // from runtime config when provided, otherwise fall back to "default".
        let consumer_group = raw_config
            .get("consumer_group")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("default")
            .to_string();
        let source_id = Self::config_identity_value(&raw_config, "source_id");
        let runner_pack = Self::config_identity_value(&raw_config, "runner_pack");
        let checkpoint_identity = Self::config_identity_value(&raw_config, "checkpoint_identity")
            .or_else(|| source_id.clone())
            .unwrap_or_else(|| service_name.to_string());
        let control_identity = Self::config_identity_value(&raw_config, "control_identity")
            .or_else(|| source_id.clone());
        let consumer_name = Self::checkpoint_consumer_name(
            self.module.module_kind(),
            &raw_config,
            &checkpoint_identity,
            host.as_str(),
        );

        // Initialize checkpoint manager with KV
        let checkpoint_manager = Arc::new(CheckpointManager::with_missing_checkpoint_warning(
            kv_store,
            checkpoint_identity.clone(),
            consumer_group,
            consumer_name.clone(),
            matches!(self.module.module_kind(), ModuleKind::Automaton),
        ));

        // NATS is the only transport
        let transport_type = "NATS";

        // Determine if automaton to enable LeaderStandby
        if matches!(self.module.module_kind(), ModuleKind::Automaton) {
            self.processing_model = ProcessingModel::LeaderStandby;
        } else {
            self.processing_model = ProcessingModel::StatelessWorker;
        }

        #[cfg(feature = "db")]
        let module_run_id = if let Some(pool) = db_pool.as_ref() {
            self.register_runtime_identity(
                pool,
                &service_name,
                &instance_id,
                &host,
                &version,
                &raw_config,
            )
            .await?
        } else {
            None
        };
        #[cfg(not(feature = "db"))]
        let module_run_id = None;

        let mut event_emitter = {
            #[cfg(feature = "messaging")]
            if let Some(validator) = schema_validator {
                EventEmitter::with_validator(event_sender_raw.clone(), dry_run, validator)
            } else {
                EventEmitter::new(event_sender_raw, dry_run)
            }

            #[cfg(not(feature = "messaging"))]
            EventEmitter::new(event_sender_raw, dry_run)
        };

        if let Some(module_run_id) = module_run_id {
            event_emitter = event_emitter.with_default_module_run_id(module_run_id);
        }

        // No LeaseManager passed to handles
        // No LeaseManager passed to handles
        let handles = {
            #[cfg(feature = "db")]
            if let Some(pool) = db_pool {
                RuntimeHandles::new(
                    pool,
                    checkpoint_manager.clone(),
                    event_emitter.clone(),
                    transport_for_context,
                    schema_cache.clone(),
                )
            } else {
                RuntimeHandles::new_edge(
                    checkpoint_manager.clone(),
                    event_emitter.clone(),
                    transport_for_context,
                    schema_cache.clone(),
                )
            }

            #[cfg(not(feature = "db"))]
            RuntimeHandles::new_edge(
                checkpoint_manager.clone(),
                event_emitter.clone(),
                transport_for_context,
                schema_cache.clone(),
            )
        };

        let service_info = ServiceInfo::new_with_runtime_identity(
            service_name.clone(),
            self.module.module_name().to_string(),
            source_id.clone(),
            runner_pack.clone(),
            host.clone(),
            work_dir.clone(),
            dry_run,
            instance_id,
            version,
            module_run_id,
        )
        .with_runtime_identities(Some(checkpoint_identity.clone()), control_identity);
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir).unwrap_or_else(|_| {
            Utf8PathBuf::from_path_buf(sinex_primitives::environment::environment().temp_dir())
                .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex"))
        });

        // The typed `T::Config` deserialization happens inside the erased
        // `initialize` boundary (see `ErasedRuntimeModule`), so the runner is no
        // longer generic over the module. It receives only the non-typed parts.
        let init_context = ErasedInitContext {
            raw_config: raw_config.clone(),
            service: service_info.clone(),
            handles: handles.clone(),
            work_dir_utf8: work_dir_utf8.clone(),
        };

        if let Err(e) = self.module.initialize(init_context).await {
            #[cfg(feature = "db")]
            if let Some(pool) = handles.db_pool().cloned() {
                Self::update_registered_run_status(&pool, &service_info, ModuleState::Failed).await;
            }
            self.lifecycle = RunnerLifecycle::Created;
            return Err(e);
        }

        self.handles = Some(handles);
        self.service_info = Some(service_info);
        self.raw_config = Some(raw_config.clone());
        let batcher_work_dir = work_dir_utf8.as_std_path().to_path_buf();
        self.work_dir_utf8 = Some(work_dir_utf8);

        let batcher_config = {
            let mut cfg = EventBatcherConfig::default();
            if let Some(v) = raw_config
                .get("batch_size")
                .and_then(serde_json::Value::as_u64)
            {
                cfg.batch_size = v as usize;
            }
            if let Some(v) = raw_config
                .get("batch_timeout_ms")
                .and_then(serde_json::Value::as_u64)
            {
                cfg.batch_timeout_ms = v;
            }
            // Set envelope identity fields from runtime configuration.
            cfg.source_id = source_id.clone().unwrap_or_default();
            cfg.parser_id = self.module.module_name().to_string();
            cfg.parser_version = env!("CARGO_PKG_VERSION").to_string();
            cfg
        };
        self.event_batcher_handle = Some(spawn_event_batcher(
            transport_clone_for_runner,
            batcher_config,
            event_receiver,
            batcher_shutdown_receiver,
            batcher_work_dir,
        ));

        self.lifecycle = RunnerLifecycle::Initialized;

        info!(
            service = %service_name,
            module = %self.module.module_name(),
            source = source_id.as_deref().unwrap_or("none"),
            runner_pack = runner_pack.as_deref().unwrap_or("none"),
            checkpoint_identity = %checkpoint_identity,
            checkpoint_consumer = %consumer_name,
            module_kind = ?self.module.module_kind(),
            transport = transport_type,
            "RuntimeModule initialized"
        );

        Ok(())
    }

    pub(super) fn checkpoint_consumer_name(
        _module_kind: ModuleKind,
        raw_config: &HashMap<String, serde_json::Value>,
        checkpoint_identity: &str,
        _host: &str,
    ) -> String {
        if let Some(configured) = Self::config_identity_value(raw_config, "consumer_name") {
            return configured;
        }

        checkpoint_identity.to_string()
    }
}
