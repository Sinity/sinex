//! `NodeRunner<T>` and its associated lifecycle/runtime helpers.
//!
//! This is the long-lived runtime kernel of stream nodes. Keeping it isolated
//! from wire types, listener plumbing, and control-message helpers makes the
//! file navigable; further role splits inside this module are tracked as
//! follow-up work.

use super::{
    Checkpoint, ContinuousStart, EventEmitter, EventSender, EventStream, MaterialReplayContext,
    Node, NodeCapabilities, NodeHandles, NodeInitContext, NodeRuntimeState, NodeScanAck,
    NodeScanCommand, NodeScanProgress, NodeType, ProcessingStats, ResolvedReplayMaterial,
    RunnerLifecycle, RuntimeDrainController, ScanArgs, ScanEstimate, ScanReport,
    SchemaBroadcastCache, SchemaBroadcastEntry, ServiceInfo, TimeHorizon,
};
use super::control_protocol::{
    ensure_control_payload_fits, encode_control_message, MAX_CONTROL_MESSAGE_BYTES,
};
#[cfg(feature = "messaging")]
use super::control_protocol::{ControlCommandKind, NodeDrainComplete, control_command_kind};
use super::listener::{
    CONFIRMED_EVENT_CHANNEL_CAPACITY, LISTENER_RETRY_DELAY, LISTENER_STARTUP_GRACE_PERIOD,
    RunnerConfirmedEventHandler, TASK_SHUTDOWN_GRACE_PERIOD, create_checkpoint_kv,
    maybe_start_schema_listener, run_resubscribing_listener,
};
use crate::{
    NodeResult, SinexError,
    checkpoint::CheckpointManager,
    confirmation_handler::{ConfirmedEventHandler, ProcessingModel, ProvisionalEvent},
    error_helpers::env_parse_with_default,
    event_node::{EventBatcherConfig, EventTransport, spawn_event_batcher},
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    systemd_notify,
};
use async_nats::jetstream::kv;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
use sinex_db::SourceMaterialRecord;
use sinex_db::models::SourceMaterial;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::{EventId, Provenance};
use sinex_primitives::nats::{
    NatsTrafficClass, create_or_open_kv_store, insert_traffic_class_header,
};
use sinex_primitives::{
    EventSource, EventType, HostName, Id, JsonValue, OffsetKind, Timestamp, Uuid,
    domain::{NodeName, NodeState},
    non_empty::NonEmptyVec,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::{RwLock, oneshot, watch};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

const DEFAULT_EVENT_CHANNEL_SIZE: usize = 1024;

/// Unified runner for nodes
type NodeFactory<T> = Arc<dyn Fn() -> T + Send + Sync>;

pub struct NodeRunner<T: Node> {
    node: T,
    node_factory: Option<NodeFactory<T>>,
    lifecycle: RunnerLifecycle,
    handles: Option<NodeHandles>,
    service_info: Option<ServiceInfo>,
    raw_config: Option<HashMap<String, serde_json::Value>>,
    work_dir_utf8: Option<Utf8PathBuf>,
    event_batcher_handle: Option<tokio::task::JoinHandle<NodeResult<()>>>,
    event_batcher_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    schema_listener_shutdown: Option<watch::Sender<bool>>,
    schema_listener_handle: Option<tokio::task::JoinHandle<()>>,
    checkpoint_cleanup_shutdown: Option<watch::Sender<bool>>,
    checkpoint_cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    consumer_handle: Option<tokio::task::JoinHandle<()>>,
    command_listener_shutdown: Option<watch::Sender<bool>>,
    command_listener_handle: Option<tokio::task::JoinHandle<()>>,
    processing_model: ProcessingModel,
    leader_state: Option<LeaderState>,
}

struct LeaderState {
    kv_client: sinex_primitives::coordination::CoordinationKvClient,
    instance_id: String,
    heartbeat_shutdown: tokio::sync::oneshot::Sender<()>,
    heartbeat_handle: tokio::task::JoinHandle<()>,
}

/// Batch of events resolved from provisional confirmations.
#[cfg(feature = "messaging")]
struct ResolvedBatch {
    events: Vec<Event<JsonValue>>,
    last_event_id: Option<Uuid>,
}

#[cfg(feature = "messaging")]
struct DispatchedScanOutcome {
    report: ScanReport,
    events_emitted: u64,
}

#[cfg(feature = "messaging")]
struct FailedDispatchedScanOutcome {
    error: SinexError,
    events_emitted: u64,
}

mod shutdown_helpers;
mod control_messages;

impl<T: Node + 'static> NodeRunner<T> {
    #[cfg(feature = "db")]
    async fn register_runtime_identity(
        &self,
        pool: &PgPool,
        service_name: &str,
        instance_id: &str,
        host: &str,
        version: &str,
        raw_config: &HashMap<String, serde_json::Value>,
    ) -> NodeResult<Option<Uuid>> {
        let node_name = NodeName::new(self.node.node_name());
        let node_type = match self.node.node_type() {
            NodeType::Ingestor => sinex_primitives::domain::NodeType::Ingestor,
            NodeType::Automaton => sinex_primitives::domain::NodeType::Automaton,
        };
        let manifest = pool
            .state()
            .register_node(&node_name, node_type, version, None)
            .await
            .map_err(|error| {
                SinexError::processing(format!(
                    "Failed to register node manifest for {}: {error}",
                    self.node.node_name()
                ))
            })?;
        let (config_hash, effective_config) = Self::effective_config(raw_config)?;
        let node_run = pool
            .state()
            .start_node_run(
                manifest.id,
                service_name,
                instance_id,
                host,
                config_hash.as_deref(),
                effective_config.as_ref(),
            )
            .await
            .map_err(|error| {
                SinexError::processing(format!(
                    "Failed to register node run for {}: {error}",
                    self.node.node_name()
                ))
            })?;
        Ok(Some(node_run.id))
    }

    #[cfg(feature = "db")]
    async fn update_registered_run_status(
        pool: &PgPool,
        service_info: &ServiceInfo,
        status: NodeState,
    ) {
        let Some(node_run_id) = service_info.node_run_id() else {
            return;
        };
        if let Err(error) = pool
            .state()
            .update_node_run_status(node_run_id, status)
            .await
        {
            warn!(
                node = %service_info.node_name(),
                service = %service_info.service_name(),
                node_run_id = %node_run_id,
                target_status = %status,
                error = %error,
                "Failed to persist node run terminal status"
            );
        }
    }

    /// Create a new node runner
    pub fn new(node: T) -> Self {
        Self::new_with_optional_factory(node, None)
    }

    /// Create a node runner with a factory for fresh worker instances.
    pub fn new_with_factory(node: T, node_factory: NodeFactory<T>) -> Self {
        Self::new_with_optional_factory(node, Some(node_factory))
    }

    fn new_with_optional_factory(node: T, node_factory: Option<NodeFactory<T>>) -> Self {
        Self {
            node,
            node_factory,
            lifecycle: RunnerLifecycle::Created,
            handles: None,
            service_info: None,
            raw_config: None,
            work_dir_utf8: None,
            event_batcher_handle: None,
            event_batcher_shutdown: None,
            schema_listener_shutdown: None,
            schema_listener_handle: None,
            checkpoint_cleanup_shutdown: None,
            checkpoint_cleanup_handle: None,
            consumer_handle: None,
            command_listener_shutdown: None,
            command_listener_handle: None,
            processing_model: ProcessingModel::StatelessWorker,
            leader_state: None,
        }
    }

    fn config_identity_value(
        raw_config: &HashMap<String, serde_json::Value>,
        key: &str,
    ) -> Option<String> {
        raw_config
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    }

    async fn drain_completion_checkpoint_description(&self) -> Option<String> {
        let node_checkpoint = self.node.current_checkpoint().await.ok();
        if let Some(checkpoint) = node_checkpoint.clone()
            && !matches!(checkpoint, Checkpoint::None)
        {
            return Some(checkpoint.description());
        }

        if let Some(handles) = &self.handles
            && let Ok(checkpoint_state) = handles.checkpoint_manager().load_checkpoint().await
            && !matches!(checkpoint_state.checkpoint, Checkpoint::None)
        {
            return Some(checkpoint_state.checkpoint.description());
        }

        node_checkpoint.map(|checkpoint| checkpoint.description())
    }

    /// Returns the current lifecycle state of this runner.
    pub fn lifecycle(&self) -> RunnerLifecycle {
        self.lifecycle
    }

    /// Return the underlying node type.
    pub fn node_type(&self) -> NodeType {
        self.node.node_type()
    }

    /// Reconstruct the current runtime state if the runner has been initialized
    pub fn runtime_state(&self) -> Option<NodeRuntimeState> {
        let handles = self.handles.clone()?;
        let service_info = self.service_info.clone()?;
        let raw_config = self.raw_config.clone()?;
        let work_dir_utf8 = self.work_dir_utf8.clone()?;

        Some(NodeRuntimeState::new(
            service_info,
            handles,
            raw_config,
            work_dir_utf8,
        ))
    }

    /// Initialize the node with a specific transport
    pub async fn initialize_with_transport(
        &mut self,
        service_name: String,
        raw_config: HashMap<String, serde_json::Value>,
        #[cfg(feature = "db")] db_pool: Option<PgPool>,
        transport: EventTransport,
        work_dir: std::path::PathBuf,
        dry_run: bool,
    ) -> NodeResult<()> {
        // Re-entrancy guard: only allow initialization from Created state
        match self.lifecycle {
            RunnerLifecycle::Created => {}
            RunnerLifecycle::Initializing => {
                return Err(SinexError::lifecycle(
                    "Node is already being initialized (concurrent initialize call detected)"
                        .to_string(),
                ));
            }
            RunnerLifecycle::Initialized
            | RunnerLifecycle::Running
            | RunnerLifecycle::ShutdownFailed
            | RunnerLifecycle::ShutDown => {
                return Err(SinexError::lifecycle(format!(
                    "Cannot initialize node: runner is in '{}' state (expected 'Created')",
                    self.lifecycle,
                )));
            }
        }
        self.lifecycle = RunnerLifecycle::Initializing;

        // DATABASE_URL is optional - nodes that need it will call
        // require_db_pool() which provides a clear error message.

        // Create bounded event channel
        let (event_sender_raw, event_receiver) =
            mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);

        // Create shutdown channels
        let (batcher_shutdown_sender, batcher_shutdown_receiver) = tokio::sync::oneshot::channel();
        self.event_batcher_shutdown = Some(batcher_shutdown_sender);

        // Get hostname
        let host = sinex_primitives::events::builder::get_hostname();
        let consumer_name = format!("{host}-{}", std::process::id());
        let instance_id = Self::build_instance_id(host.as_str());
        let version = crate::version::node_version().map_or_else(
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
                crate::checkpoint::CheckpointCleanupConfig::from_env().enabled
            }
            #[cfg(not(feature = "messaging"))]
            {
                false
            }
        };

        if cleanup_enabled {
            #[cfg(feature = "messaging")]
            {
                let cleanup_config = crate::checkpoint::CheckpointCleanupConfig::from_env();
                let kv_for_cleanup = kv_store.clone();
                let (cleanup_shutdown_tx, cleanup_shutdown_rx) = watch::channel(false);
                let cleanup_handle = crate::checkpoint::spawn_checkpoint_cleanup_task(
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
        let source_unit_id = Self::config_identity_value(&raw_config, "source_unit_id");
        let runner_pack = Self::config_identity_value(&raw_config, "runner_pack");
        let checkpoint_identity = source_unit_id
            .clone()
            .unwrap_or_else(|| service_name.clone());

        // Initialize checkpoint manager with KV
        let checkpoint_manager = Arc::new(CheckpointManager::with_missing_checkpoint_warning(
            kv_store,
            checkpoint_identity.clone(),
            consumer_group,
            consumer_name.clone(),
            matches!(self.node.node_type(), NodeType::Automaton),
        ));

        // NATS is the only transport
        let transport_type = "NATS";

        // Determine if automaton to enable LeaderStandby
        let confirmation_buffer_opt = if matches!(self.node.node_type(), NodeType::Automaton) {
            self.processing_model = ProcessingModel::LeaderStandby;
            Some(Arc::new(crate::ConfirmationBuffer::new(
                std::time::Duration::from_mins(1),
            )))
        } else {
            self.processing_model = ProcessingModel::StatelessWorker;
            None
        };

        #[cfg(feature = "db")]
        let node_run_id = if let Some(pool) = db_pool.as_ref() {
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
        let node_run_id = None;

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

        if let Some(node_run_id) = node_run_id {
            event_emitter = event_emitter.with_default_node_run_id(node_run_id);
        }

        // No LeaseManager passed to handles
        // No LeaseManager passed to handles
        let handles = {
            #[cfg(feature = "db")]
            if let Some(pool) = db_pool {
                NodeHandles::new(
                    pool,
                    checkpoint_manager.clone(),
                    event_emitter.clone(),
                    transport_for_context,
                    confirmation_buffer_opt,
                    schema_cache.clone(),
                )
            } else {
                NodeHandles::new_edge(
                    checkpoint_manager.clone(),
                    event_emitter.clone(),
                    transport_for_context,
                    confirmation_buffer_opt,
                    schema_cache.clone(),
                )
            }

            #[cfg(not(feature = "db"))]
            NodeHandles::new_edge(
                checkpoint_manager.clone(),
                event_emitter.clone(),
                transport_for_context,
                confirmation_buffer_opt,
                schema_cache.clone(),
            )
        };

        let service_info = ServiceInfo::new_with_runtime_identity(
            service_name.clone(),
            self.node.node_name().to_string(),
            source_unit_id.clone(),
            runner_pack.clone(),
            host.clone(),
            work_dir.clone(),
            dry_run,
            instance_id,
            version,
            node_run_id,
        );
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir).unwrap_or_else(|_| {
            Utf8PathBuf::from_path_buf(sinex_primitives::environment::environment().temp_dir())
                .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex"))
        });

        let typed_config = if raw_config.is_empty() {
            T::Config::default()
        } else {
            let config_value = serde_json::to_value(&raw_config).map_err(|e| {
                SinexError::configuration(format!("Failed to serialize node config: {e}"))
            })?;
            serde_json::from_value(config_value).map_err(|e| {
                SinexError::configuration(format!("Failed to parse node config: {e}"))
            })?
        };

        let init_context = NodeInitContext::new(
            typed_config,
            raw_config.clone(),
            service_info.clone(),
            handles.clone(),
            work_dir_utf8.clone(),
        );

        if let Err(e) = self.node.initialize(init_context).await {
            #[cfg(feature = "db")]
            if let Some(pool) = handles.db_pool().cloned() {
                Self::update_registered_run_status(&pool, &service_info, NodeState::Failed).await;
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
            node = %self.node.node_name(),
            source_unit = source_unit_id.as_deref().unwrap_or("none"),
            runner_pack = runner_pack.as_deref().unwrap_or("none"),
            checkpoint_identity = %checkpoint_identity,
            node_type = ?self.node.node_type(),
            transport = transport_type,
            "Node initialized"
        );

        Ok(())
    }

    /// Run a scan operation
    pub async fn run_scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
        match self.lifecycle {
            RunnerLifecycle::Initialized | RunnerLifecycle::Running => {}
            other => {
                return Err(SinexError::lifecycle(format!(
                    "Cannot run scan: runner is in '{other}' state (expected 'Initialized' or 'Running')",
                )));
            }
        }

        info!(
            node = %self.node.node_name(),
            from = %from.description(),
            until = ?until,
            dry_run = args.dry_run,
            "Starting scan operation"
        );

        let start_time = std::time::Instant::now();
        let result = self.node.scan(from, until, args).await;

        match &result {
            Ok(report) => {
                info!(
                    node = %self.node.node_name(),
                    events_processed = report.events_processed,
                    duration_ms = start_time.elapsed().as_millis(),
                    "Scan operation completed successfully"
                );
            }
            Err(e) => {
                warn!(
                    node = %self.node.node_name(),
                    error = %e,
                    duration_ms = start_time.elapsed().as_millis(),
                    "Scan operation failed"
                );
            }
        }

        result
    }

    /// Run in service mode with startup sequence
    pub async fn run_service(&mut self) -> NodeResult<()>
    where
        T: Default,
    {
        match self.lifecycle {
            RunnerLifecycle::Initialized => {}
            RunnerLifecycle::Running => {
                return Err(SinexError::lifecycle(
                    "Node is already running (concurrent run_service call detected)".to_string(),
                ));
            }
            other => {
                return Err(SinexError::lifecycle(format!(
                    "Cannot run service: runner is in '{other}' state (expected 'Initialized')",
                )));
            }
        }
        self.lifecycle = RunnerLifecycle::Running;

        let node_type = self.node.node_type();
        info!(
            node = %self.node.node_name(),
            node_type = ?node_type,
            "Starting service with startup sequence"
        );

        let heartbeat_interval = env_parse_with_default(
            "SINEX_COORDINATION_HEARTBEAT",
            30_u64,
            "node coordination heartbeat",
        );
        let runtime = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?;
        let heartbeat = crate::heartbeat::HeartbeatEmitter::from_runtime(
            &runtime,
            sinex_primitives::Seconds::from_secs(heartbeat_interval),
        );
        let heartbeat_identity = serde_json::json!({
            "node_name": runtime.node_name(),
            "source_unit_id": runtime.source_unit_id(),
            "runner_pack": runtime.runner_pack(),
            "service_instance": runtime.service_info().service_name(),
            "checkpoint_identity": runtime.checkpoint_identity(),
            "control_identity": runtime.control_identity(),
            "host": runtime.service_info().host().as_str(),
            "run_id": runtime.node_run_id().map(|id| id.to_string()),
        });
        let (heartbeat_shutdown_tx, heartbeat_shutdown_rx) = tokio::sync::oneshot::channel();
        let heartbeat_handle = tokio::spawn(async move {
            tokio::select! {
                () = heartbeat.start_periodic_heartbeat(Some(Box::new(move || Some(heartbeat_identity.clone())))) => {}
                _ = heartbeat_shutdown_rx => {}
            }
        });
        let watchdog_handle = systemd_notify::spawn_watchdog("sinex-node");
        let drain_controller = runtime.handles().runtime_drain();

        // Start command listener for node-dispatch replay (scan commands via NATS).
        // This allows the gateway to dispatch historical scans to running nodes.
        #[cfg(feature = "messaging")]
        self.start_command_listener();

        let service_result = match node_type {
            NodeType::Ingestor => {
                // Ingestor startup sequence: Snapshot -> Gap-fill -> Continuous
                self.run_ingestor_startup_sequence().await
            }
            NodeType::Automaton => {
                #[cfg(feature = "messaging")]
                {
                    // Automaton startup: consume events from NATS streams
                    self.run_automaton_continuous_mode().await
                }
                #[cfg(not(feature = "messaging"))]
                {
                    Err(SinexError::configuration(
                        "Messaging feature required for Automaton mode".to_string(),
                    ))
                }
            }
        };

        Self::signal_shutdown_channel(heartbeat_shutdown_tx, "heartbeat");
        let heartbeat_result = Self::shutdown_join_result("heartbeat", heartbeat_handle.await);

        systemd_notify::stop_watchdog(watchdog_handle, "sinex-node").await;
        systemd_notify::notify_stopping("sinex-node");

        let shutdown_result = self.shutdown().await;

        #[cfg(feature = "messaging")]
        let drain_complete_result =
            if drain_controller.is_requested() && service_result.is_ok() && shutdown_result.is_ok()
            {
                let checkpoint = self.drain_completion_checkpoint_description().await;
                let payload = NodeDrainComplete {
                    node_name: runtime.control_identity().to_string(),
                    timestamp: Timestamp::now(),
                    checkpoint,
                };
                Some(
                    Self::publish_drain_complete(
                        &runtime.nats_client().ok_or_else(|| {
                            SinexError::lifecycle(
                                "NATS client missing during drain completion".to_string(),
                            )
                        })?,
                        runtime.control_identity(),
                        &payload,
                    )
                    .await,
                )
            } else {
                None
            };

        let mut terminal_errors = Vec::new();
        Self::push_shutdown_error(&mut terminal_errors, "service", service_result);
        Self::push_shutdown_error(&mut terminal_errors, "heartbeat", heartbeat_result);
        Self::push_shutdown_error(&mut terminal_errors, "shutdown", shutdown_result);
        #[cfg(feature = "messaging")]
        if let Some(result) = drain_complete_result {
            Self::push_shutdown_error(&mut terminal_errors, "drain_complete", result);
        }
        let terminal_result = Self::collapse_shutdown_errors(terminal_errors);

        #[cfg(feature = "db")]
        if let Some(pool) = runtime.handles().db_pool().cloned() {
            let terminal = if terminal_result.is_ok() {
                NodeState::Stopped
            } else {
                NodeState::Failed
            };
            Self::update_registered_run_status(&pool, runtime.service_info(), terminal).await;
        }

        terminal_result
    }

    /// Start the NATS command listener for node-dispatch replay.
    ///
    /// Subscribes to `sinex.control.nodes.<node_name>.scan` using NATS request-reply.
    /// When a `NodeScanCommand` arrives, the listener:
    /// 1. Replies with `NodeScanAck` (accepted or rejected)
    /// 2. If accepted, spawns an isolated replay worker for the same node type/config
    /// 3. Publishes `NodeScanProgress` updates to `sinex.control.replay.progress.<operation_id>`
    ///
    /// Only ingestor nodes accept scan commands; automata reject them (they receive
    /// re-derived events naturally via `JetStream`).
    #[cfg(feature = "messaging")]
    fn start_command_listener(&mut self) {
        let handles = if let Some(h) = &self.handles {
            h.clone()
        } else {
            warn!("Cannot start command listener: handles not initialized");
            return;
        };
        let service_info = if let Some(service_info) = &self.service_info {
            service_info.clone()
        } else {
            warn!("Cannot start command listener: service info not initialized");
            return;
        };
        let work_dir_utf8 = if let Some(work_dir_utf8) = &self.work_dir_utf8 {
            work_dir_utf8.clone()
        } else {
            warn!("Cannot start command listener: work dir not initialized");
            return;
        };

        let nats_client = match handles.transport() {
            EventTransport::Nats(publisher) => publisher.nats_client().clone(),
        };

        let node_name = service_info.control_identity().to_string();
        let node_type = self.node.node_type();
        let supports_historical = self.node.capabilities().supports_historical;
        let env = sinex_primitives::environment::environment().clone();
        let raw_config = self.raw_config.clone().unwrap_or_default();
        let dry_run = service_info.dry_run();
        let node_factory = self.node_factory.clone();
        let drain_controller = handles.runtime_drain();
        #[cfg(feature = "db")]
        let db_pool = handles.db_pool().cloned();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let subject = env.nats_subject(&format!("sinex.control.nodes.{node_name}.*"));
            let active_scan = Arc::new(AtomicBool::new(false));
            let subscribe_client = nats_client.clone();
            let subscribe_subject = subject.clone();
            let helper_shutdown_rx = shutdown_rx.clone();
            let subscription_shutdown_rx = shutdown_rx.clone();

            run_resubscribing_listener(
                "command listener",
                &subject,
                LISTENER_RETRY_DELAY,
                helper_shutdown_rx,
                move || {
                    let client = subscribe_client.clone();
                    let subject = subscribe_subject.clone();
                    async move { client.subscribe(subject).await }
                },
                move |mut sub| {
                    let loop_client = nats_client.clone();
                    let loop_env = env.clone();
                    let loop_node_name = node_name.clone();
                    let loop_handles = handles.clone();
                    let loop_service_info = service_info.clone();
                    let loop_raw_config = raw_config.clone();
                    let loop_work_dir_utf8 = work_dir_utf8.clone();
                    let loop_node_factory = node_factory.clone();
                    let loop_active_scan = active_scan.clone();
                    let loop_drain_controller = drain_controller.clone();
                    #[cfg(feature = "db")]
                    let loop_db_pool = db_pool.clone();
                    let mut shutdown_rx = subscription_shutdown_rx.clone();
                    async move {
                        loop {
                            let msg = tokio::select! {
                                maybe_msg = sub.next() => {
                                    let Some(msg) = maybe_msg else {
                                        return true;
                                    };
                                    msg
                                }
                                changed = shutdown_rx.changed() => {
                                    if changed.is_err() || *shutdown_rx.borrow() {
                                        debug!(node = %loop_node_name, "Command listener subscription received shutdown");
                                        return false;
                                    }
                                    continue;
                                }
                            };
                            match control_command_kind(msg.subject.as_ref()) {
                                Some(ControlCommandKind::Drain) => {
                                    Self::handle_drain_command(
                                        &loop_node_name,
                                        &msg.payload,
                                        &loop_drain_controller,
                                        #[cfg(feature = "db")]
                                        loop_db_pool.clone(),
                                        &loop_service_info,
                                    )
                                    .await;
                                }
                                Some(ControlCommandKind::Resume) => {
                                    warn!(
                                        node = %loop_node_name,
                                        "Resume command received, but runtime drain is currently a one-way shutdown signal"
                                    );
                                }
                                Some(ControlCommandKind::Scan) => {
                                    let command: NodeScanCommand = match serde_json::from_slice(&msg.payload) {
                                        Ok(cmd) => cmd,
                                        Err(err) => {
                                            warn!(error = %err, "Failed to deserialize NodeScanCommand");
                                            if let Some(reply) = msg.reply {
                                                let nack = NodeScanAck {
                                                    operation_id: Uuid::now_v7(),
                                                    node_name: loop_node_name.clone(),
                                                    accepted: false,
                                                    error: Some(format!("Failed to deserialize command: {err}")),
                                                };
                                                if let Err(error) =
                                                    Self::publish_scan_ack(&loop_client, Some(reply), &nack).await
                                                {
                                                    warn!(
                                                        node = %loop_node_name,
                                                        error = %error,
                                                        "Failed to publish malformed-command rejection"
                                                    );
                                                }
                                            }
                                            continue;
                                        }
                                    };

                                    let operation_id = command.operation_id;
                                    let Some(reply) = msg.reply.clone() else {
                                        warn!(
                                            operation_id = %operation_id,
                                            node = %loop_node_name,
                                            "Ignoring scan command without reply subject"
                                        );
                                        continue;
                                    };

                                    if loop_drain_controller.is_requested() {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some("Node is draining and cannot accept replay scans".to_string()),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    if node_type != NodeType::Ingestor {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some(format!(
                                                "Node '{loop_node_name}' is a {node_type:?}, not an Ingestor. Automata receive replay events via JetStream."
                                            )),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    if !supports_historical {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some(format!(
                                                "Node '{loop_node_name}' does not support historical scans (supports_historical = false)"
                                            )),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    if dry_run {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some(
                                                "Node is running in dry-run mode and cannot execute replay scans"
                                                    .to_string(),
                                            ),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    let Some(factory) = loop_node_factory.clone() else {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some("Node was started without a replay worker factory".to_string()),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    };

                                    if loop_active_scan.swap(true, Ordering::SeqCst) {
                                        let ack = NodeScanAck {
                                            operation_id,
                                            node_name: loop_node_name.clone(),
                                            accepted: false,
                                            error: Some("A scan is already in progress on this node".to_string()),
                                        };
                                        if let Err(error) =
                                            Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                        {
                                            warn!(
                                                operation_id = %operation_id,
                                                node = %loop_node_name,
                                                error = %error,
                                                "Failed to publish scan rejection"
                                            );
                                        }
                                        continue;
                                    }

                                    let ack = NodeScanAck {
                                        operation_id,
                                        node_name: loop_node_name.clone(),
                                        accepted: true,
                                        error: None,
                                    };
                                    if let Err(error) =
                                        Self::publish_scan_ack(&loop_client, Some(reply.clone()), &ack).await
                                    {
                                        error!(
                                            operation_id = %operation_id,
                                            node = %loop_node_name,
                                            error = %error,
                                            "Failed to publish scan acceptance; aborting dispatched scan"
                                        );
                                        loop_active_scan.store(false, Ordering::SeqCst);
                                        continue;
                                    }

                                    info!(
                                        operation_id = %operation_id,
                                        node = %loop_node_name,
                                        "Accepted scan command, spawning historical scan task"
                                    );

                                    let scan_client = loop_client.clone();
                                    let scan_env = loop_env.clone();
                                    let scan_node_name = loop_node_name.clone();
                                    let scan_active = loop_active_scan.clone();
                                    let scan_handles = loop_handles.clone();
                                    let scan_service_info = loop_service_info.clone();
                                    let scan_raw_config = loop_raw_config.clone();
                                    let scan_work_dir_utf8 = loop_work_dir_utf8.clone();
                                    let scan_command = command.clone();

                                    tokio::spawn(async move {
                                        struct ActiveScanGuard(Arc<AtomicBool>);

                                        impl Drop for ActiveScanGuard {
                                            fn drop(&mut self) {
                                                self.0.store(false, Ordering::SeqCst);
                                            }
                                        }

                                        let _active_scan_guard = ActiveScanGuard(scan_active.clone());
                                        let progress_subject = scan_env
                                            .nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

                                        let start_progress = NodeScanProgress {
                                            operation_id,
                                            node_name: scan_node_name.clone(),
                                            events_processed: 0,
                                            events_emitted: 0,
                                            final_report: None,
                                            error: None,
                                        };
                                        if let Err(error) = Self::publish_scan_progress(
                                            &scan_client,
                                            progress_subject.clone(),
                                            &start_progress,
                                        )
                                        .await
                                        {
                                            error!(
                                                operation_id = %operation_id,
                                                node = %scan_node_name,
                                                error = %error,
                                                "Failed to publish initial scan progress; aborting dispatched scan"
                                            );
                                            return;
                                        }

                                        let scan_outcome = Self::execute_dispatched_scan(
                                            factory,
                                            scan_handles,
                                            scan_service_info,
                                            scan_raw_config,
                                            scan_work_dir_utf8,
                                            scan_command,
                                        )
                                        .await;

                                        let final_progress = match scan_outcome {
                                            Ok(outcome) => {
                                                let mut report = outcome.report;
                                                report
                                                    .node_stats
                                                    .entry("events_emitted".to_string())
                                                    .or_insert(outcome.events_emitted);
                                                NodeScanProgress {
                                                    operation_id,
                                                    node_name: scan_node_name.clone(),
                                                    events_processed: report.events_processed,
                                                    events_emitted: outcome.events_emitted,
                                                    final_report: Some(report),
                                                    error: None,
                                                }
                                            }
                                            Err(outcome) => {
                                                warn!(
                                                    operation_id = %operation_id,
                                                    node = %scan_node_name,
                                                    error = %outcome.error,
                                                    events_emitted = outcome.events_emitted,
                                                    "Dispatched scan failed"
                                                );
                                                NodeScanProgress {
                                                    operation_id,
                                                    node_name: scan_node_name.clone(),
                                                    events_processed: outcome.events_emitted,
                                                    events_emitted: outcome.events_emitted,
                                                    final_report: None,
                                                    error: Some(outcome.error.to_string()),
                                                }
                                            }
                                        };

                                        if let Err(error) =
                                            Self::publish_scan_progress(&scan_client, progress_subject, &final_progress)
                                                .await
                                        {
                                            error!(
                                                operation_id = %operation_id,
                                                node = %scan_node_name,
                                                error = %error,
                                                "Failed to publish final scan progress"
                                            );
                                        }
                                    });
                                }
                                None => {
                                    warn!(
                                        node = %loop_node_name,
                                        subject = %msg.subject,
                                        "Ignoring unsupported node control subject"
                                    );
                                }
                            }
                        }
                    }
                },
            )
            .await;
        });

        self.command_listener_shutdown = Some(shutdown_tx);
        self.command_listener_handle = Some(handle);
    }

    #[cfg(feature = "messaging")]
    async fn execute_dispatched_scan(
        node_factory: NodeFactory<T>,
        base_handles: NodeHandles,
        base_service_info: ServiceInfo,
        raw_config: HashMap<String, serde_json::Value>,
        work_dir_utf8: Utf8PathBuf,
        command: NodeScanCommand,
    ) -> Result<DispatchedScanOutcome, FailedDispatchedScanOutcome> {
        let replay_service_name = format!(
            "{}.replay.{}",
            base_service_info.service_name(),
            command.operation_id.simple()
        );
        let replay_service_info = ServiceInfo::new_with_runtime_identity(
            replay_service_name.clone(),
            base_service_info.node_name().to_string(),
            base_service_info.source_unit_id().map(ToOwned::to_owned),
            base_service_info.runner_pack().map(ToOwned::to_owned),
            base_service_info.host().clone(),
            base_service_info.work_dir().clone(),
            base_service_info.dry_run(),
            base_service_info.instance_id().to_string(),
            base_service_info.version().to_string(),
            base_service_info.node_run_id(),
        );

        let (replay_handles, emitted_counter, forwarder_handle) =
            Self::build_replay_worker_handles(
                &base_handles,
                &replay_service_name,
                command.operation_id,
            )
            .await
            .map_err(|error| FailedDispatchedScanOutcome {
                error,
                events_emitted: 0,
            })?;

        let typed_config = if raw_config.is_empty() {
            T::Config::default()
        } else {
            let config_value =
                serde_json::to_value(&raw_config).map_err(|error| FailedDispatchedScanOutcome {
                    error: SinexError::configuration(format!(
                        "Failed to serialize replay worker config: {error}"
                    )),
                    events_emitted: 0,
                })?;
            serde_json::from_value(config_value).map_err(|error| FailedDispatchedScanOutcome {
                error: SinexError::configuration(format!(
                    "Failed to parse replay worker config: {error}"
                )),
                events_emitted: 0,
            })?
        };

        let init_context = NodeInitContext::new(
            typed_config,
            raw_config,
            replay_service_info,
            replay_handles,
            work_dir_utf8,
        );

        let mut worker = node_factory();
        if let Err(error) = worker.initialize(init_context).await {
            return Err(FailedDispatchedScanOutcome {
                error,
                events_emitted: 0,
            });
        }

        let scan_result = worker
            .scan(command.from.clone(), command.until.clone(), command.args)
            .await;
        let shutdown_result = worker.shutdown().await;
        drop(worker);

        let forwarder_result =
            Self::finish_replay_forwarder(forwarder_handle, emitted_counter).await;

        match (scan_result, shutdown_result, forwarder_result) {
            (Ok(report), Ok(()), Ok(events_emitted)) => Ok(DispatchedScanOutcome {
                report,
                events_emitted,
            }),
            (Err(error), Ok(()), Ok(events_emitted)) => Err(FailedDispatchedScanOutcome {
                error,
                events_emitted,
            }),
            (Ok(_), Err(error), Ok(events_emitted)) => Err(FailedDispatchedScanOutcome {
                error,
                events_emitted,
            }),
            (Err(scan_error), Err(shutdown_error), Ok(events_emitted)) => {
                Err(FailedDispatchedScanOutcome {
                    error: scan_error.with_context("shutdown_error", shutdown_error.to_string()),
                    events_emitted,
                })
            }
            (Ok(_), Ok(()), Err(forwarder_error)) => Err(forwarder_error),
            (Err(scan_error), Ok(()), Err(forwarder_error)) => Err(FailedDispatchedScanOutcome {
                error: scan_error
                    .with_context("replay_forwarder_error", forwarder_error.error.to_string()),
                events_emitted: forwarder_error.events_emitted,
            }),
            (Ok(_), Err(shutdown_error), Err(forwarder_error)) => {
                Err(FailedDispatchedScanOutcome {
                    error: shutdown_error
                        .with_context("replay_forwarder_error", forwarder_error.error.to_string()),
                    events_emitted: forwarder_error.events_emitted,
                })
            }
            (Err(scan_error), Err(shutdown_error), Err(forwarder_error)) => {
                Err(FailedDispatchedScanOutcome {
                    error: scan_error
                        .with_context("shutdown_error", shutdown_error.to_string())
                        .with_context("replay_forwarder_error", forwarder_error.error.to_string()),
                    events_emitted: forwarder_error.events_emitted,
                })
            }
        }
    }

    #[cfg(feature = "messaging")]
    async fn build_replay_worker_handles(
        base_handles: &NodeHandles,
        replay_service_name: &str,
        operation_id: Uuid,
    ) -> NodeResult<(
        NodeHandles,
        Arc<AtomicU64>,
        tokio::task::JoinHandle<NodeResult<()>>,
    )> {
        let checkpoint_kv = create_checkpoint_kv(base_handles.transport()).await?;
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            checkpoint_kv,
            replay_service_name.to_string(),
            format!("replay-{}", operation_id.simple()),
            format!("dispatch-{}", operation_id.simple()),
        ));

        let (replay_sender, mut replay_receiver) =
            mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);
        let replay_emitter = base_handles
            .emitter()
            .clone_with_sender(replay_sender)
            .with_default_created_by_operation_id(operation_id);
        let target_sender = base_handles.emitter().sender();
        let emitted_counter = Arc::new(AtomicU64::new(0));
        let counter = emitted_counter.clone();
        let forwarder_handle = tokio::spawn(async move {
            while let Some(event) = replay_receiver.recv().await {
                target_sender.send(event).await.map_err(|_| {
                    SinexError::processing("Replay forwarder target channel closed".to_string())
                })?;
                counter.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        });

        let confirmation_buffer = base_handles.confirmation_buffer();
        let schema_cache = base_handles.schema_cache();
        #[cfg(feature = "db")]
        let replay_handles = match base_handles.db_pool().cloned() {
            Some(db_pool) => NodeHandles::new(
                db_pool,
                checkpoint_manager,
                replay_emitter,
                base_handles.transport().clone(),
                confirmation_buffer,
                schema_cache,
            ),
            None => NodeHandles::new_edge(
                checkpoint_manager,
                replay_emitter,
                base_handles.transport().clone(),
                confirmation_buffer,
                schema_cache,
            ),
        };
        #[cfg(not(feature = "db"))]
        let replay_handles = NodeHandles::new_edge(
            checkpoint_manager,
            replay_emitter,
            base_handles.transport().clone(),
            confirmation_buffer,
            schema_cache,
        );

        Ok((replay_handles, emitted_counter, forwarder_handle))
    }

    #[cfg(feature = "messaging")]
    async fn finish_replay_forwarder(
        forwarder_handle: tokio::task::JoinHandle<NodeResult<()>>,
        emitted_counter: Arc<AtomicU64>,
    ) -> Result<u64, FailedDispatchedScanOutcome> {
        let events_emitted = emitted_counter.load(Ordering::SeqCst);
        match forwarder_handle.await {
            Ok(Ok(())) => Ok(events_emitted),
            Ok(Err(error)) => Err(FailedDispatchedScanOutcome {
                error,
                events_emitted,
            }),
            Err(join_error) => Err(FailedDispatchedScanOutcome {
                error: SinexError::processing("Replay forwarder join failed".to_string())
                    .with_std_error(&join_error),
                events_emitted,
            }),
        }
    }

    /// Run ingestor startup sequence (Snapshot -> Gap-fill -> Continuous)
    async fn run_ingestor_startup_sequence(&mut self) -> NodeResult<()> {
        let preexisting_checkpoint = self.node.current_checkpoint().await?;
        let drain_controller = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?
            .handles()
            .runtime_drain();

        // Phase 1: Snapshot (if supported)
        if self.node.capabilities().supports_snapshot {
            info!("Phase 1: Taking initial snapshot");
            let snapshot_report = self
                .node
                .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
                .await?;

            debug!(
                events = snapshot_report.events_processed,
                "Snapshot phase completed"
            );

            if drain_controller.is_requested() {
                info!("Drain requested during snapshot phase; skipping later startup phases");
                return Ok(());
            }
        }

        // Phase 2: Gap-filling (if supported and needed)
        if self.node.capabilities().supports_historical {
            // Only gap-fill from a checkpoint that existed before startup began.
            if !matches!(preexisting_checkpoint, Checkpoint::None) {
                info!("Phase 2: Gap-filling from last checkpoint");
                let gap_fill_report = self
                    .node
                    .scan(
                        preexisting_checkpoint.clone(),
                        TimeHorizon::Historical {
                            end_time: sinex_primitives::temporal::Timestamp::now(),
                        },
                        ScanArgs::default(),
                    )
                    .await?;

                debug!(
                    events = gap_fill_report.events_processed,
                    "Gap-fill phase completed"
                );
            }

            if drain_controller.is_requested() {
                info!("Drain requested during gap-fill phase; skipping continuous startup");
                return Ok(());
            }
        }

        // Phase 3: Continuous processing (traditional scan method)
        if self.node.capabilities().supports_continuous {
            info!("Phase 3: Starting continuous processing");
            let current_checkpoint = self.node.current_checkpoint().await?;
            systemd_notify::notify_ready("sinex-node");

            // This should run indefinitely until shutdown
            let continuous_report = self
                .node
                .scan(
                    current_checkpoint,
                    TimeHorizon::Continuous,
                    ScanArgs::default(),
                )
                .await?;

            if drain_controller.is_requested() {
                info!(
                    events_processed = continuous_report.events_processed,
                    "Continuous scan exited after runtime drain request"
                );
            } else {
                // If continuous scan returns, it means it exited unexpectedly.
                // Log so operators can investigate (M4: silent exit prevention).
                warn!(
                    events_processed = continuous_report.events_processed,
                    "Continuous scan returned unexpectedly - service will exit. \
                     This may indicate the scan implementation does not block indefinitely."
                );
            }
        } else {
            warn!("Node does not support continuous mode - service will exit");
        }

        Ok(())
    }

    /// Run automaton in continuous mode
    #[cfg(feature = "messaging")]
    async fn run_automaton_continuous_mode(&mut self) -> NodeResult<()> {
        info!("Starting automaton continuous mode");
        let drain_controller = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?
            .handles()
            .runtime_drain();

        // Get current checkpoint to resume from previous state if available
        let current_checkpoint = self.node.current_checkpoint().await?;
        let capabilities = self.node.capabilities();

        if capabilities.supports_continuous {
            info!("Starting continuous event processing for automaton");

            // A standby automaton is still a healthy, ready service. Satisfy
            // the systemd notify contract before waiting on lease handoff or
            // expiry so host activation does not fail on a legitimate standby.
            systemd_notify::notify_ready("sinex-node");

            if self.processing_model == ProcessingModel::LeaderStandby {
                let leader_acquired = self.acquire_leader_standby().await?;
                if !leader_acquired {
                    info!("Drain requested while waiting in leader standby; exiting cleanly");
                    return Ok(());
                }
            }

            if capabilities.manages_own_continuous_loop {
                let _continuous_report = self
                    .node
                    .scan(
                        current_checkpoint,
                        TimeHorizon::Continuous,
                        ScanArgs::default(),
                    )
                    .await?;
            } else {
                self.run_automaton_event_bridge(current_checkpoint).await?;
            }

            if drain_controller.is_requested() {
                info!("Automaton continuous processing completed after runtime drain");
            } else {
                info!("Automaton continuous processing completed");
            }
        } else {
            // Automata can also run in batch mode for historical processing
            if capabilities.supports_historical {
                info!("Running automaton in historical batch mode");

                // Process all historical events up to now
                let _historical_report = self
                    .node
                    .scan(
                        current_checkpoint,
                        TimeHorizon::Historical {
                            end_time: sinex_primitives::temporal::Timestamp::now(),
                        },
                        ScanArgs::default(),
                    )
                    .await?;

                info!("Automaton historical processing completed");
            } else {
                warn!("Automaton does not support continuous or historical mode");
            }
        }

        Ok(())
    }

    /// Acquire leadership for `LeaderStandby` processing model.
    ///
    /// If another instance currently holds the lease, remain in standby and
    /// retry until the lease is handed off or expires.
    async fn acquire_leader_standby(&mut self) -> NodeResult<bool> {
        let rs = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?;
        let drain_controller = rs.handles().runtime_drain();

        #[cfg(feature = "messaging")]
        {
            let nc = rs
                .nats_client()
                .ok_or_else(|| SinexError::lifecycle("NATS client missing".to_string()))?;
            let service = rs.service_info().service_name().to_string();
            let host = rs.service_info().host().as_str().to_string();
            let pid = std::process::id();
            let instance_id = format!("{host}-{pid}");

            let js = async_nats::jetstream::new(nc);
            let kv_client =
                sinex_primitives::coordination::CoordinationKvClient::new(js, service.clone());
            let heartbeat_interval = kv_client.heartbeat_interval();
            let mut announced_standby = false;

            loop {
                if drain_controller.is_requested() {
                    return Ok(false);
                }

                let is_leader = kv_client
                    .acquire_leadership(&instance_id)
                    .await
                    .map_err(|e| {
                        SinexError::processing(format!("Failed to acquire leadership: {e}"))
                    })?;

                if is_leader {
                    break;
                }

                if !announced_standby {
                    info!(
                        service = %service,
                        heartbeat_interval_ms = heartbeat_interval.as_millis(),
                        "Not leader; entering standby and waiting for lease handoff or expiry"
                    );
                    announced_standby = true;
                }

                tokio::time::sleep(heartbeat_interval).await;
            }

            info!("Confirmed as leader, proceeding with processing");

            // Reuse the configured coordination heartbeat interval so stream-mode
            // leader/standby timing matches the main coordination runtime.
            let kv_clone = kv_client.clone();
            let instance_id_clone = instance_id.clone();
            let (heartbeat_shutdown, heartbeat_shutdown_rx) = tokio::sync::oneshot::channel();
            let heartbeat_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(heartbeat_interval);
                let mut heartbeat_shutdown_rx = heartbeat_shutdown_rx;
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if let Err(e) = kv_clone.acquire_leadership(&instance_id_clone).await {
                                warn!("Heartbeat failed: {e}");
                            }
                        }
                        _ = &mut heartbeat_shutdown_rx => {
                            break;
                        }
                    }
                }
            });

            self.leader_state = Some(LeaderState {
                kv_client,
                instance_id,
                heartbeat_shutdown,
                heartbeat_handle,
            });
        }

        #[cfg(not(feature = "messaging"))]
        {
            let _ = rs; // suppress unused variable
            warn!("LeaderStandby mode requires messaging feature. Skipping leadership check.");
        }

        Ok(true)
    }

    #[cfg(feature = "messaging")]
    async fn run_automaton_event_bridge(&mut self, from: Checkpoint) -> NodeResult<()> {
        let handles = self
            .handles
            .as_ref()
            .ok_or_else(|| SinexError::lifecycle("Runner handles not initialized".to_string()))?;
        let drain_controller = handles.runtime_drain();

        #[cfg(feature = "db")]
        let db_pool = handles.db_pool().cloned();
        // No db_pool variable if db feature is off
        #[cfg(feature = "db")]
        let db_backed_confirmations = db_pool.is_some();
        #[cfg(not(feature = "db"))]
        let db_backed_confirmations = false;
        let transport = handles.transport().clone();

        let service_name = self.service_info.as_ref().map_or_else(
            || self.node.node_name().to_string(),
            |info| info.service_name().to_string(),
        );

        let (sender, mut receiver) =
            mpsc::channel::<ProvisionalEvent>(CONFIRMED_EVENT_CHANNEL_CAPACITY);
        let handler = Arc::new(RunnerConfirmedEventHandler::new(sender));

        let env = sinex_primitives::environment::environment().clone();

        let nats_client = match &transport {
            EventTransport::Nats(publisher) => publisher.nats_client().clone(),
        };

        let consumer_config = JetStreamEventConsumerConfig {
            processing_model: self.processing_model,
            batch_size: 128,
            confirmation_timeout: std::time::Duration::from_mins(1),
            consumer_name: if db_backed_confirmations {
                format!("{}-automaton-confirmed-v2", service_name.replace('.', "_"))
            } else {
                format!("{}-automaton", service_name.replace('.', "_"))
            },
            enable_provisional_processing: false,
            buffer_raw_events: !db_backed_confirmations,
            accept_unbuffered_confirmations: db_backed_confirmations,
            deliver_policy: if db_backed_confirmations {
                async_nats::jetstream::consumer::DeliverPolicy::New
            } else {
                async_nats::jetstream::consumer::DeliverPolicy::All
            },
            ..Default::default()
        };

        let consumer = Arc::new(JetStreamEventConsumer::new(
            nats_client,
            env,
            consumer_config,
            handler,
            None,
        ));

        let consumer_failure = Arc::new(tokio::sync::Mutex::new(None));
        let consumer_runner = consumer.clone();
        let consumer_failure_reporter = Arc::clone(&consumer_failure);
        let consumer_handle = tokio::spawn(async move {
            if let Err(err) = consumer_runner.run().await {
                warn!(error = %err, "Automaton JetStream consumer terminated unexpectedly");
                let mut guard = consumer_failure_reporter.lock().await;
                *guard = Some(err);
            }
        });
        drain_controller.register_runtime_abort(consumer_handle.abort_handle());
        self.consumer_handle = Some(consumer_handle);

        if !matches!(from, Checkpoint::None) && self.node.capabilities().supports_historical {
            info!("Processing historical backlog before entering continuous mode");
            let _ = self
                .node
                .scan(
                    from,
                    TimeHorizon::Historical {
                        end_time: sinex_primitives::temporal::Timestamp::now(),
                    },
                    ScanArgs::default(),
                )
                .await?;
        }

        if drain_controller.is_requested() {
            let _ = drain_controller.abort_runtime_work();
            info!("Drain requested before automaton bridge entered live processing");
        }

        let bridge_manages_checkpoints = !self.node.capabilities().manages_own_checkpoints;
        if !bridge_manages_checkpoints {
            debug!(
                node = %self.node.node_name(),
                "Skipping generic automaton-bridge checkpoint tracking because the node persists its own state"
            );
        }

        // Periodic checkpoint saves: prevent data loss on crash by persisting
        // progress every CHECKPOINT_EVENT_INTERVAL events or CHECKPOINT_TIME_INTERVAL.
        const CHECKPOINT_EVENT_INTERVAL: u64 = 100;
        const CHECKPOINT_TIME_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

        let checkpoint_manager = bridge_manages_checkpoints.then(|| handles.checkpoint_manager());
        let mut checkpoint_state = if let Some(manager) = checkpoint_manager.as_deref() {
            Some(Self::load_bridge_checkpoint_state(manager).await?)
        } else {
            None
        };

        let mut processed_events = 0u64;
        let mut events_since_checkpoint = 0u64;
        let mut last_checkpoint_time = std::time::Instant::now();
        let mut last_event_id: Option<Uuid> = None;
        let mut consecutive_checkpoint_failures = 0u32;

        // Batch processing: accumulate up to BATCH_SIZE events before processing.
        // Block on the first event, then non-blocking drain whatever else is queued.
        const BATCH_SIZE: usize = 100;

        loop {
            // Normal mode blocks for more work. Once drain is requested, the
            // runner-owned consumer is aborted and the bridge switches to
            // draining whatever is already buffered before exiting cleanly.
            let next_event = if drain_controller.is_requested() {
                match receiver.try_recv() {
                    Ok(event) => Some(event),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                    | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => None,
                }
            } else {
                receiver.recv().await
            };

            let Some(first) = next_event else {
                if let Some(error) = consumer_failure.lock().await.take() {
                    return Err(error);
                }
                break;
            };

            // Non-blocking drain: grab whatever else is already queued
            let mut provisionals = vec![first];
            while provisionals.len() < BATCH_SIZE {
                match receiver.try_recv() {
                    Ok(p) => provisionals.push(p),
                    Err(_) => break,
                }
            }

            // Resolve each provisional to a full Event
            let resolve_result = Self::resolve_provisionals_to_events(
                &provisionals,
                #[cfg(feature = "db")]
                &db_pool,
            )
            .await?;

            if resolve_result.events.is_empty() {
                continue;
            }

            let batch_count = Self::process_batch_with_dlq_fallback(
                &mut self.node,
                &transport,
                resolve_result.events,
            )
            .await?;

            processed_events += batch_count;
            events_since_checkpoint += batch_count;
            if let Some(eid) = resolve_result.last_event_id {
                last_event_id = Some(eid);
            }

            // Periodic checkpoint save: every N events or M seconds
            if bridge_manages_checkpoints
                && (events_since_checkpoint >= CHECKPOINT_EVENT_INTERVAL
                    || last_checkpoint_time.elapsed() >= CHECKPOINT_TIME_INTERVAL)
                && let (Some(manager), Some(state)) =
                    (checkpoint_manager.as_deref(), checkpoint_state.as_mut())
                && let Some(revision) = Self::try_save_checkpoint(
                    manager,
                    state,
                    last_event_id,
                    processed_events,
                    &mut consecutive_checkpoint_failures,
                )
                .await?
            {
                state.revision = revision;
                events_since_checkpoint = 0;
                last_checkpoint_time = std::time::Instant::now();
            }
        }

        // Save final checkpoint on clean exit
        if bridge_manages_checkpoints
            && let (Some(manager), Some(state)) =
                (checkpoint_manager.as_deref(), checkpoint_state.as_mut())
            && Self::try_save_checkpoint(
                manager,
                state,
                last_event_id,
                processed_events,
                &mut consecutive_checkpoint_failures,
            )
            .await?
            .is_some()
        {
            info!(processed_events, "Final checkpoint saved on clean shutdown");
        }

        if drain_controller.is_requested() {
            info!(
                processed_events,
                "JetStream bridge drained after runtime drain request"
            );
        } else {
            info!(
                processed_events,
                "JetStream confirmed event channel closed; stopping automaton bridge"
            );
        }

        consumer.stop().await;
        drain_controller.clear_runtime_abort();

        if let Some(handle) = self.consumer_handle.take() {
            match handle.await {
                Ok(()) => {}
                Err(err) if err.is_cancelled() => {
                    debug!(error = ?err, "Automaton consumer task aborted during shutdown");
                }
                Err(err) => {
                    return Err(SinexError::service(format!(
                        "Failed to join automaton consumer task: {err}"
                    )));
                }
            }
        }

        Ok(())
    }

    #[cfg(feature = "messaging")]
    async fn load_bridge_checkpoint_state(
        checkpoint_manager: &CheckpointManager,
    ) -> NodeResult<crate::checkpoint::CheckpointState> {
        checkpoint_manager.load_checkpoint().await.map_err(|error| {
            SinexError::checkpoint("Failed to load checkpoint state for automaton bridge")
                .with_source(error)
        })
    }

    #[cfg(feature = "db")]
    async fn fetch_persisted_event(
        pool: &PgPool,
        event_id: &EventId,
    ) -> NodeResult<Option<Event<JsonValue>>> {
        let event_id_str = event_id.to_string();
        pool.events().get_by_id(*event_id).await.map_err(|err| {
            SinexError::processing(format!(
                "Failed to load confirmed event {event_id_str} from database: {err}"
            ))
        })
    }

    fn parse_uuid(value: &str, field: &str) -> NodeResult<Uuid> {
        value.parse::<Uuid>().map_err(|err| {
            SinexError::processing(format!("Invalid UUID for {field}: {value} ({err})"))
        })
    }

    fn parse_offset_kind(kind: Option<&str>) -> OffsetKind {
        match kind {
            Some("line") => OffsetKind::Line,
            Some("rowid") => OffsetKind::Record,
            Some("logical") => OffsetKind::Character,
            Some("byte") | None => OffsetKind::Byte,
            Some(_) => OffsetKind::Byte,
        }
    }

    fn build_event_from_provisional(
        provisional: &ProvisionalEvent,
    ) -> NodeResult<Event<JsonValue>> {
        #[derive(Deserialize)]
        struct PublishedEventPayload {
            source: String,
            event_type: String,
            host: String,
            #[serde(rename = "payload")]
            event_payload: JsonValue,
            node_run_id: Option<String>,
            payload_schema_id: Option<String>,
            associated_blob_ids: Option<Vec<String>>,
            source_material_id: Option<String>,
            anchor_byte: Option<i64>,
            offset_start: Option<i64>,
            offset_end: Option<i64>,
            offset_kind: Option<String>,
            source_event_ids: Option<Vec<String>>,
        }

        let published: PublishedEventPayload = serde_json::from_value(provisional.payload.clone())
            .map_err(|err| {
                SinexError::processing(format!("Failed to parse provisional event payload: {err}"))
            })?;

        // Parse provenance fields for flat Event struct
        let provenance = match (published.source_material_id, published.source_event_ids) {
            (Some(material_id), None) => {
                let anchor_byte = published.anchor_byte.ok_or_else(|| {
                    SinexError::processing("Material provenance missing anchor_byte".to_string())
                })?;
                let material_uuid = Self::parse_uuid(&material_id, "source_material_id")?;
                Provenance::Material {
                    id: Id::<SourceMaterial>::from_uuid(material_uuid),
                    anchor_byte,
                    offset_start: published.offset_start,
                    offset_end: published.offset_end,
                    offset_kind: Self::parse_offset_kind(published.offset_kind.as_deref()),
                }
            }
            (None, Some(source_ids)) => {
                let mut ids = Vec::new();
                for raw_id in source_ids {
                    let source_uuid = Self::parse_uuid(&raw_id, "source_event_ids")?;
                    ids.push(EventId::from_uuid(source_uuid));
                }
                let source_event_ids = NonEmptyVec::from_vec(ids).ok_or_else(|| {
                    SinexError::processing(
                        "Synthesis provenance missing source_event_ids".to_string(),
                    )
                })?;
                Provenance::Synthesis {
                    source_event_ids,
                    operation_id: None,
                }
            }
            (Some(_), Some(_)) => {
                return Err(SinexError::processing(
                    "Provisional event contains both material and synthesis provenance".to_string(),
                ));
            }
            (None, None) => {
                return Err(SinexError::processing(
                    "Provisional event missing provenance".to_string(),
                ));
            }
        };

        let payload_schema_id = published
            .payload_schema_id
            .map(|value| Self::parse_uuid(&value, "payload_schema_id"))
            .transpose()?;
        let associated_blob_ids = match published.associated_blob_ids {
            Some(ids) => {
                let mut parsed = Vec::with_capacity(ids.len());
                for raw_id in ids {
                    parsed.push(Self::parse_uuid(&raw_id, "associated_blob_ids")?);
                }
                Some(parsed)
            }
            None => None,
        };
        let node_run_id = published
            .node_run_id
            .as_deref()
            .map(|value| Self::parse_uuid(value, "node_run_id"))
            .transpose()?;

        Ok(Event {
            id: Some(provisional.event_id),
            source: EventSource::from(published.source),
            event_type: EventType::from(published.event_type),
            payload: published.event_payload,
            ts_orig: Some(provisional.ts_orig),
            host: HostName::new(published.host).map_err(|error| {
                SinexError::processing("Invalid host in provisional event payload")
                    .with_source(error)
            })?,
            node_run_id,
            payload_schema_id,
            provenance,
            associated_blob_ids,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        })
    }

    // ── Helper methods extracted from run_automaton_event_bridge ──

    /// Resolve provisional event confirmations into full `Event` values.
    ///
    /// With `db` feature: fetches persisted events from the database when a pool
    /// is available, falling back to parsing the provisional payload directly.
    /// Without `db`: always parses from the provisional payload.
    #[cfg(feature = "messaging")]
    async fn resolve_provisionals_to_events(
        provisionals: &[ProvisionalEvent],
        #[cfg(feature = "db")] db_pool: &Option<PgPool>,
    ) -> NodeResult<ResolvedBatch> {
        let mut events = Vec::with_capacity(provisionals.len());
        let mut last_event_id = None;

        for provisional in provisionals {
            let event_id = &provisional.event_id;
            let event = {
                #[cfg(feature = "db")]
                {
                    match db_pool {
                        Some(pool) => {
                            if let Some(event) = Self::fetch_persisted_event(pool, event_id).await?
                            {
                                Some(event)
                            } else {
                                return Err(Self::confirmed_event_missing_error(event_id));
                            }
                        }
                        None => Some(
                            Self::build_event_from_provisional(provisional)
                                .map_err(|error| Self::provisional_decode_error(event_id, error))?,
                        ),
                    }
                }
                #[cfg(not(feature = "db"))]
                {
                    Some(
                        Self::build_event_from_provisional(provisional)
                            .map_err(|error| Self::provisional_decode_error(event_id, error))?,
                    )
                }
            };

            if let Some(event) = event {
                last_event_id = Some(*event_id.as_uuid());
                events.push(event);
            }
        }

        Ok(ResolvedBatch {
            events,
            last_event_id,
        })
    }

    #[cfg(feature = "messaging")]
    fn confirmed_event_missing_error(event_id: &EventId) -> SinexError {
        SinexError::processing("Confirmed event missing from database")
            .with_context("event_id", event_id.to_string())
    }

    #[cfg(feature = "messaging")]
    fn provisional_decode_error(event_id: &EventId, error: SinexError) -> SinexError {
        SinexError::processing(
            "Confirmed event could not be reconstructed from provisional payload",
        )
        .with_context("event_id", event_id.to_string())
        .with_source(error)
    }

    /// Process a batch of events, falling back to per-event processing with DLQ
    /// routing if the batch fails. Returns the total number of events processed
    /// (including those routed to the DLQ).
    #[cfg(feature = "messaging")]
    async fn process_batch_with_dlq_fallback(
        node: &mut T,
        transport: &EventTransport,
        events: Vec<Event<JsonValue>>,
    ) -> NodeResult<u64> {
        let batch_size = events.len();
        let events_backup = events.clone();

        match node.process_event_batch(events).await {
            Ok(stats) => {
                if batch_size > 1 {
                    debug!(
                        batch_size,
                        processed = stats.processed,
                        "Processed event batch"
                    );
                }
                Ok(stats.processed as u64)
            }
            Err(batch_err) => {
                // Fatal errors (NodeFatal, TransportDegraded) apply to the
                // entire node, not to any one event. Per-event DLQ fallback
                // would route every event in the batch — and every subsequent
                // batch — to DLQ while the node keeps consuming, generating
                // an unbounded log/IO storm. Issue #581 observed 221K
                // consecutive failures producing 1.6M journal entries and
                // 54 GB of NATS traffic on sinnix-prime before I/O saturation
                // halted the host.
                //
                // Use the new error_class() classification instead of
                // hardcoding individual variants. Checkpoint, Lifecycle,
                // Configuration, PermissionDenied, and live-context
                // ChannelSend are all NodeFatal.
                let error_class = batch_err.error_class();
                if error_class.is_fatal() {
                    error!(
                        error = %batch_err,
                        ?error_class,
                        batch_size,
                        "Fatal error in batch processing; halting node (per-event DLQ fallback would loop on every event)"
                    );
                    return Err(batch_err);
                }
                warn!(
                    error = %batch_err,
                    ?error_class,
                    batch_size,
                    "Batch processing failed; falling back to per-event processing with DLQ routing"
                );
                let node_name = node.node_name().to_string();
                let mut succeeded = 0u64;
                for event in events_backup {
                    match node.process_event_batch(vec![event.clone()]).await {
                        Ok(stats) => {
                            succeeded += stats.processed as u64;
                        }
                        Err(event_err) => {
                            // Same defense as the batch path — fatal errors
                            // are not data errors. Halt immediately.
                            if event_err.error_class().is_fatal() {
                                error!(
                                    error = %event_err,
                                    "Checkpoint error during per-event fallback; halting node"
                                );
                                return Err(event_err);
                            }
                            let event_id = event.id;
                            warn!(
                                error = %event_err,
                                ?event_id,
                                "Event processing failed; routing to DLQ"
                            );
                            if let Err(dlq_err) = transport
                                .send_to_processing_failure_queue(
                                    &event,
                                    &event_err.to_string(),
                                    &node_name,
                                )
                                .await
                            {
                                return Err(SinexError::processing(
                                    "failed to route failed automaton event to processing-failure stream",
                                )
                                .with_context("node", node_name.clone())
                                .with_context(
                                    "event_id",
                                    event_id.as_ref().map_or_else(
                                        || "missing".to_string(),
                                        std::string::ToString::to_string,
                                    ),
                                )
                                .with_context("source", event.source.as_str().to_string())
                                .with_context("event_type", event.event_type.as_str().to_string())
                                .with_context("processing_error", event_err.to_string())
                                .with_source(dlq_err));
                            }
                        }
                    }
                }
                let dlq_count = batch_size as u64 - succeeded;
                info!(succeeded, dlq_count, "Per-event fallback complete");
                // Count DLQ'd events as processed for checkpoint advancement
                Ok(batch_size as u64)
            }
        }
    }

    /// Save a checkpoint if `last_event_id` is `Some`. Returns the new revision
    /// on success, or `None` if there was nothing to save or the save failed.
    ///
    /// Tracks consecutive failures in `consecutive_failures`. Resets to 0 on success.
    /// Returns a hard error after 3 consecutive failures to prevent silent progress loss
    /// on crash+restart (which would cause duplicate event processing).
    #[cfg(feature = "messaging")]
    async fn try_save_checkpoint(
        checkpoint_manager: &CheckpointManager,
        checkpoint_state: &mut crate::checkpoint::CheckpointState,
        last_event_id: Option<Uuid>,
        processed_events: u64,
        consecutive_failures: &mut u32,
    ) -> NodeResult<Option<u64>> {
        let Some(eid) = last_event_id else {
            return Ok(None);
        };
        checkpoint_state.checkpoint = Checkpoint::Internal {
            event_id: eid,
            message_count: processed_events,
        };
        checkpoint_state.processed_count = processed_events;
        checkpoint_state.last_activity = sinex_primitives::temporal::Timestamp::now();
        match checkpoint_manager.save_checkpoint(checkpoint_state).await {
            Ok(revision) => {
                *consecutive_failures = 0;
                debug!(processed_events, revision, "Checkpoint saved");
                Ok(Some(revision))
            }
            Err(err) => {
                *consecutive_failures += 1;
                error!(
                    error = %err,
                    consecutive_failures = *consecutive_failures,
                    "Failed to save checkpoint; will retry next interval"
                );
                if *consecutive_failures >= 3 {
                    return Err(SinexError::checkpoint(format!(
                        "Checkpoint save failed {} consecutive times; halting to prevent \
                         silent progress loss on crash+restart",
                        *consecutive_failures
                    )));
                }
                Ok(None)
            }
        }
    }

    /// Get node capabilities
    pub fn get_capabilities(&self) -> NodeCapabilities {
        self.node.capabilities()
    }

    /// Get scan estimate
    pub async fn estimate_scan_scope(
        &self,
        from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
        self.node.estimate_scan_scope(from, until, args).await
    }

    /// Graceful shutdown.
    ///
    /// Idempotent: safe to call multiple times or on a never-initialized runner.
    pub async fn shutdown(&mut self) -> NodeResult<()> {
        if matches!(self.lifecycle, RunnerLifecycle::ShutDown) {
            debug!("shutdown() called on already shut-down runner; no-op");
            return Ok(());
        }
        if matches!(self.lifecycle, RunnerLifecycle::Created) {
            debug!("shutdown() called on never-initialized runner; no-op");
            self.lifecycle = RunnerLifecycle::ShutDown;
            return Ok(());
        }

        info!("Shutting down stream node runner");

        let mut shutdown_errors = Vec::new();
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "schema broadcast listener",
            Self::shutdown_task(
                &mut self.schema_listener_handle,
                self.schema_listener_shutdown.take(),
                "schema broadcast listener",
            )
            .await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "command listener",
            Self::shutdown_task(
                &mut self.command_listener_handle,
                self.command_listener_shutdown.take(),
                "command listener",
            )
            .await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "coordination",
            self.shutdown_leader_state().await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "automaton consumer",
            Self::shutdown_task(&mut self.consumer_handle, None, "automaton consumer").await,
        );
        // Save checkpoint BEFORE draining the event batcher. This ensures the
        // checkpoint reflects the last fully-processed position. Events still in
        // the batcher channel will be published during drain but are "ahead" of
        // the checkpoint — on restart they'll be re-processed (at-least-once).
        // The previous order (batcher first, then checkpoint) could silently drop
        // events if the batcher's 250ms grace period expired mid-flush.
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "node shutdown",
            self.node.shutdown().await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "event batcher",
            self.shutdown_event_batcher().await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "checkpoint cleanup",
            Self::shutdown_task(
                &mut self.checkpoint_cleanup_handle,
                self.checkpoint_cleanup_shutdown.take(),
                "checkpoint cleanup",
            )
            .await,
        );

        match Self::collapse_shutdown_errors(shutdown_errors) {
            Ok(()) => {
                self.lifecycle = RunnerLifecycle::ShutDown;
                Ok(())
            }
            Err(error) => {
                self.lifecycle = RunnerLifecycle::ShutdownFailed;
                Err(error)
            }
        }
    }

    async fn shutdown_task(
        handle: &mut Option<tokio::task::JoinHandle<()>>,
        shutdown_tx: Option<watch::Sender<bool>>,
        name: &str,
    ) -> NodeResult<()> {
        if let Some(shutdown_tx) = shutdown_tx {
            Self::signal_watch_shutdown(shutdown_tx, name);
        }
        if let Some(mut h) = handle.take() {
            if let Ok(result) = tokio::time::timeout(TASK_SHUTDOWN_GRACE_PERIOD, &mut h).await {
                Self::shutdown_join_result(name, result)
            } else {
                debug!(
                    task = name,
                    grace_period_ms = TASK_SHUTDOWN_GRACE_PERIOD.as_millis(),
                    "Task did not exit within shutdown grace period; aborting"
                );
                h.abort();
                Self::shutdown_join_result(name, h.await)
            }
        } else {
            Ok(())
        }
    }

    async fn shutdown_leader_state(&mut self) -> NodeResult<()> {
        if let Some(state) = self.leader_state.take() {
            let mut shutdown_errors = Vec::new();
            Self::signal_shutdown_channel(state.heartbeat_shutdown, "coordination heartbeat");
            Self::push_shutdown_error(
                &mut shutdown_errors,
                "coordination heartbeat",
                Self::shutdown_join_result("coordination heartbeat", state.heartbeat_handle.await),
            );
            Self::push_shutdown_error(
                &mut shutdown_errors,
                "coordination leadership release",
                Self::leadership_release_result(
                    &state.instance_id,
                    state.kv_client.release_leadership(&state.instance_id).await,
                ),
            );
            Self::collapse_shutdown_errors(shutdown_errors)
        } else {
            Ok(())
        }
    }

    async fn shutdown_event_batcher(&mut self) -> NodeResult<()> {
        if let Some(shutdown_tx) = self.event_batcher_shutdown.take() {
            Self::signal_shutdown_channel(shutdown_tx, "event batcher");
        }
        if let Some(handle) = self.event_batcher_handle.take() {
            Self::event_batcher_shutdown_result(handle.await)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests;
