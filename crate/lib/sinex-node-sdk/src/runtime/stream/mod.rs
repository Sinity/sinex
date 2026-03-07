#![doc = include_str!("../../../docs/stream_node.md")]

mod checkpoint;
mod handles;
mod kernel;
mod runtime_state;
mod stats;
mod time_horizon;

pub use checkpoint::Checkpoint;
pub use handles::{
    EventEmitter, EventSender, EventStream, NodeHandles, NodeInitContext, ServiceInfo,
};
#[cfg(feature = "db")]
pub use kernel::replay_source_window;
pub use kernel::{
    PullConsumerSpec, ReplayPumpConfig, ReplayPumpProgress, ShadowConsumerSpec,
    build_replay_publish_envelope, consume_pull_loop, create_shadow_consumer, delete_consumer,
    ensure_pull_consumer, list_consumers, publish_replay_event, pull_batch,
    validate_pull_consumer_config,
};
pub use runtime_state::NodeRuntimeState;
pub use stats::ProcessingStats;
pub use time_horizon::TimeHorizon;

use crate::{
    NodeResult, SinexError,
    checkpoint::CheckpointManager,
    confirmation_handler::{ConfirmedEventHandler, ProcessingModel, ProvisionalEvent},
    event_node::{EventBatcherConfig, EventTransport, spawn_event_batcher},
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
};
use async_nats::jetstream::kv;
use async_trait::async_trait;
use camino::Utf8PathBuf;

use serde::{Deserialize, Serialize};
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
use sinex_db::models::SourceMaterial;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::{EventId, Provenance};
const DEFAULT_EVENT_CHANNEL_SIZE: usize = 1024;
use sinex_primitives::{
    EventSource, EventType, HostName, Id, JsonValue, OffsetKind, Uuid, non_empty::NonEmptyVec,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug, Default)]
pub struct SchemaBroadcastCache {
    schemas: Arc<RwLock<Vec<SchemaBroadcastEntry>>>,
}

impl SchemaBroadcastCache {
    pub async fn update(&self, entries: Vec<SchemaBroadcastEntry>) {
        let mut guard = self.schemas.write().await;
        *guard = entries;
    }

    pub async fn get(&self) -> Vec<SchemaBroadcastEntry> {
        self.schemas.read().await.clone()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaBroadcastEntry {
    pub name: String,
    pub version: String,
    pub schema_id: String,
}
const CONFIRMED_EVENT_CHANNEL_CAPACITY: usize = 1024;

struct RunnerConfirmedEventHandler {
    sender: mpsc::Sender<ProvisionalEvent>,
}

impl RunnerConfirmedEventHandler {
    fn new(sender: mpsc::Sender<ProvisionalEvent>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl ConfirmedEventHandler for RunnerConfirmedEventHandler {
    async fn handle_confirmed(&self, event: &ProvisionalEvent) -> NodeResult<()> {
        self.sender.send(event.clone()).await.map_err(|_| {
            // Channel closed = receiver dropped = shutdown in progress.
            // Return a shutdown-specific error so callers can distinguish
            // normal shutdown from unexpected processing failures.
            SinexError::lifecycle(
                "Confirmed event channel closed (node is shutting down)".to_string(),
            )
        })
    }
}

/// Scan operation arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanArgs {
    /// Paths to scan (for ingestors) or filters (for automata)
    pub targets: Vec<String>,

    /// Dry run mode - analyze but don't emit events
    pub dry_run: bool,

    /// Interactive mode - prompt user for decisions
    pub interactive: bool,

    /// Maximum events to process (0 = unlimited)
    pub max_events: u64,

    /// Skip duplicate detection
    pub skip_duplicates: bool,

    /// Node-specific configuration
    pub config: HashMap<String, serde_json::Value>,
}

impl Default for ScanArgs {
    fn default() -> Self {
        Self {
            targets: Vec::new(),
            dry_run: false,
            interactive: false,
            max_events: 0,
            skip_duplicates: true,
            config: HashMap::new(),
        }
    }
}

async fn create_checkpoint_kv(transport: &EventTransport) -> NodeResult<kv::Store> {
    // NATS KV is now mandatory
    let client = match transport {
        EventTransport::Nats(publisher) => publisher.nats_client().clone(),
    };

    let js = async_nats::jetstream::new(client);
    let env = sinex_primitives::environment::environment();
    // nats_kv_bucket_name() returns base_name (e.g. "dev_sinex_checkpoints")
    // We need to prepend "KV_" prefix for NATS bucket naming
    let bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_checkpoints"));
    let kv_store = match js
        .create_key_value(kv::Config {
            bucket: bucket.clone(),
            ..Default::default()
        })
        .await
    {
        Ok(store) => store,
        Err(create_err) => js.get_key_value(&bucket).await.map_err(|e| {
            SinexError::lifecycle(format!(
                "Failed to create/open checkpoint KV bucket (create: {create_err}, open: {e})"
            ))
        })?,
    };

    Ok(kv_store)
}

async fn maybe_start_schema_listener(
    transport: &EventTransport,
) -> NodeResult<(
    Option<Arc<SchemaBroadcastCache>>,
    Option<Arc<crate::schema_validator::NodeSchemaValidator>>,
    Option<tokio::task::JoinHandle<()>>,
)> {
    // Enable schema cache and validation when infrastructure is available.
    // Schemas are broadcast from ingestd and stored in NATS KV.
    // In edge mode (without full infrastructure), gracefully skip schema validation.

    let client = match transport {
        EventTransport::Nats(publisher) => publisher.nats_client().clone(),
    };
    let env = sinex_primitives::environment::environment();
    let subject = env.nats_subject("system.schemas.active");
    let sub = match client.subscribe(subject.clone()).await {
        Ok(sub) => sub,
        Err(e) => {
            debug!("Schema broadcast subscription unavailable (edge mode): {e}");
            return Ok((None, None, None));
        }
    };
    let mut sub = sub;

    // Get KV bucket for fetching full schemas - if unavailable, skip schema validation
    let js = async_nats::jetstream::new(client);
    let env = sinex_primitives::environment::environment();
    let schema_bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_schemas"));
    let kv = match js.get_key_value(&schema_bucket).await {
        Ok(kv) => kv,
        Err(e) => {
            debug!("Schema KV bucket unavailable (edge mode): {e}");
            return Ok((None, None, None));
        }
    };

    // Create schema cache and validator
    let cache = Arc::new(SchemaBroadcastCache::default());
    let cache_clone = cache.clone();
    let validator = Arc::new(crate::schema_validator::NodeSchemaValidator::new());
    let validator_clone = validator.clone();

    // Background task to update cache and validator
    let handle = tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            match serde_json::from_slice::<Vec<SchemaBroadcastEntry>>(&msg.payload) {
                Ok(entries) => {
                    // Update metadata cache
                    cache_clone.update(entries.clone()).await;

                    // Update validator with full schemas from KV
                    match validator_clone.update_from_broadcast(entries, &kv).await {
                        Ok(count) => {
                            debug!(count, "Updated schema validator from broadcast");
                        }
                        Err(err) => {
                            warn!(error = %err, "Failed to update schema validator");
                        }
                    }
                }
                Err(err) => {
                    warn!(error = %err, "Failed to decode schema broadcast payload");
                }
            }
        }
        debug!("Schema broadcast listener task ended");
    });

    info!("Started schema broadcast listener and validator for {subject}");

    Ok((Some(cache), Some(validator), Some(handle)))
}

/// Report from a completed scan operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanReport {
    /// Total events processed/generated
    pub events_processed: u64,

    /// Duration of the scan operation
    pub duration: std::time::Duration,

    /// Final checkpoint after scan
    pub final_checkpoint: Checkpoint,

    /// Time range covered by the scan
    pub time_range: Option<(
        sinex_primitives::temporal::Timestamp,
        sinex_primitives::temporal::Timestamp,
    )>,

    /// Node-specific statistics
    pub node_stats: HashMap<String, u64>,

    /// Targets that were successfully processed
    pub successful_targets: Vec<String>,

    /// Targets that failed processing with error messages
    pub failed_targets: Vec<(String, String)>,

    /// Warnings encountered during processing
    pub warnings: Vec<String>,
}

/// Unified trait for all stream nodes (ingestors and automata).
pub trait Node: Send + Sync {
    type Config: for<'de> Deserialize<'de> + Default + Send + Sync;

    fn initialize(
        &mut self,
        init: NodeInitContext<Self::Config>,
    ) -> impl std::future::Future<Output = NodeResult<()>> + Send;

    fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> impl std::future::Future<Output = NodeResult<ScanReport>> + Send;

    fn node_name(&self) -> &str;
    fn node_type(&self) -> NodeType;

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    fn current_checkpoint(
        &self,
    ) -> impl std::future::Future<Output = NodeResult<Checkpoint>> + Send;

    fn health_check(&self) -> impl std::future::Future<Output = NodeResult<bool>> + Send {
        async { Ok(true) }
    }

    fn process_event_batch(
        &mut self,
        _events: Vec<Event<JsonValue>>,
    ) -> impl std::future::Future<Output = NodeResult<ProcessingStats>> + Send {
        async {
            Err(SinexError::processing(
                "This node does not support event batch processing. Only automata should implement this method.".to_string()
            ))
        }
    }

    fn shutdown(&mut self) -> impl std::future::Future<Output = NodeResult<()>> + Send {
        async {
            info!(node = %self.node_name(), "Node shutting down");
            Ok(())
        }
    }

    fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> impl std::future::Future<Output = NodeResult<ScanEstimate>> + Send {
        async { Ok(ScanEstimate::default()) }
    }

    fn config_schema(&self) -> Option<serde_json::Value> {
        None
    }
}

/// Type of stream node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    /// Ingestor: External World -> Event Stream
    Ingestor,
    /// Automaton: Event Stream -> `DerivedEvent` Stream
    Automaton,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ingestor => write!(f, "ingestor"),
            Self::Automaton => write!(f, "automaton"),
        }
    }
}

/// Node capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Supports continuous scanning (sensor mode)
    pub supports_continuous: bool,

    /// Supports historical scanning
    pub supports_historical: bool,

    /// Supports snapshot scanning
    pub supports_snapshot: bool,

    /// Supports interactive mode
    pub supports_interactive: bool,

    /// Maximum recommended scan size
    pub max_scan_size: Option<u64>,

    /// Supports concurrent processing
    pub supports_concurrent: bool,

    /// Node manages its own continuous loop (runner skips `JetStream` bridge)
    pub manages_own_continuous_loop: bool,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            supports_continuous: true,
            supports_historical: false,
            supports_snapshot: false,
            supports_interactive: false,
            max_scan_size: None,
            supports_concurrent: false,
            manages_own_continuous_loop: false,
        }
    }
}

/// Scan operation estimate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanEstimate {
    /// Estimated number of events to be processed
    pub estimated_events: u64,

    /// Estimated processing duration
    pub estimated_duration: std::time::Duration,

    /// Estimated data size to be processed
    pub estimated_data_size: u64,

    /// Number of targets that will be processed
    pub estimated_targets: u64,

    /// Warnings about potential issues
    pub warnings: Vec<String>,

    /// Confidence level of estimate (0.0 to 1.0)
    pub confidence: f32,
}

impl Default for ScanEstimate {
    fn default() -> Self {
        Self {
            estimated_events: 0,
            estimated_duration: std::time::Duration::from_secs(0),
            estimated_data_size: 0,
            estimated_targets: 0,
            warnings: Vec::new(),
            confidence: 0.5,
        }
    }
}

/// Lifecycle state of a [`NodeRunner`].
///
/// Guards against re-entrant calls to `initialize`, `run_service`/`run_scan`,
/// and `shutdown`. State transitions are strictly forward-only:
///
/// ```text
/// Created ──► Initializing ──► Initialized ──► Running ──► ShutDown
///                                                  │
///                                                  └──► ShutDown (via shutdown())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerLifecycle {
    /// Freshly constructed, not yet initialized.
    Created,
    /// `initialize_with_transport` is executing.
    Initializing,
    /// Initialization complete; ready for `run_service` / `run_scan`.
    Initialized,
    /// `run_service` or `run_scan` is executing.
    Running,
    /// `shutdown` has completed (or was never initialized).
    ShutDown,
}

impl std::fmt::Display for RunnerLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Initializing => write!(f, "Initializing"),
            Self::Initialized => write!(f, "Initialized"),
            Self::Running => write!(f, "Running"),
            Self::ShutDown => write!(f, "ShutDown"),
        }
    }
}

/// Unified runner for nodes
pub struct NodeRunner<T: Node> {
    node: T,
    lifecycle: RunnerLifecycle,
    handles: Option<NodeHandles>,
    service_info: Option<ServiceInfo>,
    raw_config: Option<HashMap<String, serde_json::Value>>,
    work_dir_utf8: Option<Utf8PathBuf>,
    event_batcher_handle: Option<tokio::task::JoinHandle<NodeResult<()>>>,
    event_batcher_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    schema_listener_handle: Option<tokio::task::JoinHandle<()>>,
    checkpoint_cleanup_handle: Option<tokio::task::JoinHandle<()>>,
    consumer_handle: Option<tokio::task::JoinHandle<()>>,
    processing_model: ProcessingModel,
    leader_state: Option<LeaderState>,
}

struct LeaderState {
    kv_client: sinex_primitives::coordination::CoordinationKvClient,
    instance_id: String,
    heartbeat_handle: tokio::task::JoinHandle<()>,
}

/// Batch of events resolved from provisional confirmations.
#[cfg(feature = "messaging")]
struct ResolvedBatch {
    events: Vec<Event<JsonValue>>,
    last_event_id: Option<Uuid>,
}

impl<T: Node + 'static> NodeRunner<T> {
    /// Create a new node runner
    pub fn new(node: T) -> Self {
        Self {
            node,
            lifecycle: RunnerLifecycle::Created,
            handles: None,
            service_info: None,
            raw_config: None,
            work_dir_utf8: None,
            event_batcher_handle: None,
            event_batcher_shutdown: None,
            schema_listener_handle: None,
            checkpoint_cleanup_handle: None,
            consumer_handle: None,
            processing_model: ProcessingModel::StatelessWorker,
            leader_state: None,
        }
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
            RunnerLifecycle::Initialized | RunnerLifecycle::Running | RunnerLifecycle::ShutDown => {
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
        let host = gethostname::gethostname().to_string_lossy().to_string();
        let consumer_name = format!("{host}-{}", std::process::id());
        let transport_for_context = transport.clone();
        let transport_clone_for_runner = transport.clone();

        let kv_store = create_checkpoint_kv(&transport).await?;

        #[cfg(feature = "messaging")]
        let (schema_cache, schema_validator, schema_listener_handle) =
            maybe_start_schema_listener(&transport).await?;
        #[cfg(not(feature = "messaging"))]
        let (schema_cache, schema_validator, schema_listener_handle) = (
            Option::<Arc<crate::runtime::stream::SchemaBroadcastCache>>::None,
            Option::<()>::None,
            Option::<tokio::task::JoinHandle<()>>::None,
        );
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
                let cleanup_handle = crate::checkpoint::spawn_checkpoint_cleanup_task(
                    kv_for_cleanup,
                    cleanup_config,
                );
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

        // Initialize checkpoint manager with KV
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv_store,
            service_name.clone(),
            consumer_group,
            consumer_name.clone(),
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

        let event_emitter = {
            #[cfg(feature = "messaging")]
            if let Some(validator) = schema_validator {
                EventEmitter::with_validator(event_sender_raw.clone(), dry_run, validator)
            } else {
                EventEmitter::new(event_sender_raw, dry_run)
            }

            #[cfg(not(feature = "messaging"))]
            EventEmitter::new(event_sender_raw, dry_run)
        };

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

        let service_info = ServiceInfo::new(
            service_name.clone(),
            host.clone(),
            work_dir.clone(),
            dry_run,
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
            self.lifecycle = RunnerLifecycle::Created;
            return Err(e);
        }

        self.handles = Some(handles);
        self.service_info = Some(service_info);
        self.raw_config = Some(raw_config.clone());
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
        ));

        self.lifecycle = RunnerLifecycle::Initialized;

        info!(
            service = %service_name,
            node = %self.node.node_name(),
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
    pub async fn run_service(&mut self) -> NodeResult<()> {
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

        match node_type {
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
        }
    }

    /// Run ingestor startup sequence (Snapshot -> Gap-fill -> Continuous)
    async fn run_ingestor_startup_sequence(&mut self) -> NodeResult<()> {
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
        }

        // Phase 2: Gap-filling (if supported and needed)
        if self.node.capabilities().supports_historical {
            let current_checkpoint = self.node.current_checkpoint().await?;

            // Only gap-fill if we have a previous checkpoint
            if !matches!(current_checkpoint, Checkpoint::None) {
                info!("Phase 2: Gap-filling from last checkpoint");
                let gap_fill_report = self
                    .node
                    .scan(
                        current_checkpoint,
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
        }

        // Phase 3: Continuous processing (traditional scan method)
        if self.node.capabilities().supports_continuous {
            info!("Phase 3: Starting continuous processing");
            let current_checkpoint = self.node.current_checkpoint().await?;

            // This should run indefinitely until shutdown
            let continuous_report = self
                .node
                .scan(
                    current_checkpoint,
                    TimeHorizon::Continuous,
                    ScanArgs::default(),
                )
                .await?;

            // If continuous scan returns, it means it exited unexpectedly.
            // Log so operators can investigate (M4: silent exit prevention).
            warn!(
                events_processed = continuous_report.events_processed,
                "Continuous scan returned unexpectedly - service will exit. \
                 This may indicate the scan implementation does not block indefinitely."
            );
        } else {
            warn!("Node does not support continuous mode - service will exit");
        }

        Ok(())
    }

    /// Run automaton in continuous mode
    #[cfg(feature = "messaging")]
    async fn run_automaton_continuous_mode(&mut self) -> NodeResult<()> {
        info!("Starting automaton continuous mode");

        // Get current checkpoint to resume from previous state if available
        let current_checkpoint = self.node.current_checkpoint().await?;
        let capabilities = self.node.capabilities();

        if capabilities.supports_continuous {
            info!("Starting continuous event processing for automaton");

            // Acquire leadership if running in LeaderStandby mode
            if self.processing_model == ProcessingModel::LeaderStandby
                && !self.acquire_leader_standby().await?
            {
                return Ok(());
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

            info!("Automaton continuous processing completed");
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
    /// Returns `true` if this instance is the leader and should proceed.
    async fn acquire_leader_standby(&mut self) -> NodeResult<bool> {
        let rs = self
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state missing".to_string()))?;

        #[cfg(feature = "messaging")]
        {
            let nc = rs
                .nats_client()
                .ok_or_else(|| SinexError::lifecycle("NATS client missing".to_string()))?;
            let service = rs.service_info().service_name().to_string();
            let host = rs.service_info().host().to_string();
            let pid = std::process::id();
            let instance_id = format!("{host}-{pid}");

            let js = async_nats::jetstream::new(nc);
            let kv_client =
                sinex_primitives::coordination::CoordinationKvClient::new(js, service.clone());

            let is_leader = kv_client
                .acquire_leadership(&instance_id)
                .await
                .map_err(|e| {
                    SinexError::processing(format!("Failed to acquire leadership: {e}"))
                })?;

            if !is_leader {
                info!("Not leader, skipping processing");
                return Ok(false);
            }

            info!("Confirmed as leader, proceeding with processing");

            // Spawn a simplified heartbeater (3s interval with default 10s TTL
            // gives 7s margin for network delays)
            let kv_clone = kv_client.clone();
            let instance_id_clone = instance_id.clone();
            let heartbeat_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
                loop {
                    interval.tick().await;
                    if let Err(e) = kv_clone.acquire_leadership(&instance_id_clone).await {
                        warn!("Heartbeat failed: {e}");
                    }
                }
            });

            self.leader_state = Some(LeaderState {
                kv_client,
                instance_id,
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

        #[cfg(feature = "db")]
        let db_pool = handles.db_pool().cloned();
        // No db_pool variable if db feature is off
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
            consumer_name: format!("{}-automaton", service_name.replace('.', "_")),
            enable_provisional_processing: false,
            ..Default::default()
        };

        let consumer = Arc::new(JetStreamEventConsumer::new(
            nats_client,
            env,
            consumer_config,
            handler,
            None,
        ));

        let consumer_runner = consumer.clone();
        let consumer_handle = tokio::spawn(async move {
            if let Err(err) = consumer_runner.run().await {
                warn!(error = %err, "Automaton JetStream consumer terminated unexpectedly");
            }
        });
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

        // Periodic checkpoint saves: prevent data loss on crash by persisting
        // progress every CHECKPOINT_EVENT_INTERVAL events or CHECKPOINT_TIME_INTERVAL.
        const CHECKPOINT_EVENT_INTERVAL: u64 = 100;
        const CHECKPOINT_TIME_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

        let checkpoint_manager = handles.checkpoint_manager();
        let mut checkpoint_state = checkpoint_manager.load_checkpoint().await.unwrap_or_else(|e| {
            warn!(error = %e, "Failed to load checkpoint state for periodic saves; starting fresh");
            crate::checkpoint::CheckpointState::default()
        });

        let mut processed_events = 0u64;
        let mut events_since_checkpoint = 0u64;
        let mut last_checkpoint_time = std::time::Instant::now();
        let mut last_event_id: Option<Uuid> = None;

        // Batch processing: accumulate up to BATCH_SIZE events before processing.
        // Block on the first event, then non-blocking drain whatever else is queued.
        const BATCH_SIZE: usize = 100;

        loop {
            // Block until at least one event arrives (or channel closes)
            let Some(first) = receiver.recv().await else {
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
            .await;

            processed_events += batch_count;
            events_since_checkpoint += batch_count;
            if let Some(eid) = resolve_result.last_event_id {
                last_event_id = Some(eid);
            }

            // Periodic checkpoint save: every N events or M seconds
            if (events_since_checkpoint >= CHECKPOINT_EVENT_INTERVAL
                || last_checkpoint_time.elapsed() >= CHECKPOINT_TIME_INTERVAL)
                && let Some(revision) = Self::try_save_checkpoint(
                    &checkpoint_manager,
                    &mut checkpoint_state,
                    last_event_id,
                    processed_events,
                )
                .await
            {
                checkpoint_state.revision = revision;
                events_since_checkpoint = 0;
                last_checkpoint_time = std::time::Instant::now();
            }
        }

        // Save final checkpoint on clean exit
        if Self::try_save_checkpoint(
            &checkpoint_manager,
            &mut checkpoint_state,
            last_event_id,
            processed_events,
        )
        .await
        .is_some()
        {
            info!(processed_events, "Final checkpoint saved on clean shutdown");
        }

        info!(
            processed_events,
            "JetStream confirmed event channel closed; stopping automaton bridge"
        );

        consumer.stop().await;

        if let Some(handle) = self.consumer_handle.take()
            && let Err(err) = handle.await
        {
            warn!(error = %err, "Failed to join automaton consumer task");
        }

        Ok(())
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
            node_version: Option<String>,
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

        Ok(Event {
            id: Some(provisional.event_id),
            source: EventSource::from(published.source),
            event_type: EventType::from(published.event_type),
            payload: published.event_payload,
            ts_orig: Some(provisional.ts_orig),
            host: HostName::from(published.host),
            node_version: published.node_version,
            payload_schema_id,
            provenance,
            associated_blob_ids,
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
                                warn!(
                                    "Confirmed event {:?} missing from database; skipping",
                                    event_id
                                );
                                None
                            }
                        }
                        None => match Self::build_event_from_provisional(provisional) {
                            Ok(event) => Some(event),
                            Err(err) => {
                                warn!(error = %err, "Failed to build event from provisional payload");
                                None
                            }
                        },
                    }
                }
                #[cfg(not(feature = "db"))]
                {
                    match Self::build_event_from_provisional(provisional) {
                        Ok(event) => Some(event),
                        Err(err) => {
                            warn!(error = %err, "Failed to build event from provisional payload");
                            None
                        }
                    }
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

    /// Process a batch of events, falling back to per-event processing with DLQ
    /// routing if the batch fails. Returns the total number of events processed
    /// (including those routed to the DLQ).
    #[cfg(feature = "messaging")]
    async fn process_batch_with_dlq_fallback(
        node: &mut T,
        transport: &EventTransport,
        events: Vec<Event<JsonValue>>,
    ) -> u64 {
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
                stats.processed as u64
            }
            Err(batch_err) => {
                warn!(
                    error = %batch_err,
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
                            let event_id = event.id;
                            warn!(
                                error = %event_err,
                                ?event_id,
                                "Event processing failed; routing to DLQ"
                            );
                            if let Err(dlq_err) = transport
                                .send_to_dlq(&event, &event_err.to_string(), &node_name)
                                .await
                            {
                                error!(
                                    error = %event_err,
                                    dlq_error = %dlq_err,
                                    ?event_id,
                                    "Failed to route event to DLQ"
                                );
                            }
                        }
                    }
                }
                let dlq_count = batch_size as u64 - succeeded;
                info!(succeeded, dlq_count, "Per-event fallback complete");
                // Count DLQ'd events as processed for checkpoint advancement
                batch_size as u64
            }
        }
    }

    /// Save a checkpoint if `last_event_id` is `Some`. Returns the new revision
    /// on success, or `None` if there was nothing to save or the save failed.
    #[cfg(feature = "messaging")]
    async fn try_save_checkpoint(
        checkpoint_manager: &CheckpointManager,
        checkpoint_state: &mut crate::checkpoint::CheckpointState,
        last_event_id: Option<Uuid>,
        processed_events: u64,
    ) -> Option<u64> {
        let eid = last_event_id?;
        checkpoint_state.checkpoint = Checkpoint::Internal {
            event_id: eid,
            message_count: processed_events,
        };
        checkpoint_state.processed_count = processed_events;
        checkpoint_state.last_activity = sinex_primitives::temporal::Timestamp::now();
        match checkpoint_manager.save_checkpoint(checkpoint_state).await {
            Ok(revision) => {
                debug!(processed_events, revision, "Checkpoint saved");
                Some(revision)
            }
            Err(err) => {
                warn!(error = %err, "Failed to save checkpoint; will retry next interval");
                None
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
        self.lifecycle = RunnerLifecycle::ShutDown;

        info!("Shutting down stream node runner");

        Self::abort_task(
            &mut self.schema_listener_handle,
            "schema broadcast listener",
        )
        .await;
        self.shutdown_leader_state().await;
        self.shutdown_event_batcher().await;
        Self::abort_task(&mut self.consumer_handle, "automaton consumer").await;
        Self::abort_task(&mut self.checkpoint_cleanup_handle, "checkpoint cleanup").await;

        self.node.shutdown().await
    }

    async fn abort_task(handle: &mut Option<tokio::task::JoinHandle<()>>, name: &str) {
        if let Some(h) = handle.take() {
            h.abort();
            let _ = h.await;
            debug!("Aborted {name}");
        }
    }

    async fn shutdown_leader_state(&mut self) {
        if let Some(state) = self.leader_state.take() {
            state.heartbeat_handle.abort();
            let _ = state.heartbeat_handle.await;
            if let Err(err) = state.kv_client.release_leadership(&state.instance_id).await {
                warn!(error = %err, "Failed to release leadership on shutdown");
            }
        }
    }

    async fn shutdown_event_batcher(&mut self) {
        if let Some(shutdown_tx) = self.event_batcher_shutdown.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.event_batcher_handle.take() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => error!(error = %err, "Event batcher failed during shutdown"),
                Err(join_err) => error!(error = %join_err, "Failed to join event batcher task"),
            }
        }
    }
}
