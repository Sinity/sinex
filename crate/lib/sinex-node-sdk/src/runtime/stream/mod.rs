#![doc = include_str!("../../../docs/stream_processor.md")]

mod checkpoint;
mod handles;
mod runtime_state;
mod stats;
mod time_horizon;

pub use checkpoint::Checkpoint;
pub use handles::{
    EventEmitter, EventSender, EventStream, NodeHandles, NodeInitContext, ServiceInfo,
};
pub use runtime_state::NodeRuntimeState;
pub use stats::ProcessingStats;
pub use time_horizon::TimeHorizon;

use crate::{
    checkpoint::CheckpointManager,
    confirmation_handler::{ConfirmedEventHandler, ProcessingModel, ProvisionalEvent},
    event_node::{spawn_event_processor, EventBatcherConfig, EventTransport},
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    NodeResult, SinexError,
};
use async_nats::jetstream::kv;
use async_trait::async_trait;
use camino::Utf8PathBuf;

use serde::{Deserialize, Serialize};
use sinex_db::models::SourceMaterial;
use sinex_db::repositories::DbPoolExt;
#[cfg(feature = "db")]
use sinex_db::DbPool as PgPool;
use sinex_primitives::events::builder::{EventId, Provenance};
use sinex_primitives::events::Event;
const DEFAULT_EVENT_CHANNEL_SIZE: usize = 1024;
use sinex_primitives::{
    non_empty::NonEmptyVec, EventSource, EventType, HostName, Id, JsonValue, OffsetKind, Ulid,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
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
        self.sender.send(event.clone()).await.map_err(|err| {
            SinexError::processing(format!(
                "Failed to forward confirmed event to automaton: {err}"
            ))
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

    /// Processor-specific configuration
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

    /// Processor-specific statistics
    pub processor_stats: HashMap<String, u64>,

    /// Targets that were successfully processed
    pub successful_targets: Vec<String>,

    /// Targets that failed processing with error messages
    pub failed_targets: Vec<(String, String)>,

    /// Warnings encountered during processing
    pub warnings: Vec<String>,
}

/// Unified trait for all stream processors (ingestors and automata).
#[async_trait]
pub trait Node: Send + Sync {
    type Config: for<'de> Deserialize<'de> + Default + Send + Sync;

    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()>;

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport>;

    fn node_name(&self) -> &str;
    fn node_type(&self) -> NodeType;

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint>;

    async fn health_check(&self) -> NodeResult<bool> {
        Ok(true)
    }

    async fn process_event_batch(
        &mut self,
        _events: Vec<Event<JsonValue>>,
    ) -> NodeResult<ProcessingStats> {
        Err(SinexError::processing(
            "This processor does not support event batch processing. Only automata should implement this method.".to_string()
        ))
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        info!(node = %self.node_name(), "Node shutting down");
        Ok(())
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        _until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
        Ok(ScanEstimate::default())
    }

    fn config_schema(&self) -> Option<serde_json::Value> {
        None
    }
}

/// Type of stream processor
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    /// Ingestor: External World -> Event Stream
    Ingestor,
    /// Automaton: Event Stream -> DerivedEvent Stream
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

/// Processor capabilities
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

    /// Processor manages its own continuous loop (runner skips JetStream bridge)
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

/// Unified runner for nodes
pub struct NodeRunner<T: Node> {
    node: T,
    handles: Option<NodeHandles>,
    service_info: Option<ServiceInfo>,
    raw_config: Option<HashMap<String, serde_json::Value>>,
    work_dir_utf8: Option<Utf8PathBuf>,
    event_processor_handle: Option<tokio::task::JoinHandle<NodeResult<()>>>,
    event_processor_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
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

impl<T: Node + 'static> NodeRunner<T> {
    /// Create a new node runner
    pub fn new(node: T) -> Self {
        Self {
            node,
            handles: None,
            service_info: None,
            raw_config: None,
            work_dir_utf8: None,
            event_processor_handle: None,
            event_processor_shutdown: None,
            schema_listener_handle: None,
            checkpoint_cleanup_handle: None,
            consumer_handle: None,
            processing_model: ProcessingModel::StatelessWorker,
            leader_state: None,
        }
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

    /// Initialize the processor with a specific transport
    pub async fn initialize_with_transport(
        &mut self,
        service_name: String,
        raw_config: HashMap<String, serde_json::Value>,
        #[cfg(feature = "db")] db_pool: Option<PgPool>,
        transport: EventTransport,
        work_dir: std::path::PathBuf,
        dry_run: bool,
    ) -> NodeResult<()> {
        // DATABASE_URL is optional - processors that need it will call
        // require_db_pool() which provides a clear error message.

        // Create bounded event channel
        let (event_sender_raw, event_receiver) =
            mpsc::channel::<Event<JsonValue>>(DEFAULT_EVENT_CHANNEL_SIZE);

        // Create shutdown channels
        let (processor_shutdown_sender, processor_shutdown_receiver) =
            tokio::sync::oneshot::channel();
        self.event_processor_shutdown = Some(processor_shutdown_sender);

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

        // Initialize checkpoint manager with KV
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv_store,
            service_name.clone(),
            "default".to_string(),
            consumer_name.clone(),
        ));

        // NATS is the only transport
        let transport_type = "NATS";

        // Determine if automaton to enable LeaderStandby
        let confirmation_buffer_opt = if matches!(self.node.node_type(), NodeType::Automaton) {
            self.processing_model = ProcessingModel::LeaderStandby;
            Some(Arc::new(crate::ConfirmationBuffer::new(
                std::time::Duration::from_secs(60),
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
                SinexError::configuration(format!("Failed to serialize processor config: {e}"))
            })?;
            serde_json::from_value(config_value).map_err(|e| {
                SinexError::configuration(format!("Failed to parse processor config: {e}"))
            })?
        };

        let init_context = NodeInitContext::new(
            typed_config,
            raw_config.clone(),
            service_info.clone(),
            handles.clone(),
            work_dir_utf8.clone(),
        );

        self.node.initialize(init_context).await?;

        self.handles = Some(handles);
        self.service_info = Some(service_info);
        self.raw_config = Some(raw_config.clone());
        self.work_dir_utf8 = Some(work_dir_utf8);

        let processor_config = EventBatcherConfig::default();
        self.event_processor_handle = Some(spawn_event_processor(
            transport_clone_for_runner,
            processor_config,
            event_receiver,
            processor_shutdown_receiver,
        ));

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
        if self.handles.is_none() {
            return Err(SinexError::lifecycle("Node not initialized".to_string()));
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
        if self.handles.is_none() {
            return Err(SinexError::lifecycle("Node not initialized".to_string()));
        }

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
            let _continuous_report = self
                .node
                .scan(
                    current_checkpoint,
                    TimeHorizon::Continuous,
                    ScanArgs::default(),
                )
                .await?;
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

            // Check lease status before processing (for LeaderStandby model)
            if self.processing_model == ProcessingModel::LeaderStandby {
                // Use CoordinationKvClient to check leadership
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
                    let kv_client = sinex_primitives::coordination::CoordinationKvClient::new(
                        js,
                        service.clone(),
                    );

                    // Single-shot leadership acquisition/check
                    let is_leader =
                        kv_client
                            .acquire_leadership(&instance_id)
                            .await
                            .map_err(|e| {
                                SinexError::processing(format!("Failed to acquire leadership: {e}"))
                            })?;

                    if !is_leader {
                        info!("Not leader, skipping processing");
                        return Ok(());
                    }

                    info!("Confirmed as leader, proceeding with processing");

                    // Spawn a simplified heartbeater
                    let kv_clone = kv_client.clone();
                    let instance_id_clone = instance_id.clone();
                    let heartbeat_handle = tokio::spawn(async move {
                        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                        loop {
                            interval.tick().await;
                            // Basic heartbeat logic - just keep refreshing leadership
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
                warn!("LeaderStandby mode requires messaging feature. Skipping leadership check.");
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

        let service_name = self
            .service_info
            .as_ref()
            .map(|info| info.service_name().to_string())
            .unwrap_or_else(|| self.node.node_name().to_string());

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
            confirmation_timeout: std::time::Duration::from_secs(60),
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
        let mut last_event_id: Option<Ulid> = None;

        // Batch processing: accumulate up to BATCH_SIZE events before processing.
        // Block on the first event, then non-blocking drain whatever else is queued.
        const BATCH_SIZE: usize = 100;

        loop {
            // Block until at least one event arrives (or channel closes)
            let first = match receiver.recv().await {
                Some(p) => p,
                None => break,
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
            let mut events = Vec::with_capacity(provisionals.len());
            let mut batch_last_event_id = None;

            for provisional in &provisionals {
                let event_id = &provisional.event_id;
                let event = {
                    #[cfg(feature = "db")]
                    match &db_pool {
                        Some(pool) => match Self::fetch_persisted_event(pool, event_id).await? {
                            Some(event) => Some(event),
                            None => {
                                warn!(
                                    "Confirmed event {:?} missing from database; skipping",
                                    event_id
                                );
                                None
                            }
                        },
                        None => match Self::build_event_from_provisional(provisional) {
                            Ok(event) => Some(event),
                            Err(err) => {
                                warn!(error = %err, "Failed to build event from provisional payload");
                                None
                            }
                        },
                    }
                    #[cfg(not(feature = "db"))]
                    match Self::build_event_from_provisional(provisional) {
                        Ok(event) => Some(event),
                        Err(err) => {
                            warn!(error = %err, "Failed to build event from provisional payload");
                            None
                        }
                    }
                };

                if let Some(event) = event {
                    batch_last_event_id = Some(*event_id.as_ulid());
                    events.push(event);
                }
            }

            if events.is_empty() {
                continue;
            }

            let batch_size = events.len();
            match self.node.process_event_batch(events).await {
                Ok(stats) => {
                    processed_events += stats.processed as u64;
                    events_since_checkpoint += stats.processed as u64;
                    if let Some(eid) = batch_last_event_id {
                        last_event_id = Some(eid);
                    }
                    if batch_size > 1 {
                        debug!(batch_size, processed_events, "Processed event batch");
                    }
                }
                Err(err) => {
                    // Save checkpoint before bailing so we don't lose ALL progress
                    if let Some(eid) = last_event_id {
                        checkpoint_state.checkpoint = Checkpoint::Internal {
                            event_id: eid,
                            message_count: processed_events,
                        };
                        checkpoint_state.processed_count = processed_events;
                        checkpoint_state.last_activity =
                            sinex_primitives::temporal::Timestamp::now();
                        let _ = checkpoint_manager.save_checkpoint(&checkpoint_state).await;
                    }
                    error!(error = %err, batch_size, "Automaton batch processing failed - stopping node to prevent data loss");
                    return Err(err);
                }
            }

            // Periodic checkpoint save: every N events or M seconds
            if events_since_checkpoint >= CHECKPOINT_EVENT_INTERVAL
                || last_checkpoint_time.elapsed() >= CHECKPOINT_TIME_INTERVAL
            {
                if let Some(eid) = last_event_id {
                    checkpoint_state.checkpoint = Checkpoint::Internal {
                        event_id: eid,
                        message_count: processed_events,
                    };
                    checkpoint_state.processed_count = processed_events;
                    checkpoint_state.last_activity = sinex_primitives::temporal::Timestamp::now();
                    match checkpoint_manager.save_checkpoint(&checkpoint_state).await {
                        Ok(revision) => {
                            checkpoint_state.revision = revision;
                            events_since_checkpoint = 0;
                            last_checkpoint_time = std::time::Instant::now();
                            debug!(processed_events, revision, "Periodic checkpoint saved");
                        }
                        Err(err) => {
                            warn!(error = %err, "Failed to save periodic checkpoint; will retry next interval");
                        }
                    }
                }
            }
        }

        // Save final checkpoint on clean exit
        if let Some(eid) = last_event_id {
            checkpoint_state.checkpoint = Checkpoint::Internal {
                event_id: eid,
                message_count: processed_events,
            };
            checkpoint_state.processed_count = processed_events;
            checkpoint_state.last_activity = sinex_primitives::temporal::Timestamp::now();
            if let Err(err) = checkpoint_manager.save_checkpoint(&checkpoint_state).await {
                warn!(error = %err, "Failed to save final checkpoint on shutdown");
            } else {
                info!(processed_events, "Final checkpoint saved on clean shutdown");
            }
        }

        info!(
            processed_events,
            "JetStream confirmed event channel closed; stopping automaton bridge"
        );

        consumer.stop().await;

        if let Some(handle) = self.consumer_handle.take() {
            if let Err(err) = handle.await {
                warn!(error = %err, "Failed to join automaton consumer task");
            }
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

    fn parse_ulid(value: &str, field: &str) -> NodeResult<Ulid> {
        value.parse::<Ulid>().map_err(|err| {
            SinexError::processing(format!("Invalid ULID for {field}: {value} ({err})"))
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
            ingestor_version: Option<String>,
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
                SinexError::processing(format!(
                    "Failed to parse provisional event payload: {}",
                    err
                ))
            })?;

        // Parse provenance fields for flat Event struct
        let provenance = match (published.source_material_id, published.source_event_ids) {
            (Some(material_id), None) => {
                let anchor_byte = published.anchor_byte.ok_or_else(|| {
                    SinexError::processing("Material provenance missing anchor_byte".to_string())
                })?;
                let material_ulid = Self::parse_ulid(&material_id, "source_material_id")?;
                Provenance::Material {
                    id: Id::<SourceMaterial>::from_ulid(material_ulid),
                    anchor_byte,
                    offset_start: published.offset_start,
                    offset_end: published.offset_end,
                    offset_kind: Self::parse_offset_kind(published.offset_kind.as_deref()),
                }
            }
            (None, Some(source_ids)) => {
                let mut ids = Vec::new();
                for raw_id in source_ids {
                    let ulid = Self::parse_ulid(&raw_id, "source_event_ids")?;
                    ids.push(EventId::from_ulid(ulid));
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
                ))
            }
            (None, None) => {
                return Err(SinexError::processing(
                    "Provisional event missing provenance".to_string(),
                ))
            }
        };

        let payload_schema_id = published
            .payload_schema_id
            .map(|value| Self::parse_ulid(&value, "payload_schema_id"))
            .transpose()?;
        let associated_blob_ids = match published.associated_blob_ids {
            Some(ids) => {
                let mut parsed = Vec::with_capacity(ids.len());
                for raw_id in ids {
                    parsed.push(Self::parse_ulid(&raw_id, "associated_blob_ids")?);
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
            ingestor_version: published.ingestor_version,
            payload_schema_id,
            provenance,
            associated_blob_ids,
        })
    }

    /// Get processor capabilities
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

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> NodeResult<()> {
        info!("Shutting down stream processor runner");

        // Abort and await schema listener task if running
        if let Some(handle) = self.schema_listener_handle.take() {
            handle.abort();
            let _ = handle.await;
            debug!("Aborted and joined schema broadcast listener task");
        }

        // Clean up leader state
        if let Some(state) = self.leader_state.take() {
            state.heartbeat_handle.abort();
            let _ = state.heartbeat_handle.await;
            if let Err(err) = state.kv_client.release_leadership(&state.instance_id).await {
                warn!(error = %err, "Failed to release leadership on shutdown");
            }
        }

        // Signal event processor to shutdown and await it
        if let Some(shutdown_tx) = self.event_processor_shutdown.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(handle) = self.event_processor_handle.take() {
            match handle.await {
                Ok(result) => {
                    if let Err(err) = result {
                        error!(error = %err, "Event processor failed during shutdown");
                    }
                }
                Err(join_err) => {
                    error!(error = %join_err, "Failed to join event processor task");
                }
            }
        }

        if let Some(handle) = self.consumer_handle.take() {
            handle.abort();
            let _ = handle.await;
            debug!("Aborted automaton consumer task");
        }

        if let Some(handle) = self.checkpoint_cleanup_handle.take() {
            handle.abort();
            let _ = handle.await;
            debug!("Aborted checkpoint cleanup task");
        }

        self.node.shutdown().await
    }
}
