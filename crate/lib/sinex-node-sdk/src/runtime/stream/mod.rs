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
    event_processor::{spawn_event_processor, EventProcessorConfig, EventTransport},
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    NodeError, NodeResult,
};
use async_nats::jetstream::kv;
use async_trait::async_trait;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use color_eyre::eyre::eyre;
use serde::{Deserialize, Serialize};
use sinex_core::db::models::{Event, EventId, Provenance, SourceMaterial};
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::db::SqlxPgPool as PgPool;
use sinex_core::types::buffers::DEFAULT_EVENT_CHANNEL_SIZE;
use sinex_core::types::non_empty::NonEmptyVec;
use sinex_core::{EventSource, EventType, HostName, JsonValue, OffsetKind, Ulid};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

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
            NodeError::Processing(format!(
                "Failed to forward confirmed event to automaton: {}",
                err
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
    let bucket = "KV_sinex_checkpoints";
    let kv_store = match js
        .create_key_value(kv::Config {
            bucket: bucket.to_string(),
            ..Default::default()
        })
        .await
    {
        Ok(store) => store,
        Err(create_err) => js.get_key_value(bucket).await.map_err(|e| {
            NodeError::General(eyre!(
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
)> {
    // Always enable schema cache and validation for node-side validation.
    // Schemas are broadcast from ingestd and stored in NATS KV.

    let client = match transport {
        EventTransport::Nats(publisher) => publisher.nats_client().clone(),
    };
    let env = sinex_core::environment();
    let subject = env.nats_subject("system.schemas.active");
    let mut sub = client
        .subscribe(subject.clone())
        .await
        .map_err(|e| NodeError::General(eyre!("Failed to subscribe to schema broadcasts: {e}")))?;

    // Create schema cache and validator
    let cache = Arc::new(SchemaBroadcastCache::default());
    let cache_clone = cache.clone();
    let validator = Arc::new(crate::schema_validator::NodeSchemaValidator::new());
    let validator_clone = validator.clone();

    // Get KV bucket for fetching full schemas
    let js = async_nats::jetstream::new(client);
    let kv = js
        .get_key_value("KV_sinex_schemas")
        .await
        .map_err(|e| NodeError::General(eyre!("Failed to get schema KV bucket: {e}")))?;

    // Background task to update cache and validator
    tokio::spawn(async move {
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
    });

    info!(
        "Started schema broadcast listener and validator for {}",
        subject
    );

    Ok((Some(cache), Some(validator)))
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
    pub time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,

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

    fn processor_name(&self) -> &str;
    fn processor_type(&self) -> NodeType;

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
        Err(NodeError::General(eyre!(
            "This processor does not support event batch processing. Only automata should implement this method."
        )))
    }

    async fn shutdown(&mut self) -> NodeResult<()> {
        info!(processor = %self.processor_name(), "Stream processor shutting down");
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

/// Unified runner for stream processors
pub struct StreamProcessorRunner<T: Node> {
    processor: T,
    handles: Option<NodeHandles>,
    service_info: Option<ServiceInfo>,
    raw_config: Option<HashMap<String, serde_json::Value>>,
    work_dir_utf8: Option<Utf8PathBuf>,
    shutdown_receiver: Option<tokio::sync::oneshot::Receiver<()>>,
    event_processor_handle: Option<tokio::task::JoinHandle<NodeResult<()>>>,
    event_processor_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    processing_model: ProcessingModel,
    leader_state: Option<LeaderState>,
}

struct LeaderState {
    kv_client: sinex_core::coordination::kv_client::CoordinationKvClient,
    instance_id: String,
    heartbeat_handle: tokio::task::JoinHandle<()>,
}

impl<T: Node + 'static> StreamProcessorRunner<T> {
    /// Create a new stream processor runner
    pub fn new(processor: T) -> Self {
        Self {
            processor,
            handles: None,
            service_info: None,
            raw_config: None,
            work_dir_utf8: None,
            shutdown_receiver: None,
            event_processor_handle: None,
            event_processor_shutdown: None,
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
        db_pool: Option<PgPool>,
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
        let (_shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let (processor_shutdown_sender, processor_shutdown_receiver) =
            tokio::sync::oneshot::channel();
        self.shutdown_receiver = Some(shutdown_receiver);
        self.event_processor_shutdown = Some(processor_shutdown_sender);

        // Get hostname
        let host = gethostname::gethostname().to_string_lossy().to_string();
        let consumer_name = format!("{}-{}", host, std::process::id());
        let transport_for_context = transport.clone();
        let transport_clone_for_runner = transport.clone();

        let kv_store = create_checkpoint_kv(&transport).await?;
        let (schema_cache, schema_validator) = maybe_start_schema_listener(&transport).await?;

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
        let confirmation_buffer_opt =
            if matches!(self.processor.processor_type(), NodeType::Automaton) {
                self.processing_model = ProcessingModel::LeaderStandby;
                Some(Arc::new(crate::ConfirmationBuffer::new(
                    std::time::Duration::from_secs(60),
                )))
            } else {
                self.processing_model = ProcessingModel::StatelessWorker;
                None
            };

        let event_emitter = if let Some(validator) = schema_validator {
            EventEmitter::with_validator(event_sender_raw.clone(), dry_run, validator)
        } else {
            EventEmitter::new(event_sender_raw, dry_run)
        };

        // No LeaseManager passed to handles
        let handles = if let Some(pool) = db_pool {
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
        };

        let service_info = ServiceInfo::new(
            service_name.clone(),
            host.clone(),
            work_dir.clone(),
            dry_run,
        );
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir).unwrap_or_else(|_| {
            Utf8PathBuf::from_path_buf(sinex_core::environment().temp_dir())
                .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex"))
        });

        let typed_config = if raw_config.is_empty() {
            T::Config::default()
        } else {
            let config_value = serde_json::to_value(&raw_config).map_err(|e| {
                NodeError::Configuration(format!("Failed to serialize processor config: {e}"))
            })?;
            serde_json::from_value(config_value).map_err(|e| {
                NodeError::Configuration(format!("Failed to parse processor config: {e}"))
            })?
        };

        let init_context = NodeInitContext::new(
            typed_config,
            raw_config.clone(),
            service_info.clone(),
            handles.clone(),
            work_dir_utf8.clone(),
        );

        self.processor.initialize(init_context).await?;

        self.handles = Some(handles);
        self.service_info = Some(service_info);
        self.raw_config = Some(raw_config.clone());
        self.work_dir_utf8 = Some(work_dir_utf8);

        let processor_config = EventProcessorConfig::default();
        self.event_processor_handle = Some(spawn_event_processor(
            transport_clone_for_runner,
            processor_config,
            event_receiver,
            processor_shutdown_receiver,
        ));

        info!(
            service = %service_name,
            processor = %self.processor.processor_name(),
            processor_type = ?self.processor.processor_type(),
            transport = transport_type,
            "Stream processor initialized"
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
            return Err(NodeError::Lifecycle(
                "Stream processor not initialized".to_string(),
            ));
        }

        info!(
            processor = %self.processor.processor_name(),
            from = %from.description(),
            until = ?until,
            dry_run = args.dry_run,
            "Starting scan operation"
        );

        let start_time = std::time::Instant::now();
        let result = self.processor.scan(from, until, args).await;

        match &result {
            Ok(report) => {
                info!(
                    processor = %self.processor.processor_name(),
                    events_processed = report.events_processed,
                    duration_ms = start_time.elapsed().as_millis(),
                    "Scan operation completed successfully"
                );
            }
            Err(e) => {
                warn!(
                    processor = %self.processor.processor_name(),
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
            return Err(NodeError::Lifecycle(
                "Stream processor not initialized".to_string(),
            ));
        }

        let processor_type = self.processor.processor_type();
        info!(
            processor = %self.processor.processor_name(),
            processor_type = ?processor_type,
            "Starting service with startup sequence"
        );

        match processor_type {
            NodeType::Ingestor => {
                // Ingestor startup sequence: Snapshot -> Gap-fill -> Continuous
                self.run_ingestor_startup_sequence().await
            }
            NodeType::Automaton => {
                // Automaton startup: consume events from NATS streams
                self.run_automaton_continuous_mode().await
            }
        }
    }

    /// Run ingestor startup sequence (Snapshot -> Gap-fill -> Continuous)
    async fn run_ingestor_startup_sequence(&mut self) -> NodeResult<()> {
        // Phase 1: Snapshot (if supported)
        if self.processor.capabilities().supports_snapshot {
            info!("Phase 1: Taking initial snapshot");
            let snapshot_report = self
                .processor
                .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
                .await?;

            debug!(
                events = snapshot_report.events_processed,
                "Snapshot phase completed"
            );
        }

        // Phase 2: Gap-filling (if supported and needed)
        if self.processor.capabilities().supports_historical {
            let current_checkpoint = self.processor.current_checkpoint().await?;

            // Only gap-fill if we have a previous checkpoint
            if !matches!(current_checkpoint, Checkpoint::None) {
                info!("Phase 2: Gap-filling from last checkpoint");
                let gap_fill_report = self
                    .processor
                    .scan(
                        current_checkpoint,
                        TimeHorizon::Historical {
                            end_time: Utc::now(),
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
        if self.processor.capabilities().supports_continuous {
            info!("Phase 3: Starting continuous processing");
            let current_checkpoint = self.processor.current_checkpoint().await?;

            // This should run indefinitely until shutdown
            let _continuous_report = self
                .processor
                .scan(
                    current_checkpoint,
                    TimeHorizon::Continuous,
                    ScanArgs::default(),
                )
                .await?;
        } else {
            warn!("Processor does not support continuous mode - service will exit");
        }

        Ok(())
    }

    /// Run automaton in continuous mode
    async fn run_automaton_continuous_mode(&mut self) -> NodeResult<()> {
        info!("Starting automaton continuous mode");

        // Get current checkpoint to resume from previous state if available
        let current_checkpoint = self.processor.current_checkpoint().await?;
        let capabilities = self.processor.capabilities();

        if capabilities.supports_continuous {
            info!("Starting continuous event processing for automaton");

            // Check lease status before processing (for LeaderStandby model)
            if self.processing_model == ProcessingModel::LeaderStandby {
                // Use CoordinationKvClient to check leadership
                let rs = self
                    .runtime_state()
                    .ok_or_else(|| NodeError::General(eyre!("Runtime state missing")))?;
                let nc = rs
                    .nats_client()
                    .ok_or_else(|| NodeError::General(eyre!("NATS client missing")))?;
                let service = rs.service_info().service_name().to_string();
                let host = rs.service_info().host().to_string();
                let pid = std::process::id();
                let instance_id = format!("{}-{}", host, pid);

                let js = async_nats::jetstream::new(nc);
                let kv_client = sinex_core::coordination::kv_client::CoordinationKvClient::new(
                    js,
                    service.clone(),
                );

                // Single-shot leadership acquisition/check
                let is_leader = kv_client
                    .acquire_leadership(&instance_id)
                    .await
                    .map_err(|e| {
                        NodeError::General(eyre!("Failed to acquire leadership: {}", e))
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
                            warn!("Heartbeat failed: {}", e);
                        }
                    }
                });

                self.leader_state = Some(LeaderState {
                    kv_client,
                    instance_id,
                    heartbeat_handle,
                });
            }

            if capabilities.manages_own_continuous_loop {
                let _continuous_report = self
                    .processor
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
                    .processor
                    .scan(
                        current_checkpoint,
                        TimeHorizon::Historical {
                            end_time: Utc::now(),
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

    async fn run_automaton_event_bridge(&mut self, from: Checkpoint) -> NodeResult<()> {
        let handles = self
            .handles
            .as_ref()
            .ok_or_else(|| NodeError::Lifecycle("Runner handles not initialized".to_string()))?;

        let db_pool = handles.db_pool().cloned();
        let transport = handles.transport().clone();

        let service_name = self
            .service_info
            .as_ref()
            .map(|info| info.service_name().to_string())
            .unwrap_or_else(|| self.processor.processor_name().to_string());

        let (sender, mut receiver) =
            mpsc::channel::<ProvisionalEvent>(CONFIRMED_EVENT_CHANNEL_CAPACITY);
        let handler = Arc::new(RunnerConfirmedEventHandler::new(sender));

        let env = sinex_core::environment().clone();

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

        if !matches!(from, Checkpoint::None) && self.processor.capabilities().supports_historical {
            info!("Processing historical backlog before entering continuous mode");
            let _ = self
                .processor
                .scan(
                    from,
                    TimeHorizon::Historical {
                        end_time: Utc::now(),
                    },
                    ScanArgs::default(),
                )
                .await?;
        }

        let mut processed_events = 0u64;

        while let Some(provisional) = receiver.recv().await {
            let event_id = EventId::from_ulid(provisional.event_id);
            let event = match &db_pool {
                Some(pool) => match Self::fetch_persisted_event(pool, &event_id).await? {
                    Some(event) => Some(event),
                    None => {
                        warn!(
                            "Confirmed event {:?} missing from database; skipping",
                            event_id
                        );
                        None
                    }
                },
                None => match Self::build_event_from_provisional(&provisional) {
                    Ok(event) => Some(event),
                    Err(err) => {
                        warn!(error = %err, "Failed to build event from provisional payload");
                        None
                    }
                },
            };

            if let Some(event) = event {
                match self.processor.process_event_batch(vec![event]).await {
                    Ok(stats) => {
                        processed_events += stats.processed as u64;
                    }
                    Err(err) => {
                        warn!(error = %err, "Automaton batch processing failed");
                    }
                }
            }
        }

        info!(
            processed_events,
            "JetStream confirmed event channel closed; stopping automaton bridge"
        );

        consumer.stop().await;

        if let Err(err) = consumer_handle.await {
            warn!(error = %err, "Failed to join automaton consumer task");
        }

        Ok(())
    }

    async fn fetch_persisted_event(
        pool: &PgPool,
        event_id: &EventId,
    ) -> NodeResult<Option<Event<JsonValue>>> {
        let event_id_str = event_id.to_string();
        pool.events()
            .get_by_id(event_id.clone())
            .await
            .map_err(|err| {
                NodeError::Processing(format!(
                    "Failed to load confirmed event {} from database: {}",
                    event_id_str, err
                ))
            })
    }

    fn parse_ulid(value: &str, field: &str) -> NodeResult<Ulid> {
        value.parse::<Ulid>().map_err(|err| {
            NodeError::Processing(format!("Invalid ULID for {}: {} ({})", field, value, err))
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
                NodeError::Processing(format!(
                    "Failed to parse provisional event payload: {}",
                    err
                ))
            })?;

        let provenance = match (published.source_material_id, published.source_event_ids) {
            (Some(material_id), None) => {
                let anchor = published.anchor_byte.ok_or_else(|| {
                    NodeError::Processing("Material provenance missing anchor_byte".to_string())
                })?;
                let material_ulid = Self::parse_ulid(&material_id, "source_material_id")?;
                Provenance::Material {
                    id: sinex_core::Id::<SourceMaterial>::from_ulid(material_ulid),
                    anchor_byte: anchor,
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
                let non_empty = NonEmptyVec::from_vec(ids).ok_or_else(|| {
                    NodeError::Processing(
                        "Synthesis provenance missing source_event_ids".to_string(),
                    )
                })?;
                Provenance::Synthesis {
                    source_event_ids: non_empty,
                    operation_id: None,
                }
            }
            (Some(_), Some(_)) => {
                return Err(NodeError::Processing(
                    "Provisional event contains both material and synthesis provenance".to_string(),
                ))
            }
            (None, None) => {
                return Err(NodeError::Processing(
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
            id: Some(EventId::from_ulid(provisional.event_id)),
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
        self.processor.capabilities()
    }

    /// Get scan estimate
    pub async fn estimate_scan_scope(
        &self,
        from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
        self.processor.estimate_scan_scope(from, until, args).await
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> NodeResult<()> {
        info!("Shutting down stream processor runner");
        if let Some(state) = self.leader_state.take() {
            state.heartbeat_handle.abort();
            if let Err(err) = state.kv_client.release_leadership(&state.instance_id).await {
                warn!(error = %err, "Failed to release leadership on shutdown");
            }
        }
        self.processor.shutdown().await
    }
}
