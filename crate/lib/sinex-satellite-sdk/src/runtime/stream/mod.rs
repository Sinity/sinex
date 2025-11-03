#![doc = include_str!("../../../doc/stream_processor.md")]

mod checkpoint;
mod handles;
mod stats;
mod time_horizon;

pub use checkpoint::Checkpoint;
pub use handles::{
    EventEmitter, EventSender, EventStream, ProcessorHandles, ProcessorInitContext, ServiceInfo,
};
pub use stats::ProcessingStats;
pub use time_horizon::TimeHorizon;

use crate::{
    checkpoint::CheckpointManager,
    confirmation_handler::{ConfirmedEventHandler, ProcessingModel, ProvisionalEvent},
    event_processor::{spawn_event_processor, EventProcessorConfig, EventTransport},
    jetstream_consumer::{JetStreamEventConsumer, JetStreamEventConsumerConfig},
    SatelliteError, SatelliteResult,
};
use async_trait::async_trait;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use color_eyre::eyre::eyre;
use serde::{Deserialize, Serialize};
use sinex_core::db::models::{Event, EventId};
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::db::SqlxPgPool as PgPool;
use sinex_core::JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
#[derive(Clone)]
struct RunnerConfirmedEventHandler {
    sender: mpsc::UnboundedSender<ProvisionalEvent>,
}

impl RunnerConfirmedEventHandler {
    fn new(sender: mpsc::UnboundedSender<ProvisionalEvent>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl ConfirmedEventHandler for RunnerConfirmedEventHandler {
    async fn handle_confirmed(&self, event: &ProvisionalEvent) -> SatelliteResult<()> {
        self.sender.send(event.clone()).map_err(|err| {
            SatelliteError::Processing(format!(
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

/// Context provided to stream processors during operations
pub struct StreamProcessorContext {
    /// Service/processor name
    pub service_name: String,

    /// Hostname where the processor is running
    pub host: String,

    /// Working directory for temporary files
    pub work_dir: std::path::PathBuf,

    /// Whether running in dry-run mode
    pub dry_run: bool,

    /// Database connection pool
    pub db_pool: PgPool,

    /// Event transport mechanism (gRPC or NATS)
    pub transport: crate::event_processor::EventTransport,

    /// Checkpoint manager for state persistence
    pub checkpoint_manager: Arc<CheckpointManager>,

    /// Legacy processor-specific configuration (deprecated).
    ///
    /// This field is maintained for backward compatibility but should not be used
    /// by new processors. Use the typed configuration passed to `initialize()` instead.
    /// This will be removed in a future version.
    pub config: HashMap<String, serde_json::Value>,

    /// Event sender channel for scan operations
    pub event_sender: EventSender,

    /// Lease manager for leader election (automata only)
    pub lease_manager: Option<Arc<crate::LeaseManager>>,

    /// Confirmation buffer for provisional events (automata only)
    pub confirmation_buffer: Option<Arc<crate::ConfirmationBuffer>>,
}

impl std::fmt::Debug for StreamProcessorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamProcessorContext")
            .field("service_name", &self.service_name)
            .field("host", &self.host)
            .field("work_dir", &self.work_dir)
            .field("dry_run", &self.dry_run)
            .finish()
    }
}

impl StreamProcessorContext {
    pub fn from_runtime(
        service: &ServiceInfo,
        handles: &ProcessorHandles,
        config: HashMap<String, serde_json::Value>,
        work_dir_utf8: Utf8PathBuf,
    ) -> Self {
        let sender_arc = handles.emitter().sender();
        Self {
            service_name: service.service_name().to_string(),
            host: service.host().to_string(),
            work_dir: work_dir_utf8.into_std_path_buf(),
            dry_run: service.dry_run(),
            db_pool: handles.db_pool().clone(),
            transport: handles.transport().clone(),
            checkpoint_manager: handles.checkpoint_manager(),
            config,
            event_sender: (*sender_arc).clone(),
            lease_manager: handles.lease_manager(),
            confirmation_buffer: handles.confirmation_buffer(),
        }
    }

    /// Send an event through the event channel
    #[cfg_attr(
        feature = "macros",
        sinex_macros::auto_event_metrics(event_type = "emit")
    )]
    pub async fn emit_event(&self, event: Event<JsonValue>) -> SatelliteResult<()> {
        let _start = std::time::Instant::now();
        let event_type = event.event_type.clone();

        if self.dry_run {
            info!(
                source = %event.source,
                event_type = %event_type,
                "DRY RUN: Would emit event"
            );
            return Ok(());
        }

        let result = self
            .event_sender
            .send(event)
            .map_err(|_| SatelliteError::General(eyre!("Event channel closed")));

        result
    }

    /// Send multiple events through the event channel
    pub async fn emit_events(&self, events: Vec<Event<JsonValue>>) -> SatelliteResult<()> {
        for event in events {
            self.emit_event(event).await?;
        }
        Ok(())
    }
}

/// Unified trait for all stream processors (ingestors and automata).
///
/// This trait implements the "Deep Symmetry" architecture where both ingestors
/// and automata share the same core `scan()` interface, differing only in their
/// data sources and processing logic.
///
/// # Architecture
/// - **Ingestors**: External World → Event Stream (e.g., file watchers, log parsers)
/// - **Automata**: Event Stream → DerivedEvent Stream (e.g., command canonicalizers)
///
/// # Implementation Notes
/// - Implementations must be thread-safe (`Send + Sync`)
/// - The `scan()` method is the core interface - other methods provide metadata
/// - Checkpointing is handled externally via `StreamProcessorContext`
/// - Graceful shutdown should be implemented in `shutdown()`
/// - Each processor defines its own `Config` type for type-safe configuration
///
/// # Examples
/// ```ignore
/// use sinex_satellite_sdk::*;
/// use async_trait::async_trait;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Deserialize, Serialize)]
/// struct MyIngestorConfig {
///     max_files: usize,
///     watch_interval: std::time::Duration,
/// }
///
/// impl Default for MyIngestorConfig {
///     fn default() -> Self {
///         Self {
///             max_files: 100,
///             watch_interval: std::time::Duration::from_secs(60),
///         }
///     }
/// }
///
/// struct MyIngestor;
///
/// #[async_trait]
/// impl StatefulStreamProcessor for MyIngestor {
///     type Config = MyIngestorConfig;
///
///     async fn initialize(&mut self, ctx: StreamProcessorContext, config: Self::Config) -> SatelliteResult<()> {
///         // Initialize with context and typed configuration
///         Ok(())
///     }
///
///     async fn scan(
///         &mut self,
///         from: Checkpoint,
///         until: TimeHorizon,
///         args: ScanArgs,
///     ) -> SatelliteResult<ScanReport> {
///         // Implement scanning logic
///         Ok(ScanReport::default())
///     }
///
///     fn processor_name(&self) -> &str { "my-ingestor" }
///     fn processor_type(&self) -> ProcessorType { ProcessorType::Ingestor }
/// }
/// ```
#[async_trait]
pub trait StatefulStreamProcessor: Send + Sync {
    /// Associated configuration type for this processor.
    ///
    /// This type must implement Deserialize for parsing from JSON/TOML configuration,
    /// Default for fallback values, and Send + Sync for thread safety.
    /// The 'de lifetime bound enables deserialization from any source.
    type Config: for<'de> Deserialize<'de> + Default + Send + Sync;

    /// Initialize the processor with the given context and typed configuration.
    ///
    /// The configuration is parsed once at the system boundary and passed as a
    /// strongly-typed object, eliminating the need for processors to handle
    /// configuration parsing and validation internally.
    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        config: Self::Config,
    ) -> SatelliteResult<()>;

    /// Initialize using the new runtime handles. Default implementation builds the
    /// legacy context and defers to `initialize` for backward compatibility.
    async fn initialize_with_runtime(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (config, raw_config, service, handles, work_dir_utf8) = init.into_parts();
        let legacy_ctx =
            StreamProcessorContext::from_runtime(&service, &handles, raw_config, work_dir_utf8);
        self.initialize(legacy_ctx, config).await
    }

    /// Backward compatibility method for processors that need access to legacy config format.
    ///
    /// This method is called by the old initialization path and can be used to convert
    /// from the legacy HashMap format. Most processors should implement the new
    /// `initialize` method instead.
    async fn initialize_legacy(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()> {
        // Extract processor-specific config from the legacy format
        let config = if ctx.config.is_empty() {
            Self::Config::default()
        } else {
            // Try to deserialize from the generic config map
            let config_value = serde_json::to_value(&ctx.config).map_err(|e| {
                SatelliteError::Configuration(format!("Failed to serialize legacy config: {}", e))
            })?;

            serde_json::from_value(config_value).map_err(|e| {
                SatelliteError::Configuration(format!("Failed to parse legacy config: {}", e))
            })?
        };

        self.initialize(ctx, config).await
    }

    /// Core scan method - the heart of the unified architecture.
    ///
    /// This method implements the unified interface that replaces both:
    /// - EventSource::start_streaming() + run_scanner() for ingestors
    /// - Automaton event processing for automata
    ///
    /// # Parameters
    /// - `from`: Starting checkpoint (where to resume processing)
    /// - `until`: Time horizon (how far/long to process)
    /// - `args`: Additional scan configuration and filters
    ///
    /// # Behavior by TimeHorizon
    /// - **Historical**: Bounded scan from checkpoint to end_time
    /// - **Continuous**: Unbounded scan from checkpoint (sensor mode) - should not return
    /// - **Snapshot**: Instantaneous state capture
    ///
    /// # Error Handling
    /// - Return `SatelliteError::Processing` for recoverable errors
    /// - Use `SatelliteError::Lifecycle` for initialization/shutdown issues
    /// - Database errors are typically non-recoverable
    ///
    /// # Performance Notes
    /// - Emit events incrementally via `StreamProcessorContext::emit_event()`
    /// - Use `args.max_events` to limit processing scope
    /// - Respect `args.dry_run` for testing scenarios
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport>;

    /// Get processor name for identification
    fn processor_name(&self) -> &str;

    /// Get processor type (ingestor or automaton)
    fn processor_type(&self) -> ProcessorType;

    /// Check processor capabilities
    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities::default()
    }

    /// Get the current checkpoint for this processor
    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint>;

    /// Health check
    async fn health_check(&self) -> SatelliteResult<bool> {
        Ok(true)
    }

    /// Process a batch of events (for automata in continuous mode)
    ///
    /// This method is called by the StreamProcessorRunner when running automata
    /// in continuous mode. The runner internally manages NATS consumption and
    /// feeds batches of events to this method.
    ///
    /// Default implementation returns NotImplemented error for non-automata.
    async fn process_event_batch(
        &mut self,
        events: Vec<Event<JsonValue>>,
    ) -> SatelliteResult<ProcessingStats> {
        let _ = events; // Suppress unused parameter warning
        Err(SatelliteError::General(eyre!(
            "This processor does not support event batch processing. Only automata should implement this method."
        )))
    }

    // Event filtering is now integrated into scan methods per satellite architecture

    /// Graceful shutdown
    async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!(processor = %self.processor_name(), "Stream processor shutting down");
        Ok(())
    }

    /// Estimate scan scope for planning purposes
    #[allow(unused_variables)]
    async fn estimate_scan_scope(
        &self,
        from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        Ok(ScanEstimate::default())
    }

    /// Get processor-specific configuration schema
    fn config_schema(&self) -> Option<serde_json::Value> {
        None
    }
}

/// Type of stream processor
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessorType {
    /// Ingestor: External World -> Event Stream
    Ingestor,
    /// Automaton: Event Stream -> DerivedEvent Stream
    Automaton,
}

impl std::fmt::Display for ProcessorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ingestor => write!(f, "ingestor"),
            Self::Automaton => write!(f, "automaton"),
        }
    }
}

/// Processor capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorCapabilities {
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

impl Default for ProcessorCapabilities {
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
pub struct StreamProcessorRunner<T: StatefulStreamProcessor> {
    processor: T,
    handles: Option<ProcessorHandles>,
    service_info: Option<ServiceInfo>,
    raw_config: Option<HashMap<String, serde_json::Value>>,
    work_dir_utf8: Option<Utf8PathBuf>,
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    shutdown_receiver: Option<tokio::sync::oneshot::Receiver<()>>,
    event_processor_handle: Option<tokio::task::JoinHandle<SatelliteResult<()>>>,
    event_processor_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    lease_manager: Option<Arc<crate::LeaseManager>>,
    confirmation_buffer: Option<Arc<crate::ConfirmationBuffer>>,
    processing_model: ProcessingModel,
}

impl<T: StatefulStreamProcessor + 'static> StreamProcessorRunner<T> {
    /// Create a new stream processor runner
    pub fn new(processor: T) -> Self {
        Self {
            processor,
            handles: None,
            service_info: None,
            raw_config: None,
            work_dir_utf8: None,
            checkpoint_manager: None,
            shutdown_receiver: None,
            event_processor_handle: None,
            event_processor_shutdown: None,
            lease_manager: None,
            confirmation_buffer: None,
            processing_model: ProcessingModel::StatelessWorker,
        }
    }

    /// Initialize the processor with a specific transport
    pub async fn initialize_with_transport(
        &mut self,
        service_name: String,
        raw_config: HashMap<String, serde_json::Value>,
        db_pool: PgPool,
        transport: EventTransport,
        work_dir: std::path::PathBuf,
        dry_run: bool,
    ) -> SatelliteResult<()> {
        // Create bounded event channel (capacity: 10000 for high-throughput event processing)
        let (event_sender_raw, event_receiver) = mpsc::unbounded_channel::<Event<JsonValue>>();

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

        // Initialize checkpoint manager
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            db_pool.clone(),
            service_name.clone(),
            "default".to_string(), // Default consumer group
            consumer_name.clone(), // Unique consumer name
        ));

        // NATS is the only transport
        let transport_type = "NATS";

        // Set up LeaseManager and ConfirmationBuffer for Automatons BEFORE creating context
        let processor_type = self.processor.processor_type();
        let (lease_manager_opt, confirmation_buffer_opt) =
            if matches!(processor_type, ProcessorType::Automaton) {
                match &transport {
                    EventTransport::Nats(nats_publisher) => {
                        let nats_client = nats_publisher.nats_client().clone();
                        let env = sinex_core::environment().clone();

                        let lease_config = crate::LeaseManagerConfig {
                            processor_name: service_name.clone(),
                            instance_id: format!(
                                "{}-{}",
                                gethostname::gethostname().to_string_lossy(),
                                std::process::id()
                            ),
                            lease_ttl: std::time::Duration::from_secs(30),
                            renewal_interval: std::time::Duration::from_secs(10),
                        };
                        let lease_manager =
                            Arc::new(crate::LeaseManager::new(nats_client, env, lease_config));

                        let confirmation_buffer = Arc::new(crate::ConfirmationBuffer::new(
                            std::time::Duration::from_secs(60),
                        ));

                        self.processing_model = ProcessingModel::LeaderStandby;
                        info!(
                        "Automaton configured with LeaderStandby model and confirmation buffering"
                    );

                        (Some(lease_manager), Some(confirmation_buffer))
                    }
                }
            } else {
                (None, None)
            };

        self.lease_manager = lease_manager_opt.clone();
        self.confirmation_buffer = confirmation_buffer_opt.clone();

        let event_emitter = EventEmitter::new(event_sender_raw, dry_run);
        let handles = ProcessorHandles::new(
            db_pool.clone(),
            checkpoint_manager.clone(),
            event_emitter.clone(),
            transport_for_context,
            lease_manager_opt,
            confirmation_buffer_opt,
        );

        let service_info = ServiceInfo::new(
            service_name.clone(),
            host.clone(),
            work_dir.clone(),
            dry_run,
        );
        let work_dir_utf8 = Utf8PathBuf::from_path_buf(work_dir)
            .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex"));

        let typed_config = if raw_config.is_empty() {
            T::Config::default()
        } else {
            let config_value = serde_json::to_value(&raw_config).map_err(|e| {
                SatelliteError::Configuration(format!("Failed to serialize processor config: {e}"))
            })?;
            serde_json::from_value(config_value).map_err(|e| {
                SatelliteError::Configuration(format!("Failed to parse processor config: {e}"))
            })?
        };

        let init_context = ProcessorInitContext::new(
            typed_config,
            raw_config.clone(),
            service_info.clone(),
            handles.clone(),
            work_dir_utf8.clone(),
        );

        self.processor.initialize_with_runtime(init_context).await?;

        self.handles = Some(handles);
        self.service_info = Some(service_info);
        self.raw_config = Some(raw_config.clone());
        self.work_dir_utf8 = Some(work_dir_utf8);
        self.checkpoint_manager = Some(checkpoint_manager.clone());

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
    ) -> SatelliteResult<ScanReport> {
        if self.handles.is_none() {
            return Err(SatelliteError::Lifecycle(
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
    pub async fn run_service(&mut self) -> SatelliteResult<()> {
        if self.handles.is_none() {
            return Err(SatelliteError::Lifecycle(
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
            ProcessorType::Ingestor => {
                // Ingestor startup sequence: Snapshot -> Gap-fill -> Continuous
                self.run_ingestor_startup_sequence().await
            }
            ProcessorType::Automaton => {
                // Automaton startup: consume events from NATS streams
                self.run_automaton_continuous_mode().await
            }
        }
    }

    /// Run ingestor startup sequence (Snapshot -> Gap-fill -> Continuous)
    async fn run_ingestor_startup_sequence(&mut self) -> SatelliteResult<()> {
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
    async fn run_automaton_continuous_mode(&mut self) -> SatelliteResult<()> {
        info!("Starting automaton continuous mode");

        // Start LeaseManager if configured (for LeaderStandby model)
        if let Some(lease_manager) = &self.lease_manager {
            info!("Starting lease manager for leader election");
            lease_manager.start().await?;

            // Wait for initial lease acquisition
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let status = lease_manager.status().await;
            info!(?status, "Lease manager started with initial status");
        }

        // Get current checkpoint to resume from previous state if available
        let current_checkpoint = self.processor.current_checkpoint().await?;
        let capabilities = self.processor.capabilities();

        // Automata primarily process events in continuous mode
        // They consume events from message queues or databases
        if capabilities.supports_continuous {
            info!("Starting continuous event processing for automaton");

            // Check lease status before processing (for LeaderStandby model)
            if self.processing_model == ProcessingModel::LeaderStandby {
                if let Some(lease_manager) = &self.lease_manager {
                    let status = lease_manager.status().await;
                    if status != crate::LeaseStatus::Leader {
                        info!(?status, "Not leader, skipping processing");
                        return Ok(());
                    }
                    info!("Confirmed as leader, proceeding with processing");
                }
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

    async fn run_automaton_event_bridge(&mut self, from: Checkpoint) -> SatelliteResult<()> {
        let handles = self.handles.as_ref().ok_or_else(|| {
            SatelliteError::Lifecycle("Runner handles not initialized".to_string())
        })?;

        let db_pool = handles.db_pool().clone();
        let transport = handles.transport().clone();

        let service_name = self
            .service_info
            .as_ref()
            .map(|info| info.service_name().to_string())
            .unwrap_or_else(|| self.processor.processor_name().to_string());

        let (sender, mut receiver) = mpsc::unbounded_channel::<ProvisionalEvent>();
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

            match Self::fetch_persisted_event(&db_pool, &event_id).await? {
                Some(event) => match self.processor.process_event_batch(vec![event]).await {
                    Ok(stats) => {
                        processed_events += stats.processed as u64;
                    }
                    Err(err) => {
                        warn!(error = %err, "Automaton batch processing failed");
                    }
                },
                None => {
                    warn!(
                        "Confirmed event {:?} missing from database; skipping",
                        event_id
                    );
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
    ) -> SatelliteResult<Option<Event<JsonValue>>> {
        let event_id_str = event_id.to_string();
        pool.events()
            .get_by_id(event_id.clone())
            .await
            .map_err(|err| {
                SatelliteError::Processing(format!(
                    "Failed to load confirmed event {} from database: {}",
                    event_id_str, err
                ))
            })
    }
    // }

    // /// Read a batch of events from NATS (internal helper)
    // async fn read_event_batch_from_nats(
    //     &self,
    //     nats_consumer: &mut NatsStreamConsumer,
    // ) -> SatelliteResult<Vec<Event<JsonValue>>> {
    //     // REMOVED: This method used NatsStreamConsumer which has been deprecated
    //     Err(SatelliteError::Processing(
    //         "NATS batch reading not yet implemented after NatsStreamConsumer removal".to_string()
    //     ))
    // }

    /// Get processor capabilities
    pub fn get_capabilities(&self) -> ProcessorCapabilities {
        self.processor.capabilities()
    }

    /// Get scan estimate
    pub async fn estimate_scan_scope(
        &self,
        from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        self.processor.estimate_scan_scope(from, until, args).await
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!("Shutting down stream processor runner");

        // Stop LeaseManager if running
        if let Some(lease_manager) = &self.lease_manager {
            info!("Stopping lease manager");
            lease_manager.stop().await;
        }

        self.processor.shutdown().await
    }
}
