//! Unified Stream Processor Architecture for Sinex
//!
//! This module implements the "Deep Symmetry" vision from Part 16 of the design discussion,
//! unifying ingestors and automata as both being "Stateful Stream Processors" with a single
//! scan(from: Checkpoint, until: TimeHorizon) interface.

use crate::{
    checkpoint::CheckpointManager, grpc_client::IngestClient, SatelliteError, SatelliteResult,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_db::SqlxPgPool as PgPool;
use sinex_events::RawEvent;
use sinex_ulid::Ulid;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Time horizon defines the scope and mode of scanning operations.
///
/// This enum controls how a stream processor scans events:
/// - `Historical`: Bounded scan from checkpoint to a specific end time
/// - `Continuous`: Unbounded scan for real-time streaming (sensor mode)
/// - `Snapshot`: Instantaneous state capture for point-in-time analysis
///
/// # Examples
/// ```
/// use sinex_satellite_sdk::{TimeHorizon, Checkpoint};
/// use chrono::{DateTime, Utc};
///
/// // Historical scan: process events from last checkpoint to noon today
/// let historical = TimeHorizon::Historical {
///     end_time: DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z").unwrap().with_timezone(&Utc)
/// };
///
/// // Continuous scan: process events indefinitely from checkpoint
/// let continuous = TimeHorizon::Continuous;
///
/// // Snapshot scan: capture current state only
/// let snapshot = TimeHorizon::Snapshot;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeHorizon {
    /// Historical scan: Process from checkpoint up to a defined point in the past
    Historical {
        /// End time for historical processing
        end_time: DateTime<Utc>,
    },
    /// Continuous scan: Process from checkpoint and continue forever (sensor mode)
    Continuous,
    /// Snapshot scan: Instantaneous scan for sources like udev or systemd
    Snapshot,
}

impl TimeHorizon {
    /// Check if this is a continuous (streaming) operation.
    ///
    /// Returns `true` for `TimeHorizon::Continuous`, which indicates
    /// unbounded processing that should continue indefinitely.
    pub fn is_continuous(&self) -> bool {
        matches!(self, TimeHorizon::Continuous)
    }

    /// Check if this is a bounded operation.
    ///
    /// Returns `true` for `Historical` and `Snapshot` modes, which have
    /// defined endpoints and will eventually complete.
    pub fn is_bounded(&self) -> bool {
        matches!(self, TimeHorizon::Historical { .. } | TimeHorizon::Snapshot)
    }

    /// Get the end time if applicable.
    ///
    /// Returns `Some(end_time)` for `Historical` mode, `None` for other modes.
    /// Used by processors to determine when to stop processing.
    pub fn end_time(&self) -> Option<DateTime<Utc>> {
        match self {
            TimeHorizon::Historical { end_time } => Some(*end_time),
            _ => None,
        }
    }
}

/// Unified checkpoint representation for tracking progress across both ingestors and automata.
///
/// Checkpoints enable resumable processing by storing the last processed position.
/// Different checkpoint types support various data sources:
/// - `External`: For ingestors tracking external system state (files, logs, etc.)
/// - `Internal`: For automata tracking processed event IDs in the event stream
/// - `Stream`: For Redis Stream-based message processing
/// - `Timestamp`: For time-based processing resumption
///
/// # Examples
/// ```
/// use sinex_satellite_sdk::Checkpoint;
/// use sinex_ulid::Ulid;
/// use chrono::Utc;
///
/// // External checkpoint for file position
/// let file_pos = Checkpoint::external(
///     serde_json::json!({"file_offset": 1024, "line_number": 42}),
///     "Processing from line 42 of /var/log/app.log"
/// );
///
/// // Internal checkpoint for event processing
/// let event_id = Ulid::new();
/// let internal = Checkpoint::internal(event_id, 150);
///
/// // Stream checkpoint for Redis processing
/// let stream = Checkpoint::stream("1234567890-0", Some(event_id));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Checkpoint {
    /// No checkpoint - start from beginning
    None,
    /// External position for ingestors (file offset, timestamp, log line, etc.)
    External {
        /// External system identifier (varies by source)
        position: serde_json::Value,
        /// Human-readable description
        description: String,
    },
    /// Internal event ID for automata (ULID of last processed event)
    Internal {
        /// Last processed event ULID
        event_id: Ulid,
        /// Message count for verification
        message_count: u64,
    },
    /// Redis Stream message ID for stream-based processing
    Stream {
        /// Redis Stream message ID
        message_id: String,
        /// Associated event ULID if known
        event_id: Option<Ulid>,
    },
    /// Timestamp-based checkpoint for time-ordered sources
    Timestamp {
        /// Last processed timestamp
        timestamp: DateTime<Utc>,
        /// Optional metadata
        metadata: Option<serde_json::Value>,
    },
}

impl Checkpoint {
    /// Create a checkpoint from an external position.
    ///
    /// Used by ingestors to track progress in external systems.
    /// The position can be any JSON-serializable value representing
    /// the current state (file offset, timestamp, log line number, etc.).
    ///
    /// # Examples
    /// ```
    /// use sinex_satellite_sdk::Checkpoint;
    ///
    /// // File position
    /// let pos = Checkpoint::external(
    ///     serde_json::json!({"file": "/var/log/app.log", "offset": 1024}),
    ///     "Processing from byte 1024 of app.log"
    /// );
    ///
    /// // Database sequence
    /// let seq = Checkpoint::external(
    ///     serde_json::json!({"table": "events", "last_id": 12345}),
    ///     "Last processed event ID: 12345"
    /// );
    /// ```
    pub fn external(position: serde_json::Value, description: impl Into<String>) -> Self {
        Self::External {
            position,
            description: description.into(),
        }
    }

    /// Create a checkpoint from an event ULID.
    ///
    /// Used by automata to track progress through the internal event stream.
    /// The event_id represents the last processed event, and message_count
    /// provides verification and debugging information.
    ///
    /// # Parameters
    /// - `event_id`: ULID of the last successfully processed event
    /// - `message_count`: Total number of messages processed (for verification)
    pub fn internal(event_id: Ulid, message_count: u64) -> Self {
        Self::Internal {
            event_id,
            message_count,
        }
    }

    /// Create a checkpoint from a Redis Stream message ID.
    ///
    /// Used for Redis Stream-based processing. The message_id follows
    /// Redis Stream format (e.g., "1234567890-0"), and event_id
    /// provides correlation with the internal event stream.
    ///
    /// # Parameters
    /// - `message_id`: Redis Stream message ID (format: "timestamp-sequence")
    /// - `event_id`: Optional ULID of the corresponding internal event
    pub fn stream(message_id: impl Into<String>, event_id: Option<Ulid>) -> Self {
        Self::Stream {
            message_id: message_id.into(),
            event_id,
        }
    }

    /// Create a checkpoint from a timestamp.
    ///
    /// Used for time-based processing resumption. Suitable for sources
    /// that can be queried by timestamp (logs, database tables, etc.).
    ///
    /// # Parameters
    /// - `timestamp`: The last processed timestamp
    /// - `metadata`: Optional source-specific metadata for context
    pub fn timestamp(timestamp: DateTime<Utc>, metadata: Option<serde_json::Value>) -> Self {
        Self::Timestamp {
            timestamp,
            metadata,
        }
    }

    /// Get a human-readable description of this checkpoint
    pub fn description(&self) -> String {
        match self {
            Checkpoint::None => "start".to_string(),
            Checkpoint::External { description, .. } => description.clone(),
            Checkpoint::Internal {
                event_id,
                message_count,
            } => {
                format!("event {} (#{message_count})", event_id)
            }
            Checkpoint::Stream {
                message_id,
                event_id,
            } => {
                if let Some(event_id) = event_id {
                    format!("stream {} (event {})", message_id, event_id)
                } else {
                    format!("stream {}", message_id)
                }
            }
            Checkpoint::Timestamp { timestamp, .. } => {
                format!("timestamp {}", timestamp.format("%Y-%m-%d %H:%M:%S UTC"))
            }
        }
    }
}

/// Stream of events produced by scanning operations
pub type EventStream = mpsc::UnboundedReceiver<RawEvent>;

/// Sender for events during scanning operations
pub type EventSender = mpsc::UnboundedSender<RawEvent>;

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
#[derive(Debug)]
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

    /// Ingest client for sending events
    pub ingest_client: IngestClient,

    /// Checkpoint manager for state persistence
    pub checkpoint_manager: CheckpointManager,

    /// Processor-specific configuration
    pub config: HashMap<String, serde_json::Value>,

    /// Event sender channel for scan operations
    pub event_sender: EventSender,
}

impl StreamProcessorContext {
    /// Send an event through the event channel
    #[sinex_macros::auto_event_metrics(event_type = "emit")]
    pub async fn emit_event(&self, event: RawEvent) -> SatelliteResult<()> {
        if self.dry_run {
            info!(
                source = %event.source,
                event_type = %event.event_type,
                "DRY RUN: Would emit event"
            );
            return Ok(());
        }

        self.event_sender
            .send(event)
            .map_err(|_| SatelliteError::General(anyhow::anyhow!("Event channel closed")))?;

        Ok(())
    }

    /// Send multiple events through the event channel
    pub async fn emit_events(&self, events: Vec<RawEvent>) -> SatelliteResult<()> {
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
/// - **Ingestors**: External World → RawEvent Stream (e.g., file watchers, log parsers)
/// - **Automata**: RawEvent Stream → DerivedEvent Stream (e.g., command canonicalizers)
///
/// # Implementation Notes
/// - Implementations must be thread-safe (`Send + Sync`)
/// - The `scan()` method is the core interface - other methods provide metadata
/// - Checkpointing is handled externally via `StreamProcessorContext`
/// - Graceful shutdown should be implemented in `shutdown()`
///
/// # Examples
/// ```ignore
/// use sinex_satellite_sdk::*;
/// use async_trait::async_trait;
///
/// struct MyIngestor;
///
/// #[async_trait]
/// impl StatefulStreamProcessor for MyIngestor {
///     async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()> {
///         // Initialize with context
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
    /// Initialize the processor with the given context
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()>;

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

    /// Graceful shutdown
    async fn shutdown(&mut self) -> SatelliteResult<()> {
        info!(processor = %self.processor_name(), "Stream processor shutting down");
        Ok(())
    }

    /// Estimate scan scope for planning purposes
    async fn estimate_scan_scope(
        &self,
        from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        let _ = (from, until, args);
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
    /// Ingestor: External World -> RawEvent Stream
    Ingestor,
    /// Automaton: RawEvent Stream -> DerivedEvent Stream
    Automaton,
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
    context: Option<StreamProcessorContext>,
    shutdown_receiver: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl<T: StatefulStreamProcessor + 'static> StreamProcessorRunner<T> {
    /// Create a new stream processor runner
    pub fn new(processor: T) -> Self {
        Self {
            processor,
            context: None,
            shutdown_receiver: None,
        }
    }

    /// Initialize the processor with configuration
    pub async fn initialize(
        &mut self,
        service_name: String,
        config: HashMap<String, serde_json::Value>,
        db_pool: PgPool,
        ingest_client: IngestClient,
        work_dir: std::path::PathBuf,
        dry_run: bool,
    ) -> SatelliteResult<()> {
        // Create event channel
        let (event_sender, _event_receiver) = mpsc::unbounded_channel::<RawEvent>();

        // Create shutdown channel
        let (_shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        self.shutdown_receiver = Some(shutdown_receiver);

        // Get hostname
        let host = gethostname::gethostname().to_string_lossy().to_string();

        // Initialize checkpoint manager
        let checkpoint_manager = CheckpointManager::new(
            db_pool.clone(),
            service_name.clone(),
            "default".to_string(), // Default consumer group
            format!("{}-{}", host, std::process::id()), // Unique consumer name
        );

        // Create context
        let context = StreamProcessorContext {
            service_name: service_name.clone(),
            host,
            work_dir,
            dry_run,
            db_pool,
            ingest_client,
            checkpoint_manager,
            config,
            event_sender,
        };

        // Initialize the processor
        self.processor.initialize(context).await?;

        info!(
            service = %service_name,
            processor = %self.processor.processor_name(),
            processor_type = ?self.processor.processor_type(),
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
        if self.context.is_none() {
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
        if self.context.is_none() {
            return Err(SatelliteError::Lifecycle(
                "Stream processor not initialized".to_string(),
            ));
        }

        info!(
            processor = %self.processor.processor_name(),
            "Starting service with startup sequence"
        );

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

        // Phase 3: Continuous processing
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
        self.processor.shutdown().await
    }
}
