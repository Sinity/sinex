//! Unified filesystem processor using sensd MaterialSliceStream
//!
//! This module implements filesystem monitoring through sensd's source material capture system.
//! Instead of directly monitoring filesystem events, it submits TreeWatch jobs to sensd
//! and processes the resulting MaterialSliceStream to generate events with proper provenance.

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::{
    db::models::{event::OffsetKind, Event, Provenance},
    events::{
        DirCreatedPayload, DirDeletedPayload, DirDiscoveredPayload, FileCreatedPayload,
        FileDeletedPayload, FileDiscoveredPayload, FileModifiedPayload, FileMovedPayload,
    },
    types::{
        domain::{EventSource, EventType, SanitizedPath},
        Id, Ulid,
    },
    JsonValue,
};
use sinex_satellite_sdk::{
    checkpoint::CheckpointManager,
    cli::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        MissingItem, SourceState,
    },
    stage_as_you_go::StageAsYouGoContext,
    stream_processor::{
        Checkpoint, ProcessorCapabilities, ProcessorType, ScanArgs, ScanEstimate, ScanReport,
        StatefulStreamProcessor, StreamProcessorContext, TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn};
use validator::{Validate, ValidationError};

#[cfg(test)]
mod config_validation_tests;

// Use shared MaterialSlice from sensd crate
use sinex_sensd::material_stream::MaterialSlice;

/// Filesystem monitoring configuration for sensd integration
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct FilesystemConfig {
    /// Directories to monitor for filesystem changes
    #[validate(length(min = 1, message = "At least one watch path must be specified"))]
    pub watch_paths: Vec<String>,

    /// Maximum directory traversal depth (None = unlimited)
    #[validate(custom(
        function = "validate_max_depth",
        message = "Max depth must be reasonable (1-100)"
    ))]
    pub max_depth: Option<usize>,

    /// Follow symbolic links during monitoring
    pub follow_symlinks: bool,

    /// Batch size for processing material slices
    #[validate(range(min = 1, max = 1000, message = "Batch size must be between 1 and 1000"))]
    pub batch_size: usize,

    /// Processing interval in milliseconds
    #[validate(range(
        min = 100,
        max = 60000,
        message = "Processing interval must be between 100ms and 60 seconds"
    ))]
    pub processing_interval_ms: u64,
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            watch_paths: vec![],
            max_depth: Some(10),
            follow_symlinks: false,
            batch_size: 100,
            processing_interval_ms: 1000,
        }
    }
}

impl FilesystemConfig {
    /// Validate the configuration and return detailed error messages
    pub fn validate_config(&self) -> Result<(), String> {
        use validator::Validate as ValidateTrait;

        ValidateTrait::validate(self)
            .map_err(|e| format!("Filesystem configuration validation failed: {:?}", e))
    }
}

/// Custom validation functions
fn validate_max_depth(depth: usize) -> Result<(), ValidationError> {
    if depth == 0 {
        return Err(ValidationError::new("depth_zero"));
    }
    if depth > 100 {
        return Err(ValidationError::new("depth_too_large"));
    }
    Ok(())
}

/// Filesystem state snapshot for exploration and diagnostics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemState {
    /// When the snapshot was taken
    pub captured_at: DateTime<Utc>,

    /// Active sensd jobs for filesystem monitoring
    pub active_jobs: HashMap<Ulid, String>,

    /// Directories being monitored
    pub watch_paths: Vec<String>,

    /// Last processed material offsets per job
    pub job_offsets: HashMap<Ulid, i64>,
}

/// Unified filesystem processor using sensd MaterialSliceStream
pub struct FilesystemProcessor {
    /// Current processing context
    context: Option<StreamProcessorContext>,

    /// Filesystem monitoring configuration
    config: FilesystemConfig,

    /// Database connection pool
    db_pool: Option<PgPool>,

    /// Active sensd jobs
    active_jobs: HashMap<Ulid, String>,

    /// Last captured filesystem state for snapshots
    last_state: Option<FilesystemState>,

    /// Checkpoint manager for state persistence
    checkpoint_manager: Option<CheckpointManager>,

    /// Stage-as-you-go context for real-time provenance
    stage_context: Option<StageAsYouGoContext>,

    /// Event channel for sending processed events
    event_sender: Option<mpsc::Sender<Event<JsonValue>>>,
}

impl FilesystemProcessor {
    /// Create a new unified filesystem processor
    pub fn new() -> Self {
        Self {
            context: None,
            config: FilesystemConfig::default(),
            db_pool: None,
            active_jobs: HashMap::new(),
            last_state: None,
            checkpoint_manager: None,
            stage_context: None,
            event_sender: None,
        }
    }

    /// Create processor with custom configuration
    pub fn with_config(config: FilesystemConfig) -> Self {
        Self {
            context: None,
            config,
            db_pool: None,
            active_jobs: HashMap::new(),
            last_state: None,
            checkpoint_manager: None,
            stage_context: None,
            event_sender: None,
        }
    }

    /// Submit a TreeWatch job to sensd for filesystem monitoring
    #[instrument(skip(self), fields(processor = "filesystem", path = %path))]
    async fn submit_tree_watch_job(&mut self, path: &str) -> SatelliteResult<Ulid> {
        let db_pool = self
            .db_pool
            .as_ref()
            .ok_or_else(|| SatelliteError::General(eyre!("Database pool not initialized")))?;

        let job_id = Ulid::new();
        let source_identifier = format!("fs-watch:{}", path);

        // Insert TreeWatch job into sensor_jobs table (current canonical schema)
        sqlx::query!(
            r#"
            INSERT INTO raw.sensor_jobs (
                id, sensor_type, target_uri, config
            )
            VALUES ($1::ulid, 'tree_watch', $2, $3)
            "#,
            job_id as Ulid,
            path,
            json!({
                "mode": "continuous",
                "recursive": true,
                "follow_symlinks": self.config.follow_symlinks,
                "max_depth": self.config.max_depth,
                "source_identifier": source_identifier
            }),
        )
        .execute(db_pool)
        .await
        .map_err(|e| SatelliteError::General(eyre!("Failed to submit TreeWatch job: {}", e)))?;

        info!("Submitted TreeWatch job {} for path: {}", job_id, path);
        self.active_jobs.insert(job_id, path.to_string());

        Ok(job_id)
    }

    /// Process a material slice from sensd into filesystem events
    #[instrument(skip(self, slice), fields(processor = "filesystem", material_id = %slice.material_id, offset_start = slice.offset_start, offset_end = slice.offset_end))]
    async fn process_material_slice(
        &self,
        slice: MaterialSlice,
    ) -> SatelliteResult<Vec<Event<JsonValue>>> {
        let mut events = Vec::new();

        // Parse metadata from the slice
        let path = slice
            .metadata
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let event_kind = slice
            .metadata
            .get("event_kind")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let file_size = slice
            .metadata
            .get("size")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let is_directory = slice
            .metadata
            .get("is_directory")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Create event with material provenance
        let provenance = Provenance::Material {
            id: Id::from(slice.material_id),
            anchor_byte: slice.offset_start,
            offset_kind: OffsetKind::Byte,
            offset_start: Some(slice.offset_start),
            offset_end: Some(slice.offset_end),
        };

        let sanitized_path = SanitizedPath::new(path.clone());
        let timestamp = slice.ts_capture_start;

        // Create typed event based on event type
        let event: Event<JsonValue> = if is_directory {
            match event_kind {
                kind if kind.contains("Create") => {
                    let payload = DirCreatedPayload {
                        path: sanitized_path,
                        created_at: timestamp,
                    };
                    Event::new(payload, provenance.clone())
                        .at_time(timestamp)
                        .to_json_event()?
                }
                kind if kind.contains("Remove") => {
                    let payload = DirDeletedPayload {
                        path: sanitized_path,
                        deleted_at: timestamp,
                    };
                    Event::new(payload, provenance.clone())
                        .at_time(timestamp)
                        .to_json_event()?
                }
                _ => {
                    let payload = DirDiscoveredPayload {
                        path: sanitized_path,
                        modified_at: timestamp,
                    };
                    Event::new(payload, provenance.clone())
                        .at_time(timestamp)
                        .to_json_event()?
                }
            }
        } else {
            match event_kind {
                kind if kind.contains("Create") => {
                    let payload = FileCreatedPayload {
                        path: sanitized_path,
                        size: file_size as u64,
                        created_at: timestamp,
                        permissions: None,
                    };
                    Event::new(payload, provenance.clone())
                        .at_time(timestamp)
                        .to_json_event()?
                }
                kind if kind.contains("Modify") => {
                    let payload = FileModifiedPayload {
                        path: sanitized_path,
                        size: file_size as u64,
                        modified_at: timestamp,
                        modification_type: event_kind.to_string(),
                    };
                    Event::new(payload, provenance.clone())
                        .at_time(timestamp)
                        .to_json_event()?
                }
                kind if kind.contains("Remove") => {
                    let payload = FileDeletedPayload {
                        path: sanitized_path,
                        deleted_at: timestamp,
                    };
                    Event::new(payload, provenance.clone())
                        .at_time(timestamp)
                        .to_json_event()?
                }
                kind if kind.contains("Rename") => {
                    // For rename, we need both old and new paths
                    // This is a simplification - in reality we'd need to track the rename
                    let payload = FileMovedPayload {
                        old_path: sanitized_path.clone(),
                        new_path: sanitized_path,
                        moved_at: timestamp,
                    };
                    Event::new(payload, provenance.clone())
                        .at_time(timestamp)
                        .to_json_event()?
                }
                _ => {
                    let payload = FileDiscoveredPayload {
                        path: sanitized_path,
                        size: file_size as u64,
                        modified_at: timestamp,
                        permissions: None,
                    };
                    Event::new(payload, provenance.clone())
                        .at_time(timestamp)
                        .to_json_event()?
                }
            }
        };

        events.push(event);

        debug!(
            "Generated filesystem event for path: {} (material: {}, offsets: {}-{})",
            path, slice.material_id, slice.offset_start, slice.offset_end
        );

        Ok(events)
    }

    /// Create material stream for the given material_id
    async fn create_material_stream(
        &self,
        material_id: Ulid,
    ) -> Result<impl tokio_stream::Stream<Item = Result<MaterialSlice>> + '_> {
        let db_pool = self
            .db_pool
            .as_ref()
            .ok_or_else(|| eyre!("Database pool not initialized"))?;

        let stream = async_stream::stream! {
            let mut offset = 0i64;

            loop {
                let slices = sqlx::query!(
                    r#"
                    SELECT 
                        tl.source_material_id as "material_id: Ulid",
                        tl.offset_start,
                        tl.offset_end,
                        tl.ts_capture,
                        NULL::text as note,
                        NULL::bytea as "inline_data?: Vec<u8>"
                    FROM raw.temporal_ledger tl
                    LEFT JOIN raw.source_material_registry sm 
                        ON sm.id = tl.source_material_id
                    WHERE tl.source_material_id = $1::ulid
                    AND tl.offset_start >= $2
                    ORDER BY tl.offset_start
                    LIMIT $3
                    "#,
                    material_id as Ulid,
                    offset,
                    self.config.batch_size as i64,
                )
                .fetch_all(db_pool)
                .await;

                match slices {
                    Ok(records) => {
                        if records.is_empty() {
                            break;
                        }

                        for record in records {
                            let data = record.inline_data.unwrap_or_default();

                            let slice = MaterialSlice {
                                material_id: record.material_id,
                                offset_start: record.offset_start,
                                offset_end: record.offset_end,
                                ts_capture_start: record.ts_capture,
                                ts_capture_end: record.ts_capture,
                                data,
                                metadata: serde_json::from_str(&record.note.unwrap_or("{}".to_string())).unwrap_or_default(),
                            };

                            offset = record.offset_end;
                            yield Ok(slice);
                        }
                    }
                    Err(e) => {
                        yield Err(eyre!("Failed to fetch slices: {}", e));
                        break;
                    }
                }
            }
        };

        Ok(stream)
    }

    /// Process completed sensd jobs and generate events from their materials
    #[instrument(skip(self), fields(processor = "filesystem"))]
    async fn process_completed_jobs(&mut self) -> SatelliteResult<u64> {
        let db_pool = self
            .db_pool
            .as_ref()
            .ok_or_else(|| SatelliteError::General(eyre!("Database pool not initialized")))?;

        let mut total_events = 0;

        // Query for retired TreeWatch jobs with matching source materials not yet processed
        let completed_jobs = sqlx::query!(
            r#"
            SELECT 
                sj.id as "job_id: Ulid",
                sm.id as "material_id: Ulid",
                sj.target_uri
            FROM raw.sensor_jobs sj
            LEFT JOIN raw.source_material_registry sm 
                ON sm.source_identifier LIKE '%' || sj.target_uri || '%'
            WHERE sj.status = 'retired'
              AND sj.sensor_type = 'tree_watch'
              AND sm.id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM core.events 
                WHERE source_material_id = sm.id
              )
            ORDER BY sj.updated_at DESC
            LIMIT 10
            "#,
        )
        .fetch_all(db_pool)
        .await
        .map_err(|e| SatelliteError::General(eyre!("Failed to query completed jobs: {}", e)))?;

        for job in completed_jobs {
            let material_id = job.material_id;
            info!(
                "Processing material {} from TreeWatch job {} (path: {})",
                material_id, job.job_id, job.target_uri
            );

            // Create material stream for this job's output
            let stream = self
                .create_material_stream(material_id)
                .await
                .map_err(|e| SatelliteError::General(e))?;
            let mut stream = stream;
            tokio::pin!(stream);

            // Process all slices from this material
            while let Some(slice_result) = stream.next().await {
                match slice_result {
                    Ok(slice) => {
                        // Convert slice to filesystem events
                        let events = self.process_material_slice(slice).await?;

                        // Send events through the context or store them
                        for event in events {
                            if let Some(ref context) = self.context {
                                context.emit_event(event).await?;
                                total_events += 1;
                            } else if let Some(ref sender) = self.event_sender {
                                sender.send(event).await.map_err(|_| {
                                    SatelliteError::General(eyre!("Failed to send event"))
                                })?;
                                total_events += 1;
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Error processing slice from material {}: {}",
                            material_id, e
                        );
                    }
                }
            }

            info!(
                "Completed processing material {} with {} events",
                material_id, total_events
            );
        }

        Ok(total_events)
    }

    /// Take a snapshot of current filesystem state
    #[instrument(skip(self), fields(processor = "filesystem", jobs_count = self.active_jobs.len()))]
    async fn take_snapshot(&mut self) -> SatelliteResult<FilesystemState> {
        let state = FilesystemState {
            captured_at: Utc::now(),
            active_jobs: self.active_jobs.clone(),
            watch_paths: self.config.watch_paths.clone(),
            job_offsets: HashMap::new(), // Would be populated from database in real implementation
        };

        self.last_state = Some(state.clone());
        Ok(state)
    }

    /// Start continuous monitoring by submitting jobs and processing materials
    #[instrument(skip(self), fields(processor = "filesystem", paths_count = self.config.watch_paths.len()))]
    async fn start_continuous_monitoring(&mut self) -> SatelliteResult<()> {
        info!("Starting continuous filesystem monitoring via sensd");

        // Submit TreeWatch jobs for all configured paths
        for path in &self.config.watch_paths.clone() {
            match self.submit_tree_watch_job(path).await {
                Ok(job_id) => {
                    info!(
                        "Successfully submitted TreeWatch job {} for path: {}",
                        job_id, path
                    );
                }
                Err(e) => {
                    error!("Failed to submit TreeWatch job for path {}: {}", path, e);
                    return Err(e);
                }
            }
        }

        // Start processing loop
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(
            self.config.processing_interval_ms,
        ));

        info!(
            "Starting material processing loop (interval: {}ms)",
            self.config.processing_interval_ms
        );

        loop {
            interval.tick().await;

            match self.process_completed_jobs().await {
                Ok(event_count) => {
                    if event_count > 0 {
                        debug!("Processed {} events from completed sensd jobs", event_count);
                    }
                }
                Err(e) => {
                    error!("Error processing completed jobs: {}", e);
                    // Continue processing despite errors
                }
            }
        }
    }
}

impl Default for FilesystemProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StatefulStreamProcessor for FilesystemProcessor {
    type Config = FilesystemConfig;

    #[instrument(skip(self, ctx), fields(processor = "filesystem", service = %ctx.service_name))]
    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        config: Self::Config,
    ) -> SatelliteResult<()> {
        info!(
            processor = self.processor_name(),
            service = %ctx.service_name,
            "Initializing filesystem processor with sensd integration"
        );

        // Store configuration
        self.config = config;

        // Initialize database connection
        self.db_pool = Some(ctx.db_pool.clone());

        // Initialize checkpoint manager
        self.checkpoint_manager = Some(ctx.checkpoint_manager.clone());

        // Initialize stage-as-you-go context
        self.stage_context = Some(StageAsYouGoContext::new(
            ctx.db_pool.clone(),
            ctx.ingest_client.clone(),
        ));

        // Set up default watch paths if none specified
        if self.config.watch_paths.is_empty() {
            if let Some(home) = dirs::home_dir() {
                self.config.watch_paths = vec![
                    home.join("Documents").to_string_lossy().to_string(),
                    home.join("Downloads").to_string_lossy().to_string(),
                    home.join("Desktop").to_string_lossy().to_string(),
                ];
                info!("Using default watch paths: {:?}", self.config.watch_paths);
            } else {
                return Err(SatelliteError::General(eyre!(
                    "No watch paths configured and could not determine home directory"
                )));
            }
        }

        // Validate configuration
        self.config.validate_config().map_err(|e| {
            SatelliteError::General(eyre!("Configuration validation failed: {}", e))
        })?;

        info!(
            watch_paths = ?self.config.watch_paths,
            max_depth = ?self.config.max_depth,
            follow_symlinks = self.config.follow_symlinks,
            batch_size = self.config.batch_size,
            processing_interval_ms = self.config.processing_interval_ms,
            "Filesystem processor configuration"
        );

        self.context = Some(ctx);
        Ok(())
    }

    #[instrument(skip(self), fields(processor = "filesystem", from = %from.description(), dry_run = args.dry_run, targets_count = args.targets.len()))]
    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = std::time::Instant::now();
        let mut events_processed = 0;
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut warnings = Vec::new();

        info!(
            processor = self.processor_name(),
            from = %from.description(),
            until = ?until,
            targets = args.targets.len(),
            dry_run = args.dry_run,
            "Starting filesystem scan via sensd"
        );

        match until {
            TimeHorizon::Snapshot => {
                // Take current state snapshot
                let _state = self.take_snapshot().await?;

                // Process any completed jobs
                if !args.dry_run {
                    events_processed = self.process_completed_jobs().await?;
                }

                successful_targets = if args.targets.is_empty() {
                    self.config.watch_paths.clone()
                } else {
                    args.targets.clone()
                };
            }

            TimeHorizon::Historical { end_time: _ } => {
                warnings.push(
                    "Historical filesystem scanning via sensd depends on material capture times"
                        .to_string(),
                );

                // Process any completed jobs within the time range
                if !args.dry_run {
                    events_processed = self.process_completed_jobs().await?;
                }

                successful_targets = if args.targets.is_empty() {
                    self.config.watch_paths.clone()
                } else {
                    args.targets.clone()
                };
            }

            TimeHorizon::Continuous => {
                // Start continuous monitoring
                info!("Starting continuous filesystem monitoring via sensd");
                self.start_continuous_monitoring().await?;
                events_processed = 0; // Continuous monitoring runs indefinitely
            }
        }

        let final_checkpoint = Checkpoint::timestamp(Utc::now(), None);

        Ok(ScanReport {
            events_processed,
            duration: start_time.elapsed(),
            final_checkpoint,
            time_range: Some((
                match &from {
                    Checkpoint::Timestamp { timestamp, .. } => *timestamp,
                    _ => Utc::now() - chrono::Duration::hours(1),
                },
                Utc::now(),
            )),
            processor_stats: HashMap::from([
                ("active_jobs".to_string(), self.active_jobs.len() as u64),
                (
                    "watch_paths".to_string(),
                    self.config.watch_paths.len() as u64,
                ),
                (
                    "successful_targets".to_string(),
                    successful_targets.len() as u64,
                ),
                ("failed_targets".to_string(), failed_targets.len() as u64),
            ]),
            successful_targets,
            failed_targets,
            warnings,
        })
    }

    fn processor_name(&self) -> &str {
        "fs-processor-sensd"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: true,
            supports_historical: true,
            supports_snapshot: true,
            supports_interactive: false,
            max_scan_size: Some(100000),
            supports_concurrent: false,
        }
    }

    #[instrument(skip(self), fields(processor = "filesystem"))]
    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::timestamp(Utc::now(), None))
    }

    #[instrument(skip(self, args), fields(processor = "filesystem", from = %_from.description(), targets_count = args.targets.len()))]
    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        until: &TimeHorizon,
        args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        let estimated_events = self.active_jobs.len() as u64 * 100; // Rough estimate
        let warnings = vec!["Estimates are based on active sensd jobs".to_string()];

        let (duration_factor, confidence) = match until {
            TimeHorizon::Snapshot => (1.0, 0.7),
            TimeHorizon::Historical { .. } => (0.5, 0.5),
            TimeHorizon::Continuous => (f64::INFINITY, 0.2),
        };

        let adjusted_events = (estimated_events as f64 * duration_factor) as u64;
        let targets_count = if args.targets.is_empty() {
            self.config.watch_paths.len()
        } else {
            args.targets.len()
        };

        Ok(ScanEstimate {
            estimated_events: adjusted_events,
            estimated_duration: std::time::Duration::from_millis(adjusted_events * 5),
            estimated_data_size: adjusted_events * 512,
            estimated_targets: targets_count as u64,
            warnings,
            confidence,
        })
    }
}

// Implementation of ExplorationProvider for diagnostics
impl ExplorationProvider for FilesystemProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let recent_activity = if let Some(ref state) = self.last_state {
            vec![ActivityEntry {
                timestamp: state.captured_at,
                description: format!(
                    "Snapshot taken: {} active jobs for {} watch paths",
                    state.active_jobs.len(),
                    state.watch_paths.len()
                ),
                data: Some(serde_json::to_value(state)?),
            }]
        } else {
            vec![]
        };

        Ok(SourceState {
            description: format!(
                "Filesystem processor via sensd monitoring {} paths with {} active jobs",
                self.config.watch_paths.len(),
                self.active_jobs.len()
            ),
            last_updated: self
                .last_state
                .as_ref()
                .map(|s| s.captured_at)
                .unwrap_or_else(Utc::now),
            total_items: Some(self.active_jobs.len() as u64),
            metadata: HashMap::from([
                (
                    "watch_paths".to_string(),
                    serde_json::to_value(&self.config.watch_paths)?,
                ),
                (
                    "max_depth".to_string(),
                    serde_json::to_value(self.config.max_depth)?,
                ),
                (
                    "follow_symlinks".to_string(),
                    serde_json::to_value(self.config.follow_symlinks)?,
                ),
                (
                    "batch_size".to_string(),
                    serde_json::to_value(self.config.batch_size)?,
                ),
                (
                    "processor_type".to_string(),
                    serde_json::Value::String("sensd-ingestor".to_string()),
                ),
            ]),
            healthy: true,
            recent_activity,
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        // Would query database for job completion history
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        let (start_time, end_time) = time_range.unwrap_or_else(|| {
            let now = Utc::now();
            let hour_ago = now - chrono::Duration::hours(1);
            (hour_ago, now)
        });

        Ok(CoverageAnalysis {
            time_range: (start_time, end_time),
            source_total: self.active_jobs.len() as u64,
            sinex_total: 0, // Would query from database
            coverage_percentage: 0.0,
            missing_count: self.active_jobs.len() as u64,
            missing_samples: vec![MissingItem {
                source_id: "sensd".to_string(),
                timestamp: end_time,
                description: "TreeWatch jobs pending completion".to_string(),
                missing_reason: Some("Waiting for sensd job completion".to_string()),
            }],
            duplicate_count: 0,
            recommendations: vec![
                "Monitor sensd job status for completion".to_string(),
                "Check TreeWatch sensor configuration".to_string(),
                "Verify material processing pipeline".to_string(),
            ],
        })
    }

    fn export_data(
        &self,
        path: &sinex_core::SanitizedPath,
        format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        if let Some(ref state) = self.last_state {
            let content = match format {
                ExportFormat::Json => serde_json::to_string_pretty(state)?,
                ExportFormat::Csv => {
                    let mut csv = "job_id,target_uri\n".to_string();
                    for (job_id, path) in &state.active_jobs {
                        csv.push_str(&format!("{},{}\n", job_id, path));
                    }
                    csv
                }
                ExportFormat::Raw => format!("{:#?}", state),
            };
            std::fs::write(path.as_str(), content)?;
        } else {
            let config_data = serde_json::json!({
                "watch_paths": self.config.watch_paths,
                "max_depth": self.config.max_depth,
                "follow_symlinks": self.config.follow_symlinks,
                "batch_size": self.config.batch_size,
                "active_jobs": self.active_jobs
            });

            let content = match format {
                ExportFormat::Json => serde_json::to_string_pretty(&config_data)?,
                ExportFormat::Raw => format!("{:#?}", config_data),
                ExportFormat::Csv => "No state data available\n".to_string(),
            };

            std::fs::write(path.as_str(), content)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::prelude::*;

    #[sinex_test]
    async fn test_processor_initialization(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let config = FilesystemConfig {
            watch_paths: vec!["/tmp/test".to_string()],
            max_depth: Some(5),
            follow_symlinks: false,
            batch_size: 50,
            processing_interval_ms: 500,
        };

        let mut processor = FilesystemProcessor::with_config(config.clone());

        assert_eq!(processor.config.watch_paths, config.watch_paths);
        assert_eq!(processor.config.max_depth, config.max_depth);
        assert_eq!(processor.config.follow_symlinks, config.follow_symlinks);
        assert_eq!(processor.config.batch_size, config.batch_size);

        Ok(())
    }

    #[sinex_test]
    async fn test_config_validation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Valid config
        let valid_config = FilesystemConfig {
            watch_paths: vec!["/tmp/test".to_string()],
            max_depth: Some(10),
            follow_symlinks: false,
            batch_size: 100,
            processing_interval_ms: 1000,
        };
        assert!(valid_config.validate_config().is_ok());

        // Invalid config - empty watch paths
        let invalid_config = FilesystemConfig {
            watch_paths: vec![],
            ..valid_config.clone()
        };
        assert!(invalid_config.validate_config().is_err());

        // Invalid config - batch size too large
        let invalid_config = FilesystemConfig {
            batch_size: 2000,
            ..valid_config.clone()
        };
        assert!(invalid_config.validate_config().is_err());

        Ok(())
    }
}
