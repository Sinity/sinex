//! Example implementation of StatefulStreamProcessor for filesystem monitoring
//!
//! This example demonstrates how to refactor an existing EventSource to use
//! the new unified StatefulStreamProcessor interface from Part 16.

use crate::{
    cli::{
        ActivityEntry, CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry,
        SourceState,
    },
    stream_processor::{
        Checkpoint, ProcessorCapabilities, ProcessorType, ScanArgs, ScanEstimate, ScanReport,
        StatefulStreamProcessor, StreamProcessorContext, TimeHorizon,
    },
    SatelliteResult,
};
use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use chrono::{DateTime, Utc};
use color_eyre::eyre;
use serde::{Deserialize, Serialize};
use sinex_core::db::models::RawEvent;
use sinex_core::types::events::Event;
use std::collections::HashMap;
use tokio::fs;
use tracing::{debug, info, warn};

/// Configuration for the filesystem processor
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FilesystemProcessorConfig {
    /// Maximum number of files to process in one scan
    pub max_files: Option<usize>,
    /// File extensions to include (empty means all)
    pub include_extensions: Vec<String>,
    /// File extensions to exclude
    pub exclude_extensions: Vec<String>,
    /// Follow symbolic links
    pub follow_symlinks: bool,
}

impl Default for FilesystemProcessorConfig {
    fn default() -> Self {
        Self {
            max_files: None,
            include_extensions: Vec::new(),
            exclude_extensions: Vec::new(),
            follow_symlinks: false,
        }
    }
}

/// Example filesystem processor implementing unified stream processor interface
#[derive(Debug)]
pub struct FilesystemProcessor {
    /// Base directories to monitor
    watch_paths: Vec<Utf8PathBuf>,

    /// Current context (set during initialization)
    context: Option<StreamProcessorContext>,

    /// Last known filesystem state
    last_state: Option<FilesystemState>,
}

/// Snapshot of filesystem state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemState {
    /// Timestamp when state was captured
    pub captured_at: DateTime<Utc>,

    /// File count by directory
    pub file_counts: HashMap<Utf8PathBuf, u64>,

    /// Total files monitored
    pub total_files: u64,

    /// Directories monitored
    pub directories: Vec<Utf8PathBuf>,
}

impl FilesystemProcessor {
    /// Create new filesystem processor
    pub fn new(watch_paths: Vec<Utf8PathBuf>) -> Self {
        Self {
            watch_paths,
            context: None,
            last_state: None,
        }
    }

    /// Scan directory and generate file events (non-recursive for simplicity)
    async fn scan_directory_simple(
        &self,
        path: &Utf8Path,
        checkpoint: &Checkpoint,
        emit_events: bool,
    ) -> SatelliteResult<u64> {
        let mut event_count = 0;

        // Determine scan cutoff based on checkpoint
        let cutoff_time = match checkpoint {
            Checkpoint::Timestamp { timestamp, .. } => Some(*timestamp),
            Checkpoint::External { position, .. } => {
                // Try to parse timestamp from position
                serde_json::from_value::<DateTime<Utc>>(position.clone()).ok()
            }
            _ => None,
        };

        info!(path = %path.as_str(), "Scanning directory");

        let mut entries = fs::read_dir(path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let entry_path = entry.path();
            let metadata = entry.metadata().await?;

            // Skip files older than checkpoint
            if let Some(cutoff) = cutoff_time {
                if let Ok(modified) = metadata.modified() {
                    let modified_dt: DateTime<Utc> = modified.into();
                    if modified_dt <= cutoff {
                        continue;
                    }
                }
            }

            if emit_events {
                use std::os::unix::fs::PermissionsExt;

                let event: RawEvent = if metadata.is_file() {
                    let modified_time = metadata
                        .modified()
                        .ok()
                        .map(|t| {
                            let dt: DateTime<Utc> = t.into();
                            dt
                        })
                        .unwrap_or_else(Utc::now);

                    Event::from_payload(sinex_core::events::FileDiscoveredPayload {
                        path: entry_path.to_string_lossy().to_string(),
                        size: metadata.len(),
                        modified_at: modified_time,
                        permissions: Some(metadata.permissions().mode()),
                    })
                    .with_ts_orig(Some(chrono::Utc::now()))
                    .into()
                } else if metadata.is_dir() {
                    let modified_time = metadata
                        .modified()
                        .ok()
                        .map(|t| {
                            let dt: DateTime<Utc> = t.into();
                            dt
                        })
                        .unwrap_or_else(Utc::now);

                    Event::from_payload(sinex_core::events::DirDiscoveredPayload {
                        path: entry_path.to_string_lossy().to_string(),
                        modified_at: modified_time,
                    })
                    .with_ts_orig(Some(chrono::Utc::now()))
                    .into()
                } else {
                    continue;
                };

                if let Some(ref context) = self.context {
                    context.emit_event(event).await?;
                }
            }

            event_count += 1;

            // Note: Not recursing into subdirectories for simplicity in this example
        }

        debug!(path = %path.as_str(), events = event_count, "Directory scan completed");
        Ok(event_count)
    }

    /// Take a snapshot of current filesystem state
    async fn take_snapshot(&mut self) -> SatelliteResult<FilesystemState> {
        let mut file_counts = HashMap::new();
        let mut total_files = 0;

        for watch_path in &self.watch_paths {
            if watch_path.exists() {
                let count = self.count_files_simple(watch_path).await?;
                file_counts.insert(watch_path.clone(), count);
                total_files += count;
            }
        }

        let state = FilesystemState {
            captured_at: Utc::now(),
            file_counts,
            total_files,
            directories: self.watch_paths.clone(),
        };

        self.last_state = Some(state.clone());
        Ok(state)
    }

    /// Count files in a directory (non-recursive for simplicity)
    async fn count_files_simple(&self, path: &Utf8Path) -> SatelliteResult<u64> {
        let mut count = 0;
        let mut entries = fs::read_dir(path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_file() {
                count += 1;
            }
            // Note: Not recursing into subdirectories for simplicity
        }

        Ok(count)
    }
}

#[async_trait]
impl StatefulStreamProcessor for FilesystemProcessor {
    type Config = FilesystemProcessorConfig;

    async fn initialize(
        &mut self,
        ctx: StreamProcessorContext,
        _config: Self::Config,
    ) -> SatelliteResult<()> {
        info!(
            processor = self.processor_name(),
            watch_paths = ?self.watch_paths,
            "Initializing filesystem processor"
        );

        // Validate watch paths exist
        for path in &self.watch_paths {
            if !path.exists() {
                warn!(path = %path.as_str(), "Watch path does not exist");
            }
        }

        self.context = Some(ctx);
        Ok(())
    }

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
            from = %from.description(),
            until = ?until,
            targets = args.targets.len(),
            "Starting filesystem scan"
        );

        match until {
            TimeHorizon::Snapshot => {
                // Take current state snapshot
                let _state = self.take_snapshot().await?;

                // Scan all watch paths
                for watch_path in &self.watch_paths {
                    if watch_path.exists() {
                        match self
                            .scan_directory_simple(watch_path, &from, !args.dry_run)
                            .await
                        {
                            Ok(count) => {
                                events_processed += count;
                                successful_targets.push(watch_path.to_string());
                            }
                            Err(e) => {
                                failed_targets.push((watch_path.to_string(), e.to_string()));
                            }
                        }
                    } else {
                        warnings.push(format!("Path does not exist: {}", watch_path.as_str()));
                    }
                }
            }

            TimeHorizon::Historical { end_time } => {
                // Historical scan from checkpoint to end_time
                warnings.push(
                    "Historical filesystem scanning is limited to modification times".to_string(),
                );

                for watch_path in &self.watch_paths {
                    if watch_path.exists() {
                        match self
                            .scan_directory_simple(watch_path, &from, !args.dry_run)
                            .await
                        {
                            Ok(count) => {
                                events_processed += count;
                                successful_targets.push(watch_path.to_string());
                            }
                            Err(e) => {
                                failed_targets.push((watch_path.to_string(), e.to_string()));
                            }
                        }
                    }
                }

                debug!(end_time = %end_time, "Historical scan completed");
            }

            TimeHorizon::Continuous => {
                // Continuous monitoring (would use file system watcher in real implementation)
                warnings.push(
                    "Continuous filesystem monitoring not implemented in this example".to_string(),
                );
                return Err(crate::SatelliteError::Processing(
                    "Continuous mode not implemented for filesystem processor example".to_string(),
                ));
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
                (
                    "directories_scanned".to_string(),
                    self.watch_paths.len() as u64,
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
        "filesystem-example"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Ingestor
    }

    fn capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            supports_continuous: true, // Would support with proper file watcher
            supports_historical: true, // Limited by file modification times
            supports_snapshot: true,   // Full directory scanning
            supports_interactive: false,
            max_scan_size: Some(10000), // Limit for large directories
            supports_concurrent: false,
        }
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        // Return timestamp-based checkpoint
        Ok(Checkpoint::timestamp(Utc::now(), None))
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> SatelliteResult<ScanEstimate> {
        let mut estimated_events = 0;
        let mut warnings = Vec::new();

        // Estimate based on current file counts
        for watch_path in &self.watch_paths {
            if watch_path.exists() {
                match self.count_files_simple(watch_path).await {
                    Ok(count) => estimated_events += count,
                    Err(_) => warnings.push(format!("Cannot access path: {}", watch_path.as_str())),
                }
            }
        }

        // Adjust estimate based on time horizon
        let (duration_factor, confidence) = match until {
            TimeHorizon::Snapshot => (1.0, 0.9),
            TimeHorizon::Historical { .. } => (0.3, 0.6), // Fewer files modified recently
            TimeHorizon::Continuous => (f64::INFINITY, 0.1), // Unknown duration
        };

        let adjusted_events = (estimated_events as f64 * duration_factor) as u64;

        Ok(ScanEstimate {
            estimated_events: adjusted_events,
            estimated_duration: std::time::Duration::from_millis(adjusted_events * 10), // ~10ms per file
            estimated_data_size: adjusted_events * 1024, // ~1KB per event
            estimated_targets: self.watch_paths.len() as u64,
            warnings,
            confidence,
        })
    }
}

impl ExplorationProvider for FilesystemProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let recent_activity = if let Some(ref state) = self.last_state {
            vec![ActivityEntry {
                timestamp: state.captured_at,
                description: format!(
                    "Snapshot taken: {} files in {} directories",
                    state.total_files,
                    state.directories.len()
                ),
                data: Some(serde_json::to_value(state)?),
            }]
        } else {
            vec![]
        };

        Ok(SourceState {
            description: format!(
                "Filesystem processor monitoring {} paths",
                self.watch_paths.len()
            ),
            last_updated: self
                .last_state
                .as_ref()
                .map(|s| s.captured_at)
                .unwrap_or_else(Utc::now),
            total_items: self.last_state.as_ref().map(|s| s.total_files),
            metadata: HashMap::from([
                (
                    "watch_paths".to_string(),
                    serde_json::to_value(&self.watch_paths)?,
                ),
                (
                    "processor_type".to_string(),
                    serde_json::Value::String("ingestor".to_string()),
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
        // In a real implementation, this would query the database for scan history
        Ok(vec![])
    }

    fn get_coverage_analysis(
        &self,
        _time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        // In a real implementation, this would compare filesystem state with Sinex events
        let now = Utc::now();
        let hour_ago = now - chrono::Duration::hours(1);

        Ok(CoverageAnalysis {
            time_range: (hour_ago, now),
            source_total: self.last_state.as_ref().map(|s| s.total_files).unwrap_or(0),
            sinex_total: 0, // Would query from database
            coverage_percentage: 0.0,
            missing_count: 0,
            missing_samples: vec![],
            duplicate_count: 0,
            recommendations: vec![
                "Run a full snapshot scan to capture current state".to_string(),
                "Enable continuous monitoring for real-time updates".to_string(),
            ],
        })
    }

    fn export_data(
        &self,
        path: &Utf8PathBuf,
        format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        if let Some(ref state) = self.last_state {
            let content = match format {
                ExportFormat::Json => serde_json::to_string_pretty(state)?,
                ExportFormat::Csv => {
                    // Simple CSV export
                    let mut csv = "path,file_count\n".to_string();
                    for (path, count) in &state.file_counts {
                        csv.push_str(&format!("{},{}\n", path.as_str(), count));
                    }
                    csv
                }
                ExportFormat::Raw => format!("{:#?}", state),
            };

            std::fs::write(path, content)?;
        }

        Ok(())
    }
}
