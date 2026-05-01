//! Example implementation of Node for filesystem monitoring
//!
//! This example demonstrates how to refactor an existing `EventSource` to use
//! the new unified Node interface from Part 16.

use crate::{
    NodeResult, SinexError,
    acquisition_manager::{AcquisitionManager, RotationPolicy, SourceMaterialHandle},
    runtime::stream::{
        Checkpoint, Node, NodeCapabilities, NodeInitContext, NodeRuntimeState, NodeType, ScanArgs,
        ScanEstimate, ScanReport, TimeHorizon,
    },
};
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{DirDiscoveredPayload, FileDiscoveredPayload};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{RecordedPath, SanitizedPath};
use std::collections::HashMap;
use tokio::fs;
use tracing::{debug, info, warn};

/// Configuration for the filesystem node
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct FilesystemNodeConfig {
    /// Maximum number of files to process in one scan
    pub max_files: Option<usize>,
    /// File extensions to include (empty means all)
    pub include_extensions: Vec<String>,
    /// File extensions to exclude
    pub exclude_extensions: Vec<String>,
    /// Follow symbolic links
    pub follow_symlinks: bool,
}

/// Example filesystem node implementing unified stream node interface
pub struct FilesystemNode {
    /// Base directories to monitor
    watch_paths: Vec<Utf8PathBuf>,

    /// Runtime handles captured during initialization
    runtime: Option<NodeRuntimeState>,

    /// Last known filesystem state
    last_state: Option<FilesystemState>,
}

/// Snapshot of filesystem state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemState {
    /// Timestamp when state was captured
    pub captured_at: Timestamp,

    /// File count by directory
    pub file_counts: HashMap<Utf8PathBuf, u64>,

    /// Total files monitored
    pub total_files: u64,

    /// Directories monitored
    pub directories: Vec<Utf8PathBuf>,
}

impl FilesystemNode {
    /// Create new filesystem node
    #[must_use]
    pub fn new(watch_paths: Vec<Utf8PathBuf>) -> Self {
        Self {
            watch_paths,
            runtime: None,
            last_state: None,
        }
    }

    /// Scan directory and generate file events (non-recursive for simplicity)
    async fn scan_directory_simple(
        &self,
        path: &Utf8Path,
        checkpoint: &Checkpoint,
        emit_events: bool,
    ) -> NodeResult<u64> {
        let mut event_count = 0;

        // Determine scan cutoff based on checkpoint
        let cutoff_time = match checkpoint {
            Checkpoint::Timestamp { timestamp, .. } => Some(*timestamp),
            Checkpoint::External { position, .. } => {
                // Try to parse timestamp from position
                serde_json::from_value::<Timestamp>(position.clone()).ok()
            }
            _ => None,
        };

        info!(path = %path.as_str(), "Scanning directory");

        let runtime = self.runtime.as_ref();
        let mut material_context = if emit_events {
            if let Some(runtime) = runtime {
                let manager =
                    runtime.acquisition_manager(RotationPolicy::default(), "filesystem.example")?;
                let handle = manager
                    .begin_material_with_metadata(
                        path.as_str(),
                        json!({
                            "example": true,
                            "scan_kind": "directory_simple",
                            "source_path": path.as_str(),
                        }),
                    )
                    .await?;
                Some((manager, handle))
            } else {
                None
            }
        } else {
            None
        };

        let mut entries = fs::read_dir(path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let entry_path = entry.path();
            let metadata = entry.metadata().await?;

            // Skip files older than checkpoint
            if let Some(cutoff) = cutoff_time
                && let Ok(modified) = metadata.modified()
            {
                let modified_dt: Timestamp = Timestamp::from(modified);
                if modified_dt <= cutoff {
                    continue;
                }
            }

            if emit_events {
                use std::os::unix::fs::PermissionsExt;

                if metadata.is_file() {
                    let modified_time = metadata
                        .modified()
                        .ok()
                        .map_or_else(Timestamp::now, Timestamp::from);

                    let payload = FileDiscoveredPayload {
                        #[allow(clippy::expect_used)] // Example code; infallible for valid paths
                        path: RecordedPath::from_observed(entry_path.to_string_lossy().to_string())
                            .expect("Path should not contain null bytes"),
                        size: metadata.len(),
                        modified_at: modified_time,
                        permissions: Some(metadata.permissions().mode()),
                    };

                    if let (Some(runtime), Some((manager, handle))) =
                        (runtime, material_context.as_mut())
                    {
                        Self::emit_payload_from_material(runtime, manager, handle, payload).await?;
                    }
                } else if metadata.is_dir() {
                    let modified_time = metadata
                        .modified()
                        .ok()
                        .map_or_else(Timestamp::now, Timestamp::from);

                    let payload = DirDiscoveredPayload {
                        #[allow(clippy::expect_used)] // Example code; infallible for valid paths
                        path: RecordedPath::from_observed(entry_path.to_string_lossy().to_string())
                            .expect("Path should not contain null bytes"),
                        modified_at: modified_time,
                    };

                    if let (Some(runtime), Some((manager, handle))) =
                        (runtime, material_context.as_mut())
                    {
                        Self::emit_payload_from_material(runtime, manager, handle, payload).await?;
                    }
                } else {
                    continue;
                }
            }

            event_count += 1;

            // Note: Not recursing into subdirectories for simplicity in this example
        }

        if let Some((manager, mut handle)) = material_context {
            manager
                .finalize_with_metadata(
                    &mut handle,
                    "scan_complete",
                    json!({
                        "scan_kind": "directory_simple",
                        "source_path": path.as_str(),
                        "events": event_count,
                    }),
                )
                .await?;
        }

        debug!(path = %path.as_str(), events = event_count, "Directory scan completed");
        Ok(event_count)
    }

    async fn emit_payload_from_material<P>(
        runtime: &NodeRuntimeState,
        manager: &AcquisitionManager,
        handle: &mut SourceMaterialHandle,
        payload: P,
    ) -> NodeResult<()>
    where
        P: EventPayload,
    {
        let mut record = serde_json::to_vec(&payload)?;
        record.push(b'\n');
        let anchors = manager
            .append_record_batch(handle, std::slice::from_ref(&record))
            .await?;
        let anchor = anchors.first().copied().ok_or_else(|| {
            SinexError::invalid_state("material append returned no anchor for filesystem event")
        })?;
        let event = payload
            .from_material_at(anchor.material_id, anchor.offset_start)
            .build()?
            .to_json_event()?;
        runtime.event_emitter().emit(event).await?;
        Ok(())
    }

    /// Take a snapshot of current filesystem state
    async fn take_snapshot(&mut self) -> NodeResult<FilesystemState> {
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
            captured_at: sinex_primitives::temporal::Timestamp::now(),
            file_counts,
            total_files,
            directories: self.watch_paths.clone(),
        };

        self.last_state = Some(state.clone());
        Ok(state)
    }

    /// Count files in a directory (non-recursive for simplicity)
    async fn count_files_simple(&self, path: &Utf8Path) -> NodeResult<u64> {
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

impl Node for FilesystemNode {
    type Config = FilesystemNodeConfig;

    async fn initialize(&mut self, init: NodeInitContext<Self::Config>) -> NodeResult<()> {
        let (_config, raw_config, service_info, handles, work_dir_utf8) = init.into_parts();
        info!(
            node = self.node_name(),
            service = %service_info.service_name(),
            watch_paths = ?self.watch_paths,
            "Initializing filesystem node"
        );

        // Validate watch paths exist
        for path in &self.watch_paths {
            if !path.exists() {
                warn!(path = %path.as_str(), "Watch path does not exist");
            }
        }

        let runtime = NodeRuntimeState::new(service_info, handles, raw_config, work_dir_utf8);
        self.runtime = Some(runtime);
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> NodeResult<ScanReport> {
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
                // Continuous monitoring (polling scan in this example implementation)
                warnings.push(
                    "Continuous filesystem monitoring uses polling scans in this example"
                        .to_string(),
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
            }
        }

        let final_checkpoint =
            Checkpoint::timestamp(sinex_primitives::temporal::Timestamp::now(), None);

        Ok(ScanReport {
            events_processed,
            duration: start_time.elapsed(),
            final_checkpoint,
            time_range: Some((
                match &from {
                    Checkpoint::Timestamp { timestamp, .. } => *timestamp,
                    _ => sinex_primitives::temporal::Timestamp::now() - time::Duration::hours(1),
                },
                sinex_primitives::temporal::Timestamp::now(),
            )),
            node_stats: HashMap::from([
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

    fn node_name(&self) -> &'static str {
        "filesystem-example"
    }

    fn node_type(&self) -> NodeType {
        NodeType::Ingestor
    }

    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            supports_continuous: true, // Would support with proper file watcher
            supports_historical: true, // Limited by file modification times
            supports_snapshot: true,   // Full directory scanning
            supports_interactive: false,
            max_scan_size: Some(10000), // Limit for large directories
            supports_concurrent: false,
            manages_own_continuous_loop: false,
            manages_own_checkpoints: false,
        }
    }

    async fn current_checkpoint(&self) -> NodeResult<Checkpoint> {
        // Return timestamp-based checkpoint
        Ok(Checkpoint::timestamp(
            sinex_primitives::temporal::Timestamp::now(),
            None,
        ))
    }

    async fn estimate_scan_scope(
        &self,
        _from: &Checkpoint,
        until: &TimeHorizon,
        _args: &ScanArgs,
    ) -> NodeResult<ScanEstimate> {
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

use crate::exploration::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};

impl ExplorationProvider for FilesystemNode {
    fn get_source_state(&self) -> NodeResult<SourceState> {
        Ok(SourceState {
            is_connected: true,
            healthy: true,
            description: "Filesystem node running".to_string(),
            last_updated: None,
            lag_seconds: None,
            recent_activity: Vec::new(),
            total_items: None,
            metadata: HashMap::new(),
        })
    }
    fn get_ingestion_history(&self, _limit: u64) -> NodeResult<Vec<IngestionHistoryEntry>> {
        Err(SinexError::invalid_state(
            "ingestion history is not implemented in the example filesystem node",
        ))
    }
    fn get_coverage_analysis(
        &self,
        _time_range: Option<(Timestamp, Timestamp)>,
    ) -> NodeResult<CoverageAnalysis> {
        crate::exploration::coverage_analysis_unavailable(
            "coverage analysis is not implemented in the example filesystem node",
        )
    }
    fn export_data(&self, _path: &SanitizedPath, _format: ExportFormat) -> NodeResult<()> {
        Err(SinexError::invalid_state(
            "data export is not implemented in the example filesystem node",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{FilesystemNode, Utf8PathBuf};
    use crate::exploration::{ExplorationProvider, ExportFormat};
    use sinex_primitives::SanitizedPath;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn example_ingestion_history_is_explicitly_unavailable() -> xtask::sandbox::TestResult<()>
    {
        let node = FilesystemNode::new(Vec::<Utf8PathBuf>::new());

        let error = ExplorationProvider::get_ingestion_history(&node, 10)
            .expect_err("example must not report empty ingestion history as success");

        assert!(error.to_string().contains("example filesystem node"));
        Ok(())
    }

    #[sinex_test]
    async fn example_export_is_explicitly_unavailable() -> xtask::sandbox::TestResult<()> {
        let node = FilesystemNode::new(Vec::<Utf8PathBuf>::new());
        let path = SanitizedPath::from_static("/tmp/filesystem-example-export.json");

        let error = ExplorationProvider::export_data(&node, &path, ExportFormat::Json)
            .expect_err("example must not report export success without writing data");

        assert!(error.to_string().contains("example filesystem node"));
        Ok(())
    }
}
