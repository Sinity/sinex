//! Terminal scanner for historical data processing
//!
//! Provides batch processing capabilities for terminal historical data including:
//! - Atuin shell history database scanning
//! - Shell history file parsing  
//! - Terminal recording file processing
//! - Bulk import with progress tracking

use crate::{SatelliteResult, TerminalConfig};
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use serde_json::json;
use sinex_core::RawEvent;
use sinex_events::RawEventBuilder;
use sinex_satellite_sdk::stream_processor::{ScanReport, ScanArgs, ScanEstimate};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Terminal scanner for processing historical data
pub struct TerminalScanner {
    config: TerminalConfig,
}

impl TerminalScanner {
    /// Create a new terminal scanner
    pub fn new(config: TerminalConfig) -> Self {
        Self { config }
    }

    /// Scan historical terminal data and generate events
    pub async fn scan_historical_data(&self, args: ScanArgs) -> SatelliteResult<ScanReport> {
        let start_time = Instant::now();
        let mut stats = HashMap::new();
        let mut total_events = 0;
        let mut earliest_time: Option<DateTime<Utc>> = None;
        let mut latest_time: Option<DateTime<Utc>> = None;

        info!("Starting terminal historical scan with {} paths", args.paths.len());

        // Determine what to scan
        let scan_paths = if args.targets.is_empty() {
            self.discover_scan_paths()?
        } else {
            args.targets.clone()
        };

        info!("Discovered {} paths to scan", scan_paths.len());
        
        let mut processed_paths = Vec::new();
        let mut failed_paths = Vec::new();

        // Process each path
        for path_str in &scan_paths {
            let path = PathBuf::from(path_str);
            
            if !path.exists() {
                warn!("Path does not exist: {}", path.display());
                failed_paths.push((path_str.clone(), "Path does not exist".to_string()));
                continue;
            }

            info!("Processing path: {}", path.display());
            
            match self.scan_path(&path, &args).await {
                Ok((events, path_stats, time_range)) => {
                    total_events += events;
                    processed_paths.push(path_str.clone());
                    
                    // Update time range
                    if let Some((start, end)) = time_range {
                        earliest_time = earliest_time.map_or(Some(start), |e| Some(e.min(start)));
                        latest_time = latest_time.map_or(Some(end), |l| Some(l.max(end)));
                    }
                    
                    // Merge path stats
                    for (key, value) in path_stats {
                        *stats.entry(key).or_insert(0) += value;
                    }
                }
                Err(e) => {
                    warn!("Failed to process path {}: {}", path.display(), e);
                    failed_paths.push((path_str.clone(), e.to_string()));
                }
            }
        }

        let duration = start_time.elapsed();
        
        info!(
            "Scanner completed: {} events from {} paths in {:?}",
            total_events, scan_paths.len(), duration
        );

        Ok(ScanReport {
            events_processed: total_events,
            duration,
            final_checkpoint: sinex_satellite_sdk::stream_processor::Checkpoint::timestamp(Utc::now(), None),
            time_range: earliest_time.zip(latest_time),
            processor_stats: stats,
            successful_targets: processed_paths,
            failed_targets: failed_paths.into_iter().map(|(path, err)| (path, err)).collect(),
            warnings: Vec::new(),
        })
    }

    /// Estimate the scope of a scanner operation
    pub async fn estimate_scope(&self, args: &ScanArgs) -> SatelliteResult<ScanEstimate> {
        let scan_paths = if args.targets.is_empty() {
            self.discover_scan_paths()?
        } else {
            args.targets.clone()
        };

        let mut estimated_events = 0;
        let mut estimated_data_size = 0;
        let mut warnings = Vec::new();

        for path_str in &scan_paths {
            let path = PathBuf::from(path_str);
            
            if !path.exists() {
                warnings.push(format!("Path does not exist: {}", path.display()));
                continue;
            }

            let (events, size, path_warnings) = self.estimate_path(&path).await?;
            estimated_events += events;
            estimated_data_size += size;
            warnings.extend(path_warnings);
        }

        // Estimate processing time based on events (rough heuristic)
        let estimated_duration = Duration::from_millis(estimated_events / 10); // ~100 events per second

        Ok(ScanEstimate {
            estimated_events,
            estimated_duration,
            estimated_data_size,
            estimated_paths: scan_paths.len() as u64,
            warnings,
        })
    }

    /// Discover default paths to scan
    fn discover_scan_paths(&self) -> SatelliteResult<Vec<String>> {
        let mut paths = Vec::new();

        // Add Atuin database if configured and exists
        if let Some(ref atuin_path) = self.config.atuin_db_path {
            if atuin_path.exists() {
                paths.push(atuin_path.display().to_string());
            }
        }

        // Add existing history files
        for history_file in &self.config.history_files {
            if history_file.exists() {
                paths.push(history_file.display().to_string());
            }
        }

        // Add recording directory if configured and exists
        if let Some(ref recording_dir) = self.config.recording_output_dir {
            if recording_dir.exists() {
                paths.push(recording_dir.display().to_string());
            }
        }

        if paths.is_empty() {
            warn!("No terminal data paths discovered for scanning");
        } else {
            info!("Discovered {} terminal data paths", paths.len());
        }

        Ok(paths)
    }

    /// Scan a specific path and return (event_count, stats, time_range)
    async fn scan_path(
        &self,
        path: &Path,
        args: &ScanArgs,
    ) -> SatelliteResult<(u64, HashMap<String, u64>, Option<(DateTime<Utc>, DateTime<Utc>)>)> {
        let mut stats = HashMap::new();
        
        if path.is_file() {
            // Check file type and scan accordingly
            if path.file_name().and_then(|n| n.to_str()) == Some("history.db") {
                // Atuin database
                self.scan_atuin_database(path, args, &mut stats).await
            } else if path.extension().and_then(|e| e.to_str()) == Some("cast") {
                // Terminal recording file
                self.scan_recording_file(path, args, &mut stats).await
            } else {
                // Shell history file
                self.scan_history_file(path, args, &mut stats).await
            }
        } else if path.is_dir() {
            // Scan directory for relevant files
            self.scan_directory(path, args, &mut stats).await
        } else {
            warn!("Unsupported path type: {}", path.display());
            Ok((0, stats, None))
        }
    }

    /// Scan Atuin database for historical commands
    async fn scan_atuin_database(
        &self,
        db_path: &Path,
        args: &ScanArgs,
        stats: &mut HashMap<String, u64>,
    ) -> SatelliteResult<(u64, HashMap<String, u64>, Option<(DateTime<Utc>, DateTime<Utc>)>)> {
        let db_path = db_path.to_path_buf();
        let time_range = args.time_range;
        let max_events = args.max_events;
        let dry_run = args.dry_run;
        let batch_size = self.config.scanner_batch_size;

        info!("Scanning Atuin database: {}", db_path.display());

        let (events, time_range_found) = tokio::task::spawn_blocking(move || -> SatelliteResult<(Vec<RawEvent>, Option<(DateTime<Utc>, DateTime<Utc>)>)> {
            let conn = Connection::open(&db_path)
                .map_err(|e| crate::SatelliteError::EventSource(format!("Failed to open Atuin DB: {}", e)))?;

            // Build query with time range filter if provided
            let (query, params_values): (String, Vec<rusqlite::types::Value>) = if let Some((start, end)) = time_range {
                let start_ns = start.timestamp_nanos_opt().unwrap_or(0);
                let end_ns = end.timestamp_nanos_opt().unwrap_or(i64::MAX);
                let mut q = "SELECT id, timestamp, duration, exit, command, cwd, session, hostname FROM history WHERE timestamp BETWEEN ?1 AND ?2 ORDER BY timestamp ASC".to_string();
                if max_events > 0 {
                    q.push_str(&format!(" LIMIT {}", max_events));
                }
                (q, vec![start_ns.into(), end_ns.into()])
            } else {
                let mut q = "SELECT id, timestamp, duration, exit, command, cwd, session, hostname FROM history ORDER BY timestamp ASC".to_string();
                if max_events > 0 {
                    q.push_str(&format!(" LIMIT {}", max_events));
                }
                (q, vec![])
            };

            debug!("Atuin query: {}", query);

            let mut stmt = conn.prepare(&query)
                .map_err(|e| crate::SatelliteError::EventSource(format!("Failed to prepare query: {}", e)))?;

            // Define the row mapper closure once to avoid type conflicts
            let row_mapper = |row: &rusqlite::Row| -> Result<(String, i64, i64, i32, String, String, String, String), rusqlite::Error> {
                Ok((
                    row.get::<_, String>(0)?,     // id
                    row.get::<_, i64>(1)?,        // timestamp
                    row.get::<_, i64>(2)?,        // duration
                    row.get::<_, i32>(3)?,        // exit
                    row.get::<_, String>(4)?,     // command
                    row.get::<_, String>(5)?,     // cwd
                    row.get::<_, String>(6)?,     // session
                    row.get::<_, String>(7)?,     // hostname
                ))
            };

            let rows = if params_values.is_empty() {
                stmt.query_map([], row_mapper)
            } else {
                stmt.query_map(rusqlite::params_from_iter(params_values.iter()), row_mapper)
            }
            .map_err(|e| crate::SatelliteError::EventSource(format!("Query execution failed: {}", e)))?;

            let mut events = Vec::new();
            let mut min_time: Option<DateTime<Utc>> = None;
            let mut max_time: Option<DateTime<Utc>> = None;
            let mut processed = 0;

            for row_result in rows {
                let (id, timestamp_ns, duration_ns, exit_code, command, cwd, session, hostname) = row_result
                    .map_err(|e| crate::SatelliteError::EventSource(format!("Row parsing failed: {}", e)))?;

                let ts_end = DateTime::from_timestamp_nanos(timestamp_ns);
                let duration_secs = duration_ns as f64 / 1_000_000_000.0;
                let ts_start = ts_end - chrono::Duration::milliseconds((duration_secs * 1000.0) as i64);

                min_time = min_time.map_or(Some(ts_start), |m| Some(m.min(ts_start)));
                max_time = max_time.map_or(Some(ts_end), |m| Some(m.max(ts_end)));

                if !dry_run {
                    let payload = json!({
                        "command_string": command,
                        "cwd": cwd,
                        "exit_code": exit_code,
                        "duration_ns": duration_ns,
                        "atuin_history_id": id,
                        "atuin_session_id": session,
                        "timestamp": timestamp_ns,
                        "ts_start_orig": ts_start,
                        "ts_end_orig": ts_end,
                        "hostname": hostname,
                        "scanner_generated": true,
                    });

                    let event = RawEventBuilder::new(sinex_core::sources::SHELL_ATUIN, "command.imported", payload)
                        .with_host(&hostname)
                        .build();

                    events.push(event);
                }

                processed += 1;
                if processed % batch_size == 0 {
                    debug!("Processed {} Atuin entries", processed);
                }
            }

            info!("Scanned {} Atuin entries", processed);
            Ok((events, min_time.zip(max_time)))
        })
        .await
        .map_err(|e| crate::SatelliteError::EventSource(format!("Spawn blocking failed: {}", e)))??;

        stats.insert("atuin_entries_scanned".to_string(), events.len() as u64);
        
        // Send events if not dry run (this would be implemented in the actual scanner runtime)
        if !dry_run {
            debug!("Generated {} Atuin events for ingestion", events.len());
        }

        Ok((events.len() as u64, stats.clone(), time_range_found))
    }

    /// Scan shell history file
    async fn scan_history_file(
        &self,
        file_path: &Path,
        _args: &ScanArgs,
        stats: &mut HashMap<String, u64>,
    ) -> SatelliteResult<(u64, HashMap<String, u64>, Option<(DateTime<Utc>, DateTime<Utc>)>)> {
        info!("Scanning history file: {}", file_path.display());
        
        // For now, return placeholder implementation
        // This would be expanded to parse various shell history formats
        stats.insert("history_files_scanned".to_string(), 1);
        warn!("History file scanning not yet implemented: {}", file_path.display());
        
        Ok((0, stats.clone(), None))
    }

    /// Scan terminal recording file
    async fn scan_recording_file(
        &self,
        file_path: &Path,
        _args: &ScanArgs,
        stats: &mut HashMap<String, u64>,
    ) -> SatelliteResult<(u64, HashMap<String, u64>, Option<(DateTime<Utc>, DateTime<Utc>)>)> {
        info!("Scanning recording file: {}", file_path.display());
        
        // For now, return placeholder implementation
        // This would be expanded to parse asciinema and other recording formats
        stats.insert("recording_files_scanned".to_string(), 1);
        warn!("Recording file scanning not yet implemented: {}", file_path.display());
        
        Ok((0, stats.clone(), None))
    }

    /// Scan directory for terminal data files
    async fn scan_directory(
        &self,
        dir_path: &Path,
        args: &ScanArgs,
        stats: &mut HashMap<String, u64>,
    ) -> SatelliteResult<(u64, HashMap<String, u64>, Option<(DateTime<Utc>, DateTime<Utc>)>)> {
        info!("Scanning directory: {}", dir_path.display());
        
        let mut total_events = 0;
        let mut earliest_time: Option<DateTime<Utc>> = None;
        let mut latest_time: Option<DateTime<Utc>> = None;

        // Recursively scan directory for relevant files
        let entries = fs::read_dir(dir_path)
            .map_err(|e| crate::SatelliteError::EventSource(format!("Failed to read directory: {}", e)))?;

        for entry in entries {
            let entry = entry
                .map_err(|e| crate::SatelliteError::EventSource(format!("Directory entry error: {}", e)))?;
            
            let path = entry.path();
            
            // Check file size limits
            if let Ok(metadata) = entry.metadata() {
                let size_mb = metadata.len() / (1024 * 1024);
                if size_mb > self.config.scanner_max_file_size_mb {
                    warn!("Skipping large file ({}MB): {}", size_mb, path.display());
                    continue;
                }
            }

            let (events, path_stats, time_range) = Box::pin(self.scan_path(&path, args)).await?;
            
            total_events += events;
            
            // Update time range
            if let Some((start, end)) = time_range {
                earliest_time = earliest_time.map_or(Some(start), |e| Some(e.min(start)));
                latest_time = latest_time.map_or(Some(end), |l| Some(l.max(end)));
            }
            
            // Merge stats
            for (key, value) in path_stats {
                *stats.entry(key).or_insert(0) += value;
            }
        }

        Ok((total_events, stats.clone(), earliest_time.zip(latest_time)))
    }

    /// Estimate processing for a specific path
    async fn estimate_path(&self, path: &Path) -> SatelliteResult<(u64, u64, Vec<String>)> {
        let mut warnings = Vec::new();
        
        if !path.exists() {
            warnings.push(format!("Path does not exist: {}", path.display()));
            return Ok((0, 0, warnings));
        }

        let metadata = fs::metadata(path)
            .map_err(|e| crate::SatelliteError::EventSource(format!("Failed to get metadata: {}", e)))?;
        
        let size = metadata.len();
        
        // Rough estimation heuristics
        let estimated_events = if path.is_file() {
            if path.file_name().and_then(|n| n.to_str()) == Some("history.db") {
                // Atuin DB: estimate based on file size (rough heuristic)
                size / 200 // Assume ~200 bytes per command entry
            } else {
                // History file: estimate based on lines
                size / 50 // Assume ~50 chars per command
            }
        } else {
            // Directory: rough estimate
            size / 100
        };

        if size > (self.config.scanner_max_file_size_mb * 1024 * 1024) {
            warnings.push(format!(
                "Large file may be slow to process: {} ({}MB)", 
                path.display(), 
                size / (1024 * 1024)
            ));
        }

        Ok((estimated_events, size, warnings))
    }
}