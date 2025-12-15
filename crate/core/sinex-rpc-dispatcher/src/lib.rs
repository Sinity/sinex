#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]

//! RPC Dispatcher - Unified `StatefulStreamProcessor` implementation.

// External crates
use async_trait::async_trait;
use chrono::Utc;
use color_eyre::eyre::eyre;
use serde::{Deserialize, Serialize};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use sinex_satellite_sdk::{
    stream_processor::{
        Checkpoint, ProcessorInitContext, ProcessorType, ScanArgs, ScanReport,
        StatefulStreamProcessor, TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use validator::Validate;

// Standard library
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Write;
use tracing::info;

/// Configuration for RPC Dispatcher processor
#[derive(Debug, Clone, Deserialize, Serialize, Validate, bon::Builder)]
#[builder(derive(Debug))]
pub struct RpcDispatcherConfig {
    /// Maximum number of concurrent connections
    #[validate(range(
        min = 1,
        max = 10000,
        message = "Max connections must be between 1 and 10000"
    ))]
    pub max_connections: Option<u32>,
    /// Request timeout in seconds
    #[validate(range(
        min = 1,
        max = 300,
        message = "Request timeout must be between 1 and 300 seconds"
    ))]
    pub request_timeout_secs: Option<u64>,
    /// Time range for historical scans in hours
    #[validate(range(
        min = 1,
        max = 8760,
        message = "Historical scan hours must be between 1 and 8760 (1 year)"
    ))]
    pub historical_scan_hours: Option<u64>,
    /// RPC server host to bind to
    pub server_host: Option<String>,
    /// RPC server port to bind to
    #[validate(range(
        min = 1024,
        max = 65535,
        message = "Server port must be between 1024 and 65535"
    ))]
    pub server_port: Option<u16>,
    /// Enable TLS for RPC server
    pub enable_tls: bool,
    /// Path to TLS certificate file
    pub tls_cert_path: Option<String>,
    /// Path to TLS private key file
    pub tls_key_path: Option<String>,
    /// Maximum RPC payload size in MB
    #[validate(range(
        min = 1,
        max = 1024,
        message = "Max payload size must be between 1 and 1024 MB"
    ))]
    pub max_payload_size_mb: Option<u64>,
}

impl Default for RpcDispatcherConfig {
    fn default() -> Self {
        Self::builder()
            .max_connections(1000)
            .request_timeout_secs(30)
            .historical_scan_hours(24)
            .server_host("127.0.0.1".to_string())
            .server_port(8080)
            .enable_tls(false)
            .max_payload_size_mb(64)
            .build()
    }
}

/// RPC Dispatcher Processor using unified StatefulStreamProcessor architecture
pub struct RpcDispatcherProcessor {
    config: RpcDispatcherConfig,
    service_name: String,
    history: VecDeque<IngestionHistoryEntry>,
    last_checkpoint: Checkpoint,
    max_history: usize,
}

impl RpcDispatcherProcessor {
    pub fn new() -> Self {
        Self {
            config: RpcDispatcherConfig::default(),
            service_name: "rpc-dispatcher".to_string(),
            history: VecDeque::new(),
            last_checkpoint: Checkpoint::None,
            max_history: 64,
        }
    }

    fn record_history(&mut self, entry: IngestionHistoryEntry) {
        self.history.push_front(entry);
        while self.history.len() > self.max_history {
            self.history.pop_back();
        }
    }
}

#[async_trait]
impl StatefulStreamProcessor for RpcDispatcherProcessor {
    type Config = RpcDispatcherConfig;

    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (config, _raw_config, service_info, _handles, _work_dir) = init.into_parts();
        config.validate().map_err(|err| {
            SatelliteError::Configuration(format!("rpc dispatcher config invalid: {}", err))
        })?;

        self.config = config;
        self.service_name = service_info.service_name().to_string();
        info!(service = %service_info.service_name(), "Initializing RPC dispatcher processor");
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = Utc::now();
        let events_processed = 0u64;
        let mut warnings = Vec::new();
        let mut processor_stats: HashMap<String, u64> = HashMap::new();
        let mut successful_targets = args.targets;
        let failed_targets: Vec<(String, String)> = Vec::new();

        let final_checkpoint = match until {
            TimeHorizon::Snapshot => {
                info!("RPC dispatcher taking snapshot of current RPC configuration");
                processor_stats.insert("snapshot_taken".to_string(), 1);
                Checkpoint::timestamp(Utc::now(), None)
            }
            TimeHorizon::Historical { end_time } => {
                let hours = self.config.historical_scan_hours.unwrap_or(24);
                let start = end_time - chrono::Duration::hours(hours as i64);
                info!(
                    start = %start,
                    end = %end_time,
                    hours,
                    "RPC dispatcher historical scan window"
                );
                processor_stats.insert("historical_windows_processed".to_string(), 1);
                processor_stats.insert("historical_window_hours".to_string(), hours);
                successful_targets.push(format!("historical:{}->{}", start, end_time));
                Checkpoint::timestamp(end_time, None)
            }
            TimeHorizon::Continuous => {
                info!("RPC dispatcher starting continuous RPC monitoring (stub)");
                processor_stats.insert("continuous_monitoring".to_string(), 1);
                warnings.push(
                    "RPC dispatcher continuous mode is stubbed; wire RPC metrics here".to_string(),
                );
                from.clone()
            }
        };

        let report = ScanReport {
            events_processed,
            duration: std::time::Duration::from_millis(
                (Utc::now() - start_time).num_milliseconds() as u64,
            ),
            final_checkpoint: final_checkpoint.clone(),
            time_range: Some((start_time, Utc::now())),
            processor_stats: processor_stats
                .into_iter()
                .chain([
                    ("rpc_handlers_registered".to_string(), 0),
                    ("active_connections".to_string(), 0),
                ])
                .collect(),
            successful_targets,
            failed_targets,
            warnings,
        };

        self.last_checkpoint = report.final_checkpoint.clone();
        self.record_history(IngestionHistoryEntry {
            id: sinex_core::Ulid::new().to_string(),
            started_at: start_time,
            completed_at: Some(Utc::now()),
            events_generated: events_processed,
            scan_report: Some(report.clone()),
            error: None,
        });

        Ok(report)
    }

    fn processor_name(&self) -> &str {
        "rpc-dispatcher"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(self.last_checkpoint.clone())
    }
}

impl Default for RpcDispatcherProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplorationProvider for RpcDispatcherProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        let latest = self.history.front();
        let metadata = HashMap::from([
            (
                "service_name".to_string(),
                serde_json::json!(self.service_name),
            ),
            (
                "last_checkpoint".to_string(),
                serde_json::json!(self.last_checkpoint.description()),
            ),
            (
                "history_depth".to_string(),
                serde_json::json!(self.history.len()),
            ),
        ]);

        Ok(SourceState {
            description: "RPC dispatcher status".to_string(),
            last_updated: latest.and_then(|h| h.completed_at).unwrap_or_else(Utc::now),
            total_items: Some(self.history.len() as u64),
            metadata,
            healthy: true,
            recent_activity: latest
                .map(|entry| {
                    vec![sinex_processor_runtime::ActivityEntry {
                        timestamp: entry.completed_at.unwrap_or(entry.started_at),
                        description: format!(
                            "Last scan processed {} targets",
                            entry
                                .scan_report
                                .as_ref()
                                .map(|r| r.successful_targets.len())
                                .unwrap_or(0)
                        ),
                        data: entry
                            .scan_report
                            .as_ref()
                            .map(|r| serde_json::json!(r.processor_stats)),
                    }]
                })
                .unwrap_or_default(),
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        Ok(self.history.iter().cloned().collect())
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        // Use provided time range or default to configured historical scan hours
        let now = chrono::Utc::now();
        let default_hours = self.config.historical_scan_hours.unwrap_or(24);
        let default_hours_i64 = i64::try_from(default_hours)
            .map_err(|_| eyre!("historical_scan_hours {} exceeds i64 range", default_hours))?;

        let (start, end) =
            time_range.unwrap_or_else(|| (now - chrono::Duration::hours(default_hours_i64), now));

        let source_total: u64 = self.history.iter().map(|h| h.events_generated).sum();
        let sinex_total: u64 = self
            .history
            .iter()
            .filter_map(|h| h.scan_report.as_ref())
            .map(|r| r.events_processed)
            .sum();
        let coverage_percentage = if source_total == 0 {
            0.0
        } else {
            (sinex_total as f64 / source_total as f64) * 100.0
        };

        let mut missing_samples = Vec::new();
        if coverage_percentage < 100.0 {
            for entry in self.history.iter().take(3) {
                missing_samples.push(sinex_processor_runtime::MissingItem {
                    source_id: entry.id.clone(),
                    timestamp: entry.started_at,
                    description: "Missing dispatcher statistics".to_string(),
                    missing_reason: Some("Dispatcher wiring not completed".to_string()),
                });
            }
        }

        Ok(CoverageAnalysis {
            time_range: (start, end),
            coverage_percentage,
            missing_count: missing_samples.len() as u64,
            duplicate_count: 0,
            source_total,
            sinex_total,
            missing_samples,
            recommendations: vec![
                "Wire dispatcher into RPC server metrics to report coverage".to_string()
            ],
        })
    }

    fn export_data(
        &self,
        path: &sinex_core::SanitizedPath,
        format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        let path = camino::Utf8Path::new(path.as_str());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        match format {
            ExportFormat::Json => {
                let data = serde_json::to_vec_pretty(&self.history)?;
                fs::write(path.as_std_path(), data)?;
            }
            ExportFormat::Csv => {
                let mut w = fs::File::create(path.as_std_path())?;
                writeln!(
                    w,
                    "id,started_at,completed_at,events_generated,successful_targets,warnings"
                )?;
                for entry in &self.history {
                    let started = entry.started_at.to_rfc3339();
                    let completed = entry
                        .completed_at
                        .map(|d| d.to_rfc3339())
                        .unwrap_or_else(|| "".to_string());
                    let (targets, warnings) = entry
                        .scan_report
                        .as_ref()
                        .map(|r| (r.successful_targets.join("|"), r.warnings.join("|")))
                        .unwrap_or_default();
                    writeln!(
                        w,
                        "{},{},{},{},{},{}",
                        entry.id, started, completed, entry.events_generated, targets, warnings
                    )?;
                }
            }
            ExportFormat::Raw => {
                let mut w = fs::File::create(path.as_std_path())?;
                for entry in &self.history {
                    writeln!(w, "{entry:?}")?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    async fn scan_all_horizons_succeeds() -> color_eyre::Result<()> {
        let mut proc = RpcDispatcherProcessor::default();

        // Snapshot
        let report = proc
            .scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
            .await?;
        assert!(report.processor_stats.contains_key("snapshot_taken"));
        assert_eq!(proc.history.len(), 1);

        // Historical
        let end = Utc::now();
        let report = proc
            .scan(
                Checkpoint::None,
                TimeHorizon::Historical { end_time: end },
                ScanArgs {
                    targets: vec!["rpc-history".into()],
                    ..Default::default()
                },
            )
            .await?;
        assert!(report
            .processor_stats
            .contains_key("historical_windows_processed"));
        assert!(matches!(proc.last_checkpoint, Checkpoint::Timestamp { .. }));

        // Continuous
        let report = proc
            .scan(
                Checkpoint::None,
                TimeHorizon::Continuous,
                ScanArgs::default(),
            )
            .await?;
        assert!(report.processor_stats.contains_key("continuous_monitoring"));
        assert!(proc.history.len() >= 3);

        Ok(())
    }

    #[sinex_test]
    async fn exploration_provider_returns_stubbed_data() -> color_eyre::Result<()> {
        let mut proc = RpcDispatcherProcessor::default();
        proc.scan(Checkpoint::None, TimeHorizon::Snapshot, ScanArgs::default())
            .await?;

        let state = proc.get_source_state()?;
        assert!(state.metadata.contains_key("service_name"));

        let history = proc.get_ingestion_history(10)?;
        assert!(!history.is_empty());

        let coverage = proc.get_coverage_analysis(None)?;
        assert!(coverage.coverage_percentage >= 0.0);

        let path = sinex_core::SanitizedPath::from_str_validated("/tmp/rpc-export")
            .map_err(|e| color_eyre::eyre::eyre!(e))?;
        proc.export_data(&path, ExportFormat::Json)?;

        Ok(())
    }
}
