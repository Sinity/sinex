#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../../docs/current/architecture/UserInteraction_And_Query_Architecture.md")]

//! RPC Dispatcher - Unified `StatefulStreamProcessor` implementation.

// External crates
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sinex_processor_runtime::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, IngestionHistoryEntry, SourceState,
};
use sinex_satellite_sdk::stream_processor::{
    Checkpoint, ProcessorInitContext, ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor,
    TimeHorizon,
};
use sinex_satellite_sdk::{SatelliteError, SatelliteResult};
use validator::Validate;

// Standard library
use std::collections::HashMap;
use tracing::{info, warn};

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
pub struct RpcDispatcherProcessor;

impl RpcDispatcherProcessor {
    pub fn new() -> Self {
        Self
    }

    fn scan_snapshot(&self) -> SatelliteResult<ScanReport> {
        Ok(ScanReport {
            events_processed: 0,
            duration: std::time::Duration::from_millis(0),
            final_checkpoint: Checkpoint::None,
            time_range: None,
            processor_stats: HashMap::from([
                ("rpc_handlers_registered".to_string(), 0),
                ("active_connections".to_string(), 0),
            ]),
            successful_targets: Vec::new(),
            failed_targets: Vec::new(),
            warnings: Vec::new(),
        })
    }
}

#[async_trait]
impl StatefulStreamProcessor for RpcDispatcherProcessor {
    type Config = RpcDispatcherConfig;

    async fn initialize(
        &mut self,
        init: ProcessorInitContext<Self::Config>,
    ) -> SatelliteResult<()> {
        let (_config, _raw_config, service_info, _handles, _work_dir) = init.into_parts();
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
        let events_processed = 0;
        let mut warnings = Vec::new();

        match until {
            TimeHorizon::Snapshot => {
                info!("RPC dispatcher taking snapshot of current RPC configuration");
                return self.scan_snapshot();
            }
            TimeHorizon::Historical { .. } => {
                info!("RPC dispatcher scanning historical RPC invocations");
                warnings.push(
                    "Historical scan not yet implemented (would pull RPC call history/logs)"
                        .to_string(),
                );
            }
            TimeHorizon::Continuous => {
                info!("RPC dispatcher starting continuous RPC monitoring");
                warnings.push(
                    "Continuous monitoring not yet implemented (would stream RPC call metrics)"
                        .to_string(),
                );
            }
        }

        Ok(ScanReport {
            events_processed,
            duration: std::time::Duration::from_millis(
                (Utc::now() - start_time).num_milliseconds() as u64,
            ),
            final_checkpoint: Checkpoint::None,
            time_range: Some((start_time, Utc::now())),
            processor_stats: HashMap::from([
                ("rpc_handlers_registered".to_string(), 0),
                ("active_connections".to_string(), 0),
            ]),
            successful_targets: args.targets,
            failed_targets: Vec::new(),
            warnings,
        })
    }

    fn processor_name(&self) -> &str {
        "rpc-dispatcher"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

impl Default for RpcDispatcherProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl ExplorationProvider for RpcDispatcherProcessor {
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState> {
        Ok(SourceState {
            description: "RPC dispatcher (scan/explore entrypoint)".to_string(),
            last_updated: Utc::now(),
            total_items: None,
            metadata: HashMap::from([
                ("status".to_string(), serde_json::json!("operational")),
                ("active_handlers".to_string(), serde_json::json!(0)),
            ]),
            healthy: true,
            recent_activity: Vec::new(),
        })
    }

    fn get_ingestion_history(
        &self,
        _limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>> {
        Ok(Vec::new())
    }

    fn get_coverage_analysis(
        &self,
        time_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis> {
        let now = chrono::Utc::now();
        let (start, end) = time_range.unwrap_or_else(|| (now - chrono::Duration::hours(1), now));
        Ok(CoverageAnalysis {
            time_range: (start, end),
            source_total: 0,
            sinex_total: 0,
            coverage_percentage: 0.0,
            missing_count: 0,
            missing_samples: Vec::new(),
            duplicate_count: 0,
            recommendations: vec!["RPC dispatcher coverage analysis not yet implemented".to_string()],
        })
    }

    fn export_data(
        &self,
        _path: &sinex_core::SanitizedPath,
        _format: ExportFormat,
    ) -> color_eyre::eyre::Result<()> {
        warn!("RPC dispatcher data export requested but not implemented");
        Err(color_eyre::eyre::eyre!(
            "RPC dispatcher data export not implemented - would export RPC call logs and metrics"
        ))
    }
}
