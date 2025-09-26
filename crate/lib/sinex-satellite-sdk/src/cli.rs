//! Unified CLI structure for stream processor satellites
//!
//! This module provides the standardized CLI interface for all satellite binaries
//! implementing the service/scan/explore subcommand pattern.

use crate::stream_processor::{Checkpoint, ScanReport, TimeHorizon};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use color_eyre::eyre::{self, Context};
use serde::{Deserialize, Serialize};
use sinex_core::SanitizedPath;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::info;
use tracing::warn;

/// Standard CLI arguments for all stream processor satellites
#[derive(Parser, Debug, Clone)]
#[command(
    name = "sinex-processor",
    about = "Sinex Stream Processor Satellite",
    version
)]
pub struct ProcessorCli {
    /// Socket path for ingestd communication
    #[arg(long, default_value = "/tmp/sinex-ingestd.sock", value_parser = validate_socket_path)]
    pub ingest_socket_path: SanitizedPath,

    /// Database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Service name for identification
    #[arg(long)]
    pub service_name: Option<String>,

    /// Working directory for temporary files
    #[arg(long, value_parser = validate_work_dir)]
    pub work_dir: Option<SanitizedPath>,

    /// Enable verbose logging
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Processor-specific configuration as JSON
    #[arg(long)]
    pub processor_config: Option<String>,

    /// DEPRECATED: Direct NATS publishing bypasses ingestd single-writer principle (ignored)
    #[arg(long, env = "SINEX_USE_NATS", hide = true)]
    #[allow(dead_code)]
    pub use_nats: bool,

    /// DEPRECATED: NATS server URLs - no longer used, all events go through gRPC to ingestd
    #[arg(long, env = "SINEX_NATS_SERVERS", hide = true)]
    #[allow(dead_code)]
    pub nats_servers: Option<String>,

    #[command(subcommand)]
    pub command: ProcessorCommand,
}

/// Available subcommands for stream processors
#[derive(Subcommand, Debug, Clone)]
pub enum ProcessorCommand {
    /// Run as a long-running service (with startup sequence)
    Service {
        /// Enable dry-run mode
        #[arg(long)]
        dry_run: bool,

        /// Override consumer group name
        #[arg(long)]
        consumer_group: Option<String>,
    },

    /// Perform a one-off scan operation
    Scan {
        /// Checkpoint to start from (JSON format or "none")
        #[arg(long, default_value = "none")]
        from: String,

        /// Time horizon for scan ("continuous", "snapshot", or ISO timestamp)
        #[arg(long, default_value = "snapshot")]
        until: String,

        /// Targets to scan (paths, filters, etc.)
        #[arg(long, value_parser = validate_scan_target)]
        targets: Vec<SanitizedPath>,

        /// Enable dry-run mode (don't emit events)
        #[arg(long)]
        dry_run: bool,

        /// Enable interactive mode
        #[arg(long)]
        interactive: bool,

        /// Maximum events to process (0 = unlimited)
        #[arg(long, default_value = "0")]
        max_events: u64,

        /// Skip duplicate detection
        #[arg(long)]
        no_skip_duplicates: bool,

        /// Show estimation before execution
        #[arg(long)]
        estimate: bool,
    },

    /// Interactive exploration and diagnostics
    Explore {
        /// Show current source state
        #[arg(long)]
        source_state: bool,

        /// Show ingestion history
        #[arg(long)]
        ingestion_history: bool,

        /// Show coverage analysis (diff between source and Sinex)
        #[arg(long)]
        coverage_analysis: bool,

        /// Number of recent entries to show
        #[arg(long, default_value = "10")]
        limit: u64,

        /// Export data to file
        #[arg(long, value_parser = validate_export_path)]
        export_to: Option<SanitizedPath>,
    },
}

/// Parse checkpoint as JSON
fn parse_checkpoint_json(checkpoint_str: &str) -> eyre::Result<Checkpoint> {
    serde_json::from_str::<serde_json::Value>(checkpoint_str)
        .and_then(serde_json::from_value::<Checkpoint>)
        .context("Invalid checkpoint JSON")
}

/// Parse checkpoint as timestamp
fn parse_checkpoint_timestamp(checkpoint_str: &str) -> eyre::Result<Checkpoint> {
    checkpoint_str
        .parse::<DateTime<Utc>>()
        .map(|ts| Checkpoint::timestamp(ts, None))
        .context("Invalid timestamp format")
}

/// Parse checkpoint as stream ID
fn parse_checkpoint_stream(checkpoint_str: &str) -> Checkpoint {
    Checkpoint::stream(checkpoint_str, None)
}

/// Parse checkpoint from string representation
pub fn parse_checkpoint(checkpoint_str: &str) -> eyre::Result<Checkpoint> {
    if matches!(
        checkpoint_str,
        "none" | "start" | "None" | "Start" | "NONE" | "START"
    ) {
        Ok(Checkpoint::None)
    } else {
        parse_checkpoint_json(checkpoint_str)
            .or_else(|_| parse_checkpoint_timestamp(checkpoint_str))
            .or_else(|_| Ok(parse_checkpoint_stream(checkpoint_str)))
    }
}

/// Validate and parse socket path argument
pub fn validate_socket_path(s: &str) -> Result<SanitizedPath, String> {
    if s.is_empty() {
        return Err("Socket path cannot be empty".to_string());
    }
    SanitizedPath::from_str(s)
}

/// Validate and parse working directory argument
pub fn validate_work_dir(s: &str) -> Result<SanitizedPath, String> {
    if s.is_empty() {
        return Err("Working directory path cannot be empty".to_string());
    }
    SanitizedPath::from_str(s)
}

/// Validate and parse scan target path
pub fn validate_scan_target(s: &str) -> Result<SanitizedPath, String> {
    if s.is_empty() {
        return Err("Scan target path cannot be empty".to_string());
    }
    SanitizedPath::from_str(s)
}

/// Validate and parse export file path
pub fn validate_export_path(s: &str) -> Result<SanitizedPath, String> {
    if s.is_empty() {
        return Err("Export path cannot be empty".to_string());
    }
    SanitizedPath::from_str(s)
}

/// Parse time horizon from string representation
pub fn parse_time_horizon(horizon_str: &str) -> eyre::Result<TimeHorizon> {
    if matches!(
        horizon_str,
        "continuous"
            | "stream"
            | "sensor"
            | "Continuous"
            | "Stream"
            | "Sensor"
            | "CONTINUOUS"
            | "STREAM"
            | "SENSOR"
    ) {
        Ok(TimeHorizon::Continuous)
    } else if matches!(
        horizon_str,
        "snapshot"
            | "current"
            | "now"
            | "Snapshot"
            | "Current"
            | "Now"
            | "SNAPSHOT"
            | "CURRENT"
            | "NOW"
    ) {
        Ok(TimeHorizon::Snapshot)
    } else {
        // Try to parse as ISO timestamp for historical scan
        horizon_str
            .parse::<DateTime<Utc>>()
            .map(|dt| TimeHorizon::Historical { end_time: dt })
            .with_context(|| {
                format!(
                    "Invalid time horizon '{}'. Use 'continuous', 'snapshot', or ISO timestamp",
                    horizon_str
                )
            })
    }
}

/// Source state information for exploration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceState {
    /// Human-readable description of current state
    pub description: String,

    /// Last update timestamp
    pub last_updated: DateTime<Utc>,

    /// Total items/records available
    pub total_items: Option<u64>,

    /// Source-specific metadata
    pub metadata: HashMap<String, serde_json::Value>,

    /// Health status
    pub healthy: bool,

    /// Recent activity summary
    pub recent_activity: Vec<ActivityEntry>,
}

/// Activity entry for source state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityEntry {
    /// Timestamp of activity
    pub timestamp: DateTime<Utc>,

    /// Activity description
    pub description: String,

    /// Optional associated data
    pub data: Option<serde_json::Value>,
}

/// Ingestion history entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionHistoryEntry {
    /// Scan/ingestion ID
    pub id: String,

    /// Start time
    pub started_at: DateTime<Utc>,

    /// End time (if completed)
    pub completed_at: Option<DateTime<Utc>>,

    /// Number of events generated
    pub events_generated: u64,

    /// Scan report summary
    pub scan_report: Option<ScanReport>,

    /// Error message if failed
    pub error: Option<String>,
}

/// Coverage analysis comparing source vs Sinex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageAnalysis {
    /// Time range analyzed
    pub time_range: (DateTime<Utc>, DateTime<Utc>),

    /// Total items in source
    pub source_total: u64,

    /// Total events in Sinex for this source
    pub sinex_total: u64,

    /// Coverage percentage
    pub coverage_percentage: f64,

    /// Missing items in Sinex
    pub missing_count: u64,

    /// Sample of missing items
    pub missing_samples: Vec<MissingItem>,

    /// Duplicate items in Sinex
    pub duplicate_count: u64,

    /// Recommendations for improving coverage
    pub recommendations: Vec<String>,
}

/// Missing item information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingItem {
    /// Item identifier in source system
    pub source_id: String,

    /// Item timestamp
    pub timestamp: DateTime<Utc>,

    /// Brief description
    pub description: String,

    /// Reason for being missing
    pub missing_reason: Option<String>,
}

/// Trait for processor-specific exploration capabilities
pub trait ExplorationProvider {
    /// Get current source state
    fn get_source_state(&self) -> color_eyre::eyre::Result<SourceState>;

    /// Get ingestion history
    fn get_ingestion_history(
        &self,
        limit: u64,
    ) -> color_eyre::eyre::Result<Vec<IngestionHistoryEntry>>;

    /// Perform coverage analysis
    fn get_coverage_analysis(
        &self,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> color_eyre::eyre::Result<CoverageAnalysis>;

    /// Export data for debugging
    fn export_data(
        &self,
        path: &SanitizedPath,
        format: ExportFormat,
    ) -> color_eyre::eyre::Result<()>;
}

/// Export format options
#[derive(Debug, Clone, Copy)]
pub enum ExportFormat {
    Json,
    Csv,
    Raw,
}

/// Macro to generate default ExplorationProvider implementation for automata
///
/// This reduces code duplication across automaton processors by providing
/// a standard stub implementation with only the description customizable.
///
/// # Usage
///
/// ```rust
/// use sinex_satellite_sdk::default_exploration_provider;
///
/// struct MyProcessor;
///
/// default_exploration_provider!(MyProcessor, "My processor description");
/// ```
#[macro_export]
macro_rules! default_exploration_provider {
    ($processor_type:ty, $description:expr) => {
        impl $crate::cli::ExplorationProvider for $processor_type {
            fn get_source_state(&self) -> color_eyre::eyre::Result<$crate::cli::SourceState> {
                use std::collections::HashMap;

                Ok($crate::cli::SourceState {
                    description: $description.to_string(),
                    last_updated: chrono::Utc::now(),
                    total_items: Some(0),
                    metadata: HashMap::new(),
                    healthy: true,
                    recent_activity: Vec::new(),
                })
            }

            fn get_ingestion_history(
                &self,
                _limit: u64,
            ) -> color_eyre::eyre::Result<Vec<$crate::cli::IngestionHistoryEntry>> {
                Ok(Vec::new())
            }

            fn get_coverage_analysis(
                &self,
                _time_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
            ) -> color_eyre::eyre::Result<$crate::cli::CoverageAnalysis> {
                let now = chrono::Utc::now();
                Ok($crate::cli::CoverageAnalysis {
                    time_range: (now - chrono::Duration::days(1), now),
                    source_total: 0,
                    sinex_total: 0,
                    coverage_percentage: 0.0,
                    missing_count: 0,
                    missing_samples: Vec::new(),
                    duplicate_count: 0,
                    recommendations: Vec::new(),
                })
            }

            fn export_data(
                &self,
                _path: &sinex_core::SanitizedPath,
                _format: $crate::cli::ExportFormat,
            ) -> color_eyre::eyre::Result<()> {
                Ok(())
            }
        }
    };
}

/// Generic CLI runner for stream processor satellites
///
/// This provides a standardized way to run any StatefulStreamProcessor with
/// the unified CLI interface supporting service/scan/explore subcommands.
pub struct ProcessorCliRunner<
    T: crate::stream_processor::StatefulStreamProcessor + ExplorationProvider + 'static,
> {
    processor: Option<T>,
}

impl<T: crate::stream_processor::StatefulStreamProcessor + ExplorationProvider + 'static>
    ProcessorCliRunner<T>
{
    /// Create new CLI runner with a processor instance
    pub fn new(processor: T) -> Self {
        Self {
            processor: Some(processor),
        }
    }

    /// Run the CLI with parsed arguments
    pub async fn run(&mut self, args: ProcessorCli) -> color_eyre::eyre::Result<()> {
        use crate::grpc_client::IngestClient;
        use crate::stream_processor::{ScanArgs, StreamProcessorRunner};
        use sinex_core::db::SqlxPgPool;

        // Initialize logging based on verbosity
        let log_level = match args.verbose {
            0 => "info",
            1 => "debug",
            _ => "trace",
        };

        tracing_subscriber::fmt()
            .with_env_filter(format!("sinex={}", log_level))
            .init();

        // Parse processor configuration
        let processor_config: HashMap<String, serde_json::Value> =
            if let Some(config_str) = args.processor_config {
                serde_json::from_str(&config_str)
                    .context("Failed to parse processor configuration JSON")?
            } else {
                HashMap::new()
            };

        // Take ownership of the processor
        let processor = self
            .processor
            .take()
            .ok_or_else(|| eyre::eyre!("Processor already consumed"))?;

        match args.command {
            ProcessorCommand::Service {
                dry_run,
                consumer_group: _,
            } => {
                info!("Starting processor service mode");

                // Create stream processor runner
                let mut runner = StreamProcessorRunner::new(processor);

                // Set up dependencies
                let service_name = args
                    .service_name
                    .unwrap_or_else(|| "sinex-processor".to_string());
                let work_dir = args
                    .work_dir
                    .unwrap_or_else(|| SanitizedPath::new_unchecked("/tmp/sinex/processor"));

                // Create database pool
                let db_pool = if let Some(db_url) = args.database_url {
                    SqlxPgPool::connect(&db_url)
                        .await
                        .context("Failed to connect to database")?
                } else {
                    let db_url = std::env::var("DATABASE_URL")
                        .context("DATABASE_URL environment variable not set")?;
                    SqlxPgPool::connect(&db_url)
                        .await
                        .context("Failed to connect to database using DATABASE_URL")?
                };

                // Always use gRPC to enforce single-writer principle through ingestd
                if args.use_nats {
                    warn!("--use-nats flag is deprecated and ignored. All events now go through gRPC to ingestd to enforce single-writer principle.");
                }

                info!("Using gRPC for event publishing");

                // Create ingest client
                let ingest_client = IngestClient::new(args.ingest_socket_path.as_str())
                    .await
                    .context("Failed to create ingest client")?;

                // Initialize runner
                runner
                    .initialize(
                        service_name.clone(),
                        processor_config,
                        db_pool.clone(),
                        ingest_client,
                        std::path::PathBuf::from(work_dir.as_str()),
                        dry_run,
                    )
                    .await?;

                // Run service with satellite coordination
                if dry_run {
                    // Skip coordination for dry runs
                    runner.run_service().await?;
                } else {
                    use crate::coordination::SatelliteCoordination;

                    use std::sync::Arc;
                    use tokio::sync::Mutex;
                    use uuid::Uuid;

                    // Create coordination with generated instance ID
                    let instance_id = Uuid::new_v4().to_string();

                    let coordination =
                        SatelliteCoordination::new(service_name.clone(), instance_id, db_pool);

                    // Wrap runner in Arc<Mutex<>> for sharing
                    let runner = Arc::new(Mutex::new(runner));

                    // Run with coordination (hot standby pattern)
                    coordination?
                        .run_coordination_loop(move || {
                            let runner = runner.clone();
                            async move {
                                // Only leader processes events
                                let mut runner = runner.lock().await;
                                runner.run_service().await.map_err(|e| {
                                    sinex_core::SinexError::service(format!(
                                        "Satellite error: {}",
                                        e
                                    ))
                                })
                            }
                        })
                        .await?;
                }
            }

            ProcessorCommand::Scan {
                from,
                until,
                targets,
                dry_run,
                interactive,
                max_events,
                no_skip_duplicates,
                estimate,
            } => {
                info!("Running scan operation");

                let checkpoint = parse_checkpoint(&from).context("Failed to parse checkpoint")?;
                let time_horizon =
                    parse_time_horizon(&until).context("Failed to parse time horizon")?;

                // Create stream processor runner
                let mut runner = StreamProcessorRunner::new(processor);

                // Set up minimal dependencies for scan mode
                let service_name = args
                    .service_name
                    .unwrap_or_else(|| "sinex-processor".to_string());
                let work_dir = args
                    .work_dir
                    .unwrap_or_else(|| SanitizedPath::new_unchecked("/tmp/sinex/processor"));

                // For scan mode, database connection is optional for dry runs
                let db_pool = if dry_run {
                    // Create dummy pool for dry runs - the processor should handle this gracefully
                    match SqlxPgPool::connect("postgresql://localhost/dummy").await {
                        Ok(pool) => pool,
                        Err(_) => {
                            // If no database available, try environment variable
                            if let Ok(db_url) = std::env::var("DATABASE_URL") {
                                SqlxPgPool::connect(&db_url)
                                    .await
                                    .context("Failed to connect to database")?
                            } else {
                                return Err(eyre::eyre!(
                                    "Database connection required even for dry runs"
                                ));
                            }
                        }
                    }
                } else if let Some(db_url) = args.database_url {
                    SqlxPgPool::connect(&db_url)
                        .await
                        .context("Failed to connect to database")?
                } else {
                    let db_url = std::env::var("DATABASE_URL")
                        .context("DATABASE_URL environment variable not set")?;
                    SqlxPgPool::connect(&db_url)
                        .await
                        .context("Failed to connect to database using DATABASE_URL")?
                };

                // Initialize runner with gRPC by default (always for dry runs, optional NATS bypass)
                if args.use_nats {
                    warn!("--use-nats flag is deprecated and ignored. All events now go through gRPC to ingestd to enforce single-writer principle.");
                }

                info!("Using gRPC for event publishing");

                let ingest_client = IngestClient::new(args.ingest_socket_path.as_str())
                    .await
                    .context("Failed to create ingest client")?;

                // Initialize runner
                runner
                    .initialize(
                        service_name,
                        processor_config,
                        db_pool,
                        ingest_client,
                        std::path::PathBuf::from(work_dir.as_str()),
                        dry_run,
                    )
                    .await?;

                // Create scan args
                let scan_args = ScanArgs {
                    targets: targets.into_iter().map(|p| p.to_string()).collect(),
                    dry_run,
                    interactive,
                    max_events,
                    skip_duplicates: !no_skip_duplicates,
                    config: HashMap::new(),
                };

                // Run estimation if requested
                if estimate {
                    let estimate_result = runner
                        .estimate_scan_scope(&checkpoint, &time_horizon, &scan_args)
                        .await?;
                    println!("Scan Estimation:");
                    println!("  Estimated events: {}", estimate_result.estimated_events);
                    println!(
                        "  Estimated duration: {:?}",
                        estimate_result.estimated_duration
                    );
                    println!(
                        "  Estimated data size: {} bytes",
                        estimate_result.estimated_data_size
                    );
                    println!("  Estimated targets: {}", estimate_result.estimated_targets);
                    println!("  Confidence: {:.1}%", estimate_result.confidence * 100.0);
                    if !estimate_result.warnings.is_empty() {
                        println!("  Warnings:");
                        for warning in &estimate_result.warnings {
                            println!("    - {}", warning);
                        }
                    }
                    println!();

                    if interactive {
                        print!("Proceed with scan? [y/N] ");
                        use std::io::{self, Write};
                        io::stdout().flush()?;
                        let mut input = String::new();
                        io::stdin().read_line(&mut input)?;
                        if !input.trim().to_lowercase().starts_with('y') {
                            println!("Scan cancelled");
                            return Ok(());
                        }
                    }
                }

                // Run scan
                let report = runner.run_scan(checkpoint, time_horizon, scan_args).await?;

                // Display results
                println!("Scan Results:");
                println!("  Events processed: {}", report.events_processed);
                println!("  Duration: {:?}", report.duration);
                println!(
                    "  Final checkpoint: {}",
                    report.final_checkpoint.description()
                );

                if let Some((start, end)) = report.time_range {
                    println!(
                        "  Time range: {} to {}",
                        start.format("%Y-%m-%d %H:%M:%S"),
                        end.format("%Y-%m-%d %H:%M:%S")
                    );
                }

                if !report.processor_stats.is_empty() {
                    println!("  Processor stats:");
                    for (key, value) in &report.processor_stats {
                        println!("    {}: {}", key, value);
                    }
                }

                if !report.successful_targets.is_empty() {
                    println!("  Successful targets: {}", report.successful_targets.len());
                    for target in &report.successful_targets {
                        println!("    - {}", target);
                    }
                }

                if !report.failed_targets.is_empty() {
                    println!("  Failed targets:");
                    for (target, error) in &report.failed_targets {
                        println!("    - {}: {}", target, error);
                    }
                }

                if !report.warnings.is_empty() {
                    println!("  Warnings:");
                    for warning in &report.warnings {
                        println!("    - {}", warning);
                    }
                }
            }

            ProcessorCommand::Explore {
                source_state,
                ingestion_history,
                coverage_analysis,
                limit,
                export_to,
            } => {
                info!("Running exploration mode");

                // For exploration, we can work with the processor directly
                if source_state {
                    match processor.get_source_state() {
                        Ok(state) => {
                            println!("Source State:");
                            println!("  Description: {}", state.description);
                            println!(
                                "  Last updated: {}",
                                state.last_updated.format("%Y-%m-%d %H:%M:%S")
                            );
                            if let Some(total) = state.total_items {
                                println!("  Total items: {}", total);
                            }
                            println!("  Healthy: {}", state.healthy);

                            if !state.recent_activity.is_empty() {
                                println!("  Recent activity:");
                                for activity in &state.recent_activity {
                                    println!(
                                        "    - {}: {}",
                                        activity.timestamp.format("%H:%M:%S"),
                                        activity.description
                                    );
                                }
                            }

                            if !state.metadata.is_empty() {
                                println!("  Metadata:");
                                for (key, value) in &state.metadata {
                                    println!("    {}: {}", key, value);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to get source state: {}", e);
                        }
                    }
                    println!();
                }

                if ingestion_history {
                    match processor.get_ingestion_history(limit) {
                        Ok(history) => {
                            println!("Ingestion History ({} entries):", history.len());
                            for entry in &history {
                                println!("  ID: {}", entry.id);
                                println!(
                                    "    Started: {}",
                                    entry.started_at.format("%Y-%m-%d %H:%M:%S")
                                );
                                if let Some(completed) = entry.completed_at {
                                    println!(
                                        "    Completed: {}",
                                        completed.format("%Y-%m-%d %H:%M:%S")
                                    );
                                }
                                println!("    Events: {}", entry.events_generated);
                                if let Some(error) = &entry.error {
                                    println!("    Error: {}", error);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to get ingestion history: {}", e);
                        }
                    }
                    println!();
                }

                if coverage_analysis {
                    match processor.get_coverage_analysis(None) {
                        Ok(analysis) => {
                            println!("Coverage Analysis:");
                            println!(
                                "  Time range: {} to {}",
                                analysis.time_range.0.format("%Y-%m-%d %H:%M:%S"),
                                analysis.time_range.1.format("%Y-%m-%d %H:%M:%S")
                            );
                            println!("  Source total: {}", analysis.source_total);
                            println!("  Sinex total: {}", analysis.sinex_total);
                            println!("  Coverage: {:.1}%", analysis.coverage_percentage);
                            println!("  Missing: {}", analysis.missing_count);
                            println!("  Duplicates: {}", analysis.duplicate_count);

                            if !analysis.missing_samples.is_empty() {
                                println!("  Missing samples:");
                                for sample in &analysis.missing_samples {
                                    println!(
                                        "    - {}: {} ({})",
                                        sample.source_id,
                                        sample.description,
                                        sample.missing_reason.as_deref().unwrap_or("Unknown")
                                    );
                                }
                            }

                            if !analysis.recommendations.is_empty() {
                                println!("  Recommendations:");
                                for rec in &analysis.recommendations {
                                    println!("    - {}", rec);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to get coverage analysis: {}", e);
                        }
                    }
                    println!();
                }

                if let Some(export_path) = export_to {
                    let path_buf = std::path::PathBuf::from(export_path.as_str());
                    let format = match path_buf.extension().and_then(|ext| ext.to_str()) {
                        Some("json") => ExportFormat::Json,
                        Some("csv") => ExportFormat::Csv,
                        _ => ExportFormat::Raw,
                    };

                    match processor.export_data(&export_path, format) {
                        Ok(_) => {
                            println!("Data exported to: {}", export_path.as_str());
                        }
                        Err(e) => {
                            eprintln!("Failed to export data: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Helper macro for creating processor CLI main functions with unified architecture
#[macro_export]
macro_rules! processor_main {
    ($processor_type:ty) => {
        #[tokio::main]
        async fn main() -> color_eyre::eyre::Result<()> {
            color_eyre::install()?;
            use clap::Parser;
            use $crate::cli::{ProcessorCli, ProcessorCliRunner, ProcessorCommand};
            use $crate::heartbeat::HeartbeatEmitter;

            let args = ProcessorCli::parse();
            let processor = <$processor_type>::new();
            let mut runner = ProcessorCliRunner::new(processor);

            // Auto-spawn HeartbeatEmitter and Coordination for service mode
            if matches!(args.command, ProcessorCommand::Service { .. }) {
                let service_name = args
                    .service_name
                    .clone()
                    .unwrap_or_else(|| "sinex-processor".to_string());

                let heartbeat_emitter = HeartbeatEmitter::new(service_name.clone(), 30);

                // Spawn heartbeat task concurrently
                tokio::spawn(async move {
                    heartbeat_emitter.start_periodic_heartbeat(None).await;
                });

                // SatelliteCoordination integrated below in service mode
            }

            runner.run(args).await
        }
    };

    ($processor_type:ty, $processor_expr:expr) => {
        #[tokio::main]
        async fn main() -> color_eyre::eyre::Result<()> {
            color_eyre::install()?;
            use clap::Parser;
            use $crate::cli::{ProcessorCli, ProcessorCliRunner, ProcessorCommand};
            use $crate::heartbeat::HeartbeatEmitter;

            let args = ProcessorCli::parse();
            let processor = $processor_expr;
            let mut runner = ProcessorCliRunner::new(processor);

            // Auto-spawn HeartbeatEmitter and Coordination for service mode
            if matches!(args.command, ProcessorCommand::Service { .. }) {
                let service_name = args
                    .service_name
                    .clone()
                    .unwrap_or_else(|| "sinex-processor".to_string());

                let heartbeat_emitter = HeartbeatEmitter::new(service_name.clone(), 30);

                // Spawn heartbeat task concurrently
                tokio::spawn(async move {
                    heartbeat_emitter.start_periodic_heartbeat(None).await;
                });

                // SatelliteCoordination integrated below in service mode
            }

            runner.run(args).await
        }
    };
}
