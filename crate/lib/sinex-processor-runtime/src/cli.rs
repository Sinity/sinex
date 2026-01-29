#![doc = include_str!("../docs/cli_framework.md")]

//! Unified CLI structure for stream processor nodes
//!
//! This module provides the standardized CLI interface for all node binaries
//! implementing the service/scan/explore subcommand pattern.

use clap::{Parser, Subcommand};
use sinex_db::SqlxPgPool;
use sinex_primitives::{SanitizedPath, Timestamp};
use sinex_node_sdk::config::ReplayConfig;
use sinex_node_sdk::event_node::EventTransport;
pub use sinex_node_sdk::exploration::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, MissingItem, SourceState,
};
use sinex_node_sdk::stream_processor::{Checkpoint, NodeRunner, TimeHorizon};
use sinex_node_sdk::{SinexError, NodeResult};
use time::OffsetDateTime;

// Re-export common types from sinex_node_sdk::automaton_base
// These are the canonical definitions used by all automatons
pub use sinex_node_sdk::{ActivityEntry, IngestionHistoryEntry};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::{info, warn};

use crate::replay::{ReplayFilters, ReplayMode, ReplayProgress, ReplayResult, ReplayRuntimeExt};

pub fn command_requires_heartbeat(command: &NodeCommand) -> bool {
    matches!(
        command,
        NodeCommand::Service { .. } | NodeCommand::Scan { .. } | NodeCommand::Explore { .. }
    )
}

/// Standard CLI arguments for all stream processor nodes
#[derive(Parser, Debug, Clone)]
#[command(name = "sinex-node", about = "Sinex Stream Node", version)]
pub struct NodeCli {
    /// NATS connection configuration
    #[command(flatten)]
    pub nats: NatsArgs,

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
    pub node_config: Option<String>,

    #[command(subcommand)]
    pub command: NodeCommand,
}

/// CLI arguments for configuring NATS connection options.
#[derive(clap::Args, Debug, Clone)]
pub struct NatsArgs {
    /// NATS server URL (nats:// or tls://)
    #[arg(long, env = "SINEX_NATS_URL", default_value = "nats://localhost:4222")]
    pub url: String,

    /// Optional logical connection name
    #[arg(long, env = "SINEX_NATS_NAME")]
    pub name: Option<String>,

    /// Require TLS for the connection
    // TODO: CRITICAL - TLS disabled by default. Production must explicitly enable it.
    #[arg(long, env = "SINEX_NATS_REQUIRE_TLS", default_value_t = false)]
    pub require_tls: bool,

    /// Root CA certificate path (PEM)
    #[arg(long, env = "SINEX_NATS_CA_CERT")]
    pub ca_cert: Option<PathBuf>,

    /// Client certificate path (PEM)
    #[arg(long, env = "SINEX_NATS_CLIENT_CERT")]
    pub client_cert: Option<PathBuf>,

    /// Client key path (PEM)
    #[arg(long, env = "SINEX_NATS_CLIENT_KEY")]
    pub client_key: Option<PathBuf>,

    /// Credentials file path (JWT + Key)
    #[arg(long, env = "SINEX_NATS_CREDS")]
    pub creds_file: Option<PathBuf>,

    /// NKey seed file path
    #[arg(long, env = "SINEX_NATS_NKEY_SEED")]
    pub nkey_file: Option<PathBuf>,

    /// Auth token
    #[arg(long, env = "SINEX_NATS_TOKEN")]
    pub token: Option<String>,
}

impl NatsArgs {
    fn to_config(&self) -> sinex_primitives::nats::NatsConnectionConfig {
        let mut config = sinex_primitives::nats::NatsConnectionConfig::from_env();

        config.url = self.url.clone();
        config.name = self.name.clone();
        config.require_tls = self.require_tls;

        if let Some(path) = &self.ca_cert {
            config.ca_cert = Some(path.clone());
        }
        if let Some(path) = &self.client_cert {
            config.client_cert = Some(path.clone());
        }
        if let Some(path) = &self.client_key {
            config.client_key = Some(path.clone());
        }
        if let Some(path) = &self.creds_file {
            config.creds_file = Some(path.clone());
        }
        if let Some(path) = &self.nkey_file {
            config.nkey_file = Some(path.clone());
        }
        if let Some(token) = &self.token {
            config.token = Some(token.clone());
        }

        config
    }
}

/// Available subcommands for stream processors
#[derive(Subcommand, Debug, Clone)]
pub enum NodeCommand {
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
fn parse_checkpoint_json(checkpoint_str: &str) -> NodeResult<Checkpoint> {
    // TODO: Add size limit to prevent DoS via massive JSON checkpoint strings
    let val: serde_json::Value =
        serde_json::from_str(checkpoint_str).map_err(|e| SinexError::serialization(e))?;
    serde_json::from_value(val).map_err(|e| SinexError::serialization(e))
}

/// Parse checkpoint as timestamp
fn parse_checkpoint_timestamp(checkpoint_str: &str) -> NodeResult<Checkpoint> {
    OffsetDateTime::parse(
        checkpoint_str,
        &time::format_description::well_known::Rfc3339,
    )
    .map(|ts| Checkpoint::timestamp(Timestamp::from(ts), None))
    .map_err(|e| SinexError::general(format!("Invalid timestamp format: {e}")))
}

/// Parse checkpoint as stream ID
fn parse_checkpoint_stream(checkpoint_str: &str) -> Checkpoint {
    Checkpoint::stream(checkpoint_str, None)
}

/// Parse checkpoint from string representation
pub fn parse_checkpoint(checkpoint_str: &str) -> NodeResult<Checkpoint> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    fn scan_mode_emits_heartbeats() -> TestResult<()> {
        let command = NodeCommand::Scan {
            from: "none".to_string(),
            until: "snapshot".to_string(),
            targets: Vec::new(),
            dry_run: false,
            interactive: false,
            max_events: 0,
            no_skip_duplicates: false,
            estimate: false,
        };

        assert!(command_requires_heartbeat(&command));

        Ok(())
    }

    #[sinex_test]
    fn explore_mode_emits_heartbeats() -> TestResult<()> {
        let command = NodeCommand::Explore {
            source_state: true,
            ingestion_history: false,
            coverage_analysis: false,
            limit: 5,
            export_to: None,
        };

        assert!(command_requires_heartbeat(&command));

        Ok(())
    }
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
pub fn parse_time_horizon(horizon_str: &str) -> NodeResult<TimeHorizon> {
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
        OffsetDateTime::parse(horizon_str, &time::format_description::well_known::Rfc3339)
            .map(|dt| TimeHorizon::Historical { end_time: Timestamp::from(dt) })
            .map_err(|e| {
                SinexError::general(format!(
                    "Invalid time horizon '{}': {}. Use 'continuous', 'snapshot', or ISO timestamp",
                    horizon_str, e
                ))
            })
    }
}

/// Generic CLI runner for stream processor nodes
///
/// This provides a standardized way to run any Node with
/// the unified CLI interface supporting service/scan/explore subcommands.
pub struct NodeCliRunner<T: sinex_node_sdk::stream_processor::Node + ExplorationProvider + 'static>
{
    node: Option<T>,
}

impl<T: sinex_node_sdk::stream_processor::Node + ExplorationProvider + 'static> NodeCliRunner<T> {
    /// Create new CLI runner with a node instance
    pub fn new(node: T) -> Self {
        Self { node: Some(node) }
    }

    /// Run the CLI with parsed arguments
    pub async fn run(&mut self, args: NodeCli) -> NodeResult<()> {
        use sinex_node_sdk::stream_processor::{NodeRunner, ScanArgs};

        // Initialize logging based on verbosity
        let log_level = match args.verbose {
            0 => "info",
            1 => "debug",
            _ => "trace",
        };

        if tracing_subscriber::fmt()
            .with_env_filter(format!("sinex={}", log_level))
            .try_init()
            .is_err()
        {
            tracing::debug!("Tracing already initialized, skipping reconfiguration");
        }

        // Parse processor configuration
        let mut node_config: HashMap<String, serde_json::Value> =
            if let Some(config_str) = args.node_config.clone() {
                serde_json::from_str(&config_str).map_err(|e| {
                    SinexError::general(format!(
                        "{}: {}",
                        "Failed to parse node configuration JSON", e
                    ))
                })?
            } else {
                HashMap::new()
            };

        if let NodeCommand::Service { consumer_group, .. } = &args.command {
            if let Some(group) = consumer_group {
                node_config
                    .entry("consumer_group".to_string())
                    .or_insert_with(|| serde_json::json!(group));
            }
        }

        let replay_config = Self::extract_replay_config(&mut node_config)?;

        // Take ownership of the node
        let node = self
            .node
            .take()
            .ok_or_else(|| SinexError::general("Node already consumed"))?;

        match args.command {
            NodeCommand::Service { dry_run, .. } => {
                info!("Starting node service mode");

                // Create stream processor runner
                let mut runner = NodeRunner::new(node);

                // Set up dependencies
                let service_name = Self::resolve_service_name(&args);
                let work_dir = Self::resolve_work_dir(&args);

                // Create database pool
                let db_pool = if dry_run {
                    None
                } else {
                    match Self::connect_primary_db(&args).await {
                        Ok(pool) => Some(pool),
                        Err(err) => {
                            // Check if we allow running without DB (Edge Mode)
                            let edge_mode = std::env::var("SINEX_EDGE_MODE").is_ok();
                            if edge_mode && args.database_url.is_none() {
                                warn!("Running in Edge Mode without database connection");
                                None
                            } else {
                                return Err(err);
                            }
                        }
                    }
                };
                let transport = Self::connect_nats_transport(&args.nats.to_config()).await?;

                // Initialize runner with transport
                runner
                    .initialize_with_transport(
                        service_name.clone(),
                        node_config.clone(),
                        db_pool.clone(),
                        transport,
                        std::path::PathBuf::from(work_dir.as_str()),
                        dry_run,
                    )
                    .await?;

                if !dry_run {
                    if let Some(cfg) = replay_config.clone() {
                        if let Err(err) = Self::execute_replay(&mut runner, cfg).await {
                            warn!(error = %err, "Replay execution failed to complete");
                        }
                    }
                } else if replay_config.is_some() {
                    warn!("Replay configuration ignored in dry-run mode");
                }

                let coordination_disabled = std::env::var("SINEX_COORDINATION_DISABLED")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);

                // Run service with optional coordination
                if dry_run || coordination_disabled {
                    runner.run_service().await?;
                } else {
                    use sinex_node_sdk::coordination::NodeCoordination;

                    use std::sync::Arc;
                    use tokio::sync::Mutex;
                    use uuid::Uuid;

                    let runtime_snapshot = runner.runtime_state().ok_or_else(|| {
                        SinexError::general("Runtime state unavailable for coordination")
                    })?;

                    // Create coordination with generated instance ID
                    let instance_id = Uuid::new_v4().to_string();

                    let coordination =
                        NodeCoordination::from_runtime(&runtime_snapshot, instance_id);

                    // Wrap runner in Arc<Mutex<>> for sharing
                    let runner = Arc::new(Mutex::new(runner));

                    // Run with coordination (hot standby pattern)
                    coordination
                        .await?
                        .run_coordination_loop(move || {
                            let runner = runner.clone();
                            async move {
                                // Only leader processes events
                                let mut runner = runner.lock().await;
                                runner.run_service().await.map_err(|e| {
                                    sinex_primitives::SinexError::service(format!("Node error: {}", e))
                                })
                            }
                        })
                        .await?;
                }
            }

            NodeCommand::Scan {
                ref from,
                ref until,
                ref targets,
                dry_run,
                interactive,
                max_events,
                no_skip_duplicates,
                estimate,
            } => {
                info!("Running scan operation");

                let checkpoint = parse_checkpoint(from).map_err(|e| {
                    SinexError::general(format!("{}: {}", "Failed to parse checkpoint", e))
                })?;
                let time_horizon = parse_time_horizon(until).map_err(|e| {
                    SinexError::general(format!("{}: {}", "Failed to parse time horizon", e))
                })?;

                // Create stream processor runner
                let mut runner = NodeRunner::new(node);

                // Set up minimal dependencies for scan mode
                let service_name = args
                    .service_name
                    .as_deref()
                    .unwrap_or("sinex-node")
                    .to_string();
                let work_dir = Self::resolve_work_dir(&args);

                // For scan mode, database connection is optional for dry runs
                let db_pool = if dry_run {
                    None
                } else {
                    Some(Self::connect_primary_db(&args).await?)
                };

                let transport = Self::connect_nats_transport(&args.nats.to_config()).await?;

                // Initialize runner with transport
                runner
                    .initialize_with_transport(
                        service_name,
                        node_config,
                        db_pool,
                        transport,
                        std::path::PathBuf::from(work_dir.as_str()),
                        dry_run,
                    )
                    .await?;

                // Create scan args
                let scan_args = ScanArgs {
                    targets: targets.iter().map(|p| p.to_string()).collect(),
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
                        start
                            .format(
                                &time::format_description::parse(
                                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                                )
                                .unwrap()
                            )
                            .unwrap_or_default(),
                        end.format(
                            &time::format_description::parse(
                                "[year]-[month]-[day] [hour]:[minute]:[second]"
                            )
                            .unwrap()
                        )
                        .unwrap_or_default()
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

            NodeCommand::Explore {
                source_state,
                ingestion_history,
                coverage_analysis,
                limit,
                export_to,
            } => {
                info!("Running exploration mode");

                // For exploration, we can work with the processor directly
                if source_state {
                    match node.get_source_state() {
                        Ok(state) => {
                            println!("Source State:");
                            println!("  Description: {}", state.description);
                            println!(
                                "  Last updated: {}",
                                state
                                    .last_updated
                                    .format(
                                        &time::format_description::parse(
                                            "[year]-[month]-[day] [hour]:[minute]:[second]"
                                        )
                                        .unwrap()
                                    )
                                    .unwrap_or_default()
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
                                        activity
                                            .timestamp
                                            .format(
                                                &time::format_description::parse(
                                                    "[hour]:[minute]:[second]"
                                                )
                                                .unwrap()
                                            )
                                            .unwrap_or_default(),
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
                            warn!(error = %e, "Failed to get source state");
                        }
                    }
                    println!();
                }

                if ingestion_history {
                    match node.get_ingestion_history(limit) {
                        Ok(history) => {
                            println!("Ingestion History ({} entries):", history.len());
                            for entry in &history {
                                println!("  ID: {}", entry.id);
                                println!(
                                    "    Started: {}",
                                    entry
                                        .started_at
                                        .format(
                                            &time::format_description::parse(
                                                "[year]-[month]-[day] [hour]:[minute]:[second]"
                                            )
                                            .unwrap()
                                        )
                                        .unwrap_or_default()
                                );
                                if let Some(completed) = entry.completed_at {
                                    println!(
                                        "    Completed: {}",
                                        completed
                                            .format(
                                                &time::format_description::parse(
                                                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                                                )
                                                .unwrap()
                                            )
                                            .unwrap_or_default()
                                    );
                                }
                                println!("    Events: {}", entry.events_generated);
                                if let Some(error) = &entry.error {
                                    println!("    Error: {}", error);
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to get ingestion history");
                        }
                    }
                    println!();
                }

                if coverage_analysis {
                    match node.get_coverage_analysis(None) {
                        Ok(analysis) => {
                            println!("Coverage Analysis:");
                            println!(
                                "  Time range: {} to {}",
                                analysis
                                    .time_range
                                    .0
                                    .format(
                                        &time::format_description::parse(
                                            "[year]-[month]-[day] [hour]:[minute]:[second]"
                                        )
                                        .unwrap()
                                    )
                                    .unwrap_or_default(),
                                analysis
                                    .time_range
                                    .1
                                    .format(
                                        &time::format_description::parse(
                                            "[year]-[month]-[day] [hour]:[minute]:[second]"
                                        )
                                        .unwrap()
                                    )
                                    .unwrap_or_default()
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
                            warn!(error = %e, "Failed to get coverage analysis");
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

                    match node.export_data(&export_path, format) {
                        Ok(_) => {
                            println!("Data exported to: {}", export_path.as_str());
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to export data");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn resolve_service_name(args: &NodeCli) -> String {
        args.service_name
            .clone()
            .unwrap_or_else(|| "sinex-node".to_string())
    }

    fn resolve_work_dir(args: &NodeCli) -> SanitizedPath {
        args.work_dir.clone().unwrap_or_else(|| {
            let env = sinex_primitives::environment();
            let namespaced = env.work_directory("/tmp/sinex/node");
            let namespaced = namespaced.to_string_lossy();
            SanitizedPath::from_str(namespaced.as_ref())
                // TODO: CRITICAL - Validation bypass! new_unchecked defeats the purpose of SanitizedPath.
                .unwrap_or_else(|_| SanitizedPath::new_unchecked(namespaced.as_ref()))
        })
    }

    async fn connect_primary_db(args: &NodeCli) -> NodeResult<SqlxPgPool> {
        let base_url = if let Some(db_url) = &args.database_url {
            db_url.clone()
        } else {
            std::env::var("DATABASE_URL").map_err(|e| {
                SinexError::general(format!(
                    "{}: {}",
                    "DATABASE_URL environment variable not set", e
                ))
            })?
        };
        let env = sinex_primitives::environment();
        let namespaced_url = env
            .database_url(&base_url)
            .unwrap_or_else(|_| base_url.clone());
        SqlxPgPool::connect(&namespaced_url)
            .await
            .map_err(|e| SinexError::general(format!("Failed to connect to database: {}", e)))
    }

    async fn connect_nats_transport(
        config: &sinex_primitives::nats::NatsConnectionConfig,
    ) -> NodeResult<EventTransport> {
        info!(url = %config.url, "Using NATS for event publishing");

        // Create NATS publisher
        let nats_publisher = sinex_node_sdk::NatsPublisher::new(
            config
                .connect()
                .await
                .map_err(|e| SinexError::general(format!("Failed to connect to NATS: {e}")))?,
        );

        Ok(EventTransport::Nats(std::sync::Arc::new(nats_publisher)))
    }

    fn extract_replay_config(
        config: &mut HashMap<String, serde_json::Value>,
    ) -> NodeResult<Option<ReplayConfig>> {
        if let Some(raw) = config.remove("replay") {
            if raw.is_null() {
                return Ok(None);
            }

            let cfg: ReplayConfig =
                serde_json::from_value(raw).map_err(|e| SinexError::serialization(e))?;

            if cfg.enabled {
                Ok(Some(cfg))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn execute_replay(runner: &mut NodeRunner<T>, config: ReplayConfig) -> NodeResult<()> {
        let runtime = runner
            .runtime_state()
            .ok_or_else(|| SinexError::lifecycle("Runtime state not initialized before replay"))?;

        let Some(mode) = Self::derive_replay_mode(&config)? else {
            return Ok(());
        };

        let mut service = runtime
            .replay_service(mode)
            .with_batch_size(config.replay_batch_size);

        let progress_logger = |progress: &ReplayProgress| {
            info!(
                phase = ?progress.phase,
                processed = progress.processed_events,
                total = progress.total_events,
                "Replay progress"
            );
        };

        let replay_result: ReplayResult = service
            .replay_into_emitter(runtime.event_emitter(), Some(progress_logger))
            .await
            .map_err(|err| SinexError::general(format!("Replay execution failed: {err}")))?;

        info!(
            processed = replay_result.total_processed,
            batches = replay_result.total_batches,
            "Replay completed"
        );

        for error in replay_result.errors {
            warn!(error = %error, "Replay reported error");
        }

        Ok(())
    }

    fn derive_replay_mode(config: &ReplayConfig) -> NodeResult<Option<ReplayMode>> {
        if !config.enabled {
            return Ok(None);
        }

        let start_time = Self::parse_timestamp(config.start_time.as_deref())?;
        let end_time = Self::parse_timestamp(config.end_time.as_deref())?;

        if !config.sources.is_empty() || !config.event_types.is_empty() {
            let filters = ReplayFilters {
                sources: if config.sources.is_empty() {
                    None
                } else {
                    Some(config.sources.clone())
                },
                event_types: if config.event_types.is_empty() {
                    None
                } else {
                    Some(config.event_types.clone())
                },
                hosts: None,
                start_time,
                end_time,
                limit: None,
                payload_filters: None,
            };

            Ok(Some(ReplayMode::Custom { filters }))
        } else {
            let start = start_time.unwrap_or(Timestamp::UNIX_EPOCH);
            Ok(Some(ReplayMode::TimeRange {
                start_time: start,
                end_time,
            }))
        }
    }

    fn parse_timestamp(value: Option<&str>) -> NodeResult<Option<Timestamp>> {
        if let Some(raw) = value {
            let parsed = OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
                .map_err(|e| {
                    SinexError::general(format!(
                        "Invalid RFC3339 timestamp in replay configuration: {e}"
                    ))
                })?;
            Ok(Some(Timestamp::from(parsed)))
        } else {
            Ok(None)
        }
    }
}

/// Helper macro for creating processor CLI main functions with unified architecture
#[macro_export]
macro_rules! processor_main {
    ($processor_type:ty) => {
        #[tokio::main]
        async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
            use clap::Parser;
            use sinex_node_sdk::heartbeat::HeartbeatEmitter;
            use $crate::cli::{NodeCli, NodeCliRunner, NodeCommand};

            let args = NodeCli::parse();
            let processor = <$processor_type as Default>::default();
            let mut runner = NodeCliRunner::new(processor);

            // Auto-spawn HeartbeatEmitter and Coordination for service mode
            if $crate::cli::command_requires_heartbeat(&args.command) {
                let service_name = args
                    .service_name
                    .clone()
                    .unwrap_or_else(|| "sinex-processor".to_string());

                let heartbeat_emitter = HeartbeatEmitter::new(
                    service_name.clone(),
                    sinex_primitives::Seconds::from_secs(30),
                );

                // Spawn heartbeat task concurrently
                tokio::spawn(async move {
                    heartbeat_emitter.start_periodic_heartbeat(None).await;
                });

                // NodeCoordination integrated below in service mode
            }

            runner.run(args).await.map_err(|e| e.into())
        }
    };

    ($processor_type:ty, $processor_expr:expr) => {
        #[tokio::main]
        async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
            use clap::Parser;
            use sinex_node_sdk::heartbeat::HeartbeatEmitter;
            use $crate::cli::{NodeCli, NodeCliRunner, NodeCommand};

            let args = NodeCli::parse();
            let processor = $processor_expr;
            let mut runner = NodeCliRunner::new(processor);

            runner.run(args).await.map_err(|e| e.into())
        }
    };
}
