#![doc = include_str!("../docs/cli_framework.md")]

//! Unified CLI structure for Sinex nodes
//!
//! This module provides the standardized CLI interface for all node binaries
//! implementing the service/scan/explore subcommand pattern.

use crate::event_node::EventTransport;
pub use crate::exploration::{
    CoverageAnalysis, ExplorationProvider, ExportFormat, MissingItem, SourceState,
};
use crate::runtime::stream::{Checkpoint, NodeRunner, NodeType, TimeHorizon};
use crate::{NodeResult, SinexError};
use clap::{Parser, Subcommand};
use sinex_primitives::SanitizedPath;
use sinex_primitives::temporal::Timestamp;

// Re-export common activity/history types used by exploration flows.
pub use crate::{ActivityEntry, IngestionHistoryEntry};
use sqlx::PgPool;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};

#[must_use]
pub fn command_requires_heartbeat(command: &NodeCommand) -> bool {
    matches!(
        command,
        NodeCommand::Service { .. } | NodeCommand::Scan { .. } | NodeCommand::Explore { .. }
    )
}

/// Standard CLI arguments for all nodes
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

    /// Node-specific configuration as JSON
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

    /// Require TLS for the connection (auto-detected from URL if not explicitly set)
    #[arg(long, env = "SINEX_NATS_REQUIRE_TLS")]
    pub require_tls: Option<bool>,

    /// Root CA certificate path (PEM)
    #[arg(long, env = "SINEX_NATS_CA_CERT")]
    pub ca_cert: Option<PathBuf>,

    /// Client certificate path (PEM)
    #[arg(long, env = "SINEX_NATS_CLIENT_CERT")]
    pub client_cert: Option<PathBuf>,

    /// Client key path (PEM)
    #[arg(long, env = "SINEX_NATS_CLIENT_KEY")]
    pub client_key: Option<PathBuf>,

    /// Credentials file path (JWT + seed).
    ///
    /// This is the preferred deployed auth mode when using `.creds` bundles.
    #[arg(long, env = "SINEX_NATS_CREDS_FILE")]
    pub creds_file: Option<PathBuf>,

    /// `NKey` seed file path.
    ///
    /// Use this only when the deployment expects direct `NKey` auth.
    #[arg(long, env = "SINEX_NATS_NKEY_SEED_FILE")]
    pub nkey_seed_file: Option<PathBuf>,

    /// Inline auth token for direct/manual runs.
    #[arg(long, env = "SINEX_NATS_TOKEN")]
    pub token: Option<String>,

    /// File containing the auth token.
    ///
    /// This is the preferred simple file-backed auth mode for deployed setups.
    #[arg(long, env = "SINEX_NATS_TOKEN_FILE")]
    pub token_file: Option<PathBuf>,
}

impl NatsArgs {
    fn to_config(&self) -> sinex_primitives::nats::NatsConnectionConfig {
        let mut config = sinex_primitives::nats::NatsConnectionConfig::from_env();

        config.url.clone_from(&self.url);
        config.name.clone_from(&self.name);

        // Auto-detect TLS from URL scheme if not explicitly set
        let url_requires_tls =
            self.url.starts_with("tls://") || self.url.starts_with("nats+tls://");
        let require_tls = self.require_tls.unwrap_or(url_requires_tls);

        // Warn if TLS is disabled in production environment
        if !require_tls {
            let env = sinex_primitives::environment();
            if env.is_prod() {
                warn!(
                    url = %self.url,
                    "TLS is disabled in production environment. This is a security risk!"
                );
            }
        }

        config.require_tls = require_tls;

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
        if let Some(path) = &self.nkey_seed_file {
            config.nkey_seed_file = Some(path.clone());
        }
        if let Some(token) = &self.token {
            config.token = Some(token.clone());
        }
        if let Some(path) = &self.token_file {
            config.token_file = Some(path.clone());
        }

        config
    }
}

/// Available subcommands for nodes
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

/// Maximum checkpoint JSON string size (1 MB). Prevents `DoS` via massive inputs.
const MAX_CHECKPOINT_JSON_BYTES: usize = 1_024 * 1_024;

/// Parse checkpoint as JSON
fn parse_checkpoint_json(checkpoint_str: &str) -> NodeResult<Checkpoint> {
    if checkpoint_str.len() > MAX_CHECKPOINT_JSON_BYTES {
        return Err(SinexError::validation(format!(
            "Checkpoint JSON exceeds maximum size ({} bytes > {} bytes)",
            checkpoint_str.len(),
            MAX_CHECKPOINT_JSON_BYTES
        )));
    }
    let val: serde_json::Value =
        serde_json::from_str(checkpoint_str).map_err(SinexError::serialization)?;
    serde_json::from_value(val).map_err(SinexError::serialization)
}

/// Parse checkpoint as timestamp
fn parse_checkpoint_timestamp(checkpoint_str: &str) -> NodeResult<Checkpoint> {
    Timestamp::parse_rfc3339(checkpoint_str)
        .map(|ts| Checkpoint::timestamp(ts, None))
        .map_err(|e| SinexError::unknown(format!("Invalid timestamp format: {e}")))
}

/// Parse checkpoint as stream ID
fn parse_checkpoint_stream(checkpoint_str: &str) -> Checkpoint {
    Checkpoint::stream(checkpoint_str, None)
}

/// Parse checkpoint from string representation
pub fn parse_checkpoint(checkpoint_str: &str) -> NodeResult<Checkpoint> {
    if ["none", "start"]
        .iter()
        .any(|token| checkpoint_str.eq_ignore_ascii_case(token))
    {
        Ok(Checkpoint::None)
    } else {
        parse_checkpoint_json(checkpoint_str)
            .or_else(|_| parse_checkpoint_timestamp(checkpoint_str))
            .or_else(|_| Ok(parse_checkpoint_stream(checkpoint_str)))
    }
}

fn parse_non_empty_path_arg(value: &str, label: &str) -> NodeResult<SanitizedPath> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(SinexError::configuration(format!(
            "{label} path cannot be empty"
        )));
    }
    SanitizedPath::from_str(trimmed).map_err(SinexError::configuration)
}

/// Validate and parse working directory argument
pub fn validate_work_dir(s: &str) -> Result<SanitizedPath, String> {
    parse_non_empty_path_arg(s, "Working directory").map_err(|e| e.to_string())
}

/// Validate and parse scan target path
pub fn validate_scan_target(s: &str) -> Result<SanitizedPath, String> {
    parse_non_empty_path_arg(s, "Scan target").map_err(|e| e.to_string())
}

/// Validate and parse export file path
pub fn validate_export_path(s: &str) -> Result<SanitizedPath, String> {
    parse_non_empty_path_arg(s, "Export").map_err(|e| e.to_string())
}

/// Parse time horizon from string representation
pub fn parse_time_horizon(horizon_str: &str) -> NodeResult<TimeHorizon> {
    if ["continuous", "stream", "sensor"]
        .iter()
        .any(|token| horizon_str.eq_ignore_ascii_case(token))
    {
        Ok(TimeHorizon::Continuous)
    } else if ["snapshot", "current", "now"]
        .iter()
        .any(|token| horizon_str.eq_ignore_ascii_case(token))
    {
        Ok(TimeHorizon::Snapshot)
    } else {
        // Try to parse as ISO timestamp for historical scan
        Timestamp::parse_rfc3339(horizon_str)
            .map(|ts| TimeHorizon::Historical {
                end_time: ts,
            })
            .map_err(|e| {
                SinexError::unknown(format!(
                    "Invalid time horizon '{horizon_str}': {e}. Use 'continuous', 'snapshot', or ISO timestamp"
                ))
            })
    }
}

/// Generic CLI runner for nodes
///
/// This provides a standardized way to run any Node with
/// the unified CLI interface supporting service/scan/explore subcommands.
pub struct NodeCliRunner<T: crate::runtime::stream::Node + ExplorationProvider + Default + 'static>
{
    node: Option<T>,
    node_factory: Arc<dyn Fn() -> T + Send + Sync>,
}

impl<T: crate::runtime::stream::Node + ExplorationProvider + Default + 'static> NodeCliRunner<T> {
    /// Create new CLI runner with a node instance
    pub fn new(node: T) -> Self {
        Self::new_with_factory(node, Arc::new(T::default))
    }

    /// Create a new CLI runner with an explicit factory for fresh worker instances.
    pub fn new_with_factory(node: T, node_factory: Arc<dyn Fn() -> T + Send + Sync>) -> Self {
        Self {
            node: Some(node),
            node_factory,
        }
    }

    /// Run the CLI with parsed arguments
    pub async fn run(&mut self, args: NodeCli) -> NodeResult<()> {
        // Initialize logging based on verbosity
        let log_level = match args.verbose {
            0 => "info",
            1 => "debug",
            _ => "trace",
        };

        if tracing_subscriber::fmt()
            .with_env_filter(format!("sinex={log_level}"))
            .try_init()
            .is_err()
        {
            tracing::debug!("Tracing already initialized, skipping reconfiguration");
        }

        // Parse node configuration
        let mut node_config: HashMap<String, serde_json::Value> =
            if let Some(config_str) = args.node_config.clone() {
                serde_json::from_str(&config_str).map_err(|e| {
                    SinexError::unknown(format!("Failed to parse node configuration JSON: {e}"))
                })?
            } else {
                HashMap::new()
            };

        if let NodeCommand::Service {
            consumer_group: Some(group),
            ..
        } = &args.command
        {
            node_config
                .entry("consumer_group".to_string())
                .or_insert_with(|| serde_json::json!(group));
        }

        // Take ownership of the node
        let node = self
            .node
            .take()
            .ok_or_else(|| SinexError::unknown("Node already consumed"))?;

        match args.command {
            NodeCommand::Service { dry_run, .. } => {
                self.handle_service_command(node, node_config, args, dry_run)
                    .await
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
                self.handle_scan_command(
                    node,
                    node_config,
                    from,
                    until,
                    targets,
                    dry_run,
                    interactive,
                    max_events,
                    no_skip_duplicates,
                    estimate,
                    &args,
                )
                .await
            }
            NodeCommand::Explore {
                source_state,
                ingestion_history,
                coverage_analysis,
                limit,
                ref export_to,
            } => {
                self.handle_explore_command(
                    node,
                    source_state,
                    ingestion_history,
                    coverage_analysis,
                    limit,
                    export_to.as_ref(),
                )
                .await
            }
        }
    }

    async fn handle_service_command(
        &self,
        node: T,
        node_config: HashMap<String, serde_json::Value>,
        args: NodeCli,
        dry_run: bool,
    ) -> NodeResult<()> {
        info!("Starting node service mode");

        // Create node runner
        let mut runner = NodeRunner::new_with_factory(node, self.node_factory.clone());

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

        let coordination_disabled = std::env::var("SINEX_COORDINATION_DISABLED")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
        let node_type = runner.node_type();

        // Run service with optional coordination
        if dry_run || coordination_disabled {
            runner.run_service().await?;
        } else if matches!(node_type, NodeType::Automaton) {
            // Automata already execute leader/standby acquisition in NodeRunner.
            // Avoid stacking a second coordination loop around the same runtime.
            info!(
                "Automaton uses internal leader/standby coordination; skipping outer coordination wrapper"
            );
            runner.run_service().await?;
        } else {
            use crate::coordination::NodeCoordination;

            use std::sync::Arc;
            use tokio::sync::Mutex;
            use uuid::Uuid;

            let runtime_snapshot = runner
                .runtime_state()
                .ok_or_else(|| SinexError::unknown("Runtime state unavailable for coordination"))?;

            // Create coordination with generated instance ID
            let instance_id = Uuid::new_v4().to_string();

            let coordination = NodeCoordination::from_runtime(&runtime_snapshot, instance_id);

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
                            sinex_primitives::SinexError::service(format!("Node error: {e}"))
                        })
                    }
                })
                .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    async fn handle_scan_command(
        &self,
        node: T,
        node_config: HashMap<String, serde_json::Value>,
        from: &str,
        until: &str,
        targets: &[SanitizedPath],
        dry_run: bool,
        interactive: bool,
        max_events: u64,
        no_skip_duplicates: bool,
        estimate: bool,
        args: &NodeCli,
    ) -> NodeResult<()> {
        use crate::runtime::stream::ScanArgs;

        info!("Running scan operation");

        let checkpoint = parse_checkpoint(from)
            .map_err(|e| SinexError::unknown(format!("Failed to parse checkpoint: {e}")))?;
        let time_horizon = parse_time_horizon(until)
            .map_err(|e| SinexError::unknown(format!("Failed to parse time horizon: {e}")))?;

        // Create node runner
        let mut runner = NodeRunner::new(node);

        // Set up minimal dependencies for scan mode
        let service_name = args
            .service_name
            .as_deref()
            .unwrap_or("sinex-node")
            .to_string();
        let work_dir = Self::resolve_work_dir(args);

        // For scan mode, database connection is optional for dry runs
        let db_pool = if dry_run {
            None
        } else {
            Some(Self::connect_primary_db(args).await?)
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
            targets: targets.iter().map(ToString::to_string).collect(),
            dry_run,
            interactive,
            max_events,
            skip_duplicates: !no_skip_duplicates,
            config: HashMap::new(),
            replay: None,
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
                    println!("    - {warning}");
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
                    .format(time::macros::format_description!(
                        "[year]-[month]-[day] [hour]:[minute]:[second]"
                    ))
                    .unwrap_or_default(),
                end.format(time::macros::format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                ))
                .unwrap_or_default()
            );
        }

        if !report.node_stats.is_empty() {
            println!("  Node stats:");
            for (key, value) in &report.node_stats {
                println!("    {key}: {value}");
            }
        }

        if !report.successful_targets.is_empty() {
            println!("  Successful targets: {}", report.successful_targets.len());
            for target in &report.successful_targets {
                println!("    - {target}");
            }
        }

        if !report.failed_targets.is_empty() {
            println!("  Failed targets:");
            for (target, error) in &report.failed_targets {
                println!("    - {target}: {error}");
            }
        }

        if !report.warnings.is_empty() {
            println!("  Warnings:");
            for warning in &report.warnings {
                println!("    - {warning}");
            }
        }
        Ok(())
    }

    async fn handle_explore_command(
        &self,
        node: T,
        source_state: bool,
        ingestion_history: bool,
        coverage_analysis: bool,
        limit: u64,
        export_to: Option<&SanitizedPath>,
    ) -> NodeResult<()> {
        info!("Running exploration mode");

        // For exploration, we can work with the node directly
        if source_state {
            match node.get_source_state() {
                Ok(state) => {
                    println!("Source State:");
                    println!("  Description: {}", state.description);
                    println!(
                        "  Last updated: {}",
                        state
                            .last_updated
                            .format(time::macros::format_description!(
                                "[year]-[month]-[day] [hour]:[minute]:[second]"
                            ))
                            .unwrap_or_default()
                    );
                    if let Some(total) = state.total_items {
                        println!("  Total items: {total}");
                    }
                    println!("  Healthy: {}", state.healthy);

                    if !state.recent_activity.is_empty() {
                        println!("  Recent activity:");
                        for activity in &state.recent_activity {
                            println!(
                                "    - {}: {}",
                                activity
                                    .timestamp
                                    .format(time::macros::format_description!(
                                        "[hour]:[minute]:[second]"
                                    ))
                                    .unwrap_or_default(),
                                activity.description
                            );
                        }
                    }

                    if !state.metadata.is_empty() {
                        println!("  Metadata:");
                        for (key, value) in &state.metadata {
                            println!("    {key}: {value}");
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
                                .format(time::macros::format_description!(
                                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                                ))
                                .unwrap_or_default()
                        );
                        if let Some(completed) = entry.completed_at {
                            println!(
                                "    Completed: {}",
                                completed
                                    .format(time::macros::format_description!(
                                        "[year]-[month]-[day] [hour]:[minute]:[second]"
                                    ))
                                    .unwrap_or_default()
                            );
                        }
                        println!("    Events: {}", entry.events_generated);
                        if let Some(error) = &entry.error {
                            println!("    Error: {error}");
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
                            .format(time::macros::format_description!(
                                "[year]-[month]-[day] [hour]:[minute]:[second]"
                            ))
                            .unwrap_or_default(),
                        analysis
                            .time_range
                            .1
                            .format(time::macros::format_description!(
                                "[year]-[month]-[day] [hour]:[minute]:[second]"
                            ))
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
                            println!("    - {rec}");
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

            match node.export_data(export_path, format) {
                Ok(()) => {
                    println!("Data exported to: {}", export_path.as_str());
                }
                Err(e) => {
                    warn!(error = %e, "Failed to export data");
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

    #[allow(clippy::panic)] // Internal invariant: environment-generated paths must validate
    fn resolve_work_dir(args: &NodeCli) -> SanitizedPath {
        args.work_dir.clone().unwrap_or_else(|| {
            let env = sinex_primitives::environment();
            let namespaced = env.work_directory("/tmp/sinex/node");
            let namespaced_str = namespaced.to_string_lossy();

            // Environment-generated paths should always be valid. If validation fails,
            // this indicates a bug in environment namespacing logic, not user input.
            SanitizedPath::from_str(namespaced_str.as_ref()).unwrap_or_else(|err| {
                panic!(
                    "Environment-generated work directory '{namespaced_str}' failed validation: {err}. \
                     This is a bug in environment namespacing logic."
                )
            })
        })
    }

    async fn connect_primary_db(args: &NodeCli) -> NodeResult<PgPool> {
        let base_url = if let Some(db_url) = &args.database_url {
            db_url.clone()
        } else {
            std::env::var("DATABASE_URL").map_err(|e| {
                SinexError::unknown(format!("DATABASE_URL environment variable not set: {e}"))
            })?
        };
        let env = sinex_primitives::environment();
        let namespaced_url = env
            .database_url(&base_url)
            .unwrap_or_else(|_| base_url.clone());
        PgPool::connect(&namespaced_url)
            .await
            .map_err(|e| SinexError::unknown(format!("Failed to connect to database: {e}")))
    }

    async fn connect_nats_transport(
        config: &sinex_primitives::nats::NatsConnectionConfig,
    ) -> NodeResult<EventTransport> {
        info!(url = %config.url, "Using NATS for event publishing");

        // Create NATS publisher
        let nats_publisher = crate::NatsPublisher::new(
            config
                .connect()
                .await
                .map_err(|e| SinexError::unknown(format!("Failed to connect to NATS: {e}")))?,
        );

        Ok(EventTransport::Nats(std::sync::Arc::new(nats_publisher)))
    }
}

/// Helper macro for creating node CLI main functions with unified architecture
#[macro_export]
macro_rules! node_entrypoint {
    ($node_type:ty) => {
        #[tokio::main]
        async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
            human_panic::setup_panic!();

            use clap::Parser;
            use $crate::heartbeat::HeartbeatEmitter;
            use $crate::node_cli::{NodeCli, NodeCliRunner};

            let args = NodeCli::parse();
            let node = <$node_type as Default>::default();
            let mut runner = NodeCliRunner::new(node);

            // Auto-spawn HeartbeatEmitter and Coordination for service mode
            if $crate::node_cli::command_requires_heartbeat(&args.command) {
                let service_name = args
                    .service_name
                    .clone()
                    .unwrap_or_else(|| "sinex-node".to_string());

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

    ($node_type:ty, $node_expr:expr) => {
        #[tokio::main]
        async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
            human_panic::setup_panic!();

            use clap::Parser;
            use $crate::heartbeat::HeartbeatEmitter;
            use $crate::node_cli::{NodeCli, NodeCliRunner};

            let args = NodeCli::parse();
            let node = $node_expr;
            let mut runner = NodeCliRunner::new(node);

            // Keep behavior consistent with the 1-arg macro arm.
            if $crate::node_cli::command_requires_heartbeat(&args.command) {
                let service_name = args
                    .service_name
                    .clone()
                    .unwrap_or_else(|| "sinex-node".to_string());

                let heartbeat_emitter = HeartbeatEmitter::new(
                    service_name.clone(),
                    sinex_primitives::Seconds::from_secs(30),
                );

                tokio::spawn(async move {
                    heartbeat_emitter.start_periodic_heartbeat(None).await;
                });
            }

            runner.run(args).await.map_err(|e| e.into())
        }
    };
}
