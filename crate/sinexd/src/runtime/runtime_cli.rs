//! Unified CLI structure for Sinex nodes
//!
//! This module provides the standardized CLI interface for all node binaries
//! implementing the service/scan/explore subcommand pattern.

use crate::runtime::event_transport::EventTransport;
pub use crate::runtime::exploration::{ExplorationProvider, ExportFormat, SourceState};
use crate::runtime::stream::{Checkpoint, RuntimeRunner, ModuleKind, TimeHorizon};
use crate::runtime::{RuntimeResult, SinexError};
use clap::{Parser, Subcommand};
use sinex_primitives::SanitizedPath;
use sinex_primitives::domain::ServiceName;
use sinex_primitives::env as shared_env;
use sinex_primitives::temporal::Timestamp;

// Re-export common activity/history types used by exploration flows.
pub use crate::runtime::{ActivityEntry, IngestionHistoryEntry};
use sqlx::PgPool;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};

/// Standard CLI arguments for all nodes
#[derive(Parser, Debug, Clone)]
#[command(name = "sinex-runtime", about = "Sinex Stream RuntimeActor", version)]
pub struct RuntimeCli {
    /// NATS connection configuration
    #[command(flatten)]
    pub nats: NatsArgs,

    /// Database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Service name for identification
    #[arg(long)]
    pub service_name: Option<String>,

    /// Semantic source identity hosted by this runner pack
    #[arg(long, env = "SINEX_SOURCE", value_parser = validate_identity_token)]
    pub source: Option<String>,

    /// Runner-pack identity for binaries that host multiple source contracts
    #[arg(long, env = "SINEX_RUNNER_PACK", value_parser = validate_identity_token)]
    pub runner_pack: Option<String>,

    /// Working directory for temporary files
    #[arg(long, value_parser = validate_work_dir)]
    pub work_dir: Option<SanitizedPath>,

    /// NATS namespace for subject/stream isolation.
    #[arg(long, env = "SINEX_NAMESPACE")]
    pub namespace: Option<String>,

    /// Enable verbose logging
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// RuntimeActor-specific configuration as JSON
    #[arg(long)]
    pub runtime_config: Option<String>,

    #[command(subcommand)]
    pub command: RuntimeCommand,
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
pub enum RuntimeCommand {
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
fn parse_checkpoint_json(checkpoint_str: &str) -> RuntimeResult<Checkpoint> {
    if checkpoint_str.len() > MAX_CHECKPOINT_JSON_BYTES {
        return Err(SinexError::validation(format!(
            "Checkpoint JSON exceeds maximum size ({} bytes > {} bytes)",
            checkpoint_str.len(),
            MAX_CHECKPOINT_JSON_BYTES
        )));
    }
    let val: serde_json::Value = serde_json::from_str(checkpoint_str).map_err(|error| {
        SinexError::serialization("Failed to parse checkpoint JSON").with_std_error(&error)
    })?;
    serde_json::from_value(val).map_err(|error| {
        SinexError::serialization("Failed to decode checkpoint JSON").with_std_error(&error)
    })
}

/// Parse checkpoint as timestamp
fn parse_checkpoint_timestamp(checkpoint_str: &str) -> RuntimeResult<Checkpoint> {
    Timestamp::parse_rfc3339(checkpoint_str)
        .map(|ts| Checkpoint::timestamp(ts, None))
        .map_err(|e| SinexError::unknown(format!("Invalid timestamp format: {e}")))
}

/// Parse checkpoint as stream ID
fn parse_checkpoint_stream(checkpoint_str: &str) -> Checkpoint {
    Checkpoint::stream(checkpoint_str, None)
}

fn checkpoint_looks_like_json(checkpoint_str: &str) -> bool {
    matches!(checkpoint_str.chars().next(), Some('{' | '[' | '"'))
}

fn checkpoint_looks_like_rfc3339(checkpoint_str: &str) -> bool {
    let bytes = checkpoint_str.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_digit)
        && checkpoint_str.contains('T')
        && checkpoint_str.contains(':')
}

/// Parse checkpoint from string representation
pub fn parse_checkpoint(checkpoint_str: &str) -> RuntimeResult<Checkpoint> {
    let checkpoint_str = checkpoint_str.trim();
    if ["none", "start"]
        .iter()
        .any(|token| checkpoint_str.eq_ignore_ascii_case(token))
    {
        Ok(Checkpoint::None)
    } else if checkpoint_looks_like_json(checkpoint_str) {
        parse_checkpoint_json(checkpoint_str)
    } else if checkpoint_looks_like_rfc3339(checkpoint_str) {
        parse_checkpoint_timestamp(checkpoint_str)
    } else {
        parse_checkpoint_json(checkpoint_str)
            .or_else(|_| parse_checkpoint_timestamp(checkpoint_str))
            .map_or_else(|_| Ok(parse_checkpoint_stream(checkpoint_str)), Ok)
    }
}

fn parse_non_empty_path_arg(value: &str, label: &str) -> RuntimeResult<SanitizedPath> {
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

fn resolve_primary_database_url(args: &RuntimeCli) -> RuntimeResult<String> {
    let base_url = if let Some(db_url) = &args.database_url {
        db_url.clone()
    } else {
        std::env::var("DATABASE_URL").map_err(|e| {
            SinexError::unknown(format!("DATABASE_URL environment variable not set: {e}"))
        })?
    };
    sinex_db::resolve_effective_database_url(&base_url).map_err(|error| {
        SinexError::configuration("Failed to validate node DATABASE_URL").with_std_error(&error)
    })
}

/// Validate and parse scan target path
pub fn validate_scan_target(s: &str) -> Result<SanitizedPath, String> {
    parse_non_empty_path_arg(s, "Scan target").map_err(|e| e.to_string())
}

/// Validate a source or runner-pack identifier.
pub fn validate_identity_token(s: &str) -> Result<String, String> {
    let value = s.trim();
    if value.is_empty() {
        return Err("identity token cannot be empty".to_string());
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_lowercase()
            || byte.is_ascii_uppercase()
            || byte.is_ascii_digit()
            || matches!(byte, b'.' | b'-' | b'_')
    }) {
        return Err(
            "identity token may contain only ASCII letters, digits, '.', '-', and '_'".to_string(),
        );
    }
    Ok(value.to_string())
}

/// Validate and parse export file path
pub fn validate_export_path(s: &str) -> Result<SanitizedPath, String> {
    parse_non_empty_path_arg(s, "Export").map_err(|e| e.to_string())
}

/// Parse time horizon from string representation
pub fn parse_time_horizon(horizon_str: &str) -> RuntimeResult<TimeHorizon> {
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
/// This provides a standardized way to run any RuntimeActor with
/// the unified CLI interface supporting service/scan/explore subcommands.
pub struct RuntimeCliRunner<
    T: crate::runtime::stream::RuntimeActor + ExplorationProvider + Default + 'static,
> {
    node: Option<T>,
    source_factory: Arc<dyn Fn() -> T + Send + Sync>,
}

fn unavailable_section(label: &str, error: &str) -> String {
    format!("{label}:\n  Unavailable: {error}")
}

fn print_unavailable_section(label: &str, error: &crate::runtime::SinexError) {
    println!("{}", unavailable_section(label, &error.to_string()));
}

fn handle_export_result(path: &SanitizedPath, result: RuntimeResult<()>) -> RuntimeResult<()> {
    match result {
        Ok(()) => {
            println!("Data exported to: {}", path.as_str());
            Ok(())
        }
        Err(error) => {
            print_unavailable_section("Data Export", &error);
            Err(
                SinexError::processing("failed to export node exploration data")
                    .with_context("path", path.as_str())
                    .with_source(error),
            )
        }
    }
}

fn render_cli_value<E: Display>(result: Result<String, E>) -> String {
    result.unwrap_or_else(|error| format!("<format error: {error}>"))
}

fn render_cli_timestamp(timestamp: Timestamp) -> String {
    render_cli_value(timestamp.format(time::macros::format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second]"
    )))
}

fn render_optional_cli_timestamp(timestamp: Option<Timestamp>) -> String {
    timestamp.map_or_else(|| "unknown".to_string(), render_cli_timestamp)
}

fn render_cli_time(timestamp: Timestamp) -> String {
    render_cli_value(timestamp.format(time::macros::format_description!(
        "[hour]:[minute]:[second]"
    )))
}

fn edge_mode_enabled(database_url_supplied: bool) -> bool {
    shared_env::bool_or("SINEX_EDGE_MODE", false, "node cli edge mode") && !database_url_supplied
}

fn default_service_name(args: &RuntimeCli) -> ServiceName {
    let name = args
        .service_name
        .clone()
        .or_else(|| {
            args.source
                .as_ref()
                .map(|unit| format!("sinex-{unit}"))
        })
        .unwrap_or_else(|| "sinex-runtime".to_string());
    ServiceName::new(name)
}

impl<T: crate::runtime::stream::RuntimeActor + ExplorationProvider + Default + 'static>
    RuntimeCliRunner<T>
{
    /// Create new CLI runner with a node instance
    pub fn new(node: T) -> Self {
        Self::new_with_factory(node, Arc::new(T::default))
    }

    /// Create a new CLI runner with an explicit factory for fresh worker instances.
    pub fn new_with_factory(node: T, source_factory: Arc<dyn Fn() -> T + Send + Sync>) -> Self {
        Self {
            node: Some(node),
            source_factory,
        }
    }

    /// Run the CLI with parsed arguments
    pub async fn run(&mut self, args: RuntimeCli) -> RuntimeResult<()> {
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

        // Parse runtime configuration
        let mut runtime_config: HashMap<String, serde_json::Value> =
            if let Some(config_str) = args.runtime_config.clone() {
                serde_json::from_str(&config_str).map_err(|e| {
                    SinexError::unknown(format!("Failed to parse runtime configuration JSON: {e}"))
                })?
            } else {
                HashMap::new()
            };

        if let RuntimeCommand::Service {
            consumer_group: Some(group),
            ..
        } = &args.command
        {
            runtime_config
                .entry("consumer_group".to_string())
                .or_insert_with(|| serde_json::json!(group));
        }

        Self::insert_identity_arg(
            &mut runtime_config,
            "source_id",
            args.source.as_deref(),
        )?;
        Self::insert_identity_arg(&mut runtime_config, "runner_pack", args.runner_pack.as_deref())?;

        // Take ownership of the node
        let node = self
            .node
            .take()
            .ok_or_else(|| SinexError::unknown("RuntimeActor already consumed"))?;

        match args.command {
            RuntimeCommand::Service { dry_run, .. } => {
                self.handle_service_command(node, runtime_config, args, dry_run)
                    .await
            }
            RuntimeCommand::Scan {
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
                    runtime_config,
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
            RuntimeCommand::Explore {
                source_state,
                ingestion_history,
                limit,
                ref export_to,
            } => self.handle_explore_command(
                node,
                source_state,
                ingestion_history,
                limit,
                export_to.as_ref(),
            ),
        }
    }

    async fn handle_service_command(
        &self,
        node: T,
        runtime_config: HashMap<String, serde_json::Value>,
        args: RuntimeCli,
        dry_run: bool,
    ) -> RuntimeResult<()> {
        info!("Starting node service mode");

        // Create node runner
        let mut runner = RuntimeRunner::new_with_factory(node, self.source_factory.clone());

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
                    if edge_mode_enabled(args.database_url.is_some()) {
                        warn!("Running in Edge Mode without database connection");
                        None
                    } else {
                        return Err(err);
                    }
                }
            }
        };
        let transport =
            Self::connect_nats_transport(&args.nats.to_config(), args.namespace.clone()).await?;

        // Initialize runner with transport
        runner
            .initialize_with_transport(
                service_name.clone(),
                runtime_config.clone(),
                db_pool.clone(),
                transport,
                std::path::PathBuf::from(work_dir.as_str()),
                dry_run,
            )
            .await?;

        let coordination_disabled =
            shared_env::bool_or("SINEX_COORDINATION_DISABLED", false, "node coordination");
        let module_kind = runner.module_kind();

        // Run service with optional coordination
        if dry_run || coordination_disabled {
            runner.run_service().await?;
        } else if matches!(module_kind, ModuleKind::Automaton) {
            // Automata already execute leader/standby acquisition in RuntimeRunner.
            // Avoid stacking a second coordination loop around the same runtime.
            info!(
                "Automaton uses internal leader/standby coordination; skipping outer coordination wrapper"
            );
            runner.run_service().await?;
        } else {
            use crate::runtime::coordination::RuntimeCoordination;

            use std::sync::Arc;
            use tokio::sync::Mutex;
            use uuid::Uuid;

            let runtime_snapshot = runner
                .runtime_state()
                .ok_or_else(|| SinexError::unknown("Runtime state unavailable for coordination"))?;

            // Create coordination with generated instance ID
            let instance_id = Uuid::new_v4().to_string();

            let mut coordination = RuntimeCoordination::from_runtime(&runtime_snapshot, instance_id)?;

            // Wrap runner in Arc<Mutex<>> for sharing
            let runner = Arc::new(Mutex::new(runner));

            // Run with coordination (hot standby pattern)
            coordination
                .run_coordination_loop(move || {
                    let runner = runner.clone();
                    async move {
                        // Only leader processes events
                        let mut runner = runner.lock().await;
                        runner.run_service().await.map_err(|e| {
                            sinex_primitives::SinexError::service(format!("RuntimeActor error: {e}"))
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
        runtime_config: HashMap<String, serde_json::Value>,
        from: &str,
        until: &str,
        targets: &[SanitizedPath],
        dry_run: bool,
        interactive: bool,
        max_events: u64,
        no_skip_duplicates: bool,
        estimate: bool,
        args: &RuntimeCli,
    ) -> RuntimeResult<()> {
        use crate::runtime::stream::ScanArgs;

        info!("Running scan operation");

        let checkpoint = parse_checkpoint(from)
            .map_err(|e| SinexError::unknown(format!("Failed to parse checkpoint: {e}")))?;
        let time_horizon = parse_time_horizon(until)
            .map_err(|e| SinexError::unknown(format!("Failed to parse time horizon: {e}")))?;

        // Create node runner
        let mut runner = RuntimeRunner::new(node);

        // Set up minimal dependencies for scan mode
        let service_name = args
            .service_name
            .as_deref()
            .unwrap_or("sinex-runtime")
            .to_string();
        let work_dir = Self::resolve_work_dir(args);

        // For scan mode, database connection is optional for dry runs
        let db_pool = if dry_run {
            None
        } else {
            Some(Self::connect_primary_db(args).await?)
        };

        let transport =
            Self::connect_nats_transport(&args.nats.to_config(), args.namespace.clone()).await?;

        // Initialize runner with transport
        runner
            .initialize_with_transport(
                service_name,
                runtime_config,
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

        let workflow_result: RuntimeResult<Option<crate::runtime::stream::ScanReport>> =
            async {
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
                            return Ok(None);
                        }
                    }
                }

                let report = runner.run_scan(checkpoint, time_horizon, scan_args).await?;
                Ok(Some(report))
            }
            .await;

        let shutdown_result = runner.shutdown().await;
        let maybe_report = match (workflow_result, shutdown_result) {
            (Ok(report), Ok(())) => report,
            (Err(scan_error), Ok(())) => return Err(scan_error),
            (Ok(_), Err(shutdown_error)) => return Err(shutdown_error),
            (Err(scan_error), Err(shutdown_error)) => {
                return Err(SinexError::lifecycle(
                    "scan command failed and runner shutdown also failed".to_string(),
                )
                .with_context("scan_error", scan_error.to_string())
                .with_source(shutdown_error));
            }
        };

        let Some(report) = maybe_report else {
            return Ok(());
        };

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
                render_cli_timestamp(start),
                render_cli_timestamp(end)
            );
        }

        if !report.runtime_stats.is_empty() {
            println!("  RuntimeActor stats:");
            for (key, value) in &report.runtime_stats {
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

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Generic parameter moved into method calls"
    )]
    fn handle_explore_command(
        &self,
        node: T,
        source_state: bool,
        ingestion_history: bool,
        limit: u64,
        export_to: Option<&SanitizedPath>,
    ) -> RuntimeResult<()> {
        info!("Running exploration mode");

        // For exploration, we can work with the node directly
        if source_state {
            match node.get_source_state() {
                Ok(state) => {
                    println!("Source State:");
                    println!("  Description: {}", state.description);
                    println!(
                        "  Last updated: {}",
                        render_optional_cli_timestamp(state.last_updated)
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
                                render_cli_time(activity.timestamp),
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
                    print_unavailable_section("Source State", &e);
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
                        println!("    Started: {}", render_cli_timestamp(entry.started_at));
                        if let Some(completed) = entry.completed_at {
                            println!("    Completed: {}", render_cli_timestamp(completed));
                        }
                        println!("    Events: {}", entry.events_generated);
                        if let Some(error) = &entry.error {
                            println!("    Error: {error}");
                        }
                    }
                }
                Err(e) => {
                    print_unavailable_section("Ingestion History", &e);
                    warn!(error = %e, "Failed to get ingestion history");
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
            handle_export_result(export_path, node.export_data(export_path, format))?;
        }
        Ok(())
    }

    fn resolve_service_name(args: &RuntimeCli) -> ServiceName {
        default_service_name(args)
    }

    fn insert_identity_arg(
        runtime_config: &mut HashMap<String, serde_json::Value>,
        key: &str,
        value: Option<&str>,
    ) -> RuntimeResult<()> {
        let Some(value) = value else {
            return Ok(());
        };
        match runtime_config.get(key).and_then(serde_json::Value::as_str) {
            Some(existing) if existing != value => Err(SinexError::configuration(format!(
                "`--{}` conflicts with runtime_config.{key}",
                key.replace('_', "-")
            ))
            .with_context("cli_value", value.to_string())
            .with_context("config_value", existing.to_string())),
            Some(_) => Ok(()),
            None => {
                runtime_config.insert(key.to_string(), serde_json::json!(value));
                Ok(())
            }
        }
    }

    #[allow(clippy::panic)] // Internal invariant: environment-generated paths must validate
    fn resolve_work_dir(args: &RuntimeCli) -> SanitizedPath {
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

    async fn connect_primary_db(args: &RuntimeCli) -> RuntimeResult<PgPool> {
        let database_url = resolve_primary_database_url(args)?;
        PgPool::connect(&database_url)
            .await
            .map_err(|e| SinexError::unknown(format!("Failed to connect to database: {e}")))
    }

    async fn connect_nats_transport(
        config: &sinex_primitives::nats::NatsConnectionConfig,
        namespace: Option<String>,
    ) -> RuntimeResult<EventTransport> {
        info!(url = %config.url, namespace = ?namespace, "Using NATS for event publishing");

        // Create NATS publisher
        let nats_publisher = crate::runtime::NatsPublisher::with_namespace(
            config
                .connect()
                .await
                .map_err(|e| SinexError::unknown(format!("Failed to connect to NATS: {e}")))?,
            namespace,
        );

        Ok(EventTransport::Nats(std::sync::Arc::new(nats_publisher)))
    }
}

#[cfg(test)]
#[allow(
    clippy::items_after_test_module,
    reason = "Additional pub use items sit below the tests and cannot be reordered without breaking downstream re-exports"
)]
mod tests {
    use super::{
        NatsArgs, RuntimeCli, RuntimeCommand, default_service_name, edge_mode_enabled,
        handle_export_result, parse_checkpoint, render_cli_value, render_optional_cli_timestamp,
        resolve_primary_database_url, validate_identity_token,
    };
    use crate::runtime::SinexError;
    use crate::runtime::stream::Checkpoint;
    use sinex_primitives::SanitizedPath;
    use std::str::FromStr;
    use xtask::sandbox::sinex_serial_test;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn export_result_surfaces_failure_with_path_context() -> TestResult<()> {
        let path =
            SanitizedPath::from_str("/tmp/export.json").expect("test export path should validate");
        let error = handle_export_result(&path, Err(SinexError::io("disk full while exporting")))
            .expect_err("export failures should not be swallowed");

        let message = format!("{error:#}");
        assert!(message.contains("failed to export node exploration data"));
        assert!(message.contains("/tmp/export.json"));
        assert!(message.contains("disk full while exporting"));
        Ok(())
    }

    #[sinex_test]
    async fn render_cli_value_is_explicit_on_format_failure() -> TestResult<()> {
        let rendered = render_cli_value::<&str>(Err("bad timestamp field"));

        assert_eq!(rendered, "<format error: bad timestamp field>");
        Ok(())
    }

    #[sinex_test]
    async fn render_optional_cli_timestamp_is_explicit_when_unknown() -> TestResult<()> {
        assert_eq!(render_optional_cli_timestamp(None), "unknown");
        Ok(())
    }

    fn test_cli_with_database_url(database_url: Option<&str>) -> RuntimeCli {
        RuntimeCli {
            nats: NatsArgs {
                url: "nats://localhost:4222".to_string(),
                name: None,
                require_tls: None,
                ca_cert: None,
                client_cert: None,
                client_key: None,
                creds_file: None,
                nkey_seed_file: None,
                token: None,
                token_file: None,
            },
            database_url: database_url.map(ToOwned::to_owned),
            service_name: None,
            source: None,
            runner_pack: None,
            work_dir: None,
            namespace: None,
            verbose: 0,
            runtime_config: None,
            command: RuntimeCommand::Service {
                dry_run: true,
                consumer_group: None,
            },
        }
    }

    #[sinex_test]
    async fn parse_checkpoint_rejects_malformed_json_input() -> TestResult<()> {
        let error = parse_checkpoint("{ definitely-not-json")
            .expect_err("JSON-like checkpoint input must not silently fall back to a stream id");

        assert!(format!("{error:#}").contains("Failed to parse checkpoint JSON"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_checkpoint_rejects_invalid_timestamp_like_input() -> TestResult<()> {
        let error = parse_checkpoint("2026-03-28T25:61:61Z").expect_err(
            "timestamp-like checkpoint input must not silently fall back to a stream id",
        );

        assert!(format!("{error:#}").contains("Invalid timestamp format"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_checkpoint_accepts_stream_ids_after_structured_parsers_fail() -> TestResult<()> {
        let checkpoint = parse_checkpoint("1234567890-0")?;
        match checkpoint {
            Checkpoint::Stream { message_id, .. } => {
                assert_eq!(message_id, "1234567890-0");
            }
            other => {
                return Err(SinexError::validation(format!(
                    "expected stream checkpoint, got {}",
                    other.description()
                ))
                .into());
            }
        }
        Ok(())
    }

    #[sinex_test]
    async fn validate_identity_token_accepts_source_spelling() -> TestResult<()> {
        assert_eq!(
            validate_identity_token("terminal.atuin-history").expect("valid source"),
            "terminal.atuin-history"
        );
        Ok(())
    }

    #[sinex_test]
    async fn validate_identity_token_rejects_shell_syntax() -> TestResult<()> {
        let error = validate_identity_token("terminal;rm -rf")
            .expect_err("identity tokens must not accept shell syntax");
        assert!(error.contains("ASCII letters"));
        Ok(())
    }

    #[sinex_test]
    async fn source_supplies_default_service_name() -> TestResult<()> {
        let mut cli = test_cli_with_database_url(None);
        cli.source = Some("terminal.atuin-history".to_string());

        assert_eq!(
            default_service_name(&cli).as_str(),
            "sinex-terminal.atuin-history"
        );
        Ok(())
    }

    #[sinex_test]
    async fn resolve_primary_database_url_rejects_invalid_namespaced_url() -> TestResult<()> {
        let cli = test_cli_with_database_url(Some("not-a-valid-postgres-url"));
        let error = resolve_primary_database_url(&cli)
            .expect_err("invalid database URLs must not silently bypass namespacing");

        let rendered = format!("{error:#}");
        assert!(rendered.contains("Failed to validate node DATABASE_URL"));
        Ok(())
    }

    #[sinex_serial_test]
    async fn edge_mode_requires_truthy_boolean_override() -> xtask::sandbox::TestResult<()> {
        unsafe { std::env::set_var("SINEX_EDGE_MODE", "enabled") };

        assert!(
            !edge_mode_enabled(false),
            "invalid edge-mode override must not silently enable DB-less execution"
        );

        unsafe { std::env::remove_var("SINEX_EDGE_MODE") };
        Ok(())
    }
}

/// Helper macro for creating node CLI main functions with unified architecture
#[macro_export]
macro_rules! node_entrypoint {
    ($module_kind:ty) => {
        #[tokio::main]
        async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
            use clap::Parser;
            use $crate::runtime::runtime_cli::{RuntimeCli, RuntimeCliRunner};

            let args = RuntimeCli::parse();
            let node = <$module_kind as Default>::default();
            let mut runner = RuntimeCliRunner::new(node);

            runner.run(args).await.map_err(|e| e.into())
        }
    };

    ($module_kind:ty, $node_expr:expr) => {
        #[tokio::main]
        async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
            use clap::Parser;
            use $crate::runtime::runtime_cli::{RuntimeCli, RuntimeCliRunner};

            let args = RuntimeCli::parse();
            let node = $node_expr;
            let mut runner = RuntimeCliRunner::new(node);

            runner.run(args).await.map_err(|e| e.into())
        }
    };
}
