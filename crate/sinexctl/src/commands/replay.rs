use clap::Subcommand;
use color_eyre::eyre::eyre;
use sinex_primitives::rpc::replay::{ReplayGateOverrides, ReplayState};
use sinex_primitives::rpc::sources::{SourcesShowRequest, SourcesShowResponse};
use sinex_primitives::sources::continuity::{MaterialReplayabilityScorecard, Replayability};
use tokio::time::{Duration, sleep};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, ProgressReporter, format_json, format_yaml};
use crate::model::OutputFormat;

/// Replay operations — re-ingest source materials through the full pipeline
#[derive(Debug, Subcommand)]
#[command(after_help = "\
LIFECYCLE:
    plan → preview → approve → execute

EXAMPLES:
    # Create a replay plan for a source
    sinexctl ops replay plan --source terminal.zsh-history

    # Create with scope filters
    sinexctl ops replay plan --source fs --since 1h --material <UUID>

    # Preview what will be replayed. When the scope crosses material
    # boundaries (more than one source_material_id), the preview adds a
    # per-material replayability scorecard so the operator can see which
    # material drags the aggregate down. Each row shows the material id,
    # source identifier, replayability score (out of 5), and weakness
    # dimensions (timing / anchor / blob / parser / privacy).
    sinexctl ops replay preview <OPERATION_ID>

    # Approve and execute separately
    sinexctl ops replay approve <OPERATION_ID>
    sinexctl ops replay execute <OPERATION_ID>

    # Or use submit as shorthand for approve+execute
    sinexctl ops replay submit <OPERATION_ID>

    # Full convenience: plan+preview+approve+execute
    sinexctl ops replay run --source terminal.zsh-history --since 24h

    # Watch progress
    sinexctl ops replay watch <OPERATION_ID>

    # Cancel an operation
    sinexctl ops replay cancel <OPERATION_ID> --reason 'wrong scope'

    # List all operations
    sinexctl ops replay list
    sinexctl ops replay list -f json
")]
pub enum ReplayCommands {
    /// Create a replay plan (planning state)
    Plan {
        /// Source ID to replay events for
        #[arg(long)]
        source: String,

        /// Start time (RFC3339 or relative like "1h", "24h", "7d")
        #[arg(long)]
        since: Option<String>,

        /// End time (RFC3339 or relative, defaults to now)
        #[arg(long)]
        until: Option<String>,

        /// Filter by source material ID (repeatable)
        #[arg(long = "material", value_name = "UUID")]
        materials: Vec<String>,

        /// Filter by event type (repeatable)
        #[arg(long = "event-type", value_name = "TYPE")]
        event_types: Vec<String>,
    },

    /// Preview what a replay operation will affect
    Preview {
        /// Operation ID
        operation_id: String,
    },

    /// Approve a previewed replay operation for execution
    Approve {
        /// Operation ID
        operation_id: String,
    },

    /// Execute an approved replay operation
    Execute {
        /// Operation ID
        operation_id: String,

        /// Permit replay when previewed anchor churn exceeds the default threshold
        #[arg(long)]
        allow_anchor_churn: bool,

        /// Permit replay when timestamp-quality flips exceed the default threshold
        #[arg(long)]
        allow_time_quality_flips: bool,

        /// Permit replay when previewed cascade depth exceeds the default warning gate
        #[arg(long)]
        allow_deep_cascade: bool,

        /// Permit replay across a payload-schema boundary
        #[arg(long)]
        force_schema_mismatch: bool,
    },

    /// Approve and execute in one step (convenience)
    Submit {
        /// Operation ID
        operation_id: String,

        /// Permit replay when previewed anchor churn exceeds the default threshold
        #[arg(long)]
        allow_anchor_churn: bool,

        /// Permit replay when timestamp-quality flips exceed the default threshold
        #[arg(long)]
        allow_time_quality_flips: bool,

        /// Permit replay when previewed cascade depth exceeds the default warning gate
        #[arg(long)]
        allow_deep_cascade: bool,

        /// Permit replay across a payload-schema boundary
        #[arg(long)]
        force_schema_mismatch: bool,
    },

    /// Cancel a replay operation
    Cancel {
        /// Operation ID
        operation_id: String,

        /// Reason for cancellation
        #[arg(long)]
        reason: Option<String>,
    },

    /// Get replay operation status
    Status {
        /// Operation ID
        operation_id: String,
    },

    /// Watch replay operation progress in real-time
    Watch {
        /// Operation ID
        operation_id: String,

        /// Poll interval in seconds
        #[arg(long, default_value = "2")]
        interval: u64,
    },

    /// List replay operations
    #[command(alias = "ls", alias = "history")]
    List {
        /// Filter by state
        #[arg(long, value_enum)]
        state: Option<ReplayStateFilter>,

        /// Filter by source ID
        #[arg(long)]
        source: Option<String>,

        /// Maximum number of results
        #[arg(long, default_value = "50")]
        limit: i64,
    },

    /// Full lifecycle: plan + preview + approve + execute (convenience)
    Run {
        /// Source ID to replay events for
        #[arg(long)]
        source: String,

        /// Start time (RFC3339 or relative like "1h", "24h", "7d")
        #[arg(long)]
        since: Option<String>,

        /// End time (RFC3339 or relative, defaults to now)
        #[arg(long)]
        until: Option<String>,

        /// Filter by source material ID (repeatable)
        #[arg(long = "material", value_name = "UUID")]
        materials: Vec<String>,

        /// Filter by event type (repeatable)
        #[arg(long = "event-type", value_name = "TYPE")]
        event_types: Vec<String>,

        /// Dry-run: stop after preview without approving or executing any changes
        #[arg(long)]
        dry_run: bool,

        /// Permit replay when previewed anchor churn exceeds the default threshold
        #[arg(long)]
        allow_anchor_churn: bool,

        /// Permit replay when timestamp-quality flips exceed the default threshold
        #[arg(long)]
        allow_time_quality_flips: bool,

        /// Permit replay when previewed cascade depth exceeds the default warning gate
        #[arg(long)]
        allow_deep_cascade: bool,

        /// Permit replay across a payload-schema boundary
        #[arg(long)]
        force_schema_mismatch: bool,
    },
}

/// CLI filter for replay states (maps to `ReplayState`)
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum ReplayStateFilter {
    Planning,
    Previewed,
    Approved,
    Executing,
    Committing,
    Completed,
    Failed,
    Cancelled,
}

impl From<ReplayStateFilter> for ReplayState {
    fn from(f: ReplayStateFilter) -> Self {
        match f {
            ReplayStateFilter::Planning => ReplayState::Planning,
            ReplayStateFilter::Previewed => ReplayState::Previewed,
            ReplayStateFilter::Approved => ReplayState::Approved,
            ReplayStateFilter::Executing => ReplayState::Executing,
            ReplayStateFilter::Committing => ReplayState::Committing,
            ReplayStateFilter::Completed => ReplayState::Completed,
            ReplayStateFilter::Failed => ReplayState::Failed,
            ReplayStateFilter::Cancelled => ReplayState::Cancelled,
        }
    }
}

use sinex_primitives::rpc::replay::ReplayOperation;

impl ReplayCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Plan {
                source,
                since,
                until,
                materials,
                event_types,
            } => {
                let operation = client
                    .replay_plan(
                        source,
                        since.as_deref(),
                        until.as_deref(),
                        materials,
                        event_types,
                    )
                    .await?;
                CommandOutput::single(operation, format_replay_plan_table).display(&format)?;
            }

            Self::Preview { operation_id } => {
                let (operation, preview) = client.replay_preview(operation_id).await?;
                let scorecards = collect_material_scorecards(client, &operation).await?;
                match format {
                    OutputFormat::Json => println!(
                        "{}",
                        format_json(&serde_json::json!({
                            "operation": operation,
                            "preview": preview,
                            "per_material_replayability": scorecards,
                        }))?
                    ),
                    OutputFormat::Yaml => println!(
                        "{}",
                        format_yaml(&serde_json::json!({
                            "operation": operation,
                            "preview": preview,
                            "per_material_replayability": scorecards,
                        }))?
                    ),
                    _ => {
                        println!("{}", format_replay_preview_table(&operation, &preview));
                        if !scorecards.is_empty() {
                            println!();
                            println!("{}", format_per_material_scorecard_table(&scorecards));
                        }
                    }
                }
            }

            Self::Approve { operation_id } => {
                let operation = client.replay_approve(operation_id).await?;
                CommandOutput::single(operation, format_replay_approve_table).display(&format)?;
            }

            Self::Execute {
                operation_id,
                allow_anchor_churn,
                allow_time_quality_flips,
                allow_deep_cascade,
                force_schema_mismatch,
            } => {
                let operation = client
                    .replay_execute_with_overrides(
                        operation_id,
                        ReplayGateOverrides {
                            allow_anchor_churn: *allow_anchor_churn,
                            allow_time_quality_flips: *allow_time_quality_flips,
                            allow_deep_cascade: *allow_deep_cascade,
                            force_schema_mismatch: *force_schema_mismatch,
                        },
                    )
                    .await?;
                CommandOutput::single(operation, format_replay_execute_table).display(&format)?;
            }

            Self::Submit {
                operation_id,
                allow_anchor_churn,
                allow_time_quality_flips,
                allow_deep_cascade,
                force_schema_mismatch,
            } => {
                let operation = client
                    .replay_submit_with_overrides(
                        operation_id,
                        ReplayGateOverrides {
                            allow_anchor_churn: *allow_anchor_churn,
                            allow_time_quality_flips: *allow_time_quality_flips,
                            allow_deep_cascade: *allow_deep_cascade,
                            force_schema_mismatch: *force_schema_mismatch,
                        },
                    )
                    .await?;
                CommandOutput::single(operation, format_replay_submit_table).display(&format)?;
            }

            Self::Cancel {
                operation_id,
                reason,
            } => {
                let operation = client
                    .replay_cancel(operation_id, reason.as_deref())
                    .await?;
                match format {
                    OutputFormat::Json => println!(
                        "{}",
                        format_json(&serde_json::json!({
                            "operation_id": operation_id,
                            "state": operation.state,
                            "cancelled": true,
                        }))?
                    ),
                    _ => {
                        println!(
                            "Replay operation {operation_id} cancelled (state: {:?})",
                            operation.state
                        );
                    }
                }
            }

            Self::Status { operation_id } => {
                let operation = client.replay_status(operation_id).await?;
                CommandOutput::single(operation, format_replay_status_table).display(&format)?;
            }

            Self::Watch {
                operation_id,
                interval,
            } => {
                execute_watch(client, operation_id, *interval, &format).await?;
            }

            Self::List {
                state,
                source,
                limit,
            } => {
                let operations = client
                    .replay_list_filtered(state.map(Into::into), source.as_deref(), Some(*limit))
                    .await?;
                CommandOutput::list(
                    operations,
                    "No replay operations found.",
                    format_replay_list_table,
                )
                .display(&format)?;
            }

            Self::Run {
                source,
                since,
                until,
                materials,
                event_types,
                dry_run,
                allow_anchor_churn,
                allow_time_quality_flips,
                allow_deep_cascade,
                force_schema_mismatch,
            } => {
                execute_run(
                    client,
                    source,
                    since.as_deref(),
                    until.as_deref(),
                    materials,
                    event_types,
                    *dry_run,
                    ReplayGateOverrides {
                        allow_anchor_churn: *allow_anchor_churn,
                        allow_time_quality_flips: *allow_time_quality_flips,
                        allow_deep_cascade: *allow_deep_cascade,
                        force_schema_mismatch: *force_schema_mismatch,
                    },
                    &format,
                )
                .await?;
            }
        }
        Ok(())
    }
}

async fn execute_watch(
    client: &GatewayClient,
    operation_id: &str,
    interval: u64,
    format: &OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Table => {
            let op = client.replay_status(operation_id).await?;
            let progress = ProgressReporter::new(op.checkpoint.total_events, "Replay operation");

            loop {
                let op = client.replay_status(operation_id).await?;
                progress.set_position(op.checkpoint.processed_events);

                match op.state {
                    ReplayState::Completed => {
                        progress.finish_with_message("Completed successfully");
                        break;
                    }
                    ReplayState::Failed => {
                        let msg = format!(
                            "Failed: {}",
                            op.error_details.as_deref().unwrap_or("Unknown error")
                        );
                        progress.abandon_with_message(&msg);
                        return Err(color_eyre::eyre::eyre!(msg));
                    }
                    ReplayState::Cancelled => {
                        progress.abandon_with_message("Cancelled");
                        break;
                    }
                    _ => {
                        sleep(Duration::from_secs(interval)).await;
                    }
                }
            }
        }
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => loop {
            let op = client.replay_status(operation_id).await?;
            println!("{}", format_json(&op)?);
            if op.state.is_terminal() {
                break;
            }
            sleep(Duration::from_secs(interval)).await;
        },
        OutputFormat::Yaml => {
            let op = client.replay_status(operation_id).await?;
            println!("{}", format_yaml(&op)?);
        }
    }
    Ok(())
}

async fn execute_run(
    client: &GatewayClient,
    source: &str,
    since: Option<&str>,
    until: Option<&str>,
    materials: &[String],
    event_types: &[String],
    dry_run: bool,
    gate_overrides: ReplayGateOverrides,
    format: &OutputFormat,
) -> Result<()> {
    eprintln!("Creating replay plan for source '{source}'...");
    let operation = client
        .replay_plan(source, since, until, materials, event_types)
        .await?;
    let op_id = operation.operation_id.clone();
    eprintln!("  Operation: {op_id}");

    eprintln!("Computing preview...");
    let (previewed_operation, preview) = client.replay_preview(&op_id).await?;
    let total = preview_total_events(&preview)?;
    eprintln!("  Preview: {total} direct events in scope");

    // Show cascade impact if available
    if let Some(cascade) = preview.get("cascade_impact")
        && !cascade.is_null()
    {
        let derived = cascade
            .get("derived_events")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if derived > 0 {
            let cascade_total = cascade
                .get("cascade_total")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            eprintln!("  Cascade: {cascade_total} total ({total} direct + {derived} derived)");
            if let Some(modules) = cascade.get("affected_modules").and_then(|v| v.as_array()) {
                let names: Vec<&str> = modules.iter().filter_map(|n| n.as_str()).collect();
                if !names.is_empty() {
                    eprintln!("  Affected: {}", names.join(", "));
                }
            }
        }
    }

    if total == 0 {
        eprintln!("No events to replay. Cancelling.");
        client.replay_cancel(&op_id, Some("empty scope")).await?;
        return Ok(());
    }

    if dry_run {
        eprintln!("Dry-run complete. Preview captured; no approval or execution was performed.");
        match format {
            OutputFormat::Json => println!(
                "{}",
                format_json(&serde_json::json!({
                    "operation": previewed_operation,
                    "preview": preview,
                    "dry_run": true,
                }))?
            ),
            OutputFormat::Yaml => println!(
                "{}",
                format_yaml(&serde_json::json!({
                    "operation": previewed_operation,
                    "preview": preview,
                    "dry_run": true,
                }))?
            ),
            _ => println!(
                "{}",
                format_replay_preview_table(&previewed_operation, &preview)
            ),
        }
        return Ok(());
    }

    eprintln!("Approving...");
    client.replay_approve(&op_id).await?;

    eprintln!("Executing replay...");
    let operation = client
        .replay_execute_with_overrides(&op_id, gate_overrides)
        .await?;

    execute_watch(client, &op_id, 2, format).await?;

    let _ = operation;
    Ok(())
}

fn preview_total_events(preview: &serde_json::Value) -> Result<u64> {
    preview
        .get("total_events")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| eyre!("Replay preview is missing numeric `total_events`"))
}

fn format_replay_plan_table(operation: &ReplayOperation) -> String {
    let mut output = String::new();
    output.push_str("Replay Plan Created:\n");
    output.push_str(&format!("  Operation ID: {}\n", operation.operation_id));
    output.push_str(&format!("  State:        {:?}\n", operation.state));
    output.push_str(&format!(
        "  Source:         {}\n",
        operation.scope.source_name
    ));
    if let Some(ref window) = operation.scope.time_window {
        output.push_str(&format!("  Time Window:  {} to {}\n", window.0, window.1));
    }
    output.push_str(&format!("  Created:      {}\n", operation.created_at));
    output.push_str(&format!(
        "\nNext: sinexctl ops replay preview {}\n",
        operation.operation_id
    ));
    output
}

fn format_optional_percent(preview: &serde_json::Value, key: &str) -> String {
    preview
        .get(key)
        .and_then(serde_json::Value::as_f64)
        .map_or_else(
            || "not measured".to_string(),
            |value| format!("{value:.2}%"),
        )
}

fn format_replay_preview_table(operation: &ReplayOperation, preview: &serde_json::Value) -> String {
    let mut output = String::new();
    output.push_str("Replay Preview:\n");
    output.push_str(&format!("  Operation ID: {}\n", operation.operation_id));
    output.push_str(&format!("  State:        {:?}\n", operation.state));
    output.push_str(&format!(
        "  Source:         {}\n",
        operation.scope.source_name
    ));

    if let Some(total) = preview
        .get("total_events")
        .and_then(serde_json::Value::as_u64)
    {
        output.push_str(&format!("  Direct Events: {total}\n"));
    }
    if let Some(window) = preview.get("time_window")
        && let (Some(start), Some(end)) = (
            window.get("start").and_then(|v| v.as_str()),
            window.get("end").and_then(|v| v.as_str()),
        )
    {
        output.push_str(&format!("  Time Window:   {start} to {end}\n"));
    }

    output.push_str(&format!(
        "  Anchor Churn: {}\n",
        format_optional_percent(preview, "anchor_churn_pct")
    ));
    output.push_str(&format!(
        "  Time Quality Flips: {}\n",
        format_optional_percent(preview, "time_quality_flip_pct")
    ));
    output.push_str(&format!(
        "  Max Cascade Depth: {}\n",
        preview
            .get("max_observed_depth")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    ));
    output.push_str(&format!(
        "  Schema Boundary: {}\n",
        preview
            .get("schema_boundary_crossed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    ));

    if let Some(gates) = preview
        .get("replay_gates")
        .and_then(|v| v.get("gates"))
        .and_then(serde_json::Value::as_array)
    {
        let tripped = gates
            .iter()
            .filter(|gate| {
                gate.get("tripped")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
            })
            .filter_map(|gate| {
                let name = gate.get("name").and_then(serde_json::Value::as_str)?;
                let flag = gate
                    .get("override_flag")
                    .and_then(serde_json::Value::as_str)?;
                Some(format!("{name} ({flag})"))
            })
            .collect::<Vec<_>>();
        if !tripped.is_empty() {
            output.push_str(&format!("  Gates Tripped: {}\n", tripped.join(", ")));
        }
    }

    // Cascade impact section
    if let Some(cascade) = preview.get("cascade_impact")
        && !cascade.is_null()
    {
        let cascade_total = cascade
            .get("cascade_total")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let direct = cascade
            .get("direct_events")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let derived = cascade
            .get("derived_events")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);

        if derived > 0 {
            output.push_str(&format!(
                "  Cascade Total: {cascade_total} ({direct} direct + {derived} derived)\n"
            ));

            if let Some(modules) = cascade.get("affected_modules").and_then(|v| v.as_array()) {
                let names: Vec<&str> = modules.iter().filter_map(|n| n.as_str()).collect();
                if !names.is_empty() {
                    output.push_str(&format!("  Affected Modules: {}\n", names.join(", ")));
                }
            }

            if let Some(scopes) = cascade.get("affected_scopes").and_then(|v| v.as_array()) {
                let scope_count = scopes.len();
                let type_count = scopes
                    .iter()
                    .filter_map(|s| s.get("event_type").and_then(|v| v.as_str()))
                    .collect::<std::collections::HashSet<_>>()
                    .len();
                if scope_count > 0 {
                    output.push_str(&format!(
                        "  Affected Scopes: {scope_count} scope keys across {type_count} event types\n"
                    ));
                }
            }
        }
    }

    if let Some(safety_analysis) = preview.get("safety_analysis")
        && safety_analysis
            .get("status")
            .and_then(serde_json::Value::as_str)
            == Some("failed")
    {
        output.push_str(
            "  Safety Warning: analysis failed; review safety_analysis details before approval\n",
        );
        if let Some(message) = safety_analysis
            .get("error")
            .and_then(serde_json::Value::as_str)
        {
            output.push_str(&format!("  Safety Error:   {message}\n"));
        }
        if let Some(warning) = safety_analysis
            .get("warning")
            .and_then(serde_json::Value::as_str)
        {
            output.push_str(&format!("  Safety Detail:  {warning}\n"));
        }
    }

    output.push_str(&format!(
        "\nNext: sinexctl ops replay approve {}\n",
        operation.operation_id
    ));
    output
}

fn format_replay_approve_table(operation: &ReplayOperation) -> String {
    let mut output = String::new();
    output.push_str("Replay Approved:\n");
    output.push_str(&format!("  Operation ID: {}\n", operation.operation_id));
    output.push_str(&format!("  State:        {:?}\n", operation.state));
    output.push_str(&format!(
        "\nNext: sinexctl ops replay execute {}\n",
        operation.operation_id
    ));
    output
}

fn format_replay_execute_table(operation: &ReplayOperation) -> String {
    let mut output = String::new();
    output.push_str("Replay Execution Started:\n");
    output.push_str(&format!("  Operation ID: {}\n", operation.operation_id));
    output.push_str(&format!("  State:        {:?}\n", operation.state));
    output.push_str(&format!(
        "  Total Events: {}\n",
        operation.checkpoint.total_events
    ));
    output.push_str(&format!(
        "\nWatch: sinexctl ops replay watch {}\n",
        operation.operation_id
    ));
    output
}

fn format_replay_submit_table(operation: &ReplayOperation) -> String {
    let mut output = String::new();
    output.push_str("Replay Submitted (approved + executing):\n");
    output.push_str(&format!("  Operation ID: {}\n", operation.operation_id));
    output.push_str(&format!("  State:        {:?}\n", operation.state));
    output.push_str(&format!(
        "  Total Events: {}\n",
        operation.checkpoint.total_events
    ));
    output.push_str(&format!(
        "\nWatch: sinexctl ops replay watch {}\n",
        operation.operation_id
    ));
    output
}

fn format_replay_status_table(operation: &ReplayOperation) -> String {
    let mut output = String::new();
    output.push_str("Replay Operation:\n");
    output.push_str(&format!("  Operation ID: {}\n", operation.operation_id));
    output.push_str(&format!("  State:        {:?}\n", operation.state));
    output.push_str(&format!(
        "  Source:         {}\n",
        operation.scope.source_name
    ));
    output.push_str(&format!("  Actor:        {}\n", operation.actor));
    output.push_str(&format!(
        "  Progress:     {}/{}\n",
        operation.checkpoint.processed_events, operation.checkpoint.total_events
    ));
    output.push_str(&format!("  Created:      {}\n", operation.created_at));
    if let Some(ref started) = operation.started_at {
        output.push_str(&format!("  Started:      {started}\n"));
    }
    if let Some(ref finished) = operation.finished_at {
        output.push_str(&format!("  Finished:     {finished}\n"));
    }
    if let Some(ref error) = operation.error_details {
        output.push_str(&format!("  Error:        {error}\n"));
    }
    output
}

fn format_replay_list_table(operations: &[ReplayOperation]) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "{:<28} {:<12} {:<20} {:<10} {:<10}\n",
        "OPERATION ID", "STATE", "NODE", "PROGRESS", "CREATED"
    ));
    for op in operations {
        let progress = format!(
            "{}/{}",
            op.checkpoint.processed_events, op.checkpoint.total_events
        );
        let created = if op.created_at.len() > 19 {
            &op.created_at[..19]
        } else {
            &op.created_at
        };
        output.push_str(&format!(
            "{:<28} {:<12} {:<20} {:<10} {:<10}\n",
            op.operation_id,
            format!("{:?}", op.state),
            op.scope.source_name,
            progress,
            created,
        ));
    }
    output
}

/// Collect per-material replayability scorecards for every source material
/// referenced by the replay scope.
///
/// Returns an empty vec when:
///   - the scope has no material filter (replay covers a module-wide window),
///   - the filter resolves to a single material (the aggregate scorecard
///     already represents the same content; per-row breakdown is noise),
///   - or `sources.show` cannot resolve a UUID — that material's row is
///     skipped silently and the partial scorecard is returned, since the
///     operator-facing replay preview should not block on a missing
///     material lookup.
async fn collect_material_scorecards(
    client: &GatewayClient,
    operation: &ReplayOperation,
) -> Result<Vec<MaterialReplayabilityScorecard>> {
    // Collect every material id named on the scope. The replay scope can
    // reference materials in two places — the legacy multi-id
    // `material_filter` array and the typed-scope `source_material_id`
    // field — so we union them and dedupe.
    let mut ids: Vec<String> = Vec::new();
    if let Some(materials) = operation.scope.material_filter.as_ref() {
        ids.extend(materials.iter().cloned());
    }
    if let Some(single) = operation.scope.source_material_id.as_ref() {
        ids.push(single.clone());
    }
    ids.sort();
    ids.dedup();

    // Per-material breakdown is only useful when the scope crosses
    // material boundaries — otherwise the aggregate scorecard already
    // represents the same content and we'd just print noise.
    if ids.len() < 2 {
        return Ok(Vec::new());
    }

    let mut out: Vec<MaterialReplayabilityScorecard> = Vec::with_capacity(ids.len());
    for material_id in ids {
        let req = SourcesShowRequest {
            material_id: material_id.clone(),
        };
        let show: SourcesShowResponse = match client.sources_show(req).await {
            Ok(v) => v,
            Err(err) => {
                // A missing material in scope is not fatal at the preview
                // layer — operators may have archived a material out from
                // under a stale plan. Skip silently for those.
                //
                // Transport / auth / server / schema errors, however,
                // should surface as warnings so the preview is not silently
                // partial; a healthy-looking scorecard built on swallowed
                // failures lets operators make decisions on incomplete
                // diagnostics. (PR #1187 codex P2.)
                let msg = err.to_string();
                let is_not_found = msg.contains("not found")
                    || msg.contains("Not found")
                    || msg.contains("NotFound");
                if !is_not_found {
                    tracing::warn!(
                        material_id = %material_id,
                        error = %err,
                        "scorecard collection skipped material due to non-not-found error; preview will be partial"
                    );
                }
                continue;
            }
        };
        let m = &show.material;
        let has_blob = m.optional_blob_id.is_some();
        let replayability = Replayability::from_material_facts(
            m.status,
            has_blob,
            m.timing_info_type,
            m.total_bytes,
        );
        out.push(MaterialReplayabilityScorecard {
            material_id: m.id.clone(),
            source_identifier: m.source_identifier.clone(),
            material_kind: m.material_kind.to_string(),
            status: m.status,
            replayability,
        });
    }

    Ok(out)
}

/// Render the per-material replayability scorecard as a table. Each row
/// names the material id (truncated for readability), the source
/// identifier, the replayability score (`N/5` green dimensions), and the
/// weakness dimensions (the human-readable `weak_points` collapsed to
/// the dimension keys: timing/anchor/blob/parser/privacy). The final row
/// is the aggregate column — green dimensions averaged across all rows
/// — so the operator can see how the per-material rows compose into the
/// aggregate.
fn format_per_material_scorecard_table(rows: &[MaterialReplayabilityScorecard]) -> String {
    let mut out = String::new();
    out.push_str("Per-Material Replayability:\n");
    out.push_str(&format!(
        "  {:<14} {:<28} {:<14} {:<6} {}\n",
        "MATERIAL", "SOURCE", "STATUS", "SCORE", "WEAKNESSES"
    ));

    let mut score_total: u32 = 0;
    for row in rows {
        // Char-aware truncation. Byte-slicing identifiers panicked at the
        // table renderer when a source path or material id contained
        // multi-byte UTF-8, so operators lost the preview exactly when
        // replay scope was large enough to need truncation. PR #1187 codex P1.
        let mid = truncate_head_chars(&row.material_id, 12);
        let src = truncate_tail_chars(&row.source_identifier, 26, 25);
        let score = row.replayability.green_count();
        score_total += u32::from(score);
        let weak = weakness_dimensions(&row.replayability);
        let weak_str = if weak.is_empty() {
            "-".to_string()
        } else {
            weak.join(",")
        };
        out.push_str(&format!(
            "  {mid:<14} {src:<28} {status:<14} {score:>3}/5 {weak_str}\n",
            mid = mid,
            src = src,
            status = row.status,
            score = score,
            weak_str = weak_str,
        ));
    }

    // Aggregate row: average green-count across rows, rounded down.
    if !rows.is_empty() {
        #[allow(clippy::cast_possible_truncation)]
        let avg = (score_total / rows.len() as u32) as u8;
        out.push_str(&format!(
            "  {:<14} {:<28} {:<14} {:>3}/5 (aggregate; {} materials)\n",
            "—",
            "—",
            "—",
            avg,
            rows.len(),
        ));
    }

    out
}

/// Truncate the head of a string to at most `max_chars` characters and
/// append an ellipsis when truncation occurs. Char-aware so it never
/// panics on multi-byte UTF-8 boundaries.
fn truncate_head_chars(s: &str, max_chars: usize) -> String {
    let count = s.chars().count();
    if count <= max_chars {
        return s.to_string();
    }
    let head: String = s.chars().take(max_chars).collect();
    format!("{head}…")
}

/// Truncate the tail of a string to the last `tail_chars` characters and
/// prepend an ellipsis. Activates only when the source has more than
/// `threshold_chars` characters (so we don't expand short strings).
fn truncate_tail_chars(s: &str, threshold_chars: usize, tail_chars: usize) -> String {
    let count = s.chars().count();
    if count <= threshold_chars {
        return s.to_string();
    }
    let skip = count.saturating_sub(tail_chars);
    let tail: String = s.chars().skip(skip).collect();
    format!("…{tail}")
}

/// Compact dimension labels for the weakness column. Returns the
/// dimensions that are NOT green so the operator-facing table only
/// surfaces the failure modes, not the green dimensions.
fn weakness_dimensions(r: &Replayability) -> Vec<&'static str> {
    let mut out = Vec::new();
    if !r.raw_bytes_preserved {
        out.push("blob");
    }
    if !r.timing_quality {
        out.push("timing");
    }
    if !r.anchor_stability {
        out.push("anchor");
    }
    if !r.parser_determinism {
        out.push("parser");
    }
    if !r.privacy_safe_replay {
        out.push("privacy");
    }
    out
}

#[cfg(test)]
#[path = "replay_test.rs"]
mod tests;
