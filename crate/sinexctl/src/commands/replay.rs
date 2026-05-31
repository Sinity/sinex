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
    # Create a replay plan for a node
    sinexctl replay plan --node terminal-ingestor

    # Create with scope filters
    sinexctl replay plan --node fs-ingestor --since 1h --material <UUID>

    # Preview what will be replayed. When the scope crosses material
    # boundaries (more than one source_material_id), the preview adds a
    # per-material replayability scorecard so the operator can see which
    # material drags the aggregate down. Each row shows the material id,
    # source identifier, replayability score (out of 5), and weakness
    # dimensions (timing / anchor / blob / parser / privacy).
    sinexctl replay preview <OPERATION_ID>

    # Approve and execute separately
    sinexctl replay approve <OPERATION_ID>
    sinexctl replay execute <OPERATION_ID>

    # Or use submit as shorthand for approve+execute
    sinexctl replay submit <OPERATION_ID>

    # Full convenience: plan+preview+approve+execute
    sinexctl replay run --node terminal-ingestor --since 24h

    # Watch progress
    sinexctl replay watch <OPERATION_ID>

    # Cancel an operation
    sinexctl replay cancel <OPERATION_ID> --reason 'wrong scope'

    # List all operations
    sinexctl replay list
    sinexctl replay list -f json
")]
pub enum ReplayCommands {
    /// Create a replay plan (planning state)
    Plan {
        /// Node ID to replay events for
        #[arg(long)]
        node: String,

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

        /// Filter by node ID
        #[arg(long)]
        node: Option<String>,

        /// Maximum number of results
        #[arg(long, default_value = "50")]
        limit: i64,
    },

    /// Full lifecycle: plan + preview + approve + execute (convenience)
    Run {
        /// Node ID to replay events for
        #[arg(long)]
        node: String,

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
                node,
                since,
                until,
                materials,
                event_types,
            } => {
                let operation = client
                    .replay_plan(
                        node,
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

            Self::List { state, node, limit } => {
                let operations = client
                    .replay_list_filtered(state.map(Into::into), node.as_deref(), Some(*limit))
                    .await?;
                CommandOutput::list(
                    operations,
                    "No replay operations found.",
                    format_replay_list_table,
                )
                .display(&format)?;
            }

            Self::Run {
                node,
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
                    node,
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
        OutputFormat::Json | OutputFormat::Dot => loop {
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
    node: &str,
    since: Option<&str>,
    until: Option<&str>,
    materials: &[String],
    event_types: &[String],
    dry_run: bool,
    gate_overrides: ReplayGateOverrides,
    format: &OutputFormat,
) -> Result<()> {
    eprintln!("Creating replay plan for node '{node}'...");
    let operation = client
        .replay_plan(node, since, until, materials, event_types)
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
            if let Some(nodes) = cascade.get("affected_nodes").and_then(|v| v.as_array()) {
                let names: Vec<&str> = nodes.iter().filter_map(|n| n.as_str()).collect();
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
    output.push_str(&format!("  Node:         {}\n", operation.scope.node_id));
    if let Some(ref window) = operation.scope.time_window {
        output.push_str(&format!("  Time Window:  {} to {}\n", window.0, window.1));
    }
    output.push_str(&format!("  Created:      {}\n", operation.created_at));
    output.push_str(&format!(
        "\nNext: sinexctl replay preview {}\n",
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
    output.push_str(&format!("  Node:         {}\n", operation.scope.node_id));

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

            if let Some(nodes) = cascade.get("affected_nodes").and_then(|v| v.as_array()) {
                let names: Vec<&str> = nodes.iter().filter_map(|n| n.as_str()).collect();
                if !names.is_empty() {
                    output.push_str(&format!("  Affected Nodes: {}\n", names.join(", ")));
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
        "\nNext: sinexctl replay approve {}\n",
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
        "\nNext: sinexctl replay execute {}\n",
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
        "\nWatch: sinexctl replay watch {}\n",
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
        "\nWatch: sinexctl replay watch {}\n",
        operation.operation_id
    ));
    output
}

fn format_replay_status_table(operation: &ReplayOperation) -> String {
    let mut output = String::new();
    output.push_str("Replay Operation:\n");
    output.push_str(&format!("  Operation ID: {}\n", operation.operation_id));
    output.push_str(&format!("  State:        {:?}\n", operation.state));
    output.push_str(&format!("  Node:         {}\n", operation.scope.node_id));
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
            op.scope.node_id,
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
///   - the scope has no material filter (replay covers a node-wide window),
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
            material_kind: m.material_kind.clone(),
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
mod tests {
    use super::{
        MaterialReplayabilityScorecard, Replayability, format_per_material_scorecard_table,
        format_replay_preview_table, preview_total_events, truncate_head_chars,
        truncate_tail_chars, weakness_dimensions,
    };
    use serde_json::json;
    use sinex_primitives::rpc::replay::{
        ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState,
    };
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn preview_total_events_accepts_valid_counts() -> TestResult<()> {
        assert_eq!(preview_total_events(&json!({ "total_events": 0 }))?, 0);
        assert_eq!(preview_total_events(&json!({ "total_events": 42 }))?, 42);
        Ok(())
    }

    #[sinex_test]
    async fn truncate_helpers_handle_multi_byte_utf8() -> TestResult<()> {
        // Mix of 1-byte ASCII, 2-byte (e), 3-byte (β), 4-byte (𝛼) characters.
        // Byte slicing here would panic at the 12-byte / len-25 boundaries
        // when those land in the middle of a code point — char-based
        // truncation must always succeed.
        let s = "/home/usér/φιλε-βυcket/path/𝛼-final-segment-with-extra-padding";
        // Just verify the calls don't panic and the return is non-empty.
        let head = truncate_head_chars(s, 12);
        assert!(!head.is_empty());
        let tail = truncate_tail_chars(s, 26, 25);
        assert!(!tail.is_empty());

        // Short strings are returned unchanged (no ellipsis).
        let short = "abc";
        assert_eq!(truncate_head_chars(short, 12), "abc");
        assert_eq!(truncate_tail_chars(short, 26, 25), "abc");

        // Length above threshold gets ellipsis.
        let long = "x".repeat(40);
        assert!(truncate_head_chars(&long, 12).ends_with('…'));
        assert!(truncate_tail_chars(&long, 26, 25).starts_with('…'));
        Ok(())
    }

    #[sinex_test]
    async fn preview_total_events_rejects_missing_field() -> TestResult<()> {
        let error = preview_total_events(&json!({})).expect_err("missing total_events must fail");
        assert!(error.to_string().contains("total_events"));
        Ok(())
    }

    #[sinex_test]
    async fn preview_total_events_rejects_non_numeric_field() -> TestResult<()> {
        let error = preview_total_events(&json!({ "total_events": "zero" }))
            .expect_err("non-numeric total_events must fail");
        assert!(error.to_string().contains("total_events"));
        Ok(())
    }

    #[sinex_test]
    async fn replay_preview_table_surfaces_failed_safety_analysis() -> TestResult<()> {
        let operation = ReplayOperation {
            operation_id: "op-1".to_string(),
            state: ReplayState::Previewed,
            scope: ReplayScope {
                node_id: "terminal-ingestor".to_string(),
                time_window: None,
                material_filter: None,
                filters: std::collections::HashMap::new(),
                source_unit_id: None,
                source_material_id: None,
                parser_id: None,
                parser_version: None,
            },
            preview_summary: None,
            checkpoint: ReplayCheckpoint {
                processed_events: 0,
                total_events: 0,
                last_event_id: None,
                batch_number: 0,
                savepoint_id: None,
                updated_at: "2026-04-04T00:00:00Z".to_string(),
            },
            actor: "tester".to_string(),
            created_at: "2026-04-04T00:00:00Z".to_string(),
            approved_by: None,
            approved_at: None,
            executor_node: None,
            started_at: None,
            finished_at: None,
            outcome: None,
            error_details: None,
        };
        let preview = json!({
            "total_events": 3,
            "anchor_churn_pct": null,
            "time_quality_flip_pct": null,
            "max_observed_depth": 7,
            "schema_boundary_crossed": true,
            "replay_gates": {
                "gates": [
                    {
                        "name": "anchor_churn_threshold_percent",
                        "tripped": false,
                        "advisory": true,
                        "observed": "not measured (advisory)",
                        "override_flag": "--allow-anchor-churn"
                    },
                    {
                        "name": "require_force_on_schema_mismatch",
                        "tripped": true,
                        "override_flag": "--force-schema-mismatch"
                    }
                ]
            },
            "safety_analysis": {
                "status": "failed",
                "error": "integrity analyzer unavailable",
                "warning": "Cascade impact could not be determined. Approve with caution."
            }
        });

        let rendered = format_replay_preview_table(&operation, &preview);

        assert!(rendered.contains("Safety Warning: analysis failed"));
        assert!(rendered.contains("Anchor Churn: not measured"));
        assert!(rendered.contains("Time Quality Flips: not measured"));
        assert!(rendered.contains("Max Cascade Depth: 7"));
        assert!(rendered.contains("Schema Boundary: true"));
        assert!(
            rendered.contains(
                "Gates Tripped: require_force_on_schema_mismatch (--force-schema-mismatch)"
            )
        );
        assert!(rendered.contains("Safety Error:   integrity analyzer unavailable"));
        assert!(rendered.contains(
            "Safety Detail:  Cascade impact could not be determined. Approve with caution."
        ));
        Ok(())
    }

    fn make_scorecard(
        material_id: &str,
        source: &str,
        status: sinex_primitives::MaterialStatus,
        replayability: Replayability,
    ) -> MaterialReplayabilityScorecard {
        MaterialReplayabilityScorecard {
            material_id: material_id.to_string(),
            source_identifier: source.to_string(),
            material_kind: "annex".to_string(),
            status,
            replayability,
        }
    }

    #[sinex_test]
    async fn weakness_dimensions_lists_failed_axes_only() -> TestResult<()> {
        // All-green scorecard reports no weaknesses.
        let strong = Replayability::from_material_facts(
            sinex_primitives::MaterialStatus::Completed,
            true,
            sinex_primitives::domain::SourceMaterialTimingInfoType::Intrinsic,
            Some(1024),
        );
        assert!(weakness_dimensions(&strong).is_empty());

        // Sensing material with no blob and inferred timing must surface
        // blob, timing, and anchor as weakness axes.
        let weak = Replayability::from_material_facts(
            sinex_primitives::MaterialStatus::Sensing,
            false,
            sinex_primitives::domain::SourceMaterialTimingInfoType::Inferred,
            None,
        );
        let dims = weakness_dimensions(&weak);
        assert!(dims.contains(&"blob"));
        assert!(dims.contains(&"timing"));
        assert!(dims.contains(&"anchor"));
        Ok(())
    }

    #[sinex_test]
    async fn per_material_scorecard_table_contains_aggregate_row() -> TestResult<()> {
        // Two materials with distinct replayability shapes — one strong,
        // one weak — should compose into an aggregate row that names the
        // material count and a midpoint score.
        let strong = Replayability::from_material_facts(
            sinex_primitives::MaterialStatus::Completed,
            true,
            sinex_primitives::domain::SourceMaterialTimingInfoType::Intrinsic,
            Some(2048),
        );
        let weak = Replayability::from_material_facts(
            sinex_primitives::MaterialStatus::Sensing,
            false,
            sinex_primitives::domain::SourceMaterialTimingInfoType::Inferred,
            None,
        );
        let rows = vec![
            make_scorecard(
                "mat-a-uuid",
                "/path/strong.csv",
                sinex_primitives::MaterialStatus::Completed,
                strong,
            ),
            make_scorecard(
                "mat-b-uuid",
                "/path/weak.csv",
                sinex_primitives::MaterialStatus::Sensing,
                weak,
            ),
        ];

        let rendered = format_per_material_scorecard_table(&rows);
        assert!(rendered.contains("Per-Material Replayability:"));
        assert!(rendered.contains("MATERIAL"));
        assert!(rendered.contains("WEAKNESSES"));
        // Both rows present (truncated material id prefix).
        assert!(rendered.contains("mat-a-uuid"));
        assert!(rendered.contains("mat-b-uuid"));
        // Aggregate row mentions the material count.
        assert!(rendered.contains("aggregate; 2 materials"));
        // Weak row surfaces the dimension labels in the WEAKNESSES column.
        assert!(rendered.contains("blob") || rendered.contains("timing"));
        Ok(())
    }
}
