use clap::Subcommand;
use sinex_primitives::rpc::replay::ReplayState;
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

    # Preview what will be replayed
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

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Preview what a replay operation will affect
    Preview {
        /// Operation ID
        operation_id: String,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Approve a previewed replay operation for execution
    Approve {
        /// Operation ID
        operation_id: String,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Execute an approved replay operation
    Execute {
        /// Operation ID
        operation_id: String,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Approve and execute in one step (convenience)
    Submit {
        /// Operation ID
        operation_id: String,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Cancel a replay operation
    Cancel {
        /// Operation ID
        operation_id: String,

        /// Reason for cancellation
        #[arg(long)]
        reason: Option<String>,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Get replay operation status
    Status {
        /// Operation ID
        operation_id: String,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Watch replay operation progress in real-time
    Watch {
        /// Operation ID
        operation_id: String,

        /// Poll interval in seconds
        #[arg(long, default_value = "2")]
        interval: u64,

        /// Output format (json for streaming updates)
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// List replay operations
    #[command(alias = "ls")]
    List {
        /// Filter by state
        #[arg(long, value_enum)]
        state: Option<ReplayStateFilter>,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
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

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
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
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::Plan {
                node,
                since,
                until,
                materials,
                event_types,
                format,
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
                CommandOutput::single(operation, format_replay_plan_table).display(format)?;
            }

            Self::Preview {
                operation_id,
                format,
            } => {
                let (operation, preview) = client.replay_preview(operation_id).await?;
                match format {
                    OutputFormat::Json => println!(
                        "{}",
                        format_json(&serde_json::json!({
                            "operation": operation,
                            "preview": preview,
                        }))?
                    ),
                    OutputFormat::Yaml => println!(
                        "{}",
                        format_yaml(&serde_json::json!({
                            "operation": operation,
                            "preview": preview,
                        }))?
                    ),
                    _ => {
                        println!("{}", format_replay_preview_table(&operation, &preview));
                    }
                }
            }

            Self::Approve {
                operation_id,
                format,
            } => {
                let operation = client.replay_approve(operation_id).await?;
                CommandOutput::single(operation, format_replay_approve_table).display(format)?;
            }

            Self::Execute {
                operation_id,
                format,
            } => {
                let operation = client.replay_execute(operation_id).await?;
                CommandOutput::single(operation, format_replay_execute_table).display(format)?;
            }

            Self::Submit {
                operation_id,
                format,
            } => {
                let operation = client.replay_submit(operation_id).await?;
                CommandOutput::single(operation, format_replay_submit_table).display(format)?;
            }

            Self::Cancel {
                operation_id,
                reason,
                format,
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

            Self::Status {
                operation_id,
                format,
            } => {
                let operation = client.replay_status(operation_id).await?;
                CommandOutput::single(operation, format_replay_status_table).display(format)?;
            }

            Self::Watch {
                operation_id,
                interval,
                format,
            } => {
                execute_watch(client, operation_id, *interval, format).await?;
            }

            Self::List { state, format } => {
                let operations = client.replay_list_filtered(state.map(Into::into)).await?;
                CommandOutput::list(
                    operations,
                    "No replay operations found.",
                    format_replay_list_table,
                )
                .display(format)?;
            }

            Self::Run {
                node,
                since,
                until,
                materials,
                event_types,
                format,
            } => {
                execute_run(
                    client,
                    node,
                    since.as_deref(),
                    until.as_deref(),
                    materials,
                    event_types,
                    format,
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
    format: &OutputFormat,
) -> Result<()> {
    eprintln!("Creating replay plan for node '{node}'...");
    let operation = client
        .replay_plan(node, since, until, materials, event_types)
        .await?;
    let op_id = operation.operation_id.clone();
    eprintln!("  Operation: {op_id}");

    eprintln!("Computing preview...");
    let (_operation, preview) = client.replay_preview(&op_id).await?;
    let total = preview
        .get("total_events")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    eprintln!("  Preview: {total} events in scope");

    if total == 0 {
        eprintln!("No events to replay. Cancelling.");
        let _ = client.replay_cancel(&op_id, Some("empty scope")).await;
        return Ok(());
    }

    eprintln!("Approving...");
    let _ = client.replay_approve(&op_id).await?;

    eprintln!("Executing replay...");
    let operation = client.replay_execute(&op_id).await?;

    execute_watch(client, &op_id, 2, format).await?;

    let _ = operation;
    Ok(())
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
        output.push_str(&format!("  Total Events: {total}\n"));
    }
    if let Some(window) = preview.get("time_window")
        && let (Some(start), Some(end)) = (
            window.get("start").and_then(|v| v.as_str()),
            window.get("end").and_then(|v| v.as_str()),
        )
    {
        output.push_str(&format!("  Time Window:  {start} to {end}\n"));
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
