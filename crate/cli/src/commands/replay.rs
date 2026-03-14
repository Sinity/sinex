use clap::Subcommand;
use sinex_primitives::rpc::replay::ReplayState;
use tokio::time::{Duration, sleep};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, ProgressReporter, format_json, format_yaml};
use crate::model::OutputFormat;

/// Replay operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # Create a replay plan for a node
    sinexctl replay plan --node terminal-ingestor

    # Create a replay plan with time window
    sinexctl replay plan --node terminal-ingestor --since 1h

    # Submit a replay plan for execution
    sinexctl replay submit 01HQ2KM...

    # Watch replay progress in real-time
    sinexctl replay watch 01HQ2KM...

    # Watch with custom poll interval
    sinexctl replay watch 01HQ2KM... --interval 5

    # Stream progress as JSON (for integration)
    sinexctl replay watch 01HQ2KM... -f json

    # List all replay operations
    sinexctl replay list

    # List in JSON format
    sinexctl replay list -f json
")]
pub enum ReplayCommands {
    /// Create a replay plan
    Plan {
        /// Node ID to replay events for
        #[arg(long)]
        node: String,

        /// Start time (RFC3339 or relative like "1h", "24h")
        #[arg(long)]
        since: Option<String>,

        /// End time (RFC3339 or relative, defaults to now)
        #[arg(long)]
        until: Option<String>,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Submit a replay plan for execution
    Submit {
        /// Plan ID
        plan_id: String,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Watch replay operation progress
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
        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },
}

impl ReplayCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::Plan {
                node,
                since,
                until,
                format,
            } => {
                let operation = client
                    .replay_plan(node, since.as_deref(), until.as_deref())
                    .await?;
                CommandOutput::single(operation, format_replay_plan_table).display(format)?;
            }
            Self::Submit { plan_id, format } => {
                let operation = client.replay_submit(plan_id).await?;
                CommandOutput::single(operation, format_replay_submit_table).display(format)?;
            }
            Self::Watch {
                operation_id,
                interval,
                format,
            } => {
                match format {
                    OutputFormat::Table => {
                        // Progress bar mode
                        let op = client.replay_status(operation_id).await?;
                        let progress =
                            ProgressReporter::new(op.checkpoint.total_events, "Replay operation");

                        loop {
                            let op = client.replay_status(operation_id).await?;
                            progress.set_position(op.checkpoint.processed_events);

                            match op.state {
                                ReplayState::Completed => {
                                    progress.finish_with_message("✓ Completed successfully");
                                    break;
                                }
                                ReplayState::Failed => {
                                    let msg = format!(
                                        "✗ Failed: {}",
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
                                    // Continue watching
                                    sleep(Duration::from_secs(*interval)).await;
                                }
                            }
                        }
                    }
                    OutputFormat::Json | OutputFormat::Dot => {
                        // Streaming JSON mode
                        loop {
                            let op = client.replay_status(operation_id).await?;
                            println!("{}", format_json(&op)?);

                            if op.state.is_terminal() {
                                break;
                            }

                            sleep(Duration::from_secs(*interval)).await;
                        }
                    }
                    OutputFormat::Yaml => {
                        // Just show final status
                        let op = client.replay_status(operation_id).await?;
                        println!("{}", format_yaml(&op)?);
                    }
                }
            }
            Self::List { format } => {
                let operations = client.replay_list().await?;
                CommandOutput::list(
                    operations,
                    "No replay operations found.",
                    format_replay_list_table,
                )
                .display(format)?;
            }
        }
        Ok(())
    }
}

use sinex_primitives::rpc::replay::ReplayOperation;

/// Format replay plan creation as table
fn format_replay_plan_table(operation: &ReplayOperation) -> String {
    let mut output = String::new();
    output.push_str("Replay Operation Created:\n");
    output.push_str(&format!("  Operation ID: {}\n", operation.operation_id));
    output.push_str(&format!("  State: {:?}\n", operation.state));
    output.push_str(&format!("  Node: {}\n", operation.scope.node_id));
    if let Some(ref window) = operation.scope.time_window {
        output.push_str(&format!("  Time Window: {} to {}\n", window.0, window.1));
    }
    output.push_str(&format!("  Created: {}\n", operation.created_at));
    output.push_str(&format!(
        "\nTo execute: sinexctl replay submit {}\n",
        operation.operation_id
    ));
    output
}

/// Format replay submission as table
fn format_replay_submit_table(operation: &ReplayOperation) -> String {
    let mut output = String::new();
    output.push_str("Replay Operation Started:\n");
    output.push_str(&format!("  Operation ID: {}\n", operation.operation_id));
    output.push_str(&format!("  State: {:?}\n", operation.state));
    output.push_str(&format!(
        "  Total Events: {}\n",
        operation.checkpoint.total_events
    ));
    output.push_str(&format!(
        "\nTo watch progress: sinexctl replay watch {}\n",
        operation.operation_id
    ));
    output
}

/// Format replay operations list as table
fn format_replay_list_table(operations: &[ReplayOperation]) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "{:<28} {:<12} {:<20} {:<10}\n",
        "OPERATION ID", "STATE", "NODE", "EVENTS"
    ));
    for op in operations {
        output.push_str(&format!(
            "{:<28} {:<12} {:<20} {:<10}\n",
            op.operation_id,
            format!("{:?}", op.state),
            op.scope.node_id,
            op.checkpoint.total_events
        ));
    }
    output
}
