use clap::Subcommand;
use tokio::time::{sleep, Duration};

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_table_replay, format_yaml, ProgressReporter};
use crate::model::replay::ReplayStatus;
use crate::model::OutputFormat;
use crate::Result;

/// Replay operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # Create a replay plan for a query
    sinexctl replay plan --query 'source:terminal since:1h'

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
        /// Query specification
        #[arg(long)]
        query: String,

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
            Self::Plan { query, format } => {
                let plan = client.replay_plan(query).await?;
                match format {
                    OutputFormat::Table => {
                        println!("Replay Plan Created:");
                        println!("  Plan ID: {}", plan.id);
                        println!("  Event Count: {}", plan.event_count);
                        println!("  Query: {}", plan.query);
                        println!("  Created: {}", plan.created_at);
                        println!("\nTo execute: sinexctl replay submit {}", plan.id);
                    }
                    OutputFormat::Json => {
                        println!("{}", format_json(&plan)?);
                    }
                    OutputFormat::Yaml => {
                        println!("{}", format_yaml(&plan)?);
                    }
                }
            }
            Self::Submit { plan_id, format } => {
                let operation = client.replay_submit(plan_id).await?;
                match format {
                    OutputFormat::Table => {
                        println!("Replay Operation Started:");
                        println!("  Operation ID: {}", operation.id);
                        println!("  Status: {}", operation.status);
                        println!("  Total Events: {}", operation.total_events);
                        println!(
                            "\nTo watch progress: sinexctl replay watch {}",
                            operation.id
                        );
                    }
                    OutputFormat::Json => {
                        println!("{}", format_json(&operation)?);
                    }
                    OutputFormat::Yaml => {
                        println!("{}", format_yaml(&operation)?);
                    }
                }
            }
            Self::Watch {
                operation_id,
                interval,
                format,
            } => {
                match format {
                    OutputFormat::Table => {
                        // Progress bar mode
                        let status = client.replay_status(operation_id).await?;
                        let progress =
                            ProgressReporter::new(status.total_events, "Replay operation");

                        loop {
                            let status = client.replay_status(operation_id).await?;
                            progress.set_position(status.events_processed);

                            match status.status {
                                ReplayStatus::Completed => {
                                    progress.finish_with_message("✓ Completed successfully");
                                    break;
                                }
                                ReplayStatus::Failed => {
                                    let msg = format!(
                                        "✗ Failed: {}",
                                        status.error.as_deref().unwrap_or("Unknown error")
                                    );
                                    progress.abandon_with_message(&msg);
                                    return Err(color_eyre::eyre::eyre!(msg));
                                }
                                ReplayStatus::Cancelled => {
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
                    OutputFormat::Json => {
                        // Streaming JSON mode
                        loop {
                            let status = client.replay_status(operation_id).await?;
                            println!("{}", format_json(&status)?);

                            if matches!(
                                status.status,
                                ReplayStatus::Completed
                                    | ReplayStatus::Failed
                                    | ReplayStatus::Cancelled
                            ) {
                                break;
                            }

                            sleep(Duration::from_secs(*interval)).await;
                        }
                    }
                    OutputFormat::Yaml => {
                        // Just show final status
                        let status = client.replay_status(operation_id).await?;
                        println!("{}", format_yaml(&status)?);
                    }
                }
            }
            Self::List { format } => {
                let operations = client.replay_list().await?;
                match format {
                    OutputFormat::Table => {
                        if operations.is_empty() {
                            println!("No replay operations found.");
                        } else {
                            println!("{}", format_table_replay(&operations));
                        }
                    }
                    OutputFormat::Json => {
                        for op in &operations {
                            println!("{}", format_json(op)?);
                        }
                    }
                    OutputFormat::Yaml => {
                        println!("{}", format_yaml(&operations)?);
                    }
                }
            }
        }
        Ok(())
    }
}
