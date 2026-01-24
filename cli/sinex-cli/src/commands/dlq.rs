use clap::Subcommand;

use crate::client::GatewayClient;
use crate::fmt::{with_spinner_result, CommandOutput, Spinner};
use crate::model::OutputFormat;
use crate::Result;

/// Dead letter queue operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # Show DLQ statistics
    sinexctl dlq list

    # Peek at messages in the DLQ
    sinexctl dlq peek -n 5

    # Requeue a specific message for retry
    sinexctl dlq requeue --event-id 01HQ2KM...

    # Requeue all failed messages
    sinexctl dlq requeue --all

    # Purge all messages (requires confirmation)
    sinexctl dlq purge --confirm
")]
pub enum DlqCommands {
    /// Show DLQ statistics
    #[command(alias = "ls")]
    List {
        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Peek at messages in the DLQ
    Peek {
        /// Number of messages to peek
        #[arg(long, short = 'n', default_value = "10")]
        limit: usize,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Requeue messages from DLQ back to processing
    Requeue {
        /// Specific event ID to requeue (optional)
        #[arg(long)]
        event_id: Option<String>,

        /// Requeue all messages
        #[arg(long)]
        all: bool,
    },

    /// Purge all messages from DLQ
    Purge {
        /// Confirm purge operation
        #[arg(long)]
        confirm: bool,
    },
}

impl DlqCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::List { format } => {
                let stats = client.dlq_list().await?;
                CommandOutput::single(stats, format_dlq_stats_table).display(format)?;
            }
            Self::Peek { limit, format } => {
                let response = client.dlq_peek(Some(*limit)).await?;
                CommandOutput::list(
                    response.messages,
                    "No messages in DLQ.",
                    format_dlq_messages_table,
                )
                .display(format)?;
            }
            Self::Requeue { event_id, all } => {
                if !all && event_id.is_none() {
                    return Err(color_eyre::eyre::eyre!(
                        "Must specify either --event-id or --all"
                    ));
                }

                let msg = if *all {
                    "Requeuing all messages...".to_string()
                } else {
                    format!("Requeuing event {}...", event_id.as_ref().unwrap())
                };

                let response = with_spinner_result(
                    msg,
                    "Messages requeued",
                    client.dlq_requeue(event_id.clone(), *all),
                )
                .await?;

                println!(
                    "{}: {} messages requeued",
                    response.status, response.requeued_count
                );
            }
            Self::Purge { confirm } => {
                // First, check how many messages would be deleted
                let stats = {
                    let spinner = Spinner::new("Checking DLQ...");
                    let stats = client.dlq_list().await?;
                    spinner.finish_and_clear();
                    stats
                };

                if stats.total_messages == 0 {
                    println!("DLQ is already empty");
                    return Ok(());
                }

                // Require confirmation flag
                if !confirm {
                    eprintln!(
                        "Purge would delete {} messages from DLQ",
                        stats.total_messages
                    );
                    eprintln!();
                    eprintln!("Use --confirm to proceed with purge");
                    std::process::exit(1);
                }

                // Interactive confirmation for safety
                let prompt_msg = format!(
                    "Delete {} messages from DLQ? This cannot be undone.",
                    stats.total_messages
                );
                let proceed = inquire::Confirm::new(&prompt_msg)
                    .with_default(false)
                    .prompt()?;

                if !proceed {
                    println!("Cancelled");
                    return Ok(());
                }

                // Proceed with purge
                let response = with_spinner_result(
                    format!("Purging {} messages...", stats.total_messages),
                    "DLQ purged",
                    client.dlq_purge(true),
                )
                .await?;

                println!(
                    "{}: {} messages purged",
                    response.status, response.purged_count
                );
            }
        }
        Ok(())
    }
}

use sinex_core::rpc::dlq::{DlqListResponse, DlqMessagePeek};

/// Format DLQ statistics as table
fn format_dlq_stats_table(stats: &DlqListResponse) -> String {
    let mut output = String::new();
    output.push_str("DLQ Statistics:\n");
    output.push_str(&format!("  Total messages: {}\n", stats.total_messages));
    output.push_str(&format!("  Total bytes: {}\n", stats.total_bytes));
    output.push_str(&format!("  First sequence: {}\n", stats.first_seq));
    output.push_str(&format!("  Last sequence: {}\n", stats.last_seq));
    output
}

/// Format DLQ messages as table
fn format_dlq_messages_table(messages: &[DlqMessagePeek]) -> String {
    let mut output = String::new();
    output.push_str("DLQ Messages:\n");
    output.push_str(&format!("{}\n", "─".repeat(80)));
    for (i, msg) in messages.iter().enumerate() {
        output.push_str(&format!(
            "\nMessage #{} (seq: {}, retries: {})\n",
            i + 1,
            msg.sequence,
            msg.retry_count
        ));
        output.push_str(&format!("  Subject: {}\n", msg.subject));
        if let Some(ref orig) = msg.original_subject {
            output.push_str(&format!("  Original subject: {}\n", orig));
        }
        output.push_str(&format!("  Preview: {}\n", msg.payload_preview));
        if i < messages.len() - 1 {
            output.push_str(&format!("{}\n", "─".repeat(80)));
        }
    }
    output
}
