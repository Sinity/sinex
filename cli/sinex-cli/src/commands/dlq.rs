use clap::Subcommand;

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_table_dlq, format_yaml, Spinner};
use crate::model::OutputFormat;
use crate::Result;

/// Dead letter queue operations
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List all DLQ entries
    sinexctl dlq list

    # Peek at messages in a specific queue
    sinexctl dlq peek events.failed -n 5

    # Requeue a specific message for retry
    sinexctl dlq requeue --event-id 01HQ2KM...

    # Requeue all failed messages
    sinexctl dlq requeue --all

    # Purge all messages (requires confirmation)
    sinexctl dlq purge --confirm
")]
pub enum DlqCommands {
    /// List dead letter queues
    #[command(alias = "ls")]
    List {
        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Peek at messages in a DLQ
    Peek {
        /// Subject name
        subject: String,

        /// Number of messages to peek
        #[arg(long, short = 'n', default_value = "10")]
        limit: u32,

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
                let queues = client.dlq_list().await?;
                match format {
                    OutputFormat::Table => {
                        if queues.is_empty() {
                            println!("No dead letter queues found.");
                        } else {
                            println!("{}", format_table_dlq(&queues));
                        }
                    }
                    OutputFormat::Json => {
                        for queue in &queues {
                            println!("{}", format_json(queue)?);
                        }
                    }
                    OutputFormat::Yaml => {
                        println!("{}", format_yaml(&queues)?);
                    }
                }
            }
            Self::Peek {
                subject,
                limit,
                format,
            } => {
                let messages = client.dlq_peek(subject, Some(*limit)).await?;
                match format {
                    OutputFormat::Table => {
                        if messages.is_empty() {
                            println!("No messages in queue.");
                        } else {
                            println!("Messages in {}:", subject);
                            println!("{}", "─".repeat(80));
                            for (i, msg) in messages.iter().enumerate() {
                                println!("\nMessage #{} (ID: {})", i + 1, msg.id);
                                println!("  Received: {}", msg.received_at);
                                if let Some(error) = &msg.error {
                                    println!("  Error: {}", error);
                                }
                                println!("  Payload:");
                                println!("{}", serde_json::to_string_pretty(&msg.payload)?);
                                if i < messages.len() - 1 {
                                    println!("{}", "─".repeat(80));
                                }
                            }
                        }
                    }
                    OutputFormat::Json => {
                        for msg in &messages {
                            println!("{}", format_json(msg)?);
                        }
                    }
                    OutputFormat::Yaml => {
                        println!("{}", format_yaml(&messages)?);
                    }
                }
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
                let spinner = Spinner::new(&msg);

                match client.dlq_requeue(event_id.clone(), *all).await {
                    Ok(()) => {
                        if *all {
                            spinner.finish_with_message("All messages requeued");
                        } else if let Some(id) = event_id {
                            spinner.finish_with_message(&format!("Event {} requeued", id));
                        }
                    }
                    Err(e) => {
                        spinner.abandon_with_message("Failed to requeue");
                        return Err(e);
                    }
                }
            }
            Self::Purge { confirm } => {
                // First, check how many messages would be deleted
                let spinner = Spinner::new("Checking DLQ...");
                let queues = client.dlq_list().await?;
                let message_count: u64 = queues.iter().map(|q| q.message_count).sum();
                spinner.finish_and_clear();

                if message_count == 0 {
                    println!("DLQ is already empty");
                    return Ok(());
                }

                // Require confirmation flag
                if !confirm {
                    eprintln!("Purge would delete {} messages from DLQ", message_count);
                    eprintln!();
                    eprintln!("Use --confirm to proceed with purge");
                    std::process::exit(1);
                }

                // Interactive confirmation for safety
                let prompt_msg = format!(
                    "Delete {} messages from DLQ? This cannot be undone.",
                    message_count
                );
                let proceed = inquire::Confirm::new(&prompt_msg)
                    .with_default(false)
                    .prompt()?;

                if !proceed {
                    println!("Cancelled");
                    return Ok(());
                }

                // Proceed with purge
                let spinner = Spinner::new(&format!("Purging {} messages...", message_count));
                match client.dlq_purge(true).await {
                    Ok(()) => {
                        spinner.finish_with_message(&format!(
                            "Purged {} messages from DLQ",
                            message_count
                        ));
                    }
                    Err(e) => {
                        spinner.abandon_with_message("Failed to purge DLQ");
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }
}
