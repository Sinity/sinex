use clap::Subcommand;

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_table_dlq, format_yaml};
use crate::model::OutputFormat;
use crate::Result;

/// Dead letter queue operations
#[derive(Debug, Subcommand)]
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
        }
        Ok(())
    }
}
