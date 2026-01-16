use clap::Subcommand;
use serde_json::Value;

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use crate::util::json::get_str;
use crate::Result;

/// Operations log commands
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List recent operations
    sinexctl ops list

    # List only replay operations
    sinexctl ops list -t replay

    # List failed operations
    sinexctl ops list -s failed

    # Get operation details
    sinexctl ops get 01HQ2KM...

    # Start a new maintenance operation
    sinexctl ops start -t maintenance -o admin@example.com

    # Cancel an operation
    sinexctl ops cancel 01HQ2KM... -r 'No longer needed'
")]
pub enum OpsCommands {
    /// Start a new operation
    Start {
        /// Operation type (e.g., "replay", "migration", "maintenance")
        #[arg(long, short = 't')]
        operation_type: String,

        /// Operator identifier (user or service name)
        #[arg(long, short = 'o')]
        operator: String,

        /// Scope JSON (optional)
        #[arg(long, short = 's')]
        scope: Option<String>,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// List operations
    #[command(alias = "ls")]
    List {
        /// Filter by operation type
        #[arg(long, short = 't')]
        operation_type: Option<String>,

        /// Filter by status
        #[arg(long, short = 's')]
        status: Option<String>,

        /// Maximum number of results
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Get operation details
    Get {
        /// Operation ID
        operation_id: String,

        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "table")]
        format: OutputFormat,
    },

    /// Cancel an operation
    Cancel {
        /// Operation ID
        operation_id: String,

        /// Cancellation reason
        #[arg(long, short = 'r')]
        reason: Option<String>,
    },
}

impl OpsCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::Start {
                operation_type,
                operator,
                scope,
                format,
            } => {
                let scope_json: Option<Value> = scope
                    .as_ref()
                    .map(|s| serde_json::from_str(s))
                    .transpose()?;

                let operation_id = client
                    .ops_start(operation_type, operator, scope_json)
                    .await?;

                match format {
                    OutputFormat::Table => {
                        println!("Operation started successfully");
                        println!("  ID: {}", operation_id);
                        println!("  Type: {}", operation_type);
                        println!("  Operator: {}", operator);
                    }
                    OutputFormat::Json => {
                        println!(
                            "{}",
                            format_json(&serde_json::json!({
                                "operation_id": operation_id,
                                "operation_type": operation_type,
                                "operator": operator
                            }))?
                        );
                    }
                    OutputFormat::Yaml => {
                        println!(
                            "{}",
                            format_yaml(&serde_json::json!({
                                "operation_id": operation_id,
                                "operation_type": operation_type,
                                "operator": operator
                            }))?
                        );
                    }
                }
            }
            Self::List {
                operation_type,
                status,
                limit,
                format,
            } => {
                let operations = client
                    .ops_list(operation_type.clone(), status.clone(), Some(*limit))
                    .await?;

                match format {
                    OutputFormat::Table => {
                        if operations.is_empty() {
                            println!("No operations found.");
                        } else {
                            println!("Operations:");
                            println!("{}", "─".repeat(80));
                            for op in &operations {
                                println!("ID: {}", get_str(op, "operation_id"));
                                println!("Type: {}", get_str(op, "operation_type"));
                                println!("Status: {}", get_str(op, "status"));
                                println!("Started: {}", get_str(op, "started_at"));
                                println!("{}", "─".repeat(80));
                            }
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
            Self::Get {
                operation_id,
                format,
            } => {
                let operation = client.ops_get(operation_id).await?;

                match format {
                    OutputFormat::Table => {
                        println!("Operation Details:");
                        println!("  ID: {}", get_str(&operation, "operation_id"));
                        println!("  Type: {}", get_str(&operation, "operation_type"));
                        println!("  Status: {}", get_str(&operation, "status"));
                        println!("  Operator: {}", get_str(&operation, "operator"));
                        println!("  Started: {}", get_str(&operation, "started_at"));
                        if operation.get("completed_at").is_some() {
                            println!("  Completed: {}", get_str(&operation, "completed_at"));
                        }
                        if let Some(scope) = operation.get("scope") {
                            println!("  Scope: {}", serde_json::to_string_pretty(scope)?);
                        }
                    }
                    OutputFormat::Json => {
                        println!("{}", format_json(&operation)?);
                    }
                    OutputFormat::Yaml => {
                        println!("{}", format_yaml(&operation)?);
                    }
                }
            }
            Self::Cancel {
                operation_id,
                reason,
            } => {
                client.ops_cancel(operation_id, reason.clone()).await?;
                println!("Operation {} cancelled successfully", operation_id);
                if let Some(r) = reason {
                    println!("Reason: {}", r);
                }
            }
        }
        Ok(())
    }
}
