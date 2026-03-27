use clap::Subcommand;
use serde_json::Value;
use sinex_primitives::rpc::ops::{Operation as OpsOperation, OpsStartResponse};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, with_spinner_result};
use crate::model::OutputFormat;

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
    sinexctl ops start -t maintenance

    # Cancel an operation
    sinexctl ops cancel 01HQ2KM... -r 'No longer needed'
")]
pub enum OpsCommands {
    /// Start a new operation
    Start {
        /// Operation type (e.g., "replay", "migration", "maintenance")
        #[arg(long, short = 't')]
        operation_type: String,

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
                scope,
                format,
            } => {
                let scope_json: Option<Value> = scope
                    .as_ref()
                    .map(|s| serde_json::from_str(s))
                    .transpose()?;

                let response = with_spinner_result(
                    format!("Starting {operation_type} operation..."),
                    "Operation started",
                    client.ops_start(operation_type, scope_json),
                )
                .await?;

                CommandOutput::single(response, format_ops_start_table).display(format)?;
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

                CommandOutput::list(operations, "No operations found.", format_ops_list_table)
                    .display(format)?;
            }
            Self::Get {
                operation_id,
                format,
            } => {
                let operation = client.ops_get(operation_id).await?;
                CommandOutput::single(operation, format_ops_get_table).display(format)?;
            }
            Self::Cancel {
                operation_id,
                reason,
            } => {
                with_spinner_result(
                    format!("Cancelling operation {operation_id}..."),
                    format!("Operation {operation_id} cancelled"),
                    client.ops_cancel(operation_id, reason.clone()),
                )
                .await?;

                if let Some(r) = reason {
                    println!("Reason: {r}");
                }
            }
        }
        Ok(())
    }
}

/// Format ops start response as table
fn format_ops_start_table(response: &OpsStartResponse) -> String {
    let mut output = String::new();
    output.push_str("Operation started successfully\n");
    output.push_str(&format!("  ID: {}\n", response.operation.id));
    output.push_str(&format!("  Type: {}\n", response.operation.operation_type));
    output.push_str(&format!("  Operator: {}\n", response.operation.operator));
    output
}

/// Format ops list as table
fn format_ops_list_table(operations: &[OpsOperation]) -> String {
    let mut output = String::new();
    output.push_str("Operations:\n");
    output.push_str(&format!("{}\n", "─".repeat(80)));
    for op in operations {
        output.push_str(&format!("ID: {}\n", op.id));
        output.push_str(&format!("Type: {}\n", op.operation_type));
        output.push_str(&format!("Status: {}\n", op.result_status));
        output.push_str(&format!("Operator: {}\n", op.operator));
        if let Some(duration_ms) = op.duration_ms {
            output.push_str(&format!("Duration: {} ms\n", duration_ms));
        }
        if let Some(message) = op.result_message.as_deref() {
            output.push_str(&format!("Message: {message}\n"));
        }
        output.push_str(&format!("{}\n", "─".repeat(80)));
    }
    output
}

/// Format ops get response as table
fn format_ops_get_table(operation: &OpsOperation) -> String {
    let mut output = String::new();
    output.push_str("Operation Details:\n");
    output.push_str(&format!("  ID: {}\n", operation.id));
    output.push_str(&format!("  Type: {}\n", operation.operation_type));
    output.push_str(&format!("  Status: {}\n", operation.result_status));
    output.push_str(&format!("  Operator: {}\n", operation.operator));
    if let Some(duration_ms) = operation.duration_ms {
        output.push_str(&format!("  Duration: {} ms\n", duration_ms));
    }
    if let Some(message) = operation.result_message.as_deref() {
        output.push_str(&format!("  Message: {message}\n"));
    }
    if let Some(scope) = operation.scope.as_ref()
        && let Ok(pretty_scope) = serde_json::to_string_pretty(scope)
    {
        output.push_str(&format!("  Scope: {pretty_scope}\n"));
    }
    if let Some(summary) = operation.preview_summary.as_ref()
        && let Ok(pretty_summary) = serde_json::to_string_pretty(summary)
    {
        output.push_str(&format!("  Summary: {pretty_summary}\n"));
    }
    output
}
