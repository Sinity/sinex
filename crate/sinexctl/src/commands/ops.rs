use clap::Subcommand;
use serde_json::Value;
use sinex_primitives::rpc::ops::{Operation as OpsOperation, OpsStartResponse};
use sinex_primitives::views::{OperationJobListView, OperationView, ViewEnvelope};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, render_envelope, with_spinner_result};
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
    },

    /// Get operation details
    Get {
        /// Operation ID
        operation_id: String,
    },

    /// Cancel an operation
    Cancel {
        /// Operation ID
        operation_id: String,

        /// Cancellation reason
        #[arg(long, short = 'r')]
        reason: Option<String>,
    },

    /// Read-only job view — enumerate and inspect operations via ViewEnvelope
    #[command(subcommand)]
    Jobs(JobsCommands),
}

/// Read-only operation job surface (rendered through ViewEnvelope)
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List recent operations (all kinds)
    sinexctl ops jobs list

    # List only replay jobs
    sinexctl ops jobs list -t replay

    # List failed jobs, JSON output
    sinexctl ops jobs list -s failed --format json

    # Show a specific operation
    sinexctl ops jobs show 01HQ2KM...
")]
pub enum JobsCommands {
    /// List operations as a ViewEnvelope (all kinds, or filtered)
    #[command(alias = "ls")]
    List {
        /// Filter by operation kind (replay, archive, restore, purge, tombstone)
        #[arg(long, short = 't')]
        kind: Option<String>,

        /// Filter by result status (running, success, failed, cancelled, pending)
        #[arg(long, short = 's')]
        status: Option<String>,

        /// Maximum number of results
        #[arg(long, short = 'n', default_value = "50")]
        limit: i64,
    },

    /// Show a single operation as a ViewEnvelope
    Show {
        /// Operation ID
        operation_id: String,
    },
}

impl OpsCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Start {
                operation_type,
                scope,
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

                CommandOutput::single(response, format_ops_start_table).display(&format)?;
            }
            Self::List {
                operation_type,
                status,
                limit,
            } => {
                let operations = client
                    .ops_list(operation_type.clone(), status.clone(), Some(*limit))
                    .await?;

                CommandOutput::list(operations, "No operations found.", format_ops_list_table)
                    .display(&format)?;
            }
            Self::Get { operation_id } => {
                let operation = client.ops_get(operation_id).await?;
                CommandOutput::single(operation, format_ops_get_table).display(&format)?;
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
            Self::Jobs(jobs_cmd) => {
                jobs_cmd.execute(client, format).await?;
            }
        }
        Ok(())
    }
}

impl JobsCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List { kind, status, limit } => {
                let operations = client
                    .ops_list(kind.clone(), status.clone(), Some(*limit))
                    .await?;

                let views: Vec<OperationView> = operations
                    .iter()
                    .map(operation_to_view)
                    .collect();

                let envelope = ViewEnvelope::new(
                    "sinexctl.ops.jobs.list",
                    OperationJobListView::new(views.clone()),
                )
                .with_query_echo(serde_json::json!({
                    "kind": kind,
                    "status": status,
                    "limit": limit,
                }));

                if let Some(output) = render_envelope(&envelope, &views, format)? {
                    print!("{output}");
                    if !output.is_empty() && !output.ends_with('\n') {
                        println!();
                    }
                    return Ok(());
                }
                // Table format — human rendering
                if envelope.payload.jobs.is_empty() {
                    println!("No operations found.");
                } else {
                    println!("{}", format_jobs_list_table(&envelope.payload.jobs));
                }
            }
            Self::Show { operation_id } => {
                let operation = client.ops_get(operation_id).await?;
                let view = operation_to_view(&operation);

                let envelope = ViewEnvelope::new("sinexctl.ops.jobs.show", view.clone());

                if let Some(output) = render_envelope(&envelope, &[view.clone()], format)? {
                    print!("{output}");
                    if !output.is_empty() && !output.ends_with('\n') {
                        println!();
                    }
                    return Ok(());
                }
                // Table format — human rendering
                println!("{}", format_job_show_table(&view));
            }
        }
        Ok(())
    }
}

/// Convert the RPC `Operation` type to an [`OperationView`] for CLI rendering.
fn operation_to_view(op: &OpsOperation) -> OperationView {
    OperationView::from_rpc(
        op.id.clone(),
        &op.operation_type,
        op.operator.clone(),
        op.result_status,
        op.duration_ms,
        op.result_message.clone(),
        op.scope.clone(),
        op.preview_summary.clone(),
    )
}

/// Format ops jobs list as a human-readable table.
fn format_jobs_list_table(views: &[OperationView]) -> String {
    let mut output = String::new();
    output.push_str(&format!("{}\n", "─".repeat(80)));
    for view in views {
        output.push_str(&format!("ID:       {}\n", view.id));
        output.push_str(&format!("Kind:     {}\n", view.kind));
        output.push_str(&format!("Status:   {}\n", view.status));
        output.push_str(&format!("Operator: {}\n", view.operator));
        if let Some(ms) = view.duration_ms {
            output.push_str(&format!("Duration: {ms} ms\n"));
        }
        if let Some(msg) = view.result_message.as_deref() {
            output.push_str(&format!("Message:  {msg}\n"));
        }
        output.push_str(&format!("{}\n", "─".repeat(80)));
    }
    output
}

/// Format a single ops job as a human-readable detail view.
fn format_job_show_table(view: &OperationView) -> String {
    let mut output = String::new();
    output.push_str("Operation Job:\n");
    output.push_str(&format!("  ID:       {}\n", view.id));
    output.push_str(&format!("  Kind:     {}\n", view.kind));
    output.push_str(&format!("  Status:   {}\n", view.status));
    output.push_str(&format!("  Operator: {}\n", view.operator));
    if let Some(ms) = view.duration_ms {
        output.push_str(&format!("  Duration: {ms} ms\n"));
    }
    if let Some(msg) = view.result_message.as_deref() {
        output.push_str(&format!("  Message:  {msg}\n"));
    }
    if let Some(scope) = view.scope.as_ref() {
        if let Ok(pretty) = serde_json::to_string_pretty(scope) {
            output.push_str(&format!("  Scope:\n{pretty}\n"));
        }
    }
    if let Some(summary) = view.preview_summary.as_ref() {
        if let Ok(pretty) = serde_json::to_string_pretty(summary) {
            output.push_str(&format!("  Summary:\n{pretty}\n"));
        }
    }
    output
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
            output.push_str(&format!("Duration: {duration_ms} ms\n"));
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
        output.push_str(&format!("  Duration: {duration_ms} ms\n"));
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
