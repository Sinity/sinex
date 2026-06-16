use clap::Subcommand;
use serde_json::Value;
use sinex_primitives::rpc::ops::{Operation as OpsOperation, OpsStartResponse};
use sinex_primitives::views::{OperationJobListView, OperationView, ViewEnvelope};

use crate::commands::audit::AuditCommand;
use crate::commands::dlq::DlqCommands;
use crate::commands::lifecycle::LifecycleCommands;
use crate::commands::replay::ReplayCommands;
use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, print_finite_envelope, render_envelope, with_spinner_result};
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

    /// Dead letter queue operations
    #[command(subcommand)]
    Dlq(DlqCommands),

    /// Replay operations
    #[command(subcommand)]
    Replay(ReplayCommands),

    /// Data lifecycle management (archive, restore, tombstone)
    #[command(subcommand)]
    Lifecycle(LifecycleCommands),

    /// Audit trail for an operation
    Audit(AuditCommand),
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
                let views = operations_to_views(&operations);
                let envelope = ViewEnvelope::new(
                    "sinexctl.ops.list",
                    OperationJobListView::new(views.clone()),
                )
                .with_query_echo(serde_json::json!({
                    "operation_type": operation_type,
                    "status": status,
                    "limit": limit,
                }));

                if let Some(output) = render_envelope(&envelope, &views, format)? {
                    print_machine_output(&output);
                    return Ok(());
                }

                if views.is_empty() {
                    println!("No operations found.");
                } else {
                    println!("{}", format_jobs_list_table(&views));
                }
            }
            Self::Get { operation_id } => {
                let operation = client.ops_get(operation_id).await?;
                let view = operation_to_view(&operation);
                let envelope = ViewEnvelope::new("sinexctl.ops.get", view.clone());

                if print_finite_envelope(&envelope, format)? {
                    return Ok(());
                }

                println!("{}", format_job_show_table(&view));
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
            Self::Dlq(cmd) => cmd.execute(client, format).await?,
            Self::Replay(cmd) => cmd.execute(client, format).await?,
            Self::Lifecycle(cmd) => cmd.execute(client, format).await?,
            Self::Audit(cmd) => cmd.execute(client, format).await?,
        }
        Ok(())
    }
}

impl JobsCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List {
                kind,
                status,
                limit,
            } => {
                let operations = client
                    .ops_list(kind.clone(), status.clone(), Some(*limit))
                    .await?;

                let views = operations_to_views(&operations);

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
                    print_machine_output(&output);
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

                if print_finite_envelope(&envelope, format)? {
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

fn operations_to_views(operations: &[OpsOperation]) -> Vec<OperationView> {
    operations.iter().map(operation_to_view).collect()
}

fn print_machine_output(output: &str) {
    print!("{output}");
    if !output.is_empty() && !output.ends_with('\n') {
        println!();
    }
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use sinex_primitives::domain::OperationStatus;
    use xtask::sandbox::sinex_test;

    fn fixture_operation(id: &str, operation_type: &str) -> OpsOperation {
        OpsOperation {
            id: id.to_string(),
            operation_type: operation_type.to_string(),
            operator: "operator.local".to_string(),
            scope: Some(serde_json::json!({"source": "test"})),
            result_status: OperationStatus::Success,
            result_message: Some("complete".to_string()),
            preview_summary: Some(serde_json::json!({"events": 2})),
            duration_ms: Some(42),
        }
    }

    #[sinex_test]
    async fn ops_list_json_renders_operation_view_envelope() -> xtask::TestResult<()> {
        let operations = vec![fixture_operation("op-1", "replay")];
        let views = operations_to_views(&operations);
        let envelope = ViewEnvelope::new(
            "sinexctl.ops.list",
            OperationJobListView::new(views.clone()),
        );

        let output =
            render_envelope(&envelope, &views, OutputFormat::Json)?.expect("json renders envelope");
        let parsed: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(parsed["source_surface"], "sinexctl.ops.list");
        assert_eq!(parsed["payload"]["count"], 1);
        assert_eq!(parsed["payload"]["jobs"][0]["kind"], "replay");
        assert!(parsed["payload"]["jobs"][0]["actions"].is_array());
        Ok(())
    }

    #[sinex_test]
    async fn ops_list_ndjson_renders_operation_view_records() -> xtask::TestResult<()> {
        let operations = vec![
            fixture_operation("op-1", "replay"),
            fixture_operation("op-2", "archive"),
        ];
        let views = operations_to_views(&operations);
        let envelope = ViewEnvelope::new(
            "sinexctl.ops.list",
            OperationJobListView::new(views.clone()),
        );

        let output = render_envelope(&envelope, &views, OutputFormat::Ndjson)?
            .expect("ndjson renders records");
        let lines: Vec<&str> = output.trim_end_matches('\n').split('\n').collect();

        assert_eq!(lines.len(), 2);
        let first: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(first["kind"], "replay");
        assert!(first.get("schema_version").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn ops_get_ndjson_is_rejected_as_finite_view() -> xtask::TestResult<()> {
        let operation = fixture_operation("op-1", "replay");
        let view = operation_to_view(&operation);
        let envelope = ViewEnvelope::new("sinexctl.ops.get", view);

        let err = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Ndjson)
            .expect_err("finite operation view rejects ndjson");
        assert!(err.to_string().contains("finite view"));
        Ok(())
    }
}
