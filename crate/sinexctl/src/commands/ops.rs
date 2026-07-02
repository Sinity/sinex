use std::str::FromStr as _;

use base64::Engine as _;
use clap::{Subcommand, ValueEnum};
use serde_json::Value;
use sinex_primitives::evidence_bundle::{
    EvidenceBundleDiagnosticExcerptView, EvidenceBundleOmissionView,
    EvidenceBundleRuntimeHealthView, EvidenceBundleSavedArtifactView, EvidenceBundleSeedKind,
    EvidenceBundleSeedView, EvidenceBundleSpec, EvidenceBundleView,
};
use sinex_primitives::public_ref::{PublicSinexRef, ResolvedObjectView};
use sinex_primitives::rpc::content::StoreBlobRequest;
use sinex_primitives::rpc::dlq::DlqListResponse;
use sinex_primitives::rpc::ops::{Operation as OpsOperation, OpsStartResponse};
use sinex_primitives::rpc::runtime::RuntimeHealthResponse;
use sinex_primitives::rpc::sources::{SourceCoverageEntry, SourcePackageCompletenessPackageView};
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, ActionSideEffect, CaveatView, DebtKind,
    DebtListView, DebtOwnerView, DebtRowView, DebtStage, OperationJobListView, OperationView,
    SinexObjectKind, SinexObjectRef, SourceCoverageContinuity, SourceCoverageListView,
    SourceCoverageReadiness, SourceCoverageView, ViewEnvelope,
};
use sinex_primitives::{DerivationSpec, InvalidationTrigger, affected_derivations};

use crate::Result;
use crate::client::GatewayClient;
use crate::commands::audit::AuditCommand;
use crate::commands::blob::BlobCommands;
use crate::commands::demo::DemoCommand;
use crate::commands::dlq::DlqCommands;
use crate::commands::instructions::InstructionsCommand;
use crate::commands::lifecycle::LifecycleCommands;
use crate::commands::replay::ReplayCommands;
use crate::commands::show::resolve_ref;
use crate::commands::state::StateCommands;
use crate::commands::verify::VerifyCommand;
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

    /// Read-only debt view over work stuck between Sinex planes
    #[command(subcommand)]
    Debt(DebtCommands),

    /// Compile a finite evidence bundle from existing Sinex read surfaces
    #[command(subcommand)]
    Evidence(EvidenceCommands),

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

    /// Blob and content-store maintenance
    #[command(subcommand)]
    Blob(BlobCommands),

    /// Runtime state snapshot and restore operations
    #[command(subcommand)]
    State(StateCommands),

    /// Local desired-state instructions and actuator dispatch
    Instructions(InstructionsCommand),

    /// Check bounded runtime evidence and optional smoke probes
    Verify(VerifyCommand),

    /// Seed deterministic demo events directly into the database
    Demo(DemoCommand),
}

mod debt;
mod evidence;
mod jobs;

pub use debt::{DebtCommands, DebtProjectionTrigger};
pub(crate) use debt::{
    debt_rows_from_derivation_trigger, debt_rows_from_dlq, debt_rows_from_source_coverage,
    debt_rows_from_source_material_remediation, debt_rows_from_source_status_coverage,
    projection_trigger_name,
};
pub use evidence::EvidenceCommands;
pub use jobs::JobsCommands;


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
            Self::Debt(debt_cmd) => debt_cmd.execute(client, format).await?,
            Self::Evidence(evidence_cmd) => evidence_cmd.execute(client, format).await?,
            Self::Dlq(cmd) => cmd.execute(client, format).await?,
            Self::Replay(cmd) => cmd.execute(client, format).await?,
            Self::Lifecycle(cmd) => cmd.execute(client, format).await?,
            Self::Audit(cmd) => cmd.execute(client, format).await?,
            Self::Blob(cmd) => cmd.execute(format).await?,
            Self::State(cmd) => cmd.execute(format)?,
            Self::Instructions(cmd) => cmd.execute(client, format).await?,
            Self::Verify(cmd) => cmd.execute(client, format).await?,
            Self::Demo(cmd) => cmd.execute().await?,
        }
        Ok(())
    }
}

/// Convert the RPC `Operation` type to an [`OperationView`] for CLI rendering.
pub(crate) fn operation_to_view(op: &OpsOperation) -> OperationView {
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

pub(crate) fn operations_to_views(operations: &[OpsOperation]) -> Vec<OperationView> {
    operations.iter().map(operation_to_view).collect()
}

fn print_machine_output(output: &str) {
    print!("{output}");
    if !output.is_empty() && !output.ends_with('\n') {
        println!();
    }
}

fn format_debt_table(rows: &[DebtRowView]) -> String {
    let mut output = String::new();
    output.push_str("Debt:\n");
    output.push_str(&format!("{}\n", "─".repeat(80)));
    for row in rows {
        output.push_str(&format!("ID:      {}\n", row.id));
        output.push_str(&format!("Kind:    {:?}\n", row.kind));
        output.push_str(&format!("Stage:   {:?}\n", row.stage));
        output.push_str(&format!("Summary: {}\n", row.summary));
        if !row.refs.is_empty() {
            let refs = row
                .refs
                .iter()
                .map(|r| format!("{}:{}", object_kind_label(&r.kind), r.id))
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(&format!("Refs:    {refs}\n"));
        }
        if !row.actions.is_empty() {
            let actions = row
                .actions
                .iter()
                .filter_map(|action| action.command_hint.as_deref())
                .collect::<Vec<_>>()
                .join(", ");
            if !actions.is_empty() {
                output.push_str(&format!("Actions: {actions}\n"));
            }
        }
        output.push_str(&format!("{}\n", "─".repeat(80)));
    }
    output
}

fn evidence_debt_query_label(
    include_debt: bool,
    include_capture: bool,
    projection_trigger: Option<DebtProjectionTrigger>,
) -> String {
    let mut parts = Vec::new();
    if include_debt {
        parts.push("dlq");
    }
    if include_capture {
        parts.push("capture");
    }
    if let Some(trigger) = projection_trigger {
        parts.push(projection_trigger_name(trigger.into_invalidation_trigger()));
    }
    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join("+")
    }
}

fn debt_projection_trigger_from_name(name: &str) -> Option<InvalidationTrigger> {
    match name {
        "replay" => Some(InvalidationTrigger::Replay),
        "archive" => Some(InvalidationTrigger::Archive),
        "redaction" => Some(InvalidationTrigger::Redaction),
        "source_material_change" => Some(InvalidationTrigger::SourceMaterialChange),
        "parser_semantics_change" => Some(InvalidationTrigger::ParserSemanticsChange),
        "disclosure_policy_change" => Some(InvalidationTrigger::DisclosurePolicyChange),
        _ => None,
    }
}

fn format_evidence_bundle_table(view: &EvidenceBundleView) -> String {
    let mut output = String::new();
    output.push_str("Evidence Bundle:\n");
    output.push_str(&format!("  Schema:           {}\n", view.schema_version));
    output.push_str(&format!("  Generated:        {}\n", view.generated_at));
    output.push_str(&format!("  Source surface:   {}\n", view.source_surface));
    output.push_str(&format!("  Seeds:            {}\n", view.seeds.len()));
    output.push_str(&format!("  Target refs:      {}\n", view.target_refs.len()));
    output.push_str(&format!("  Included sections: {}\n", view.section_count()));
    output.push_str(&format!(
        "  Evidence rows:    {}\n",
        view.evidence_row_count()
    ));
    output.push_str(&format!(
        "  Runtime health:   {}\n",
        if view.runtime_health.is_some() {
            "included"
        } else {
            "not included"
        }
    ));
    output.push_str(&format!(
        "  Package rows:     {}\n",
        view.package_completeness.len()
    ));
    output.push_str(&format!(
        "  Diagnostic excerpts: {}\n",
        view.diagnostic_excerpts.len()
    ));
    output.push_str(&format!("  Caveats:          {}\n", view.caveats.len()));
    output.push_str(&format!(
        "  Disclosure caveats: {}\n",
        view.disclosure_caveats.len()
    ));
    output.push_str(&format!("  Actions:          {}\n", view.actions.len()));
    if let Some(artifact) = view.saved_artifact.as_ref() {
        output.push_str(&format!("  Saved artifact:   {}\n", artifact.ref_));
    }
    output.push_str(&format!(
        "  Omitted sections: {}\n",
        view.omitted_sections.len()
    ));
    if !view.omitted_sections.is_empty() {
        output.push_str("Omissions:\n");
        for omission in &view.omitted_sections {
            output.push_str(&format!("  - {}: {}\n", omission.section, omission.reason));
        }
    }
    if !view.diagnostic_excerpts.is_empty() {
        output.push_str("Diagnostics:\n");
        for excerpt in &view.diagnostic_excerpts {
            let suffix = if excerpt.truncated { "..." } else { "" };
            output.push_str(&format!(
                "  - {}: {}{}\n",
                excerpt.section, excerpt.excerpt, suffix
            ));
        }
    }
    output
}

fn object_kind_label(kind: &SinexObjectKind) -> &'static str {
    match kind {
        SinexObjectKind::DlqMessage => "dlq_message",
        SinexObjectKind::RpcMethod => "rpc_method",
        SinexObjectKind::Operation => "operation",
        SinexObjectKind::Projection => "projection",
        SinexObjectKind::Artifact => "artifact",
        SinexObjectKind::AdmissionOutcome => "admission_outcome",
        SinexObjectKind::Policy => "policy",
        _ => "object",
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
#[path = "ops_test.rs"]
mod tests;
