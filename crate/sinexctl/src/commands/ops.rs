use std::str::FromStr as _;

use clap::{Subcommand, ValueEnum};
use serde_json::Value;
use sinex_primitives::evidence_bundle::{
    EvidenceBundleOmissionView, EvidenceBundleSeedView, EvidenceBundleView,
};
use sinex_primitives::public_ref::PublicSinexRef;
use sinex_primitives::rpc::dlq::DlqListResponse;
use sinex_primitives::rpc::ops::{Operation as OpsOperation, OpsStartResponse};
use sinex_primitives::rpc::sources::{SourceCoverageEntry, SourcesCoverageRequest};
use sinex_primitives::views::{
    ActionAvailability, ActionAvailabilityState, ActionSideEffect, CaveatView, DebtKind,
    DebtListView, DebtOwnerView, DebtRowView, DebtStage, OperationJobListView, OperationView,
    SinexObjectKind, SinexObjectRef, ViewEnvelope,
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

/// Read-only debt surface (rendered through ViewEnvelope)
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # List operator-visible debt rows
    sinexctl ops debt list

    # Include source coverage gaps as capture debt rows
    sinexctl ops debt list --include-capture

    # Render debt rows as JSON
    sinexctl ops debt list --format json
")]
pub enum DebtCommands {
    /// List debt rows from currently wired providers
    #[command(alias = "ls")]
    List {
        /// Include capture debt rows derived from the source coverage view.
        #[arg(long)]
        include_capture: bool,
        /// Include derivations invalidated by the selected trigger as projection debt.
        #[arg(long, value_enum)]
        projection_trigger: Option<DebtProjectionTrigger>,
    },
}

/// Portable read-profile compiler over existing Sinex observability surfaces.
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    sinexctl ops evidence compile --ref operation:01HQ2KM...
    sinexctl ops evidence compile --operation 01HQ2KM... --include-debt
    sinexctl ops evidence compile --source-driver media.screen-ocr --include-debt --include-capture
")]
pub enum EvidenceCommands {
    /// Compile a finite evidence bundle from explicit seeds.
    Compile {
        /// Public Sinex refs to resolve through `sinexctl show` semantics.
        #[arg(long = "ref", value_name = "REF")]
        refs: Vec<String>,
        /// Operation ids to include via OperationView.
        #[arg(long, value_name = "OPERATION_ID")]
        operation: Vec<String>,
        /// Source/package driver ids to include from SourceCoverage.
        #[arg(long = "source-driver", value_name = "SOURCE_ID")]
        source_driver: Vec<String>,
        /// Include currently wired debt providers.
        #[arg(long)]
        include_debt: bool,
        /// Include capture debt rows derived from source coverage.
        #[arg(long)]
        include_capture: bool,
        /// Include projection debt derived from the selected invalidation trigger.
        #[arg(long, value_enum)]
        projection_trigger: Option<DebtProjectionTrigger>,
        /// Non-authoritative operator note to carry with the bundle.
        #[arg(long = "note", value_name = "TEXT")]
        notes: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DebtProjectionTrigger {
    Replay,
    Archive,
    Redaction,
    SourceMaterialChange,
    ParserSemanticsChange,
    DisclosurePolicyChange,
}

impl DebtProjectionTrigger {
    const fn into_invalidation_trigger(self) -> InvalidationTrigger {
        match self {
            Self::Replay => InvalidationTrigger::Replay,
            Self::Archive => InvalidationTrigger::Archive,
            Self::Redaction => InvalidationTrigger::Redaction,
            Self::SourceMaterialChange => InvalidationTrigger::SourceMaterialChange,
            Self::ParserSemanticsChange => InvalidationTrigger::ParserSemanticsChange,
            Self::DisclosurePolicyChange => InvalidationTrigger::DisclosurePolicyChange,
        }
    }
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

impl EvidenceCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Compile {
                refs,
                operation,
                source_driver,
                include_debt,
                include_capture,
                projection_trigger,
                notes,
            } => {
                let bundle = compile_evidence_bundle(
                    client,
                    refs,
                    operation,
                    source_driver,
                    *include_debt,
                    *include_capture,
                    *projection_trigger,
                    notes,
                )
                .await?;
                let envelope = ViewEnvelope::new("sinexctl.ops.evidence.compile", bundle)
                    .with_query_echo(serde_json::json!({
                        "refs": refs,
                        "operation": operation,
                        "source_driver": source_driver,
                        "include_debt": include_debt,
                        "include_capture": include_capture,
                        "projection_trigger": projection_trigger
                            .map(|trigger| projection_trigger_name(trigger.into_invalidation_trigger())),
                    }));

                if print_finite_envelope(&envelope, format)? {
                    return Ok(());
                }

                println!("{}", format_evidence_bundle_table(&envelope.payload));
            }
        }
        Ok(())
    }
}

async fn compile_evidence_bundle(
    client: &GatewayClient,
    refs: &[String],
    operation_ids: &[String],
    source_driver_ids: &[String],
    include_debt: bool,
    include_capture: bool,
    projection_trigger: Option<DebtProjectionTrigger>,
    notes: &[String],
) -> Result<EvidenceBundleView> {
    let mut bundle = EvidenceBundleView::new("sinexctl.ops.evidence.compile").with_target_context(
        (!refs.is_empty() || !operation_ids.is_empty() || !source_driver_ids.is_empty())
            .then(|| "explicit operator-selected seeds".to_string()),
    );

    for ref_text in refs {
        let public_ref = PublicSinexRef::from_str(ref_text)?;
        bundle.seeds.push(EvidenceBundleSeedView::public_ref(
            public_ref.clone().into_object_ref(),
        ));
        bundle
            .resolved_objects
            .push(resolve_ref(client, public_ref).await?.payload);
    }

    for operation_id in operation_ids {
        bundle
            .seeds
            .push(EvidenceBundleSeedView::operation(operation_id.clone()));
        let operation = client.ops_get(operation_id).await?;
        bundle.operations.push(operation_to_view(&operation));
    }

    if !source_driver_ids.is_empty() {
        let coverage = client.sources_status_view().await?;
        for source_id in source_driver_ids {
            bundle
                .seeds
                .push(EvidenceBundleSeedView::source_driver(source_id.clone()));
            if let Some(source) = coverage
                .payload
                .sources
                .iter()
                .find(|source| source.source_id == *source_id)
            {
                bundle.source_coverage.push(source.clone());
            } else {
                bundle.omitted_sections.push(EvidenceBundleOmissionView::new(
                    format!("source_coverage:{source_id}"),
                    "source-driver seed was requested but the source coverage view had no matching row",
                ));
            }
        }
    }

    for note in notes {
        bundle
            .seeds
            .push(EvidenceBundleSeedView::operator_note(note.clone()));
    }

    if include_debt || include_capture || projection_trigger.is_some() {
        bundle.seeds.push(EvidenceBundleSeedView::debt_query(
            evidence_debt_query_label(include_debt, include_capture, projection_trigger),
        ));
        if include_debt {
            bundle
                .debt_rows
                .extend(debt_rows_from_dlq(&client.dlq_list().await?));
        }
        if include_capture {
            let coverage = client.sources_coverage(SourcesCoverageRequest {}).await?;
            bundle
                .debt_rows
                .extend(debt_rows_from_source_coverage(&coverage.sources));
        }
        if let Some(trigger) = projection_trigger {
            bundle.debt_rows.extend(debt_rows_from_derivation_trigger(
                trigger.into_invalidation_trigger(),
            ));
        }
    }

    if bundle.evidence_row_count() == 0 {
        bundle.omitted_sections.push(EvidenceBundleOmissionView::new(
            "evidence_rows",
            "no requested seed produced evidence rows through the currently wired read surfaces",
        ));
    }

    Ok(bundle)
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

impl DebtCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List {
                include_capture,
                projection_trigger,
            } => {
                let dlq = client.dlq_list().await?;
                let mut rows = debt_rows_from_dlq(&dlq);
                if *include_capture {
                    let coverage = client.sources_coverage(SourcesCoverageRequest {}).await?;
                    rows.extend(debt_rows_from_source_coverage(&coverage.sources));
                }
                if let Some(trigger) = projection_trigger {
                    rows.extend(debt_rows_from_derivation_trigger(
                        trigger.into_invalidation_trigger(),
                    ));
                }
                let operations = client
                    .ops_list(Some("replay".to_string()), None, Some(100))
                    .await?;
                let replay_debt = debt_rows_from_replay_operations(&operations);
                if !replay_debt.is_empty() {
                    rows.extend(replay_debt);
                }
                let mut providers = vec!["raw_ingest_dlq"];
                if *include_capture {
                    providers.push("source_coverage");
                }
                if projection_trigger.is_some() {
                    providers.push("derivation_specs");
                }
                providers.push("replay_operations");
                let envelope =
                    ViewEnvelope::new("sinexctl.ops.debt", DebtListView::new(rows.clone()))
                        .with_query_echo(serde_json::json!({
                            "providers": providers,
                            "projection_trigger": projection_trigger
                                .map(|trigger| projection_trigger_name(trigger.into_invalidation_trigger())),
                        }));

                if let Some(output) = render_envelope(&envelope, &rows, format)? {
                    print_machine_output(&output);
                    return Ok(());
                }

                if envelope.payload.rows.is_empty() {
                    println!("No debt rows reported by wired providers.");
                } else {
                    println!("{}", format_debt_table(&envelope.payload.rows));
                }
            }
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

pub(crate) fn debt_rows_from_dlq(stats: &DlqListResponse) -> Vec<DebtRowView> {
    if stats.total_messages == 0 {
        return Vec::new();
    }

    vec![DebtRowView {
        id: "debt:admission:raw-ingest-dlq".to_string(),
        kind: DebtKind::Admission,
        stage: DebtStage::CandidateQuarantined,
        summary: format!(
            "{} raw-ingest message(s) are pending in DLQ pressure={} span={}",
            stats.total_messages, stats.pressure_level, stats.pending_sequence_span
        ),
        refs: vec![SinexObjectRef::new(
            SinexObjectKind::DlqMessage,
            format!("raw-ingest-dlq:{}..{}", stats.first_seq, stats.last_seq),
        )],
        owner: Some(DebtOwnerView::admission_policy("raw-ingest-dlq")),
        age_secs: None,
        freshness: None,
        caveats: vec![CaveatView {
            id: format!("raw_ingest_dlq.{}", stats.pressure_level),
            message: stats.action_reason.clone(),
            ref_: Some(SinexObjectRef::new(SinexObjectKind::RpcMethod, "dlq.list")),
        }],
        actions: vec![
            ActionAvailability::read("debt.inspect", "Inspect", ActionAvailabilityState::Enabled)
                .with_command_hint(format!("sinexctl {}", stats.recommended_action))
                .with_rpc_method("dlq.peek"),
        ],
    }]
}

pub(crate) fn debt_rows_from_source_coverage(sources: &[SourceCoverageEntry]) -> Vec<DebtRowView> {
    sources
        .iter()
        .flat_map(debt_rows_for_source_coverage)
        .collect()
}

fn debt_rows_for_source_coverage(source: &SourceCoverageEntry) -> Vec<DebtRowView> {
    let material_count = source.material_count.unwrap_or_default();
    let event_count = source.event_count.unwrap_or_default();

    if material_count > 0 && event_count == 0 {
        vec![capture_debt_row(
            source,
            "material-without-events",
            DebtStage::MaterialReady,
            format!(
                "source `{}` has {} `{}` material record(s) but no admitted events",
                source.source_identifier, material_count, source.material_kind
            ),
        )]
    } else if event_count > 0 && material_count == 0 {
        vec![capture_debt_row(
            source,
            "events-without-material",
            DebtStage::Capturing,
            format!(
                "source `{}` has {} admitted event(s) but no registered `{}` material",
                source.source_identifier, event_count, source.material_kind
            ),
        )]
    } else {
        Vec::new()
    }
}

fn capture_debt_row(
    source: &SourceCoverageEntry,
    id_segment: &str,
    stage: DebtStage,
    summary: String,
) -> DebtRowView {
    let actions = vec![
        ActionAvailability::read(
            "source.coverage.inspect",
            "Inspect",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint("sinexctl sources coverage")
        .with_rpc_method("sources.coverage"),
    ];

    DebtRowView {
        id: format!(
            "debt:capture:{}:{}:{id_segment}",
            debt_id_segment(&source.source_identifier),
            debt_id_segment(&source.material_kind),
        ),
        kind: DebtKind::Capture,
        stage,
        summary,
        refs: vec![
            SinexObjectRef::new(SinexObjectKind::RpcMethod, "sources.coverage"),
            SinexObjectRef::new(SinexObjectKind::Command, "sources coverage"),
        ],
        owner: Some(DebtOwnerView {
            package_ref: Some(source.source_identifier.clone()),
            mode_ref: None,
            policy_ref: None,
            operation_ref: None,
        }),
        age_secs: None,
        freshness: None,
        caveats: Vec::new(),
        actions,
    }
}

fn debt_id_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}

pub(crate) fn debt_rows_from_derivation_trigger(trigger: InvalidationTrigger) -> Vec<DebtRowView> {
    affected_derivations(trigger)
        .map(|spec| debt_row_from_derivation(spec, trigger))
        .collect()
}

pub(crate) fn debt_rows_from_replay_operations(operations: &[OpsOperation]) -> Vec<DebtRowView> {
    operations
        .iter()
        .filter_map(debt_row_from_replay_operation)
        .collect()
}

fn debt_row_from_replay_operation(operation: &OpsOperation) -> Option<DebtRowView> {
    let marker = operation
        .preview_summary
        .as_ref()?
        .get("scope_invalidation")?;
    if marker.get("phase").and_then(Value::as_str) != Some("pending") {
        return None;
    }

    let archived_count = marker.get("archived_count").and_then(Value::as_u64);
    let bucket_count = marker.get("bucket_count").and_then(Value::as_u64);
    let scope_key_count = marker.get("scope_key_count").and_then(Value::as_u64);
    let event_count = marker.get("event_count").and_then(Value::as_u64);
    let summary = format!(
        "replay operation `{}` archived {} event(s) with {} pending invalidation bucket(s), {} scope key(s), {} affected event id(s)",
        operation.id,
        archived_count.unwrap_or_default(),
        bucket_count.unwrap_or_default(),
        scope_key_count.unwrap_or_default(),
        event_count.unwrap_or_default()
    );
    let operation_ref = SinexObjectRef::new(SinexObjectKind::Operation, operation.id.clone());

    Some(DebtRowView {
        id: format!("debt:projection:replay-invalidation:{}", operation.id),
        kind: DebtKind::Projection,
        stage: DebtStage::ProjectionStale,
        summary,
        refs: vec![operation_ref.clone()],
        owner: Some(DebtOwnerView::operation(operation_ref.clone())),
        age_secs: None,
        freshness: None,
        caveats: vec![CaveatView {
            id: "replay.invalidation.pending".to_string(),
            message: "archive committed before the replay scope invalidation marker was cleared; inspect or rerun replay recovery before treating affected projections as fresh".to_string(),
            ref_: Some(operation_ref.clone()),
        }],
        actions: vec![
            ActionAvailability::read(
                "replay.operation.inspect",
                "Inspect",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint(format!("sinexctl ops jobs show {}", operation.id))
            .with_rpc_method("ops.get"),
            ActionAvailability::read(
                "projection.explain",
                "Explain",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint("sinexctl ops debt list --projection-trigger replay"),
        ],
    })
}

fn debt_row_from_derivation(spec: &DerivationSpec, trigger: InvalidationTrigger) -> DebtRowView {
    DebtRowView {
        id: format!("debt:projection:{}:{trigger:?}", spec.id),
        kind: DebtKind::Projection,
        stage: DebtStage::ProjectionStale,
        summary: format!(
            "derived output `{}` is invalidated by {trigger:?}",
            spec.output_id
        ),
        refs: vec![SinexObjectRef::new(
            SinexObjectKind::Projection,
            spec.output_id,
        )],
        owner: Some(DebtOwnerView {
            package_ref: None,
            mode_ref: None,
            policy_ref: spec.rebuild_resource_policy_ref.map(ToOwned::to_owned),
            operation_ref: None,
        }),
        age_secs: None,
        freshness: None,
        caveats: vec![CaveatView {
            id: "projection.invalidated".to_string(),
            message: format!(
                "derivation `{}` should be rebuilt or explained before the output is treated as fresh",
                spec.id
            ),
            ref_: spec
                .disclosure_policy_ref
                .map(|policy| SinexObjectRef::new(SinexObjectKind::Policy, policy)),
        }],
        actions: vec![
            ActionAvailability {
                id: "projection.rebuild".to_string(),
                label: "Rebuild".to_string(),
                state: ActionAvailabilityState::Disabled,
                reason: Some(
                    "projection rebuild operations are tracked by #1974".to_string(),
                ),
                command_hint: None,
                rpc_method: None,
                side_effect: ActionSideEffect::Write,
                requires_confirmation: false,
                dry_run_available: false,
                audit_output_ref: None,
            }
            .with_command_hint(format!(
                "sinexctl ops start -t projection-rebuild -s '{{\"derivation\":\"{}\"}}'",
                spec.id
            )),
            ActionAvailability::read(
                "projection.explain",
                "Explain",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint(format!(
                "sinexctl ops debt list --projection-trigger {}",
                projection_trigger_name(trigger)
            )),
        ],
    }
}

const fn projection_trigger_name(trigger: InvalidationTrigger) -> &'static str {
    match trigger {
        InvalidationTrigger::Replay => "replay",
        InvalidationTrigger::Archive => "archive",
        InvalidationTrigger::Redaction => "redaction",
        InvalidationTrigger::SourceMaterialChange => "source-material-change",
        InvalidationTrigger::ParserSemanticsChange => "parser-semantics-change",
        InvalidationTrigger::DisclosurePolicyChange => "disclosure-policy-change",
    }
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

fn format_evidence_bundle_table(view: &EvidenceBundleView) -> String {
    let mut output = String::new();
    output.push_str("Evidence Bundle:\n");
    output.push_str(&format!("  Schema:           {}\n", view.schema_version));
    output.push_str(&format!("  Generated:        {}\n", view.generated_at));
    output.push_str(&format!("  Source surface:   {}\n", view.source_surface));
    output.push_str(&format!("  Seeds:            {}\n", view.seeds.len()));
    output.push_str(&format!("  Included sections: {}\n", view.section_count()));
    output.push_str(&format!(
        "  Evidence rows:    {}\n",
        view.evidence_row_count()
    ));
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
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use sinex_primitives::domain::OperationStatus;
    use sinex_primitives::public_ref::ResolvedObjectView;
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

    fn fixture_replay_operation_with_invalidation_phase(phase: &str) -> OpsOperation {
        OpsOperation {
            id: "op-replay-1".to_string(),
            operation_type: "replay".to_string(),
            operator: "operator.local".to_string(),
            scope: Some(serde_json::json!({"source_name": "test"})),
            result_status: OperationStatus::Running,
            result_message: Some("executing".to_string()),
            preview_summary: Some(serde_json::json!({
                "state": "Executing",
                "scope_invalidation": {
                    "phase": phase,
                    "archived_count": 3,
                    "bucket_count": 2,
                    "scope_key_count": 2,
                    "event_count": 3,
                    "recorded_at": "2026-06-19T20:00:00Z"
                }
            })),
            duration_ms: None,
        }
    }

    #[sinex_test]
    async fn evidence_debt_query_label_names_included_providers() -> xtask::TestResult<()> {
        assert_eq!(evidence_debt_query_label(false, false, None), "none");
        assert_eq!(evidence_debt_query_label(true, false, None), "dlq");
        assert_eq!(
            evidence_debt_query_label(true, true, Some(DebtProjectionTrigger::Replay)),
            "dlq+capture+replay"
        );
        Ok(())
    }

    #[sinex_test]
    async fn evidence_bundle_table_summarizes_existing_view_sections() -> xtask::TestResult<()> {
        let mut view = EvidenceBundleView::new("sinexctl.ops.evidence.compile");
        view.seeds
            .push(EvidenceBundleSeedView::public_ref(SinexObjectRef::new(
                SinexObjectKind::Command,
                "show",
            )));
        view.resolved_objects
            .push(ResolvedObjectView::unsupported(SinexObjectRef::new(
                SinexObjectKind::Command,
                "show",
            )));
        view.operations
            .push(operation_to_view(&fixture_operation("op-1", "replay")));
        view.debt_rows.extend(debt_rows_from_derivation_trigger(
            InvalidationTrigger::Replay,
        ));

        let table = format_evidence_bundle_table(&view);

        assert!(table.contains("Evidence Bundle"));
        assert!(table.contains("sinex.evidence-bundle/v1"));
        assert!(table.contains("Seeds:            1"));
        assert!(table.contains("Included sections: 3"));
        Ok(())
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

    fn fixture_dlq(total_messages: u64) -> DlqListResponse {
        let pressure_level = if total_messages > 10 {
            "critical"
        } else if total_messages > 0 {
            "warning"
        } else {
            "nominal"
        };
        let recommended_action = if total_messages == 0 {
            "none"
        } else {
            "ops dlq peek"
        };
        let action_reason = if total_messages == 0 {
            "raw-ingest DLQ is empty"
        } else {
            "inspect raw-ingest DLQ before retry"
        };
        DlqListResponse {
            total_messages,
            total_bytes: total_messages * 1024,
            first_seq: if total_messages == 0 { 0 } else { 10 },
            last_seq: if total_messages == 0 {
                0
            } else {
                10 + total_messages
            },
            pressure_level: pressure_level.to_string(),
            resource_pressure: sinex_primitives::rpc::dlq::DlqPressureSignal {
                pressure_level: pressure_level.to_string(),
                runtime_action: if total_messages > 10 {
                    "throttle".to_string()
                } else if total_messages > 0 {
                    "inspect".to_string()
                } else {
                    "admit".to_string()
                },
                pending_messages: total_messages,
                pending_bytes: total_messages * 1024,
                retry_batch_size: 10,
                recommended_action: recommended_action.to_string(),
                reason: action_reason.to_string(),
            },
            pending_sequence_span: total_messages,
            recommended_action: recommended_action.to_string(),
            action_reason: action_reason.to_string(),
        }
    }

    fn fixture_source_coverage(
        material_count: Option<i64>,
        event_count: Option<i64>,
    ) -> SourceCoverageEntry {
        SourceCoverageEntry {
            source_identifier: "terminal.shell-history".to_string(),
            material_kind: "shell_history".to_string(),
            earliest_ts: None,
            latest_ts: None,
            event_count,
            material_count,
        }
    }

    #[sinex_test]
    async fn debt_rows_from_dlq_reports_only_pending_admission_debt() -> xtask::TestResult<()> {
        assert!(debt_rows_from_dlq(&fixture_dlq(0)).is_empty());

        let rows = debt_rows_from_dlq(&fixture_dlq(3));
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.kind, DebtKind::Admission);
        assert_eq!(row.stage, DebtStage::CandidateQuarantined);
        assert_eq!(row.refs[0].kind, SinexObjectKind::DlqMessage);
        assert_eq!(
            row.actions[0].command_hint.as_deref(),
            Some("sinexctl ops dlq peek")
        );
        assert_eq!(row.caveats[0].id, "raw_ingest_dlq.warning");
        Ok(())
    }

    #[sinex_test]
    async fn debt_rows_from_source_coverage_reports_material_without_events()
    -> xtask::TestResult<()> {
        let rows = debt_rows_from_source_coverage(&[fixture_source_coverage(Some(12), Some(0))]);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.kind, DebtKind::Capture);
        assert_eq!(row.stage, DebtStage::MaterialReady);
        assert_eq!(
            row.owner
                .as_ref()
                .and_then(|owner| owner.package_ref.as_deref()),
            Some("terminal.shell-history")
        );
        assert_eq!(row.refs[0].kind, SinexObjectKind::RpcMethod);
        assert_eq!(row.refs[0].id, "sources.coverage");
        assert!(
            row.actions
                .iter()
                .any(|action| action.command_hint.as_deref() == Some("sinexctl sources coverage"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn debt_rows_from_source_coverage_reports_events_without_material()
    -> xtask::TestResult<()> {
        let rows = debt_rows_from_source_coverage(&[fixture_source_coverage(Some(0), Some(7))]);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, DebtKind::Capture);
        assert_eq!(rows[0].stage, DebtStage::Capturing);
        assert!(rows[0].summary.contains("no registered"));
        Ok(())
    }

    #[sinex_test]
    async fn debt_rows_from_source_coverage_omits_ready_active_sources() -> xtask::TestResult<()> {
        let rows = debt_rows_from_source_coverage(&[fixture_source_coverage(Some(2), Some(2))]);

        assert!(rows.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn debt_rows_from_derivation_trigger_reports_projection_debt() -> xtask::TestResult<()> {
        let rows = debt_rows_from_derivation_trigger(InvalidationTrigger::Replay);

        assert!(!rows.is_empty());
        let row = rows
            .iter()
            .find(|row| row.id.contains("domain.current_objects"))
            .expect("current objects projection reports replay debt");

        assert_eq!(row.kind, DebtKind::Projection);
        assert_eq!(row.stage, DebtStage::ProjectionStale);
        assert_eq!(row.refs[0].kind, SinexObjectKind::Projection);
        assert_eq!(row.refs[0].id, "domain.current_objects");
        assert_eq!(
            row.owner
                .as_ref()
                .and_then(|owner| owner.policy_ref.as_deref()),
            Some("resource-policy:projection.rebuild.standard")
        );
        assert_eq!(row.caveats[0].id, "projection.invalidated");
        assert_eq!(
            row.caveats[0].ref_.as_ref().map(|ref_| &ref_.kind),
            Some(&SinexObjectKind::Policy)
        );

        let rebuild = row
            .actions
            .iter()
            .find(|action| action.id == "projection.rebuild")
            .expect("rebuild action is advertised");
        assert_eq!(rebuild.side_effect, ActionSideEffect::Write);
        assert_eq!(rebuild.state, ActionAvailabilityState::Disabled);
        assert!(
            rebuild
                .command_hint
                .as_deref()
                .unwrap_or_default()
                .contains("projection-rebuild")
        );

        let explain = row
            .actions
            .iter()
            .find(|action| action.id == "projection.explain")
            .expect("explain action is advertised");
        assert_eq!(explain.side_effect, ActionSideEffect::Read);
        assert_eq!(explain.state, ActionAvailabilityState::Enabled);
        assert_eq!(
            explain.command_hint.as_deref(),
            Some("sinexctl ops debt list --projection-trigger replay")
        );

        assert!(
            debt_rows_from_derivation_trigger(InvalidationTrigger::SourceMaterialChange).is_empty()
        );
        Ok(())
    }

    #[sinex_test]
    async fn debt_rows_from_replay_operations_reports_pending_invalidation() -> xtask::TestResult<()>
    {
        let rows = debt_rows_from_replay_operations(&[
            fixture_replay_operation_with_invalidation_phase("pending"),
            fixture_replay_operation_with_invalidation_phase("published"),
        ]);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.kind, DebtKind::Projection);
        assert_eq!(row.stage, DebtStage::ProjectionStale);
        assert_eq!(row.refs[0].kind, SinexObjectKind::Operation);
        assert_eq!(row.refs[0].id, "op-replay-1");
        assert_eq!(
            row.owner
                .as_ref()
                .and_then(|owner| owner.operation_ref.as_ref())
                .map(|ref_| (&ref_.kind, ref_.id.as_str())),
            Some((&SinexObjectKind::Operation, "op-replay-1"))
        );
        assert!(row.summary.contains("3 event(s)"));
        assert!(row.caveats[0].id.contains("replay.invalidation.pending"));
        assert!(
            row.actions
                .iter()
                .any(|action| action.command_hint.as_deref()
                    == Some("sinexctl ops jobs show op-replay-1"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn ops_debt_list_json_renders_finite_debt_envelope() -> xtask::TestResult<()> {
        let mut rows = debt_rows_from_dlq(&fixture_dlq(12));
        rows.extend(debt_rows_from_derivation_trigger(
            InvalidationTrigger::Replay,
        ));
        let envelope = ViewEnvelope::new("sinexctl.ops.debt", DebtListView::new(rows.clone()));

        let output =
            render_envelope(&envelope, &rows, OutputFormat::Json)?.expect("json renders envelope");
        let parsed: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(parsed["source_surface"], "sinexctl.ops.debt");
        assert_eq!(parsed["payload"]["count"], rows.len());
        assert_eq!(parsed["payload"]["rows"][0]["kind"], "admission");
        assert_eq!(
            parsed["payload"]["rows"][0]["refs"][0]["kind"],
            "dlq_message"
        );
        let debt_rows = parsed["payload"]["rows"]
            .as_array()
            .expect("debt rows render as an array");
        assert!(debt_rows.iter().any(|row| {
            row["kind"] == "projection" && row["refs"][0]["id"] == "desktop.project_context"
        }));
        Ok(())
    }
}
