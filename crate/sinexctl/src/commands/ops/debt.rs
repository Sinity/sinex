use super::*;

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
    pub(super) const fn into_invalidation_trigger(self) -> InvalidationTrigger {
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
                    let coverage = client.sources_status_view().await?;
                    rows.extend(debt_rows_from_source_status_coverage(
                        &coverage.payload.sources,
                    ));
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

pub(crate) fn debt_rows_from_source_status_coverage(
    sources: &[SourceCoverageView],
) -> Vec<DebtRowView> {
    sources
        .iter()
        .flat_map(debt_rows_for_source_status_coverage)
        .collect()
}

pub(super) fn debt_rows_for_source_status_coverage(
    source: &SourceCoverageView,
) -> Vec<DebtRowView> {
    if matches!(
        source.readiness,
        SourceCoverageReadiness::Ready | SourceCoverageReadiness::Proposed
    ) && matches!(source.continuity, SourceCoverageContinuity::Active)
    {
        return Vec::new();
    }

    let (id_segment, stage, summary) = if source.material_count > 0 && source.event_count == 0 {
        (
            "material-without-events",
            DebtStage::MaterialReady,
            format!(
                "source `{}` has {} material record(s) but no admitted events",
                source.source_id, source.material_count
            ),
        )
    } else if source.event_count > 0 && source.material_count == 0 {
        (
            "events-without-material",
            DebtStage::Capturing,
            format!(
                "source `{}` has {} admitted event(s) but no registered material",
                source.source_id, source.event_count
            ),
        )
    } else if source
        .caveats
        .iter()
        .any(|caveat| caveat.id == "source.runtime_bridge.unobserved")
    {
        (
            "runtime-bridge-unobserved",
            DebtStage::Capturing,
            format!(
                "runtime bridge source `{}` is declared but has no observed material or admitted events",
                source.source_id
            ),
        )
    } else if !source.gaps.is_empty() || !source.caveats.is_empty() {
        (
            "coverage-caveat",
            DebtStage::Capturing,
            format!(
                "source `{}` reports {} coverage gap(s) and {} caveat(s)",
                source.source_id,
                source.gaps.len(),
                source.caveats.len()
            ),
        )
    } else {
        return Vec::new();
    };

    vec![capture_debt_row_from_status(
        source, id_segment, stage, summary,
    )]
}

pub(super) fn capture_debt_row_from_status(
    source: &SourceCoverageView,
    id_segment: &str,
    stage: DebtStage,
    summary: String,
) -> DebtRowView {
    let mut refs = vec![
        SinexObjectRef::new(SinexObjectKind::RpcMethod, "sources.status.view"),
        SinexObjectRef::new(SinexObjectKind::Command, "sources status"),
        SinexObjectRef::new(SinexObjectKind::SourceDriver, source.source_id.clone()),
    ];
    refs.extend(
        source
            .caveats
            .iter()
            .filter_map(|caveat| caveat.ref_.clone()),
    );

    let mut actions = vec![
        ActionAvailability::read(
            "source.status.inspect",
            "Inspect",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint("sinexctl sources status --format json")
        .with_rpc_method("sources.status.view"),
    ];
    actions.extend(source.actions.iter().cloned());

    DebtRowView {
        id: format!(
            "debt:capture:{}:{id_segment}",
            debt_id_segment(&source.source_id),
        ),
        kind: DebtKind::Capture,
        stage,
        summary,
        refs,
        owner: Some(DebtOwnerView {
            package_ref: Some(source.source_id.clone()),
            mode_ref: Some(source.source_id.clone()),
            policy_ref: None,
            operation_ref: None,
        }),
        age_secs: None,
        freshness: None,
        caveats: source.caveats.clone(),
        actions,
    }
}

pub(super) fn debt_rows_for_source_coverage(source: &SourceCoverageEntry) -> Vec<DebtRowView> {
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

pub(super) fn capture_debt_row(
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
            debt_id_segment(source.material_kind.as_str()),
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

pub(super) fn debt_id_segment(value: &str) -> String {
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

pub(super) fn debt_row_from_replay_operation(operation: &OpsOperation) -> Option<DebtRowView> {
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
            projection_rebuild_action(format!(
                "sinexctl ops start -t projection-rebuild -s '{}'",
                serde_json::json!({
                    "source": "replay-invalidation",
                    "replay_operation_id": operation.id,
                })
            )),
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

pub(super) fn debt_row_from_derivation(
    spec: &DerivationSpec,
    trigger: InvalidationTrigger,
) -> DebtRowView {
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
            projection_rebuild_action(format!(
                "sinexctl ops start -t projection-rebuild -s '{}'",
                serde_json::json!({"derivation": spec.id})
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

pub(super) fn projection_rebuild_action(command_hint: String) -> ActionAvailability {
    ActionAvailability {
        id: "projection.rebuild".to_string(),
        label: "Rebuild".to_string(),
        state: ActionAvailabilityState::Enabled,
        reason: Some(
            "starts a projection-rebuild operation from the current debt row scope".to_string(),
        ),
        command_hint: Some(command_hint),
        rpc_method: Some("ops.start".to_string()),
        side_effect: ActionSideEffect::Write,
        requires_confirmation: true,
        dry_run_available: true,
        audit_output_ref: None,
    }
}

pub(crate) const fn projection_trigger_name(trigger: InvalidationTrigger) -> &'static str {
    match trigger {
        InvalidationTrigger::Replay => "replay",
        InvalidationTrigger::Archive => "archive",
        InvalidationTrigger::Redaction => "redaction",
        InvalidationTrigger::SourceMaterialChange => "source-material-change",
        InvalidationTrigger::ParserSemanticsChange => "parser-semantics-change",
        InvalidationTrigger::DisclosurePolicyChange => "disclosure-policy-change",
    }
}
