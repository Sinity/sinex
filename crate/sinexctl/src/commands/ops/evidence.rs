use super::*;

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
        /// Include the runtime health aggregate section.
        #[arg(long)]
        include_runtime_health: bool,
        /// Include package-completeness rows. Source-driver seeds include
        /// matching package rows even when this flag is not set.
        #[arg(long)]
        include_package_completeness: bool,
        /// Persist the compiled bundle payload in the content store and return
        /// the content-addressed artifact ref in the bundle.
        #[arg(long)]
        save_artifact: bool,
    },
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
                include_runtime_health,
                include_package_completeness,
                save_artifact,
            } => {
                let spec = build_evidence_bundle_spec(
                    refs,
                    operation,
                    source_driver,
                    *include_debt,
                    *include_capture,
                    *projection_trigger,
                    *include_runtime_health,
                    *include_package_completeness,
                    *save_artifact,
                )?;
                let mut bundle = compile_evidence_bundle(client, &spec).await?;
                if spec.save_artifact {
                    bundle.saved_artifact =
                        Some(save_evidence_bundle_artifact(client, &bundle).await?);
                }
                let envelope = ViewEnvelope::new("sinexctl.ops.evidence.compile", bundle)
                    .with_query_echo(serde_json::json!({
                        "refs": refs,
                        "operation": operation,
                        "source_driver": source_driver,
                        "include_debt": include_debt,
                        "include_capture": include_capture,
                        "include_runtime_health": include_runtime_health,
                        "include_package_completeness": include_package_completeness,
                        "save_artifact": save_artifact,
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

pub(super) fn build_evidence_bundle_spec(
    refs: &[String],
    operation_ids: &[String],
    source_driver_ids: &[String],
    include_debt: bool,
    include_capture: bool,
    projection_trigger: Option<DebtProjectionTrigger>,
    include_runtime_health: bool,
    include_package_completeness: bool,
    save_artifact: bool,
) -> Result<EvidenceBundleSpec> {
    let mut spec = EvidenceBundleSpec::new();
    spec.target_context =
        (!refs.is_empty() || !operation_ids.is_empty() || !source_driver_ids.is_empty())
            .then(|| "explicit operator-selected seeds".to_string());
    spec.include_debt = include_debt;
    spec.include_capture = include_capture;
    spec.projection_trigger = projection_trigger
        .map(|trigger| projection_trigger_name(trigger.into_invalidation_trigger()).to_string());
    spec.include_runtime_health = include_runtime_health;
    spec.include_package_completeness = include_package_completeness;
    spec.save_artifact = save_artifact;

    for ref_text in refs {
        let public_ref = PublicSinexRef::from_str(ref_text)?;
        spec.seeds.push(EvidenceBundleSeedView::public_ref(
            public_ref.into_object_ref(),
        ));
    }
    for operation_id in operation_ids {
        spec.seeds
            .push(EvidenceBundleSeedView::operation(operation_id.clone()));
    }
    for source_id in source_driver_ids {
        spec.seeds
            .push(EvidenceBundleSeedView::source_driver(source_id.clone()));
    }
    if include_debt || include_capture || projection_trigger.is_some() {
        spec.seeds.push(EvidenceBundleSeedView::debt_query(
            evidence_debt_query_label(include_debt, include_capture, projection_trigger),
        ));
    }

    Ok(spec)
}

pub(super) async fn compile_evidence_bundle(
    client: &GatewayClient,
    spec: &EvidenceBundleSpec,
) -> Result<EvidenceBundleView> {
    let ref_seeds = spec
        .seeds
        .iter()
        .filter(|seed| seed.kind == EvidenceBundleSeedKind::PublicRef)
        .collect::<Vec<_>>();
    let operation_seeds = spec
        .seeds
        .iter()
        .filter(|seed| seed.kind == EvidenceBundleSeedKind::Operation)
        .collect::<Vec<_>>();
    let source_driver_seeds = spec
        .seeds
        .iter()
        .filter(|seed| seed.kind == EvidenceBundleSeedKind::SourceDriver)
        .collect::<Vec<_>>();

    let mut rows = EvidenceBundleReadSurfaceRows::default();

    for seed in &ref_seeds {
        let public_ref = PublicSinexRef::from_str(&seed.value)?;
        rows.resolved_objects
            .push(resolve_ref(client, public_ref).await?.payload);
    }

    for seed in &operation_seeds {
        rows.operations.push(client.ops_get(&seed.value).await?);
    }

    if !source_driver_seeds.is_empty() || spec.include_capture {
        rows.source_coverage = Some(client.sources_status_view().await?.payload);
    }

    if spec.include_runtime_health {
        rows.runtime_health = Some(client.runtime_health(300).await?);
    }

    if spec.include_package_completeness || !source_driver_seeds.is_empty() {
        rows.package_completeness = Some(client.sources_package_completeness().await?.packages);
    }

    if spec.include_debt {
        rows.dlq = Some(client.dlq_list().await?);
    }

    compile_evidence_bundle_from_rows(spec, rows)
}

#[derive(Default)]
pub(super) struct EvidenceBundleReadSurfaceRows {
    pub(super) resolved_objects: Vec<ResolvedObjectView>,
    pub(super) operations: Vec<OpsOperation>,
    pub(super) source_coverage: Option<SourceCoverageListView>,
    pub(super) runtime_health: Option<RuntimeHealthResponse>,
    pub(super) package_completeness: Option<Vec<SourcePackageCompletenessPackageView>>,
    pub(super) dlq: Option<DlqListResponse>,
}

pub(super) fn compile_evidence_bundle_from_rows(
    spec: &EvidenceBundleSpec,
    rows: EvidenceBundleReadSurfaceRows,
) -> Result<EvidenceBundleView> {
    let mut bundle = EvidenceBundleView::new("sinexctl.ops.evidence.compile")
        .with_target_context(spec.target_context.clone());

    let ref_seeds = spec
        .seeds
        .iter()
        .filter(|seed| seed.kind == EvidenceBundleSeedKind::PublicRef)
        .collect::<Vec<_>>();
    let operation_seeds = spec
        .seeds
        .iter()
        .filter(|seed| seed.kind == EvidenceBundleSeedKind::Operation)
        .collect::<Vec<_>>();
    let source_driver_seeds = spec
        .seeds
        .iter()
        .filter(|seed| seed.kind == EvidenceBundleSeedKind::SourceDriver)
        .collect::<Vec<_>>();

    for (index, seed) in ref_seeds.iter().enumerate() {
        bundle.seeds.push((*seed).clone());
        if let Some(resolved) = rows.resolved_objects.get(index) {
            bundle.resolved_objects.push(resolved.clone());
        } else {
            bundle.omitted_sections.push(omitted_evidence_section(
                format!("resolved_ref:{}", seed.value),
                "public ref seed was requested but the ref resolver read surface was unavailable",
                seed.ref_.clone(),
            ));
        }
    }

    for (index, seed) in operation_seeds.iter().enumerate() {
        bundle.seeds.push((*seed).clone());
        if let Some(operation) = rows.operations.get(index) {
            bundle.operations.push(operation_to_view(operation));
        } else {
            bundle.omitted_sections.push(omitted_evidence_section(
                format!("operation:{}", seed.value),
                "operation seed was requested but the operation read surface was unavailable",
                seed.ref_.clone(),
            ));
        }
    }

    if !source_driver_seeds.is_empty() {
        let coverage = rows.source_coverage.as_ref();
        for seed in &source_driver_seeds {
            let source_id = &seed.value;
            bundle.seeds.push((*seed).clone());
            if let Some(source) = coverage.and_then(|coverage| {
                coverage
                    .sources
                    .iter()
                    .find(|source| source.source_id == *source_id)
            }) {
                bundle.source_coverage.push(source.clone());
            } else {
                bundle.omitted_sections.push(omitted_evidence_section(
                    format!("source_coverage:{source_id}"),
                    "source-driver seed was requested but the source coverage view had no matching row",
                    Some(SinexObjectRef::new(
                        SinexObjectKind::SourceDriver,
                        source_id.clone(),
                    )),
                ));
            }
        }
    }

    if spec.include_runtime_health {
        if let Some(runtime_health) = rows.runtime_health {
            bundle.runtime_health = Some(runtime_health_to_bundle_view(runtime_health, 300));
        } else {
            bundle.omitted_sections.push(omitted_evidence_section(
                "runtime_health",
                "runtime health was requested but the runtime health read surface was unavailable",
                Some(SinexObjectRef::new(
                    SinexObjectKind::RpcMethod,
                    "runtime.health",
                )),
            ));
        }
    }

    if spec.include_package_completeness || !source_driver_seeds.is_empty() {
        match rows.package_completeness {
            Some(completeness)
                if spec.include_package_completeness && source_driver_seeds.is_empty() =>
            {
                bundle.package_completeness = completeness;
            }
            Some(completeness) => {
                for seed in &source_driver_seeds {
                    let source_id = &seed.value;
                    let matching = completeness
                        .iter()
                        .filter(|package| package_matches_source_seed(package, source_id))
                        .cloned()
                        .collect::<Vec<_>>();

                    if matching.is_empty() {
                        bundle.omitted_sections.push(omitted_evidence_section(
                            format!("package_completeness:{source_id}"),
                            "source-driver seed was requested but package completeness had no matching package or mode row",
                            Some(SinexObjectRef::new(
                                SinexObjectKind::SourceDriver,
                                source_id.clone(),
                            )),
                        ));
                    } else {
                        bundle.package_completeness.extend(matching);
                    }
                }
                bundle
                    .package_completeness
                    .sort_by(|a, b| a.package_id.cmp(&b.package_id));
                bundle
                    .package_completeness
                    .dedup_by(|a, b| a.package_id == b.package_id);
            }
            None => {
                bundle.omitted_sections.push(omitted_evidence_section(
                    "package_completeness",
                    "package completeness was requested but the package completeness read surface was unavailable",
                    Some(SinexObjectRef::new(
                        SinexObjectKind::RpcMethod,
                        "sources.package_completeness",
                    )),
                ));
            }
        }
    }

    if spec.include_debt || spec.include_capture || spec.projection_trigger.is_some() {
        for seed in spec
            .seeds
            .iter()
            .filter(|seed| seed.kind == EvidenceBundleSeedKind::DebtQuery)
        {
            bundle.seeds.push(seed.clone());
        }
        if spec.include_debt {
            if let Some(dlq) = rows.dlq.as_ref() {
                bundle.debt_rows.extend(debt_rows_from_dlq(dlq));
            } else {
                bundle.omitted_sections.push(omitted_evidence_section(
                    "debt_rows:dlq",
                    "debt rows were requested but the DLQ read surface was unavailable",
                    Some(SinexObjectRef::new(SinexObjectKind::RpcMethod, "dlq.list")),
                ));
            }
        }
        if spec.include_capture {
            if let Some(coverage) = rows.source_coverage.as_ref() {
                bundle
                    .debt_rows
                    .extend(debt_rows_from_source_status_coverage(&coverage.sources));
            } else {
                bundle.omitted_sections.push(omitted_evidence_section(
                    "debt_rows:capture",
                    "capture debt rows were requested but source coverage was unavailable",
                    Some(SinexObjectRef::new(
                        SinexObjectKind::RpcMethod,
                        "sources.status.view",
                    )),
                ));
            }
        }
        if let Some(trigger) = spec
            .projection_trigger
            .as_deref()
            .and_then(debt_projection_trigger_from_name)
        {
            bundle
                .debt_rows
                .extend(debt_rows_from_derivation_trigger(trigger));
        }
    }

    if bundle.evidence_row_count() == 0 {
        bundle.omitted_sections.push(omitted_evidence_section(
            "evidence_rows",
            "no requested seed produced evidence rows through the currently wired read surfaces",
            None,
        ));
    }

    attach_evidence_bundle_context(&mut bundle);
    attach_bounded_diagnostic_excerpts(&mut bundle);

    Ok(bundle)
}

pub(super) fn omitted_evidence_section(
    section: impl Into<String>,
    reason: impl Into<String>,
    ref_: Option<SinexObjectRef>,
) -> EvidenceBundleOmissionView {
    let section = section.into();
    let reason = reason.into();
    let mut omission = EvidenceBundleOmissionView::new(section.clone(), reason.clone());
    omission.caveats.push(CaveatView {
        id: "evidence_bundle.section_unavailable".to_string(),
        message: reason,
        ref_,
    });
    omission
}

pub(super) fn attach_evidence_bundle_context(bundle: &mut EvidenceBundleView) {
    let mut target_refs = Vec::new();
    let mut caveats = Vec::new();
    let mut actions = Vec::new();

    for seed in &bundle.seeds {
        push_unique_ref_opt(&mut target_refs, seed.ref_.clone());
    }
    for resolved in &bundle.resolved_objects {
        push_unique_ref(&mut target_refs, resolved.ref_.clone());
        push_unique_actions(&mut actions, resolved.actions.iter().cloned());
    }
    for source in &bundle.source_coverage {
        push_unique_ref(
            &mut target_refs,
            SinexObjectRef::new(SinexObjectKind::SourceDriver, source.source_id.clone()),
        );
        push_unique_caveats(&mut caveats, source.caveats.iter().cloned());
        push_unique_actions(&mut actions, source.actions.iter().cloned());
    }
    for debt in &bundle.debt_rows {
        push_unique_refs(&mut target_refs, debt.refs.iter().cloned());
        if let Some(owner) = &debt.owner {
            push_unique_ref_opt(&mut target_refs, owner.operation_ref.clone());
        }
        push_unique_caveats(&mut caveats, debt.caveats.iter().cloned());
        push_unique_actions(&mut actions, debt.actions.iter().cloned());
    }
    for operation in &bundle.operations {
        push_unique_ref(
            &mut target_refs,
            SinexObjectRef::new(SinexObjectKind::Operation, operation.id.clone()),
        );
        push_unique_actions(&mut actions, operation.actions.iter().cloned());
    }
    for omission in &bundle.omitted_sections {
        push_unique_refs(
            &mut target_refs,
            omission
                .caveats
                .iter()
                .filter_map(|caveat| caveat.ref_.clone()),
        );
        push_unique_caveats(&mut caveats, omission.caveats.iter().cloned());
    }

    let disclosure_caveats = caveats
        .iter()
        .filter(|caveat| is_disclosure_caveat(caveat))
        .cloned()
        .collect();

    bundle.target_refs = target_refs;
    bundle.caveats = caveats;
    bundle.disclosure_caveats = disclosure_caveats;
    bundle.actions = actions;
}

pub(super) fn push_unique_ref(target: &mut Vec<SinexObjectRef>, ref_: SinexObjectRef) {
    if !target.contains(&ref_) {
        target.push(ref_);
    }
}

pub(super) fn push_unique_ref_opt(target: &mut Vec<SinexObjectRef>, ref_: Option<SinexObjectRef>) {
    if let Some(ref_) = ref_ {
        push_unique_ref(target, ref_);
    }
}

pub(super) fn push_unique_refs(
    target: &mut Vec<SinexObjectRef>,
    refs: impl IntoIterator<Item = SinexObjectRef>,
) {
    for ref_ in refs {
        push_unique_ref(target, ref_);
    }
}

pub(super) fn is_disclosure_caveat(caveat: &CaveatView) -> bool {
    let id = caveat.id.as_str();
    let message = caveat.message.as_str();
    [id, message].iter().any(|value| {
        value.contains("disclosure")
            || value.contains("privacy")
            || value.contains("redact")
            || value.contains("hidden")
    })
}

pub(super) fn push_unique_caveats(
    target: &mut Vec<CaveatView>,
    caveats: impl IntoIterator<Item = CaveatView>,
) {
    for caveat in caveats {
        if !target.contains(&caveat) {
            target.push(caveat);
        }
    }
}

pub(super) fn push_unique_actions(
    target: &mut Vec<ActionAvailability>,
    actions: impl IntoIterator<Item = ActionAvailability>,
) {
    for action in actions {
        if !target.contains(&action) {
            target.push(action);
        }
    }
}

pub(super) const EVIDENCE_BUNDLE_MAX_DIAGNOSTIC_EXCERPTS: usize = 8;
pub(super) const EVIDENCE_BUNDLE_MAX_DIAGNOSTIC_EXCERPT_CHARS: usize = 240;

pub(super) fn attach_bounded_diagnostic_excerpts(bundle: &mut EvidenceBundleView) {
    for source in &bundle.source_coverage {
        for caveat in &source.caveats {
            push_diagnostic_excerpt(
                &mut bundle.diagnostic_excerpts,
                "source_coverage",
                caveat.ref_.clone().or_else(|| {
                    Some(SinexObjectRef::new(
                        SinexObjectKind::SourceDriver,
                        source.source_id.clone(),
                    ))
                }),
                &caveat.message,
            );
        }
    }

    for debt in &bundle.debt_rows {
        for caveat in &debt.caveats {
            push_diagnostic_excerpt(
                &mut bundle.diagnostic_excerpts,
                "debt_rows",
                caveat.ref_.clone().or_else(|| debt.refs.first().cloned()),
                &caveat.message,
            );
        }
    }

    for omission in &bundle.omitted_sections {
        push_diagnostic_excerpt(
            &mut bundle.diagnostic_excerpts,
            "omitted_sections",
            omission
                .caveats
                .first()
                .and_then(|caveat| caveat.ref_.clone()),
            &omission.reason,
        );
    }
}

pub(super) fn push_diagnostic_excerpt(
    excerpts: &mut Vec<EvidenceBundleDiagnosticExcerptView>,
    section: impl Into<String>,
    source_ref: Option<SinexObjectRef>,
    message: &str,
) {
    if message.is_empty() || excerpts.len() >= EVIDENCE_BUNDLE_MAX_DIAGNOSTIC_EXCERPTS {
        return;
    }

    let (excerpt, truncated) =
        bounded_excerpt(message, EVIDENCE_BUNDLE_MAX_DIAGNOSTIC_EXCERPT_CHARS);
    excerpts.push(EvidenceBundleDiagnosticExcerptView {
        section: section.into(),
        source_ref,
        excerpt,
        max_chars: EVIDENCE_BUNDLE_MAX_DIAGNOSTIC_EXCERPT_CHARS,
        truncated,
    });
}

pub(super) fn bounded_excerpt(message: &str, max_chars: usize) -> (String, bool) {
    let mut chars = message.chars();
    let excerpt = chars.by_ref().take(max_chars).collect::<String>();
    let truncated = chars.next().is_some();
    (excerpt, truncated)
}

pub(super) async fn save_evidence_bundle_artifact(
    client: &GatewayClient,
    bundle: &EvidenceBundleView,
) -> Result<EvidenceBundleSavedArtifactView> {
    let bytes = serde_json::to_vec_pretty(bundle)?;
    let content = base64::engine::general_purpose::STANDARD.encode(bytes);
    let response = client
        .store_blob(StoreBlobRequest {
            content,
            filename: Some("evidence-bundle.json".to_string()),
            content_type: Some("application/vnd.sinex.evidence-bundle+json".to_string()),
            source: Some("sinexctl.ops.evidence.compile".to_string()),
        })
        .await?;

    Ok(EvidenceBundleSavedArtifactView {
        ref_: SinexObjectRef::new(SinexObjectKind::Artifact, response.content_key.clone()),
        content_key: response.content_key,
        content_type: "application/vnd.sinex.evidence-bundle+json".to_string(),
        size: response.size,
        blake3_hash: response.blake3_hash,
    })
}

pub(super) fn runtime_health_to_bundle_view(
    response: RuntimeHealthResponse,
    stale_after_secs: u64,
) -> EvidenceBundleRuntimeHealthView {
    EvidenceBundleRuntimeHealthView {
        stale_after_secs,
        active_count: response.active_count,
        inactive_count: response.inactive_count,
        unique_modules: response.unique_modules,
        active_run_count: response.active_run_count,
        oldest_heartbeat: response.oldest_heartbeat,
    }
}

pub(super) fn package_matches_source_seed(
    package: &SourcePackageCompletenessPackageView,
    source_id: &str,
) -> bool {
    package.package_id == source_id
        || package.display_namespace == source_id
        || package.modes.iter().any(|mode| {
            mode.mode_id == source_id
                || mode.subject.as_deref() == Some(source_id)
                || mode
                    .event_contract_refs
                    .iter()
                    .any(|value| value == source_id)
                || mode
                    .admission_policy_refs
                    .iter()
                    .any(|value| value == source_id)
                || mode
                    .coverage_debt_refs
                    .iter()
                    .any(|value| value == source_id)
                || mode.operation_refs.iter().any(|value| value == source_id)
        })
}
