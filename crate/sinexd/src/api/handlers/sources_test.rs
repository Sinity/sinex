use super::*;
use serde_json::json;
use sinex_primitives::rpc::sources::SourceReadinessCost;
use sinex_primitives::temporal::Timestamp;
use xtask::sandbox::prelude::sinex_test;

fn continuity_report(source_family: &str) -> SourceContinuityReport {
    SourceContinuityReport {
        source_family: SourceFamily::new(source_family).expect("valid source family"),
        coverage_contract: CoverageContract::Continuous,
        is_declared: true,
        replayability: Replayability {
            raw_bytes_preserved: true,
            timing_quality: true,
            anchor_stability: true,
            parser_determinism: true,
            privacy_safe_replay: true,
            weak_points: Vec::new(),
        },
        seams: Vec::new(),
        gaps: Vec::new(),
        earliest_ts: None,
        latest_ts: None,
        material_count: 1,
        event_count: 1,
    }
}

fn readiness(source_family: &str, source_identifier: &str) -> SourceReadiness {
    SourceReadiness {
        binding_id: None,
        source_family: source_family.to_string(),
        source_id: None,
        parser_id: None,
        source_identifier: source_identifier.to_string(),
        status: SourceReadinessStatus::Available,
        cost: SourceReadinessCost::LocalFast,
        freshness_seconds: Some(1),
        material_count: 1,
        parsed_event_count: Some(1),
        last_success_at: Some("1970-01-01 00:00:00 UTC".to_string()),
        caveats: Vec::new(),
        evidence: serde_json::Value::Null,
    }
}

fn remediation_candidate(
    id: &str,
    status: MaterialStatus,
    staged_at: &str,
    event_count: i64,
    failure_reason: Option<&str>,
    recovery_reason: Option<&str>,
    decision: &str,
    severity: &str,
) -> SourceMaterialRemediationCandidate {
    SourceMaterialRemediationCandidate {
        material: SourceMaterialSummary {
            id: id.to_string(),
            material_kind: MaterialStorageKind::LocalCas,
            source_identifier: "browser.history".to_string(),
            status,
            timing_info_type: SourceMaterialTimingInfoType::Intrinsic,
            format: Some(SourceMaterialFormat::Json),
            contract_version: Some(1),
            staged_at: Some(staged_at.to_string()),
            staged_by: Some("test".to_string()),
            size_bytes: Some(128),
            event_count: Some(event_count),
            mime_type: Some("application/json".to_string()),
        },
        failure_reason: failure_reason.map(ToOwned::to_owned),
        recovery_reason: recovery_reason.map(ToOwned::to_owned),
        decision: decision.to_string(),
        severity: severity.to_string(),
        suggested_action: "inspect fixture material".to_string(),
    }
}

#[sinex_test]
async fn coverage_source_identifier_normalizer_collapses_material_suffixes()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        normalize_coverage_source_identifier(
            "sinex.self-observation.browser.history#material=019f231e-1fb7-7a38-bf78-98854bc450bc"
        ),
        "sinex.self-observation.browser.history"
    );
    assert_eq!(
        normalize_coverage_source_identifier("browser.history"),
        "browser.history"
    );
    assert_eq!(
        normalize_coverage_source_identifier("/realm/project/sinex/.agent/OPERATING-LOG.md"),
        "/realm/project/sinex/.agent/OPERATING-LOG.md"
    );
    Ok(())
}

#[sinex_test]
async fn remediation_candidates_sort_by_event_count_before_page()
-> xtask::sandbox::TestResult<()> {
    let mut candidates = vec![
        remediation_candidate(
            "low-recent",
            MaterialStatus::Failed,
            "2026-06-03T00:00:00Z",
            10,
            Some("slice_arrival_timeout"),
            None,
            "inspect_failed_eventful",
            "high",
        ),
        remediation_candidate(
            "high-old",
            MaterialStatus::Failed,
            "2026-06-01T00:00:00Z",
            100,
            Some("slice_arrival_timeout"),
            None,
            "inspect_failed_eventful",
            "high",
        ),
        remediation_candidate(
            "middle",
            MaterialStatus::RecoveredPartial,
            "2026-06-02T00:00:00Z",
            50,
            Some("slice_arrival_timeout"),
            Some("slice_arrival_timeout_with_admitted_events"),
            "review_partial_recovery",
            "medium",
        ),
    ];

    sort_remediation_candidates(&mut candidates, REMEDIATION_PLAN_SORT_EVENT_COUNT);

    let ids = candidates
        .into_iter()
        .map(|candidate| candidate.material.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["high-old", "middle", "low-recent"]);
    Ok(())
}

#[sinex_test]
async fn remediation_summary_counts_global_candidate_set() -> xtask::sandbox::TestResult<()> {
    let candidates = vec![
        remediation_candidate(
            "failed",
            MaterialStatus::Failed,
            "2026-06-01T00:00:00Z",
            12,
            Some("material_persist_failed"),
            None,
            "inspect_failed_eventful",
            "high",
        ),
        remediation_candidate(
            "partial",
            MaterialStatus::RecoveredPartial,
            "2026-06-02T00:00:00Z",
            5,
            Some("slice_arrival_timeout"),
            Some("slice_arrival_timeout_with_admitted_events"),
            "review_partial_recovery",
            "medium",
        ),
        remediation_candidate(
            "second-failed",
            MaterialStatus::Failed,
            "2026-06-03T00:00:00Z",
            7,
            Some("material_persist_failed"),
            None,
            "inspect_failed_eventful",
            "high",
        ),
    ];

    let summary = summarize_remediation_candidates(&candidates);

    assert_eq!(summary.total_candidates, 3);
    assert_eq!(summary.total_admitted_events, 24);
    assert_eq!(summary.by_status["failed"], 2);
    assert_eq!(summary.by_status["recovered_partial"], 1);
    assert_eq!(summary.by_decision["inspect_failed_eventful"], 2);
    assert_eq!(summary.by_severity["high"], 2);
    assert_eq!(summary.by_reason["material_persist_failed"], 2);
    assert_eq!(summary.by_reason["slice_arrival_timeout"], 1);
    Ok(())
}

#[sinex_test]
async fn stage_material_contract_records_package_mode_binding() -> xtask::sandbox::TestResult<()>
{
    let request = SourcesStageRequest {
        file_path: "/tmp/sinex-fixtures/screenshot/session.json".to_string(),
        format: Some(SourceMaterialFormat::Json),
        timing_info_type: Some(SourceMaterialTimingInfoType::Intrinsic),
        reason: Some("operator import".to_string()),
        tags: vec!["media".to_string()],
        binding_name: Some("source:media.screen-ocr.screenshot-ocr-staged".to_string()),
        with_bytes: true,
    };

    let contract = stage_material_contract(
        "/tmp/sinex-fixtures/screenshot/session.json",
        SourceMaterialFormat::Json,
        SourceMaterialTimingInfoType::Intrinsic,
        &request,
    );

    let origin = contract.origin.as_ref().expect("origin expected");
    assert_eq!(
        origin.source_uri.as_deref(),
        Some("/tmp/sinex-fixtures/screenshot/session.json")
    );
    assert_eq!(
        origin.binding_id.as_deref(),
        Some("source:media.screen-ocr.screenshot-ocr-staged")
    );
    assert_eq!(contract.format, SourceMaterialFormat::Json);
    assert_eq!(contract.timing, SourceMaterialTimingInfoType::Intrinsic);
    assert_eq!(
        contract
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.reason.as_deref()),
        Some("operator import")
    );
    Ok(())
}

#[sinex_test]
async fn private_mode_readiness_overlay_blocks_matching_family()
-> xtask::sandbox::TestResult<()> {
    let mut sources = vec![
        readiness("desktop", "/capture/desktop"),
        readiness("terminal", "/capture/terminal"),
    ];
    let mut state = RuntimePrivateModeState::enabled_by(
        "operator",
        vec!["desktop".to_string()],
        Timestamp::UNIX_EPOCH,
    );
    state.updated_by_operation_id = Some("op-1".to_string());

    apply_private_mode_state_readiness_overlay(&mut sources, &state);

    assert_eq!(sources[0].status, SourceReadinessStatus::Blocked);
    assert_eq!(
        sources[0].caveats[0].code,
        caveat_codes::POLICY_RAW_MATERIAL_BLOCKED
    );
    assert_eq!(sources[0].caveats[0].evidence_ref.as_deref(), Some("op-1"));
    assert_eq!(sources[1].status, SourceReadinessStatus::Available);
    assert!(sources[1].caveats.is_empty());
    Ok(())
}

#[sinex_test]
async fn private_mode_readiness_overlay_blocks_all_when_scope_empty()
-> xtask::sandbox::TestResult<()> {
    let mut sources = vec![
        readiness("desktop", "/capture/desktop"),
        readiness("terminal", "/capture/terminal"),
    ];
    let state =
        RuntimePrivateModeState::enabled_by("operator", Vec::new(), Timestamp::UNIX_EPOCH);

    apply_private_mode_state_readiness_overlay(&mut sources, &state);

    assert!(
        sources
            .iter()
            .all(|source| source.status == SourceReadinessStatus::Blocked)
    );
    Ok(())
}

#[sinex_test]
async fn private_mode_unavailable_readiness_overlay_blocks_fail_closed()
-> xtask::sandbox::TestResult<()> {
    let mut sources = vec![readiness("desktop", "/capture/desktop")];
    let error = SinexError::io("private-mode state unavailable");

    apply_private_mode_unavailable_readiness_overlay(&mut sources, &error);

    assert_eq!(sources[0].status, SourceReadinessStatus::Blocked);
    assert_eq!(
        sources[0].caveats[0].code,
        caveat_codes::POLICY_PRIVATE_MODE_STATE_UNAVAILABLE
    );
    assert_eq!(sources[0].caveats[0].severity, CaveatSeverity::Blocking);
    Ok(())
}

#[sinex_test]
async fn private_mode_continuity_overlay_adds_coarse_gap_for_matching_family()
-> xtask::sandbox::TestResult<()> {
    let now = Timestamp::UNIX_EPOCH;
    let mut reports = vec![continuity_report("desktop"), continuity_report("terminal")];
    let mut state =
        RuntimePrivateModeState::enabled_by("operator", vec!["desktop".to_string()], now);
    state.updated_by_operation_id = Some("op-private".to_string());

    apply_private_mode_state_continuity_overlay(&mut reports, &state, now);

    assert_eq!(reports[0].gaps.len(), 1);
    assert_eq!(reports[0].gaps[0].kind, GapKind::PrivateMode);
    assert_eq!(reports[0].gaps[0].from_ts, now);
    assert_eq!(reports[0].gaps[0].to_ts, now);
    assert_eq!(
        reports[0].gaps[0].attribution.as_deref(),
        Some("runtime private mode active (op-private)")
    );
    assert!(reports[1].gaps.is_empty());
    Ok(())
}

#[sinex_test]
async fn private_mode_continuity_get_synthesizes_no_material_report()
-> xtask::sandbox::TestResult<()> {
    let now = Timestamp::UNIX_EPOCH;
    let source_family = SourceFamily::new("clipboard")?;
    let state =
        RuntimePrivateModeState::enabled_by("operator", vec!["clipboard".to_string()], now);
    let mut report = None;

    apply_private_mode_state_continuity_get_overlay(&mut report, &source_family, &state, now);

    let report = report.expect("private-mode overlay should synthesize report");
    assert_eq!(report.source_family, source_family);
    assert_eq!(report.material_count, 0);
    assert_eq!(report.event_count, 0);
    assert_eq!(report.gaps.len(), 1);
    assert_eq!(report.gaps[0].kind, GapKind::PrivateMode);
    assert!(
        report
            .replayability
            .weak_points
            .iter()
            .any(|weak_point| weak_point.contains("private-mode caveat only"))
    );
    Ok(())
}

#[sinex_test]
async fn source_shape_drift_extraction_reads_checkpoint_user_state()
-> xtask::sandbox::TestResult<()> {
    let drifts = extract_checkpoint_drifts(
        "source.default.host-a",
        Some(&json!({
            "user_state": {
                "recent_input_drifts": [
                    {
                        "source_id": "browser.history",
                        "previous_hash": "old",
                        "current_hash": "new",
                        "format": "csv",
                        "added_keys": ["url"],
                        "removed_keys": ["visit_id"],
                        "required_input_keys": ["visit_id"],
                        "type_changes": [
                            ["title", "string", "null"],
                            {
                                "key": "visit_time",
                                "previous_type": "number",
                                "current_type": "string"
                            }
                        ],
                        "observed_at": "2026-05-21T10:00:00Z"
                    }
                ]
            }
        })),
    )?;

    assert_eq!(drifts.len(), 1);
    let drift = &drifts[0];
    assert_eq!(drift.checkpoint_key, "source.default.host-a");
    assert_eq!(drift.source_id.as_str(), "browser.history");
    assert_eq!(drift.consumer_group.as_deref(), Some("default"));
    assert_eq!(drift.consumer_name.as_deref(), Some("host-a"));
    assert_eq!(drift.added_keys, ["url"]);
    assert_eq!(drift.removed_keys, ["visit_id"]);
    assert_eq!(drift.required_input_keys, ["visit_id"]);
    assert_eq!(drift.type_changes.len(), 2);
    assert_eq!(drift.type_changes[0].key, "title");
    assert_eq!(drift.type_changes[0].previous_type, "string");
    assert_eq!(drift.type_changes[0].current_type, "null");
    assert_eq!(drift.type_changes[1].key, "visit_time");
    assert_eq!(drift.observed_at, "2026-05-21T10:00:00Z");
    Ok(())
}

#[sinex_test]
async fn source_shape_drift_extraction_ignores_checkpoints_without_drift()
-> xtask::sandbox::TestResult<()> {
    let drifts = extract_checkpoint_drifts(
        "source.default.host-a",
        Some(&json!({ "user_state": { "other": [] } })),
    )?;

    assert!(drifts.is_empty());
    Ok(())
}

#[sinex_test]
async fn source_shape_drift_readiness_overlay_adds_latest_degraded_caveats()
-> xtask::sandbox::TestResult<()> {
    let source_id = sinex_primitives::parser::SourceId::new("browser.history")?;
    let mut sources = vec![readiness("browser", "history.sqlite")];
    sources[0].source_id = Some(source_id.clone());

    let drifts = vec![
        SourceShapeDriftObservation {
            checkpoint_key: "source.default.host-a".to_string(),
            source_id: source_id.clone(),
            consumer_group: Some("default".to_string()),
            consumer_name: Some("host-a".to_string()),
            previous_hash: "old-1".to_string(),
            current_hash: "new-1".to_string(),
            format: "sqlite_schema".to_string(),
            added_keys: vec!["title".to_string()],
            removed_keys: Vec::new(),
            type_changes: Vec::new(),
            required_input_keys: Vec::new(),
            observed_at: "2026-05-21T09:00:00Z".to_string(),
        },
        SourceShapeDriftObservation {
            checkpoint_key: "source.default.host-a".to_string(),
            source_id,
            consumer_group: Some("default".to_string()),
            consumer_name: Some("host-a".to_string()),
            previous_hash: "old-2".to_string(),
            current_hash: "new-2".to_string(),
            format: "sqlite_schema".to_string(),
            added_keys: Vec::new(),
            removed_keys: vec!["visit_id".to_string()],
            type_changes: vec![SourceShapeTypeChange {
                key: "visit_time".to_string(),
                previous_type: "integer".to_string(),
                current_type: "text".to_string(),
            }],
            required_input_keys: Vec::new(),
            observed_at: "2026-05-21T10:00:00Z".to_string(),
        },
    ];

    apply_shape_drift_readiness_overlay(&mut sources, &drifts);

    assert_eq!(sources[0].status, SourceReadinessStatus::Partial);
    let codes = sources[0]
        .caveats
        .iter()
        .map(|caveat| caveat.code.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        codes,
        [
            caveat_codes::PARSER_FIELD_TYPE_CHANGED,
            caveat_codes::PARSER_REQUIRED_FIELD_MISSING
        ]
    );
    assert!(
        sources[0]
            .caveats
            .iter()
            .all(|caveat| caveat.severity == CaveatSeverity::Degraded)
    );
    assert!(
        sources[0]
            .caveats
            .iter()
            .all(|caveat| caveat.evidence_ref.as_deref() == Some("drift:new-2"))
    );
    Ok(())
}

#[sinex_test]
async fn source_shape_drift_readiness_overlay_keeps_additive_drift_available()
-> xtask::sandbox::TestResult<()> {
    let source_id = sinex_primitives::parser::SourceId::new("browser.history")?;
    let mut sources = vec![readiness("browser", "history.csv")];
    sources[0].source_id = Some(source_id.clone());

    let drifts = vec![SourceShapeDriftObservation {
        checkpoint_key: "source.default.host-a".to_string(),
        source_id,
        consumer_group: Some("default".to_string()),
        consumer_name: Some("host-a".to_string()),
        previous_hash: "old".to_string(),
        current_hash: "new".to_string(),
        format: "csv".to_string(),
        added_keys: vec!["title".to_string()],
        removed_keys: Vec::new(),
        type_changes: Vec::new(),
        required_input_keys: Vec::new(),
        observed_at: "2026-05-21T10:00:00Z".to_string(),
    }];

    apply_shape_drift_readiness_overlay(&mut sources, &drifts);

    assert_eq!(sources[0].status, SourceReadinessStatus::Available);
    assert_eq!(sources[0].caveats.len(), 1);
    assert_eq!(
        sources[0].caveats[0].code,
        caveat_codes::SOURCE_SHAPE_CHANGED
    );
    assert_eq!(sources[0].caveats[0].severity, CaveatSeverity::Info);
    Ok(())
}

#[sinex_test]
async fn source_shape_drift_readiness_overlay_matches_family_when_unit_unknown()
-> xtask::sandbox::TestResult<()> {
    let mut sources = vec![
        readiness("browser", "history.csv"),
        readiness("terminal", "history.txt"),
    ];

    let drifts = vec![SourceShapeDriftObservation {
        checkpoint_key: "source.default.host-a".to_string(),
        source_id: sinex_primitives::parser::SourceId::new("browser.history")?,
        consumer_group: Some("default".to_string()),
        consumer_name: Some("host-a".to_string()),
        previous_hash: "old".to_string(),
        current_hash: "new".to_string(),
        format: "csv".to_string(),
        added_keys: Vec::new(),
        removed_keys: vec!["visit_id".to_string()],
        type_changes: Vec::new(),
        required_input_keys: Vec::new(),
        observed_at: "2026-05-21T10:00:00Z".to_string(),
    }];

    apply_shape_drift_readiness_overlay(&mut sources, &drifts);

    assert_eq!(sources[0].status, SourceReadinessStatus::Partial);
    assert_eq!(
        sources[0].caveats[0].code,
        caveat_codes::PARSER_REQUIRED_FIELD_MISSING
    );
    assert_eq!(sources[1].status, SourceReadinessStatus::Available);
    assert!(sources[1].caveats.is_empty());
    Ok(())
}

#[sinex_test]
async fn source_shape_drift_readiness_overlay_blocks_required_input_removal()
-> xtask::sandbox::TestResult<()> {
    let source_id = sinex_primitives::parser::SourceId::new("browser.history")?;
    let mut sources = vec![readiness("browser", "history.sqlite")];
    sources[0].source_id = Some(source_id.clone());

    let drifts = vec![SourceShapeDriftObservation {
        checkpoint_key: "source.default.host-a".to_string(),
        source_id,
        consumer_group: Some("default".to_string()),
        consumer_name: Some("host-a".to_string()),
        previous_hash: "old".to_string(),
        current_hash: "new".to_string(),
        format: "sqlite_schema".to_string(),
        added_keys: Vec::new(),
        removed_keys: vec!["visit_id".to_string()],
        type_changes: Vec::new(),
        required_input_keys: vec!["visit_id".to_string()],
        observed_at: "2026-05-21T10:00:00Z".to_string(),
    }];

    apply_shape_drift_readiness_overlay(&mut sources, &drifts);

    assert_eq!(sources[0].status, SourceReadinessStatus::Partial);
    assert!(
        sources[0].caveats.iter().any(|caveat| {
            caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
                && caveat.severity == CaveatSeverity::Blocking
        }),
        "expected required input removal to surface as blocking: {:?}",
        sources[0].caveats
    );
    Ok(())
}

#[sinex_test]
async fn private_mode_explain_gap_overlay_uses_active_window() -> xtask::sandbox::TestResult<()>
{
    let now = Timestamp::UNIX_EPOCH;
    let source_family = SourceFamily::new("desktop")?;
    let state =
        RuntimePrivateModeState::enabled_by("operator", vec!["desktop".to_string()], now);
    let mut gap = None;

    if gap.is_none()
        && private_mode_applies_to_source_family(&source_family, &state)
        && private_mode_state_covers_at(&state, now)
    {
        gap = private_mode_gap_for_state(&state, now);
    }

    let gap = gap.expect("private-mode active window should explain absence");
    assert_eq!(gap.kind, GapKind::PrivateMode);
    assert_eq!(gap.from_ts, now);
    assert_eq!(gap.to_ts, now);
    Ok(())
}
