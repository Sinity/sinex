    #![allow(clippy::unwrap_used)]

    use super::*;
    use super::debt::*;
    use super::evidence::*;
    use sinex_primitives::domain::OperationStatus;
    use sinex_primitives::public_ref::ResolvedObjectView;
    use sinex_primitives::views::CoverageGapView;
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

    fn fixture_package(package_id: &str, mode_id: &str) -> SourcePackageCompletenessPackageView {
        SourcePackageCompletenessPackageView {
            package_id: package_id.to_string(),
            family: "terminal".to_string(),
            display_namespace: "terminal.activity".to_string(),
            modes: vec![
                sinex_primitives::rpc::sources::SourcePackageCompletenessModeView {
                    mode_id: mode_id.to_string(),
                    package_id: package_id.to_string(),
                    mode_state: "accepted".to_string(),
                    completeness: "complete".to_string(),
                    subject: Some("terminal.kitty-osc-live".to_string()),
                    acquisition_kind: "stream".to_string(),
                    operator_enablement: "enabled".to_string(),
                    missing: Vec::new(),
                    caveats: Vec::new(),
                    event_contract_refs: vec!["terminal.command.executed".to_string()],
                    admission_policy_refs: vec!["terminal.activity.admission".to_string()],
                    coverage_debt_refs: vec!["terminal.activity.coverage".to_string()],
                    operation_refs: vec!["terminal.activity.pause".to_string()],
                },
            ],
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
    async fn evidence_bundle_spec_records_seed_and_section_requests() -> xtask::TestResult<()> {
        let spec = build_evidence_bundle_spec(
            &["operation:op-1".to_string()],
            &["op-1".to_string()],
            &["terminal.kitty-osc-live".to_string()],
            true,
            true,
            Some(DebtProjectionTrigger::Replay),
            true,
            true,
            true,
        )?;

        assert_eq!(spec.schema_version, "sinex.evidence-bundle-spec/v2");
        assert_eq!(
            spec.target_context.as_deref(),
            Some("explicit operator-selected seeds")
        );
        assert!(spec.include_debt);
        assert!(spec.include_capture);
        assert_eq!(spec.projection_trigger.as_deref(), Some("replay"));
        assert!(spec.include_runtime_health);
        assert!(spec.include_package_completeness);
        assert!(spec.save_artifact);
        assert!(
            spec.seeds
                .iter()
                .any(|seed| seed.kind == EvidenceBundleSeedKind::PublicRef)
        );
        assert!(
            spec.seeds
                .iter()
                .any(|seed| seed.kind == EvidenceBundleSeedKind::Operation)
        );
        assert!(
            spec.seeds
                .iter()
                .any(|seed| seed.kind == EvidenceBundleSeedKind::SourceDriver)
        );
        assert!(
            spec.seeds
                .iter()
                .any(|seed| seed.kind == EvidenceBundleSeedKind::DebtQuery)
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
        attach_bounded_diagnostic_excerpts(&mut view);
        view.runtime_health = Some(EvidenceBundleRuntimeHealthView {
            stale_after_secs: 300,
            active_count: 1,
            inactive_count: 0,
            unique_modules: 1,
            active_run_count: 1,
            oldest_heartbeat: None,
        });
        view.package_completeness.push(fixture_package(
            "terminal.activity",
            "terminal.kitty-osc-live",
        ));
        view.saved_artifact = Some(EvidenceBundleSavedArtifactView {
            ref_: SinexObjectRef::new(SinexObjectKind::Artifact, "SINEXBLAKE3-test"),
            content_key: "SINEXBLAKE3-test".to_string(),
            content_type: "application/vnd.sinex.evidence-bundle+json".to_string(),
            size: 42,
            blake3_hash: "hash".to_string(),
        });

        let table = format_evidence_bundle_table(&view);

        assert!(table.contains("Evidence Bundle"));
        assert!(table.contains("sinex.evidence-bundle/v2"));
        assert!(table.contains("Seeds:            1"));
        assert!(table.contains("Target refs:      0"));
        assert!(table.contains("Included sections: 6"));
        assert!(table.contains("Evidence rows:"));
        assert!(table.contains("Runtime health:   included"));
        assert!(table.contains("Package rows:     1"));
        assert!(!view.diagnostic_excerpts.is_empty());
        assert!(view.diagnostic_excerpts.len() <= EVIDENCE_BUNDLE_MAX_DIAGNOSTIC_EXCERPTS);
        assert!(table.contains("Diagnostic excerpts:"));
        assert!(table.contains("Caveats:          0"));
        assert!(table.contains("Disclosure caveats: 0"));
        assert!(table.contains("Actions:          0"));
        assert!(table.contains("Diagnostics:"));
        assert!(table.contains("derivation"));
        assert!(table.contains("Saved artifact:   artifact:SINEXBLAKE3-test"));
        Ok(())
    }

    #[sinex_test]
    async fn evidence_bundle_diagnostic_excerpts_are_bounded() -> xtask::TestResult<()> {
        let mut view = EvidenceBundleView::new("sinexctl.ops.evidence.compile");
        view.debt_rows.push(DebtRowView {
            id: "debt:projection:test".to_string(),
            kind: DebtKind::Projection,
            stage: DebtStage::ProjectionStale,
            summary: "projection needs rebuild".to_string(),
            refs: vec![SinexObjectRef::new(SinexObjectKind::Projection, "p1")],
            owner: None,
            age_secs: None,
            freshness: None,
            caveats: vec![CaveatView {
                id: "projection.long_diagnostic".to_string(),
                message: "x".repeat(EVIDENCE_BUNDLE_MAX_DIAGNOSTIC_EXCERPT_CHARS + 16),
                ref_: Some(SinexObjectRef::new(SinexObjectKind::Projection, "p1")),
            }],
            actions: Vec::new(),
        });

        attach_bounded_diagnostic_excerpts(&mut view);

        assert_eq!(view.diagnostic_excerpts.len(), 1);
        let excerpt = &view.diagnostic_excerpts[0];
        assert_eq!(excerpt.section, "debt_rows");
        assert_eq!(excerpt.excerpt.chars().count(), excerpt.max_chars);
        assert!(excerpt.truncated);
        assert_eq!(
            excerpt
                .source_ref
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("projection:p1")
        );
        Ok(())
    }

    #[sinex_test]
    async fn evidence_bundle_preserves_underlying_targets_caveats_and_actions()
    -> xtask::TestResult<()> {
        let mut view = EvidenceBundleView::new("sinexctl.ops.evidence.compile");
        let source_ref =
            SinexObjectRef::new(SinexObjectKind::SourceDriver, "terminal.kitty-osc-live");
        let source_action = ActionAvailability::read(
            "source.status.inspect",
            "Inspect Source Status",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint("sinexctl sources status --format json");
        let debt_action = ActionAvailability::read(
            "debt.inspect",
            "Inspect Debt",
            ActionAvailabilityState::Enabled,
        )
        .with_command_hint("sinexctl ops debt list --format json");

        let mut source = fixture_source_status_coverage(
            SourceCoverageReadiness::MissingMaterial,
            SourceCoverageContinuity::Gapped,
            0,
            0,
        );
        source.source_id = "terminal.kitty-osc-live".to_string();
        source.caveats.push(CaveatView {
            id: "policy.disclosure_applied".to_string(),
            message: "terminal command text is hidden by view disclosure policy".to_string(),
            ref_: Some(source_ref.clone()),
        });
        source.actions.push(source_action.clone());
        view.source_coverage.push(source);
        view.debt_rows.push(DebtRowView {
            id: "debt:capture:terminal.kitty-osc-live".to_string(),
            kind: DebtKind::Capture,
            stage: DebtStage::Capturing,
            summary: "runtime bridge is unobserved".to_string(),
            refs: vec![source_ref.clone()],
            owner: None,
            age_secs: None,
            freshness: None,
            caveats: vec![CaveatView {
                id: "capture.runtime_unobserved".to_string(),
                message: "capture debt keeps the source caveat visible".to_string(),
                ref_: Some(source_ref.clone()),
            }],
            actions: vec![debt_action.clone()],
        });
        view.operations
            .push(operation_to_view(&fixture_operation("op-1", "replay")));

        attach_evidence_bundle_context(&mut view);

        assert!(
            view.target_refs.contains(&source_ref),
            "bundle target refs should identify source/debt target refs"
        );
        assert!(
            view.target_refs
                .iter()
                .any(|ref_| ref_.to_string() == "operation:op-1"),
            "bundle target refs should identify operation rows"
        );
        assert!(
            view.caveats
                .iter()
                .any(|caveat| caveat.id == "policy.disclosure_applied")
        );
        assert!(
            view.caveats
                .iter()
                .any(|caveat| caveat.id == "capture.runtime_unobserved")
        );
        assert_eq!(view.disclosure_caveats.len(), 1);
        assert_eq!(view.disclosure_caveats[0].id, "policy.disclosure_applied");
        assert!(view.actions.contains(&source_action));
        assert!(view.actions.contains(&debt_action));
        assert!(view.actions.iter().any(|action| action.id == "ops.show"));
        Ok(())
    }

    #[sinex_test]
    async fn evidence_bundle_view_has_stable_json_fields() -> xtask::TestResult<()> {
        let mut view = EvidenceBundleView::new("sinexctl.ops.evidence.compile");
        view.seeds
            .push(EvidenceBundleSeedView::operation("op-json-shape"));
        attach_evidence_bundle_context(&mut view);
        view.caveats.push(CaveatView {
            id: "evidence_bundle.test".to_string(),
            message: "test caveat".to_string(),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Operation,
                "op-json-shape",
            )),
        });
        view.actions.push(ActionAvailability::read(
            "ops.show",
            "Show",
            ActionAvailabilityState::Enabled,
        ));
        view.diagnostic_excerpts
            .push(EvidenceBundleDiagnosticExcerptView {
                section: "debt_rows".to_string(),
                source_ref: Some(SinexObjectRef::new(
                    SinexObjectKind::Operation,
                    "op-json-shape",
                )),
                excerpt: "bounded diagnostic".to_string(),
                max_chars: EVIDENCE_BUNDLE_MAX_DIAGNOSTIC_EXCERPT_CHARS,
                truncated: false,
            });

        let envelope = ViewEnvelope::new("sinexctl.ops.evidence.compile", view);
        let json = serde_json::to_value(&envelope)?;

        assert_eq!(json["source_surface"], "sinexctl.ops.evidence.compile");
        assert_eq!(
            json["payload"]["schema_version"],
            "sinex.evidence-bundle/v2"
        );
        assert_eq!(json["payload"]["seeds"][0]["kind"], "operation");
        assert_eq!(json["payload"]["target_refs"][0]["kind"], "operation");
        assert_eq!(json["payload"]["target_refs"][0]["id"], "op-json-shape");
        assert_eq!(json["payload"]["caveats"][0]["id"], "evidence_bundle.test");
        assert_eq!(json["payload"]["actions"][0]["id"], "ops.show");
        assert_eq!(
            json["payload"]["diagnostic_excerpts"][0]["source_ref"]["kind"],
            "operation"
        );
        assert_eq!(
            json["payload"]["diagnostic_excerpts"][0]["source_ref"]["id"],
            "op-json-shape"
        );
        Ok(())
    }

    #[sinex_test]
    async fn evidence_bundle_omissions_carry_target_caveats_and_diagnostics()
    -> xtask::TestResult<()> {
        let mut view = EvidenceBundleView::new("sinexctl.ops.evidence.compile");
        view.omitted_sections.push(omitted_evidence_section(
            "source_coverage:terminal.unknown-live",
            "source-driver seed was requested but no matching source coverage row exists",
            Some(SinexObjectRef::new(
                SinexObjectKind::SourceDriver,
                "terminal.unknown-live",
            )),
        ));

        attach_evidence_bundle_context(&mut view);
        attach_bounded_diagnostic_excerpts(&mut view);

        let omission = view
            .omitted_sections
            .first()
            .ok_or_else(|| color_eyre::eyre::eyre!("omission expected"))?;
        let caveat = omission
            .caveats
            .first()
            .ok_or_else(|| color_eyre::eyre::eyre!("omission caveat expected"))?;
        assert_eq!(caveat.id, "evidence_bundle.section_unavailable");
        assert_eq!(
            caveat.ref_.as_ref().map(ToString::to_string).as_deref(),
            Some("source-driver:terminal.unknown-live")
        );
        assert!(view.caveats.contains(caveat));
        assert!(view.diagnostic_excerpts.iter().any(|excerpt| {
            excerpt.section == "omitted_sections"
                && excerpt
                    .source_ref
                    .as_ref()
                    .map(ToString::to_string)
                    .as_deref()
                    == Some("source-driver:terminal.unknown-live")
        }));
        Ok(())
    }

    #[sinex_test]
    async fn evidence_bundle_command_is_registered_as_finite_view() -> xtask::TestResult<()> {
        let registry = crate::model::format_registry::build();
        let capability = registry
            .get("ops evidence compile")
            .expect("ops evidence compile must have a format registry entry");

        assert!(capability.supports(OutputFormat::Table));
        assert!(capability.supports(OutputFormat::Json));
        assert!(capability.supports(OutputFormat::Yaml));
        assert!(!capability.supports(OutputFormat::Ndjson));
        assert!(!capability.streaming);

        let catalog = crate::model::format_registry::command_catalog();
        let entry = catalog
            .iter()
            .find(|entry| entry.path == "ops evidence compile")
            .expect("ops evidence compile must have a command catalog entry");
        for method in [
            "ops.get",
            "dlq.list",
            "runtime.health",
            "sources.package_completeness",
            "sources.status.view",
        ] {
            assert!(
                entry.backing_rpc_methods.contains(&method),
                "ops evidence compile should advertise backing RPC `{method}`"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn evidence_bundle_package_matching_accepts_package_mode_and_subject()
    -> xtask::TestResult<()> {
        let package = fixture_package("terminal.activity", "terminal.kitty-osc-live");

        assert!(package_matches_source_seed(&package, "terminal.activity"));
        assert!(package_matches_source_seed(
            &package,
            "terminal.kitty-osc-live"
        ));
        assert!(package_matches_source_seed(
            &package,
            "terminal.command.executed"
        ));
        assert!(!package_matches_source_seed(&package, "browser.web"));
        Ok(())
    }

    #[sinex_test]
    async fn evidence_bundle_compiler_assembles_required_seed_classes_from_read_surfaces()
    -> xtask::TestResult<()> {
        let spec = build_evidence_bundle_spec(
            &["operation:op-1".to_string()],
            &["op-1".to_string()],
            &["terminal.kitty-osc-live".to_string()],
            true,
            true,
            Some(DebtProjectionTrigger::Replay),
            true,
            true,
            false,
        )?;

        let mut source = fixture_source_status_coverage(
            SourceCoverageReadiness::MissingMaterial,
            SourceCoverageContinuity::Gapped,
            0,
            0,
        );
        source.caveats.push(CaveatView {
            id: "policy.disclosure_applied".to_string(),
            message: "terminal command text is hidden by view disclosure policy".to_string(),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::SourceDriver,
                "terminal.kitty-osc-live",
            )),
        });

        let rows = EvidenceBundleReadSurfaceRows {
            resolved_objects: vec![ResolvedObjectView::resolved(
                SinexObjectRef::new(SinexObjectKind::Operation, "op-1"),
                "sinexctl.ops.get",
                serde_json::json!({"id": "op-1"}),
            )],
            operations: vec![fixture_operation("op-1", "replay")],
            source_coverage: Some(SourceCoverageListView::new(vec![source])),
            runtime_health: Some(RuntimeHealthResponse {
                active_count: 1,
                inactive_count: 0,
                unique_modules: 1,
                active_run_count: 1,
                oldest_heartbeat: None,
            }),
            package_completeness: Some(vec![fixture_package(
                "terminal.activity",
                "terminal.kitty-osc-live",
            )]),
            dlq: Some(fixture_dlq(12)),
        };

        let bundle = compile_evidence_bundle_from_rows(&spec, rows)?;

        assert!(
            bundle
                .seeds
                .iter()
                .any(|seed| seed.kind == EvidenceBundleSeedKind::PublicRef)
        );
        assert!(
            bundle
                .seeds
                .iter()
                .any(|seed| seed.kind == EvidenceBundleSeedKind::Operation)
        );
        assert!(
            bundle
                .seeds
                .iter()
                .any(|seed| seed.kind == EvidenceBundleSeedKind::SourceDriver)
        );
        assert!(
            bundle
                .seeds
                .iter()
                .any(|seed| seed.kind == EvidenceBundleSeedKind::DebtQuery)
        );
        assert_eq!(bundle.resolved_objects.len(), 1);
        assert_eq!(bundle.operations.len(), 1);
        assert_eq!(bundle.source_coverage.len(), 1);
        assert!(bundle.runtime_health.is_some());
        assert_eq!(bundle.package_completeness.len(), 1);
        assert!(
            bundle
                .debt_rows
                .iter()
                .any(|row| row.kind == DebtKind::Admission)
        );
        assert!(
            bundle
                .debt_rows
                .iter()
                .any(|row| row.kind == DebtKind::Capture)
        );
        assert!(
            bundle
                .debt_rows
                .iter()
                .any(|row| row.kind == DebtKind::Projection)
        );
        assert!(
            bundle
                .target_refs
                .iter()
                .any(|ref_| ref_.to_string() == "operation:op-1")
        );
        assert!(
            bundle
                .target_refs
                .iter()
                .any(|ref_| ref_.to_string() == "source-driver:terminal.kitty-osc-live")
        );
        assert!(
            bundle
                .disclosure_caveats
                .iter()
                .any(|caveat| caveat.id == "policy.disclosure_applied")
        );
        assert!(
            bundle
                .diagnostic_excerpts
                .iter()
                .any(|excerpt| excerpt.section == "source_coverage")
        );
        assert!(bundle.omitted_sections.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn evidence_bundle_compiler_reports_unavailable_requested_sections()
    -> xtask::TestResult<()> {
        let spec = build_evidence_bundle_spec(
            &["operation:missing-op".to_string()],
            &["missing-op".to_string()],
            &["terminal.unknown-live".to_string()],
            true,
            true,
            None,
            true,
            true,
            false,
        )?;

        let bundle =
            compile_evidence_bundle_from_rows(&spec, EvidenceBundleReadSurfaceRows::default())?;

        for section in [
            "resolved_ref:operation:missing-op",
            "operation:missing-op",
            "source_coverage:terminal.unknown-live",
            "runtime_health",
            "package_completeness",
            "debt_rows:dlq",
            "debt_rows:capture",
            "evidence_rows",
        ] {
            assert!(
                bundle
                    .omitted_sections
                    .iter()
                    .any(|omission| omission.section == section),
                "bundle should report omitted section `{section}`"
            );
        }
        assert!(
            bundle
                .caveats
                .iter()
                .all(|caveat| caveat.id == "evidence_bundle.section_unavailable")
        );
        assert!(
            bundle
                .diagnostic_excerpts
                .iter()
                .any(|excerpt| excerpt.section == "omitted_sections")
        );
        assert!(
            bundle
                .target_refs
                .iter()
                .any(|ref_| ref_.to_string() == "operation:missing-op")
        );
        assert!(
            bundle
                .target_refs
                .iter()
                .any(|ref_| ref_.to_string() == "source-driver:terminal.unknown-live")
        );
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
            sinex_primitives::RuntimePressureLevel::Critical
        } else if total_messages > 0 {
            sinex_primitives::RuntimePressureLevel::Warning
        } else {
            sinex_primitives::RuntimePressureLevel::Nominal
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
            pressure_level,
            resource_pressure: sinex_primitives::rpc::dlq::DlqPressureSignal {
                pressure_level,
                runtime_action: if total_messages > 10 {
                    sinex_primitives::RuntimePressureAction::Throttle
                } else if total_messages > 0 {
                    sinex_primitives::RuntimePressureAction::Inspect
                } else {
                    sinex_primitives::RuntimePressureAction::Admit
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
            material_kind: sinex_primitives::MaterialStorageKind::Annex,
            earliest_ts: None,
            latest_ts: None,
            event_count,
            material_count,
            completed_material_count: material_count,
            failed_material_count: Some(0),
            recovered_partial_material_count: Some(0),
            sensing_material_count: Some(0),
            cancelled_material_count: Some(0),
            total_bytes: Some(0),
        }
    }

    fn fixture_source_status_coverage(
        readiness: SourceCoverageReadiness,
        continuity: SourceCoverageContinuity,
        material_count: i64,
        event_count: i64,
    ) -> SourceCoverageView {
        SourceCoverageView {
            source_id: "terminal.kitty-osc-live".to_string(),
            namespace: "terminal".to_string(),
            event_types: vec!["shell.kitty/command.executed".to_string()],
            readiness,
            continuity,
            last_material_at: None,
            last_event_at: None,
            material_count,
            event_count,
            binding_count: 1,
            accepted_binding_count: 1,
            proposed_binding_count: 0,
            gaps: Vec::new(),
            caveats: Vec::new(),
            privacy: sinex_primitives::views::SourcePrivacyPosture {
                tier: "sensitive".to_string(),
                context: "command".to_string(),
                proposed: false,
            },
            resource_budget: None,
            modes: Vec::new(),
            actions: Vec::new(),
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
    async fn debt_rows_from_source_status_reports_unobserved_runtime_bridge()
    -> xtask::TestResult<()> {
        let mut source = fixture_source_status_coverage(
            SourceCoverageReadiness::MissingMaterial,
            SourceCoverageContinuity::Gapped,
            0,
            0,
        );
        source
            .caveats
            .push(CaveatView {
                id: "source.runtime_bridge.unobserved".to_string(),
                message: "runtime bridge `kitty_osc` is declared, but no material or admitted events have been observed for this source".to_string(),
                ref_: Some(SinexObjectRef::new(
                    SinexObjectKind::SourceDriver,
                    "terminal.kitty-osc-live",
                )),
            });
        source.actions.push(
            ActionAvailability {
                id: "terminal.activity.reconnect".to_string(),
                label: "Reconnect Bridge".to_string(),
                state: ActionAvailabilityState::Enabled,
                reason: Some(
                    "package declares `terminal.activity.reconnect` for source `terminal.kitty-osc-live`"
                        .to_string(),
                ),
                command_hint: Some("sinexctl runtime resume terminal-source".to_string()),
                rpc_method: Some("runtime.resume".to_string()),
                side_effect: ActionSideEffect::Admin,
                requires_confirmation: true,
                dry_run_available: false,
                audit_output_ref: None,
            },
        );

        let rows = debt_rows_from_source_status_coverage(&[source]);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            row.id,
            "debt:capture:terminal.kitty-osc-live:runtime-bridge-unobserved"
        );
        assert_eq!(row.kind, DebtKind::Capture);
        assert_eq!(row.stage, DebtStage::Capturing);
        assert!(
            row.summary
                .contains("runtime bridge source `terminal.kitty-osc-live`"),
            "capture debt should name the live package mode"
        );
        assert!(
            row.caveats
                .iter()
                .any(|caveat| caveat.id == "source.runtime_bridge.unobserved"),
            "status caveats must carry into the debt row"
        );
        assert!(
            row.refs.iter().any(|ref_| {
                ref_.kind == SinexObjectKind::SourceDriver && ref_.id == "terminal.kitty-osc-live"
            }),
            "debt row should remain addressable by source-driver ref"
        );
        assert!(
            row.actions.iter().any(|action| {
                action.id == "source.status.inspect"
                    && action.command_hint.as_deref()
                        == Some("sinexctl sources status --format json")
            }),
            "debt row should point operators back to the status surface"
        );
        let reconnect = row
            .actions
            .iter()
            .find(|action| action.id == "terminal.activity.reconnect")
            .ok_or_else(|| color_eyre::eyre::eyre!("reconnect action expected"))?;
        assert_eq!(reconnect.state, ActionAvailabilityState::Enabled);
        assert_eq!(reconnect.side_effect, ActionSideEffect::Admin);
        assert_eq!(reconnect.rpc_method.as_deref(), Some("runtime.resume"));
        assert_eq!(
            reconnect.command_hint.as_deref(),
            Some("sinexctl runtime resume terminal-source")
        );
        Ok(())
    }

    #[sinex_test]
    async fn debt_rows_from_source_status_carry_media_package_actions() -> xtask::TestResult<()> {
        let mut source = fixture_source_status_coverage(
            SourceCoverageReadiness::MissingMaterial,
            SourceCoverageContinuity::Gapped,
            0,
            0,
        );
        source.source_id = "media.audio-transcript".to_string();
        source.namespace = "media".to_string();
        source.gaps.push(CoverageGapView {
            kind: "missing_material".to_string(),
            message: "no source material is directly registered under this source id".to_string(),
        });
        source.actions.extend([
            ActionAvailability {
                id: "media.audio-transcript.import-transcript".to_string(),
                label: "Import Transcript".to_string(),
                state: ActionAvailabilityState::Enabled,
                reason: Some(
                    "package declares `media.audio-transcript.import-transcript` for source `media.audio-transcript`"
                        .to_string(),
                ),
                command_hint: Some("sinexctl sources stage <path> --format json".to_string()),
                rpc_method: Some("sources.stage".to_string()),
                side_effect: ActionSideEffect::Write,
                requires_confirmation: false,
                dry_run_available: false,
                audit_output_ref: None,
            },
            ActionAvailability {
                id: "media.audio-transcript.run-model".to_string(),
                label: "Run Local Model".to_string(),
                state: ActionAvailabilityState::Unavailable,
                reason: Some(
                    "package declares `media.audio-transcript.run-model` for source `media.audio-transcript`, but no operator actuator command is wired yet"
                        .to_string(),
                ),
                command_hint: None,
                rpc_method: None,
                side_effect: ActionSideEffect::Admin,
                requires_confirmation: true,
                dry_run_available: false,
                audit_output_ref: None,
            },
        ]);

        let rows = debt_rows_from_source_status_coverage(&[source]);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            row.owner
                .as_ref()
                .and_then(|owner| owner.package_ref.as_deref()),
            Some("media.audio-transcript")
        );
        let import = row
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.import-transcript")
            .ok_or_else(|| color_eyre::eyre::eyre!("media import action expected"))?;
        assert_eq!(import.state, ActionAvailabilityState::Enabled);
        assert_eq!(
            import.command_hint.as_deref(),
            Some("sinexctl sources stage <path> --format json")
        );
        assert_eq!(import.rpc_method.as_deref(), Some("sources.stage"));

        let run_model = row
            .actions
            .iter()
            .find(|action| action.id == "media.audio-transcript.run-model")
            .ok_or_else(|| color_eyre::eyre::eyre!("media run-model action expected"))?;
        assert_eq!(run_model.state, ActionAvailabilityState::Unavailable);
        assert_eq!(run_model.side_effect, ActionSideEffect::Admin);
        assert!(run_model.requires_confirmation);
        Ok(())
    }

    #[sinex_test]
    async fn debt_rows_from_source_status_omits_ready_active_sources() -> xtask::TestResult<()> {
        let source = fixture_source_status_coverage(
            SourceCoverageReadiness::Ready,
            SourceCoverageContinuity::Active,
            1,
            1,
        );

        assert!(debt_rows_from_source_status_coverage(&[source]).is_empty());
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
        assert_eq!(rebuild.state, ActionAvailabilityState::Enabled);
        assert!(rebuild.requires_confirmation);
        assert!(rebuild.dry_run_available);
        assert_eq!(rebuild.rpc_method.as_deref(), Some("ops.start"));
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
        let rebuild = row
            .actions
            .iter()
            .find(|action| action.id == "projection.rebuild")
            .expect("pending replay invalidation should be drainable through rebuild operation");
        assert_eq!(rebuild.state, ActionAvailabilityState::Enabled);
        assert_eq!(rebuild.side_effect, ActionSideEffect::Write);
        assert!(rebuild.requires_confirmation);
        assert!(rebuild.command_hint.as_deref().is_some_and(|hint| {
            hint.contains("projection-rebuild") && hint.contains("replay_operation_id")
        }));
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
