use super::*;
use crate::fmt::render_finite_envelope;
use serde_json::json;
use sinex_primitives::domain::HostName;
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::ReadinessCaveatId;
use sinex_primitives::{Id, Uuid};
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn private_mode_table_summary_keeps_coarse_scope() -> xtask::sandbox::TestResult<()> {
    let state = RuntimePrivateModeState::enabled_by(
        "sinity",
        vec!["clipboard".to_string()],
        Timestamp::UNIX_EPOCH,
    );
    let summary = format_private_mode_state(&state);

    assert!(summary.contains("Private mode: enabled"));
    assert!(summary.contains("Actor: sinity"));
    assert!(summary.contains("Source classes: clipboard"));
    Ok(())
}

#[sinex_test]
async fn private_mode_status_envelope_caveats_enabled_state() -> xtask::sandbox::TestResult<()> {
    let state = RuntimePrivateModeState::enabled_by(
        "sinity",
        vec!["clipboard".to_string()],
        Timestamp::UNIX_EPOCH,
    );
    let envelope = private_mode_status_envelope(state);

    assert_eq!(envelope.source_surface, "sinexctl.privacy.private_mode.status");
    assert!(envelope.payload.enabled);
    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(
        envelope.caveats[0].id,
        ReadinessCaveatId::WindowPartial.as_str()
    );
    Ok(())
}

#[sinex_test]
async fn privacy_audit_summarizes_posture_without_source_identifier_leak()
-> xtask::sandbox::TestResult<()> {
    let report = build_privacy_audit_report(
        RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["desktop".to_string()],
            Timestamp::UNIX_EPOCH,
        ),
        &DlqListResponse {
            total_messages: 2,
            total_bytes: 128,
            first_seq: 1,
            last_seq: 2,
            pressure_level: sinex_primitives::RuntimePressureLevel::Warning,
            resource_pressure: sinex_primitives::rpc::dlq::DlqPressureSignal {
                pressure_level: sinex_primitives::RuntimePressureLevel::Warning,
                runtime_action: sinex_primitives::RuntimePressureAction::Inspect,
                pending_messages: 2,
                pending_bytes: 128,
                retry_batch_size: 10,
                recommended_action: "ops dlq peek".to_string(),
                reason: "inspect failures before running paced requeue or purge".to_string(),
            },
            pending_sequence_span: 2,
            recommended_action: "ops dlq peek".to_string(),
            action_reason: "inspect failures before running paced requeue or purge".to_string(),
        },
        &SourcesReadinessListResponse {
            sources: vec![SourceReadiness {
                binding_id: None,
                source_family: "desktop".to_string(),
                source_id: None,
                parser_id: None,
                source_identifier: "/home/sinity/private/window.log".to_string(),
                status: SourceReadinessStatus::Blocked,
                cost: sinex_primitives::rpc::sources::SourceReadinessCost::Unavailable,
                freshness_seconds: None,
                material_count: 1,
                parsed_event_count: None,
                last_success_at: None,
                caveats: vec![SourceCaveat {
                    code: "policy.raw_material_blocked".to_string(),
                    severity: CaveatSeverity::Blocking,
                    message: "blocked by private mode".to_string(),
                    evidence_ref: Some("/home/sinity/private/window.log".to_string()),
                }],
                evidence: json!({"raw_path": "/home/sinity/private/window.log"}),
            }],
        },
    );

    assert!(report.private_mode.enabled);
    assert!(report.dlq.has_backlog);
    assert_eq!(report.sources.blocked, 1);
    assert_eq!(report.sources.privacy_caveats, 1);
    assert_eq!(report.sources.blocking_caveats, 1);
    assert_eq!(report.findings.len(), 3);

    let table = format_privacy_audit_report(&report);
    assert!(table.contains("privacy.private_mode_enabled"));
    assert!(table.contains("privacy.dlq_backlog"));
    assert!(table.contains("policy.raw_material_blocked"));
    assert!(!table.contains("/home/sinity/private/window.log"));
    Ok(())
}

#[sinex_test]
async fn privacy_audit_envelope_carries_posture_caveats() -> xtask::sandbox::TestResult<()> {
    let report = PrivacyAuditReport {
        private_mode: PrivacyAuditPrivateMode {
            enabled: true,
            reason_class: "operator".to_string(),
            actor: "sinity".to_string(),
            started_at: Some("1970-01-01T00:00:00Z".to_string()),
            source_classes: vec!["desktop".to_string()],
            updated_by_operation_id: None,
        },
        dlq: PrivacyAuditDlq {
            total_messages: 1,
            total_bytes: 64,
            has_backlog: true,
        },
        sources: PrivacyAuditSources {
            total: 0,
            available: 0,
            blocked: 0,
            degraded_or_error: 0,
            privacy_caveats: 0,
            blocking_caveats: 0,
        },
        findings: Vec::new(),
    };
    let args = PrivacyAuditArgs {
        source_family: Some("desktop".to_string()),
        stale_after_seconds: Some(60),
    };
    let envelope = privacy_audit_envelope(report, &args);
    let caveat_ids: Vec<&str> = envelope.caveats.iter().map(|caveat| caveat.id.as_str()).collect();

    assert_eq!(envelope.source_surface, "sinexctl.privacy.audit");
    assert!(caveat_ids.contains(&ReadinessCaveatId::WindowPartial.as_str()));
    assert!(caveat_ids.contains(&ReadinessCaveatId::SourceAbsent.as_str()));
    assert!(caveat_ids.contains(&ReadinessCaveatId::CoverageUnmeasurable.as_str()));
    assert_eq!(envelope.query_echo.as_ref().unwrap()["source_family"], "desktop");
    Ok(())
}

#[sinex_test]
async fn privacy_export_renderers_omit_payload_and_snippet_material()
-> xtask::sandbox::TestResult<()> {
    let event = Event {
        id: Some(Id::from_uuid(Uuid::from_u128(1))),
        source: EventSource::from_static("terminal"),
        event_type: EventType::from_static("shell.command"),
        payload: json!({
            "command": "export TOKEN=secret",
            "cwd": "/home/sinity/private"
        }),
        ts_orig: Some(Timestamp::UNIX_EPOCH),
        host: HostName::from_static("sinnix-prime"),
        module_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::from_material(
            Id::<SourceMaterial>::from_uuid(Uuid::from_u128(2)),
            42,
            None,
            None,
        ),
        associated_blob_ids: Some(vec![Uuid::from_u128(3)]),
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
        anchor_payload_hash: None,
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
    };
    let report = build_privacy_export_report(
        EventQueryResult::Events {
            events: vec![QueryResultEvent {
                event,
                relevance_score: Some(0.8),
                snippet: Some("TOKEN=secret".to_string()),
            }],
            next_cursor: None,
            total_estimate: Some(1),
        },
        PrivacyExportScope {
            sources: vec!["terminal".to_string()],
            event_types: vec!["shell.command".to_string()],
            since: Some("24h".to_string()),
            until: None,
            text_search_used: true,
            limit: 100,
        },
    );

    assert_eq!(report.exported_events, 1);
    assert_eq!(report.disclosure_context, "export");
    assert_eq!(report.scope.sources, vec!["terminal".to_string()]);
    assert!(report.scope.text_search_used);

    for format in [
        OutputFormat::Table,
        OutputFormat::Json,
        OutputFormat::Ndjson,
        OutputFormat::Yaml,
    ] {
        let rendered = render_privacy_export_report(&report, format)?;
        assert!(
            rendered.contains("metadata_only_payloads_and_snippets_omitted"),
            "{format:?} should disclose metadata-only export policy"
        );
        assert!(
            !rendered.contains("TOKEN=secret"),
            "{format:?} leaked snippet or payload text"
        );
        assert!(
            !rendered.contains("/home/sinity/private"),
            "{format:?} leaked payload path material"
        );
        assert!(
            !rendered.contains("\"payload\""),
            "{format:?} should not render raw payload fields"
        );
        assert!(
            !rendered.contains("\"snippet\""),
            "{format:?} should not render raw snippet fields"
        );
    }

    let encoded = serde_json::to_string(&report)?;
    assert!(encoded.contains("\"disclosure_context\":\"export\""));
    assert!(encoded.contains("\"payload_redacted\":true"));
    assert!(encoded.contains("\"snippet_redacted\":true"));
    assert!(encoded.contains("\"associated_blob_count\":1"));
    Ok(())
}

#[sinex_test]
async fn privacy_export_envelope_caveats_empty_and_partial_results()
-> xtask::sandbox::TestResult<()> {
    let report = PrivacyExportReport {
        schema_version: 1,
        disclosure_context: "export",
        payload_policy: "metadata_only_payloads_and_snippets_omitted",
        scope: PrivacyExportScope {
            sources: vec!["terminal".to_string()],
            event_types: Vec::new(),
            since: Some("24h".to_string()),
            until: None,
            text_search_used: false,
            limit: 1,
        },
        exported_events: 0,
        total_estimate: Some(10),
        next_cursor: None,
        events: Vec::new(),
    };
    let args = PrivacyExportArgs {
        source: vec![EventSource::from_static("terminal")],
        event_type: Vec::new(),
        since: Some("24h".to_string()),
        until: None,
        query: None,
        limit: 1,
        output: None,
    };
    let envelope = privacy_export_envelope(report, &args);
    let caveat_ids: Vec<&str> = envelope.caveats.iter().map(|caveat| caveat.id.as_str()).collect();

    assert_eq!(envelope.source_surface, "sinexctl.privacy.export");
    assert!(caveat_ids.contains(&ReadinessCaveatId::CoverageUnmeasurable.as_str()));
    assert!(caveat_ids.contains(&ReadinessCaveatId::WindowPartial.as_str()));
    assert_eq!(envelope.query_echo.as_ref().unwrap()["source"][0], "terminal");
    Ok(())
}

#[sinex_test]
async fn privacy_export_requires_explicit_scope() -> xtask::sandbox::TestResult<()> {
    let args = PrivacyExportArgs {
        source: Vec::new(),
        event_type: Vec::new(),
        since: None,
        until: None,
        query: None,
        limit: 100,
        output: None,
    };

    let error = args
        .to_event_query()
        .expect_err("unscoped privacy export should be refused");
    assert!(
        format!("{error:#}").contains("requires an explicit scope"),
        "error should explain scope requirement: {error:#}"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_policy_table_summarizes_without_matcher_value_leak()
-> xtask::sandbox::TestResult<()> {
    let rule_id = Uuid::new_v4();
    let report = PrivacyPolicyListResponse {
        rules: vec![PrivacyPolicyRule {
            id: rule_id,
            name: "api-token".to_string(),
            description: "token fixture".to_string(),
            matcher_type: "regex".to_string(),
            matcher_value: "SECRET_TOKEN_SHOULD_NOT_RENDER".to_string(),
            matcher_config: json!({}),
            context_words: vec![],
            recognizer_backend_id: None,
            recognizer_kind: "local_pattern".to_string(),
            case_sensitive: false,
            action: "redact".to_string(),
            action_label: Some("<TOKEN>".to_string()),
            key_namespace: "default".to_string(),
            enabled: true,
        }],
        field_scopes: vec![],
        key_namespaces: vec![],
        recognizer_backends: vec![],
        dictionaries: vec![],
    };

    let table = format_privacy_policy_list(&report);

    assert!(table.contains("Privacy Policy"));
    assert!(table.contains("api-token"));
    assert!(table.contains("matcher=regex"));
    assert!(!table.contains("SECRET_TOKEN_SHOULD_NOT_RENDER"));
    Ok(())
}

#[sinex_test]
async fn privacy_policy_list_envelope_omits_matcher_value_and_caveats_empty()
-> xtask::sandbox::TestResult<()> {
    let rule_id = Uuid::new_v4();
    let report = PrivacyPolicyListResponse {
        rules: vec![PrivacyPolicyRule {
            id: rule_id,
            name: "api-token".to_string(),
            description: "token fixture".to_string(),
            matcher_type: "regex".to_string(),
            matcher_value: "SECRET_TOKEN_SHOULD_NOT_RENDER".to_string(),
            matcher_config: json!({"raw": "SECRET_TOKEN_SHOULD_NOT_RENDER"}),
            context_words: vec![],
            recognizer_backend_id: None,
            recognizer_kind: "local_pattern".to_string(),
            case_sensitive: false,
            action: "redact".to_string(),
            action_label: Some("<TOKEN>".to_string()),
            key_namespace: "default".to_string(),
            enabled: true,
        }],
        field_scopes: vec![],
        key_namespaces: vec![],
        recognizer_backends: vec![],
        dictionaries: vec![],
    };
    let envelope = privacy_policy_list_envelope(&report, false);
    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");

    assert!(rendered.contains("api-token"));
    assert!(!rendered.contains("SECRET_TOKEN_SHOULD_NOT_RENDER"));
    assert_eq!(envelope.payload.rule_count, 1);
    assert_eq!(envelope.payload.enabled_rule_count, 1);

    let empty = PrivacyPolicyListResponse {
        rules: Vec::new(),
        field_scopes: Vec::new(),
        key_namespaces: Vec::new(),
        recognizer_backends: Vec::new(),
        dictionaries: Vec::new(),
    };
    let empty_envelope = privacy_policy_list_envelope(&empty, false);
    assert_eq!(
        empty_envelope.caveats[0].id,
        ReadinessCaveatId::SourceAbsent.as_str()
    );
    Ok(())
}

#[sinex_test]
async fn privacy_policy_rule_add_parses_matcher_config_without_receipt_leak()
-> xtask::sandbox::TestResult<()> {
    let args = PolicyRuleAddArgs {
        name: "local-secret".to_string(),
        description: "fixture".to_string(),
        matcher_type: "regex".to_string(),
        matcher_value: "SECRET_TOKEN_SHOULD_NOT_RENDER".to_string(),
        matcher_config: r#"{"entity":"API_KEY","score_threshold":0.8}"#.to_string(),
        context_word: vec![],
        recognizer_backend_id: None,
        recognizer_kind: "local_pattern".to_string(),
        case_sensitive: false,
        action: "redact".to_string(),
        action_label: Some("<SECRET>".to_string()),
        key_namespace: "default".to_string(),
    };

    let request = args.to_request()?;
    assert_eq!(request.matcher_type, "regex");
    assert_eq!(request.matcher_config["entity"], "API_KEY");
    assert_eq!(request.matcher_config["score_threshold"], 0.8);

    let receipt = PrivacyPolicyMutationResponse {
        id: Uuid::new_v4(),
        kind: "rule".to_string(),
        name: request.name,
    };
    let table = format_privacy_policy_mutation(&receipt);
    assert!(table.contains("Privacy Policy Mutation"));
    assert!(table.contains("local-secret"));
    assert!(!table.contains("SECRET_TOKEN_SHOULD_NOT_RENDER"));
    Ok(())
}

#[sinex_test]
async fn privacy_policy_seed_builtin_formats_idempotent_counts()
-> xtask::sandbox::TestResult<()> {
    let args = PolicySeedBuiltinArgs { enabled: false };
    let response = PrivacyPolicySeedBuiltinResponse {
        inserted: 37,
        updated: 0,
        unchanged: 0,
        total: 37,
    };

    let table = format_privacy_policy_seed(&response);
    assert!(!args.enabled);
    assert!(table.contains("Privacy Policy Seed"));
    assert!(table.contains("Inserted: 37"));
    assert!(table.contains("Total: 37"));
    Ok(())
}

#[sinex_test]
async fn privacy_policy_backend_add_parses_config_and_enabled_state()
-> xtask::sandbox::TestResult<()> {
    let args = PolicyBackendAddArgs {
        name: "presidio-local".to_string(),
        kind: "presidio".to_string(),
        endpoint_url: Some("http://127.0.0.1:5001/analyze".to_string()),
        config: r#"{"language":"en","entities":["EMAIL_ADDRESS"]}"#.to_string(),
        disabled: true,
    };

    let request = args.to_request()?;
    assert_eq!(request.name, "presidio-local");
    assert_eq!(request.kind, "presidio");
    assert_eq!(
        request.endpoint_url.as_deref(),
        Some("http://127.0.0.1:5001/analyze")
    );
    assert_eq!(request.config["language"], "en");
    assert_eq!(request.config["entities"][0], "EMAIL_ADDRESS");
    assert!(!request.enabled);
    Ok(())
}

#[sinex_test]
async fn privacy_policy_dictionary_add_preserves_terms_and_tags()
-> xtask::sandbox::TestResult<()> {
    let args = PolicyDictionaryAddArgs {
        name: "local-projects".to_string(),
        description: "project deny-list".to_string(),
        language: Some("en".to_string()),
        source_kind: "user".to_string(),
        tag: vec!["project".to_string(), "local".to_string()],
        term: vec!["sinex".to_string(), "sinity".to_string()],
    };

    let request = args.to_request();
    assert_eq!(request.name, "local-projects");
    assert_eq!(request.description, "project deny-list");
    assert_eq!(request.language.as_deref(), Some("en"));
    assert_eq!(request.source_kind, "user");
    assert_eq!(
        request.tags,
        vec!["project".to_string(), "local".to_string()]
    );
    assert_eq!(
        request.terms,
        vec!["sinex".to_string(), "sinity".to_string()]
    );
    Ok(())
}

#[sinex_test]
async fn privacy_policy_scope_bind_preserves_field_hint_scope() -> xtask::sandbox::TestResult<()>
{
    let args = PolicyScopeBindArgs {
        rule_name: "window-title-sensitive".to_string(),
        event_source: Some("desktop".to_string()),
        event_type: Some("window.focus".to_string()),
        field_path: Some("title".to_string()),
        priority: 20,
    };

    let request = args.to_request();
    assert_eq!(request.rule_name, "window-title-sensitive");
    assert_eq!(request.event_source.as_deref(), Some("desktop"));
    assert_eq!(request.event_type.as_deref(), Some("window.focus"));
    assert_eq!(request.field_path.as_deref(), Some("title"));
    assert_eq!(request.priority, 20);
    Ok(())
}
