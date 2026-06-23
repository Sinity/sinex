#![allow(clippy::unwrap_used)]

use super::*;
use crate::events::builder::{OperationMarker, Provenance};
use crate::events::{Event, SourceMaterial};
use crate::ids::Id;
use crate::non_empty::NonEmptyVec;
use crate::query::QueryResultEvent;
use crate::rpc::dlq::{DlqListResponse, DlqPressureSignal};
use crate::rpc::replay::{ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState};
use crate::temporal::Timestamp;
use crate::{EventSource, EventType, HostName, JsonValue};
use serde_json::json;
use std::collections::HashMap;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn event_card_preserves_refs_actions_and_payload_preview() -> xtask::TestResult<()> {
    let event_id = Id::<Event<JsonValue>>::new();
    let material_id = Id::<SourceMaterial>::new();
    let result = QueryResultEvent {
        event: Event {
            id: Some(event_id),
            source: EventSource::new("shell.atuin")?,
            event_type: EventType::new("command.executed")?,
            payload: json!({
                "command": "xtask test -p sinex-primitives",
                "cwd": "/realm/project/sinex",
                "extra": [1, 2, 3, 4, 5, 6],
            }),
            ts_orig: Some(Timestamp::now()),
            ts_quality: None,
            host: HostName::new("sinnix-prime")?,
            module_run_id: None,
            payload_schema_id: None,
            provenance: Provenance::Material {
                id: material_id,
                anchor_byte: 42,
                offset_start: None,
                offset_end: None,
                offset_kind: crate::OffsetKind::Byte,
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            automaton_model: None,
            anchor_payload_hash: None,
        },
        relevance_score: Some(0.9),
        snippet: Some("ran a focused test".to_string()),
    };

    let card = EventCardView::from_query_event(&result);

    assert_eq!(card.ref_.kind, SinexObjectKind::Event);
    assert_eq!(card.ref_.id, event_id.to_string());
    assert_eq!(card.source.family, "shell");
    assert_eq!(card.origin_kind, EventOriginKind::Material);
    assert_eq!(card.summary, "ran a focused test");
    assert_eq!(card.material_refs.len(), 2);
    assert!(card.trace_refs.is_empty());
    assert_eq!(card.trace_links.len(), 2);
    assert_eq!(
        card.trace_links[0].relation,
        EventTraceRelation::SourceMaterial
    );
    assert_eq!(
        card.trace_links[1].relation,
        EventTraceRelation::MaterialAnchor
    );
    assert!(card.actions.iter().any(
        |action| action.id == "event.trace" && action.state == ActionAvailabilityState::Enabled
    ));
    assert!(
        card.actions
            .iter()
            .any(|action| action.id == "event.inspect"
                && action.state == ActionAvailabilityState::Target
                && action.reason.is_some())
    );
    assert!(card.payload_preview.is_some());
    Ok(())
}

#[sinex_test]
async fn operation_control_card_replay_execute_keeps_dangerous_action_reason()
-> xtask::TestResult<()> {
    let operation = ReplayOperation {
        operation_id: "op-fixture".to_string(),
        state: ReplayState::Approved,
        scope: ReplayScope {
            source_name: "fixture.replay".to_string(),
            time_window: None,
            material_filter: None,
            filters: HashMap::new(),
            source_id: Some("source-fixture".to_string()),
            source_material_id: Some("material-fixture".to_string()),
            parser_id: Some("parser-fixture".to_string()),
            parser_version: None,
        },
        preview_summary: None,
        checkpoint: ReplayCheckpoint {
            processed_events: 42,
            total_events: 100,
            last_event_id: None,
            batch_number: 3,
            savepoint_id: None,
            updated_at: "2026-06-19T00:00:00Z".to_string(),
        },
        actor: "operator.local".to_string(),
        created_at: "2026-06-19T00:00:00Z".to_string(),
        approved_by: Some("operator.local".to_string()),
        approved_at: Some("2026-06-19T00:00:01Z".to_string()),
        executor_module: None,
        started_at: None,
        finished_at: None,
        outcome: None,
        error_details: None,
    };

    let card = OperationControlCardView::from_replay_operation(&operation);
    let execute = card
        .actions
        .iter()
        .find(|action| action.id == "replay.execute")
        .expect("approved replay exposes execute action");

    assert_eq!(card.phase, "approved");
    assert_eq!(execute.state, ActionAvailabilityState::Dangerous);
    assert_eq!(
        execute.command_hint.as_deref(),
        Some("sinexctl ops replay execute op-fixture")
    );
    assert!(
        execute
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("mutates admitted events"))
    );
    assert!(
        card.caveats
            .iter()
            .any(|caveat| caveat.contains("staged-source replay"))
    );
    Ok(())
}

#[sinex_test]
async fn operation_control_card_empty_dlq_disables_mutating_actions_with_reason()
-> xtask::TestResult<()> {
    let card = OperationControlCardView::from_dlq_status(&DlqListResponse {
        total_messages: 0,
        total_bytes: 0,
        first_seq: 0,
        last_seq: 0,
        pressure_level: crate::RuntimePressureLevel::Nominal,
        resource_pressure: DlqPressureSignal {
            pressure_level: crate::RuntimePressureLevel::Nominal,
            runtime_action: crate::RuntimePressureAction::None,
            pending_messages: 0,
            pending_bytes: 0,
            retry_batch_size: 10,
            recommended_action: "none".to_string(),
            reason: "raw-ingest DLQ is empty".to_string(),
        },
        pending_sequence_span: 0,
        recommended_action: "none".to_string(),
        action_reason: "raw-ingest DLQ is empty".to_string(),
    });

    let requeue = card
        .actions
        .iter()
        .find(|action| action.id == "dlq.requeue")
        .expect("DLQ card exposes requeue action");
    let purge = card
        .actions
        .iter()
        .find(|action| action.id == "dlq.purge")
        .expect("DLQ card exposes purge action");

    assert_eq!(card.phase, "clear");
    assert_eq!(requeue.state, ActionAvailabilityState::Disabled);
    assert_eq!(purge.state, ActionAvailabilityState::Disabled);
    assert_eq!(requeue.reason.as_deref(), Some("DLQ is empty"));
    assert_eq!(purge.reason.as_deref(), Some("DLQ is empty"));
    Ok(())
}

#[sinex_test]
async fn event_card_splits_origin_kind_from_trace_links() -> xtask::TestResult<()> {
    let source_event_id = Id::<Event<JsonValue>>::new();
    let operation_id = Id::<OperationMarker>::new();
    let result = QueryResultEvent {
        event: Event {
            id: Some(Id::<Event<JsonValue>>::new()),
            source: EventSource::new("projection.context")?,
            event_type: EventType::new("context.updated")?,
            payload: json!({ "summary": "projection updated" }),
            ts_orig: None,
            ts_quality: None,
            host: HostName::new("sinnix-prime")?,
            module_run_id: None,
            payload_schema_id: None,
            provenance: Provenance::Derived {
                source_event_ids: NonEmptyVec::single(source_event_id),
                operation_id: Some(operation_id),
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            automaton_model: None,
            anchor_payload_hash: None,
        },
        relevance_score: None,
        snippet: None,
    };

    let card = EventCardView::from_query_event(&result);

    assert_eq!(card.origin_kind, EventOriginKind::Derived);
    assert_eq!(card.trace_refs.len(), 1);
    assert_eq!(card.trace_refs[0].id, source_event_id.to_string());
    assert_eq!(card.trace_links.len(), 2);
    assert_eq!(
        card.trace_links[0].relation,
        EventTraceRelation::SourceEvent
    );
    assert_eq!(card.trace_links[0].target.id, source_event_id.to_string());
    assert_eq!(card.trace_links[1].relation, EventTraceRelation::Operation);
    assert_eq!(card.trace_links[1].target.id, operation_id.to_string());

    let roundtrip: EventCardView = serde_json::from_value(serde_json::to_value(&card)?)?;
    assert_eq!(roundtrip.origin_kind, EventOriginKind::Derived);
    assert_eq!(roundtrip.trace_links, card.trace_links);

    let unknown = serde_json::from_value::<EventOriginKind>(json!("mystery_origin"));
    assert!(unknown.is_err(), "unknown origin kind must fail loudly");

    Ok(())
}

#[sinex_test]
async fn event_trace_relation_vocabulary_covers_issue_contract() -> xtask::TestResult<()> {
    let relations = [
        (EventTraceRelation::SourceMaterial, "source_material"),
        (EventTraceRelation::MaterialAnchor, "material_anchor"),
        (EventTraceRelation::SourceEvent, "source_event"),
        (EventTraceRelation::QueryRun, "query_run"),
        (EventTraceRelation::Proposal, "proposal"),
        (EventTraceRelation::Judgment, "judgment"),
        (EventTraceRelation::Operation, "operation"),
        (EventTraceRelation::ExternalRef, "external_ref"),
        (EventTraceRelation::Policy, "policy"),
    ];

    for (relation, wire) in relations {
        assert_eq!(serde_json::to_value(relation)?, json!(wire));
    }

    let object_kinds = [
        (SinexObjectKind::QueryRun, "query_run"),
        (SinexObjectKind::Proposal, "proposal"),
        (SinexObjectKind::Judgment, "judgment"),
        (SinexObjectKind::Operation, "operation"),
        (SinexObjectKind::ExternalRef, "external_ref"),
        (SinexObjectKind::Policy, "policy"),
    ];

    for (kind, wire) in object_kinds {
        assert_eq!(serde_json::to_value(kind)?, json!(wire));
    }

    Ok(())
}

#[sinex_test]
async fn view_envelope_serializes_schema_version_and_payload() -> xtask::TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.recent",
        EventCardListView {
            schema_version: EVENT_CARD_LIST_SCHEMA_VERSION.to_string(),
            count: 0,
            cards: Vec::new(),
            next_cursor: None,
            total_estimate: None,
        },
    )
    .with_query_echo(json!({ "since": "1h", "limit": 20 }));

    let value = serde_json::to_value(&envelope)?;
    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.recent");
    assert_eq!(
        value["payload"]["schema_version"],
        EVENT_CARD_LIST_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["count"], 0);
    Ok(())
}

#[sinex_test]
async fn source_coverage_list_view_serializes_status_shape() -> xtask::TestResult<()> {
    let view = SourceCoverageView {
        source_id: "fixture.source".to_string(),
        namespace: "fixture".to_string(),
        event_types: vec!["fixture/fixture.event".to_string()],
        readiness: SourceCoverageReadiness::Ready,
        continuity: SourceCoverageContinuity::Active,
        last_material_at: None,
        last_event_at: None,
        material_count: 2,
        event_count: 3,
        binding_count: 1,
        live_binding_count: 1,
        proposed_binding_count: 0,
        gaps: Vec::new(),
        caveats: Vec::new(),
        privacy: SourcePrivacyPosture {
            tier: "sensitive".to_string(),
            context: "command".to_string(),
            proposed: false,
        },
        resource_budget: Some(SourceResourceBudgetView {
            resource_profile: "bounded_stream".to_string(),
            work_class: "admission_hot".to_string(),
            steady_memory_mib: 256,
            burst_memory_mib: 512,
            cpu_weight: 100,
            max_input_bytes_per_sec: Some(32 * 1024 * 1024),
            max_input_events_per_sec: Some(10_000),
            max_pending_material_bytes: 128 * 1024 * 1024,
            max_pending_candidates: 25_000,
            max_unacked_transport_messages: Some(1_000),
            batch_size: Some(2_000),
            flush_interval_ms: Some(500),
            checkpoint_interval_ms: Some(2_000),
            pressure_actions: vec![
                "throttle".to_string(),
                "defer".to_string(),
                "retry".to_string(),
                "inspect".to_string(),
            ],
        }),
        actions: vec![ActionAvailability::read(
            "sources.readiness",
            "Readiness",
            ActionAvailabilityState::Enabled,
        )],
    };
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.status",
        SourceCoverageListView::new(vec![view]),
    );

    let value = serde_json::to_value(&envelope)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(
        value["payload"]["schema_version"],
        SOURCE_COVERAGE_LIST_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["sources"][0]["readiness"], "ready");
    assert_eq!(value["payload"]["sources"][0]["continuity"], "active");
    assert_eq!(
        value["payload"]["sources"][0]["resource_budget"]["work_class"],
        "admission_hot"
    );
    assert_eq!(
        value["payload"]["sources"][0]["resource_budget"]["pressure_actions"][2],
        "retry"
    );
    Ok(())
}

#[sinex_test]
async fn debt_list_view_represents_admission_and_projection_debt() -> xtask::TestResult<()> {
    let admission_row = DebtRowView {
        id: "debt:admission:fixture".to_string(),
        kind: DebtKind::Admission,
        stage: DebtStage::CandidateQuarantined,
        summary: "candidate quarantined by admission policy".to_string(),
        refs: vec![
            SinexObjectRef::new(SinexObjectKind::SourceMaterial, "material:fixture"),
            SinexObjectRef::new(SinexObjectKind::AdmissionOutcome, "outcome:fixture"),
        ],
        owner: Some(DebtOwnerView::admission_policy("admission-policy:fixture")),
        age_secs: Some(42),
        freshness: None,
        caveats: vec![CaveatView {
            id: "admission.quarantined".to_string(),
            message: "operator action is required before admission can continue".to_string(),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Policy,
                "admission-policy:fixture",
            )),
        }],
        actions: vec![
            ActionAvailability::read("debt.inspect", "Inspect", ActionAvailabilityState::Enabled)
                .with_command_hint("sinexctl ops debt inspect debt:admission:fixture"),
        ],
    };
    let projection_row = DebtRowView {
        id: "debt:projection:fixture".to_string(),
        kind: DebtKind::Projection,
        stage: DebtStage::ProjectionStale,
        summary: "projection is stale after replay".to_string(),
        refs: vec![SinexObjectRef::new(
            SinexObjectKind::Projection,
            "projection:fixture",
        )],
        owner: Some(DebtOwnerView::operation(SinexObjectRef::new(
            SinexObjectKind::Operation,
            "operation:rebuild-fixture",
        ))),
        age_secs: Some(300),
        freshness: Some(FreshnessView {
            generated_at: Timestamp::now(),
            stale_after_secs: Some(60),
        }),
        caveats: vec![CaveatView {
            id: "projection.stale".to_string(),
            message: "derived output needs rebuild".to_string(),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Artifact,
                "artifact:fixture",
            )),
        }],
        actions: vec![ActionAvailability {
            id: "projection.rebuild".to_string(),
            label: "Rebuild".to_string(),
            state: ActionAvailabilityState::Enabled,
            reason: None,
            command_hint: Some("sinexctl ops replay submit --ref projection:fixture".to_string()),
            rpc_method: None,
            side_effect: ActionSideEffect::Write,
            requires_confirmation: true,
            dry_run_available: true,
            audit_output_ref: None,
        }],
    };

    let envelope = ViewEnvelope::new(
        "sinexctl.ops.debt",
        DebtListView::new(vec![admission_row, projection_row]),
    );
    let value = serde_json::to_value(&envelope)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["payload"]["schema_version"], DEBT_LIST_SCHEMA_VERSION);
    assert_eq!(value["payload"]["count"], 2);
    assert_eq!(value["payload"]["rows"][0]["kind"], "admission");
    assert_eq!(
        value["payload"]["rows"][0]["stage"],
        "candidate_quarantined"
    );
    assert_eq!(
        value["payload"]["rows"][0]["refs"][1]["kind"],
        "admission_outcome"
    );
    assert_eq!(value["payload"]["rows"][1]["kind"], "projection");
    assert_eq!(value["payload"]["rows"][1]["stage"], "projection_stale");
    assert_eq!(
        value["payload"]["rows"][1]["owner"]["operation_ref"]["kind"],
        "operation"
    );
    assert_eq!(
        value["payload"]["rows"][1]["actions"][0]["side_effect"],
        "write"
    );
    Ok(())
}

#[sinex_test]
async fn event_card_json_uses_contract_field_names() -> xtask::TestResult<()> {
    let result = QueryResultEvent {
        event: Event {
            id: None,
            source: EventSource::new("test.source")?,
            event_type: EventType::new("test.event")?,
            payload: json!({ "summary": "fixture summary" }),
            ts_orig: None,
            ts_quality: None,
            host: HostName::new("test-host")?,
            module_run_id: None,
            payload_schema_id: None,
            provenance: Provenance::Derived {
                source_event_ids: NonEmptyVec::single(Id::<Event<JsonValue>>::new()),
                operation_id: None,
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            automaton_model: None,
            anchor_payload_hash: None,
        },
        relevance_score: None,
        snippet: None,
    };

    let value = serde_json::to_value(EventCardView::from_query_event(&result))?;
    assert!(value.get("ref").is_some());
    assert!(value.get("ref_").is_none());
    assert_eq!(value["summary"], "fixture summary");
    assert_eq!(value["actions"][0]["state"], "unavailable");
    assert!(value["actions"][0].get("reason").is_some());
    Ok(())
}

#[sinex_test]
async fn desktop_context_view_carries_evidence_caveats_and_actions() -> xtask::TestResult<()> {
    let window_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:window-focused")
        .with_label("wm.hyprland · window.focused");
    let browser_coverage_ref =
        SinexObjectRef::new(SinexObjectKind::Projection, "source-coverage:browser.web")
            .with_label("browser.web coverage");
    let policy_ref = SinexObjectRef::new(
        SinexObjectKind::Policy,
        "disclosure-policy:desktop.context.view",
    );

    let view = DesktopContextView::current(
        crate::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID,
        vec![
            DesktopContextInputEvidence {
                family: "wm.hyprland".to_string(),
                state: DesktopContextInputState::Included,
                refs: vec![window_ref.clone()],
                caveats: Vec::new(),
                actions: Vec::new(),
            },
            DesktopContextInputEvidence {
                family: "browser.web".to_string(),
                state: DesktopContextInputState::Missing,
                refs: vec![browser_coverage_ref.clone()],
                caveats: vec![CaveatView {
                    id: "input.browser.missing".to_string(),
                    message: "browser context is unavailable for this view".to_string(),
                    ref_: Some(browser_coverage_ref.clone()),
                }],
                actions: vec![
                    ActionAvailability::read(
                        "sources.browser.check",
                        "Check Browser",
                        ActionAvailabilityState::Enabled,
                    )
                    .with_command_hint("sinexctl sources status --family browser"),
                ],
            },
            DesktopContextInputEvidence {
                family: "terminal.activity".to_string(),
                state: DesktopContextInputState::Redacted,
                refs: vec![policy_ref.clone()],
                caveats: vec![CaveatView {
                    id: "input.terminal.redacted".to_string(),
                    message: "terminal command text is hidden by view disclosure policy"
                        .to_string(),
                    ref_: Some(policy_ref.clone()),
                }],
                actions: Vec::new(),
            },
        ],
    )
    .with_caveat(
        "context.partial",
        "desktop context is partial because one input family is unavailable",
        Some(browser_coverage_ref),
    );

    let value = serde_json::to_value(view.into_envelope("sinexctl.desktop.context.current"))?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.desktop.context.current");
    assert_eq!(
        value["payload"]["schema_version"],
        DESKTOP_CONTEXT_VIEW_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["output_kind"], "current_view");
    assert_eq!(
        value["payload"]["derivation_ref"],
        crate::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID
    );
    assert_eq!(value["payload"]["inputs"][0]["state"], "included");
    assert_eq!(value["payload"]["inputs"][1]["state"], "missing");
    assert_eq!(value["payload"]["inputs"][2]["state"], "redacted");
    assert_eq!(
        value["payload"]["inputs"][1]["actions"][0]["command_hint"],
        "sinexctl sources status --family browser"
    );
    assert_eq!(value["caveats"][0]["id"], "context.partial");
    assert_eq!(value["actions"][0]["id"], "desktop.context.explain");
    assert_eq!(window_ref.kind, SinexObjectKind::Event);
    Ok(())
}

#[sinex_test]
async fn desktop_notification_pressure_view_carries_projection_contract() -> xtask::TestResult<()> {
    let event_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:notification-sent")
        .with_label("desktop.notification · notification.sent");
    let mut view = DesktopNotificationPressureView::new(
        crate::DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID,
        "2h",
    );
    view.sent_count = 1;
    view.total_notification_events = 1;
    view.evidence_refs.push(event_ref.clone());
    view.caveats.push(CaveatView {
        id: "notification_pressure.partial".to_string(),
        message: "fixture pressure view is partial".to_string(),
        ref_: Some(SinexObjectRef::new(
            SinexObjectKind::Projection,
            "desktop.notification_pressure",
        )),
    });

    let envelope = view
        .into_envelope("sinexctl.events.context.desktop.notification_pressure")
        .with_query_echo(json!({ "mode": "desktop_notification_pressure" }));
    let value = serde_json::to_value(&envelope)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(
        value["payload"]["schema_version"],
        DESKTOP_NOTIFICATION_PRESSURE_SCHEMA_VERSION
    );
    assert_eq!(
        value["payload"]["derivation_ref"],
        crate::DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID
    );
    assert_eq!(
        value["payload"]["output_kind"],
        "notification_pressure_projection"
    );
    assert_eq!(
        value["payload"]["output_id"],
        "desktop.notification_pressure"
    );
    assert_eq!(value["payload"]["evidence_refs"][0]["id"], event_ref.id);
    assert_eq!(value["caveats"][0]["id"], "notification_pressure.partial");
    Ok(())
}

#[sinex_test]
async fn desktop_focus_session_list_carries_projection_contract() -> xtask::TestResult<()> {
    let window_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:window-focused")
        .with_label("wm.hyprland · window.focused");
    let terminal_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:command-executed")
        .with_label("shell.atuin · command.executed");
    let mut view =
        DesktopFocusSessionListView::new(crate::DESKTOP_FOCUS_SESSION_DERIVATION_ID, "2h");
    view.sessions.push(DesktopFocusSessionView {
        session_id: "desktop.focus_session:event:window-focused..event:command-executed"
            .to_string(),
        started_at: None,
        ended_at: None,
        event_count: 2,
        input_families: vec!["desktop".to_string(), "terminal".to_string()],
        evidence_refs: vec![window_ref.clone(), terminal_ref.clone()],
        caveats: vec![CaveatView {
            id: "focus_session.open_window".to_string(),
            message: "fixture focus session is still open".to_string(),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.focus_session",
            )),
        }],
    });
    view.session_count = view.sessions.len();

    let envelope = view
        .into_envelope("sinexctl.events.context.desktop.focus_sessions")
        .with_query_echo(json!({ "mode": "desktop_focus_sessions" }));
    let value = serde_json::to_value(&envelope)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(
        value["payload"]["schema_version"],
        DESKTOP_FOCUS_SESSION_LIST_SCHEMA_VERSION
    );
    assert_eq!(
        value["payload"]["derivation_ref"],
        crate::DESKTOP_FOCUS_SESSION_DERIVATION_ID
    );
    assert_eq!(value["payload"]["output_kind"], "focus_session_projection");
    assert_eq!(value["payload"]["output_id"], "desktop.focus_session");
    assert_eq!(value["payload"]["sessions"][0]["event_count"], 2);
    assert_eq!(
        value["payload"]["sessions"][0]["evidence_refs"][0]["id"],
        window_ref.id
    );
    assert_eq!(value["actions"][0]["id"], "desktop.focus_session.explain");
    Ok(())
}

#[sinex_test]
async fn desktop_project_context_list_carries_projection_contract() -> xtask::TestResult<()> {
    let terminal_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:terminal-cwd")
        .with_label("shell.atuin · command.executed");
    let browser_ref = SinexObjectRef::new(SinexObjectKind::Event, "event:browser-tab")
        .with_label("activitywatch · browser.tab.active");
    let mut view =
        DesktopProjectContextListView::new(crate::DESKTOP_PROJECT_CONTEXT_DERIVATION_ID, "2h");
    view.rows.push(DesktopProjectContextRowView {
        label: "sinex".to_string(),
        confidence: 0.74,
        focus_session_ref: Some(SinexObjectRef::new(
            SinexObjectKind::Projection,
            "desktop.focus_session:event:terminal-cwd..event:browser-tab",
        )),
        input_families: vec!["browser".to_string(), "terminal".to_string()],
        evidence_refs: vec![terminal_ref.clone(), browser_ref.clone()],
        proposal_ref: None,
        caveats: vec![CaveatView {
            id: "project_context.ranked_view_only".to_string(),
            message: "fixture project context is a ranked projection candidate".to_string(),
            ref_: Some(SinexObjectRef::new(
                SinexObjectKind::Projection,
                "desktop.project_context",
            )),
        }],
    });
    view.row_count = view.rows.len();

    let envelope = view
        .into_envelope("sinexctl.events.context.desktop.project_contexts")
        .with_query_echo(json!({ "mode": "desktop_project_contexts" }));
    let value = serde_json::to_value(&envelope)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(
        value["payload"]["schema_version"],
        DESKTOP_PROJECT_CONTEXT_LIST_SCHEMA_VERSION
    );
    assert_eq!(
        value["payload"]["derivation_ref"],
        crate::DESKTOP_PROJECT_CONTEXT_DERIVATION_ID
    );
    assert_eq!(
        value["payload"]["output_kind"],
        "project_context_projection"
    );
    assert_eq!(value["payload"]["output_id"], "desktop.project_context");
    assert_eq!(value["payload"]["rows"][0]["label"], "sinex");
    assert_eq!(
        value["payload"]["rows"][0]["evidence_refs"][0]["id"],
        terminal_ref.id
    );
    assert_eq!(value["actions"][0]["id"], "desktop.project_context.explain");
    Ok(())
}

#[sinex_test]
async fn desktop_context_derivations_are_not_canonical_events() -> xtask::TestResult<()> {
    let current = crate::find_derivation_spec(crate::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID)
        .expect("desktop current-view derivation is registered");
    let focus = crate::find_derivation_spec(crate::DESKTOP_FOCUS_SESSION_DERIVATION_ID)
        .expect("desktop focus-session derivation is registered");
    let project = crate::find_derivation_spec(crate::DESKTOP_PROJECT_CONTEXT_DERIVATION_ID)
        .expect("desktop project-context derivation is registered");
    let notification =
        crate::find_derivation_spec(crate::DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID)
            .expect("desktop notification-pressure derivation is registered");

    assert_eq!(current.output_id, "desktop.context.current_view");
    assert_eq!(current.output_kind, crate::OutputKind::EphemeralView);
    assert_eq!(focus.output_kind, crate::OutputKind::ProjectionRow);
    assert_eq!(project.output_kind, crate::OutputKind::ProjectionRow);
    assert_eq!(notification.output_kind, crate::OutputKind::ProjectionRow);
    assert!(focus.invalidates_on(crate::InvalidationTrigger::Redaction));
    assert!(project.invalidates_on(crate::InvalidationTrigger::Replay));
    assert!(current.invalidates_on(crate::InvalidationTrigger::DisclosurePolicyChange));
    assert!(!current.output_kind.is_canonical_event());
    assert!(!focus.output_kind.is_canonical_event());
    assert!(!project.output_kind.is_canonical_event());

    assert_eq!(
        crate::declared_output_kind("desktop.context.current_view"),
        Some(crate::OutputKind::EphemeralView)
    );
    assert_eq!(
        crate::declared_output_kind("desktop.focus_session"),
        Some(crate::OutputKind::ProjectionRow)
    );
    assert_eq!(
        crate::declared_output_kind("desktop.project_context"),
        Some(crate::OutputKind::ProjectionRow)
    );
    assert_eq!(
        crate::declared_output_kind("desktop.notification_pressure"),
        Some(crate::OutputKind::ProjectionRow)
    );
    Ok(())
}

#[sinex_test]
async fn desktop_context_candidate_confidence_requires_authority_ref_for_durable_label()
-> xtask::TestResult<()> {
    let judged = DesktopContextCandidateView {
        label: "sinex".to_string(),
        confidence: 0.91,
        evidence_refs: vec![SinexObjectRef::new(SinexObjectKind::Event, "event:cwd")],
        proposal_ref: Some(SinexObjectRef::new(
            SinexObjectKind::Proposal,
            "proposal:desktop-context-sinex",
        )),
    };
    let unjudged = DesktopContextCandidateView {
        label: "unknown".to_string(),
        confidence: 0.99,
        evidence_refs: vec![SinexObjectRef::new(SinexObjectKind::Event, "event:title")],
        proposal_ref: None,
    };
    let view = DesktopContextView::current(
        crate::DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID,
        Vec::new(),
    );
    let mut value: DesktopContextView = serde_json::from_value(serde_json::to_value(view)?)?;
    value.candidates = vec![judged, unjudged];

    let durable_candidates = value
        .candidates
        .iter()
        .filter(|candidate| candidate.proposal_ref.is_some())
        .count();

    assert_eq!(durable_candidates, 1);
    assert!(
        value
            .candidates
            .iter()
            .any(|candidate| candidate.confidence > 0.95 && candidate.proposal_ref.is_none()),
        "high confidence alone remains only a ranked view candidate"
    );
    assert_eq!(
        value.candidates[0].proposal_ref.as_ref().unwrap().kind,
        SinexObjectKind::Proposal
    );
    Ok(())
}

#[sinex_test]
async fn view_schema_generation_covers_card_and_envelope() -> xtask::TestResult<()> {
    let card_schema = serde_json::to_value(schemars::schema_for!(EventCardView))?;
    let context_envelope_schema =
        serde_json::to_value(schemars::schema_for!(ViewEnvelope<ContextSummaryView>))?;
    let desktop_context_envelope_schema =
        serde_json::to_value(schemars::schema_for!(ViewEnvelope<DesktopContextView>))?;
    let desktop_project_context_envelope_schema = serde_json::to_value(schemars::schema_for!(
        ViewEnvelope<DesktopProjectContextListView>
    ))?;
    let envelope_schema =
        serde_json::to_value(schemars::schema_for!(ViewEnvelope<EventCardListView>))?;
    let debt_envelope_schema =
        serde_json::to_value(schemars::schema_for!(ViewEnvelope<DebtListView>))?;
    let error_envelope_schema =
        serde_json::to_value(schemars::schema_for!(ViewEnvelope<EventErrorListView>))?;
    let query_envelope_schema =
        serde_json::to_value(schemars::schema_for!(ViewEnvelope<EventQueryListView>))?;

    assert_eq!(card_schema["title"], "EventCardView");
    assert!(
        card_schema["properties"].get("ref").is_some(),
        "card schema should expose the contract `ref` field"
    );
    assert!(
        context_envelope_schema["properties"]
            .get("payload")
            .is_some(),
        "context envelope schema should include the typed summary payload"
    );
    assert!(
        desktop_context_envelope_schema["properties"]
            .get("payload")
            .is_some(),
        "desktop-context envelope schema should include the typed view payload"
    );
    assert!(
        desktop_project_context_envelope_schema["properties"]
            .get("payload")
            .is_some(),
        "desktop project-context envelope schema should include the typed list payload"
    );
    assert!(envelope_schema["properties"].get("payload").is_some());
    assert!(debt_envelope_schema["properties"].get("payload").is_some());
    assert!(
        envelope_schema["properties"]
            .get("source_surface")
            .is_some(),
        "envelope schema should include source surface metadata"
    );
    assert!(
        query_envelope_schema["properties"].get("payload").is_some(),
        "query envelope schema should include the typed query-list payload"
    );
    assert!(
        error_envelope_schema["properties"].get("payload").is_some(),
        "error envelope schema should include the typed error-list payload"
    );
    Ok(())
}
