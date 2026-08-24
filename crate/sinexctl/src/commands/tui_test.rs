use super::*;
use crate::client::ClientConfig;
use crate::client::RetryConfig;
use ratatui::backend::TestBackend;
use sinex_primitives::domain::{ModuleKind, ModuleName, OperationStatus};
use sinex_primitives::rpc::dlq::{DlqListResponse, DlqPressureSignal};
use sinex_primitives::rpc::runtime::RuntimeHeartbeatSource;
use sinex_primitives::views::{
    CaveatView, CoverageGapView, EventSourceView, EventTimestampView, PrivacyStateView,
    SinexObjectRef, SourcePrivacyPosture,
};
use sinex_primitives::{RuntimePressureAction, RuntimePressureLevel};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn ux_mk3_source_state_matrix_snapshot() -> TestResult<()> {
    let rows = [
        coverage_fixture(
            "ux.runtime.ready",
            SourceCoverageReadiness::Ready,
            SourceCoverageContinuity::Active,
            Vec::new(),
            12,
            Vec::new(),
            Vec::new(),
        ),
        coverage_fixture(
            "ux.runtime.material-only",
            SourceCoverageReadiness::Ready,
            SourceCoverageContinuity::MaterialOnly,
            Vec::new(),
            0,
            Vec::new(),
            Vec::new(),
        ),
        coverage_fixture(
            "ux.runtime.drift",
            SourceCoverageReadiness::Ready,
            SourceCoverageContinuity::Active,
            vec![caveat("parser.version_drift", "parser version drift")],
            30,
            Vec::new(),
            Vec::new(),
        ),
        coverage_fixture(
            "ux.runtime.unparsed",
            SourceCoverageReadiness::MissingEvents,
            SourceCoverageContinuity::MaterialOnly,
            vec![caveat(
                "material.staged_unparsed",
                "material staged but not parsed",
            )],
            0,
            vec![CoverageGapView {
                kind: "material-only".to_string(),
                message: "material has not produced events".to_string(),
            }],
            Vec::new(),
        ),
        coverage_fixture(
            "ux.runtime.blocked",
            SourceCoverageReadiness::MissingBinding,
            SourceCoverageContinuity::Unknown,
            vec![caveat(
                "policy.raw_material_blocked",
                "policy blocks raw material",
            )],
            0,
            Vec::new(),
            vec![
                ActionAvailability::read(
                    "sources.readiness",
                    "Readiness",
                    ActionAvailabilityState::Disabled,
                )
                .with_reason("binding unavailable"),
            ],
        ),
    ];
    let matrix = rows
        .iter()
        .map(|source| {
            serde_json::json!({
                "fixture": source.source_id,
                "readiness": readiness_label(source.readiness),
                "continuity": continuity_label(source.continuity),
                "cockpit_state": source_state_label(source_cockpit_state(source)),
                "caveats": source.caveats.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    insta::assert_json_snapshot!("ux_mk3_source_state_matrix", matrix);
    Ok(())
}

#[sinex_test]
async fn source_detail_renders_shared_coverage_actions() -> TestResult<()> {
    let mut terminal = Terminal::new(TestBackend::new(96, 36))?;
    let mut source = coverage_fixture(
        "ux.runtime.actions",
        SourceCoverageReadiness::Ready,
        SourceCoverageContinuity::Gapped,
        vec![caveat(
            "parser.operation_evidence_unjoined",
            "parser/source-worker operation evidence is reported by operation and debt surfaces",
        )],
        4,
        vec![CoverageGapView {
            kind: "gapped".to_string(),
            message: "latest material has no parsed event".to_string(),
        }],
        vec![
            ActionAvailability::read(
                "sources.readiness",
                "Readiness",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint("sinexctl sources readiness ux.runtime.actions"),
            ActionAvailability::read(
                "sources.continuity",
                "Continuity",
                ActionAvailabilityState::Target,
            )
            .with_rpc_method("sources.continuity"),
        ],
    );
    let mut mode = mode_fixture();
    mode.mailbox_projection_message_count = Some(2);
    mode.mailbox_projection_thread_count = Some(1);
    mode.mailbox_projection_body_bytes = Some(64);
    mode.mailbox_projection_attachment_count = Some(3);
    mode.mailbox_projection_attachment_observed_count = Some(1);
    mode.mailbox_projection_last_observed_at = Some(Timestamp::UNIX_EPOCH);
    source.modes.push(mode);
    let app = App {
        current_tab: Tab::Sources,
        should_quit: false,
        client: GatewayClient::new(ClientConfig {
            token: Some("fixture-token".to_string()),
            ..ClientConfig::default()
        })?,
        refresh_interval: 0,
        modules: Vec::new(),
        dlq_stats: None,
        dlq_operation_card: None,
        automaton_dlq_operation_card: None,
        ops_jobs: OperationJobListView::new(Vec::new()),
        replay_operations: Vec::new(),
        lifecycle_operation_card: None,
        private_mode: None,
        source_coverage: vec![source],
        recent_events: Vec::new(),
        recent_event_rows: Vec::new(),
        gateway_version: "fixture".to_string(),
        loading: false,
        last_refresh: Instant::now(),
        error: None,
        refresh_errors: Vec::new(),
        refresh_state: RefreshState::default(),
        selected_index: 0,
        show_help: false,
        copy_menu_open: false,
        copy_index: 0,
        payload_raw: false,
        feedback: None,
    };

    terminal.draw(|f| render_source_detail(f, f.area(), &app))?;

    let rendered = buffer_to_text(terminal.backend().buffer());
    assert!(rendered.contains("Readiness [enabled]"));
    assert!(rendered.contains("sinexctl sources readiness ux.runtime.actions"));
    assert!(rendered.contains("Continuity [target] sources.continuity"));
    assert!(rendered.contains("fixture.mode [accepted] on_demand via direct"));
    assert!(rendered.contains("adapter=FixtureAdapter lifecycle=retain_raw"));
    assert!(rendered.contains("mailbox messages=2 threads=1 body_bytes=64 attachments=1/3"));
    assert!(rendered.contains("action Import Fixture [enabled] sinexctl sources stage"));
    assert!(rendered.contains("latest material has no parsed event"));
    Ok(())
}

#[sinex_test]
async fn ux_mk3_event_card_view_dto_snapshot() -> TestResult<()> {
    let cards = vec![
        event_card_fixture(
            "ux.event.full_provenance",
            PrivacyStateKind::RawVisible,
            vec![
                SinexObjectRef::new(SinexObjectKind::MaterialAnchor, "material:fixture:42")
                    .with_label("fixture.csv:42"),
            ],
            Vec::new(),
        ),
        event_card_fixture(
            "ux.event.redacted",
            PrivacyStateKind::Redacted,
            vec![
                SinexObjectRef::new(SinexObjectKind::MaterialAnchor, "material:fixture:secret")
                    .with_label("redacted fixture"),
            ],
            vec![CaveatView {
                id: "privacy.redacted".to_string(),
                message: "payload field redacted by fixture policy".to_string(),
                ref_: None,
            }],
        ),
        event_card_fixture(
            "ux.event.missing_material_anchor",
            PrivacyStateKind::MetadataOnly,
            Vec::new(),
            vec![CaveatView {
                id: "event.missing_material_anchor".to_string(),
                message: "event has no material anchor reference".to_string(),
                ref_: None,
            }],
        ),
    ];

    insta::assert_json_snapshot!("ux_mk3_event_card_view_dtos", cards);
    Ok(())
}

#[sinex_test]
async fn ux_mk3_operations_room_terminal_grid_snapshot() -> TestResult<()> {
    let card = OperationRoomCard {
        title: "operation ux.operation.failed/audited".to_string(),
        authority: "admin".to_string(),
        phase: "failed".to_string(),
        progress: "42 / 100 events, batch 3".to_string(),
        affected_refs: vec![
            "source: fixture.replay".to_string(),
            "source-material: material-fixture".to_string(),
        ],
        caveats: vec![
            "mutating replay phase: confirmation/audit trail required".to_string(),
            "error: fixture replay failed after preview".to_string(),
        ],
        actions: vec![
            operation_room_action(
                "replay.status",
                "status",
                ActionAvailabilityState::Enabled,
                "sinexctl ops replay status op-fixture",
                ActionSideEffect::Read,
            ),
            operation_room_action(
                "replay.execute",
                "execute",
                ActionAvailabilityState::Dangerous,
                "sinexctl ops replay execute op-fixture",
                ActionSideEffect::Admin,
            ),
            operation_room_action(
                "ops.evidence",
                "evidence",
                ActionAvailabilityState::Enabled,
                "sinexctl ops evidence compile --operation op-fixture --include-debt --include-runtime",
                ActionSideEffect::Read,
            ),
        ],
        audit_refs: vec!["sinexctl ops audit op-fixture".to_string()],
    };
    let mut terminal = Terminal::new(TestBackend::new(84, 22))?;
    terminal.draw(|f| render_operation_card_detail(f, f.area(), &card))?;

    insta::assert_snapshot!(
        "ux_mk3_operations_room_terminal_grid",
        buffer_to_text(terminal.backend().buffer())
    );
    Ok(())
}

#[sinex_test]
async fn operation_room_ops_card_uses_shared_operation_actions() -> TestResult<()> {
    let operation = OperationView::from_rpc(
        "op-fixture".to_string(),
        "replay",
        "operator.local".to_string(),
        OperationStatus::Failed,
        Some(42),
        Some("done".to_string()),
        Some(serde_json::json!({"source": "fixture"})),
        Some(serde_json::json!({"events": 12})),
    );

    let card = ops_operation_card(&operation);
    let actions = card
        .actions
        .iter()
        .map(|action| {
            (
                action.label.as_str(),
                action.state,
                action.command_hint.as_deref().unwrap_or(""),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(card.title, "operation op-fixture (replay)");
    assert!(actions.contains(&(
        "Show",
        ActionAvailabilityState::Enabled,
        "sinexctl ops get op-fixture",
    )));
    assert!(actions.contains(&(
        "Cancel",
        ActionAvailabilityState::Disabled,
        "sinexctl ops cancel op-fixture",
    )));
    assert!(actions.contains(&(
        "Replay",
        ActionAvailabilityState::Dangerous,
        "sinexctl ops replay submit --ref-op op-fixture",
    )));
    Ok(())
}

#[sinex_test]
async fn privacy_operation_card_only_advertises_current_commands() -> TestResult<()> {
    let card = privacy_operation_card_for_state(None);
    let command_hints = card
        .actions
        .iter()
        .filter_map(|action| action.command_hint.as_deref())
        .collect::<Vec<_>>();

    assert_eq!(
        command_hints,
        vec![
            "sinexctl privacy audit",
            "sinexctl privacy export --since 24h --source <source> --output <file>",
        ]
    );
    assert!(
        card.actions.iter().all(|action| action
            .command_hint
            .as_deref()
            .is_some_and(|hint| hint.starts_with("sinexctl privacy "))),
        "privacy operation card must advertise concrete sinexctl privacy commands"
    );
    assert!(
        card.actions
            .iter()
            .all(|action| action.side_effect != ActionSideEffect::Destructive),
        "privacy operation card must not advertise destructive commands without an implemented operation surface"
    );
    Ok(())
}

fn coverage_fixture(
    id: &str,
    readiness: SourceCoverageReadiness,
    continuity: SourceCoverageContinuity,
    caveats: Vec<CaveatView>,
    event_count: i64,
    gaps: Vec<CoverageGapView>,
    actions: Vec<ActionAvailability>,
) -> SourceCoverageView {
    SourceCoverageView {
        source_id: id.to_string(),
        namespace: "ux-mk3".to_string(),
        event_types: vec!["ux-mk3/event.fixture".to_string()],
        readiness,
        continuity,
        last_material_at: Some(Timestamp::UNIX_EPOCH),
        last_event_at: Some(Timestamp::UNIX_EPOCH),
        material_count: 1,
        event_count,
        binding_count: 1,
        accepted_binding_count: 1,
        proposed_binding_count: 0,
        gaps,
        caveats,
        privacy: SourcePrivacyPosture {
            tier: "sensitive".to_string(),
            context: "metadata".to_string(),
            proposed: false,
        },
        work_budget: None,
        modes: Vec::new(),
        actions,
    }
}

fn mode_fixture() -> SourceModeStatusView {
    SourceModeStatusView {
        mode_id: "fixture.mode".to_string(),
        binding_id: "binding.fixture.mode".to_string(),
        implementation: "fixture-implementation".to_string(),
        adapter: "FixtureAdapter".to_string(),
        output_event_type: "fixture.event".to_string(),
        proposed: false,
        runner_pack: "staged".to_string(),
        runtime_shape: "on_demand".to_string(),
        checkpoint_family: "file_cursor".to_string(),
        material_lifecycle: "retain_raw".to_string(),
        transport: "direct".to_string(),
        delivery: "synchronous".to_string(),
        ordering: "input_order".to_string(),
        replayability_class: "retained_material".to_string(),
        catch_up_authority: "source_material".to_string(),
        accepted_loss_policy: serde_json::json!("none"),
        transport_replayable: true,
        dlq: false,
        backpressure: false,
        privacy_context: "metadata".to_string(),
        work_budget: sinex_primitives::views::SourceWorkBudgetView {
            work_class: "bulk_import".to_string(),
            steady_memory_mib: 16,
            burst_memory_mib: 32,
            cpu_weight: 10,
            max_input_bytes_per_sec: None,
            max_input_events_per_sec: None,
            max_pending_material_bytes: 1024,
            max_pending_candidates: 16,
            max_unacked_transport_messages: None,
            batch_size: Some(8),
            flush_interval_ms: None,
            checkpoint_interval_ms: None,
            pressure_actions: vec!["pause".to_string()],
        },
        criticality: None,
        runtime_observed: None,
        runtime_live: None,
        last_heartbeat_at: None,
        last_output_at: None,
        recent_output_count: None,
        provider_operation_status: None,
        provider_auth_state: None,
        provider_network_state: None,
        provider_sync_state: None,
        provider_rate_limit_state: None,
        provider_failure_class: None,
        provider_required_action: None,
        provider_retry_after_secs: None,
        provider_reconnect_state: None,
        provider_operation_id: None,
        provider_coverage_ref: None,
        provider_debt_ref: None,
        mailbox_projection_message_count: None,
        mailbox_projection_thread_count: None,
        mailbox_projection_body_bytes: None,
        mailbox_projection_attachment_count: None,
        mailbox_projection_attachment_observed_count: None,
        mailbox_projection_last_observed_at: None,
        actions: vec![
            ActionAvailability::read(
                "sources.stage.fixture",
                "Import Fixture",
                ActionAvailabilityState::Enabled,
            )
            .with_command_hint("sinexctl sources stage fixture.mode"),
        ],
    }
}

fn caveat(code: &str, message: &str) -> CaveatView {
    CaveatView {
        id: code.to_string(),
        message: message.to_string(),
        ref_: Some(SinexObjectRef::new(SinexObjectKind::Caveat, code)),
    }
}

fn event_card_fixture(
    id: &str,
    privacy: PrivacyStateKind,
    material_refs: Vec<SinexObjectRef>,
    caveats: Vec<CaveatView>,
) -> EventCardView {
    EventCardView {
        ref_: SinexObjectRef::new(SinexObjectKind::Event, id),
        timestamp: EventTimestampView {
            original: Some(Timestamp::UNIX_EPOCH),
            ingested: Some(Timestamp::UNIX_EPOCH),
            quality: "fixture".to_string(),
        },
        source: EventSourceView {
            family: "ux-mk3".to_string(),
            raw: "fixture.source".to_string(),
            source_ref: Some(SinexObjectRef::new(
                SinexObjectKind::SourceDriver,
                "ux.fixture-source",
            )),
        },
        event_type: "ux.fixture".to_string(),
        origin_kind: sinex_primitives::views::EventOriginKind::Derived,
        summary: id.to_string(),
        payload_preview: Some(serde_json::json!({
            "fixture": id,
            "stable": true
        })),
        material_refs,
        privacy_state: PrivacyStateView {
            state: privacy,
            reason: Some("ux fixture".to_string()),
        },
        caveats,
        trace_refs: vec![SinexObjectRef::new(
            SinexObjectKind::ReplayRun,
            "replay-fixture",
        )],
        trace_links: vec![sinex_primitives::views::EventTraceLink {
            relation: sinex_primitives::views::EventTraceRelation::Operation,
            target: SinexObjectRef::new(SinexObjectKind::ReplayRun, "replay-fixture"),
        }],
        projection_badges: vec!["ux-mk3".to_string()],
        actions: vec![
            ActionAvailability::read("trace", "Trace", ActionAvailabilityState::Enabled)
                .with_command_hint(format!("sinexctl events trace {id}")),
            ActionAvailability {
                id: "redact".to_string(),
                label: "Redact".to_string(),
                state: ActionAvailabilityState::Target,
                reason: Some("target-only fixture".to_string()),
                command_hint: None,
                rpc_method: None,
                side_effect: ActionSideEffect::Destructive,
                requires_confirmation: true,
                dry_run_available: true,
                audit_output_ref: None,
            },
        ],
    }
}

fn fixture_query_result_event(payload: serde_json::Value) -> QueryResultEvent {
    let mut event =
        sinex_primitives::events::DynamicPayload::new("ux-mk3.fixture", "ux.fixture", payload)
            .from_material(sinex_primitives::ids::Id::<
                sinex_primitives::events::SourceMaterial,
            >::new())
            .build()
            .expect("fixture event should build");
    event.id = Some(sinex_primitives::ids::Id::new());
    QueryResultEvent {
        event,
        relevance_score: None,
        snippet: None,
    }
}

/// sinex-eisk: the raw/pretty payload toggle and event/payload-JSON copy
/// actions have been permanently dead since #1923 replaced the raw-row
/// data source (`recent_event_rows`) with `.clear()` -- `selected_event_row()`
/// always returns `None`, so `payload_lines()`/`event_copy_actions()` never
/// take their real-data branch. This is a regression guard for the eventual
/// fix (repopulating `recent_event_rows` from a privacy-safe raw-row fetch):
/// it proves the `Some(row)` branch of both functions behaves correctly
/// (real raw JSON, enabled copy actions) so a future regression back to the
/// always-None state is caught by CI, not just discovered by an operator
/// pressing 'p' and getting nothing.
#[test]
fn event_consumers_use_real_row_when_present_not_just_the_truncated_card_preview() {
    let card = event_card_fixture(
        "019f0000-0000-7000-8000-000000000001",
        PrivacyStateKind::RawVisible,
        Vec::new(),
        Vec::new(),
    );
    let row = fixture_query_result_event(serde_json::json!({
        "full_body": "this is the real untruncated payload, not card.payload_preview",
        "field_only_in_raw_row": true,
    }));

    let raw_lines = payload_lines(&card, Some(&row), true);
    let rendered: String = raw_lines
        .iter()
        .flat_map(|line| line.iter())
        .map(|span| span.content.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        rendered.contains("field_only_in_raw_row"),
        "with a real row present, payload_lines(raw=true) must render the \
         actual event payload, not fall back to card.payload_preview"
    );

    let actions = event_copy_actions(&card, Some(&row));
    assert!(
        actions
            .iter()
            .any(|action| action.label.contains("event") && action.disabled_reason.is_none()),
        "with a real row present, the event-JSON copy action must be \
         enabled, not disabled with 'raw query event is unavailable'"
    );
    assert!(
        actions
            .iter()
            .any(|action| action.label.contains("payload") && action.disabled_reason.is_none()),
        "with a real row present, the payload-JSON copy action must be \
         enabled, not disabled with 'raw query event is unavailable'"
    );
}

fn buffer_to_text(buffer: &ratatui::buffer::Buffer) -> String {
    let width = usize::from(buffer.area.width);
    buffer
        .content()
        .chunks(width)
        .map(|row| {
            row.iter()
                .map(ratatui::buffer::Cell::symbol)
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[sinex_test]
async fn tui_module_liveness_uses_canonical_policy_and_run_status() -> TestResult<()> {
    let now = Timestamp::now();
    let mut module = RuntimeInfo {
        module_name: ModuleName::new("fixture-module"),
        module_kind: ModuleKind::Source,
        version: "test".to_string(),
        description: None,
        service_name: None,
        instance_id: None,
        module_run_id: None,
        host: None,
        status: "running".to_string(),
        last_heartbeat_at: Some(now),
        started_at: Some(now),
        heartbeat_source: RuntimeHeartbeatSource::Run,
    };
    assert_eq!(
        module_liveness(&module, now),
        RuntimeLivenessStatus::Healthy
    );

    module.status = "draining".to_string();
    assert_eq!(
        module_liveness(&module, now),
        RuntimeLivenessStatus::Degraded
    );

    module.status = "running".to_string();
    module.last_heartbeat_at = Some(now - time::Duration::seconds(301));
    assert_eq!(module_liveness(&module, now), RuntimeLivenessStatus::Stale);

    module.last_heartbeat_at = Some(now);
    module.status = "failed".to_string();
    assert_eq!(
        module_liveness(&module, now),
        RuntimeLivenessStatus::Unhealthy
    );
    Ok(())
}

#[sinex_test]
async fn tui_refresh_marks_gateway_up_and_down_differently() -> TestResult<()> {
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(|request: &wiremock::Request| {
            let body: serde_json::Value = match serde_json::from_slice(&request.body) {
                Ok(body) => body,
                Err(error) => {
                    return ResponseTemplate::new(400).set_body_string(error.to_string());
                }
            };
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": body["id"],
                "result": if body["method"] == "system.version" { json!("fixture-up") } else { json!({}) }
            }))
        })
        .mount(&server)
        .await;

    let config = ClientConfig {
        url: server.uri(),
        token: Some("fixture-token".to_string()),
        insecure: true,
        retry_config: RetryConfig::builder().max_attempts(1).build(),
        ..ClientConfig::default()
    };
    let mut up = App::new(GatewayClient::new(config)?, Tab::Dashboard, 0);
    up.refresh().await;
    assert_eq!(up.gateway_version, "fixture-up");
    assert!(
        up.refresh_state.panels[&RefreshPanel::Gateway]
            .last_success
            .is_some()
    );
    assert!(!up.refresh_state.is_unavailable(RefreshPanel::Gateway));

    let down_config = ClientConfig {
        url: "http://127.0.0.1:9".to_string(),
        token: Some("fixture-token".to_string()),
        insecure: true,
        timeout: 1,
        retry_config: RetryConfig::builder().max_attempts(1).build(),
        ..ClientConfig::default()
    };
    let mut down = App::new(GatewayClient::new(down_config)?, Tab::Dashboard, 0);
    down.refresh().await;
    assert_eq!(down.gateway_version, "unknown");
    assert!(down.refresh_state.is_unavailable(RefreshPanel::Gateway));
    assert!(down.refresh_state.is_unavailable(RefreshPanel::Modules));
    assert!(
        down.error
            .as_deref()
            .is_some_and(|error| error.contains("Failed to connect"))
    );
    assert_ne!(up.gateway_version, down.gateway_version);
    Ok(())
}

#[sinex_test]
async fn tui_surfaces_multiple_panel_failures_and_stale_titles() -> TestResult<()> {
    let mut app = App::new(
        GatewayClient::new(ClientConfig {
            token: Some("fixture-token".to_string()),
            ..ClientConfig::default()
        })?,
        Tab::Dashboard,
        0,
    );
    app.modules.push(RuntimeInfo {
        module_name: ModuleName::new("fixture-module"),
        module_kind: ModuleKind::Source,
        version: "test".to_string(),
        description: None,
        service_name: None,
        instance_id: None,
        module_run_id: None,
        host: None,
        status: "running".to_string(),
        last_heartbeat_at: Some(Timestamp::now()),
        started_at: Some(Timestamp::now()),
        heartbeat_source: RuntimeHeartbeatSource::Run,
    });
    app.refresh_state.mark_success(RefreshPanel::Modules);
    app.refresh_state.mark_success(RefreshPanel::Dlq);
    app.refresh_state.mark_failure(RefreshPanel::Modules);
    app.refresh_state.mark_failure(RefreshPanel::Dlq);
    app.record_refresh_error("modules failed".to_string());
    app.record_refresh_error("dlq failed".to_string());
    app.error = Some(app.refresh_errors.join("; "));
    assert!(
        app.error.as_deref().is_some_and(|error| {
            error.contains("modules failed") && error.contains("dlq failed")
        })
    );
    assert_eq!(
        app.panel_title("Modules", RefreshPanel::Modules),
        "Modules [STALE]"
    );
    assert_eq!(
        app.panel_title("Raw Ingest DLQ", RefreshPanel::Dlq),
        "Raw Ingest DLQ [STALE]"
    );

    let dlq = DlqListResponse {
        total_messages: 11,
        total_bytes: 1024,
        first_seq: 4,
        last_seq: 14,
        pressure_level: RuntimePressureLevel::Critical,
        resource_pressure: DlqPressureSignal {
            pressure_level: RuntimePressureLevel::Critical,
            runtime_action: RuntimePressureAction::Throttle,
            pending_messages: 11,
            pending_bytes: 1024,
            retry_batch_size: 10,
            recommended_action: "ops dlq triage --tail 20".to_string(),
            reason: "pressure requires paced triage".to_string(),
        },
        pending_sequence_span: 11,
        recommended_action: "ops dlq triage --tail 20".to_string(),
        action_reason: "pressure requires paced triage".to_string(),
    };
    app.dlq_stats = Some(dlq);
    let mut terminal = Terminal::new(TestBackend::new(96, 24))?;
    terminal.draw(|f| render_dlq(f, f.area(), &app))?;
    let rendered = buffer_to_text(terminal.backend().buffer());
    assert!(rendered.contains("Pressure: critical"));
    assert!(rendered.contains("Recommended action: ops dlq triage --tail 20"));
    assert!(rendered.contains("Action reason: pressure requires paced triage"));
    Ok(())
}
