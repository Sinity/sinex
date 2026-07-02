use super::*;
use crate::client::ClientConfig;
use ratatui::backend::TestBackend;
use sinex_primitives::domain::OperationStatus;
use sinex_primitives::views::{
    CaveatView, CoverageGapView, EventSourceView, EventTimestampView, PrivacyStateView,
    SinexObjectRef, SourcePrivacyPosture,
};
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
        resource_budget: None,
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
        replayable: true,
        dlq: false,
        backpressure: false,
        privacy_context: "metadata".to_string(),
        resource_budget: sinex_primitives::views::SourceResourceBudgetView {
            resource_profile: "bounded_file".to_string(),
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
