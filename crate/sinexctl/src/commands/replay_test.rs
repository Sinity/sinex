use super::{
    MaterialReplayabilityScorecard, Replayability, execute_run, execute_watch,
    format_per_material_scorecard_table, format_replay_preview_table, preview_total_events,
    replay_list_envelope, replay_operation_caveats, replay_preview_envelope,
    replay_status_envelope, truncate_head_chars, truncate_tail_chars, weakness_dimensions,
};
use crate::client::{ClientConfig, GatewayClient};
use crate::fmt::render_finite_envelope;
use crate::model::OutputFormat;
use serde_json::json;
use sinex_primitives::rpc::replay::{
    ReplayCheckpoint, ReplayGateOverrides, ReplayOperation, ReplayScope, ReplayState,
};
use sinex_primitives::rpc::sources::SourcesImportReportResponse;
use sinex_primitives::views::{ReadinessCaveatId, VIEW_ENVELOPE_SCHEMA_VERSION};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use xtask::sandbox::prelude::*;

fn fixture_replay_operation(id: &str, state: ReplayState, total_events: u64) -> ReplayOperation {
    ReplayOperation {
        operation_id: id.to_string(),
        state,
        scope: ReplayScope {
            source_name: "terminal.zsh-history".to_string(),
            time_window: None,
            material_filter: None,
            filters: std::collections::HashMap::new(),
            source_id: None,
            source_material_id: None,
            parser_id: None,
            parser_version: None,
        },
        preview_summary: None,
        checkpoint: ReplayCheckpoint {
            processed_events: 0,
            total_events,
            last_event_id: None,
            batch_number: 0,
            savepoint_id: None,
            updated_at: "2026-04-04T00:00:00Z".to_string(),
        },
        actor: "tester".to_string(),
        created_at: "2026-04-04T00:00:00Z".to_string(),
        approved_by: None,
        approved_at: None,
        executor_module: None,
        started_at: None,
        finished_at: None,
        outcome: None,
        error_details: None,
    }
}

#[sinex_test]
async fn preview_total_events_accepts_valid_counts() -> TestResult<()> {
    assert_eq!(preview_total_events(&json!({ "total_events": 0 }))?, 0);
    assert_eq!(preview_total_events(&json!({ "total_events": 42 }))?, 42);
    Ok(())
}

#[sinex_test]
async fn truncate_helpers_handle_multi_byte_utf8() -> TestResult<()> {
    // Mix of 1-byte ASCII, 2-byte (e), 3-byte (β), 4-byte (𝛼) characters.
    // Byte slicing here would panic at the 12-byte / len-25 boundaries
    // when those land in the middle of a code point — char-based
    // truncation must always succeed.
    let s = "/home/usér/φιλε-βυcket/path/𝛼-final-segment-with-extra-padding";
    // Just verify the calls don't panic and the return is non-empty.
    let head = truncate_head_chars(s, 12);
    assert!(!head.is_empty());
    let tail = truncate_tail_chars(s, 26, 25);
    assert!(!tail.is_empty());

    // Short strings are returned unchanged (no ellipsis).
    let short = "abc";
    assert_eq!(truncate_head_chars(short, 12), "abc");
    assert_eq!(truncate_tail_chars(short, 26, 25), "abc");

    // Length above threshold gets ellipsis.
    let long = "x".repeat(40);
    assert!(truncate_head_chars(&long, 12).ends_with('…'));
    assert!(truncate_tail_chars(&long, 26, 25).starts_with('…'));
    Ok(())
}

#[sinex_test]
async fn preview_total_events_rejects_missing_field() -> TestResult<()> {
    let error = preview_total_events(&json!({})).expect_err("missing total_events must fail");
    assert!(error.to_string().contains("total_events"));
    Ok(())
}

#[sinex_test]
async fn preview_total_events_rejects_non_numeric_field() -> TestResult<()> {
    let error = preview_total_events(&json!({ "total_events": "zero" }))
        .expect_err("non-numeric total_events must fail");
    assert!(error.to_string().contains("total_events"));
    Ok(())
}

#[sinex_test]
async fn replay_preview_table_surfaces_failed_safety_analysis() -> TestResult<()> {
    let operation = fixture_replay_operation("op-1", ReplayState::Previewed, 0);
    let preview = json!({
        "total_events": 3,
        "anchor_churn_pct": null,
        "time_quality_flip_pct": null,
        "max_observed_depth": 7,
        "schema_boundary_crossed": true,
        "replay_gates": {
            "gates": [
                {
                    "name": "anchor_churn_threshold_percent",
                    "tripped": false,
                    "advisory": true,
                    "observed": "not measured (advisory)",
                    "override_flag": "--allow-anchor-churn"
                },
                {
                    "name": "require_force_on_schema_mismatch",
                    "tripped": true,
                    "override_flag": "--force-schema-mismatch"
                }
            ]
        },
        "safety_analysis": {
            "status": "failed",
            "error": "integrity analyzer unavailable",
            "warning": "Cascade impact could not be determined. Approve with caution."
        }
    });

    let rendered = format_replay_preview_table(&operation, &preview);

    assert!(rendered.contains("Safety Warning: analysis failed"));
    assert!(rendered.contains("Anchor Churn: not measured"));
    assert!(rendered.contains("Time Quality Flips: not measured"));
    assert!(rendered.contains("Max Cascade Depth: 7"));
    assert!(rendered.contains("Schema Boundary: true"));
    assert!(
        rendered
            .contains("Gates Tripped: require_force_on_schema_mismatch (--force-schema-mismatch)")
    );
    assert!(rendered.contains("Safety Error:   integrity analyzer unavailable"));
    assert!(
        rendered.contains(
            "Safety Detail:  Cascade impact could not be determined. Approve with caution."
        )
    );
    Ok(())
}

#[sinex_test]
async fn replay_preview_envelope_caveats_empty_and_unmeasured_preview() -> TestResult<()> {
    let operation = fixture_replay_operation("op-empty", ReplayState::Previewed, 0);
    let preview = json!({
        "total_events": 0,
        "replay_gates": {
            "gates": [
                {
                    "name": "require_force_on_schema_mismatch",
                    "tripped": true,
                    "override_flag": "--force-schema-mismatch"
                }
            ]
        },
        "safety_analysis": {
            "status": "failed"
        }
    });

    let envelope = replay_preview_envelope(operation, preview, Vec::new(), "op-empty");
    let caveat_ids: Vec<&str> = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect();

    assert_eq!(envelope.source_surface, "sinexctl.ops.replay.preview");
    assert!(caveat_ids.contains(&ReadinessCaveatId::SourceAbsent.as_str()));
    assert!(caveat_ids.contains(&ReadinessCaveatId::CoverageUnmeasurable.as_str()));
    assert!(caveat_ids.contains(&ReadinessCaveatId::WindowPartial.as_str()));
    assert_eq!(
        envelope.query_echo.as_ref().unwrap()["operation_id"],
        "op-empty"
    );
    Ok(())
}

#[sinex_test]
async fn replay_status_envelope_caveats_failed_zero_progress() -> TestResult<()> {
    let mut operation = fixture_replay_operation("op-failed", ReplayState::Failed, 0);
    operation.error_details = Some("source adapter failed".to_string());

    let envelope = replay_status_envelope(operation, "op-failed");
    let caveat_ids: Vec<&str> = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect();

    assert_eq!(envelope.source_surface, "sinexctl.ops.replay.status");
    assert!(caveat_ids.contains(&ReadinessCaveatId::WindowPartial.as_str()));
    assert!(caveat_ids.contains(&ReadinessCaveatId::CoverageUnmeasurable.as_str()));
    assert_eq!(
        envelope.query_echo.as_ref().unwrap()["operation_id"],
        "op-failed"
    );
    Ok(())
}

#[sinex_test]
async fn replay_list_envelope_caveats_empty_operation_log() -> TestResult<()> {
    let envelope = replay_list_envelope(
        Vec::new(),
        Some(super::ReplayStateFilter::Completed),
        Some("terminal.zsh-history"),
        25,
    );

    assert_eq!(envelope.source_surface, "sinexctl.ops.replay.list");
    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(
        envelope.caveats[0].id,
        ReadinessCaveatId::SourceAbsent.as_str()
    );
    assert_eq!(envelope.query_echo.as_ref().unwrap()["state"], "completed");
    assert_eq!(
        envelope.query_echo.as_ref().unwrap()["source"],
        "terminal.zsh-history"
    );
    assert_eq!(envelope.query_echo.as_ref().unwrap()["limit"], 25);
    Ok(())
}

#[sinex_test]
async fn replay_list_envelope_renders_finite_json() -> TestResult<()> {
    let envelope = replay_list_envelope(
        vec![fixture_replay_operation("op-1", ReplayState::Completed, 7)],
        None,
        None,
        50,
    );

    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.ops.replay.list");
    assert_eq!(parsed["payload"][0]["operation_id"], "op-1");
    assert_eq!(parsed["query_echo"]["limit"], 50);
    Ok(())
}

fn make_scorecard(
    material_id: &str,
    source: &str,
    status: sinex_primitives::MaterialStatus,
    replayability: Replayability,
) -> MaterialReplayabilityScorecard {
    MaterialReplayabilityScorecard {
        material_id: material_id.to_string(),
        source_identifier: source.to_string(),
        material_kind: "annex".to_string(),
        status,
        replayability,
    }
}

#[sinex_test]
async fn weakness_dimensions_lists_failed_axes_only() -> TestResult<()> {
    // All-green scorecard reports no weaknesses.
    let strong = Replayability::from_material_facts(
        sinex_primitives::MaterialStatus::Completed,
        true,
        sinex_primitives::domain::SourceMaterialTimingInfoType::Intrinsic,
        Some(1024),
    );
    assert!(weakness_dimensions(&strong).is_empty());

    // Sensing material with no blob and inferred timing must surface
    // blob, timing, and anchor as weakness axes.
    let weak = Replayability::from_material_facts(
        sinex_primitives::MaterialStatus::Sensing,
        false,
        sinex_primitives::domain::SourceMaterialTimingInfoType::Inferred,
        None,
    );
    let dims = weakness_dimensions(&weak);
    assert!(dims.contains(&"blob"));
    assert!(dims.contains(&"timing"));
    assert!(dims.contains(&"anchor"));
    Ok(())
}

#[sinex_test]
async fn per_material_scorecard_table_contains_aggregate_row() -> TestResult<()> {
    // Two materials with distinct replayability shapes — one strong,
    // one weak — should compose into an aggregate row that names the
    // material count and a midpoint score.
    let strong = Replayability::from_material_facts(
        sinex_primitives::MaterialStatus::Completed,
        true,
        sinex_primitives::domain::SourceMaterialTimingInfoType::Intrinsic,
        Some(2048),
    );
    let weak = Replayability::from_material_facts(
        sinex_primitives::MaterialStatus::Sensing,
        false,
        sinex_primitives::domain::SourceMaterialTimingInfoType::Inferred,
        None,
    );
    let rows = vec![
        make_scorecard(
            "mat-a-uuid",
            "/path/strong.csv",
            sinex_primitives::MaterialStatus::Completed,
            strong,
        ),
        make_scorecard(
            "mat-b-uuid",
            "/path/weak.csv",
            sinex_primitives::MaterialStatus::Sensing,
            weak,
        ),
    ];

    let rendered = format_per_material_scorecard_table(&rows);
    assert!(rendered.contains("Per-Material Replayability:"));
    assert!(rendered.contains("MATERIAL"));
    assert!(rendered.contains("WEAKNESSES"));
    // Both rows present (truncated material id prefix).
    assert!(rendered.contains("mat-a-uuid"));
    assert!(rendered.contains("mat-b-uuid"));
    // Aggregate row mentions the material count.
    assert!(rendered.contains("aggregate; 2 materials"));
    // Weak row surfaces the dimension labels in the WEAKNESSES column.
    assert!(rendered.contains("blob") || rendered.contains("timing"));
    Ok(())
}

async fn mount_failed_replay_status_fixture(operation: &ReplayOperation) -> MockServer {
    let server = MockServer::start().await;
    let operation = operation.clone();
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("valid JSON-RPC request body");
            assert_eq!(body["method"], "replay.operation_status");
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": body["id"],
                "result": { "operation": operation }
            }))
        })
        .mount(&server)
        .await;
    server
}

fn fixture_gateway_client(server: &MockServer) -> color_eyre::Result<GatewayClient> {
    GatewayClient::new(ClientConfig {
        url: server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..Default::default()
    })
}

fn fixture_import_report(operation_id: &str) -> SourcesImportReportResponse {
    SourcesImportReportResponse {
        operation_id: operation_id.to_string(),
        operation_type: "replay".to_string(),
        operation_status: "success".to_string(),
        scope: json!({"source_name": "terminal.zsh-history"}),
        source: Some("terminal.zsh-history".to_string()),
        source_material_ids: vec!["material-fixture".to_string()],
        attempted: 11,
        new: 2,
        suppressed: 3,
        superseded: 1,
        failures: 1,
        dlq: 1,
        unresolved: 3,
        breakdown: Vec::new(),
        examples: Vec::new(),
    }
}

#[sinex_test]
async fn ordinary_replay_run_fetches_the_durable_idempotence_report() -> TestResult<()> {
    let operation = fixture_replay_operation("op-completed-report", ReplayState::Completed, 11);
    let mut planning = operation.clone();
    planning.state = ReplayState::Planning;
    let mut previewed = operation.clone();
    previewed.state = ReplayState::Previewed;
    let mut approved = operation.clone();
    approved.state = ReplayState::Approved;
    let mut executing = operation.clone();
    executing.state = ReplayState::Executing;
    let report = fixture_import_report(&operation.operation_id);
    let server = MockServer::start().await;
    let planning_for_response = planning.clone();
    let previewed_for_response = previewed.clone();
    let approved_for_response = approved.clone();
    let executing_for_response = executing.clone();
    let operation_for_response = operation.clone();
    let report_for_response = report.clone();
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("valid JSON-RPC request body");
            let result = match body["method"].as_str() {
                Some("replay.create_operation") => json!({
                    "operation": planning_for_response,
                }),
                Some("replay.preview_operation") => json!({
                    "operation": previewed_for_response,
                    "preview": {"total_events": 11},
                }),
                Some("replay.approve_operation") => json!({
                    "operation": approved_for_response,
                }),
                Some("replay.execute_operation") => json!({
                    "operation": executing_for_response,
                }),
                Some("replay.operation_status") => json!({
                    "operation": operation_for_response,
                }),
                Some("sources.import_report") => json!(report_for_response),
                Some("curation.duplicate_candidates.list") => json!({"clusters": []}),
                method => panic!("unexpected method in completion fixture: {method:?}"),
            };
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": body["id"],
                "result": result,
            }))
        })
        .mount(&server)
        .await;
    let client = fixture_gateway_client(&server)?;

    execute_run(
        &client,
        "terminal.zsh-history",
        None,
        None,
        &[],
        &[],
        false,
        ReplayGateOverrides::default(),
        &OutputFormat::Table,
    )
    .await?;

    Ok(())
}

// sinex-2bti: JSON/Ndjson/Dot output formats for `sinexctl ops replay watch`
// must detect ReplayState::Failed and return an error (non-zero exit),
// matching the Table format's existing behavior — a CI/cron script parsing
// only the exit code (the overwhelmingly common case) must not see success
// (0) for a replay that failed server-side.
#[sinex_test]
#[ignore = "sinex-2bti open: JSON watch format doesn't check ReplayState::Failed; \
            un-ignore once the bead is fixed"]
async fn execute_watch_json_format_errors_on_replay_failed() -> TestResult<()> {
    let mut operation = fixture_replay_operation("op-failed-json", ReplayState::Failed, 0);
    operation.error_details = Some("source adapter failed".to_string());
    let server = mount_failed_replay_status_fixture(&operation).await;
    let client = fixture_gateway_client(&server)?;

    let result = execute_watch(&client, "op-failed-json", 0, &OutputFormat::Json).await;

    assert!(
        result.is_err(),
        "execute_watch with --format json must return Err on ReplayState::Failed, \
         matching the Table format's behavior (sinex-2bti)"
    );
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-2bti open: Dot watch format doesn't check ReplayState::Failed; \
            un-ignore once the bead is fixed"]
async fn execute_watch_dot_format_errors_on_replay_failed() -> TestResult<()> {
    let mut operation = fixture_replay_operation("op-failed-dot", ReplayState::Failed, 0);
    operation.error_details = Some("source adapter failed".to_string());
    let server = mount_failed_replay_status_fixture(&operation).await;
    let client = fixture_gateway_client(&server)?;

    let result = execute_watch(&client, "op-failed-dot", 0, &OutputFormat::Dot).await;

    assert!(
        result.is_err(),
        "execute_watch with --format dot must return Err on ReplayState::Failed, matching the \
         Table format's behavior (sinex-2bti) -- Dot shares the exact same match arm as JSON/Ndjson \
         (replay.rs:575) so it has the identical bug, it was just untested"
    );
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-pfsm open: no replay caveat discloses replay's non-idempotent-by-design \
            semantics; un-ignore once a caveat is added for at least the Completed state"]
async fn completed_replay_operation_caveats_never_disclose_non_idempotence() -> TestResult<()> {
    let operation = fixture_replay_operation("op-completed-pfsm", ReplayState::Completed, 5);

    let caveats = replay_operation_caveats(&operation);

    assert!(
        caveats.iter().any(|c| {
            let detail = format!("{c:?}").to_lowercase();
            detail.contains("idempot") || detail.contains("re-run") || detail.contains("rerun")
        }),
        "sinex-pfsm: none of sinexctl's replay-command caveat/table-formatting code discloses \
         that replay is non-idempotent by design (re-derives under CURRENT rules, mints new ids) \
         -- an operator reading a Completed operation's caveats has no way to learn this from the \
         CLI, only from source or docs"
    );
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-2bti open: Ndjson watch format doesn't check ReplayState::Failed; \
            un-ignore once the bead is fixed"]
async fn execute_watch_ndjson_format_errors_on_replay_failed() -> TestResult<()> {
    let mut operation = fixture_replay_operation("op-failed-ndjson", ReplayState::Failed, 0);
    operation.error_details = Some("source adapter failed".to_string());
    let server = mount_failed_replay_status_fixture(&operation).await;
    let client = fixture_gateway_client(&server)?;

    let result = execute_watch(&client, "op-failed-ndjson", 0, &OutputFormat::Ndjson).await;

    assert!(
        result.is_err(),
        "execute_watch with --format ndjson must return Err on ReplayState::Failed (sinex-2bti)"
    );
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-2bti open: Yaml watch format doesn't poll or check ReplayState::Failed; \
            un-ignore once the bead is fixed"]
async fn execute_watch_yaml_format_errors_on_replay_failed() -> TestResult<()> {
    let mut operation = fixture_replay_operation("op-failed-yaml", ReplayState::Failed, 0);
    operation.error_details = Some("source adapter failed".to_string());
    let server = mount_failed_replay_status_fixture(&operation).await;
    let client = fixture_gateway_client(&server)?;

    let result = execute_watch(&client, "op-failed-yaml", 0, &OutputFormat::Yaml).await;

    assert!(
        result.is_err(),
        "execute_watch with --format yaml must return Err on ReplayState::Failed, matching \
         the other formats (sinex-2bti) — the Yaml branch currently does a single unpolled \
         status fetch and never inspects state at all"
    );
    Ok(())
}

#[sinex_test]
async fn execute_watch_json_format_succeeds_on_replay_completed() -> TestResult<()> {
    // Guard the positive case alongside the failing one: a genuinely
    // successful terminal state must NOT be turned into an error by
    // whatever fix closes sinex-2bti.
    let operation = fixture_replay_operation("op-completed-json", ReplayState::Completed, 5);
    let server = mount_failed_replay_status_fixture(&operation).await;
    let client = fixture_gateway_client(&server)?;

    let result = execute_watch(&client, "op-completed-json", 0, &OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "execute_watch must still return Ok on ReplayState::Completed: {result:?}"
    );
    Ok(())
}
