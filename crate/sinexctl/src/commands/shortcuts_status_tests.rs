use super::*;
use sinex_primitives::privacy::{
    PRIVATE_MODE_STATE_RELATIVE_PATH, RuntimePrivateModeState, save_private_mode_state,
};
use sinex_primitives::query::QueryResultEvent;
use sinex_primitives::rpc::sources::SourceReadinessCost;
use sinex_primitives::testing::event_fixture;
use sinex_primitives::views::{EVENT_ERROR_LIST_SCHEMA_VERSION, VIEW_ENVELOPE_SCHEMA_VERSION};
use xtask::sandbox::prelude::sinex_test;

fn readiness(status: SourceReadinessStatus) -> SourceReadiness {
    SourceReadiness {
        binding_id: None,
        source_family: "test".to_string(),
        source_id: None,
        parser_id: None,
        source_identifier: format!("test.{status:?}"),
        status,
        cost: SourceReadinessCost::LocalFast,
        freshness_seconds: None,
        material_count: 1,
        parsed_event_count: Some(1),
        last_success_at: None,
        caveats: Vec::new(),
        evidence: serde_json::Value::Null,
    }
}

fn error_event_fixture() -> QueryResultEvent {
    QueryResultEvent {
        event: event_fixture(
            sinex_primitives::EventSource::from_static("test"),
            sinex_primitives::EventType::from_static("test.error"),
            json!({ "message": "error: fixture" }),
        ),
        relevance_score: None,
        snippet: Some("error: fixture".to_string()),
    }
}

#[sinex_test]
async fn status_machine_output_uses_view_envelope_json() -> xtask::sandbox::TestResult<()> {
    let snapshot = RuntimeStatusSnapshot {
        target: RuntimeTargetDescriptor {
            name: "test-target".to_string(),
            kind: RuntimeTargetKind::Test,
            ..Default::default()
        },
        signals: vec![RuntimeStatusSignal {
            name: "gateway".to_string(),
            status: RuntimeStatusSignalStatus::Healthy,
            source: "fixture".to_string(),
            message: Some("ok".to_string()),
        }],
        warnings: Vec::new(),
    };
    let output = render_status_machine_output(&snapshot, OutputFormat::Json)?
        .ok_or_else(|| color_eyre::eyre::eyre!("json output expected"))?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.status");
    assert_eq!(value["payload"]["target"]["name"], "test-target");
    assert_eq!(value["payload"]["signals"][0]["name"], "gateway");
    Ok(())
}

#[sinex_test]
async fn status_machine_output_rejects_ndjson() -> xtask::sandbox::TestResult<()> {
    let snapshot = RuntimeStatusSnapshot::default();
    let result = render_status_machine_output(&snapshot, OutputFormat::Ndjson);
    assert!(result.is_err(), "status must remain a finite view");
    Ok(())
}

#[sinex_test]
async fn errors_machine_output_uses_view_envelope_json() -> xtask::sandbox::TestResult<()> {
    let cards = EventCardListView::from_query_events(&[error_event_fixture()]);
    let output = render_errors_machine_output(&cards, "24h", OutputFormat::Json)?
        .ok_or_else(|| color_eyre::eyre::eyre!("json output expected"))?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.events.errors");
    assert_eq!(value["query_echo"]["since"], "24h");
    assert_eq!(
        value["payload"]["schema_version"],
        EVENT_ERROR_LIST_SCHEMA_VERSION
    );
    assert_eq!(value["payload"]["since"], "24h");
    assert_eq!(value["payload"]["count"], 1);
    assert_eq!(value["payload"]["cards"][0]["event_type"], "test.error");
    Ok(())
}

#[sinex_test]
async fn errors_machine_output_rejects_ndjson() -> xtask::sandbox::TestResult<()> {
    let cards = EventCardListView::from_query_events(&[error_event_fixture()]);
    let result = render_errors_machine_output(&cards, "24h", OutputFormat::Ndjson);
    assert!(result.is_err(), "errors must remain a finite view");
    Ok(())
}

#[sinex_test]
async fn private_mode_status_signal_defaults_disabled() -> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let signal = private_mode_signal(Some(dir.path())).map_err(|warning| {
        color_eyre::eyre::eyre!("unexpected private-mode warning: {}", warning.message)
    })?;

    assert_eq!(signal.name, "private-mode");
    assert_eq!(signal.status, RuntimeStatusSignalStatus::Healthy);
    assert_eq!(signal.message.as_deref(), Some("disabled"));
    Ok(())
}

#[sinex_test]
async fn private_mode_status_signal_reports_enabled_scope() -> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let state = RuntimePrivateModeState::enabled_by(
        "operator",
        vec!["desktop".to_string(), "weechat".to_string()],
        Timestamp::UNIX_EPOCH,
    );
    save_private_mode_state(dir.path(), &state)?;

    let signal = private_mode_signal(Some(dir.path())).map_err(|warning| {
        color_eyre::eyre::eyre!("unexpected private-mode warning: {}", warning.message)
    })?;

    assert_eq!(signal.status, RuntimeStatusSignalStatus::Degraded);
    assert_eq!(
        signal.message.as_deref(),
        Some("enabled (scope: desktop,weechat, actor: operator)")
    );
    Ok(())
}

#[sinex_test]
async fn private_mode_unavailable_status_reports_fail_closed_privacy_caveat()
-> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let state_path = dir.path().join(PRIVATE_MODE_STATE_RELATIVE_PATH);
    std::fs::create_dir_all(state_path.parent().ok_or_else(|| {
        color_eyre::eyre::eyre!("private-mode state path should have a parent")
    })?)?;
    std::fs::write(&state_path, b"{not-valid-json")?;

    let warning = private_mode_signal(Some(dir.path()))
        .expect_err("malformed private-mode state should be unavailable");
    let privacy_signal = private_mode_unavailable_privacy_signal();
    let privacy_warning = private_mode_unavailable_privacy_warning();

    assert_eq!(warning.source, "private-mode");
    assert_eq!(privacy_signal.name, "privacy-private-mode");
    assert_eq!(privacy_signal.status, RuntimeStatusSignalStatus::Degraded);
    assert!(
        privacy_signal
            .message
            .as_deref()
            .is_some_and(|message| message.contains("fail closed"))
    );
    assert_eq!(privacy_warning.source, "privacy.private-mode");
    assert!(privacy_warning.message.contains("high-sensitivity"));
    assert!(!privacy_warning.message.contains("payload"));
    assert!(!privacy_warning.message.contains("sample"));
    Ok(())
}

#[sinex_test]
async fn privacy_dlq_status_is_quiet_when_backlog_empty() -> xtask::sandbox::TestResult<()> {
    assert!(privacy_dlq_signal(0).is_none());
    assert!(privacy_dlq_warning(0).is_none());
    Ok(())
}

#[sinex_test]
async fn privacy_dlq_status_reports_sanitized_backlog() -> xtask::sandbox::TestResult<()> {
    let signal = privacy_dlq_signal(3)
        .ok_or_else(|| color_eyre::eyre::eyre!("privacy DLQ signal expected"))?;
    let warning = privacy_dlq_warning(3)
        .ok_or_else(|| color_eyre::eyre::eyre!("privacy DLQ warning expected"))?;

    assert_eq!(signal.name, "privacy-dlq");
    assert_eq!(signal.status, RuntimeStatusSignalStatus::Degraded);
    assert_eq!(
        signal.message.as_deref(),
        Some("3 raw DLQ message(s) require sanitized inspection")
    );
    assert_eq!(warning.source, "privacy.dlq");
    assert!(!warning.message.contains("payload"));
    assert!(!warning.message.contains("sample"));
    Ok(())
}

#[sinex_test]
async fn source_readiness_status_reports_capture_gap_counts() -> xtask::sandbox::TestResult<()>
{
    let summary = summarize_source_readiness(&[
        readiness(SourceReadinessStatus::Available),
        readiness(SourceReadinessStatus::Disabled),
        readiness(SourceReadinessStatus::Partial),
        readiness(SourceReadinessStatus::Stale),
        readiness(SourceReadinessStatus::Error),
        readiness(SourceReadinessStatus::Missing),
        readiness(SourceReadinessStatus::Blocked),
        readiness(SourceReadinessStatus::Unknown),
    ]);
    let signal = source_readiness_signal(&summary);
    let warning = source_readiness_warning(&summary)
        .ok_or_else(|| color_eyre::eyre::eyre!("source readiness warning expected"))?;

    assert_eq!(summary.degraded_count(), 6);
    assert_eq!(summary.blocking_count(), 3);
    assert_eq!(signal.name, "source-readiness");
    assert_eq!(signal.status, RuntimeStatusSignalStatus::Unhealthy);
    let message = signal.message.as_deref().ok_or_else(|| {
        color_eyre::eyre::eyre!("source readiness signal should explain counts")
    })?;
    assert!(message.contains("partial=1"));
    assert!(message.contains("stale=1"));
    assert!(message.contains("error=1"));
    assert!(message.contains("missing=1"));
    assert!(message.contains("blocked=1"));
    assert!(message.contains("unknown=1"));
    assert_eq!(warning.source, "sources.readiness");
    assert!(warning.message.contains("capture readiness"));
    Ok(())
}

#[sinex_test]
async fn source_readiness_status_is_healthy_when_available_or_disabled()
-> xtask::sandbox::TestResult<()> {
    let summary = summarize_source_readiness(&[
        readiness(SourceReadinessStatus::Available),
        readiness(SourceReadinessStatus::Disabled),
    ]);
    let signal = source_readiness_signal(&summary);

    assert_eq!(signal.status, RuntimeStatusSignalStatus::Healthy);
    assert!(source_readiness_warning(&summary).is_none());
    assert_eq!(
        signal.message.as_deref(),
        Some("1 source(s) available, 1 disabled")
    );
    Ok(())
}
