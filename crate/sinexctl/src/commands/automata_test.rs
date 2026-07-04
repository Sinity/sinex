#![allow(clippy::unwrap_used)]

use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::domain::ModuleName;
use sinex_primitives::rpc::automata::{AutomataStatusResponse, AutomatonStatus};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use xtask::sandbox::sinex_test;

fn fixture_response(automata: Vec<AutomatonStatus>) -> AutomataStatusResponse {
    AutomataStatusResponse {
        generated_at: Timestamp::now(),
        stale_after_secs: 300,
        recent_window_secs: 300,
        automata,
    }
}

fn fixture_automaton(name: &str) -> AutomatonStatus {
    AutomatonStatus {
        module_name: ModuleName::new(name),
        version: "test-version".to_string(),
        description: None,
        manifest_status: "registered".to_string(),
        live: true,
        service_name: Some(name.to_string()),
        instance_id: Some(format!("{name}-instance")),
        module_run_id: None,
        host: Some("testhost".to_string()),
        run_status: Some("running".to_string()),
        started_at: Some(Timestamp::now()),
        last_heartbeat_at: Some(Timestamp::now()),
        events_processed_current_run: Some(42),
        checkpoint_kind: Some("consumer".to_string()),
        checkpoint_position: Some("42".to_string()),
        checkpoint_revision: Some(7),
        checkpoint_recorded_at: Some(Timestamp::now()),
        pending_invalidation_count: Some(0),
        error_rate_5m: Some(0.0),
        event_lag_p50_ms: Some(1.0),
        event_lag_p99_ms: Some(2.0),
        tick_runtime_p99_ms: Some(3.0),
        throughput_eps: Some(4.0),
        recent_output_count: 1,
        last_output_at: Some(Timestamp::now()),
        last_replay_at: None,
    }
}

#[sinex_test]
async fn automata_status_json_renders_finite_view_envelope() -> xtask::TestResult<()> {
    let envelope = automata_status_envelope(fixture_response(vec![fixture_automaton(
        "session-detector",
    )]));
    let output = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must return Some");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.runtime.automata");
    assert_eq!(parsed["payload"]["automata"][0]["module_name"], "session-detector");
    assert!(
        parsed.get("caveats").is_none(),
        "live automata with recent output should not emit readiness caveats"
    );
    Ok(())
}

#[sinex_test]
async fn automata_status_empty_response_names_absent_source() -> xtask::TestResult<()> {
    let envelope = automata_status_envelope(fixture_response(Vec::new()));

    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "source.absent");
    assert!(
        envelope.caveats[0]
            .message
            .contains("no automata are registered"),
        "empty aggregate must explain that the automata source set is absent"
    );
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.rpc_method.as_deref()),
        Some("automata.status")
    );
    Ok(())
}

#[sinex_test]
async fn automata_status_caveats_name_stale_and_missing_output() -> xtask::TestResult<()> {
    let mut stale = fixture_automaton("analytics");
    stale.live = false;

    let mut quiet = fixture_automaton("health-aggregator");
    quiet.recent_output_count = 0;
    quiet.last_output_at = None;

    let envelope = automata_status_envelope(fixture_response(vec![stale, quiet]));
    let ids = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect::<Vec<_>>();

    assert!(
        ids.contains(&"window.partial"),
        "non-live automata must mark the live automata window partial"
    );
    assert!(
        ids.contains(&"coverage.unmeasurable"),
        "automata without recent outputs must be called out as unmeasured coverage"
    );
    assert!(
        envelope
            .caveats
            .iter()
            .any(|caveat| caveat.message.contains("analytics")),
        "stale caveat should name at least one affected automaton"
    );
    assert!(
        envelope
            .caveats
            .iter()
            .any(|caveat| caveat.message.contains("health-aggregator")),
        "missing-output caveat should name at least one affected automaton"
    );
    Ok(())
}
