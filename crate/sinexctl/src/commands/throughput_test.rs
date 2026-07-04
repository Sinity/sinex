use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::rpc::telemetry::{ThroughputComponentEntry, ThroughputSourceEntry};
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use xtask::sandbox::sinex_test;

fn throughput_response(
    per_source: Vec<ThroughputSourceEntry>,
    per_component: Vec<ThroughputComponentEntry>,
) -> TelemetryThroughputResponse {
    TelemetryThroughputResponse {
        per_source,
        per_component,
    }
}

fn source_entry(events_last_1h: i64, events_last_24h: i64) -> ThroughputSourceEntry {
    ThroughputSourceEntry {
        source: "terminal.atuin".to_string(),
        events_last_1h,
        events_last_24h,
        eps_1h: 0.1,
        eps_24h: 0.01,
    }
}

fn component_entry() -> ThroughputComponentEntry {
    ThroughputComponentEntry {
        component: "event_engine".to_string(),
        eps_1h: 1.0,
        eps_24h: 0.5,
    }
}

#[sinex_test]
async fn throughput_envelope_caveats_empty_read_models() -> xtask::TestResult<()> {
    let envelope = throughput_envelope(throughput_response(Vec::new(), Vec::new()));
    let caveat_ids: Vec<&str> = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect();

    assert_eq!(
        caveat_ids,
        vec!["coverage.unmeasurable", "coverage.unmeasurable"]
    );
    assert!(
        envelope.caveats[0]
            .message
            .contains("not proof that no sources are configured")
    );
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref()),
        Some("sinexctl metrics throughput")
    );
    Ok(())
}

#[sinex_test]
async fn throughput_envelope_caveats_all_zero_sources() -> xtask::TestResult<()> {
    let envelope = throughput_envelope(throughput_response(
        vec![source_entry(0, 0)],
        vec![component_entry()],
    ));

    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "window.partial");
    assert!(
        envelope.caveats[0]
            .message
            .contains("live capture may be idle")
    );
    Ok(())
}

#[sinex_test]
async fn throughput_envelope_renders_finite_json() -> xtask::TestResult<()> {
    let envelope = throughput_envelope(throughput_response(
        vec![source_entry(10, 100)],
        vec![component_entry()],
    ));
    let output = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must render a finite envelope");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.metrics.throughput");
    assert_eq!(parsed["payload"]["per_source"][0]["source"], "terminal.atuin");
    assert_eq!(
        parsed["payload"]["per_component"][0]["component"],
        "event_engine"
    );
    assert!(
        parsed.get("caveats").is_none(),
        "non-zero source throughput with component rows should not emit caveats"
    );
    Ok(())
}
