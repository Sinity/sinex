use super::*;
use sinex_primitives::rpc::telemetry::{EventEngineValidationSnapshot, GatewayStatsBucket};
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use xtask::sandbox::prelude::sinex_test;

fn gateway_bucket() -> GatewayStatsBucket {
    GatewayStatsBucket {
        bucket: "2026-06-19T18:00:00Z".to_string(),
        source: "sinex.gateway".to_string(),
        stat_events: 1,
        avg_total_requests: Some(3.0),
        total_rate_limited: Some(0),
        avg_latency_ms: Some(5.0),
        max_p99_latency_ms: Some(9.0),
    }
}

#[sinex_test]
async fn otel_projection_table_summarizes_gateway_metric_projection() -> xtask::TestResult<()> {
    let projection = gateway_stats_to_otel_metrics_projection(vec![gateway_bucket()]);

    let table = format_otel_metrics_projection_table(&projection);

    assert!(table.contains("sinex.otel.metrics-projection/v1"));
    assert!(table.contains("sinexctl.metrics.telemetry.gateway-stats"));
    assert!(table.contains("raw_event_payload"));
    Ok(())
}

#[sinex_test]
async fn telemetry_list_json_renders_finite_view_envelope() -> xtask::TestResult<()> {
    let envelope = telemetry_list_envelope(
        "sinexctl.metrics.telemetry.gateway-stats",
        "gateway_stats_bucket",
        vec![gateway_bucket()],
    );
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must return Some");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(
        parsed["source_surface"],
        "sinexctl.metrics.telemetry.gateway-stats"
    );
    assert_eq!(parsed["payload"]["schema_version"], TELEMETRY_LIST_SCHEMA_VERSION);
    assert_eq!(parsed["payload"]["row_kind"], "gateway_stats_bucket");
    assert_eq!(parsed["payload"]["count"], 1);
    assert_eq!(parsed["payload"]["rows"][0]["source"], "sinex.gateway");
    assert!(
        parsed.get("caveats").is_none(),
        "non-empty telemetry list should not emit readiness caveats"
    );
    Ok(())
}

#[sinex_test]
async fn telemetry_list_empty_rows_name_unmeasurable_window() -> xtask::TestResult<()> {
    let envelope = telemetry_list_envelope::<GatewayStatsBucket>(
        "sinexctl.metrics.telemetry.gateway-stats",
        "gateway_stats_bucket",
        Vec::new(),
    );

    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "coverage.unmeasurable");
    assert!(
        envelope.caveats[0].message.contains("empty read-model window"),
        "empty telemetry rows must not imply the source signal never existed"
    );
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref()),
        Some("sinexctl metrics telemetry gateway-stats")
    );
    Ok(())
}

#[sinex_test]
async fn telemetry_otel_projection_empty_metrics_is_unmeasurable()
-> xtask::TestResult<()> {
    let projection = gateway_stats_to_otel_metrics_projection(Vec::new());
    let envelope = telemetry_otel_envelope(projection);
    let output = crate::fmt::render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must return Some");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(
        parsed["source_surface"],
        "sinexctl.metrics.telemetry.gateway-stats.otel"
    );
    assert_eq!(parsed["caveats"][0]["id"], "coverage.unmeasurable");
    assert!(
        parsed["caveats"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("contains no data points"))
    );
    Ok(())
}

#[sinex_test]
async fn telemetry_validation_missing_snapshot_is_unmeasurable() -> xtask::TestResult<()> {
    let envelope = telemetry_snapshot_envelope::<EventEngineValidationSnapshot>(
        "sinexctl.metrics.telemetry.event-engine-validation",
        "event_engine_validation",
        None,
    );

    assert_eq!(envelope.payload.schema_version, TELEMETRY_SNAPSHOT_SCHEMA_VERSION);
    assert!(envelope.payload.snapshot.is_none());
    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "coverage.unmeasurable");
    assert!(
        envelope.caveats[0].message.contains("validation coverage"),
        "missing event-engine validation snapshot should name the missing proof"
    );
    Ok(())
}
