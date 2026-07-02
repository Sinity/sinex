use super::*;
use xtask::sandbox::prelude::sinex_test;

fn gateway_bucket() -> GatewayStatsBucket {
    GatewayStatsBucket {
        bucket: "2026-06-19T18:00:00Z".to_string(),
        source: "sinex.gateway".to_string(),
        stat_events: 2,
        avg_total_requests: Some(10.0),
        total_rate_limited: Some(1),
        avg_latency_ms: Some(7.5),
        max_p99_latency_ms: Some(20.0),
    }
}

#[sinex_test]
async fn gateway_stats_projection_maps_existing_telemetry_to_metrics()
-> xtask::sandbox::TestResult<()> {
    let view = gateway_stats_to_otel_metrics_projection(vec![gateway_bucket()]);

    assert_eq!(view.schema_version, OTEL_METRICS_PROJECTION_SCHEMA_VERSION);
    assert_eq!(view.signal, OtelSignalKind::Metrics);
    assert_eq!(view.metric_count(), 5);
    assert_eq!(view.point_count(), 5);
    assert!(view.metrics.iter().any(|metric| metric.name
        == "sinex.gateway.requests.rate_limited"
        && metric.kind == OtelMetricKind::Sum
        && metric.aggregation_temporality == OtelAggregationTemporality::Delta));
    Ok(())
}

#[sinex_test]
async fn gateway_stats_projection_uses_refs_counts_and_timings_not_raw_payloads()
-> serde_json::Result<()> {
    let view = gateway_stats_to_otel_metrics_projection(vec![gateway_bucket()]);
    let serialized = serde_json::to_string(&view)?;

    assert!(view.disclosure.policy.contains("telemetry disclosure"));
    assert!(serialized.contains("sinex.source"));
    assert!(serialized.contains("sinex.gateway.latency.average"));
    assert!(!serialized.contains("raw_payload"));
    assert!(!serialized.contains("email_body_value"));
    assert!(!serialized.contains("terminal_command_value"));
    assert!(
        view.disclosure
            .omitted_attribute_families
            .contains(&"raw_event_payload".to_string())
    );
    Ok(())
}
