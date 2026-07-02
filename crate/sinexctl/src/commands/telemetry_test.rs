use super::*;
use sinex_primitives::rpc::telemetry::GatewayStatsBucket;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn otel_projection_table_summarizes_gateway_metric_projection() -> xtask::TestResult<()> {
    let projection = gateway_stats_to_otel_metrics_projection(vec![GatewayStatsBucket {
        bucket: "2026-06-19T18:00:00Z".to_string(),
        source: "sinex.gateway".to_string(),
        stat_events: 1,
        avg_total_requests: Some(3.0),
        total_rate_limited: Some(0),
        avg_latency_ms: Some(5.0),
        max_p99_latency_ms: Some(9.0),
    }]);

    let table = format_otel_metrics_projection_table(&projection);

    assert!(table.contains("sinex.otel.metrics-projection/v1"));
    assert!(table.contains("sinexctl.metrics.telemetry.gateway-stats"));
    assert!(table.contains("raw_event_payload"));
    Ok(())
}
