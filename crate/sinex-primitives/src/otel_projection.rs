//! OpenTelemetry-compatible projection DTOs.
//!
//! These types describe export/read-model projections from Sinex telemetry
//! DTOs into the OpenTelemetry metrics data model. They are not canonical
//! Sinex storage, not an OTLP wire encoder, and not an internal telemetry SDK.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::rpc::telemetry::GatewayStatsBucket;

pub const OTEL_METRICS_PROJECTION_SCHEMA_VERSION: &str = "sinex.otel.metrics-projection/v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OtelSignalKind {
    Metrics,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OtelMetricKind {
    Gauge,
    Sum,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OtelAggregationTemporality {
    Delta,
    Unspecified,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum OtelAttributeValue {
    String(String),
    I64(i64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OtelAttribute {
    pub key: String,
    pub value: OtelAttributeValue,
}

impl OtelAttribute {
    #[must_use]
    pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: OtelAttributeValue::String(value.into()),
        }
    }

    #[must_use]
    pub fn i64(key: impl Into<String>, value: i64) -> Self {
        Self {
            key: key.into(),
            value: OtelAttributeValue::I64(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OtelNumberDataPoint {
    pub time: String,
    pub value: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attributes: Vec<OtelAttribute>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OtelMetricProjection {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub kind: OtelMetricKind,
    pub aggregation_temporality: OtelAggregationTemporality,
    pub data_points: Vec<OtelNumberDataPoint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OtelDisclosureBoundary {
    pub policy: String,
    pub omitted_attribute_families: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct OtelMetricsProjectionView {
    pub schema_version: String,
    pub signal: OtelSignalKind,
    pub source_surface: String,
    pub source_response: String,
    pub resource_attributes: Vec<OtelAttribute>,
    pub disclosure: OtelDisclosureBoundary,
    pub metrics: Vec<OtelMetricProjection>,
}

impl OtelMetricsProjectionView {
    #[must_use]
    pub fn metric_count(&self) -> usize {
        self.metrics.len()
    }

    #[must_use]
    pub fn point_count(&self) -> usize {
        self.metrics
            .iter()
            .map(|metric| metric.data_points.len())
            .sum()
    }
}

#[must_use]
pub fn gateway_stats_to_otel_metrics_projection(
    buckets: Vec<GatewayStatsBucket>,
) -> OtelMetricsProjectionView {
    let request_avg = metric_from_gateway_buckets(
        "sinex.gateway.requests.average",
        "Average gateway requests observed in each telemetry bucket.",
        "{request}",
        OtelMetricKind::Gauge,
        OtelAggregationTemporality::Unspecified,
        &buckets,
        |bucket| bucket.avg_total_requests,
    );
    let rate_limited = metric_from_gateway_buckets(
        "sinex.gateway.requests.rate_limited",
        "Rate-limited gateway requests counted in each telemetry bucket.",
        "{request}",
        OtelMetricKind::Sum,
        OtelAggregationTemporality::Delta,
        &buckets,
        |bucket| bucket.total_rate_limited.map(|value| value as f64),
    );
    let avg_latency = metric_from_gateway_buckets(
        "sinex.gateway.latency.average",
        "Average gateway latency observed in each telemetry bucket.",
        "ms",
        OtelMetricKind::Gauge,
        OtelAggregationTemporality::Unspecified,
        &buckets,
        |bucket| bucket.avg_latency_ms,
    );
    let max_p99_latency = metric_from_gateway_buckets(
        "sinex.gateway.latency.p99.max",
        "Maximum p99 gateway latency observed in each telemetry bucket.",
        "ms",
        OtelMetricKind::Gauge,
        OtelAggregationTemporality::Unspecified,
        &buckets,
        |bucket| bucket.max_p99_latency_ms,
    );
    let stat_events = metric_from_gateway_buckets(
        "sinex.gateway.stat_events",
        "Gateway stat events contributing to each telemetry bucket.",
        "{event}",
        OtelMetricKind::Sum,
        OtelAggregationTemporality::Delta,
        &buckets,
        |bucket| Some(bucket.stat_events as f64),
    );

    OtelMetricsProjectionView {
        schema_version: OTEL_METRICS_PROJECTION_SCHEMA_VERSION.to_string(),
        signal: OtelSignalKind::Metrics,
        source_surface: "sinexctl.metrics.telemetry.gateway-stats".to_string(),
        source_response: "TelemetryGatewayStatsResponse".to_string(),
        resource_attributes: vec![
            OtelAttribute::string("service.name", "sinex"),
            OtelAttribute::string(
                "sinex.telemetry.source_surface",
                "sinex_telemetry.gateway_stats_1h",
            ),
        ],
        disclosure: default_otel_disclosure_boundary(),
        metrics: vec![
            request_avg,
            rate_limited,
            avg_latency,
            max_p99_latency,
            stat_events,
        ],
    }
}

fn metric_from_gateway_buckets(
    name: &str,
    description: &str,
    unit: &str,
    kind: OtelMetricKind,
    aggregation_temporality: OtelAggregationTemporality,
    buckets: &[GatewayStatsBucket],
    value: impl Fn(&GatewayStatsBucket) -> Option<f64>,
) -> OtelMetricProjection {
    let data_points = buckets
        .iter()
        .filter_map(|bucket| {
            value(bucket).map(|value| OtelNumberDataPoint {
                time: bucket.bucket.clone(),
                value,
                attributes: gateway_bucket_attributes(bucket),
            })
        })
        .collect();

    OtelMetricProjection {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        kind,
        aggregation_temporality,
        data_points,
    }
}

fn gateway_bucket_attributes(bucket: &GatewayStatsBucket) -> Vec<OtelAttribute> {
    vec![
        OtelAttribute::string("sinex.source", bucket.source.clone()),
        OtelAttribute::string("sinex.telemetry.bucket", bucket.bucket.clone()),
        OtelAttribute::i64("sinex.telemetry.stat_events", bucket.stat_events),
    ]
}

fn default_otel_disclosure_boundary() -> OtelDisclosureBoundary {
    OtelDisclosureBoundary {
        policy: "telemetry disclosure: project stable refs, counts, timings, and bounded aggregate attributes only"
            .to_string(),
        omitted_attribute_families: vec![
            "raw_event_payload".to_string(),
            "raw_material_bytes".to_string(),
            "source_material_payload".to_string(),
            "dlq_payload".to_string(),
            "browser_url".to_string(),
            "email_body".to_string(),
            "terminal_command_text".to_string(),
            "ocr_or_transcript_text".to_string(),
            "private_log_body".to_string(),
        ],
    }
}

#[cfg(test)]
#[path = "otel_projection_test.rs"]
mod tests;
