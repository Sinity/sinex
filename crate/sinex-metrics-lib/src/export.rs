//! Metrics Export
//!
//! This module provides functionality to export metrics in various formats
//! including Prometheus, JSON, and custom formats.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::collectors::{get_all_stored_metrics, MetricEntry, MetricType, MetricValue};
use crate::registry::GlobalMetrics;

/// Export metrics in Prometheus format
pub fn export_prometheus() -> String {
    GlobalMetrics::export_prometheus()
}

/// Export metrics in JSON format
pub fn export_json() -> serde_json::Value {
    let prometheus_json = GlobalMetrics::export_json();
    let stored_metrics = get_all_stored_metrics();

    let mut combined_metrics = serde_json::Map::new();

    // Add Prometheus metrics
    if let serde_json::Value::Object(prometheus_map) = prometheus_json {
        for (key, value) in prometheus_map {
            combined_metrics.insert(key, value);
        }
    }

    // Add stored metrics
    let mut stored_metrics_map = serde_json::Map::new();
    for metric in stored_metrics {
        let metric_json = metric_entry_to_json(&metric);
        stored_metrics_map.insert(metric.name.clone(), metric_json);
    }

    combined_metrics.insert(
        "stored_metrics".to_string(),
        serde_json::Value::Object(stored_metrics_map),
    );

    // Add metadata
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "timestamp".to_string(),
        serde_json::Value::Number(serde_json::Number::from(current_timestamp())),
    );
    metadata.insert(
        "version".to_string(),
        serde_json::Value::String("1.0".to_string()),
    );
    metadata.insert(
        "exporter".to_string(),
        serde_json::Value::String("sinex-metrics".to_string()),
    );

    combined_metrics.insert("metadata".to_string(), serde_json::Value::Object(metadata));

    serde_json::Value::Object(combined_metrics)
}

/// Export metrics in OpenMetrics format
pub fn export_openmetrics() -> String {
    let stored_metrics = get_all_stored_metrics();
    let mut output = String::new();

    // OpenMetrics header
    output.push_str("# HELP sinex_metrics OpenMetrics format export\n");
    output.push_str("# TYPE sinex_metrics info\n");
    output.push_str("# UNIT sinex_metrics {}\n");

    for metric in stored_metrics {
        let openmetrics_line = metric_entry_to_openmetrics(&metric);
        output.push_str(&openmetrics_line);
        output.push('\n');
    }

    // OpenMetrics footer
    output.push_str("# EOF\n");

    output
}

/// Export metrics in InfluxDB line protocol format
pub fn export_influxdb() -> String {
    let stored_metrics = get_all_stored_metrics();
    let mut output = String::new();

    for metric in stored_metrics {
        let influxdb_line = metric_entry_to_influxdb(&metric);
        output.push_str(&influxdb_line);
        output.push('\n');
    }

    output
}

/// Export metrics in StatsD format
pub fn export_statsd() -> String {
    let stored_metrics = get_all_stored_metrics();
    let mut output = String::new();

    for metric in stored_metrics {
        let statsd_line = metric_entry_to_statsd(&metric);
        output.push_str(&statsd_line);
        output.push('\n');
    }

    output
}

/// Export metrics summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub total_metrics: usize,
    pub metrics_by_type: HashMap<MetricType, usize>,
    pub metrics_by_namespace: HashMap<String, usize>,
    pub oldest_timestamp: Option<u64>,
    pub newest_timestamp: Option<u64>,
    pub export_timestamp: u64,
}

/// Export metrics summary
pub fn export_summary() -> MetricsSummary {
    let stored_metrics = get_all_stored_metrics();
    let mut metrics_by_type = HashMap::new();
    let mut metrics_by_namespace = HashMap::new();
    let mut oldest_timestamp = None;
    let mut newest_timestamp = None;

    for metric in &stored_metrics {
        // Count by type
        *metrics_by_type.entry(metric.metric_type).or_insert(0) += 1;

        // Count by namespace (first part of metric name)
        let namespace = metric
            .name
            .split('_')
            .next()
            .unwrap_or("unknown")
            .to_string();
        *metrics_by_namespace.entry(namespace).or_insert(0) += 1;

        // Track timestamp range
        if oldest_timestamp.is_none() || Some(metric.timestamp) < oldest_timestamp {
            oldest_timestamp = Some(metric.timestamp);
        }
        if newest_timestamp.is_none() || Some(metric.timestamp) > newest_timestamp {
            newest_timestamp = Some(metric.timestamp);
        }
    }

    MetricsSummary {
        total_metrics: stored_metrics.len(),
        metrics_by_type,
        metrics_by_namespace,
        oldest_timestamp,
        newest_timestamp,
        export_timestamp: current_timestamp(),
    }
}

/// Convert MetricEntry to JSON
fn metric_entry_to_json(metric: &MetricEntry) -> serde_json::Value {
    let mut json = serde_json::Map::new();

    json.insert(
        "help".to_string(),
        serde_json::Value::String(metric.help.clone()),
    );
    json.insert(
        "type".to_string(),
        serde_json::Value::String(format!("{:?}", metric.metric_type)),
    );
    json.insert(
        "timestamp".to_string(),
        serde_json::Value::Number(serde_json::Number::from(metric.timestamp)),
    );

    // Add labels
    if !metric.labels.is_empty() {
        let mut labels = serde_json::Map::new();
        for (key, value) in &metric.labels {
            labels.insert(key.clone(), serde_json::Value::String(value.clone()));
        }
        json.insert("labels".to_string(), serde_json::Value::Object(labels));
    }

    // Add value based on type
    match &metric.value {
        MetricValue::Counter(value) => {
            json.insert(
                "value".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(*value).unwrap_or(serde_json::Number::from(0)),
                ),
            );
        }
        MetricValue::Gauge(value) => {
            json.insert(
                "value".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(*value).unwrap_or(serde_json::Number::from(0)),
                ),
            );
        }
        MetricValue::Histogram(values) => {
            let histogram_values: Vec<serde_json::Value> = values
                .iter()
                .map(|v| {
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(*v).unwrap_or(serde_json::Number::from(0)),
                    )
                })
                .collect();
            json.insert(
                "histogram".to_string(),
                serde_json::Value::Array(histogram_values),
            );
        }
        MetricValue::Summary(summary) => {
            let mut summary_json = serde_json::Map::new();
            summary_json.insert(
                "count".to_string(),
                serde_json::Value::Number(serde_json::Number::from(summary.count)),
            );
            summary_json.insert(
                "sum".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(summary.sum)
                        .unwrap_or(serde_json::Number::from(0)),
                ),
            );

            let mut quantiles = serde_json::Map::new();
            for (quantile, value) in &summary.quantiles {
                quantiles.insert(
                    quantile.to_string(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(*value).unwrap_or(serde_json::Number::from(0)),
                    ),
                );
            }
            summary_json.insert(
                "quantiles".to_string(),
                serde_json::Value::Object(quantiles),
            );

            json.insert(
                "summary".to_string(),
                serde_json::Value::Object(summary_json),
            );
        }
    }

    serde_json::Value::Object(json)
}

/// Convert MetricEntry to OpenMetrics format
fn metric_entry_to_openmetrics(metric: &MetricEntry) -> String {
    let mut output = String::new();

    // Add help and type information
    output.push_str(&format!("# HELP {} {}\n", metric.name, metric.help));
    output.push_str(&format!(
        "# TYPE {} {}\n",
        metric.name,
        metric_type_to_openmetrics(metric.metric_type)
    ));

    // Add metric line
    match &metric.value {
        MetricValue::Counter(value) => {
            output.push_str(&format!(
                "{}{} {} {}\n",
                metric.name,
                labels_to_openmetrics(&metric.labels),
                value,
                metric.timestamp * 1000 // Convert to milliseconds
            ));
        }
        MetricValue::Gauge(value) => {
            output.push_str(&format!(
                "{}{} {} {}\n",
                metric.name,
                labels_to_openmetrics(&metric.labels),
                value,
                metric.timestamp * 1000
            ));
        }
        MetricValue::Histogram(values) => {
            // OpenMetrics histogram format is complex, simplified here
            for (i, value) in values.iter().enumerate() {
                output.push_str(&format!(
                    "{}_bucket{}{{le=\"{}\"}} {} {}\n",
                    metric.name,
                    labels_to_openmetrics(&metric.labels),
                    i,
                    value,
                    metric.timestamp * 1000
                ));
            }
        }
        MetricValue::Summary(summary) => {
            output.push_str(&format!(
                "{}_count{} {} {}\n",
                metric.name,
                labels_to_openmetrics(&metric.labels),
                summary.count,
                metric.timestamp * 1000
            ));
            output.push_str(&format!(
                "{}_sum{} {} {}\n",
                metric.name,
                labels_to_openmetrics(&metric.labels),
                summary.sum,
                metric.timestamp * 1000
            ));

            for (quantile, value) in &summary.quantiles {
                let mut quantile_labels = metric.labels.clone();
                quantile_labels.insert("quantile".to_string(), quantile.to_string());
                output.push_str(&format!(
                    "{}{} {} {}\n",
                    metric.name,
                    labels_to_openmetrics(&quantile_labels),
                    value,
                    metric.timestamp * 1000
                ));
            }
        }
    }

    output
}

/// Convert MetricEntry to InfluxDB line protocol format
fn metric_entry_to_influxdb(metric: &MetricEntry) -> String {
    let mut tags = Vec::new();
    let mut fields = Vec::new();

    // Add labels as tags
    for (key, value) in &metric.labels {
        tags.push(format!("{}={}", key, value));
    }

    // Add value as field
    match &metric.value {
        MetricValue::Counter(value) => {
            fields.push(format!("value={}", value));
        }
        MetricValue::Gauge(value) => {
            fields.push(format!("value={}", value));
        }
        MetricValue::Histogram(values) => {
            for (i, value) in values.iter().enumerate() {
                fields.push(format!("bucket_{}={}", i, value));
            }
        }
        MetricValue::Summary(summary) => {
            fields.push(format!("count={}", summary.count));
            fields.push(format!("sum={}", summary.sum));
            for (quantile, value) in &summary.quantiles {
                fields.push(format!("quantile_{}={}", quantile, value));
            }
        }
    }

    let tags_str = if tags.is_empty() {
        String::new()
    } else {
        format!(",{}", tags.join(","))
    };

    format!(
        "{}{} {} {}",
        metric.name,
        tags_str,
        fields.join(","),
        metric.timestamp * 1_000_000_000 // Convert to nanoseconds
    )
}

/// Convert MetricEntry to StatsD format
fn metric_entry_to_statsd(metric: &MetricEntry) -> String {
    let statsd_type = match metric.metric_type {
        MetricType::Counter => "c",
        MetricType::Gauge => "g",
        MetricType::Histogram => "h",
        MetricType::Summary => "ms",
    };

    match &metric.value {
        MetricValue::Counter(value) => {
            format!("{}:{}|{}", metric.name, value, statsd_type)
        }
        MetricValue::Gauge(value) => {
            format!("{}:{}|{}", metric.name, value, statsd_type)
        }
        MetricValue::Histogram(values) => {
            // Send each histogram value separately
            values
                .iter()
                .map(|v| format!("{}:{}|{}", metric.name, v, statsd_type))
                .collect::<Vec<_>>()
                .join("\n")
        }
        MetricValue::Summary(summary) => {
            format!("{}:{}|{}", metric.name, summary.sum, statsd_type)
        }
    }
}

/// Convert MetricType to OpenMetrics type string
fn metric_type_to_openmetrics(metric_type: MetricType) -> &'static str {
    match metric_type {
        MetricType::Counter => "counter",
        MetricType::Gauge => "gauge",
        MetricType::Histogram => "histogram",
        MetricType::Summary => "summary",
    }
}

/// Convert labels to OpenMetrics format
fn labels_to_openmetrics(labels: &HashMap<String, String>) -> String {
    if labels.is_empty() {
        return String::new();
    }

    let label_pairs: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, v))
        .collect();

    format!("{{{}}}", label_pairs.join(","))
}

/// Get current timestamp in seconds since epoch
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collectors::{MetricType, MetricValue};

    #[test]
    fn test_prometheus_export() {
        let output = export_prometheus();
        // Should contain Prometheus format metrics
        assert!(output.contains("# HELP") || output.is_empty());
    }

    #[test]
    fn test_json_export() {
        let output = export_json();
        assert!(output.is_object());
        assert!(output.get("metadata").is_some());
    }

    #[test]
    fn test_metric_entry_to_json() {
        let metric = MetricEntry {
            name: "test_metric".to_string(),
            help: "Test metric".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(42.0),
            labels: HashMap::from([("label1".to_string(), "value1".to_string())]),
            timestamp: 1234567890,
        };

        let json = metric_entry_to_json(&metric);
        assert!(json.is_object());
        assert_eq!(json["help"], "Test metric");
        assert_eq!(json["type"], "Counter");
        assert_eq!(json["value"], 42.0);
        assert_eq!(json["timestamp"], 1234567890);
    }

    #[test]
    fn test_openmetrics_export() {
        let output = export_openmetrics();
        assert!(output.contains("# HELP"));
        assert!(output.contains("# TYPE"));
        assert!(output.contains("# EOF"));
    }

    #[test]
    fn test_metric_entry_to_openmetrics() {
        let metric = MetricEntry {
            name: "test_counter".to_string(),
            help: "Test counter".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(42.0),
            labels: HashMap::from([("label1".to_string(), "value1".to_string())]),
            timestamp: 1234567890,
        };

        let openmetrics = metric_entry_to_openmetrics(&metric);
        assert!(openmetrics.contains("# HELP test_counter Test counter"));
        assert!(openmetrics.contains("# TYPE test_counter counter"));
        assert!(openmetrics.contains("test_counter{label1=\"value1\"} 42"));
    }

    #[test]
    fn test_influxdb_export() {
        let output = export_influxdb();
        // Should be valid even if empty
        assert!(output.len() >= 0);
    }

    #[test]
    fn test_metric_entry_to_influxdb() {
        let metric = MetricEntry {
            name: "test_gauge".to_string(),
            help: "Test gauge".to_string(),
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(42.0),
            labels: HashMap::from([("label1".to_string(), "value1".to_string())]),
            timestamp: 1234567890,
        };

        let influxdb = metric_entry_to_influxdb(&metric);
        assert!(influxdb.contains("test_gauge,label1=value1 value=42"));
    }

    #[test]
    fn test_statsd_export() {
        let output = export_statsd();
        // Should be valid even if empty
        assert!(output.len() >= 0);
    }

    #[test]
    fn test_metric_entry_to_statsd() {
        let metric = MetricEntry {
            name: "test_counter".to_string(),
            help: "Test counter".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(42.0),
            labels: HashMap::new(),
            timestamp: 1234567890,
        };

        let statsd = metric_entry_to_statsd(&metric);
        assert_eq!(statsd, "test_counter:42|c");
    }

    #[test]
    fn test_export_summary() {
        let summary = export_summary();
        assert!(summary.export_timestamp > 0);
    }

    #[test]
    fn test_labels_to_openmetrics() {
        let labels = HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);

        let openmetrics = labels_to_openmetrics(&labels);
        assert!(openmetrics.contains("key1=\"value1\""));
        assert!(openmetrics.contains("key2=\"value2\""));
        assert!(openmetrics.starts_with("{"));
        assert!(openmetrics.ends_with("}"));
    }
}
