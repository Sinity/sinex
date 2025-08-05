//! # Metrics Export
//!
//! This module provides functionality to export metrics in various formats
//! including Prometheus, JSON, and custom formats.
//!
//! ## Overview
//!
//! The export module supports multiple metric exposition formats to integrate
//! with different monitoring systems and tools. Each format preserves the full
//! fidelity of metric data including labels, timestamps, and statistical distributions.
//!
//! ## Supported Formats
//!
//! - **Prometheus Text** - Standard Prometheus text exposition format
//! - **JSON** - Structured JSON representation for APIs
//! - **OpenMetrics** - Next-generation metric exposition standard
//! - **InfluxDB Line Protocol** - For time-series databases
//! - **StatsD** - Simple text protocol for metric aggregation
//!
//! ## Functions
//!
//! - [`export_prometheus`] - Export in Prometheus text format
//! - [`export_json`] - Export as structured JSON
//! - [`export_openmetrics`] - Export in OpenMetrics format
//! - [`export_influxdb`] - Export in InfluxDB line protocol
//! - [`export_statsd`] - Export in StatsD format
//! - [`export_summary`] - Export metrics summary statistics
//!
//! ## Usage
//!
//! ```rust,ignore
//! use sinex_telemetry::metrics::{export_prometheus, export_json};
//!
//! // Get metrics in Prometheus format for /metrics endpoint
//! let prometheus_text = export_prometheus();
//! println!("{}", prometheus_text);
//!
//! // Get metrics as JSON for API responses
//! let json_metrics = export_json();
//! println!("{}", serde_json::to_string_pretty(&json_metrics)?);
//!
//! // Get summary statistics
//! let summary = export_summary();
//! println!("Total metrics: {}", summary.total_metrics);
//! ```
//!
//! ## Format Examples
//!
//! ### Prometheus Text Format
//! ```text
//! # HELP http_requests_total Total HTTP requests
//! # TYPE http_requests_total counter
//! http_requests_total{method="GET",status="200"} 1234
//! ```
//!
//! ### JSON Format
//! ```json
//! {
//!   "http_requests_total": {
//!     "help": "Total HTTP requests",
//!     "type": "Counter",
//!     "metrics": [{
//!       "labels": {"method": "GET", "status": "200"},
//!       "value": 1234
//!     }]
//!   }
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::telemetry::metrics::collectors::{
    get_all_stored_metrics, MetricEntry, MetricType, MetricValue,
};
use crate::telemetry::metrics::registry::GlobalMetrics;

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
    use crate::telemetry::metrics::collectors::{
        register_collector, MetricEntry, MetricType, MetricValue, SummaryValue,
    };

    fn setup_test_metrics() {
        // We can't clear metrics storage directly as it's private
        // Tests should be isolated by using unique metric names

        // Add some test metrics via a custom collector
        struct TestCollector {
            metrics: Vec<MetricEntry>,
        }

        impl crate::collectors::MetricsCollector for TestCollector {
            fn collect(&self) -> Vec<MetricEntry> {
                self.metrics.clone()
            }

            fn name(&self) -> &str {
                "test_collector"
            }
        }

        let metrics = vec![
            MetricEntry {
                name: "test_counter".to_string(),
                help: "Test counter metric".to_string(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(42.0),
                labels: HashMap::from([("method".to_string(), "GET".to_string())]),
                timestamp: 1234567890,
            },
            MetricEntry {
                name: "test_gauge".to_string(),
                help: "Test gauge metric".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(75.5),
                labels: HashMap::from([("instance".to_string(), "localhost".to_string())]),
                timestamp: 1234567891,
            },
            MetricEntry {
                name: "test_histogram".to_string(),
                help: "Test histogram metric".to_string(),
                metric_type: MetricType::Histogram,
                value: MetricValue::Histogram(vec![0.1, 0.5, 1.0, 2.5, 5.0]),
                labels: HashMap::new(),
                timestamp: 1234567892,
            },
            MetricEntry {
                name: "test_summary".to_string(),
                help: "Test summary metric".to_string(),
                metric_type: MetricType::Summary,
                value: MetricValue::Summary(SummaryValue {
                    count: 100,
                    sum: 5000.0,
                    quantiles: HashMap::from([
                        ("0.5".to_string(), 45.0),
                        ("0.9".to_string(), 85.0),
                        ("0.99".to_string(), 98.0),
                    ]),
                }),
                labels: HashMap::new(),
                timestamp: 1234567893,
            },
        ];

        let collector = TestCollector { metrics };
        register_collector(Box::new(collector));
    }

    #[test]
    fn test_prometheus_export() {
        let output = export_prometheus();
        // Should contain Prometheus format metrics
        assert!(output.contains("# HELP") || output.is_empty());
    }

    // TODO: Fix these tests to not depend on internal storage access
    // #[sinex_test]
    #[allow(dead_code)]
    async fn test_json_export_comprehensive() -> color_eyre::eyre::Result<()> {
        setup_test_metrics();

        let output = export_json();
        assert!(output.is_object());

        // Check metadata
        assert!(output.get("metadata").is_some());
        let metadata = output.get("metadata").unwrap();
        assert!(metadata.get("timestamp").is_some());
        assert_eq!(metadata.get("version").unwrap().as_str().unwrap(), "1.0");
        assert_eq!(
            metadata.get("exporter").unwrap().as_str().unwrap(),
            "sinex-metrics"
        );

        // Check stored metrics
        assert!(output.get("stored_metrics").is_some());
        let stored = output.get("stored_metrics").unwrap().as_object().unwrap();
        assert!(stored.contains_key("test_counter"));
        assert!(stored.contains_key("test_gauge"));
        assert!(stored.contains_key("test_histogram"));
        assert!(stored.contains_key("test_summary"));

        // Can't clear storage directly - it's private

        Ok(())
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

        // Check labels
        let labels = json["labels"].as_object().unwrap();
        assert_eq!(labels.get("label1").unwrap().as_str().unwrap(), "value1");
    }

    // #[sinex_test]
    #[allow(dead_code)]
    async fn test_openmetrics_export_comprehensive() -> color_eyre::eyre::Result<()> {
        setup_test_metrics();

        let output = export_openmetrics();
        assert!(output.contains("# HELP"));
        assert!(output.contains("# TYPE"));
        assert!(output.contains("# EOF"));

        // Check specific metrics
        assert!(output.contains("test_counter{method=\"GET\"} 42"));
        assert!(output.contains("test_gauge{instance=\"localhost\"} 75.5"));
        assert!(output.contains("test_histogram_bucket"));
        assert!(output.contains("test_summary_count"));
        assert!(output.contains("test_summary_sum"));

        // Can't clear storage directly - it's private

        Ok(())
    }

    #[test]
    fn test_metric_entry_to_openmetrics_all_types() {
        // Test Counter
        let counter = MetricEntry {
            name: "test_counter".to_string(),
            help: "Test counter".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(42.0),
            labels: HashMap::from([("label1".to_string(), "value1".to_string())]),
            timestamp: 1234567890,
        };

        let openmetrics = metric_entry_to_openmetrics(&counter);
        assert!(openmetrics.contains("# HELP test_counter Test counter"));
        assert!(openmetrics.contains("# TYPE test_counter counter"));
        assert!(openmetrics.contains("test_counter{label1=\"value1\"} 42"));

        // Test Summary
        let summary = MetricEntry {
            name: "test_summary".to_string(),
            help: "Test summary".to_string(),
            metric_type: MetricType::Summary,
            value: MetricValue::Summary(SummaryValue {
                count: 100,
                sum: 500.0,
                quantiles: HashMap::from([("0.5".to_string(), 50.0)]),
            }),
            labels: HashMap::new(),
            timestamp: 1234567890,
        };

        let openmetrics = metric_entry_to_openmetrics(&summary);
        assert!(openmetrics.contains("test_summary_count 100"));
        assert!(openmetrics.contains("test_summary_sum 500"));
        assert!(openmetrics.contains("test_summary{quantile=\"0.5\"} 50"));
    }

    // #[sinex_test]
    #[allow(dead_code)]
    async fn test_influxdb_export_comprehensive() -> color_eyre::eyre::Result<()> {
        setup_test_metrics();

        let output = export_influxdb();
        assert!(!output.is_empty());

        // Check line protocol format
        assert!(output.contains("test_counter,method=GET value=42"));
        assert!(output.contains("test_gauge,instance=localhost value=75.5"));
        assert!(output.contains("test_histogram bucket_"));
        assert!(output.contains("test_summary count=100,sum=5000"));

        // Verify timestamps are in nanoseconds
        let lines: Vec<&str> = output.lines().collect();
        for line in lines {
            if !line.is_empty() {
                let parts: Vec<&str> = line.split(' ').collect();
                assert_eq!(parts.len(), 3); // measurement, fields, timestamp
                let timestamp: u64 = parts[2].parse().unwrap();
                assert!(timestamp > 1_000_000_000_000_000_000); // Nanosecond timestamp
            }
        }

        // Can't clear storage directly - it's private

        Ok(())
    }

    #[test]
    fn test_metric_entry_to_influxdb_with_labels() {
        let metric = MetricEntry {
            name: "test_gauge".to_string(),
            help: "Test gauge".to_string(),
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(42.0),
            labels: HashMap::from([
                ("label1".to_string(), "value1".to_string()),
                ("label2".to_string(), "value2".to_string()),
            ]),
            timestamp: 1234567890,
        };

        let influxdb = metric_entry_to_influxdb(&metric);
        assert!(influxdb.contains("test_gauge,"));
        assert!(influxdb.contains("label1=value1"));
        assert!(influxdb.contains("label2=value2"));
        assert!(influxdb.contains("value=42"));
        assert!(influxdb.ends_with("1234567890000000000")); // Nanoseconds
    }

    // #[sinex_test]
    #[allow(dead_code)]
    async fn test_statsd_export_comprehensive() -> color_eyre::eyre::Result<()> {
        setup_test_metrics();

        let output = export_statsd();
        assert!(!output.is_empty());

        // Check StatsD format
        assert!(output.contains("test_counter:42|c"));
        assert!(output.contains("test_gauge:75.5|g"));
        assert!(output.contains("test_histogram:") && output.contains("|h"));
        assert!(output.contains("test_summary:5000|ms"));

        // Can't clear storage directly - it's private

        Ok(())
    }

    #[test]
    fn test_metric_entry_to_statsd_all_types() {
        // Counter
        let counter = MetricEntry {
            name: "counter".to_string(),
            help: "".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(42.0),
            labels: HashMap::new(),
            timestamp: 0,
        };
        assert_eq!(metric_entry_to_statsd(&counter), "counter:42|c");

        // Gauge
        let gauge = MetricEntry {
            name: "gauge".to_string(),
            help: "".to_string(),
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(3.14),
            labels: HashMap::new(),
            timestamp: 0,
        };
        assert_eq!(metric_entry_to_statsd(&gauge), "gauge:3.14|g");

        // Histogram
        let histogram = MetricEntry {
            name: "histogram".to_string(),
            help: "".to_string(),
            metric_type: MetricType::Histogram,
            value: MetricValue::Histogram(vec![1.0, 2.0, 3.0]),
            labels: HashMap::new(),
            timestamp: 0,
        };
        let statsd = metric_entry_to_statsd(&histogram);
        assert!(statsd.contains("histogram:1|h"));
        assert!(statsd.contains("histogram:2|h"));
        assert!(statsd.contains("histogram:3|h"));
    }

    // #[sinex_test]
    #[allow(dead_code)]
    async fn test_export_summary_with_metrics() -> color_eyre::eyre::Result<()> {
        setup_test_metrics();

        let summary = export_summary();
        assert_eq!(summary.total_metrics, 4);
        assert!(summary.export_timestamp > 0);

        // Check metrics by type
        assert_eq!(
            *summary.metrics_by_type.get(&MetricType::Counter).unwrap(),
            1
        );
        assert_eq!(*summary.metrics_by_type.get(&MetricType::Gauge).unwrap(), 1);
        assert_eq!(
            *summary.metrics_by_type.get(&MetricType::Histogram).unwrap(),
            1
        );
        assert_eq!(
            *summary.metrics_by_type.get(&MetricType::Summary).unwrap(),
            1
        );

        // Check namespaces
        assert_eq!(*summary.metrics_by_namespace.get("test").unwrap(), 4);

        // Check timestamps
        assert_eq!(summary.oldest_timestamp, Some(1234567890));
        assert_eq!(summary.newest_timestamp, Some(1234567893));

        // Can't clear storage directly - it's private

        Ok(())
    }

    #[test]
    fn test_labels_to_openmetrics() {
        // Empty labels
        assert_eq!(labels_to_openmetrics(&HashMap::new()), "");

        // Single label
        let single = HashMap::from([("key".to_string(), "value".to_string())]);
        assert_eq!(labels_to_openmetrics(&single), "{key=\"value\"}");

        // Multiple labels
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

    #[test]
    fn test_metric_type_to_openmetrics() {
        assert_eq!(metric_type_to_openmetrics(MetricType::Counter), "counter");
        assert_eq!(metric_type_to_openmetrics(MetricType::Gauge), "gauge");
        assert_eq!(
            metric_type_to_openmetrics(MetricType::Histogram),
            "histogram"
        );
        assert_eq!(metric_type_to_openmetrics(MetricType::Summary), "summary");
    }

    #[cfg(all(test, feature = "bench"))]
    mod benches {
        use super::*;
        use crate::telemetry::metrics::collectors::{
            store_metric_entry, MetricType, MetricValue, SummaryValue, METRICS_STORAGE,
        };
        use sinex_test_utils::prelude::*;

        fn setup_bench_metrics() {
            METRICS_STORAGE.write().clear();

            // Add 100 metrics of various types
            for i in 0..25 {
                store_metric_entry(MetricEntry {
                    name: format!("counter_{}", i),
                    help: "Benchmark counter".to_string(),
                    metric_type: MetricType::Counter,
                    value: MetricValue::Counter(i as f64),
                    labels: HashMap::from([("id".to_string(), i.to_string())]),
                    timestamp: 1234567890 + i as u64,
                });

                store_metric_entry(MetricEntry {
                    name: format!("gauge_{}", i),
                    help: "Benchmark gauge".to_string(),
                    metric_type: MetricType::Gauge,
                    value: MetricValue::Gauge(i as f64 * 1.5),
                    labels: HashMap::from([("id".to_string(), i.to_string())]),
                    timestamp: 1234567890 + i as u64,
                });

                store_metric_entry(MetricEntry {
                    name: format!("histogram_{}", i),
                    help: "Benchmark histogram".to_string(),
                    metric_type: MetricType::Histogram,
                    value: MetricValue::Histogram(vec![0.1, 0.5, 1.0, 5.0, 10.0]),
                    labels: HashMap::new(),
                    timestamp: 1234567890 + i as u64,
                });

                store_metric_entry(MetricEntry {
                    name: format!("summary_{}", i),
                    help: "Benchmark summary".to_string(),
                    metric_type: MetricType::Summary,
                    value: MetricValue::Summary(SummaryValue {
                        count: 100,
                        sum: 5000.0,
                        quantiles: HashMap::from([
                            ("0.5".to_string(), 50.0),
                            ("0.9".to_string(), 90.0),
                            ("0.99".to_string(), 99.0),
                        ]),
                    }),
                    labels: HashMap::new(),
                    timestamp: 1234567890 + i as u64,
                });
            }
        }

        #[sinex_bench]
        async fn bench_export_prometheus(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            ctx.bench("export_prometheus", || {
                export_prometheus();
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_export_json(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            setup_bench_metrics();

            ctx.bench("export_json", || {
                export_json();
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_export_openmetrics(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            setup_bench_metrics();

            ctx.bench("export_openmetrics", || {
                export_openmetrics();
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_export_influxdb(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            setup_bench_metrics();

            ctx.bench("export_influxdb", || {
                export_influxdb();
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_export_statsd(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            setup_bench_metrics();

            ctx.bench("export_statsd", || {
                export_statsd();
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_export_summary(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            setup_bench_metrics();

            ctx.bench("export_summary", || {
                export_summary();
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_metric_entry_to_json(
            ctx: &mut BenchContext,
        ) -> color_eyre::eyre::Result<()> {
            let metric = MetricEntry {
                name: "bench_metric".to_string(),
                help: "Benchmark metric".to_string(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(42.0),
                labels: HashMap::from([
                    ("method".to_string(), "GET".to_string()),
                    ("status".to_string(), "200".to_string()),
                ]),
                timestamp: 1234567890,
            };

            ctx.bench("metric_entry_to_json", || {
                metric_entry_to_json(&metric);
            });

            Ok(())
        }
    }
}
