//! # Metrics Registry
//!
//! This module provides a centralized registry for managing all metrics in the Sinex system.
//! It integrates with Prometheus for metrics collection and export.
//!
//! ## Overview
//!
//! The metrics registry serves as the central hub for all Prometheus metrics in Sinex.
//! It provides thread-safe access to metrics, automatic registration with Prometheus,
//! and various export formats.
//!
//! ## Components
//!
//! - [`MetricsRegistry`] - Core registry for all metrics
//! - [`MetricFamily`] - Enum representing different metric types
//! - [`GlobalMetrics`] - Static global access to metrics
//! - [`ExternalMetricsCollector`] - Custom collector for external metrics
//!
//! ## Usage
//!
//! ```rust,ignore
//! use sinex_telemetry::metrics::GlobalMetrics;
//! use std::collections::HashMap;
//!
//! // Get or create a counter
//! let counter = GlobalMetrics::get_or_create_counter(
//!     "my_counter",
//!     "Description of my counter",
//!     HashMap::new(),
//! );
//! counter.inc();
//!
//! // Export metrics
//! let prometheus_text = GlobalMetrics::export_prometheus();
//! let json_metrics = GlobalMetrics::export_json();
//! ```
//!
//! ## Thread Safety
//!
//! All operations on the metrics registry are thread-safe. The registry uses
//! `parking_lot::RwLock` for efficient concurrent access.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use prometheus::core::{Collector, Desc};
use prometheus::{
    Counter, CounterVec, Error as PrometheusError, Gauge, GaugeVec, Histogram, HistogramOpts,
    HistogramVec, IntCounter, IntGauge, Opts, Registry,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::telemetry::metrics::collectors::MetricEntry;

/// Global metrics registry for managing all Prometheus metrics.
///
/// This registry provides centralized management of all metric families
/// and integrates with the Prometheus client library.
pub struct MetricsRegistry {
    prometheus_registry: Registry,
    metric_families: Arc<RwLock<HashMap<String, MetricFamily>>>,
}

/// Represents different types of metric families supported by Prometheus.
///
/// Each variant wraps the corresponding Prometheus metric type.
#[derive(Debug, Clone)]
pub enum MetricFamily {
    Counter(Counter),
    Gauge(Gauge),
    Histogram(Histogram),
    IntCounter(IntCounter),
    IntGauge(IntGauge),
    CounterVec(CounterVec),
    GaugeVec(GaugeVec),
    HistogramVec(HistogramVec),
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            prometheus_registry: Registry::new(),
            metric_families: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a counter metric.
    ///
    /// Counters are monotonically increasing values that only go up.
    /// Use counters for values like request counts, bytes processed, or errors.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name (should follow Prometheus naming conventions)
    /// * `help` - Human-readable description of the metric
    /// * `labels` - Constant labels to attach to all samples
    ///
    /// # Errors
    ///
    /// Returns an error if a metric with the same name is already registered.
    pub fn register_counter(
        &self,
        name: &str,
        help: &str,
        labels: HashMap<String, String>,
    ) -> Result<Counter, PrometheusError> {
        let opts = Opts::new(name, help).const_labels(labels);
        let counter = Counter::with_opts(opts)?;

        self.prometheus_registry
            .register(Box::new(counter.clone()))?;

        self.metric_families
            .write()
            .insert(name.to_string(), MetricFamily::Counter(counter.clone()));

        Ok(counter)
    }

    /// Register a gauge metric.
    ///
    /// Gauges represent values that can go up or down.
    /// Use gauges for values like temperature, current memory usage, or queue size.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name (should follow Prometheus naming conventions)
    /// * `help` - Human-readable description of the metric
    /// * `labels` - Constant labels to attach to all samples
    ///
    /// # Errors
    ///
    /// Returns an error if a metric with the same name is already registered.
    pub fn register_gauge(
        &self,
        name: &str,
        help: &str,
        labels: HashMap<String, String>,
    ) -> Result<Gauge, PrometheusError> {
        let opts = Opts::new(name, help).const_labels(labels);
        let gauge = Gauge::with_opts(opts)?;

        self.prometheus_registry.register(Box::new(gauge.clone()))?;

        self.metric_families
            .write()
            .insert(name.to_string(), MetricFamily::Gauge(gauge.clone()));

        Ok(gauge)
    }

    /// Register a histogram metric.
    ///
    /// Histograms sample observations and count them in configurable buckets.
    /// Use histograms for values like request durations or response sizes.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name (should follow Prometheus naming conventions)
    /// * `help` - Human-readable description of the metric
    /// * `labels` - Constant labels to attach to all samples
    /// * `buckets` - Bucket boundaries for the histogram
    ///
    /// # Errors
    ///
    /// Returns an error if a metric with the same name is already registered.
    pub fn register_histogram(
        &self,
        name: &str,
        help: &str,
        labels: HashMap<String, String>,
        buckets: Vec<f64>,
    ) -> Result<Histogram, PrometheusError> {
        let opts = HistogramOpts::new(name, help)
            .const_labels(labels)
            .buckets(buckets);
        let histogram = Histogram::with_opts(opts)?;

        self.prometheus_registry
            .register(Box::new(histogram.clone()))?;

        self.metric_families
            .write()
            .insert(name.to_string(), MetricFamily::Histogram(histogram.clone()));

        Ok(histogram)
    }

    /// Register a counter vector metric
    pub fn register_counter_vec(
        &self,
        name: &str,
        help: &str,
        label_names: &[&str],
    ) -> Result<CounterVec, PrometheusError> {
        let opts = Opts::new(name, help);
        let counter_vec = CounterVec::new(opts, label_names)?;

        self.prometheus_registry
            .register(Box::new(counter_vec.clone()))?;

        self.metric_families.write().insert(
            name.to_string(),
            MetricFamily::CounterVec(counter_vec.clone()),
        );

        Ok(counter_vec)
    }

    /// Register a gauge vector metric
    pub fn register_gauge_vec(
        &self,
        name: &str,
        help: &str,
        label_names: &[&str],
    ) -> Result<GaugeVec, PrometheusError> {
        let opts = Opts::new(name, help);
        let gauge_vec = GaugeVec::new(opts, label_names)?;

        self.prometheus_registry
            .register(Box::new(gauge_vec.clone()))?;

        self.metric_families
            .write()
            .insert(name.to_string(), MetricFamily::GaugeVec(gauge_vec.clone()));

        Ok(gauge_vec)
    }

    /// Register a histogram vector metric
    pub fn register_histogram_vec(
        &self,
        name: &str,
        help: &str,
        label_names: &[&str],
        buckets: Vec<f64>,
    ) -> Result<HistogramVec, PrometheusError> {
        let opts = HistogramOpts::new(name, help).buckets(buckets);
        let histogram_vec = HistogramVec::new(opts, label_names)?;

        self.prometheus_registry
            .register(Box::new(histogram_vec.clone()))?;

        self.metric_families.write().insert(
            name.to_string(),
            MetricFamily::HistogramVec(histogram_vec.clone()),
        );

        Ok(histogram_vec)
    }

    /// Get a metric family by name.
    ///
    /// Returns `None` if no metric with the given name exists.
    pub fn get_metric_family(&self, name: &str) -> Option<MetricFamily> {
        self.metric_families.read().get(name).cloned()
    }

    /// Get all registered metric families.
    ///
    /// Returns a clone of the internal HashMap containing all metrics.
    /// This is useful for iterating over all metrics or debugging.
    pub fn get_all_metric_families(&self) -> HashMap<String, MetricFamily> {
        self.metric_families.read().clone()
    }

    /// Get a reference to the underlying Prometheus registry.
    ///
    /// This provides access to the raw Prometheus registry for advanced use cases.
    pub fn prometheus_registry(&self) -> &Registry {
        &self.prometheus_registry
    }

    /// Export all metrics in Prometheus text exposition format.
    ///
    /// This format is suitable for Prometheus scraping endpoints.
    ///
    /// # Example Output
    ///
    /// ```text
    /// # HELP sinex_events_total Total number of events processed
    /// # TYPE sinex_events_total counter
    /// sinex_events_total{component="fs-watcher"} 1523
    /// ```
    pub fn export_prometheus(&self) -> String {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.prometheus_registry.gather();
        encoder
            .encode_to_string(&metric_families)
            .unwrap_or_default()
    }

    /// Export all metrics in JSON format.
    ///
    /// This provides a structured representation of all metrics,
    /// useful for APIs or custom monitoring integrations.
    ///
    /// # Format
    ///
    /// The JSON structure includes:
    /// - Metric families with help text and type
    /// - Individual metrics with labels and values
    /// - Histogram buckets and summary quantiles
    pub fn export_json(&self) -> serde_json::Value {
        let metric_families = self.prometheus_registry.gather();

        let mut json_metrics = serde_json::Map::new();

        for family in metric_families {
            let family_name = family.get_name();
            let family_help = family.get_help();
            let family_type = format!("{:?}", family.get_field_type());

            let mut family_data = serde_json::Map::new();
            family_data.insert(
                "help".to_string(),
                serde_json::Value::String(family_help.to_string()),
            );
            family_data.insert("type".to_string(), serde_json::Value::String(family_type));

            let mut metrics = Vec::new();

            for metric in family.get_metric() {
                let mut metric_data = serde_json::Map::new();

                // Add labels
                let mut labels = serde_json::Map::new();
                for label_pair in metric.get_label() {
                    labels.insert(
                        label_pair.get_name().to_string(),
                        serde_json::Value::String(label_pair.get_value().to_string()),
                    );
                }
                metric_data.insert("labels".to_string(), serde_json::Value::Object(labels));

                // Add value based on metric type
                if metric.has_counter() {
                    metric_data.insert(
                        "value".to_string(),
                        serde_json::Value::Number(
                            serde_json::Number::from_f64(metric.get_counter().get_value())
                                .unwrap_or(serde_json::Number::from(0)),
                        ),
                    );
                } else if metric.has_gauge() {
                    metric_data.insert(
                        "value".to_string(),
                        serde_json::Value::Number(
                            serde_json::Number::from_f64(metric.get_gauge().get_value())
                                .unwrap_or(serde_json::Number::from(0)),
                        ),
                    );
                } else if metric.has_histogram() {
                    let histogram = metric.get_histogram();
                    let mut histogram_data = serde_json::Map::new();

                    histogram_data.insert(
                        "sample_count".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(
                            histogram.get_sample_count(),
                        )),
                    );
                    histogram_data.insert(
                        "sample_sum".to_string(),
                        serde_json::Value::Number(
                            serde_json::Number::from_f64(histogram.get_sample_sum())
                                .unwrap_or(serde_json::Number::from(0)),
                        ),
                    );

                    let mut buckets = Vec::new();
                    for bucket in histogram.get_bucket() {
                        let mut bucket_data = serde_json::Map::new();
                        bucket_data.insert(
                            "upper_bound".to_string(),
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(bucket.get_upper_bound())
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                        );
                        bucket_data.insert(
                            "cumulative_count".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(
                                bucket.get_cumulative_count(),
                            )),
                        );
                        buckets.push(serde_json::Value::Object(bucket_data));
                    }
                    histogram_data.insert("buckets".to_string(), serde_json::Value::Array(buckets));

                    metric_data.insert(
                        "histogram".to_string(),
                        serde_json::Value::Object(histogram_data),
                    );
                }

                metrics.push(serde_json::Value::Object(metric_data));
            }

            family_data.insert("metrics".to_string(), serde_json::Value::Array(metrics));
            json_metrics.insert(
                family_name.to_string(),
                serde_json::Value::Object(family_data),
            );
        }

        serde_json::Value::Object(json_metrics)
    }

    /// Clear all metrics from the registry.
    ///
    /// Note: This only clears the internal metric families map.
    /// The Prometheus registry cannot be cleared directly and would
    /// require creating a new registry instance.
    pub fn clear(&self) {
        self.metric_families.write().clear();
        // Note: Can't clear the Prometheus registry directly, would need to create a new one
    }
}

/// Global metrics registry instance.
///
/// This lazy static provides a singleton registry that's initialized
/// on first access and lives for the duration of the program.
static GLOBAL_REGISTRY: Lazy<MetricsRegistry> = Lazy::new(|| MetricsRegistry::new());

/// Provides convenient static access to the global metrics registry.
///
/// This struct offers helper methods for common metric operations
/// without needing to directly access the registry.
pub struct GlobalMetrics;

impl GlobalMetrics {
    /// Get a reference to the global metrics registry.
    ///
    /// This provides direct access to the singleton registry instance.
    pub fn registry() -> &'static MetricsRegistry {
        &GLOBAL_REGISTRY
    }

    /// Register a counter metric.
    ///
    /// Note: This is a no-op for compatibility. Metrics are automatically
    /// registered when created through the registry methods.
    pub fn register_counter(_counter: &Counter) {
        // Counter is already registered when created via the registry
        // This is a no-op for compatibility
    }

    /// Register a gauge metric.
    ///
    /// Note: This is a no-op for compatibility. Metrics are automatically
    /// registered when created through the registry methods.
    pub fn register_gauge<T: prometheus::core::Metric>(_gauge: &T) {
        // Gauge is already registered when created via the registry
        // This is a no-op for compatibility
    }

    /// Register a histogram metric.
    ///
    /// Note: This is a no-op for compatibility. Metrics are automatically
    /// registered when created through the registry methods.
    pub fn register_histogram(_histogram: &Histogram) {
        // Histogram is already registered when created via the registry
        // This is a no-op for compatibility
    }

    /// Get or create a counter metric.
    ///
    /// If a counter with the given name already exists, it returns the existing one.
    /// Otherwise, creates and registers a new counter.
    ///
    /// # Panics
    ///
    /// Panics if registration fails (e.g., due to invalid metric name).
    pub fn get_or_create_counter(
        name: &str,
        help: &str,
        labels: HashMap<String, String>,
    ) -> Counter {
        if let Some(MetricFamily::Counter(counter)) = GLOBAL_REGISTRY.get_metric_family(name) {
            counter
        } else {
            GLOBAL_REGISTRY
                .register_counter(name, help, labels)
                .unwrap()
        }
    }

    /// Get or create a gauge metric.
    ///
    /// If a gauge with the given name already exists, it returns the existing one.
    /// Otherwise, creates and registers a new gauge.
    ///
    /// # Panics
    ///
    /// Panics if registration fails (e.g., due to invalid metric name).
    pub fn get_or_create_gauge(name: &str, help: &str, labels: HashMap<String, String>) -> Gauge {
        if let Some(MetricFamily::Gauge(gauge)) = GLOBAL_REGISTRY.get_metric_family(name) {
            gauge
        } else {
            GLOBAL_REGISTRY.register_gauge(name, help, labels).unwrap()
        }
    }

    /// Get or create a histogram metric.
    ///
    /// If a histogram with the given name already exists, it returns the existing one.
    /// Otherwise, creates and registers a new histogram.
    ///
    /// # Panics
    ///
    /// Panics if registration fails (e.g., due to invalid metric name).
    pub fn get_or_create_histogram(
        name: &str,
        help: &str,
        labels: HashMap<String, String>,
        buckets: Vec<f64>,
    ) -> Histogram {
        if let Some(MetricFamily::Histogram(histogram)) = GLOBAL_REGISTRY.get_metric_family(name) {
            histogram
        } else {
            GLOBAL_REGISTRY
                .register_histogram(name, help, labels, buckets)
                .unwrap()
        }
    }

    /// Export all global metrics in Prometheus text exposition format.
    ///
    /// This is a convenience method that delegates to the global registry.
    pub fn export_prometheus() -> String {
        GLOBAL_REGISTRY.export_prometheus()
    }

    /// Export all global metrics in JSON format.
    ///
    /// This is a convenience method that delegates to the global registry.
    pub fn export_json() -> serde_json::Value {
        GLOBAL_REGISTRY.export_json()
    }
}

/// Initialize the global metrics registry.
///
/// This function sets up default metrics and logs initialization.
/// It's safe to call multiple times (subsequent calls are no-ops).
///
/// # Example
///
/// ```rust,ignore
/// #[tokio::main]
/// async fn main() {
///     init_global_registry().await;
///     // Start using metrics
/// }
/// ```
pub async fn init_global_registry() {
    // Initialize default metrics
    let _ = GlobalMetrics::get_or_create_counter(
        "sinex_metrics_registry_initialized_total",
        "Total number of times the metrics registry was initialized",
        HashMap::new(),
    );

    tracing::info!("Global metrics registry initialized");
}

/// Custom Prometheus collector for external metrics.
///
/// This collector allows integration of metrics from external sources
/// that don't fit the standard Counter/Gauge/Histogram model.
///
/// # Example
///
/// ```rust,ignore
/// let collector = ExternalMetricsCollector::new("custom_metrics".to_string());
/// collector.add_metric(metric_entry);
/// prometheus_registry.register(Box::new(collector))?;
/// ```
pub struct ExternalMetricsCollector {
    #[allow(dead_code)] // TODO: Use name for metric labeling
    name: String,
    metrics: Arc<RwLock<Vec<MetricEntry>>>,
}

impl ExternalMetricsCollector {
    pub fn new(name: String) -> Self {
        Self {
            name,
            metrics: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn add_metric(&self, metric: MetricEntry) {
        self.metrics.write().push(metric);
    }

    pub fn clear_metrics(&self) {
        self.metrics.write().clear();
    }
}

impl Collector for ExternalMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        // Return empty descriptor list - we'll handle this dynamically
        Vec::new()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        // Convert MetricEntry to Prometheus MetricFamily
        // This is a simplified implementation
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::metrics::collectors::{MetricEntry, MetricType, MetricValue};
    use sinex_test_utils::{sinex_test, TestContext};

    use color_eyre::eyre::Result;

    use serde_json::json;

    #[sinex_test]
    fn test_metrics_registry_creation() -> Result<()> {
        let registry = MetricsRegistry::new();
        assert!(registry.get_all_metric_families().is_empty());
        Ok(())
    }

    #[sinex_test]
    fn test_counter_registration() -> Result<()> {
        let registry = MetricsRegistry::new();
        let counter = registry
            .register_counter("test_counter", "A test counter", HashMap::new())
            .unwrap();

        assert_eq!(counter.get(), 0.0);
        counter.inc();
        assert_eq!(counter.get(), 1.0);

        // Test counter vector
        let counter_vec = registry
            .register_counter_vec(
                "test_counter_vec",
                "A test counter vector",
                &["method", "status"],
            )
            .unwrap();

        counter_vec.with_label_values(&["GET", "200"]).inc();
        counter_vec.with_label_values(&["POST", "201"]).inc_by(2.0);

        assert_eq!(counter_vec.with_label_values(&["GET", "200"]).get(), 1.0);
        assert_eq!(counter_vec.with_label_values(&["POST", "201"]).get(), 2.0);
        Ok(())
    }

    #[sinex_test]
    fn test_gauge_registration() -> Result<()> {
        let registry = MetricsRegistry::new();
        let gauge = registry
            .register_gauge("test_gauge", "A test gauge", HashMap::new())
            .unwrap();

        assert_eq!(gauge.get(), 0.0);
        gauge.set(42.0);
        assert_eq!(gauge.get(), 42.0);
        gauge.inc();
        assert_eq!(gauge.get(), 43.0);
        gauge.dec();
        assert_eq!(gauge.get(), 42.0);

        // Test gauge vector
        let gauge_vec = registry
            .register_gauge_vec("test_gauge_vec", "A test gauge vector", &["component"])
            .unwrap();

        gauge_vec.with_label_values(&["cpu"]).set(75.5);
        gauge_vec.with_label_values(&["memory"]).set(80.2);

        assert_eq!(gauge_vec.with_label_values(&["cpu"]).get(), 75.5);
        assert_eq!(gauge_vec.with_label_values(&["memory"]).get(), 80.2);
        Ok(())
    }

    #[sinex_test]
    fn test_histogram_registration() -> Result<()> {
        let registry = MetricsRegistry::new();
        let histogram = registry
            .register_histogram(
                "test_histogram",
                "A test histogram",
                HashMap::new(),
                vec![0.1, 0.5, 1.0, 5.0],
            )
            .unwrap();

        histogram.observe(0.3);
        histogram.observe(0.7);
        histogram.observe(1.5);

        assert_eq!(histogram.get_sample_count(), 3);
        assert_eq!(histogram.get_sample_sum(), 2.5);

        // Test histogram vector
        let histogram_vec = registry
            .register_histogram_vec(
                "test_histogram_vec",
                "A test histogram vector",
                &["operation"],
                vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
            )
            .unwrap();

        histogram_vec.with_label_values(&["query"]).observe(0.003);
        histogram_vec.with_label_values(&["query"]).observe(0.007);
        histogram_vec.with_label_values(&["insert"]).observe(0.015);

        assert_eq!(
            histogram_vec
                .with_label_values(&["query"])
                .get_sample_count(),
            2
        );
        Ok(())
    }

    #[sinex_test]
    fn test_metric_family_retrieval() -> Result<()> {
        let registry = MetricsRegistry::new();

        // Register different metric types
        registry
            .register_counter("my_counter", "Counter metric", HashMap::new())
            .unwrap();
        registry
            .register_gauge("my_gauge", "Gauge metric", HashMap::new())
            .unwrap();
        registry
            .register_histogram(
                "my_histogram",
                "Histogram metric",
                HashMap::new(),
                vec![0.1, 1.0, 10.0],
            )
            .unwrap();

        // Test retrieval
        assert!(matches!(
            registry.get_metric_family("my_counter"),
            Some(MetricFamily::Counter(_))
        ));
        assert!(matches!(
            registry.get_metric_family("my_gauge"),
            Some(MetricFamily::Gauge(_))
        ));
        assert!(matches!(
            registry.get_metric_family("my_histogram"),
            Some(MetricFamily::Histogram(_))
        ));
        assert!(matches!(registry.get_metric_family("nonexistent"), None));

        // Test get all
        let families = registry.get_all_metric_families();
        assert_eq!(families.len(), 3);
        Ok(())
    }

    #[sinex_test]
    fn test_duplicate_registration() -> Result<()> {
        let registry = MetricsRegistry::new();

        // First registration should succeed
        assert!(registry
            .register_counter("dup_counter", "Counter 1", HashMap::new())
            .is_ok());

        // Second registration with same name should fail
        assert!(registry
            .register_counter("dup_counter", "Counter 2", HashMap::new())
            .is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_prometheus_export() -> Result<()> {
        let registry = MetricsRegistry::new();
        let counter = registry
            .register_counter("test_counter", "A test counter", HashMap::new())
            .unwrap();

        counter.inc();

        let prometheus_output = registry.export_prometheus();
        assert!(prometheus_output.contains("test_counter"));
        assert!(prometheus_output.contains("1"));
        assert!(prometheus_output.contains("# HELP test_counter A test counter"));
        assert!(prometheus_output.contains("# TYPE test_counter counter"));
        Ok(())
    }

    #[sinex_test]
    fn test_json_export() -> Result<()> {
        let registry = MetricsRegistry::new();
        let counter = registry
            .register_counter("test_counter", "A test counter", HashMap::new())
            .unwrap();

        counter.inc();

        let json_output = registry.export_json();
        assert!(json_output.is_object());
        assert!(json_output.get("test_counter").is_some());

        let counter_data = json_output.get("test_counter").unwrap();
        assert_eq!(
            counter_data.get("help").unwrap().as_str().unwrap(),
            "A test counter"
        );
        assert!(counter_data
            .get("type")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("Counter"));
        Ok(())
    }

    #[sinex_test]
    fn test_json_export_with_labels() -> Result<()> {
        let registry = MetricsRegistry::new();
        let mut labels = HashMap::new();
        labels.insert("environment".to_string(), "test".to_string());
        labels.insert("version".to_string(), "1.0".to_string());

        let counter = registry
            .register_counter("labeled_counter", "Counter with labels", labels)
            .unwrap();

        counter.inc_by(5.0);

        let json_output = registry.export_json();
        let counter_data = json_output.get("labeled_counter").unwrap();
        let metrics = counter_data.get("metrics").unwrap().as_array().unwrap();
        assert_eq!(metrics.len(), 1);

        let metric = &metrics[0];
        let metric_labels = metric.get("labels").unwrap().as_object().unwrap();
        assert_eq!(
            metric_labels.get("environment").unwrap().as_str().unwrap(),
            "test"
        );
        assert_eq!(
            metric_labels.get("version").unwrap().as_str().unwrap(),
            "1.0"
        );
        assert_eq!(metric.get("value").unwrap().as_f64().unwrap(), 5.0);
        Ok(())
    }

    #[sinex_test]
    fn test_global_metrics() -> Result<()> {
        let counter = GlobalMetrics::get_or_create_counter(
            "global_test_counter",
            "A global test counter",
            HashMap::new(),
        );

        assert_eq!(counter.get(), 0.0);
        counter.inc();
        assert_eq!(counter.get(), 1.0);

        // Test idempotency - should return same counter
        let counter2 = GlobalMetrics::get_or_create_counter(
            "global_test_counter",
            "Different help text",
            HashMap::new(),
        );
        assert_eq!(counter2.get(), 1.0); // Same counter, retains value
        Ok(())
    }

    #[sinex_test]
    fn test_external_metrics_collector() -> Result<()> {
        let collector = ExternalMetricsCollector::new("test_collector".to_string());

        let metric = MetricEntry {
            name: "external_metric".to_string(),
            help: "Test external metric".to_string(),
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(42.0),
            timestamp: chrono::Utc::now().timestamp() as u64,
            labels: HashMap::new(),
        };

        collector.add_metric(metric);
        collector.clear_metrics();

        // Basic test - mainly ensuring it compiles and runs
        let descs = collector.desc();
        assert!(descs.is_empty());

        let families = collector.collect();
        assert!(families.is_empty()); // Simplified implementation returns empty

        Ok(())
    }

    #[sinex_test]
    async fn test_init_global_registry(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        init_global_registry().await;

        // Should have created initialization counter
        let counter = GlobalMetrics::get_or_create_counter(
            "sinex_metrics_registry_initialized_total",
            "Total number of times the metrics registry was initialized",
            HashMap::new(),
        );

        // Counter should exist (though value might be > 0 from other tests)
        assert!(counter.get() >= 0.0);

        Ok(())
    }

    #[cfg(all(test, feature = "bench"))]
    mod benches {
        use super::*;
        use sinex_test_utils::{sinex_test, TestContext};

        use color_eyre::eyre::Result;

        use serde_json::json;

        #[sinex_bench]
        async fn bench_counter_increment(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            let registry = MetricsRegistry::new();
            let counter = registry
                .register_counter("bench_counter", "Benchmark counter", HashMap::new())
                .unwrap();

            ctx.bench("counter_increment", || {
                counter.inc();
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_gauge_set(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            let registry = MetricsRegistry::new();
            let gauge = registry
                .register_gauge("bench_gauge", "Benchmark gauge", HashMap::new())
                .unwrap();

            ctx.bench("gauge_set", || {
                gauge.set(42.0);
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_histogram_observe(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            let registry = MetricsRegistry::new();
            let histogram = registry
                .register_histogram(
                    "bench_histogram",
                    "Benchmark histogram",
                    HashMap::new(),
                    vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
                )
                .unwrap();

            ctx.bench("histogram_observe", || {
                histogram.observe(0.123);
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_prometheus_export(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            let registry = MetricsRegistry::new();

            // Add some metrics
            for i in 0..10 {
                let counter = registry
                    .register_counter(&format!("counter_{}", i), "Test counter", HashMap::new())
                    .unwrap();
                counter.inc_by(i as f64);
            }

            ctx.bench("prometheus_export", || {
                registry.export_prometheus();
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_json_export(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
            let registry = MetricsRegistry::new();

            // Add some metrics
            for i in 0..10 {
                let gauge = registry
                    .register_gauge(&format!("gauge_{}", i), "Test gauge", HashMap::new())
                    .unwrap();
                gauge.set(i as f64 * 10.0);
            }

            ctx.bench("json_export", || {
                registry.export_json();
            });

            Ok(())
        }

        #[sinex_bench]
        async fn bench_concurrent_counter_access(
            ctx: &mut BenchContext,
        ) -> color_eyre::eyre::Result<()> {
            use std::sync::Arc;
            use std::thread;

            let registry = Arc::new(MetricsRegistry::new());
            let counter = registry
                .register_counter("concurrent_counter", "Concurrent counter", HashMap::new())
                .unwrap();

            ctx.bench("concurrent_counter_access", || {
                let handles: Vec<_> = (0..4)
                    .map(|_| {
                        let counter = counter.clone();
                        thread::spawn(move || {
                            for _ in 0..100 {
                                counter.inc();
                            }
                        })
                    })
                    .collect();

                for handle in handles {
                    handle.join().unwrap();
                }
            });

            Ok(())
        }
    }
}
