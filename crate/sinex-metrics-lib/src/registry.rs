//! Metrics Registry
//!
//! This module provides a centralized registry for managing all metrics in the Sinex system.
//! It integrates with Prometheus for metrics collection and export.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use prometheus::core::{Collector, Desc};
use prometheus::{
    Counter, CounterVec, Error as PrometheusError, Gauge, GaugeVec, Histogram, HistogramOpts,
    HistogramVec, IntCounter, IntGauge, Opts, Registry,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::collectors::MetricEntry;

/// Global metrics registry
pub struct MetricsRegistry {
    prometheus_registry: Registry,
    metric_families: Arc<RwLock<HashMap<String, MetricFamily>>>,
}

/// Metric family types
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

    /// Register a counter metric
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

    /// Register a gauge metric
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

    /// Register a histogram metric
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

    /// Get metric family by name
    pub fn get_metric_family(&self, name: &str) -> Option<MetricFamily> {
        self.metric_families.read().get(name).cloned()
    }

    /// Get all registered metric families
    pub fn get_all_metric_families(&self) -> HashMap<String, MetricFamily> {
        self.metric_families.read().clone()
    }

    /// Get the Prometheus registry
    pub fn prometheus_registry(&self) -> &Registry {
        &self.prometheus_registry
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.prometheus_registry.gather();
        encoder
            .encode_to_string(&metric_families)
            .unwrap_or_default()
    }

    /// Export metrics in JSON format
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

    /// Clear all metrics
    pub fn clear(&self) {
        self.metric_families.write().clear();
        // Note: Can't clear the Prometheus registry directly, would need to create a new one
    }
}

/// Global metrics registry instance
static GLOBAL_REGISTRY: Lazy<MetricsRegistry> = Lazy::new(|| MetricsRegistry::new());

/// Global metrics access
pub struct GlobalMetrics;

impl GlobalMetrics {
    /// Get the global metrics registry
    pub fn registry() -> &'static MetricsRegistry {
        &GLOBAL_REGISTRY
    }

    /// Register a counter metric
    pub fn register_counter(_counter: &Counter) {
        // Counter is already registered when created via the registry
        // This is a no-op for compatibility
    }

    /// Register a gauge metric
    pub fn register_gauge<T: prometheus::core::Metric>(_gauge: &T) {
        // Gauge is already registered when created via the registry
        // This is a no-op for compatibility
    }

    /// Register a histogram metric
    pub fn register_histogram(_histogram: &Histogram) {
        // Histogram is already registered when created via the registry
        // This is a no-op for compatibility
    }

    /// Get or create a counter metric
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

    /// Get or create a gauge metric
    pub fn get_or_create_gauge(name: &str, help: &str, labels: HashMap<String, String>) -> Gauge {
        if let Some(MetricFamily::Gauge(gauge)) = GLOBAL_REGISTRY.get_metric_family(name) {
            gauge
        } else {
            GLOBAL_REGISTRY.register_gauge(name, help, labels).unwrap()
        }
    }

    /// Get or create a histogram metric
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

    /// Export metrics in Prometheus format
    pub fn export_prometheus() -> String {
        GLOBAL_REGISTRY.export_prometheus()
    }

    /// Export metrics in JSON format
    pub fn export_json() -> serde_json::Value {
        GLOBAL_REGISTRY.export_json()
    }
}

/// Initialize the global metrics registry
pub async fn init_global_registry() {
    // Initialize default metrics
    let _ = GlobalMetrics::get_or_create_counter(
        "sinex_metrics_registry_initialized_total",
        "Total number of times the metrics registry was initialized",
        HashMap::new(),
    );

    tracing::info!("Global metrics registry initialized");
}

/// Custom collector for external metrics
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

    #[test]
    fn test_metrics_registry_creation() {
        let registry = MetricsRegistry::new();
        assert!(registry.get_all_metric_families().is_empty());
    }

    #[test]
    fn test_counter_registration() {
        let registry = MetricsRegistry::new();
        let counter = registry
            .register_counter("test_counter", "A test counter", HashMap::new())
            .unwrap();

        assert_eq!(counter.get(), 0.0);
        counter.inc();
        assert_eq!(counter.get(), 1.0);
    }

    #[test]
    fn test_gauge_registration() {
        let registry = MetricsRegistry::new();
        let gauge = registry
            .register_gauge("test_gauge", "A test gauge", HashMap::new())
            .unwrap();

        assert_eq!(gauge.get(), 0.0);
        gauge.set(42.0);
        assert_eq!(gauge.get(), 42.0);
    }

    #[test]
    fn test_histogram_registration() {
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
    }

    #[test]
    fn test_prometheus_export() {
        let registry = MetricsRegistry::new();
        let counter = registry
            .register_counter("test_counter", "A test counter", HashMap::new())
            .unwrap();

        counter.inc();

        let prometheus_output = registry.export_prometheus();
        assert!(prometheus_output.contains("test_counter"));
        assert!(prometheus_output.contains("1"));
    }

    #[test]
    fn test_json_export() {
        let registry = MetricsRegistry::new();
        let counter = registry
            .register_counter("test_counter", "A test counter", HashMap::new())
            .unwrap();

        counter.inc();

        let json_output = registry.export_json();
        assert!(json_output.is_object());
    }

    #[test]
    fn test_global_metrics() {
        let counter = GlobalMetrics::get_or_create_counter(
            "global_test_counter",
            "A global test counter",
            HashMap::new(),
        );

        assert_eq!(counter.get(), 0.0);
        counter.inc();
        assert_eq!(counter.get(), 1.0);
    }
}
