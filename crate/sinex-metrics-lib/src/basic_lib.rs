//! Basic Sinex Metrics Library
//!
//! This module provides the core functionality for automatic metrics collection
//! in a simplified form that can be easily compiled and tested.

use std::collections::HashMap;
use std::time::Instant;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use prometheus::{Counter, Histogram, Gauge};
use serde::{Deserialize, Serialize};
use sinex_events::constants::{event_types};

/// Global metrics registry
static METRICS_REGISTRY: Lazy<RwLock<HashMap<String, MetricInstance>>> = 
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Types of metrics that can be collected
#[derive(Debug, Clone)]
pub enum MetricInstance {
    Counter(Counter),
    Histogram(Histogram),
    Gauge(Gauge),
}

/// Metric configuration
#[derive(Debug, Clone)]
pub struct MetricConfig {
    pub name: String,
    pub help: String,
    pub labels: HashMap<String, String>,
}

/// Initialize the metrics system
pub async fn init_metrics() {
    println!("Initializing sinex-metrics system...");
    
    // Register some basic metrics
    register_metric(MetricConfig {
        name: "sinex_function_calls_total".to_string(),
        help: "Total number of function calls".to_string(),
        labels: HashMap::new(),
    });
    
    register_metric(MetricConfig {
        name: "sinex_function_duration_seconds".to_string(),
        help: "Function execution duration in seconds".to_string(),
        labels: HashMap::new(),
    });
    
    println!("Metrics system initialized successfully");
}

/// Register a metric
pub fn register_metric(config: MetricConfig) {
    let metric = match config.name.as_str() {
        name if name.contains("_total") => {
            let counter = Counter::with_opts(
                prometheus::Opts::new(&config.name, &config.help)
                    .const_labels(config.labels)
            ).unwrap();
            MetricInstance::Counter(counter)
        },
        name if name.contains("_seconds") => {
            let histogram = Histogram::with_opts(
                prometheus::HistogramOpts::new(&config.name, &config.help)
                    .const_labels(config.labels)
                    .buckets(vec![0.001, 0.01, 0.1, 1.0, 10.0])
            ).unwrap();
            MetricInstance::Histogram(histogram)
        },
        _ => {
            let gauge = Gauge::with_opts(
                prometheus::Opts::new(&config.name, &config.help)
                    .const_labels(config.labels)
            ).unwrap();
            MetricInstance::Gauge(gauge)
        }
    };
    
    METRICS_REGISTRY.write().insert(config.name.clone(), metric);
}

/// Record a function call
pub fn record_function_call(function_name: &str, duration: std::time::Duration, success: bool) {
    let registry = METRICS_REGISTRY.read();
    
    // Increment call counter
    if let Some(MetricInstance::Counter(counter)) = registry.get("sinex_function_calls_total") {
        counter.inc();
    }
    
    // Record duration
    if let Some(MetricInstance::Histogram(histogram)) = registry.get("sinex_function_duration_seconds") {
        histogram.observe(duration.as_secs_f64());
    }
    
    tracing::debug!(
        function = function_name,
        duration_ms = duration.as_millis(),
        success = success,
        "Function call recorded"
    );
}

/// Record a database operation
pub fn record_database_operation(operation: &str, duration: std::time::Duration, success: bool) {
    tracing::debug!(
        operation = operation,
        duration_ms = duration.as_millis(),
        success = success,
        "Database operation recorded"
    );
}

/// Record an event processing operation
pub fn record_event_processing(event_type: &str, duration: std::time::Duration, success: bool) {
    tracing::debug!(
        event_type = event_type,
        duration_ms = duration.as_millis(),
        success = success,
        "Event processing recorded"
    );
}

/// Record resource usage
pub fn record_resource_usage(function_name: &str, memory_bytes: u64, cpu_percent: f64) {
    tracing::debug!(
        function = function_name,
        memory_bytes = memory_bytes,
        cpu_percent = cpu_percent,
        "Resource usage recorded"
    );
}

/// Export metrics in Prometheus format
pub fn export_prometheus() -> String {
    let registry = prometheus::Registry::new();
    
    // Register all metrics with the registry
    for (_, metric) in METRICS_REGISTRY.read().iter() {
        match metric {
            MetricInstance::Counter(counter) => {
                let _ = registry.register(Box::new(counter.clone()));
            },
            MetricInstance::Histogram(histogram) => {
                let _ = registry.register(Box::new(histogram.clone()));
            },
            MetricInstance::Gauge(gauge) => {
                let _ = registry.register(Box::new(gauge.clone()));
            },
        }
    }
    
    let encoder = prometheus::TextEncoder::new();
    let metric_families = registry.gather();
    encoder.encode_to_string(&metric_families).unwrap_or_default()
}

/// Export metrics in JSON format
pub fn export_json() -> serde_json::Value {
    let mut metrics = serde_json::Map::new();
    
    // Add basic metadata
    metrics.insert("timestamp".to_string(), serde_json::Value::Number(
        serde_json::Number::from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        )
    ));
    
    metrics.insert("total_metrics".to_string(), serde_json::Value::Number(
        serde_json::Number::from(METRICS_REGISTRY.read().len())
    ));
    
    serde_json::Value::Object(metrics)
}

/// Export metrics summary
pub fn export_summary() -> MetricsSummary {
    let registry = METRICS_REGISTRY.read();
    
    MetricsSummary {
        total_metrics: registry.len(),
        export_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    }
}

/// Metrics summary structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub total_metrics: usize,
    pub export_timestamp: u64,
}

/// Function call guard for automatic cleanup
pub struct FunctionCallGuard {
    function_name: String,
    start_time: Instant,
}

impl FunctionCallGuard {
    pub fn new(function_name: String) -> Self {
        Self {
            function_name,
            start_time: Instant::now(),
        }
    }
}

impl Drop for FunctionCallGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        record_function_call(&self.function_name, duration, true);
    }
}

/// Convenience macro for instrumenting functions
#[macro_export]
macro_rules! instrument_function {
    ($name:expr, $body:expr) => {{
        let _guard = $crate::FunctionCallGuard::new($name.to_string());
        $body
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_basic_metrics() {
        init_metrics().await;
        
        // Test function call recording
        record_function_call("test_function", Duration::from_millis(100), true);
        
        // Test database operation recording
        record_database_operation("SELECT", Duration::from_millis(50), true);
        
        // Test event processing recording
        record_event_processing(event_types::file::CREATED, Duration::from_millis(25), true);
        
        // Test resource usage recording
        record_resource_usage("test_function", 1024 * 1024, 25.0);
        
        // Test exports
        let prometheus_output = export_prometheus();
        assert!(!prometheus_output.is_empty());
        
        let json_output = export_json();
        assert!(json_output.is_object());
        
        let summary = export_summary();
        assert!(summary.total_metrics > 0);
    }

    #[tokio::test]
    async fn test_function_guard() {
        init_metrics().await;
        
        {
            let _guard = FunctionCallGuard::new("test_guard_function".to_string());
            sleep(Duration::from_millis(10)).await;
        }
        
        // Guard should have recorded the function call
        let prometheus_output = export_prometheus();
        assert!(prometheus_output.contains("sinex_function_calls_total"));
    }

    #[test]
    fn test_instrument_function_macro() {
        let result = instrument_function!("test_macro", {
            42
        });
        
        assert_eq!(result, 42);
    }
}