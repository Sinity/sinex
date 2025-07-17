//! Automatic Function Metrics
//!
//! This module provides utilities for automatic instrumentation of functions with comprehensive metrics.
//! It provides runtime metrics collection that can be embedded in functions.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use prometheus::{Counter, Gauge, Histogram, IntGauge};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::registry::GlobalMetrics;

/// Function metrics collector
#[derive(Debug, Clone)]
pub struct FunctionMetrics {
    pub name: String,
    pub module: String,
    pub calls: Counter,
    pub duration: Histogram,
    pub errors: Counter,
    pub active_calls: IntGauge,
    pub memory_usage: Gauge,
    pub labels: HashMap<String, String>,
}

impl FunctionMetrics {
    pub fn new(name: &str, module: &str, labels: HashMap<String, String>) -> Self {
        let calls = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_function_calls_total",
                "Total number of function calls",
            )
            .namespace("sinex")
            .subsystem("function")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "sinex_function_duration_seconds",
                "Function execution duration in seconds",
            )
            .namespace("sinex")
            .subsystem("function")
            .const_labels(labels.clone())
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
        )
        .unwrap();

        let errors = Counter::with_opts(
            prometheus::Opts::new(
                "sinex_function_errors_total",
                "Total number of function errors",
            )
            .namespace("sinex")
            .subsystem("function")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let active_calls = IntGauge::with_opts(
            prometheus::Opts::new(
                "sinex_function_active_calls",
                "Number of currently active function calls",
            )
            .namespace("sinex")
            .subsystem("function")
            .const_labels(labels.clone()),
        )
        .unwrap();

        let memory_usage = Gauge::with_opts(
            prometheus::Opts::new(
                "sinex_function_memory_bytes",
                "Memory usage of function in bytes",
            )
            .namespace("sinex")
            .subsystem("function")
            .const_labels(labels.clone()),
        )
        .unwrap();

        // Register with global metrics
        GlobalMetrics::register_counter(&calls);
        GlobalMetrics::register_histogram(&duration);
        GlobalMetrics::register_counter(&errors);
        GlobalMetrics::register_gauge(&active_calls);
        GlobalMetrics::register_gauge(&memory_usage);

        Self {
            name: name.to_string(),
            module: module.to_string(),
            calls,
            duration,
            errors,
            active_calls,
            memory_usage,
            labels,
        }
    }

    pub fn record_call_start(&self) {
        self.calls.inc();
        self.active_calls.inc();
    }

    pub fn record_call_complete(&self, duration: std::time::Duration) {
        self.duration.observe(duration.as_secs_f64());
        self.active_calls.dec();
    }

    pub fn record_error(&self) {
        self.errors.inc();
        self.active_calls.dec();
    }

    pub fn record_memory_usage(&self, bytes: f64) {
        self.memory_usage.set(bytes);
    }
}

/// Function call guard that automatically records metrics
pub struct FunctionCallGuard {
    metrics: Arc<FunctionMetrics>,
    start_time: Instant,
}

impl FunctionCallGuard {
    pub fn new(metrics: Arc<FunctionMetrics>) -> Self {
        metrics.record_call_start();
        Self {
            metrics,
            start_time: Instant::now(),
        }
    }

    pub fn record_error(self) {
        let duration = self.start_time.elapsed();
        self.metrics.duration.observe(duration.as_secs_f64());
        self.metrics.record_error();
    }
}

impl Drop for FunctionCallGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        self.metrics.record_call_complete(duration);
    }
}

/// Global function metrics
static FUNCTION_METRICS: Lazy<Arc<RwLock<HashMap<String, Arc<FunctionMetrics>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Get or create function metrics
pub fn get_function_metrics(
    function_name: &str,
    module_name: &str,
    labels: HashMap<String, String>,
) -> Arc<FunctionMetrics> {
    let key = format!("{}::{}", module_name, function_name);

    // Try to get existing metrics
    if let Some(metrics) = FUNCTION_METRICS.read().get(&key) {
        return metrics.clone();
    }

    // Create new metrics
    let metrics = Arc::new(FunctionMetrics::new(function_name, module_name, labels));
    FUNCTION_METRICS.write().insert(key, metrics.clone());

    metrics
}

/// Create a function call guard for automatic metrics
pub fn track_function_call(function_name: &str, module_name: &str) -> FunctionCallGuard {
    let metrics = get_function_metrics(function_name, module_name, HashMap::new());
    FunctionCallGuard::new(metrics)
}

/// Convenience macro for tracking function calls
#[macro_export]
macro_rules! track_function {
    () => {
        let _guard = $crate::auto_metrics::track_function_call(
            &format!("{}", stringify!(function_name!())),
            module_path!(),
        );
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_function_metrics() {
        let metrics = get_function_metrics("test_function", "test_module", HashMap::new());

        let guard = FunctionCallGuard::new(metrics.clone());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        drop(guard);

        // Verify metrics were recorded
        assert!(metrics.calls.get() > 0.0);
    }

    #[tokio::test]
    async fn test_function_error_tracking() {
        let metrics = get_function_metrics("error_function", "test_module", HashMap::new());

        let guard = FunctionCallGuard::new(metrics.clone());
        guard.record_error();

        // Verify error was recorded
        assert!(metrics.errors.get() > 0.0);
    }
}
