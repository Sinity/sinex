//! # Automatic Function Metrics
//!
//! This module provides utilities for automatic instrumentation of functions with comprehensive metrics.
//! It provides runtime metrics collection that can be embedded in functions.
//!
//! ## Overview
//!
//! The auto_metrics module enables automatic collection of function-level metrics
//! including execution time, call counts, error rates, and concurrent execution tracking.
//! It integrates seamlessly with both Prometheus metrics and telemetry events.
//!
//! ## Components
//!
//! - [`FunctionMetrics`] - Metrics collector for individual functions
//! - [`FunctionCallGuard`] - RAII guard that tracks function execution
//! - [`track_function_call`] - Helper to create instrumented function calls
//! - [`get_function_metrics`] - Retrieve or create metrics for a function
//!
//! ## Usage
//!
//! ### Manual Instrumentation
//!
//! ```rust,ignore
//! use sinex_telemetry::instrumentation::track_function_call;
//!
//! async fn my_function() -> Result<(), Error> {
//!     let _guard = track_function_call("my_function", module_path!());
//!     
//!     // Function body here
//!     // Metrics are automatically recorded when guard drops
//!     Ok(())
//! }
//! ```
//!
//! ### With Error Tracking
//!
//! ```rust,ignore
//! async fn fallible_function() -> Result<String, Error> {
//!     let guard = track_function_call("fallible_function", module_path!());
//!     
//!     match do_work() {
//!         Ok(result) => Ok(result),
//!         Err(e) => {
//!             guard.record_error(); // Explicitly record error
//!             Err(e)
//!         }
//!     }
//! }
//! ```
//!
//! ## Metrics Collected
//!
//! For each instrumented function, the following metrics are collected:
//!
//! - `sinex_function_calls_total` - Total number of function calls
//! - `sinex_function_duration_seconds` - Execution time histogram
//! - `sinex_function_errors_total` - Total number of errors
//! - `sinex_function_active_calls` - Current number of concurrent executions
//! - `sinex_function_memory_bytes` - Memory usage (when recorded)
//!
//! ## Integration with Telemetry
//!
//! Function metrics are automatically recorded in the telemetry system,
//! enabling long-term analysis of function performance trends.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use prometheus::{Counter, Gauge, Histogram, IntGauge};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::telemetry::metrics::registry::GlobalMetrics;

/// Metrics collector for tracking function-level performance and behavior.
///
/// This struct maintains all metrics for a single function including
/// call counts, execution times, error rates, and resource usage.
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

/// RAII guard that automatically records function metrics on drop.
///
/// This guard is created when entering a function and automatically
/// records execution time and decrements active call count when dropped.
/// For functions that can fail, use `record_error()` to explicitly
/// track errors before the guard is dropped.
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

        // Also record in telemetry
        crate::telemetry::record_function_telemetry(
            &self.metrics.module,
            &self.metrics.name,
            duration.as_secs_f64() * 1000.0, // Convert to milliseconds
            true,                            // This is an error
        );
    }
}

impl Drop for FunctionCallGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        self.metrics.record_call_complete(duration);

        // Also record in telemetry
        crate::telemetry::record_function_telemetry(
            &self.metrics.module,
            &self.metrics.name,
            duration.as_secs_f64() * 1000.0, // Convert to milliseconds
            false,                           // Not an error
        );
    }
}

/// Global function metrics
static FUNCTION_METRICS: Lazy<Arc<RwLock<HashMap<String, Arc<FunctionMetrics>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Get or create function metrics for the specified function.
///
/// This function maintains a global cache of function metrics, returning
/// existing metrics if available or creating new ones as needed.
///
/// # Arguments
///
/// * `function_name` - Name of the function to track
/// * `module_name` - Module path of the function
/// * `labels` - Additional labels to attach to metrics
///
/// # Returns
///
/// An `Arc<FunctionMetrics>` that can be shared across threads.
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

/// Create a function call guard for automatic metrics collection.
///
/// This is a convenience function that creates metrics for a function
/// and returns a guard that will track the execution.
///
/// # Arguments
///
/// * `function_name` - Name of the function to track
/// * `module_name` - Module path of the function
///
/// # Example
///
/// ```rust,ignore
/// let _guard = track_function_call("my_function", module_path!());
/// ```
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
    use sinex_test_utils::prelude::*;

    #[sinex_test]
    async fn test_function_metrics(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Clear any existing metrics
        FUNCTION_METRICS.write().clear();

        let metrics = get_function_metrics("test_function", "test_module", HashMap::new());

        // Initial state
        assert_eq!(metrics.calls.get(), 0.0);
        assert_eq!(metrics.active_calls.get(), 0);
        assert_eq!(metrics.errors.get(), 0.0);

        // Track a function call
        let guard = FunctionCallGuard::new(metrics.clone());

        // While call is active
        assert_eq!(metrics.calls.get(), 1.0);
        assert_eq!(metrics.active_calls.get(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        drop(guard);

        // After call completes
        assert_eq!(metrics.calls.get(), 1.0);
        assert_eq!(metrics.active_calls.get(), 0);
        assert_eq!(metrics.errors.get(), 0.0);

        // Duration should be recorded
        assert!(metrics.duration.get_sample_count() > 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_function_error_tracking(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let metrics = get_function_metrics("error_function", "test_module", HashMap::new());

        let initial_errors = metrics.errors.get();

        let guard = FunctionCallGuard::new(metrics.clone());

        // Active call
        assert_eq!(metrics.active_calls.get(), 1);

        guard.record_error();

        // After error
        assert_eq!(metrics.errors.get(), initial_errors + 1.0);
        assert_eq!(metrics.active_calls.get(), 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_function_metrics_with_labels(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let mut labels = HashMap::new();
        labels.insert("environment".to_string(), "test".to_string());
        labels.insert("version".to_string(), "1.0".to_string());

        let metrics = get_function_metrics("labeled_function", "test_module", labels.clone());

        // Labels should be stored
        assert_eq!(metrics.labels, labels);

        // Multiple calls should track correctly
        for _ in 0..5 {
            let guard = FunctionCallGuard::new(metrics.clone());
            drop(guard);
        }

        assert_eq!(metrics.calls.get(), 5.0);

        Ok(())
    }

    #[sinex_test]
    async fn test_memory_usage_tracking(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let metrics = get_function_metrics("memory_function", "test_module", HashMap::new());

        // Record memory usage
        metrics.record_memory_usage(1024.0);
        assert_eq!(metrics.memory_usage.get(), 1024.0);

        metrics.record_memory_usage(2048.0);
        assert_eq!(metrics.memory_usage.get(), 2048.0);

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrent_function_calls(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        use tokio::task::JoinSet;

        let metrics = get_function_metrics("concurrent_function", "test_module", HashMap::new());

        let mut tasks = JoinSet::new();

        // Spawn multiple concurrent calls
        for i in 0..10 {
            let metrics_clone = metrics.clone();
            tasks.spawn(async move {
                let guard = FunctionCallGuard::new(metrics_clone);
                tokio::time::sleep(std::time::Duration::from_millis(10 + i)).await;
                drop(guard);
            });
        }

        // Wait for all tasks
        while let Some(result) = tasks.join_next().await {
            result?;
        }

        // Verify all calls were tracked
        assert_eq!(metrics.calls.get(), 10.0);
        assert_eq!(metrics.active_calls.get(), 0);
        assert_eq!(metrics.duration.get_sample_count(), 10);

        Ok(())
    }

    #[sinex_test]
    async fn test_function_metrics_caching(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // First call creates metrics
        let metrics1 = get_function_metrics("cached_function", "test_module", HashMap::new());

        // Second call should return same instance
        let metrics2 = get_function_metrics("cached_function", "test_module", HashMap::new());

        // Verify they're the same instance
        assert!(Arc::ptr_eq(&metrics1, &metrics2));

        // Different function should get different instance
        let metrics3 = get_function_metrics("other_function", "test_module", HashMap::new());
        assert!(!Arc::ptr_eq(&metrics1, &metrics3));

        Ok(())
    }

    #[sinex_test]
    async fn test_drop_behavior(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let metrics = get_function_metrics("drop_test", "test_module", HashMap::new());

        // Test normal drop
        {
            let _guard = FunctionCallGuard::new(metrics.clone());
            assert_eq!(metrics.active_calls.get(), 1);
            // Guard drops here
        }
        assert_eq!(metrics.active_calls.get(), 0);

        // Test panic scenario (guard still drops)
        let result = std::panic::catch_unwind(|| {
            let _guard = FunctionCallGuard::new(metrics.clone());
            assert_eq!(metrics.active_calls.get(), 1);
            panic!("Test panic");
        });

        assert!(result.is_err());
        assert_eq!(metrics.active_calls.get(), 0);

        Ok(())
    }

    #[sinex_test]
    fn test_track_function_helper() {
        let guard = track_function_call("helper_test", "test_module");

        // Guard should be created
        assert_eq!(guard.metrics.name, "helper_test");
        assert_eq!(guard.metrics.module, "test_module");
    }
}

#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use sinex_test_utils::prelude::*;

    #[sinex_bench]
    async fn bench_function_metrics_creation(
        ctx: &mut BenchContext,
    ) -> color_eyre::eyre::Result<()> {
        ctx.bench("function_metrics_creation", || {
            let metrics = FunctionMetrics::new("bench_function", "bench_module", HashMap::new());
            metrics
        });

        Ok(())
    }

    #[sinex_bench]
    async fn bench_function_call_tracking(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
        let metrics = Arc::new(FunctionMetrics::new(
            "bench_function",
            "bench_module",
            HashMap::new(),
        ));

        ctx.bench("function_call_tracking", || {
            let guard = FunctionCallGuard::new(metrics.clone());
            drop(guard);
        });

        Ok(())
    }

    #[sinex_bench]
    async fn bench_get_function_metrics(ctx: &mut BenchContext) -> color_eyre::eyre::Result<()> {
        // Pre-populate some metrics
        for i in 0..10 {
            get_function_metrics(&format!("function_{}", i), "bench_module", HashMap::new());
        }

        ctx.bench("get_function_metrics", || {
            get_function_metrics("function_5", "bench_module", HashMap::new());
        });

        Ok(())
    }

    #[sinex_bench]
    async fn bench_concurrent_metric_access(
        ctx: &mut BenchContext,
    ) -> color_eyre::eyre::Result<()> {
        use std::thread;

        let metrics = Arc::new(FunctionMetrics::new(
            "concurrent_bench",
            "bench_module",
            HashMap::new(),
        ));

        ctx.bench("concurrent_metric_access", || {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let metrics = metrics.clone();
                    thread::spawn(move || {
                        for _ in 0..10 {
                            metrics.record_call_start();
                            metrics.record_call_complete(std::time::Duration::from_micros(100));
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
