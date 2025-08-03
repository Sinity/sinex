//! Benchmark Utilities - Helper Macros and Utilities for Benchmarking
//!
//! This module provides convenience macros and utilities to reduce boilerplate
//! when writing benchmarks, especially for database operations. It integrates
//! with Divan for consistent benchmark execution.
//!
//! # Main Components
//!
//! - `bench_with_db!` - Macro for database benchmarks with automatic setup
//! - `BENCH_CONTEXT` - Global benchmark context (re-exported)
//! - Helper functions for common benchmark patterns
//!
//! # Usage
//!
//! ```rust
//! use sinex_test_utils::bench::*;
//!
//! // Simple database benchmark
//! bench_with_db!(bench_insert_event, |ctx: &BenchContext| async move {
//!     let event = create_test_event();
//!     insert_event(ctx.pool(), event).await
//! });
//!
//! // Parameterized benchmark
//! #[divan::bench(args = [10, 100, 1000])]
//! fn bench_bulk_insert(bencher: Bencher, count: usize) {
//!     let ctx = &*BENCH_CONTEXT;
//!     let events: Vec<_> = (0..count).map(|_| create_test_event()).collect();
//!     
//!     bencher.bench_local(|| {
//!         ctx.runtime.block_on(async {
//!             insert_events(ctx.pool(), &events).await.unwrap()
//!         })
//!     });
//! }
//! ```

#[cfg(feature = "bench")]
pub use crate::bench_context::{BenchContext, DualMeasurement, BENCH_CONTEXT};
pub use crate::static_fixtures::DatasetSize;

#[cfg(feature = "bench")]
pub use crate::bench_results::{BenchmarkResult, BenchmarkRun, ComparisonReport};

/// Helper macro for database benchmarks
///
/// This macro reduces boilerplate for common database benchmark patterns.
/// It automatically handles:
/// - Accessing the global benchmark context
/// - Running async operations in the benchmark runtime
/// - Resetting database state
/// - Error handling
///
/// # Syntax
///
/// ```rust
/// bench_with_db!(benchmark_name, |ctx: &BenchContext| async move {
///     // Your async benchmark code here
///     // Has access to ctx.pool() for database operations
/// });
/// ```
///
/// # Generated Code
///
/// The macro expands to a standard Divan benchmark function that:
/// 1. Gets the global BENCH_CONTEXT
/// 2. Resets database and loads standard fixture
/// 3. Runs your async code in the benchmark runtime
/// 4. Handles errors by unwrapping
///
/// # Example
///
/// ```rust
/// use sinex_test_utils::bench::*;
///
/// bench_with_db!(bench_event_query, |ctx: &BenchContext| async move {
///     query_events_by_source(ctx.pool(), "test", 100).await
/// });
/// ```
#[cfg(feature = "bench")]
#[macro_export]
macro_rules! bench_with_db {
    ($name:ident, $body:expr) => {
        #[divan::bench]
        fn $name(bencher: divan::Bencher) {
            use $crate::bench::BENCH_CONTEXT;
            let ctx = &*BENCH_CONTEXT;

            bencher.bench_local(|| {
                ctx.runtime.block_on(async {
                    ctx.reset_and_load("standard").await.unwrap();
                    $body(ctx).await.unwrap()
                })
            });
        }
    };
}

/// Helper for creating benchmark results from Divan output
#[cfg(feature = "bench")]
pub fn create_benchmark_result(
    name: &str,
    suite: &str,
    dataset: &str,
    mean_ns: u64,
    samples: usize,
) -> BenchmarkResult {
    BenchmarkResult {
        name: name.to_string(),
        suite: suite.to_string(),
        dataset: dataset.to_string(),
        timestamp: chrono::Utc::now(),
        mean_ns: Some(mean_ns),
        median_ns: None,  // Would need to calculate from samples
        std_dev_ns: None, // Would need to calculate from samples
        samples: Some(samples),
        cold_cache_ns: None,
        warm_cache_ns: None,
        instructions: None,
        l1_accesses: None,
        l2_accesses: None,
        ram_accesses: None,
        estimated_cycles: None,
    }
}

/// Extract suite name from fully qualified function name
///
/// Examples:
/// - `sinex_db::events::bench_insert` -> `sinex_db::events`
/// - `bench_simple` -> `bench`
#[cfg(feature = "bench")]
pub fn extract_suite(benchmark_name: &str) -> String {
    if let Some(pos) = benchmark_name.rfind("::") {
        benchmark_name[..pos].to_string()
    } else {
        "bench".to_string()
    }
}

/// Format nanoseconds as human-readable duration
///
/// Examples:
/// - 1_500 -> "1.5µs"
/// - 1_500_000 -> "1.5ms"
/// - 1_500_000_000 -> "1.5s"
#[cfg(feature = "bench")]
pub fn format_duration_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{}ns", ns)
    } else if ns < 1_000_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}

/// Calculate percentage change between two values
#[cfg(feature = "bench")]
pub fn calculate_change_percent(baseline: u64, current: u64) -> f64 {
    if baseline == 0 {
        return 0.0;
    }
    ((current as f64 - baseline as f64) / baseline as f64) * 100.0
}

/// Check if a percentage change is statistically significant
///
/// This is a simple threshold-based check. In practice, you'd want
/// to use proper statistical tests considering standard deviation.
#[cfg(feature = "bench")]
pub fn is_significant_change(change_percent: f64, std_dev_percent: Option<f64>) -> bool {
    match std_dev_percent {
        Some(std_dev) => change_percent.abs() > 2.0 * std_dev,
        None => change_percent.abs() > 5.0, // Default 5% threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "bench")]
    #[test]
    fn test_extract_suite() {
        assert_eq!(
            extract_suite("sinex_db::events::bench_insert"),
            "sinex_db::events"
        );
        assert_eq!(extract_suite("bench_simple"), "bench");
        assert_eq!(
            extract_suite("crate::module::submodule::bench_test"),
            "crate::module::submodule"
        );
    }

    #[cfg(feature = "bench")]
    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration_ns(500), "500ns");
        assert_eq!(format_duration_ns(1_500), "1.5µs");
        assert_eq!(format_duration_ns(1_500_000), "1.5ms");
        assert_eq!(format_duration_ns(1_500_000_000), "1.50s");
    }

    #[cfg(feature = "bench")]
    #[test]
    fn test_calculate_change() {
        assert_eq!(calculate_change_percent(100, 110), 10.0);
        assert_eq!(calculate_change_percent(100, 90), -10.0);
        assert_eq!(calculate_change_percent(0, 100), 0.0); // Avoid division by zero
    }

    #[cfg(feature = "bench")]
    #[test]
    fn test_significance() {
        // Without std dev
        assert!(is_significant_change(10.0, None));
        assert!(!is_significant_change(3.0, None));

        // With std dev
        assert!(is_significant_change(10.0, Some(2.0))); // 10% > 2*2%
        assert!(!is_significant_change(3.0, Some(2.0))); // 3% < 2*2%
    }
}
