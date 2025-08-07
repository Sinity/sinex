//! Benchmark Context - Infrastructure for Database Benchmarks
//!
//! This module provides a shared context for running database benchmarks with
//! proper isolation, state management, and measurement capabilities. It ensures
//! benchmarks run with consistent conditions and provides utilities for both
//! cold and warm cache measurements.
//!
//! # Architecture
//!
//! The benchmark context provides:
//! - **Shared Database Pool**: Single benchmark database to avoid pool contention
//! - **State Isolation**: Reset and fixture loading between benchmark runs
//! - **Cache Control**: Utilities for cold/warm cache measurements
//! - **Runtime Management**: Shared async runtime for database operations
//! - **Result Collection**: Integration with benchmark result storage
//!
//! # Usage
//!
//! The benchmark context is typically accessed through the global `BENCH_CONTEXT`:
//!
//! ```rust
//! use sinex_test_utils::bench::BENCH_CONTEXT;
//!
//! #[divan::bench]
//! fn bench_query(bencher: divan::Bencher) {
//!     let ctx = &*BENCH_CONTEXT;
//!     
//!     bencher.bench_local(|| {
//!         ctx.runtime.block_on(async {
//!             ctx.reset_and_load("small").await.unwrap();
//!             // Run benchmark operation
//!         })
//!     });
//! }
//! ```

use crate::db_common;
use color_eyre::eyre::{eyre, Result};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "bench")]
use crate::bench_results::BenchmarkResult;
use sinex_db::DbPool;

/// Global benchmark context for all benchmarks
#[cfg(feature = "bench")]
pub static BENCH_CONTEXT: Lazy<Arc<BenchContext>> = Lazy::new(|| {
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create benchmark runtime");

    runtime.block_on(async {
        BenchContext::new()
            .await
            .expect("Failed to initialize benchmark context")
    })
});

/// Benchmark context providing database access and utilities
#[cfg(feature = "bench")]
pub struct BenchContext {
    /// Database pool for benchmark operations
    pub pool: DbPool,
    /// Async runtime for database operations
    pub runtime: tokio::runtime::Runtime,
    /// Results collected during benchmarking
    pub results: Mutex<Vec<BenchmarkResult>>,
}

#[cfg(feature = "bench")]
impl BenchContext {
    /// Create a new benchmark context
    async fn new() -> Result<Arc<Self>> {
        // Use a dedicated benchmark database URL if provided
        let database_url = std::env::var("BENCH_DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_bench?host=/run/postgresql".to_string());

        // Create connection pool
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(20) // Higher for benchmark load
            .min_connections(5)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&database_url)
            .await?;

        // Run migrations if needed
        sinex_db::run_migrations(&pool)
            .await
            .map_err(|e| eyre!("Migration failed: {}", e))?;

        // Apply benchmark optimizations
        db_common::apply_test_optimizations(&pool).await?;

        // Create new runtime for benchmark operations
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4) // Limited threads for consistent results
            .enable_all()
            .build()?;

        Ok(Arc::new(Self {
            pool,
            runtime,
            results: Mutex::new(Vec::new()),
        }))
    }

    /// Reset database and load a fixture
    ///
    /// This is the primary method for preparing the database for benchmarks.
    /// It ensures a clean, known state before each benchmark iteration.
    ///
    /// # Fixtures
    ///
    /// - `empty` - Clean database
    /// - `small` - 1K events
    /// - `medium` - 100K events
    /// - `large` - 10M events
    /// - Custom fixtures from fixtures/datasets/
    pub async fn reset_and_load(&self, fixture: &str) -> Result<()> {
        db_common::reset_database(&self.pool).await?;
        db_common::load_fixture(&self.pool, fixture).await?;
        Ok(())
    }

    /// Clear PostgreSQL caches for cold cache measurements
    pub async fn clear_cache(&self) -> Result<()> {
        db_common::clear_pg_cache(&self.pool)
            .await
            .map_err(|e| eyre!("Failed to clear cache: {}", e))
    }

    /// Run a benchmark with isolation and dual measurement
    ///
    /// This method provides both cold and warm cache measurements for
    /// database operations, giving insight into cache effects.
    pub async fn run_isolated<F, Fut>(&self, dataset: &str, f: F) -> Result<DualMeasurement>
    where
        F: Fn(&DbPool) -> Fut + Clone,
        Fut: std::future::Future<Output = Result<()>>,
    {
        // Reset to known state
        self.reset_and_load(dataset).await?;

        // Cold cache measurement
        self.clear_cache().await?;
        let cold_start = Instant::now();
        f(&self.pool).await?;
        let cold_duration = cold_start.elapsed();

        // Warm cache measurement (immediate re-run)
        let warm_start = Instant::now();
        f(&self.pool).await?;
        let warm_duration = warm_start.elapsed();

        Ok(DualMeasurement {
            cold_cache: cold_duration,
            warm_cache: warm_duration,
        })
    }

    /// Get the database pool
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Convenience method for loading fixtures
    ///
    /// This is the primary way to load fixtures in benchmarks:
    /// ```
    /// ctx.load_fixture(&standard_fixtures::QUERY_BENCH_FIXTURE, DatasetSize::Medium).await?;
    /// ```
    pub async fn load_fixture(
        &self,
        fixture_set: &crate::static_fixtures::FixtureSet,
        size: crate::static_fixtures::DatasetSize,
    ) -> Result<()> {
        crate::static_fixtures::ensure_fixture(&self.pool, fixture_set, size)
            .await
            .map_err(|e| eyre!("Failed to load fixture: {}", e))
    }

    /// Load standard time-series fixture
    ///
    /// Convenience method for the most common benchmarking scenario.
    /// Provides realistic event distribution patterns.
    ///
    /// # Example
    /// ```rust
    /// ctx.time_series(DatasetSize::Medium).await?;
    /// ```
    pub async fn time_series(&self, size: crate::static_fixtures::DatasetSize) -> Result<()> {
        use crate::standard_fixtures::TIME_SERIES_FIXTURE;
        self.load_fixture(&TIME_SERIES_FIXTURE, size).await
    }

    /// Load standard query benchmark fixture
    ///
    /// Optimized for query performance testing with appropriate indexes
    /// and data distribution.
    ///
    /// # Example
    /// ```rust
    /// ctx.query_bench(DatasetSize::Large).await?;
    /// ```
    pub async fn query_bench(&self, size: crate::static_fixtures::DatasetSize) -> Result<()> {
        use crate::standard_fixtures::QUERY_BENCH_FIXTURE;
        self.load_fixture(&QUERY_BENCH_FIXTURE, size).await
    }

    /// Load standard load test fixture
    ///
    /// High-volume fixture for stress testing without checkpoints.
    /// Designed for pure write performance benchmarks.
    ///
    /// # Example
    /// ```rust
    /// ctx.load_test(DatasetSize::Large).await?;
    /// ```
    pub async fn load_test(&self, size: crate::static_fixtures::DatasetSize) -> Result<()> {
        use crate::standard_fixtures::LOAD_TEST_FIXTURE;
        self.load_fixture(&LOAD_TEST_FIXTURE, size).await
    }

    /// Load satellite benchmark fixture
    ///
    /// Fixture with patterns specific to satellite processing,
    /// including realistic source distributions and event types.
    ///
    /// # Example
    /// ```rust
    /// ctx.satellite_bench(DatasetSize::Medium).await?;
    /// ```
    pub async fn satellite_bench(&self, size: crate::static_fixtures::DatasetSize) -> Result<()> {
        use crate::standard_fixtures::SATELLITE_BENCH_FIXTURE;
        self.load_fixture(&SATELLITE_BENCH_FIXTURE, size).await
    }

    /// Load operations benchmark fixture
    ///
    /// Mixed read/write workload for operation performance testing.
    /// Includes both events and checkpoints for realistic processing scenarios.
    ///
    /// # Example
    /// ```rust
    /// ctx.operations_bench(DatasetSize::Small).await?;
    /// ```
    pub async fn operations_bench(&self, size: crate::static_fixtures::DatasetSize) -> Result<()> {
        use crate::standard_fixtures::OPERATIONS_FIXTURE;
        self.load_fixture(&OPERATIONS_FIXTURE, size).await
    }

    /// Record a benchmark result
    #[cfg(feature = "bench")]
    pub fn record_result(&self, result: BenchmarkResult) {
        let mut results = self.results.lock();
        results.push(result);
    }

    /// Get all recorded results
    #[cfg(feature = "bench")]
    pub fn get_results(&self) -> Vec<BenchmarkResult> {
        self.results.lock().clone()
    }
}

/// Measurement with both cold and warm cache timings
#[derive(Debug, Clone, Copy)]
pub struct DualMeasurement {
    /// Time with cold cache (after cache clear)
    pub cold_cache: Duration,
    /// Time with warm cache (immediate re-run)
    pub warm_cache: Duration,
}

impl DualMeasurement {
    /// Get the cache speedup ratio (warm/cold)
    pub fn cache_speedup(&self) -> f64 {
        self.cold_cache.as_secs_f64() / self.warm_cache.as_secs_f64()
    }

    /// Get the cache penalty in milliseconds
    pub fn cache_penalty_ms(&self) -> f64 {
        (self.cold_cache.as_secs_f64() - self.warm_cache.as_secs_f64()) * 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::DatasetSize;

    #[tokio::test]
    async fn test_dataset_size() {
        assert_eq!(DatasetSize::Small.fixture_name(), "small");
        assert_eq!(DatasetSize::Small.event_count(), 1_000);
        assert_eq!(DatasetSize::Large.fixture_name(), "large");
        assert_eq!(DatasetSize::Large.event_count(), 10_000_000);
    }

    #[tokio::test]
    async fn test_dual_measurement() {
        let measurement = DualMeasurement {
            cold_cache: Duration::from_millis(100),
            warm_cache: Duration::from_millis(25),
        };

        assert_eq!(measurement.cache_speedup(), 4.0);
        assert_eq!(measurement.cache_penalty_ms(), 75.0);
    }

    #[cfg(feature = "bench")]
    #[tokio::test]
    async fn test_bench_context_creation() -> Result<()> {
        // This will fail if database isn't available, which is OK for tests
        if std::env::var("BENCH_DATABASE_URL").is_ok() {
            let ctx = BenchContext::new().await?;

            // Verify pool works
            let result: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&ctx.pool).await?;
            assert_eq!(result, 1);
        }
        Ok(())
    }
}
