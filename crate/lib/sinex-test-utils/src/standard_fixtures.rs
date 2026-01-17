//! Standard fixtures for common benchmarking scenarios
//!
//! This module provides universal fixtures that are useful across multiple crates.
//! These cover common patterns like:
//! - Basic event distributions
//! - Time-series patterns
//! - Load testing scenarios
//!
//! Crates should still define domain-specific fixtures for their unique needs,
//! but can use these for standard benchmarking scenarios.

use crate::static_fixtures::{DatasetSize, FixtureConfig, FixtureSet};
use camino::Utf8PathBuf;
use once_cell::sync::Lazy;

/// Standard time-series fixture with realistic event distribution
///
/// Simulates a typical production pattern:
/// - Events clustered during business hours
/// - Lower activity at night
/// - Bursts of activity
/// - Multiple event sources
pub static TIME_SERIES_FIXTURE: Lazy<FixtureSet> = Lazy::new(|| {
    FixtureSet::new()
        .with_events(DatasetSize::Small, 2024)
        .with_events(DatasetSize::Medium, 2025)
        .with_events(DatasetSize::Large, 2026)
        .with_checkpoints(100) // Checkpoint every 100 events
        .with_operations(50)
        .with_config(FixtureConfig {
            base_dir: Utf8PathBuf::from("target/bench-fixtures/time-series"),
            ..Default::default()
        })
});

/// Load testing fixture with high event volume
///
/// Designed for stress testing:
/// - Maximum event throughput
/// - Large payloads
/// - Concurrent sources
/// - No checkpoints (pure write load)
pub static LOAD_TEST_FIXTURE: Lazy<FixtureSet> = Lazy::new(|| {
    FixtureSet::new()
        .with_events(DatasetSize::Medium, 9999)
        .with_events(DatasetSize::Large, 8888)
        .with_events(DatasetSize::Custom(50_000_000), 7777) // 50M events
        .with_checkpoints(0) // No checkpoints for pure load testing
        .with_operations(0)
        .with_config(FixtureConfig {
            base_dir: Utf8PathBuf::from("target/bench-fixtures/load-test"),
            ..Default::default()
        })
});

/// Query benchmark fixture with varied data patterns
///
/// Optimized for testing query performance:
/// - Multiple event types for filtering
/// - Time-based distribution for range queries
/// - Checkpoints for join operations
/// - Mixed payload sizes
pub static QUERY_BENCH_FIXTURE: Lazy<FixtureSet> = Lazy::new(|| {
    FixtureSet::new()
        .with_events(DatasetSize::Small, 1111)
        .with_events(DatasetSize::Medium, 2222)
        .with_checkpoints(1000) // More checkpoints for join testing
        .with_operations(100)
        .with_config(FixtureConfig {
            base_dir: Utf8PathBuf::from("target/bench-fixtures/query"),
            ..Default::default()
        })
});

/// Minimal fixture for smoke tests and quick checks
///
/// Just enough data to verify functionality:
/// - Small event count
/// - Basic checkpoints
/// - Fast generation
pub static SMOKE_TEST_FIXTURE: Lazy<FixtureSet> = Lazy::new(|| {
    FixtureSet::new()
        .with_events(DatasetSize::Custom(100), 42) // Just 100 events
        .with_checkpoints(10)
        .with_operations(5)
        .with_config(FixtureConfig {
            base_dir: Utf8PathBuf::from("target/bench-fixtures/smoke"),
            max_age_days: None, // Never expire smoke test data
            ..Default::default()
        })
});

/// Integration test fixture with mixed workload
///
/// Simulates real-world usage patterns:
/// - Various event sources
/// - Checkpoint processing
/// - Operations lifecycle
/// - Moderate data volume
pub static INTEGRATION_FIXTURE: Lazy<FixtureSet> = Lazy::new(|| {
    FixtureSet::new()
        .with_events(DatasetSize::Small, 3333)
        .with_events(DatasetSize::Custom(10_000), 4444)
        .with_checkpoints(50)
        .with_operations(25)
        .with_config(FixtureConfig {
            base_dir: Utf8PathBuf::from("target/test-fixtures/integration"),
            max_age_days: Some(7), // Regenerate weekly for integration tests
            ..Default::default()
        })
});

/// Satellite benchmark fixture
///
/// For testing satellite-specific patterns:
/// - Burst patterns (scanner mode)
/// - Continuous streams (sensor mode)
/// - Error scenarios
/// - Retry patterns
pub static SATELLITE_BENCH_FIXTURE: Lazy<FixtureSet> = Lazy::new(|| {
    FixtureSet::new()
        .with_events(DatasetSize::Small, 5555)
        .with_events(DatasetSize::Medium, 6666)
        .with_checkpoints(500) // Frequent checkpoints for satellite progress
        .with_operations(10)
        .with_config(FixtureConfig {
            base_dir: Utf8PathBuf::from("target/bench-fixtures/satellite"),
            ..Default::default()
        })
});

/// Operations benchmark fixture with mixed workload
///
/// Combination of:
/// - Event insertions
/// - Query operations
/// - Checkpoint management
/// - Mixed read/write patterns
pub static OPERATIONS_FIXTURE: Lazy<FixtureSet> = Lazy::new(|| {
    FixtureSet::new()
        .with_events(DatasetSize::Small, 4321)
        .with_events(DatasetSize::Medium, 4322)
        .with_events(DatasetSize::Large, 4323)
        .with_checkpoints(200)
        .with_operations(500)
        .with_config(FixtureConfig {
            base_dir: Utf8PathBuf::from("target/bench-fixtures/operations"),
            ..Default::default()
        })
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;

    #[sinex_test]
    fn test_standard_fixtures_have_unique_seeds() -> TestResult<()> {
        // Ensure each fixture uses different seeds to avoid correlation
        let fixtures = vec![
            &*TIME_SERIES_FIXTURE,
            &*LOAD_TEST_FIXTURE,
            &*QUERY_BENCH_FIXTURE,
            &*SMOKE_TEST_FIXTURE,
            &*INTEGRATION_FIXTURE,
            &*SATELLITE_BENCH_FIXTURE,
        ];

        let mut seeds = std::collections::HashSet::new();
        for fixture in fixtures {
            for (_, &seed) in &fixture.events {
                assert!(seeds.insert(seed), "Duplicate seed found: {}", seed);
            }
        }
        Ok(())
    }

    #[sinex_test]
    fn test_standard_fixtures_have_appropriate_sizes() -> TestResult<()> {
        // Smoke test should be small
        assert!(SMOKE_TEST_FIXTURE
            .events
            .contains_key(&DatasetSize::Custom(100)));

        // Load test should have large option
        assert!(LOAD_TEST_FIXTURE.events.contains_key(&DatasetSize::Large));

        // Query bench should have medium for reasonable performance
        assert!(QUERY_BENCH_FIXTURE
            .events
            .contains_key(&DatasetSize::Medium));
        Ok(())
    }
}

#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use crate::database_pool::acquire_test_database;
    use crate::static_fixtures::ensure_fixture;
    use crate::{sinex_bench, TestResult};

    // Benchmark the standard fixtures themselves
    #[sinex_bench]
    fn bench_time_series_small() -> TestResult<()> {
        let db = acquire_test_database().await?;
        ensure_fixture(db.pool(), &TIME_SERIES_FIXTURE, DatasetSize::Small).await?;
        // Just measure fixture load time
        Ok(())
    }

    #[sinex_bench]
    fn bench_query_fixture_medium() -> TestResult<()> {
        let db = acquire_test_database().await?;
        ensure_fixture(db.pool(), &QUERY_BENCH_FIXTURE, DatasetSize::Medium).await?;
        // Measure fixture generation/load
        Ok(())
    }
}
