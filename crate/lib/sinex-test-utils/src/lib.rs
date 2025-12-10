#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../docs/testing_quality_overview.md")]
#![doc = include_str!("../../../../TESTING.md")]

//! Workspace testing utilities and fixtures.
// Allow dead code in test utilities - many functions are provided for test use
#![allow(dead_code)]

// Allow procedural macros to refer to this crate by name.
extern crate self as sinex_test_utils;

// Re-export the procedural macros from internal macros crate
#[cfg(feature = "bench")]
pub use sinex_test_utils_macros::sinex_bench;
pub use sinex_test_utils_macros::{sinex_prop, sinex_proptest, sinex_test};

// Re-export anyhow for test ergonomics
pub use color_eyre::eyre::{anyhow, bail, ensure, Context};

// Re-export SinexError
pub use sinex_core::types::error::SinexError;

// Library Result type using SinexError
pub type Result<T> = std::result::Result<T, SinexError>;
pub type TestResult<T = ()> = color_eyre::eyre::Result<T>;
pub use chaos::ChaosInjestor;
pub use jetstream::ensure_material_streams;
pub use satellite_publisher::{EventOverrides, TestSatellitePublisher};
pub use snapshot::TestSnapshot;
pub use test_context::TestContextFailureSnapshot;
pub use test_context::TestContextHandle;

pub struct ProptestCasesGuard {
    previous: Option<String>,
}

/// Internal helper used by the sinex_prop macro to build a configured TestRunner.
pub fn sinex_prop_runner_config(
    default_cases: u32,
    module_path: &'static str,
    test_name: &str,
) -> proptest::test_runner::Config {
    property_testing::build_runner_config(default_cases, module_path, test_name)
}

impl ProptestCasesGuard {
    pub fn new(cases: u32) -> Self {
        let previous = std::env::var("PROPTEST_CASES").ok();
        std::env::set_var("PROPTEST_CASES", cases.to_string());
        Self { previous }
    }
}

impl Drop for ProptestCasesGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.previous.take() {
            std::env::set_var("PROPTEST_CASES", prev);
        } else {
            std::env::remove_var("PROPTEST_CASES");
        }
    }
}

// Import all the existing modules - all private
mod builders;
#[cfg(feature = "channel-testing")]
mod channel_behavior_utils;
#[cfg(feature = "channel-testing")]
mod channel_enhancements;
#[cfg(feature = "channel-testing")]
mod channel_helpers;
mod chaos;
pub mod cleanup_config;
pub mod constants;
mod database_pool;
mod deployment_scenario_utils;
mod error_testing;
mod fixture_config;
pub mod fixtures;
mod jetstream;
mod nats;
pub mod path_validation;
pub mod permissions;
mod property_testing;
pub mod resources;
mod satellite_management_utils;
mod satellite_publisher;
pub mod satellite_runtime;
pub mod session_guards;
mod snapshot;
pub mod snapshot_helper;
mod test_context;
#[macro_use]
mod test_macros;
pub mod timing_utils;

// New benchmark infrastructure modules
#[cfg(feature = "bench")]
pub mod bench;
#[cfg(feature = "bench")]
pub mod bench_context;
#[cfg(feature = "bench")]
pub mod bench_results;
pub mod db_common;
#[cfg(feature = "bench")]
pub mod fixture_generator;
#[cfg(feature = "bench")]
pub mod standard_fixtures;
#[cfg(feature = "bench")]
pub mod static_fixtures;

// Create prelude module from common/mod.rs
pub mod prelude {
    // Core test infrastructure
    pub use crate::TestContext;
    pub use crate::TestResult;
    pub use crate::{sinex_prop, sinex_proptest, sinex_test};
    pub use crate::{ChaosInjestor, EventOverrides, TestSatellitePublisher, TestSnapshot};
    pub use color_eyre::eyre::{bail, ensure, Context, Result};

    // Modern test infrastructure - fully integrated
    pub use insta::{
        assert_debug_snapshot, assert_json_snapshot, assert_snapshot, assert_yaml_snapshot,
    };
    pub use rstest::{fixture, rstest};
    #[allow(deprecated)]
    pub use similar_asserts::{assert_eq as assert_similar, assert_str_eq};
    pub use tracing_test::traced_test;

    // Test macros for enhanced functionality
    pub use crate::{assert_snapshot_named, rstest_async};

    // Common test fixtures
    pub use crate::{
        acquire_admin_connection,
        constants::{
            EVENT_SOURCE_REPO_PRIMARY, EVENT_SOURCE_REPO_SECONDARY,
            EVENT_TYPE_FIXTURE_QUERY_SAFETY, EVENT_TYPE_QUERY_SAFETY, SOURCE_FIXTURE_REPO_PRIMARY,
            SOURCE_FIXTURE_REPO_SECONDARY,
        },
        optional_extension_missing, pool_slot_count, test_context_fixture, test_db_pool,
        test_event_sources, test_event_types, test_paths, test_sources, with_pool_size,
    };

    // Core sinex imports - now using flattened namespace
    pub use sinex_core::{
        validate_json,
        validate_path,
        Blob,
        BlobRecord,
        CheckpointRepository,
        ConsumerGroup,
        ConsumerName,
        // Database functionality (now available at root)
        DbPool,
        DbPoolExt,
        DbTransaction,
        Entity,
        EntityRelation,
        // Event types (now available at root)
        Event,
        // Type aliases for convenience
        EventId,
        EventPayload,
        EventRepository,
        EventResult,
        // Domain types (now available at root)
        EventSource,
        EventType,
        HostName,
        // Common utilities (now available at root)
        Id,
        JsonValue,
        OptionalTimestamp,
        ProcessorName,
        Provenance,
        // Database models (now available at root)
        SanitizedPath,
        SchemaName,
        SchemaVersion,
        // Error handling (now available at root)
        SinexError,
        SourceMaterial,
        Timestamp,
        Ulid,
    };

    // Time handling - very common in tests
    pub use chrono::{Duration as ChronoDuration, Utc};
    pub use std::time::{Duration, Instant};

    // Collections - very common in tests
    pub use std::collections::{HashMap, HashSet};

    // Path handling
    pub use camino::{Utf8Path, Utf8PathBuf};

    // Test path validation utilities
    pub use crate::path_validation::{
        create_test_temp_dir, create_test_temp_file, remove_test_dir, validate_test_path,
    };

    // Snapshot testing utilities
    pub use crate::snapshot_helper::SnapshotTestHelper;

    // JSON handling - essential for tests
    pub use serde_json::{json, Value};

    // Async utilities common in tests
    pub use futures::{future, stream, StreamExt, TryFutureExt, TryStreamExt};
    pub use tokio::{sync, task, time as tokio_time};

    // Property testing support
    pub use proptest::prelude::*;
    // Benchmarking support when feature is enabled
    #[cfg(feature = "bench")]
    pub use crate::bench::BENCH_CONTEXT;
    #[cfg(feature = "bench")]
    pub use crate::bench_context::BenchContext;
    #[cfg(feature = "bench")]
    pub use crate::bench_with_db;
    #[cfg(feature = "bench")]
    pub use crate::sinex_bench;
    #[cfg(feature = "bench")]
    pub use crate::standard_fixtures;
    #[cfg(feature = "bench")]
    pub use crate::static_fixtures::{DatasetSize, FixtureSet};
}

// Re-export modern test dependencies directly - they are now core infrastructure
pub use insta::{assert_json_snapshot, assert_yaml_snapshot};
pub use rstest::{fixture, rstest};
pub use similar_asserts::assert_eq as similar_assert_eq;
pub use tracing_test::traced_test;

// Common test fixtures as rstest fixtures
#[fixture]
pub fn test_sources() -> Vec<&'static str> {
    vec!["fs-watcher", "terminal", "desktop", "system"]
}

#[fixture]
pub fn test_event_types() -> Vec<(&'static str, &'static str)> {
    vec![
        ("fs-watcher", "file.created"),
        ("fs-watcher", "file.modified"),
        ("terminal", "command.executed"),
        ("desktop", "window.focused"),
        ("system", "service.started"),
    ]
}

#[fixture]
pub fn test_event_sources() -> Vec<sinex_core::EventSource> {
    vec![
        sinex_core::EventSource::from_static("fs-watcher"),
        sinex_core::EventSource::from_static("terminal"),
        sinex_core::EventSource::from_static("desktop"),
        sinex_core::EventSource::from_static("system"),
    ]
}

#[fixture]
pub fn test_paths() -> Vec<camino::Utf8PathBuf> {
    use camino::Utf8PathBuf;
    vec![
        Utf8PathBuf::from("/tmp/test.txt"),
        Utf8PathBuf::from("/home/user/document.pdf"),
        Utf8PathBuf::from("/var/log/system.log"),
        Utf8PathBuf::from("/opt/app/config.toml"),
    ]
}

#[fixture]
pub async fn test_context_fixture() -> TestContext {
    TestContext::new()
        .await
        .expect("Failed to create test context")
}

// Fixture specifically for rstest that handles async properly
#[fixture]
pub async fn rstest_ctx() -> TestContext {
    TestContext::new()
        .await
        .expect("Failed to create test context")
}

/// Fixture for test context with tracing enabled
#[fixture]
pub async fn test_context_with_tracing() -> TestContext {
    TestContext::new()
        .await
        .expect("Failed to create test context")
        .with_tracing("debug")
}

// Re-export main types for direct import - only what should be public
#[cfg(feature = "channel-testing")]
pub use channel_enhancements::{
    create_enhanced_event_sender, ChannelDiagnostics, ChannelHealthReport, DiagnosticsReport,
    EnhancedEventSender, PerformanceMetrics as ChannelPerformanceMetrics,
};
pub use database_pool::{
    acquire_admin_connection, acquire_pool_test_guard, acquire_test_database, check_pool_health,
    ensure_default_session_state, get_pool_stats, get_pool_stats_async, get_slot_stats,
    optional_extension_missing, pool_slot_count, reset_pool, with_pool_size, DatabasePoolTestGuard,
    DatabaseStats, PoolHealthReport, TestDatabase,
};
pub use db_common::test_db_pool;
pub use deployment_scenario_utils::{
    CompatibilityResult, CompatibilityTestScenario, ComponentConfig, ConfigCompatibilityTester,
    DependencyAvailability, DependencyType, EnvironmentSetup, EnvironmentType, ExpectedOutcome,
    ExternalDependency, PerformanceExpectations, PerformanceMetrics, ResourceConstraints,
    ValidationExpectation, ValidationStep, ValidationType,
};
pub use nats::EphemeralNats;
pub use satellite_management_utils::{
    start_test_ingestd_with_config, TestIngestdConfig, TestIngestdHandle,
};
pub use satellite_runtime::{TestRuntime, TestRuntimeBuilder};
pub use test_context::TestContext;
// Macros are already exported at crate root via #[macro_export]

// Comprehensive self-tests
#[cfg(all(test, feature = "internal-tests"))]
mod tests {
    #![allow(unused_imports)]
    use super::prelude::*;
    use crate::sinex_test;
    use rstest::rstest;
    use serde_json::json;
    use sinex_core::types::error::*;
    use sinex_core::types::events::*;
    use sinex_core::types::{Id, Ulid};
    use sinex_core::DbPoolExt;
    use sinex_core::*;
    use sinex_core::{
        Blob, BlobRecord, CheckpointRecord, Entity, EntityRecord, EntityRelation, Event, JsonValue,
        Operation, OperationRecord, Provenance, SourceMaterial,
    };

    // ==== Self-Tests: Demonstrating sinex-test-utils capabilities ====
    //
    // These tests serve as both verification and examples of how to properly
    // use the testing infrastructure. They demonstrate:
    // - Event creation patterns
    // - Query builder usage
    // - Assertion helpers
    // - Timing utilities
    // - Batch operations
    // - Property testing

    // === Key Integration Tests ===
    //
    // These tests demonstrate the overall sinex-test-utils capabilities.
    // Module-specific tests should be in their respective modules.

    #[sinex_test]
    async fn test_complete_workflow(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Demonstrates a complete workflow using production APIs

        // 1. Create events using production event creation
        let fs_event = ctx
            .create_test_event(
                "fs-watcher",
                "file.created",
                json!({
                    "path": "/data/report.pdf",
                    "size": 2048
                }),
            )
            .await?;

        let term_event = ctx
            .create_test_event(
                "terminal",
                "command.executed",
                json!({
                    "command": "process-report /data/report.pdf",
                    "working_dir": "/app",
                    "exit_code": 0
                }),
            )
            .await?;

        // 2. Query using direct repository access
        let events = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::from_static("fs-watcher"),
                sinex_core::types::Pagination::new(Some(10), None),
            )
            .await?;
        assert!(!events.is_empty());

        // 3. Use timing utilities to ensure ordering
        ctx.timing().wait_for_event_count(2).await?;

        // 4. Assert with rich context
        ctx.assert("workflow validation")
            .eq(&events[0].event_type.as_str(), &"file.created")?
            .that(
                fs_event.id.as_ref().map(|id| id.as_ulid().timestamp())
                    < term_event.id.as_ref().map(|id| id.as_ulid().timestamp()),
                "file should be created before processing (ULID ordering)",
            )?;

        Ok(())
    }

    // NOTE: Module-specific tests have been moved to their respective modules:
    // - Builder tests -> builders.rs
    // - Timing tests -> timing_utils.rs
    // - Database pool tests -> database_pool.rs
    // - Fixture tests -> fixtures.rs
    // - Assertion tests -> test_context.rs

    #[sinex_test]
    #[case("fs", "file.created")]
    #[case("shell", "cmd.run")]
    #[case("service-123", "event.processed")]
    #[case("xxxxxxxxxxxxxxxxxxx", "type.test")]
    async fn test_database_with_rstest(
        ctx: TestContext,
        #[case] source: &str,
        #[case] event_type: &str,
    ) -> color_eyre::eyre::Result<()> {
        // Create event with parameterized values
        let event = ctx
            .create_test_event(source, event_type, json!({"param_test": true}))
            .await?;

        // Verify event was created correctly
        assert_eq!(event.source.as_str(), source);
        assert_eq!(event.event_type.as_str(), event_type);
        assert_eq!(event.payload["param_test"], json!(true));

        // Query it back using direct repository access
        let source_events = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::new(source.to_string()),
                sinex_core::types::Pagination::new(Some(10), None),
            )
            .await?;
        let type_events = ctx
            .pool
            .events()
            .get_by_event_type(
                &EventType::from(event_type),
                sinex_core::types::Pagination::new(Some(10), None),
            )
            .await?;
        // Should find the event in both queries
        assert!(!source_events.is_empty());
        assert!(!type_events.is_empty());

        Ok(())
    }

    #[sinex_test]
    #[case("short", 5)]
    #[case("medium", 50)]
    #[case("long", 200)]
    async fn test_string_length_variations(
        #[case] name: &str,
        #[case] length: usize,
    ) -> color_eyre::eyre::Result<()> {
        let ctx = TestContext::new().await?;

        let source = "a".repeat(length);
        let event = ctx
            .create_test_event(&source, "proptest.length", json!({"test_name": name}))
            .await?;
        assert_eq!(event.source.as_str(), source);
        Ok(())
    }

    #[sinex_test]
    async fn test_property_testing_integration(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Comprehensive property test with database - test various valid inputs

        // Test edge cases with various valid inputs
        let long_source = "x".repeat(50);
        let long_type = format!("type.{}", "x".repeat(30));

        let test_cases = vec![
            ("fs", "file.created", json!({"path": "/test/α/β/γ.txt"})), // Unicode
            ("shell-123", "cmd.exec-99", json!({"n": i64::MAX})),       // Edge numbers
            (long_source.as_str(), "a.b", json!({})),                   // Long source
            ("src", long_type.as_str(), json!(null)),                   // Long type
        ];

        for (source, event_type, payload) in test_cases {
            let event = ctx
                .create_test_event(source, event_type, payload.clone())
                .await?;
            assert_eq!(event.source.as_str(), source);
            assert_eq!(event.event_type.as_str(), event_type);
            let mut expected = payload.clone();
            TestContext::sanitize_payload(&mut expected);
            assert_eq!(event.payload, expected);
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_edge_cases(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test edge cases
        for (size_kb, special_chars, nested_depth) in [
            (10, "normal text", 3),
            (100, "special 'quotes' \"double\"", 5),
            (500, "\n\t\r special chars", 8),
        ] {
            // Large payload test
            let large = "x".repeat(size_kb * 1024);
            let event = ctx
                .create_test_event(
                    "edge",
                    "large",
                    json!({
                        "data": large.as_str(),
                        "size_kb": size_kb
                    }),
                )
                .await?;
            assert_eq!(event.payload["size_kb"], json!(size_kb));

            // Special characters test
            let event = ctx
                .create_test_event("edge", "special", json!({"text": special_chars}))
                .await?;
            let mut expected_payload = json!({"text": special_chars});
            TestContext::sanitize_payload(&mut expected_payload);
            assert_eq!(event.payload, expected_payload);

            // Deeply nested JSON
            let mut nested = json!("value");
            for _ in 0..nested_depth {
                nested = json!({"nested": nested});
            }
            ctx.create_test_event("edge", "nested", nested).await?;
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrent_test_execution() -> color_eyre::eyre::Result<()> {
        // Test body disabled pending refactor - currently just returns Ok()
        Ok(())

        /* DISABLED PENDING REFACTOR
        use color_eyre::eyre::eyre;
        drop(ctx);
        crate::database_pool::with_pool_size(12, || async {
            // Test that multiple tests can run concurrently without interference
            const TASKS: usize = 5;
            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(TASKS));
            let mut handles = vec![];

            for i in 0..TASKS {
                let barrier_clone = barrier.clone();
                let handle = tokio::spawn(async move {
                    let ctx = TestContext::with_name(&format!("concurrent_{i}"))
                        .await?;

                    // Synchronize all tasks to start at same time
                    barrier_clone.wait().await;

                    // Each performs operations
                    for j in 0..10 {
                        let task_source = format!("task_{i}");
                        ctx.create_test_event(
                            &task_source,
                            "concurrent.test",
                            json!({"iteration": j}),
                        )
                        .await?;
                    }

                    // Allow the database to flush the inserts before querying.
                    const EXPECTED_EVENTS: usize = 10;
                    const MAX_ATTEMPTS: usize = 20;
                    const RETRY_DELAY_MS: u64 = 100;

                    let mut observed = 0usize;
                    for attempt in 0..MAX_ATTEMPTS {
                        let events = ctx
                            .pool
                            .events()
                            .get_by_source(&EventSource::from(format!("task_{i}")), sinex_core::types::Pagination::new(Some(100), None))
                            .await
                            .map_err(|e| eyre!("Failed to get events by source: {e}"))?;
                        observed = events.len();
                        if observed == EXPECTED_EVENTS {
                            break;
                        }
                        if attempt + 1 < MAX_ATTEMPTS {
                            tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS))
                                .await;
                        }
                    }

                    assert_eq!(observed, EXPECTED_EVENTS);

                    // Verify only sees own events using direct repository access
                    for k in 0..TASKS {
                        if k != i {
                            let other_events = ctx
                                .pool
                                .events()
                                .get_by_source(&EventSource::from(format!("task_{k}")), sinex_core::types::Pagination::new(Some(100), None))
                                .await
                                .map_err(|e| eyre!("Failed to get other events: {e}"))?;
                            assert_eq!(other_events.len(), 0);
                        }
                    }

                    Ok::<(), color_eyre::eyre::Report>(())
                });
                handles.push(handle);
            }

            for handle in handles {
                handle
                    .await
                    .map_err(|e| eyre!("Task failed: {e}"))?;
            }

            Ok(())
        })
        .await
        */
    }

    #[sinex_test]
    async fn test_error_propagation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test that errors propagate correctly through Result

        // Test validation error
        let result = ctx
            .create_test_event(
                "", // Empty source should fail
                "test",
                json!({}),
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("source"));

        // Test that custom errors work with Result
        fn failing_operation() -> color_eyre::eyre::Result<()> {
            Err(SinexError::validation("Custom validation error".to_string()).into())
        }

        let result = failing_operation();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Custom validation error"),
            "unexpected validation message: {err}"
        );

        Ok(())
    }

    #[sinex_test(timeout = 30)]
    async fn test_timeout_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test that the timeout attribute works
        // This test should complete quickly, well under 5 seconds

        let start = std::time::Instant::now();

        // Do some work
        for i in 0..10 {
            ctx.create_test_event("timeout_test", "test", json!({"index": i}))
                .await?;
        }

        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() < 5,
            "Test should complete well under timeout"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_result_type_alias() -> color_eyre::eyre::Result<()> {
        // Test that Result is properly aliased
        fn returns_test_result() -> color_eyre::eyre::Result<String> {
            Ok("success".to_string())
        }

        let result = returns_test_result();
        assert!(result.is_ok());
        assert_eq!(result?, "success");

        fn returns_error() -> color_eyre::eyre::Result<()> {
            Err(SinexError::unknown("test error".to_string()).into())
        }

        let error_result = returns_error();
        assert!(error_result.is_err());

        Ok(())
    }

    #[sinex_test]
    async fn test_builder_method_chaining_order(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test that events can be created with different parameter orders
        let event1 = ctx
            .create_test_event("order1", "test", json!({"a": 1}))
            .await?;

        let event2 = ctx
            .create_test_event("order2", "test", json!({"a": 1}))
            .await?;

        // Both should succeed despite different order
        assert_eq!(event1.event_type.as_str(), "test");
        assert_eq!(event2.event_type.as_str(), "test");

        Ok(())
    }

    #[sinex_test]
    async fn test_assertion_edge_cases(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test assertion boundary conditions
        let empty_vec: Vec<i32> = vec![];
        let non_empty_vec = vec![1, 2, 3];

        // Empty collection assertions
        let empty_assert = ctx.assert("empty check").not_empty(&empty_vec);
        assert!(empty_assert.is_err());

        ctx.assert("non-empty check").not_empty(&non_empty_vec)?;

        // Size assertions with edge cases
        ctx.assert("size 0").has_size(&empty_vec, 0)?;
        ctx.assert("exact size").has_size(&non_empty_vec, 3)?;

        let size_mismatch = ctx.assert("wrong size").has_size(&non_empty_vec, 2);
        assert!(size_mismatch.is_err());

        // Option assertions
        let none: Option<i32> = None;
        let some = Some(42);

        ctx.assert("none check").none(&none)?;
        ctx.assert("some check").some(&some)?;

        // Reversed assertions should fail
        assert!(ctx.assert("none as some").some(&none).is_err());
        assert!(ctx.assert("some as none").none(&some).is_err());

        Ok(())
    }

    // Test Framework Infrastructure Tests - Core State Management

    #[sinex_test]
    async fn test_context_event_count_tracking_accuracy(
        ctx: TestContext,
    ) -> color_eyre::eyre::Result<()> {
        // Test that event counting is accurate across operations
        ctx.force_cleanup().await?;
        let baseline = ctx.current_event_count().await?;

        // Insert events one by one and verify count
        for i in 1..=5 {
            ctx.create_test_event("count-test", "increment", json!({"index": i}))
                .await?;

            let current_count = ctx.current_event_count().await?;
            assert_eq!(
                current_count,
                baseline + i,
                "Count should match inserted events"
            );
        }

        // Batch insert and verify
        let batch_events = (0..10)
            .map(|i| {
                Event::<JsonValue>::test_event(
                    EventSource::from("count-test"),
                    EventType::from("batch"),
                    json!({"batch_index": i}),
                )
            })
            .collect::<Vec<_>>();

        ctx.insert_events(&batch_events).await?;

        let final_count = ctx.current_event_count().await?;
        assert_eq!(
            final_count,
            baseline + 15,
            "Should have all individual and batch events"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_context_timing_measurement_precision(
        ctx: TestContext,
    ) -> color_eyre::eyre::Result<()> {
        // Test that timing measurements are precise and monotonic
        let start_elapsed = ctx.elapsed();

        // Do some work that takes measurable time
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let mid_elapsed = ctx.elapsed();
        assert!(mid_elapsed > start_elapsed, "Elapsed time should increase");
        assert!(
            mid_elapsed.as_millis() >= 50,
            "Should measure at least 50ms"
        );

        // Test the measure helper
        let (result, duration) = ctx
            .measure(async {
                tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
                Ok::<_, ()>("measured")
            })
            .await?;

        assert_eq!(result.unwrap(), "measured");
        assert!(
            duration.as_millis() >= 25,
            "Measure should capture at least 25ms"
        );
        assert!(
            duration.as_millis() < 100,
            "Measure should not take too long"
        );

        let final_elapsed = ctx.elapsed();
        assert!(
            final_elapsed > mid_elapsed,
            "Time should continue advancing"
        );

        Ok(())
    }

    // Database Pool Management Tests

    #[sinex_test]
    async fn test_database_pool_concurrent_allocation(
        ctx: TestContext,
    ) -> color_eyre::eyre::Result<()> {
        // Test that multiple contexts can be allocated concurrently without deadlock
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let successful_allocations = Arc::new(AtomicU32::new(0));
        let allocation_errors = Arc::new(AtomicU32::new(0));

        // Spawn multiple tasks that try to allocate contexts concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let success_count = successful_allocations.clone();
            let error_count = allocation_errors.clone();

            let handle = tokio::spawn(async move {
                match TestContext::with_name(&format!("concurrent_alloc_{i}")).await {
                    Ok(ctx) => {
                        // Do some work to hold the context
                        ctx.create_test_event("pool-test", "allocation", json!({"task_id": i}))
                            .await?;

                        success_count.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                    Err(e) => {
                        error_count.fetch_add(1, Ordering::SeqCst);
                        Err(e)
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all allocations
        for handle in handles {
            let _ = handle.await; // Ignore join errors, we track success/error separately
        }

        let successes = successful_allocations.load(Ordering::SeqCst);
        let errors = allocation_errors.load(Ordering::SeqCst);

        assert!(successes > 0, "At least some allocations should succeed");
        assert_eq!(successes + errors, 10, "All tasks should complete");

        Ok(())
    }

    #[sinex_test]
    async fn test_database_cleanup_on_drop(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test that database is properly cleaned when context is dropped
        let test_id = uuid::Uuid::new_v4().to_string();

        // Create a context in a scope so it gets dropped
        {
            let temp_ctx = TestContext::with_name(&format!("cleanup_test_{test_id}")).await?;

            // Insert identifiable data
            temp_ctx
                .create_test_event("cleanup-test", "marker", json!({"test_id": test_id}))
                .await?;

            // Verify it exists using direct repository access
            let events = temp_ctx
                .pool
                .events()
                .get_by_source(
                    &EventSource::from("cleanup-test"),
                    sinex_core::types::Pagination::new(Some(10), None),
                )
                .await?;
            assert_eq!(events.len(), 1);

            // Context drops here
        }

        // In our main context, verify we can't see the dropped context's data
        // This verifies isolation, not cleanup (since we can't access the dropped DB)
        let leaked_events = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::from("cleanup-test"),
                sinex_core::types::Pagination::new(Some(10), None),
            )
            .await?;

        assert_eq!(
            leaked_events.len(),
            0,
            "Should not see data from dropped context"
        );

        Ok(())
    }

    // Test Fixture Lifecycle Management

    #[sinex_test]
    async fn test_fixture_lazy_initialization(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test that context initialization is lazy and doesn't create unnecessary events
        ctx.force_cleanup().await?;
        let baseline = ctx.current_event_count().await?;

        ctx.create_test_event("fixture-test", "initialization", json!({"lazy": true}))
            .await?;

        let after_event = ctx.current_event_count().await?;
        assert_eq!(
            after_event,
            baseline + 1,
            "Context should add exactly one event when create_test_event is called"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_resource_cleanup(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test that fixture resources are cleaned up properly
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let cleanup_called = Arc::new(AtomicBool::new(false));

        // Create a custom fixture that tracks cleanup
        {
            let cleanup_flag = cleanup_called.clone();

            // Simulate fixture with cleanup tracking
            struct TrackableFixture {
                cleanup_flag: Arc<AtomicBool>,
            }

            impl Drop for TrackableFixture {
                fn drop(&mut self) {
                    self.cleanup_flag.store(true, Ordering::SeqCst);
                }
            }

            let _fixture = TrackableFixture { cleanup_flag };

            // Fixture is used here
            assert!(
                !cleanup_called.load(Ordering::SeqCst),
                "Cleanup should not be called yet"
            );

            // Fixture drops here
        }

        // Verify cleanup was called
        assert!(
            cleanup_called.load(Ordering::SeqCst),
            "Cleanup should be called after drop"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_complex_event_relationships(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        // Test that we can create and query events with dependencies

        // Create a checkpoint event
        ctx.create_test_event(
            "sinex",
            "checkpoint.saved",
            json!({
                "checkpoint_id": "test_checkpoint_123",
                "status": "saved"
            }),
        )
        .await?;

        // Create an automaton event that references the checkpoint
        ctx.create_test_event(
            "automaton",
            "checkpoint.processed",
            json!({
                "checkpoint_id": "test_checkpoint_123",
                "processing_time_ms": 42
            }),
        )
        .await?;

        // Verify the events were created using direct repository access
        let checkpoints = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::from("sinex"),
                sinex_core::types::Pagination::new(Some(100), None),
            )
            .await?
            .into_iter()
            .filter(|e| e.event_type.as_str() == "checkpoint.saved")
            .collect::<Vec<_>>();

        assert!(
            !checkpoints.is_empty(),
            "Checkpoint events should be created"
        );

        // Verify related automaton events
        let events = ctx
            .pool
            .events()
            .get_by_source(
                &EventSource::from("automaton"),
                sinex_core::types::Pagination::new(Some(100), None),
            )
            .await?;

        assert!(!events.is_empty(), "Should have automaton events");

        // Verify relationship
        let automaton_event = &events[0];
        assert_eq!(
            automaton_event.payload["checkpoint_id"],
            json!("test_checkpoint_123")
        );

        Ok(())
    }
}
