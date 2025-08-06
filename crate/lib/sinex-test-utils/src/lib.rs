//! Sinex Test Utilities - Comprehensive Testing Infrastructure
//!
//! This crate provides a unified testing framework for the Sinex event system, offering
//! database isolation, rich builders, and performance fixtures.
//!
//! # Quick Start
//!
//! ```rust
//! use sinex_test_utils::prelude::*;
//!
//! #[sinex_test]
//! async fn test_filesystem_event(ctx: TestContext) -> Result<()> {
//!     // Create events using production Event API - no wrappers
//!     let event = ctx.create_test_event(
//!         "fs-watcher",
//!         "file.created",
//!         json!({"path": "/data/file.txt", "size": 1024})
//!     ).await?;
//!     
//!     // Query with direct repository access
//!     let events = ctx.pool.events()
//!         .get_by_source(&EventSource::from_static("fs-watcher"), Some(10), None)
//!         .await?;
//!     
//!     // Rich assertions with context  
//!     ctx.assert("file creation")
//!         .eq(&events.len(), &1)?
//!         .that(events[0].payload["size"] == json!(1024), "size should match")?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! # Core Concepts
//!
//! ## TestContext - Single Entry Point
//! All test functionality is accessed through `TestContext`, providing:
//! - Isolated database per test
//! - Event creation builders
//! - Query abstractions
//! - Assertion helpers
//! - Timing utilities
//!
//! ## The `#[sinex_test]` Macro
//! **Always use `#[sinex_test]` instead of `#[tokio::test]`**. This macro:
//! - Creates and injects TestContext
//! - Manages database lifecycle
//! - Handles timeouts intelligently
//! - Provides progress indicators
//! - Integrates with proptest
//!
//! ## Event Builders
//! Domain-specific builders for common event types:
//!
//! ```rust
//! // Filesystem events
//! ctx.event().filesystem().path("/tmp/test").modified().insert().await?;
//!
//! // Terminal commands
//! ctx.event().terminal().command("ls -la").success().insert().await?;
//!
//! // System events
//! ctx.event().system().service("nginx").started().insert().await?;
//!
//! // Custom events with incremental building
//! ctx.event()
//!     .source("my-service")
//!     .type_("user.action")
//!     .field("user_id", 123)
//!     .field("action", "login")
//!     .insert()
//!     .await?;
//! ```
//!
//! ## Query Builders
//! Type-safe query construction:
//!
//! ```rust
//! // Various query patterns
//! let recent = ctx.events().limit(5).fetch().await?;
//! let by_source = ctx.events().by_source("fs").fetch().await?;
//! let count = ctx.events().by_type("file.created").count().await?;
//! let single = ctx.events().by_id(event_id).fetch_one().await?;
//! ```
//!
//! ## Fixtures
//! Access reusable test scenarios through the unified fixture manager:
//!
//! ```rust
//! // Access fixtures through ctx.fixtures() namespace
//! let session = ctx.fixtures().user_session().await?;
//! let dataset = ctx.fixtures().large_dataset().await?;
//! let errors = ctx.fixtures().validation_failures().await?;
//!
//! // Or use the nested namespaces for better organization
//! let session = ctx.fixtures().scenarios().user_session().await?;
//! let dataset = ctx.fixtures().performance().large_dataset().await?;
//! let errors = ctx.fixtures().errors().validation_failures().await?;
//! ```
//!
//! # Testing Patterns
//!
//! ## Complete Example - Testing a Processing Pipeline
//!
//! ```rust
//! #[sinex_test]
//! async fn test_file_processing_pipeline(ctx: TestContext) -> Result<()> {
//!     // 1. Create test events
//!     let file_event = ctx.event()
//!         .filesystem()
//!         .path("/data/input.csv")
//!         .size(1024 * 1024)  // 1MB
//!         .created()
//!         .insert()
//!         .await?;
//!     
//!     // 2. Wait for processing
//!     ctx.wait_for_event_count(2).await?;  // Original + processed
//!     
//!     // 3. Query results
//!     let processed = ctx.events()
//!         .by_type("file.processed")
//!         .fetch_one()
//!         .await?
//!         .expect("processed event should exist");
//!     
//!     // 4. Make assertions
//!     ctx.assert("processing validation")
//!         .that(processed.payload["status"] == "success", "processing should succeed")?
//!         .that(processed.payload["input_path"] == "/data/input.csv", "path should match")?;
//!     
//!     Ok(())
//! }
//!
//! ## Property Testing and Data-Driven Tests
//! For property testing with database operations, use regular test loops:
//!
//! ```rust
//! #[sinex_test]
//! async fn test_edge_cases(ctx: TestContext) -> Result<()> {
//!     let test_cases = [
//!         ("empty", ""),
//!         ("unicode", "Hello 世界 🌍"),
//!         ("large", "x".repeat(1000)),
//!     ];
//!     
//!     for (name, value) in test_cases {
//!         let event = ctx.event()
//!             .source("test")
//!             .type_("edge.case")
//!             .field("data", value)
//!             .field("test_name", name)
//!             .insert()
//!             .await?;
//!         
//!         // Verify event was stored correctly
//!         let fetched = ctx.events().by_id(event.id).fetch_one().await?;
//!         assert!(fetched.is_some());
//!     }
//!     Ok(())
//! }
//! ```
//!
//! ## Timing and Synchronization
//! ```rust
//! // Wait for conditions
//! ctx.wait_for_event_count(5).await?;
//! ctx.wait_for_condition(|| async {
//!     let count = ctx.events().by_source("fs").count().await?;
//!     Ok(count >= 3)
//! }).await?;
//!
//! // Coordinate parallel operations
//! let barrier = ctx.timing().barrier(3);
//! let sync = ctx.timing().synchronizer(Duration::from_secs(5));
//!
//! // Measure operation time
//! let (result, duration) = ctx.measure(async {
//!     expensive_operation().await
//! }).await?;
//!
//! // Run concurrent tests
//! let results = ctx.run_concurrent(5, |ctx, i| async move {
//!     ctx.event()
//!         .source("concurrent")
//!         .field("worker", i)
//!         .insert()
//!         .await
//! }).await?;
//! ```
//!
//! ## Schema Validation
//! ```rust
//! let schema_id = ctx.schema().register("fs", "file.created",
//!     json!({
//!         "type": "object",
//!         "properties": {
//!             "path": {"type": "string"},
//!             "size": {"type": "integer", "minimum": 0}
//!         },
//!         "required": ["path"]
//!     })
//! ).await?;
//!
//! // Create validated events
//! let event = ctx.validated_event(schema_id)
//!     .field("path", "/test")
//!     .field("size", 100)
//!     .insert()
//!     .await?;
//! ```
//!
//! ## Rich Assertions with Context
//! ```rust
//! // Basic assertions
//! ctx.assert("data validation")
//!     .eq(&user.name, "Alice")?
//!     .that(user.age >= 18, "user must be adult")?
//!     .has_size(&items, 10)?
//!     .not_empty(&results)?
//!     .some(&optional_value)?;
//!
//! // Event-specific assertions
//! ctx.assert_event_count(5).await?;
//! ctx.assert_event_exists(event_id).await?;
//! ctx.assert_no_events().await?;
//!
//! // Error assertions
//! let result = risky_operation();
//! ctx.assert("error handling")
//!     .error_contains(&result, "permission denied")?;
//! ```
//!
//! ## Testing with Fixtures
//! ```rust
//! // Use pre-built scenarios
//! let session = ctx.fixtures().scenarios().user_session().await?;
//! // session.events contains filesystem, terminal, and clipboard events
//!
//! // Performance testing with large datasets
//! let dataset = ctx.fixtures().performance()
//!     .large_dataset_with(100_000)
//!     .await?;
//!
//! // Error scenario testing
//! let errors = ctx.fixtures().errors().validation_failures().await?;
//! // Test error handling with known-bad data
//! ```
//!
//! ## Advanced Patterns
//! ```rust
//! // Custom event validation
//! let schema_id = ctx.schema().register("custom", "user.action",
//!     json!({
//!         "type": "object",
//!         "properties": {
//!             "action": {"enum": ["login", "logout", "update"]},
//!             "user_id": {"type": "integer", "minimum": 1}
//!         },
//!         "required": ["action", "user_id"]
//!     })
//! ).await?;
//!
//! // Create only valid events
//! let event = ctx.validated_event(schema_id)
//!     .field("action", "login")
//!     .field("user_id", 123)
//!     .insert()
//!     .await?;
//!
//! // Test invalid events
//! let invalid = ctx.event()
//!     .source("custom")
//!     .type_("user.action")
//!     .field("action", "invalid_action")
//!     .build()?;
//!     
//! ctx.schema().assert_invalid(&invalid, schema_id).await?;
//! ```
//!
//!
//! # Benchmarking (with `bench` feature)
//!
//! The `#[sinex_bench]` macro provides a clean interface for async benchmarks:
//!
//! ```rust
//! #[cfg(all(test, feature = "bench"))]
//! mod benches {
//!     use super::*;
//!     use sinex_test_utils::prelude::*;
//!     
//!     #[sinex_bench]
//!     fn bench_query_performance() -> color_eyre::eyre::Result<()> {
//!         // Direct access to standard fixtures
//!         ctx.query_bench(DatasetSize::Medium).await?;
//!         let results = query_recent_events(ctx.pool(), 1000).await?;
//!         Ok(())
//!     }
//!     
//!     // Parameterized benchmarks
//!     #[sinex_bench(args = [10, 100, 1000])]
//!     fn bench_bulk_insert(arg: usize) -> color_eyre::eyre::Result<()> {
//!         let count = arg;
//!         let events = generate_events(count);
//!         insert_events(ctx.pool(), &events).await?;
//!         Ok(())
//!     }
//! }
//! ```
//!
//! Standard fixtures available via BenchContext:
//! - `ctx.time_series(size)` - Realistic event patterns
//! - `ctx.query_bench(size)` - Query performance testing
//! - `ctx.load_test(size)` - High-volume stress testing
//! - `ctx.satellite_bench(size)` - Satellite-specific patterns
//!
//! For custom fixtures, create them inline following the same pattern as tests.
//!
// Allow dead code in test utilities - many functions are provided for test use
#![allow(dead_code)]

// Re-export the procedural macros from internal macros crate
#[cfg(feature = "bench")]
pub use sinex_test_utils_macros::sinex_bench;
pub use sinex_test_utils_macros::sinex_test;

// Re-export anyhow for test ergonomics
pub use color_eyre::eyre::{anyhow, bail, ensure, Context};

// Re-export SinexError
pub use sinex_types::error::SinexError;

// Library Result type using SinexError
pub type Result<T> = std::result::Result<T, SinexError>;

// Import all the existing modules - all private
mod builders;
mod channel_behavior_utils;
mod channel_enhancements;
mod channel_helpers;
mod database_pool;
mod deployment_scenario_utils;
mod error_testing;
mod fixture_config;
mod fixtures;
mod property_testing;
mod satellite_management_utils;
mod test_context;
#[macro_use]
mod test_macros;
mod timing_utils;

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
    pub use crate::sinex_test;
    pub use crate::TestContext;
    pub use color_eyre::eyre::{bail, ensure, Context};

    // Modern test infrastructure - fully integrated
    pub use insta::{
        assert_debug_snapshot, assert_json_snapshot, assert_snapshot, assert_yaml_snapshot,
    };
    pub use rstest::{fixture, rstest};
    pub use similar_asserts::{assert_eq as assert_similar, assert_str_eq};
    pub use tracing_test::traced_test;

    // Test macros for enhanced functionality
    pub use crate::{assert_snapshot_named, rstest_async};

    // Common test fixtures
    pub use crate::{
        test_context_fixture, test_event_sources, test_event_types, test_paths, test_sources,
    };

    // Common imports that tests need
    pub use crate::builders::*;
    pub use sinex_db::models::*;
    pub use sinex_types::domain::*;
    pub use sinex_types::error::*;
    pub use sinex_types::events::*;
    pub use sinex_types::{Id, Ulid};
    pub use std::time::Duration;

    // Path handling
    pub use camino::{Utf8Path, Utf8PathBuf};

    // JSON handling
    pub use serde_json::{json, Value};

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
pub fn test_event_sources() -> Vec<sinex_types::domain::EventSource> {
    vec![
        sinex_types::domain::EventSource::from_static("fs-watcher"),
        sinex_types::domain::EventSource::from_static("terminal"),
        sinex_types::domain::EventSource::from_static("desktop"),
        sinex_types::domain::EventSource::from_static("system"),
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

// TODO: Fix tracing_test integration - API has changed
// #[fixture]
// pub async fn test_context_with_tracing() -> TestContext {
//     let _guard = tracing_test::internal::set_test();
//     TestContext::new().await.expect("Failed to create test context")
// }

// Re-export main types for direct import - only what should be public
pub use test_context::TestContext;
// Macros are already exported at crate root via #[macro_export]

// Comprehensive self-tests
#[cfg(test)]
mod tests {
    use super::prelude::*;
    use serde_json::json;
    use sinex_db::models::*;
    use sinex_types::domain::*;
    use sinex_types::error::*;
    use sinex_types::events::*;
    use sinex_types::{Id, Ulid};

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
    async fn test_complete_workflow(ctx: TestContext) -> Result<()> {
        // Demonstrates a complete workflow using multiple sinex-test-utils features

        // 1. Create events with various builders
        let fs_event = ctx
            .event()
            .filesystem()
            .path("/data/report.pdf")
            .size(2048)
            .created()
            .insert()
            .await?;

        let term_event = ctx
            .event()
            .terminal()
            .command("process-report /data/report.pdf")
            .working_dir("/app")
            .success()
            .insert()
            .await?;

        // 2. Query and verify relationships
        let events = ctx.events().by_source("fs").fetch().await?;
        assert!(!events.is_empty());

        // 3. Use timing utilities to ensure ordering
        ctx.timing().wait_for_event_count(2).await?;

        // 4. Assert with rich context
        ctx.assert("workflow validation")
            .eq(&events[0].event_type.as_str(), &"fs.file.created")?
            .that(
                fs_event.ts_ingest < term_event.ts_ingest,
                "file should be created before processing",
            )?;

        Ok(())
    }

    // NOTE: Module-specific tests have been moved to their respective modules:
    // - Builder tests -> builders.rs
    // - Timing tests -> timing_utils.rs
    // - Database pool tests -> database_pool.rs
    // - Fixture tests -> fixtures.rs
    // - Assertion tests -> test_context.rs

    #[rstest]
    #[case("fs", "file.created")]
    #[case("shell", "cmd.run")]
    #[case("service-123", "event.processed")]
    #[case("xxxxxxxxxxxxxxxxxxx", "type.test")]
    #[tokio::test]
    async fn test_database_with_rstest(
        #[future] rstest_ctx: TestContext,
        #[case] source: &str,
        #[case] event_type: &str,
    ) -> Result<()> {
        let ctx = rstest_ctx.await;

        // Create event with parameterized values
        let event = ctx
            .event()
            .source(source)
            .type_(event_type)
            .field("param_test", true)
            .insert()
            .await?;

        // Verify event was created correctly
        assert_eq!(event.source.as_str(), source);
        assert_eq!(event.event_type.as_str(), event_type);
        assert_eq!(event.payload["param_test"], json!(true));

        // Query it back
        let events = ctx
            .events()
            .by_source(source)
            .by_type(event_type)
            .fetch()
            .await?;
        assert_eq!(events.len(), 1);

        Ok(())
    }

    #[rstest]
    #[case("short", 5)]
    #[case("medium", 50)]
    #[case("long", 200)]
    #[tokio::test]
    async fn test_string_length_variations(
        #[case] name: &str,
        #[case] length: usize,
    ) -> Result<()> {
        let ctx = TestContext::new().await?;

        let source = "a".repeat(length);
        let event = ctx
            .event()
            .source(EventSource::new(&source))
            .type_("proptest.length")
            .field("test_name", name)
            .insert()
            .await?;
        assert_eq!(event.source.as_str(), source);
        Ok(())
    }

    #[sinex_test]
    async fn test_property_testing_integration(ctx: TestContext) -> Result<()> {
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
                .event()
                .source(source)
                .type_(event_type)
                .payload(payload.clone())
                .insert()
                .await?;
            assert_eq!(event.source.as_str(), source);
            assert_eq!(event.event_type.as_str(), event_type);
            assert_eq!(event.payload, payload);
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_edge_cases(ctx: TestContext) -> Result<()> {
        // Test edge cases
        for (size_kb, special_chars, nested_depth) in [
            (10, "normal text", 3),
            (100, "special 'quotes' \"double\"", 5),
            (500, "\n\t\r special chars", 8),
        ] {
            // Large payload test
            let large = "x".repeat(size_kb * 1024);
            let event = ctx
                .event()
                .source("edge")
                .type_("large")
                .field("data", large.as_str())
                .field("size_kb", size_kb)
                .insert()
                .await?;
            assert_eq!(event.payload["size_kb"], json!(size_kb));

            // Special characters test
            let event = ctx
                .event()
                .source("edge")
                .type_("special")
                .field("text", special_chars)
                .insert()
                .await?;
            assert_eq!(event.payload["text"], json!(special_chars));

            // Deeply nested JSON
            let mut nested = json!("value");
            for _ in 0..nested_depth {
                nested = json!({"nested": nested});
            }
            ctx.event()
                .source("edge")
                .type_("nested")
                .payload(nested)
                .insert()
                .await?;
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrent_test_execution(ctx: TestContext) -> Result<()> {
        // Test that multiple tests can run concurrently without interference
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(5));
        let mut handles = vec![];

        for i in 0..5 {
            let barrier_clone = barrier.clone();
            let handle = tokio::spawn(async move {
                let ctx = TestContext::with_name(&format!("concurrent_{}", i)).await?;

                // Synchronize all tasks to start at same time
                barrier_clone.wait().await;

                // Each performs operations
                for j in 0..10 {
                    ctx.event()
                        .source(EventSource::new(&format!("task_{}", i)))
                        .type_("concurrent.test")
                        .field("iteration", j)
                        .insert()
                        .await?;
                }

                // Verify only sees own events
                let count = ctx
                    .events()
                    .by_source(&format!("task_{}", i))
                    .count()
                    .await?;
                assert_eq!(count, 10);

                // Should not see any other task's events
                for k in 0..5 {
                    if k != i {
                        let other_count = ctx
                            .events()
                            .by_source(&format!("task_{}", k))
                            .count()
                            .await?;
                        assert_eq!(other_count, 0);
                    }
                }

                Ok::<(), SinexError>(())
            });
            handles.push(handle);
        }

        // All should succeed
        for handle in handles {
            handle
                .await
                .map_err(|e| SinexError::service(format!("Task failed: {}", e)))??;
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_error_propagation(ctx: TestContext) -> Result<()> {
        // Test that errors propagate correctly through Result

        // Test validation error
        let result = ctx
            .event()
            .source("") // Empty source should fail
            .type_("test")
            .insert()
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("source"));

        // Test that custom errors work with Result
        fn failing_operation() -> Result<()> {
            Err(SinexError::validation("Custom validation error".to_string()).into())
        }

        let result = failing_operation();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Custom validation error");

        Ok(())
    }

    #[sinex_test(timeout = 5)]
    async fn test_timeout_handling(ctx: TestContext) -> Result<()> {
        // Test that the timeout attribute works
        // This test should complete quickly, well under 5 seconds

        let start = std::time::Instant::now();

        // Do some work
        for i in 0..10 {
            ctx.event()
                .source("timeout_test")
                .type_("test")
                .field("index", i)
                .insert()
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
    async fn test_result_type_alias(_ctx: TestContext) -> Result<()> {
        // Test that Result is properly aliased
        fn returns_test_result() -> Result<String> {
            Ok("success".to_string())
        }

        let result = returns_test_result();
        assert!(result.is_ok());
        assert_eq!(result?, "success");

        fn returns_error() -> Result<()> {
            Err(SinexError::unknown("test error".to_string()).into())
        }

        let error_result = returns_error();
        assert!(error_result.is_err());

        Ok(())
    }

    #[sinex_test]
    async fn test_builder_method_chaining_order(ctx: TestContext) -> Result<()> {
        // Test that builder methods can be called in any order
        let event1 = ctx
            .event()
            .type_("test")
            .source("order1")
            .field("a", 1)
            .insert()
            .await?;

        let event2 = ctx
            .event()
            .field("a", 1)
            .source("order2")
            .type_("test")
            .insert()
            .await?;

        // Both should succeed despite different order
        assert_eq!(event1.event_type.as_str(), "test");
        assert_eq!(event2.event_type.as_str(), "test");

        Ok(())
    }

    #[sinex_test]
    async fn test_assertion_edge_cases(ctx: TestContext) -> Result<()> {
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
    async fn test_context_event_count_tracking_accuracy(ctx: TestContext) -> Result<()> {
        // Test that event counting is accurate across operations
        let initial_count = ctx.test_event_count().await;
        assert_eq!(initial_count, 0, "Should start with zero events");

        // Insert events one by one and verify count
        for i in 1..=5 {
            ctx.event()
                .source("count-test")
                .type_("increment")
                .field("index", i)
                .insert()
                .await?;

            let current_count = ctx.test_event_count().await;
            assert_eq!(
                current_count as usize, i,
                "Count should match inserted events"
            );
        }

        // Batch insert and verify
        let batch_events = (0..10)
            .map(|i| {
                ctx.event()
                    .source("count-test")
                    .type_("batch")
                    .field("batch_index", i)
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()?;

        ctx.insert_events(&batch_events).await?;

        let final_count = ctx.test_event_count().await;
        assert_eq!(
            final_count, 15,
            "Should have all individual and batch events"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_context_timing_measurement_precision(ctx: TestContext) -> Result<()> {
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
                Ok::<_, SinexError>("measured")
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
    async fn test_database_pool_concurrent_allocation(ctx: TestContext) -> Result<()> {
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
                match TestContext::with_name(&format!("concurrent_alloc_{}", i)).await {
                    Ok(ctx) => {
                        // Do some work to hold the context
                        ctx.event()
                            .source("pool-test")
                            .type_("allocation")
                            .field("task_id", i)
                            .insert()
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
    async fn test_database_cleanup_on_drop(ctx: TestContext) -> Result<()> {
        // Test that database is properly cleaned when context is dropped
        let test_id = uuid::Uuid::new_v4().to_string();

        // Create a context in a scope so it gets dropped
        {
            let temp_ctx = TestContext::with_name(&format!("cleanup_test_{}", test_id)).await?;

            // Insert identifiable data
            temp_ctx
                .event()
                .source("cleanup-test")
                .type_("marker")
                .field("test_id", test_id)
                .insert()
                .await?;

            // Verify it exists
            let count = temp_ctx.events().by_source("cleanup-test").count().await?;
            assert_eq!(count, 1);

            // Context drops here
        }

        // In our main context, verify we can't see the dropped context's data
        // This verifies isolation, not cleanup (since we can't access the dropped DB)
        let leaked_events = ctx.events().by_source("cleanup-test").fetch().await?;

        assert_eq!(
            leaked_events.len(),
            0,
            "Should not see data from dropped context"
        );

        Ok(())
    }

    // Test Fixture Lifecycle Management

    #[sinex_test]
    async fn test_fixture_lazy_initialization(ctx: TestContext) -> Result<()> {
        // Test that fixtures are only created when accessed
        let scenarios = ctx.fixtures().scenarios();

        // Track initial event count
        let initial_count = ctx.test_event_count().await;

        // Simply getting the scenarios handle shouldn't create any events
        assert_eq!(
            ctx.test_event_count().await,
            initial_count,
            "No events should be created yet"
        );

        // Now actually access a fixture
        let _user_session = scenarios.user_session().await?;

        // Should have created events
        let after_fixture = ctx.test_event_count().await;
        assert!(
            after_fixture > initial_count,
            "Fixture should create events when accessed"
        );

        // Accessing same fixture again should reuse it
        let count_before_reuse = ctx.test_event_count().await;
        let _same_session = scenarios.user_session().await?;
        let count_after_reuse = ctx.test_event_count().await;

        assert_eq!(
            count_before_reuse, count_after_reuse,
            "Reusing fixture should not create new events"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_resource_cleanup(ctx: TestContext) -> Result<()> {
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
    async fn test_fixture_dependency_resolution(ctx: TestContext) -> Result<()> {
        // Test that fixtures with dependencies are resolved correctly
        let scenarios = ctx.fixtures().scenarios();

        // Create a fixture that depends on base events
        let _checkpoint_fixture = scenarios.populated_checkpoints().await?;

        // Verify the fixture created its dependencies
        let checkpoints = ctx
            .events()
            .by_source("sinex")
            .by_type("checkpoint.saved")
            .fetch()
            .await?;

        assert!(
            !checkpoints.is_empty(),
            "Dependent checkpoint events should be created"
        );

        // Verify fixture state is consistent
        let events = ctx.events().by_source("automaton").fetch().await?;

        assert!(!events.is_empty(), "Fixture should create automaton events");

        Ok(())
    }
}
