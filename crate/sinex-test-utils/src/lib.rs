//! Sinex Test Utilities - Unified Testing Through TestContext
//!
//! **IMPORTANT FOR WRITING TESTS**: All Sinex tests should use the `#[sinex_test]` macro
//! and access functionality through `TestContext`. Do not use `#[tokio::test]` directly.
//!
//! # Quick Example
//! ```rust
//! use sinex_test_utils::prelude::*;
//! 
//! #[sinex_test]
//! async fn test_example(ctx: TestContext) -> TestResult<()> {
//!     // Everything is accessed through ctx
//!     let event = ctx.event()
//!         .source("my-service")
//!         .type_("user.created")
//!         .insert()
//!         .await?;
//!     
//!     assert_eq!(event.source, "my-service");
//!     Ok(())
//! }
//! ```
//!
//! # Key Features
//! - **Automatic database isolation** - Each test gets its own database
//! - **Automatic cleanup** - Database rollback on test completion  
//! - **No manual setup** - The `#[sinex_test]` macro handles everything
//! - **Unified API** - All functionality through `TestContext`
//!
//! See `/TESTING.md` in repository root for comprehensive guide.

// Re-export the procedural macro from internal macros crate
pub use sinex_test_utils_macros::sinex_test;

// Type aliases for test infrastructure
pub use sinex_core_types::Result as TestResult;

// Import all the existing modules - all private
mod test_context;
mod database_pool;
mod builders;
mod test_macros;
mod error_testing;
mod timing_utils;
mod fixtures;
mod property_testing;
mod channel_behavior_utils;
mod satellite_management_utils;
mod deployment_scenario_utils;
mod coverage_assurance;
mod mocks;

// Create prelude module from common/mod.rs
pub mod prelude {
    // Core test infrastructure - only what's needed
    pub use crate::sinex_test;
    pub use crate::{TestContext, TestResult};
    
    // Export our test macros
    pub use crate::parameterized;
    
    // Common imports that tests need
    pub use sinex_core_types::{CoreError, RawEvent};
    pub use sinex_error::ErrorContext;
}

// Re-export main types for direct import - only what should be public
pub use test_context::TestContext;
// Macros are already exported at crate root via #[macro_export]

// Comprehensive self-tests
#[cfg(test)]
mod tests {
    use super::prelude::*;
    use crate::database_pool::acquire_test_database;
    use serde_json::json;
    use std::time::Duration;
    
    #[sinex_test]
    async fn test_basic_functionality(ctx: TestContext) -> TestResult<()> {
        // Test event creation and querying
        let event = ctx.event()
            .source("test")
            .type_("test.event")
            .field("key", "value")
            .insert()
            .await?;
            
        assert_eq!(event.source, "test");
        let events = ctx.events().by_source("test").fetch().await?;
        assert_eq!(events.len(), 1);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_event_builders(ctx: TestContext) -> TestResult<()> {
        // Test specialized event builders work
        let fs_event = ctx.event().filesystem().path("/test").created().insert().await?;
        let term_event = ctx.event().terminal().command("ls").insert().await?;
        let clip_event = ctx.event().clipboard().content("text").copied().insert().await?;
        let win_event = ctx.event().window().title("App").focused().insert().await?;
        let sys_event = ctx.event().system().boot().insert().await?;
        
        let events = vec![fs_event, term_event, clip_event, win_event, sys_event];
        
        for event in events {
            assert!(!event.id.to_string().is_empty());
        }
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_validation(ctx: TestContext) -> TestResult<()> {
        // Empty source/type should fail
        assert!(ctx.event().source("").type_("test").insert().await.is_err());
        assert!(ctx.event().source("test").type_("").insert().await.is_err());
        Ok(())
    }
    
    #[sinex_test]
    async fn test_queries(ctx: TestContext) -> TestResult<()> {
        // Create test data
        for i in 0..3 {
            ctx.event()
                .source("query-test")
                .type_(&format!("type.{}", i))
                .insert()
                .await?;
        }
        
        // Test queries work
        assert_eq!(ctx.events().by_source("query-test").fetch().await?.len(), 3);
        assert_eq!(ctx.events().by_type("type.1").fetch().await?.len(), 1);
        assert_eq!(ctx.events().by_source("query-test").limit(2).fetch().await?.len(), 2);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_assertions(ctx: TestContext) -> TestResult<()> {
        // Basic assertions
        ctx.assert("test").eq(&1, &1)?;
        assert!(ctx.assert("fail").eq(&1, &2).is_err());
        
        // Event count
        ctx.event().source("count").type_("test").insert().await?;
        ctx.event().source("count").type_("test").insert().await?;
        ctx.assert_event_count(2).await?;
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_timing(ctx: TestContext) -> TestResult<()> {
        // Create event and wait for it
        ctx.event().source("timing").type_("test").insert().await?;
        ctx.timing().wait_for_event_count(1).await?;
        
        // Measure operation
        let (_, duration) = ctx.measure(async {
            tokio::time::sleep(Duration::from_millis(10)).await
        }).await?;
        
        assert!(duration >= Duration::from_millis(10));
        Ok(())
    }
    
    #[sinex_test]
    async fn test_concurrent(ctx: TestContext) -> TestResult<()> {
        // Run concurrent tasks
        let results = ctx.run_concurrent(3, |ctx, i| async move {
            ctx.event().source("concurrent").type_(&format!("t{}", i)).insert().await?;
            Ok(i)
        }).await?;
        
        assert_eq!(results, vec![0, 1, 2]);
        assert_eq!(ctx.events().by_source("concurrent").fetch().await?.len(), 3);
        Ok(())
    }
    
    #[sinex_test]
    async fn test_database_pool(ctx: TestContext) -> TestResult<()> {
        // Test basic pool functionality
        let result: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(ctx.pool())
            .await?;
        assert_eq!(result, 1);
        
        // Test isolation
        let db1 = acquire_test_database().await?;
        let db2 = acquire_test_database().await?;
        assert_ne!(db1.name(), db2.name());
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_fixtures(ctx: TestContext) -> TestResult<()> {
        // Fixtures provide reusable test data
        let session = ctx.scenarios().user_session().await?;
        assert!(session.resource().await.is_some());
        Ok(())
    }
    
    #[sinex_test]
    async fn test_mocks(ctx: TestContext) -> TestResult<()> {
        // Basic mock functionality
        let fs = ctx.mocks().filesystem();
        fs.create_file(std::path::Path::new("/test.txt"), b"content").await?;
        assert!(fs.exists(std::path::Path::new("/test.txt")).await);
        
        // Other mocks can be created
        let _db = ctx.mocks().database();
        let _redis = ctx.mocks().redis();
        
        Ok(())
    }
    
    #[test]
    fn test_builder_validation() {
        // Test builders with various inputs
        use crate::builders::TestEventBuilder;
        
        let long_source = "a".repeat(50);
        let test_cases = vec![
            ("fs", "file.created"),
            ("shell-terminal", "command.executed"),
            ("service_123", "event.processed_ok"),
            (long_source.as_str(), "type.very_long_name"),
            ("x-y-z", "a.b.c.d.e"),
        ];
        
        for (source, event_type) in test_cases {
            let event = TestEventBuilder::new(source, event_type).build();
            assert_eq!(event.source, source);
            assert_eq!(event.event_type, event_type);
            assert!(!event.id.to_string().is_empty());
            assert!(!event.host.is_empty());
        }
    }
    
    #[test]
    fn test_builder_with_proptest() {
        // Property test for pure builder functions
        use ::proptest::prelude::*;
        use crate::builders::TestEventBuilder;
        
        proptest!(|(
            source in "[a-zA-Z][a-zA-Z0-9_.-]{2,50}",
            event_type in "[a-zA-Z][a-zA-Z0-9_-]{1,30}\\.[a-zA-Z][a-zA-Z0-9_-]{1,30}"
        )| {
            let event = TestEventBuilder::new(&source, &event_type).build();
            prop_assert_eq!(event.source, source);
            prop_assert_eq!(event.event_type, event_type);
            prop_assert!(!event.id.to_string().is_empty());
        });
    }
    
    #[sinex_test]
    async fn test_database_with_parameterized(ctx: TestContext) -> TestResult<()> {
        // For tests that need database, use parameterized! for a reasonable number of cases
        // Property tests with thousands of DB operations would be too slow anyway
        parameterized!([
            ("fs", "file.created"),
            ("shell", "cmd.run"),
            ("service-123", "event.processed"),
            ("xxxxxxxxxxxxxxxxxxx", "type.test"),
        ], |(source, event_type)| {
            // Each case runs with the same TestContext
            let event = ctx.event()
                .source(source)
                .type_(event_type)
                .field("param_test", true)
                .insert()
                .await?;
                
            // Verify event was created correctly
            assert_eq!(event.source, source);
            assert_eq!(event.event_type, event_type);
            assert_eq!(event.payload["param_test"], json!(true));
            
            // Query it back
            let events = ctx.events()
                .by_source(source)
                .by_type(event_type)
                .fetch()
                .await?;
            assert_eq!(events.len(), 1);
            
            Ok(())
        });
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_property_testing_integration(ctx: TestContext) -> TestResult<()> {
        // Property test with database - test various valid inputs
        let long_source = "x".repeat(50);
        let long_type = format!("type.{}", "x".repeat(30));
        
        let test_cases = vec![
            ("fs", "file.created", json!({"path": "/test/α/β/γ.txt"})), // Unicode
            ("shell-123", "cmd.exec-99", json!({"n": i64::MAX})), // Edge numbers
            (long_source.as_str(), "a.b", json!({})), // Long source
            ("src", long_type.as_str(), json!(null)), // Long type
        ];
        
        for (source, event_type, payload) in test_cases {
            let event = ctx.event()
                .source(source)
                .type_(event_type)
                .payload(payload.clone())
                .insert()
                .await?;
            assert_eq!(event.source, source);
            assert_eq!(event.event_type, event_type);
            assert_eq!(event.payload, payload);
        }
        Ok(())
    }
    
    #[sinex_test] 
    async fn test_parameterized_pattern(ctx: TestContext) -> TestResult<()> {
        // Use the parameterized! macro for data-driven tests
        parameterized!([
            // (source, event_type, expected_count)
            ("test1", "type.a", 1),
            ("test2", "type.b", 2),
            ("test3", "type.c", 3),
        ], |(source, event_type, count)| {
            // Insert 'count' events
            for i in 0..count {
                ctx.event()
                    .source(source)
                    .type_(event_type)
                    .field("index", i)
                    .insert()
                    .await?;
            }
            
            // Verify count
            let events = ctx.events()
                .by_source(source)
                .by_type(event_type)
                .fetch()
                .await?;
            assert_eq!(events.len(), count as usize);
            Ok(())
        });
        Ok(())
    }
    
    #[sinex_test]
    async fn test_edge_cases_with_parameterized(ctx: TestContext) -> TestResult<()> {
        // Test with proptest! macro for edge cases
        // For edge cases that need database, use parameterized approach
        parameterized!([
            (10, "normal text", 3),
            (100, "special 'quotes' \"double\"", 5),
            (500, "\n\t\r special chars", 8),
        ], |(size_kb, special_chars, nested_depth)| {
            // Large payload test
            let large = "x".repeat(size_kb * 1024);
            let event = ctx.event()
                .source("edge")
                .type_("large")
                .field("data", large.as_str())
                .field("size_kb", size_kb)
                .insert()
                .await?;
            assert_eq!(event.payload["size_kb"], json!(size_kb));
            
            // Special characters test
            let event = ctx.event()
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
            
            Ok(())
        });
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_isolation(ctx: TestContext) -> TestResult<()> {
        // Events are isolated between tests
        ctx.event().source("isolation").type_("test").insert().await?;
        
        let ctx2 = TestContext::with_name("other").await?;
        let events = ctx2.events().by_source("isolation").fetch().await?;
        assert_eq!(events.len(), 0);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_database_isolation_comprehensive(ctx: TestContext) -> TestResult<()> {
        // Create multiple contexts and verify complete isolation
        let contexts = vec![
            TestContext::with_name("isolation_1").await?,
            TestContext::with_name("isolation_2").await?,
            TestContext::with_name("isolation_3").await?,
        ];
        
        // Each context inserts events with unique source
        for (i, test_ctx) in contexts.iter().enumerate() {
            for j in 0..3 {
                test_ctx.event()
                    .source(&format!("ctx_{}", i))
                    .type_("isolation.test")
                    .field("context_id", i)
                    .field("event_num", j)
                    .insert()
                    .await?;
            }
        }
        
        // Verify each context only sees its own events
        for (i, test_ctx) in contexts.iter().enumerate() {
            let own_events = test_ctx.events()
                .by_source(&format!("ctx_{}", i))
                .fetch()
                .await?;
            assert_eq!(own_events.len(), 3, "Context {} should see exactly 3 of its own events", i);
            
            // Should not see events from other contexts
            for j in 0..3 {
                if i != j {
                    let other_events = test_ctx.events()
                        .by_source(&format!("ctx_{}", j))
                        .fetch()
                        .await?;
                    assert_eq!(other_events.len(), 0, 
                        "Context {} should not see events from context {}", i, j);
                }
            }
        }
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_concurrent_test_execution(ctx: TestContext) -> TestResult<()> {
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
                        .source(&format!("task_{}", i))
                        .type_("concurrent.test")
                        .field("iteration", j)
                        .insert()
                        .await?;
                }
                
                // Verify only sees own events
                let count = ctx.events()
                    .by_source(&format!("task_{}", i))
                    .count()
                    .await?;
                assert_eq!(count, 10);
                
                // Should not see any other task's events
                for k in 0..5 {
                    if k != i {
                        let other_count = ctx.events()
                            .by_source(&format!("task_{}", k))
                            .count()
                            .await?;
                        assert_eq!(other_count, 0);
                    }
                }
                
                Ok::<(), CoreError>(())
            });
            handles.push(handle);
        }
        
        // All should succeed
        for handle in handles {
            handle.await.map_err(|e| CoreError::Service(format!("Task failed: {}", e)))??;
        }
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_error_propagation(ctx: TestContext) -> TestResult<()> {
        // Test that errors propagate correctly through TestResult
        
        // Test validation error
        let result = ctx.event()
            .source("") // Empty source should fail
            .type_("test")
            .insert()
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("source"));
        
        // Test that custom errors work with TestResult
        fn failing_operation() -> TestResult<()> {
            Err(CoreError::Validation("Custom validation error".to_string()))
        }
        
        let result = failing_operation();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Custom validation error");
        
        Ok(())
    }
    
    #[sinex_test(timeout = 5)]
    async fn test_timeout_handling(ctx: TestContext) -> TestResult<()> {
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
        assert!(elapsed.as_secs() < 5, "Test should complete well under timeout");
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_test_context_helpers(ctx: TestContext) -> TestResult<()> {
        // Test various TestContext helper methods
        
        // Test name should be set
        assert!(!ctx.test_name().is_empty());
        
        // Pool should be valid
        let pool_result: Result<i32, sqlx::Error> = sqlx::query_scalar("SELECT 1")
            .fetch_one(ctx.pool())
            .await;
        assert_eq!(pool_result?, 1);
        
        // Test elapsed time tracking
        let initial_elapsed = ctx.elapsed();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let after_elapsed = ctx.elapsed();
        assert!(after_elapsed > initial_elapsed);
        
        // Test event count tracking
        let initial_count = ctx.test_event_count().await;
        ctx.event().source("helper_test").type_("test").insert().await?;
        let after_count = ctx.test_event_count().await;
        assert_eq!(after_count, initial_count + 1);
        
        Ok(())
    }
    
    #[sinex_test]
    async fn test_assertion_helpers(ctx: TestContext) -> TestResult<()> {
        // Test the contextual assertion API
        
        // Basic assertions
        ctx.assert("equality test").eq(&5, &5)?;
        ctx.assert("condition test").that(true, "should be true")?;
        
        // Collection assertions
        let items = vec![1, 2, 3];
        ctx.assert("size test").has_size(&items, 3)?;
        ctx.assert("not empty test").not_empty(&items)?;
        
        // Option assertions
        let some_value = Some(42);
        let none_value: Option<i32> = None;
        ctx.assert("some test").some(&some_value)?;
        ctx.assert("none test").none(&none_value)?;
        
        // Error assertions
        let error_result: Result<(), CoreError> = Err(CoreError::Validation("test error".to_string()));
        ctx.assert("error test").error_contains(&error_result, "test error")?;
        
        // Test that assertions fail appropriately
        let bad_assertion = ctx.assert("should fail").eq(&5, &6);
        assert!(bad_assertion.is_err());
        
        Ok(())
    }
    
    #[test]
    fn test_result_type_alias() {
        // Test that TestResult is properly aliased
        fn returns_test_result() -> TestResult<String> {
            Ok("success".to_string())
        }
        
        let result = returns_test_result();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        
        fn returns_error() -> TestResult<()> {
            Err(CoreError::Unknown("test error".to_string()))
        }
        
        let error_result = returns_error();
        assert!(error_result.is_err());
    }
}