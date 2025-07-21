//! Test macros for reducing repetition and improving test consistency
//!
//! These macros provide reusable patterns for common test scenarios,
//! making tests more concise and maintainable.

/// Test event insertion with automatic verification
#[macro_export]
macro_rules! test_event_insertion {
    ($test_name:ident, $source:expr, $event_type:expr, $payload:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use crate::common::builders::TestEventBuilder;
            use crate::common::query_helpers::TestQueries;
            
            let pool = ctx.pool();
            
            // Insert event
            let event = TestEventBuilder::new($source, $event_type)
                .with_payload($payload)
                .insert(&pool)
                .await?;
            
            // Verify insertion
            let retrieved = TestQueries::get_event(&pool, event.id).await?;
            assert_eq!(retrieved.source, $source);
            assert_eq!(retrieved.event_type, $event_type);
            assert_eq!(retrieved.payload, $payload);
            
            Ok(())
        }
    };
}

/// Test that event insertion fails with validation error
#[macro_export]
macro_rules! test_invalid_event {
    ($test_name:ident, $source:expr, $event_type:expr, $payload:expr, $error_pattern:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use crate::common::builders::TestEventBuilder;
            
            let pool = ctx.pool();
            
            // Attempt to insert invalid event
            let result = TestEventBuilder::new($source, $event_type)
                .with_payload($payload)
                .insert(&pool)
                .await;
            
            // Verify it failed with expected error
            assert!(result.is_err());
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains($error_pattern),
                "Expected error containing '{}', got: {}",
                $error_pattern,
                error
            );
            
            Ok(())
        }
    };
}

/// Test batch event operations
#[macro_export]
macro_rules! test_batch_events {
    ($test_name:ident, $source:expr, $event_type:expr, $count:expr, $verification:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use crate::common::builders::BatchEventBuilder;
            use crate::common::query_helpers::TestQueries;
            
            let pool = ctx.pool();
            
            // Insert batch
            let events = BatchEventBuilder::new($source, $event_type, $count)
                .insert(&pool)
                .await?;
            
            assert_eq!(events.len(), $count);
            
            // Run custom verification
            $verification(&pool, &events).await?;
            
            Ok(())
        }
    };
}

/// Test checkpoint operations
#[macro_export]
macro_rules! test_checkpoint_flow {
    ($test_name:ident, $automaton:expr, $initial_count:expr, $updated_count:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use crate::common::builders::TestCheckpointBuilder;
            use crate::common::query_helpers::TestQueries;
            
            let pool = ctx.pool();
            
            // Create initial checkpoint
            TestCheckpointBuilder::new($automaton)
                .with_processed_count($initial_count)
                .insert(&pool)
                .await?;
            
            // Verify initial state
            let checkpoint = TestQueries::get_checkpoint(&pool, $automaton)
                .await?
                .expect("Checkpoint should exist");
            assert_eq!(checkpoint.processed_count, $initial_count);
            
            // Update checkpoint
            TestCheckpointBuilder::new($automaton)
                .with_processed_count($updated_count)
                .with_last_processed("updated-id")
                .insert(&pool)
                .await?;
            
            // Verify update
            let updated = TestQueries::get_checkpoint(&pool, $automaton)
                .await?
                .expect("Checkpoint should exist");
            assert_eq!(updated.processed_count, $updated_count);
            
            Ok(())
        }
    };
}

/// Test concurrent operations
#[macro_export]
macro_rules! test_concurrent_operations {
    ($test_name:ident, $task_count:expr, $operation:expr, $verification:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use std::sync::Arc;
            
            let pool = Arc::new(ctx.pool());
            let mut handles = vec![];
            
            // Spawn concurrent tasks
            for i in 0..$task_count {
                let pool_clone = pool.clone();
                let handle = tokio::spawn(async move {
                    $operation(pool_clone, i).await
                });
                handles.push(handle);
            }
            
            // Wait for all tasks
            let results: Vec<_> = futures::future::try_join_all(handles).await?;
            
            // Verify all succeeded
            for result in &results {
                assert!(result.is_ok());
            }
            
            // Run custom verification
            $verification(&pool, &results).await?;
            
            Ok(())
        }
    };
}

/// Test time-based queries
#[macro_export]
macro_rules! test_time_range_query {
    ($test_name:ident, $event_count:expr, $spacing:expr, $range_start:expr, $range_end:expr, $expected_count:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use crate::common::builders::BatchEventBuilder;
            use crate::common::query_helpers::TestQueries;
            use chrono::Utc;
            
            let pool = ctx.pool();
            let now = Utc::now();
            
            // Insert time-spaced events
            BatchEventBuilder::new("timed", "test.event", $event_count)
                .with_start_time(now - chrono::Duration::hours(2))
                .with_time_spacing($spacing)
                .insert(&pool)
                .await?;
            
            // Query time range
            let start = now + $range_start;
            let end = now + $range_end;
            let events = TestQueries::get_events_in_range(&pool, start, end, None).await?;
            
            assert_eq!(
                events.len(), 
                $expected_count,
                "Expected {} events in range {:?} to {:?}, got {}",
                $expected_count, start, end, events.len()
            );
            
            Ok(())
        }
    };
}

/// Test event filtering
#[macro_export]
macro_rules! test_event_filter {
    ($test_name:ident, $sources:expr, $events_per_source:expr, $filter_source:expr, $expected_count:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use crate::common::builders::TestEventBuilder;
            use crate::common::query_helpers::TestQueries;
            
            let pool = ctx.pool();
            
            // Insert events from multiple sources
            for source in $sources {
                for i in 0..$events_per_source {
                    TestEventBuilder::new(source, "test.event")
                        .with_field("index", json!(i))
                        .insert(&pool)
                        .await?;
                }
            }
            
            // Query filtered events
            let filtered = TestQueries::get_events_by_source(&pool, $filter_source, None).await?;
            
            assert_eq!(filtered.len(), $expected_count);
            for event in &filtered {
                assert_eq!(event.source, $filter_source);
            }
            
            Ok(())
        }
    };
}

/// Test scenario with setup and teardown
#[macro_export]
macro_rules! test_with_scenario {
    ($test_name:ident, $setup:expr, $test_body:expr, $cleanup:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            let pool = ctx.pool();
            
            // Setup
            let setup_result = $setup(&pool).await?;
            
            // Test body
            let test_result = $test_body(&pool, setup_result).await;
            
            // Cleanup (always runs)
            $cleanup(&pool).await?;
            
            // Return test result
            test_result
        }
    };
}

/// Parameterized test for multiple cases
#[macro_export]
macro_rules! parameterized_test {
    ($test_name:ident, $params:expr, $test_body:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            let pool = ctx.pool();
            
            for (name, param) in $params {
                println!("Testing case: {}", name);
                $test_body(&pool, param).await
                    .map_err(|e| anyhow::anyhow!("Test case '{}' failed: {}", name, e))?;
            }
            
            Ok(())
        }
    };
}

/// Test event flow from source to processing
#[macro_export]
macro_rules! test_event_flow {
    ($test_name:ident, $source:expr, $event_type:expr, $processor:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder};
            use crate::common::query_helpers::TestQueries;
            
            let pool = ctx.pool();
            
            // Insert event
            let event = TestEventBuilder::new($source, $event_type)
                .with_field("test", json!(true))
                .insert(&pool)
                .await?;
            
            // Simulate processing
            TestCheckpointBuilder::new($processor)
                .with_last_processed(&event.id.to_string())
                .with_processed_count(1)
                .insert(&pool)
                .await?;
            
            // Verify flow
            let checkpoint = TestQueries::get_checkpoint(&pool, $processor)
                .await?
                .expect("Checkpoint should exist");
            
            assert_eq!(checkpoint.last_processed_id, Some(event.id.to_string()));
            assert_eq!(checkpoint.processed_count, 1);
            
            Ok(())
        }
    };
}

/// Test EventFactory creation patterns
#[macro_export]
macro_rules! test_event_factory {
    ($test_name:ident, $source:expr, $event_type:expr, $payload:expr, $verification:expr) => {
        #[sinex_test]
        async fn $test_name(_ctx: TestContext) -> TestResult {
            use sinex_events::EventFactory;
            use pretty_assertions::assert_eq;
            
            let event = EventFactory::new($source).create_event(
                $event_type,
                $payload,
            );
            
            // Basic assertions
            assert_eq!(event.source, $source);
            assert_eq!(event.event_type, $event_type);
            assert_eq!(event.payload, $payload);
            assert!(event.id.to_string().len() == 26); // ULID length
            
            // Custom verification
            $verification(event);
            
            Ok(())
        }
    };
}

/// Test data setup and verification pattern
#[macro_export]
macro_rules! test_with_data {
    ($test_name:ident, $test_data:expr, $test_body:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            let pool = ctx.pool();
            let test_data = $test_data;
            
            $test_body(&pool, test_data).await
        }
    };
}

/// Test insert and query pattern
#[macro_export]
macro_rules! test_insert_and_query {
    ($test_name:ident, $source:expr, $event_type:expr, $payload:expr, $query:expr, $verification:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use crate::common::builders::TestEventBuilder;
            
            let pool = ctx.pool();
            
            // Insert event
            let event = TestEventBuilder::new($source, $event_type)
                .with_payload($payload)
                .insert(&pool)
                .await?;
            
            // Query
            let result = $query(&pool, &event).await?;
            
            // Verify
            $verification(&event, result)?;
            
            Ok(())
        }
    };
}

/// Test security validation patterns
#[macro_export]
macro_rules! test_security_validation {
    ($test_name:ident, $malicious_inputs:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use crate::common::builders::TestEventBuilder;
            
            let pool = ctx.pool();
            
            for (name, payload) in $malicious_inputs {
                println!("Testing malicious input: {}", name);
                
                let result = TestEventBuilder::new("security.test", name)
                    .with_payload(payload)
                    .insert(&pool)
                    .await;
                
                // Most security tests should fail validation
                if name.contains("injection") || name.contains("xss") || name.contains("path_traversal") {
                    assert!(result.is_err(), "Malicious input '{}' should be rejected", name);
                } else {
                    // Some inputs might be valid JSON but still suspicious
                    if let Ok(event) = result {
                        // Verify it was stored correctly
                        assert_eq!(event.source, "security.test");
                    }
                }
            }
            
            Ok(())
        }
    };
}