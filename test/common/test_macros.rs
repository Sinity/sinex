//! Test macros for reducing repetition and improving test consistency
//!
//! These macros provide reusable patterns for common test scenarios,
//! making tests more concise and maintainable.
//!
//! Enhanced with sophisticated error testing from error_test_macros.

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
/// Enhanced with sophisticated error testing from error_test_macros
#[macro_export]
macro_rules! test_invalid_event {
    ($test_name:ident, $source:expr, $event_type:expr, $payload:expr, $error_pattern:expr) => {
        use crate::common::error_helpers::test_validation_error;
        
        test_validation_error!(
            $test_name,
            "event_payload", 
            $payload,
            $error_pattern
        );
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

/// Test Redis stream operations with automatic setup and verification
#[macro_export]
macro_rules! test_redis_stream_operations {
    ($test_name:ident, $stream_key:expr, $consumer_group:expr, $message_count:expr, $verification:expr) => {
        #[sinex_test]
        async fn $test_name(_ctx: TestContext) -> TestResult {
            use redis::{cmd, AsyncCommands};
            use sinex_satellite_sdk::RedisStreamClient;
            use std::collections::HashMap;
            
            let redis_client = RedisStreamClient::new("redis://localhost:6379")?;
            let mut redis_conn = redis_client.get_connection().await?;
            let stream_key = $stream_key;
            let consumer_group = $consumer_group;
            
            // Clean up existing stream
            let _: Result<i32, _> = redis_conn.del(stream_key).await;
            
            // Create consumer group
            match cmd("XGROUP")
                .arg("CREATE")
                .arg(stream_key)
                .arg(consumer_group)
                .arg("0")
                .arg("MKSTREAM")
                .query_async::<_, ()>(&mut redis_conn).await {
                Ok(_) => {},
                Err(e) if e.to_string().contains("BUSYGROUP") => {}, // Group already exists
                Err(e) => return Err(e.into()),
            }
            
            // Add messages to stream
            let mut message_ids = Vec::new();
            for i in 0..$message_count {
                let index_str = i.to_string();
                let test_data = format!("test-{}", i);
                let timestamp = chrono::Utc::now().to_rfc3339();
                let message_data = &[
                    ("index", index_str.as_str()),
                    ("test_data", test_data.as_str()),
                    ("timestamp", timestamp.as_str()),
                ];
                
                let id: String = redis_conn.xadd(stream_key, "*", message_data).await?;
                message_ids.push(id);
            }
            
            // Read messages from stream
            let result = cmd("XREADGROUP")
                .arg("GROUP")
                .arg(consumer_group)
                .arg("test-consumer")
                .arg("COUNT")
                .arg($message_count)
                .arg("STREAMS")
                .arg(stream_key)
                .arg(">")
                .query_async::<_, redis::streams::StreamReadReply>(&mut redis_conn)
                .await?;
            
            // Acknowledge messages
            for stream in &result.keys {
                for message in &stream.ids {
                    let _: i32 = redis_conn.xack(stream_key, consumer_group, &[&message.id]).await?;
                }
            }
            
            // Run custom verification
            let verification_fn = $verification;
            verification_fn(&mut redis_conn, &stream_key, &result, &message_ids).await?;
            
            // Cleanup
            let _: Result<i32, _> = redis_conn.del(stream_key).await;
            
            Ok(())
        }
    };
}

/// Test schema validation with comprehensive error checking
#[macro_export]
macro_rules! test_schema_validation {
    ($test_name:ident, $valid_payload:expr, $invalid_payload:expr, $schema:expr, $expected_error:expr) => {
        #[sinex_test]
        async fn $test_name(_ctx: TestContext) -> TestResult {
            use sinex_validation::ValidationChain;
            use serde_json::Value;
            
            let schema = $schema;
            let valid_payload: Value = $valid_payload;
            let invalid_payload: Value = $invalid_payload;
            
            // Test valid payload passes validation
            let valid_result = validate_against_schema(&valid_payload, &schema);
            assert!(
                valid_result.is_ok(),
                "Valid payload should pass schema validation: {:?}",
                valid_result.err()
            );
            
            // Test invalid payload fails validation
            let invalid_result = validate_against_schema(&invalid_payload, &schema);
            assert!(
                invalid_result.is_err(),
                "Invalid payload should fail schema validation"
            );
            
            // Check error message contains expected pattern
            if let Err(error) = invalid_result {
                let error_msg = error.to_string();
                assert!(
                    error_msg.contains($expected_error),
                    "Error message '{}' should contain '{}'",
                    error_msg,
                    $expected_error
                );
            }
            
            Ok(())
        }
    };
    
    // Simplified version that just tests one payload against schema
    ($test_name:ident, $payload:expr, $schema:expr, $should_pass:expr) => {
        #[sinex_test]
        async fn $test_name(_ctx: TestContext) -> TestResult {
            use sinex_validation::ValidationChain;
            use serde_json::Value;
            
            let schema = $schema;
            let payload: Value = $payload;
            let should_pass = $should_pass;
            
            let result = validate_against_schema(&payload, &schema);
            
            if should_pass {
                assert!(
                    result.is_ok(),
                    "Payload should pass schema validation: {:?}",
                    result.err()
                );
            } else {
                assert!(
                    result.is_err(),
                    "Payload should fail schema validation"
                );
            }
            
            Ok(())
        }
    };
}

