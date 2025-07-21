//! Example property tests demonstrating the new property builders
//!
//! This file shows how property builders make property-based testing
//! easier and more consistent with the rest of the test framework.

use crate::common::prelude::*;
use crate::common::property_builders::*;
use crate::common::property_builders::valid_events::*;
use crate::common::property_builders::invalid_events::*;
use crate::common::property_builders::batch_patterns::*;
use sinex_satellite_sdk::checkpoint::{CheckpointManager, CheckpointState};
use sinex_satellite_sdk::stream_processor::Checkpoint;

/// Example: Basic event generation and insertion
proptest! {
    #[test]
    fn example_basic_event_generation(event in arbitrary_event()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // The generated event is a TestEventBuilder
            let inserted = event.insert(&pool).await.unwrap();
            
            // Verify basic properties
            assert!(!inserted.source.is_empty());
            assert!(!inserted.event_type.is_empty());
            assert!(!inserted.host.is_empty());
        });
    }
}

/// Example: Testing with specific event types
proptest! {
    #[test]
    fn example_filesystem_event_processing(
        fs_event in filesystem_event(),
        shell_event in shell_command_event(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert filesystem event
            let fs_inserted = fs_event.insert(&pool).await.unwrap();
            assert_eq!(fs_inserted.source, "fs");
            
            // Insert shell command event  
            let shell_inserted = shell_event.insert(&pool).await.unwrap();
            assert_eq!(shell_inserted.source, "shell");
            assert_eq!(shell_inserted.event_type, "command.executed");
        });
    }
}

/// Example: Batch processing with time ordering
proptest! {
    #[test]
    fn example_batch_processing_with_checkpoints(
        event_batch in arbitrary_event_batch(5..20),
        automaton_name in automaton_names(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Create checkpoint manager
            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            );
            
            // Process events in batch
            let mut processed_count = 0u64;
            for event_builder in event_batch {
                let event = event_builder.insert(&pool).await.unwrap();
                processed_count += 1;
                
                // Update checkpoint
                let state = CheckpointState {
                    checkpoint: Checkpoint::Database { event_id: event.id },
                    processed_count,
                    last_activity: chrono::Utc::now(),
                    data: Some(json!({"event_type": event.event_type})),
                    version: 2,
                };
                
                checkpoint_manager.save_checkpoint(&state).await.unwrap();
            }
            
            // Verify final state
            let stats = checkpoint_manager.get_checkpoint_stats().await.unwrap();
            assert_eq!(stats.max_processed, processed_count);
        });
    }
}

/// Example: Testing error handling with invalid events
proptest! {
    #[test] 
    fn example_invalid_event_handling(
        massive_event in massive_payload_event(),
        nested_event in deeply_nested_event(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Test massive payload handling
            let massive_result = massive_event.insert(&pool).await;
            // Should succeed - database can handle large payloads
            assert!(massive_result.is_ok());
            
            // Test deeply nested JSON
            let nested_result = nested_event.insert(&pool).await;
            assert!(nested_result.is_ok());
        });
    }
}

/// Example: Using batch patterns for realistic scenarios
#[test]
fn example_user_activity_simulation() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let ctx = crate::common::test_context::TestContext::new().await.unwrap();
        let pool = ctx.pool().clone();
        
        // Generate a 15-minute user session
        let session_events = user_activity_batch("test-session-123", 15);
        
        // Insert all events
        let mut event_count = 0;
        let mut session_started = false;
        let mut session_ended = false;
        
        for event_builder in session_events {
            let event = event_builder.insert(&pool).await.unwrap();
            event_count += 1;
            
            match event.event_type.as_str() {
                "session.started" => session_started = true,
                "session.ended" => session_ended = true,
                "command.executed" => {
                    // Verify command events have session_id
                    assert!(event.payload.get("session_id").is_some());
                }
                _ => {}
            }
        }
        
        // Verify session structure
        assert!(session_started);
        assert!(session_ended);
        assert!(event_count > 2); // At least start, one command, and end
    });
}

/// Example: Complex property test with multiple strategies
proptest! {
    #[test]
    fn example_complex_scenario(
        events in arbitrary_event_batch(10..30),
        checkpoint in arbitrary_checkpoint(),
        (start_time, end_time) in arbitrary_time_range(),
        (start_ulid, end_ulid) in arbitrary_ulid_range(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert checkpoint
            checkpoint.insert(&pool).await.unwrap();
            
            // Insert events within time range
            let time_step = (end_time - start_time) / events.len() as i32;
            let mut events_in_range = 0;
            
            for (i, mut event_builder) in events.into_iter().enumerate() {
                let event_time = start_time + time_step * i as i32;
                event_builder = event_builder.with_timestamp(event_time);
                
                let event = event_builder.insert(&pool).await.unwrap();
                
                // Verify event is in expected time range
                if let Some(ts_orig) = event.ts_orig {
                    assert!(ts_orig >= start_time && ts_orig <= end_time);
                    events_in_range += 1;
                }
            }
            
            // Verify ULID ordering
            assert!(start_ulid <= end_ulid);
            
            // Verify we processed events
            assert!(events_in_range > 0);
        });
    }
}

/// Example: Testing with custom payload generation
proptest! {
    #[test]
    fn example_custom_event_modification(
        base_event in arbitrary_event()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Modify the generated event
            let custom_event = base_event
                .with_field("custom_field", json!("custom_value"))
                .with_field("test_id", json!(Ulid::new().to_string()))
                .with_host("test-host");
            
            let inserted = custom_event.insert(&pool).await.unwrap();
            
            // Verify modifications
            assert_eq!(inserted.host, "test-host");
            assert_eq!(inserted.payload["custom_field"], "custom_value");
            assert!(inserted.payload["test_id"].is_string());
        });
    }
}

/// Example: Performance testing with controlled batch sizes
proptest! {
    #![proptest_config(ProptestConfig::with_cases(5))] // Reduce iterations for performance test
    #[test]
    fn example_performance_batch_test(
        batch_size in 100usize..500usize
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            let start = std::time::Instant::now();
            
            // Generate and insert batch
            let batch = BatchEventBuilder::new("perf_test", "test.event", batch_size)
                .with_payload_generator(|i| json!({"index": i, "batch_test": true}))
                .insert(&pool)
                .await
                .unwrap();
            
            let duration = start.elapsed();
            
            // Verify batch
            assert_eq!(batch.len(), batch_size);
            
            // Performance assertion (should handle 100+ events/second)
            let events_per_second = batch_size as f64 / duration.as_secs_f64();
            assert!(events_per_second > 100.0, "Performance too low: {} events/sec", events_per_second);
        });
    }
}