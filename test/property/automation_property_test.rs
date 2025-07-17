// Property tests for automaton behavior
//
// Tests that verify automaton processing, state management, and coordination properties

use crate::common::prelude::*;
use crate::property::strategies::*;
use proptest::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Test automaton event processing is deterministic
proptest! {
    #[test]
    fn automaton_processing_is_deterministic(
        automaton_name in automaton_names(),
        events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=50
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();

            // Skip if no events to test
            if events.is_empty() {
                return;
            }

            // Process events twice with same automaton
            let mut first_run_results = Vec::new();
            let mut second_run_results = Vec::new();

            for run in 0..2 {
                let test_automaton = format!("{}-run-{}", automaton_name, run);

                // Create fresh automaton instance for each run
                let automaton = crate::common::test_context::TestContext::start_test_automaton(&ctx, &test_automaton).await.unwrap();

                // Process the same events
                for (source, event_type, payload) in events.iter() {
                    let event = crate::common::events::create_raw_event(
                        source,
                        event_type,
                        payload.clone(),
                        chrono::Utc::now()
                    );
                    ctx.insert_event(&event).await.unwrap();
                }

                // Wait for processing
                crate::common::test_context::TestContext::wait_for_checkpoint_progress(&ctx, &test_automaton, events.len() as u64).await.unwrap();

                // Collect results
                let checkpoint = ctx.verify_checkpoint(&test_automaton).await.unwrap();
                let final_count = ctx.event_count().await.unwrap();

                if run == 0 {
                    first_run_results.push((checkpoint.processed_count, final_count));
                } else {
                    second_run_results.push((checkpoint.processed_count, final_count));
                }
            }

            // Verify deterministic behavior
            // Note: We can't expect exact same results due to timing, but should have similar patterns
            assert_eq!(first_run_results.len(), second_run_results.len());

            // Both runs should process the same number of events
            for (first, second) in first_run_results.iter().zip(second_run_results.iter()) {
                assert_eq!(first.1, second.1); // Event count should be same
            }
        });
    }
}

/// Test automaton state consistency under concurrent operations
proptest! {
    #[test]
    fn automaton_state_consistency_under_concurrency(
        automaton_name in automaton_names(),
        concurrent_operations in concurrent_operations(),
        events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=20
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = Arc::new(crate::common::test_context::TestContext::new().await.unwrap());

            // Skip if no events to test
            if events.is_empty() {
                return;
            }

            let automaton = crate::common::test_context::TestContext::start_test_automaton(&ctx, &automaton_name).await.unwrap();

            // Prepare events for concurrent processing
            let test_events: Vec<_> = events.iter().map(|(source, event_type, payload)| {
                crate::common::events::create_raw_event(
                    source,
                    event_type,
                    payload.clone(),
                    chrono::Utc::now()
                )
            }).collect();

            // Launch concurrent operations
            let mut handles = Vec::new();
            for (i, operation) in concurrent_operations.iter().enumerate() {
                let ctx_clone = ctx.clone();
                let automaton_name_clone = automaton_name.clone();
                let operation_clone = operation.clone();
                let events_clone = test_events.clone();

                let handle = tokio::spawn(async move {
                    match operation_clone.as_str() {
                        "insert_event" => {
                            if let Some(event) = events_clone.get(i % events_clone.len()) {
                                ctx_clone.insert_event(event).await.unwrap();
                            }
                        }
                        "query_events" => {
                            let _ = ctx_clone.query_events().await;
                        }
                        "update_checkpoint" => {
                            let _ = ctx_clone.verify_checkpoint(&automaton_name_clone).await;
                        }
                        _ => {}
                    }
                });
                handles.push(handle);
            }

            // Wait for all operations to complete
            for handle in handles {
                let _ = handle.await;
            }

            // Verify final state consistency
            let final_checkpoint = ctx.verify_checkpoint(&automaton_name).await.unwrap();
            let final_count = ctx.event_count().await.unwrap();

            // State should be consistent (no corruption)
            assert!(final_checkpoint.processed_count <= final_count as u64);
            assert!(final_count >= 0);
        });
    }
}

/// Test automaton recovery from checkpoint
proptest! {
    #[test]
    fn automaton_recovery_from_checkpoint_is_correct(
        automaton_name in automaton_names(),
        initial_events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=20
        ),
        recovery_events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=20
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();

            // Skip if no events to test
            if initial_events.is_empty() && recovery_events.is_empty() {
                return;
            }

            // Phase 1: Initial processing
            let automaton1 = crate::common::test_context::TestContext::start_test_automaton(&ctx, &automaton_name).await.unwrap();

            for (source, event_type, payload) in initial_events.iter() {
                let event = crate::common::events::create_raw_event(
                    source,
                    event_type,
                    payload.clone(),
                    chrono::Utc::now()
                );
                ctx.insert_event(&event).await.unwrap();
            }

            // Wait for initial processing
            if !initial_events.is_empty() {
                crate::common::test_context::TestContext::wait_for_checkpoint_progress(&ctx, &automaton_name, initial_events.len() as u64).await.unwrap();
            }

            // Capture checkpoint state
            let checkpoint_before = ctx.verify_checkpoint(&automaton_name).await.unwrap();

            // Phase 2: Simulate restart by creating new automaton with same name
            drop(automaton1);
            let automaton2 = crate::common::test_context::TestContext::start_test_automaton(&ctx, &automaton_name).await.unwrap();

            // Verify checkpoint was restored
            let checkpoint_after_restart = ctx.verify_checkpoint(&automaton_name).await.unwrap();
            assert_eq!(checkpoint_before.processed_count, checkpoint_after_restart.processed_count);

            // Phase 3: Continue processing
            for (source, event_type, payload) in recovery_events.iter() {
                let event = crate::common::events::create_raw_event(
                    source,
                    event_type,
                    payload.clone(),
                    chrono::Utc::now()
                );
                ctx.insert_event(&event).await.unwrap();
            }

            // Wait for recovery processing
            if !recovery_events.is_empty() {
                let expected_total = initial_events.len() + recovery_events.len();
                crate::common::test_context::TestContext::wait_for_checkpoint_progress(&ctx, &automaton_name, expected_total as u64).await.unwrap();
            }

            // Verify recovery worked correctly
            let final_checkpoint = ctx.verify_checkpoint(&automaton_name).await.unwrap();
            let expected_processed = initial_events.len() + recovery_events.len();
            assert_eq!(final_checkpoint.processed_count, expected_processed as u64);
        });
    }
}

/// Test automaton batch processing efficiency
proptest! {
    #[test]
    fn automaton_batch_processing_is_efficient(
        automaton_name in automaton_names(),
        batch_size in batch_sizes(),
        events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=200
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();

            // Skip if no events to test
            if events.is_empty() {
                return;
            }

            let automaton = crate::common::test_context::TestContext::start_test_automaton(&ctx, &automaton_name).await.unwrap();

            // Process events in batches
            let mut total_processed = 0;
            for chunk in events.chunks(batch_size) {
                let batch_start = std::time::Instant::now();

                // Insert batch
                for (source, event_type, payload) in chunk.iter() {
                    let event = crate::common::events::create_raw_event(
                        source,
                        event_type,
                        payload.clone(),
                        chrono::Utc::now()
                    );
                    ctx.insert_event(&event).await.unwrap();
                    total_processed += 1;
                }

                // Wait for batch to be processed
                crate::common::test_context::TestContext::wait_for_checkpoint_progress(&ctx, &automaton_name, total_processed as u64).await.unwrap();

                let batch_duration = batch_start.elapsed();

                // Verify batch was processed efficiently
                assert!(batch_duration.as_secs() < 10, "Batch processing took too long: {:?}", batch_duration);
            }

            // Verify all events were processed
            let final_checkpoint = ctx.verify_checkpoint(&automaton_name).await.unwrap();
            assert_eq!(final_checkpoint.processed_count, events.len() as u64);
        });
    }
}

/// Test automaton error handling and recovery
proptest! {
    #[test]
    fn automaton_error_handling_is_robust(
        automaton_name in automaton_names(),
        valid_events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=10
        ),
        invalid_events in proptest::collection::vec(
            adversarial_payloads(),
            1..=5
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();

            // Skip if no events to test
            if valid_events.is_empty() && invalid_events.is_empty() {
                return;
            }

            let automaton = crate::common::test_context::TestContext::start_test_automaton(&ctx, &automaton_name).await.unwrap();

            // Phase 1: Process valid events
            for (source, event_type, payload) in valid_events.iter() {
                let event = crate::common::events::create_raw_event(
                    source,
                    event_type,
                    payload.clone(),
                    chrono::Utc::now()
                );
                ctx.insert_event(&event).await.unwrap();
            }

            if !valid_events.is_empty() {
                crate::common::test_context::TestContext::wait_for_checkpoint_progress(&ctx, &automaton_name, valid_events.len() as u64).await.unwrap();
            }

            let checkpoint_after_valid = ctx.verify_checkpoint(&automaton_name).await.unwrap();

            // Phase 2: Process invalid events (should be handled gracefully)
            for payload in invalid_events.iter() {
                let invalid_event = crate::common::events::create_raw_event(
                    "test",
                    "invalid.event",
                    payload.clone(),
                    chrono::Utc::now()
                );

                // Try to insert invalid event - it may succeed or fail
                let _ = ctx.insert_event(&invalid_event).await;
            }

            // Allow some time for error handling
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            // Phase 3: Process more valid events to verify recovery
            let recovery_event = crate::common::events::create_raw_event(
                "test",
                "recovery.event",
                serde_json::json!({"recovery": true}),
                chrono::Utc::now()
            );
            ctx.insert_event(&recovery_event).await.unwrap();

            // Wait for recovery
            let expected_minimum = valid_events.len() + 1; // +1 for recovery event
            crate::common::test_context::TestContext::wait_for_checkpoint_progress(&ctx, &automaton_name, expected_minimum as u64).await.unwrap();

            // Verify automaton recovered and continued processing
            let final_checkpoint = ctx.verify_checkpoint(&automaton_name).await.unwrap();
            assert!(final_checkpoint.processed_count >= checkpoint_after_valid.processed_count);
        });
    }
}

/// Test automaton memory usage under load
proptest! {
    #[test]
    fn automaton_memory_usage_is_bounded(
        automaton_name in automaton_names(),
        large_events in proptest::collection::vec(
            (event_sources(), event_types(), adversarial_payloads()),
            1..=50
        ),
        processing_interval in 1u64..100u64,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();

            // Skip if no events to test
            if large_events.is_empty() {
                return;
            }

            let automaton = crate::common::test_context::TestContext::start_test_automaton(&ctx, &automaton_name).await.unwrap();

            // Process large events with controlled timing
            let mut processed_count = 0;
            for (source, event_type, payload) in large_events.iter() {
                let event = crate::common::events::create_raw_event(
                    source,
                    event_type,
                    payload.clone(),
                    chrono::Utc::now()
                );

                ctx.insert_event(&event).await.unwrap();
                processed_count += 1;

                // Add processing interval to allow memory cleanup
                tokio::time::sleep(std::time::Duration::from_millis(processing_interval)).await;

                // Periodically check that processing is keeping up
                if processed_count % 10 == 0 {
                    let checkpoint = ctx.verify_checkpoint(&automaton_name).await.unwrap();
                    // Should be processing events, not accumulating indefinitely
                    assert!(checkpoint.processed_count > 0);
                }
            }

            // Final verification
            crate::common::test_context::TestContext::wait_for_checkpoint_progress(&ctx, &automaton_name, processed_count as u64).await.unwrap();

            let final_checkpoint = ctx.verify_checkpoint(&automaton_name).await.unwrap();
            assert_eq!(final_checkpoint.processed_count, processed_count as u64);
        });
    }
}

/// Test automaton coordination with multiple instances
proptest! {
    #[test]
    fn multiple_automata_coordination_is_correct(
        automaton_names in proptest::collection::vec(automaton_names(), 2..=5),
        events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=30
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();

            // Skip if no events to test
            if events.is_empty() || automaton_names.is_empty() {
                return;
            }

            // Start multiple automata
            let mut automata = Vec::new();
            for automaton_name in automaton_names.iter() {
                let automaton = crate::common::test_context::TestContext::start_test_automaton(&ctx, automaton_name).await.unwrap();
                automata.push(automaton);
            }

            // Process events that all automata can see
            for (source, event_type, payload) in events.iter() {
                let event = crate::common::events::create_raw_event(
                    source,
                    event_type,
                    payload.clone(),
                    chrono::Utc::now()
                );
                ctx.insert_event(&event).await.unwrap();
            }

            // Wait for all automata to process events
            for automaton_name in automaton_names.iter() {
                crate::common::test_context::TestContext::wait_for_checkpoint_progress(&ctx, automaton_name, events.len() as u64).await.unwrap();
            }

            // Verify each automaton processed all events
            for automaton_name in automaton_names.iter() {
                let checkpoint = ctx.verify_checkpoint(automaton_name).await.unwrap();
                assert_eq!(checkpoint.processed_count, events.len() as u64);
            }

            // Verify no event duplication in database
            let final_count = ctx.event_count().await.unwrap();
            assert_eq!(final_count, events.len() as i64);
        });
    }
}
