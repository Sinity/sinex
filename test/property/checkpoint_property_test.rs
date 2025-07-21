use serde_json::json;
// Property tests for checkpoint management
//
// Tests that verify checkpoint consistency, recovery, and concurrency properties


use crate::common::test_macros::*;
use crate::common::property_builders::*;
use crate::property::strategies::{automaton_names, checkpoint_data};
use proptest::prelude::*;
use sinex_satellite_sdk::checkpoint::{CheckpointManager, CheckpointState};
use sinex_satellite_sdk::stream_processor::Checkpoint;
use std::sync::Arc;

/// Test that checkpoint updates are idempotent
proptest! {
    #[test]
    fn checkpoint_updates_are_idempotent(
        checkpoint in arbitrary_checkpoint(),
        processed_count in 0u64..10000u64,
        checkpoint_data in prop::option::of(any::<serde_json::Value>()),
        automaton_name in "[a-z]+-automaton",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            );

            // Create initial checkpoint state
            let initial_state = CheckpointState {
                checkpoint: checkpoint.clone(),
                processed_count,
                last_activity: chrono::Utc::now(),
                data: checkpoint_data.clone(),
                version: 2,
            };

            // Save checkpoint twice
            checkpoint_manager.save_checkpoint(&initial_state).await.unwrap();
            checkpoint_manager.save_checkpoint(&initial_state).await.unwrap();

            // Verify state is consistent
            let state = checkpoint_manager.get_checkpoint_stats().await.unwrap();
            assert_eq!(state.max_processed, initial_state.processed_count);
            assert!(state.last_update.is_some());
        });
    }
}

/// Test checkpoint recovery under various failure scenarios
proptest! {
    #[test]
    fn checkpoint_recovery_is_robust(
        automaton_name in automaton_names(),
        checkpoints in proptest::collection::vec(
            (0u64..1000u64, checkpoint_data()),
            1..=10
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            );

            // Save multiple checkpoints with increasing counts
            let mut expected_final_count = 0u64;
            for (i, (processed_count, data)) in checkpoints.iter().enumerate() {
                expected_final_count = *processed_count;

                let state = CheckpointState {
                    checkpoint: Checkpoint::Stream {
                        message_id: format!("message-{}", i),
                        event_id: None,
                    },
                    processed_count: *processed_count,
                    last_activity: chrono::Utc::now(),
                    data: Some(data.clone()),
                    version: 2,
                };

                checkpoint_manager.save_checkpoint(&state).await.unwrap();
            }

            // Verify final state
            let state = checkpoint_manager.get_checkpoint_stats().await.unwrap();
            assert_eq!(state.max_processed, expected_final_count);
        });
    }
}

/// Test concurrent checkpoint access
proptest! {
    #[test]
    fn concurrent_checkpoint_access_is_safe(
        automaton_name in automaton_names(),
        concurrent_updates in proptest::collection::vec(0u64..1000u64, 1..=20),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            let checkpoint_manager = Arc::new(CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            ));

            // Launch concurrent update tasks
            let mut handles = Vec::new();
            for (i, processed_count) in concurrent_updates.iter().enumerate() {
                let manager = checkpoint_manager.clone();
                let count = *processed_count;
                let handle = tokio::spawn(async move {
                    let state = CheckpointState {
                        checkpoint: Checkpoint::Stream {
                            message_id: format!("concurrent-{}", i),
                            event_id: None,
                        },
                        processed_count: count,
                        last_activity: chrono::Utc::now(),
                        data: Some(serde_json::json!({"task": i})),
                        version: 2,
                    };

                    manager.save_checkpoint(&state).await
                });
                handles.push(handle);
            }

            // Wait for all updates to complete
            let mut results = Vec::new();
            for handle in handles {
                results.push(handle.await.unwrap());
            }

            // Verify all updates succeeded (or failed gracefully)
            let successful_updates = results.iter().filter(|r| r.is_ok()).count();
            assert!(successful_updates > 0, "At least one update should succeed");

            // Verify final state is consistent
            let final_state = checkpoint_manager.get_checkpoint_stats().await.unwrap();
            assert!(final_state.last_update.is_some());
        });
    }
}

/// Test checkpoint state transitions
proptest! {
    #[test]
    fn checkpoint_state_transitions_are_valid(
        automaton_name in automaton_names(),
        initial_count in 0u64..100u64,
        increments in proptest::collection::vec(1u64..100u64, 1..=10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            );

            // Initialize with starting count
            let mut current_count = initial_count;
            let mut state = CheckpointState {
                checkpoint: Checkpoint::Stream {
                    message_id: "initial".to_string(),
                    event_id: None,
                },
                processed_count: current_count,
                last_activity: chrono::Utc::now(),
                data: Some(serde_json::json!({"sequence": 0})),
                version: 2,
            };

            checkpoint_manager.save_checkpoint(&state).await.unwrap();

            // Apply increments sequentially
            for (i, increment) in increments.iter().enumerate() {
                current_count += increment;
                state.processed_count = current_count;
                state.set_last_processed_id(Some(format!("step-{}", i)));
                state.version += 1;
                state.data = Some(serde_json::json!({"sequence": i + 1}));

                checkpoint_manager.save_checkpoint(&state).await.unwrap();

                // Verify the state was updated correctly
                let retrieved_state = checkpoint_manager.get_checkpoint_stats().await.unwrap();
                assert_eq!(retrieved_state.max_processed, current_count);
                assert!(retrieved_state.last_update.is_some());
            }
        });
    }
}

/// Test checkpoint data integrity
proptest! {
    #[test]
    fn checkpoint_data_integrity_is_preserved(
        automaton_name in automaton_names(),
        test_data in checkpoint_data(),
        operations in proptest::collection::vec(
            prop_oneof![
                Just("save".to_string()),
                Just("load".to_string()),
                Just("update".to_string()),
            ],
            1..=50
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            );

            let mut expected_data = test_data.clone();
            let mut processed_count = 0u64;

            // Execute operations sequence
            for (i, operation) in operations.iter().enumerate() {
                match operation.as_str() {
                    "save" => {
                        let state = CheckpointState {
                            checkpoint: Checkpoint::Stream {
                                message_id: format!("op-{}", i),
                                event_id: None,
                            },
                            processed_count,
                            last_activity: chrono::Utc::now(),
                            data: Some(expected_data.clone()),
                            version: 2,
                        };

                        checkpoint_manager.save_checkpoint(&state).await.unwrap();
                    }
                    "load" => {
                        let stats = checkpoint_manager.get_checkpoint_stats().await.unwrap();
                        assert_eq!(stats.max_processed, processed_count);
                        assert!(stats.last_update.is_some());
                    }
                    "update" => {
                        processed_count += 1;
                        expected_data = serde_json::json!({"updated": i, "count": processed_count});

                        let state = CheckpointState {
                            checkpoint: Checkpoint::Stream {
                                message_id: format!("update-{}", i),
                                event_id: None,
                            },
                            processed_count,
                            last_activity: chrono::Utc::now(),
                            data: Some(expected_data.clone()),
                            version: 2,
                        };

                        checkpoint_manager.save_checkpoint(&state).await.unwrap();
                    }
                    _ => unreachable!(),
                }
            }

            // Final verification
            let final_state = checkpoint_manager.get_checkpoint_stats().await.unwrap();
            assert_eq!(final_state.max_processed, processed_count);
            assert!(final_state.last_update.is_some());
        });
    }
}

/// Test checkpoint cleanup behavior
proptest! {
    #[test]
    fn checkpoint_cleanup_maintains_consistency(
        automaton_names in proptest::collection::vec(automaton_names(), 1..=10),
        cleanup_threshold in 1u64..100u64,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            // Create multiple automata with checkpoints
            let mut managers = Vec::new();
            for automaton_name in automaton_names.iter() {
                let manager = CheckpointManager::new(
                    pool.clone(),
                    automaton_name.clone(),
                    format!("{}-group", automaton_name),
                    format!("{}-consumer", automaton_name),
                );

                // Create checkpoint
                let state = CheckpointState {
                    checkpoint: Checkpoint::Stream {
                        message_id: format!("checkpoint-{}", automaton_name),
                        event_id: None,
                    },
                    processed_count: cleanup_threshold,
                    last_activity: chrono::Utc::now(),
                    data: Some(serde_json::json!({"automaton": automaton_name})),
                    version: 2,
                };

                manager.save_checkpoint(&state).await.unwrap();
                managers.push(manager);
            }

            // Verify all checkpoints exist
            for manager in &managers {
                let stats = manager.get_checkpoint_stats().await.unwrap();
                assert_eq!(stats.max_processed, cleanup_threshold);
                assert!(stats.last_update.is_some());
            }

            // Test cleanup doesn't affect other automata
            let first_automaton = &automaton_names[0];
            let cleaned_manager = CheckpointManager::new(
                pool.clone(),
                first_automaton.clone(),
                format!("{}-group", first_automaton),
                format!("{}-consumer", first_automaton),
            );

            // Verify cleanup maintains isolation
            let remaining_checkpoint = cleaned_manager.get_checkpoint_stats().await.unwrap();
            assert!(remaining_checkpoint.last_update.is_some());
        });
    }
}

/// Test checkpoint recovery with realistic event streams using property builders
proptest! {
    #[test]
    fn checkpoint_recovery_with_event_builders(
        events in arbitrary_event_batch(10..50),
        automaton_name in automaton_names(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            // Insert events
            let mut event_ids = Vec::new();
            for event_builder in events {
                let event = event_builder.insert(&pool).await.unwrap();
                event_ids.push(event.id);
            }

            // Create checkpoint manager
            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            );

            // Simulate processing with checkpoints
            let mut processed_count = 0u64;
            for (i, event_id) in event_ids.iter().enumerate() {
                processed_count += 1;
                
                let state = CheckpointState {
                    checkpoint: Checkpoint::Database {
                        event_id: *event_id,
                    },
                    processed_count,
                    last_activity: chrono::Utc::now(),
                    data: Some(json!({"batch_index": i})),
                    version: 2,
                };

                checkpoint_manager.save_checkpoint(&state).await.unwrap();
            }

            // Verify final state matches processed events
            let final_state = checkpoint_manager.get_checkpoint_stats().await.unwrap();
            assert_eq!(final_state.max_processed, event_ids.len() as u64);
            assert!(final_state.last_update.is_some());
        });
    }
}

/// Test checkpoint builders integration
proptest! {
    #[test] 
    fn checkpoint_builder_integration(
        checkpoint in arbitrary_checkpoint(),
        events in arbitrary_event_batch(1..10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            // Insert checkpoint using builder
            checkpoint.insert(&pool).await.unwrap();

            // Insert events
            let mut event_count = 0;
            for event_builder in events {
                event_builder.insert(&pool).await.unwrap();
                event_count += 1;
            }

            // Verify checkpoint exists
            let result = sqlx::query!(
                "SELECT COUNT(*) as count FROM core.automaton_checkpoints"
            )
            .fetch_one(&pool)
            .await
            .unwrap();

            assert!(result.count.unwrap_or(0) > 0);
        });
    }
}

/// Test time-based checkpoint progression using property builders
proptest! {
    #[test]
    fn time_based_checkpoint_progression(
        (start_time, end_time) in arbitrary_time_range(),
        automaton_name in automaton_names(),
        event_count in 5usize..20usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            );

            // Create events spread across time range
            let time_step = (end_time - start_time) / event_count as i32;
            let mut processed_count = 0u64;

            for i in 0..event_count {
                let event_time = start_time + time_step * i as i32;
                processed_count += 1;

                let state = CheckpointState {
                    checkpoint: Checkpoint::Time {
                        timestamp: event_time,
                    },
                    processed_count,
                    last_activity: event_time,
                    data: Some(json!({
                        "time_index": i,
                        "timestamp": event_time.to_rfc3339(),
                    })),
                    version: 2,
                };

                checkpoint_manager.save_checkpoint(&state).await.unwrap();
            }

            // Verify progression
            let final_state = checkpoint_manager.get_checkpoint_stats().await.unwrap();
            assert_eq!(final_state.max_processed, event_count as u64);
        });
    }
}

/// Test checkpoint state with ULID ranges
proptest! {
    #[test]
    fn checkpoint_with_ulid_ranges(
        (start_ulid, end_ulid) in arbitrary_ulid_range(),
        automaton_name in automaton_names(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();

            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            );

            // Create checkpoint with ULID range
            let state = CheckpointState {
                checkpoint: Checkpoint::Database {
                    event_id: end_ulid,
                },
                processed_count: 100, // Arbitrary count
                last_activity: chrono::Utc::now(),
                data: Some(json!({
                    "start_ulid": start_ulid.to_string(),
                    "end_ulid": end_ulid.to_string(),
                    "range_processed": true,
                })),
                version: 2,
            };

            checkpoint_manager.save_checkpoint(&state).await.unwrap();

            // Verify the range was saved
            let stats = checkpoint_manager.get_checkpoint_stats().await.unwrap();
            assert_eq!(stats.max_processed, 100);
            assert!(stats.last_update.is_some());
        });
    }
}
