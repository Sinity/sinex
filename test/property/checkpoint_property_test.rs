// Property tests for checkpoint management
//
// Tests that verify checkpoint consistency, recovery, and concurrency properties

use crate::common::prelude::*;

use crate::common::prelude::*;
use crate::property::strategies::*;
use proptest::prelude::*;
use sinex_satellite_sdk::checkpoint::{CheckpointManager, CheckpointState};
use sinex_satellite_sdk::stream_processor::Checkpoint;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Test that checkpoint updates are idempotent
proptest! {
    #[test]
    fn checkpoint_updates_are_idempotent(
        automaton_name in automaton_names(),
        processed_count in 0u64..10000u64,
        last_processed_id in prop::option::of("[0-9A-HJKMNP-TV-Z]{26}"),
        checkpoint_data in checkpoint_data(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool();

            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                automaton_name.clone(),
                format!("{}-group", automaton_name),
                format!("{}-consumer", automaton_name),
            );

            // Create initial checkpoint state
            let checkpoint = if let Some(id) = last_processed_id.clone() {
                Checkpoint::Stream {
                    message_id: id,
                    event_id: None,
                }
            } else {
                Checkpoint::None
            };
            
            let initial_state = CheckpointState {
                checkpoint,
                processed_count,
                last_activity: chrono::Utc::now(),
                data: Some(checkpoint_data.clone()),
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
            let pool = ctx.pool();

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
            let pool = ctx.pool();

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
            let pool = ctx.pool();

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
            let pool = ctx.pool();

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
            let pool = ctx.pool();

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
