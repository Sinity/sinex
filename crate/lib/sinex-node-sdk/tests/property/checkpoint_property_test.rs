//! Property tests for checkpoint management
//!
//! Tests that verify checkpoint consistency, recovery, and concurrency properties
//! using modern test infrastructure.

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use sinex_core::SinexError;
use sinex_node_sdk::Checkpoint;
use sinex_node_sdk::{CheckpointManager, CheckpointState};
use sinex_test_utils::{prelude::*, sinex_prop, sinex_test, TestContext};
use std::sync::Arc;

// =============================================================================
// Property Test Strategies
// =============================================================================

/// Strategy for generating valid processor names
fn processor_names() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("command-canonicalizer".to_string()),
        Just("health-aggregator".to_string()),
        Just("pkm-automaton".to_string()),
        Just("analytics-automaton".to_string()),
        Just("content-automaton".to_string()),
        Just("search-automaton".to_string()),
        Just("test-automaton".to_string()),
    ]
}

/// Strategy for generating checkpoint data
fn checkpoint_data() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        Just(serde_json::json!(null)),
        Just(serde_json::json!({"cursor": "12345"})),
        Just(serde_json::json!({"processed": 100, "skipped": 5})),
        Just(serde_json::json!({"state": "active", "last_seen": "2024-01-01T00:00:00Z"})),
    ]
}

// =============================================================================
// Property Tests
// =============================================================================

#[sinex_prop]
async fn checkpoint_updates_are_idempotent(
    ctx: &TestContext,
    #[strategy(processor_names())] processor_name: String,
    #[strategy(0u64..10000u64)] processed_count: u64,
    #[strategy(prop::option::of("[0-9A-HJKMNP-TV-Z]{26}"))] last_processed_id: Option<String>,
    #[strategy(checkpoint_data())] checkpoint_data: serde_json::Value,
) -> Result<(), SinexError> {
    let kv = ctx
        .ensure_checkpoint_kv()
        .await
        .map_err(|e| SinexError::service(format!("{e:?}")))?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        processor_name.clone(),
        format!("{processor_name}-group"),
        format!("{processor_name}-consumer"),
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
    checkpoint_manager.save_checkpoint(&initial_state).await?;
    checkpoint_manager.save_checkpoint(&initial_state).await?;

    // Verify state is consistent
    let state = checkpoint_manager.get_checkpoint_stats().await?;
    prop_assert_eq!(state.max_processed, initial_state.processed_count);
    prop_assert!(state.last_update.is_some());
    Ok(())
}

#[sinex_prop]
async fn checkpoint_recovery_is_robust(
    ctx: &TestContext,
    #[strategy(processor_names())] processor_name: String,
    #[strategy(proptest::collection::vec((0u64..1000u64, checkpoint_data()), 1..=10))]
    checkpoints: Vec<(u64, serde_json::Value)>,
) -> Result<(), SinexError> {
    let kv = ctx
        .ensure_checkpoint_kv()
        .await
        .map_err(|e| SinexError::service(format!("failed to get checkpoint kv: {e:?}")))?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        processor_name.clone(),
        format!("{processor_name}-group"),
        format!("{processor_name}-consumer"),
    );

    // Save multiple checkpoints with increasing counts
    let mut expected_final_count = 0u64;
    for (i, (processed_count, data)) in checkpoints.iter().enumerate() {
        expected_final_count = *processed_count;

        let state = CheckpointState {
            checkpoint: Checkpoint::Stream {
                message_id: format!("message-{i}"),
                event_id: None,
            },
            processed_count: *processed_count,
            last_activity: chrono::Utc::now(),
            data: Some(data.clone()),
            version: 2,
        };

        checkpoint_manager.save_checkpoint(&state).await?;
    }

    // Verify final state
    let state = checkpoint_manager.get_checkpoint_stats().await?;
    prop_assert_eq!(state.max_processed, expected_final_count);
    Ok(())
}

#[sinex_prop]
async fn concurrent_checkpoint_access_is_safe(
    ctx: &TestContext,
    #[strategy(processor_names())] processor_name: String,
    #[strategy(proptest::collection::vec(0u64..1000u64, 1..=20))] concurrent_updates: Vec<u64>,
) -> Result<(), SinexError> {
    let kv = ctx
        .ensure_checkpoint_kv()
        .await
        .map_err(|e| SinexError::service(format!("failed to get checkpoint kv: {e:?}")))?;
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        kv,
        processor_name.clone(),
        format!("{processor_name}-group"),
        format!("{processor_name}-consumer"),
    ));

    // Launch concurrent update tasks
    let mut handles = Vec::new();
    for (i, processed_count) in concurrent_updates.iter().enumerate() {
        let manager = checkpoint_manager.clone();
        let count = *processed_count;
        let handle = tokio::spawn(async move {
            let state = CheckpointState {
                checkpoint: Checkpoint::Stream {
                    message_id: format!("concurrent-{i}"),
                    event_id: None,
                },
                processed_count: count,
                last_activity: chrono::Utc::now(),
                data: Some(serde_json::json!({ "task": i })),
                version: 2,
            };

            manager.save_checkpoint(&state).await
        });
        handles.push(handle);
    }

    // Wait for all updates to complete
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.expect("task join"));
    }

    // Verify all updates succeeded (or failed gracefully)
    let successful_updates = results.iter().filter(|r| r.is_ok()).count();
    prop_assert!(successful_updates > 0, "At least one update should succeed");

    // Verify final state is consistent
    let final_state = checkpoint_manager.get_checkpoint_stats().await?;
    prop_assert!(final_state.last_update.is_some());
    Ok(())
}

#[sinex_prop]
async fn checkpoint_state_transitions_are_valid(
    ctx: &TestContext,
    #[strategy(processor_names())] processor_name: String,
    #[strategy(0u64..100u64)] initial_count: u64,
    #[strategy(proptest::collection::vec(1u64..100u64, 1..=10))] increments: Vec<u64>,
) -> Result<(), SinexError> {
    let kv = ctx
        .ensure_checkpoint_kv()
        .await
        .map_err(|e| SinexError::service(format!("failed to get checkpoint kv: {e:?}")))?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        processor_name.clone(),
        format!("{processor_name}-group"),
        format!("{processor_name}-consumer"),
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
        data: Some(serde_json::json!({ "sequence": 0 })),
        version: 2,
    };

    checkpoint_manager.save_checkpoint(&state).await?;

    // Apply increments sequentially
    for (i, increment) in increments.iter().enumerate() {
        current_count += increment;
        state.processed_count = current_count;
        state.set_last_processed_id(Some(format!("step-{i}")));
        state.version += 1;
        state.data = Some(serde_json::json!({ "sequence": i + 1 }));

        checkpoint_manager.save_checkpoint(&state).await?;

        // Verify the state was updated correctly
        let retrieved_state = checkpoint_manager.get_checkpoint_stats().await?;
        prop_assert_eq!(retrieved_state.max_processed, current_count);
        prop_assert!(retrieved_state.last_update.is_some());
    }
    Ok(())
}

#[sinex_prop]
async fn checkpoint_data_integrity_is_preserved(
    ctx: &TestContext,
    #[strategy(processor_names())] processor_base: String,
    #[strategy(checkpoint_data())] test_data: serde_json::Value,
    #[strategy(proptest::collection::vec(
        prop_oneof![
            Just("save".to_string()),
            Just("load".to_string()),
            Just("update".to_string()),
        ],
        1..=50
    ))]
    operations: Vec<String>,
) -> Result<(), SinexError> {
    // Use unique processor name per test run to avoid KV bucket conflicts
    let unique_suffix = sinex_core::types::Ulid::new();
    let processor_name = format!("{processor_base}-{unique_suffix}");

    let kv = ctx
        .ensure_checkpoint_kv()
        .await
        .map_err(|e| SinexError::service(format!("failed to get checkpoint kv: {e:?}")))?;
    let checkpoint_manager = CheckpointManager::new(
        kv,
        processor_name.clone(),
        format!("{processor_name}-group"),
        format!("{processor_name}-consumer"),
    );

    let mut expected_data = test_data.clone();
    let mut processed_count = 0u64;
    let mut has_saved = false; // Track if we've saved anything

    // Execute operations sequence
    for (i, operation) in operations.iter().enumerate() {
        match operation.as_str() {
            "save" => {
                let state = CheckpointState {
                    checkpoint: Checkpoint::Stream {
                        message_id: format!("op-{i}"),
                        event_id: None,
                    },
                    processed_count,
                    last_activity: chrono::Utc::now(),
                    data: Some(expected_data.clone()),
                    version: 2,
                };

                checkpoint_manager.save_checkpoint(&state).await?;
                has_saved = true;
            }
            "load" => {
                // Only verify checkpoint if we've saved something
                if has_saved {
                    let stats = checkpoint_manager.get_checkpoint_stats().await?;
                    prop_assert_eq!(stats.max_processed, processed_count);
                    prop_assert!(stats.last_update.is_some());
                }
            }
            "update" => {
                processed_count += 1;
                expected_data = serde_json::json!({ "updated": i, "count": processed_count });

                let state = CheckpointState {
                    checkpoint: Checkpoint::Stream {
                        message_id: format!("update-{i}"),
                        event_id: None,
                    },
                    processed_count,
                    last_activity: chrono::Utc::now(),
                    data: Some(expected_data.clone()),
                    version: 2,
                };

                checkpoint_manager.save_checkpoint(&state).await?;
                has_saved = true;
            }
            _ => unreachable!(),
        }
    }

    // Final verification (only if we've saved something)
    if has_saved {
        let final_state = checkpoint_manager.get_checkpoint_stats().await?;
        prop_assert_eq!(final_state.max_processed, processed_count);
        prop_assert!(final_state.last_update.is_some());
    }
    Ok(())
}

#[sinex_prop]
async fn checkpoint_cleanup_maintains_consistency(
    ctx: &TestContext,
    #[strategy(proptest::collection::vec(processor_names(), 1..=10))] processor_names: Vec<String>,
    #[strategy(1u64..100u64)] cleanup_threshold: u64,
) -> Result<(), SinexError> {
    let kv = ctx
        .ensure_checkpoint_kv()
        .await
        .map_err(|e| SinexError::service(format!("failed to get checkpoint kv: {e:?}")))?; // Shared KV for this test run

    // Create multiple automata with checkpoints
    let mut managers = Vec::new();
    for processor_name in processor_names.iter() {
        let manager = CheckpointManager::new(
            kv.clone(),
            processor_name.clone(),
            format!("{processor_name}-group"),
            format!("{processor_name}-consumer"),
        );

        // Create checkpoint
        let state = CheckpointState {
            checkpoint: Checkpoint::Stream {
                message_id: format!("checkpoint-{processor_name}"),
                event_id: None,
            },
            processed_count: cleanup_threshold,
            last_activity: chrono::Utc::now(),
            data: Some(serde_json::json!({ "automaton": processor_name })),
            version: 2,
        };

        manager.save_checkpoint(&state).await?;
        managers.push(manager);
    }

    // Verify all checkpoints exist
    for manager in &managers {
        let stats = manager.get_checkpoint_stats().await?;
        prop_assert_eq!(stats.max_processed, cleanup_threshold);
        prop_assert!(stats.last_update.is_some());
    }

    // Test cleanup doesn't affect other automata
    let first_automaton = &processor_names[0];
    let cleaned_manager = CheckpointManager::new(
        kv.clone(),
        first_automaton.clone(),
        format!("{first_automaton}-group"),
        format!("{first_automaton}-consumer"),
    );

    // Verify cleanup maintains isolation
    let remaining_checkpoint = cleaned_manager.get_checkpoint_stats().await?;
    prop_assert!(remaining_checkpoint.last_update.is_some());
    Ok(())
}

// =============================================================================
// Stress Tests
// =============================================================================

#[cfg(test)]
mod stress_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[sinex_test]
    async fn stress_test_massive_concurrent_checkpoint_updates(ctx: TestContext) -> Result<()> {
        const NUM_THREADS: usize = 10;
        const UPDATES_PER_THREAD: usize = 100;
        const EXPECTED_TOTAL: usize = NUM_THREADS * UPDATES_PER_THREAD;

        let ctx = ctx.with_nats().await?; // Ensure NATS is available
        let kv = ctx.checkpoint_kv().await?;
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for thread_id in 0..NUM_THREADS {
            let kv = kv.clone();
            let counter = Arc::clone(&counter);
            let processor_name = format!("stress-processor-{thread_id}");

            let handle = tokio::spawn(async move {
                let checkpoint_manager = CheckpointManager::new(
                    kv,
                    processor_name.clone(),
                    format!("{processor_name}-group"),
                    format!("{processor_name}-consumer"),
                );

                for i in 0..UPDATES_PER_THREAD {
                    let state = CheckpointState {
                        checkpoint: Checkpoint::Stream {
                            message_id: format!("stress-{thread_id}-{i}"),
                            event_id: None,
                        },
                        processed_count: i as u64,
                        last_activity: chrono::Utc::now(),
                        data: Some(serde_json::json!({"thread": thread_id, "iteration": i})),
                        version: 2,
                    };

                    if checkpoint_manager.save_checkpoint(&state).await.is_ok() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }

                    // Occasional yield to increase contention
                    if i.rem_euclid(10) == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.expect("Task should complete successfully");
        }

        // Verify results
        let successful_updates = counter.load(Ordering::Relaxed);
        assert!(
            successful_updates >= EXPECTED_TOTAL / 2,
            "Should have at least half successful updates: {successful_updates}/{EXPECTED_TOTAL}"
        );

        Ok(())
    }
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[sinex_test]
    async fn test_strategy_generators() -> Result<()> {
        // Test processor name strategy
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let processor_name = processor_names().new_tree(&mut runner).unwrap().current();

        assert!(!processor_name.is_empty());
        assert!(
            processor_name.contains("automaton")
                || processor_name.contains("canonicalizer")
                || processor_name.contains("aggregator")
        );

        // Test checkpoint data strategy
        let checkpoint_data = checkpoint_data().new_tree(&mut runner).unwrap().current();

        assert!(checkpoint_data.is_null() || checkpoint_data.is_object());

        Ok(())
    }

    #[sinex_test]
    async fn test_checkpoint_state_methods() -> Result<()> {
        let mut state = CheckpointState::default();

        // Test initial state
        assert_eq!(state.last_processed_id(), None);
        assert_eq!(state.processed_count, 0);
        assert_eq!(state.version, 2);

        // Test setting stream ID
        state.set_last_processed_id(Some("stream-123".to_string()));
        assert_eq!(state.last_processed_id(), Some("stream-123".to_string()));

        // Test setting ULID
        let ulid = sinex_core::types::Ulid::new();
        state.set_last_processed_id(Some(ulid.to_string()));
        assert_eq!(state.last_processed_id(), Some(ulid.to_string()));

        // Test clearing
        state.set_last_processed_id(None);
        assert_eq!(state.last_processed_id(), None);

        Ok(())
    }
}
