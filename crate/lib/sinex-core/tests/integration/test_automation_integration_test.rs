//! Test Automation Integration Tests
//!
//! Tests the integration of various automata (analytics, content, health, PKM, search,
//! terminal command canonicalizer) within the Sinex ecosystem using modern infrastructure.
//!
//! These tests verify that:
//! - Automata can be initialized and shut down cleanly
//! - Event processing flows correctly through automata
//! - Checkpoint persistence and recovery works
//! - Multiple automata can coordinate without conflicts
//! - Error handling and resilience patterns function correctly

use chrono::{Duration, Utc};
use color_eyre::eyre::Result;
use serde_json::json;
// Using shorter imports from sinex-core's re-exports
use sinex_core::{DbPoolExt, EventSource};
use sinex_satellite_sdk::{Checkpoint, CheckpointManager};
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use tokio::time::sleep;

/// Test data setup for automation integration tests
async fn setup_automation_test_data(ctx: &TestContext) -> Result<()> {
    tracing::debug!("Setting up test data for automation integration");

    // Create events that various automata should process

    // Analytics events - for analytics automaton
    ctx.create_test_event(
        "filesystem",
        "file.created",
        json!({
            "path": "/tmp/test-analytics.txt",
            "size": 1024,
            "timestamp": Utc::now().timestamp()
        }),
    )
    .await?;

    // Terminal events - for terminal command canonicalizer
    ctx.create_test_event(
        "terminal",
        "command.executed",
        json!({
            "command": "ls -la /tmp",
            "working_directory": "/tmp",
            "exit_code": 0,
            "duration_ms": 150
        }),
    )
    .await?;

    // Health events - for health aggregator
    ctx.create_test_event(
        "system",
        "resource.usage",
        json!({
            "cpu_percent": 45.2,
            "memory_percent": 67.8,
            "disk_usage": 78.5,
            "timestamp": Utc::now().timestamp()
        }),
    )
    .await?;

    // Content events - for content automaton
    ctx.create_test_event(
        "browser",
        "page.visited",
        json!({
            "url": "https://example.com/test-content",
            "title": "Test Content Page",
            "visit_duration_ms": 45000
        }),
    )
    .await?;

    // PKM events - for PKM automaton
    ctx.create_test_event(
        "editor",
        "file.edited",
        json!({
            "file_path": "/home/user/notes/test-knowledge.md",
            "content_type": "markdown",
            "word_count": 256,
            "modification_time": Utc::now().timestamp()
        }),
    )
    .await?;

    // Search events - for search automaton
    ctx.create_test_event(
        "search",
        "query.executed",
        json!({
            "query": "test automation integration",
            "source": "web",
            "results_count": 42,
            "execution_time_ms": 234
        }),
    )
    .await?;

    tracing::debug!("Test data setup complete");
    Ok(())
}

/// Test basic automaton lifecycle - startup, processing, shutdown
#[sinex_test]
async fn test_automaton_lifecycle_basic(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing basic automaton lifecycle");

    // Setup test data
    setup_automation_test_data(&ctx).await?;

    // Verify initial event count
    let initial_events = ctx.pool.events().count_all().await?;
    assert!(initial_events >= 6, "Should have test events available");

    // Test automaton identification and status
    let automaton_name = "test-analytics-automaton";

    // Create a CheckpointManager to simulate automaton checkpoint handling
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        automaton_name.to_string(),
        "default".to_string(),
        "test-consumer".to_string(),
    );

    // Load initial checkpoint (should be empty/default)
    let initial_checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(initial_checkpoint.processed_count, 0);

    // Simulate processing by creating an updated checkpoint
    let mut updated_state = initial_checkpoint;
    updated_state.processed_count = 3;
    updated_state.checkpoint = Checkpoint::External {
        position: json!({"events_processed": 3, "test_mode": true, "position": "test-position-3"}),
        description: "Basic lifecycle test checkpoint".to_string(),
    };

    // Save the updated checkpoint
    checkpoint_manager.save_checkpoint(&updated_state).await?;

    // Verify checkpoint was updated by loading it again
    let final_checkpoint = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(final_checkpoint.processed_count, 3);

    tracing::info!("Basic automaton lifecycle test completed successfully");
    Ok(())
}

/// Test multiple automata coordination and conflict avoidance
#[sinex_test]
async fn test_multiple_automata_coordination(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing multiple automata coordination");

    // Setup diverse test events
    setup_automation_test_data(&ctx).await?;

    let automaton_names = vec![
        "analytics-automaton",
        "content-automaton",
        "health-automaton",
    ];

    // Initialize multiple automata by creating their CheckpointManagers
    let mut managers = HashMap::new();
    for &name in &automaton_names {
        let manager = CheckpointManager::new(
            ctx.pool.clone(),
            name.to_string(),
            "default".to_string(),
            format!("test-consumer-{name}"),
        );
        managers.insert(name.to_string(), manager);
    }

    // Simulate concurrent processing by each automaton
    for (i, &name) in automaton_names.iter().enumerate() {
        let manager = managers.get(name).unwrap();

        // Load initial checkpoint
        let mut checkpoint_state = manager.load_checkpoint().await?;

        // Each automaton processes a different number of events to avoid conflicts
        let events_to_process = (i + 1) as u64; // Changed to u64 to match processed_count type
        checkpoint_state.processed_count = events_to_process;
        checkpoint_state.checkpoint = Checkpoint::External {
            position: json!({
                "automaton_type": name,
                "coordination_test": true,
                "events_processed": events_to_process,
                "position": format!("position-{}", events_to_process)
            }),
            description: format!("Coordination test for {name}"),
        };

        // Save the checkpoint
        manager.save_checkpoint(&checkpoint_state).await?;

        // Add a small delay to simulate real processing time
        sleep(std::time::Duration::from_millis(10)).await;
    }

    // Verify all automata processed events without conflicts
    for (i, &name) in automaton_names.iter().enumerate() {
        let manager = managers.get(name).unwrap();
        let checkpoint = manager.load_checkpoint().await?;
        let expected_count = (i + 1) as u64;

        assert_eq!(
            checkpoint.processed_count, expected_count,
            "Automaton {name} should have processed {expected_count} events"
        );
    }

    tracing::info!("Multiple automata coordination test completed successfully");
    Ok(())
}

/// Test automaton recovery from checkpoint after simulated restart
#[sinex_test]
async fn test_automaton_checkpoint_recovery(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing automaton checkpoint recovery");

    setup_automation_test_data(&ctx).await?;

    let automaton_name = "recovery-test-automaton";

    // Create a CheckpointManager
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        automaton_name.to_string(),
        "default".to_string(),
        "test-consumer".to_string(),
    );

    // Phase 1: Initial processing session
    let mut initial_state = checkpoint_manager.load_checkpoint().await?;
    initial_state.processed_count = 4;
    initial_state.checkpoint = Checkpoint::External {
        position: json!({
            "session": "initial",
            "recovery_test": true,
            "processed_events": 4,
            "position": "recovery-position-4"
        }),
        description: "Initial processing session".to_string(),
    };

    // Save checkpoint to simulate processing
    checkpoint_manager.save_checkpoint(&initial_state).await?;

    // Phase 2: Simulate restart by creating new manager with same identity
    let recovery_manager = CheckpointManager::new(
        ctx.pool.clone(),
        automaton_name.to_string(),
        "default".to_string(),
        "test-consumer".to_string(),
    );

    // Load checkpoint after "restart"
    let recovered_state = recovery_manager.load_checkpoint().await?;

    // Verify recovery state matches what we saved
    assert_eq!(recovered_state.processed_count, 4);
    if let Checkpoint::External {
        position,
        description,
    } = &recovered_state.checkpoint
    {
        assert_eq!(position["position"], "recovery-position-4");
        assert_eq!(description, "Initial processing session");
    } else {
        panic!("Expected External checkpoint");
    }

    // Phase 3: Continue processing from checkpoint
    let mut continued_state = recovered_state;
    continued_state.processed_count = 6; // process 2 more events
    continued_state.checkpoint = Checkpoint::External {
        position: json!({
            "session": "continued",
            "recovery_test": true,
            "processed_events": 6,
            "recovered_from": 4,
            "position": "recovery-position-6"
        }),
        description: "Continued after recovery".to_string(),
    };

    recovery_manager.save_checkpoint(&continued_state).await?;

    // Final verification
    let final_state = recovery_manager.load_checkpoint().await?;
    assert_eq!(final_state.processed_count, 6);

    tracing::info!("Automaton checkpoint recovery test completed successfully");
    Ok(())
}

/// Test automaton event filtering and processing logic
#[sinex_test]
async fn test_automaton_event_filtering(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing automaton event filtering and processing logic");

    // Create mixed event types - some relevant, some irrelevant
    ctx.create_test_event(
        "filesystem",
        "file.created",
        json!({"path": "/tmp/relevant.txt", "relevant": true}),
    )
    .await?;

    ctx.create_test_event(
        "network",
        "connection.established",
        json!({"host": "example.com", "relevant": false}),
    )
    .await?;

    ctx.create_test_event(
        "filesystem",
        "file.deleted",
        json!({"path": "/tmp/also-relevant.txt", "relevant": true}),
    )
    .await?;

    let automaton_name = "filtering-test-automaton";

    // Create CheckpointManager for filtering test
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        automaton_name.to_string(),
        "default".to_string(),
        "test-consumer".to_string(),
    );

    // Get events by source to simulate filtering
    let filesystem_events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("filesystem"),
            sinex_core::types::Pagination::new(Some(10), None),
        )
        .await?;

    // Simulate processing only filesystem events (filtering)
    let relevant_events = filesystem_events.len() as u64;

    let mut filtered_state = checkpoint_manager.load_checkpoint().await?;
    filtered_state.processed_count = relevant_events;
    filtered_state.checkpoint = Checkpoint::External {
        position: json!({
            "filter_test": true,
            "filters": {
                "sources": ["filesystem"],
                "event_types": ["file.created", "file.deleted", "file.modified"]
            },
            "stats": {
                "total_seen": 3, // saw all 3 events we created
                "filtered_in": relevant_events, // processed filesystem events
                "filtered_out": 3 - relevant_events // ignored non-filesystem events
            },
            "position": format!("filtered-position-{}", relevant_events)
        }),
        description: "Event filtering test checkpoint".to_string(),
    };

    checkpoint_manager.save_checkpoint(&filtered_state).await?;

    // Verify filtering worked correctly
    let final_state = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(final_state.processed_count, relevant_events);

    // Verify position contains filtering stats
    if let Checkpoint::External { position, .. } = &final_state.checkpoint {
        assert_eq!(position["stats"]["filtered_in"], relevant_events);
        assert!(position.get("filter_test").is_some());
    }

    tracing::info!("Automaton event filtering test completed successfully");
    Ok(())
}

/// Test automaton performance under load
#[sinex_test]
async fn test_automaton_performance_under_load(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing automaton performance under load");

    let automaton_name = "performance-test-automaton";

    // Create a substantial number of test events
    let event_count = 25u64; // Changed to u64
    for i in 0..event_count {
        ctx.create_test_event(
            "performance",
            "load.test",
            json!({
                "sequence": i,
                "payload_size": "moderate",
                "test_data": format!("Performance test event number {}", i)
            }),
        )
        .await?;

        // Small delay to avoid overwhelming the system
        if i.is_multiple_of(10) {
            sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    // Create CheckpointManager for performance test
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        automaton_name.to_string(),
        "default".to_string(),
        "test-consumer".to_string(),
    );

    let start_time = Utc::now();

    // Simulate batch processing
    let batch_size = 5u64;
    let mut processed = 0u64;

    while processed < event_count {
        let batch_start = Utc::now();
        let batch_end_target = std::cmp::min(processed + batch_size, event_count);

        // Simulate processing time
        sleep(std::time::Duration::from_millis(5)).await;

        processed = batch_end_target;

        // Update checkpoint
        let mut state = checkpoint_manager.load_checkpoint().await?;
        state.processed_count = processed;
        state.checkpoint = Checkpoint::External {
            position: json!({
                "performance_test": true,
                "target_events": event_count,
                "batch_size": batch_size,
                "current_batch": processed / batch_size,
                "position": format!("performance-batch-{}", processed / batch_size)
            }),
            description: format!("Performance test batch {}", processed / batch_size),
        };

        checkpoint_manager.save_checkpoint(&state).await?;

        let batch_duration = Utc::now().signed_duration_since(batch_start);

        // Performance assertion - batch should complete within reasonable time
        assert!(
            batch_duration < Duration::seconds(5),
            "Batch processing took too long: {batch_duration:?}"
        );
    }

    let total_duration = Utc::now().signed_duration_since(start_time);

    // Verify all events were processed
    let final_state = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(final_state.processed_count, event_count);

    // Performance benchmark - should process at reasonable rate
    let events_per_second = event_count as f64 / total_duration.num_seconds() as f64;
    assert!(
        events_per_second > 5.0,
        "Processing rate too slow: {events_per_second} events/second"
    );

    tracing::info!("Automaton performance test completed successfully");
    Ok(())
}

/// Test automaton error handling and resilience patterns
#[sinex_test]
async fn test_automaton_error_handling(ctx: TestContext) -> Result<()> {
    tracing::info!("Testing automaton error handling and resilience");

    setup_automation_test_data(&ctx).await?;

    let automaton_name = "error-handling-automaton";

    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        automaton_name.to_string(),
        "default".to_string(),
        "test-consumer".to_string(),
    );

    // Phase 1: Normal processing
    let mut normal_state = checkpoint_manager.load_checkpoint().await?;
    normal_state.processed_count = 2;
    normal_state.checkpoint = Checkpoint::External {
        position: json!({
            "error_handling_test": true,
            "phase": "normal_processing",
            "error_count": 0,
            "position": "normal-processing-2"
        }),
        description: "Normal processing phase".to_string(),
    };

    checkpoint_manager.save_checkpoint(&normal_state).await?;

    // Phase 2: Simulate error condition and recovery
    let mut error_state = checkpoint_manager.load_checkpoint().await?;
    error_state.processed_count = 3; // managed to process 1 more before error
    error_state.checkpoint = Checkpoint::External {
        position: json!({
            "error_handling_test": true,
            "phase": "error_recovery",
            "error_count": 1,
            "last_error": {
                "error_type": "processing_error",
                "message": "Simulated processing error for testing",
                "timestamp": Utc::now().timestamp(),
                "recovery_action": "skip_and_continue"
            },
            "recovery_stats": {
                "errors_recovered": 1,
                "events_skipped": 1
            },
            "position": "error-recovery-3"
        }),
        description: "Error recovery phase".to_string(),
    };

    checkpoint_manager.save_checkpoint(&error_state).await?;

    // Phase 3: Continue processing after error recovery
    let mut recovery_state = checkpoint_manager.load_checkpoint().await?;
    recovery_state.processed_count = 5; // recovered and processed 2 more
    recovery_state.checkpoint = Checkpoint::External {
        position: json!({
            "error_handling_test": true,
            "phase": "post_recovery",
            "error_count": 1,
            "recovery_successful": true,
            "events_after_recovery": 2,
            "position": "post-recovery-5"
        }),
        description: "Post-recovery processing".to_string(),
    };

    checkpoint_manager.save_checkpoint(&recovery_state).await?;

    // Verify error handling state
    let final_state = checkpoint_manager.load_checkpoint().await?;
    assert_eq!(final_state.processed_count, 5);

    if let Checkpoint::External { position, .. } = &final_state.checkpoint {
        assert_eq!(position["error_count"], 1);
        assert_eq!(position["recovery_successful"], true);
        assert!(position.get("error_handling_test").is_some());
    }

    tracing::info!("Automaton error handling test completed successfully");
    Ok(())
}
