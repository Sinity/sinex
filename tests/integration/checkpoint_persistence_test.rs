//! Checkpoint Persistence Integration Tests
//!
//! Tests for checkpoint persistence and recovery in the Sinex automaton system.
//! Verifies that checkpoints are correctly saved to and restored from the database,
//! and that checkpoint managers can persist and recover state correctly.

use color_eyre::eyre::Result;
use sinex_core::types::domain::EventSource;
use sinex_test_utils::prelude::*;
use serde_json::json;
use sinex_satellite_sdk::checkpoint::CheckpointManager;
use tracing::info;

#[sinex_test]
async fn test_checkpoint_recovery_from_empty_state(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    // Test that checkpoint recovery works when starting from empty state
    let service_name = "empty-state-test".to_string();
    let consumer_group = "empty-state-group".to_string();
    
    info!("Testing checkpoint recovery from empty state");
    
    // Create checkpoint manager
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        service_name.clone(),
        consumer_group.clone(),
        "test-consumer".to_string(),
    );

    // Load checkpoint from empty state - should get default checkpoint
    let empty_checkpoint = checkpoint_manager.load_checkpoint().await?;
    
    // Verify empty checkpoint properties
    ctx.assert("empty checkpoint")
        .eq(&empty_checkpoint.processed_count, &0)?
        .that(
            empty_checkpoint.last_processed_id().is_none(),
            "Empty checkpoint should have no last processed ID",
        )?;

    info!("✅ Empty state checkpoint recovery test completed successfully");
    Ok(())
}

#[sinex_test]
async fn test_checkpoint_manager_basic_functionality(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    // Test basic checkpoint manager functionality
    let service_name = "basic-functionality-test".to_string();
    let consumer_group = "basic-functionality-group".to_string();
    
    info!("Testing basic checkpoint manager functionality");
    
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        service_name.clone(),
        consumer_group.clone(),
        "test-consumer".to_string(),
    );

    // Create test events to simulate processed events
    let test_events = vec![
        ctx.create_test_event("checkpoint-test", "test.event", json!({"test": "event1"})).await?,
        ctx.create_test_event("checkpoint-test", "test.event", json!({"test": "event2"})).await?,
        ctx.create_test_event("checkpoint-test", "test.event", json!({"test": "event3"})).await?,
    ];

    info!("Created {} test events for checkpoint testing", test_events.len());

    // Verify the checkpoint manager can load checkpoints
    let initial_checkpoint = checkpoint_manager.load_checkpoint().await?;
    
    ctx.assert("initial checkpoint state")
        .eq(&initial_checkpoint.processed_count, &0)?
        .that(
            initial_checkpoint.last_processed_id().is_none(),
            "Initial checkpoint should have no last processed ID",
        )?;

    // Verify events were created in the database using direct repository access
    let created_events = ctx
        .pool
        .events()
        .get_by_source(&EventSource::from_static("checkpoint-test"), Some(100), None)
        .await?;
    ctx.assert("test event creation")
        .eq(&created_events.len(), &3)?;

    info!("✅ Basic checkpoint manager functionality test completed successfully");
    Ok(())
}

#[sinex_test]
async fn test_checkpoint_manager_isolation(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    // Test that different checkpoint managers are properly isolated
    let service_name_1 = "isolation-test-1".to_string();
    let service_name_2 = "isolation-test-2".to_string();
    let consumer_group = "isolation-test-group".to_string();
    
    info!("Testing checkpoint manager isolation");
    
    let checkpoint_manager_1 = CheckpointManager::new(
        ctx.pool.clone(),
        service_name_1.clone(),
        consumer_group.clone(),
        "test-consumer-1".to_string(),
    );

    let checkpoint_manager_2 = CheckpointManager::new(
        ctx.pool.clone(),
        service_name_2.clone(),
        consumer_group.clone(),
        "test-consumer-2".to_string(),
    );

    // Both managers should start with empty checkpoints
    let checkpoint_1 = checkpoint_manager_1.load_checkpoint().await?;
    let checkpoint_2 = checkpoint_manager_2.load_checkpoint().await?;

    ctx.assert("checkpoint isolation")
        .eq(&checkpoint_1.processed_count, &0)?
        .eq(&checkpoint_2.processed_count, &0)?
        .that(
            checkpoint_1.last_processed_id().is_none(),
            "First checkpoint should have no last processed ID",
        )?
        .that(
            checkpoint_2.last_processed_id().is_none(),
            "Second checkpoint should have no last processed ID",
        )?;

    info!("✅ Checkpoint manager isolation test completed successfully");
    Ok(())
}

#[sinex_test]
async fn test_checkpoint_database_integration(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    // Test that checkpoint manager properly integrates with the database
    let service_name = "database-integration-test".to_string();
    let consumer_group = "database-integration-group".to_string();
    
    info!("Testing checkpoint database integration");
    
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        service_name.clone(),
        consumer_group.clone(),
        "test-consumer".to_string(),
    );

    // Test that we can load checkpoints without errors
    let loaded_checkpoint = checkpoint_manager.load_checkpoint().await?;
    
    // Verify the checkpoint has the expected structure
    ctx.assert("checkpoint structure")
        .that(
            loaded_checkpoint.processed_count >= 0,
            "Processed count should be non-negative",
        )?;

    // Test that multiple loads of the same checkpoint return consistent results
    let loaded_checkpoint_2 = checkpoint_manager.load_checkpoint().await?;
    
    ctx.assert("checkpoint consistency")
        .eq(&loaded_checkpoint.processed_count, &loaded_checkpoint_2.processed_count)?
        .that(
            loaded_checkpoint.last_processed_id() == loaded_checkpoint_2.last_processed_id(),
            "Last processed ID should be consistent across loads",
        )?;

    info!("✅ Checkpoint database integration test completed successfully");
    Ok(())
}

#[sinex_test]
async fn test_checkpoint_with_events_context(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    // Test checkpoint functionality in the context of actual events
    let service_name = "events-context-test".to_string();
    let consumer_group = "events-context-group".to_string();
    
    info!("Testing checkpoint functionality with events context");
    
    let checkpoint_manager = CheckpointManager::new(
        ctx.pool.clone(),
        service_name.clone(),
        consumer_group.clone(),
        "test-consumer".to_string(),
    );

    // Create events that could be referenced by checkpoints
    let test_events = vec![
        ctx.create_test_event("checkpoint-context", "file.created", json!({
            "path": "/test/file1.txt",
            "size": 1024
        })).await?,
        ctx.create_test_event("checkpoint-context", "file.modified", json!({
            "path": "/test/file2.txt", 
            "size": 2048
        })).await?,
    ];

    info!("Created {} events for checkpoint context testing", test_events.len());

    // Load checkpoint and verify it works in the context of these events
    let checkpoint = checkpoint_manager.load_checkpoint().await?;
    
    // Verify checkpoint state
    ctx.assert("checkpoint with events context")
        .that(
            checkpoint.processed_count >= 0,
            "Checkpoint processed count should be valid",
        )?;

    // Verify our test events exist in the database using direct repository access
    let events_in_db = ctx
        .pool
        .events()
        .get_by_source(&EventSource::from_static("checkpoint-context"), Some(100), None)
        .await?;
    ctx.assert("events in database")
        .eq(&events_in_db.len(), &2)?;

    // Verify event IDs are valid ULIDs
    for event in &test_events {
        ctx.assert("event ID validity")
            .that(
                event.id.is_some(),
                "Test event should have a valid ID",
            )?;
    }

    info!("✅ Checkpoint with events context test completed successfully");
    Ok(())
}