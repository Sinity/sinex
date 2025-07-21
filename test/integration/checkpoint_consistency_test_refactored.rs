// Checkpoint consistency verification integration tests - Refactored with Test Macros
//
// Tests for checkpoint state consistency using test macros where applicable

use crate::common::prelude::*;
use crate::common::query_helpers::TestQueries;
use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder};
use sinex_db::integrity::{checkpoint_verification, IntegrityTestConfig, IntegrityTester};
use sinex_db::validation::CheckpointInconsistencyType;
use sinex_events::EventFactory;
use std::collections::HashMap;

// Import test macros
use crate::test_checkpoint_flow;
use crate::test_batch_events;
use crate::parameterized_test;
use crate::test_event_flow;

// =============================================================================
// SIMPLE CHECKPOINT TESTS - Using Macros
// =============================================================================

// Basic checkpoint flow tests
test_checkpoint_flow!(
    test_basic_checkpoint_creation,
    "test-automaton-basic",
    0,
    100
);

test_checkpoint_flow!(
    test_checkpoint_update_progress,
    "test-automaton-progress",
    50,
    150
);

test_checkpoint_flow!(
    test_checkpoint_reset,
    "test-automaton-reset",
    1000,
    0
);

// Parameterized checkpoint state tests
parameterized_test!(
    test_checkpoint_states_validation,
    vec![
        ("Empty", ("empty-automaton", 0, None, json!({}))),
        ("Started", ("started-automaton", 10, Some("event-10"), json!({"status": "started"}))),
        ("Processing", ("processing-automaton", 500, Some("event-500"), json!({"status": "processing", "batch": 5}))),
        ("Completed", ("completed-automaton", 1000, Some("event-1000"), json!({"status": "completed"}))),
    ],
    |pool, (automaton, count, last_id, state)| async move {
        // Create checkpoint with specific state
        let mut builder = TestCheckpointBuilder::new(automaton)
            .with_processed_count(count)
            .with_state(state.clone());
        
        if let Some(id) = last_id {
            builder = builder.with_last_processed(id);
        }
        
        builder.insert(pool).await?;
        
        // Verify checkpoint was created correctly
        let checkpoint = TestQueries::get_checkpoint(pool, automaton)
            .await?
            .expect("Checkpoint should exist");
        
        assert_eq!(checkpoint.processed_count, count);
        assert_eq!(checkpoint.last_processed_id, last_id.map(String::from));
        assert_eq!(checkpoint.state_data, Some(state));
        
        Ok(())
    }
);

// Event to checkpoint flow tests
test_event_flow!(
    test_event_to_checkpoint_basic,
    "test-source",
    "test.event",
    "test-processor"
);

test_event_flow!(
    test_filesystem_checkpoint_flow,
    "fs",
    "file.created",
    "fs-processor"
);

test_event_flow!(
    test_command_checkpoint_flow,
    "shell",
    "command.executed",
    "command-processor"
);

// Batch events with checkpoint verification
test_batch_events!(
    test_batch_with_checkpoint,
    "batch-test",
    "batch.event",
    20,
    |pool, events| async move {
        // Create checkpoint for batch
        TestCheckpointBuilder::new("batch-processor")
            .with_processed_count(events.len() as i64)
            .with_last_processed(&events.last().unwrap().id.to_string())
            .insert(pool)
            .await?;
        
        // Verify checkpoint
        let checkpoint = TestQueries::get_checkpoint(pool, "batch-processor")
            .await?
            .expect("Checkpoint should exist");
        
        assert_eq!(checkpoint.processed_count, 20);
        Ok(())
    }
);

// =============================================================================
// COMPLEX CONSISTENCY TESTS - Direct Implementation
// =============================================================================

#[sinex_test]
async fn test_checkpoint_consistency_validation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton
    let automaton_name = format!("test_automaton_{}", Ulid::new());

    // processor_manifests table doesn't have query builders yet
    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
         VALUES ($1, 'automaton', '1.0.0', 'test-host')",
        automaton_name
    )
    .execute(&pool)
    .await?;

    // Insert some test events
    let mut event_ulids = Vec::new();
    for i in 0..10 {
        let event = TestEventBuilder::new("test.checkpoint", "consistency_test")
            .with_field("sequence", json!(i))
            .insert(&pool)
            .await?;
        event_ulids.push(event.id);

        // Small delay between events
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Create initial checkpoint pointing to the 5th event
    let checkpoint_ulid = event_ulids[4];
    TestCheckpointBuilder::new(&automaton_name)
        .with_last_processed(&checkpoint_ulid.to_string())
        .with_processed_count(5)
        .with_state(json!({"processed": 5}))
        .insert(&pool)
        .await?;

    // Test checkpoint consistency verification
    let issues =
        checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &automaton_name)
            .await?;

    println!(
        "Checkpoint consistency issues for {}: {}",
        automaton_name,
        issues.len()
    );
    for issue in &issues {
        println!("  - {}", issue);
    }

    // Should detect that there are newer events that haven't been processed
    assert!(!issues.is_empty(), "Should detect unprocessed events");
    assert!(
        issues
            .iter()
            .any(|issue| issue.contains("not updated") || issue.contains("behind")),
        "Should detect processing lag"
    );

    // Cleanup - still need raw SQL for processor_manifests
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
        automaton_name
    )
    .execute(&pool)
    .await?;
    // processor_manifests table doesn't have query builders yet
    sqlx::query!(
        "DELETE FROM core.processor_manifests WHERE processor_name = $1",
        automaton_name
    )
    .execute(&pool)
    .await?;
    TestQueries::cleanup_test_events(&pool).await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_gap_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // This test requires complex timing and state manipulation
    // that would be obscured by macros
    
    // Create test automaton
    let automaton_name = format!("gap_test_automaton_{}", Ulid::new());

    // processor_manifests table doesn't have query builders yet
    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
         VALUES ($1, 'automaton', '1.0.0', 'test-host')",
        automaton_name
    )
    .execute(&pool)
    .await?;

    // Insert events in two batches with a gap
    let mut batch1_events = Vec::new();
    for i in 0..5 {
        let event = TestEventBuilder::new("test.gap_detection", "batch1")
            .with_field("batch", json!(1))
            .with_field("sequence", json!(i))
            .insert(&pool)
            .await?;
        batch1_events.push(event.id);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Create checkpoint at end of batch1 - need raw SQL for last_activity manipulation
    let last_batch1_ulid = *batch1_events.last().unwrap();
    // This requires raw SQL to set specific last_activity time
    sqlx::query!(
        r#"
        INSERT INTO core.automaton_checkpoints 
        (automaton_name, last_processed_id, processed_count, last_activity, state_data, consumer_group, consumer_name)
        VALUES ($1, $2::text, 5, NOW() - INTERVAL '2 hours', '{"batch1_complete": true}'::jsonb, $3, $4)
        "#,
        automaton_name,
        last_batch1_ulid.to_string(),
        format!("{}-group", automaton_name),
        format!("{}-consumer", automaton_name)
    )
    .execute(&pool)
    .await?;

    // Wait a bit and insert batch2 (simulating gap)
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut batch2_events = Vec::new();
    for i in 0..8 {
        let event = TestEventBuilder::new("test.gap_detection", "batch2")
            .with_field("batch", json!(2))
            .with_field("sequence", json!(i))
            .insert(&pool)
            .await?;
        batch2_events.push(event.id);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Run integrity check to detect gap
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 100,
        check_window_hours: 1,
        include_deep_validation: true,
        validate_checkpoints: true,
        validate_ulid_ordering: false,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    println!("Gap detection results:");
    println!(
        "  Checkpoint inconsistencies: {}",
        results.check_report.checkpoint_inconsistencies.len()
    );

    // Should detect that checkpoint is behind current events
    let checkpoint_issues: Vec<_> = results
        .check_report
        .checkpoint_inconsistencies
        .iter()
        .filter(|inc| inc.automaton_name == automaton_name)
        .collect();

    assert!(
        !checkpoint_issues.is_empty(),
        "Should detect checkpoint inconsistencies"
    );

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
        automaton_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!(
        "DELETE FROM core.processor_manifests WHERE processor_name = $1",
        automaton_name
    )
    .execute(&pool)
    .await?;
    TestQueries::cleanup_test_events(&pool).await?;

    Ok(())
}