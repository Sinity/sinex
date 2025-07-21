//! Refactored database integration tests using centralized query builders
//!
//! This file demonstrates the new test patterns using:
//! - TestQueries for all database operations
//! - TestEventBuilder for creating test data
//! - No raw SQL queries

use crate::common::prelude::*;
use crate::common::query_helpers::{TestQueries, CheckpointRecord};
use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder, TestEvents, BatchEventBuilder};
use chrono::{Duration, Utc};
use sinex_ulid::Ulid;

/// Test basic event lifecycle using query builders
#[sinex_test]
async fn test_event_lifecycle(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert event using builder
    let event = TestEvents::filesystem("/test/file.txt")
        .with_field("operation", json!("created"))
        .insert(&pool)
        .await?;

    // Retrieve using TestQueries
    let retrieved = TestQueries::get_event(&pool, event.id).await?;
    
    // Verify
    assert_eq!(retrieved.source, "fs");
    assert_eq!(retrieved.event_type, "file.created");
    assert_eq!(retrieved.payload["path"], "/test/file.txt");

    Ok(())
}

/// Test batch event insertion
#[sinex_test]
async fn test_batch_insertion(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert batch of events
    let events = BatchEventBuilder::new("test", "batch.event", 10)
        .with_payload_generator(|i| json!({
            "index": i,
            "data": format!("test-{}", i)
        }))
        .insert(&pool)
        .await?;

    assert_eq!(events.len(), 10);

    // Verify count
    let count = TestQueries::count_events_by_source(&pool, "test").await?;
    assert_eq!(count, 10);

    Ok(())
}

/// Test checkpoint operations
#[sinex_test]
async fn test_checkpoint_operations(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create checkpoint using builder
    TestCheckpointBuilder::new("test-automaton")
        .with_last_processed("1234567890")
        .with_processed_count(42)
        .with_state(json!({"key": "value"}))
        .insert(&pool)
        .await?;

    // Retrieve checkpoint
    let checkpoint = TestQueries::get_checkpoint(&pool, "test-automaton")
        .await?
        .expect("Checkpoint should exist");

    assert_eq!(checkpoint.automaton_name, "test-automaton");
    assert_eq!(checkpoint.processed_count, 42);
    assert_eq!(checkpoint.last_processed_id, Some("1234567890".to_string()));

    Ok(())
}

/// Test event filtering by source
#[sinex_test]
async fn test_event_filtering(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert events from different sources
    for source in ["fs", "shell", "clipboard"] {
        for i in 0..3 {
            TestEventBuilder::new(source, "test.event")
                .with_field("index", json!(i))
                .insert(&pool)
                .await?;
        }
    }

    // Query by source
    let fs_events = TestQueries::get_events_by_source(&pool, "fs", Some(10)).await?;
    let shell_events = TestQueries::get_events_by_source(&pool, "shell", Some(10)).await?;

    assert_eq!(fs_events.len(), 3);
    assert_eq!(shell_events.len(), 3);

    // Verify all fs events
    for event in &fs_events {
        assert_eq!(event.source, "fs");
    }

    Ok(())
}

/// Test time-based event queries
#[sinex_test]
async fn test_time_range_queries(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let now = Utc::now();

    // Insert events at different times
    let events = BatchEventBuilder::new("timed", "test.event", 5)
        .with_start_time(now - Duration::hours(2))
        .with_time_spacing(Duration::minutes(30))
        .insert(&pool)
        .await?;

    // Query specific time range
    let start = now - Duration::hours(1) - Duration::minutes(15);
    let end = now - Duration::minutes(15);
    
    let range_events = TestQueries::get_events_in_range(&pool, start, end, None).await?;
    
    // Should get events from -75min, -45min (2 events)
    assert_eq!(range_events.len(), 2);

    Ok(())
}

/// Test concurrent event insertion
#[sinex_test]
async fn test_concurrent_insertion(ctx: TestContext) -> TestResult {
    let pool = Arc::new(ctx.pool());

    // Spawn concurrent tasks
    let mut handles = vec![];
    
    for i in 0..5 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            TestEvents::shell_command(&format!("command-{}", i))
                .with_field("task_id", json!(i))
                .insert(&pool_clone)
                .await
        });
        handles.push(handle);
    }

    // Wait for all tasks
    let results: Vec<_> = futures::future::try_join_all(handles).await?;
    
    // Verify all succeeded
    for result in results {
        assert!(result.is_ok());
    }

    // Verify count
    let count = TestQueries::count_events_by_source(&pool, "shell").await?;
    assert_eq!(count, 5);

    Ok(())
}

/// Test event cleanup
#[sinex_test]
async fn test_cleanup_operations(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert test events
    for i in 0..5 {
        TestEventBuilder::new("test-cleanup", "test.event")
            .with_field("index", json!(i))
            .insert(&pool)
            .await?;
    }

    // Verify they exist
    let count_before = TestQueries::count_events_by_source(&pool, "test-cleanup").await?;
    assert_eq!(count_before, 5);

    // Clean up test events
    TestQueries::cleanup_test_events(&pool).await?;

    // Verify cleanup (only affects sources starting with "test")
    let count_after = TestQueries::count_events_by_source(&pool, "test-cleanup").await?;
    assert_eq!(count_after, 0);

    Ok(())
}

/// Test checkpoint updates
#[sinex_test]
async fn test_checkpoint_updates(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let automaton_name = "update-test";

    // Initial checkpoint
    TestCheckpointBuilder::new(automaton_name)
        .with_processed_count(10)
        .insert(&pool)
        .await?;

    // Update checkpoint
    TestCheckpointBuilder::new(automaton_name)
        .with_processed_count(20)
        .with_last_processed("new-id")
        .insert(&pool)
        .await?;

    // Verify update
    let checkpoint = TestQueries::get_checkpoint(&pool, automaton_name)
        .await?
        .expect("Checkpoint should exist");

    assert_eq!(checkpoint.processed_count, 20);
    assert_eq!(checkpoint.last_processed_id, Some("new-id".to_string()));

    Ok(())
}

/// Test recent events query
#[sinex_test]
async fn test_recent_events(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Insert events with slight delays to ensure ordering
    let mut event_ids = vec![];
    for i in 0..5 {
        let event = TestEvents::minimal()
            .with_field("sequence", json!(i))
            .insert(&pool)
            .await?;
        event_ids.push(event.id);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }

    // Get recent events
    let recent = TestQueries::get_recent_events(&pool, 3).await?;
    
    assert_eq!(recent.len(), 3);
    
    // Verify they are the most recent (reverse order)
    assert_eq!(recent[0].payload["sequence"], 4);
    assert_eq!(recent[1].payload["sequence"], 3);
    assert_eq!(recent[2].payload["sequence"], 2);

    Ok(())
}

/// Test event relationships using source_event_ids
#[sinex_test]
async fn test_event_relationships(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create parent events
    let parent1 = TestEvents::filesystem("/parent1.txt").insert(&pool).await?;
    let parent2 = TestEvents::filesystem("/parent2.txt").insert(&pool).await?;

    // Create child event with parent relationships
    let child = TestEventBuilder::new("fs", "file.merged")
        .with_field("path", json!("/merged.txt"))
        .with_source_events(vec![parent1.id, parent2.id])
        .insert(&pool)
        .await?;

    // Verify relationships
    assert_eq!(child.source_event_ids, Some(vec![parent1.id, parent2.id]));

    Ok(())
}

#[cfg(test)]
mod builder_pattern_tests {
    use super::*;
    use crate::common::builders::TestScenarioBuilder;

    /// Test complex scenario using scenario builder
    #[sinex_test]
    async fn test_scenario_builder(ctx: TestContext) -> TestResult {
        let pool = ctx.pool();

        // Build complex scenario
        let result = TestScenarioBuilder::new()
            .with_events_from_source("fs", "file.created", 5)
            .with_events_from_source("shell", "command.executed", 3)
            .with_checkpoint(
                TestCheckpointBuilder::new("file-processor")
                    .with_processed_count(5)
            )
            .with_checkpoint(
                TestCheckpointBuilder::new("command-processor")
                    .with_processed_count(3)
            )
            .execute(&pool)
            .await?;

        assert_eq!(result.event_count, 8);

        // Verify checkpoints
        let file_checkpoint = TestQueries::get_checkpoint(&pool, "file-processor").await?;
        let cmd_checkpoint = TestQueries::get_checkpoint(&pool, "command-processor").await?;

        assert!(file_checkpoint.is_some());
        assert!(cmd_checkpoint.is_some());

        Ok(())
    }
}