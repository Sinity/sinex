//! Example of a fully refactored test file using new patterns
//!
//! This file demonstrates best practices for writing tests with:
//! - Query builders instead of raw SQL
//! - Test builders for data creation
//! - Test macros for common patterns
//! - Proper abstractions and no repetition

use crate::common::prelude::*;
use crate::common::query_helpers::{TestQueries, CheckpointRecord};
use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder, TestEvents, 
                               BatchEventBuilder, TestScenarioBuilder};
use crate::common::test_macros::*;
use chrono::{Duration, Utc};
use sinex_ulid::Ulid;

// Use test macros for simple cases
test_event_insertion!(
    test_basic_filesystem_event,
    "fs",
    "file.created",
    json!({"path": "/test.txt", "size": 1024})
);

test_batch_events!(
    test_bulk_shell_commands,
    "shell",
    "command.executed",
    50,
    |pool, events| async move {
        // Custom verification
        let count = TestQueries::count_events_by_source(pool, "shell").await?;
        assert_eq!(count, 50);
        Ok(())
    }
);

test_checkpoint_flow!(
    test_automaton_progress,
    "file-processor",
    0,  // initial count
    100 // updated count
);

// More complex tests use builders directly
#[sinex_test]
async fn test_event_processing_pipeline(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Setup: Create test scenario
    let scenario = TestScenarioBuilder::new()
        .with_events_from_source("fs", "file.created", 10)
        .with_events_from_source("fs", "file.modified", 5)
        .with_checkpoint(
            TestCheckpointBuilder::new("fs-processor")
                .with_processed_count(15)
                .with_state(json!({
                    "last_scan": Utc::now(),
                    "total_files": 15
                }))
        )
        .execute(&pool)
        .await?;

    // Verify scenario execution
    assert_eq!(scenario.event_count, 15);

    // Query and verify events
    let fs_events = TestQueries::get_events_by_source(&pool, "fs", None).await?;
    assert_eq!(fs_events.len(), 15);

    // Verify checkpoint
    let checkpoint = TestQueries::get_checkpoint(&pool, "fs-processor")
        .await?
        .expect("Checkpoint should exist");
    
    assert_eq!(checkpoint.processed_count, 15);
    assert!(checkpoint.state_data.is_some());

    Ok(())
}

#[sinex_test]
async fn test_time_based_event_analysis(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let base_time = Utc::now() - Duration::hours(24);

    // Create events distributed over 24 hours
    let events = BatchEventBuilder::new("monitoring", "metric.recorded", 24)
        .with_start_time(base_time)
        .with_time_spacing(Duration::hours(1))
        .with_payload_generator(|hour| json!({
            "metric": "cpu_usage",
            "value": 50.0 + (hour as f64 * 2.0),
            "hour": hour
        }))
        .insert(&pool)
        .await?;

    // Query events from last 12 hours
    let recent_start = Utc::now() - Duration::hours(12);
    let recent_events = TestQueries::get_events_in_range(
        &pool, 
        recent_start, 
        Utc::now(), 
        None
    ).await?;

    // Should get approximately 12 events (depending on exact timing)
    assert!(recent_events.len() >= 11 && recent_events.len() <= 13);

    // Verify they're all from the recent period
    for event in &recent_events {
        let ts = event.ts_orig.unwrap_or(event.ts_ingest);
        assert!(ts > recent_start);
    }

    Ok(())
}

#[sinex_test]
async fn test_event_relationships(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create source events
    let file1 = TestEvents::filesystem("/source1.txt").insert(&pool).await?;
    let file2 = TestEvents::filesystem("/source2.txt").insert(&pool).await?;
    
    // Create derived event
    let merged = TestEventBuilder::new("fs", "files.merged")
        .with_field("result_path", json!("/merged.txt"))
        .with_field("source_files", json!(["/source1.txt", "/source2.txt"]))
        .with_source_events(vec![file1.id, file2.id])
        .insert(&pool)
        .await?;

    // Verify relationships
    assert_eq!(merged.source_event_ids, Some(vec![file1.id, file2.id]));

    // Query the merged event
    let retrieved = TestQueries::get_event(&pool, merged.id).await?;
    assert_eq!(retrieved.source_event_ids, Some(vec![file1.id, file2.id]));

    Ok(())
}

#[sinex_test]
async fn test_concurrent_checkpoint_updates(ctx: TestContext) -> TestResult {
    let pool = Arc::new(ctx.pool());
    let automaton_name = "concurrent-test";

    // Initial checkpoint
    TestCheckpointBuilder::new(automaton_name)
        .with_processed_count(0)
        .insert(&pool)
        .await?;

    // Simulate concurrent updates
    let mut handles = vec![];
    for i in 0..5 {
        let pool_clone = pool.clone();
        let automaton = automaton_name.to_string();
        
        let handle = tokio::spawn(async move {
            // Each task processes 10 events
            let start = i * 10;
            let end = start + 10;
            
            // Simulate processing
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            
            // Update checkpoint
            TestCheckpointBuilder::new(&automaton)
                .with_processed_count(end)
                .with_last_processed(&format!("event_{}", end - 1))
                .insert(&pool_clone)
                .await
        });
        
        handles.push(handle);
    }

    // Wait for all updates
    futures::future::try_join_all(handles).await?;

    // Verify final state
    let final_checkpoint = TestQueries::get_checkpoint(&pool, automaton_name)
        .await?
        .expect("Checkpoint should exist");

    // One of the concurrent updates should have won
    // The processed count should be one of: 10, 20, 30, 40, or 50
    assert!(final_checkpoint.processed_count % 10 == 0);
    assert!(final_checkpoint.processed_count >= 10);
    assert!(final_checkpoint.processed_count <= 50);

    Ok(())
}

// Parameterized test example
parameterized_test!(
    test_event_types,
    vec![
        ("filesystem", ("fs", "file.created", json!({"path": "/test.txt"}))),
        ("shell", ("shell", "command.executed", json!({"command": "ls"}))),
        ("clipboard", ("clipboard", "content.changed", json!({"content": "test"}))),
    ],
    |pool, (source, event_type, payload)| async move {
        let event = TestEventBuilder::new(source, event_type)
            .with_payload(payload.clone())
            .insert(pool)
            .await?;
        
        assert_eq!(event.source, source);
        assert_eq!(event.event_type, event_type);
        assert_eq!(event.payload, payload);
        Ok(())
    }
);

// Test with cleanup
test_with_scenario!(
    test_with_proper_cleanup,
    |pool| async move {
        // Setup: Insert test data
        BatchEventBuilder::new("temp", "test.data", 100)
            .insert(pool)
            .await?;
        Ok(())
    },
    |pool, _| async move {
        // Test: Verify data exists
        let count = TestQueries::count_events_by_source(pool, "temp").await?;
        assert_eq!(count, 100);
        Ok(())
    },
    |pool| async move {
        // Cleanup: Remove test data
        use sinex_db::queries::EventQueries;
        EventQueries::delete_by_source("temp".to_string())
            .execute(pool)
            .await?;
        Ok(())
    }
);

#[cfg(test)]
mod performance_tests {
    use super::*;

    #[sinex_test]
    async fn test_batch_performance(ctx: TestContext) -> TestResult {
        let pool = ctx.pool();
        let start = std::time::Instant::now();

        // Insert 1000 events in batches
        for batch in 0..10 {
            BatchEventBuilder::new("perf-test", "batch.event", 100)
                .with_payload_generator(move |i| json!({
                    "batch": batch,
                    "index": i,
                    "data": "x".repeat(100)
                }))
                .insert(&pool)
                .await?;
        }

        let duration = start.elapsed();
        println!("Inserted 1000 events in {:?}", duration);

        // Verify
        let count = TestQueries::count_events_by_source(&pool, "perf-test").await?;
        assert_eq!(count, 1000);

        // Query performance
        let query_start = std::time::Instant::now();
        let _events = TestQueries::get_events_by_source(&pool, "perf-test", Some(100)).await?;
        let query_duration = query_start.elapsed();
        println!("Queried 100 events in {:?}", query_duration);

        Ok(())
    }
}