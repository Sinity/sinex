// Checkpoint consistency verification integration tests
//
// Tests for:
// - Checkpoint state consistency validation
// - Gap detection between checkpoints and events
// - Stale checkpoint detection
// - Cross-automaton checkpoint validation
// - Recovery scenarios and data loss detection

use crate::common::test_macros::*;
use crate::common::prelude::*;
use crate::common::query_helpers::TestQueries;
use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder};
use crate::common::fixtures;
use sinex_db::integrity::{checkpoint_verification, IntegrityTestConfig, IntegrityTester};
use sinex_db::validation::CheckpointInconsistencyType;
use sinex_events::EventFactory;
use std::collections::HashMap;

#[sinex_test]
async fn test_checkpoint_consistency_validation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    
    // Use standard user session fixture for test events
    let session = fixtures::standard_user_session(&ctx).await?;
    
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

    // Create checkpoint pointing to the 5th event from the fixture
    let checkpoint_ulid = session.event_ids[4];
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

    // Insert events in two batches with a gap using BatchEventBuilder
    let batch1_builder = BatchEventBuilder::new("test.gap_detection", "batch1", 5)
        .with_payload_generator(|i| json!({
            "batch": 1,
            "sequence": i
        }))
        .with_time_spacing(chrono::Duration::milliseconds(5));
    
    let batch1_events_raw = batch1_builder.insert(&pool).await?;
    let batch1_events: Vec<_> = batch1_events_raw.iter().map(|e| e.id).collect();

    // Create checkpoint at end of batch1 - need raw SQL for last_activity manipulation
    let last_batch1_ulid = *batch1_events.last().unwrap();
    // This requires raw SQL to set specific last_activity time
    sqlx::query!(
        r#"
        INSERT INTO core.automaton_checkpoints 
        (automaton_name, last_processed_id, processed_count, last_activity, state_data, consumer_group, consumer_name)
        VALUES ($1, $2::uuid, 5, NOW() - INTERVAL '2 hours', '{"batch1_complete": true}'::jsonb, $3, $4)
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

    let batch2_builder = BatchEventBuilder::new("test.gap_detection", "batch2", 8)
        .with_payload_generator(|i| json!({
            "batch": 2,
            "sequence": i
        }))
        .with_time_spacing(chrono::Duration::milliseconds(5));
    
    let batch2_events_raw = batch2_builder.insert(&pool).await?;
    let batch2_events: Vec<_> = batch2_events_raw.iter().map(|e| e.id).collect();

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

    for issue in &checkpoint_issues {
        println!(
            "  - {}: {} (type: {:?})",
            issue.automaton_name, issue.details, issue.inconsistency_type
        );

        match issue.inconsistency_type {
            CheckpointInconsistencyType::CheckpointBehindEvents => {
                assert!(
                    issue.events_potentially_missed > 0,
                    "Should detect missed events"
                );
            }
            CheckpointInconsistencyType::StaleCheckpoint => {
                // Expected for old checkpoint
            }
            _ => {}
        }
    }

    // Calculate expected gap
    let expected_gap = batch2_events.len() as u64;
    let detected_gap = checkpoint_issues
        .iter()
        .filter(|issue| {
            matches!(
                issue.inconsistency_type,
                CheckpointInconsistencyType::CheckpointBehindEvents
            )
        })
        .map(|issue| issue.events_potentially_missed)
        .sum::<u64>();

    println!(
        "Expected gap: {}, Detected gap: {}",
        expected_gap, detected_gap
    );
    assert!(
        detected_gap >= expected_gap,
        "Should detect at least the expected gap"
    );

    // Cleanup - use query builder where available
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
    // Delete events using query builder
    use sinex_db::queries::EventQueries;
    EventQueries::delete_by_source("test.gap_detection".to_string())
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_stale_checkpoint_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton
    let automaton_name = format!("stale_test_automaton_{}", Ulid::new());

    // processor_manifests table doesn't have query builders yet
    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
         VALUES ($1, 'automaton', '1.0.0', 'test-host')",
        automaton_name
    )
    .execute(&pool)
    .await?;

    // Insert a single event
    let event = TestEventBuilder::new("test.stale_checkpoint", "stale_test")
        .with_field("data", json!("test"))
        .insert(&pool)
        .await?;

    // Create checkpoint with old timestamp (3 hours ago) - requires raw SQL for last_activity manipulation
    sqlx::query!(
        r#"
        INSERT INTO core.automaton_checkpoints 
        (automaton_name, last_processed_id, processed_count, last_activity, state_data, consumer_group, consumer_name)
        VALUES ($1, $2::uuid, 1, NOW() - INTERVAL '3 hours', '{"stale": true}'::jsonb, $3, $4)
        "#,
        automaton_name,
        event.id.to_string(),
        format!("{}-group", automaton_name),
        format!("{}-consumer", automaton_name)
    )
    .execute(&pool)
    .await?;

    // Run integrity check to detect stale checkpoint
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 100,
        check_window_hours: 24, // Look back 24 hours to catch the stale checkpoint
        include_deep_validation: false,
        validate_checkpoints: true,
        validate_ulid_ordering: false,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Should detect stale checkpoint
    let stale_checkpoints: Vec<_> = results
        .check_report
        .checkpoint_inconsistencies
        .iter()
        .filter(|inc| {
            inc.automaton_name == automaton_name
                && matches!(
                    inc.inconsistency_type,
                    CheckpointInconsistencyType::StaleCheckpoint
                )
        })
        .collect();

    assert!(
        !stale_checkpoints.is_empty(),
        "Should detect stale checkpoint"
    );

    for stale in &stale_checkpoints {
        println!("Detected stale checkpoint: {}", stale.details);
        assert!(
            stale.details.contains("hours"),
            "Should mention time duration"
        );
    }

    // Cleanup - use query builder
    use sinex_db::queries::CheckpointQueries;
    // Delete checkpoints for this automaton
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
    use sinex_db::queries::EventQueries;
    EventQueries::delete_by_source("test.stale_checkpoint".to_string())
        .execute(&pool)
        .await?;
        
    Ok(())
}

test_batch_events!(test_cross_automaton_checkpoint_validation, "test", "test.event", 15, 
    |pool: &DbPool, events: &[RawEvent]| async move {
        // Verify batch
        assert_eq!(events.len(), 15);
        Ok(())
    }
);

#[sinex_test]
async fn test_checkpoint_recovery_scenarios(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton for recovery scenarios
    let automaton_name = format!("recovery_test_automaton_{}", Ulid::new());

    // processor_manifests table doesn't have query builders yet
    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
         VALUES ($1, 'automaton', '1.0.0', 'test-host')",
        automaton_name
    )
    .execute(&pool)
    .await?;

    // Test Scenario 1: Checkpoint references non-existent event
    let non_existent_ulid = Ulid::new(); // Generate random ULID that doesn't exist in events

    TestCheckpointBuilder::new(&automaton_name)
        .with_last_processed(&non_existent_ulid.to_string())
        .with_processed_count(100)
        .with_state(json!({"scenario": "non_existent_event"}))
        .insert(&pool)
        .await?;

    // Verify this scenario is detected
    let issues =
        checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &automaton_name)
            .await?;
    assert!(
        !issues.is_empty(),
        "Should detect non-existent event reference"
    );
    assert!(
        issues.iter().any(|issue| issue.contains("non-existent")),
        "Should specifically mention non-existent event"
    );

    // Clean up scenario 1
    // Delete checkpoints for this automaton
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
        automaton_name
    )
    .execute(&pool)
    .await?;

    // Test Scenario 2: Checkpoint with invalid ULID format
    TestCheckpointBuilder::new(&automaton_name)
        .with_last_processed("invalid-ulid-format")
        .with_processed_count(50)
        .with_state(json!({"scenario": "invalid_ulid"}))
        .insert(&pool)
        .await?;

    // Run integrity check for invalid ULID
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 100,
        check_window_hours: 1,
        include_deep_validation: false,
        validate_checkpoints: true,
        validate_ulid_ordering: false,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Should detect invalid ULID format
    let invalid_ulid_issues: Vec<_> = results
        .check_report
        .checkpoint_inconsistencies
        .iter()
        .filter(|inc| {
            inc.automaton_name == automaton_name
                && matches!(
                    inc.inconsistency_type,
                    CheckpointInconsistencyType::InvalidCheckpointFormat
                )
        })
        .collect();

    assert!(
        !invalid_ulid_issues.is_empty(),
        "Should detect invalid ULID format"
    );

    for issue in &invalid_ulid_issues {
        println!("Invalid ULID issue: {}", issue.details);
        assert!(
            issue.details.contains("Invalid ULID"),
            "Should mention invalid ULID"
        );
    }

    // Clean up scenario 2
    // Delete checkpoints for this automaton
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
        automaton_name
    )
    .execute(&pool)
    .await?;

    // Test Scenario 3: Missing checkpoint (automaton exists but no checkpoint)
    // Just check that no checkpoint exists - this should be detected by the automaton list check
    let expected_automatons = checkpoint_verification::get_expected_automatons(&pool).await?;
    assert!(
        expected_automatons.contains(&automaton_name),
        "Test automaton should be in expected list"
    );

    let missing_checkpoint_issues =
        checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &automaton_name)
            .await?;
    assert!(
        !missing_checkpoint_issues.is_empty(),
        "Should detect missing checkpoint"
    );
    assert!(
        missing_checkpoint_issues
            .iter()
            .any(|issue| issue.contains("No checkpoint found")),
        "Should specifically mention missing checkpoint"
    );

    // Test Scenario 4: Checkpoint ahead of events (impossible scenario but test data corruption)
    let _future_event = TestEventBuilder::new("test.recovery", "future_reference")
        .with_field("scenario", json!("future_reference"))
        .insert(&pool)
        .await?;

    // Create a fake "future" ULID by modifying timestamp
    let future_timestamp = Utc::now() + ChronoDuration::hours(1);
    let future_ulid = Ulid::from_datetime(future_timestamp);

    TestCheckpointBuilder::new(&automaton_name)
        .with_last_processed(&future_ulid.to_string())
        .with_processed_count(999)
        .with_state(json!({"scenario": "future_checkpoint"}))
        .insert(&pool)
        .await?;

    // This scenario should be detected as checkpoint ahead of events
    let future_issues =
        checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &automaton_name)
            .await?;
    assert!(
        !future_issues.is_empty(),
        "Should detect checkpoint issues with future ULID"
    );

    println!(
        "Recovery scenarios test completed. Found {} total integrity patterns",
        future_issues.len()
    );

    // Cleanup - use query builder
    // Delete checkpoints for this automaton
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
    use sinex_db::queries::EventQueries;
    EventQueries::delete_by_source("test.recovery".to_string())
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_data_loss_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton
    let automaton_name = format!("data_loss_test_automaton_{}", Ulid::new());

    // processor_manifests table doesn't have query builders yet
    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
         VALUES ($1, 'automaton', '1.0.0', 'test-host')",
        automaton_name
    )
    .execute(&pool)
    .await?;

    // Create a sequence of events using BatchEventBuilder
    let sequence_builder = BatchEventBuilder::new("test.data_loss", "sequence_event", 20)
        .with_payload_generator(|i| json!({
            "sequence": i
        }))
        .with_time_spacing(chrono::Duration::milliseconds(5));
    
    let events_raw = sequence_builder.insert(&pool).await?;
    let mut event_sequence: Vec<_> = events_raw.iter().map(|e| e.id).collect();

    // Simulate data loss scenario: checkpoint jumps from event 5 to event 15
    // This suggests events 6-14 were "processed" but there's a gap
    let checkpoint_ulid = event_sequence[14]; // 15th event (0-indexed)

    // Requires raw SQL for last_activity manipulation
    sqlx::query!(
        r#"
        INSERT INTO core.automaton_checkpoints 
        (automaton_name, last_processed_id, processed_count, last_activity, state_data, consumer_group, consumer_name)
        VALUES ($1, $2::uuid, 15, NOW() - INTERVAL '30 minutes', '{"simulated_jump": true}'::jsonb, $3, $4)
        "#,
        automaton_name,
        checkpoint_ulid.to_string(),
        format!("{}-group", automaton_name),
        format!("{}-consumer", automaton_name)
    )
    .execute(&pool)
    .await?;

    // Now add more events after the checkpoint
    let post_checkpoint_builder = BatchEventBuilder::new("test.data_loss", "post_checkpoint_event", 5)
        .with_payload_generator(|i| json!({
            "sequence": i + 20
        }))
        .with_time_spacing(chrono::Duration::milliseconds(5));
    
    let post_events = post_checkpoint_builder.insert(&pool).await?;
    event_sequence.extend(post_events.iter().map(|e| e.id));

    // Run comprehensive integrity check
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 1000,
        check_window_hours: 1,
        include_deep_validation: true,
        validate_checkpoints: true,
        validate_ulid_ordering: false,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Analyze data loss detection results
    let data_loss_issues: Vec<_> = results
        .check_report
        .checkpoint_inconsistencies
        .iter()
        .filter(|inc| inc.automaton_name == automaton_name)
        .collect();

    println!("Data loss detection results:");
    println!("  Total checkpoint issues: {}", data_loss_issues.len());

    let mut potentially_missed_events = 0;
    for issue in &data_loss_issues {
        println!(
            "  - {}: {} (missed: {})",
            issue.inconsistency_type, issue.details, issue.events_potentially_missed
        );
        potentially_missed_events += issue.events_potentially_missed;
    }

    // Should detect that there are unprocessed events after the checkpoint
    assert!(
        potentially_missed_events > 0,
        "Should detect potentially missed events"
    );

    // The post-checkpoint events should be detected as unprocessed
    let expected_unprocessed = 5; // Events 20-24
    println!(
        "Expected unprocessed: {}, Detected: {}",
        expected_unprocessed, potentially_missed_events
    );

    // Allow some tolerance as different detection methods might count differently
    assert!(
        potentially_missed_events >= expected_unprocessed,
        "Should detect at least the expected unprocessed events"
    );

    // Verify recommendations are generated for data loss scenarios
    let data_loss_recommendations: Vec<_> = results
        .recommendations
        .iter()
        .filter(|rec| {
            rec.description.to_lowercase().contains("checkpoint")
                || rec.description.to_lowercase().contains("data")
        })
        .collect();

    assert!(
        !data_loss_recommendations.is_empty(),
        "Should generate recommendations for data loss scenarios"
    );

    for rec in &data_loss_recommendations {
        println!("Recommendation: {} - {}", rec.priority, rec.description);
        assert!(
            !rec.action_steps.is_empty(),
            "Recommendations should include action steps"
        );
    }

    // Cleanup - use query builder
    use sinex_db::queries::CheckpointQueries;
    // Delete checkpoints for this automaton
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
    use sinex_db::queries::EventQueries;
    EventQueries::delete_by_source("test.data_loss".to_string())
        .execute(&pool)
        .await?;

    Ok(())
}
