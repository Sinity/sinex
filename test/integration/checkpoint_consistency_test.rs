// Checkpoint consistency verification integration tests
//
// Tests for:
// - Checkpoint state consistency validation
// - Gap detection between checkpoints and events
// - Stale checkpoint detection
// - Cross-automaton checkpoint validation
// - Recovery scenarios and data loss detection

use sinex_db::integrity::{checkpoint_verification, IntegrityTestConfig, IntegrityTester};
use sinex_db::validation::{CheckpointInconsistency, CheckpointInconsistencyType};
use sinex_events::{event_types, services, EventFactory};
use sinex_test_utils::prelude::*;
use std::collections::HashMap;

#[sinex_test]
async fn test_checkpoint_consistency_validation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton
    let processor_name = format!("test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Test automaton for checkpoint validation')",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Insert some test events
    let mut event_ulids = Vec::new();
    for i in 0..10 {
        let factory = EventFactory::new("test.checkpoint");
        let event = factory.create_event("consistency_test", json!({"sequence": i}));
        let inserted_event = insert_event_with_validator(&pool, &event, None).await?;
        event_ulids.push(inserted_event.id);

        // Small delay between events
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Create initial checkpoint pointing to the 5th event
    let checkpoint_ulid = event_ulids[4];
    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, state_data)
        VALUES ($1, $2, 5, NOW(), '{"processed": 5}'::jsonb)
        "#,
        processor_name,
        checkpoint_ulid.to_string()
    )
    .execute(&pool)
    .await?;

    // Test checkpoint consistency verification
    let issues =
        checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &processor_name)
            .await?;

    println!(
        "Checkpoint consistency issues for {}: {}",
        processor_name,
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

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!(
        "DELETE FROM sinex_schemas.processor_manifests WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.checkpoint'")
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_gap_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton
    let processor_name = format!("gap_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Gap detection test automaton')",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Insert events in two batches with a gap
    let mut batch1_events = Vec::new();
    for i in 0..5 {
        let event = {
            let factory = EventFactory::new("test.gap_detection");
            let event = factory.create_event("batch1", json!({"batch": 1, "sequence": i}));
            sinex_db::insert_event_with_validator(&pool, &event, None).await?
        };
        batch1_events.push(event.id);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Create checkpoint at end of batch1
    let last_batch1_ulid = *batch1_events.last().unwrap();
    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, state_data)
        VALUES ($1, $2, 5, NOW() - INTERVAL '2 hours', '{"batch1_complete": true}'::jsonb)
        "#,
        processor_name,
        last_batch1_ulid.to_string()
    )
    .execute(&pool)
    .await?;

    // Wait a bit and insert batch2 (simulating gap)
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut batch2_events = Vec::new();
    for i in 0..8 {
        let event = {
            let factory = EventFactory::new("test.gap_detection");
            let event = factory.create_event("batch2", json!({"batch": 2, "sequence": i}));
            sinex_db::insert_event_with_validator(&pool, &event, None).await?
        };
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
        .filter(|inc| inc.processor_name == processor_name)
        .collect();

    assert!(
        !checkpoint_issues.is_empty(),
        "Should detect checkpoint inconsistencies"
    );

    for issue in &checkpoint_issues {
        println!(
            "  - {}: {} (type: {:?})",
            issue.processor_name, issue.details, issue.inconsistency_type
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

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!(
        "DELETE FROM sinex_schemas.processor_manifests WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.gap_detection'")
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_stale_checkpoint_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton
    let processor_name = format!("stale_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Stale checkpoint test automaton')",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Insert a single event
    let factory = EventFactory::new("test.stale_checkpoint");
    let raw_event = factory.create_event("stale_test", json!({"data": "test"}));
    let event = sinex_db::insert_event_with_validator(&pool, &raw_event, None).await?;

    // Create checkpoint with old timestamp (3 hours ago)
    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, state_data)
        VALUES ($1, $2, 1, NOW() - INTERVAL '3 hours', '{"stale": true}'::jsonb)
        "#,
        processor_name,
        event.id.to_string()
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
            inc.processor_name == processor_name
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

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!(
        "DELETE FROM sinex_schemas.processor_manifests WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.stale_checkpoint'")
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_cross_automaton_checkpoint_validation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create multiple test automatons
    let processor_names: Vec<String> = (0..3)
        .map(|i| format!("cross_test_automaton_{}_{}", i, Ulid::new()))
        .collect();

    for name in &processor_names {
        sqlx::query!(
            "INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version, description)
             VALUES ($1, 'automaton', '1.0.0', 'Cross-validation test automaton')",
            name
        )
        .execute(&pool)
        .await?;
    }

    // Insert events for cross-validation
    let mut shared_events = Vec::new();
    for i in 0..15 {
        let factory = EventFactory::new("test.cross_validation");
        let event = factory.create_event("shared_event", json!({"sequence": i}));
        let inserted_event = insert_event_with_validator(&pool, &event, None).await?;
        shared_events.push(inserted_event.id);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Create checkpoints for automatons at different points
    let checkpoint_configs = vec![
        (&processor_names[0], 5, "NOW()"), // Up to date
        (&processor_names[1], 10, "NOW() - INTERVAL '1 hour'"), // Behind but recent
        (&processor_names[2], 3, "NOW() - INTERVAL '4 hours'"), // Far behind and stale
    ];

    for (name, processed_count, last_activity) in checkpoint_configs {
        let checkpoint_ulid = shared_events[processed_count - 1];

        sqlx::query(&format!(
            r#"
            INSERT INTO core.processor_checkpoints 
            (processor_name, last_processed_id, processed_count, last_activity, state_data)
            VALUES ($1, $2, $3, {}, '{{"checkpoint_test": true}}'::jsonb)
            "#,
            last_activity
        ))
        .bind(name)
        .bind(checkpoint_ulid.to_string())
        .bind(processed_count as i64)
        .execute(&pool)
        .await?;
    }

    // Get expected automatons for validation
    let expected_automatons = checkpoint_verification::get_expected_automatons(&pool).await?;
    println!("Expected automatons: {}", expected_automatons.len());

    // All test automatons should be in the expected list
    for name in &processor_names {
        assert!(
            expected_automatons.contains(name),
            "Test automaton {} should be in expected list",
            name
        );
    }

    // Validate each automaton's checkpoint consistency
    let mut all_issues = HashMap::new();

    for name in &processor_names {
        let issues =
            checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, name).await?;
        println!("Automaton {}: {} issues", name, issues.len());
        for issue in &issues {
            println!("  - {}", issue);
        }
        all_issues.insert(name.clone(), issues);
    }

    // Verify expected patterns
    let automaton0_issues = all_issues.get(&processor_names[0]).unwrap();
    let automaton1_issues = all_issues.get(&processor_names[1]).unwrap();
    let automaton2_issues = all_issues.get(&processor_names[2]).unwrap();

    // Automaton 0 (up to date) should have fewest issues
    println!(
        "Issues count - Automaton 0: {}, 1: {}, 2: {}",
        automaton0_issues.len(),
        automaton1_issues.len(),
        automaton2_issues.len()
    );

    // Automaton 2 (far behind and stale) should have most issues
    assert!(
        automaton2_issues.len() >= automaton0_issues.len(),
        "Far behind automaton should have more issues"
    );
    assert!(
        automaton2_issues.len() >= automaton1_issues.len(),
        "Stale automaton should have more issues"
    );

    // Run comprehensive integrity check
    let integrity_tester = IntegrityTester::new(&pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 100,
        check_window_hours: 24,
        include_deep_validation: true,
        validate_checkpoints: true,
        validate_ulid_ordering: false,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    // Should detect issues across all automatons
    let cross_validation_issues: Vec<_> = results
        .check_report
        .checkpoint_inconsistencies
        .iter()
        .filter(|inc| processor_names.contains(&inc.processor_name))
        .collect();

    println!(
        "Cross-validation detected {} checkpoint issues",
        cross_validation_issues.len()
    );

    // Should detect different types of issues
    let issue_types: std::collections::HashSet<_> = cross_validation_issues
        .iter()
        .map(|issue| &issue.inconsistency_type)
        .collect();

    println!("Detected issue types: {:?}", issue_types);
    assert!(
        !issue_types.is_empty(),
        "Should detect various checkpoint inconsistency types"
    );

    // Cleanup
    for name in &processor_names {
        sqlx::query!(
            "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
            name
        )
        .execute(&pool)
        .await?;
        sqlx::query!(
            "DELETE FROM sinex_schemas.processor_manifests WHERE processor_name = $1",
            name
        )
        .execute(&pool)
        .await?;
    }
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.cross_validation'")
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_recovery_scenarios(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton for recovery scenarios
    let processor_name = format!("recovery_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Recovery scenario test automaton')",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Test Scenario 1: Checkpoint references non-existent event
    let non_existent_ulid = Ulid::new(); // Generate random ULID that doesn't exist in events

    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, state_data)
        VALUES ($1, $2, 100, NOW(), '{"scenario": "non_existent_event"}'::jsonb)
        "#,
        processor_name,
        non_existent_ulid.to_string()
    )
    .execute(&pool)
    .await?;

    // Verify this scenario is detected
    let issues =
        checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &processor_name)
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
    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Test Scenario 2: Checkpoint with invalid ULID format
    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, state_data)
        VALUES ($1, 'invalid-ulid-format', 50, NOW(), '{"scenario": "invalid_ulid"}'::jsonb)
        "#,
        processor_name
    )
    .execute(&pool)
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
            inc.processor_name == processor_name
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
    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Test Scenario 3: Missing checkpoint (automaton exists but no checkpoint)
    // Just check that no checkpoint exists - this should be detected by the automaton list check
    let expected_automatons = checkpoint_verification::get_expected_automatons(&pool).await?;
    assert!(
        expected_automatons.contains(&processor_name),
        "Test automaton should be in expected list"
    );

    let missing_checkpoint_issues =
        checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &processor_name)
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
    let factory = EventFactory::new("test.recovery");
    let event = factory.create_event("future_reference", json!({"scenario": "future_reference"}));
    let future_event = insert_event_with_validator(&pool, &event, None).await?;

    // Create a fake "future" ULID by modifying timestamp
    let future_timestamp = Utc::now() + ChronoDuration::hours(1);
    let future_ulid = Ulid::from_datetime(future_timestamp);

    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, state_data)
        VALUES ($1, $2, 999, NOW(), '{"scenario": "future_checkpoint"}'::jsonb)
        "#,
        processor_name,
        future_ulid.to_string()
    )
    .execute(&pool)
    .await?;

    // This scenario should be detected as checkpoint ahead of events
    let future_issues =
        checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &processor_name)
            .await?;
    assert!(
        !future_issues.is_empty(),
        "Should detect checkpoint issues with future ULID"
    );

    println!(
        "Recovery scenarios test completed. Found {} total integrity patterns",
        future_issues.len()
    );

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!(
        "DELETE FROM sinex_schemas.processor_manifests WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.recovery'")
        .execute(&pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_data_loss_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create test automaton
    let processor_name = format!("data_loss_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO sinex_schemas.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Data loss detection test automaton')",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Create a sequence of events
    let mut event_sequence = Vec::new();
    for i in 0..20 {
        let event = {
            let factory = EventFactory::new("test.data_loss");
            let event = factory.create_event("sequence_event", json!({"sequence": i}));
            sinex_db::insert_event_with_validator(&pool, &event, None).await?
        };
        event_sequence.push(event.id);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Simulate data loss scenario: checkpoint jumps from event 5 to event 15
    // This suggests events 6-14 were "processed" but there's a gap
    let checkpoint_ulid = event_sequence[14]; // 15th event (0-indexed)

    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, state_data)
        VALUES ($1, $2, 15, NOW() - INTERVAL '30 minutes', '{"simulated_jump": true}'::jsonb)
        "#,
        processor_name,
        checkpoint_ulid.to_string()
    )
    .execute(&pool)
    .await?;

    // Now add more events after the checkpoint
    for i in 20..25 {
        let event = {
            let factory = EventFactory::new("test.data_loss");
            let event = factory.create_event("post_checkpoint_event", json!({"sequence": i}));
            sinex_db::insert_event_with_validator(&pool, &event, None).await?
        };
        event_sequence.push(event.id);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

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
        .filter(|inc| inc.processor_name == processor_name)
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

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!(
        "DELETE FROM sinex_schemas.processor_manifests WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.data_loss'")
        .execute(&pool)
        .await?;

    Ok(())
}
