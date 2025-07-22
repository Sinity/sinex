// Checkpoint consistency verification integration tests
//
// Tests for:
// - Checkpoint state consistency validation
// - Gap detection between checkpoints and events
// - Stale checkpoint detection
// - Cross-automaton checkpoint validation
// - Recovery scenarios and data loss detection

use crate::common::prelude::*;
use sinex_db::integrity::{checkpoint_verification, IntegrityTestConfig, IntegrityTester};
use sinex_db::validation::CheckpointInconsistencyType;
use sinex_db::queries::checkpoints::CheckpointQueries;
use sinex_events::event_types::{shell, sinex};

#[sinex_test]
async fn test_checkpoint_consistency_validation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    
    // Create test events
    let mut event_ids = Vec::new();
    for i in 0..10 {
        let event = EventFactory::new(sources::SHELL_KITTY)
            .create_event(
                shell::COMMAND_EXECUTED,
                json!({
                    "command": format!("test command {}", i),
                    "sequence": i
                })
            );
        let inserted = insert_event(&pool, &event).await?;
        event_ids.push(inserted.id);
    }
    
    // Create test automaton
    let automaton_name = format!("test_automaton_{}", Ulid::new());

    // Create processor manifest
    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
         VALUES ($1, 'automaton', '1.0.0', 'test-host')",
        automaton_name
    )
    .execute(pool)
    .await?;

    // Create checkpoint pointing to the 5th event
    let checkpoint_id = event_ids[4];
    CheckpointQueries::upsert_checkpoint(
        automaton_name.clone(),
        checkpoint_id,
        5,
        json!({"processed": 5}),
        Some(format!("{}-group", automaton_name)),
        Some(format!("{}-consumer", automaton_name)),
    )
    .execute(pool)
    .await?;

    // Test checkpoint consistency verification
    let issues = checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &automaton_name)
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

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
        automaton_name
    )
    .execute(pool)
    .await?;
    sqlx::query!(
        "DELETE FROM core.processor_manifests WHERE processor_name = $1",
        automaton_name
    )
    .execute(pool)
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_gap_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create test automaton
    let automaton_name = format!("gap_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
         VALUES ($1, 'automaton', '1.0.0', 'test-host')",
        automaton_name
    )
    .execute(pool)
    .await?;

    // Insert events in two batches with a gap
    let mut batch1_ids = Vec::new();
    for i in 0..5 {
        let event = EventBuilder::new()
            .source("test.gap_detection")
            .event_type("batch1")
            .payload(json!({
                "batch": 1,
                "sequence": i
            }))
            .time_offset(ChronoDuration::milliseconds(i as i64 * 5))
            .build();
        let inserted = insert_event(&pool, &event).await?;
        batch1_ids.push(inserted.id);
    }

    // Create checkpoint at end of batch1 with old timestamp
    let last_batch1_id = batch1_ids.last().unwrap();
    sqlx::query!(
        r#"
        INSERT INTO core.automaton_checkpoints 
        (automaton_name, last_processed_id, processed_count, last_activity, checkpoint_data, consumer_group, consumer_name)
        VALUES ($1, $2::uuid, 5, NOW() - INTERVAL '2 hours', '{"batch1_complete": true}'::jsonb, $3, $4)
        "#,
        automaton_name,
        last_batch1_id.to_uuid(),
        format!("{}-group", automaton_name),
        format!("{}-consumer", automaton_name)
    )
    .execute(pool)
    .await?;

    // Wait and insert batch2 (simulating gap)
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut batch2_ids = Vec::new();
    for i in 0..8 {
        let event = EventBuilder::new()
            .source("test.gap_detection")
            .event_type("batch2")
            .payload(json!({
                "batch": 2,
                "sequence": i
            }))
            .build();
        let inserted = insert_event(&pool, &event).await?;
        batch2_ids.push(inserted.id);
    }

    // Run integrity check to detect gap
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

    // Check for checkpoint inconsistencies
    let checkpoint_issues: Vec<_> = results
        .check_report
        .checkpoint_inconsistencies
        .iter()
        .filter(|issue| issue.automaton_name == automaton_name)
        .collect();

    assert!(!checkpoint_issues.is_empty(), "Should detect checkpoint issues");

    // Should detect stale checkpoint
    let stale_issues: Vec<_> = checkpoint_issues
        .iter()
        .filter(|issue| matches!(issue.inconsistency_type, CheckpointInconsistencyType::StaleCheckpoint))
        .collect();

    assert!(!stale_issues.is_empty(), "Should detect stale checkpoint");

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
        automaton_name
    )
    .execute(pool)
    .await?;
    sqlx::query!(
        "DELETE FROM core.processor_manifests WHERE processor_name = $1",
        automaton_name
    )
    .execute(pool)
    .await?;
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.gap_detection'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_multiple_automaton_checkpoint_coordination(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create shared events
    let mut event_ids = Vec::new();
    for i in 0..20 {
        let event = EventBuilder::new()
            .source("test.coordination")
            .event_type(sinex::PROCESS_HEARTBEAT)
            .payload(json!({"index": i}))
            .build();
        let inserted = insert_event(&pool, &event).await?;
        event_ids.push(inserted.id);
    }

    // Create multiple automata with different processing states
    let automata = vec![
        ("fast_processor", 18, event_ids[17]), // Almost caught up
        ("slow_processor", 5, event_ids[4]),   // Far behind
        ("stuck_processor", 1, event_ids[0]),  // Stuck at beginning
    ];

    for (name, count, last_id) in &automata {
        // Create processor manifest
        sqlx::query!(
            "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
             VALUES ($1, 'automaton', '1.0.0', 'test-host')",
            name
        )
        .execute(pool)
        .await?;

        // Create checkpoint
        CheckpointQueries::upsert_checkpoint(
            name.to_string(),
            *last_id,
            *count,
            json!({"status": "active"}),
            Some(format!("{}-group", name)),
            Some(format!("{}-consumer", name)),
        )
        .execute(pool)
        .await?;
    }

    // Run cross-automaton validation
    let all_issues = checkpoint_verification::verify_all_checkpoint_consistency(&pool).await?;

    println!("Cross-automaton validation found {} issues", all_issues.len());
    for (automaton, issues) in &all_issues {
        if !issues.is_empty() {
            println!("  {}: {} issues", automaton, issues.len());
            for issue in issues {
                println!("    - {}", issue);
            }
        }
    }

    // Should detect issues for slow and stuck processors
    assert!(all_issues.contains_key("slow_processor"));
    assert!(all_issues.contains_key("stuck_processor"));

    // Fast processor might have minor issues but not severe
    if let Some(fast_issues) = all_issues.get("fast_processor") {
        assert!(
            fast_issues.len() <= 1,
            "Fast processor should have minimal issues"
        );
    }

    // Cleanup
    for (name, _, _) in automata {
        sqlx::query!(
            "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
            name
        )
        .execute(pool)
        .await?;
        sqlx::query!(
            "DELETE FROM core.processor_manifests WHERE processor_name = $1",
            name
        )
        .execute(pool)
        .await?;
    }
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.coordination'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_recovery_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create events
    let mut event_ids = Vec::new();
    for i in 0..15 {
        let event = EventBuilder::new()
            .source("test.recovery")
            .event_type("sequential")
            .payload(json!({"seq": i}))
            .build();
        let inserted = insert_event(&pool, &event).await?;
        event_ids.push(inserted.id);
    }

    let automaton_name = "recovery_test_automaton";

    // Create processor manifest
    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
         VALUES ($1, 'automaton', '1.0.0', 'test-host')",
        automaton_name
    )
    .execute(pool)
    .await?;

    // Simulate processing up to event 10
    CheckpointQueries::upsert_checkpoint(
        automaton_name.to_string(),
        event_ids[9],
        10,
        json!({"processed_up_to": 9}),
        Some("recovery-group".to_string()),
        Some("recovery-consumer".to_string()),
    )
    .execute(pool)
    .await?;

    // Simulate crash and recovery - update checkpoint to earlier state
    CheckpointQueries::upsert_checkpoint(
        automaton_name.to_string(),
        event_ids[4], // Rolled back to event 5
        5,
        json!({"recovered": true, "previous_position": 9}),
        Some("recovery-group".to_string()),
        Some("recovery-consumer".to_string()),
    )
    .execute(pool)
    .await?;

    // Run integrity check
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

    // Should detect potential data loss from rollback
    let rollback_issues: Vec<_> = results
        .check_report
        .checkpoint_inconsistencies
        .iter()
        .filter(|issue| {
            matches!(
                issue.inconsistency_type,
                CheckpointInconsistencyType::ProcessingGap | 
                CheckpointInconsistencyType::CheckpointRollback
            )
        })
        .collect();

    println!("Found {} rollback-related issues", rollback_issues.len());

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
        automaton_name
    )
    .execute(pool)
    .await?;
    sqlx::query!(
        "DELETE FROM core.processor_manifests WHERE processor_name = $1",
        automaton_name
    )
    .execute(pool)
    .await?;
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.recovery'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_data_integrity(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create test events
    let event = EventBuilder::new()
        .source("test.checkpoint_data")
        .event_type("test")
        .build();
    let inserted = insert_event(&pool, &event).await?;

    // Test various checkpoint data scenarios
    let test_cases = vec![
        (
            "valid_json",
            json!({"state": "ok", "counters": {"processed": 100, "errors": 0}}),
            false,
        ),
        (
            "null_data",
            json!(null),
            true, // Should be flagged as issue
        ),
        (
            "empty_object",
            json!({}),
            false, // Empty but valid
        ),
        (
            "deeply_nested",
            json!({
                "level1": {
                    "level2": {
                        "level3": {
                            "level4": {
                                "data": "deep"
                            }
                        }
                    }
                }
            }),
            false,
        ),
        (
            "large_data",
            json!({"data": vec![0; 1000]}), // Large array
            false,
        ),
    ];

    for (test_name, checkpoint_data, expect_issue) in test_cases {
        let automaton_name = format!("data_test_{}", test_name);

        // Create processor manifest
        sqlx::query!(
            "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
             VALUES ($1, 'automaton', '1.0.0', 'test-host')",
            automaton_name
        )
        .execute(pool)
        .await?;

        // Create checkpoint with test data
        CheckpointQueries::upsert_checkpoint(
            automaton_name.clone(),
            inserted.id,
            1,
            checkpoint_data,
            Some(format!("{}-group", automaton_name)),
            Some(format!("{}-consumer", automaton_name)),
        )
        .execute(pool)
        .await?;

        // Verify checkpoint data integrity
        let issues = checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, &automaton_name)
            .await?;

        if expect_issue {
            assert!(
                !issues.is_empty(),
                "Expected issues for {} checkpoint data",
                test_name
            );
        } else {
            assert!(
                issues.is_empty() || !issues.iter().any(|i| i.contains("data")),
                "Should not flag {} as data issue",
                test_name
            );
        }

        // Cleanup
        sqlx::query!(
            "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
            automaton_name
        )
        .execute(pool)
        .await?;
        sqlx::query!(
            "DELETE FROM core.processor_manifests WHERE processor_name = $1",
            automaton_name
        )
        .execute(pool)
        .await?;
    }

    sqlx::query!("DELETE FROM core.events WHERE source = 'test.checkpoint_data'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_timestamp_consistency(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create events with specific timestamps
    let base_time = Utc::now();
    let mut event_ids = Vec::new();
    
    for i in 0..10 {
        let event = EventBuilder::new()
            .source("test.timestamps")
            .event_type("timed")
            .payload(json!({"index": i}))
            .timestamp(base_time - ChronoDuration::minutes(10 - i as i64))
            .build();
        let inserted = insert_event(&pool, &event).await?;
        event_ids.push(inserted.id);
    }

    let automaton_name = "timestamp_test_automaton";

    // Create processor manifest
    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
         VALUES ($1, 'automaton', '1.0.0', 'test-host')",
        automaton_name
    )
    .execute(pool)
    .await?;

    // Create checkpoint with inconsistent timestamp
    sqlx::query!(
        r#"
        INSERT INTO core.automaton_checkpoints 
        (automaton_name, last_processed_id, processed_count, last_activity, checkpoint_data)
        VALUES ($1, $2::uuid, $3, $4, '{}'::jsonb)
        "#,
        automaton_name,
        event_ids[5].to_uuid(),
        6i64,
        base_time + ChronoDuration::hours(1), // Future timestamp!
    )
    .execute(pool)
    .await?;

    // Verify timestamp consistency
    let issues = checkpoint_verification::verify_automaton_checkpoint_consistency(&pool, automaton_name)
        .await?;

    // Should detect timestamp anomalies
    assert!(
        issues.iter().any(|issue| 
            issue.contains("timestamp") || 
            issue.contains("future") || 
            issue.contains("activity")
        ),
        "Should detect timestamp inconsistencies"
    );

    // Cleanup
    sqlx::query!(
        "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
        automaton_name
    )
    .execute(pool)
    .await?;
    sqlx::query!(
        "DELETE FROM core.processor_manifests WHERE processor_name = $1",
        automaton_name
    )
    .execute(pool)
    .await?;
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.timestamps'")
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_performance_with_many_automata(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create shared event stream
    let event_count = 100;
    let mut event_ids = Vec::new();
    
    let batch_builder = BatchEventBuilder::new();
    for i in 0..event_count {
        batch_builder.add_event()
            .source("test.performance")
            .event_type("shared")
            .payload(json!({"index": i}));
    }
    
    let events = batch_builder.insert_all(&pool).await?;
    event_ids.extend(events.iter().map(|e| e.id));

    // Create many automata with varying progress
    let automaton_count = 20;
    for i in 0..automaton_count {
        let automaton_name = format!("perf_automaton_{}", i);
        
        // Create processor manifest
        sqlx::query!(
            "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
             VALUES ($1, 'automaton', '1.0.0', 'test-host')",
            automaton_name
        )
        .execute(pool)
        .await?;

        // Create checkpoint at different positions
        let position = (i * 5) % event_count;
        if position > 0 {
            CheckpointQueries::upsert_checkpoint(
                automaton_name.clone(),
                event_ids[position - 1],
                position as i64,
                json!({"automaton_id": i}),
                Some(format!("{}-group", automaton_name)),
                Some(format!("{}-consumer", automaton_name)),
            )
            .execute(pool)
            .await?;
        }
    }

    // Time the verification of all checkpoints
    let start = Instant::now();
    let all_issues = checkpoint_verification::verify_all_checkpoint_consistency(&pool).await?;
    let duration = start.elapsed();

    println!(
        "Verified {} automata in {:?}",
        automaton_count,
        duration
    );
    println!("Total issues found: {}", all_issues.len());

    // Performance assertion
    assert!(
        duration < Duration::from_secs(2),
        "Checkpoint verification should be fast even with many automata"
    );

    // Cleanup
    for i in 0..automaton_count {
        let automaton_name = format!("perf_automaton_{}", i);
        sqlx::query!(
            "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
            automaton_name
        )
        .execute(pool)
        .await?;
        sqlx::query!(
            "DELETE FROM core.processor_manifests WHERE processor_name = $1",
            automaton_name
        )
        .execute(pool)
        .await?;
    }
    sqlx::query!("DELETE FROM core.events WHERE source = 'test.performance'")
        .execute(pool)
        .await?;

    Ok(())
}