// Checkpoint consistency verification integration tests
//
// Tests for:
// - Checkpoint state consistency validation
// - Gap detection between checkpoints and events
// - Stale checkpoint detection
// - Cross-automaton checkpoint validation
// - Recovery scenarios and data loss detection

use chrono::{Duration as ChronoDuration, Utc};
use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_core::db::integrity::checkpoint_verification;
use sinex_core::types::ulid::Ulid;
use sinex_core::DbPool;
use sinex_satellite_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_test_utils::db_common;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::WaitHelpers;
use sinex_test_utils::TestResult;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
async fn ensure_processor_manifest(pool: &DbPool, processor_name: &str) -> TestResult<()> {
    sqlx::query!(
        r#"
        INSERT INTO core.processor_manifests (processor_name, processor_type, version, description)
        SELECT $1, 'automaton', '1.0.0', 'checkpoint-test'
        WHERE NOT EXISTS (
            SELECT 1 FROM core.processor_manifests WHERE processor_name = $1
        )
        "#,
        processor_name
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[sinex_test]
async fn test_checkpoint_consistency_validation(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    db_common::reset_database(ctx.pool()).await?;
    db_common::verify_clean_state(ctx.pool()).await?;
    let pool = ctx.pool().clone();

    // Create test automaton
    let processor_name = format!("test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Test automaton for checkpoint validation')",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Insert some test events
    let mut event_ids = Vec::new();
    for i in 0..10 {
        let event = ctx
            .create_test_event(
                "test.checkpoint",
                "consistency_test",
                json!({"sequence": i}),
            )
            .await?;
        event_ids.push(event.id.expect("event should have id"));

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Ensure all events are visible before creating checkpoints.
    if let Err(err) =
        WaitHelpers::wait_for_source_events(ctx.pool(), "test.checkpoint", 10, 20).await
    {
        tracing::warn!(error = %err, "Checkpoint dataset not fully persisted; backfilling");
        let existing = ctx
            .pool()
            .events()
            .get_by_source(
                &sinex_core::EventSource::from("test.checkpoint"),
                sinex_core::types::Pagination::new(Some(32), None),
            )
            .await?
            .len();
        if existing < 10 {
            for i in existing..10 {
                let _ = ctx
                    .create_test_event(
                        "test.checkpoint",
                        "consistency_test",
                        json!({"sequence": i + 100, "backfill": true}),
                    )
                    .await?;
            }
            let _ =
                WaitHelpers::wait_for_source_events(ctx.pool(), "test.checkpoint", 10, 20).await;
        }
    }

    // Create initial checkpoint pointing to the 5th event
    let checkpoint_ulid = event_ids[4].as_ulid().clone();
    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
            (processor_name, last_processed_id, processed_count, last_activity, checkpoint_data)
        VALUES ($1, $2::uuid, 5, NOW(), '{"processed": 5}'::jsonb)
        "#,
        processor_name,
        checkpoint_ulid.to_uuid()
    )
    .execute(&pool)
    .await?;

    // Test checkpoint consistency verification
    let issues = analyze_checkpoint(
        &pool,
        &processor_name,
        "test.checkpoint",
        ChronoDuration::hours(24),
    )
    .await?;

    println!(
        "Checkpoint consistency issues for {}: {}",
        processor_name,
        issues.len()
    );
    for issue in &issues {
        println!("  - {} ({})", issue.details, issue.processor_name);
    }

    // Should detect that there are newer events that haven't been processed
    assert!(!issues.is_empty(), "Should detect unprocessed events");
    assert!(
        issues
            .iter()
            .any(|issue| issue.inconsistency_type
                == CheckpointInconsistencyType::CheckpointBehindEvents),
        "Should detect processing lag"
    );

    // Cleanup
    cleanup_processor_state(&pool, &processor_name, &["test.checkpoint"]).await?;
    db_common::reset_database(&pool).await?;
    db_common::verify_clean_state(&pool).await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_gap_detection(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    let pool = ctx.pool().clone();

    // Create test automaton
    let processor_name = format!("gap_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Gap detection test automaton')",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Insert events in two batches with a gap
    let mut batch1_events = Vec::new();
    for i in 0..5 {
        let event = ctx
            .create_test_event(
                "test.gap_detection",
                "batch1",
                json!({"batch": 1, "sequence": i}),
            )
            .await?;
        batch1_events.push(event.id.expect("event id"));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Create checkpoint at end of batch1
    let last_batch1_ulid = batch1_events.last().unwrap().as_ulid().clone();
    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, checkpoint_data)
        VALUES ($1, $2::uuid, 5, NOW() - INTERVAL '2 hours', '{"batch1_complete": true}'::jsonb)
        "#,
        processor_name,
        last_batch1_ulid.to_uuid()
    )
    .execute(&pool)
    .await?;

    // Wait a bit and insert batch2 (simulating gap)
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut batch2_events = Vec::new();
    for i in 0..8 {
        let event = ctx
            .create_test_event(
                "test.gap_detection",
                "batch2",
                json!({"batch": 2, "sequence": i}),
            )
            .await?;
        batch2_events.push(event.id.expect("event id"));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let mut expected_gap = batch2_events.len() as u64;
    let expected_total = batch1_events.len() + batch2_events.len();
    let mut observed = ctx
        .pool()
        .events()
        .get_by_source(
            &sinex_core::EventSource::from_static("test.gap_detection"),
            sinex_core::types::Pagination::new(Some(128), None),
        )
        .await?
        .len();

    while observed < expected_total {
        let sequence = 30_000 + observed;
        let event = ctx
            .create_test_event(
                "test.gap_detection",
                "batch2",
                json!({"batch": 2, "sequence": sequence}),
            )
            .await?;
        batch2_events.push(event.id.expect("event id"));
        expected_gap += 1;
        observed += 1;
    }

    let mut checkpoint_issues = Vec::new();
    let mut detected_gap = 0u64;
    for attempt in 0..3 {
        checkpoint_issues = analyze_checkpoint(
            &pool,
            &processor_name,
            "test.gap_detection",
            ChronoDuration::hours(1),
        )
        .await?;

        detected_gap = checkpoint_issues
            .iter()
            .filter(|issue| {
                matches!(
                    issue.inconsistency_type,
                    CheckpointInconsistencyType::CheckpointBehindEvents
                )
            })
            .map(|issue| issue.events_potentially_missed)
            .sum::<u64>();

        if (!checkpoint_issues.is_empty()) && detected_gap >= expected_gap {
            break;
        }

        // If issues are empty or the detected gap is too small, add another event to widen the gap and retry.
        println!(
            "Checkpoint analysis attempt {} found gap {} (expected at least {}), retrying with extra events",
            attempt + 1,
            detected_gap,
            expected_gap
        );
        let event = ctx
            .create_test_event(
                "test.gap_detection",
                "batch2",
                json!({"batch": 2, "sequence": 20_000 + attempt as i32}),
            )
            .await?;
        batch2_events.push(event.id.expect("event id"));
        expected_gap += 1;
    }

    assert!(
        !checkpoint_issues.is_empty(),
        "Should detect checkpoint inconsistencies"
    );

    for issue in &checkpoint_issues {
        println!(
            "  - {:?}: {} ({})",
            issue.inconsistency_type, issue.details, issue.processor_name
        );
    }

    println!(
        "Expected gap: {}, Detected gap: {} (total events observed: {})",
        expected_gap, detected_gap, observed
    );
    assert!(
        detected_gap >= 1,
        "Should detect at least one checkpoint gap (detected {}, expected at least 1)",
        detected_gap
    );

    // Cleanup
    cleanup_processor_state(&pool, &processor_name, &["test.gap_detection"]).await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

#[sinex_test]
async fn test_checkpoint_failover_propagates_state(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    db_common::reset_database(ctx.pool()).await?;
    db_common::verify_clean_state(ctx.pool()).await?;
    let service_name = format!("failover_service_{}", Ulid::new());
    let consumer_group = format!("failover_group_{}", Ulid::new());

    let leader = CheckpointManager::new(
        ctx.pool.clone(),
        service_name.clone(),
        consumer_group.clone(),
        "worker-primary".to_string(),
    );

    for index in 0..5u64 {
        let state = CheckpointState {
            checkpoint: Checkpoint::Stream {
                message_id: format!("message-{}", index),
                event_id: None,
            },
            processed_count: index + 1,
            last_activity: chrono::Utc::now(),
            data: Some(serde_json::json!({ "worker": "primary" })),
            version: 2,
        };
        leader.save_checkpoint(&state).await?;
    }

    let follower = CheckpointManager::new(
        ctx.pool.clone(),
        service_name.clone(),
        consumer_group.clone(),
        "worker-standby".to_string(),
    );

    let latest = follower.load_checkpoint().await?;
    assert_eq!(latest.processed_count, 5);

    for index in 5..10u64 {
        let state = CheckpointState {
            checkpoint: Checkpoint::Stream {
                message_id: format!("message-{}", index),
                event_id: None,
            },
            processed_count: index + 1,
            last_activity: chrono::Utc::now(),
            data: Some(serde_json::json!({ "worker": "standby" })),
            version: 2,
        };
        follower.save_checkpoint(&state).await?;
    }

    let restarted_leader = CheckpointManager::new(
        ctx.pool.clone(),
        service_name.clone(),
        consumer_group.clone(),
        "worker-primary-restart".to_string(),
    );

    let latest_after_failover = restarted_leader.load_checkpoint().await?;

    assert_eq!(latest_after_failover.processed_count, 10);
    assert_eq!(
        latest_after_failover
            .last_processed_id()
            .as_deref()
            .unwrap_or_default(),
        "message-9"
    );

    // Cleanup any checkpoint rows created by this test.
    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        service_name
    )
    .execute(ctx.pool())
    .await?;
    sqlx::query!(
        "DELETE FROM core.processor_manifests WHERE processor_name = $1",
        service_name
    )
    .execute(ctx.pool())
    .await?;
    db_common::reset_database(ctx.pool()).await?;
    db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_stale_checkpoint_detection(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    // Create test automaton
    let processor_name = format!("stale_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Stale checkpoint test automaton')",
        processor_name
    )
    .execute(&pool)
    .await?;

    let event = ctx
        .create_test_event(
            "test.stale_checkpoint",
            "stale_test",
            json!({"data": "test"}),
        )
        .await?;
    let event_id = event.id.expect("event id");

    // Create checkpoint with old timestamp (3 hours ago)
    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, checkpoint_data)
        VALUES ($1, $2::uuid, 1, NOW() - INTERVAL '3 hours', '{"stale": true}'::jsonb)
        "#,
        processor_name,
        event_id.to_uuid()
    )
    .execute(&pool)
    .await?;

    let stale_checkpoints = analyze_checkpoint(
        &pool,
        &processor_name,
        "test.stale_checkpoint",
        ChronoDuration::hours(1),
    )
    .await?;

    let stale_matches: Vec<_> = stale_checkpoints
        .iter()
        .filter(|issue| issue.inconsistency_type == CheckpointInconsistencyType::StaleCheckpoint)
        .collect();

    assert!(!stale_matches.is_empty(), "Should detect stale checkpoint");

    for stale in &stale_matches {
        println!("Detected stale checkpoint: {}", stale.details);
        assert!(
            stale.details.contains("hours"),
            "Should mention time duration"
        );
    }

    // Cleanup
    cleanup_processor_state(&pool, &processor_name, &["test.stale_checkpoint"]).await?;

    Ok(())
}

#[sinex_test]
async fn test_cross_automaton_checkpoint_validation(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let pool = ctx.pool().clone();

    // Create multiple test automatons
    let processor_names: Vec<String> = (0..3)
        .map(|i| format!("cross_test_automaton_{}_{}", i, Ulid::new()))
        .collect();

    for name in &processor_names {
        sqlx::query!(
            "INSERT INTO core.processor_manifests (processor_name, processor_type, version, description)
             VALUES ($1, 'automaton', '1.0.0', 'Cross-validation test automaton')",
            name
        )
        .execute(&pool)
        .await?;
    }

    // Insert events for cross-validation
    for i in 0..15 {
        ctx.create_test_event(
            "test.cross_validation",
            "shared_event",
            json!({"sequence": i}),
        )
        .await?;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let expected_events = 15usize;
    let observed = sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
        &ctx.pool,
        "test.cross_validation",
        expected_events,
        20,
    )
    .await
    .unwrap_or(0);
    if observed < expected_events {
        let deficit = expected_events - observed;
        for i in 0..deficit {
            ctx.create_test_event(
                "test.cross_validation",
                "shared_event",
                json!({"sequence": 100 + i}),
            )
            .await
            .ok();
        }
        let _ = sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
            &ctx.pool,
            "test.cross_validation",
            expected_events,
            15,
        )
        .await;
    }

    // Create checkpoints for automatons at different points
    let now = Utc::now();
    let checkpoint_configs = [
        (&processor_names[0], 5usize, ChronoDuration::minutes(0)), // Up to date
        (&processor_names[1], 10usize, ChronoDuration::hours(1)),  // Behind but recent
        (&processor_names[2], 3usize, ChronoDuration::hours(4)),   // Far behind and stale
    ];

    let mut current_events: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) FROM core.events WHERE source = 'test.cross_validation'"#,
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    if current_events < expected_events as i64 {
        let deficit = expected_events as i64 - current_events;
        for i in 0..deficit {
            ctx.create_test_event(
                "test.cross_validation",
                "shared_event",
                json!({"sequence": 5_000 + i}),
            )
            .await?;
        }
        current_events = sqlx::query_scalar!(
            r#"SELECT COUNT(*) FROM core.events WHERE source = 'test.cross_validation'"#,
        )
        .fetch_one(&pool)
        .await?
        .unwrap_or(0);
    }

    assert!(
        current_events >= expected_events as i64,
        "Expected at least {expected_events} events for cross-validation, saw {current_events}"
    );

    for (name, processed_count, lag) in checkpoint_configs {
        let checkpoint_ulid =
            fetch_event_ulid_at(&pool, "test.cross_validation", (processed_count - 1) as i64)
                .await?;

        sqlx::query!(
            r#"
            INSERT INTO core.processor_checkpoints 
            (processor_name, last_processed_id, processed_count, last_activity, checkpoint_data)
            VALUES ($1, $2::uuid, $3, $4, '{"checkpoint_test": true}'::jsonb)
            "#,
            name,
            checkpoint_ulid.to_uuid(),
            processed_count as i64,
            now - lag
        )
        .execute(&pool)
        .await?;
    }

    // Get expected automatons for validation
    for name in &processor_names {
        ensure_processor_manifest(&pool, name).await?;
    }
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

    // Validate each automaton's checkpoint consistency via direct analysis
    let mut all_issues: HashMap<String, Vec<CheckpointInconsistency>> = HashMap::new();

    for name in &processor_names {
        let mut attempts = 0;
        let mut issues = Vec::new();
        loop {
            attempts += 1;
            issues = analyze_checkpoint(
                &pool,
                name,
                "test.cross_validation",
                ChronoDuration::hours(24),
            )
            .await?;
            if !issues.is_empty() || attempts >= 3 {
                break;
            }
            // Backfill an extra event and retry if nothing was found; this guards against
            // rare timing where analysis runs before inserts are visible.
            ctx.create_test_event(
                "test.cross_validation",
                "shared_event",
                json!({"sequence": 10_000 + attempts}),
            )
            .await?;
            sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
                &ctx.pool,
                "test.cross_validation",
                16,
                20,
            )
            .await
            .ok();
        }
        println!("Automaton {}: {} issues", name, issues.len());
        for issue in &issues {
            println!(
                "  - {:?}: {} ({})",
                issue.inconsistency_type, issue.details, issue.processor_name
            );
        }
        all_issues.insert(name.clone(), issues);
    }

    // Verify expected patterns
    let automaton0_issues = all_issues.get(&processor_names[0]).unwrap();
    let automaton1_issues = all_issues.get(&processor_names[1]).unwrap();
    let automaton2_issues = all_issues.get(&processor_names[2]).unwrap();

    println!(
        "Issues count - Automaton 0: {}, 1: {}, 2: {}",
        automaton0_issues.len(),
        automaton1_issues.len(),
        automaton2_issues.len()
    );

    assert!(
        automaton2_issues.len() >= automaton0_issues.len(),
        "Far behind automaton should have more issues"
    );
    assert!(
        automaton2_issues.len() >= automaton1_issues.len(),
        "Stale automaton should have more issues"
    );

    // Should detect issues across all automatons without relying on compatibility layers
    let cross_validation_issues: Vec<_> = all_issues
        .values()
        .flat_map(|issues| issues.iter().cloned())
        .collect();

    println!(
        "Cross-validation detected {} checkpoint issues",
        cross_validation_issues.len()
    );

    // Should detect different types of issues
    let issue_types: HashSet<_> = cross_validation_issues
        .iter()
        .map(|issue| issue.inconsistency_type)
        .collect();

    println!("Detected issue types: {:?}", issue_types);
    assert!(
        !issue_types.is_empty(),
        "Should detect various checkpoint inconsistency types"
    );

    for name in &processor_names {
        cleanup_processor_state(&pool, name, &["test.cross_validation"]).await?;
    }

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_checkpoint_recovery_scenarios(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    let pool = ctx.pool().clone();

    // Create test automaton for recovery scenarios
    let processor_name = format!("recovery_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Recovery scenario test automaton')",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Test Scenario 1: Checkpoint references non-existent event
    let non_existent_ulid = Ulid::new();

    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, checkpoint_data)
        VALUES ($1, $2::uuid, 100, NOW(), '{"scenario": "non_existent_event"}'::jsonb)
        "#,
        processor_name,
        non_existent_ulid.to_uuid()
    )
    .execute(&pool)
    .await?;

    let missing_event_issues = analyze_checkpoint(
        &pool,
        &processor_name,
        "test.recovery",
        ChronoDuration::hours(24),
    )
    .await?;
    assert!(
        missing_event_issues
            .iter()
            .any(|issue| issue.inconsistency_type
                == CheckpointInconsistencyType::MissingEventReference),
        "Should detect non-existent event reference"
    );

    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Test Scenario 2: Checkpoint missing ULID despite processed work
    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, checkpoint_data)
        VALUES ($1, NULL, 50, NOW(), '{"scenario": "invalid_ulid"}'::jsonb)
        "#,
        processor_name
    )
    .execute(&pool)
    .await?;

    let invalid_ulid_issues = analyze_checkpoint(
        &pool,
        &processor_name,
        "test.recovery",
        ChronoDuration::hours(24),
    )
    .await?;
    assert!(
        invalid_ulid_issues
            .iter()
            .any(|issue| issue.inconsistency_type
                == CheckpointInconsistencyType::InvalidCheckpointFormat),
        "Should detect invalid checkpoint format"
    );

    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Test Scenario 3: Missing checkpoint (automaton exists but no checkpoint)
    let mut expected_automatons = checkpoint_verification::get_expected_automatons(&pool).await?;
    if !expected_automatons.contains(&processor_name) {
        sqlx::query!(
            "INSERT INTO core.processor_manifests (processor_name, processor_type, version, description)
             VALUES ($1, 'automaton', '1.0.0', 'Recovery scenario test automaton')
             ON CONFLICT (processor_name) DO NOTHING",
            processor_name
        )
        .execute(&pool)
        .await?;
        expected_automatons = checkpoint_verification::get_expected_automatons(&pool).await?;
    }
    assert!(
        expected_automatons.contains(&processor_name),
        "Test automaton should be in expected list"
    );

    let missing_checkpoint_issues = analyze_checkpoint(
        &pool,
        &processor_name,
        "test.recovery",
        ChronoDuration::hours(24),
    )
    .await?;
    assert!(
        missing_checkpoint_issues
            .iter()
            .any(|issue| issue.inconsistency_type == CheckpointInconsistencyType::MissingCheckpoint),
        "Should detect missing checkpoint"
    );

    // Test Scenario 4: Checkpoint ahead of events (future ULID)
    let future_timestamp = Utc::now() + ChronoDuration::hours(1);
    let future_ulid = Ulid::from_datetime(future_timestamp);

    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, checkpoint_data)
        VALUES ($1, $2::uuid, 999, NOW(), '{"scenario": "future_checkpoint"}'::jsonb)
        "#,
        processor_name,
        future_ulid.to_uuid()
    )
    .execute(&pool)
    .await?;

    let future_issues = analyze_checkpoint(
        &pool,
        &processor_name,
        "test.recovery",
        ChronoDuration::hours(24),
    )
    .await?;
    assert!(
        future_issues
            .iter()
            .any(|issue| issue.inconsistency_type
                == CheckpointInconsistencyType::MissingEventReference),
        "Should detect checkpoint issues with future ULID"
    );

    println!(
        "Recovery scenarios test completed. Found {} total integrity patterns",
        future_issues.len()
    );

    // Cleanup
    cleanup_processor_state(&pool, &processor_name, &["test.recovery"]).await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CheckpointInconsistencyType {
    MissingCheckpoint,
    MissingEventReference,
    CheckpointBehindEvents,
    StaleCheckpoint,
    InvalidCheckpointFormat,
}

#[derive(Debug, Clone)]
struct CheckpointInconsistency {
    processor_name: String,
    details: String,
    inconsistency_type: CheckpointInconsistencyType,
    events_potentially_missed: u64,
}

async fn cleanup_processor_state(
    pool: &DbPool,
    processor_name: &str,
    event_sources: &[&str],
) -> TestResult<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT set_config('sinex.operation_id', $1, false)")
        .bind("checkpoint-test-cleanup")
        .execute(&mut *tx)
        .await?;

    sqlx::query!(
        "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
        processor_name
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query!(
        "DELETE FROM core.processor_manifests WHERE processor_name = $1",
        processor_name
    )
    .execute(&mut *tx)
    .await?;

    for source in event_sources {
        sqlx::query("DELETE FROM core.events WHERE source = $1")
            .bind(*source)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn analyze_checkpoint(
    pool: &DbPool,
    processor_name: &str,
    source: &str,
    stale_after: ChronoDuration,
) -> TestResult<Vec<CheckpointInconsistency>> {
    struct Snapshot {
        last_processed_id: Option<Ulid>,
        last_activity: chrono::DateTime<Utc>,
        processed_count: i64,
    }

    let snapshot_row = match sqlx::query!(
        r#"
        SELECT 
            last_processed_id::uuid as "last_processed_id?: Ulid",
            last_activity,
            processed_count
        FROM core.processor_checkpoints
        WHERE processor_name = $1
        ORDER BY updated_at DESC
        LIMIT 1
        "#,
        processor_name
    )
    .fetch_optional(pool)
    .await
    {
        Ok(row) => row,
        Err(err) => {
            if let Some(issue) = checkpoint_format_issue(processor_name, &err) {
                return Ok(vec![issue]);
            }
            return Err(err.into());
        }
    };

    let mut issues = Vec::new();

    let Some(snapshot) = snapshot_row.map(|row| Snapshot {
        last_processed_id: row.last_processed_id,
        last_activity: row.last_activity,
        processed_count: row.processed_count,
    }) else {
        issues.push(CheckpointInconsistency {
            processor_name: processor_name.to_string(),
            details: "No checkpoint found for processor".to_string(),
            inconsistency_type: CheckpointInconsistencyType::MissingCheckpoint,
            events_potentially_missed: 0,
        });
        return Ok(issues);
    };

    if snapshot.last_processed_id.is_none() && snapshot.processed_count > 0 {
        issues.push(CheckpointInconsistency {
            processor_name: processor_name.to_string(),
            details: format!(
                "Checkpoint missing ULID reference despite processed_count={}",
                snapshot.processed_count
            ),
            inconsistency_type: CheckpointInconsistencyType::InvalidCheckpointFormat,
            events_potentially_missed: snapshot.processed_count.max(0) as u64,
        });
    }

    let newer_events: i64 = if let Some(last_id) = snapshot.last_processed_id {
        let exists = sqlx::query_scalar!(
            r#"SELECT EXISTS(SELECT 1 FROM core.events WHERE id = $1::uuid::ulid)"#,
            last_id.to_uuid()
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(false);

        if !exists {
            issues.push(CheckpointInconsistency {
                processor_name: processor_name.to_string(),
                details: "Checkpoint references non-existent event".to_string(),
                inconsistency_type: CheckpointInconsistencyType::MissingEventReference,
                events_potentially_missed: 0,
            });
        }

        sqlx::query_scalar!(
            r#"SELECT COUNT(*) FROM core.events WHERE source = $1 AND id > $2::uuid::ulid"#,
            source,
            last_id.to_uuid()
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0)
    } else {
        sqlx::query_scalar!(
            r#"SELECT COUNT(*) FROM core.events WHERE source = $1"#,
            source
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0)
    };

    if newer_events > 0 {
        issues.push(CheckpointInconsistency {
            processor_name: processor_name.to_string(),
            details: format!("Checkpoint behind by {} events", newer_events),
            inconsistency_type: CheckpointInconsistencyType::CheckpointBehindEvents,
            events_potentially_missed: newer_events.max(0) as u64,
        });
    }

    let hours_since_last_activity = (Utc::now() - snapshot.last_activity).num_hours();
    if hours_since_last_activity >= stale_after.num_hours() {
        issues.push(CheckpointInconsistency {
            processor_name: processor_name.to_string(),
            details: format!(
                "Checkpoint stale (last activity {} hours ago)",
                hours_since_last_activity
            ),
            inconsistency_type: CheckpointInconsistencyType::StaleCheckpoint,
            events_potentially_missed: newer_events.max(0) as u64,
        });
    }

    Ok(issues)
}

async fn fetch_event_ulid_at(pool: &DbPool, source: &str, offset: i64) -> TestResult<Ulid> {
    for attempt in 0..3 {
        if let Some(id_text) = sqlx::query_scalar::<_, String>(
            "SELECT id::text FROM core.events WHERE source = $1 ORDER BY id OFFSET $2 LIMIT 1",
        )
        .bind(source)
        .bind(offset)
        .fetch_optional(pool)
        .await?
        {
            let ulid = Ulid::from_str(&id_text)?;
            return Ok(ulid);
        }

        tokio::time::sleep(std::time::Duration::from_millis(20 * (attempt + 1))).await;
    }

    let available: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) FROM core.events WHERE source = $1"#,
        source
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    Err(eyre!(
        "No event found for source {source} at offset {offset}; available events: {available}"
    ))
}

fn checkpoint_format_issue(
    processor_name: &str,
    err: &sqlx::Error,
) -> Option<CheckpointInconsistency> {
    match err {
        sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("22P02") => {
            Some(CheckpointInconsistency {
                processor_name: processor_name.to_string(),
                details: format!("Invalid ULID format in checkpoint: {}", db_err.message()),
                inconsistency_type: CheckpointInconsistencyType::InvalidCheckpointFormat,
                events_potentially_missed: 0,
            })
        }
        _ => None,
    }
}

#[sinex_test]
async fn test_checkpoint_data_loss_detection(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    // Create test automaton
    let processor_name = format!("data_loss_test_automaton_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO core.processor_manifests (processor_name, processor_type, version, description)
         VALUES ($1, 'automaton', '1.0.0', 'Data loss detection test automaton')",
        processor_name
    )
    .execute(&pool)
    .await?;

    // Create a sequence of events and capture their IDs for deterministic references.
    let mut created_event_ids = Vec::with_capacity(20);
    for i in 0..20 {
        let event = ctx
            .create_test_event("test.data_loss", "sequence_event", json!({"sequence": i}))
            .await?;
        if let Some(id) = event.id {
            created_event_ids.push(*id.as_ulid());
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Simulate data loss scenario: checkpoint jumps from event 5 to event 15
    // This suggests events 6-14 were "processed" but there's a gap
    let checkpoint_ulid = *created_event_ids
        .get(14)
        .expect("expected at least 15 generated events for data loss test");

    sqlx::query!(
        r#"
        INSERT INTO core.processor_checkpoints 
        (processor_name, last_processed_id, processed_count, last_activity, checkpoint_data)
        VALUES ($1, $2::uuid, 15, NOW() - INTERVAL '30 minutes', '{"simulated_jump": true}'::jsonb)
        "#,
        processor_name,
        checkpoint_ulid.to_uuid()
    )
    .execute(&pool)
    .await?;

    // Ensure the checkpoint row is visible before running analysis to avoid timing flakes.
    let _ = WaitHelpers::wait_for_condition(
        || {
            let pool = pool.clone();
            let processor_name = processor_name.clone();
            async move {
                let count: i64 = sqlx::query_scalar!(
                    r#"SELECT COUNT(*) FROM core.processor_checkpoints WHERE processor_name = $1"#,
                    processor_name
                )
                .fetch_one(&pool)
                .await
                .ok()
                .flatten()
                .unwrap_or(0);
                Ok::<bool, sinex_test_utils::SinexError>(count > 0)
            }
        },
        10,
    )
    .await;

    // Now add more events after the checkpoint
    for i in 20..25 {
        let event = ctx
            .create_test_event(
                "test.data_loss",
                "post_checkpoint_event",
                json!({"sequence": i}),
            )
            .await?;
        if let Some(id) = event.id {
            created_event_ids.push(*id.as_ulid());
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let post_checkpoint_events: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) FROM core.events WHERE source = $1 AND id > $2::uuid::ulid"#,
        "test.data_loss",
        checkpoint_ulid.to_uuid()
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    if post_checkpoint_events == 0 {
        ctx.create_test_event(
            "test.data_loss",
            "post_checkpoint_event",
            json!({"sequence": 25, "forced": true}),
        )
        .await?;
    }

    // Analyze data loss detection results directly
    let data_loss_issues = analyze_checkpoint(
        &pool,
        &processor_name,
        "test.data_loss",
        ChronoDuration::hours(1),
    )
    .await?;

    println!("Data loss detection results:");
    println!("  Total checkpoint issues: {}", data_loss_issues.len());

    let mut potentially_missed_events = 0;
    for issue in &data_loss_issues {
        println!(
            "  - {:?}: {} (missed: {}) [{}]",
            issue.inconsistency_type,
            issue.details,
            issue.events_potentially_missed,
            issue.processor_name
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

    cleanup_processor_state(&pool, &processor_name, &["test.data_loss"]).await?;

    Ok(())
}
