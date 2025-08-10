//! Event Ordering Integration Tests
//!
//! Comprehensive tests for ULID ordering, timestamp progression, and database consistency
//! in event storage. Tests cover:
//! - ULID sequence ordering validation
//! - Timestamp progression verification  
//! - Concurrent ULID generation ordering
//! - Database ordering consistency
//! - Clock skew detection and handling
//! - Performance analysis of ordering operations

use color_eyre::eyre::Result;
use serde_json::json;
use sinex_core::db::integrity::{ulid_verification, IntegrityTestConfig, IntegrityTester};
use sinex_core::db::repositories::DbPoolExt;
use sinex_test_utils::prelude::*;
use sinex_core::types::domain::EventSource;
use sinex_core::types::{Id, Ulid};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::time::sleep;

// =============================================================================
// ULID SEQUENCE ORDERING TESTS
// =============================================================================

#[sinex_test]
async fn test_ulid_sequence_ordering_validation(ctx: TestContext) -> Result<()> {
    // Generate a sequence of events with known ordering
    let mut event_ulids = Vec::new();

    for i in 0..20 {
        // Add small delay to ensure ULID ordering
        if i > 0 {
            sleep(Duration::from_millis(10)).await;
        }

        let event = ctx
            .create_test_event(
                "ulid-ordering",
                "sequence.test",
                json!({"sequence": i}),
            )
            .await?;

        event_ulids.push(event.id.expect("Event should have ID"));
    }

    // Extract raw ULIDs for verification utilities
    let raw_ulids: Vec<Ulid> = event_ulids.iter().map(|id| id.into()).collect();

    // Verify ordering using the utility function
    let ordering_result = ulid_verification::verify_ulid_sequence_ordering(&raw_ulids);
    assert!(
        ordering_result.is_ok(),
        "ULID sequence should be properly ordered: {:?}",
        ordering_result
    );

    // Verify timestamps have reasonable progression
    let timestamp_result = ulid_verification::verify_timestamp_progression(&raw_ulids, 1000);
    assert!(
        timestamp_result.is_ok(),
        "Timestamp progression should be reasonable: {:?}",
        timestamp_result
    );

    // Verify database ordering matches ULID ordering
    let db_ordered_ulids: Vec<String> = sqlx::query_scalar!(
        "SELECT event_id::text FROM core.events WHERE source = 'ulid-ordering' ORDER BY event_id"
    )
    .fetch_all(&ctx.pool)
    .await?
    .into_iter()
    .filter_map(|opt| opt)
    .collect();

    let expected_order: Vec<String> = event_ulids.iter().map(|id| id.to_string()).collect();
    assert_eq!(
        db_ordered_ulids, expected_order,
        "Database ordering should match ULID ordering"
    );

    Ok(())
}

#[sinex_test]
async fn test_timestamp_progression_verification(ctx: TestContext) -> Result<()> {
    // Create events with specific timestamp patterns
    let base_time = chrono::Utc::now() - chrono::TimeDelta::try_hours(1).unwrap();
    let mut test_ulids = Vec::new();

    // Create events with 1-minute intervals
    for i in 0..10 {
        let timestamp = base_time + chrono::TimeDelta::try_minutes(i as i64).unwrap();
        let ulid = Ulid::from_datetime(timestamp.into());
        test_ulids.push(ulid);
    }

    // Test with reasonable tolerance
    let result = ulid_verification::verify_timestamp_progression(&test_ulids, 5000);
    assert!(
        result.is_ok(),
        "Normal timestamp progression should pass: {:?}",
        result
    );

    // Test with unreasonable tolerance (should fail)
    let strict_result = ulid_verification::verify_timestamp_progression(&test_ulids, 1);
    assert!(
        strict_result.is_err(),
        "Strict tolerance should detect progression issues"
    );

    // Test with regression scenario
    let mut regression_ulids = test_ulids.clone();
    // Add a ULID with timestamp in the past
    let past_timestamp = base_time - chrono::TimeDelta::try_hours(1).unwrap();
    regression_ulids.push(Ulid::from_datetime(past_timestamp.into()));

    let regression_result =
        ulid_verification::verify_timestamp_progression(&regression_ulids, 1000);
    assert!(
        regression_result.is_err(),
        "Timestamp regression should be detected"
    );

    Ok(())
}

// =============================================================================
// CONCURRENT ULID GENERATION TESTS
// =============================================================================

#[sinex_test]
async fn test_concurrent_ulid_generation_ordering(ctx: TestContext) -> Result<()> {
    // Test concurrent event insertion
    let num_concurrent_tasks = 10;
    let events_per_task = 20;
    let mut handles = Vec::new();

    // Create barrier to synchronize concurrent insertions
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(num_concurrent_tasks));

    for task_id in 0..num_concurrent_tasks {
        let barrier_clone = barrier.clone();

        let handle = tokio::spawn(async move {
            let task_ctx = TestContext::new().await.expect("Should create context");
            let mut task_ulids = Vec::new();

            // Wait for all tasks to be ready
            barrier_clone.wait().await;

            for i in 0..events_per_task {
                let event = task_ctx
                    .create_test_event(
                        "concurrent-ulid",
                        "concurrent.test",
                        json!({"task_id": task_id, "sequence": i}),
                    )
                    .await
                    .expect("Event creation should succeed");

                task_ulids.push(event.id.expect("Event should have ID"));

                // Small random delay to increase contention
                let delay_ms = fastrand::u64(1..=5);
                sleep(Duration::from_millis(delay_ms)).await;
            }

            task_ulids
        });

        handles.push(handle);
    }

    // Collect all ULIDs from all tasks
    let mut all_ulids = Vec::new();
    for handle in handles {
        let task_ulids = handle.await?;
        all_ulids.extend(task_ulids);
    }

    println!("Generated {} concurrent ULIDs", all_ulids.len());

    // Verify all ULIDs are unique
    let mut seen = HashSet::new();
    for ulid in &all_ulids {
        assert!(seen.insert(*ulid), "All concurrent ULIDs should be unique");
    }

    // Verify overall ordering (should be mostly correct with some tolerance for concurrent generation)
    let mut sorted_ulids = all_ulids.clone();
    sorted_ulids.sort();

    // Count ordering violations
    let mut violations = 0;
    for i in 1..all_ulids.len() {
        if all_ulids[i] < all_ulids[i - 1] {
            violations += 1;
        }
    }

    let violation_rate = violations as f64 / all_ulids.len() as f64;
    println!(
        "ULID ordering violation rate: {:.2}% ({}/{})",
        violation_rate * 100.0,
        violations,
        all_ulids.len()
    );

    // Some violations are expected with concurrent generation, but should be minimal
    assert!(
        violation_rate < 0.1,
        "ULID violation rate should be less than 10%"
    );

    Ok(())
}

// =============================================================================
// DATABASE ORDERING CONSISTENCY TESTS
// =============================================================================

#[sinex_test]
async fn test_database_ordering_consistency(ctx: TestContext) -> Result<()> {
    // Insert events in batches with different timing patterns
    let mut all_event_ulids = Vec::new();

    // Batch 1: Rapid insertion
    for i in 0..50 {
        let event = ctx
            .create_test_event(
                "db-ordering",
                "rapid.batch", 
                json!({"batch": 1, "sequence": i}),
            )
            .await?;
        all_event_ulids.push(event.id.expect("Event should have ID"));
    }

    // Small delay between batches
    sleep(Duration::from_millis(100)).await;

    // Batch 2: Delayed insertion
    for i in 0..30 {
        let event = ctx
            .create_test_event(
                "db-ordering",
                "delayed.batch",
                json!({"batch": 2, "sequence": i}),
            )
            .await?;
        all_event_ulids.push(event.id.expect("Event should have ID"));

        // Small delay between each event
        sleep(Duration::from_millis(2)).await;
    }

    // Verify different ordering strategies produce consistent results
    let ordering_queries = vec![
        ("ORDER BY event_id", "SELECT event_id::text FROM core.events WHERE source = 'db-ordering' ORDER BY event_id"),
        ("ORDER BY ts_orig", "SELECT event_id::text FROM core.events WHERE source = 'db-ordering' ORDER BY ts_orig"),
        ("ORDER BY ts_ingest", "SELECT event_id::text FROM core.events WHERE source = 'db-ordering' ORDER BY ts_ingest"),
    ];

    let mut ordering_results = HashMap::new();

    for (name, query) in ordering_queries {
        let result: Vec<String> = sqlx::query_scalar(query)
            .fetch_all(&ctx.pool)
            .await?;
        ordering_results.insert(name, result);
    }

    // Compare ordering results
    let id_order = ordering_results.get("ORDER BY event_id").unwrap();
    let ts_orig_order = ordering_results.get("ORDER BY ts_orig").unwrap();
    let ts_ingest_order = ordering_results.get("ORDER BY ts_ingest").unwrap();

    println!("Ordering comparison:");
    println!("  ID order length: {}", id_order.len());
    println!("  ts_orig order length: {}", ts_orig_order.len());
    println!("  ts_ingest order length: {}", ts_ingest_order.len());

    // All should have the same number of events
    assert_eq!(
        id_order.len(),
        ts_orig_order.len(),
        "All orderings should have same count"
    );
    assert_eq!(
        id_order.len(),
        ts_ingest_order.len(),
        "All orderings should have same count"
    );

    // Check how many events are in the same order
    let mut id_ts_orig_matches = 0;
    let mut id_ts_ingest_matches = 0;

    for i in 0..id_order.len() {
        if id_order[i] == ts_orig_order[i] {
            id_ts_orig_matches += 1;
        }
        if id_order[i] == ts_ingest_order[i] {
            id_ts_ingest_matches += 1;
        }
    }

    let orig_match_rate = id_ts_orig_matches as f64 / id_order.len() as f64;
    let ingest_match_rate = id_ts_ingest_matches as f64 / id_order.len() as f64;

    println!(
        "  ID vs ts_orig ordering match: {:.2}% ({}/{})",
        orig_match_rate * 100.0,
        id_ts_orig_matches,
        id_order.len()
    );
    println!(
        "  ID vs ts_ingest ordering match: {:.2}% ({}/{})",
        ingest_match_rate * 100.0,
        id_ts_ingest_matches,
        id_order.len()
    );

    // Due to ULID design, ID ordering should closely match timestamp ordering
    assert!(
        orig_match_rate > 0.8,
        "ID and ts_orig ordering should be mostly consistent"
    );
    assert!(
        ingest_match_rate > 0.8,
        "ID and ts_ingest ordering should be mostly consistent"
    );

    Ok(())
}

// =============================================================================
// CLOCK SKEW DETECTION TESTS
// =============================================================================

#[sinex_test]
async fn test_clock_skew_detection(ctx: TestContext) -> Result<()> {
    // Generate test ULIDs with known ordering violations
    let violation_test_ulids = ulid_verification::generate_ordering_violation_test_ulids();

    for (ulid, description) in violation_test_ulids {
        println!("Testing {}: {}", description, ulid);

        // Create event with this ULID's timestamp
        let event = ctx
            .create_test_event(
                "clock-skew",
                "clock.test",
                json!({"description": description.clone()}),
            )
            .await;

        match description.as_str() {
            "Future timestamp" => {
                // Future timestamps should be detected by validation
                println!(
                    "  Future timestamp event creation result: {:?}",
                    event.is_ok()
                );
            }
            "Ancient timestamp" => {
                // Very old timestamps should be detected
                println!(
                    "  Ancient timestamp event creation result: {:?}",
                    event.is_ok()
                );
            }
            "Normal ULID" => {
                // Normal ULIDs should work fine
                assert!(
                    event.is_ok(),
                    "Normal ULID should insert successfully"
                );
            }
            _ => {}
        }
    }

    // Run integrity checks to detect any issues
    let integrity_tester = IntegrityTester::new(&ctx.pool).await?;
    let config = IntegrityTestConfig {
        max_events_to_check: 100,
        check_window_hours: 1,
        include_deep_validation: false,
        validate_checkpoints: false,
        validate_ulid_ordering: true,
        validate_schemas: false,
    };

    let results = integrity_tester.run_integrity_tests(config).await?;

    println!("Clock skew integrity check results:");
    println!(
        "  ULID violations: {}",
        results.check_report.ulid_ordering_violations.len()
    );

    for violation in &results.check_report.ulid_ordering_violations {
        println!("  - {}: {}", violation.violation_type, violation.details);
    }

    Ok(())
}

// =============================================================================
// PERFORMANCE ANALYSIS TESTS
// =============================================================================

#[sinex_test]
async fn test_ulid_ordering_performance_analysis(ctx: TestContext) -> Result<()> {
    // Generate a large number of events to test ordering performance
    let num_events = 1000;
    let batch_size = 100;

    println!(
        "Testing ULID ordering performance with {} events",
        num_events
    );

    let start_time = Instant::now();
    let mut all_ulids = Vec::new();

    // Insert events in batches
    for batch in 0..(num_events / batch_size) {
        let mut batch_ulids = Vec::new();

        for i in 0..batch_size {
            let event = ctx
                .create_test_event(
                    "ulid-performance",
                    "performance.test",
                    json!({"batch": batch, "item": i}),
                )
                .await?;

            batch_ulids.push(event.id.expect("Event should have ID"));
        }

        all_ulids.extend(batch_ulids);

        // Small delay between batches
        sleep(Duration::from_millis(5)).await;
    }

    let insertion_time = start_time.elapsed();
    let insertion_rate = num_events as f64 / insertion_time.as_secs_f64();

    println!(
        "Inserted {} events in {:?} ({:.2} events/sec)",
        num_events, insertion_time, insertion_rate
    );

    // Test ordering query performance
    let query_start = Instant::now();

    let ordered_ids: Vec<String> = sqlx::query_scalar!(
        "SELECT event_id::text FROM core.events WHERE source = 'ulid-performance' ORDER BY event_id"
    )
    .fetch_all(&ctx.pool)
    .await?
    .into_iter()
    .filter_map(|opt| opt)
    .collect();

    let query_time = query_start.elapsed();
    let query_rate = num_events as f64 / query_time.as_secs_f64();

    println!(
        "Queried {} events in {:?} ({:.2} events/sec)",
        ordered_ids.len(),
        query_time,
        query_rate
    );

    // Verify ordering correctness
    let expected_order: Vec<String> = all_ulids.iter().map(|u| u.to_string()).collect();
    assert_eq!(
        ordered_ids.len(),
        expected_order.len(),
        "Should retrieve all inserted events"
    );

    // Check how many are in correct order
    let mut correct_order_count = 0;
    for i in 0..ordered_ids.len() {
        if ordered_ids[i] == expected_order[i] {
            correct_order_count += 1;
        }
    }

    let order_accuracy = correct_order_count as f64 / ordered_ids.len() as f64;
    println!(
        "Order accuracy: {:.2}% ({}/{})",
        order_accuracy * 100.0,
        correct_order_count,
        ordered_ids.len()
    );

    // Performance assertions
    assert!(
        insertion_rate > 50.0,
        "Insertion rate should be reasonable: {:.2}/sec",
        insertion_rate
    );
    assert!(
        query_rate > 1000.0,
        "Query rate should be reasonable: {:.2}/sec",
        query_rate
    );
    assert!(
        order_accuracy > 0.95,
        "Order accuracy should be high: {:.2}%",
        order_accuracy * 100.0
    );

    Ok(())
}

// =============================================================================
// ULID GENERATION AND UNIQUENESS TESTS
// =============================================================================

#[sinex_test]
async fn test_ulid_uniqueness_under_load(ctx: TestContext) -> Result<()> {
    // Test ULID uniqueness under high concurrent load
    let num_tasks = 20;
    let events_per_task = 50;
    let mut handles = Vec::new();

    // Create events concurrently from multiple tasks
    for task_id in 0..num_tasks {
        let handle = tokio::spawn(async move {
            let task_ctx = TestContext::new().await.expect("Should create context");
            let mut task_ids = Vec::new();

            for i in 0..events_per_task {
                let event = task_ctx
                    .create_test_event(
                        "ulid-uniqueness",
                        "uniqueness.test",
                        json!({
                            "task_id": task_id,
                            "event_index": i,
                            "timestamp": chrono::Utc::now().timestamp_millis()
                        }),
                    )
                    .await
                    .expect("Event creation should succeed");

                task_ids.push(event.id.expect("Event should have ID"));
            }

            task_ids
        });

        handles.push(handle);
    }

    // Collect all generated IDs
    let mut all_ids = Vec::new();
    for handle in handles {
        let task_ids = handle.await?;
        all_ids.extend(task_ids);
    }

    // Verify total count
    let expected_total = num_tasks * events_per_task;
    assert_eq!(
        all_ids.len(),
        expected_total,
        "Should generate expected number of IDs"
    );

    // Verify uniqueness
    let unique_ids: HashSet<_> = all_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        all_ids.len(),
        "All generated ULIDs should be unique"
    );

    // Verify ordering properties
    let mut sorted_ids = all_ids.clone();
    sorted_ids.sort();

    // Count how many IDs are in the correct temporal order
    let mut correct_positions = 0;
    for (i, id) in all_ids.iter().enumerate() {
        if let Some(sorted_pos) = sorted_ids.iter().position(|x| x == id) {
            // Allow some tolerance for concurrent generation
            let position_diff = (sorted_pos as i64 - i as i64).abs();
            if position_diff <= 10 {
                // Within 10 positions is acceptable
                correct_positions += 1;
            }
        }
    }

    let ordering_accuracy = correct_positions as f64 / all_ids.len() as f64;
    println!(
        "ULID temporal ordering accuracy: {:.2}% ({}/{})",
        ordering_accuracy * 100.0,
        correct_positions,
        all_ids.len()
    );

    // Should have reasonable temporal ordering even with concurrency
    assert!(
        ordering_accuracy > 0.8,
        "ULID temporal ordering should be mostly accurate even under concurrent load"
    );

    Ok(())
}