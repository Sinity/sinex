// ULID ordering verification integration tests
//
// Tests for:
// - ULID sequence ordering validation
// - Timestamp progression verification
// - Concurrent ULID generation ordering
// - Database ordering consistency
// - Clock skew detection and handling

use crate::common::prelude::*;
use sinex_db::integrity::{ulid_verification, IntegrityTestConfig, IntegrityTester};
use sinex_db::validation::UlidOrderingViolation;
use sinex_db::queries::{EventQueries};
use sinex_db::query_builder::{QueryBuilder, QueryParam};
use sinex_events::{EventFactory, services, event_types};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[sinex_test]
async fn test_ulid_sequence_ordering_validation(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Generate a sequence of events with known ordering
    let mut event_ulids = Vec::new();

    for i in 0..20 {
        // Add small delay to ensure ULID ordering
        if i > 0 {
            sleep(Duration::from_millis(10)).await;
        }

        let event = {
            let factory = EventFactory::new("test.ulid_ordering");
            let event = factory.create_event("sequence_test", json!({"sequence": i}));
            insert_event_with_validator(
                pool,
                &event,
                None,
            )
        }
        .await?;

        event_ulids.push(event.id);
    }

    // Verify ordering using the utility function
    let ordering_result = ulid_verification::verify_ulid_sequence_ordering(&event_ulids);
    assert!(
        ordering_result.is_ok(),
        "ULID sequence should be properly ordered: {:?}",
        ordering_result
    );

    // Verify timestamps have reasonable progression
    let timestamp_result = ulid_verification::verify_timestamp_progression(&event_ulids, 1000); // 1 second tolerance
    assert!(
        timestamp_result.is_ok(),
        "Timestamp progression should be reasonable: {:?}",
        timestamp_result
    );

    // Verify database ordering matches ULID ordering
    let db_ordered_ulids: Vec<String> = sqlx::query_scalar!(
        "SELECT event_id::text FROM core.events WHERE source = 'test.ulid_ordering' ORDER BY event_id"
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .filter_map(|opt| opt)
    .collect();

    let expected_order: Vec<String> = event_ulids.iter().map(|u| u.to_string()).collect();
    assert_eq!(
        db_ordered_ulids, expected_order,
        "Database ordering should match ULID ordering"
    );

    // Cleanup
    EventQueries::delete_by_source("test.ulid_ordering".to_string())
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_timestamp_progression_verification(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Create events with specific timestamp patterns
    let base_time = Utc::now() - ChronoDuration::hours(1);
    let mut test_ulids = Vec::new();

    // Create events with 1-minute intervals
    for i in 0..10 {
        let timestamp = base_time + ChronoDuration::minutes(i as i64);
        let ulid = Ulid::from_datetime(timestamp);
        test_ulids.push(ulid);
    }

    // Test with reasonable tolerance
    let result = ulid_verification::verify_timestamp_progression(&test_ulids, 5000); // 5 second tolerance
    assert!(
        result.is_ok(),
        "Normal timestamp progression should pass: {:?}",
        result
    );

    // Test with unreasonable tolerance (should fail)
    let strict_result = ulid_verification::verify_timestamp_progression(&test_ulids, 1); // 1ms tolerance
    assert!(
        strict_result.is_err(),
        "Strict tolerance should detect progression issues"
    );

    // Test with regression scenario
    let mut regression_ulids = test_ulids.clone();
    // Add a ULID with timestamp in the past
    let past_timestamp = base_time - ChronoDuration::hours(1);
    regression_ulids.push(Ulid::from_datetime(past_timestamp));

    let regression_result =
        ulid_verification::verify_timestamp_progression(&regression_ulids, 1000);
    assert!(
        regression_result.is_err(),
        "Timestamp regression should be detected"
    );

    Ok(())
}

#[sinex_test]
async fn test_concurrent_ulid_generation_ordering(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test concurrent event insertion
    let num_concurrent_tasks = 10;
    let events_per_task = 20;
    let mut handles = Vec::new();

    // Create barrier to synchronize concurrent insertions
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(num_concurrent_tasks));

    for task_id in 0..num_concurrent_tasks {
        let pool_clone = pool.clone();
        let barrier_clone = barrier.clone();

        let handle = tokio::spawn(async move {
            let mut task_ulids = Vec::new();

            // Wait for all tasks to be ready
            barrier_clone.wait().await;

            for i in 0..events_per_task {
                let event = {
                    let factory = EventFactory::new("test.concurrent_ulid");
                    let event = factory.create_event("concurrent_test", json!({"task_id": task_id, "sequence": i}));
                    sinex_db::insert_event_with_validator(
                        &pool_clone,
                        &event,
                        None,
                    )
                    .await
                    .expect("Event insertion should succeed")
                };

                task_ulids.push(event.id);

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
    let mut seen = std::collections::HashSet::new();
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

    // Cleanup
    EventQueries::delete_by_source("test.concurrent_ulid".to_string())
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_database_ordering_consistency(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Insert events in batches with different timing patterns
    let mut all_event_ulids = Vec::new();

    // Batch 1: Rapid insertion
    for i in 0..50 {
        let event = {
            let factory = EventFactory::new("test.db_ordering");
            let event = factory.create_event("rapid_batch", json!({"batch": 1, "sequence": i}));
            sinex_db::insert_event_with_validator(
                pool,
                &event,
                None,
            )
            .await?
        };
        all_event_ulids.push(event.id);
    }

    // Small delay between batches
    sleep(Duration::from_millis(100)).await;

    // Batch 2: Delayed insertion
    for i in 0..30 {
        let event = {
            let factory = EventFactory::new("test.db_ordering");
            let event = factory.create_event("delayed_batch", json!({"batch": 2, "sequence": i}));
            sinex_db::insert_event_with_validator(
                pool,
                &event,
                None,
            )
            .await?
        };
        all_event_ulids.push(event.id);

        // Small delay between each event
        sleep(Duration::from_millis(2)).await;
    }

    // Verify different ordering strategies produce consistent results
    let ordering_queries = vec![
        ("ORDER BY event_id", "SELECT event_id::text FROM core.events WHERE source = 'test.db_ordering' ORDER BY event_id"),
        ("ORDER BY ts_orig", "SELECT event_id::text FROM core.events WHERE source = 'test.db_ordering' ORDER BY ts_orig"),
        ("ORDER BY ts_ingest", "SELECT event_id::text FROM core.events WHERE source = 'test.db_ordering' ORDER BY ts_ingest"),
    ];

    let mut ordering_results = HashMap::new();

    for (name, query) in ordering_queries {
        let result: Vec<String> = sqlx::query_scalar(query).fetch_all(pool).await?;
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

    // Cleanup
    EventQueries::delete_by_source("test.db_ordering".to_string())
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_clock_skew_detection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Generate test ULIDs with known ordering violations
    let violation_test_ulids = ulid_verification::generate_ordering_violation_test_ulids();

    for (ulid, description) in violation_test_ulids {
        println!("Testing {}: {}", description, ulid);

        // Insert event with this ULID's timestamp
        let factory = EventFactory::new("test.clock_skew");
        let mut event_with_timestamp = factory.create_event("clock_test", json!({"description": description.clone()}));
        event_with_timestamp.id = ulid;
        event_with_timestamp.ts_orig = Some(ulid.timestamp());

        // Try to insert the event (some may fail due to constraints)
        let insert_result = insert_test_event(pool, &event_with_timestamp).await;

        match description.as_str() {
            "Future timestamp" => {
                // Future timestamps should be detected by validation
                println!(
                    "  Future timestamp event insertion result: {:?}",
                    insert_result
                );
            }
            "Ancient timestamp" => {
                // Very old timestamps should be detected
                println!(
                    "  Ancient timestamp event insertion result: {:?}",
                    insert_result
                );
            }
            "Normal ULID" => {
                // Normal ULIDs should work fine
                assert!(
                    insert_result.is_ok(),
                    "Normal ULID should insert successfully"
                );
            }
            _ => {}
        }
    }

    // Run integrity checks to detect any issues
    let integrity_tester = IntegrityTester::new(pool.clone()).await?;
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

    // Cleanup
    EventQueries::delete_by_source("test.clock_skew".to_string())
        .execute(pool)
        .await?;

    Ok(())
}

#[sinex_test]
async fn test_ulid_ordering_performance_analysis(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

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
            let event = {
                let factory = EventFactory::new("test.ulid_performance");
                let event = factory.create_event("performance_test", json!({"batch": batch, "item": i}));
                sinex_db::insert_event_with_validator(
                    pool,
                    &event,
                    None,
                )
                .await?
            };

            batch_ulids.push(event.id);
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
        "SELECT event_id::text FROM core.events WHERE source = 'test.ulid_performance' ORDER BY event_id"
    )
    .fetch_all(pool)
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
    let expected_order: Vec<String> = all_ulids.iter().map(|u: &Ulid| u.to_string()).collect();
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

    // Cleanup
    EventQueries::delete_by_source("test.ulid_performance".to_string())
        .execute(pool)
        .await?;

    Ok(())
}

// Helper function to insert test events with error handling
async fn insert_test_event(pool: &DbPool, event: &RawEvent) -> AnyhowResult<RawEvent> {
    sqlx::query!(
        r#"
        INSERT INTO core.events (
            event_id, source, event_type, ts_orig, host, payload,
            source_event_ids, source_material_id, 
            associated_blob_ids, ingestor_version, payload_schema_id
        ) VALUES (
            $1::uuid, $2, $3, $4, $5, $6,
            $7::uuid[], $8::uuid,
            $9::uuid[], $10, $11::uuid
        )
        "#,
        event.id.to_uuid(),
        event.source,
        event.event_type,
        event.ts_orig,
        event.host,
        event.payload,
        event
            .source_event_ids
            .as_ref()
            .map(|ids| ids.iter().map(|u| u.to_uuid()).collect::<Vec<_>>())
            .as_deref(),
        event.source_material_id.map(|u| u.to_uuid()),
        event
            .associated_blob_ids
            .as_ref()
            .map(|ids| ids.iter().map(|u| u.to_uuid()).collect::<Vec<_>>())
            .as_deref(),
        event.ingestor_version,
        event.payload_schema_id.map(|u| u.to_uuid()),
    )
    .execute(pool)
    .await?;

    Ok(event.clone())
}
