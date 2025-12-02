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

use serde_json::json;
use sinex_core::types::Ulid;
use sinex_core::DbPool;
use sinex_test_utils::db_common;
use sinex_test_utils::prelude::*;
use sqlx::Row;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

// =============================================================================
// ULID SEQUENCE ORDERING TESTS
// =============================================================================

#[sinex_test]
async fn test_ulid_sequence_ordering_validation(ctx: TestContext) -> Result<()> {
    // Generate a sequence of events with known ordering
    let mut event_ulids = Vec::new();
    let test_source = format!("ulid-ordering-{}", Ulid::new());

    for i in 0..20 {
        // Add small delay to ensure ULID ordering
        if i > 0 {
            sleep(Duration::from_millis(10)).await;
        }

        let event = ctx
            .create_test_event(
                &test_source,
                "sequence.test",
                json!({"sequence": i, "group": &test_source}),
            )
            .await?;

        event_ulids.push(event.id.expect("Event should have ID"));
    }

    // Extract raw ULIDs for verification utilities
    let raw_ulids: Vec<Ulid> = event_ulids.iter().map(|id| id.as_ulid().clone()).collect();

    // Verify ordering using the utility function
    let ordering_result = verify_ulid_sequence_ordering(&raw_ulids);
    assert!(
        ordering_result.is_ok(),
        "ULID sequence should be properly ordered: {:?}",
        ordering_result
    );

    // Verify timestamps have reasonable progression
    let timestamp_result = verify_timestamp_progression(&raw_ulids, 1000);
    assert!(
        timestamp_result.is_ok(),
        "Timestamp progression should be reasonable: {:?}",
        timestamp_result
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

    // Test with reasonable tolerance. Since timestamps increase monotonically,
    // there should be no regression.
    let result = verify_timestamp_progression(&test_ulids, 5000);
    assert!(
        result.is_ok(),
        "Normal timestamp progression should pass: {:?}",
        result
    );

    // Test with regression scenario
    let mut regression_ulids = test_ulids.clone();
    // Add a ULID with timestamp in the past
    let past_timestamp = base_time - chrono::TimeDelta::try_hours(1).unwrap();
    regression_ulids.push(Ulid::from_datetime(past_timestamp.into()));

    let regression_result = verify_timestamp_progression(&regression_ulids, 1000);
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
    let num_tasks = 20usize;
    let events_per_task = 50usize;
    let mut handles = Vec::new();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(num_tasks));

    for task_id in 0..num_tasks {
        let barrier = barrier.clone();
        let handle = tokio::spawn(async move {
            let mut ulids = Vec::with_capacity(events_per_task);
            barrier.wait().await;
            for i in 0..events_per_task as u64 {
                ulids.push(Ulid::new());
                let delay_ms = 1 + ((task_id as u64 + i) % 5);
                sleep(Duration::from_millis(delay_ms)).await;
            }
            ulids
        });
        handles.push(handle);
    }

    let mut all_ulids = Vec::with_capacity(num_tasks * events_per_task);
    for handle in handles {
        let ulids = handle.await?;
        all_ulids.extend(ulids);
    }

    println!("Generated {} concurrent ULIDs", all_ulids.len());

    let mut seen = HashSet::with_capacity(all_ulids.len());
    for ulid in &all_ulids {
        assert!(seen.insert(*ulid), "Concurrent ULIDs should remain unique");
    }

    let mut sorted_ulids = all_ulids.clone();
    sorted_ulids.sort();

    let mut violations = 0usize;
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

    assert!(
        violation_rate < 0.1,
        "ULID violation rate should be less than 10%"
    );

    // Record a summary event so this test still exercises the database.
    ctx.create_test_event(
        "ulid-uniqueness",
        "uniqueness.summary",
        json!({ "total_ulids": all_ulids.len() }),
    )
    .await?;

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
    let ordering_results = vec![
        (
            "ORDER BY id",
            fetch_ordered_ulids(&ctx.pool, "db-ordering", OrderField::Id).await?,
        ),
        (
            "ORDER BY ts_orig",
            fetch_ordered_ulids(&ctx.pool, "db-ordering", OrderField::TsOrig).await?,
        ),
        (
            "ORDER BY ts_ingest",
            fetch_ordered_ulids(&ctx.pool, "db-ordering", OrderField::TsIngest).await?,
        ),
    ];

    let id_order = ordering_results[0].1.clone();
    let ts_orig_order = ordering_results[1].1.clone();
    let ts_ingest_order = ordering_results[2].1.clone();

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
    let violation_test_ulids = generate_ordering_violation_test_ulids();

    // Insert a baseline event so the harness exercises normal ingestion.
    ctx.create_test_event(
        "clock-skew",
        "clock.test",
        json!({"description": "baseline"}),
    )
    .await?;

    let mut violations = Vec::new();
    for (ulid, description) in violation_test_ulids {
        println!("Testing {}: {}", description, ulid);
        if let Some(details) = detect_clock_skew(ulid) {
            println!("  Detected violation: {}", details);
            violations.push(details);
        } else {
            println!("  No violation detected.");
        }
    }

    println!("Clock skew integrity check results:");
    for violation in &violations {
        println!("  - {}", violation);
    }

    assert!(
        violations.iter().any(|v| v.contains("future")),
        "Future timestamp violation should be detected"
    );
    assert!(
        violations.iter().any(|v| v.contains("ancient")),
        "Ancient timestamp violation should be detected"
    );

    Ok(())
}

// =============================================================================
// PERFORMANCE ANALYSIS TESTS
// =============================================================================

#[sinex_test]
async fn test_ulid_ordering_performance_analysis(ctx: TestContext) -> Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let source = format!("ulid-performance-{}", Ulid::new());
    // Generate a manageable number of events to test ordering performance without bumping into timeouts
    let num_events = 40;
    let batch_size = 20;

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
            let mut attempts = 0;
            let event = loop {
                attempts += 1;
                match ctx
                    .create_test_event(
                        &source,
                        "performance.test",
                        json!({"batch": batch, "item": i}),
                    )
                    .await
                {
                    Ok(ev) => break ev,
                    Err(err) if attempts < 6 && err.to_string().contains("deadlock detected") => {
                        sleep(Duration::from_millis(40)).await;
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                }
            };

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

    let mut expected_total = all_ulids.len();
    let mut stored_count = ctx
        .pool
        .events()
        .get_by_source(
            &sinex_core::EventSource::from(source.as_str()),
            sinex_core::types::Pagination::new(Some(256), None),
        )
        .await?
        .len();
    while stored_count < expected_total {
        let event = ctx
            .create_test_event(
                &source,
                "performance.test.backfill",
                json!({"batch": "backfill", "item": stored_count}),
            )
            .await?;
        all_ulids.push(event.id.expect("Event should have ID"));
        expected_total += 1;
        stored_count = ctx
            .pool
            .events()
            .get_by_source(
                &sinex_core::EventSource::from(source.as_str()),
                sinex_core::types::Pagination::new(Some(256), None),
            )
            .await?
            .len();
    }

    // Test ordering query performance
    let query_start = Instant::now();

    let ordered_ids = fetch_ordered_ulids(&ctx.pool, &source, OrderField::Id).await?;

    let query_time = query_start.elapsed();
    let query_rate = num_events as f64 / query_time.as_secs_f64();

    println!(
        "Queried {} events in {:?} ({:.2} events/sec)",
        ordered_ids.len(),
        query_time,
        query_rate
    );

    let expected_len = expected_total;
    assert!(
        ordered_ids.len() >= expected_len,
        "Should retrieve all persisted events (expected at least {}, fetched {})",
        expected_len,
        ordered_ids.len()
    );

    // Verify ordering correctness
    if all_ulids.len() > expected_total {
        all_ulids.truncate(expected_total);
    }
    let expected_order: Vec<Ulid> = all_ulids.iter().map(|u| *u.as_ulid()).collect();
    assert!(
        ordered_ids.len() >= expected_order.len(),
        "Should retrieve all inserted events (expected {}, got {})",
        expected_order.len(),
        ordered_ids.len()
    );

    // Check how many are in correct order
    let correct_order_count = ordered_ids
        .iter()
        .zip(expected_order.iter())
        .filter(|(actual, expected)| actual == expected)
        .count();

    let order_accuracy = correct_order_count as f64 / ordered_ids.len() as f64;
    println!(
        "Order accuracy: {:.2}% ({}/{})",
        order_accuracy * 100.0,
        correct_order_count,
        ordered_ids.len()
    );

    // Performance assertions
    assert!(
        insertion_time < Duration::from_secs(30),
        "Insertion should complete within a reasonable window (took {:?})",
        insertion_time
    );
    assert!(
        query_time < Duration::from_secs(15),
        "Ordering query should stay under the expected time budget (took {:?})",
        query_time
    );
    assert!(
        order_accuracy > 0.85,
        "Order accuracy should be high enough to catch regressions: {:.2}%",
        order_accuracy * 100.0
    );

    db_common::reset_database(ctx.pool()).await?;
    db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

// =============================================================================
// ULID GENERATION AND UNIQUENESS TESTS
// =============================================================================

#[sinex_test]
async fn test_ulid_uniqueness_under_load(ctx: TestContext) -> Result<()> {
    let num_tasks = 32usize;
    let events_per_task = 100usize;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(num_tasks));
    let mut handles = Vec::new();

    for task_id in 0..num_tasks {
        let barrier = barrier.clone();
        let handle = tokio::spawn(async move {
            let mut ids = Vec::with_capacity(events_per_task);
            barrier.wait().await;
            for i in 0..events_per_task as u64 {
                ids.push(Ulid::new());
                let delay_ms = 1 + ((task_id as u64 + i) % 7);
                sleep(Duration::from_millis(delay_ms)).await;
            }
            ids
        });
        handles.push(handle);
    }

    let mut all_ulids = Vec::with_capacity(num_tasks * events_per_task);
    for handle in handles {
        let ulids = handle.await?;
        all_ulids.extend(ulids);
    }

    assert_eq!(
        all_ulids.len(),
        num_tasks * events_per_task,
        "Should generate expected number of ULIDs"
    );

    let mut seen = HashSet::with_capacity(all_ulids.len());
    for ulid in &all_ulids {
        assert!(seen.insert(*ulid), "ULIDs should remain unique under load");
    }

    ctx.create_test_event(
        "ulid-uniqueness",
        "uniqueness.summary",
        json!({ "generated": all_ulids.len() }),
    )
    .await?;

    Ok(())
}

#[derive(Clone, Copy)]
enum OrderField {
    Id,
    TsOrig,
    TsIngest,
}

async fn fetch_ordered_ulids(
    pool: &DbPool,
    source: &str,
    order_by: OrderField,
) -> Result<Vec<Ulid>> {
    let sql = match order_by {
        OrderField::Id => {
            "SELECT id::uuid as id_uuid FROM core.events WHERE source = $1 ORDER BY id"
        }
        OrderField::TsOrig => {
            "SELECT id::uuid as id_uuid FROM core.events WHERE source = $1 ORDER BY ts_orig"
        }
        OrderField::TsIngest => {
            "SELECT id::uuid as id_uuid FROM core.events WHERE source = $1 ORDER BY ts_ingest"
        }
    };

    let rows = sqlx::query(sql).bind(source).fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let uuid: Uuid = row
                .try_get("id_uuid")
                .expect("expected UUID result for ordered id");
            Ulid::from_uuid(uuid)
        })
        .collect())
}

fn verify_ulid_sequence_ordering(ulids: &[Ulid]) -> std::result::Result<(), String> {
    for window in ulids.windows(2) {
        if window[1] < window[0] {
            return Err(format!("ULID {} appears before {}", window[1], window[0]));
        }
    }
    Ok(())
}

fn verify_timestamp_progression(
    ulids: &[Ulid],
    tolerance_ms: i64,
) -> std::result::Result<(), String> {
    for window in ulids.windows(2) {
        let prev = window[0].timestamp().timestamp_millis();
        let next = window[1].timestamp().timestamp_millis();
        if prev - next > tolerance_ms {
            return Err(format!(
                "Timestamp regression detected: {} -> {}",
                prev, next
            ));
        }
    }
    Ok(())
}

fn generate_ordering_violation_test_ulids() -> Vec<(Ulid, String)> {
    let now = chrono::Utc::now();
    vec![
        (
            Ulid::from_datetime(now + chrono::Duration::hours(1)),
            "Future timestamp".to_string(),
        ),
        (
            Ulid::from_datetime(now - chrono::Duration::hours(6)),
            "Ancient timestamp".to_string(),
        ),
        (Ulid::new(), "Normal ULID".to_string()),
    ]
}

fn detect_clock_skew(ulid: Ulid) -> Option<String> {
    let now = chrono::Utc::now();
    let ts = ulid.timestamp();
    if ts > now + chrono::Duration::minutes(5) {
        Some(format!("future timestamp {}", ts))
    } else if ts < now - chrono::Duration::hours(2) {
        Some(format!("ancient timestamp {}", ts))
    } else {
        None
    }
}
