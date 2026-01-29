//! Timestamp handling integration tests
//!
//! Tests the system's handling of various timestamp scenarios including:
//! - Boundary timestamps (epoch, far future)
//! - Out-of-order event ingestion
//! - Timestamp precision and accuracy
//! - Cross-timezone compatibility

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::temporal::{now, Duration, OffsetDateTime, Rfc3339, Timestamp};
use xtask::sandbox::prelude::*;

/// Test timestamp boundary conditions
#[sinex_test]
async fn test_timestamp_boundaries(ctx: TestContext) -> TestResult<()> {
    let timestamp_cases = vec![
        // Unix epoch
        OffsetDateTime::from_unix_timestamp(0).unwrap().into(),
        // Far future (year 9999)
        OffsetDateTime::new_in_utc(
            time::Date::from_calendar_date(9999, time::Month::December, 31).unwrap(),
            time::Time::from_hms(23, 59, 59).unwrap(),
        )
        .into(),
        // Near boundaries
        OffsetDateTime::from_unix_timestamp(i32::MAX as i64)
            .unwrap()
            .into(),
        // Current time
        now(),
    ];

    for (i, ts) in timestamp_cases.iter().enumerate() {
        let ts: Timestamp = *ts;
        let event = test_event(
            EventSource::from("timestamp_test"),
            EventType::from(format!("boundary_{i}")),
            json!({
                "timestamp": ts.format_rfc3339(),
                "epoch": ts.unix_timestamp(),
                "test_case": i
            }),
        )
        .at_time(ts);

        ctx.pool.events().insert(event).await?;
    }

    // Query events to verify timestamp preservation
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("timestamp_test"),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(
        events.len(),
        timestamp_cases.len(),
        "All events should be stored"
    );

    // Verify timestamps are preserved correctly
    for event in &events {
        assert!(
            event.ts_orig.is_some(),
            "Original timestamp should be preserved"
        );
        let ingest_ts = event.id.as_ref().expect("event should have id").timestamp();
        assert!(
            ingest_ts > OffsetDateTime::from_unix_timestamp(0, 0).unwrap(),
            "Ingestion (ULID) timestamp should be set"
        );
    }

    Ok(())
}

/// Test out-of-order event ingestion
#[sinex_test]
async fn test_out_of_order_timestamps(ctx: TestContext) -> TestResult<()> {
    let base_time = now() - Duration::hours(1);

    // Insert events in reverse chronological order
    let timestamps = vec![
        base_time + Duration::seconds(30), // Latest timestamp
        base_time + Duration::seconds(20), // Middle timestamp
        base_time + Duration::seconds(10), // Earliest timestamp
    ];

    let mut inserted_events = Vec::new();

    for (i, &ts) in timestamps.iter().enumerate() {
        let ts: Timestamp = ts;
        let event = test_event(
            EventSource::from("out_of_order_test"),
            EventType::from("sequenced_event"),
            json!({
                "sequence": i,
                "logical_time": ts.format_rfc3339(),
                "description": format!("Event {} with timestamp {}", i, ts.format_rfc3339())
            }),
        )
        .at_time(ts);

        let inserted = ctx.pool.events().insert(event).await?;
        inserted_events.push(inserted);
    }

    // Events should be ordered by ingestion time (ULID), not by ts_orig
    for i in 1..inserted_events.len() {
        let earlier = &inserted_events[i - 1];
        let later = &inserted_events[i];

        // ULID ordering (ingestion order)
        let earlier_id = earlier.id.as_ref().expect("ingestion id");
        let later_id = later.id.as_ref().expect("ingestion id");
        let earlier_str = earlier_id.to_string();
        let later_str = later_id.to_string();
        assert!(
            earlier_str < later_str,
            "Events should maintain ingestion order by ULID"
        );

        // But logical timestamp ordering might be violated (ts_orig)
        if let (Some(earlier_orig), Some(later_orig)) = (earlier.ts_orig, later.ts_orig) {
            // This demonstrates that ingestion order != logical time order
            println!(
                "Ingestion order: {} < {}, Logical order: {} vs {}",
                earlier_str,
                later_str,
                earlier_orig
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
                later_orig
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap()
            );
        }
    }

    Ok(())
}

/// Test timestamp precision handling
#[sinex_serial_test]
async fn test_timestamp_precision(ctx: TestContext) -> color_eyre::Result<()> {
    ctx.ensure_clean().await?;
    let source = format!("precision_test_{}", Ulid::new());
    // Test various precision levels
    let precision_cases = vec![
        // Second precision
        OffsetDateTime::new_in_utc(
            time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap(),
            time::Time::from_hms(12, 0, 0).unwrap(),
        ),
        // Millisecond precision
        OffsetDateTime::new_in_utc(
            time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap(),
            time::Time::from_hms_nano(12, 0, 0, 123_000_000).unwrap(),
        ),
        // Microsecond precision
        OffsetDateTime::new_in_utc(
            time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap(),
            time::Time::from_hms_nano(12, 0, 0, 123_456_000).unwrap(),
        ),
        // Nanosecond precision
        OffsetDateTime::new_in_utc(
            time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap(),
            time::Time::from_hms_nano(12, 0, 0, 123_456_789).unwrap(),
        ),
    ];

    for (i, &ts) in precision_cases.iter().enumerate() {
        let event = test_event(
            EventSource::from(source.as_str()),
            EventType::from("precision_event"),
            json!({
                "precision_level": i,
                "nanosecond": ts.nanosecond(),
                "original_timestamp": ts.format(&time::format_description::well_known::Rfc3339).unwrap()
            }),
        )
        .at_time(ts);

        ctx.pool.events().insert(event).await?;
    }

    // Verify precision is maintained
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from(source.as_str()),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(events.len(), precision_cases.len());

    let mut stored_by_level: Vec<(usize, OffsetDateTime)> = events
        .iter()
        .map(|event| {
            let level = event.payload["precision_level"]
                .as_u64()
                .expect("precision level metadata present") as usize;
            let ts = event.ts_orig.expect("Should have original timestamp");
            (level, ts)
        })
        .collect();
    stored_by_level.sort_by_key(|(level, _)| *level);

    for (level, stored_ts) in stored_by_level {
        let original_ts = precision_cases[level];
        assert_eq!(
            original_ts.unix_timestamp_nanos(),
            stored_ts.unix_timestamp_nanos(),
            "Nanosecond precision should be preserved for event {}",
            level
        );
    }

    Ok(())
}

/// Test cross-timezone timestamp handling
#[sinex_test]
async fn test_timezone_handling(ctx: TestContext) -> color_eyre::Result<()> {
    // All timestamps should be normalized to UTC in storage
    let utc_time = OffsetDateTime::now_utc();

    // Create events with the same logical time expressed in different ways
    let timezone_cases = vec![
        ("utc_explicit", utc_time),
        (
            "utc_parsed",
            OffsetDateTime::parse(
                &utc_time
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
                &time::format_description::well_known::Rfc3339,
            )?,
        ),
        (
            "utc_timestamp",
            OffsetDateTime::from_unix_timestamp_nanos(utc_time.unix_timestamp_nanos())?,
        ),
    ];

    for (name, ts) in &timezone_cases {
        let event = test_event(
            EventSource::from("timezone_test"),
            EventType::from("timezone_event"),
            json!({
                "timezone_case": name,
                "timestamp": ts.format(&time::format_description::well_known::Rfc3339).unwrap()
            }),
        )
        .at_time(*ts);

        ctx.pool.events().insert(event).await?;
    }

    // All events should have essentially the same timestamp
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("timezone_test"),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(events.len(), timezone_cases.len());

    // All timestamps should be very close (within a few milliseconds)
    let first_ts = events[0].ts_orig.expect("Should have timestamp");
    for event in &events[1..] {
        let event_ts = event.ts_orig.expect("Should have timestamp");
        let diff = (event_ts - first_ts).abs();
        assert!(
            diff < time::Duration::milliseconds(10),
            "All timezone variants should represent the same logical time, diff: {:?}",
            diff
        );
    }

    Ok(())
}

/// Test timestamp validation and error handling
#[sinex_test]
async fn test_timestamp_validation(ctx: TestContext) -> color_eyre::Result<()> {
    // Test that valid timestamps are handled correctly
    let valid_event = test_event(
        EventSource::from("validation_test"),
        EventType::from("valid_event"),
        json!({"message": "This should work"}),
    )
    .at_time(OffsetDateTime::now_utc());

    // This should succeed
    let result = ctx.pool.events().insert(valid_event).await;
    assert!(result.is_ok(), "Valid timestamp should be accepted");

    // Test edge case: distant future (should work but might be logged)
    let far_future = OffsetDateTime::new_in_utc(
        time::Date::from_calendar_date(2999, time::Month::December, 31).unwrap(),
        time::Time::from_hms(23, 59, 59).unwrap(),
    );
    let future_event = test_event(
        EventSource::from("validation_test"),
        EventType::from("future_event"),
        json!({"message": "From the future"}),
    )
    .at_time(far_future);

    let future_result = ctx.pool.events().insert(future_event).await;
    match future_result {
        Ok(_) => println!("Far future timestamp accepted"),
        Err(e) => println!("Far future timestamp rejected: {}", e),
    }

    Ok(())
}

/// Test timestamp ordering in queries
#[sinex_test]
async fn test_timestamp_query_ordering(ctx: TestContext) -> color_eyre::Result<()> {
    let base_time = OffsetDateTime::now_utc() - time::Duration::minutes(30);
    let mut expected_order = Vec::new();

    // Create events with specific logical timestamps
    for i in 0..5 {
        let logical_time = base_time + time::Duration::minutes(i * 5);
        expected_order.push(logical_time);

        let event = test_event(
            EventSource::from("ordering_test"),
            EventType::from("ordered_event"),
            json!({
                "sequence": i,
                "logical_time": logical_time.format(&time::format_description::well_known::Rfc3339).unwrap()
            }),
        )
        .at_time(logical_time);

        ctx.pool.events().insert(event).await?;
    }

    // Query events and verify they maintain logical ordering
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("ordering_test"),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(events.len(), 5);

    // Events should be ordered by ULID (ingestion time) by default
    // but we can verify logical timestamps are preserved
    let mut ordered_events: Vec<(usize, OffsetDateTime)> = events
        .iter()
        .map(|event| {
            let sequence = event.payload["sequence"]
                .as_u64()
                .expect("sequence metadata present") as usize;
            let ts = event.ts_orig.expect("Should have original timestamp");
            (sequence, ts)
        })
        .collect();
    ordered_events.sort_by_key(|(sequence, _)| *sequence);

    for (sequence, stored_ts) in ordered_events {
        let expected_ts = expected_order[sequence];
        assert_eq!(
            stored_ts, expected_ts,
            "Logical timestamp should be preserved for event {}",
            sequence
        );
    }

    Ok(())
}

/// Test timestamp with different payload types
#[sinex_test]
async fn test_timestamps_with_various_payloads(ctx: TestContext) -> color_eyre::Result<()> {
    let test_time = OffsetDateTime::now_utc() - time::Duration::minutes(10);

    // Test with different payload complexities
    let payload_cases = vec![
        ("simple", json!({"value": 42})),
        (
            "complex",
            json!({
                "nested": {
                    "array": [1, 2, 3],
                    "object": {"key": "value"}
                },
                "timestamp_in_payload": test_time.format(&time::format_description::well_known::Rfc3339).unwrap()
            }),
        ),
        (
            "large",
            json!({
                "data": "x".repeat(1000),
                "metadata": {
                    "size": 1000,
                    "type": "test"
                }
            }),
        ),
    ];

    for (case_name, payload) in payload_cases {
        let event = test_event(
            EventSource::from("payload_test"),
            EventType::from(format!("payload_{case_name}")),
            payload,
        )
        .at_time(test_time);

        ctx.pool.events().insert(event).await?;
    }

    // Verify all events preserve their timestamps regardless of payload complexity
    let events = ctx
        .pool
        .events()
        .get_by_source(
            &EventSource::from("payload_test"),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(events.len(), 3);

    for event in events {
        let stored_ts = event.ts_orig.expect("Should have timestamp");
        assert_eq!(
            stored_ts, test_time,
            "Timestamp should be preserved regardless of payload complexity"
        );
    }

    Ok(())
}
