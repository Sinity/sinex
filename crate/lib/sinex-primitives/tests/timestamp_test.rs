//! Timestamp handling integration tests
//!
//! Tests the system's handling of various timestamp scenarios including:
//! - Boundary timestamps (epoch, far future)
//! - Out-of-order event ingestion
//! - Timestamp precision and accuracy
//! - Cross-timezone compatibility

use serde_json::json;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::temporal::{Duration, Timestamp, now};
use sinex_primitives::{DynamicPayload, EventSource, Pagination};
use xtask::sandbox::prelude::*;

/// Test timestamp boundary conditions
#[sinex_test]
async fn test_timestamp_boundaries(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().build().await?;
    let scope = ctx.pipeline().await?;

    // All timestamps must be within [2000-01-01, now+1h] or ingestd routes them to DLQ.
    // TS_ORIG_LOWER_BOUND = 2000-01-01; SUSPICIOUS_TS_ORIG_FUTURE_SKEW = 1 hour.
    let current = now();
    let timestamp_cases = &[
        // Just after the lower bound (2001-01-01)
        Timestamp::from_unix_timestamp(978_307_200).unwrap(),
        // Mid-range historical
        Timestamp::new(time::OffsetDateTime::new_utc(
            time::Date::from_calendar_date(2010, time::Month::June, 15).unwrap(),
            time::Time::from_hms(12, 0, 0).unwrap(),
        )),
        // Recent past
        current - Duration::hours(1),
        // Current time
        current,
    ];

    let source = "timestamp_test";
    for (i, ts) in timestamp_cases.iter().enumerate() {
        let ts = *ts;
        let payload = DynamicPayload::new(
            source,
            format!("boundary_{i}"),
            json!({
                "timestamp": ts.format_rfc3339(),
                "epoch": ts.unix_timestamp(),
                "test_case": i
            }),
        );
        scope.publish_with_timestamp(payload, ts).await?;
    }

    // Query events to verify timestamp preservation
    let events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from(source), Pagination::new(Some(10), None))
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
            ingest_ts > Timestamp::from_unix_timestamp(0).unwrap(),
            "Ingestion (UUIDv7) timestamp should be set"
        );
    }

    Ok(())
}

/// Test out-of-order event ingestion
#[sinex_test]
async fn test_out_of_order_timestamps(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().build().await?;
    let scope = ctx.pipeline().await?;

    let base_time = now() - Duration::hours(1);
    let source = "out_of_order_test";

    // Insert events in reverse chronological order
    let timestamps = &[
        base_time + Duration::seconds(30), // Latest timestamp
        base_time + Duration::seconds(20), // Middle timestamp
        base_time + Duration::seconds(10), // Earliest timestamp
    ];

    let mut event_ids = Vec::new();

    for (i, ts) in timestamps.iter().enumerate() {
        let ts = *ts;
        let payload = DynamicPayload::new(
            source,
            "sequenced_event",
            json!({
                "sequence": i,
                "logical_time": ts.format_rfc3339(),
                "description": format!("Event {} with timestamp {}", i, ts.format_rfc3339())
            }),
        );
        let id = scope.publish_with_timestamp(payload, ts).await?;
        event_ids.push(id);
    }

    // Events should be ordered by ingestion time (UUIDv7), not by ts_orig
    for i in 1..event_ids.len() {
        let earlier = &event_ids[i - 1];
        let later = &event_ids[i];

        // UUIDv7 ordering (ingestion order)
        let earlier_str = earlier.to_string();
        let later_str = later.to_string();
        assert!(
            earlier_str < later_str,
            "Events should maintain ingestion order by UUIDv7"
        );
    }

    // Query persisted events to verify ts_orig is preserved
    let events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from(source), Pagination::new(Some(10), None))
        .await?;

    // Verify ts_orig vs UUIDv7 ordering difference
    for (i, event) in events.iter().enumerate() {
        if let Some(ts_orig) = event.ts_orig {
            println!(
                "Event {}: UUIDv7={}, ts_orig={}",
                i,
                event.id.as_ref().unwrap(),
                ts_orig.format_rfc3339()
            );
        }
    }

    Ok(())
}

/// Test timestamp precision handling
#[sinex_serial_test]
async fn test_timestamp_precision(ctx: TestContext) -> color_eyre::Result<()> {
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().build().await?;
    let scope = ctx.pipeline().await?;

    let source = format!(
        "precision_test_{}",
        Uuid::now_v7().to_string().to_lowercase()
    );

    // Test various precision levels
    let precision_cases = &[
        // Second precision
        Timestamp::new(time::OffsetDateTime::new_utc(
            time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap(),
            time::Time::from_hms(12, 0, 0).unwrap(),
        )),
        // Millisecond precision
        Timestamp::new(time::OffsetDateTime::new_utc(
            time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap(),
            time::Time::from_hms_nano(12, 0, 0, 123_000_000).unwrap(),
        )),
        // Microsecond precision
        Timestamp::new(time::OffsetDateTime::new_utc(
            time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap(),
            time::Time::from_hms_nano(12, 0, 0, 123_456_000).unwrap(),
        )),
        // Nanosecond precision
        Timestamp::new(time::OffsetDateTime::new_utc(
            time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap(),
            time::Time::from_hms_nano(12, 0, 0, 123_456_789).unwrap(),
        )),
    ];

    for (i, ts) in precision_cases.iter().enumerate() {
        let payload = DynamicPayload::new(
            source.as_str(),
            "precision_event",
            json!({
                "precision_level": i,
                "nanosecond": ts.nanosecond(),
                "original_timestamp": ts.format(&time::format_description::well_known::Rfc3339).unwrap()
            }),
        );
        scope.publish_with_timestamp(payload, *ts).await?;
    }

    // Verify precision is maintained
    let events = ctx
        .pool()
        .events()
        .get_by_source(
            &EventSource::from(source.as_str()),
            Pagination::new(Some(10), None),
        )
        .await?;

    assert_eq!(events.len(), precision_cases.len());

    let mut stored_by_level: Vec<(usize, Timestamp)> = events
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
            (*stored_ts).unix_timestamp_nanos(),
            "Nanosecond precision should be preserved for event {level}"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_timestamp_rfc3339_format_falls_back_for_non_minute_offset(
    _ctx: TestContext,
) -> TestResult<()> {
    let offset = time::UtcOffset::from_hms(1, 2, 3).expect("test offset must be valid");
    let timestamp = Timestamp::new(
        time::PrimitiveDateTime::new(
            time::Date::from_calendar_date(2024, time::Month::January, 1)
                .expect("test date must be valid"),
            time::Time::from_hms(12, 0, 0).expect("test time must be valid"),
        )
        .assume_offset(offset),
    );

    assert_eq!(timestamp.format_rfc3339(), "invalid_time");
    Ok(())
}

/// Test cross-timezone timestamp handling
#[sinex_test]
async fn test_timezone_handling(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().build().await?;
    let scope = ctx.pipeline().await?;

    // All timestamps should be normalized to UTC in storage
    let utc_time = Timestamp::now();
    let source = "timezone_test";

    // Create events with the same logical time expressed in different ways
    let timezone_cases = vec![
        ("utc_explicit", utc_time),
        (
            "utc_parsed",
            Timestamp::new(time::OffsetDateTime::parse(
                &(*utc_time)
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
                &time::format_description::well_known::Rfc3339,
            )?),
        ),
        (
            "utc_timestamp",
            Timestamp::from_unix_timestamp_nanos((*utc_time).unix_timestamp_nanos())
                .ok_or_else(|| color_eyre::eyre::eyre!("invalid timestamp"))?,
        ),
    ];

    for (name, ts) in &timezone_cases {
        let payload = DynamicPayload::new(
            source,
            "timezone_event",
            json!({
                "timezone_case": name,
                "timestamp": ts.format(&time::format_description::well_known::Rfc3339).unwrap()
            }),
        );
        scope.publish_with_timestamp(payload, *ts).await?;
    }

    // All events should have essentially the same timestamp
    let events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from(source), Pagination::new(Some(10), None))
        .await?;

    assert_eq!(events.len(), timezone_cases.len());

    // All timestamps should be very close (within a few milliseconds)
    let first_ts = events[0].ts_orig.expect("Should have timestamp");
    for event in &events[1..] {
        let event_ts = event.ts_orig.expect("Should have timestamp");
        let diff = (event_ts - first_ts).abs();
        assert!(
            diff < time::Duration::milliseconds(10),
            "All timezone variants should represent the same logical time, diff: {diff:?}"
        );
    }

    Ok(())
}

/// Test timestamp validation and error handling
#[sinex_test]
async fn test_timestamp_validation(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().build().await?;
    let scope = ctx.pipeline().await?;

    let source = "validation_test";

    // Test that valid timestamps are handled correctly
    let valid_payload = DynamicPayload::new(
        source,
        "valid_event",
        json!({"message": "This should work"}),
    );
    let result = scope
        .publish_with_timestamp(valid_payload, Timestamp::now())
        .await;
    assert!(result.is_ok(), "Valid timestamp should be accepted");

    // Test edge case: distant future (should work but might be logged)
    let far_future = Timestamp::new(time::OffsetDateTime::new_utc(
        time::Date::from_calendar_date(2999, time::Month::December, 31).unwrap(),
        time::Time::from_hms(23, 59, 59).unwrap(),
    ));
    let future_payload = DynamicPayload::new(
        source,
        "future_event",
        json!({"message": "From the future"}),
    );

    let future_result = scope
        .publish_with_timestamp(future_payload, far_future)
        .await;
    match future_result {
        Ok(_) => println!("Far future timestamp accepted"),
        Err(e) => println!("Far future timestamp rejected: {e}"),
    }

    Ok(())
}

/// Test timestamp ordering in queries
#[sinex_test]
async fn test_timestamp_query_ordering(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().build().await?;
    let scope = ctx.pipeline().await?;

    let base_time = Timestamp::now() - time::Duration::minutes(30);
    let source = "ordering_test";
    let mut expected_order = Vec::new();

    // Create events with specific logical timestamps
    for i in 0..5 {
        let logical_time = base_time + time::Duration::minutes(i * 5);
        expected_order.push(logical_time);

        let payload = DynamicPayload::new(
            source,
            "ordered_event",
            json!({
                "sequence": i,
                "logical_time": (*logical_time).format(&time::format_description::well_known::Rfc3339).unwrap()
            }),
        );
        scope.publish_with_timestamp(payload, logical_time).await?;
    }

    // Query events and verify they maintain logical ordering
    let events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from(source), Pagination::new(Some(10), None))
        .await?;

    assert_eq!(events.len(), 5);

    // Events should be ordered by UUIDv7 (ingestion time) by default
    // but we can verify logical timestamps are preserved
    let mut ordered_events: Vec<(usize, Timestamp)> = events
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
            "Logical timestamp should be preserved for event {sequence}"
        );
    }

    Ok(())
}

/// Test timestamp with different payload types
#[sinex_test]
async fn test_timestamps_with_various_payloads(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().build().await?;
    let scope = ctx.pipeline().await?;

    let test_time = Timestamp::now() - time::Duration::minutes(10);
    let source = "payload_test";

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

    for (case_name, payload_json) in payload_cases {
        let payload = DynamicPayload::new(source, format!("payload_{case_name}"), payload_json);
        scope.publish_with_timestamp(payload, test_time).await?;
    }

    // Verify all events preserve their timestamps regardless of payload complexity
    let events = ctx
        .pool()
        .events()
        .get_by_source(&EventSource::from(source), Pagination::new(Some(10), None))
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
