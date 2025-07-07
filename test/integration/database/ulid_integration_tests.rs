use crate::common::prelude::*;
use crate::common::timing_optimization::replacements::wait_for_filtered_event_count;
use crate::common::{assertions, events};

#[sinex_test(timeout = 40)]
async fn test_ulid_ordering_in_database(ctx: TestContext) -> TestResult {
    // Insert multiple events and collect their IDs
    let mut ulids = Vec::new();

    for i in 0..5 {
        let event = events::file_created_event(&format!("/test/file_{}.txt", i));
        let id = assertions::assert_event_inserted(ctx.pool(), &event).await?;
        ulids.push(id);

        // Small delay to ensure ULID monotonic ordering
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    // Query filesystem events to verify ordering
    let filesystem_events =
        crate::common::get_events_by_source(ctx.pool(), "fs", 5).await?;
    let retrieved_ulids: Vec<Ulid> = filesystem_events.iter().map(|e| e.id).collect();

    // Verify strict ordering by comparing ULIDs directly
    for i in 1..retrieved_ulids.len() {
        assert!(
            retrieved_ulids[i] > retrieved_ulids[i - 1],
            "Each ULID should be greater than the previous: {} > {}",
            retrieved_ulids[i],
            retrieved_ulids[i - 1]
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_ulid_timestamp_extraction(ctx: TestContext) -> TestResult {
    // Create an event with a known ULID
    let event = crate::common::create_test_event("timestamp_test_v2", "test_type_v2");
    let expected_timestamp = event.id.timestamp();

    // Insert the event and retrieve it
    let event_id = assertions::assert_event_inserted(ctx.pool(), &event).await?;
    let retrieved_event = crate::common::get_event_by_id(ctx.pool(), event_id).await?;

    // Verify ULID timestamp matches
    let extracted_timestamp = retrieved_event.id.timestamp();
    pretty_assertions::assert_eq!(
        expected_timestamp,
        extracted_timestamp,
        "Extracted timestamp should exactly match ULID timestamp"
    );

    // Verify timestamp is recent
    let age = chrono::Utc::now().signed_duration_since(extracted_timestamp);
    assert!(
        age.num_seconds() < 10,
        "ULID timestamp should be recent (within 10 seconds): age = {} seconds",
        age.num_seconds()
    );

    // Test the generated ts_ingest column matches our ULID timestamp
    let ts_ingest: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT ts_ingest FROM raw.events WHERE source = 'timestamp_test_v2'")
            .fetch_one(ctx.pool())
            .await?;

    // ts_ingest is generated from id::timestamp, should match our extraction
    let ts_ingest_diff = extracted_timestamp.signed_duration_since(ts_ingest);
    assert!(
        ts_ingest_diff.num_milliseconds().abs() <= 1,
        "ts_ingest column should match extracted timestamp: ULID={:?}, ts_ingest={:?}, diff={}ms",
        extracted_timestamp,
        ts_ingest,
        ts_ingest_diff.num_milliseconds()
    );

    Ok(())
}

#[sinex_test(timeout = 35)]
async fn test_ulid_monotonic_generation(ctx: TestContext) -> TestResult {
    // Generate multiple ULIDs rapidly to test monotonic behavior
    let mut _prev_ulid = None;
    let mut ulids = Vec::new();
    let mut unique_check = HashSet::new();

    for i in 0..10 {
        // Note: new_monotonic not available - using regular new()
        let ulid = Ulid::new();
        let ulid_str = ulid.to_string();

        // Verify uniqueness immediately
        assert!(
            unique_check.insert(ulid_str.clone()),
            "Generated non-unique ULID: {}",
            ulid_str
        );

        ulids.push(ulid_str.clone());
        _prev_ulid = Some(ulid);

        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload)
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)",
        )
        .bind(&ulid_str)
        .bind("monotonic_test_v2")
        .bind("test_type_v2")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(ctx.pool())
        .await?;
    }

    // Verify all ULIDs are unique in database using timing utility
    let unique_count =
        wait_for_filtered_event_count(ctx.pool(), "source = $1", &["monotonic_test_v2"], 10, 5)
            .await
            .unwrap_or(0);

    pretty_assertions::assert_eq!(
        unique_count,
        10,
        "All monotonic ULIDs should be unique in database"
    );

    // Verify they're in order in database
    let ordered: Vec<String> = sqlx::query_scalar(
        "SELECT id::text FROM raw.events WHERE source = 'monotonic_test_v2' ORDER BY id",
    )
    .fetch_all(ctx.pool())
    .await?;

    pretty_assertions::assert_eq!(
        ulids,
        ordered,
        "Monotonic ULIDs should maintain order in database"
    );

    // Verify strict monotonic ordering
    for i in 1..ulids.len() {
        let prev = Ulid::from_str(&ulids[i - 1])?;
        let curr = Ulid::from_str(&ulids[i])?;
        assert!(
            curr > prev,
            "Each monotonic ULID should be greater than the previous: {} > {}",
            curr,
            prev
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_ulid_range_queries(ctx: TestContext) -> TestResult {
    // Insert events with significant time separation to ensure reliable range queries
    let mut first_batch_ulids = Vec::new();

    // First batch - insert with delays
    for i in 0..5 {
        let ulid = Ulid::new();
        first_batch_ulids.push(ulid);

        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload)
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)",
        )
        .bind(ulid.to_string())
        .bind("range_test_v2")
        .bind("first_batch")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i, "batch": "first"}))
        .execute(ctx.pool())
        .await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Significant gap between batches
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    let mid_time = chrono::Utc::now();
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Second batch - insert with delays
    let mut second_batch_ulids = Vec::new();
    for i in 5..10 {
        let ulid = Ulid::new();
        second_batch_ulids.push(ulid);

        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload)
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)",
        )
        .bind(ulid.to_string())
        .bind("range_test_v2")
        .bind("second_batch")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i, "batch": "second"}))
        .execute(ctx.pool())
        .await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Wait for all events to be available for range queries
    wait_for_filtered_event_count(
        ctx.pool(),
        "source = $1",
        &["range_test_v2"],
        10, // Total expected events from both batches
        10,
    )
    .await
    .unwrap_or(0);

    // Create a ULID from the mid_time for comparison
    let mid_ulid = Ulid::from_datetime(mid_time);

    // Query using ULID comparison
    let count_before_mid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events
         WHERE source = 'range_test_v2'
         AND id < $1::ulid",
    )
    .bind(mid_ulid.to_string())
    .fetch_one(ctx.pool())
    .await?;

    let count_after_mid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events
         WHERE source = 'range_test_v2'
         AND id >= $1::ulid",
    )
    .bind(mid_ulid.to_string())
    .fetch_one(ctx.pool())
    .await?;

    // Verify range query behavior with better timing separation
    assert!(
        count_before_mid >= 4,
        "Should have at least 4 events before mid time (first batch), got {}",
        count_before_mid
    );
    assert!(
        count_after_mid >= 4,
        "Should have at least 4 events after mid time (second batch), got {}",
        count_after_mid
    );
    pretty_assertions::assert_eq!(
        count_before_mid + count_after_mid,
        10,
        "Total should be 10 events: {} before + {} after = 10",
        count_before_mid,
        count_after_mid
    );

    // Additional verification: check that all first batch ULIDs are before mid_ulid
    for ulid in &first_batch_ulids {
        assert!(
            ulid < &mid_ulid,
            "First batch ULID {} should be before mid_ulid {}",
            ulid,
            mid_ulid
        );
    }

    // And all second batch ULIDs are after mid_ulid
    for ulid in &second_batch_ulids {
        assert!(
            ulid >= &mid_ulid,
            "Second batch ULID {} should be after or equal to mid_ulid {}",
            ulid,
            mid_ulid
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_ulid_in_foreign_keys(ctx: TestContext) -> TestResult {
    // Insert agent
    sqlx::query("INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2)")
        .bind("fk_ulid_test_agent_v2")
        .bind("1.0.0")
        .execute(ctx.pool())
        .await?;

    // Insert event
    let event_id = Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload)
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)",
    )
    .bind(event_id.to_string())
    .bind("fk_test_v2")
    .bind("test_type_v2")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(ctx.pool())
    .await?;

    // Insert work queue item with ULID foreign key
    let queue_id = Ulid::new();
    sqlx::query(
        "INSERT INTO sinex_schemas.work_queue (queue_id, raw_event_id, target_agent_name)
         VALUES ($1::ulid, $2::ulid, $3)",
    )
    .bind(queue_id.to_string())
    .bind(event_id.to_string())
    .bind("fk_ulid_test_agent_v2")
    .execute(ctx.pool())
    .await?;

    // Verify we can query through the foreign key
    let found_event_id: String = sqlx::query_scalar(
        "SELECT e.id::text
         FROM raw.events e
         JOIN sinex_schemas.work_queue pq ON e.id = pq.raw_event_id
         WHERE pq.queue_id = $1::ulid",
    )
    .bind(queue_id.to_string())
    .fetch_one(ctx.pool())
    .await?;

    pretty_assertions::assert_eq!(
        event_id.to_string(),
        found_event_id,
        "Foreign key should work with ULIDs"
    );
    Ok(())
}

#[sinex_test]
async fn test_ulid_index_performance(ctx: TestContext) -> TestResult {
    // Insert events to test indexing and lookup performance
    let mut test_ulids = Vec::new();

    for i in 0..50 {
        let ulid = Ulid::new();
        test_ulids.push(ulid.to_string());

        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload)
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)",
        )
        .bind(ulid.to_string())
        .bind("perf_test_v2")
        .bind(format!("type_{}", i % 10))
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(ctx.pool())
        .await?;
    }

    // Insert a specific test ULID for lookup verification
    let lookup_ulid = Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload)
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)",
    )
    .bind(lookup_ulid.to_string())
    .bind("perf_test_v2")
    .bind("lookup_test")
    .bind("test_host")
    .bind(serde_json::json!({"lookup": true, "special": "target"}))
    .execute(ctx.pool())
    .await?;

    // Update table statistics for accurate query planning
    sqlx::query("ANALYZE raw.events")
        .execute(ctx.pool())
        .await?;

    // Test primary key lookup efficiency
    let found_event_type: Option<String> =
        sqlx::query_scalar("SELECT event_type FROM raw.events WHERE id = $1::ulid")
            .bind(lookup_ulid.to_string())
            .fetch_optional(ctx.pool())
            .await?;

    pretty_assertions::assert_eq!(
        found_event_type,
        Some("lookup_test".to_string()),
        "Should efficiently find event by ULID primary key"
    );

    // Test that we can lookup the specific payload
    let found_payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM raw.events WHERE id = $1::ulid")
            .bind(lookup_ulid.to_string())
            .fetch_one(ctx.pool())
            .await?;

    pretty_assertions::assert_eq!(
        found_payload["special"],
        "target",
        "Should retrieve correct payload for ULID lookup"
    );

    // Test range query performance with ULID ordering
    let mid_index = test_ulids.len() / 2;
    let mid_ulid = &test_ulids[mid_index];

    let count_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events
         WHERE source = 'perf_test_v2' AND id < $1::ulid",
    )
    .bind(mid_ulid)
    .fetch_one(ctx.pool())
    .await?;

    let count_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events
         WHERE source = 'perf_test_v2' AND id >= $1::ulid",
    )
    .bind(mid_ulid)
    .fetch_one(ctx.pool())
    .await?;

    // Verify range queries work correctly
    assert!(count_before > 0, "Should find events before mid ULID");
    assert!(count_after > 0, "Should find events after mid ULID");

    // Total count should be our inserted events - use timing utility
    let total_count = wait_for_filtered_event_count(
        ctx.pool(),
        "source = $1",
        &["perf_test_v2"],
        51, // 50 test events + 1 lookup event
        5,
    )
    .await
    .unwrap_or(0);

    pretty_assertions::assert_eq!(
        total_count,
        51,
        "Should have 50 test events + 1 lookup event = 51 total"
    );
    pretty_assertions::assert_eq!(
        count_before + count_after,
        total_count,
        "Range query counts should sum to total: {} + {} = {}",
        count_before,
        count_after,
        total_count
    );

    Ok(())
}
