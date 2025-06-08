use sinex_ulid::Ulid;
use std::str::FromStr;
use std::collections::HashSet;

#[sqlx::test]
async fn test_ulid_roundtrip_database(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Generate ULID in Rust
    let ulid = Ulid::new();
    let ulid_string = ulid.to_string();
    
    // Insert into database
    let inserted_id: String = sqlx::query_scalar(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb) 
         RETURNING id::text"
    )
    .bind(&ulid_string)
    .bind("roundtrip_test_v2")
    .bind("test_type_v2")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(ulid_string, inserted_id, "ULID should roundtrip correctly");
    Ok(())
}

#[sqlx::test]
async fn test_ulid_ordering_in_database(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Insert multiple events with delays to ensure proper ordering
    let mut ulids = Vec::new();
    
    for i in 0..5 {
        let ulid = Ulid::new();
        ulids.push(ulid.to_string());
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid.to_string())
        .bind("order_test_source_v2")
        .bind("order_test_v2")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await?;
        
        // Ensure different timestamps by sleeping
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    
    // Query events ordered by ID
    let ordered_ids: Vec<String> = sqlx::query_scalar(
        "SELECT id::text FROM raw.events 
         WHERE source = 'order_test_source_v2' 
         ORDER BY id"
    )
    .fetch_all(&pool)
    .await?;
    
    // Verify they're in the same order as inserted
    assert_eq!(ulids, ordered_ids, "ULIDs should maintain insertion order when sorted");
    
    // Also verify strict ordering by comparing ULIDs directly
    for i in 1..ulids.len() {
        let prev_ulid = Ulid::from_str(&ulids[i-1])?;
        let curr_ulid = Ulid::from_str(&ulids[i])?;
        assert!(curr_ulid > prev_ulid, 
            "Each ULID should be greater than the previous: {} > {}", 
            curr_ulid, prev_ulid);
    }
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_uuid_compatibility(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // Create ULID and convert to UUID
    let ulid = Ulid::new();
    let uuid = ulid.to_uuid();
    
    // Insert using ULID
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&ulid.to_string())
    .bind("uuid_compat_test_v2")
    .bind("test_type_v2")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await?;
    
    // Query by casting ULID to UUID
    let stored_uuid: uuid::Uuid = sqlx::query_scalar(
        "SELECT id::uuid FROM raw.events WHERE source = 'uuid_compat_test_v2'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(uuid, stored_uuid, "ULID should convert to UUID correctly in database");
    Ok(())
}

#[sqlx::test]
async fn test_ulid_timestamp_extraction(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let ulid = Ulid::new();
    let expected_timestamp = ulid.timestamp();
    
    // Insert ULID
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&ulid.to_string())
    .bind("timestamp_test_v2")
    .bind("test_type_v2")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await?;
    
    // Extract ULID as string and parse timestamp in Rust
    let stored_ulid_str: String = sqlx::query_scalar(
        "SELECT id::text FROM raw.events WHERE source = 'timestamp_test_v2'"
    )
    .fetch_one(&pool)
    .await?;
    
    let stored_ulid = Ulid::from_str(&stored_ulid_str)
        .map_err(|e| format!("Failed to parse stored ULID: {}", e))?;
    let extracted_timestamp = stored_ulid.timestamp();
    
    // Compare timestamps (should be identical since it's the same ULID)
    assert_eq!(expected_timestamp, extracted_timestamp,
        "Extracted timestamp should exactly match ULID timestamp: expected {:?}, got {:?}",
        expected_timestamp, extracted_timestamp
    );
    
    // Also verify that the extracted timestamp is reasonable (within last few seconds)
    let now = chrono::Utc::now();
    let age = now.signed_duration_since(extracted_timestamp);
    assert!(age.num_seconds() < 10, 
        "ULID timestamp should be recent (within 10 seconds): age = {} seconds", 
        age.num_seconds());
    
    // Test the generated ts_ingest column matches our ULID timestamp
    let ts_ingest: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT ts_ingest FROM raw.events WHERE source = 'timestamp_test_v2'"
    )
    .fetch_one(&pool)
    .await?;
    
    // ts_ingest is generated from id::timestamp, should match our extraction
    let ts_ingest_diff = extracted_timestamp.signed_duration_since(ts_ingest);
    assert!(ts_ingest_diff.num_milliseconds().abs() <= 1,
        "ts_ingest column should match extracted timestamp: ULID={:?}, ts_ingest={:?}, diff={}ms",
        extracted_timestamp, ts_ingest, ts_ingest_diff.num_milliseconds());
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_monotonic_generation(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Generate multiple ULIDs rapidly to test monotonic behavior
    let mut prev_ulid = None;
    let mut ulids = Vec::new();
    let mut unique_check = HashSet::new();
    
    for i in 0..10 {
        let ulid = Ulid::new_monotonic(prev_ulid.as_ref());
        let ulid_str = ulid.to_string();
        
        // Verify uniqueness immediately
        assert!(unique_check.insert(ulid_str.clone()), 
            "Generated non-unique ULID: {}", ulid_str);
        
        ulids.push(ulid_str.clone());
        prev_ulid = Some(ulid);
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid_str)
        .bind("monotonic_test_v2")
        .bind("test_type_v2")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await?;
    }
    
    // Verify all ULIDs are unique in database
    let unique_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT id) FROM raw.events WHERE source = 'monotonic_test_v2'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(unique_count, 10, "All monotonic ULIDs should be unique in database");
    
    // Verify they're in order in database
    let ordered: Vec<String> = sqlx::query_scalar(
        "SELECT id::text FROM raw.events WHERE source = 'monotonic_test_v2' ORDER BY id"
    )
    .fetch_all(&pool)
    .await?;
    
    assert_eq!(ulids, ordered, "Monotonic ULIDs should maintain order in database");
    
    // Verify strict monotonic ordering
    for i in 1..ulids.len() {
        let prev = Ulid::from_str(&ulids[i-1])?;
        let curr = Ulid::from_str(&ulids[i])?;
        assert!(curr > prev, 
            "Each monotonic ULID should be greater than the previous: {} > {}", 
            curr, prev);
    }
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_range_queries(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Insert events with significant time separation to ensure reliable range queries
    let mut first_batch_ulids = Vec::new();
    
    // First batch - insert with delays
    for i in 0..5 {
        let ulid = Ulid::new();
        first_batch_ulids.push(ulid);
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid.to_string())
        .bind("range_test_v2")
        .bind("first_batch")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i, "batch": "first"}))
        .execute(&pool)
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
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid.to_string())
        .bind("range_test_v2")
        .bind("second_batch")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i, "batch": "second"}))
        .execute(&pool)
        .await?;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    
    // Create a ULID from the mid_time for comparison
    let mid_ulid = Ulid::from_datetime(mid_time);
    
    // Query using ULID comparison
    let count_before_mid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events 
         WHERE source = 'range_test_v2' 
         AND id < $1::ulid"
    )
    .bind(mid_ulid.to_string())
    .fetch_one(&pool)
    .await?;
    
    let count_after_mid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events 
         WHERE source = 'range_test_v2' 
         AND id >= $1::ulid"
    )
    .bind(mid_ulid.to_string())
    .fetch_one(&pool)
    .await?;
    
    // Verify range query behavior with better timing separation
    assert!(count_before_mid >= 4, 
        "Should have at least 4 events before mid time (first batch), got {}", 
        count_before_mid);
    assert!(count_after_mid >= 4, 
        "Should have at least 4 events after mid time (second batch), got {}", 
        count_after_mid);
    assert_eq!(count_before_mid + count_after_mid, 10, 
        "Total should be 10 events: {} before + {} after = 10", 
        count_before_mid, count_after_mid);
    
    // Additional verification: check that all first batch ULIDs are before mid_ulid
    for ulid in &first_batch_ulids {
        assert!(ulid < &mid_ulid, 
            "First batch ULID {} should be before mid_ulid {}", 
            ulid, mid_ulid);
    }
    
    // And all second batch ULIDs are after mid_ulid
    for ulid in &second_batch_ulids {
        assert!(ulid >= &mid_ulid, 
            "Second batch ULID {} should be after or equal to mid_ulid {}", 
            ulid, mid_ulid);
    }
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_in_foreign_keys(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // Insert agent
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2)"
    )
    .bind("fk_ulid_test_agent_v2")
    .bind("1.0.0")
    .execute(&pool)
    .await?;
    
    // Insert event
    let event_id = Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&event_id.to_string())
    .bind("fk_test_v2")
    .bind("test_type_v2")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await?;
    
    // Insert promotion queue item with ULID foreign key
    let queue_id = Ulid::new();
    sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue (queue_id, raw_event_id, target_agent_name) 
         VALUES ($1::ulid, $2::ulid, $3)"
    )
    .bind(&queue_id.to_string())
    .bind(&event_id.to_string())
    .bind("fk_ulid_test_agent_v2")
    .execute(&pool)
    .await?;
    
    // Verify we can query through the foreign key
    let found_event_id: String = sqlx::query_scalar(
        "SELECT e.id::text 
         FROM raw.events e
         JOIN sinex_schemas.promotion_queue pq ON e.id = pq.raw_event_id
         WHERE pq.queue_id = $1::ulid"
    )
    .bind(&queue_id.to_string())
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(event_id.to_string(), found_event_id, "Foreign key should work with ULIDs");
    Ok(())
}

#[sqlx::test]
async fn test_ulid_index_performance(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Insert events to test indexing and lookup performance
    let mut test_ulids = Vec::new();
    
    for i in 0..50 {
        let ulid = Ulid::new();
        test_ulids.push(ulid.to_string());
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid.to_string())
        .bind("perf_test_v2")
        .bind(format!("type_{}", i % 10))
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await?;
    }
    
    // Insert a specific test ULID for lookup verification
    let lookup_ulid = Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&lookup_ulid.to_string())
    .bind("perf_test_v2")
    .bind("lookup_test")
    .bind("test_host")
    .bind(serde_json::json!({"lookup": true, "special": "target"}))
    .execute(&pool)
    .await?;
    
    // Update table statistics for accurate query planning
    sqlx::query("ANALYZE raw.events")
        .execute(&pool)
        .await?;
    
    // Test primary key lookup efficiency
    let found_event_type: Option<String> = sqlx::query_scalar(
        "SELECT event_type FROM raw.events WHERE id = $1::ulid"
    )
    .bind(&lookup_ulid.to_string())
    .fetch_optional(&pool)
    .await?;
    
    assert_eq!(found_event_type, Some("lookup_test".to_string()), 
        "Should efficiently find event by ULID primary key");
    
    // Test that we can lookup the specific payload  
    let found_payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM raw.events WHERE id = $1::ulid"
    )
    .bind(&lookup_ulid.to_string())
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(found_payload["special"], "target", 
        "Should retrieve correct payload for ULID lookup");
    
    // Test range query performance with ULID ordering
    let mid_index = test_ulids.len() / 2;
    let mid_ulid = &test_ulids[mid_index];
    
    let count_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events 
         WHERE source = 'perf_test_v2' AND id < $1::ulid"
    )
    .bind(mid_ulid)
    .fetch_one(&pool)
    .await?;
    
    let count_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events 
         WHERE source = 'perf_test_v2' AND id >= $1::ulid"
    )
    .bind(mid_ulid)
    .fetch_one(&pool)
    .await?;
    
    // Verify range queries work correctly
    assert!(count_before > 0, "Should find events before mid ULID");
    assert!(count_after > 0, "Should find events after mid ULID");
    
    // Total count should be our inserted events
    let total_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE source = 'perf_test_v2'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(total_count, 51, "Should have 50 test events + 1 lookup event = 51 total");
    assert_eq!(count_before + count_after, total_count, 
        "Range query counts should sum to total: {} + {} = {}", 
        count_before, count_after, total_count);
    
    Ok(())
}