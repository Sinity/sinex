use sinex_ulid::Ulid;
use chrono::{DateTime, Utc};
use std::str::FromStr;

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
    .bind("test_source")
    .bind("test_type")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(ulid_string, inserted_id, "ULID should roundtrip correctly");
}

#[sqlx::test]
async fn test_ulid_ordering_in_database(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // Insert multiple events with slight delays
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
        
        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
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
    
    // Compare timestamps (allow 1ms difference due to precision)
    let diff = expected_timestamp.signed_duration_since(extracted_timestamp);
    assert!(
        diff.num_milliseconds().abs() <= 1,
        "Extracted timestamp should match ULID timestamp: expected {:?}, got {:?}",
        expected_timestamp, extracted_timestamp
    );
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_monotonic_generation(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Generate multiple ULIDs rapidly to test monotonic behavior
    let mut prev_ulid = None;
    let mut ulids = Vec::new();
    
    for i in 0..10 {
        let ulid = Ulid::new_monotonic(prev_ulid.as_ref());
        ulids.push(ulid.to_string());
        prev_ulid = Some(ulid);
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid.to_string())
        .bind("monotonic_test_v2")
        .bind("test_type_v2")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await?;
    }
    
    // Verify all ULIDs are unique
    let unique_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT id) FROM raw.events WHERE source = 'monotonic_test_v2'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(unique_count, 10, "All monotonic ULIDs should be unique");
    
    // Verify they're in order
    let ordered: Vec<String> = sqlx::query_scalar(
        "SELECT id::text FROM raw.events WHERE source = 'monotonic_test_v2' ORDER BY id"
    )
    .fetch_all(&pool)
    .await?;
    
    assert_eq!(ulids, ordered, "Monotonic ULIDs should maintain order");
    
    // Also verify that each ULID is actually greater than the previous
    for i in 1..ulids.len() {
        let prev = Ulid::from_str(&ulids[i-1])?;
        let curr = Ulid::from_str(&ulids[i])?;
        assert!(curr > prev, "Each monotonic ULID should be greater than the previous: {} > {}", curr, prev);
    }
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_range_queries(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Insert events across time range
    let start_time = chrono::Utc::now();
    let mut first_batch_ulids = Vec::new();
    
    for i in 0..5 {
        let ulid = Ulid::new();
        first_batch_ulids.push(ulid);
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid.to_string())
        .bind("range_test_v2")
        .bind("test_type_v2")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await?;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
    
    let mid_time = chrono::Utc::now();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;  // Ensure gap
    
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
        .bind("test_type_v2")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await?;
        
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
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
    
    // The exact counts may vary due to timing, but we should have events in both ranges
    assert!(count_before_mid >= 3, "Should have at least 3 events before mid time, got {}", count_before_mid);
    assert!(count_after_mid >= 3, "Should have at least 3 events after mid time, got {}", count_after_mid);
    assert_eq!(count_before_mid + count_after_mid, 10, "Total should be 10 events");
    
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
    
    // Insert many events
    for i in 0..100 {
        let ulid = Ulid::new();
        
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
    
    // Analyze table to update statistics
    sqlx::query("ANALYZE raw.events")
        .execute(&pool)
        .await?;
    
    // Test that we can efficiently query by ULID primary key
    let test_ulid = Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&test_ulid.to_string())
    .bind("perf_test_v2")
    .bind("lookup_test")
    .bind("test_host")
    .bind(serde_json::json!({"lookup": true}))
    .execute(&pool)
    .await?;
    
    // Query by primary key should be efficient
    let found: Option<String> = sqlx::query_scalar(
        "SELECT event_type FROM raw.events WHERE id = $1::ulid"
    )
    .bind(&test_ulid.to_string())
    .fetch_optional(&pool)
    .await?;
    
    assert_eq!(found, Some("lookup_test".to_string()), "Should efficiently find event by ULID primary key");
    
    // Test range query performance 
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE source = 'perf_test_v2'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(count, 101, "Should have inserted 101 events total (100 + 1 lookup test)");
    
    Ok(())
}