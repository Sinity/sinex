use sinex_ulid::Ulid;
use sqlx::postgres::PgPoolOptions;
use std::env;

#[tokio::test]
async fn test_ulid_roundtrip_database() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
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

#[tokio::test]
async fn test_ulid_ordering_in_database() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
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
        .bind("order_test_source")
        .bind("order_test")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await
        .unwrap();
        
        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
    }
    
    // Query events ordered by ID
    let ordered_ids: Vec<String> = sqlx::query_scalar(
        "SELECT id::text FROM raw.events 
         WHERE source = 'order_test_source' 
         ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    
    // Verify they're in the same order as inserted
    assert_eq!(ulids, ordered_ids, "ULIDs should maintain insertion order when sorted");
}

#[tokio::test]
async fn test_ulid_uuid_compatibility() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Create ULID and convert to UUID
    let ulid = Ulid::new();
    let uuid = ulid.to_uuid();
    
    // Insert using ULID
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&ulid.to_string())
    .bind("uuid_compat_test")
    .bind("test_type")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await
    .unwrap();
    
    // Query by casting ULID to UUID
    let stored_uuid: uuid::Uuid = sqlx::query_scalar(
        "SELECT id::uuid FROM raw.events WHERE source = 'uuid_compat_test'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(uuid, stored_uuid, "ULID should convert to UUID correctly in database");
}

#[tokio::test]
async fn test_ulid_timestamp_extraction() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    let ulid = Ulid::new();
    let expected_timestamp = ulid.timestamp();
    
    // Insert ULID
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&ulid.to_string())
    .bind("timestamp_test")
    .bind("test_type")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await
    .unwrap();
    
    // Extract timestamp from stored ULID
    let extracted_ts: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT id::timestamp AT TIME ZONE 'UTC' FROM raw.events WHERE source = 'timestamp_test'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Compare timestamps (allow 1ms difference due to precision)
    let diff = expected_timestamp.signed_duration_since(extracted_ts);
    assert!(
        diff.num_milliseconds().abs() <= 1,
        "Extracted timestamp should match ULID timestamp"
    );
}

#[tokio::test]
async fn test_ulid_monotonic_generation() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Generate multiple ULIDs rapidly
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
        .bind("monotonic_test")
        .bind("test_type")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Verify all ULIDs are unique
    let unique_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT id) FROM raw.events WHERE source = 'monotonic_test'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(unique_count, 10, "All monotonic ULIDs should be unique");
    
    // Verify they're in order
    let ordered: Vec<String> = sqlx::query_scalar(
        "SELECT id::text FROM raw.events WHERE source = 'monotonic_test' ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    
    assert_eq!(ulids, ordered, "Monotonic ULIDs should maintain order");
}

#[tokio::test]
async fn test_ulid_range_queries() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Insert events across time range
    let start_time = chrono::Utc::now();
    
    for i in 0..5 {
        let ulid = Ulid::new();
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid.to_string())
        .bind("range_test")
        .bind("test_type")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await
        .unwrap();
        
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    
    let mid_time = chrono::Utc::now();
    
    for i in 5..10 {
        let ulid = Ulid::new();
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid.to_string())
        .bind("range_test")
        .bind("test_type")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await
        .unwrap();
        
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    
    // Query using timestamp range conversion
    let count_before_mid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events 
         WHERE source = 'range_test' 
         AND id < $1::timestamp::ulid"
    )
    .bind(mid_time)
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(count_before_mid, 5, "Should have 5 events before mid time");
    
    let count_after_mid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events 
         WHERE source = 'range_test' 
         AND id >= $1::timestamp::ulid"
    )
    .bind(mid_time)
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(count_after_mid, 5, "Should have 5 events after mid time");
}

#[tokio::test]
async fn test_ulid_in_foreign_keys() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Insert agent
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2)"
    )
    .bind("fk_ulid_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert event
    let event_id = Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&event_id.to_string())
    .bind("fk_test")
    .bind("test_type")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert promotion queue item with ULID foreign key
    let queue_id = Ulid::new();
    sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue (queue_id, raw_event_id, target_agent_name) 
         VALUES ($1::ulid, $2::ulid, $3)"
    )
    .bind(&queue_id.to_string())
    .bind(&event_id.to_string())
    .bind("fk_ulid_test_agent")
    .execute(&pool)
    .await
    .unwrap();
    
    // Verify we can query through the foreign key
    let found_event_id: String = sqlx::query_scalar(
        "SELECT e.id::text 
         FROM raw.events e
         JOIN sinex_schemas.promotion_queue pq ON e.id = pq.raw_event_id
         WHERE pq.queue_id = $1::ulid"
    )
    .bind(&queue_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(event_id.to_string(), found_event_id, "Foreign key should work with ULIDs");
}

#[tokio::test]
async fn test_ulid_index_performance() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Insert many events
    for i in 0..100 {
        let ulid = Ulid::new();
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&ulid.to_string())
        .bind("perf_test")
        .bind(format!("type_{}", i % 10))
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Analyze table to update statistics
    sqlx::query("ANALYZE raw.events")
        .execute(&pool)
        .await
        .unwrap();
    
    // Query using primary key - should use index
    let explain: String = sqlx::query_scalar(
        "EXPLAIN (FORMAT JSON) SELECT * FROM raw.events WHERE id = gen_ulid()"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(
        explain.contains("Index Scan") || explain.contains("\"Index\""),
        "Query should use index on ULID primary key"
    );
}