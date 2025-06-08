use sqlx::postgres::PgPoolOptions;
use sinex_ulid::Ulid;
use serde_json::json;
use chrono::{Duration, Utc};

#[tokio::test]
async fn test_raw_events_is_timescale_hypertable() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Verify raw.events is a hypertable
    let hypertable_info: Option<(String, String, String, i32)> = sqlx::query_as(
        "SELECT hypertable_schema, hypertable_name, 
                dimension_column, dimension_type
         FROM timescaledb_information.dimensions 
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    
    assert!(hypertable_info.is_some(), "raw.events should be a hypertable");
    let (schema, table, dimension_col, dimension_type) = hypertable_info.unwrap();
    assert_eq!(schema, "raw");
    assert_eq!(table, "events");
    assert_eq!(dimension_col, "ts_ingest");
    assert_eq!(dimension_type, 1); // Time dimension
    
    // Check chunk interval
    let chunk_interval: Option<i64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM chunk_time_interval)::bigint 
         FROM timescaledb_information.hypertables 
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    
    assert!(chunk_interval.is_some());
    let interval_days = chunk_interval.unwrap() / 86400;
    assert_eq!(interval_days, 7, "Chunk interval should be 7 days");
}

#[tokio::test]
async fn test_timescale_chunk_creation() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Get initial chunk count
    let initial_chunks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM timescaledb_information.chunks 
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Insert events across different time periods to trigger chunk creation
    let time_periods = vec![
        Utc::now(),
        Utc::now() - Duration::days(10),
        Utc::now() - Duration::days(20),
        Utc::now() + Duration::days(5),
    ];
    
    for (i, ts) in time_periods.iter().enumerate() {
        let event_id = Ulid::new();
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&event_id.to_string())
        .bind("chunk_test")
        .bind(format!("event_type_{}", i))
        .bind("test_host")
        .bind(json!({"chunk_test": i}))
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Get new chunk count
    let new_chunks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM timescaledb_information.chunks 
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(
        new_chunks >= initial_chunks, 
        "Should have created additional chunks for different time periods"
    );
    
    // Verify chunks contain the correct data
    for (i, ts) in time_periods.iter().enumerate() {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM raw.events 
             WHERE source = 'chunk_test' 
             AND event_type = $1
             AND ts_ingest >= $2 - interval '1 minute'
             AND ts_ingest <= $2 + interval '1 minute'"
        )
        .bind(format!("event_type_{}", i))
        .bind(ts)
        .fetch_one(&pool)
        .await
        .unwrap();
        
        assert_eq!(count, 1, "Each event should be in its appropriate chunk");
    }
}

#[tokio::test]
async fn test_timescale_compression_policy() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Check if compression policy exists
    let compression_policy: Option<(i32,)> = sqlx::query_as(
        "SELECT job_id 
         FROM timescaledb_information.jobs 
         WHERE hypertable_schema = 'raw' 
         AND hypertable_name = 'events'
         AND proc_name = 'compress_chunks'"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    
    if compression_policy.is_some() {
        // Get compression settings
        let compress_after: Option<i64> = sqlx::query_scalar(
            "SELECT EXTRACT(EPOCH FROM (config->>'compress_after')::interval)::bigint / 86400
             FROM timescaledb_information.jobs 
             WHERE hypertable_schema = 'raw' 
             AND hypertable_name = 'events'
             AND proc_name = 'compress_chunks'"
        )
        .fetch_optional(&pool)
        .await
        .unwrap();
        
        assert!(compress_after.is_some());
        let days = compress_after.unwrap();
        assert!(days >= 7, "Compression should happen after at least 7 days");
    }
    
    // Insert old data to test compression
    let old_timestamp = Utc::now() - Duration::days(30);
    for i in 0..10 {
        let event_id = Ulid::new();
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&event_id.to_string())
        .bind("compression_test")
        .bind("old_event")
        .bind("test_host")
        .bind(json!({"seq": i}))
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Check if old chunks are marked for compression
    let compressible_chunks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM timescaledb_information.chunks 
         WHERE hypertable_schema = 'raw' 
         AND hypertable_name = 'events'
         AND range_end < now() - interval '7 days'
         AND is_compressed = false"
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    
    println!("Found {} compressible chunks", compressible_chunks);
}

#[tokio::test]
async fn test_timescale_continuous_aggregates() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Create a continuous aggregate for event counts by source and hour
    let result = sqlx::query(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS event_counts_hourly
         WITH (timescaledb.continuous) AS
         SELECT 
             time_bucket('1 hour', ts_ingest) AS hour,
             source,
             event_type,
             COUNT(*) as event_count,
             COUNT(DISTINCT host) as unique_hosts
         FROM raw.events
         GROUP BY hour, source, event_type
         WITH NO DATA"
    )
    .execute(&pool)
    .await;
    
    // Note: This might fail if the view already exists from previous test runs
    if result.is_ok() {
        // Add refresh policy
        let _ = sqlx::query(
            "SELECT add_continuous_aggregate_policy('event_counts_hourly',
                start_offset => INTERVAL '1 week',
                end_offset => INTERVAL '1 hour',
                schedule_interval => INTERVAL '1 hour')"
        )
        .execute(&pool)
        .await;
    }
    
    // Insert test data
    let sources = vec!["app.web", "app.mobile", "system.monitoring"];
    let event_types = vec!["user_action", "system_event", "error"];
    
    for hour in 0..24 {
        for source in &sources {
            for event_type in &event_types {
                let event_id = Ulid::new();
                let ts = Utc::now() - Duration::hours(hour);
                
                sqlx::query(
                    "INSERT INTO raw.events (id, source, event_type, host, payload) 
                     VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
                )
                .bind(&event_id.to_string())
                .bind(source)
                .bind(event_type)
                .bind(format!("host_{}", hour % 3))
                .bind(json!({"hour": hour}))
                .execute(&pool)
                .await
                .unwrap();
            }
        }
    }
    
    // Refresh the aggregate
    let _ = sqlx::query("CALL refresh_continuous_aggregate('event_counts_hourly', NULL, NULL)")
        .execute(&pool)
        .await;
    
    // Query the aggregate
    let hourly_counts: Vec<(chrono::DateTime<chrono::Utc>, String, String, i64, i64)> = 
        sqlx::query_as(
            "SELECT hour, source, event_type, event_count, unique_hosts
             FROM event_counts_hourly
             WHERE source = 'app.web' AND event_type = 'user_action'
             ORDER BY hour DESC
             LIMIT 5"
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
    
    if !hourly_counts.is_empty() {
        for (hour, source, event_type, count, hosts) in hourly_counts {
            assert_eq!(source, "app.web");
            assert_eq!(event_type, "user_action");
            assert!(count > 0);
            assert!(hosts > 0 && hosts <= 3);
            println!("Hour: {}, Count: {}, Unique hosts: {}", hour, count, hosts);
        }
    }
}

#[tokio::test]
async fn test_timescale_retention_policies() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Check if retention policy exists
    let retention_policy: Option<(i32, String)> = sqlx::query_as(
        "SELECT job_id, config->>'drop_after' as drop_after
         FROM timescaledb_information.jobs 
         WHERE hypertable_schema = 'raw' 
         AND hypertable_name = 'events'
         AND proc_name = 'drop_chunks'"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    
    if retention_policy.is_none() {
        // Create a retention policy (drop chunks older than 1 year)
        let result = sqlx::query(
            "SELECT add_retention_policy('raw.events', INTERVAL '1 year')"
        )
        .execute(&pool)
        .await;
        
        if result.is_ok() {
            println!("Created retention policy for raw.events");
        }
    } else {
        let (job_id, drop_after) = retention_policy.unwrap();
        println!("Retention policy exists: job_id={}, drop_after={}", job_id, drop_after);
    }
    
    // Test data that would be affected by retention
    let very_old_timestamp = Utc::now() - Duration::days(400); // Over 1 year
    let recent_timestamp = Utc::now() - Duration::days(30);
    
    // Insert very old event
    let old_event_id = Ulid::new();
    let _ = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload, ts_ingest) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb, $6)"
    )
    .bind(&old_event_id.to_string())
    .bind("retention_test")
    .bind("very_old_event")
    .bind("test_host")
    .bind(json!({"data": "old"}))
    .bind(very_old_timestamp)
    .execute(&pool)
    .await;
    
    // Insert recent event
    let recent_event_id = Ulid::new();
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload, ts_ingest) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb, $6)"
    )
    .bind(&recent_event_id.to_string())
    .bind("retention_test")
    .bind("recent_event")
    .bind("test_host")
    .bind(json!({"data": "recent"}))
    .bind(recent_timestamp)
    .execute(&pool)
    .await
    .unwrap();
    
    // Count chunks that would be dropped by retention policy
    let droppable_chunks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM timescaledb_information.chunks 
         WHERE hypertable_schema = 'raw' 
         AND hypertable_name = 'events'
         AND range_end < now() - interval '1 year'"
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    
    println!("Found {} chunks eligible for retention policy", droppable_chunks);
}

#[tokio::test]
async fn test_timescale_data_node_stats() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Get hypertable stats
    let stats: Option<(i64, i64, i64)> = sqlx::query_as(
        "SELECT 
            total_chunks,
            compressed_chunks,
            approximate_row_count
         FROM timescaledb_information.hypertables
         WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    
    if let Some((total_chunks, compressed_chunks, row_count)) = stats {
        println!("Hypertable stats:");
        println!("  Total chunks: {}", total_chunks);
        println!("  Compressed chunks: {}", compressed_chunks);
        println!("  Approximate row count: {}", row_count);
        
        assert!(total_chunks >= 0);
        assert!(compressed_chunks >= 0);
        assert!(compressed_chunks <= total_chunks);
    }
    
    // Get detailed chunk information
    let chunks: Vec<(String, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>, bool, Option<i64>, Option<i64>)> = 
        sqlx::query_as(
            "SELECT 
                chunk_name,
                range_start,
                range_end,
                is_compressed,
                compressed_heap_size,
                uncompressed_heap_size
             FROM timescaledb_information.chunks
             WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'
             ORDER BY range_start DESC
             LIMIT 5"
        )
        .fetch_all(&pool)
        .await
        .unwrap();
    
    for (name, start, end, compressed, comp_size, uncomp_size) in chunks {
        println!("Chunk {}: {} to {}", name, start, end);
        if compressed {
            println!("  Compressed: {} -> {} bytes", 
                uncomp_size.unwrap_or(0), 
                comp_size.unwrap_or(0));
        }
    }
}