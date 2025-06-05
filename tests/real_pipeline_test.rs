use chrono::Utc;
use serde_json::json;
use sinex_shared::{DatabaseService, RawEventBuilder, sources, event_type_constants};
use std::time::Duration;

/// Simple test that verifies we can insert events and they appear in the database
#[sqlx::test]
async fn test_basic_event_insertion(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Create a test event
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/integration.txt",
            "size": 2048,
            "test_id": "basic_insertion"
        })
    )
    .with_orig_timestamp(Utc::now())
    .build();

    // Insert it
    let event_id = db_service.insert_event(&event).await?;
    println!("Inserted event with ID: {}", event_id);

    // Query it back to verify
    let row = sqlx::query!(
        r#"
        SELECT source, event_type, host, payload
        FROM raw.events
        WHERE id = $1
        "#,
        event_id
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(row.source, sources::FILESYSTEM);
    assert_eq!(row.event_type, event_type_constants::filesystem::FILE_CREATED);
    assert_eq!(row.payload["path"], "/test/integration.txt");
    assert_eq!(row.payload["test_id"], "basic_insertion");

    Ok(())
}

/// Test that the event router trigger creates promotion queue entries
#[sqlx::test]
async fn test_event_router_creates_queue_entries(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // First, ensure we have an agent that subscribes to events
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
            (agent_name, version, status, agent_type, subscribes_to_event_types)
        VALUES 
            ('test-subscriber', '1.0.0', 'running', 'promoter', 
             '{"raw.events_feed_all": [{"source_filter": "filesystem"}]}'::jsonb)
        ON CONFLICT (agent_name) DO UPDATE 
        SET status = 'running', 
            subscribes_to_event_types = '{"raw.events_feed_all": [{"source_filter": "filesystem"}]}'::jsonb
        "#
    )
    .execute(&pool)
    .await?;

    // Insert an event
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_MODIFIED,
        json!({
            "path": "/test/router_test.txt",
            "test_id": "router_trigger"
        })
    ).build();

    let event_id = db_service.insert_event(&event).await?;

    // Give the trigger a moment to execute
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check promotion queue
    let queue_entries = sqlx::query!(
        r#"
        SELECT queue_id, target_agent_name, status
        FROM sinex_schemas.promotion_queue
        WHERE raw_event_id = $1
        "#,
        event_id
    )
    .fetch_all(&pool)
    .await?;

    assert_eq!(queue_entries.len(), 1, "Should have one promotion queue entry");
    assert_eq!(queue_entries[0].target_agent_name, "test-subscriber");
    assert_eq!(queue_entries[0].status, "pending");

    Ok(())
}

/// Test batch insertion works correctly
#[sqlx::test]
async fn test_batch_event_insertion(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Create multiple events
    let events: Vec<_> = (0..5)
        .map(|i| {
            RawEventBuilder::new(
                sources::FILESYSTEM,
                event_type_constants::filesystem::FILE_CREATED,
                json!({
                    "path": format!("/test/batch_{}.txt", i),
                    "index": i,
                    "batch_id": "test_batch_001"
                })
            ).build()
        })
        .collect();

    // Insert them all
    let ids = db_service.insert_events_batch(&events).await?;
    assert_eq!(ids.len(), 5);

    // Verify they're all in the database
    let count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*)::int
        FROM raw.events
        WHERE payload->>'batch_id' = 'test_batch_001'
        "#
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(count, 5);

    Ok(())
}

/// Test ULID ordering ensures time-based sorting
#[sqlx::test]
async fn test_ulid_time_ordering(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Insert events with small delays to ensure different timestamps
    let mut timestamps = Vec::new();
    
    for i in 0..3 {
        let now = Utc::now();
        timestamps.push(now);
        
        let event = RawEventBuilder::new(
            sources::SINEX,
            event_type_constants::sinex::AGENT_HEARTBEAT,
            json!({
                "sequence": i,
                "timestamp": now.to_rfc3339()
            })
        )
        .with_orig_timestamp(now)
        .build();

        db_service.insert_event(&event).await?;
        
        // Small delay to ensure different ULID timestamps
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Query back ordered by ID
    let results = sqlx::query!(
        r#"
        SELECT payload->>'sequence' as sequence,
               payload->>'timestamp' as timestamp
        FROM raw.events
        WHERE event_type = $1
        ORDER BY id ASC
        "#,
        event_type_constants::sinex::AGENT_HEARTBEAT
    )
    .fetch_all(&pool)
    .await?;

    // Verify they come back in the correct order
    for (i, row) in results.iter().enumerate() {
        let sequence: i32 = row.sequence.as_ref().unwrap().parse()?;
        assert_eq!(sequence, i as i32);
    }

    Ok(())
}

/// Test that agent heartbeats work correctly
#[sqlx::test]
async fn test_agent_heartbeat_events(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Create an agent
    let agent_name = "test-heartbeat-agent";
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
            (agent_name, version, status, agent_type)
        VALUES ($1, '1.0.0', 'running', 'ingestor')
        ON CONFLICT (agent_name) DO UPDATE SET status = 'running'
        "#,
        agent_name
    )
    .execute(&pool)
    .await?;

    // Send heartbeat
    let heartbeat = RawEventBuilder::new(
        sources::SINEX,
        event_type_constants::sinex::AGENT_HEARTBEAT,
        json!({
            "agent_name": agent_name,
            "timestamp_iso": Utc::now().to_rfc3339(),
            "status_reported": "healthy",
            "metrics_snapshot": {
                "events_processed": 42,
                "uptime_seconds": 3600
            }
        })
    ).build();

    let heartbeat_id = db_service.insert_event(&heartbeat).await?;

    // Verify heartbeat was recorded
    let heartbeat_data = sqlx::query!(
        r#"
        SELECT payload
        FROM raw.events
        WHERE id = $1
        "#,
        heartbeat_id
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(heartbeat_data.payload["agent_name"], agent_name);
    assert_eq!(heartbeat_data.payload["status_reported"], "healthy");
    assert_eq!(heartbeat_data.payload["metrics_snapshot"]["events_processed"], 42);

    Ok(())
}

/// Test error event creation
#[sqlx::test]
async fn test_error_event_creation(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Create an error event
    let error_event = RawEventBuilder::new(
        sources::SINEX,
        event_type_constants::sinex::AGENT_ERROR,
        json!({
            "agent_name": "test-error-agent",
            "timestamp_iso": Utc::now().to_rfc3339(),
            "error_type": "connection_failed",
            "error_message": "Failed to connect to external service",
            "severity": "medium",
            "context": {
                "retry_count": 3,
                "last_attempt": "2024-01-01T12:00:00Z"
            }
        })
    ).build();

    let error_id = db_service.insert_event(&error_event).await?;

    // Verify error was recorded
    let error_data = sqlx::query!(
        r#"
        SELECT source, event_type, payload
        FROM raw.events
        WHERE id = $1
        "#,
        error_id
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(error_data.source, sources::SINEX);
    assert_eq!(error_data.event_type, event_type_constants::sinex::AGENT_ERROR);
    assert_eq!(error_data.payload["severity"], "medium");
    assert_eq!(error_data.payload["context"]["retry_count"], 3);

    Ok(())
}