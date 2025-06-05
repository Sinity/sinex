use chrono::Utc;
use serde_json::json;
use sinex_db::models::RawEvent;
use sinex_shared::{DatabaseConfig, DatabaseService, RawEventBuilder, sources, event_type_constants};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

/// Test that we can actually connect to the database and perform basic operations
#[sqlx::test]
async fn test_database_connection_and_health_check(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let config = DatabaseConfig {
        url: std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string()),
        max_connections: 5,
        min_connections: 1,
        connect_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
    };

    let db_service = DatabaseService::new(config).await?;
    db_service.health_check().await?;
    
    Ok(())
}

/// Test that we can insert events and they actually show up in the database
#[sqlx::test]
async fn test_insert_and_retrieve_event(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Create database service
    let config = DatabaseConfig::default();
    let db_service = DatabaseService::from_pool(pool.clone());

    // Create an event
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/file.txt",
            "size": 1024,
            "test_marker": "database_integration_test"
        })
    )
    .with_orig_timestamp(Utc::now())
    .build();

    // Insert the event
    let event_id = db_service.insert_event(&event).await?;
    assert!(!event_id.is_nil());

    // Query it back
    let retrieved = sqlx::query_as!(
        RawEvent,
        r#"
        SELECT id, source, event_type, ts_orig, host, 
               ingestor_version, payload_schema_id, payload
        FROM raw.events
        WHERE id = $1
        "#,
        event_id
    )
    .fetch_one(&pool)
    .await?;

    // Verify the data
    assert_eq!(retrieved.source, sources::FILESYSTEM);
    assert_eq!(retrieved.event_type, event_type_constants::filesystem::FILE_CREATED);
    assert_eq!(retrieved.payload["path"], "/test/file.txt");
    assert_eq!(retrieved.payload["test_marker"], "database_integration_test");
    // Verify ingestion timestamp from ULID
    let ts_ingest = retrieved.ts_ingest().expect("extract timestamp from ULID");
    assert!(ts_ingest > Utc::now() - chrono::Duration::seconds(5));

    Ok(())
}

/// Test batch insertion
#[sqlx::test]
async fn test_batch_insert_events(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Create multiple events
    let events: Vec<RawEvent> = (0..5)
        .map(|i| {
            RawEventBuilder::new(
                sources::FILESYSTEM,
                event_type_constants::filesystem::FILE_MODIFIED,
                json!({
                    "path": format!("/test/file_{}.txt", i),
                    "batch_index": i,
                    "batch_test": true
                })
            ).build()
        })
        .collect();

    // Insert batch
    let ids = db_service.insert_events_batch(&events).await?;
    assert_eq!(ids.len(), 5);

    // Verify all were inserted
    let count: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) 
        FROM raw.events 
        WHERE payload->>'batch_test' = 'true'
        "#
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(count, 5);

    Ok(())
}

/// Test that the event router trigger fires and creates promotion queue entries
#[sqlx::test]
async fn test_event_router_trigger(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Insert an agent manifest that subscribes to filesystem events
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
            (agent_name, version, status, agent_type, subscribes_to_event_types)
        VALUES 
            ('test-promoter', '1.0.0', 'running', 'promoter', 
             '{"raw.events_feed_all": [{"source_filter": "filesystem", "event_type_filter": "file_created"}]}'::jsonb)
        "#
    )
    .execute(&pool)
    .await?;

    // Create and insert an event
    let event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({ "path": "/test/trigger.txt" })
    ).build();

    let event_id = db_service.insert_event(&event).await?;

    // Check if promotion queue entry was created
    let promotion_entries: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT target_agent_name, status
        FROM sinex_schemas.promotion_queue
        WHERE raw_event_id = $1
        "#
    )
    .bind(event_id)
    .fetch_all(&pool)
    .await?;

    assert_eq!(promotion_entries.len(), 1);
    assert_eq!(promotion_entries[0].0, "test-promoter");
    assert_eq!(promotion_entries[0].1, "pending");

    Ok(())
}

/// Test ULID generation and ordering
#[sqlx::test]
async fn test_ulid_ordering(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Insert events with small delays
    let mut ids = Vec::new();
    for i in 0..3 {
        let event = RawEventBuilder::new(
            sources::SINEX,
            event_type_constants::sinex::AGENT_HEARTBEAT,
            json!({ "sequence": i })
        ).build();
        
        let id = db_service.insert_event(&event).await?;
        ids.push(id);
        
        // Small delay to ensure different timestamps
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Query events ordered by ID (should be time-ordered due to ULID)
    let ordered: Vec<i32> = sqlx::query_scalar(
        r#"
        SELECT (payload->>'sequence')::int
        FROM raw.events
        WHERE event_type = $1
        ORDER BY id ASC
        "#
    )
    .bind(event_type_constants::sinex::AGENT_HEARTBEAT)
    .fetch_all(&pool)
    .await?;

    assert_eq!(ordered, vec![0, 1, 2]);

    Ok(())
}

/// Test schema validation when a schema is registered
#[sqlx::test]
async fn test_event_schema_validation(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // First, register a schema
    let schema_id: uuid::Uuid = sqlx::query_scalar!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas 
            (event_source, event_type, schema_version, json_schema_definition)
        VALUES 
            ('test', 'structured_event', '1.0.0', 
             '{
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "value": {"type": "number"}
                },
                "required": ["name", "value"]
             }'::jsonb)
        RETURNING id
        "#
    )
    .fetch_one(&pool)
    .await?;

    let db_service = DatabaseService::from_pool(pool.clone());

    // Try to insert a valid event
    let valid_event = RawEvent {
        id: uuid::Uuid::new_v4(),
        source: "test".to_string(),
        event_type: "structured_event".to_string(),
        ts_orig: None,
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: Some(schema_id),
        payload: json!({
            "name": "test",
            "value": 42
        }),
    };

    // This should succeed
    db_service.insert_event(&valid_event).await?;

    // Try to insert an invalid event (if pg_jsonschema is available)
    // Note: This test is commented out because pg_jsonschema might not be available
    // let invalid_event = RawEvent {
    //     ...valid_event.clone(),
    //     id: uuid::Uuid::new_v4(),
    //     payload: json!({
    //         "name": "test",
    //         "value": "not a number" // Schema expects number
    //     }),
    // };
    // 
    // let result = db_service.insert_event(&invalid_event).await;
    // assert!(result.is_err());

    Ok(())
}

