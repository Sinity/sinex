use crate::common;

use chrono::Utc;
use serde_json::json;
use sinex_db::models::RawEvent;
use sinex_shared::{DatabaseService, sources, event_types};
use std::time::Duration;

use common::{
    database_service_from_pool, test_database_service,
    events, assertions, generators,
    test_event_insertion, test_invalid_event_insertion
};

/// Test that we can actually connect to the database and perform basic operations
#[sqlx::test]
async fn test_database_connection_and_health_check(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = test_database_service().await?;
    db_service.health_check().await?;
    Ok(())
}

/// Test that we can insert events and they actually show up in the database
#[sqlx::test]
async fn test_insert_and_retrieve_event(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = database_service_from_pool(pool.clone());
    
    // Create a test event using our utilities
    let event = events::filesystem_event(
        event_types::event_types::filesystem::FILE_CREATED,
        "/test/file.txt"
    );

    // Insert and verify using shared assertion helpers
    let event_id = assertions::assert_event_inserted(&db_service, &event).await?;

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

    // Verify using shared assertion helper
    assertions::assert_events_equivalent(&retrieved, &event);
    
    // Verify ingestion timestamp from ULID
    let ts_ingest = retrieved.ts_ingest().expect("extract timestamp from ULID");
    assert!(ts_ingest > Utc::now() - chrono::Duration::seconds(5));

    Ok(())
}

/// Test batch insertion using generators
#[sqlx::test]
async fn test_batch_insert_events(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = database_service_from_pool(pool.clone());

    // Create multiple events using our generator utilities
    let events = generators::test_events(5);

    // Insert batch
    let ids = db_service.insert_events_batch(&events).await?;
    assert_eq!(ids.len(), 5);

    // Verify all were inserted
    let count: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) 
        FROM raw.events 
        WHERE payload ? 'size'  -- All our test events have a size field
        "#
    )
    .fetch_one(&pool)
    .await?;

    assert!(count >= 5);

    Ok(())
}

/// Test that the event router trigger fires and creates promotion queue entries
#[sqlx::test]
async fn test_event_router_trigger(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = database_service_from_pool(pool.clone());

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

    // Create and insert an event using utilities
    let event = events::filesystem_event(
        event_types::event_types::filesystem::FILE_CREATED,
        "/test/trigger.txt"
    );

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
    let db_service = database_service_from_pool(pool.clone());

    // Insert events with small delays using our utilities
    let mut ids = Vec::new();
    for i in 0..3 {
        let event = events::agent_event(
            event_types::event_types::sinex::AGENT_HEARTBEAT,
            &format!("test-agent-{}", i)
        );
        
        let id = db_service.insert_event(&event).await?;
        ids.push(id);
        
        // Small delay to ensure different timestamps
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Query events ordered by ID (should be time-ordered due to ULID)
    let ordered: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT payload->>'agent_name'
        FROM raw.events
        WHERE event_type = $1
        ORDER BY id ASC
        "#
    )
    .bind(event_types::event_types::sinex::AGENT_HEARTBEAT)
    .fetch_all(&pool)
    .await?;

    assert_eq!(ordered, vec!["test-agent-0", "test-agent-1", "test-agent-2"]);

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

    let db_service = database_service_from_pool(pool.clone());

    // Try to insert a valid event
    let valid_event = RawEvent {
        id: uuid::Uuid::new_v4(),
        source: "test".to_string(),
        event_type: "structured_event".to_string(),
        ts_ingest: Utc::now(), // This will be ignored by DB due to generated column
        ts_orig: None,
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: Some(schema_id),
        payload: json!({
            "name": "test",
            "value": 42
        }),
    };

    // This should succeed - use our assertion helper
    assertions::assert_event_inserted(&db_service, &valid_event).await?;

    Ok(())
}

// Examples of using the new test macros for simple cases

// Simple filesystem event insertion test
test_event_insertion!(
    test_filesystem_event_macro,
    events::filesystem_event(
        event_types::event_types::filesystem::FILE_CREATED,
        "/macro/test/file.txt"
    )
);

// Simple kitty event insertion test  
test_event_insertion!(
    test_kitty_event_macro,
    events::kitty_event("cargo test")
);

// Test invalid event rejection
test_invalid_event_insertion!(
    test_invalid_event_macro,
    events::invalid_event()
);