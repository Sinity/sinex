use crate::common;
use crate::db_test;

use chrono::Utc;
use serde_json::json;
use sinex_db::models::RawEvent;
use sinex_shared::event_types;
use std::time::Duration;

use common::{
    database_service_from_pool,
    events, assertions, generators
};


/// Test that we can insert events and they actually show up in the database
db_test! {
    async fn test_insert_and_retrieve_event(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = database_service_from_pool(pool.clone());
    
    // Create a test event using our utilities
    let event = events::filesystem_event(
        event_types::event_types::filesystem::FILE_CREATED,
        "/test/file.txt"
    );

    // Insert and verify using shared assertion helpers
    let event_id = assertions::assert_event_inserted(&db_service, &event).await?;

    // Query it back using our helper that encapsulates the UUID conversion
    let retrieved = common::get_event_by_id(&pool, event_id).await?;

    // Verify using shared assertion helper
    assertions::assert_events_equivalent(&retrieved, &event);
    
    // Verify ingestion timestamp from ULID
    assert!(retrieved.ts_ingest > Utc::now() - chrono::Duration::seconds(5));

        Ok(())
    }
}

/// Test batch insertion using generators
db_test! {
    async fn test_batch_insert_events(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
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
    .await?
    .unwrap_or(0);

    assert!(count >= 5);

        Ok(())
    }
}

/// Test basic promotion queue functionality (manual insertion)
db_test! {
    async fn test_promotion_queue_basic(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = database_service_from_pool(pool.clone());

    // Insert an agent manifest 
    let agent_name = format!("test-promoter-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
            (agent_name, version, status, agent_type, subscribes_to_event_types)
        VALUES 
            ($1, '1.0.0', 'running', 'promoter', '{}'::jsonb)
        "#,
        agent_name
    )
    .execute(&pool)
    .await?;

    // Create and insert an event
    let event = events::filesystem_event(
        event_types::event_types::filesystem::FILE_CREATED,
        "/test/trigger.txt"
    );

    let event_id = db_service.insert_event(&event).await?;

    // Manually insert promotion queue entry to test the table structure
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name)
        VALUES ($1::uuid::ulid, $2)
        "#,
        event_id.to_uuid(),
        agent_name
    )
    .execute(&pool)
    .await?;

    // Verify the promotion queue entry was inserted correctly
    let promotion_entries: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT target_agent_name, status
        FROM sinex_schemas.promotion_queue
        WHERE raw_event_id = $1::uuid::ulid
        "#
    )
    .bind(event_id.to_uuid())
    .fetch_all(&pool)
    .await?;

    assert_eq!(promotion_entries.len(), 1);
    assert_eq!(promotion_entries[0].0, agent_name);
    assert_eq!(promotion_entries[0].1, "pending");

        Ok(())
    }
}

/// Test ULID generation and ordering
db_test! {
    async fn test_ulid_ordering(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = database_service_from_pool(pool.clone());

    // Generate unique test run ID to avoid conflicts with other test runs
    let test_run_id = &uuid::Uuid::new_v4().to_string()[..8];

    // Insert events with small delays using our utilities
    let mut ids = Vec::new();
    let mut expected_names = Vec::new();
    for i in 0..3 {
        let agent_name = format!("test-agent-{}-{}", test_run_id, i);
        expected_names.push(agent_name.clone());
        
        let event = events::agent_event(
            event_types::event_types::sinex::AGENT_HEARTBEAT,
            &agent_name
        );
        
        let id = db_service.insert_event(&event).await?;
        ids.push(id);
        
        // Small delay to ensure different timestamps
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Query only the events we just created by filtering for our specific test run
    let ordered: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT payload->>'agent_name'
        FROM raw.events
        WHERE event_type = $1
        AND payload->>'agent_name' LIKE $2
        ORDER BY id ASC
        "#
    )
    .bind(event_types::event_types::sinex::AGENT_HEARTBEAT)
    .bind(format!("test-agent-{}-%", test_run_id))
    .fetch_all(&pool)
    .await?;

    assert_eq!(ordered, expected_names);

        Ok(())
    }
}

/// Test schema validation when a schema is registered
db_test! {
    async fn test_event_schema_validation(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
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
        RETURNING id::uuid
        "#
    )
    .fetch_one(&pool)
    .await?
    .expect("Schema ID should be returned");

    let db_service = database_service_from_pool(pool.clone());

    // Try to insert a valid event
    let valid_event = RawEvent {
        id: sinex_ulid::Ulid::new(),
        source: "test".to_string(),
        event_type: "structured_event".to_string(),
        ts_ingest: Utc::now(), // This will be ignored by DB due to generated column
        ts_orig: None,
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: Some(sinex_ulid::Ulid::from(schema_id)),
        payload: json!({
            "name": "test",
            "value": 42
        }),
    };

    // This should succeed - use our assertion helper
    assertions::assert_event_inserted(&db_service, &valid_event).await?;

        Ok(())
    }
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