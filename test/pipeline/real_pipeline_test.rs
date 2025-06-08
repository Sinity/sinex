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