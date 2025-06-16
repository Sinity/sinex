use sinex_db::queries;
use sinex_core::RawEventBuilder;
use serde_json::json;
use sqlx::Row;

#[sqlx::test]
async fn test_basic_event_insertion(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple test event
    let event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({
            "path": "/test/simple_file.txt",
            "size": 1024
        })
    ).build();
    
    // Insert using the available function
    let inserted_event = queries::insert_event(&pool, &event).await?;
    
    // Verify basic fields match
    assert_eq!(inserted_event.source, event.source);
    assert_eq!(inserted_event.event_type, event.event_type);
    assert_eq!(inserted_event.payload, event.payload);
    assert_eq!(inserted_event.host, event.host);
    
    // Verify the event was actually stored
    assert_eq!(inserted_event.payload["path"], "/test/simple_file.txt");
    assert_eq!(inserted_event.payload["size"], 1024);
    
    Ok(())
}

#[test]
fn test_event_validation_creation() {
    // Test that EventValidator can be created
    let _validator = sinex_db::validation::EventValidator::new();
    // If this compiles and runs, the basic validation infrastructure works
}

#[sqlx::test]
async fn test_database_connection(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Simple test to verify database connectivity
    let result = sqlx::query("SELECT 1 as test_value")
        .fetch_one(&pool)
        .await?;
    
    // Verify we can execute queries
    assert!(result.try_get::<i32, _>("test_value").unwrap() == 1);
    
    Ok(())
}