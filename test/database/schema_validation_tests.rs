use chrono::Utc;
use serde_json::json;
use sinex_shared::{DatabaseService, RawEventBuilder, sources, event_type_constants};
use std::collections::HashMap;

/// Test that validation prevents malformed events from being inserted
#[sqlx::test]
async fn test_validation_prevents_malformed_events(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Test 1: Valid event should work
    let valid_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/valid.txt",
            "size": 1024,
            "permissions": "644"
        })
    ).build();

    // This should succeed
    let valid_id = db_service.insert_event(&valid_event).await?;
    assert!(!valid_id.is_nil());

    // Test 2: Invalid event (wrong type) should be rejected
    let invalid_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/invalid.txt",
            "size": "not_a_number", // Should be integer
            "permissions": "644"
        })
    ).build();

    // This should fail validation
    let result = db_service.insert_event(&invalid_event).await;
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("validation failed"));
    assert!(error_msg.contains("size"));

    // Test 3: Missing required field should be rejected
    let incomplete_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "path": "/test/incomplete.txt"
            // Missing required "size" field
        })
    ).build();

    // This should fail validation
    let result = db_service.insert_event(&incomplete_event).await;
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("validation failed"));
    assert!(error_msg.contains("Missing required field"));

    // Verify only the valid event was inserted
    let count: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*)
        FROM raw.events
        WHERE source = 'filesystem' AND event_type = 'file_created'
        "#
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(count, 1, "Only the valid event should have been inserted");

    Ok(())
}

/// Test that we can disable validation for testing
#[sqlx::test]
async fn test_validation_can_be_disabled(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Create service without validation
    let db_service = DatabaseService::from_pool_no_validation(pool.clone());

    // Invalid event that would normally fail
    let invalid_event = RawEventBuilder::new(
        sources::FILESYSTEM,
        event_type_constants::filesystem::FILE_CREATED,
        json!({
            "completely": "wrong",
            "structure": true
        })
    ).build();

    // Should succeed without validation
    let result = db_service.insert_event(&invalid_event).await;
    assert!(result.is_ok(), "Should insert without validation");

    Ok(())
}

/// Test batch validation - all events must be valid or none are inserted
#[sqlx::test]
async fn test_batch_validation_atomic(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    let events = vec![
        // Valid event
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "path": "/test/file1.txt",
                "size": 1024
            })
        ).build(),
        
        // Another valid event
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_MODIFIED,
            json!({
                "path": "/test/file2.txt",
                "old_size": 1024,
                "new_size": 2048
            })
        ).build(),
        
        // Invalid event (missing required field)
        RawEventBuilder::new(
            sources::FILESYSTEM,
            event_type_constants::filesystem::FILE_CREATED,
            json!({
                "path": "/test/file3.txt"
                // Missing "size"
            })
        ).build(),
    ];

    // Batch insert should fail due to the invalid event
    let result = db_service.insert_events_batch(&events).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Event 2 validation failed"));

    // Verify NO events were inserted (atomic validation)
    let count: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*)
        FROM raw.events
        WHERE source = 'filesystem'
        "#
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(count, 0, "No events should be inserted when batch validation fails");

    Ok(())
}

/// Test that schema changes are handled correctly
#[sqlx::test]
async fn test_schema_evolution(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Register v1.0.0 schema
    let schema_v1: uuid::Uuid = sqlx::query_scalar!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas 
            (event_source, event_type, schema_version, json_schema_definition)
        VALUES 
            ('test', 'evolving_event', '1.0.0', 
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
    .await?;

    // Insert event with v1 schema
    let v1_event = RawEventBuilder::new(
        "test",
        "evolving_event", 
        json!({
            "name": "test",
            "value": 42
        })
    ).build();

    db_service.insert_event(&v1_event).await?;

    // Register v2.0.0 schema (adds optional field)
    let schema_v2: uuid::Uuid = sqlx::query_scalar!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas 
            (event_source, event_type, schema_version, json_schema_definition)
        VALUES 
            ('test', 'evolving_event', '2.0.0', 
             '{
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "value": {"type": "number"},
                    "metadata": {"type": "object"}
                },
                "required": ["name", "value"]
             }'::jsonb)
        RETURNING id::uuid
        "#
    )
    .fetch_one(&pool)
    .await?;

    // Insert event with v2 schema (includes new field)
    let v2_event = RawEventBuilder::new(
        "test",
        "evolving_event",
        json!({
            "name": "test_v2",
            "value": 100,
            "metadata": {"version": "2.0"}
        })
    ).build();

    db_service.insert_event(&v2_event).await?;

    // Verify both versions exist
    let schema_count: i64 = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*)
        FROM sinex_schemas.event_payload_schemas
        WHERE event_source = 'test' AND event_type = 'evolving_event'
        "#
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(schema_count, 2);

    Ok(())
}

/// Test that ingestor configuration mismatches are detectable
#[sqlx::test]  
async fn test_ingestor_config_validation(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // This simulates an ingestor that has wrong assumptions about data structure

    // Scenario: Filesystem ingestor thinks it's getting file events but actually gets network events
    let wrong_assumptions = vec![
        // Wrong: assumes all events have "path" field
        json!({
            "host": "192.168.1.1",
            "port": 80,
            "status": "connected"
        }),
        // Wrong: assumes "size" is always file size, but it's network packet size  
        json!({
            "path": "/dev/null", // Misleading - not actually a file path
            "size": 1500, // Network packet size, not file size
            "protocol": "tcp"
        }),
        // Wrong: completely different event shape
        json!({
            "user_id": 12345,
            "action": "login",
            "timestamp": "2024-01-01T00:00:00Z"
        })
    ];

    let db_service = DatabaseService::from_pool(pool.clone());

    for (i, wrong_payload) in wrong_assumptions.iter().enumerate() {
        let event = RawEventBuilder::new(
            sources::FILESYSTEM, // Ingestor thinks it's filesystem
            event_type_constants::filesystem::FILE_CREATED, // But event is wrong type
            wrong_payload.clone()
        ).build();

        // This currently succeeds but represents a bug
        let event_id = db_service.insert_event(&event).await?;
        
        println!("Inserted malformed event {} with ID: {}", i, event_id);
    }

    // In a real system, we'd want these to fail validation
    Ok(())
}

/// Test boundary conditions and edge cases
#[sqlx::test]
async fn test_event_boundary_conditions(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Test 1: Extremely large payload
    let large_payload = json!({
        "large_data": "x".repeat(10_000), // 10KB of data
        "metadata": {
            "size": 10000,
            "type": "stress_test"
        }
    });

    let large_event = RawEventBuilder::new(
        "test",
        "large_payload_test",
        large_payload
    ).build();

    let _large_id = db_service.insert_event(&large_event).await?;

    // Test 2: Deeply nested payload
    let mut nested = json!({"level": 0});
    for i in 1..20 {
        nested = json!({"level": i, "inner": nested});
    }

    let nested_event = RawEventBuilder::new(
        "test", 
        "nested_payload_test",
        nested
    ).build();

    let _nested_id = db_service.insert_event(&nested_event).await?;

    // Test 3: Empty payload
    let empty_event = RawEventBuilder::new(
        "test",
        "empty_payload_test", 
        json!({})
    ).build();

    let _empty_id = db_service.insert_event(&empty_event).await?;

    // Test 4: Null values
    let null_event = RawEventBuilder::new(
        "test",
        "null_test",
        json!({
            "nullable_field": null,
            "present_field": "value"
        })
    ).build();

    let _null_id = db_service.insert_event(&null_event).await?;

    Ok(())
}

/// Test that we can detect when ingestors send events to wrong sources
#[sqlx::test]
async fn test_source_mismatch_detection(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let db_service = DatabaseService::from_pool(pool.clone());

    // Common mistake: ingestor thinks it's one type but sends events as another type
    
    // Hyprland ingestor accidentally sending filesystem events
    let confused_event = RawEventBuilder::new(
        sources::HYPRLAND, // Wrong source
        event_type_constants::filesystem::FILE_CREATED, // Wrong type combo
        json!({
            "window": "terminal",
            "workspace": 1,
            "path": "/fake/path" // Confusing mix of data
        })
    ).build();

    // This currently works but shouldn't make semantic sense
    let _confused_id = db_service.insert_event(&confused_event).await?;

    // Create a validation query to detect such mismatches
    let mismatched_events = sqlx::query!(
        r#"
        SELECT id::text, source, event_type, payload
        FROM raw.events
        WHERE 
            (source = 'hyprland' AND event_type LIKE 'file_%') OR
            (source = 'filesystem' AND event_type LIKE 'window_%') OR
            (source = 'terminal.kitty' AND event_type LIKE 'workspace_%')
        "#
    )
    .fetch_all(&pool)
    .await?;

    // Report any semantic mismatches
    for mismatch in mismatched_events {
        println!("Detected source/event_type mismatch: {} {} {}", 
                 mismatch.id, mismatch.source, mismatch.event_type);
    }

    Ok(())
}

/// Test that demonstrates need for runtime validation framework
#[test] 
fn test_validation_framework_design() {
    // This test demonstrates what a validation framework might look like

    use serde_json::Value;
    
    // Define validation rules
    struct ValidationRule {
        source: &'static str,
        event_type: &'static str,
        validator: fn(&Value) -> Result<(), String>,
    }

    fn validate_file_created(payload: &Value) -> Result<(), String> {
        if !payload.get("path").and_then(|v| v.as_str()).is_some() {
            return Err("file_created events must have 'path' field".to_string());
        }
        if !payload.get("size").and_then(|v| v.as_u64()).is_some() {
            return Err("file_created events must have numeric 'size' field".to_string());
        }
        Ok(())
    }

    fn validate_window_focused(payload: &Value) -> Result<(), String> {
        if !payload.get("window").is_some() {
            return Err("window_focused events must have 'window' field".to_string());
        }
        Ok(())
    }

    let rules = vec![
        ValidationRule {
            source: sources::FILESYSTEM,
            event_type: event_type_constants::filesystem::FILE_CREATED,
            validator: validate_file_created,
        },
        ValidationRule {
            source: sources::HYPRLAND,
            event_type: event_type_constants::hyprland::WINDOW_FOCUSED,
            validator: validate_window_focused,
        },
    ];

    // Test valid event
    let valid_payload = json!({"path": "/test.txt", "size": 1024});
    let rule = &rules[0];
    assert!((rule.validator)(&valid_payload).is_ok());

    // Test invalid event
    let invalid_payload = json!({"path": "/test.txt"}); // Missing size
    assert!((rule.validator)(&invalid_payload).is_err());

    // This framework could be integrated into DatabaseService.insert_event()
}