use chrono::Utc;
use serde_json::json;
use sinex_db::models::{CreateRawEvent, RawEvent};
use sinex_db::queries::raw_events::{get_event_by_id, insert_event};
use sinex_test_utils::{assert_error_contains, assert_event_inserted, sinex_test, TestContext, TestResult};
use sinex_ulid::Ulid;
use sqlx::{query, query_as};

#[sinex_test]
async fn test_schema_validation_with_valid_payload(ctx: TestContext) -> TestResult {
    // Deploy a test schema to the registry
    let schema_id = "v1/test/valid_event.json";
    let schema_content = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": format!("https://sinex.io/schemas/{}", schema_id),
        "type": "object",
        "properties": {
            "message": {
                "type": "string"
            },
            "count": {
                "type": "integer",
                "minimum": 0
            }
        },
        "required": ["message", "count"]
    });

    // Insert schema into registry
    query!(
        r#"
        INSERT INTO sinex_schemas.schema_registry (schema_id, version, schema_content)
        VALUES ($1, $2, $3::jsonb)
        "#,
        schema_id,
        "v1",
        schema_content
    )
    .execute(ctx.pool())
    .await?;

    // Create a test schema entry in event_payload_schemas
    let schema_ulid = Ulid::new();
    query!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas 
            (id, event_source, event_type, schema_version, json_schema_definition, is_active)
        VALUES ($1::uuid, $2, $3, $4, $5::jsonb, true)
        "#,
        schema_ulid.to_uuid(),
        "test",
        "valid_event",
        "v1",
        schema_content
    )
    .execute(ctx.pool())
    .await?;

    // Create event with valid payload
    let valid_payload = json!({
        "message": "Test message",
        "count": 42
    });

    let event = CreateRawEvent {
        id: Some(Ulid::new()),
        source: "test".to_string(),
        event_type: "valid_event".to_string(),
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: Some(schema_ulid),
        payload: valid_payload,
    };

    // Should succeed with valid payload
    let inserted = insert_event(ctx.pool(), &event).await?;
    assert_eq!(inserted.source, "test");
    assert_eq!(inserted.event_type, "valid_event");
    assert_eq!(inserted.payload["message"], "Test message");
    assert_eq!(inserted.payload["count"], 42);

    Ok(())
}

#[sinex_test]
async fn test_schema_validation_with_invalid_payload(ctx: TestContext) -> TestResult {
    // Deploy a test schema to the registry
    let schema_id = "v1/test/strict_event.json";
    let schema_content = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": format!("https://sinex.io/schemas/{}", schema_id),
        "type": "object",
        "properties": {
            "message": {
                "type": "string"
            },
            "count": {
                "type": "integer",
                "minimum": 0
            }
        },
        "required": ["message", "count"],
        "additionalProperties": false
    });

    // Insert schema into registry
    query!(
        r#"
        INSERT INTO sinex_schemas.schema_registry (schema_id, version, schema_content)
        VALUES ($1, $2, $3::jsonb)
        "#,
        schema_id,
        "v1",
        schema_content
    )
    .execute(ctx.pool())
    .await?;

    // Create a test schema entry in event_payload_schemas
    let schema_ulid = Ulid::new();
    query!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas 
            (id, event_source, event_type, schema_version, json_schema_definition, is_active)
        VALUES ($1::uuid, $2, $3, $4, $5::jsonb, true)
        "#,
        schema_ulid.to_uuid(),
        "test",
        "strict_event",
        "v1",
        schema_content
    )
    .execute(ctx.pool())
    .await?;

    // Test 1: Missing required field
    let invalid_payload1 = json!({
        "message": "Test message"
        // Missing required "count" field
    });

    let event1 = CreateRawEvent {
        id: Some(Ulid::new()),
        source: "test".to_string(),
        event_type: "strict_event".to_string(),
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: Some(schema_ulid),
        payload: invalid_payload1,
    };

    // Should fail validation
    let result1 = insert_event(ctx.pool(), &event1).await;
    assert!(result1.is_err());
    assert_error_contains(&result1, "does not conform to schema");

    // Test 2: Wrong type for field
    let invalid_payload2 = json!({
        "message": "Test message",
        "count": "not a number"  // Should be integer
    });

    let event2 = CreateRawEvent {
        id: Some(Ulid::new()),
        source: "test".to_string(),
        event_type: "strict_event".to_string(),
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: Some(schema_ulid),
        payload: invalid_payload2,
    };

    let result2 = insert_event(ctx.pool(), &event2).await;
    assert!(result2.is_err());
    assert_error_contains(&result2, "does not conform to schema");

    // Test 3: Additional properties not allowed
    let invalid_payload3 = json!({
        "message": "Test message",
        "count": 42,
        "extra": "not allowed"  // additionalProperties: false
    });

    let event3 = CreateRawEvent {
        id: Some(Ulid::new()),
        source: "test".to_string(),
        event_type: "strict_event".to_string(),
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: Some(schema_ulid),
        payload: invalid_payload3,
    };

    let result3 = insert_event(ctx.pool(), &event3).await;
    assert!(result3.is_err());
    assert_error_contains(&result3, "does not conform to schema");

    Ok(())
}

#[sinex_test]
async fn test_schema_validation_with_null_schema_id(ctx: TestContext) -> TestResult {
    // Event with NULL schema_id should skip validation
    let payload = json!({
        "arbitrary": "data",
        "can_be": "anything",
        "numbers": [1, 2, 3],
        "nested": {
            "objects": true
        }
    });

    let event = CreateRawEvent {
        id: Some(Ulid::new()),
        source: "test".to_string(),
        event_type: "unschematized_event".to_string(),
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: None,  // NULL schema_id
        payload,
    };

    // Should succeed without validation
    let inserted = insert_event(ctx.pool(), &event).await?;
    assert_eq!(inserted.source, "test");
    assert_eq!(inserted.event_type, "unschematized_event");
    assert!(inserted.payload_schema_id.is_none());

    Ok(())
}

#[sinex_test]
async fn test_schema_validation_with_inactive_schema(ctx: TestContext) -> TestResult {
    // Create an inactive schema
    let schema_ulid = Ulid::new();
    let schema_content = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "message": {"type": "string"}
        },
        "required": ["message"]
    });

    query!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas 
            (id, event_source, event_type, schema_version, json_schema_definition, is_active)
        VALUES ($1::uuid, $2, $3, $4, $5::jsonb, false)  -- inactive
        "#,
        schema_ulid.to_uuid(),
        "test",
        "inactive_schema_event",
        "v1",
        schema_content
    )
    .execute(ctx.pool())
    .await?;

    // Try to use inactive schema
    let event = CreateRawEvent {
        id: Some(Ulid::new()),
        source: "test".to_string(),
        event_type: "inactive_schema_event".to_string(),
        host: "test-host".to_string(),
        ingestor_version: Some("1.0.0".to_string()),
        payload_schema_id: Some(schema_ulid),
        payload: json!({"message": "test"}),
    };

    // Should fail because schema is inactive
    let result = insert_event(ctx.pool(), &event).await;
    assert!(result.is_err());
    assert_error_contains(&result, "not found or inactive");

    Ok(())
}

#[sinex_test]
async fn test_schema_registry_deployment_view(ctx: TestContext) -> TestResult {
    // Deploy multiple schemas
    for i in 1..=3 {
        let schema_id = format!("v1/test/event_{}.json", i);
        let schema_content = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": format!("https://sinex.io/schemas/{}", schema_id),
            "title": format!("Test Event {}", i),
            "type": "object",
            "properties": {
                "id": {"type": "integer"}
            }
        });

        query!(
            r#"
            INSERT INTO sinex_schemas.schema_registry 
                (schema_id, version, schema_content, git_commit_sha)
            VALUES ($1, $2, $3::jsonb, $4)
            "#,
            schema_id,
            "v1",
            schema_content,
            format!("abc123{}", i)
        )
        .execute(ctx.pool())
        .await?;
    }

    // Query deployment status view
    #[derive(Debug)]
    struct DeploymentStatus {
        schema_id: String,
        version: String,
        is_active: bool,
        git_commit_sha: Option<String>,
        schema_title: Option<String>,
    }

    let statuses: Vec<DeploymentStatus> = query_as!(
        DeploymentStatus,
        r#"
        SELECT 
            schema_id,
            version,
            is_active,
            git_commit_sha,
            schema_title
        FROM sinex_schemas.schema_deployment_status
        WHERE schema_id LIKE 'v1/test/%'
        ORDER BY schema_id
        "#
    )
    .fetch_all(ctx.pool())
    .await?;

    assert_eq!(statuses.len(), 3);
    assert!(statuses.iter().all(|s| s.is_active));
    assert!(statuses.iter().all(|s| s.version == "v1"));
    assert_eq!(statuses[0].schema_title.as_ref().unwrap(), "Test Event 1");
    assert_eq!(statuses[1].schema_title.as_ref().unwrap(), "Test Event 2");
    assert_eq!(statuses[2].schema_title.as_ref().unwrap(), "Test Event 3");

    Ok(())
}

#[sinex_test]
async fn test_schema_registry_functions(ctx: TestContext) -> TestResult {
    // Deploy a schema
    let schema_id = "v1/test/function_test.json";
    let schema_content = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": format!("https://sinex.io/schemas/{}", schema_id),
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer", "minimum": 0}
        },
        "required": ["name"]
    });

    query!(
        r#"
        INSERT INTO sinex_schemas.schema_registry (schema_id, version, schema_content)
        VALUES ($1, $2, $3::jsonb)
        "#,
        schema_id,
        "v1",
        schema_content
    )
    .execute(ctx.pool())
    .await?;

    // Test get_active_schema function
    let active_schema: Option<serde_json::Value> = query!(
        "SELECT sinex_schemas.get_active_schema($1) as schema",
        schema_id
    )
    .fetch_one(ctx.pool())
    .await?
    .schema;

    assert!(active_schema.is_some());
    let schema = active_schema.unwrap();
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["properties"]["name"]["type"], "string");

    // Test validate_against_registry function with valid data
    let valid_result: bool = query!(
        r#"
        SELECT sinex_schemas.validate_against_registry($1, $2::jsonb) as is_valid
        "#,
        schema_id,
        json!({"name": "Alice", "age": 30})
    )
    .fetch_one(ctx.pool())
    .await?
    .is_valid
    .unwrap_or(false);

    assert!(valid_result);

    // Test validate_against_registry function with invalid data
    let invalid_result: bool = query!(
        r#"
        SELECT sinex_schemas.validate_against_registry($1, $2::jsonb) as is_valid
        "#,
        schema_id,
        json!({"age": "not a number"})  // Missing required "name", wrong type for "age"
    )
    .fetch_one(ctx.pool())
    .await?
    .is_valid
    .unwrap_or(true);

    assert!(!invalid_result);

    Ok(())
}