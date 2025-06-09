use sinex_ulid::Ulid;
use serde_json::json;
use uuid::Uuid;

use crate::db_test;

db_test! {
    async fn test_json_schema_registration(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // Register a JSON Schema
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "window_id": {
                "type": "integer",
                "minimum": 0
            },
            "window_title": {
                "type": "string",
                "minLength": 1
            },
            "timestamp": {
                "type": "string",
                "format": "date-time"
            }
        },
        "required": ["window_id", "window_title"],
        "additionalProperties": false
    });
    
    // Generate unique test identifiers to avoid conflicts
    let test_run_id = &uuid::Uuid::new_v4().to_string()[..8];
    let event_source = format!("hyprland-test-{}", test_run_id);
    let event_type = format!("window_focused-{}", test_run_id);
    
    let schema_id: String = sqlx::query_scalar(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition, description) 
         VALUES ($1, $2, $3, $4::jsonb, $5) 
         RETURNING id::text"
    )
    .bind(&event_source)
    .bind(&event_type)
    .bind("v1.0")
    .bind(&schema)
    .bind("Schema for window focus events")
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Verify schema was stored correctly
    let retrieved_schema: serde_json::Value = sqlx::query_scalar(
        "SELECT json_schema_definition FROM sinex_schemas.event_payload_schemas WHERE id = $1::ulid"
    )
    .bind(&schema_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(retrieved_schema, schema, "Schema should be stored correctly");
    
    Ok(())
    }
}

db_test! {
    async fn test_json_schema_validation_constraint(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // First, register a strict schema
    let strict_schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["click", "hover", "focus"]
            },
            "element_id": {
                "type": "string",
                "pattern": "^[a-zA-Z0-9_-]+$"
            },
            "coordinates": {
                "type": "object",
                "properties": {
                    "x": {"type": "number"},
                    "y": {"type": "number"}
                },
                "required": ["x", "y"]
            }
        },
        "required": ["action", "element_id"],
        "additionalProperties": false
    });
    
    // Generate unique test identifiers to avoid conflicts
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("ui_test-{}", test_run_id);
    let event_type = format!("user_interaction-{}", test_run_id);
    
    let schema_id: String = sqlx::query_scalar(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition) 
         VALUES ($1, $2, $3, $4::jsonb) 
         RETURNING id::text"
    )
    .bind(&event_source)
    .bind(&event_type)
    .bind("v1.0")
    .bind(&strict_schema)
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Test valid payload
    let valid_payload = json!({
        "action": "click",
        "element_id": "submit-button",
        "coordinates": {
            "x": 100.5,
            "y": 200.0
        }
    });
    
    let event_id = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_id)
    .bind(&valid_payload)
    .execute(&pool)
    .await;
    
    if let Err(e) = &result {
        println!("Error inserting valid payload: {:?}", e);
        
        // Debug: Check if schema exists and is active
        let schema_check: Option<(bool, String)> = sqlx::query_as(
            "SELECT is_active, json_schema_definition::text FROM sinex_schemas.event_payload_schemas WHERE id = $1::ulid"
        )
        .bind(&schema_id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        
        println!("Schema check for ID {}: {:?}", schema_id, schema_check);
        
        // Test the function directly
        if let Some((is_active, schema_def)) = schema_check {
            let matches: bool = sqlx::query_scalar(
                "SELECT jsonb_matches_schema($1::jsonb, $2::jsonb)"
            )
            .bind(&schema_def)
            .bind(&valid_payload)
            .fetch_one(&pool)
            .await
            .unwrap();
            
            println!("Schema active: {}, Payload matches: {}", is_active, matches);
        }
    }
    assert!(result.is_ok(), "Valid payload should be accepted");
    
    // Test invalid payload - missing required field
    let invalid_payload1 = json!({
        "action": "click"
        // missing element_id
    });
    
    let event_id2 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id2.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_id)
    .bind(&invalid_payload1)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "Invalid payload missing required field should be rejected");
    
    // Test invalid payload - wrong enum value
    let invalid_payload2 = json!({
        "action": "drag", // not in enum
        "element_id": "some-element"
    });
    
    let event_id3 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id3.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_id)
    .bind(&invalid_payload2)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "Invalid payload with wrong enum value should be rejected");
    
    // Test invalid payload - additional properties
    let invalid_payload3 = json!({
        "action": "click",
        "element_id": "button",
        "extra_field": "not allowed"
    });
    
    let event_id4 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id4.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_id)
    .bind(&invalid_payload3)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "Invalid payload with additional properties should be rejected");
    
    Ok(())
    }
}

db_test! {
    async fn test_schema_versioning(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // Generate unique test identifiers to avoid conflicts
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("versioning_test-{}", test_run_id);
    let event_type = format!("message_event-{}", test_run_id);
    
    // Create v1 schema
    let schema_v1 = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "message": {"type": "string"}
        },
        "required": ["message"]
    });
    
    let schema_v1_id: String = sqlx::query_scalar(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition, is_active) 
         VALUES ($1, $2, $3, $4::jsonb, $5) 
         RETURNING id::text"
    )
    .bind(&event_source)
    .bind(&event_type)
    .bind("v1.0")
    .bind(&schema_v1)
    .bind(true) // Must be active for validation to work
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Test v1 payload validates against v1 schema (both active)
    let v1_payload = json!({"message": "Hello"});
    let event_id1 = Ulid::new();
    
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id1.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_v1_id)
    .bind(&v1_payload)
    .execute(&pool)
    .await;
    
    assert!(result.is_ok(), "V1 payload should validate against V1 schema");
    
    // Create v2 schema with additional field
    let schema_v2 = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "message": {"type": "string"},
            "priority": {
                "type": "string",
                "enum": ["low", "medium", "high"]
            }
        },
        "required": ["message", "priority"]
    });
    
    let schema_v2_id: String = sqlx::query_scalar(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition, is_active) 
         VALUES ($1, $2, $3, $4::jsonb, $5) 
         RETURNING id::text"
    )
    .bind(&event_source)
    .bind(&event_type)
    .bind("v2.0")
    .bind(&schema_v2)
    .bind(true) // This will be the active version
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Now make V1 inactive since V2 is the active version
    sqlx::query(
        "UPDATE sinex_schemas.event_payload_schemas 
         SET is_active = false 
         WHERE id = $1::ulid"
    )
    .bind(&schema_v1_id)
    .execute(&pool)
    .await
    .unwrap();
    
    // V1 payload should fail against v2 schema (since v2 requires priority field)
    let event_id2 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id2.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_v2_id)
    .bind(&v1_payload)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "V1 payload should fail against V2 schema");
    
    // V1 payload should also fail against v1 schema now that v1 is inactive
    let event_id4 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id4.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_v1_id)
    .bind(&v1_payload)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "V1 payload should fail against inactive V1 schema");
    
    // V2 payload should validate against v2 schema
    let v2_payload = json!({
        "message": "Important message",
        "priority": "high"
    });
    
    let event_id3 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id3.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_v2_id)
    .bind(&v2_payload)
    .execute(&pool)
    .await;
    
    assert!(result.is_ok(), "V2 payload should validate against V2 schema");
    
    // Query for active schema
    let active_version: String = sqlx::query_scalar(
        "SELECT schema_version FROM sinex_schemas.event_payload_schemas 
         WHERE event_source = $1 AND event_type = $2 AND is_active = true"
    )
    .bind(&event_source)
    .bind(&event_type)
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(active_version, "v2.0", "V2 should be the active version");
    
    Ok(())
    }
}

db_test! {
    async fn test_complex_schema_validation(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // Generate unique test identifiers to avoid conflicts
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("complex_test-{}", test_run_id);
    let event_type = format!("complex_event-{}", test_run_id);
    
    // Create a complex schema with nested objects, arrays, and various constraints
    let complex_schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {
                "type": "string",
                "pattern": "^[A-Z]{3}-[0-9]{4}$"
            },
            "tags": {
                "type": "array",
                "items": {
                    "type": "string",
                    "minLength": 2,
                    "maxLength": 20
                },
                "minItems": 1,
                "maxItems": 5,
                "uniqueItems": true
            },
            "metadata": {
                "type": "object",
                "properties": {
                    "created_at": {
                        "type": "string",
                        "format": "date-time"
                    },
                    "version": {
                        "type": "number",
                        "minimum": 1.0
                    },
                    "features": {
                        "type": "object",
                        "patternProperties": {
                            "^feature_": {
                                "type": "boolean"
                            }
                        },
                        "additionalProperties": false
                    }
                },
                "required": ["created_at", "version"]
            },
            "data": {
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "type": {"const": "text"},
                            "content": {"type": "string"}
                        },
                        "required": ["type", "content"]
                    },
                    {
                        "type": "object",
                        "properties": {
                            "type": {"const": "number"},
                            "value": {"type": "number"}
                        },
                        "required": ["type", "value"]
                    }
                ]
            }
        },
        "required": ["id", "tags", "metadata", "data"]
    });
    
    let schema_id: String = sqlx::query_scalar(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition) 
         VALUES ($1, $2, $3, $4::jsonb) 
         RETURNING id::text"
    )
    .bind(&event_source)
    .bind(&event_type)
    .bind("v1.0")
    .bind(&complex_schema)
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Test valid complex payload
    let valid_complex = json!({
        "id": "ABC-1234",
        "tags": ["important", "reviewed", "production"],
        "metadata": {
            "created_at": "2024-01-01T00:00:00Z",
            "version": 2.5,
            "features": {
                "feature_async": true,
                "feature_cache": false
            }
        },
        "data": {
            "type": "text",
            "content": "This is a text content"
        }
    });
    
    let event_id = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_id)
    .bind(&valid_complex)
    .execute(&pool)
    .await;
    
    assert!(result.is_ok(), "Valid complex payload should be accepted");
    
    // Test invalid pattern
    let invalid_pattern = json!({
        "id": "123-ABCD", // Wrong pattern
        "tags": ["valid"],
        "metadata": {
            "created_at": "2024-01-01T00:00:00Z",
            "version": 1.0
        },
        "data": {
            "type": "text",
            "content": "content"
        }
    });
    
    let event_id2 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id2.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_id)
    .bind(&invalid_pattern)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "Invalid pattern should be rejected");
    
    // Test invalid oneOf
    let invalid_oneof = json!({
        "id": "XYZ-9999",
        "tags": ["tag1"],
        "metadata": {
            "created_at": "2024-01-01T00:00:00Z",
            "version": 1.0
        },
        "data": {
            "type": "text",
            "value": 123 // Should be 'content' not 'value' for text type
        }
    });
    
    let event_id3 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id3.to_string())
    .bind(&event_source)
    .bind(&event_type)
    .bind("test_host")
    .bind(&schema_id)
    .bind(&invalid_oneof)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "Invalid oneOf structure should be rejected");
    
    Ok(())
    }
}

db_test! {
    async fn test_null_schema_allows_any_payload(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // Generate unique test identifiers to avoid conflicts
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("no_schema_test-{}", test_run_id);
    
    // Insert event without schema (payload_schema_id = NULL)
    let payloads = vec![
        json!({"any": "structure"}),
        json!([1, 2, 3]),
        json!("just a string"),
        json!(42),
        json!(true),
        json!(null),
        json!({"deeply": {"nested": {"object": {"with": ["arrays", {"and": "objects"}]}}}}),
    ];
    
    for (i, payload) in payloads.iter().enumerate() {
        let event_id = Ulid::new();
        let result = sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
             VALUES ($1::ulid, $2, $3, $4, NULL, $5::jsonb)"
        )
        .bind(&event_id.to_string())
        .bind(&event_source)
        .bind(format!("type_{}", i))
        .bind("test_host")
        .bind(payload)
        .execute(&pool)
        .await;
        
        assert!(result.is_ok(), "Any payload should be accepted when schema is NULL");
    }
    
    Ok(())
    }
}
