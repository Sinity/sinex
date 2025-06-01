use sqlx::postgres::PgPoolOptions;
use sinex_ulid::Ulid;
use serde_json::json;

#[tokio::test]
async fn test_json_schema_registration() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
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
    
    let schema_id: String = sqlx::query_scalar(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition, description) 
         VALUES ($1, $2, $3, $4::jsonb, $5) 
         RETURNING id::text"
    )
    .bind("hyprland")
    .bind("window_focused")
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
}

#[tokio::test]
async fn test_json_schema_validation_constraint() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
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
    
    let schema_id: String = sqlx::query_scalar(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition) 
         VALUES ($1, $2, $3, $4::jsonb) 
         RETURNING id::text"
    )
    .bind("ui_test")
    .bind("user_interaction")
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
    .bind("ui_test")
    .bind("user_interaction")
    .bind("test_host")
    .bind(&schema_id)
    .bind(&valid_payload)
    .execute(&pool)
    .await;
    
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
    .bind("ui_test")
    .bind("user_interaction")
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
    .bind("ui_test")
    .bind("user_interaction")
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
    .bind("ui_test")
    .bind("user_interaction")
    .bind("test_host")
    .bind(&schema_id)
    .bind(&invalid_payload3)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "Invalid payload with additional properties should be rejected");
}

#[tokio::test]
async fn test_schema_versioning() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
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
    .bind("versioning_test")
    .bind("message_event")
    .bind("v1.0")
    .bind(&schema_v1)
    .bind(false) // Not active initially
    .fetch_one(&pool)
    .await
    .unwrap();
    
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
    .bind("versioning_test")
    .bind("message_event")
    .bind("v2.0")
    .bind(&schema_v2)
    .bind(true) // This is the active version
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Events with v1 schema should still validate against v1
    let v1_payload = json!({"message": "Hello"});
    let event_id1 = Ulid::new();
    
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id1.to_string())
    .bind("versioning_test")
    .bind("message_event")
    .bind("test_host")
    .bind(&schema_v1_id)
    .bind(&v1_payload)
    .execute(&pool)
    .await;
    
    assert!(result.is_ok(), "V1 payload should validate against V1 schema");
    
    // V1 payload should fail against v2 schema
    let event_id2 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload_schema_id, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::ulid, $6::jsonb)"
    )
    .bind(&event_id2.to_string())
    .bind("versioning_test")
    .bind("message_event")
    .bind("test_host")
    .bind(&schema_v2_id)
    .bind(&v1_payload)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "V1 payload should fail against V2 schema");
    
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
    .bind("versioning_test")
    .bind("message_event")
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
    .bind("versioning_test")
    .bind("message_event")
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(active_version, "v2.0", "V2 should be the active version");
}

#[tokio::test]
async fn test_complex_schema_validation() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
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
    .bind("complex_test")
    .bind("complex_event")
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
    .bind("complex_test")
    .bind("complex_event")
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
    .bind("complex_test")
    .bind("complex_event")
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
    .bind("complex_test")
    .bind("complex_event")
    .bind("test_host")
    .bind(&schema_id)
    .bind(&invalid_oneof)
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "Invalid oneOf structure should be rejected");
}

#[tokio::test]
async fn test_null_schema_allows_any_payload() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
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
        .bind("no_schema_test")
        .bind(format!("type_{}", i))
        .bind("test_host")
        .bind(payload)
        .execute(&pool)
        .await;
        
        assert!(result.is_ok(), "Any payload should be accepted when schema is NULL");
    }
}