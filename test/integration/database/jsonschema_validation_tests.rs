//! Migrated version of jsonschema_validation_tests.rs using new test infrastructure
//!
//! This demonstrates:
//! - Using #[sinex_test] macro for automatic setup
//! - TestContext for unified database access
//! - Event builders for cleaner event creation
//! - No manual pool management needed

use crate::common::prelude::*;
use crate::common::schema_test_utils;
use uuid::Uuid;

#[sinex_test]
async fn test_json_schema_registration(ctx: TestContext) -> TestResult {
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
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("hyprland-test-{}", test_run_id);
    let event_type = format!("window_focused-{}", test_run_id);
    
    let schema_clone = schema.clone();
    let schema_id = schema_test_utils::register_test_schema(ctx.pool(),
        &event_source,
        &event_type,
        schema
    ).await?;
    
    // Verify schema was stored correctly
    let retrieved_schema: serde_json::Value = sqlx::query_scalar(
        "SELECT json_schema_definition FROM sinex_schemas.event_payload_schemas WHERE id = $1::ulid"
    )
    .bind(schema_id.to_uuid())
    .fetch_one(ctx.pool())
    .await?;
    
    pretty_assertions::assert_eq!(retrieved_schema, schema_clone, "Schema should be stored correctly");
    
    Ok(())
}

#[sinex_test]
async fn test_json_schema_validation_constraint(ctx: TestContext) -> TestResult {
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
    
    let schema_id = Ulid::from_uuid(sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition) 
         VALUES ($1, $2, $3, $4::jsonb) 
         RETURNING id::uuid"
    )
    .bind(&event_source)
    .bind(&event_type)
    .bind("v1.0")
    .bind(&strict_schema)
    .fetch_one(ctx.pool())
    .await?);
    
    // Test valid payload - using event builder for cleaner syntax
    let valid_event = ctx.event_builder(&event_source, &event_type)
        .payload(json!({
            "action": "click",
            "element_id": "submit-button",
            "coordinates": {
                "x": 100.5,
                "y": 200.0
            }
        }))
        .build();
    
    schema_test_utils::assert_schema_valid_event(ctx.pool(), &valid_event, schema_id).await?;
    
    // Test invalid payload - missing required field
    let invalid_event1 = ctx.event_builder(&event_source, &event_type)
        .payload(json!({
            "action": "click"
            // missing element_id
        }))
        .build();
    
    schema_test_utils::assert_schema_invalid_event(ctx.pool(), &invalid_event1, schema_id).await?;
    
    // Test invalid payload - wrong enum value
    let invalid_event2 = ctx.event_builder(&event_source, &event_type)
        .payload(json!({
            "action": "drag", // not in enum
            "element_id": "some-element"
        }))
        .build();
    
    schema_test_utils::assert_schema_invalid_event(ctx.pool(), &invalid_event2, schema_id).await?;
    
    // Test invalid payload - additional properties
    let invalid_event3 = ctx.event_builder(&event_source, &event_type)
        .payload(json!({
            "action": "click",
            "element_id": "button",
            "extra_field": "not allowed"
        }))
        .build();
    
    schema_test_utils::assert_schema_invalid_event(ctx.pool(), &invalid_event3, schema_id).await?;
    
    Ok(())
}

#[sinex_test]
async fn test_event_type_schema_caching(ctx: TestContext) -> TestResult {
    // Demonstrates using TestContext helpers for more complex scenarios
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("cache-test-{}", test_run_id);
    let event_type = format!("cached-event-{}", test_run_id);
    
    // Register schema
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "value": {"type": "integer"}
        },
        "required": ["value"]
    });
    
    let _schema_id = schema_test_utils::register_test_schema(ctx.pool(),
        &event_source,
        &event_type,
        schema
    ).await?;
    
    // Test with multiple events in a batch - demonstrates batch creation
    let events = (0..5).map(|i| {
        ctx.event_builder(&event_source, &event_type)
            .payload(json!({ "value": i }))
            .build()
    }).collect::<Vec<_>>();
    
    // Insert all events
    for event in &events {
        ctx.insert_event(event).await?;
    }
    
    // Wait for processing
    ctx.wait_for_event_count(5).await?;
    
    // Verify all events were inserted
    let count = ctx.event_count().await?;
    assert!(count >= 5, "Should have at least 5 events");
    
    Ok(())
}

#[sinex_test]
async fn test_schema_evolution(ctx: TestContext) -> TestResult {
    // Test schema version evolution
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("evolution-test-{}", test_run_id);
    let event_type = format!("evolving-event-{}", test_run_id);
    
    // Version 1 schema
    let schema_v1 = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        },
        "required": ["name"]
    });
    
    let _schema_id_v1 = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition) 
         VALUES ($1, $2, 'v1', $3::jsonb) 
         RETURNING id::uuid"
    )
    .bind(&event_source)
    .bind(&event_type)
    .bind(&schema_v1)
    .fetch_one(ctx.pool())
    .await?;
    
    // Version 2 schema (backward compatible)
    let schema_v2 = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"}
        },
        "required": ["name"]
    });
    
    let schema_id_v2 = Ulid::from_uuid(sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO sinex_schemas.event_payload_schemas 
         (event_source, event_type, schema_version, json_schema_definition) 
         VALUES ($1, $2, 'v2', $3::jsonb) 
         RETURNING id::uuid"
    )
    .bind(&event_source)
    .bind(&event_type)
    .bind(&schema_v2)
    .fetch_one(ctx.pool())
    .await?);
    
    // Old event (v1 compatible) should work with v2 schema
    let v1_event = ctx.event_builder(&event_source, &event_type)
        .payload(json!({ "name": "Test" }))
        .build();
    
    schema_test_utils::assert_schema_valid_event(ctx.pool(), &v1_event, schema_id_v2).await?;
    
    // New event with v2 fields
    let v2_event = ctx.event_builder(&event_source, &event_type)
        .payload(json!({ "name": "Test", "age": 25 }))
        .build();
    
    schema_test_utils::assert_schema_valid_event(ctx.pool(), &v2_event, schema_id_v2).await?;
    
    Ok(())
}

#[sinex_test]
async fn test_complex_nested_schema_validation(ctx: TestContext) -> TestResult {
    // Test deeply nested schema validation
    let complex_schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "user": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "format": "uuid"},
                    "profile": {
                        "type": "object",
                        "properties": {
                            "settings": {
                                "type": "object",
                                "properties": {
                                    "theme": {"type": "string", "enum": ["light", "dark"]},
                                    "notifications": {"type": "boolean"}
                                },
                                "required": ["theme"]
                            }
                        },
                        "required": ["settings"]
                    }
                },
                "required": ["id", "profile"]
            }
        },
        "required": ["user"]
    });
    
    let test_run_id = &Uuid::new_v4().to_string()[..8];
    let event_source = format!("complex-test-{}", test_run_id);
    let event_type = format!("nested-event-{}", test_run_id);
    
    let schema_id = schema_test_utils::register_test_schema(ctx.pool(),
        &event_source,
        &event_type,
        complex_schema
    ).await?;
    
    // Valid deeply nested payload
    let valid_event = ctx.event_builder(&event_source, &event_type)
        .payload(json!({
            "user": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "profile": {
                    "settings": {
                        "theme": "dark",
                        "notifications": true
                    }
                }
            }
        }))
        .build();
    
    schema_test_utils::assert_schema_valid_event(ctx.pool(), &valid_event, schema_id).await?;
    
    // Invalid - missing deep required field
    let invalid_event = ctx.event_builder(&event_source, &event_type)
        .payload(json!({
            "user": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "profile": {
                    "settings": {
                        // missing required "theme"
                        "notifications": false
                    }
                }
            }
        }))
        .build();
    
    schema_test_utils::assert_schema_invalid_event(ctx.pool(), &invalid_event, schema_id).await?;
    
    Ok(())
}