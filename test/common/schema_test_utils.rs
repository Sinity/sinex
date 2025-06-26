//! Schema test utilities for JSON schema validation

use crate::common::prelude::*;
use serde_json::{json, Value};

/// Register test schema with event source and type
pub async fn register_test_schema(pool: &DbPool, event_source: &str, event_type: &str, schema: Value) -> Result<Ulid> {
    database::insert_test_schema(&pool, event_source, event_type, "1.0", schema).await
}

/// Assert schema validates event successfully
pub async fn assert_schema_valid_event(pool: &DbPool, event: &sinex_db::models::RawEvent, schema_id: Ulid) -> Result<(), anyhow::Error> {
    // Load the schema from database
    let schema = database::get_schema(&pool, schema_id).await?
        .ok_or_else(|| anyhow::anyhow!("Schema not found: {}", schema_id))?;
    
    // Validate using jsonschema
    let is_valid = validation::validate_payload_against_schema(&event.payload, &schema)?;
    if is_valid {
        Ok(())
    } else {
        anyhow::bail!("Schema validation failed for event")
    }
}

/// Assert schema invalidates event
pub async fn assert_schema_invalid_event(pool: &DbPool, event: &sinex_db::models::RawEvent, schema_id: Ulid) -> Result<(), anyhow::Error> {
    // Load the schema from database
    let schema = database::get_schema(&pool, schema_id).await?
        .ok_or_else(|| anyhow::anyhow!("Schema not found: {}", schema_id))?;
    
    // Validate using jsonschema - expect it to fail
    let is_valid = validation::validate_payload_against_schema(&event.payload, &schema)?;
    if is_valid {
        anyhow::bail!("Expected schema validation to fail, but it passed")
    } else {
        Ok(())
    }
}

/// Schema test utilities
pub mod schemas {
    use super::*;

    /// Create a basic filesystem event schema
    pub fn filesystem_event_schema() -> Value {
        json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "minLength": 1
                },
                "size": {
                    "type": "integer",
                    "minimum": 0
                },
                "timestamp": {
                    "type": "string",
                    "format": "date-time"
                }
            },
            "required": ["path", "size"],
            "additionalProperties": false
        })
    }

    /// Create a basic terminal event schema
    pub fn terminal_event_schema() -> Value {
        json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "minLength": 1
                },
                "exit_code": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 255
                },
                "duration_ms": {
                    "type": "integer",
                    "minimum": 0
                }
            },
            "required": ["command", "exit_code"],
            "additionalProperties": true
        })
    }

    /// Create a window manager event schema
    pub fn window_event_schema() -> Value {
        json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "window_id": {
                    "type": "string",
                    "pattern": "^0x[0-9a-fA-F]+$"
                },
                "title": {
                    "type": "string"
                },
                "class": {
                    "type": "string"
                }
            },
            "required": ["window_id"],
            "additionalProperties": true
        })
    }

    /// Create a complex nested schema for testing
    pub fn complex_nested_schema() -> Value {
        json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "metadata": {
                    "type": "object",
                    "properties": {
                        "version": {"type": "string"},
                        "tags": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["version"]
                },
                "data": {
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {"type": "integer"}
                        },
                        "enabled": {"type": "boolean"}
                    }
                }
            },
            "required": ["metadata"],
            "additionalProperties": false
        })
    }

    /// Create an overly restrictive schema for testing edge cases
    pub fn restrictive_schema() -> Value {
        json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "exactly_this": {
                    "type": "string",
                    "enum": ["only_allowed_value"]
                }
            },
            "required": ["exactly_this"],
            "additionalProperties": false
        })
    }

    /// Create a permissive schema that allows almost anything
    pub fn permissive_schema() -> Value {
        json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": true
        })
    }
}

/// Test data for schema validation
pub mod test_data {
    use super::*;

    /// Valid filesystem event payload
    pub fn valid_filesystem_payload() -> Value {
        json!({
            "path": "/home/user/test.txt",
            "size": 1024,
            "timestamp": "2024-06-20T10:00:00Z"
        })
    }

    /// Invalid filesystem event payload (missing required field)
    pub fn invalid_filesystem_payload_missing_field() -> Value {
        json!({
            "path": "/home/user/test.txt"
            // Missing required "size" field
        })
    }

    /// Invalid filesystem event payload (wrong type)
    pub fn invalid_filesystem_payload_wrong_type() -> Value {
        json!({
            "path": "/home/user/test.txt",
            "size": "not_a_number", // Should be integer
            "timestamp": "2024-06-20T10:00:00Z"
        })
    }

    /// Valid terminal event payload
    pub fn valid_terminal_payload() -> Value {
        json!({
            "command": "ls -la",
            "exit_code": 0,
            "duration_ms": 150
        })
    }

    /// Invalid terminal event payload
    pub fn invalid_terminal_payload() -> Value {
        json!({
            "command": "",  // Empty string not allowed
            "exit_code": -1 // Negative exit code not allowed
        })
    }

    /// Complex valid nested payload
    pub fn valid_complex_payload() -> Value {
        json!({
            "metadata": {
                "version": "1.0",
                "tags": ["test", "event"]
            },
            "data": {
                "items": [1, 2, 3],
                "enabled": true
            }
        })
    }

    /// Complex invalid nested payload
    pub fn invalid_complex_payload() -> Value {
        json!({
            "metadata": {
                // Missing required "version" field
                "tags": ["test", "event"]
            },
            "data": {
                "items": ["not", "numbers"], // Should be array of integers
                "enabled": true
            }
        })
    }
}

/// Schema database operations
pub mod database {
    use super::*;

    /// Insert a test schema into the database
    pub async fn insert_test_schema(
        pool: &DbPool, 
        event_source: &str,
        event_type: &str,
        schema_version: &str,
        schema: Value
    ) -> Result<Ulid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas 
            (event_source, event_type, schema_version, json_schema_definition, description)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id::uuid
            "#,
            event_source,
            event_type,
            schema_version,
            schema,
            format!("Test schema for {}.{}", event_source, event_type)
        )
        .fetch_one(pool)
        .await?;

        Ok(Ulid::from_uuid(row.id.expect("Schema ID should not be null")))
    }

    /// Get a schema from the database
    pub async fn get_schema(pool: &DbPool, schema_id: Ulid) -> Result<Option<Value>> {
        let row = sqlx::query!(
            r#"
            SELECT json_schema_definition
            FROM sinex_schemas.event_payload_schemas 
            WHERE id = $1::uuid::ulid
            "#,
            schema_id.to_uuid()
        )
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|r| r.json_schema_definition))
    }

    /// List all schemas in the database
    pub async fn list_schemas(pool: &DbPool) -> Result<Vec<(Ulid, String, String, Value)>> {
        let rows = sqlx::query!(
            r#"
            SELECT id::uuid, event_source, event_type, json_schema_definition
            FROM sinex_schemas.event_payload_schemas 
            ORDER BY created_at DESC
            "#
        )
        .fetch_all(pool)
        .await?;

        let mut schemas = Vec::new();
        for row in rows {
            let schema_id = Ulid::from_uuid(row.id.expect("Schema ID should not be null"));
            let name = format!("{}.{}", row.event_source, row.event_type);
            schemas.push((schema_id, name, row.event_source, row.json_schema_definition));
        }

        Ok(schemas)
    }

    /// Delete a schema from the database
    pub async fn delete_schema(pool: &DbPool, schema_id: Ulid) -> Result<bool> {
        let result = sqlx::query!(
            r#"
            DELETE FROM sinex_schemas.event_payload_schemas 
            WHERE id = $1::uuid::ulid
            "#,
            schema_id.to_uuid()
        )
        .execute(pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Setup test schemas in database
    pub async fn setup_test_schemas(pool: &DbPool) -> Result<Vec<(String, Ulid)>> {
        let mut schema_ids = Vec::new();

        // Insert filesystem schema
        let fs_id = insert_test_schema(&pool,
            "filesystem",
            "file.created",
            "1.0",
            schemas::filesystem_event_schema()
        ).await?;
        schema_ids.push(("filesystem.file.created".to_string(), fs_id));

        // Insert terminal schema
        let term_id = insert_test_schema(&pool,
            "terminal.kitty",
            "command.executed",
            "1.0",
            schemas::terminal_event_schema()
        ).await?;
        schema_ids.push(("terminal.command.executed".to_string(), term_id));

        // Insert window manager schema
        let wm_id = insert_test_schema(&pool,
            "hyprland",
            "window.focused",
            "1.0",
            schemas::window_event_schema()
        ).await?;
        schema_ids.push(("window_manager.window.focused".to_string(), wm_id));

        Ok(schema_ids)
    }

    /// Cleanup test schemas from database
    pub async fn cleanup_test_schemas(pool: &DbPool, schema_ids: &[Ulid]) -> Result<(), anyhow::Error> {
        for &schema_id in schema_ids {
            delete_schema(&pool, schema_id).await?;
        }
        Ok(())
    }
}

/// Schema validation testing utilities
pub mod validation {
    use super::*;
    use jsonschema::JSONSchema;

    /// Test a payload against a schema
    pub fn validate_payload_against_schema(payload: &Value, schema: &Value) -> Result<bool> {
        let compiled_schema = JSONSchema::compile(schema)
            .map_err(|e| anyhow::anyhow!("Failed to compile schema: {}", e))?;

        let is_valid = compiled_schema.is_valid(payload);
        Ok(is_valid)
    }

    /// Get validation errors for a payload against a schema
    pub fn get_validation_errors(payload: &Value, schema: &Value) -> Result<Vec<String>> {
        let compiled_schema = JSONSchema::compile(schema)
            .map_err(|e| anyhow::anyhow!("Failed to compile schema: {}", e))?;

        let validation_result = compiled_schema.validate(payload);
        
        match validation_result {
            Ok(()) => Ok(Vec::new()),
            Err(errors) => {
                let error_messages: Vec<String> = errors.map(|e| e.to_string()).collect();
                Ok(error_messages)
            }
        }
    }

    /// Test schema compilation
    pub fn test_schema_compilation(schema: &Value) -> Result<(), anyhow::Error> {
        JSONSchema::compile(schema)
            .map_err(|e| anyhow::anyhow!("Schema compilation failed: {}", e))?;
        Ok(())
    }

    /// Run comprehensive validation tests
    pub fn run_schema_validation_tests() -> Result<(), anyhow::Error> {
        // Test filesystem schema
        let fs_schema = schemas::filesystem_event_schema();
        test_schema_compilation(&fs_schema)?;
        
        assert!(validate_payload_against_schema(
            &test_data::valid_filesystem_payload(),
            &fs_schema
        )?);
        
        assert!(!validate_payload_against_schema(
            &test_data::invalid_filesystem_payload_missing_field(),
            &fs_schema
        )?);

        // Test terminal schema
        let term_schema = schemas::terminal_event_schema();
        test_schema_compilation(&term_schema)?;
        
        assert!(validate_payload_against_schema(
            &test_data::valid_terminal_payload(),
            &term_schema
        )?);
        
        assert!(!validate_payload_against_schema(
            &test_data::invalid_terminal_payload(),
            &term_schema
        )?);

        // Test complex schema
        let complex_schema = schemas::complex_nested_schema();
        test_schema_compilation(&complex_schema)?;
        
        assert!(validate_payload_against_schema(
            &test_data::valid_complex_payload(),
            &complex_schema
        )?);
        
        assert!(!validate_payload_against_schema(
            &test_data::invalid_complex_payload(),
            &complex_schema
        )?);

        Ok(())
    }
}

/// Performance testing for schemas
pub mod performance {
    use super::*;
    use std::time::{Duration, Instant};

    /// Benchmark schema compilation
    pub fn benchmark_schema_compilation(schemas: &[Value], iterations: usize) -> Vec<Duration> {
        schemas.iter().map(|schema| {
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = jsonschema::JSONSchema::compile(schema);
            }
            start.elapsed()
        }).collect()
    }

    /// Benchmark validation performance
    pub fn benchmark_validation(
        schema: &Value, 
        payloads: &[Value], 
        iterations: usize
    ) -> Result<Vec<Duration>> {
        let compiled_schema = jsonschema::JSONSchema::compile(schema)
            .map_err(|e| anyhow::anyhow!("Failed to compile schema: {}", e))?;

        let results = payloads.iter().map(|payload| {
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = compiled_schema.is_valid(payload);
            }
            start.elapsed()
        }).collect();

        Ok(results)
    }

    /// Test validation performance under concurrent load
    pub async fn concurrent_validation_benchmark(
        schema: &Value,
        payload: &Value,
        concurrent_tasks: usize,
        operations_per_task: usize
    ) -> Result<Duration> {
        use tokio::task;

        let compiled_schema = jsonschema::JSONSchema::compile(schema)
            .map_err(|e| anyhow::anyhow!("Failed to compile schema: {}", e))?;

        let schema_arc = Arc::new(compiled_schema);
        let payload_arc = Arc::new(payload.clone());
        let start = Instant::now();

        let mut handles = Vec::new();
        
        for _ in 0..concurrent_tasks {
            let schema_clone = schema_arc.clone();
            let payload_clone = payload_arc.clone();
            
            let handle = task::spawn(async move {
                for _ in 0..operations_per_task {
                    let _ = schema_clone.is_valid(&payload_clone);
                }
            });
            
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await?;
        }

        Ok(start.elapsed())
    }
}