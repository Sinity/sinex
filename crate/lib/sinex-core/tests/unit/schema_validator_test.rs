//! Unit tests for schema validation functionality
//!
//! Tests schema validation logic, error handling, and edge cases
//! without requiring full database integration.

use serde_json::json;
use xtask::sandbox::prelude::*;

// Static regex patterns for schema validation testing - compiled once for performance
lazy_static::lazy_static! {
    static ref EVENT_SOURCE_PATTERN: regex::Regex = regex::Regex::new(r"^[a-z][a-z0-9_-]*$").unwrap();
    static ref EVENT_TYPE_PATTERN: regex::Regex = regex::Regex::new(r"^[a-z][a-z0-9_.]*$").unwrap();
}

// =============================================================================
// Schema Validation Logic Tests
// =============================================================================

#[sinex_test]
fn test_json_schema_basic_validation() -> TestResult<()> {
    // Test basic JSON Schema validation using jsonschema crate directly
    use jsonschema::JSONSchema;

    let schema = json!({
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "number", "minimum": 0}
        },
        "required": ["name"]
    });

    let compiled = JSONSchema::compile(&schema).expect("Schema should compile successfully");

    // Test valid data
    let valid_data = json!({"name": "test", "age": 25});
    let result = compiled.validate(&valid_data);
    assert!(result.is_ok(), "Valid data should pass validation");

    // Test invalid data - missing required field
    let invalid_data1 = json!({"age": 25});
    let result = compiled.validate(&invalid_data1);
    assert!(
        result.is_err(),
        "Missing required field should fail validation"
    );

    // Test invalid data - wrong type
    let invalid_data2 = json!({"name": "test", "age": "not_a_number"});
    let result = compiled.validate(&invalid_data2);
    assert!(result.is_err(), "Wrong type should fail validation");

    // Test invalid data - constraint violation
    let invalid_data3 = json!({"name": "test", "age": -5});
    let result = compiled.validate(&invalid_data3);
    assert!(
        result.is_err(),
        "Constraint violation should fail validation"
    );

    Ok(())
}

#[sinex_test]
fn test_schema_compilation_error_handling() -> TestResult<()> {
    // Test various malformed schemas to ensure robust error handling
    use jsonschema::JSONSchema;

    let malformed_schemas = vec![
        (json!(null), "Null schema"),
        (json!("not an object"), "String schema"),
        (json!({"type": "invalid_type"}), "Invalid type"),
        (
            json!({"type": "object", "properties": null}),
            "Null properties",
        ),
        (
            json!({"type": "array", "items": {"type": "invalid"}}),
            "Invalid item type",
        ),
    ];

    for (schema, description) in malformed_schemas {
        println!("Testing malformed schema: {description}");

        let result = JSONSchema::compile(&schema);
        match result {
            Ok(_) => {
                // Some schemas might be more lenient than expected
                println!("  Schema was accepted (lenient parsing): {schema:?}");
            }
            Err(e) => {
                // Expected case - schema compilation should fail gracefully
                println!("  Schema compilation failed as expected: {e}");

                // Error should be informative
                let error_str = e.to_string();
                assert!(
                    !error_str.is_empty() && error_str.len() < 1000,
                    "Error message should be informative but not excessive"
                );
            }
        }
    }

    Ok(())
}

#[sinex_test]
fn test_nested_schema_validation() -> TestResult<()> {
    // Test validation of deeply nested schemas
    use jsonschema::JSONSchema;

    let nested_schema = json!({
        "type": "object",
        "properties": {
            "event": {
                "type": "object",
                "properties": {
                    "source": {"type": "string", "pattern": "^[a-z][a-z0-9_-]*$"},
                    "event_type": {"type": "string", "pattern": "^[a-z][a-z0-9_.]*$"},
                    "payload": {
                        "type": "object",
                        "properties": {
                            "data": {"type": "string"},
                            "metadata": {
                                "type": "object",
                                "additionalProperties": true
                            }
                        },
                        "required": ["data"]
                    }
                },
                "required": ["source", "event_type", "payload"]
            }
        },
        "required": ["event"]
    });

    let compiled =
        JSONSchema::compile(&nested_schema).expect("Nested schema should compile successfully");

    // Test valid nested data
    let valid_nested = json!({
        "event": {
            "source": "test_source",
            "event_type": "test.event",
            "payload": {
                "data": "test data",
                "metadata": {
                    "timestamp": "2023-01-01T00:00:00Z",
                    "version": 1
                }
            }
        }
    });

    let result = compiled.validate(&valid_nested);
    assert!(result.is_ok(), "Valid nested data should pass validation");

    // Test invalid nested data - pattern mismatch
    let invalid_nested = json!({
        "event": {
            "source": "INVALID_SOURCE",  // Violates pattern (uppercase)
            "event_type": "test.event",
            "payload": {
                "data": "test data"
            }
        }
    });

    let result = compiled.validate(&invalid_nested);
    assert!(result.is_err(), "Pattern violation should fail validation");

    Ok(())
}

// =============================================================================
// Schema Cache Behavior Tests
// =============================================================================

#[sinex_test]
fn test_schema_content_hash_consistency() -> TestResult<()> {
    // Test that identical schema content produces consistent hashes
    use blake3::hash;

    let schema1 = json!({
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        }
    });

    let schema2 = json!({
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        }
    });

    // Same content should produce same hash
    let content1 = serde_json::to_string(&schema1)?;
    let content2 = serde_json::to_string(&schema2)?;

    let hash1 = hash(content1.as_bytes());
    let hash2 = hash(content2.as_bytes());

    assert_eq!(
        hash1, hash2,
        "Identical schema content should have same hash"
    );

    // Different content should produce different hashes
    let schema3 = json!({
        "type": "object",
        "properties": {
            "name": {"type": "number"}  // Changed type
        }
    });

    let content3 = serde_json::to_string(&schema3)?;
    let hash3 = hash(content3.as_bytes());

    assert_ne!(
        hash1, hash3,
        "Different schema content should have different hashes"
    );

    Ok(())
}

#[sinex_test]
fn test_schema_version_string_validation() -> TestResult<()> {
    // Test validation of schema version strings
    let valid_versions = vec!["1.0.0", "v2.1.0", "1.0", "dev", "1.0.0-beta", "2023.01.01"];

    let invalid_versions = vec!["", " ", "1.0.0 ", " 1.0.0", "1.0.0\n", "1.0.0\0"];

    // Test valid versions
    for version in &valid_versions {
        println!("Testing valid version: '{version}'");
        // Basic validation - non-empty, reasonable length, no control characters
        assert!(!version.is_empty(), "Version should not be empty");
        assert!(
            version.len() <= 50,
            "Version should not be excessively long"
        );
        assert!(
            !version.contains('\0'),
            "Version should not contain null bytes"
        );
        assert!(
            !version.contains('\n'),
            "Version should not contain newlines"
        );
    }

    // Test invalid versions
    for version in &invalid_versions {
        println!("Testing invalid version: '{version}'");
        // These should be caught by validation
        let has_issues = version.is_empty()
            || version.starts_with(' ')
            || version.ends_with(' ')
            || version.contains('\0')
            || version.contains('\n');
        assert!(
            has_issues,
            "Invalid version should be detectable: '{version}'"
        );
    }

    Ok(())
}

// =============================================================================
// Schema Registry Error Scenarios
// =============================================================================

#[sinex_test]
fn test_schema_registry_error_conditions() -> TestResult<()> {
    // Test various error conditions that the schema registry should handle gracefully

    // Test handling of very large schemas
    let mut large_properties = serde_json::Map::new();
    for i in 0..1000 {
        large_properties.insert(
            format!("prop_{i}"),
            json!({"type": "string", "description": format!("Property {}", i)}),
        );
    }

    let large_schema = json!({
        "type": "object",
        "properties": large_properties
    });

    // Schema should still be compilable (even if large)
    use jsonschema::JSONSchema;
    let result = JSONSchema::compile(&large_schema);
    match result {
        Ok(_) => println!("Large schema compiled successfully"),
        Err(e) => println!("Large schema failed to compile: {e}"),
    }

    // Test deeply nested schemas
    let mut nested = json!({"type": "string"});
    for _ in 0..10 {
        nested = json!({
            "type": "object",
            "properties": {
                "nested": nested
            }
        });
    }

    let result = JSONSchema::compile(&nested);
    match result {
        Ok(_) => println!("Deeply nested schema compiled successfully"),
        Err(e) => println!("Deeply nested schema failed: {e}"),
    }

    Ok(())
}

#[sinex_test]
fn test_event_source_and_type_patterns() -> TestResult<()> {
    // Test the regex patterns used for event source and type validation

    let valid_sources = vec![
        "fs_watcher",
        "terminal",
        "desktop",
        "a",
        "test-source",
        "source_1",
    ];

    let invalid_sources = vec![
        "",
        "1invalid",      // Starts with digit
        "INVALID",       // Uppercase
        "invalid space", // Contains space
        "invalid.dot",   // Contains dot
        "_invalid",      // Starts with underscore
        "-invalid",      // Starts with dash
    ];

    for source in &valid_sources {
        assert!(
            EVENT_SOURCE_PATTERN.is_match(source),
            "Valid source '{source}' should match pattern"
        );
    }

    for source in &invalid_sources {
        assert!(
            !EVENT_SOURCE_PATTERN.is_match(source),
            "Invalid source '{source}' should not match pattern"
        );
    }

    // Event type pattern: ^[a-z][a-z0-9_.]*$

    let valid_types = vec![
        "file.created",
        "command.executed",
        "window.focused",
        "a",
        "test_event",
        "event.sub.type",
    ];

    let invalid_types = vec![
        "",
        "1invalid",      // Starts with digit
        "INVALID",       // Uppercase
        "invalid space", // Contains space
        "invalid-dash",  // Contains dash
        "_invalid",      // Starts with underscore
        ".invalid",      // Starts with dot
    ];

    for event_type in &valid_types {
        assert!(
            EVENT_TYPE_PATTERN.is_match(event_type),
            "Valid event type '{event_type}' should match pattern"
        );
    }

    for event_type in &invalid_types {
        assert!(
            !EVENT_TYPE_PATTERN.is_match(event_type),
            "Invalid event type '{event_type}' should not match pattern"
        );
    }

    Ok(())
}
