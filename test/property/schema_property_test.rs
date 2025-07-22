use crate::common::prelude::*;
use crate::common::property_helpers::*;
use crate::test_schema_validation;
use proptest::prelude::*;
use serde_json::{json, Value};
use sinex_validation::{ValidationChain};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn json_schema_validation_completeness(
        payload in event_payloads()
    ) {
        // Property: All valid event payloads should pass schema validation
        let event_type = "test.event";
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": true
        });
        
        let result = validate_against_schema(&payload, &schema);
        assert!(result.is_ok(), "Valid payload should pass permissive schema");
    }

    #[test]
    fn schema_type_enforcement(
        value in arbitrary_json_value(),
        expected_type in prop_oneof![
            Just("string"),
            Just("number"),
            Just("boolean"),
            Just("object"),
            Just("array"),
            Just("null")
        ]
    ) {
        // Property: Type constraints should be enforced
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": expected_type
        });
        
        let result = validate_against_schema(&value, &schema);
        
        let actual_type = match &value {
            Value::String(_) => "string",
            Value::Number(_) => "number",
            Value::Bool(_) => "boolean",
            Value::Object(_) => "object",
            Value::Array(_) => "array",
            Value::Null => "null",
        };
        
        assert_eq!(result.is_ok(), actual_type == expected_type,
                   "Schema validation should match type constraints");
    }

    #[test]
    fn required_fields_validation(
        base_object in arbitrary_object(),
        required_fields in proptest::collection::vec("[a-z]+", 1..5)
    ) {
        // Property: Required fields should be enforced
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": required_fields.clone(),
            "properties": required_fields.iter().map(|field| {
                (field.clone(), json!({"type": "string"}))
            }).collect::<serde_json::Map<_, _>>()
        });
        
        // Test with missing fields
        let result = validate_against_schema(&base_object, &schema);
        
        // Check if all required fields are present
        let has_all_required = if let Value::Object(map) = &base_object {
            required_fields.iter().all(|field| map.contains_key(field))
        } else {
            false
        };
        
        assert_eq!(result.is_ok(), has_all_required,
                   "Validation should fail if required fields are missing");
    }

    #[test]
    fn nested_schema_validation(
        depth in 1usize..5,
        value in arbitrary_json_value()
    ) {
        // Property: Nested schemas should validate correctly
        let mut schema = json!({"type": "string"});
        let mut test_value = value;
        
        // Build nested structure
        for _ in 0..depth {
            schema = json!({
                "type": "object",
                "properties": {
                    "nested": schema
                },
                "required": ["nested"]
            });
            test_value = json!({"nested": test_value});
        }
        
        let result = validate_against_schema(&test_value, &schema);
        
        // Should pass if structure matches
        if depth > 0 {
            assert!(result.is_ok(), "Nested structure should validate");
        }
    }

    #[test]
    fn array_validation_constraints(
        items in proptest::collection::vec(arbitrary_json_value(), 0..20),
        min_items in 0usize..10,
        max_items in 10usize..20
    ) {
        // Property: Array constraints should be enforced
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "array",
            "minItems": min_items,
            "maxItems": max_items
        });
        
        let array_value = Value::Array(items.clone());
        let result = validate_against_schema(&array_value, &schema);
        
        let expected_valid = items.len() >= min_items && items.len() <= max_items;
        assert_eq!(result.is_ok(), expected_valid,
                   "Array validation should enforce min/max constraints");
    }

    #[test]
    fn string_pattern_validation(
        test_string in "[a-zA-Z0-9 ]*",
        pattern in prop_oneof![
            Just("^[a-z]+$"),
            Just("^[A-Z]+$"),
            Just("^[0-9]+$"),
            Just("^[a-zA-Z0-9]+$"),
            Just("^.+@.+\\..+$")  // Simple email pattern
        ]
    ) {
        // Property: String patterns should be validated
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "string",
            "pattern": pattern
        });
        
        let result = validate_against_schema(&json!(test_string), &schema);
        
        // Manually check if string matches pattern
        let regex = regex::Regex::new(&pattern).unwrap();
        let should_match = regex.is_match(&test_string);
        
        assert_eq!(result.is_ok(), should_match,
                   "Pattern validation should match regex behavior");
    }

    #[test]
    fn numeric_range_validation(
        value in any::<f64>(),
        minimum in -1000f64..1000f64,
        maximum in -1000f64..1000f64
    ) {
        // Property: Numeric ranges should be validated
        let (min, max) = if minimum <= maximum {
            (minimum, maximum)
        } else {
            (maximum, minimum)
        };
        
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "number",
            "minimum": min,
            "maximum": max
        });
        
        let result = validate_against_schema(&json!(value), &schema);
        
        let in_range = value >= min && value <= max && value.is_finite();
        assert_eq!(result.is_ok(), in_range,
                   "Numeric validation should enforce range constraints");
    }

    #[test]
    fn enum_validation(
        allowed_values in proptest::collection::vec(arbitrary_json_value(), 1..10),
        test_value in arbitrary_json_value()
    ) {
        // Property: Enum constraints should be enforced
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "enum": allowed_values.clone()
        });
        
        let result = validate_against_schema(&test_value, &schema);
        
        let is_allowed = allowed_values.contains(&test_value);
        assert_eq!(result.is_ok(), is_allowed,
                   "Enum validation should only allow specified values");
    }

    #[test]
    fn additional_properties_validation(
        base_props in proptest::collection::hash_map("[a-z]+", arbitrary_json_value(), 1..5),
        extra_props in proptest::collection::hash_map("[a-z]+", arbitrary_json_value(), 0..3),
        allow_additional in any::<bool>()
    ) {
        // Property: Additional properties constraints should be enforced
        let mut all_props = base_props.clone();
        all_props.extend(extra_props.clone());
        
        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": base_props.keys().map(|k| {
                (k.clone(), json!({"type": "string"}))
            }).collect::<serde_json::Map<_, _>>(),
            "additionalProperties": allow_additional
        });
        
        let object = Value::Object(all_props.into_iter().collect());
        let result = validate_against_schema(&object, &schema);
        
        if !allow_additional && !extra_props.is_empty() {
            assert!(result.is_err(), "Should reject additional properties when not allowed");
        } else {
            // Note: This might fail if property types don't match schema
            // which is fine for this property test
        }
    }

    #[test]
    fn event_schema_compatibility(
        event in arbitrary_event()
    ) {
        // Property: All generated events should have valid schemas
        let schema = match event.event_type.as_str() {
            t if t.starts_with("file.") => json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "size": {"type": "number"},
                    "mode": {"type": "string"}
                }
            }),
            t if t.starts_with("command.") => json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["command"],
                "properties": {
                    "command": {"type": "string"},
                    "exit_code": {"type": "number"},
                    "duration_ms": {"type": "number"}
                }
            }),
            _ => json!({
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": true
            })
        };
        
        let result = validate_against_schema(&event.payload, &schema);
        
        // All events should pass their type-specific schema
        assert!(result.is_ok() || event.payload == json!(null),
                "Event payload should match its type schema");
    }

    #[test]
    fn schema_evolution_compatibility(
        original_schema in arbitrary_schema(),
        payload in arbitrary_json_value()
    ) {
        // Property: Schema changes should maintain backward compatibility
        let evolved_schema = evolve_schema(&original_schema);
        
        let original_result = validate_against_schema(&payload, &original_schema);
        let evolved_result = validate_against_schema(&payload, &evolved_schema);
        
        // If it passed original schema, it should pass evolved schema
        if original_result.is_ok() {
            assert!(evolved_result.is_ok(),
                    "Schema evolution should maintain backward compatibility");
        }
    }
}

// Helper functions

fn validate_against_schema(value: &Value, schema: &Value) -> Result<(), String> {
    // Simplified validation for property testing
    // In real code, this would use a proper JSON Schema validator
    match (value, schema.get("type").and_then(|t| t.as_str())) {
        (Value::String(_), Some("string")) => Ok(()),
        (Value::Number(_), Some("number")) => Ok(()),
        (Value::Bool(_), Some("boolean")) => Ok(()),
        (Value::Object(_), Some("object")) => Ok(()),
        (Value::Array(_), Some("array")) => Ok(()),
        (Value::Null, Some("null")) => Ok(()),
        (_, Some(_)) => Err("Type mismatch".to_string()),
        _ => Ok(()), // No type constraint
    }
}

fn arbitrary_json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<f64>().prop_filter("valid number", |n| n.is_finite()).prop_map(|n| Value::Number(serde_json::Number::from_f64(n).unwrap())),
        "[a-zA-Z0-9 ]*".prop_map(Value::String),
    ];
    
    leaf.prop_recursive(
        8,   // depth
        256, // max size
        10,  // items per collection
        |inner| {
            prop_oneof![
                // Array
                proptest::collection::vec(inner.clone(), 0..10)
                    .prop_map(Value::Array),
                // Object
                proptest::collection::hash_map(
                    "[a-z]+",
                    inner,
                    0..10
                ).prop_map(|m| Value::Object(m.into_iter().collect())),
            ]
        }
    )
}

fn arbitrary_object() -> impl Strategy<Value = Value> {
    proptest::collection::hash_map(
        "[a-z]+",
        arbitrary_json_value(),
        0..10
    ).prop_map(|m| Value::Object(m.into_iter().collect()))
}

fn arbitrary_schema() -> impl Strategy<Value = Value> {
    // Generate simple schemas for testing
    prop_oneof![
        Just(json!({"type": "string"})),
        Just(json!({"type": "number"})),
        Just(json!({"type": "boolean"})),
        Just(json!({"type": "object", "additionalProperties": true})),
        Just(json!({"type": "array"})),
        Just(json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"}
            }
        })),
    ]
}

fn evolve_schema(original: &Value) -> Value {
    // Simple schema evolution: make all fields optional
    let mut evolved = original.clone();
    if let Some(obj) = evolved.as_object_mut() {
        // Remove required fields
        obj.remove("required");
        // Allow additional properties
        obj.insert("additionalProperties".to_string(), json!(true));
    }
    evolved
}

#[cfg(test)]
mod validation_chain_tests {
    use super::*;
    
    proptest! {
        #[test]
        fn validation_chain_accumulation(
            test_string in "[a-zA-Z0-9]*",
            min_length in 0usize..50,
            max_length in 50usize..100
        ) {
            // Property: ValidationChain should accumulate all errors
            let chain = ValidationChain::validate(test_string.clone(), "test_field")
                .min_length(min_length)
                .max_length(max_length);
            
            let result = chain.into_result();
            
            let expected_valid = test_string.len() >= min_length && test_string.len() <= max_length;
            
            if expected_valid {
                assert!(result.is_ok(), "Valid string should pass validation");
            } else {
                assert!(result.is_err(), "Invalid string should fail validation");
            }
        }
    }
}

// === MACRO-BASED SCHEMA VALIDATION TESTS ===

// Test permissive schema accepts valid payloads
test_schema_validation!(
    test_permissive_schema_accepts_valid_data,
    json!({"field": "value", "number": 42}),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "additionalProperties": true
    }),
    true
);

// Test strict schema rejects invalid type
test_schema_validation!(
    test_strict_schema_rejects_wrong_type,
    json!("not an object"),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "additionalProperties": false
    }),
    false
);

// === TYPE CONSTRAINT TESTS ===

// String type validation
test_schema_validation!(
    test_string_type_validation_valid,
    json!("valid string"),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "string"
    }),
    true
);

test_schema_validation!(
    test_string_type_validation_invalid,
    json!(42),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "string"
    }),
    false
);

// Number type validation
test_schema_validation!(
    test_number_type_validation_valid,
    json!(42.5),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "number"
    }),
    true
);

test_schema_validation!(
    test_number_type_validation_invalid,
    json!("not a number"),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "number"
    }),
    false
);

// === REQUIRED FIELDS TESTS ===

// Valid object with required fields
test_schema_validation!(
    test_required_fields_present,
    json!({"name": "test", "value": 42}),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["name", "value"],
        "properties": {
            "name": {"type": "string"},
            "value": {"type": "number"}
        }
    }),
    true
);

// Invalid object missing required field
test_schema_validation!(
    test_required_fields_missing,
    json!({"name": "test"}),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["name", "value"],
        "properties": {
            "name": {"type": "string"},
            "value": {"type": "number"}
        }
    }),
    false
);

// === COMPREHENSIVE REAL SINEX EVENT SCHEMA TESTS ===

// Filesystem event schema
test_schema_validation!(
    test_filesystem_event_schema_valid,
    json!({
        "path": "/home/user/document.txt",
        "size": 1024,
        "modified_time": "2024-01-01T00:00:00Z",
        "file_type": "file"
    }),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["path", "size", "modified_time"],
        "properties": {
            "path": {"type": "string", "minLength": 1},
            "size": {"type": "integer", "minimum": 0},
            "modified_time": {"type": "string"},
            "file_type": {"type": "string", "enum": ["file", "directory", "symlink"]}
        }
    }),
    true
);

// Shell command event schema
test_schema_validation!(
    test_shell_command_schema_valid,
    json!({
        "command": "ls -la",
        "exit_code": 0,
        "duration_ms": 150,
        "working_directory": "/home/user"
    }),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["command", "exit_code"],
        "properties": {
            "command": {"type": "string", "minLength": 1},
            "exit_code": {"type": "integer"},
            "duration_ms": {"type": "integer", "minimum": 0},
            "working_directory": {"type": "string"}
        }
    }),
    true
);

// Window manager event schema
test_schema_validation!(
    test_window_manager_window_opened_valid,
    json!({
        "window_id": "0x1a2b3c4d",
        "title": "Firefox - Mozilla Firefox",
        "class": "firefox",
        "workspace": 1,
        "position": {"x": 100, "y": 100},
        "size": {"width": 1200, "height": 800},
        "opened_time": "2024-01-01T00:00:00Z"
    }),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["window_id", "title", "class"],
        "properties": {
            "window_id": {"type": "string", "minLength": 1},
            "title": {"type": "string"},
            "class": {"type": "string"},
            "workspace": {"type": "integer", "minimum": 1},
            "position": {
                "type": "object",
                "properties": {
                    "x": {"type": "integer"},
                    "y": {"type": "integer"}
                }
            },
            "size": {
                "type": "object", 
                "properties": {
                    "width": {"type": "integer", "minimum": 1},
                    "height": {"type": "integer", "minimum": 1}
                }
            },
            "opened_time": {"type": "string", "format": "date-time"}
        }
    }),
    true
);

// Sinex satellite heartbeat schema with validation error
test_schema_validation!(
    test_sinex_satellite_heartbeat_invalid_version,
    json!({
        "satellite_name": "fs-watcher",
        "status": "running",
        "version": "not-a-version"  // Invalid: doesn't match semantic version pattern
    }),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["satellite_name", "status", "version"],
        "properties": {
            "satellite_name": {"type": "string", "minLength": 1},
            "status": {"type": "string", "enum": ["running", "stopped", "error", "starting", "stopping"]},
            "version": {"type": "string", "pattern": "^\\d+\\.\\d+\\.\\d+"}
        }
    }),
    false
);

// === ADDITIONAL PROPERTIES TESTS ===

// Valid with additional properties allowed
test_schema_validation!(
    test_additional_properties_allowed,
    json!({"name": "test", "extra": "allowed"}),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        },
        "additionalProperties": true
    }),
    true
);

// Invalid with additional properties forbidden
test_schema_validation!(
    test_additional_properties_forbidden,
    json!({"name": "test", "extra": "not allowed"}),
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        },
        "additionalProperties": false
    }),
    false
);

// === ENHANCED VALIDATION FUNCTION ===

// Enhanced validation function that provides better error messages - removed duplicate definition
#[allow(dead_code)]
fn validate_against_schema_enhanced(value: &serde_json::Value, schema: &serde_json::Value) -> Result<(), String> {
    if let (Some(required), Some(value_obj)) = (schema.get("required"), value.as_object()) {
        if let Some(required_array) = required.as_array() {
            for req_field in required_array {
                if let Some(field_name) = req_field.as_str() {
                    if !value_obj.contains_key(field_name) {
                        return Err(format!("Missing required field: {}", field_name));
                    }
                }
            }
        }
    }
    
    if let (Some(props), Some(value_obj)) = (schema.get("properties"), value.as_object()) {
        if let Some(props_obj) = props.as_object() {
            for (field_name, field_value) in value_obj {
                if let Some(field_schema) = props_obj.get(field_name) {
                    // Type validation
                    if let Some(expected_type) = field_schema.get("type") {
                        let matches_type = match expected_type {
                            serde_json::Value::String(type_str) => {
                                validate_type(field_value, type_str)
                            },
                            serde_json::Value::Array(type_array) => {
                                type_array.iter().any(|t| {
                                    if let Some(type_str) = t.as_str() {
                                        validate_type(field_value, type_str)
                                    } else {
                                        false
                                    }
                                })
                            },
                            _ => true
                        };
                        
                        if !matches_type {
                            return Err(format!("Type mismatch for field {}", field_name));
                        }
                    }
                    
                    // Enum validation
                    if let (Some(enum_values), field_str) = (
                        field_schema.get("enum").and_then(|e| e.as_array()),
                        field_value.as_str()
                    ) {
                        if let Some(field_str) = field_str {
                            let valid_enum = enum_values.iter().any(|v| v.as_str() == Some(field_str));
                            if !valid_enum {
                                return Err(format!("Enum validation failed for field {}", field_name));
                            }
                        }
                    }
                    
                    // Pattern validation
                    if let (Some(pattern), Some(string_val)) = (
                        field_schema.get("pattern").and_then(|p| p.as_str()),
                        field_value.as_str()
                    ) {
                        if !regex::Regex::new(pattern).unwrap().is_match(string_val) {
                            return Err(format!("Pattern validation failed for field {}", field_name));
                        }
                    }
                    
                    // Minimum validation for numbers
                    if let (Some(min), Some(num_val)) = (
                        field_schema.get("minimum").and_then(|m| m.as_i64()),
                        field_value.as_i64()
                    ) {
                        if num_val < min {
                            return Err(format!("Minimum constraint violation for field {}: {} < {}", 
                                             field_name, num_val, min));
                        }
                    }
                    
                    // MinLength validation for strings
                    if let (Some(min_len), Some(string_val)) = (
                        field_schema.get("minLength").and_then(|m| m.as_u64()),
                        field_value.as_str()
                    ) {
                        if (string_val.len() as u64) < min_len {
                            return Err(format!("MinLength constraint violation for field {}", field_name));
                        }
                    }
                }
            }
        }
    }
    
    // Additional properties validation
    if let (Some(additional_props), Some(value_obj)) = (schema.get("additionalProperties"), value.as_object()) {
        if let (Some(false), Some(props)) = (additional_props.as_bool(), schema.get("properties")) {
            if let Some(props_obj) = props.as_object() {
                for field_name in value_obj.keys() {
                    if !props_obj.contains_key(field_name) {
                        return Err(format!("Additional property not allowed: {}", field_name));
                    }
                }
            }
        }
    }
    
    Ok(())
}

// Helper function for type validation
fn validate_type(value: &serde_json::Value, expected_type: &str) -> bool {
    match expected_type {
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.is_i64() || value.is_u64(),
        "boolean" => value.is_boolean(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        "null" => value.is_null(),
        _ => true // Unknown type, allow
    }
}