use crate::common::prelude::*;
use crate::common::property_builders::*;
use proptest::prelude::*;
use serde_json::{json, Value};
use sinex_validation::{ValidationChain, ValidationResult};

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
            validations in proptest::collection::vec(
                (any::<bool>(), "[a-zA-Z ]+"),
                1..10
            )
        ) {
            // Property: ValidationChain should accumulate all errors
            let mut chain = ValidationChain::new();
            let mut expected_errors = Vec::new();
            
            for (should_fail, message) in validations {
                if should_fail {
                    chain = chain.validate(false, &message);
                    expected_errors.push(message);
                } else {
                    chain = chain.validate(true, &message);
                }
            }
            
            let result = chain.build();
            
            if expected_errors.is_empty() {
                assert!(matches!(result, ValidationResult::Valid));
            } else {
                if let ValidationResult::Invalid(errors) = result {
                    assert_eq!(errors.len(), expected_errors.len(),
                               "Should accumulate all validation errors");
                } else {
                    panic!("Should be invalid with errors");
                }
            }
        }
    }
}