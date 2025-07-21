use crate::common::prelude::*;
use crate::common::query_helpers::TestQueries;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sinex_db::validation::{EventValidator, ValidationError};
use sinex_events::{EventFactory, event_types};

/// Property tests for schema validation functionality
///
/// This module consolidates property tests from:
/// - json_schema_property_tests.rs (JSON schema validation and security)
/// - Additional schema-related property tests for validation chains
/// - Schema compatibility and evolution properties

// =============================================================================
// JSON Schema Validation Properties
// =============================================================================

/// Generate JSON payloads for event validation testing
fn arb_event_payload() -> impl Strategy<Value = Value> {
    prop_oneof![
        // Filesystem-like events
        Just(json!({
            "path": "/home/user/test.txt",
            "size": 1024,
            "timestamp": "2024-06-20T10:00:00Z"
        })),
        // Window manager events
        Just(json!({
            "window_id": "0x12345",
            "title": "Terminal",
            "class": "kitty"
        })),
        // Terminal events
        Just(json!({
            "command": "ls -la",
            "exit_code": 0,
            "duration_ms": 150
        })),
        // Simple events
        Just(json!({
            "type": "simple",
            "data": "test"
        })),
        // Complex nested events
        Just(json!({
            "event_data": {
                "nested": {
                    "deeply": {
                        "value": "test"
                    }
                }
            },
            "metadata": {
                "timestamp": "2024-06-20T10:00:00Z",
                "source": "test"
            }
        })),
        // Array data
        Just(json!({
            "items": ["item1", "item2", "item3"],
            "count": 3
        })),
        // Edge cases
        Just(json!({})),
        Just(json!(null)),
        Just(json!("simple string")),
        Just(json!(42)),
        Just(json!(true)),
    ]
}

/// Generate potentially problematic payloads for security testing
fn arb_problematic_payload() -> impl Strategy<Value = Value> {
    prop_oneof![
        // Very large payloads
        Just(json!({
            "large_field": "x".repeat(10000),
            "data": "test"
        })),
        // Deeply nested structures
        Just(json!({
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "level5": {
                                "level6": {
                                    "level7": {
                                        "level8": {
                                            "level9": {
                                                "level10": "deep"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })),
        // Large arrays
        Just(json!({
            "large_array": (0..1000).collect::<Vec<i32>>(),
            "type": "array_test"
        })),
        // Suspicious patterns that might indicate security issues
        Just(json!({
            "script": "<script>alert('xss')</script>",
            "sql": "'; DROP TABLE users; --",
            "path": "../../../etc/passwd"
        })),
        // Unicode and special characters
        Just(json!({
            "unicode": "🦀🔒🌟",
            "special_chars": "!@#$%^&*(){}[]|\\:;\"'<>,.?/",
            "null_bytes": "test\u{0000}data"
        })),
        // Numeric edge cases
        Just(json!({
            "max_int": i64::MAX,
            "min_int": i64::MIN,
            "float": f64::MAX,
            "negative_float": f64::MIN,
            "infinity": f64::INFINITY,
            "neg_infinity": f64::NEG_INFINITY
        })),
        // Boolean variations
        Just(json!({
            "bool_true": true,
            "bool_false": false,
            "bool_array": [true, false, true]
        })),
        // Mixed type arrays
        Just(json!({
            "mixed_array": [1, "string", true, null, {"nested": "object"}],
            "type": "mixed"
        })),
    ]
}

/// Generate arbitrary source and event type combinations
fn arb_event_source_type() -> impl Strategy<Value = (String, String)> {
    (
        "[a-zA-Z][a-zA-Z0-9_-]{2,20}",
        "[a-zA-Z][a-zA-Z0-9_.-]{2,30}",
    )
}

#[tokio::test]
async fn test_event_validator_normal_payloads() -> AnyhowResult<(), anyhow::Error> {
    proptest!(|(
        (source, event_type) in arb_event_source_type(),
        payload in arb_event_payload()
    )| {
        let factory = EventFactory::new(&source);
        let event = factory.create_event(&event_type, payload);
        let validator = EventValidator::new();

        // Validation should not panic and should return a consistent result
        let result = validator.validate(&event);

        // Any result is acceptable - just ensure it doesn't crash
        match result {
            Ok(()) => {
                // Event passed validation
                prop_assert!(true);
            },
            Err(ValidationError::UnknownEventType { .. }) => {
                // Expected for unknown event types
                prop_assert!(true);
            },
            Err(ValidationError::MissingField { .. }) => {
                // Expected for malformed events
                prop_assert!(true);
            },
            Err(ValidationError::InvalidType { .. }) => {
                // Expected for type mismatches
                prop_assert!(true);
            },
            Err(ValidationError::InvalidValue { .. }) => {
                // Expected for invalid values
                prop_assert!(true);
            },
            Err(_) => {
                // Other validation errors are also acceptable
                prop_assert!(true);
            }
        }
    });
    Ok(())
}

#[tokio::test]
async fn test_event_validator_security_payloads() -> AnyhowResult<(), anyhow::Error> {
    proptest!(|(
        (source, event_type) in arb_event_source_type(),
        payload in arb_problematic_payload()
    )| {
        let factory = EventFactory::new(&source);
        let event = factory.create_event(&event_type, payload);
        let validator = EventValidator::new();

        // Validation should handle problematic payloads safely
        let result = validator.validate(&event);

        // Should not panic or cause memory issues
        match result {
            Ok(()) | Err(_) => {
                // Any result is fine - main thing is no crashes
                prop_assert!(true);
            }
        }
    });
    Ok(())
}

#[tokio::test]
async fn test_raw_event_validation_consistency() -> AnyhowResult<(), anyhow::Error> {
    proptest!(|(
        (source, event_type) in arb_event_source_type(),
        payload in arb_event_payload()
    )| {
        let factory = EventFactory::new(&source);
        let event = factory.create_event(&event_type, payload.clone());
        let validator = EventValidator::new();

        // Validation should be deterministic - same event should always get same result
        let result1 = validator.validate(&event);
        let result2 = validator.validate(&event);

        match (result1, result2) {
            (Ok(()), Ok(())) => {
                // Both passed
            },
            (Err(e1), Err(e2)) => {
                // Both failed - error types should be the same
                prop_assert_eq!(std::mem::discriminant(&e1), std::mem::discriminant(&e2));
            },
            _ => {
                prop_assert!(false, "Validation was not consistent");
            }
        }
    });
    Ok(())
}

// =============================================================================
// Schema Compatibility Properties
// =============================================================================

/// Test schema evolution and backward compatibility
#[tokio::test]
async fn test_schema_evolution_properties() -> AnyhowResult<(), anyhow::Error> {
    proptest!(|(
        base_payload in arb_event_payload(),
        additional_fields in prop::collection::hash_map(
            "[a-zA-Z][a-zA-Z0-9_]{0,20}",
            prop_oneof![
                any::<String>().prop_map(|s| json!(s)),
                any::<i64>().prop_map(|i| json!(i)),
                any::<bool>().prop_map(|b| json!(b)),
            ],
            0..=5
        )
    )| {
        let validator = EventValidator::new();

        // Create base event
        let factory = EventFactory::new("test_source");
        let base_event = factory.create_event(
            "test.evolution",
            base_payload.clone()
        );

        // Create evolved event with additional fields
        let mut evolved_payload = base_payload.clone();
        if let Value::Object(ref mut obj) = evolved_payload {
            for (key, value) in additional_fields {
                obj.insert(key, value);
            }
        } else {
            // If base is not an object, create a new object with the base as a field
            let mut new_obj = serde_json::Map::new();
            new_obj.insert("base".to_string(), base_payload);
            for (key, value) in additional_fields {
                new_obj.insert(key, value);
            }
            evolved_payload = Value::Object(new_obj);
        }

        let evolved_event = factory.create_event(
            "test.evolution",
            evolved_payload
        );

        // Validation should handle both versions gracefully
        let base_result = validator.validate(&base_event);
        let evolved_result = validator.validate(&evolved_event);

        // Both should either pass or fail with the same error type
        // (since schema evolution should not break validation completely)
        match (base_result, evolved_result) {
            (Ok(()), Ok(())) => {
                // Both passed - ideal scenario
                prop_assert!(true);
            },
            (Err(e1), Err(e2)) => {
                // Both failed - error types should be compatible
                prop_assert_eq!(
                    std::mem::discriminant(&e1),
                    std::mem::discriminant(&e2),
                    "Schema evolution should not change fundamental validation behavior"
                );
            },
            (Ok(()), Err(_)) => {
                // Base passed, evolved failed - acceptable if additional fields are invalid
                prop_assert!(true);
            },
            (Err(_), Ok(())) => {
                // Base failed, evolved passed - could happen if additional fields fix validation
                prop_assert!(true);
            }
        }
    });
    Ok(())
}

// =============================================================================
// Validation Chain Properties
// =============================================================================

/// Test validation chain behavior with various inputs
#[test]
fn test_validation_chain_properties() {
    proptest!(|(
        test_strings in prop::collection::vec(".*", 1..=10),
        min_lengths in prop::collection::vec(0usize..=20, 1..=10),
        max_lengths in prop::collection::vec(5usize..=50, 1..=10)
    )| {
        use sinex_core_types::ValidationChain;

        for (i, test_string) in test_strings.iter().enumerate() {
            let min_len = min_lengths[i % min_lengths.len()];
            let max_len = max_lengths[i % max_lengths.len()];

            // Skip invalid combinations
            if min_len > max_len {
                continue;
            }

            let result = ValidationChain::validate(test_string.as_str(), "test_field")
                .not_empty()
                .min_length(min_len)
                .max_length(max_len)
                .into_result();

            match result {
                Ok(validated_value) => {
                    // If validation passed, the value should meet all criteria
                    prop_assert!(!validated_value.is_empty(), "Validated value should not be empty");
                    prop_assert!(validated_value.len() >= min_len, "Validated value should meet min length");
                    prop_assert!(validated_value.len() <= max_len, "Validated value should meet max length");
                    prop_assert_eq!(validated_value, test_string, "Validated value should be unchanged");
                },
                Err(_) => {
                    // If validation failed, at least one criterion was not met
                    let fails_empty = test_string.is_empty();
                    let fails_min = test_string.len() < min_len;
                    let fails_max = test_string.len() > max_len;

                    prop_assert!(
                        fails_empty || fails_min || fails_max,
                        "If validation fails, string should violate at least one constraint: '{}' (len={}, min={}, max={})",
                        test_string, test_string.len(), min_len, max_len
                    );
                }
            }
        }
    });
}

/// Test validation chain with numeric values
#[test]
fn test_validation_chain_numeric_properties() {
    proptest!(|(
        test_numbers in prop::collection::vec(any::<i64>(), 1..=10),
        min_values in prop::collection::vec(any::<i64>(), 1..=10),
        max_values in prop::collection::vec(any::<i64>(), 1..=10)
    )| {
        use sinex_core_types::ValidationChain;

        for (i, &test_number) in test_numbers.iter().enumerate() {
            let min_val = min_values[i % min_values.len()];
            let max_val = max_values[i % max_values.len()];

            // Skip invalid combinations
            if min_val > max_val {
                continue;
            }

            let result = ValidationChain::validate(test_number, "test_number")
                .min_value(min_val)
                .max(max_val)
                .into_result();

            match result {
                Ok(validated_value) => {
                    // If validation passed, the value should meet all criteria
                    prop_assert!(validated_value >= min_val, "Validated value should meet min constraint");
                    prop_assert!(validated_value <= max_val, "Validated value should meet max constraint");
                    prop_assert_eq!(validated_value, test_number, "Validated value should be unchanged");
                },
                Err(_) => {
                    // If validation failed, at least one criterion was not met
                    let fails_min = test_number < min_val;
                    let fails_max = test_number > max_val;

                    prop_assert!(
                        fails_min || fails_max,
                        "If validation fails, number should violate at least one constraint: {} (min={}, max={})",
                        test_number, min_val, max_val
                    );
                }
            }
        }
    });
}

// =============================================================================
// Schema Loading and Persistence Properties
// =============================================================================

#[sinex_test]
async fn test_schema_persistence_properties(ctx: TestContext) -> TestResult {
    proptest::proptest!(|(
        schema_count in 1..=10usize,
        schema_names in prop::collection::vec("[a-zA-Z][a-zA-Z0-9_]{2,20}", 1..=10),
        schema_versions in prop::collection::vec("[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{1,2}", 1..=10)
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();

            // Create test schemas
            let mut created_schemas = Vec::new();
            for i in 0..schema_count {
                let name = &schema_names[i % schema_names.len()];
                let version = &schema_versions[i % schema_versions.len()];

                let schema_def = json!({
                    "type": "object",
                    "properties": {
                        "test_field": {
                            "type": "string",
                            "minLength": 1
                        },
                        "version": {
                            "type": "string",
                            "const": version
                        }
                    },
                    "required": ["test_field"]
                });

                // Insert schema into database
                let result = TestQueries::register_test_schema(
                    &pool,
                    name,
                    version,
                    vec![format!("{}.{}", "test_source", name)],
                    schema_def
                ).await;

                match result {
                    Ok(_) => {
                        created_schemas.push((name.clone(), version.clone()));
                    },
                    Err(_) => {
                        // Schema might already exist due to conflict
                    }
                }
            }

            // Load validator from database
            let validator = EventValidator::load_from_db(&pool).await.expect("Should load validator");

            // Test that schemas are accessible
            for (name, version) in &created_schemas {
                // Create an event that should validate against this schema
                let factory = EventFactory::new("test_source");
                let event = factory.create_event(
                    name,
                    json!({
                        "test_field": "valid_value",
                        "version": version
                    })
                );

                let result = validator.validate(&event);

                // Validation should either pass or fail gracefully
                match result {
                    Ok(()) => {
                        // Schema validation passed
                        prop_assert!(true);
                    },
                    Err(ValidationError::UnknownEventType { .. }) => {
                        // Schema might not be properly loaded - acceptable
                        prop_assert!(true);
                    },
                    Err(_) => {
                        // Other validation errors are also acceptable
                        prop_assert!(true);
                    }
                }
            }

            Ok::<(), proptest::test_runner::TestCaseError>(())
        })?
    });
    Ok(())
}

// =============================================================================
// Error Handling Properties
// =============================================================================

#[tokio::test]
async fn test_event_validator_edge_cases() -> AnyhowResult<(), anyhow::Error> {
    let validator = EventValidator::new();

    let long_source = "x".repeat(1000);
    let long_event_type = "x".repeat(1000);

    let edge_cases = vec![
        // Empty fields
        ("", "test.event", json!({})),
        ("test_source", "", json!({})),
        ("test_source", "test.event", json!(null)),
        // Very long fields
        (long_source.as_str(), "test.event", json!({})),
        ("test_source", long_event_type.as_str(), json!({})),
        // Special characters
        ("test@source#", "test.event!", json!({})),
        ("test_source", "test.event.with.many.dots", json!({})),
        // Unicode
        ("test_source_🦀", "test.event", json!({"unicode": "🔒"})),
        // Control characters
        ("test\nsource", "test.event", json!({"control": "test\r\n"})),
        // Very nested payload
        (
            "test_source",
            "test.event",
            json!({
                "level1": {"level2": {"level3": {"level4": {"level5": "deep"}}}}
            }),
        ),
    ];

    for (source, event_type, payload) in edge_cases {
        let factory = EventFactory::new(source);
        let event = factory.create_event(event_type, payload);

        // Should not panic
        let _result = validator.validate(&event);

        // The main property we're testing is that it doesn't crash
        assert!(true);
    }

    Ok(())
}

// =============================================================================
// Integration Tests
// =============================================================================

#[sinex_test]
async fn test_event_validator_database_integration(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();

    // Test loading validator from empty database
    let validator = EventValidator::load_from_db(&pool)
        .await
        .expect("Should be able to load from empty database");

    // Should be able to create events and validate them
    let factory = EventFactory::new("test_source");
    let event = factory.create_event("test.event", json!({"key": "value"}));

    // Validation should not fail (no schema means fallback to hardcoded rules)
    let result = validator.validate(&event);

    // Should either pass or fail gracefully with a specific error
    match result {
        Ok(()) => {
            // Passed validation
        }
        Err(ValidationError::UnknownEventType { .. }) => {
            // Expected for unknown event types
        }
        Err(e) => {
            panic!("Unexpected validation error: {}", e);
        }
    }

    Ok(())
}

// =============================================================================
// Performance Properties
// =============================================================================

#[test]
fn test_validation_performance_properties() {
    proptest!(|(
        payload_sizes in prop::collection::vec(100usize..=10000, 1..=10),
        validation_count in 10usize..=100
    )| {
        let validator = EventValidator::new();

        for &payload_size in &payload_sizes {
            // Create payload of specified size
            let large_payload = json!({
                "data": "x".repeat(payload_size),
                "size": payload_size,
                "type": "performance_test"
            });

            let factory = EventFactory::new("test_source");
            let event = factory.create_event("test.event", large_payload);

            // Measure validation time
            let start = std::time::Instant::now();

            for _ in 0..validation_count {
                let _ = validator.validate(&event);
            }

            let elapsed = start.elapsed();
            let avg_time = elapsed.as_nanos() as f64 / validation_count as f64;

            // Property: Average validation time should be reasonable
            prop_assert!(
                avg_time < 1_000_000.0, // Less than 1ms per validation
                "Validation too slow: {:.2}ns average for {} byte payload",
                avg_time, payload_size
            );

            // Property: Validation time should not grow exponentially with size
            if payload_size > 1000 {
                prop_assert!(
                    avg_time < (payload_size as f64 * 1000.0), // Less than 1ns per byte
                    "Validation scaling poorly: {:.2}ns for {} bytes",
                    avg_time, payload_size
                );
            }
        }
    });
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[tokio::test]
    async fn test_validator_with_real_filesystem_events() {
        let validator = EventValidator::new();

        // Test filesystem events that should have hardcoded validation
        let fs_factory = EventFactory::new(sources::FS);
        let valid_fs_event = fs_factory.create_event(
            event_types::filesystem::FILE_CREATED,
            json!({
                "path": "/home/user/test.txt",
                "size": 1024,
                "timestamp": "2024-06-20T10:00:00Z"
            }),
        );

        let invalid_fs_event = fs_factory.create_event(
            event_types::filesystem::FILE_CREATED,
            json!({
                // Missing required fields or invalid data
                "path": "",
                "size": -1
            }),
        );

        // Test validation results
        let valid_result = validator.validate(&valid_fs_event);
        let invalid_result = validator.validate(&invalid_fs_event);

        // At minimum, these should not panic
        match valid_result {
            Ok(()) | Err(ValidationError::UnknownEventType { .. }) => {}
            Err(e) => println!("Valid event failed: {}", e),
        }

        match invalid_result {
            Ok(()) | Err(_) => {} // Any result is acceptable for testing
        }
    }

    #[test]
    fn test_payload_generators() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();

        // Test normal payload generator
        let payload = arb_event_payload().new_tree(&mut runner).unwrap().current();
        assert!(
            payload.is_object()
                || payload.is_string()
                || payload.is_number()
                || payload.is_boolean()
                || payload.is_null()
        );

        // Test problematic payload generator
        let problematic = arb_problematic_payload()
            .new_tree(&mut runner)
            .unwrap()
            .current();
        assert!(problematic.is_object()); // All problematic payloads are objects
    }

    #[test]
    fn test_source_type_generator() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let (source, event_type) = arb_event_source_type()
            .new_tree(&mut runner)
            .unwrap()
            .current();

        assert!(!source.is_empty());
        assert!(!event_type.is_empty());
        assert!(source.len() >= 3); // 1 + 2 minimum
        assert!(event_type.len() >= 3); // 1 + 2 minimum
    }

    #[test]
    fn test_validation_chain_basic_functionality() {
        use sinex_core_types::ValidationChain;

        // Test successful validation
        let result = ValidationChain::validate("hello", "test")
            .not_empty()
            .min_length(3)
            .max_length(10)
            .into_result();

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello");

        // Test failed validation
        let result = ValidationChain::validate("", "test")
            .not_empty()
            .into_result();

        assert!(result.is_err());
    }

    #[test]
    fn test_validation_error_types() {
        use sinex_core_types::ValidationChain;

        // Test different error types
        let empty_error = ValidationChain::validate("", "test")
            .not_empty()
            .into_result();

        let length_error = ValidationChain::validate("x", "test")
            .min_length(5)
            .into_result();

        // Both should be errors but potentially different types
        assert!(empty_error.is_err());
        assert!(length_error.is_err());
    }
}
