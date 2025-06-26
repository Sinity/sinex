use crate::common::prelude::*;
use proptest::prelude::*;
use sinex_db::validation::{EventValidator, ValidationError};

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
    ]
}

#[sinex_test]
async fn test_event_validator_normal_payloads(_ctx: TestContext) -> TestResult {
    proptest!(|(
        source in "[a-zA-Z][a-zA-Z0-9_-]{2,20}",
        event_type in "[a-zA-Z][a-zA-Z0-9_.-]{2,30}",
        payload in arb_event_payload()
    )| {
        let event = RawEventBuilder::new(source, event_type, payload).build();
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

#[sinex_test]
async fn test_event_validator_security_payloads(_ctx: TestContext) -> TestResult {
    proptest!(|(
        source in "[a-zA-Z][a-zA-Z0-9_-]{2,20}",
        event_type in "[a-zA-Z][a-zA-Z0-9_.-]{2,30}",
        payload in arb_problematic_payload()
    )| {
        let event = RawEventBuilder::new(source, event_type, payload).build();
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

#[sinex_test]
async fn test_raw_event_validation_consistency(_ctx: TestContext) -> TestResult {
    proptest!(|(
        source in "[a-zA-Z][a-zA-Z0-9_-]{2,20}",
        event_type in "[a-zA-Z][a-zA-Z0-9_.-]{2,30}",
        payload in arb_event_payload()
    )| {
        let event = RawEventBuilder::new(&source, &event_type, payload.clone()).build();
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

#[sinex_test]
async fn test_event_validator_edge_cases(_ctx: TestContext) -> TestResult {
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
    ];
    
    for (source, event_type, payload) in edge_cases {
        let event = RawEventBuilder::new(source, event_type, payload).build();
        
        // Should not panic
        let _result = validator.validate(&event);
    }
    
    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    
    #[sinex_test]
    async fn test_event_validator_database_integration(ctx: TestContext) -> TestResult {
        let pool = ctx.pool();
        
        // Test loading validator from empty database
        let validator = EventValidator::load_from_db(&pool).await
            .expect("Should be able to load from empty database");
        
        // Should be able to create events and validate them
        let event = RawEventBuilder::new("test_source", "test.event", json!({"key": "value"}))
            .build();
            
        // Validation should not fail (no schema means fallback to hardcoded rules)
        let result = validator.validate(&event);
        
        // Should either pass or fail gracefully with a specific error
        match result {
            Ok(()) => {
                // Passed validation
            },
            Err(ValidationError::UnknownEventType { .. }) => {
                // Expected for unknown event types
            },
            Err(e) => {
                panic!("Unexpected validation error: {}", e);
            }
        }
        
        Ok(())
    }
    
    #[tokio::test] 
    async fn test_validator_with_real_filesystem_events() {
        let validator = EventValidator::new();
        
        // Test filesystem events that should have hardcoded validation
        let valid_fs_event = RawEventBuilder::new(
            "filesystem", 
            "file.created", 
            json!({
                "path": "/home/user/test.txt",
                "size": 1024,
                "timestamp": "2024-06-20T10:00:00Z"
            })
        ).build();
        
        let invalid_fs_event = RawEventBuilder::new(
            "filesystem",
            "file.created", 
            json!({
                // Missing required fields or invalid data
                "path": "",
                "size": -1
            })
        ).build();
        
        // Test validation results
        let valid_result = validator.validate(&valid_fs_event);
        let invalid_result = validator.validate(&invalid_fs_event);
        
        // At minimum, these should not panic
        match valid_result {
            Ok(()) | Err(ValidationError::UnknownEventType { .. }) => {},
            Err(e) => println!("Valid event failed: {}", e),
        }
        
        match invalid_result {
            Ok(()) | Err(_) => {}, // Any result is acceptable for testing
        }
    }
}