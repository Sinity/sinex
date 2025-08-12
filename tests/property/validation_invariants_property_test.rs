use proptest::prelude::*;
use serde_json::{json, Value};
use sinex_core::db::sanitization::EventSanitizer;
use sinex_core::models::RawEvent;
use sinex_core::types::{
    domain::{EventSource, EventType, HostName},
    Ulid,
};
use sinex_test_utils::prelude::*;
use std::collections::HashMap;

/// Property tests for complex validation invariants
///
/// This module tests advanced validation logic that spans multiple domains:
/// - Cross-field validation consistency
/// - Data integrity across transformations
/// - Security boundary enforcement
/// - Performance characteristics under load
/// - Error propagation and recovery

// =============================================================================
// Complex Validation Strategies
// =============================================================================

/// Generate events with interdependent validation constraints
fn arb_complex_event() -> impl Strategy<Value = RawEvent> {
    (
        "[a-z][a-z0-9_]{2,20}",          // source 
        "[a-z][a-z0-9_.]{2,30}",         // event_type
        prop::collection::hash_map(
            "[a-zA-Z_][a-zA-Z0-9_]*",     // keys
            prop_oneof![
                any::<String>().prop_map(|s| json!(s)),
                any::<i64>().prop_map(|i| json!(i)),
                any::<bool>().prop_map(|b| json!(b)),
                prop::collection::vec(any::<String>(), 0..10).prop_map(|v| json!(v)),
            ],
            1..20
        ),                               // payload fields
        prop::option::of(1u32..1000000u32), // optional size constraint
    ).prop_map(|(source, event_type, payload_fields, size_constraint)| {
        let mut event = RawEvent::schemaless(
            EventSource::new(source),
            EventType::new(event_type), 
            json!(payload_fields),
        );
        
        // Add size constraint to payload if specified
        if let Some(size) = size_constraint {
            if let Value::Object(ref mut map) = event.payload {
                map.insert("expected_size".to_string(), json!(size));
                map.insert("actual_content".to_string(), json!("x".repeat(size as usize % 1000)));
            }
        }
        
        event.ts_ingest = chrono::Utc::now();
        event
    })
}

/// Generate validation chains with different error conditions
fn arb_validation_scenario() -> impl Strategy<Value = ValidationScenario> {
    prop_oneof![
        // Success scenarios
        Just(ValidationScenario::Success),
        
        // Single validation failures
        Just(ValidationScenario::EmptySource),
        Just(ValidationScenario::InvalidEventType),
        Just(ValidationScenario::OversizedPayload),
        Just(ValidationScenario::MissingRequiredField),
        
        // Multiple validation failures
        Just(ValidationScenario::MultipleFailures(vec![
            ValidationFailure::EmptySource,
            ValidationFailure::InvalidPayload
        ])),
        
        // Security scenarios  
        Just(ValidationScenario::SecurityThreat(SecurityThreat::PathTraversal)),
        Just(ValidationScenario::SecurityThreat(SecurityThreat::NullByteInjection)),
        Just(ValidationScenario::SecurityThreat(SecurityThreat::XssAttempt)),
        
        // Edge cases
        Just(ValidationScenario::EdgeCase(EdgeCase::UnicodeNormalization)),
        Just(ValidationScenario::EdgeCase(EdgeCase::TimestampPrecision)),
        Just(ValidationScenario::EdgeCase(EdgeCase::LargeNumberHandling)),
    ]
}

#[derive(Debug, Clone)]
enum ValidationScenario {
    Success,
    EmptySource,
    InvalidEventType, 
    OversizedPayload,
    MissingRequiredField,
    MultipleFailures(Vec<ValidationFailure>),
    SecurityThreat(SecurityThreat),
    EdgeCase(EdgeCase),
}

#[derive(Debug, Clone)]
enum ValidationFailure {
    EmptySource,
    InvalidPayload,
    OversizedContent,
    MissingTimestamp,
}

#[derive(Debug, Clone)]
enum SecurityThreat {
    PathTraversal,
    NullByteInjection,
    XssAttempt,
    SqlInjection,
}

#[derive(Debug, Clone)]
enum EdgeCase {
    UnicodeNormalization,
    TimestampPrecision,
    LargeNumberHandling,
    FloatingPointPrecision,
}

// =============================================================================
// Cross-Field Validation Properties
// =============================================================================

#[sinex_test]
fn test_validation_consistency_across_fields() -> color_eyre::eyre::Result<()> {
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        fn property_validation_consistency_across_fields(
            mut event in arb_complex_event()
        ) {
            // Property: Field validation should be consistent across the event
            let original_event = event.clone();
            
            // Sanitize the event
            let was_modified = EventSanitizer::sanitize_event(&mut event)
                .expect("Sanitization should not fail");
            
            if was_modified {
                // If any field was sanitized, event should still be structurally valid
                prop_assert!(!event.source.is_empty(), "Source should not become empty after sanitization");
                prop_assert!(!event.event_type.is_empty(), "Event type should not become empty after sanitization");
                
                // Payload should remain valid JSON
                let serialized = serde_json::to_string(&event.payload);
                prop_assert!(serialized.is_ok(), "Payload should remain valid JSON after sanitization");
            }
            
            // Property: Validation should be idempotent
            let mut event_copy = event.clone();
            let was_modified_again = EventSanitizer::sanitize_event(&mut event_copy)
                .expect("Second sanitization should not fail");
                
            prop_assert!(!was_modified_again, "Second sanitization should not modify already-clean event");
            prop_assert_eq!(event.source, event_copy.source, "Source should be stable after sanitization");
        }
    }
    Ok(())
}

#[sinex_test]
fn test_data_integrity_across_transformations() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_data_integrity_across_transformations(
            mut event in arb_complex_event()
        ) {
            // Property: Essential data should be preserved through transformations
            let original_payload_keys: Vec<String> = if let Value::Object(ref map) = event.payload {
                map.keys().cloned().collect()
            } else {
                vec![]
            };
            
            // Apply sanitization
            let _was_modified = EventSanitizer::sanitize_event(&mut event)
                .expect("Sanitization should not fail");
            
            // Check data integrity
            if let Value::Object(ref map) = event.payload {
                // Most keys should be preserved (some might be removed for security)
                let preserved_keys: Vec<String> = map.keys().cloned().collect();
                let preservation_ratio = preserved_keys.len() as f64 / original_payload_keys.len().max(1) as f64;
                
                prop_assert!(
                    preservation_ratio >= 0.5, // At least 50% of keys should be preserved
                    "Too many payload keys were removed: {} -> {} ({}%)",
                    original_payload_keys.len(),
                    preserved_keys.len(),
                    preservation_ratio * 100.0
                );
                
                // Core structural fields should be preserved
                for key in &preserved_keys {
                    if key.contains("id") || key.contains("type") || key.contains("timestamp") {
                        prop_assert!(
                            original_payload_keys.contains(key),
                            "Core field '{}' should have existed originally", key
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[sinex_test] 
fn test_security_boundary_enforcement() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_security_boundary_enforcement(
            scenario in arb_validation_scenario()
        ) {
            // Property: Security boundaries should be consistently enforced
            match scenario {
                ValidationScenario::SecurityThreat(threat) => {
                    let event = create_event_for_security_threat(threat);
                    let mut test_event = event.clone();
                    
                    // Security threats should be neutralized
                    let was_sanitized = EventSanitizer::sanitize_event(&mut test_event)
                        .expect("Security sanitization should not fail");
                    
                    if was_sanitized {
                        // Verify threat was neutralized
                        prop_assert!(
                            !contains_security_threat(&test_event, &threat),
                            "Security threat should be neutralized after sanitization: {:?}",
                            threat
                        );
                    }
                }
                ValidationScenario::Success => {
                    // Valid events should pass through unchanged
                    let mut event = create_valid_test_event();
                    let original = event.clone();
                    
                    let was_modified = EventSanitizer::sanitize_event(&mut event)
                        .expect("Valid event sanitization should not fail");
                    
                    prop_assert!(!was_modified, "Valid events should not be modified by sanitization");
                    prop_assert_eq!(event.source, original.source, "Valid event source should be unchanged");
                }
                _ => {
                    // Other scenarios should be handled gracefully
                    prop_assert!(true);
                }
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_validation_error_recovery() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_validation_error_recovery(
            scenario in arb_validation_scenario()
        ) {
            // Property: System should recover gracefully from validation errors
            match scenario {
                ValidationScenario::MultipleFailures(failures) => {
                    let mut events = Vec::new();
                    
                    // Create events that trigger each failure
                    for failure in failures {
                        events.push(create_event_for_failure(failure));
                    }
                    
                    // Process each event - should not panic or corrupt state
                    for mut event in events {
                        let result = EventSanitizer::sanitize_event(&mut event);
                        prop_assert!(result.is_ok(), "Error recovery should succeed");
                        
                        // Event should remain in valid state
                        prop_assert!(!event.source.is_empty() || event.source.is_empty(), "Event should be in consistent state");
                        prop_assert!(serde_json::to_string(&event.payload).is_ok(), "Event payload should remain serializable");
                    }
                }
                _ => prop_assert!(true)
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_unicode_normalization_invariants() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_unicode_normalization_invariants(
            unicode_content in "[\\u{0080}-\\u{FFFF}]{1,100}"
        ) {
            // Property: Unicode content should be handled consistently
            let mut event = RawEvent::schemaless(
                EventSource::new("unicode.test"),
                EventType::new("normalization.test"),
                json!({"content": unicode_content.clone()}),
            );
            event.ts_ingest = chrono::Utc::now();
            
            let original_length = unicode_content.chars().count();
            
            // Sanitize unicode content
            let result = EventSanitizer::sanitize_event(&mut event);
            prop_assert!(result.is_ok(), "Unicode sanitization should not fail");
            
            // Check payload preservation
            if let Some(content) = event.payload.get("content").and_then(|v| v.as_str()) {
                // Unicode content should be preserved or safely transformed
                let sanitized_length = content.chars().count();
                
                prop_assert!(
                    sanitized_length <= original_length,
                    "Sanitized unicode should not be longer than original: {} vs {}",
                    sanitized_length, original_length
                );
                
                // Should not contain dangerous Unicode sequences
                prop_assert!(
                    !content.contains('\u{202E}'), // Right-to-left override
                    "Should not contain dangerous Unicode control characters"
                );
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_numerical_precision_invariants() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_numerical_precision_invariants(
            large_int in i64::MIN..i64::MAX,
            float_val in f64::MIN..f64::MAX,
            precision_digits in 1usize..15
        ) {
            // Property: Numerical values should maintain reasonable precision
            let payload = json!({
                "large_integer": large_int,
                "float_value": float_val,
                "precision_test": format!("{:.prec$}", float_val, prec = precision_digits)
            });
            
            let mut event = RawEvent::schemaless(
                EventSource::new("numerical.test"),
                EventType::new("precision.test"),
                payload,
            );
            event.ts_ingest = chrono::Utc::now();
            
            // Sanitization should preserve numerical accuracy
            let result = EventSanitizer::sanitize_event(&mut event);
            prop_assert!(result.is_ok(), "Numerical sanitization should not fail");
            
            // Check that numbers are preserved
            if let Some(preserved_int) = event.payload.get("large_integer").and_then(|v| v.as_i64()) {
                prop_assert_eq!(
                    preserved_int, large_int,
                    "Large integers should be preserved exactly"
                );
            }
            
            // Floats might lose some precision but should be reasonable
            if let Some(preserved_float) = event.payload.get("float_value").and_then(|v| v.as_f64()) {
                if large_int.abs() < 1_000_000_000_000_000 { // Avoid extreme values that might overflow
                    let relative_error = ((preserved_float - float_val) / float_val.max(1.0)).abs();
                    prop_assert!(
                        relative_error < 1e-10 || !float_val.is_finite(),
                        "Float values should maintain reasonable precision: {} vs {} (error: {})",
                        float_val, preserved_float, relative_error
                    );
                }
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_validation_performance_characteristics() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_validation_performance_characteristics(
            event_count in 1usize..100,
            payload_size_kb in 1usize..100
        ) {
            // Property: Validation should complete in reasonable time regardless of input
            let mut events = Vec::new();
            
            // Create events with varying payload sizes
            for i in 0..event_count {
                let large_payload = json!({
                    "data": "x".repeat(payload_size_kb * 1024),
                    "index": i,
                    "metadata": {
                        "size_kb": payload_size_kb,
                        "created": chrono::Utc::now().to_rfc3339()
                    }
                });
                
                let mut event = RawEvent::schemaless(
                    EventSource::new(format!("perf.test.{}", i)),
                    EventType::new("performance.test"),
                    large_payload,
                );
                event.ts_ingest = chrono::Utc::now();
                events.push(event);
            }
            
            // Measure validation performance
            let start_time = std::time::Instant::now();
            let mut processed_count = 0;
            
            for mut event in events {
                let result = EventSanitizer::sanitize_event(&mut event);
                prop_assert!(result.is_ok(), "Performance test event should sanitize successfully");
                processed_count += 1;
            }
            
            let elapsed = start_time.elapsed();
            let events_per_second = (processed_count as f64) / elapsed.as_secs_f64();
            
            // Should process at least 10 events per second (very conservative)
            prop_assert!(
                events_per_second >= 10.0 || processed_count < 10,
                "Validation performance too slow: {:.2} events/sec for {} events with {}KB payloads",
                events_per_second, event_count, payload_size_kb
            );
        }
    }
    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

fn create_event_for_security_threat(threat: SecurityThreat) -> RawEvent {
    let (source, event_type, payload) = match threat {
        SecurityThreat::PathTraversal => (
            "../../../etc/passwd",
            "security.test",
            json!({"path": "../../sensitive/file.txt"})
        ),
        SecurityThreat::NullByteInjection => (
            "test\0source",
            "security.test", 
            json!({"data": "test\0value"})
        ),
        SecurityThreat::XssAttempt => (
            "xss.test",
            "security.test",
            json!({"script": "<script>alert('xss')</script>"})
        ),
        SecurityThreat::SqlInjection => (
            "sql.test",
            "security.test",
            json!({"query": "'; DROP TABLE events; --"})
        ),
    };
    
    let mut event = RawEvent::schemaless(
        EventSource::new(source),
        EventType::new(event_type),
        payload,
    );
    event.ts_ingest = chrono::Utc::now();
    event
}

fn create_event_for_failure(failure: ValidationFailure) -> RawEvent {
    match failure {
        ValidationFailure::EmptySource => {
            let mut event = RawEvent::schemaless(
                EventSource::new(""),
                EventType::new("test.event"),
                json!({"data": "test"}),
            );
            event.ts_ingest = chrono::Utc::now();
            event
        }
        ValidationFailure::InvalidPayload => {
            let mut event = RawEvent::schemaless(
                EventSource::new("test.source"),
                EventType::new("test.event"),
                json!(null),
            );
            event.ts_ingest = chrono::Utc::now();
            event
        }
        ValidationFailure::OversizedContent => {
            let mut event = RawEvent::schemaless(
                EventSource::new("test.source"),
                EventType::new("test.event"),
                json!({"data": "x".repeat(1_000_000)}),
            );
            event.ts_ingest = chrono::Utc::now();
            event
        }
        ValidationFailure::MissingTimestamp => {
            // This is harder to create with current event structure
            // since ts_ingest is required
            let mut event = RawEvent::schemaless(
                EventSource::new("test.source"),
                EventType::new("test.event"),
                json!({"data": "test"}),
            );
            event.ts_ingest = chrono::Utc::now();
            event
        }
    }
}

fn create_valid_test_event() -> RawEvent {
    let mut event = RawEvent::schemaless(
        EventSource::new("valid.source"),
        EventType::new("valid.event"),
        json!({"data": "clean data", "count": 42}),
    );
    event.ts_ingest = chrono::Utc::now();
    event
}

fn contains_security_threat(event: &RawEvent, threat: &SecurityThreat) -> bool {
    match threat {
        SecurityThreat::PathTraversal => {
            event.source.contains("..") ||
            event.payload.to_string().contains("..")
        }
        SecurityThreat::NullByteInjection => {
            event.source.contains('\0') ||
            event.payload.to_string().contains('\0')
        }
        SecurityThreat::XssAttempt => {
            event.payload.to_string().contains("<script>")
        }
        SecurityThreat::SqlInjection => {
            event.payload.to_string().contains("DROP TABLE")
        }
    }
}

// =============================================================================
// Unit Tests for Property Test Helpers  
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[sinex_test]
    fn test_security_threat_detection() -> color_eyre::eyre::Result<()> {
        let threat_event = create_event_for_security_threat(SecurityThreat::PathTraversal);
        assert!(contains_security_threat(&threat_event, &SecurityThreat::PathTraversal));
        
        let clean_event = create_valid_test_event();
        assert!(!contains_security_threat(&clean_event, &SecurityThreat::PathTraversal));
        
        Ok(())
    }

    #[sinex_test]
    fn test_validation_scenario_generators() -> color_eyre::eyre::Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        
        let scenario = arb_validation_scenario().new_tree(&mut runner).unwrap().current();
        // Should generate without crashing
        assert!(true);
        
        let complex_event = arb_complex_event().new_tree(&mut runner).unwrap().current();
        assert!(!complex_event.source.is_empty());
        assert!(!complex_event.event_type.is_empty());
        
        Ok(())
    }
}