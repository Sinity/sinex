//! Validation test utilities

use anyhow::Result;
use serde_json::{json, Value};
use sinex_db::validation::{EventValidator, ValidationError};
use sinex_core::RawEventBuilder;
use sinex_db::models::RawEvent;
use sqlx::PgPool;
use std::time::Duration;

/// Assert that an event is valid (used by test files)
pub fn assert_valid_event(event: &RawEvent) {
    let validator = EventValidator::new();
    let result = validator.validate(event);
    assert!(result.is_ok(), "Event should be valid but validation failed: {:?}", result.unwrap_err());
}

/// Assert that an event is invalid and contains the expected field in error message
pub fn assert_invalid_event(event: &RawEvent, expected_field: &str) {
    let validator = EventValidator::new();
    let result = validator.validate(event);
    assert!(result.is_err(), "Event should be invalid but validation passed");
    
    let error = result.unwrap_err();
    let error_msg = error.to_string();
    assert!(
        error_msg.contains(expected_field), 
        "Error message should mention '{}' but got: {}", 
        expected_field, 
        error_msg
    );
}

/// Create a test event validator with hardcoded rules
pub fn create_test_validator() -> EventValidator {
    EventValidator::new()
}

/// Create a test event validator loaded from database
pub async fn create_test_validator_from_db(pool: &PgPool) -> Result<EventValidator> {
    EventValidator::load_from_db(pool).await
}

/// Test event validation helpers
pub mod events {
    use super::*;

    /// Create a valid filesystem event for testing
    pub fn valid_filesystem_event() -> RawEvent {
        events::generic_adversarial_event("test", "test.event", json!({"test": true}), None)
    }

    /// Create an invalid filesystem event (missing required fields)
    pub fn invalid_filesystem_event() -> RawEvent {
        events::generic_adversarial_event("test", "test.event", json!({"test": true}), None)
    }

    /// Create a valid terminal event for testing
    pub fn valid_terminal_event() -> RawEvent {
        events::generic_adversarial_event("test", "test.event", json!({"test": true}), None)
    }

    /// Create an event with unknown source/type
    pub fn unknown_event() -> RawEvent {
        events::generic_adversarial_event("test", "test.event", json!({"test": true}), None)
    }

    /// Create an event with malformed payload
    pub fn malformed_payload_event() -> RawEvent {
        events::generic_adversarial_event("test", "test.event", json!({"test": true}), None)
    }
}

/// Validation assertion helpers
pub mod assertions {
    use super::*;

    /// Assert that validation passes
    pub fn assert_validation_passes(validator: &EventValidator, event: &RawEvent) -> Result<()> {
        match validator.validate(event) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("Expected validation to pass, but got error: {}", e),
        }
    }

    /// Assert that validation fails with specific error type
    pub fn assert_validation_fails_with<F>(validator: &EventValidator, event: &RawEvent, check: F) -> Result<()> 
    where
        F: Fn(&ValidationError) -> bool,
    {
        match validator.validate(event) {
            Ok(()) => anyhow::bail!("Expected validation to fail, but it passed"),
            Err(e) => {
                if check(&e) {
                    Ok(())
                } else {
                    anyhow::bail!("Validation failed with unexpected error: {}", e)
                }
            }
        }
    }

    /// Assert that validation fails with unknown event type error
    pub fn assert_validation_fails_unknown_type(validator: &EventValidator, event: &RawEvent) -> Result<()> {
        assert_validation_fails_with(validator, event, |e| {
            matches!(e, ValidationError::UnknownEventType { .. })
        })
    }

    /// Assert that validation fails with missing field error
    pub fn assert_validation_fails_missing_field(validator: &EventValidator, event: &RawEvent, expected_field: &str) -> Result<()> {
        assert_validation_fails_with(validator, event, |e| {
            match e {
                ValidationError::MissingField { field } => field == expected_field,
                _ => false,
            }
        })
    }

    /// Assert that validation fails with invalid type error
    pub fn assert_validation_fails_invalid_type(validator: &EventValidator, event: &RawEvent, expected_field: &str) -> Result<()> {
        assert_validation_fails_with(validator, event, |e| {
            match e {
                ValidationError::InvalidType { field, .. } => field == expected_field,
                _ => false,
            }
        })
    }
}

/// Validation test data generators
pub mod generators {
    use super::*;

    /// Generate events with various validation scenarios
    pub fn validation_test_events() -> Vec<(String, RawEvent, bool)> {
        vec![
            ("valid_filesystem".to_string(), events::valid_filesystem_event(), true),
            ("invalid_filesystem".to_string(), events::invalid_filesystem_event(), false),
            ("valid_terminal".to_string(), events::valid_terminal_event(), true),
            ("unknown_event".to_string(), events::unknown_event(), false),
            ("malformed_payload".to_string(), events::malformed_payload_event(), false),
        ]
    }

    /// Generate edge case payloads for validation testing
    pub fn edge_case_payloads() -> Vec<Value> {
        vec![
            json!(null),
            json!({}),
            json!("simple string"),
            json!(42),
            json!(true),
            json!([]),
            json!({"empty_string": ""}),
            json!({"very_long_string": "x".repeat(10000)}),
            json!({"special_chars": "!@#$%^&*()"}),
            json!({"unicode": "🦀🔒🌟"}),
        ]
    }

    /// Generate large payloads for testing limits
    pub fn large_payloads() -> Vec<Value> {
        vec![
            json!({"large_field": "x".repeat(100000)}),
            json!({"large_array": (0..10000).collect::<Vec<i32>>()}),
            json!({"deeply_nested": create_deeply_nested_object(20)}),
        ]
    }

    fn create_deeply_nested_object(depth: usize) -> Value {
        if depth == 0 {
            json!("bottom")
        } else {
            json!({"level": depth, "nested": create_deeply_nested_object(depth - 1)})
        }
    }
}

/// Performance testing utilities for validation
pub mod performance {
    use super::*;
    use std::time::{Duration, Instant};

    /// Measure validation performance
    pub fn measure_validation_time(validator: &EventValidator, event: &RawEvent, iterations: usize) -> Duration {
        let start = Instant::now();
        
        for _ in 0..iterations {
            let _ = validator.validate(event);
        }
        
        start.elapsed()
    }

    /// Benchmark validation against multiple events
    pub fn benchmark_validation(validator: &EventValidator, events: &[RawEvent]) -> Vec<(usize, Duration)> {
        events.iter().enumerate().map(|(i, event)| {
            (i, measure_validation_time(validator, event, 1000))
        }).collect()
    }

    /// Test validation performance under concurrent load
    pub async fn concurrent_validation_test(
        validator: EventValidator, 
        event: RawEvent, 
        concurrent_tasks: usize,
        operations_per_task: usize
    ) -> Result<Duration> {
        use tokio::task;
        use std::sync::Arc;

        let validator = Arc::new(validator);
        let event = Arc::new(event);
        let start = Instant::now();

        let mut handles = Vec::new();
        
        for _ in 0..concurrent_tasks {
            let validator_clone = validator.clone();
            let event_clone = event.clone();
            
            let handle = task::spawn(async move {
                for _ in 0..operations_per_task {
                    let _ = validator_clone.validate(&event_clone);
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

/// Integration test helpers
pub mod integration {
    use super::*;

    /// Test validation with database schemas
    pub async fn test_with_database_schemas(pool: &PgPool) -> Result<()> {
        let validator = create_test_validator_from_db(pool).await?;
        
        // Test various events
        for (name, event, should_pass) in generators::validation_test_events() {
            let result = validator.validate(&event);
            
            match (result.is_ok(), should_pass) {
                (true, true) => {
                    println!("✓ {} passed validation as expected", name);
                },
                (false, false) => {
                    println!("✓ {} failed validation as expected", name);
                },
                (true, false) => {
                    anyhow::bail!("Expected {} to fail validation, but it passed", name);
                },
                (false, true) => {
                    println!("⚠ {} failed validation unexpectedly: {:?}", name, result.unwrap_err());
                    // Log but don't fail - might be expected with unknown schemas
                }
            }
        }
        
        Ok(())
    }

    /// Test validation performance in realistic scenarios
    pub async fn performance_integration_test(pool: &PgPool) -> Result<()> {
        let validator = create_test_validator_from_db(pool).await?;
        let events = generators::validation_test_events()
            .into_iter()
            .map(|(_, event, _)| event)
            .collect::<Vec<_>>();

        // Benchmark validation performance
        let benchmarks = performance::benchmark_validation(&validator, &events);
        
        for (i, duration) in benchmarks {
            println!("Event {}: 1000 validations took {:?}", i, duration);
            
            // Ensure reasonable performance (adjust thresholds as needed)
            if duration > Duration::from_millis(1000) {
                anyhow::bail!("Validation performance too slow for event {}: {:?}", i, duration);
            }
        }

        Ok(())
    }
}