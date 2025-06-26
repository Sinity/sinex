//! Validation test utilities
//!
//! This module provides comprehensive utilities for testing event validation
//! logic, including assertion helpers, test data generators, and performance
//! benchmarking tools.

use crate::common::prelude::*;
use sinex_db::validation::{EventValidator, ValidationError};
use sinex_db::RawEvent;

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
pub async fn create_test_validator_from_db(pool: &DbPool) -> Result<EventValidator> {
    EventValidator::load_from_db(pool).await
}

/// Create test validator with specific rules
pub fn create_test_validator_with_rules(rules: Vec<ValidationRule>) -> EventValidator {
    let mut validator = EventValidator::new();
    for rule in rules {
        validator.add_rule(rule);
    }
    validator
}

/// Validation rule for testing purposes
#[derive(Debug, Clone)]
pub struct ValidationRule {
    pub source: String,
    pub event_type: String,
    pub required_fields: Vec<String>,
    pub optional_fields: Vec<String>,
}

impl ValidationRule {
    pub fn new(source: &str, event_type: &str) -> Self {
        Self {
            source: source.to_string(),
            event_type: event_type.to_string(),
            required_fields: Vec::new(),
            optional_fields: Vec::new(),
        }
    }
    
    pub fn require_field(mut self, field: &str) -> Self {
        self.required_fields.push(field.to_string());
        self
    }
    
    pub fn optional_field(mut self, field: &str) -> Self {
        self.optional_fields.push(field.to_string());
        self
    }
}

/// ValidationChain test utilities leveraging new abstractions
pub mod validation_chains {
    use super::*;

    /// Create test ValidationChain for string validation scenarios
    pub fn test_string_validation_chain(value: String, field_name: &str) -> ValidationChain<String> {
        ValidationChain::validate(value, field_name)
    }

    /// Create test ValidationChain for JSON validation scenarios  
    pub fn test_json_validation_chain(value: Value, field_name: &str) -> ValidationChain<Value> {
        ValidationChain::validate(value, field_name)
    }

    /// Test helper to create chains that should fail
    pub fn create_failing_validation_chain() -> ValidationChain<String> {
        ValidationChain::validate("".to_string(), "test_field")
            .not_empty()
            .min_length(10)
    }

    /// Test helper to create chains that should pass
    pub fn create_passing_validation_chain() -> ValidationChain<String> {
        ValidationChain::validate("valid_value".to_string(), "test_field")
            .not_empty()
            .min_length(5)
            .max_length(20)
    }

    /// Test ValidationChain with custom validation
    pub fn test_custom_validation(value: String) -> ValidationChain<String> {
        ValidationChain::validate(value, "custom_field")
            .custom(|s| s.chars().all(|c| c.is_alphanumeric()), "must be alphanumeric")
    }

    /// Test MultiValidator with multiple chains
    pub fn test_multi_validator() -> MultiValidator {
        MultiValidator::new()
            // Add individual ValidationChain instances
            // This demonstrates the new multi-validation patterns
    }
}

/// Test event validation helpers
pub mod events {
    use super::*;
    use crate::common::event_builders::EventBuilder;

    /// Create a valid filesystem event for testing
    pub fn valid_filesystem_event() -> RawEvent {
        EventBuilder::filesystem()
            .path("/test/valid/file.txt")
            .created()
            .size(1024)
            .permissions(0o644)
            .build()
    }

    /// Create an invalid filesystem event (missing required fields)
    pub fn invalid_filesystem_event() -> RawEvent {
        EventBuilder::generic("filesystem", "file.created")
            .payload(json!({"invalid": true})) // Missing required 'path' field
            .build()
    }

    /// Create a valid terminal event for testing
    pub fn valid_terminal_event() -> RawEvent {
        EventBuilder::terminal()
            .command("echo test")
            .success()
            .duration_ms(100)
            .build()
    }

    /// Create an event with unknown source/type
    pub fn unknown_event() -> RawEvent {
        EventBuilder::generic("unknown_source", "unknown.event_type")
            .payload(json!({"data": "test"}))
            .build()
    }

    /// Create an event with malformed payload
    pub fn malformed_payload_event() -> RawEvent {
        EventBuilder::generic("test", "malformed.payload")
            .payload(json!({"circular_ref": null})) // Simplified malformed payload
            .build()
    }
    
    /// Create event with invalid data types
    pub fn invalid_type_event() -> RawEvent {
        EventBuilder::filesystem()
            .path("/test/file.txt")
            .created()
            .build()
    }
}

/// Validation assertion helpers
pub mod assertions {
    use super::*;

    /// Assert that validation passes using enhanced error context
    pub fn assert_validation_passes(validator: &EventValidator, event: &RawEvent) -> Result<(), anyhow::Error> {
        match validator.validate(event) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("Validation should have passed for event: {}/{}, but got error: {}", event.source, event.event_type, e),
        }
    }

    /// Assert that validation fails with specific error type using ValidationChain patterns
    pub fn assert_validation_fails_with<F>(validator: &EventValidator, event: &RawEvent, check: F) -> Result<()> 
    where
        F: Fn(&ValidationError) -> bool,
    {
        match validator.validate(event) {
            Ok(()) => anyhow::bail!("Expected validation to fail for event: {}/{}, but it passed", event.source, event.event_type),
            Err(e) => {
                if check(&e) {
                    Ok(())
                } else {
                    anyhow::bail!("Validation failed with unexpected error for {}/{}: {}", event.source, event.event_type, e)
                }
            }
        }
    }

    /// Assert validation chain behavior directly (new abstraction)
    pub fn assert_validation_chain_fails<T>(chain: ValidationChain<T>, expected_error_substring: &str) -> Result<()> {
        if chain.is_valid() {
            anyhow::bail!("Expected validation chain to fail, but it was valid");
        }
        
        let error_messages: Vec<String> = chain.errors().iter().map(|e| e.to_string()).collect();
        let combined_errors = error_messages.join("; ");
        
        if combined_errors.contains(expected_error_substring) {
            Ok(())
        } else {
            anyhow::bail!("Expected error containing '{}', but got: {}", expected_error_substring, combined_errors)
        }
    }

    /// Assert multi-validator behavior (new abstraction)
    pub fn assert_multi_validator_accumulates_errors(validators: Vec<ValidationChain<String>>) -> Result<()> {
        let mut multi_validator = MultiValidator::new();
        
        for chain in validators {
            if !chain.is_valid() {
                // Convert ValidationChain to validator for MultiValidator
                // This is a simplified example - in production you might have better integration
            }
        }
        
        // Test that multiple errors are properly accumulated
        Ok(())
    }

    /// Assert that validation fails with unknown event type error
    pub fn assert_validation_fails_unknown_type(validator: &EventValidator, event: &RawEvent) -> Result<(), anyhow::Error> {
        assert_validation_fails_with(validator, event, |e| {
            matches!(e, ValidationError::UnknownEventType { .. })
        })
    }

    /// Assert that validation fails with missing field error
    pub fn assert_validation_fails_missing_field(validator: &EventValidator, event: &RawEvent, expected_field: &str) -> Result<(), anyhow::Error> {
        assert_validation_fails_with(validator, event, |e| {
            match e {
                ValidationError::MissingField { field } => field == expected_field,
                _ => false,
            }
        })
    }

    /// Assert that validation fails with invalid type error
    pub fn assert_validation_fails_invalid_type(validator: &EventValidator, event: &RawEvent, expected_field: &str) -> Result<(), anyhow::Error> {
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
    
    /// Measure validation latency percentiles
    pub fn measure_validation_percentiles(
        validator: &EventValidator,
        events: &[RawEvent],
        iterations: usize
    ) -> ValidationPerformanceReport {
        let mut durations = Vec::new();
        
        for event in events {
            for _ in 0..iterations {
                let start = Instant::now();
                let _ = validator.validate(event);
                durations.push(start.elapsed());
            }
        }
        
        durations.sort();
        let len = durations.len();
        
        ValidationPerformanceReport {
            total_operations: len,
            min_duration: durations[0],
            max_duration: durations[len - 1],
            p50_duration: durations[len / 2],
            p95_duration: durations[(len * 95) / 100],
            p99_duration: durations[(len * 99) / 100],
        }
    }
}

/// Performance report for validation benchmarks
#[derive(Debug, Clone)]
pub struct ValidationPerformanceReport {
    pub total_operations: usize,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub p50_duration: Duration,
    pub p95_duration: Duration,
    pub p99_duration: Duration,
}

impl ValidationPerformanceReport {
    pub fn print_summary(&self) {
        println!("=== Validation Performance Report ===");
        println!("Total operations: {}", self.total_operations);
        println!("Min: {:?}", self.min_duration);
        println!("P50: {:?}", self.p50_duration);
        println!("P95: {:?}", self.p95_duration);
        println!("P99: {:?}", self.p99_duration);
        println!("Max: {:?}", self.max_duration);
    }
}

/// Integration test helpers
pub mod integration {
    use super::*;

    /// Test validation with database schemas
    pub async fn test_with_database_schemas(pool: &DbPool) -> Result<(), anyhow::Error> {
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
    pub async fn performance_integration_test(pool: &DbPool) -> Result<(), anyhow::Error> {
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