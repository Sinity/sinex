//! Basic tests for satellite processing macros
//!
//! This test suite verifies the basic functionality of satellite derive macros
//! without requiring the full Sinex ecosystem.

use serde::{Deserialize, Serialize};
use sinex_macros::{EventHandler, PayloadExtractor, SatelliteConfig, SatelliteProcessor};

/// Test struct for SatelliteProcessor derive macro
#[derive(Debug, Default, SatelliteProcessor)]
pub struct TestProcessor {
    config: TestConfig,
    last_scan_time: Option<chrono::DateTime<chrono::Utc>>,
    processed_count: u64,
}

/// Test struct for EventHandler derive macro  
#[derive(Debug, Default, EventHandler)]
pub struct TestEventHandler {
    batch_size: usize,
    max_retries: u32,
}

/// Test struct for SatelliteConfig derive macro
#[derive(Debug, Default, Clone, Serialize, Deserialize, SatelliteConfig)]
pub struct TestConfig {
    pub watch_patterns: Vec<String>,
    pub debounce_ms: u64,
    pub enabled: bool,
    pub batch_size: usize,
}

/// Test struct for PayloadExtractor derive macro
#[derive(Debug, Default, PayloadExtractor)]
pub struct TestPayloadExtractor {
    schema: Option<serde_json::Value>,
}

/// Test payload struct for extraction
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestPayload {
    pub path: String,
    pub size: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[test]
fn test_satellite_processor_basic_functionality() {
    let processor = TestProcessor::new();

    // Test basic methods
    assert_eq!(processor.processor_name(), "TestProcessor");

    // Test that the processor was created with defaults (from Default trait)
    assert_eq!(processor.config.batch_size, 0);
    assert_eq!(processor.config.debounce_ms, 0);
    assert_eq!(processor.config.enabled, false);
    assert_eq!(processor.processed_count, 0);
}

#[test]
fn test_event_handler_basic_functionality() {
    let handler = TestEventHandler::default();

    // Test batch size method
    assert_eq!(handler.get_batch_size(), 100);

    // Test that the handler was created with defaults
    assert_eq!(handler.batch_size, 0);
    assert_eq!(handler.max_retries, 0);
}

#[test]
fn test_satellite_config_basic_functionality() {
    let config = TestConfig::default();

    // Test default values (from Default trait)
    assert_eq!(config.watch_patterns, Vec::<String>::new());
    assert_eq!(config.debounce_ms, 0);
    assert_eq!(config.enabled, false);
    assert_eq!(config.batch_size, 0);

    // Test validation
    assert!(config.validate().is_ok());
    assert!(config.is_valid());
}

#[test]
fn test_satellite_config_environment_loading() {
    let config = TestConfig::from_env();

    // Test that environment loading works (uses defaults when no env vars set)
    assert_eq!(config.debounce_ms, 0);
    assert_eq!(config.enabled, false);
    assert_eq!(config.batch_size, 0);
}

#[test]
fn test_satellite_config_hierarchical_loading() {
    let result = TestConfig::load();
    assert!(result.is_ok());

    let config = result.unwrap();
    assert!(config.is_valid());
}

#[test]
fn test_satellite_config_json_serialization() {
    let config = TestConfig::default();

    // Test JSON serialization
    let json_result = config.to_json();
    assert!(json_result.is_ok());

    let json_str = json_result.unwrap();
    assert!(json_str.contains("watch_patterns"));
    assert!(json_str.contains("debounce_ms"));

    // Test JSON deserialization
    let from_json_result = TestConfig::from_json(&json_str);
    assert!(from_json_result.is_ok());

    let from_json_config = from_json_result.unwrap();
    assert_eq!(from_json_config.debounce_ms, config.debounce_ms);
    assert_eq!(from_json_config.enabled, config.enabled);
}

#[test]
fn test_payload_extractor_basic_functionality() {
    let extractor = TestPayloadExtractor::default();

    // Create test payload
    let test_payload = TestPayload {
        path: "/test/path".to_string(),
        size: 1024,
        timestamp: chrono::Utc::now(),
    };

    let json_payload = serde_json::to_value(&test_payload).unwrap();

    // Test payload extraction
    let extracted_result = extractor.extract_payload::<TestPayload>(&json_payload);
    assert!(extracted_result.is_ok());

    let extracted = extracted_result.unwrap();
    assert_eq!(extracted.path, test_payload.path);
    assert_eq!(extracted.size, test_payload.size);
}

#[test]
fn test_payload_extractor_validation() {
    let extractor = TestPayloadExtractor::default();

    let test_payload = TestPayload {
        path: "/test/path".to_string(),
        size: 1024,
        timestamp: chrono::Utc::now(),
    };

    let json_payload = serde_json::to_value(&test_payload).unwrap();

    // Test extraction with validation
    let validated_result = extractor.extract_and_validate::<TestPayload>(&json_payload);
    assert!(validated_result.is_ok());

    let validated = validated_result.unwrap();
    assert_eq!(validated.path, test_payload.path);
    assert_eq!(validated.size, test_payload.size);
}

#[test]
fn test_payload_extractor_can_extract() {
    let extractor = TestPayloadExtractor::default();

    let test_payload = TestPayload {
        path: "/test/path".to_string(),
        size: 1024,
        timestamp: chrono::Utc::now(),
    };

    let json_payload = serde_json::to_value(&test_payload).unwrap();

    // Test can_extract method
    assert!(extractor.can_extract(&json_payload));

    // Test with invalid payload
    let invalid_payload = serde_json::json!({"invalid": "data"});
    // This should still return true for generic serde_json::Value extraction
    assert!(extractor.can_extract(&invalid_payload));
}

#[test]
fn test_payload_extractor_type_coercion() {
    let extractor = TestPayloadExtractor::default();

    let test_payload = TestPayload {
        path: "/test/path".to_string(),
        size: 1024,
        timestamp: chrono::Utc::now(),
    };

    let json_payload = serde_json::to_value(&test_payload).unwrap();

    // Test type coercion (should fall back to direct extraction)
    let coerced_result = extractor.extract_with_coercion::<TestPayload>(&json_payload);
    assert!(coerced_result.is_ok());

    let coerced = coerced_result.unwrap();
    assert_eq!(coerced.path, test_payload.path);
    assert_eq!(coerced.size, test_payload.size);
}

#[test]
fn test_integration_all_macros() {
    // Test that all macros work together in a realistic scenario
    let processor = TestProcessor::new();
    let handler = TestEventHandler::default();
    let config = TestConfig::default();
    let extractor = TestPayloadExtractor::default();

    // Test processor initialization
    assert_eq!(processor.processor_name(), "TestProcessor");

    // Test configuration
    assert!(config.is_valid());
    assert_eq!(config.batch_size, 0);

    // Test payload extraction
    let test_payload = TestPayload {
        path: "/integration/test".to_string(),
        size: 2048,
        timestamp: chrono::Utc::now(),
    };

    let json_payload = serde_json::to_value(&test_payload).unwrap();
    let extracted = extractor.extract_payload::<TestPayload>(&json_payload);
    assert!(extracted.is_ok());

    // Test batch size
    assert_eq!(handler.get_batch_size(), 100);
}

#[test]
fn test_error_handling_scenarios() {
    let extractor = TestPayloadExtractor::default();

    // Test with malformed JSON
    let malformed_json = serde_json::json!({"path": 123, "size": "invalid"});
    let result = extractor.extract_payload::<TestPayload>(&malformed_json);
    assert!(result.is_err());

    // Test with missing required fields
    let incomplete_json = serde_json::json!({"path": "/test"});
    let result2 = extractor.extract_payload::<TestPayload>(&incomplete_json);
    assert!(result2.is_err());
}

#[test]
fn test_empty_collections() {
    let config = TestConfig::default();

    // Test with empty collections
    assert_eq!(config.watch_patterns.len(), 0);
    assert!(config.is_valid());
}

#[test]
fn test_configuration_field_access() {
    let config = TestConfig::default();

    // Test field access (would return None in basic implementation)
    let field_result = config.get_field("debounce_ms");
    assert!(field_result.is_none()); // Expected with current implementation

    // Test field setting (would fail in basic implementation)
    let mut config_mut = config.clone();
    let set_result = config_mut.set_field("debounce_ms", serde_json::json!(1000));
    assert!(set_result.is_err()); // Expected with current implementation
}

#[test]
fn test_macro_generated_methods_exist() {
    // Test that all expected methods are generated and callable
    let processor = TestProcessor::new();
    let handler = TestEventHandler::default();
    let config = TestConfig::default();
    let extractor = TestPayloadExtractor::default();

    // Test SatelliteProcessor methods
    assert_eq!(processor.processor_name(), "TestProcessor");

    // Test EventHandler methods
    assert_eq!(handler.get_batch_size(), 100);

    // Test SatelliteConfig methods
    assert!(config.is_valid());
    assert!(config.validate().is_ok());
    let _ = config.to_json();

    // Test PayloadExtractor methods
    let test_json = serde_json::json!({"test": "value"});
    assert!(extractor.can_extract(&test_json));
}
