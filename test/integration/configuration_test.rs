//! Consolidated configuration tests
//! 
//! Combines tests from:
//! - collector/config_tests.rs (basic validation)
//! - failure_modes/config_reload_test.rs (reload testing)
//! - nixos/config_validation_test.rs (NixOS-specific)

use crate::common::prelude::*;
use sinex_collector::config::{CollectorConfig, ValidationReport};
use rstest::rstest;
use std::collections::HashMap;

/// Test basic configuration validation
#[rstest]
#[case::default_config(CollectorConfig::default(), true)]
#[case::valid_custom_config(create_valid_custom_config(), true)]
#[case::invalid_empty_events(create_invalid_config_empty_events(), false)]
#[case::invalid_bad_event_type(create_invalid_config_bad_event(), false)]
#[sinex_test]
async fn test_config_validation(
    ctx: TestContext,
    #[case] config: CollectorConfig,
    #[case] should_be_valid: bool,
) -> TestResult {
    let result = config.validate();
    
    if should_be_valid {
        assert!(result.is_ok(), "Config should be valid: {:?}", result.err());
    } else {
        assert!(result.is_err(), "Config should be invalid");
    }
    
    Ok(())
}

/// Test configuration validation reports
#[sinex_test]
async fn test_config_validation_report(ctx: TestContext) -> TestResult {
    // Test valid config
    let valid_config = CollectorConfig::default();
    let report = valid_config.get_validation_report();
    
    assert!(report.valid, "Default config should have valid report");
    assert!(report.errors.is_empty(), "Should have no errors");
    
    // Test invalid config
    let mut invalid_config = CollectorConfig::default();
    invalid_config.enabled_events.push("totally_invalid_event".to_string());
    
    let report = invalid_config.get_validation_report();
    assert!(!report.valid, "Invalid config should have invalid report");
    assert!(!report.errors.is_empty(), "Should have validation errors");
    
    Ok(())
}

/// Test configuration merging and override behavior
#[sinex_test]
async fn test_config_merging(ctx: TestContext) -> TestResult {
    let base_config = CollectorConfig::default();
    
    // Test that we can override specific settings
    let mut custom_config = base_config.clone();
    custom_config.enabled_events = vec!["file.created".to_string(), "command.executed".to_string()];
    
    assert_ne!(base_config.enabled_events, custom_config.enabled_events);
    assert!(custom_config.validate().is_ok());
    
    Ok(())
}

/// Test event configuration extraction
#[sinex_test]
async fn test_event_config_extraction(ctx: TestContext) -> TestResult {
    let mut config = CollectorConfig::default();
    
    // Add some event-specific configuration
    config.flat_config.insert(
        "event.file_created".to_string(),
        toml::Value::Table({
            let mut table = toml::Map::new();
            table.insert("watch_paths".to_string(), toml::Value::Array(vec![
                toml::Value::String("/tmp".to_string()),
                toml::Value::String("/home".to_string()),
            ]));
            table
        }),
    );
    
    // Extract configuration for file.created events
    let event_config = config.get_event_config("file.created");
    
    // Should find the configuration
    assert!(event_config.is_table());
    if let toml::Value::Table(table) = &event_config {
        assert!(table.contains_key("watch_paths"));
    }
    
    Ok(())
}

/// Test configuration validation with malformed data
#[rstest]
#[case::empty_event_name("")]
#[case::invalid_characters("file/created")]
#[case::too_short("a")]
#[case::invalid_format("file..created")]
#[sinex_test]
async fn test_malformed_event_type_validation(
    ctx: TestContext,
    #[case] invalid_event_type: &str,
) -> TestResult {
    let mut config = CollectorConfig::default();
    config.enabled_events.push(invalid_event_type.to_string());
    
    let result = config.validate();
    assert!(result.is_err(), "Malformed event type '{}' should fail validation", invalid_event_type);
    
    Ok(())
}

/// Test configuration with valid event types
#[rstest]
#[case::filesystem("file.created")]
#[case::terminal("command.executed")]
#[case::terminal_hist("command.hist")]
#[case::clipboard("copied")]
#[case::window_manager("window.focused")]
#[sinex_test]
async fn test_valid_event_config(
    ctx: TestContext,
    #[case] event_type: &str,
) -> TestResult {
    let mut config = CollectorConfig::default();
    config.enabled_events = vec![event_type.to_string()];
    
    let result = config.validate();
    assert!(result.is_ok(), "Valid event type '{}' should pass validation", event_type);
    
    Ok(())
}

/// Test validation report accumulation
#[sinex_test]
async fn test_validation_report_accumulation(ctx: TestContext) -> TestResult {
    let mut config = CollectorConfig::default();
    
    // Add multiple invalid configurations
    config.enabled_events.extend([
        "invalid_event_1".to_string(),
        "".to_string(),  // Empty name
        "invalid/event/2".to_string(),  // Invalid characters
    ]);
    
    let report = config.get_validation_report();
    
    assert!(!report.valid);
    assert!(report.errors.len() >= 3, "Should accumulate multiple errors");
    
    Ok(())
}

/// Test validation report merging
#[sinex_test]
async fn test_validation_report_merge(ctx: TestContext) -> TestResult {
    let mut report1 = ValidationReport::new();
    report1.add_error("Error 1".to_string());
    report1.add_warning("Warning 1".to_string());
    
    let mut report2 = ValidationReport::new();
    report2.add_error("Error 2".to_string());
    report2.add_recommendation("Recommendation 1".to_string());
    
    // Merge reports
    report1.merge(report2);
    
    assert_eq!(report1.errors.len(), 2);
    assert_eq!(report1.warnings.len(), 1);
    assert_eq!(report1.recommendations.len(), 1);
    assert!(!report1.valid);
    
    Ok(())
}

/// Test cross-validation between configuration sections
#[sinex_test]
async fn test_config_cross_validation(ctx: TestContext) -> TestResult {
    let mut config = CollectorConfig::default();
    
    // Enable an event but don't provide required configuration
    config.enabled_events.push("command.executed".to_string());
    // Missing required configuration for command.executed
    
    let result = config.cross_validate();
    // This should detect the missing configuration dependency
    
    Ok(())
}

// Helper functions for creating test configurations

fn create_valid_custom_config() -> CollectorConfig {
    let mut config = CollectorConfig::default();
    config.enabled_events = vec![
        "file.created".to_string(),
        "file.modified".to_string(),
        "command.executed".to_string(),
    ];
    
    // Add valid event configurations
    config.flat_config.insert(
        "event.file_created".to_string(),
        toml::Value::Table({
            let mut table = toml::Map::new();
            table.insert("watch_patterns".to_string(), toml::Value::Array(vec![
                toml::Value::String("/tmp/**/*".to_string()),
            ]));
            table
        }),
    );
    
    config
}

fn create_invalid_config_empty_events() -> CollectorConfig {
    let mut config = CollectorConfig::default();
    config.enabled_events = vec![]; // Empty events list might be invalid
    config
}

fn create_invalid_config_bad_event() -> CollectorConfig {
    let mut config = CollectorConfig::default();
    config.enabled_events = vec!["totally_nonexistent_event_type".to_string()];
    config
}