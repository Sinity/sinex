//! Configuration management utilities for the Sinex event capture system
//!
//! This crate provides type-safe configuration extraction, validation,
//! and merging utilities for the Sinex ecosystem.

pub mod duration_parser;
pub mod extractors;
pub mod helpers;
pub mod validation_framework;
pub mod validators;

#[cfg(test)]
mod integration_test;

// Re-export main types
pub use duration_parser::parse_duration;
pub use helpers::{
    ConfigFactory, DatabaseConfig,
    ObservabilityConfig, SourcesConfig,
};

pub use validation_framework::{
    create_sinex_validator, ConfigurationValidator, CrossFieldCondition, CrossFieldRule,
    EnvironmentRule, RuleType, Severity, ValidatedConfigBuilder, ValidationIssue, ValidationResult,
    ValidationRule, ValidationValue,
};
pub use validators::*;

// Common type alias for external compatibility
pub type ConfigValue = toml::Value;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_configuration_validator_basic() {
        let validator = create_sinex_validator();

        // Test with valid configuration
        let valid_config = HashMap::from([
            (
                "service_name".to_string(),
                ValidationValue::String("test-service".to_string()),
            ),
            (
                "log_level".to_string(),
                ValidationValue::String("info".to_string()),
            ),
            (
                "database_url".to_string(),
                ValidationValue::String("postgresql://localhost/test".to_string()),
            ),
            (
                "database_pool_size".to_string(),
                ValidationValue::Integer(10),
            ),
        ]);

        let result = validator.validate(&valid_config, Some("development"));
        assert!(result.valid, "Valid configuration should pass validation");
        assert!(
            result.issues.is_empty(),
            "Valid configuration should have no issues"
        );
    }

    #[test]
    fn test_configuration_validator_missing_required() {
        let validator = create_sinex_validator();

        // Test with missing required field
        let invalid_config = HashMap::from([(
            "log_level".to_string(),
            ValidationValue::String("info".to_string()),
        )]);

        let result = validator.validate(&invalid_config, Some("development"));
        assert!(
            !result.valid,
            "Configuration missing required fields should fail"
        );
        assert!(
            !result.issues.is_empty(),
            "Configuration should have validation issues"
        );

        // Check that the issue mentions service_name
        let service_name_issue = result
            .issues
            .iter()
            .find(|issue| issue.field_path == "service_name");
        assert!(
            service_name_issue.is_some(),
            "Should have issue for missing service_name"
        );
    }

    #[test]
    fn test_configuration_validator_invalid_log_level() {
        let validator = create_sinex_validator();

        // Test with invalid log level
        let invalid_config = HashMap::from([
            (
                "service_name".to_string(),
                ValidationValue::String("test-service".to_string()),
            ),
            (
                "log_level".to_string(),
                ValidationValue::String("invalid_level".to_string()),
            ),
        ]);

        let result = validator.validate(&invalid_config, Some("development"));
        assert!(
            !result.valid,
            "Configuration with invalid log level should fail"
        );

        // Check that there's an issue about log level
        let log_level_issue = result
            .issues
            .iter()
            .find(|issue| issue.field_path == "log_level");
        assert!(
            log_level_issue.is_some(),
            "Should have issue for invalid log level"
        );
    }

    #[test]
    fn test_validation_value_types() {
        // Test different ValidationValue types
        assert_eq!(
            ValidationValue::String("test".to_string()),
            ValidationValue::String("test".to_string())
        );
        assert_eq!(ValidationValue::Integer(42), ValidationValue::Integer(42));
        assert_eq!(
            ValidationValue::Boolean(true),
            ValidationValue::Boolean(true)
        );
        assert_eq!(ValidationValue::Null, ValidationValue::Null);
    }

    #[test]
    fn test_severity_ordering() {
        // Test that severity levels have correct ordering
        assert!(Severity::Info < Severity::Warning);
        assert!(Severity::Warning < Severity::Error);
        assert!(Severity::Error < Severity::Critical);
    }
}
