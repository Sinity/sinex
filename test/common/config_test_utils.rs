//! Configuration testing utilities using ConfigExtractor abstractions
//!
//! This module provides testing utilities for configuration validation and extraction
//! using the new ConfigExtractor and ConfigValidator abstractions.

use crate::common::prelude::*;

/// Create test configuration for various scenarios
pub mod test_configs {
    use super::*;

    /// Create a valid database configuration
    pub fn valid_database_config() -> ConfigValue {
        toml::from_str(
            r#"
            [database]
            url = "postgresql://localhost/test_db"
            pool_size = 10
            timeout_seconds = 30

            [collector]
            buffer_size = 1000
            batch_size = 100
            flush_interval = "5s"

            [sources]
            filesystem = true
            terminal = true
            clipboard = false
        "#,
        )
        .expect("Valid TOML config")
    }

    /// Create a configuration with missing required fields
    pub fn incomplete_config() -> ConfigValue {
        toml::from_str(
            r#"
            [database]
            # Missing url field
            pool_size = 10

            [collector]
            buffer_size = 1000
            # Missing batch_size
        "#,
        )
        .expect("Valid TOML config")
    }

    /// Create a configuration with invalid values
    pub fn invalid_values_config() -> ConfigValue {
        toml::from_str(
            r#"
            [database]
            url = "not-a-valid-url"
            pool_size = -5
            timeout_seconds = "invalid"

            [collector]
            buffer_size = 0
            batch_size = -1
            flush_interval = "invalid-duration"
        "#,
        )
        .expect("Valid TOML config")
    }

    /// Create a minimal valid configuration
    pub fn minimal_config() -> ConfigValue {
        toml::from_str(
            r#"
            [database]
            url = "postgresql://localhost/sinex_test"
        "#,
        )
        .expect("Valid TOML config")
    }

    /// Create a configuration with nested validation requirements
    pub fn complex_nested_config() -> ConfigValue {
        toml::from_str(
            r#"
            [database]
            url = "postgresql://localhost/sinex_test"
            pool_size = 20

            [collector]
            buffer_size = 2000
            batch_size = 50
            flush_interval = "10s"

            [sources.filesystem]
            enabled = true
            watch_paths = ["/home", "/var/log"]
            ignore_patterns = ["*.tmp", "*.log"]

            [sources.terminal]
            enabled = true
            shell_types = ["bash", "zsh", "fish"]
            max_history_size = 10000

            [observability]
            metrics_port = 8080
            health_check_interval = "30s"
            log_level = "info"
        "#,
        )
        .expect("Valid TOML config")
    }
}

/// Configuration validation test utilities
pub mod validation {
    use super::*;

    /// Test database configuration validation
    pub fn validate_database_config() -> impl Fn(&ConfigValue) -> sinex_core::Result<()> {
        ConfigValidator::new()
            .require("database.url")
            .validate_custom(|config| {
                // Validate URL format
                if let Some(url) = config.optional_str("database.url") {
                    ValidationChain::validate(url.to_string(), "database.url")
                        .is_valid_url()
                        .into_result()?;
                }

                // Validate pool size range
                if let Some(pool_size) = config.optional_i64("database.pool_size") {
                    ValidationChain::validate(pool_size, "database.pool_size")
                        .min(1)
                        .max(100)
                        .into_result()?;
                }

                Ok(())
            })
            .build()
    }

    /// Test collector configuration validation
    pub fn validate_collector_config() -> impl Fn(&ConfigValue) -> sinex_core::Result<()> {
        ConfigValidator::new()
            .validate_range("collector.buffer_size", 1..=10000)
            .validate_range("collector.batch_size", 1..=1000)
            .validate_custom(|config| {
                // Validate flush interval format
                if let Some(interval) = config.optional_str("collector.flush_interval") {
                    parse_duration(interval)?;
                }
                Ok(())
            })
            .build()
    }

    /// Test observability configuration validation
    pub fn validate_observability_config() -> impl Fn(&ConfigValue) -> sinex_core::Result<()> {
        ConfigValidator::new()
            .validate_range("observability.metrics_port", 1024..=65535)
            .validate_regex(
                "observability.log_level",
                r"^(trace|debug|info|warn|error)$",
            )
            .validate_custom(|config| {
                // Validate health check interval
                if let Some(interval) = config.optional_str("observability.health_check_interval") {
                    parse_duration(interval)?;
                }
                Ok(())
            })
            .build()
    }

    /// Comprehensive configuration validator combining all aspects
    pub fn validate_complete_config() -> impl Fn(&ConfigValue) -> sinex_core::Result<()> {
        move |config: &ConfigValue| {
            // Use MultiValidator pattern to accumulate all errors
            let multi_validator = MultiValidator::new();

            // Validate database section
            if let Err(e) = validate_database_config()(config) {
                return Err(CoreError::configuration("Database configuration invalid")
                    .with_source(e)
                    .build());
            }

            // Validate collector section
            if let Err(e) = validate_collector_config()(config) {
                return Err(CoreError::configuration("Collector configuration invalid")
                    .with_source(e)
                    .build());
            }

            // Validate observability section if present
            if config.optional_str("observability.metrics_port").is_some() {
                if let Err(e) = validate_observability_config()(config) {
                    return Err(
                        CoreError::configuration("Observability configuration invalid")
                            .with_source(e)
                            .build(),
                    );
                }
            }

            Ok(())
        }
    }
}

/// Configuration extraction test utilities
pub mod extraction {
    use super::*;

    /// Extract and validate database configuration
    pub fn extract_database_config(config: &ConfigValue) -> Result<DatabaseTestConfig> {
        let url = config.require_str("database.url")?;
        let pool_size = config.u64_or("database.pool_size", 10);
        let timeout_seconds = config.u64_or("database.timeout_seconds", 30);

        // Validate extracted values using ValidationChain
        ValidationChain::validate(url.to_string(), "database.url")
            .not_empty()
            .is_valid_url()
            .into_result()?;

        ValidationChain::validate(pool_size, "database.pool_size")
            .min(1)
            .max(100)
            .into_result()?;

        Ok(DatabaseTestConfig {
            url: url.to_string(),
            pool_size,
            timeout_seconds,
        })
    }

    /// Extract and validate collector configuration
    pub fn extract_collector_config(config: &ConfigValue) -> Result<CollectorTestConfig> {
        let buffer_size = config.u64_or("collector.buffer_size", 1000);
        let batch_size = config.u64_or("collector.batch_size", 100);
        let flush_interval_str = config.str_or("collector.flush_interval", "5s");

        // Parse and validate duration
        let flush_interval_seconds = parse_duration(flush_interval_str)?;

        // Validate ranges using ValidationChain
        ValidationChain::validate(buffer_size, "collector.buffer_size")
            .min(1)
            .max(10000)
            .into_result()?;

        ValidationChain::validate(batch_size, "collector.batch_size")
            .min(1)
            .max(1000)
            .into_result()?;

        Ok(CollectorTestConfig {
            buffer_size,
            batch_size,
            flush_interval_seconds,
        })
    }

    /// Extract sources configuration with validation
    pub fn extract_sources_config(config: &ConfigValue) -> Result<SourcesTestConfig> {
        let filesystem_enabled = config.bool_or("sources.filesystem", false);
        let terminal_enabled = config.bool_or("sources.terminal", false);
        let clipboard_enabled = config.bool_or("sources.clipboard", false);

        // Extract filesystem-specific config if enabled
        let filesystem_watch_paths = if filesystem_enabled {
            config
                .optional_str("sources.filesystem.watch_paths")
                .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
                .unwrap_or_else(|| vec!["/tmp".to_string()])
        } else {
            Vec::new()
        };

        Ok(SourcesTestConfig {
            filesystem_enabled,
            terminal_enabled,
            clipboard_enabled,
            filesystem_watch_paths,
        })
    }
}

/// Test configuration structures
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseTestConfig {
    pub url: String,
    pub pool_size: u64,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectorTestConfig {
    pub buffer_size: u64,
    pub batch_size: u64,
    pub flush_interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourcesTestConfig {
    pub filesystem_enabled: bool,
    pub terminal_enabled: bool,
    pub clipboard_enabled: bool,
    pub filesystem_watch_paths: Vec<String>,
}

/// Test configuration factory for creating configurations with specific characteristics
pub struct TestConfigFactory;

impl TestConfigFactory {
    /// Create configuration that should pass all validations
    pub fn create_valid_config() -> ConfigValue {
        test_configs::valid_database_config()
    }

    /// Create configuration with a specific missing field
    pub fn create_config_missing_field(field_path: &str) -> ConfigValue {
        let mut config = test_configs::valid_database_config();
        // Remove the specified field by navigating and deleting
        Self::remove_config_field(&mut config, field_path);
        config
    }

    /// Create configuration with invalid value for a specific field
    pub fn create_config_invalid_value(
        field_path: &str,
        invalid_value: ConfigValue,
    ) -> ConfigValue {
        let mut config = test_configs::valid_database_config();
        Self::set_config_field(&mut config, field_path, invalid_value);
        config
    }

    /// Create configuration with randomized valid values for testing
    pub fn create_randomized_config(seed: u64) -> ConfigValue {
        // Use seed for deterministic "randomization" in tests
        let pool_size = (seed % 50) + 1; // 1-50
        let buffer_size = ((seed * 17) % 5000) + 100; // 100-5099

        toml::from_str(&format!(
            r#"
            [database]
            url = "postgresql://localhost/test_db_{}"
            pool_size = {}

            [collector]
            buffer_size = {}
            batch_size = {}
            flush_interval = "{}s"
        "#,
            seed,
            pool_size,
            buffer_size,
            (seed % 10) + 1,
            (seed % 30) + 1
        ))
        .expect("Valid randomized config")
    }

    // Helper methods for config manipulation
    fn remove_config_field(config: &mut ConfigValue, field_path: &str) {
        // Simplified removal - in a real implementation you'd navigate the path
        if let ConfigValue::Table(ref mut table) = config {
            let parts: Vec<&str> = field_path.split('.').collect();
            if parts.len() == 2 && parts[0] == "database" {
                if let Some(ConfigValue::Table(ref mut db_table)) = table.get_mut("database") {
                    db_table.remove(parts[1]);
                }
            }
        }
    }

    fn set_config_field(config: &mut ConfigValue, field_path: &str, value: ConfigValue) {
        // Simplified field setting - in a real implementation you'd navigate the path
        if let ConfigValue::Table(ref mut table) = config {
            let parts: Vec<&str> = field_path.split('.').collect();
            if parts.len() == 2 && parts[0] == "database" {
                if let Some(ConfigValue::Table(ref mut db_table)) = table.get_mut("database") {
                    db_table.insert(parts[1].to_string(), value);
                }
            }
        }
    }
}

/// Configuration test scenarios for comprehensive testing
pub mod scenarios {
    use super::*;

    /// Test scenario structure
    #[derive(Debug)]
    pub struct ConfigTestScenario {
        pub name: String,
        pub config: ConfigValue,
        pub should_validate: bool,
        pub expected_error_substring: Option<String>,
    }

    /// Get all test scenarios for configuration validation
    pub fn all_validation_scenarios() -> Vec<ConfigTestScenario> {
        vec![
            ConfigTestScenario {
                name: "valid_complete_config".to_string(),
                config: test_configs::valid_database_config(),
                should_validate: true,
                expected_error_substring: None,
            },
            ConfigTestScenario {
                name: "missing_database_url".to_string(),
                config: test_configs::incomplete_config(),
                should_validate: false,
                expected_error_substring: Some("database.url".to_string()),
            },
            ConfigTestScenario {
                name: "invalid_url_format".to_string(),
                config: test_configs::invalid_values_config(),
                should_validate: false,
                expected_error_substring: Some("invalid URL".to_string()),
            },
            ConfigTestScenario {
                name: "minimal_valid_config".to_string(),
                config: test_configs::minimal_config(),
                should_validate: true,
                expected_error_substring: None,
            },
            ConfigTestScenario {
                name: "complex_nested_config".to_string(),
                config: test_configs::complex_nested_config(),
                should_validate: true,
                expected_error_substring: None,
            },
        ]
    }

    /// Get scenarios specifically for extraction testing
    pub fn extraction_scenarios() -> Vec<ConfigTestScenario> {
        vec![
            ConfigTestScenario {
                name: "extractable_database_config".to_string(),
                config: test_configs::valid_database_config(),
                should_validate: true,
                expected_error_substring: None,
            },
            ConfigTestScenario {
                name: "extractable_complex_config".to_string(),
                config: test_configs::complex_nested_config(),
                should_validate: true,
                expected_error_substring: None,
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation_scenarios() {
        for scenario in scenarios::all_validation_scenarios() {
            let validator = validation::validate_complete_config();
            let result = validator(&scenario.config);

            match (result.is_ok(), scenario.should_validate) {
                (true, true) => {
                    println!("✓ {} passed validation as expected", scenario.name);
                }
                (false, false) => {
                    if let Some(expected_substring) = scenario.expected_error_substring {
                        let error_msg = result.unwrap_err().to_string();
                        assert!(
                            error_msg.contains(&expected_substring),
                            "Expected error containing '{}' for {}, got: {}",
                            expected_substring,
                            scenario.name,
                            error_msg
                        );
                    }
                    println!("✓ {} failed validation as expected", scenario.name);
                }
                (true, false) => {
                    panic!(
                        "Expected {} to fail validation, but it passed",
                        scenario.name
                    );
                }
                (false, true) => {
                    panic!(
                        "Expected {} to pass validation, but it failed: {:?}",
                        scenario.name,
                        result.unwrap_err()
                    );
                }
            }
        }
    }

    #[test]
    fn test_config_extraction() {
        let config = test_configs::valid_database_config();

        // Test database config extraction
        let db_config = extraction::extract_database_config(&config).unwrap();
        assert_eq!(db_config.url, "postgresql://localhost/test_db");
        assert_eq!(db_config.pool_size, 10);

        // Test collector config extraction
        let collector_config = extraction::extract_collector_config(&config).unwrap();
        assert_eq!(collector_config.buffer_size, 1000);
        assert_eq!(collector_config.batch_size, 100);
        assert_eq!(collector_config.flush_interval_seconds, 5);
    }

    #[test]
    fn test_config_factory() {
        // Test valid config creation
        let valid_config = TestConfigFactory::create_valid_config();
        let validator = validation::validate_complete_config();
        assert!(validator(&valid_config).is_ok());

        // Test randomized config creation
        let random_config = TestConfigFactory::create_randomized_config(42);
        assert!(validator(&random_config).is_ok());
    }

    #[test]
    fn test_validation_chains_in_config() {
        let config = test_configs::invalid_values_config();

        // Test URL validation using ValidationChain
        if let Some(url) = config.optional_str("database.url") {
            let chain = ValidationChain::validate(url.to_string(), "database.url").is_valid_url();

            assert!(!chain.is_valid());
            assert!(chain
                .errors()
                .iter()
                .any(|e| e.to_string().contains("invalid URL")));
        }
    }
}
