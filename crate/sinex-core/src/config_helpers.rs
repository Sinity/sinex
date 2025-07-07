//! Production configuration helpers for common configuration scenarios
//!
//! This module provides production-ready configuration helpers, validators,
//! and extraction utilities that were originally developed in the test suite.

use crate::{ConfigValue, CoreError, Result, ValidationChain};
use crate::config_extractors::{ConfigExtractor, ConfigValidator, parse_duration};
use std::collections::HashMap;

/// Configuration factory for creating validated configurations
pub struct ConfigFactory;

impl ConfigFactory {
    /// Create a configuration validator for database settings
    pub fn database_validator() -> impl Fn(&ConfigValue) -> Result<()> {
        ConfigValidator::new()
            .require("database.url")
            .validate_custom(|config| {
                // Validate URL format
                if let Some(url) = config.optional_str("database.url") {
                    ValidationChain::validate(url.to_string(), "database.url")
                        .not_empty()
                        .is_valid_url()
                        .into_result()?;
                }

                // Validate pool size range
                if let Some(pool_size) = config.optional_i64("database.pool_size") {
                    ValidationChain::validate(pool_size, "database.pool_size")
                        .min(1)
                        .max(1000)
                        .into_result()?;
                }

                Ok(())
            })
            .build()
    }

    /// Create a configuration validator for collector settings
    pub fn collector_validator() -> impl Fn(&ConfigValue) -> Result<()> {
        ConfigValidator::new()
            .validate_range("collector.buffer_size", 1..=100000)
            .validate_range("collector.batch_size", 1..=10000)
            .validate_custom(|config| {
                // Validate flush interval format
                if let Some(interval) = config.optional_str("collector.flush_interval") {
                    parse_duration(interval)?;
                }
                Ok(())
            })
            .build()
    }

    /// Create a configuration validator for observability settings
    pub fn observability_validator() -> impl Fn(&ConfigValue) -> Result<()> {
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

    /// Create a comprehensive validator that combines all configuration aspects
    pub fn comprehensive_validator() -> impl Fn(&ConfigValue) -> Result<()> {
        move |config: &ConfigValue| {
            // Validate database section
            if let Err(e) = Self::database_validator()(config) {
                return Err(CoreError::Configuration(format!("Database configuration invalid: {}", e)));
            }

            // Validate collector section if present
            if config.optional_str("collector.buffer_size").is_some() {
                if let Err(e) = Self::collector_validator()(config) {
                    return Err(CoreError::Configuration(format!("Collector configuration invalid: {}", e)));
                }
            }

            // Validate observability section if present
            if config.optional_str("observability.metrics_port").is_some() {
                if let Err(e) = Self::observability_validator()(config) {
                    return Err(CoreError::Configuration(format!("Observability configuration invalid: {}", e)));
                }
            }

            Ok(())
        }
    }
}

/// Configuration extraction utilities
pub struct ConfigExtraction;

impl ConfigExtraction {
    /// Extract and validate database configuration
    pub fn extract_database_config(config: &ConfigValue) -> Result<DatabaseConfig> {
        let url = config.require_str("database.url")?;
        let pool_size = config.u64_or("database.pool_size", 10);
        let timeout_seconds = config.u64_or("database.timeout_seconds", 30);
        let max_connections = config.u64_or("database.max_connections", 100);

        // Validate extracted values using ValidationChain
        ValidationChain::validate(url.to_string(), "database.url")
            .not_empty()
            .is_valid_url()
            .into_result()?;

        ValidationChain::validate(pool_size, "database.pool_size")
            .min(1)
            .max(1000)
            .into_result()?;

        ValidationChain::validate(timeout_seconds, "database.timeout_seconds")
            .min(1)
            .max(300)
            .into_result()?;

        Ok(DatabaseConfig {
            url: url.to_string(),
            pool_size,
            timeout_seconds,
            max_connections,
        })
    }

    /// Extract and validate collector configuration
    pub fn extract_collector_config(config: &ConfigValue) -> Result<CollectorConfig> {
        let buffer_size = config.u64_or("collector.buffer_size", 1000);
        let batch_size = config.u64_or("collector.batch_size", 100);
        let flush_interval_str = config.str_or("collector.flush_interval", "5s");
        let max_retries = config.u64_or("collector.max_retries", 3);

        // Parse and validate duration
        let flush_interval_seconds = parse_duration(flush_interval_str)?;

        // Validate ranges using ValidationChain
        ValidationChain::validate(buffer_size, "collector.buffer_size")
            .min(1)
            .max(100000)
            .into_result()?;

        ValidationChain::validate(batch_size, "collector.batch_size")
            .min(1)
            .max(10000)
            .into_result()?;

        Ok(CollectorConfig {
            buffer_size,
            batch_size,
            flush_interval_seconds,
            max_retries,
        })
    }

    /// Extract and validate observability configuration  
    pub fn extract_observability_config(config: &ConfigValue) -> Result<ObservabilityConfig> {
        let metrics_port = config.u64_or("observability.metrics_port", 8080);
        let health_check_interval_str = config.str_or("observability.health_check_interval", "30s");
        let log_level = config.str_or("observability.log_level", "info");
        let tracing_enabled = config.bool_or("observability.tracing_enabled", false);

        // Parse and validate duration
        let health_check_interval_seconds = parse_duration(health_check_interval_str)?;

        // Validate port range
        ValidationChain::validate(metrics_port, "observability.metrics_port")
            .min(1024)
            .max(65535)
            .into_result()?;

        // Validate log level
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&log_level) {
            return Err(CoreError::Configuration(format!(
                "Invalid log level '{}', must be one of: {}",
                log_level, valid_levels.join(", ")
            )));
        }

        Ok(ObservabilityConfig {
            metrics_port,
            health_check_interval_seconds,
            log_level: log_level.to_string(),
            tracing_enabled,
        })
    }

    /// Extract sources configuration with validation
    pub fn extract_sources_config(config: &ConfigValue) -> Result<SourcesConfig> {
        let filesystem_enabled = config.bool_or("sources.filesystem", false);
        let terminal_enabled = config.bool_or("sources.terminal", false);
        let clipboard_enabled = config.bool_or("sources.clipboard", false);
        let desktop_enabled = config.bool_or("sources.desktop", false);

        // Extract filesystem-specific config if enabled
        let filesystem_watch_paths = if filesystem_enabled {
            if let Ok(array) = config.require_array("sources.filesystem.watch_paths") {
                array
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            } else {
                vec!["/tmp".to_string()]
            }
        } else {
            Vec::new()
        };

        // Validate watch paths
        for path in &filesystem_watch_paths {
            ValidationChain::validate(path.clone(), "sources.filesystem.watch_paths")
                .not_empty()
                .into_result()?;
            
            // Additional path validation
            if crate::validation::validate_path(path).is_err() {
                return Err(CoreError::Configuration(format!(
                    "Invalid filesystem watch path: {}", path
                )));
            }
        }

        Ok(SourcesConfig {
            filesystem_enabled,
            terminal_enabled,
            clipboard_enabled,
            desktop_enabled,
            filesystem_watch_paths,
        })
    }
}

/// Configuration structures for typed access

#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseConfig {
    pub url: String,
    pub pool_size: u64,
    pub timeout_seconds: u64,
    pub max_connections: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectorConfig {
    pub buffer_size: u64,
    pub batch_size: u64,
    pub flush_interval_seconds: u64,
    pub max_retries: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObservabilityConfig {
    pub metrics_port: u64,
    pub health_check_interval_seconds: u64,
    pub log_level: String,
    pub tracing_enabled: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourcesConfig {
    pub filesystem_enabled: bool,
    pub terminal_enabled: bool,
    pub clipboard_enabled: bool,
    pub desktop_enabled: bool,
    pub filesystem_watch_paths: Vec<String>,
}

/// Configuration merge utilities for layered configuration loading
pub struct ConfigMerger;

impl ConfigMerger {
    /// Merge two configurations, with the second taking precedence
    pub fn merge_configs(base: ConfigValue, overlay: ConfigValue) -> Result<ConfigValue> {
        match (base, overlay) {
            (ConfigValue::Table(mut base_table), ConfigValue::Table(overlay_table)) => {
                for (key, value) in overlay_table {
                    if let Some(base_value) = base_table.get(&key) {
                        // Recursive merge for nested tables
                        if matches!(base_value, ConfigValue::Table(_)) && matches!(value, ConfigValue::Table(_)) {
                            base_table.insert(key, Self::merge_configs(base_value.clone(), value)?);
                        } else {
                            // Replace value for non-table types
                            base_table.insert(key, value);
                        }
                    } else {
                        // New key, just insert
                        base_table.insert(key, value);
                    }
                }
                Ok(ConfigValue::Table(base_table))
            }
            (_, overlay) => {
                // Non-table overlay always replaces base
                Ok(overlay)
            }
        }
    }

    /// Create a layered configuration loader that merges multiple sources
    pub fn load_layered_config(
        default_config: ConfigValue,
        file_configs: Vec<ConfigValue>,
        env_overrides: HashMap<String, String>,
    ) -> Result<ConfigValue> {
        // Start with default config
        let mut result = default_config;

        // Apply file configs in order
        for file_config in file_configs {
            result = Self::merge_configs(result, file_config)?;
        }

        // Apply environment overrides
        result = Self::apply_env_overrides(result, env_overrides)?;

        Ok(result)
    }

    /// Apply environment variable overrides using dot notation
    fn apply_env_overrides(
        mut config: ConfigValue,
        env_overrides: HashMap<String, String>,
    ) -> Result<ConfigValue> {
        for (key, value) in env_overrides {
            // Convert environment variable to nested config value
            Self::set_nested_value(&mut config, &key, value)?;
        }
        Ok(config)
    }

    /// Set a nested value in the configuration using dot notation
    fn set_nested_value(config: &mut ConfigValue, path: &str, value: String) -> Result<()> {
        let parts: Vec<&str> = path.split('.').collect();
        
        if parts.is_empty() {
            return Err(CoreError::Configuration("Empty configuration path".to_string()));
        }

        // Navigate to the parent table
        let mut current = config;
        for part in &parts[..parts.len() - 1] {
            match current {
                ConfigValue::Table(ref mut table) => {
                    current = table.entry(part.to_string()).or_insert_with(|| {
                        ConfigValue::Table(toml::map::Map::new())
                    });
                }
                _ => {
                    return Err(CoreError::Configuration(format!(
                        "Cannot navigate into non-table value at '{}'",
                        part
                    )));
                }
            }
        }

        // Set the final value
        let final_key = parts[parts.len() - 1];
        if let ConfigValue::Table(ref mut table) = current {
            // Try to parse as different types
            let config_value = if let Ok(b) = value.parse::<bool>() {
                ConfigValue::Boolean(b)
            } else if let Ok(i) = value.parse::<i64>() {
                ConfigValue::Integer(i)
            } else if let Ok(f) = value.parse::<f64>() {
                ConfigValue::Float(f)
            } else {
                ConfigValue::String(value)
            };
            
            table.insert(final_key.to_string(), config_value);
        } else {
            return Err(CoreError::Configuration(format!(
                "Cannot set value on non-table at '{}'",
                path
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use toml;

    #[test]
    fn test_database_config_extraction() {
        let config: ConfigValue = toml::from_str(
            r#"
            [database]
            url = "postgresql://localhost/test_db"
            pool_size = 20
            timeout_seconds = 60
            max_connections = 150
        "#,
        )
        .unwrap();

        let db_config = ConfigExtraction::extract_database_config(&config).unwrap();
        assert_eq!(db_config.url, "postgresql://localhost/test_db");
        assert_eq!(db_config.pool_size, 20);
        assert_eq!(db_config.timeout_seconds, 60);
        assert_eq!(db_config.max_connections, 150);
    }

    #[test]
    fn test_collector_config_extraction() {
        let config: ConfigValue = toml::from_str(
            r#"
            [collector]
            buffer_size = 2000
            batch_size = 200
            flush_interval = "10s"
            max_retries = 5
        "#,
        )
        .unwrap();

        let collector_config = ConfigExtraction::extract_collector_config(&config).unwrap();
        assert_eq!(collector_config.buffer_size, 2000);
        assert_eq!(collector_config.batch_size, 200);
        assert_eq!(collector_config.flush_interval_seconds, 10);
        assert_eq!(collector_config.max_retries, 5);
    }

    #[test]
    fn test_config_merging() {
        let base: ConfigValue = toml::from_str(
            r#"
            [database]
            url = "postgresql://localhost/default"
            pool_size = 10
            
            [collector]
            buffer_size = 1000
        "#,
        )
        .unwrap();

        let overlay: ConfigValue = toml::from_str(
            r#"
            [database]
            pool_size = 20
            
            [observability]
            metrics_port = 8080
        "#,
        )
        .unwrap();

        let merged = ConfigMerger::merge_configs(base, overlay).unwrap();
        
        assert_eq!(merged.require_str("database.url").unwrap(), "postgresql://localhost/default");
        assert_eq!(merged.require_i64("database.pool_size").unwrap(), 20);
        assert_eq!(merged.require_i64("collector.buffer_size").unwrap(), 1000);
        assert_eq!(merged.require_i64("observability.metrics_port").unwrap(), 8080);
    }

    #[test]
    fn test_comprehensive_validation() {
        let valid_config: ConfigValue = toml::from_str(
            r#"
            [database]
            url = "postgresql://localhost/test"
            pool_size = 20
            
            [collector]
            buffer_size = 2000
            batch_size = 100
            flush_interval = "5s"
        "#,
        )
        .unwrap();

        let validator = ConfigFactory::comprehensive_validator();
        assert!(validator(&valid_config).is_ok());

        let invalid_config: ConfigValue = toml::from_str(
            r#"
            [database]
            url = "invalid-url"
            pool_size = -5
        "#,
        )
        .unwrap();

        assert!(validator(&invalid_config).is_err());
    }
}