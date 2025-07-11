//! Configuration extraction and validation utilities
//!
//! This module provides declarative configuration extraction helpers that work with
//! both TOML and JSON configuration formats. It provides type-safe access to nested
//! configuration values with clear error messages and validation support.

use crate::{ConfigValue, CoreError, Result};
use regex::Regex;
use sinex_macros::with_context;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Trait for extracting typed values from configuration
pub trait ConfigExtractor {
    /// Extract a required string value at the given path
    fn require_str(&self, path: &str) -> Result<&str>;

    /// Extract a required i64 value at the given path
    fn require_i64(&self, path: &str) -> Result<i64>;

    /// Extract a required u64 value at the given path
    fn require_u64(&self, path: &str) -> Result<u64>;

    /// Extract a required bool value at the given path
    fn require_bool(&self, path: &str) -> Result<bool>;

    /// Extract a required array at the given path
    fn require_array(&self, path: &str) -> Result<&Vec<ConfigValue>>;

    /// Extract an optional string value at the given path
    fn optional_str(&self, path: &str) -> Option<&str>;

    /// Extract an optional i64 value at the given path
    fn optional_i64(&self, path: &str) -> Option<i64>;

    /// Extract an optional u64 value at the given path
    fn optional_u64(&self, path: &str) -> Option<u64>;

    /// Extract an optional bool value at the given path
    fn optional_bool(&self, path: &str) -> Option<bool>;

    /// Extract a string value or return a default if not found
    fn str_or<'a>(&'a self, path: &str, default: &'a str) -> &'a str;

    /// Extract an i64 value or return a default if not found
    fn i64_or(&self, path: &str, default: i64) -> i64;

    /// Extract a u64 value or return a default if not found
    fn u64_or(&self, path: &str, default: u64) -> u64;

    /// Extract a bool value or return a default if not found
    fn bool_or(&self, path: &str, default: bool) -> bool;
}

impl ConfigExtractor for ConfigValue {
    fn require_str(&self, path: &str) -> Result<&str> {
        let value = navigate_path(self, path)?;
        value.as_str().ok_or_else(|| {
            CoreError::Configuration(format!("Configuration value at '{}' is not a string", path))
        })
    }

    fn require_i64(&self, path: &str) -> Result<i64> {
        let value = navigate_path(self, path)?;
        value.as_integer().ok_or_else(|| {
            CoreError::Configuration(format!(
                "Configuration value at '{}' is not an integer",
                path
            ))
        })
    }

    #[with_context(operation = "require_u64")]
    fn require_u64(&self, path: &str) -> Result<u64> {
        let value = navigate_path(self, path)?;
        let i = value.as_integer().ok_or_else(|| {
            CoreError::Configuration(format!(
                "Configuration value at '{}' is not an integer",
                path
            ))
        })?;

        if i < 0 {
            return Err(CoreError::Configuration(format!(
                "Configuration value at '{}' must be non-negative, got {}",
                path, i
            )));
        }

        Ok(i as u64)
    }

    fn require_bool(&self, path: &str) -> Result<bool> {
        let value = navigate_path(self, path)?;
        value.as_bool().ok_or_else(|| {
            CoreError::Configuration(format!(
                "Configuration value at '{}' is not a boolean",
                path
            ))
        })
    }

    fn require_array(&self, path: &str) -> Result<&Vec<ConfigValue>> {
        let value = navigate_path(self, path)?;
        value.as_array().ok_or_else(|| {
            CoreError::Configuration(format!("Configuration value at '{}' is not an array", path))
        })
    }

    fn optional_str(&self, path: &str) -> Option<&str> {
        navigate_path(self, path).ok()?.as_str()
    }

    fn optional_i64(&self, path: &str) -> Option<i64> {
        navigate_path(self, path).ok()?.as_integer()
    }

    fn optional_u64(&self, path: &str) -> Option<u64> {
        let i = navigate_path(self, path).ok()?.as_integer()?;
        if i < 0 {
            None
        } else {
            Some(i as u64)
        }
    }

    fn optional_bool(&self, path: &str) -> Option<bool> {
        navigate_path(self, path).ok()?.as_bool()
    }

    fn str_or<'a>(&'a self, path: &str, default: &'a str) -> &'a str {
        self.optional_str(path).unwrap_or(default)
    }

    fn i64_or(&self, path: &str, default: i64) -> i64 {
        self.optional_i64(path).unwrap_or(default)
    }

    fn u64_or(&self, path: &str, default: u64) -> u64 {
        self.optional_u64(path).unwrap_or(default)
    }

    fn bool_or(&self, path: &str, default: bool) -> bool {
        self.optional_bool(path).unwrap_or(default)
    }
}

/// Navigate a nested path in a ConfigValue
fn navigate_path<'a>(value: &'a ConfigValue, path: &str) -> Result<&'a ConfigValue> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;

    for (i, part) in parts.iter().enumerate() {
        match current {
            ConfigValue::Table(table) => {
                current = table.get(*part).ok_or_else(|| {
                    let partial_path = parts[..=i].join(".");
                    CoreError::Configuration(format!(
                        "Configuration key '{}' not found at path '{}'",
                        part, partial_path
                    ))
                })?;
            }
            _ => {
                let partial_path = parts[..i].join(".");
                return Err(CoreError::Configuration(format!(
                    "Cannot navigate into non-table value at '{}'",
                    partial_path
                )));
            }
        }
    }

    Ok(current)
}

/// Type alias for configuration validators to reduce complexity
type ConfigValidatorFn = Box<dyn Fn(&ConfigValue) -> Result<()>>;

/// Builder for configuration validation
pub struct ConfigValidator {
    required_fields: Vec<String>,
    validators: Vec<ConfigValidatorFn>,
}

impl ConfigValidator {
    /// Create a new configuration validator
    pub fn new() -> Self {
        Self {
            required_fields: Vec::new(),
            validators: Vec::new(),
        }
    }

    /// Require a field to be present
    pub fn require(mut self, path: &str) -> Self {
        self.required_fields.push(path.to_string());
        self
    }

    /// Validate that a numeric field is within a range
    pub fn validate_range(mut self, path: &str, range: std::ops::RangeInclusive<i64>) -> Self {
        let path = path.to_string();
        let validator = Box::new(move |config: &ConfigValue| {
            if let Some(value) = config.optional_i64(&path) {
                if !range.contains(&value) {
                    return Err(CoreError::Configuration(format!(
                        "Value at '{}' ({}) is outside allowed range {:?}",
                        path, value, range
                    )));
                }
            }
            Ok(())
        });
        self.validators.push(validator);
        self
    }

    /// Validate that a string field matches a regex pattern
    pub fn validate_regex(mut self, path: &str, pattern: &str) -> Self {
        let path = path.to_string();
        let regex = match Regex::new(pattern) {
            Ok(r) => Arc::new(r),
            Err(e) => panic!("Invalid regex pattern '{}': {}", pattern, e),
        };

        let validator = Box::new(move |config: &ConfigValue| {
            if let Some(value) = config.optional_str(&path) {
                if !regex.is_match(value) {
                    return Err(CoreError::Configuration(format!(
                        "Value at '{}' ('{}') does not match pattern '{}'",
                        path,
                        value,
                        regex.as_str()
                    )));
                }
            }
            Ok(())
        });
        self.validators.push(validator);
        self
    }

    /// Add a custom validation function
    pub fn validate_custom<F>(mut self, validator: F) -> Self
    where
        F: Fn(&ConfigValue) -> Result<()> + 'static,
    {
        self.validators.push(Box::new(validator));
        self
    }

    /// Validate that a path field is absolute or starts with ~/
    pub fn validate_path_format(mut self, path: &str) -> Self {
        let path = path.to_string();
        let validator = Box::new(move |config: &ConfigValue| {
            if let Some(path_str) = config.optional_str(&path) {
                // Use the core validation function for consistency
                if crate::validation::validate_path(path_str).is_err() {
                    return Err(CoreError::Configuration(format!(
                        "Path at '{}' contains dangerous content",
                        path
                    )));
                }

                if !Path::new(path_str).is_absolute() && !path_str.starts_with("~/") {
                    return Err(CoreError::Configuration(format!(
                        "Path at '{}' must be an absolute path or start with ~/",
                        path
                    )));
                }
            }
            Ok(())
        });
        self.validators.push(validator);
        self
    }

    /// Validate that a path field points to an absolute path
    pub fn validate_absolute_path(mut self, path: &str) -> Self {
        let path = path.to_string();
        let validator = Box::new(move |config: &ConfigValue| {
            if let Some(path_str) = config.optional_str(&path) {
                // Use the core validation function for consistency
                if crate::validation::validate_path(path_str).is_err() {
                    return Err(CoreError::Configuration(format!(
                        "Path at '{}' contains dangerous content",
                        path
                    )));
                }

                if !Path::new(path_str).is_absolute() {
                    return Err(CoreError::Configuration(format!(
                        "Path at '{}' must be an absolute path",
                        path
                    )));
                }
            }
            Ok(())
        });
        self.validators.push(validator);
        self
    }

    /// Validate that all elements in an array field match a condition
    pub fn validate_array_elements<F>(mut self, path: &str, element_validator: F) -> Self
    where
        F: Fn(&str, &ConfigValue) -> Result<()> + 'static,
    {
        let path = path.to_string();
        let validator = Box::new(move |config: &ConfigValue| {
            if let Ok(array) = config.require_array(&path) {
                for (i, element) in array.iter().enumerate() {
                    element_validator(&format!("{}[{}]", path, i), element)?;
                }
            }
            Ok(())
        });
        self.validators.push(validator);
        self
    }

    /// Validate that all string elements in an array are valid paths
    pub fn validate_path_array(mut self, path: &str) -> Self {
        let path = path.to_string();
        let validator = Box::new(move |config: &ConfigValue| {
            if let Ok(array) = config.require_array(&path) {
                for (i, element) in array.iter().enumerate() {
                    if let Some(path_str) = element.as_str() {
                        // Use the core validation function and check for shell metacharacters
                        if crate::validation::validate_path(path_str).is_err() {
                            return Err(CoreError::Configuration(format!(
                                "Path pattern at '{}[{}]' ('{}') contains dangerous content",
                                path, i, path_str
                            )));
                        }

                        // Check for command injection patterns
                        if crate::validation::contains_shell_metacharacters(path_str) {
                            return Err(CoreError::Configuration(format!(
                                "Path pattern at '{}[{}]' ('{}') contains shell metacharacters",
                                path, i, path_str
                            )));
                        }

                        if !path_str.starts_with('/')
                            && !path_str.starts_with("~/")
                            && !path_str.contains('*')
                        {
                            return Err(CoreError::Configuration(format!(
                                "Path pattern at '{}[{}]' ('{}') should be an absolute path, start with ~/, or contain wildcards", 
                                path, i, path_str
                            )));
                        }
                    } else {
                        return Err(CoreError::Configuration(format!(
                            "Element at '{}[{}]' must be a string",
                            path, i
                        )));
                    }
                }
            }
            Ok(())
        });
        self.validators.push(validator);
        self
    }

    /// Validate that a numeric field is positive (greater than 0)
    pub fn validate_positive(mut self, path: &str) -> Self {
        let path = path.to_string();
        let validator = Box::new(move |config: &ConfigValue| {
            if let Some(value) = config.optional_i64(&path) {
                if value <= 0 {
                    return Err(CoreError::Configuration(format!(
                        "Value at '{}' must be greater than 0, got {}",
                        path, value
                    )));
                }
            }
            Ok(())
        });
        self.validators.push(validator);
        self
    }

    /// Validate that string fields don't contain shell metacharacters
    pub fn validate_no_shell_metacharacters(mut self, path: &str) -> Self {
        let path = path.to_string();
        let validator = Box::new(move |config: &ConfigValue| {
            if let Some(value) = config.optional_str(&path) {
                if crate::validation::contains_shell_metacharacters(value) {
                    return Err(CoreError::Configuration(format!(
                        "Value at '{}' contains shell metacharacters",
                        path
                    )));
                }
            }
            Ok(())
        });
        self.validators.push(validator);
        self
    }

    /// Build and return a validation function
    pub fn build(self) -> impl Fn(&ConfigValue) -> Result<()> {
        move |config: &ConfigValue| {
            // Check required fields
            for field in &self.required_fields {
                navigate_path(config, field)?;
            }

            // Run custom validators
            for validator in &self.validators {
                validator(config)?;
            }

            Ok(())
        }
    }
}

impl Default for ConfigValidator {
    fn default() -> Self {
        Self::new()
    }
}

// Common validators

/// Validate that a path exists on the filesystem
pub fn validate_path_exists(config: &ConfigValue, path_field: &str) -> Result<()> {
    if let Some(path_str) = config.optional_str(path_field) {
        let path = expand_home_dir(path_str);
        if !Path::new(&path).exists() {
            return Err(CoreError::Configuration(format!(
                "Path at '{}' does not exist: {}",
                path_field, path_str
            )));
        }
    }
    Ok(())
}

/// Validate that a path is a file
pub fn validate_is_file(config: &ConfigValue, path_field: &str) -> Result<()> {
    if let Some(path_str) = config.optional_str(path_field) {
        let path = expand_home_dir(path_str);
        let path_buf = Path::new(&path);
        if !path_buf.exists() {
            return Err(CoreError::Configuration(format!(
                "Path at '{}' does not exist: {}",
                path_field, path_str
            )));
        }
        if !path_buf.is_file() {
            return Err(CoreError::Configuration(format!(
                "Path at '{}' is not a file: {}",
                path_field, path_str
            )));
        }
    }
    Ok(())
}

/// Validate that a path is a directory
pub fn validate_is_dir(config: &ConfigValue, path_field: &str) -> Result<()> {
    if let Some(path_str) = config.optional_str(path_field) {
        let path = expand_home_dir(path_str);
        let path_buf = Path::new(&path);
        if !path_buf.exists() {
            return Err(CoreError::Configuration(format!(
                "Path at '{}' does not exist: {}",
                path_field, path_str
            )));
        }
        if !path_buf.is_dir() {
            return Err(CoreError::Configuration(format!(
                "Path at '{}' is not a directory: {}",
                path_field, path_str
            )));
        }
    }
    Ok(())
}

/// Validate that a string is a valid URL
pub fn validate_url(config: &ConfigValue, url_field: &str) -> Result<()> {
    if let Some(url_str) = config.optional_str(url_field) {
        // Basic URL validation - check for protocol and valid characters
        let url_regex = Regex::new(r"^(https?|ftp|postgresql)://[^\s/$.?#].[^\s]*$").unwrap();
        if !url_regex.is_match(url_str) {
            return Err(CoreError::Configuration(format!(
                "Invalid URL at '{}': {}",
                url_field, url_str
            )));
        }
    }
    Ok(())
}

/// Validate that a port number is in valid range
pub fn validate_port(config: &ConfigValue, port_field: &str) -> Result<()> {
    if let Some(port) = config.optional_i64(port_field) {
        if !(1..=65535).contains(&port) {
            return Err(CoreError::Configuration(format!(
                "Port at '{}' must be between 1 and 65535, got {}",
                port_field, port
            )));
        }
    }
    Ok(())
}

/// Parse a duration string (e.g., "30s", "5m", "1h") into seconds
pub fn parse_duration(duration_str: &str) -> Result<u64> {
    let duration_regex = Regex::new(r"^(\d+)(s|m|h|d)$").unwrap();

    if let Some(captures) = duration_regex.captures(duration_str) {
        let value: u64 = captures[1].parse().map_err(|_| {
            CoreError::Configuration(format!("Invalid duration value: {}", duration_str))
        })?;

        let multiplier = match &captures[2] {
            "s" => 1,
            "m" => 60,
            "h" => 3600,
            "d" => 86400,
            _ => unreachable!(),
        };

        Ok(value * multiplier)
    } else {
        Err(CoreError::Configuration(format!(
            "Invalid duration format: '{}'. Use format like '30s', '5m', '1h', or '2d'",
            duration_str
        )))
    }
}

/// Validate and parse a duration field
pub fn validate_duration(config: &ConfigValue, duration_field: &str) -> Result<()> {
    if let Some(duration_str) = config.optional_str(duration_field) {
        parse_duration(duration_str)?;
    }
    Ok(())
}

/// Create a validation function for an email address
pub fn email_validator() -> Regex {
    Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap()
}

/// Expand ~ to home directory in paths
fn expand_home_dir(path: &str) -> String {
    if path.starts_with("~/") {
        if let Some(home) = dirs::home_dir() {
            return path.replacen("~", &home.to_string_lossy(), 1);
        }
    }
    path.to_string()
}

/// Helper to extract configuration into a HashMap for easier access
pub fn flatten_config(config: &ConfigValue) -> HashMap<String, String> {
    let mut result = HashMap::new();
    flatten_config_recursive(config, String::new(), &mut result);
    result
}

fn flatten_config_recursive(
    config: &ConfigValue,
    prefix: String,
    result: &mut HashMap<String, String>,
) {
    match config {
        ConfigValue::Table(table) => {
            for (key, value) in table {
                let new_prefix = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_config_recursive(value, new_prefix, result);
            }
        }
        ConfigValue::String(s) => {
            result.insert(prefix, s.clone());
        }
        ConfigValue::Integer(i) => {
            result.insert(prefix, i.to_string());
        }
        ConfigValue::Float(f) => {
            result.insert(prefix, f.to_string());
        }
        ConfigValue::Boolean(b) => {
            result.insert(prefix, b.to_string());
        }
        ConfigValue::Array(arr) => {
            // Store array as comma-separated values for simple cases
            let values: Vec<String> = arr
                .iter()
                .filter_map(|v| match v {
                    ConfigValue::String(s) => Some(s.clone()),
                    ConfigValue::Integer(i) => Some(i.to_string()),
                    ConfigValue::Float(f) => Some(f.to_string()),
                    ConfigValue::Boolean(b) => Some(b.to_string()),
                    _ => None,
                })
                .collect();
            if !values.is_empty() {
                result.insert(prefix, values.join(","));
            }
        }
        _ => {} // Skip other types
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use toml;

    #[test]
    fn test_config_extractor_basic() {
        let config_str = r#"
            name = "test"
            port = 8080
            enabled = true
            
            [database]
            url = "postgresql://localhost/test"
            pool_size = 10
        "#;

        let config: ConfigValue = toml::from_str(config_str).unwrap();

        // Test required values
        assert_eq!(config.require_str("name").unwrap(), "test");
        assert_eq!(config.require_i64("port").unwrap(), 8080);
        assert_eq!(config.require_u64("port").unwrap(), 8080);
        assert!(config.require_bool("enabled").unwrap());

        // Test nested access
        assert_eq!(
            config.require_str("database.url").unwrap(),
            "postgresql://localhost/test"
        );
        assert_eq!(config.require_i64("database.pool_size").unwrap(), 10);
    }

    #[test]
    fn test_config_extractor_optional() {
        let config: ConfigValue = toml::from_str("name = \"test\"").unwrap();

        assert_eq!(config.optional_str("name"), Some("test"));
        assert_eq!(config.optional_str("missing"), None);
        assert_eq!(config.str_or("missing", "default"), "default");
    }

    #[test]
    fn test_config_validator() {
        let validator = ConfigValidator::new()
            .require("database.url")
            .validate_range("port", 1..=65535)
            .validate_regex("email", r"^[^@]+@[^@]+\.[^@]+$")
            .build();

        let valid_config: ConfigValue = toml::from_str(
            r#"
            port = 8080
            email = "test@example.com"
            [database]
            url = "postgresql://localhost/test"
        "#,
        )
        .unwrap();

        assert!(validator(&valid_config).is_ok());

        let invalid_config: ConfigValue = toml::from_str(
            r#"
            port = 70000
            email = "invalid-email"
        "#,
        )
        .unwrap();

        assert!(validator(&invalid_config).is_err());
    }

    #[test]
    fn test_duration_parsing() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
        assert_eq!(parse_duration("5m").unwrap(), 300);
        assert_eq!(parse_duration("2h").unwrap(), 7200);
        assert_eq!(parse_duration("1d").unwrap(), 86400);

        assert!(parse_duration("invalid").is_err());
        assert!(parse_duration("30x").is_err());
    }

    #[test]
    fn test_flatten_config() {
        let config: ConfigValue = toml::from_str(
            r#"
            name = "test"
            port = 8080
            
            [database]
            url = "postgresql://localhost/test"
            
            [server]
            hosts = ["localhost", "127.0.0.1"]
        "#,
        )
        .unwrap();

        let flat = flatten_config(&config);

        assert_eq!(flat.get("name"), Some(&"test".to_string()));
        assert_eq!(flat.get("port"), Some(&"8080".to_string()));
        assert_eq!(
            flat.get("database.url"),
            Some(&"postgresql://localhost/test".to_string())
        );
        assert_eq!(
            flat.get("server.hosts"),
            Some(&"localhost,127.0.0.1".to_string())
        );
    }

    #[test]
    fn test_config_validator_path_validation() {
        let validator = ConfigValidator::new()
            .validate_path_format("db_path")
            .validate_absolute_path("socket_path")
            .build();

        // Valid paths
        let valid_config: ConfigValue = toml::from_str(
            r#"
            db_path = "~/data/test.db"
            socket_path = "/tmp/socket"
        "#,
        )
        .unwrap();
        assert!(validator(&valid_config).is_ok());

        // Invalid relative path for db_path
        let invalid_config: ConfigValue = toml::from_str(
            r#"
            db_path = "data/test.db"
            socket_path = "/tmp/socket"
        "#,
        )
        .unwrap();
        assert!(validator(&invalid_config).is_err());

        // Invalid relative path for socket_path
        let invalid_config2: ConfigValue = toml::from_str(
            r#"
            db_path = "~/data/test.db"
            socket_path = "tmp/socket"
        "#,
        )
        .unwrap();
        assert!(validator(&invalid_config2).is_err());
    }

    #[test]
    fn test_config_validator_array_validation() {
        let validator = ConfigValidator::new()
            .validate_path_array("watch_patterns")
            .build();

        // Valid array with good paths
        let valid_config: ConfigValue = toml::from_str(
            r#"
            watch_patterns = ["/home/user/docs", "/home/user/Downloads"]
        "#,
        )
        .unwrap();
        assert!(validator(&valid_config).is_ok());

        // Invalid array with bad path
        let invalid_config: ConfigValue = toml::from_str(
            r#"
            watch_patterns = ["/home/user/docs", "relative/path"]
        "#,
        )
        .unwrap();
        assert!(validator(&invalid_config).is_err());

        // Invalid array with non-string element
        let invalid_config2: ConfigValue = toml::from_str(
            r#"
            watch_patterns = ["/home/user/docs", 123]
        "#,
        )
        .unwrap();
        assert!(validator(&invalid_config2).is_err());
    }

    #[test]
    fn test_config_validator_positive_validation() {
        let validator = ConfigValidator::new().validate_positive("interval").build();

        // Valid positive number
        let valid_config: ConfigValue = toml::from_str(
            r#"
            interval = 10
        "#,
        )
        .unwrap();
        assert!(validator(&valid_config).is_ok());

        // Invalid zero
        let invalid_config: ConfigValue = toml::from_str(
            r#"
            interval = 0
        "#,
        )
        .unwrap();
        assert!(validator(&invalid_config).is_err());

        // Invalid negative
        let invalid_config2: ConfigValue = toml::from_str(
            r#"
            interval = -5
        "#,
        )
        .unwrap();
        assert!(validator(&invalid_config2).is_err());
    }

    #[test]
    fn test_config_validator_chaining() {
        let validator = ConfigValidator::new()
            .validate_range("port", 1..=65535)
            .validate_positive("timeout")
            .validate_path_format("config_file")
            .build();

        // All valid
        let valid_config: ConfigValue = toml::from_str(
            r#"
            port = 8080
            timeout = 30
            config_file = "~/app.conf"
        "#,
        )
        .unwrap();
        assert!(validator(&valid_config).is_ok());

        // Invalid port
        let invalid_config: ConfigValue = toml::from_str(
            r#"
            port = 70000
            timeout = 30
            config_file = "~/app.conf"
        "#,
        )
        .unwrap();
        assert!(validator(&invalid_config).is_err());
    }
}
