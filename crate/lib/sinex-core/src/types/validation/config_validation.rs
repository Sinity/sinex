//! Configuration validation using the validator crate
//!
//! This module provides reusable validation components for configuration structs.
//! Includes secure path deserialization that validates all path fields during config loading.

use crate::types::domain::SanitizedPath;
use crate::types::validation::validate_path;
use camino::Utf8PathBuf;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize,
};
use std::fmt;
use validator::{Validate, ValidationError};

/// Common configuration validation traits
pub trait ConfigValidation: Validate {
    /// Validate and return formatted error messages
    fn validate_config(&self) -> Result<(), String> {
        self.validate()
            .map_err(|e| crate::validation::validation_chains::format_validation_errors(&e))
    }
}

/// Implement for all types that implement Validate
impl<T: Validate> ConfigValidation for T {}

/// Database connection configuration with validation
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct DatabaseConfig {
    #[validate(url)]
    pub url: String,

    #[validate(range(min = 1, max = 1000))]
    pub max_connections: u32,

    #[validate(range(min = 0, max = 100))]
    pub min_connections: u32,

    #[validate(range(min = 1, max = 300))]
    pub timeout_secs: u64,
}

/// Server configuration with validation
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ServerConfig {
    #[validate(length(min = 1))]
    pub name: String,

    #[validate(ip)]
    pub bind_address: String,

    #[validate(range(min = 1, max = 65535))]
    pub port: u16,

    #[validate(range(min = 1, max = 10000))]
    pub worker_threads: usize,
}

/// Path configuration with custom validation
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct PathConfig {
    #[validate(custom(function = "validate_directory_path"))]
    pub data_dir: String,

    #[validate(custom(function = "validate_directory_path"))]
    pub log_dir: String,

    #[validate(custom(function = "validate_file_path"))]
    pub config_file: Option<String>,
}

/// Custom validator for directory paths
pub fn validate_directory_path(path: &str) -> Result<(), ValidationError> {
    if path.is_empty() {
        return Err(ValidationError::new("empty_path"));
    }

    // Use our existing path validation
    crate::validate_path(path).map_err(|_| ValidationError::new("invalid_directory_path"))?;

    Ok(())
}

/// Custom validator for file paths
pub fn validate_file_path(path: &str) -> Result<(), ValidationError> {
    if path.is_empty() {
        return Err(ValidationError::new("empty_path"));
    }

    // Use our existing path validation
    let _path_buf =
        crate::validate_path(path).map_err(|_| ValidationError::new("invalid_file_path"))?;

    // Check it's not a directory indicator
    if path.ends_with('/') || path.ends_with('\\') {
        return Err(ValidationError::new("path_is_directory"));
    }

    Ok(())
}

/// Redis configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct RedisConfig {
    #[validate(url)]
    pub url: String,

    #[validate(range(min = 1, max = 100))]
    pub pool_size: u32,

    #[validate(range(min = 100, max = 60000))]
    pub timeout_ms: u64,
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct SecurityConfig {
    #[validate(length(min = 32, max = 512))]
    pub secret_key: String,

    #[validate(range(min = 60, max = 86400))]
    pub token_expiry_secs: u64,

    #[validate(range(min = 1, max = 100))]
    pub max_login_attempts: u32,
}

/// Path validation levels for different security contexts
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathValidationLevel {
    /// Basic validation - just check for null bytes and basic traversal
    Basic,
    /// Strict validation - comprehensive security checks including canonicalization
    Strict,
    /// Require absolute paths only
    AbsoluteOnly,
    /// Require relative paths only
    RelativeOnly,
}

/// A path wrapper that enforces validation during deserialization
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurePath {
    inner: SanitizedPath,
    validation_level: PathValidationLevel,
}

impl SecurePath {
    /// Create a new SecurePath with validation
    pub fn new(path: &str, level: PathValidationLevel) -> Result<Self, String> {
        let sanitized = match level {
            PathValidationLevel::Basic => {
                // Just use SanitizedPath's basic validation
                SanitizedPath::from_str_validated(path)?
            }
            PathValidationLevel::Strict => {
                // Use our comprehensive path validation
                let validated_path = validate_path(path).map_err(|e| e.to_string())?;
                SanitizedPath::new_unchecked(validated_path.to_string())
            }
            PathValidationLevel::AbsoluteOnly => {
                let sanitized = SanitizedPath::from_str_validated(path)?;
                let path_buf = Utf8PathBuf::from(sanitized.as_str());
                if !path_buf.is_absolute() {
                    return Err("Path must be absolute".to_string());
                }
                sanitized
            }
            PathValidationLevel::RelativeOnly => {
                let sanitized = SanitizedPath::from_str_validated(path)?;
                let path_buf = Utf8PathBuf::from(sanitized.as_str());
                if path_buf.is_absolute() {
                    return Err("Path must be relative".to_string());
                }
                sanitized
            }
        };

        Ok(Self {
            inner: sanitized,
            validation_level: level,
        })
    }

    /// Get the inner SanitizedPath
    pub fn inner(&self) -> &SanitizedPath {
        &self.inner
    }

    /// Get the validation level used
    pub fn validation_level(&self) -> PathValidationLevel {
        self.validation_level
    }

    /// Convert to Utf8PathBuf
    pub fn to_path_buf(&self) -> Utf8PathBuf {
        Utf8PathBuf::from(self.inner.as_str())
    }

    /// Get the path as a string
    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }
}

/// Custom deserializer for paths that validates during deserialization
pub struct ValidatedPathDeserializer {
    level: PathValidationLevel,
}

impl ValidatedPathDeserializer {
    pub fn new(level: PathValidationLevel) -> Self {
        Self { level }
    }
}

struct PathVisitor {
    level: PathValidationLevel,
}

impl<'de> Visitor<'de> for PathVisitor {
    type Value = SecurePath;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid file path")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        SecurePath::new(value, self.level)
            .map_err(|e| E::custom(format!("Invalid path '{}': {}", value, e)))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&value)
    }
}

impl<'de> Deserializer<'de> for ValidatedPathDeserializer {
    type Error = serde::de::value::Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(de::Error::custom(
            "ValidatedPathDeserializer can only deserialize strings",
        ))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char bytes
        byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // This won't be called directly, but we need to implement it
        Err(de::Error::custom("Use deserialize_string instead"))
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // This won't be called directly either, but we implement for completeness
        Err(de::Error::custom(
            "ValidatedPathDeserializer should be used with serde_with",
        ))
    }
}

/// Convenience function to create a validated path deserializer
pub fn validated_path_deserializer(level: PathValidationLevel) -> ValidatedPathDeserializer {
    ValidatedPathDeserializer::new(level)
}

/// Helper for deserializing SanitizedPath with validation
pub fn deserialize_sanitized_path<'de, D>(deserializer: D) -> Result<SanitizedPath, D::Error>
where
    D: Deserializer<'de>,
{
    let path_str = String::deserialize(deserializer)?;
    SanitizedPath::from_str_validated(&path_str)
        .map_err(|e| de::Error::custom(format!("Invalid path '{}': {}", path_str, e)))
}

/// Helper for deserializing Optional<SanitizedPath> with validation
pub fn deserialize_optional_sanitized_path<'de, D>(
    deserializer: D,
) -> Result<Option<SanitizedPath>, D::Error>
where
    D: Deserializer<'de>,
{
    let path_opt: Option<String> = Option::deserialize(deserializer)?;
    match path_opt {
        Some(path_str) => {
            let sanitized = SanitizedPath::from_str_validated(&path_str)
                .map_err(|e| de::Error::custom(format!("Invalid path '{}': {}", path_str, e)))?;
            Ok(Some(sanitized))
        }
        None => Ok(None),
    }
}

/// Helper for deserializing Vec<SanitizedPath> with validation
pub fn deserialize_sanitized_path_vec<'de, D>(
    deserializer: D,
) -> Result<Vec<SanitizedPath>, D::Error>
where
    D: Deserializer<'de>,
{
    let path_strings: Vec<String> = Vec::deserialize(deserializer)?;
    let mut sanitized_paths = Vec::new();

    for (i, path_str) in path_strings.into_iter().enumerate() {
        let sanitized = SanitizedPath::from_str_validated(&path_str).map_err(|e| {
            de::Error::custom(format!(
                "Invalid path at index {}: '{}' - {}",
                i, path_str, e
            ))
        })?;
        sanitized_paths.push(sanitized);
    }

    Ok(sanitized_paths)
}

/// Helper for deserializing Utf8PathBuf with validation
pub fn deserialize_validated_utf8_path<'de, D>(deserializer: D) -> Result<Utf8PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let path_str = String::deserialize(deserializer)?;
    let validated_path = validate_path(&path_str)
        .map_err(|e| de::Error::custom(format!("Invalid path '{}': {}", path_str, e)))?;
    Ok(validated_path)
}

/// Helper for deserializing Optional<Utf8PathBuf> with validation
pub fn deserialize_optional_validated_utf8_path<'de, D>(
    deserializer: D,
) -> Result<Option<Utf8PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let path_opt: Option<String> = Option::deserialize(deserializer)?;
    match path_opt {
        Some(path_str) => {
            let validated_path = validate_path(&path_str)
                .map_err(|e| de::Error::custom(format!("Invalid path '{}': {}", path_str, e)))?;
            Ok(Some(validated_path))
        }
        None => Ok(None),
    }
}

/// Helper for deserializing Vec<Utf8PathBuf> with validation  
pub fn deserialize_validated_utf8_path_vec<'de, D>(
    deserializer: D,
) -> Result<Vec<Utf8PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let path_strings: Vec<String> = Vec::deserialize(deserializer)?;
    let mut validated_paths = Vec::new();

    for (i, path_str) in path_strings.into_iter().enumerate() {
        let validated_path = validate_path(&path_str).map_err(|e| {
            de::Error::custom(format!(
                "Invalid path at index {}: '{}' - {}",
                i, path_str, e
            ))
        })?;
        validated_paths.push(validated_path);
    }

    Ok(validated_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::{sinex_test, TestContext};

    use color_eyre::eyre::Result;

    use serde_json::json;

    #[sinex_test]
    async fn test_database_config_validation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let valid = DatabaseConfig {
            url: "postgresql://localhost/test".to_string(),
            max_connections: 50,
            min_connections: 5,
            timeout_secs: 30,
        };
        assert!(valid.validate().is_ok());

        let invalid = DatabaseConfig {
            url: "not-a-url".to_string(),
            max_connections: 50,
            min_connections: 5,
            timeout_secs: 30,
        };
        assert!(invalid.validate().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_server_config_validation(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let valid = ServerConfig {
            name: "test-server".to_string(),
            bind_address: "127.0.0.1".to_string(),
            port: 8080,
            worker_threads: 4,
        };
        assert!(valid.validate().is_ok());

        let invalid = ServerConfig {
            name: "".to_string(), // Empty name
            bind_address: "127.0.0.1".to_string(),
            port: 8080,
            worker_threads: 4,
        };
        assert!(invalid.validate().is_err());
        Ok(())
    }

    #[sinex_test]
    async fn test_config_validation_trait(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let config = ServerConfig {
            name: "test".to_string(),
            bind_address: "not-an-ip".to_string(),
            port: 8080,
            worker_threads: 4,
        };

        let result = config.validate_config();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("bind_address"));

        Ok(())
    }
}
