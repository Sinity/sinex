//! Configuration validation using the validator crate
//!
//! This module provides reusable validation components for configuration structs.
//! Includes secure path deserialization that validates all path fields during config loading.

use crate::domain::SanitizedPath;
use crate::units::Seconds;
use crate::validation::validate_path;
use camino::Utf8PathBuf;
use serde::{Deserialize, Deserializer, Serialize, de};
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

    #[validate(custom(function = "validate_timeout_secs"))]
    pub timeout_secs: Seconds,
}

fn validate_timeout_secs(value: &Seconds) -> Result<(), ValidationError> {
    let secs = value.as_secs();
    if (1..=300).contains(&secs) {
        Ok(())
    } else {
        Err(ValidationError::new("timeout_secs_out_of_range"))
    }
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
    crate::validation::validate_path(path)
        .map_err(|_| ValidationError::new("invalid_directory_path"))?;

    Ok(())
}

/// Custom validator for file paths
pub fn validate_file_path(path: &str) -> Result<(), ValidationError> {
    if path.is_empty() {
        return Err(ValidationError::new("empty_path"));
    }

    // Use our existing path validation
    let _path_buf = crate::validation::validate_path(path)
        .map_err(|_| ValidationError::new("invalid_file_path"))?;

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

    #[validate(custom(function = "validate_token_expiry_secs"))]
    pub token_expiry_secs: Seconds,

    #[validate(range(min = 1, max = 100))]
    pub max_login_attempts: u32,
}

fn validate_token_expiry_secs(value: &Seconds) -> Result<(), ValidationError> {
    let secs = value.as_secs();
    if (60..=86_400).contains(&secs) {
        Ok(())
    } else {
        Err(ValidationError::new("token_expiry_secs_out_of_range"))
    }
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
    /// Create a new `SecurePath` with validation
    pub fn new(path: &str, level: PathValidationLevel) -> Result<Self, String> {
        let sanitized = match level {
            PathValidationLevel::Basic => {
                // Just use SanitizedPath's basic validation
                SanitizedPath::from_str_validated(path)?
            }
            PathValidationLevel::Strict => {
                // Use our comprehensive path validation
                let validated_path = validate_path(path).map_err(|e| e.to_string())?;
                SanitizedPath::new(validated_path.to_string())
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

    /// Get the inner `SanitizedPath`
    #[must_use]
    pub fn inner(&self) -> &SanitizedPath {
        &self.inner
    }

    /// Get the validation level used
    #[must_use]
    pub fn validation_level(&self) -> PathValidationLevel {
        self.validation_level
    }

    /// Convert to `Utf8PathBuf`
    #[must_use]
    pub fn to_path_buf(&self) -> Utf8PathBuf {
        Utf8PathBuf::from(self.inner.as_str())
    }

    /// Get the path as a string
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }
}

/// Helper for deserializing `SanitizedPath` with validation
pub fn deserialize_sanitized_path<'de, D>(deserializer: D) -> Result<SanitizedPath, D::Error>
where
    D: Deserializer<'de>,
{
    let path_str = String::deserialize(deserializer)?;
    SanitizedPath::from_str_validated(&path_str)
        .map_err(|e| de::Error::custom(format!("Invalid path '{path_str}': {e}")))
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
                .map_err(|e| de::Error::custom(format!("Invalid path '{path_str}': {e}")))?;
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
            de::Error::custom(format!("Invalid path at index {i}: '{path_str}' - {e}"))
        })?;
        sanitized_paths.push(sanitized);
    }

    Ok(sanitized_paths)
}

/// Helper for deserializing `Utf8PathBuf` with validation
pub fn deserialize_validated_utf8_path<'de, D>(deserializer: D) -> Result<Utf8PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let path_str = String::deserialize(deserializer)?;
    let validated_path = validate_path(&path_str)
        .map_err(|e| de::Error::custom(format!("Invalid path '{path_str}': {e}")))?;
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
                .map_err(|e| de::Error::custom(format!("Invalid path '{path_str}': {e}")))?;
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
            de::Error::custom(format!("Invalid path at index {i}: '{path_str}' - {e}"))
        })?;
        validated_paths.push(validated_path);
    }

    Ok(validated_paths)
}
