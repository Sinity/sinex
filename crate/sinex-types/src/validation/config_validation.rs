//! Configuration validation using the validator crate
//!
//! This module provides reusable validation components for configuration structs.

use serde::{Deserialize, Serialize};
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::prelude::*;

    #[sinex_test]
    async fn test_database_config_validation(ctx: TestContext) -> anyhow::Result<()> {
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
    async fn test_server_config_validation(ctx: TestContext) -> anyhow::Result<()> {
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
    async fn test_config_validation_trait(ctx: TestContext) -> anyhow::Result<()> {
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
