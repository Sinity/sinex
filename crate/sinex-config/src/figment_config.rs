//! Modern configuration management using Figment
//!
//! This module provides a unified configuration layer using Figment, which supports:
//! - Multiple configuration sources (files, env vars, defaults)
//! - Type-safe extraction with validation
//! - Configuration merging and overrides
//! - Error tracking with clear provenance

use figment::{
    providers::{Env, Format, Json, Toml, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use validator::Validate;

/// Common configuration errors
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Configuration extraction failed: {0}")]
    Extraction(#[from] figment::Error),

    #[error("Configuration validation failed: {0}")]
    Validation(String),

    #[error("Configuration file not found: {0}")]
    FileNotFound(String),
}

/// Base configuration trait that all configs must implement
pub trait BaseConfig: Serialize + for<'de> Deserialize<'de> + Validate {
    /// Get the configuration prefix for environment variables
    fn env_prefix() -> &'static str {
        "SINEX"
    }

    /// Get default configuration file paths to search
    fn default_config_paths() -> Vec<&'static str> {
        vec!["sinex.toml", "config.toml", "/etc/sinex/config.toml"]
    }
}

/// Load configuration from multiple sources using Figment
pub fn load_config<T: BaseConfig>(
    config_path: Option<&Path>,
    env_prefix: Option<&str>,
) -> Result<T, ConfigError> {
    let mut figment = Figment::new();

    // Start with defaults (if T implements Default)
    // Note: This would require T: Default constraint, which we can't express here
    // Users should use ConfigBuilder for defaults instead

    // Load from config files
    if let Some(path) = config_path {
        if !path.exists() {
            return Err(ConfigError::FileNotFound(path.display().to_string()));
        }

        match path.extension().and_then(|s| s.to_str()) {
            Some("toml") => figment = figment.merge(Toml::file(path)),
            Some("yaml") | Some("yml") => figment = figment.merge(Yaml::file(path)),
            Some("json") => figment = figment.merge(Json::file(path)),
            _ => {
                return Err(ConfigError::FileNotFound(format!(
                    "Unsupported config file type: {}",
                    path.display()
                )))
            }
        }
    } else {
        // Try default paths
        for default_path in T::default_config_paths() {
            let path = Path::new(default_path);
            if path.exists() {
                match path.extension().and_then(|s| s.to_str()) {
                    Some("toml") => figment = figment.merge(Toml::file(path)),
                    Some("yaml") | Some("yml") => figment = figment.merge(Yaml::file(path)),
                    Some("json") => figment = figment.merge(Json::file(path)),
                    _ => continue,
                }
                break;
            }
        }
    }

    // Override with environment variables
    let prefix = env_prefix.unwrap_or_else(|| T::env_prefix());
    figment = figment.merge(Env::prefixed(prefix).split("_"));

    // Extract and validate
    let config: T = figment.extract()?;
    config
        .validate()
        .map_err(|e| ConfigError::Validation(e.to_string()))?;

    Ok(config)
}

/// Macro to implement BaseConfig for a type
#[macro_export]
macro_rules! impl_base_config {
    ($type:ty) => {
        impl $crate::figment_config::BaseConfig for $type {}
    };

    ($type:ty, prefix = $prefix:expr) => {
        impl $crate::figment_config::BaseConfig for $type {
            fn env_prefix() -> &'static str {
                $prefix
            }
        }
    };

    ($type:ty, paths = [$($path:expr),+ $(,)?]) => {
        impl $crate::figment_config::BaseConfig for $type {
            fn default_config_paths() -> Vec<&'static str> {
                vec![$($path),+]
            }
        }
    };

    ($type:ty, prefix = $prefix:expr, paths = [$($path:expr),+ $(,)?]) => {
        impl $crate::figment_config::BaseConfig for $type {
            fn env_prefix() -> &'static str {
                $prefix
            }

            fn default_config_paths() -> Vec<&'static str> {
                vec![$($path),+]
            }
        }
    };
}

/// Helper to merge configurations from multiple sources
pub struct ConfigBuilder {
    figment: Figment,
}

impl ConfigBuilder {
    /// Create a new configuration builder
    pub fn new() -> Self {
        Self {
            figment: Figment::new(),
        }
    }

    /// Add defaults from a serializable value
    pub fn with_defaults<T: Serialize>(mut self, defaults: T) -> Result<Self, ConfigError> {
        let json_string =
            serde_json::to_string(&defaults).map_err(|e| ConfigError::Validation(e.to_string()))?;
        self.figment = self.figment.merge(Json::string(&json_string));
        Ok(self)
    }

    /// Add configuration from a TOML file
    pub fn with_toml_file(mut self, path: impl AsRef<Path>) -> Self {
        self.figment = self.figment.merge(Toml::file(path));
        self
    }

    /// Add configuration from a YAML file
    pub fn with_yaml_file(mut self, path: impl AsRef<Path>) -> Self {
        self.figment = self.figment.merge(Yaml::file(path));
        self
    }

    /// Add configuration from a JSON file
    pub fn with_json_file(mut self, path: impl AsRef<Path>) -> Self {
        self.figment = self.figment.merge(Json::file(path));
        self
    }

    /// Add configuration from environment variables
    pub fn with_env(mut self, prefix: &str) -> Self {
        self.figment = self.figment.merge(Env::prefixed(prefix).split("_"));
        self
    }

    /// Extract and validate the final configuration
    pub fn extract<T: BaseConfig>(self) -> Result<T, ConfigError> {
        let config: T = self.figment.extract()?;
        config
            .validate()
            .map_err(|e| ConfigError::Validation(e.to_string()))?;
        Ok(config)
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[derive(Debug, Clone, Serialize, Deserialize, Validate)]
    struct TestConfig {
        #[validate(length(min = 1))]
        name: String,
        #[validate(range(min = 1, max = 100))]
        count: u32,
        enabled: bool,
    }

    impl Default for TestConfig {
        fn default() -> Self {
            Self {
                name: "default".to_string(),
                count: 10,
                enabled: true,
            }
        }
    }

    impl_base_config!(TestConfig, prefix = "TEST");

    #[test]
    fn test_load_from_defaults() {
        let config: TestConfig = load_config(None, None).unwrap();
        assert_eq!(config.name, "default");
        assert_eq!(config.count, 10);
        assert!(config.enabled);
    }

    #[test]
    fn test_load_from_toml() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
name = "from_toml"
count = 42
enabled = false
        "#
        )
        .unwrap();

        let config: TestConfig = load_config(Some(file.path()), None).unwrap();
        assert_eq!(config.name, "from_toml");
        assert_eq!(config.count, 42);
        assert!(!config.enabled);
    }

    #[test]
    fn test_validation_error() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
name = ""
count = 200
enabled = true
        "#
        )
        .unwrap();

        let result: Result<TestConfig, _> = load_config(Some(file.path()), None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("validation"));
    }

    #[test]
    fn test_config_builder() {
        let config = ConfigBuilder::new()
            .with_defaults(TestConfig {
                name: "builder".to_string(),
                count: 5,
                enabled: false,
            })
            .unwrap()
            .extract::<TestConfig>()
            .unwrap();

        assert_eq!(config.name, "builder");
        assert_eq!(config.count, 5);
        assert!(!config.enabled);
    }
}
