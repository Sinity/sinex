//! Service Configuration Management
//!
//! Provides unified configuration management for services including
//! configuration loading, validation, and hot-reload capabilities.

use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;

use crate::{ServiceError, ServiceName, ServiceResult};

/// Configuration source types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConfigSource {
    /// Configuration from environment variables
    Environment,
    /// Configuration from command line arguments
    CommandLine,
    /// Configuration from a remote source
    Remote(String),
    /// In-memory configuration
    Memory,
}

impl fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigSource::Environment => write!(f, "environment"),
            ConfigSource::CommandLine => write!(f, "command-line"),
            ConfigSource::Remote(url) => write!(f, "remote:{}", url),
            ConfigSource::Memory => write!(f, "memory"),
        }
    }
}

/// Configuration entry with source tracking
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    /// Configuration value
    pub value: serde_json::Value,
    /// Source where this configuration came from
    pub source: ConfigSource,
    /// Timestamp when this configuration was loaded
    pub loaded_at: chrono::DateTime<chrono::Utc>,
    /// Whether this configuration can be hot-reloaded
    pub hot_reloadable: bool,
}

impl ConfigEntry {
    /// Create a new configuration entry
    pub fn new(value: serde_json::Value, source: ConfigSource) -> Self {
        Self {
            value,
            source,
            loaded_at: chrono::Utc::now(),
            hot_reloadable: true,
        }
    }

    /// Mark this configuration as not hot-reloadable
    pub fn no_hot_reload(mut self) -> Self {
        self.hot_reloadable = false;
        self
    }
}

/// Service configuration with hierarchical structure and source tracking
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Service name this configuration belongs to
    pub service_name: ServiceName,
    /// Configuration entries organized by key
    pub entries: HashMap<String, ConfigEntry>,
    /// Configuration schema for validation
    pub schema: Option<serde_json::Value>,
}

impl ServiceConfig {
    /// Create a new service configuration
    pub fn new(service_name: impl Into<ServiceName>) -> Self {
        Self {
            service_name: service_name.into(),
            entries: HashMap::new(),
            schema: None,
        }
    }

    /// Set configuration value
    pub fn set(&mut self, key: impl Into<String>, value: serde_json::Value, source: ConfigSource) {
        let entry = ConfigEntry::new(value, source);
        self.entries.insert(key.into(), entry);
    }

    /// Get configuration value
    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.entries.get(key).map(|entry| &entry.value)
    }

    /// Get configuration entry with metadata
    pub fn get_entry(&self, key: &str) -> Option<&ConfigEntry> {
        self.entries.get(key)
    }

    /// Get configuration value with type conversion
    pub fn get_typed<T>(&self, key: &str) -> ServiceResult<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        match self.get(key) {
            Some(value) => {
                let typed_value = serde_json::from_value(value.clone()).map_err(|e| {
                    ServiceError::Configuration(format!(
                        "Failed to parse config key '{}': {}",
                        key, e
                    ))
                })?;
                Ok(Some(typed_value))
            }
            None => Ok(None),
        }
    }

    /// Get required configuration value with type conversion
    pub fn get_required<T>(&self, key: &str) -> ServiceResult<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.get_typed(key)?.ok_or_else(|| {
            ServiceError::Configuration(format!("Required config key '{}' not found", key))
        })
    }

    /// Check if a configuration key exists
    pub fn has(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Remove a configuration key
    pub fn remove(&mut self, key: &str) -> Option<ConfigEntry> {
        self.entries.remove(key)
    }

    /// Get all configuration as a flat map
    pub fn as_map(&self) -> HashMap<String, serde_json::Value> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.value.clone()))
            .collect()
    }

    /// Merge another configuration into this one
    pub fn merge(&mut self, other: ServiceConfig) {
        for (key, entry) in other.entries {
            self.entries.insert(key, entry);
        }
    }

    /// Set configuration schema for validation
    pub fn set_schema(&mut self, schema: serde_json::Value) {
        self.schema = Some(schema);
    }

    /// Validate configuration against schema
    pub fn validate(&self) -> ServiceResult<()> {
        if let Some(_schema) = &self.schema {
            let config_value = serde_json::to_value(self.as_map()).map_err(|e| {
                ServiceError::Configuration(format!("Failed to serialize config: {}", e))
            })?;

            // In practice, you'd use a JSON Schema validation library here
            // For now, we'll just do basic validation
            if !config_value.is_object() {
                return Err(ServiceError::Configuration(
                    "Configuration must be an object".to_string(),
                ));
            }

            // TODO: Implement proper JSON Schema validation using jsonschema crate
        }

        Ok(())
    }

    /// Get configuration keys that can be hot-reloaded
    pub fn hot_reloadable_keys(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter(|(_, entry)| entry.hot_reloadable)
            .map(|(key, _)| key.as_str())
            .collect()
    }

    /// Get configuration sources summary
    pub fn sources_summary(&self) -> HashMap<ConfigSource, usize> {
        let mut summary = HashMap::new();
        for entry in self.entries.values() {
            *summary.entry(entry.source.clone()).or_insert(0) += 1;
        }
        summary
    }
}

/// Configuration manager that handles loading and managing service configurations
pub struct ConfigManager {
    configs: HashMap<ServiceName, ServiceConfig>,
    global_config: ServiceConfig,
}

impl ConfigManager {
    /// Create a new configuration manager
    pub fn new() -> Self {
        Self {
            configs: HashMap::new(),
            global_config: ServiceConfig::new("global"),
        }
    }

    /// Load configuration from environment variables
    pub fn load_from_env(
        &mut self,
        service_name: impl Into<ServiceName>,
        prefix: &str,
    ) -> ServiceResult<()> {
        let service_name = service_name.into();
        let config = self
            .configs
            .entry(service_name.clone())
            .or_insert_with(|| ServiceConfig::new(&service_name));

        for (key, value) in std::env::vars() {
            if key.starts_with(prefix) {
                let config_key = key
                    .strip_prefix(prefix)
                    .unwrap()
                    .trim_start_matches('_')
                    .to_lowercase();

                let json_value = parse_env_value(&value);
                config.set(config_key, json_value, ConfigSource::Environment);
            }
        }

        Ok(())
    }

    /// Set configuration value directly
    pub fn set_config(
        &mut self,
        service_name: impl Into<ServiceName>,
        key: impl Into<String>,
        value: serde_json::Value,
    ) {
        let service_name = service_name.into();
        let config = self
            .configs
            .entry(service_name.clone())
            .or_insert_with(|| ServiceConfig::new(&service_name));
        config.set(key, value, ConfigSource::Memory);
    }

    /// Get service configuration
    pub fn get_config(&self, service_name: &str) -> Option<&ServiceConfig> {
        self.configs.get(service_name)
    }

    /// Get mutable service configuration
    pub fn get_config_mut(&mut self, service_name: &str) -> Option<&mut ServiceConfig> {
        self.configs.get_mut(service_name)
    }

    /// Get global configuration
    pub fn global_config(&self) -> &ServiceConfig {
        &self.global_config
    }

    /// Get mutable global configuration
    pub fn global_config_mut(&mut self) -> &mut ServiceConfig {
        &mut self.global_config
    }

    /// Validate all configurations
    pub fn validate_all(&self) -> ServiceResult<()> {
        for (service_name, config) in &self.configs {
            config.validate().map_err(|e| {
                ServiceError::Configuration(format!(
                    "Validation failed for service {}: {}",
                    service_name, e
                ))
            })?;
        }

        self.global_config.validate().map_err(|e| {
            ServiceError::Configuration(format!("Global config validation failed: {}", e))
        })?;

        Ok(())
    }

    /// Get configuration value with fallback to global config
    pub fn get_with_fallback(&self, service_name: &str, key: &str) -> Option<&serde_json::Value> {
        self.configs
            .get(service_name)
            .and_then(|config| config.get(key))
            .or_else(|| self.global_config.get(key))
    }

    /// Get all service names with configuration
    pub fn service_names(&self) -> Vec<&str> {
        self.configs.keys().map(|s| s.as_str()).collect()
    }

    /// Hot reload configuration for a service
    pub async fn hot_reload(&mut self, service_name: &str) -> ServiceResult<Vec<String>> {
        let config = self.configs.get(service_name).ok_or_else(|| {
            ServiceError::Configuration(format!("Service {} not found", service_name))
        })?;

        let reloaded_keys = Vec::new();

        // Reload environment variables for hot-reloadable entries
        let env_entries: Vec<String> = config
            .entries
            .values()
            .filter_map(|entry| {
                if entry.hot_reloadable {
                    if let ConfigSource::Environment = &entry.source {
                        // For environment sources, we could potentially re-read env vars
                        // but typically environment variables don't change during runtime
                        None
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // Note: Hot reload for environment-only configuration is limited
        // Environment variables typically don't change during service runtime
        Ok(reloaded_keys)
    }
}

impl Default for ConfigManager {
    fn default() -> Self {
        Self::new()
    }
}


/// Parse environment variable value to appropriate JSON type
fn parse_env_value(value: &str) -> serde_json::Value {
    // Try to parse as different types in order of preference

    // Boolean
    if let Ok(bool_val) = value.parse::<bool>() {
        return serde_json::Value::Bool(bool_val);
    }

    // Integer
    if let Ok(int_val) = value.parse::<i64>() {
        return serde_json::Value::Number(int_val.into());
    }

    // Float
    if let Ok(float_val) = value.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(float_val) {
            return serde_json::Value::Number(num);
        }
    }

    // JSON array or object
    if value.starts_with('[') || value.starts_with('{') {
        if let Ok(json_val) = serde_json::from_str(value) {
            return json_val;
        }
    }

    // Default to string
    serde_json::Value::String(value.to_string())
}
