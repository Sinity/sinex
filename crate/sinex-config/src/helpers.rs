//! Configuration helper types and utilities

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub pool_size: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgresql:///sinex_dev?host=/run/postgresql".to_string(),
            pool_size: 25,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub log_level: String,
    pub metrics_enabled: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            metrics_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourcesConfig {
    pub filesystem: bool,
    pub terminal: bool,
    pub desktop: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ConfigFactory;

impl ConfigFactory {
    pub fn new() -> Self {
        Self
    }
}

// ConfigExtraction and ConfigMerger are deprecated
// These were used for file-based configuration merging which is no longer supported
