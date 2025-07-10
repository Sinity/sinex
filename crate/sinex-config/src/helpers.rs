//! Configuration helper types and utilities

use serde::{Deserialize, Serialize};
use crate::validators::ValidationReport;

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CollectorConfig {
    pub enabled_events: Vec<String>,
    pub annex_repo_path: Option<String>,
    pub database: DatabaseConfig,
}

impl CollectorConfig {
    pub fn get_validation_report(&self) -> ValidationReport {
        ValidationReport::default()
    }
    
    pub fn cross_validate(&self) -> Result<(), String> {
        Ok(())
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

#[derive(Debug, Clone)]
pub struct ConfigExtraction;

#[derive(Debug, Clone)]
pub struct ConfigMerger;