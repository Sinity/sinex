use anyhow::Result;
use serde::{Deserialize, Serialize};
use sinex_shared::ingestor_framework::IngestorConfig;
use std::path::PathBuf;

/// Configuration for the unified collector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedConfig {
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
    pub collection: CollectionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub connection_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig {
    pub enabled_sources: Vec<String>,
    pub poll_interval_secs: u64,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub heartbeat_interval_secs: u64,
}

impl Default for UnifiedConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig {
                url: "postgresql://localhost/sinex".to_string(),
                max_connections: 10,
                connection_timeout_secs: 10,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "pretty".to_string(),
            },
            collection: CollectionConfig {
                enabled_sources: vec![
                    "system".to_string(),
                    "network".to_string(),
                    "process".to_string(),
                ],
                poll_interval_secs: 5,
                batch_size: 100,
                batch_timeout_ms: 1000,
                heartbeat_interval_secs: 60,
            },
        }
    }
}

impl IngestorConfig for UnifiedConfig {
    fn load() -> Result<Self> {
        // Try to load from standard config locations
        let config_paths = vec![
            PathBuf::from("/etc/sinex/unified.toml"),
            PathBuf::from("/home/sinex/.config/sinex/unified.toml"),
            PathBuf::from("./config/unified.toml"),
        ];
        
        for path in config_paths {
            if path.exists() {
                return Self::load_from_file(&path);
            }
        }
        
        // Return default if no config found
        Ok(Self::default())
    }
    
    fn load_from_file(path: &std::path::Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }
    
    fn database_url(&self) -> &str {
        &self.database.url
    }
    
    fn set_database_url(&mut self, url: String) {
        self.database.url = url;
    }
    
    fn database_max_connections(&self) -> u32 {
        self.database.max_connections
    }
    
    fn database_connection_timeout_secs(&self) -> u64 {
        self.database.connection_timeout_secs
    }
    
    fn log_level(&self) -> &str {
        &self.logging.level
    }
    
    fn set_log_level(&mut self, level: String) {
        self.logging.level = level;
    }
}