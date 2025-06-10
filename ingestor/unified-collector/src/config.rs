use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedConfig {
    /// List of enabled event types
    pub enabled_events: Vec<String>,
    
    /// Event-specific configuration
    #[serde(default)]
    pub event: HashMap<String, toml::Value>,
    
    /// Direct event config (e.g., event.file_created)
    #[serde(flatten)]
    pub flat_config: HashMap<String, toml::Value>,
}

impl UnifiedConfig {
    pub fn load() -> Result<Self> {
        // Try standard locations
        let paths = vec![
            PathBuf::from("unified-collector.toml"),
            PathBuf::from("~/.config/sinex/unified-collector.toml"),
            PathBuf::from("/etc/sinex/unified-collector.toml"),
        ];
        
        for path in paths {
            if path.exists() {
                return Self::load_from_file(&path);
            }
        }
        
        // Default config
        Ok(Self {
            enabled_events: vec![
                "file.created".to_string(),
                "file.modified".to_string(),
                "command.executed".to_string(),
            ],
            event: HashMap::new(),
            flat_config: HashMap::new(),
        })
    }
    
    pub fn load_from_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }
    
    /// Get configuration for a specific event, with hierarchical merging
    pub fn get_event_config(&self, event_name: &str) -> toml::Value {
        let mut config = toml::Value::Table(toml::map::Map::new());
        
        // First, check for category config (e.g., event.files for file.*)
        if let Some(category) = event_name.split('.').next() {
            let category_key = format!("event.{}s", category); // files, commands, etc.
            if let Some(cat_config) = self.flat_config.get(&category_key) {
                merge_toml(&mut config, cat_config.clone());
            }
        }
        
        // Then apply specific event config
        let event_key = format!("event.{}", event_name.replace('.', "_"));
        if let Some(event_config) = self.flat_config.get(&event_key) {
            merge_toml(&mut config, event_config.clone());
        }
        
        // Also check the event map
        if let Some(event_config) = self.event.get(event_name) {
            merge_toml(&mut config, event_config.clone());
        }
        
        config
    }
}

/// Merge two TOML values, with `update` overriding values in `base`
fn merge_toml(base: &mut toml::Value, update: toml::Value) {
    match (base, update) {
        (toml::Value::Table(base_map), toml::Value::Table(update_map)) => {
            for (k, v) in update_map {
                match base_map.get_mut(&k) {
                    Some(base_val) => merge_toml(base_val, v),
                    None => { base_map.insert(k, v); }
                }
            }
        }
        (base, update) => *base = update,
    }
}