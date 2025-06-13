use anyhow::{Context, Result};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorConfig {
    /// List of enabled event types
    pub enabled_events: Vec<String>,
    
    /// Event-specific configuration
    #[serde(default)]
    pub event: HashMap<String, toml::Value>,
    
    /// Direct event config (e.g., event.file_created)
    #[serde(flatten)]
    pub flat_config: HashMap<String, toml::Value>,
    
    /// Path to git-annex repository for large content storage
    #[serde(default)]
    pub annex_repo_path: Option<String>,
}

impl CollectorConfig {
    pub fn load() -> Result<Self> {
        // Try standard locations
        let paths = vec![
            Some(PathBuf::from("sinex-collector.toml")),
            Some(PathBuf::from("unified-collector.toml")), // Legacy compatibility
            dirs::config_dir().map(|mut p| { p.push("sinex/collector.toml"); p }),
            Some(PathBuf::from("/etc/sinex/collector.toml")),
        ];
        
        for path_opt in paths {
            if let Some(path) = path_opt {
                if path.exists() {
                    return Self::load_from_file(&path);
                }
            }
        }
        
        // Default config
        Ok(Self::default())
    }
    
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        
        // Try TOML first
        if let Ok(config) = toml::from_str::<Self>(&content) {
            return Ok(config);
        }
        
        // Fall back to JSON
        let config: Self = serde_json::from_str(&content)
            .context("Failed to parse config as TOML or JSON")?;
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

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            enabled_events: vec![
                "file.created".to_string(),
                "file.modified".to_string(),
                "file.deleted".to_string(),
                "command.executed".to_string(),
                "shell.command.executed_atuin".to_string(),
                "window.focused".to_string(),
                "workspace.changed".to_string(),
            ],
            event: HashMap::new(),
            flat_config: HashMap::new(),
        }
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

/// Configuration manager with hot-reload support
pub struct ConfigManager {
    config: Arc<RwLock<CollectorConfig>>,
    config_path: Option<PathBuf>,
    update_tx: Option<mpsc::Sender<CollectorConfig>>,
}

impl ConfigManager {
    /// Create a new config manager
    pub fn new(config: CollectorConfig, config_path: Option<PathBuf>) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            config_path,
            update_tx: None,
        }
    }
    
    /// Get the current configuration
    pub async fn get_config(&self) -> CollectorConfig {
        self.config.read().await.clone()
    }
    
    /// Start watching for configuration changes
    pub async fn start_watching(&mut self) -> Result<mpsc::Receiver<CollectorConfig>> {
        let (update_tx, update_rx) = mpsc::channel(10);
        
        if let Some(config_path) = &self.config_path {
            let path = config_path.clone();
            let config = Arc::clone(&self.config);
            let tx = update_tx.clone();
            
            tokio::spawn(async move {
                if let Err(e) = Self::watch_config_file(path, config, tx).await {
                    error!("Config watching failed: {}", e);
                }
            });
        }
        
        self.update_tx = Some(update_tx);
        Ok(update_rx)
    }
    
    async fn watch_config_file(
        config_path: PathBuf,
        config: Arc<RwLock<CollectorConfig>>,
        update_tx: mpsc::Sender<CollectorConfig>,
    ) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(100);
        
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            if let Ok(event) = res {
                if matches!(event.kind, EventKind::Modify(_)) {
                    let _ = tx.blocking_send(event);
                }
            }
        })?;
        
        watcher.watch(&config_path, RecursiveMode::NonRecursive)?;
        info!("Watching config file: {:?}", config_path);
        
        while let Some(_event) = rx.recv().await {
            // Debounce rapid changes
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            
            match CollectorConfig::load_from_file(&config_path) {
                Ok(new_config) => {
                    info!("Configuration reloaded from file");
                    
                    // Update the stored config
                    {
                        let mut config_guard = config.write().await;
                        *config_guard = new_config.clone();
                    }
                    
                    // Notify listeners
                    if let Err(e) = update_tx.send(new_config).await {
                        warn!("Failed to send config update: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to reload configuration: {}", e);
                }
            }
        }
        
        Ok(())
    }
    
    /// Manually update the configuration
    pub async fn update_config(&self, new_config: CollectorConfig) -> Result<()> {
        {
            let mut config_guard = self.config.write().await;
            *config_guard = new_config.clone();
        }
        
        if let Some(tx) = &self.update_tx {
            tx.send(new_config).await
                .context("Failed to send config update")?;
        }
        
        Ok(())
    }
    
    /// Save current configuration to file
    pub async fn save_to_file(&self, path: &Path) -> Result<()> {
        let config = self.get_config().await;
        let content = toml::to_string_pretty(&config)?;
        tokio::fs::write(path, content).await?;
        info!("Configuration saved to: {:?}", path);
        Ok(())
    }
}