use anyhow::{Context, Result};
use notify::{Event, RecursiveMode, Watcher};
use serde::{de::DeserializeOwned, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};
use tracing::{error, info, warn};
use validator::Validate;

/// Base configuration trait with validation
pub trait ValidatedConfig: Serialize + DeserializeOwned + Validate + Clone + Send + Sync {
    /// Get default configuration
    fn default_config() -> Self;

    /// Post-load validation hook
    fn post_load_validation(&self) -> Result<()> {
        Ok(())
    }

    /// Sanitize sensitive values for logging
    fn sanitize_for_logging(&self) -> Self {
        self.clone()
    }
}

/// Configuration source abstraction
#[async_trait::async_trait]
pub trait ConfigSource: Send + Sync {
    async fn load(&self) -> Result<toml::Value>;
    fn supports_watch(&self) -> bool;
    fn watch_path(&self) -> Option<&Path> {
        None
    }
}

/// File-based configuration source
pub struct FileConfigSource {
    path: PathBuf,
}

impl FileConfigSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait::async_trait]
impl ConfigSource for FileConfigSource {
    async fn load(&self) -> Result<toml::Value> {
        let content = tokio::fs::read_to_string(&self.path)
            .await
            .with_context(|| format!("Failed to read config file: {:?}", self.path))?;

        toml::from_str(&content)
            .with_context(|| format!("Failed to parse TOML from: {:?}", self.path))
    }

    fn supports_watch(&self) -> bool {
        true
    }
    
    fn watch_path(&self) -> Option<&Path> {
        Some(&self.path)
    }
}

/// Environment variable configuration source
pub struct EnvConfigSource {
    prefix: String,
}

impl EnvConfigSource {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

#[async_trait::async_trait]
impl ConfigSource for EnvConfigSource {
    async fn load(&self) -> Result<toml::Value> {
        let mut table = toml::value::Table::new();

        for (key, value) in std::env::vars() {
            if key.starts_with(&self.prefix) {
                let config_key = key
                    .strip_prefix(&self.prefix)
                    .unwrap()
                    .to_lowercase()
                    .replace('_', ".");

                // Try to parse as different types
                let parsed_value = if let Ok(v) = value.parse::<bool>() {
                    toml::Value::Boolean(v)
                } else if let Ok(v) = value.parse::<i64>() {
                    toml::Value::Integer(v)
                } else if let Ok(v) = value.parse::<f64>() {
                    toml::Value::Float(v)
                } else {
                    toml::Value::String(value)
                };

                // Handle nested keys
                let parts: Vec<&str> = config_key.split('.').collect();
                insert_nested(&mut table, &parts, parsed_value);
            }
        }

        Ok(toml::Value::Table(table))
    }

    fn supports_watch(&self) -> bool {
        false
    }
}

/// Insert value into nested table structure
fn insert_nested(table: &mut toml::value::Table, keys: &[&str], value: toml::Value) {
    if keys.is_empty() {
        return;
    }

    if keys.len() == 1 {
        table.insert(keys[0].to_string(), value);
        return;
    }

    let key = keys[0].to_string();
    let rest = &keys[1..];

    let entry = table
        .entry(key)
        .or_insert_with(|| toml::Value::Table(toml::value::Table::new()));

    if let toml::Value::Table(subtable) = entry {
        insert_nested(subtable, rest, value);
    }
}

/// Configuration manager with hot-reload support
pub struct ConfigManager<T: ValidatedConfig> {
    sources: Vec<Box<dyn ConfigSource>>,
    current: Arc<RwLock<T>>,
    watcher: watch::Sender<Arc<T>>,
    _watcher_rx: watch::Receiver<Arc<T>>,
}

impl<T: ValidatedConfig + 'static> ConfigManager<T> {
    pub async fn new(sources: Vec<Box<dyn ConfigSource>>) -> Result<Self> {
        let config = Self::load_from_sources(&sources).await?;
        let config_arc = Arc::new(config);

        let (tx, rx) = watch::channel(Arc::clone(&config_arc));

        Ok(Self {
            sources,
            current: Arc::new(RwLock::new(config_arc.as_ref().clone())),
            watcher: tx,
            _watcher_rx: rx,
        })
    }

    /// Get current configuration
    pub async fn get(&self) -> Arc<T> {
        let guard = self.current.read().await;
        Arc::new(guard.clone())
    }

    /// Get a watch receiver for config changes
    pub fn watch(&self) -> watch::Receiver<Arc<T>> {
        self.watcher.subscribe()
    }

    /// Reload configuration from sources
    pub async fn reload(&self) -> Result<()> {
        let new_config = Self::load_from_sources(&self.sources).await?;

        // Update current
        {
            let mut guard = self.current.write().await;
            *guard = new_config.clone();
        }

        // Notify watchers
        let _ = self.watcher.send(Arc::new(new_config));

        info!("Configuration reloaded successfully");
        Ok(())
    }

    /// Start watching for file changes
    pub fn start_file_watcher(self: Arc<Self>) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

            let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                if event.kind.is_modify() {
                    let _ = tx.blocking_send(());
                }
            }
        })?;

        // Watch all file sources
        for source in &self.sources {
            if let Some(path) = source.watch_path() {
                watcher.watch(path, RecursiveMode::NonRecursive)?;
            }
        }

        // Spawn reload task
        tokio::spawn(async move {
            while rx.recv().await.is_some() {
                // Debounce rapid changes
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                if let Err(e) = self.reload().await {
                    error!("Failed to reload configuration: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Load and merge configuration from all sources
    async fn load_from_sources(sources: &[Box<dyn ConfigSource>]) -> Result<T> {
        let mut merged = toml::Value::Table(toml::value::Table::new());

        for source in sources {
            match source.load().await {
                Ok(value) => {
                    merge_toml(&mut merged, value);
                }
                Err(e) => {
                    warn!("Failed to load from source: {}", e);
                }
            }
        }

        // Convert to target type
        let config: T = toml::from_str(&toml::to_string(&merged)?)?;

        // Validate
        config.validate()
            .context("Configuration validation failed")?;

        config.post_load_validation()
            .context("Post-load validation failed")?;

        Ok(config)
    }
}

/// Merge two TOML values
fn merge_toml(base: &mut toml::Value, other: toml::Value) {
    match (base, other) {
        (toml::Value::Table(base_table), toml::Value::Table(other_table)) => {
            for (key, value) in other_table {
                match base_table.get_mut(&key) {
                    Some(existing) => merge_toml(existing, value),
                    None => {
                        base_table.insert(key, value);
                    }
                }
            }
        }
        (base, other) => *base = other,
    }
}

/// Secrets management
pub struct SecretResolver {
    providers: Vec<Box<dyn SecretProvider>>,
}

#[async_trait::async_trait]
pub trait SecretProvider: Send + Sync {
    async fn resolve(&self, key: &str) -> Result<Option<String>>;
}

/// Environment variable secret provider
pub struct EnvSecretProvider {
    prefix: String,
}

#[async_trait::async_trait]
impl SecretProvider for EnvSecretProvider {
    async fn resolve(&self, key: &str) -> Result<Option<String>> {
        let env_key = format!("{}_{}", self.prefix, key.to_uppercase());
        Ok(std::env::var(env_key).ok())
    }
}

/// File-based secret provider
pub struct FileSecretProvider {
    base_path: PathBuf,
}

#[async_trait::async_trait]
impl SecretProvider for FileSecretProvider {
    async fn resolve(&self, key: &str) -> Result<Option<String>> {
        let path = self.base_path.join(key);
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => Ok(Some(content.trim().to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl SecretResolver {
    pub fn new(providers: Vec<Box<dyn SecretProvider>>) -> Self {
        Self { providers }
    }

    pub async fn resolve(&self, key: &str) -> Result<String> {
        for provider in &self.providers {
            if let Some(value) = provider.resolve(key).await? {
                return Ok(value);
            }
        }

        anyhow::bail!("Secret not found: {}", key)
    }
}

/// Configuration validation macros
#[macro_export]
macro_rules! validate_config {
    ($config:expr, $($field:ident : $validator:expr),* $(,)?) => {{
        $(
            if !$validator(&$config.$field) {
                return Err(anyhow::anyhow!(
                    "Invalid configuration: {} failed validation",
                    stringify!($field)
                ));
            }
        )*
        Ok(())
    }};
}


