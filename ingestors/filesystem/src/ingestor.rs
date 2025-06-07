use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use sinex_shared::{
    ingestor_framework::{Ingestor, IngestorConfig, CommonCommands},
    EventSink, sources, event_types,
};

use crate::config::Config;
use crate::filesystem_watcher::FilesystemWatcher;

/// The filesystem ingestor implementation
pub struct FilesystemIngestor {
    config: Config,
    event_sink: Arc<dyn EventSink>,
}

impl IngestorConfig for Config {
    fn load() -> Result<Self> {
        Config::load()
    }
    
    fn load_from_file(path: &std::path::Path) -> Result<Self> {
        Config::load_from_file(&path.to_path_buf())
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

#[async_trait]
impl Ingestor for FilesystemIngestor {
    type Config = Config;
    type Commands = CommonCommands;
    
    fn name() -> &'static str {
        "filesystem-ingestor"
    }
    
    fn description() -> &'static str {
        "Monitors filesystem changes and ingests file events"
    }
    
    fn produces_events() -> HashMap<String, Vec<String>> {
        let mut produces = HashMap::new();
        produces.insert(
            sources::FILESYSTEM.to_string(),
            vec![
                event_types::event_types::filesystem::FILE_CREATED.to_string(),
                event_types::event_types::filesystem::FILE_MODIFIED.to_string(),
                event_types::event_types::filesystem::FILE_DELETED.to_string(),
                event_types::event_types::filesystem::FILE_RENAMED.to_string(),
            ],
        );
        produces
    }
    
    async fn new(config: Self::Config, event_sink: Arc<dyn EventSink>) -> Result<Self> {
        Ok(Self { config, event_sink })
    }
    
    async fn run(&mut self) -> Result<()> {
        let watcher = FilesystemWatcher::new(
            self.config.filesystem.clone(),
            Arc::clone(&self.event_sink),
        )?;
        
        watcher.start().await
    }
}