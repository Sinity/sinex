use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use sinex_shared::{
    ingestor_framework::{Ingestor, IngestorConfig, CommonCommands},
    DatabaseService, sources, event_types,
};

use crate::config::Config;
use crate::kitty_listener::KittyListener;

/// The kitty ingestor implementation
pub struct KittyIngestor {
    config: Config,
    db: Arc<DatabaseService>,
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
impl Ingestor for KittyIngestor {
    type Config = Config;
    type Commands = CommonCommands;
    
    fn name() -> &'static str {
        "kitty-ingestor"
    }
    
    fn description() -> &'static str {
        "Captures terminal commands from Kitty terminal emulator"
    }
    
    fn produces_events() -> HashMap<String, Vec<String>> {
        let mut produces = HashMap::new();
        produces.insert(
            sources::TERMINAL_KITTY.to_string(),
            vec![
                event_types::event_types::terminal::COMMAND_EXECUTED.to_string(),
            ],
        );
        produces
    }
    
    async fn new(config: Self::Config, db: Arc<DatabaseService>) -> Result<Self> {
        Ok(Self { config, db })
    }
    
    async fn run(&mut self) -> Result<()> {
        let listener = KittyListener::new(
            self.config.kitty.clone(),
            Arc::clone(&self.db),
        )?;
        
        listener.start().await
    }
}