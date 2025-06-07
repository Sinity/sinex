use anyhow::Result;
use async_trait::async_trait;
use sinex_shared::SimpleIngestor;
use tokio::sync::mpsc;
use sinex_db::models::RawEvent;

use crate::config::KittyConfig;
use crate::simple_watcher::SimpleKittyWatcher;

/// Kitty ingestor that implements SimpleIngestor trait
pub struct KittySimpleIngestor {
    config: KittyConfig,
}

impl KittySimpleIngestor {
    pub fn new(config: KittyConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl SimpleIngestor for KittySimpleIngestor {
    fn name() -> &'static str {
        "kitty-ingestor"
    }
    
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        let mut watcher = SimpleKittyWatcher::new(self.config.clone());
        watcher.watch(event_tx).await
    }
}