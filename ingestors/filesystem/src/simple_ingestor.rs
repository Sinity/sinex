use anyhow::Result;
use async_trait::async_trait;
use sinex_shared::SimpleIngestor;
use tokio::sync::mpsc;
use sinex_db::models::RawEvent;

use crate::config::FilesystemConfig;
use crate::simple_watcher::SimpleFilesystemWatcher;

/// Filesystem ingestor that implements SimpleIngestor trait
pub struct FilesystemSimpleIngestor {
    config: FilesystemConfig,
}

impl FilesystemSimpleIngestor {
    pub fn new(config: FilesystemConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl SimpleIngestor for FilesystemSimpleIngestor {
    fn name() -> &'static str {
        "filesystem-ingestor"
    }
    
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        let mut watcher = SimpleFilesystemWatcher::new(self.config.clone());
        watcher.watch(event_tx).await
    }
}