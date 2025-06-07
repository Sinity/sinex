use anyhow::Result;
use async_trait::async_trait;
use sinex_shared::SimpleIngestor;
use tokio::sync::mpsc;
use sinex_db::models::RawEvent;

use crate::config::HyprlandConfig;
use crate::simple_watcher::{SimpleHyprlandWatcher, create_startup_event, create_shutdown_event};

/// Hyprland ingestor that implements SimpleIngestor trait
pub struct HyprlandSimpleIngestor {
    config: HyprlandConfig,
}

impl HyprlandSimpleIngestor {
    pub fn new(config: HyprlandConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl SimpleIngestor for HyprlandSimpleIngestor {
    fn name() -> &'static str {
        "hyprland-ingestor"
    }
    
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        // Send startup event
        let startup_event = create_startup_event(Self::name(), Self::version());
        event_tx.send(startup_event).await?;
        
        // Create and run watcher
        let mut watcher = SimpleHyprlandWatcher::new(self.config.clone())?;
        let result = watcher.watch(event_tx.clone()).await;
        
        // Send shutdown event
        let shutdown_reason = match &result {
            Ok(_) => "normal".to_string(),
            Err(e) => format!("error: {}", e),
        };
        let shutdown_event = create_shutdown_event(Self::name(), &shutdown_reason);
        let _ = event_tx.send(shutdown_event).await;
        
        result
    }
}