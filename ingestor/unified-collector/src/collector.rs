use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use sinex_core::{EventSource, EventRegistry, unified_collector::registry::create_registry};
use sinex_events::{filesystem::FilesystemWatcher, terminal::KittySocketListener, window_manager::HyprlandListener};
use sinex_shared::{SimpleIngestor, RawEvent};

use crate::config::UnifiedConfig;

pub struct UnifiedCollector {
    config: UnifiedConfig,
    enabled_events: HashSet<String>,
    registry: EventRegistry,
}

impl UnifiedCollector {
    pub fn new(config: UnifiedConfig) -> Self {
        let enabled_events: HashSet<_> = config.enabled_events.iter().cloned().collect();
        let registry = create_registry();
        
        Self {
            config,
            enabled_events,
            registry,
        }
    }
    
    fn is_event_enabled(&self, event_name: &str) -> bool {
        self.enabled_events.contains(event_name)
    }
    
    fn needs_source(&self, source_name: &str) -> bool {
        self.registry.events_for_source(source_name)
            .iter()
            .any(|event| self.is_event_enabled(event))
    }
    
    async fn run_filesystem_source(&self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!("Starting filesystem source");
        
        // Get config for filesystem events
        let mut fs_config = sinex_events::filesystem::FilesystemConfig::default();
        
        // Apply configuration from event.files
        if let Some(config_value) = self.config.event.get("files") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                fs_config = custom_config;
            }
        }
        
        // Initialize and run the source
        let mut source = FilesystemWatcher::initialize(fs_config).await?;
        source.stream_events(tx).await?;
        
        Ok(())
    }
    
    async fn run_terminal_source(&self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!("Starting terminal source");
        
        let mut kitty_config = sinex_events::terminal::KittyConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("commands") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                kitty_config = custom_config;
            }
        }
        
        let mut source = KittySocketListener::initialize(kitty_config).await?;
        source.stream_events(tx).await?;
        
        Ok(())
    }
    
    async fn run_window_manager_source(&self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        info!("Starting window manager source");
        
        let mut hypr_config = sinex_events::window_manager::HyprlandConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("windows") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                hypr_config = custom_config;
            }
        }
        
        let mut source = HyprlandListener::initialize(hypr_config).await?;
        source.stream_events(tx).await?;
        
        Ok(())
    }
}

#[async_trait]
impl SimpleIngestor for UnifiedCollector {
    fn name() -> &'static str {
        "unified-collector"
    }
    
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        let mut tasks: Vec<JoinHandle<Result<()>>> = Vec::new();
        
        // Start only the sources we need based on enabled events
        if self.needs_source("filesystem") {
            let tx = event_tx.clone();
            let collector = self.clone(); // Need to implement Clone or use Arc
            tasks.push(tokio::spawn(async move {
                collector.run_filesystem_source(tx).await
            }));
        }
        
        if self.needs_source("terminal.kitty") {
            let tx = event_tx.clone();
            let collector = self.clone();
            tasks.push(tokio::spawn(async move {
                collector.run_terminal_source(tx).await
            }));
        }
        
        if self.needs_source("window_manager.hyprland") {
            let tx = event_tx.clone();
            let collector = self.clone();
            tasks.push(tokio::spawn(async move {
                collector.run_window_manager_source(tx).await
            }));
        }
        
        info!(
            "Started {} event sources for {} enabled events",
            tasks.len(),
            self.enabled_events.len()
        );
        
        // Wait for all sources
        for (i, task) in tasks.into_iter().enumerate() {
            match task.await {
                Ok(Ok(())) => debug!("Source {} completed successfully", i),
                Ok(Err(e)) => warn!("Source {} failed: {}", i, e),
                Err(e) => warn!("Source {} task panicked: {}", i, e),
            }
        }
        
        Ok(())
    }
}

// Implement Clone for UnifiedCollector to support spawning
impl Clone for UnifiedCollector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            enabled_events: self.enabled_events.clone(),
            registry: create_registry(), // Registry is stateless
        }
    }
}