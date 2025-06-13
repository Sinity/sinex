use anyhow::Result;
use sinex_core::{create_registry, EventRegistry, EventSource, EventSourceContext};
use sinex_db::{models::RawEvent, validation::EventValidator};
use sinex_events::{
    filesystem::{FilesystemMonitor, FilesystemConfig},
    terminal::{KittySocketListener, KittyConfig},
    window_manager::{HyprlandIPCMonitor, HyprlandConfig},
    atuin::{AtuinDbReader, AtuinConfig},
    shell_history::{ShellHistoryReader, ShellHistoryConfig},
    asciinema::{AsciinemaRecorder, AsciinemaConfig},
    scrollback::{ScrollbackCapture, ScrollbackConfig},
    dbus::{DbusMonitor, DbusConfig},
    clipboard::{ClipboardMonitor, ClipboardConfig},
    journal::{JournalMonitor, JournalConfig},
};
use sqlx::PgPool;
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, error};

use crate::config::CollectorConfig;
use crate::OutputConfig;

/// Unified collector that manages all event sources
pub struct UnifiedCollector {
    config: CollectorConfig,
    output_config: OutputConfig,
    enabled_events: HashSet<String>,
    registry: EventRegistry,
    db_pool: Option<PgPool>,
    validator: Option<EventValidator>,
    event_log_file: Option<tokio::fs::File>,
}

impl UnifiedCollector {
    pub fn new(
        config: CollectorConfig,
        output_config: OutputConfig,
        db_pool: Option<PgPool>,
        validator: Option<EventValidator>,
    ) -> Self {
        let enabled_events: HashSet<_> = config.enabled_events.iter().cloned().collect();
        let registry = create_registry();
        
        Self {
            config,
            output_config,
            enabled_events,
            registry,
            db_pool,
            validator,
            event_log_file: None,
        }
    }
    
    /// Main run loop - handles event collection and processing
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting unified collector");
        
        // Create channel for events
        let (event_tx, mut event_rx) = mpsc::channel::<RawEvent>(10000);
        
        // Start event sources
        let source_handles = self.start_sources(event_tx).await?;
        
        // Process events
        while let Some(event) = event_rx.recv().await {
            if let Err(e) = crate::output_event(
                &event,
                &self.output_config,
                self.db_pool.as_ref(),
                self.validator.as_ref(),
                &mut self.event_log_file,
            ).await {
                error!("Failed to output event: {}", e);
            }
        }
        
        // Clean up source handles
        for handle in source_handles {
            handle.abort();
        }
        
        info!("Collector stopped");
        Ok(())
    }
    
    /// Start all enabled event sources
    async fn start_sources(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<Vec<JoinHandle<()>>> {
        let mut handles = Vec::new();
        
        if self.needs_source("filesystem") {
            let handle = self.start_filesystem_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        if self.needs_source("terminal.kitty") {
            let handle = self.start_terminal_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        if self.needs_source("window_manager.hyprland") {
            let handle = self.start_window_manager_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        if self.needs_source("ingestor.atuin_db_reader") {
            let handle = self.start_atuin_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        if self.needs_source("ingestor.shell_history_reader") {
            let handle = self.start_shell_history_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        if self.needs_source("ingestor.asciinema_recorder") {
            let handle = self.start_asciinema_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        if self.needs_source("ingestor.scrollback_capture") {
            let handle = self.start_scrollback_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        if self.needs_source("dbus.monitor") {
            let handle = self.start_dbus_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        if self.needs_source("clipboard.monitor") {
            let handle = self.start_clipboard_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        if self.needs_source("journal.monitor") {
            let handle = self.start_journal_source(event_tx.clone()).await?;
            handles.push(handle);
        }
        
        info!("Started {} event sources for {} enabled events", 
              handles.len(), self.enabled_events.len());
        
        Ok(handles)
    }
    
    fn is_event_enabled(&self, event_name: &str) -> bool {
        self.enabled_events.contains(event_name)
    }
    
    fn needs_source(&self, source_name: &str) -> bool {
        self.registry.events_for_source(source_name)
            .iter()
            .any(|event| self.is_event_enabled(event))
    }
    
    async fn start_filesystem_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting filesystem source");
        
        // Get config for filesystem events
        let config_value = self.config.event.get("files")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(FilesystemConfig::default()).unwrap());
        
        // Create context with database pool and config
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        // Add database pool if available
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        // Extract annex_repo_path from config if present
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        // Initialize the source
        let mut source = FilesystemMonitor::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Filesystem source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_terminal_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting terminal source");
        
        let config_value = self.config.event.get("commands")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(KittyConfig::default()).unwrap());
        
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        let mut source = KittySocketListener::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Terminal source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_window_manager_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting window manager source");
        
        let config_value = self.config.event.get("windows")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(HyprlandConfig::default()).unwrap());
        
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        let mut source = HyprlandIPCMonitor::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Window manager source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_atuin_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting atuin source");
        
        let config_value = self.config.event.get("shell.command.executed_atuin")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(AtuinConfig::default()).unwrap());
        
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        let mut source = AtuinDbReader::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Atuin source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_shell_history_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting shell history source");
        
        let config_value = self.config.event.get("shell.history.command")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(ShellHistoryConfig::default()).unwrap());
        
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        let mut source = ShellHistoryReader::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Shell history source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_asciinema_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting asciinema recorder");
        
        let config_value = self.config.event.get("terminal.asciinema")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(AsciinemaConfig::default()).unwrap());
        
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        let mut source = AsciinemaRecorder::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Asciinema recorder failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_scrollback_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting scrollback capture");
        
        let config_value = self.config.event.get("terminal.scrollback")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(ScrollbackConfig::default()).unwrap());
        
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        let mut source = ScrollbackCapture::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Scrollback capture failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_dbus_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting D-Bus monitor");
        
        let config_value = self.config.event.get("dbus")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(DbusConfig::default()).unwrap());
        
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        let mut source = DbusMonitor::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("D-Bus monitor failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_clipboard_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting clipboard monitor");
        
        let config_value = self.config.event.get("clipboard")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(ClipboardConfig::default()).unwrap());
        
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        let mut source = ClipboardMonitor::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Clipboard monitor failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_journal_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting journal monitor");
        
        let config_value = self.config.event.get("journal")
            .cloned()
            .unwrap_or_else(|| serde_json::to_value(JournalConfig::default()).unwrap());
        
        let mut ctx = EventSourceContext::new(config_value.clone());
        
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }
        
        if let Some(annex_path) = config_value.get("annex_repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()) {
            ctx = ctx.with_annex_path(annex_path);
        }
        
        let mut source = JournalMonitor::initialize(ctx).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Journal monitor failed: {}", e);
            }
        });
        
        Ok(handle)
    }
}