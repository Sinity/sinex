use anyhow::Result;
use sinex_core::{create_registry, EventRegistry, EventSource};
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
        let mut fs_config = FilesystemConfig::default();
        
        // Apply configuration from event.files
        if let Some(config_value) = self.config.event.get("files") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                fs_config = custom_config;
            }
        }
        
        // Initialize the source
        let mut source = FilesystemMonitor::initialize(fs_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Filesystem source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_terminal_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting terminal source");
        
        let mut kitty_config = KittyConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("commands") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                kitty_config = custom_config;
            }
        }
        
        let mut source = KittySocketListener::initialize(kitty_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Terminal source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_window_manager_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting window manager source");
        
        let mut hypr_config = HyprlandConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("windows") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                hypr_config = custom_config;
            }
        }
        
        let mut source = HyprlandIPCMonitor::initialize(hypr_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Window manager source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_atuin_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting atuin source");
        
        let mut atuin_config = AtuinConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("shell.command.executed_atuin") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                atuin_config = custom_config;
            }
        }
        
        let mut source = AtuinDbReader::initialize(atuin_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Atuin source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_shell_history_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting shell history source");
        
        let mut shell_config = ShellHistoryConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("shell.history.command") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                shell_config = custom_config;
            }
        }
        
        let mut source = ShellHistoryReader::initialize(shell_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Shell history source failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_asciinema_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting asciinema recorder");
        
        let mut asciinema_config = AsciinemaConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("terminal.asciinema") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                asciinema_config = custom_config;
            }
        }
        
        let mut source = AsciinemaRecorder::initialize(asciinema_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Asciinema recorder failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_scrollback_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting scrollback capture");
        
        let mut scrollback_config = ScrollbackConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("terminal.scrollback") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                scrollback_config = custom_config;
            }
        }
        
        let mut source = ScrollbackCapture::initialize(scrollback_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Scrollback capture failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_dbus_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting D-Bus monitor");
        
        let mut dbus_config = DbusConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("dbus") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                dbus_config = custom_config;
            }
        }
        
        let mut source = DbusMonitor::initialize(dbus_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("D-Bus monitor failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_clipboard_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting clipboard monitor");
        
        let mut clipboard_config = ClipboardConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("clipboard") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                clipboard_config = custom_config;
            }
        }
        
        let mut source = ClipboardMonitor::initialize(clipboard_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Clipboard monitor failed: {}", e);
            }
        });
        
        Ok(handle)
    }
    
    async fn start_journal_source(&self, event_tx: mpsc::Sender<RawEvent>) -> Result<JoinHandle<()>> {
        info!("Starting journal monitor");
        
        let mut journal_config = JournalConfig::default();
        
        // Apply configuration
        if let Some(config_value) = self.config.event.get("journal") {
            if let Ok(custom_config) = config_value.clone().try_into() {
                journal_config = custom_config;
            }
        }
        
        let mut source = JournalMonitor::initialize(journal_config).await?;
        
        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Journal monitor failed: {}", e);
            }
        });
        
        Ok(handle)
    }
}