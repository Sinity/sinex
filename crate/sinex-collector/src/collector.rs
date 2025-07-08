use anyhow::Result;
use sinex_core::RawEvent;
use sinex_core::{
    unified_collector::{EventRegistry, EventRegistryBuilder, EventSource},
    ConfigValue, EventSender, EventSourceContext, JsonValue,
};
use sinex_db::validation::EventValidator;
use sinex_db::DbPool;
use sinex_events_desktop::{
    clipboard::{ClipboardConfig, ClipboardMonitor},
    window_manager::{HyprlandConfig, HyprlandIPCMonitor},
};
use sinex_events_fs::filesystem::{FilesystemConfig, FilesystemMonitor};
use sinex_events_system::{
    dbus::{DbusConfig, DbusMonitor},
    journal::{JournalConfig, JournalMonitor},
};
use sinex_events_terminal::{
    asciinema::{AsciinemaConfig, AsciinemaRecorder},
    atuin::{AtuinConfig, AtuinDbReader},
    kitty::{KittyConfig, KittyEventSource},
    scrollback::{ScrollbackCapture, ScrollbackConfig},
    shell_history::{ShellHistoryConfig, ShellHistoryReader},
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::config::CollectorConfig;
use crate::metrics::CollectorMetrics;
use crate::OutputConfig;

/// Create an EventRegistry with auto-registration from event crates
/// This demonstrates the pattern for eliminating manual registry maintenance
///
/// # Auto-Registration Pattern
/// 
/// Each event crate provides a `register_events(builder: &mut EventRegistryBuilder)` function
/// that automatically registers all its event types with proper schemas. This eliminates:
/// 
/// - Manual maintenance of event type lists
/// - Risk of forgetting to register new event types
/// - Schema/payload drift when types are updated
/// 
/// # Usage
/// 
/// ```rust
/// let registry = create_registry_with_auto_registration();
/// ```
/// 
/// # Implementation Status
/// 
/// - ✅ sinex-events-fs: Implemented auto-registration
/// - ✅ sinex-events-desktop: Implemented auto-registration
/// - ✅ sinex-events-terminal: Implemented auto-registration
/// - ✅ sinex-events-system: Implemented auto-registration
pub fn create_registry_with_auto_registration() -> EventRegistry {
    let mut builder = EventRegistryBuilder::new();
    
    // Auto-register event types from each event crate
    sinex_events_fs::register_events(&mut builder);
    sinex_events_desktop::register_events(&mut builder);
    sinex_events_terminal::register_events(&mut builder);
    sinex_events_system::register_events(&mut builder);
    
    builder.build()
}

/// Convert TOML value to JSON value
fn toml_to_json(toml_val: ConfigValue) -> JsonValue {
    match toml_val {
        ConfigValue::String(s) => JsonValue::String(s),
        ConfigValue::Integer(i) => JsonValue::Number(i.into()),
        ConfigValue::Float(f) => serde_json::json!(f),
        ConfigValue::Boolean(b) => JsonValue::Bool(b),
        ConfigValue::Array(arr) => JsonValue::Array(arr.into_iter().map(toml_to_json).collect()),
        ConfigValue::Table(table) => {
            let map: serde_json::Map<String, JsonValue> = table
                .into_iter()
                .map(|(k, v)| (k, toml_to_json(v)))
                .collect();
            JsonValue::Object(map)
        }
        ConfigValue::Datetime(dt) => JsonValue::String(dt.to_string()),
    }
}

/// Unified collector that manages all event sources
pub struct UnifiedCollector {
    config: CollectorConfig,
    output_config: OutputConfig,
    enabled_events: HashSet<String>,
    registry: EventRegistry,
    db_pool: Option<DbPool>,
    validator: Option<EventValidator>,
    event_log_file: Option<tokio::fs::File>,
    metrics: Arc<CollectorMetrics>,
}

impl UnifiedCollector {
    pub fn new(
        config: CollectorConfig,
        output_config: OutputConfig,
        db_pool: Option<DbPool>,
        validator: Option<EventValidator>,
    ) -> Self {
        let enabled_events: HashSet<_> = config.enabled_events.iter().cloned().collect();
        let registry = create_registry_with_auto_registration();

        Self {
            config,
            output_config,
            enabled_events,
            registry,
            db_pool,
            validator,
            event_log_file: None,
            metrics: Arc::new(CollectorMetrics::new()),
        }
    }

    /// Main run loop - handles event collection and processing
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting unified collector");

        // Create channel for events
        let (event_tx, mut event_rx) = mpsc::channel::<RawEvent>(10000);

        // Start metrics collection
        self.metrics
            .clone()
            .start(event_tx.clone(), self.db_pool.clone())
            .await;
        info!("Started high-resolution metrics collection");

        // Start event sources
        let source_handles = self.start_sources(event_tx).await?;

        // Process events
        while let Some(event) = event_rx.recv().await {
            // Record metrics for non-metrics events
            if !event.source.starts_with("sinex.metrics.") {
                self.metrics.record_event(&event.source);

                // Update source-specific metrics
                let source = event.source.clone();
                let event_size = event.payload.to_string().len() as u64;
                let metrics = self.metrics.clone();
                tokio::spawn(async move {
                    metrics
                        .update_source_metrics(&source, |m| {
                            m.events_total += 1;
                            m.bytes_processed += event_size;
                            m.last_event_time = Some(chrono::Utc::now());
                        })
                        .await;
                });
            }

            if let Err(e) = crate::output_event(
                &event,
                &self.output_config,
                self.db_pool.as_ref(),
                self.validator.as_ref(),
                &mut self.event_log_file,
            )
            .await
            {
                error!("Failed to output event: {}", e);
                self.metrics.record_error(&event.source);
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
    async fn start_sources(&self, event_tx: EventSender) -> Result<Vec<JoinHandle<()>>> {
        let mut handles = Vec::new();

        if self.needs_source("fs") {
            let handle = self.start_filesystem_source(event_tx.clone()).await?;
            handles.push(handle);
        }

        if self.needs_source("shell.kitty") {
            let handle = self.start_terminal_source(event_tx.clone()).await?;
            handles.push(handle);
        }

        if self.needs_source("wm.hyprland") {
            let handle = self.start_window_manager_source(event_tx.clone()).await?;
            handles.push(handle);
        }

        if self.needs_source("shell.atuin") {
            let handle = self.start_atuin_source(event_tx.clone()).await?;
            handles.push(handle);
        }

        if self.needs_source("shell.history") {
            let handle = self.start_shell_history_source(event_tx.clone()).await?;
            handles.push(handle);
        }

        if self.needs_source("shell.recording") {
            let handle = self.start_asciinema_source(event_tx.clone()).await?;
            handles.push(handle);
        }

        if self.needs_source("shell.scrollback") {
            let handle = self.start_scrollback_source(event_tx.clone()).await?;
            handles.push(handle);
        }

        if self.needs_source("dbus") {
            let handle = self.start_dbus_source(event_tx.clone()).await?;
            handles.push(handle);
        }

        if self.needs_source("clipboard") {
            match self.start_clipboard_source(event_tx.clone()).await {
                Ok(handle) => handles.push(handle),
                Err(e) => error!(
                    "Failed to start clipboard source: {}. Clipboard monitoring disabled.",
                    e
                ),
            }
        }

        if self.needs_source("journald") {
            let handle = self.start_journal_source(event_tx.clone()).await?;
            handles.push(handle);
        }

        info!(
            "Started {} event sources for {} enabled events",
            handles.len(),
            self.enabled_events.len()
        );

        Ok(handles)
    }

    fn is_event_enabled(&self, event_name: &str) -> bool {
        self.enabled_events.contains(event_name)
    }

    fn needs_source(&self, source_name: &str) -> bool {
        self.registry
            .events_for_source(source_name)
            .iter()
            .any(|event| self.is_event_enabled(event))
    }

    async fn start_filesystem_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting filesystem source");

        // Get config for filesystem events
        let config_json = self
            .config
            .event
            .get("files")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(FilesystemConfig::default()).unwrap());

        // Create context with database pool and config
        let mut ctx = EventSourceContext::new(config_json.clone());

        // Add database pool if available
        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        // Use global annex_repo_path from collector config
        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
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

    async fn start_terminal_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting terminal source");

        let config_json = self
            .config
            .event
            .get("commands")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(KittyConfig::default()).unwrap());

        let mut ctx = EventSourceContext::new(config_json.clone());

        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
        }

        let mut source = KittyEventSource::initialize(ctx).await?;

        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Terminal source failed: {}", e);
            }
        });

        Ok(handle)
    }

    async fn start_window_manager_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting window manager source");

        let config_json = self
            .config
            .event
            .get("windows")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(HyprlandConfig::default()).unwrap());

        let mut ctx = EventSourceContext::new(config_json.clone());

        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
        }

        let mut source = HyprlandIPCMonitor::initialize(ctx).await?;

        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Window manager source failed: {}", e);
            }
        });

        Ok(handle)
    }

    async fn start_atuin_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting atuin source");

        let config_json = self
            .config
            .event
            .get("shell.command.executed_atuin")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(AtuinConfig::default()).unwrap());

        let mut ctx = EventSourceContext::new(config_json.clone());

        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
        }

        let mut source = AtuinDbReader::initialize(ctx).await?;

        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Atuin source failed: {}", e);
            }
        });

        Ok(handle)
    }

    async fn start_shell_history_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting shell history source");

        let config_json = self
            .config
            .event
            .get("shell.history.command")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(ShellHistoryConfig::default()).unwrap());

        let mut ctx = EventSourceContext::new(config_json.clone());

        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
        }

        let mut source = ShellHistoryReader::initialize(ctx).await?;

        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Shell history source failed: {}", e);
            }
        });

        Ok(handle)
    }

    async fn start_asciinema_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting asciinema recorder");

        let config_json = self
            .config
            .event
            .get("terminal.asciinema")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(AsciinemaConfig::default()).unwrap());

        let mut ctx = EventSourceContext::new(config_json.clone());

        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
        }

        let mut source = AsciinemaRecorder::initialize(ctx).await?;

        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Asciinema recorder failed: {}", e);
            }
        });

        Ok(handle)
    }

    async fn start_scrollback_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting scrollback capture");

        let config_json = self
            .config
            .event
            .get("terminal.scrollback")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(ScrollbackConfig::default()).unwrap());

        let mut ctx = EventSourceContext::new(config_json.clone());

        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
        }

        let mut source = ScrollbackCapture::initialize(ctx).await?;

        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Scrollback capture failed: {}", e);
            }
        });

        Ok(handle)
    }

    async fn start_dbus_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting D-Bus monitor");

        let config_json = self
            .config
            .event
            .get("dbus")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(DbusConfig::default()).unwrap());

        let mut ctx = EventSourceContext::new(config_json.clone());

        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
        }

        let mut source = DbusMonitor::initialize(ctx).await?;

        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("D-Bus monitor failed: {}", e);
            }
        });

        Ok(handle)
    }

    async fn start_clipboard_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting clipboard monitor");

        let config_json = self
            .config
            .event
            .get("clipboard")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(ClipboardConfig::default()).unwrap());

        let mut ctx = EventSourceContext::new(config_json.clone());

        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
        }

        let mut source = ClipboardMonitor::initialize(ctx).await?;

        let handle = tokio::spawn(async move {
            if let Err(e) = source.stream_events(event_tx).await {
                error!("Clipboard monitor failed: {}", e);
            }
        });

        Ok(handle)
    }

    async fn start_journal_source(&self, event_tx: EventSender) -> Result<JoinHandle<()>> {
        info!("Starting journal monitor");

        let config_json = self
            .config
            .event
            .get("journal")
            .cloned()
            .map(toml_to_json)
            .unwrap_or_else(|| serde_json::to_value(JournalConfig::default()).unwrap());

        let mut ctx = EventSourceContext::new(config_json.clone());

        if let Some(pool) = &self.db_pool {
            ctx = ctx.with_db_pool(pool.clone());
        }

        if let Some(annex_path) = &self.config.annex_repo_path {
            ctx = ctx.with_annex_path(annex_path.clone());
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
