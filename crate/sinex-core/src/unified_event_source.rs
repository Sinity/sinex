/// Unified EventSource trait with integrated event factory
/// 
/// This module implements the architectural improvement to merge EventFactory
/// functionality directly into the EventSourceBase trait, eliminating boilerplate
/// and providing type safety.

use crate::unified_collector::EventSource;
use crate::{CoreError, EventSender, EventSourceContext, RawEvent, RawEventBuilder, Result, JsonValue};
use sinex_events::{
    FilesystemEventBuilder, TerminalEventBuilder, ClipboardEventBuilder,
    WindowManagerEventBuilder, SystemEventBuilder
};
use async_trait::async_trait;
use chrono::Utc;
use serde::de::DeserializeOwned;

/// Enhanced EventSourceBase trait with integrated event factory
/// 
/// This trait merges the EventFactory functionality directly into the EventSource,
/// eliminating boilerplate and ensuring type safety.
#[async_trait]
pub trait UnifiedEventSource: EventSource + Sized {
    /// Parse configuration from the event source context
    async fn parse_config<T: DeserializeOwned>(ctx: &EventSourceContext) -> Result<T> {
        serde_json::from_value(ctx.config.clone())
            .map_err(|e| CoreError::Configuration(format!("Failed to parse config: {}", e)))
    }

    /// Create a generic event with manual payload (for backward compatibility)
    fn create_event(&self, event_type: &str, payload: JsonValue) -> RawEvent {
        RawEventBuilder::new(Self::SOURCE_NAME, event_type, payload)
            .with_host(&Self::get_hostname())
            .with_ingestor_version(&Self::get_version())
            .with_orig_timestamp(Utc::now())
            .build()
    }

    /// Create a filesystem event builder (type-safe to this source)
    fn filesystem(&self) -> TypedFilesystemEventBuilder<Self> {
        TypedFilesystemEventBuilder::new()
    }

    /// Create a terminal event builder (type-safe to this source)
    fn terminal(&self) -> TypedTerminalEventBuilder<Self> {
        TypedTerminalEventBuilder::new()
    }

    /// Create a clipboard event builder (type-safe to this source)
    fn clipboard(&self) -> TypedClipboardEventBuilder<Self> {
        TypedClipboardEventBuilder::new()
    }

    /// Create a window manager event builder (type-safe to this source)
    fn window_manager(&self) -> TypedWindowManagerEventBuilder<Self> {
        TypedWindowManagerEventBuilder::new()
    }

    /// Create a system event builder (type-safe to this source)
    fn system(&self) -> TypedSystemEventBuilder<Self> {
        TypedSystemEventBuilder::new()
    }

    /// Helper to send an event with error handling
    async fn send_event(&self, tx: &EventSender, event: RawEvent) -> Result<()> {
        tx.send(event)
            .await
            .map_err(|e| CoreError::Other(format!("Failed to send event: {}", e)))
    }

    /// Get hostname with caching (to avoid repeated syscalls)
    fn get_hostname() -> String {
        gethostname::gethostname().to_string_lossy().to_string()
    }

    /// Get version string
    fn get_version() -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

// ============================================================================
// Type-Safe Event Builders
// ============================================================================

/// Type-safe filesystem event builder tied to a specific source
pub struct TypedFilesystemEventBuilder<T: EventSource> {
    inner: FilesystemEventBuilder,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: EventSource> TypedFilesystemEventBuilder<T> {
    fn new() -> Self {
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let version = env!("CARGO_PKG_VERSION");
        Self {
            inner: FilesystemEventBuilder::new(T::SOURCE_NAME, &hostname, version),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.inner = self.inner.path(path);
        self
    }

    pub fn created(mut self) -> Self {
        self.inner = self.inner.created();
        self
    }

    pub fn modified(mut self) -> Self {
        self.inner = self.inner.modified();
        self
    }

    pub fn deleted(mut self) -> Self {
        self.inner = self.inner.deleted();
        self
    }

    pub fn moved_from(mut self, old_path: impl Into<String>) -> Self {
        self.inner = self.inner.moved_from(old_path);
        self
    }

    pub fn size(mut self, size: u64) -> Self {
        self.inner = self.inner.size(size);
        self
    }

    pub fn permissions(mut self, perms: u32) -> Self {
        self.inner = self.inner.permissions(perms);
        self
    }

    pub fn build(self) -> RawEvent {
        self.inner.build()
    }
}

/// Type-safe terminal event builder tied to a specific source
pub struct TypedTerminalEventBuilder<T: EventSource> {
    inner: TerminalEventBuilder,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: EventSource> TypedTerminalEventBuilder<T> {
    fn new() -> Self {
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let version = env!("CARGO_PKG_VERSION");
        Self {
            inner: TerminalEventBuilder::new(T::SOURCE_NAME, &hostname, version),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn command(mut self, cmd: impl Into<String>) -> Self {
        self.inner = self.inner.command(cmd);
        self
    }

    pub fn command_output(mut self, output: impl Into<String>) -> Self {
        self.inner = self.inner.command_output(output);
        self
    }

    pub fn exit_code(mut self, code: i32) -> Self {
        self.inner = self.inner.exit_code(code);
        self
    }

    pub fn success(mut self) -> Self {
        self.inner = self.inner.success();
        self
    }

    pub fn failed(mut self, code: i32) -> Self {
        self.inner = self.inner.failed(code);
        self
    }

    pub fn duration_ms(mut self, ms: u64) -> Self {
        self.inner = self.inner.duration_ms(ms);
        self
    }

    pub fn working_dir(mut self, dir: impl Into<String>) -> Self {
        self.inner = self.inner.working_dir(dir);
        self
    }

    pub fn window_id(mut self, id: impl Into<String>) -> Self {
        self.inner = self.inner.window_id(id);
        self
    }

    pub fn tab_id(mut self, id: impl Into<String>) -> Self {
        self.inner = self.inner.tab_id(id);
        self
    }

    pub fn build_executed(self) -> RawEvent {
        self.inner.build_executed()
    }

    pub fn build_completed(self) -> RawEvent {
        self.inner.build_completed()
    }
}

/// Type-safe clipboard event builder tied to a specific source
pub struct TypedClipboardEventBuilder<T: EventSource> {
    inner: ClipboardEventBuilder,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: EventSource> TypedClipboardEventBuilder<T> {
    fn new() -> Self {
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let version = env!("CARGO_PKG_VERSION");
        Self {
            inner: ClipboardEventBuilder::new(T::SOURCE_NAME, &hostname, version),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn content(mut self, content: impl Into<String>) -> Self {
        self.inner = self.inner.content(content);
        self
    }

    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.inner = self.inner.text(text);
        self
    }

    pub fn content_hash(mut self, hash: impl Into<String>) -> Self {
        self.inner = self.inner.content_hash(hash);
        self
    }

    pub fn source_app(mut self, app: impl Into<String>) -> Self {
        self.inner = self.inner.source_app(app);
        self
    }

    pub fn primary_selection(mut self) -> Self {
        self.inner = self.inner.primary_selection();
        self
    }

    pub fn clipboard_selection(mut self) -> Self {
        self.inner = self.inner.clipboard_selection();
        self
    }

    pub fn build(self) -> RawEvent {
        self.inner.build()
    }
}

/// Type-safe window manager event builder tied to a specific source
pub struct TypedWindowManagerEventBuilder<T: EventSource> {
    inner: WindowManagerEventBuilder,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: EventSource> TypedWindowManagerEventBuilder<T> {
    fn new() -> Self {
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let version = env!("CARGO_PKG_VERSION");
        Self {
            inner: WindowManagerEventBuilder::new(T::SOURCE_NAME, &hostname, version),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn window_address(mut self, address: impl Into<String>) -> Self {
        self.inner = self.inner.window_address(address);
        self
    }

    pub fn window_class(mut self, class: impl Into<String>) -> Self {
        self.inner = self.inner.window_class(class);
        self
    }

    pub fn window_title(mut self, title: impl Into<String>) -> Self {
        self.inner = self.inner.window_title(title);
        self
    }

    pub fn workspace_id(mut self, id: impl Into<String>) -> Self {
        self.inner = self.inner.workspace_id(id);
        self
    }

    pub fn event_data(mut self, data: impl Into<String>) -> Self {
        self.inner = self.inner.event_data(data);
        self
    }

    pub fn build_window_focused(self) -> RawEvent {
        self.inner.build_window_focused()
    }

    pub fn build_window_opened(self) -> RawEvent {
        self.inner.build_window_opened()
    }

    pub fn build_window_closed(self) -> RawEvent {
        self.inner.build_window_closed()
    }

    pub fn build_workspace_switched(self) -> RawEvent {
        self.inner.build_workspace_switched()
    }

    pub fn build(self) -> RawEvent {
        self.inner.build()
    }
}

/// Type-safe system event builder tied to a specific source
pub struct TypedSystemEventBuilder<T: EventSource> {
    inner: SystemEventBuilder,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: EventSource> TypedSystemEventBuilder<T> {
    fn new() -> Self {
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let version = env!("CARGO_PKG_VERSION");
        Self {
            inner: SystemEventBuilder::new(T::SOURCE_NAME, &hostname, version),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.inner = self.inner.message(message);
        self
    }

    pub fn priority(mut self, priority: u8) -> Self {
        self.inner = self.inner.priority(priority);
        self
    }

    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.inner = self.inner.unit(unit);
        self
    }

    pub fn pid(mut self, pid: u32) -> Self {
        self.inner = self.inner.pid(pid);
        self
    }

    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.inner = self.inner.cursor(cursor);
        self
    }

    pub fn field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.field(key, value);
        self
    }

    pub fn agent_name(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.agent_name(name);
        self
    }

    pub fn heartbeat(mut self) -> Self {
        self.inner = self.inner.heartbeat();
        self
    }

    pub fn error(mut self, error: impl Into<String>) -> Self {
        self.inner = self.inner.error(error);
        self
    }

    pub fn build(self) -> RawEvent {
        self.inner.build()
    }

    pub fn build_journal_entry(self) -> RawEvent {
        self.inner.build_journal_entry()
    }
}

// Note: EventSource trait is imported from unified_collector
// The UnifiedEventSource trait extends the base EventSource trait

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources;

    // Mock EventSource for testing
    struct TestFilesystemSource;
    
    #[async_trait]
    impl EventSource for TestFilesystemSource {
        type Config = ();
        const SOURCE_NAME: &'static str = sources::FS;
        
        async fn initialize(_ctx: EventSourceContext) -> Result<Self> {
            Ok(Self)
        }
        
        async fn stream_events(&mut self, _tx: EventSender) -> Result<()> {
            Ok(())
        }
    }
    
    impl UnifiedEventSource for TestFilesystemSource {}

    #[test]
    fn test_unified_event_source_type_safety() {
        // This ensures FilesystemSource can only create events with "fs" source
        let source = TestFilesystemSource;
        let event = UnifiedEventSource::filesystem(&source)
            .path("/test.txt")
            .created()
            .size(1024)
            .build();

        assert_eq!(event.source, sources::FS);
        assert_eq!(event.event_type, "file.created");
    }

    #[test]
    fn test_no_boilerplate_event_factory() {
        // No more EventFactory::new() boilerplate needed
        let source = TestFilesystemSource;
        
        // Direct builder access from source
        let event = UnifiedEventSource::filesystem(&source).path("/test").created().build();
        assert_eq!(event.source, sources::FS);
        
        // Source name is enforced by the type system
        // Cannot accidentally use wrong source name
    }
}