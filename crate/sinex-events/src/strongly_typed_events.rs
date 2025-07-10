/// Strongly-typed event system to eliminate JsonValue from internal core
/// 
/// This module implements the architectural improvement to use strongly-typed
/// payloads throughout the system, deferring JSON serialization until the
/// database boundary.
use serde::{Serialize, Deserialize};
use sinex_ulid::Ulid;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

// Re-export types from sinex-events for internal use
use crate::{RawEvent, EventSender};

// Define our own error type to avoid circular dependency on sinex-core
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TypedEventError {
    #[error("Channel send error: {0}")]
    ChannelSend(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Other error: {0}")]
    Other(String),
}

pub type TypedEventResult<T> = std::result::Result<T, TypedEventError>;

// ============================================================================
// Strongly-Typed RawEvent
// ============================================================================

/// Generic RawEvent with strongly-typed payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedRawEvent<P: Serialize> {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    pub payload: P,
    pub host: String,
    pub ingestor_version: String,
    pub ts_ingest: DateTime<Utc>,
    pub ts_orig: Option<DateTime<Utc>>,
}

impl<P: Serialize> TypedRawEvent<P> {
    /// Convert to JSON-based RawEvent for database storage
    pub fn to_json_event(self) -> RawEvent {
        let payload_json = serde_json::to_value(&self.payload)
            .expect("Payload must be serializable to JSON");
        
        RawEvent {
            id: self.id,
            source: self.source,
            event_type: self.event_type,
            payload: payload_json,
            host: self.host,
            ingestor_version: Some(self.ingestor_version),
            ts_ingest: self.ts_ingest,
            ts_orig: self.ts_orig,
            payload_schema_id: None,
        }
    }
}

// ============================================================================
// Event Envelope for Type-Safe Channel Communication
// ============================================================================

/// Type-safe event envelope for MPSC channels
/// Contains strongly-typed variants for each event type
#[derive(Debug, Clone)]
pub enum EventEnvelope {
    // Filesystem events
    FileCreated(TypedRawEvent<FileCreatedPayload>),
    FileModified(TypedRawEvent<FileModifiedPayload>),
    FileDeleted(TypedRawEvent<FileDeletedPayload>),
    FileMoved(TypedRawEvent<FileMovedPayload>),
    DirCreated(TypedRawEvent<DirCreatedPayload>),
    DirDeleted(TypedRawEvent<DirDeletedPayload>),
    
    // Terminal events
    CommandExecuted(TypedRawEvent<CommandExecutedPayload>),
    CommandCompleted(TypedRawEvent<CommandCompletedPayload>),
    SessionStarted(TypedRawEvent<SessionStartedPayload>),
    SessionEnded(TypedRawEvent<SessionEndedPayload>),
    
    // Clipboard events
    ContentCopied(TypedRawEvent<ClipboardCopiedPayload>),
    ContentSelected(TypedRawEvent<ClipboardSelectedPayload>),
    
    // Window manager events
    WindowOpened(TypedRawEvent<WindowOpenedPayload>),
    WindowClosed(TypedRawEvent<WindowClosedPayload>),
    WindowFocused(TypedRawEvent<WindowFocusedPayload>),
    WorkspaceSwitched(TypedRawEvent<WorkspaceSwitchedPayload>),
    
    // System events
    JournalEntry(TypedRawEvent<JournalEntryPayload>),
    SystemStateChanged(TypedRawEvent<SystemStatePayload>),
    
    // Generic fallback for unknown events
    Unknown(RawEvent),
}

impl EventEnvelope {
    /// Convert to JSON-based RawEvent for database storage
    pub fn to_json_event(self) -> RawEvent {
        match self {
            EventEnvelope::FileCreated(event) => event.to_json_event(),
            EventEnvelope::FileModified(event) => event.to_json_event(),
            EventEnvelope::FileDeleted(event) => event.to_json_event(),
            EventEnvelope::FileMoved(event) => event.to_json_event(),
            EventEnvelope::DirCreated(event) => event.to_json_event(),
            EventEnvelope::DirDeleted(event) => event.to_json_event(),
            EventEnvelope::CommandExecuted(event) => event.to_json_event(),
            EventEnvelope::CommandCompleted(event) => event.to_json_event(),
            EventEnvelope::SessionStarted(event) => event.to_json_event(),
            EventEnvelope::SessionEnded(event) => event.to_json_event(),
            EventEnvelope::ContentCopied(event) => event.to_json_event(),
            EventEnvelope::ContentSelected(event) => event.to_json_event(),
            EventEnvelope::WindowOpened(event) => event.to_json_event(),
            EventEnvelope::WindowClosed(event) => event.to_json_event(),
            EventEnvelope::WindowFocused(event) => event.to_json_event(),
            EventEnvelope::WorkspaceSwitched(event) => event.to_json_event(),
            EventEnvelope::JournalEntry(event) => event.to_json_event(),
            EventEnvelope::SystemStateChanged(event) => event.to_json_event(),
            EventEnvelope::Unknown(event) => event,
        }
    }
    
    /// Get the event ID regardless of type
    pub fn id(&self) -> Ulid {
        match self {
            EventEnvelope::FileCreated(event) => event.id,
            EventEnvelope::FileModified(event) => event.id,
            EventEnvelope::FileDeleted(event) => event.id,
            EventEnvelope::FileMoved(event) => event.id,
            EventEnvelope::DirCreated(event) => event.id,
            EventEnvelope::DirDeleted(event) => event.id,
            EventEnvelope::CommandExecuted(event) => event.id,
            EventEnvelope::CommandCompleted(event) => event.id,
            EventEnvelope::SessionStarted(event) => event.id,
            EventEnvelope::SessionEnded(event) => event.id,
            EventEnvelope::ContentCopied(event) => event.id,
            EventEnvelope::ContentSelected(event) => event.id,
            EventEnvelope::WindowOpened(event) => event.id,
            EventEnvelope::WindowClosed(event) => event.id,
            EventEnvelope::WindowFocused(event) => event.id,
            EventEnvelope::WorkspaceSwitched(event) => event.id,
            EventEnvelope::JournalEntry(event) => event.id,
            EventEnvelope::SystemStateChanged(event) => event.id,
            EventEnvelope::Unknown(event) => event.id,
        }
    }
    
    /// Get the source name regardless of type
    pub fn source(&self) -> &str {
        match self {
            EventEnvelope::FileCreated(event) => &event.source,
            EventEnvelope::FileModified(event) => &event.source,
            EventEnvelope::FileDeleted(event) => &event.source,
            EventEnvelope::FileMoved(event) => &event.source,
            EventEnvelope::DirCreated(event) => &event.source,
            EventEnvelope::DirDeleted(event) => &event.source,
            EventEnvelope::CommandExecuted(event) => &event.source,
            EventEnvelope::CommandCompleted(event) => &event.source,
            EventEnvelope::SessionStarted(event) => &event.source,
            EventEnvelope::SessionEnded(event) => &event.source,
            EventEnvelope::ContentCopied(event) => &event.source,
            EventEnvelope::ContentSelected(event) => &event.source,
            EventEnvelope::WindowOpened(event) => &event.source,
            EventEnvelope::WindowClosed(event) => &event.source,
            EventEnvelope::WindowFocused(event) => &event.source,
            EventEnvelope::WorkspaceSwitched(event) => &event.source,
            EventEnvelope::JournalEntry(event) => &event.source,
            EventEnvelope::SystemStateChanged(event) => &event.source,
            EventEnvelope::Unknown(event) => &event.source,
        }
    }
}

// ============================================================================
// Strongly-Typed Payload Definitions
// ============================================================================

// Filesystem payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCreatedPayload {
    pub path: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub permissions: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileModifiedPayload {
    pub path: String,
    pub size: u64,
    pub modified_at: DateTime<Utc>,
    pub modification_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDeletedPayload {
    pub path: String,
    pub deleted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMovedPayload {
    pub path: String,
    pub old_path: Option<String>,
    pub moved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirCreatedPayload {
    pub path: String,
    pub created_at: DateTime<Utc>,
    pub permissions: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirDeletedPayload {
    pub path: String,
    pub deleted_at: DateTime<Utc>,
}

// Terminal payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandExecutedPayload {
    pub command: String,
    pub working_directory: Option<String>,
    pub exit_status: Option<i32>,
    pub execution_time_ms: Option<u64>,
    pub shell_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandCompletedPayload {
    pub command: String,
    pub command_output: String,
    pub working_directory: Option<String>,
    pub exit_status: Option<i32>,
    pub execution_time_ms: Option<u64>,
    pub output_size_bytes: u64,
    pub output_line_count: u32,
    pub completion_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStartedPayload {
    pub session_id: String,
    pub terminal_type: String,
    pub shell: String,
    pub working_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEndedPayload {
    pub session_id: String,
    pub duration_ms: u64,
    pub commands_executed: u32,
    pub exit_code: Option<i32>,
}

// Clipboard payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardCopiedPayload {
    pub content_type: String,
    pub content_size: u64,
    pub text_preview: Option<String>,
    pub content_hash: Option<String>,
    pub source_app: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardSelectedPayload {
    pub content_type: String,
    pub content_size: u64,
    pub text_preview: Option<String>,
    pub selection_type: String,
}

// Window manager payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowOpenedPayload {
    pub window_address: String,
    pub window_class: String,
    pub window_title: String,
    pub workspace_id: String,
    pub opened_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowClosedPayload {
    pub window_address: String,
    pub window_class: String,
    pub window_title: String,
    pub workspace_id: String,
    pub closed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFocusedPayload {
    pub window_address: String,
    pub window_class: String,
    pub window_title: String,
    pub workspace_id: String,
    pub focused_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSwitchedPayload {
    pub workspace_id: String,
    pub workspace_name: String,
    pub previous_workspace_id: Option<String>,
    pub switched_at: DateTime<Utc>,
}

// System payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntryPayload {
    pub message: String,
    pub priority: Option<u8>,
    pub unit: Option<String>,
    pub pid: Option<u32>,
    pub cursor: Option<String>,
    pub fields: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatePayload {
    pub state_type: String,
    pub state_data: serde_json::Value,
    pub changed_at: DateTime<Utc>,
}

// ============================================================================
// Type-Safe Event Builder
// ============================================================================

/// Builder for creating strongly-typed events
pub struct TypedEventBuilder<P: Serialize> {
    source: String,
    event_type: String,
    payload: P,
    host: Option<String>,
    ingestor_version: Option<String>,
    ts_orig: Option<DateTime<Utc>>,
}

impl<P: Serialize> TypedEventBuilder<P> {
    pub fn new(source: impl Into<String>, event_type: impl Into<String>, payload: P) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload,
            host: None,
            ingestor_version: None,
            ts_orig: None,
        }
    }
    
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }
    
    pub fn with_ingestor_version(mut self, version: impl Into<String>) -> Self {
        self.ingestor_version = Some(version.into());
        self
    }
    
    pub fn with_orig_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.ts_orig = Some(timestamp);
        self
    }
    
    pub fn build(self) -> TypedRawEvent<P> {
        TypedRawEvent {
            id: Ulid::new(),
            source: self.source,
            event_type: self.event_type,
            payload: self.payload,
            host: self.host.unwrap_or_else(|| {
                gethostname::gethostname().to_string_lossy().to_string()
            }),
            ingestor_version: self.ingestor_version.unwrap_or_else(|| {
                env!("CARGO_PKG_VERSION").to_string()
            }),
            ts_ingest: Utc::now(),
            ts_orig: self.ts_orig,
        }
    }
}

// ============================================================================
// Type-Safe Channel Types
// ============================================================================

/// Type-safe event sender
pub type TypedEventSender = tokio::sync::mpsc::UnboundedSender<EventEnvelope>;

/// Type-safe event receiver
pub type TypedEventReceiver = tokio::sync::mpsc::UnboundedReceiver<EventEnvelope>;

/// Create a type-safe event channel
pub fn typed_event_channel() -> (TypedEventSender, TypedEventReceiver) {
    tokio::sync::mpsc::unbounded_channel()
}

// ============================================================================
// Typed Event Pipeline Enforcement
// ============================================================================

/// Adapter to enforce typed event pipeline while maintaining EventSource compatibility
pub struct TypedEventPipelineAdapter {
    json_tx: EventSender,
}

impl TypedEventPipelineAdapter {
    /// Create new adapter that enforces typed events
    pub fn new(json_tx: EventSender) -> Self {
        Self { json_tx }
    }

    /// Run adapter loop converting typed events to JSON for legacy systems
    pub async fn run_adapter(self, mut typed_rx: TypedEventReceiver) -> TypedEventResult<()> {
        while let Some(envelope) = typed_rx.recv().await {
            // Convert typed event to JSON for database/legacy systems
            let json_event = envelope.to_json_event();
            
            // Send to legacy pipeline
            self.json_tx.send(json_event).await
                .map_err(|e| TypedEventError::ChannelSend(format!("Failed to send converted event: {}", e)))?;
        }
        Ok(())
    }
}

/// Trait for sources that produce strongly-typed events (enforcement mechanism)
/// Note: This is a simplified version that doesn't depend on sinex-core types
#[async_trait::async_trait]
pub trait EnforcedTypedEventSource: Send + Sync + 'static {
    /// Configuration type for this source
    type Config: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static;

    /// Canonical source name
    const SOURCE_NAME: &'static str;

    /// Initialize the source with config value
    async fn initialize(config: serde_json::Value) -> TypedEventResult<Self>
    where
        Self: Sized;

    /// Stream ONLY typed events (enforcement: no RawEvent allowed)
    async fn stream_typed_events(&mut self, tx: TypedEventSender) -> TypedEventResult<()>;

    /// Graceful shutdown
    async fn shutdown(&mut self) -> TypedEventResult<()> {
        Ok(())
    }
}

// Note: TypedSourceAdapter removed to avoid circular dependency with sinex-core
// It can be re-implemented in sinex-core if needed, using the TypedEventPipelineAdapter

// ============================================================================
// Event Builder Helpers
// ============================================================================

/// Helper methods for creating typed filesystem events
pub struct TypedFilesystemEventBuilder {
    source: String,
}

impl TypedFilesystemEventBuilder {
    pub fn new(source: impl Into<String>) -> Self {
        Self { source: source.into() }
    }
    
    pub fn file_created(self, path: impl Into<String>, size: u64, permissions: Option<u32>) -> EventEnvelope {
        let payload = FileCreatedPayload {
            path: path.into(),
            size,
            created_at: Utc::now(),
            permissions,
        };
        
        let event = TypedEventBuilder::new(self.source, "file.created", payload).build();
        EventEnvelope::FileCreated(event)
    }
    
    pub fn file_modified(self, path: impl Into<String>, size: u64, modification_type: impl Into<String>) -> EventEnvelope {
        let payload = FileModifiedPayload {
            path: path.into(),
            size,
            modified_at: Utc::now(),
            modification_type: modification_type.into(),
        };
        
        let event = TypedEventBuilder::new(self.source, "file.modified", payload).build();
        EventEnvelope::FileModified(event)
    }
    
    pub fn file_deleted(self, path: impl Into<String>) -> EventEnvelope {
        let payload = FileDeletedPayload {
            path: path.into(),
            deleted_at: Utc::now(),
        };
        
        let event = TypedEventBuilder::new(self.source, "file.deleted", payload).build();
        EventEnvelope::FileDeleted(event)
    }
    
    pub fn file_moved(self, path: impl Into<String>, old_path: Option<String>) -> EventEnvelope {
        let payload = FileMovedPayload {
            path: path.into(),
            old_path,
            moved_at: Utc::now(),
        };
        
        let event = TypedEventBuilder::new(self.source, "file.moved", payload).build();
        EventEnvelope::FileMoved(event)
    }
    
    pub fn dir_created(self, path: impl Into<String>, permissions: Option<u32>) -> EventEnvelope {
        let payload = DirCreatedPayload {
            path: path.into(),
            created_at: Utc::now(),
            permissions,
        };
        
        let event = TypedEventBuilder::new(self.source, "dir.created", payload).build();
        EventEnvelope::DirCreated(event)
    }
    
    pub fn dir_deleted(self, path: impl Into<String>) -> EventEnvelope {
        let payload = DirDeletedPayload {
            path: path.into(),
            deleted_at: Utc::now(),
        };
        
        let event = TypedEventBuilder::new(self.source, "dir.deleted", payload).build();
        EventEnvelope::DirDeleted(event)
    }
}

/// Helper methods for creating typed terminal events
pub struct TypedTerminalEventBuilder {
    source: String,
}

impl TypedTerminalEventBuilder {
    pub fn new(source: impl Into<String>) -> Self {
        Self { source: source.into() }
    }
    
    pub fn command_executed(
        self,
        command: impl Into<String>,
        working_directory: Option<String>,
        exit_status: Option<i32>,
        execution_time_ms: Option<u64>,
        shell_type: Option<String>,
    ) -> EventEnvelope {
        let payload = CommandExecutedPayload {
            command: command.into(),
            working_directory,
            exit_status,
            execution_time_ms,
            shell_type,
        };
        
        let event = TypedEventBuilder::new(self.source, "command.executed", payload).build();
        EventEnvelope::CommandExecuted(event)
    }
    
    pub fn session_started(
        self,
        session_id: impl Into<String>,
        terminal_type: impl Into<String>,
        shell: impl Into<String>,
        working_directory: impl Into<String>,
    ) -> EventEnvelope {
        let payload = SessionStartedPayload {
            session_id: session_id.into(),
            terminal_type: terminal_type.into(),
            shell: shell.into(),
            working_directory: working_directory.into(),
        };
        
        let event = TypedEventBuilder::new(self.source, "session.started", payload).build();
        EventEnvelope::SessionStarted(event)
    }
}

/// Helper methods for creating typed clipboard events
pub struct TypedClipboardEventBuilder {
    source: String,
}

impl TypedClipboardEventBuilder {
    pub fn new(source: impl Into<String>) -> Self {
        Self { source: source.into() }
    }
    
    pub fn content_copied(
        self,
        content_type: impl Into<String>,
        content_size: u64,
        text_preview: Option<String>,
        content_hash: Option<String>,
        source_app: Option<String>,
    ) -> EventEnvelope {
        let payload = ClipboardCopiedPayload {
            content_type: content_type.into(),
            content_size,
            text_preview,
            content_hash,
            source_app,
        };
        
        let event = TypedEventBuilder::new(self.source, "copied", payload).build();
        EventEnvelope::ContentCopied(event)
    }
}

// ============================================================================
// Migration Adapters
// ============================================================================

/// Adapter to convert typed events to JSON events during migration
pub struct TypedToJsonAdapter {
    typed_rx: TypedEventReceiver,
    json_tx: EventSender,
}

impl TypedToJsonAdapter {
    pub fn new(typed_rx: TypedEventReceiver, json_tx: EventSender) -> Self {
        Self { typed_rx, json_tx }
    }
    
    /// Run the adapter, converting typed events to JSON events
    pub async fn run(mut self) -> TypedEventResult<()> {
        while let Some(envelope) = self.typed_rx.recv().await {
            let json_event = envelope.to_json_event();
            if let Err(e) = self.json_tx.send(json_event).await {
                return Err(TypedEventError::ChannelSend(format!("Failed to send converted event: {}", e)));
            }
        }
        Ok(())
    }
}

// Note: LegacyEventSourceAdapter removed to avoid circular dependency with sinex-core
// It can be re-implemented in sinex-core if needed, using the TypedToJsonAdapter

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources;

    #[test]
    fn test_typed_event_creation() {
        let payload = FileCreatedPayload {
            path: "/test.txt".to_string(),
            size: 1024,
            created_at: Utc::now(),
            permissions: Some(0o644),
        };
        
        let event = TypedEventBuilder::new(sources::FS, "file.created", payload)
            .with_host("test-host")
            .build();
        
        assert_eq!(event.source, sources::FS);
        assert_eq!(event.event_type, "file.created");
        assert_eq!(event.payload.path, "/test.txt");
        assert_eq!(event.payload.size, 1024);
    }
    
    #[test]
    fn test_event_envelope_conversion() {
        let payload = CommandExecutedPayload {
            command: "ls -la".to_string(),
            working_directory: Some("/home/user".to_string()),
            exit_status: Some(0),
            execution_time_ms: Some(150),
            shell_type: Some("bash".to_string()),
        };
        
        let typed_event = TypedEventBuilder::new("terminal.kitty", "command.executed", payload)
            .build();
        
        let envelope = EventEnvelope::CommandExecuted(typed_event);
        let json_event = envelope.to_json_event();
        
        assert_eq!(json_event.source, "terminal.kitty");
        assert_eq!(json_event.event_type, "command.executed");
        assert_eq!(json_event.payload["command"], "ls -la");
        assert_eq!(json_event.payload["exit_status"], 0);
    }
    
    #[test]
    fn test_type_safety() {
        // This should compile - correct payload for file.created
        let _file_event = TypedEventBuilder::new(
            sources::FS, 
            "file.created", 
            FileCreatedPayload {
                path: "/test".to_string(),
                size: 0,
                created_at: Utc::now(),
                permissions: None,
            }
        ).build();
        
        // The type system prevents mismatched payloads at compile time
        // This would not compile:
        // let _wrong_event = TypedEventBuilder::new(
        //     sources::FS, 
        //     "file.created", 
        //     CommandExecutedPayload { ... }  // Wrong payload type!
        // ).build();
    }
}