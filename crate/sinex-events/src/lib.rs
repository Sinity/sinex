//! Core Event Types and Builders
//!
//! This crate provides the fundamental event types and builders used throughout
//! the Sinex system, extracted from sinex-core for focused responsibility.

pub mod constants;
pub mod event_builders;
pub mod raw_event;
pub mod strongly_typed_events;

// Re-export core event types
pub use raw_event::{JsonValue, OptionalTimestamp, RawEvent, Timestamp};

// Re-export event builders
pub use event_builders::{
    ClipboardContentType, ClipboardEventBuilder, EventFactory, FileOperation,
    FilesystemEventBuilder, SystemEventBuilder, TerminalEventBuilder, WindowManagerEventBuilder,
    WindowManagerEventType,
};

// Re-export strongly typed events
pub use strongly_typed_events::{
    typed_event_channel, AtuinEntryPayload, ClipboardCopiedPayload, ClipboardSelectedPayload,
    CommandCompletedPayload, CommandExecutedPayload, CommandImportedPayload, DirCreatedPayload,
    DirDeletedPayload, EnforcedTypedEventSource, EventEnvelope, FileCreatedPayload,
    FileDeletedPayload, FileModifiedPayload, FileMovedPayload, JournalEntryPayload,
    ProcessHeartbeatPayload, ProcessShutdownPayload, ProcessStartedPayload, ScanCompletedPayload,
    ScanStartedPayload, SensorActivatedPayload, SensorDeactivatedPayload, SessionEndedPayload,
    SessionStartedPayload, SystemStatePayload, TypedClipboardEventBuilder, TypedEventBuilder,
    TypedEventError, TypedEventPipelineAdapter, TypedEventReceiver, TypedEventResult,
    TypedEventSender, TypedFilesystemEventBuilder, TypedRawEvent, TypedTerminalEventBuilder,
    TypedToJsonAdapter, WindowClosedPayload, WindowFocusedPayload, WindowOpenedPayload,
    WorkspaceSwitchedPayload,
};

// Re-export all constants for convenient access
pub use constants::{event_types, sources, services, paths, config, test_constants, git_annex};

// Common type aliases
pub type EventSender = tokio::sync::mpsc::Sender<RawEvent>;
pub type EventReceiver = tokio::sync::mpsc::Receiver<RawEvent>;

// Agent status and error types
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AutomatonStatus {
    Starting,
    Running,
    Stopping,
    Error,
}

// Legacy alias for compatibility
pub type AgentStatus = AutomatonStatus;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ErrorSeverity {
    Warning,
    Error,
    Critical,
}
