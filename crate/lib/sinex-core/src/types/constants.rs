//! Common constants for Sinex domain types
//!
//! This module provides constants for frequently used domain values,
//! particularly EventSource and EventType instances.

use crate::types::domain::{EventSource, EventType, ServiceName};
use once_cell::sync::Lazy;

// =============================================================================
// Event Sources - Satellites and Services
// =============================================================================

/// File system watcher satellite
pub static SOURCE_FS_WATCHER: Lazy<EventSource> =
    Lazy::new(|| EventSource::from_static("fs-watcher"));

/// Terminal satellite  
pub static SOURCE_TERMINAL: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("terminal"));

/// Desktop environment satellite
pub static SOURCE_DESKTOP: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("desktop"));

/// System satellite (systemd, dbus, etc.)
pub static SOURCE_SYSTEM: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("system"));

/// Health aggregator automaton
pub static SOURCE_HEALTH: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("health"));

/// Personal knowledge management automaton
pub static SOURCE_PKM: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("pkm"));

/// Content automaton
pub static SOURCE_CONTENT: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("content"));

/// Analytics automaton
pub static SOURCE_ANALYTICS: Lazy<EventSource> =
    Lazy::new(|| EventSource::from_static("analytics"));

/// Search automaton
pub static SOURCE_SEARCH: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("search"));

/// Gateway service
pub static SOURCE_GATEWAY: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("gateway"));

/// Ingestion daemon
pub static SOURCE_INGESTD: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("ingestd"));

/// Document ingestor
pub static SOURCE_DOCUMENT: Lazy<EventSource> = Lazy::new(|| EventSource::from_static("document"));

/// Clipboard events
pub static SOURCE_CLIPBOARD: Lazy<EventSource> =
    Lazy::new(|| EventSource::from_static("clipboard"));

// =============================================================================
// Event Types - File System
// =============================================================================

/// File created event
pub static TYPE_FILE_CREATED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("file.created"));

/// File modified event
pub static TYPE_FILE_MODIFIED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("file.modified"));

/// File deleted event
pub static TYPE_FILE_DELETED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("file.deleted"));

/// File renamed/moved event
pub static TYPE_FILE_RENAMED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("file.renamed"));

/// Directory created event
pub static TYPE_DIR_CREATED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("directory.created"));

/// Directory deleted event
pub static TYPE_DIR_DELETED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("directory.deleted"));

// =============================================================================
// Event Types - Terminal/Shell
// =============================================================================

/// Command executed event
pub static TYPE_COMMAND_EXECUTED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("command.executed"));

/// Command synthesized (canonicalized)
pub static TYPE_COMMAND_SYNTHESIZED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("command.synthesized"));

/// Terminal scrollback captured
pub static TYPE_SCROLLBACK_CAPTURED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("scrollback.captured"));

/// Terminal recording started
pub static TYPE_RECORDING_STARTED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("recording.started"));

/// Terminal recording stopped
pub static TYPE_RECORDING_STOPPED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("recording.stopped"));

// =============================================================================
// Event Types - Desktop/Window
// =============================================================================

/// Window focused event
pub static TYPE_WINDOW_FOCUSED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("window.focused"));

/// Window opened event
pub static TYPE_WINDOW_OPENED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("window.opened"));

/// Window closed event
pub static TYPE_WINDOW_CLOSED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("window.closed"));

/// Clipboard copied event
pub static TYPE_CLIPBOARD_COPIED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("clipboard.copied"));

// =============================================================================
// Event Types - System
// =============================================================================

/// Service started event
pub static TYPE_SERVICE_STARTED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("service.started"));

/// Service stopped event
pub static TYPE_SERVICE_STOPPED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("service.stopped"));

/// Device added event
pub static TYPE_DEVICE_ADDED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("device.added"));

/// Device removed event
pub static TYPE_DEVICE_REMOVED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("device.removed"));

/// System boot event
pub static TYPE_SYSTEM_BOOT: Lazy<EventType> = Lazy::new(|| EventType::from_static("system.boot"));

/// System shutdown event
pub static TYPE_SYSTEM_SHUTDOWN: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("system.shutdown"));

// =============================================================================
// Event Types - Document/Content
// =============================================================================

/// Document ingested event
pub static TYPE_DOCUMENT_INGESTED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("document.ingested"));

/// Document processed event
pub static TYPE_DOCUMENT_PROCESSED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("document.processed"));

/// Content indexed event
pub static TYPE_CONTENT_INDEXED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("content.indexed"));

// =============================================================================
// Event Types - Health/Telemetry
// =============================================================================

/// Health check performed
pub static TYPE_HEALTH_CHECK: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("health.check"));

/// Metrics collected
pub static TYPE_METRICS_COLLECTED: Lazy<EventType> =
    Lazy::new(|| EventType::from_static("metrics.collected"));

// =============================================================================
// Service Names
// =============================================================================

/// Ingestion daemon service
pub static SERVICE_INGESTD: Lazy<ServiceName> = Lazy::new(|| ServiceName::from_static("ingestd"));

/// Gateway service
pub static SERVICE_GATEWAY: Lazy<ServiceName> = Lazy::new(|| ServiceName::from_static("gateway"));

/// File system watcher service
pub static SERVICE_FS_WATCHER: Lazy<ServiceName> =
    Lazy::new(|| ServiceName::from_static("fs-watcher"));

/// Terminal satellite service
pub static SERVICE_TERMINAL: Lazy<ServiceName> = Lazy::new(|| ServiceName::from_static("terminal"));

/// Desktop satellite service  
pub static SERVICE_DESKTOP: Lazy<ServiceName> = Lazy::new(|| ServiceName::from_static("desktop"));

/// System satellite service
pub static SERVICE_SYSTEM: Lazy<ServiceName> = Lazy::new(|| ServiceName::from_static("system"));
