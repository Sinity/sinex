//! Unified event builder hierarchy for test events
//!
//! This module provides a fluent, type-safe API for creating test events
//! across all domains (filesystem, terminal, clipboard, window manager).
//! 
//! The builder pattern enforces correct event structure while providing
//! domain-specific convenience methods.

use crate::common::prelude::*;
use serde_json::json;
use chrono::{DateTime, Utc};

/// Main entry point for creating test events
pub struct EventBuilder;

impl EventBuilder {
    /// Create a generic event builder (requires source and event type)
    pub fn generic(source: impl Into<String>, event_type: impl Into<String>) -> GenericEventBuilder {
        GenericEventBuilder::new(source, event_type)
    }
    
    /// Create a filesystem event builder
    pub fn filesystem() -> FilesystemEventBuilder {
        FilesystemEventBuilder::new()
    }
    
    /// Create a terminal event builder
    pub fn terminal() -> TerminalEventBuilder {
        TerminalEventBuilder::new()
    }
    
    /// Create a clipboard event builder
    pub fn clipboard() -> ClipboardEventBuilder {
        ClipboardEventBuilder::new()
    }
    
    /// Create a window manager event builder
    pub fn hyprland() -> HyprlandEventBuilder {
        HyprlandEventBuilder::new()
    }
    
    /// Create a sinex agent event builder
    pub fn agent() -> AgentEventBuilder {
        AgentEventBuilder::new()
    }
}

// ===== Generic Event Builder =====

/// Generic event builder - simple and straightforward
pub struct GenericEventBuilder {
    source: String,
    event_type: String,
    payload: Value,
    ts_orig: Option<DateTime<Utc>>,
    metadata: Option<Value>,
}

impl GenericEventBuilder {
    /// Create a new generic event builder with source and event type
    pub fn new(source: impl Into<String>, event_type: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload: json!({}),
            ts_orig: None,
            metadata: None,
        }
    }
    
    /// Set the payload
    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }
    
    /// Set the original timestamp
    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.ts_orig = Some(ts);
        self
    }
    
    /// Set metadata (currently unused, but available for future)
    pub fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
    
    /// Build the event
    pub fn build(self) -> RawEvent {
        let mut builder = RawEventBuilder::new(
            self.source,
            self.event_type,
            self.payload,
        );
        
        if let Some(ts) = self.ts_orig {
            builder = builder.with_orig_timestamp(ts);
        }
        
        builder.build()
    }
}

// ===== Filesystem Event Builder =====

#[derive(Debug)]
pub enum FileOperation {
    Create,
    Modify,
    Delete,
    Move,
    Rename,
}

impl FileOperation {
    fn as_event_type(&self) -> &'static str {
        match self {
            FileOperation::Create => "file.created",
            FileOperation::Modify => "file.modified",
            FileOperation::Delete => "file.deleted",
            FileOperation::Move => "file.moved",
            FileOperation::Rename => "file.renamed",
        }
    }
}

pub struct FilesystemEventBuilder {
    path: Option<String>,
    operation: Option<FileOperation>,
    size: Option<u64>,
    permissions: Option<u32>,
    content_hash: Option<String>,
    old_path: Option<String>, // For move/rename
    metadata: Option<Value>,
    timestamp: Option<DateTime<Utc>>,
}

impl FilesystemEventBuilder {
    pub fn new() -> Self {
        Self {
            path: None,
            operation: None,
            size: None,
            permissions: None,
            content_hash: None,
            old_path: None,
            metadata: None,
            timestamp: None,
        }
    }
    
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
    
    pub fn operation(mut self, op: FileOperation) -> Self {
        self.operation = Some(op);
        self
    }
    
    pub fn created(self) -> Self {
        self.operation(FileOperation::Create)
    }
    
    pub fn modified(self) -> Self {
        self.operation(FileOperation::Modify)
    }
    
    pub fn deleted(self) -> Self {
        self.operation(FileOperation::Delete)
    }
    
    pub fn moved_from(mut self, old_path: impl Into<String>) -> Self {
        self.old_path = Some(old_path.into());
        self.operation(FileOperation::Move)
    }
    
    pub fn renamed_from(mut self, old_path: impl Into<String>) -> Self {
        self.old_path = Some(old_path.into());
        self.operation(FileOperation::Rename)
    }
    
    pub fn size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }
    
    pub fn permissions(mut self, perms: u32) -> Self {
        self.permissions = Some(perms);
        self
    }
    
    pub fn content_hash(mut self, hash: impl Into<String>) -> Self {
        self.content_hash = Some(hash.into());
        self
    }
    
    pub fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
    
    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }
    
    pub fn build(self) -> RawEvent {
        let path = self.path.unwrap_or_else(|| "/test/file.txt".to_string());
        let operation = self.operation.unwrap_or(FileOperation::Create);
        
        let mut payload = json!({
            "path": path,
            "operation": format!("{:?}", operation).to_lowercase(),
        });
        
        if let Some(size) = self.size {
            payload["size"] = json!(size);
        }
        
        if let Some(perms) = self.permissions {
            payload["permissions"] = json!(format!("{:o}", perms));
        }
        
        if let Some(hash) = self.content_hash {
            payload["content_hash"] = json!(hash);
        }
        
        if let Some(old_path) = self.old_path {
            payload["old_path"] = json!(old_path);
        }
        
        if let Some(metadata) = self.metadata {
            payload["metadata"] = metadata;
        }
        
        let mut builder = RawEventBuilder::new(
            sources::FILESYSTEM,
            operation.as_event_type(),
            payload,
        );
        
        if let Some(ts) = self.timestamp {
            builder = builder.with_orig_timestamp(ts);
        }
        
        builder.build()
    }
}

// ===== Terminal Event Builder =====

pub struct TerminalEventBuilder {
    session_id: Option<Ulid>,
    command: Option<String>,
    exit_code: Option<i32>,
    duration_ms: Option<u64>,
    working_dir: Option<String>,
    environment: Option<HashMap<String, String>>,
    timestamp: Option<DateTime<Utc>>,
}

impl TerminalEventBuilder {
    pub fn new() -> Self {
        Self {
            session_id: None,
            command: None,
            exit_code: None,
            duration_ms: None,
            working_dir: None,
            environment: None,
            timestamp: None,
        }
    }
    
    pub fn session_id(mut self, id: Ulid) -> Self {
        self.session_id = Some(id);
        self
    }
    
    pub fn command(mut self, cmd: impl Into<String>) -> Self {
        self.command = Some(cmd.into());
        self
    }
    
    pub fn exit_code(mut self, code: i32) -> Self {
        self.exit_code = Some(code);
        self
    }
    
    pub fn success(self) -> Self {
        self.exit_code(0)
    }
    
    pub fn failed(self, code: i32) -> Self {
        self.exit_code(code)
    }
    
    pub fn duration_ms(mut self, ms: u64) -> Self {
        self.duration_ms = Some(ms);
        self
    }
    
    pub fn working_dir(mut self, dir: impl Into<String>) -> Self {
        self.working_dir = Some(dir.into());
        self
    }
    
    pub fn env_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let env = self.environment.get_or_insert_with(HashMap::new);
        env.insert(key.into(), value.into());
        self
    }
    
    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }
    
    pub fn build(self) -> RawEvent {
        let session_id = self.session_id.unwrap_or_else(Ulid::new);
        let command = self.command.unwrap_or_else(|| "echo test".to_string());
        
        let mut payload = json!({
            "session_id": session_id.to_string(),
            "command": command,
            "exit_code": self.exit_code.unwrap_or(0),
        });
        
        if let Some(ms) = self.duration_ms {
            payload["duration_ms"] = json!(ms);
        }
        
        if let Some(dir) = self.working_dir {
            payload["working_dir"] = json!(dir);
        }
        
        if let Some(env) = self.environment {
            payload["environment"] = json!(env);
        }
        
        let mut builder = RawEventBuilder::new(
            sources::TERMINAL_KITTY,
            "command.executed",
            payload,
        );
        
        if let Some(ts) = self.timestamp {
            builder = builder.with_orig_timestamp(ts);
        }
        
        builder.build()
    }
}

// ===== Clipboard Event Builder =====

#[derive(Debug, Clone)]
pub enum ClipboardContentType {
    Text,
    Html,
    Image,
    Files,
    Custom(String),
}

impl ClipboardContentType {
    fn as_str(&self) -> &str {
        match self {
            ClipboardContentType::Text => "text/plain",
            ClipboardContentType::Html => "text/html",
            ClipboardContentType::Image => "image/png",
            ClipboardContentType::Files => "text/uri-list",
            ClipboardContentType::Custom(s) => s,
        }
    }
}

pub struct ClipboardEventBuilder {
    content: Option<String>,
    content_type: Option<ClipboardContentType>,
    source_app: Option<String>,
    clipboard_type: Option<String>, // "clipboard" or "primary"
    blob_ref: Option<String>,
    timestamp: Option<DateTime<Utc>>,
}

impl ClipboardEventBuilder {
    pub fn new() -> Self {
        Self {
            content: None,
            content_type: None,
            source_app: None,
            clipboard_type: None,
            blob_ref: None,
            timestamp: None,
        }
    }
    
    pub fn content(mut self, content: impl Into<String>) -> Self {
        self.content = Some(content.into());
        self
    }
    
    pub fn content_type(mut self, ct: ClipboardContentType) -> Self {
        self.content_type = Some(ct);
        self
    }
    
    pub fn text(self, text: impl Into<String>) -> Self {
        self.content(text).content_type(ClipboardContentType::Text)
    }
    
    pub fn html(self, html: impl Into<String>) -> Self {
        self.content(html).content_type(ClipboardContentType::Html)
    }
    
    pub fn image_ref(mut self, blob_ref: impl Into<String>) -> Self {
        self.blob_ref = Some(blob_ref.into());
        self.content_type = Some(ClipboardContentType::Image);
        self
    }
    
    pub fn source_app(mut self, app: impl Into<String>) -> Self {
        self.source_app = Some(app.into());
        self
    }
    
    pub fn from_primary(mut self) -> Self {
        self.clipboard_type = Some("primary".to_string());
        self
    }
    
    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }
    
    pub fn build(self) -> RawEvent {
        let content = self.content.unwrap_or_else(|| "test clipboard content".to_string());
        let content_type = self.content_type.unwrap_or(ClipboardContentType::Text);
        
        let mut payload = json!({
            "content": content,
            "content_type": content_type.as_str(),
            "clipboard_type": self.clipboard_type.unwrap_or_else(|| "clipboard".to_string()),
        });
        
        if let Some(app) = self.source_app {
            payload["source_app"] = json!(app);
        }
        
        if let Some(blob_ref) = self.blob_ref {
            payload["blob_ref"] = json!(blob_ref);
        }
        
        let mut builder = RawEventBuilder::new(
            sources::CLIPBOARD,
            "content.changed",
            payload,
        );
        
        if let Some(ts) = self.timestamp {
            builder = builder.with_orig_timestamp(ts);
        }
        
        builder.build()
    }
}

// ===== Hyprland Event Builder =====

#[derive(Debug)]
pub enum HyprlandEventType {
    WindowCreated,
    WindowDestroyed,
    WindowFocused,
    WindowMoved,
    WorkspaceChanged,
    MonitorAdded,
    Custom(String),
}

impl HyprlandEventType {
    fn as_str(&self) -> &str {
        match self {
            HyprlandEventType::WindowCreated => "window.created",
            HyprlandEventType::WindowDestroyed => "window.destroyed",
            HyprlandEventType::WindowFocused => "window.focused",
            HyprlandEventType::WindowMoved => "window.moved",
            HyprlandEventType::WorkspaceChanged => "workspace.changed",
            HyprlandEventType::MonitorAdded => "monitor.added",
            HyprlandEventType::Custom(s) => s,
        }
    }
}

pub struct HyprlandEventBuilder {
    event_type: Option<HyprlandEventType>,
    window_id: Option<String>,
    window_class: Option<String>,
    window_title: Option<String>,
    workspace_id: Option<i32>,
    monitor_id: Option<i32>,
    geometry: Option<(i32, i32, i32, i32)>, // x, y, width, height
    custom_data: Option<Value>,
    timestamp: Option<DateTime<Utc>>,
}

impl HyprlandEventBuilder {
    pub fn new() -> Self {
        Self {
            event_type: None,
            window_id: None,
            window_class: None,
            window_title: None,
            workspace_id: None,
            monitor_id: None,
            geometry: None,
            custom_data: None,
            timestamp: None,
        }
    }
    
    pub fn event_type(mut self, et: HyprlandEventType) -> Self {
        self.event_type = Some(et);
        self
    }
    
    pub fn window_created(self) -> Self {
        self.event_type(HyprlandEventType::WindowCreated)
    }
    
    pub fn window_destroyed(self) -> Self {
        self.event_type(HyprlandEventType::WindowDestroyed)
    }
    
    pub fn window_focused(self) -> Self {
        self.event_type(HyprlandEventType::WindowFocused)
    }
    
    pub fn window_id(mut self, id: impl Into<String>) -> Self {
        self.window_id = Some(id.into());
        self
    }
    
    pub fn window_class(mut self, class: impl Into<String>) -> Self {
        self.window_class = Some(class.into());
        self
    }
    
    pub fn window_title(mut self, title: impl Into<String>) -> Self {
        self.window_title = Some(title.into());
        self
    }
    
    pub fn workspace(mut self, id: i32) -> Self {
        self.workspace_id = Some(id);
        self
    }
    
    pub fn monitor(mut self, id: i32) -> Self {
        self.monitor_id = Some(id);
        self
    }
    
    pub fn geometry(mut self, x: i32, y: i32, width: i32, height: i32) -> Self {
        self.geometry = Some((x, y, width, height));
        self
    }
    
    pub fn custom_data(mut self, data: Value) -> Self {
        self.custom_data = Some(data);
        self
    }
    
    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }
    
    pub fn build(self) -> RawEvent {
        let event_type = self.event_type.unwrap_or(HyprlandEventType::WindowCreated);
        
        let mut payload = json!({});
        
        if let Some(id) = self.window_id {
            payload["window_id"] = json!(id);
        }
        
        if let Some(class) = self.window_class {
            payload["window_class"] = json!(class);
        }
        
        if let Some(title) = self.window_title {
            payload["window_title"] = json!(title);
        }
        
        if let Some(ws_id) = self.workspace_id {
            payload["workspace_id"] = json!(ws_id);
        }
        
        if let Some(mon_id) = self.monitor_id {
            payload["monitor_id"] = json!(mon_id);
        }
        
        if let Some((x, y, w, h)) = self.geometry {
            payload["geometry"] = json!({
                "x": x,
                "y": y,
                "width": w,
                "height": h,
            });
        }
        
        if let Some(custom) = self.custom_data {
            for (k, v) in custom.as_object().unwrap_or(&serde_json::Map::new()) {
                payload[k] = v.clone();
            }
        }
        
        let mut builder = RawEventBuilder::new(
            sources::HYPRLAND,
            event_type.as_str(),
            payload,
        );
        
        if let Some(ts) = self.timestamp {
            builder = builder.with_orig_timestamp(ts);
        }
        
        builder.build()
    }
}

// ===== Agent Event Builder =====

#[derive(Debug)]
pub enum AgentStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Error,
}

impl AgentStatus {
    fn as_str(&self) -> &str {
        match self {
            AgentStatus::Starting => "starting",
            AgentStatus::Running => "running",
            AgentStatus::Stopping => "stopping",
            AgentStatus::Stopped => "stopped",
            AgentStatus::Error => "error",
        }
    }
}

pub struct AgentEventBuilder {
    agent_name: Option<String>,
    event_type: Option<String>,
    status: Option<AgentStatus>,
    version: Option<String>,
    uptime_seconds: Option<u64>,
    events_processed: Option<u64>,
    error_message: Option<String>,
    metadata: Option<Value>,
    timestamp: Option<DateTime<Utc>>,
}

impl AgentEventBuilder {
    pub fn new() -> Self {
        Self {
            agent_name: None,
            event_type: None,
            status: None,
            version: None,
            uptime_seconds: None,
            events_processed: None,
            error_message: None,
            metadata: None,
            timestamp: None,
        }
    }
    
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.agent_name = Some(name.into());
        self
    }
    
    pub fn heartbeat(mut self) -> Self {
        self.event_type = Some("agent.heartbeat".to_string());
        self.status = Some(AgentStatus::Running);
        self
    }
    
    pub fn startup(mut self) -> Self {
        self.event_type = Some("agent.startup".to_string());
        self.status = Some(AgentStatus::Starting);
        self
    }
    
    pub fn shutdown(mut self) -> Self {
        self.event_type = Some("agent.shutdown".to_string());
        self.status = Some(AgentStatus::Stopped);
        self
    }
    
    pub fn error(mut self, message: impl Into<String>) -> Self {
        self.event_type = Some("agent.error".to_string());
        self.status = Some(AgentStatus::Error);
        self.error_message = Some(message.into());
        self
    }
    
    pub fn status(mut self, status: AgentStatus) -> Self {
        self.status = Some(status);
        self
    }
    
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }
    
    pub fn uptime_seconds(mut self, seconds: u64) -> Self {
        self.uptime_seconds = Some(seconds);
        self
    }
    
    pub fn events_processed(mut self, count: u64) -> Self {
        self.events_processed = Some(count);
        self
    }
    
    pub fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
    
    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }
    
    pub fn build(self) -> RawEvent {
        let agent_name = self.agent_name.unwrap_or_else(|| "test_agent".to_string());
        let event_type = self.event_type.unwrap_or_else(|| "agent.heartbeat".to_string());
        
        let mut payload = json!({
            "agent_name": agent_name,
            "status": self.status.unwrap_or(AgentStatus::Running).as_str(),
            "version": self.version.unwrap_or_else(|| "1.0.0".to_string()),
        });
        
        if let Some(uptime) = self.uptime_seconds {
            payload["uptime_seconds"] = json!(uptime);
        }
        
        if let Some(events) = self.events_processed {
            payload["events_processed_session"] = json!(events);
        }
        
        if let Some(error) = self.error_message {
            payload["error_message"] = json!(error);
        }
        
        if let Some(metadata) = self.metadata {
            payload["metadata"] = metadata;
        }
        
        let mut builder = RawEventBuilder::new(
            sources::SINEX,
            &event_type,
            payload,
        );
        
        if let Some(ts) = self.timestamp {
            builder = builder.with_orig_timestamp(ts);
        }
        
        builder.build()
    }
}

// ===== Quick Event Creation =====

/// Quick event creation functions for common test scenarios
pub mod quick {
    use super::*;
    
    /// Quick filesystem created event
    pub fn fs_created(path: &str) -> RawEvent {
        EventBuilder::filesystem()
            .path(path)
            .created()
            .build()
    }
    
    /// Quick filesystem modified event
    pub fn fs_modified(path: &str) -> RawEvent {
        EventBuilder::filesystem()
            .path(path)
            .modified()
            .build()
    }
    
    /// Quick filesystem deleted event
    pub fn fs_deleted(path: &str) -> RawEvent {
        EventBuilder::filesystem()
            .path(path)
            .deleted()
            .build()
    }
    
    /// Quick terminal command event
    pub fn terminal_cmd(cmd: &str) -> RawEvent {
        EventBuilder::terminal()
            .command(cmd)
            .success()
            .build()
    }
    
    /// Quick terminal command with exit code
    pub fn terminal_cmd_failed(cmd: &str, exit_code: i32) -> RawEvent {
        EventBuilder::terminal()
            .command(cmd)
            .failed(exit_code)
            .build()
    }
    
    /// Quick clipboard text event
    pub fn clipboard_text(content: &str) -> RawEvent {
        EventBuilder::clipboard()
            .text(content)
            .build()
    }
    
    /// Quick window created event
    pub fn window_created(title: &str) -> RawEvent {
        EventBuilder::hyprland()
            .window_created()
            .window_title(title)
            .build()
    }
    
    /// Quick window focused event
    pub fn window_focused(title: &str) -> RawEvent {
        EventBuilder::hyprland()
            .window_focused()
            .window_title(title)
            .build()
    }
    
    /// Quick agent heartbeat event
    pub fn agent_heartbeat(name: &str) -> RawEvent {
        EventBuilder::agent()
            .name(name)
            .heartbeat()
            .build()
    }
    
    /// Quick agent error event
    pub fn agent_error(name: &str, error: &str) -> RawEvent {
        EventBuilder::agent()
            .name(name)
            .error(error)
            .build()
    }
}

// ===== Batch Event Creation =====

/// Batch event creation for performance and stress testing
pub mod batch {
    use super::*;
    
    /// Create multiple filesystem events for different paths
    pub fn fs_events(paths: &[&str]) -> Vec<RawEvent> {
        paths.iter()
            .map(|path| quick::fs_created(path))
            .collect()
    }
    
    /// Create a sequence of terminal commands
    pub fn terminal_sequence(commands: &[&str]) -> Vec<RawEvent> {
        commands.iter()
            .map(|cmd| quick::terminal_cmd(cmd))
            .collect()
    }
    
    /// Create mixed event types for testing
    pub fn mixed_events(count: usize) -> Vec<RawEvent> {
        (0..count)
            .map(|i| match i % 4 {
                0 => quick::fs_created(&format!("/test/file_{}.txt", i)),
                1 => quick::terminal_cmd(&format!("command_{}", i)),
                2 => quick::clipboard_text(&format!("clipboard_{}", i)),
                _ => quick::agent_heartbeat(&format!("agent_{}", i)),
            })
            .collect()
    }
    
    /// Create events with timestamps distributed over time
    pub fn time_distributed_events(
        count: usize,
        start_time: DateTime<Utc>,
        interval: Duration,
    ) -> Vec<RawEvent> {
        (0..count)
            .map(|i| {
                let ts = start_time + chrono::Duration::from_std(interval * i as u32).unwrap();
                EventBuilder::generic("test", "timed.event")
                    .payload(json!({"sequence": i}))
                    .timestamp(ts)
                    .build()
            })
            .collect()
    }
    
    /// Create burst pattern events
    pub fn burst_events(bursts: usize, events_per_burst: usize) -> Vec<RawEvent> {
        let mut events = Vec::new();
        let base_time = Utc::now();
        
        for burst in 0..bursts {
            let burst_time = base_time + chrono::Duration::minutes(burst as i64);
            for i in 0..events_per_burst {
                let event = EventBuilder::generic("burst", "test.event")
                    .payload(json!({
                        "burst": burst,
                        "index": i,
                    }))
                    .timestamp(burst_time + chrono::Duration::milliseconds(i as i64))
                    .build();
                events.push(event);
            }
        }
        
        events
    }
}

// ===== Invalid Event Creation =====

/// Create invalid events for error testing
pub mod invalid {
    use super::*;
    
    /// Event missing source
    pub fn missing_source() -> RawEvent {
        RawEvent {
            id: Ulid::new(),
            source: "".to_string(), // Invalid empty source
            event_type: "test.event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "test".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        }
    }
    
    /// Event missing event_type
    pub fn missing_event_type() -> RawEvent {
        RawEvent {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "".to_string(), // Invalid empty event_type
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "test".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        }
    }
    
    /// Event with huge payload
    pub fn huge_payload(mb: usize) -> RawEvent {
        let data = "x".repeat(mb * 1024 * 1024);
        EventBuilder::generic("test", "huge.payload")
            .payload(json!({"data": data}))
            .build()
    }
    
    /// Event with deeply nested JSON
    pub fn deeply_nested(depth: usize) -> RawEvent {
        fn create_nested(d: usize) -> Value {
            if d == 0 {
                json!("bottom")
            } else {
                json!({"nested": create_nested(d - 1)})
            }
        }
        
        EventBuilder::generic("test", "nested.json")
            .payload(create_nested(depth))
            .build()
    }
    
    /// Event with malformed JSON characters
    pub fn malformed_json() -> RawEvent {
        EventBuilder::generic("test", "malformed")
            .payload(json!({
                "null_byte": "\u{0000}",
                "control_chars": "\u{0001}\u{0002}\u{0003}",
                "invalid_utf8": "�����",
            }))
            .build()
    }
}

// ===== Convenience Functions (Legacy) =====

/// Quick filesystem event
pub fn fs_event(path: &str) -> RawEvent {
    quick::fs_created(path)
}

/// Quick terminal event
pub fn term_event(cmd: &str) -> RawEvent {
    quick::terminal_cmd(cmd)
}

/// Quick clipboard event
pub fn clip_event(content: &str) -> RawEvent {
    quick::clipboard_text(content)
}

/// Quick window event
pub fn window_event(title: &str) -> RawEvent {
    quick::window_created(title)
}

/// Quick agent heartbeat
pub fn agent_heartbeat(name: &str) -> RawEvent {
    quick::agent_heartbeat(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[sinex_test]
    async fn test_filesystem_builder(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
        let event = EventBuilder::filesystem()
            .path("/home/user/test.txt")
            .created()
            .size(1024)
            .permissions(0o644)
            .build();
            
        pretty_assertions::assert_eq!(event.source, sources::FILESYSTEM);
        pretty_assertions::assert_eq!(event.event_type, "file.created");
        pretty_assertions::assert_eq!(event.payload["path"], "/home/user/test.txt");
        pretty_assertions::assert_eq!(event.payload["size"], 1024);
        Ok(())
    }
    
    #[sinex_test]
    async fn test_terminal_builder(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
        let event = EventBuilder::terminal()
            .command("ls -la")
            .success()
            .duration_ms(150)
            .working_dir("/home/user")
            .build();
            
        pretty_assertions::assert_eq!(event.source, sources::TERMINAL_KITTY);
        pretty_assertions::assert_eq!(event.event_type, "command.executed");
        pretty_assertions::assert_eq!(event.payload["command"], "ls -la");
        pretty_assertions::assert_eq!(event.payload["exit_code"], 0);
        pretty_assertions::assert_eq!(event.payload["duration_ms"], 150);
        Ok(())
    }
    
    #[sinex_test]
    async fn test_generic_builder(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
        let event = EventBuilder::generic("test_source", "test.event")
            .payload(json!({"data": "test"}))
            .build();
            
        pretty_assertions::assert_eq!(event.source, "test_source");
        pretty_assertions::assert_eq!(event.event_type, "test.event");
        Ok(())
    }
}