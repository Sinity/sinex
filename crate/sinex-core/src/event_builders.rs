//! Production event builders for consistent event creation
//!
//! This module provides fluent, type-safe APIs for creating events in production code.
//! Based on proven patterns from the test infrastructure, these builders eliminate
//! manual RawEvent construction and ensure consistency across all event sources.

use crate::{RawEventBuilder, JsonValue, event_type_constants};
use chrono::{DateTime, Utc};
use serde_json::json;
use std::collections::HashMap;

/// Main entry point for creating production events
pub struct EventFactory {
    source_name: String,
    host: String,
    ingestor_version: String,
}

impl EventFactory {
    /// Create a new event factory for a specific source
    pub fn new(source_name: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            host: gethostname::gethostname().to_string_lossy().to_string(),
            ingestor_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Create a generic event with manual payload
    pub fn create_event(&self, event_type: &str, payload: JsonValue) -> crate::RawEvent {
        RawEventBuilder::new(&self.source_name, event_type, payload)
            .with_host(&self.host)
            .with_ingestor_version(&self.ingestor_version)
            .build()
    }

    /// Create a filesystem event builder
    pub fn filesystem(&self) -> FilesystemEventBuilder {
        FilesystemEventBuilder::new(&self.source_name, &self.host, &self.ingestor_version)
    }

    /// Create a terminal event builder
    pub fn terminal(&self) -> TerminalEventBuilder {
        TerminalEventBuilder::new(&self.source_name, &self.host, &self.ingestor_version)
    }

    /// Create a clipboard event builder
    pub fn clipboard(&self) -> ClipboardEventBuilder {
        ClipboardEventBuilder::new(&self.source_name, &self.host, &self.ingestor_version)
    }

    /// Create a window manager event builder
    pub fn window_manager(&self) -> WindowManagerEventBuilder {
        WindowManagerEventBuilder::new(&self.source_name, &self.host, &self.ingestor_version)
    }

    /// Create a system event builder
    pub fn system(&self) -> SystemEventBuilder {
        SystemEventBuilder::new(&self.source_name, &self.host, &self.ingestor_version)
    }
}

// ===== Filesystem Event Builder =====

#[derive(Debug)]
pub enum FileOperation {
    Create,
    Modify,
    Delete,
    Move,
}

impl FileOperation {
    fn as_event_type(&self) -> &'static str {
        match self {
            FileOperation::Create => event_type_constants::filesystem::FILE_CREATED,
            FileOperation::Modify => event_type_constants::filesystem::FILE_MODIFIED,
            FileOperation::Delete => event_type_constants::filesystem::FILE_DELETED,
            FileOperation::Move => event_type_constants::filesystem::FILE_MOVED,
        }
    }
}

pub struct FilesystemEventBuilder {
    source_name: String,
    host: String,
    ingestor_version: String,
    path: Option<String>,
    operation: Option<FileOperation>,
    size: Option<u64>,
    permissions: Option<u32>,
    old_path: Option<String>,
    timestamp: Option<DateTime<Utc>>,
}

impl FilesystemEventBuilder {
    pub fn new(source_name: &str, host: &str, ingestor_version: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            host: host.to_string(),
            ingestor_version: ingestor_version.to_string(),
            path: None,
            operation: None,
            size: None,
            permissions: None,
            old_path: None,
            timestamp: None,
        }
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
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

    pub fn operation(mut self, op: FileOperation) -> Self {
        self.operation = Some(op);
        self
    }

    pub fn size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    pub fn permissions(mut self, perms: u32) -> Self {
        self.permissions = Some(perms);
        self
    }

    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn build(self) -> crate::RawEvent {
        let path = self.path.expect("path is required for filesystem events");
        let operation = self.operation.unwrap_or(FileOperation::Create);

        let mut payload = json!({
            "path": path,
        });

        if let Some(size) = self.size {
            payload["size"] = json!(size);
        }

        if let Some(perms) = self.permissions {
            payload["permissions"] = json!(perms);
        }

        if let Some(old_path) = self.old_path {
            payload["old_path"] = json!(old_path);
        }

        // Add timestamps based on operation type
        match operation {
            FileOperation::Create => {
                payload["created_at"] = json!(self.timestamp.unwrap_or_else(Utc::now));
            }
            FileOperation::Modify => {
                payload["modified_at"] = json!(self.timestamp.unwrap_or_else(Utc::now));
            }
            FileOperation::Delete => {
                payload["deleted_at"] = json!(self.timestamp.unwrap_or_else(Utc::now));
            }
            FileOperation::Move => {
                payload["moved_at"] = json!(self.timestamp.unwrap_or_else(Utc::now));
            }
        }

        let mut builder = RawEventBuilder::new(&self.source_name, operation.as_event_type(), payload)
            .with_host(&self.host)
            .with_ingestor_version(&self.ingestor_version);

        if let Some(ts) = self.timestamp {
            builder = builder.with_orig_timestamp(ts);
        }

        builder.build()
    }
}

// ===== Terminal Event Builder =====

pub struct TerminalEventBuilder {
    source_name: String,
    host: String,
    ingestor_version: String,
    command: Option<String>,
    command_output: Option<String>,
    exit_code: Option<i32>,
    duration_ms: Option<u64>,
    working_dir: Option<String>,
    window_id: Option<String>,
    tab_id: Option<String>,
    timestamp: Option<DateTime<Utc>>,
}

impl TerminalEventBuilder {
    pub fn new(source_name: &str, host: &str, ingestor_version: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            host: host.to_string(),
            ingestor_version: ingestor_version.to_string(),
            command: None,
            command_output: None,
            exit_code: None,
            duration_ms: None,
            working_dir: None,
            window_id: None,
            tab_id: None,
            timestamp: None,
        }
    }

    pub fn command(mut self, cmd: impl Into<String>) -> Self {
        self.command = Some(cmd.into());
        self
    }

    pub fn command_output(mut self, output: impl Into<String>) -> Self {
        self.command_output = Some(output.into());
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

    pub fn window_id(mut self, id: impl Into<String>) -> Self {
        self.window_id = Some(id.into());
        self
    }

    pub fn tab_id(mut self, id: impl Into<String>) -> Self {
        self.tab_id = Some(id.into());
        self
    }

    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn build_executed(self) -> crate::RawEvent {
        self.build_with_event_type("command.executed")
    }

    pub fn build_completed(self) -> crate::RawEvent {
        self.build_with_event_type("command.completed")
    }

    fn build_with_event_type(self, event_type: &str) -> crate::RawEvent {
        let command = self.command.unwrap_or_else(|| "unknown".to_string());

        let mut payload = json!({
            "command": command,
        });

        if let Some(output) = self.command_output {
            payload["command_output"] = json!(output);
            payload["output_size_bytes"] = json!(output.len() as u64);
            payload["output_line_count"] = json!(output.lines().count() as u32);
        }

        if let Some(code) = self.exit_code {
            payload["exit_status"] = json!(code);
        }

        if let Some(duration) = self.duration_ms {
            payload["execution_time_ms"] = json!(duration);
        }

        if let Some(dir) = self.working_dir {
            payload["working_directory"] = json!(dir);
        }

        if let Some(window_id) = self.window_id {
            payload["kitty_window_id"] = json!(window_id);
        }

        if let Some(tab_id) = self.tab_id {
            payload["kitty_tab_id"] = json!(tab_id);
        }

        // Add timestamp
        payload["completion_timestamp"] = json!(self.timestamp.unwrap_or_else(Utc::now).to_rfc3339());

        let mut builder = RawEventBuilder::new(&self.source_name, event_type, payload)
            .with_host(&self.host)
            .with_ingestor_version(&self.ingestor_version);

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
}

impl ClipboardContentType {
    fn as_str(&self) -> &str {
        match self {
            ClipboardContentType::Text => "text/plain",
            ClipboardContentType::Html => "text/html",
            ClipboardContentType::Image => "image/png",
            ClipboardContentType::Files => "text/uri-list",
        }
    }
}

pub struct ClipboardEventBuilder {
    source_name: String,
    host: String,
    ingestor_version: String,
    content: Option<String>,
    content_type: Option<ClipboardContentType>,
    content_hash: Option<String>,
    source_app: Option<String>,
    selection_type: Option<String>,
    timestamp: Option<DateTime<Utc>>,
}

impl ClipboardEventBuilder {
    pub fn new(source_name: &str, host: &str, ingestor_version: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            host: host.to_string(),
            ingestor_version: ingestor_version.to_string(),
            content: None,
            content_type: None,
            content_hash: None,
            source_app: None,
            selection_type: None,
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

    pub fn content_hash(mut self, hash: impl Into<String>) -> Self {
        self.content_hash = Some(hash.into());
        self
    }

    pub fn source_app(mut self, app: impl Into<String>) -> Self {
        self.source_app = Some(app.into());
        self
    }

    pub fn primary_selection(mut self) -> Self {
        self.selection_type = Some("primary".to_string());
        self
    }

    pub fn clipboard_selection(mut self) -> Self {
        self.selection_type = Some("clipboard".to_string());
        self
    }

    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn build(self) -> crate::RawEvent {
        let content = self.content.unwrap_or_default();
        let content_type = self.content_type.unwrap_or(ClipboardContentType::Text);

        let selection_type = self.selection_type.unwrap_or_else(|| "clipboard".to_string());
        
        let mut payload = json!({
            "content_type": content_type.as_str(),
            "content_size": content.len() as u64,
            "selection_type": selection_type,
            "timestamp": self.timestamp.unwrap_or_else(Utc::now).to_rfc3339(),
        });

        // Only include content preview for reasonable sizes
        if content.len() <= 1000 {
            payload["text_preview"] = json!(content);
        } else {
            payload["text_preview"] = json!(format!("{}...", &content[..100]));
        }

        if let Some(hash) = self.content_hash {
            payload["content_hash"] = json!(hash);
        }

        if let Some(app) = self.source_app {
            payload["source_app"] = json!(app);
        }

        let event_type = match selection_type.as_str() {
            "primary" => "clipboard.selected",
            _ => "clipboard.copied",
        };

        let mut builder = RawEventBuilder::new(&self.source_name, event_type, payload)
            .with_host(&self.host)
            .with_ingestor_version(&self.ingestor_version);

        if let Some(ts) = self.timestamp {
            builder = builder.with_orig_timestamp(ts);
        }

        builder.build()
    }
}

// ===== Window Manager Event Builder =====

#[derive(Debug, Clone)]
pub enum WindowManagerEventType {
    Custom(String),
}

impl WindowManagerEventType {
    pub fn as_str(&self) -> &str {
        match self {
            WindowManagerEventType::Custom(s) => s,
        }
    }
}

pub struct WindowManagerEventBuilder {
    source_name: String,
    host: String,
    ingestor_version: String,
    window_address: Option<String>,
    window_class: Option<String>,
    window_title: Option<String>,
    workspace_id: Option<String>,
    event_data: Option<String>,
    timestamp: Option<DateTime<Utc>>,
}

impl WindowManagerEventBuilder {
    pub fn new(source_name: &str, host: &str, ingestor_version: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            host: host.to_string(),
            ingestor_version: ingestor_version.to_string(),
            window_address: None,
            window_class: None,
            window_title: None,
            workspace_id: None,
            event_data: None,
            timestamp: None,
        }
    }

    pub fn window_address(mut self, address: impl Into<String>) -> Self {
        self.window_address = Some(address.into());
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

    pub fn workspace_id(mut self, id: impl Into<String>) -> Self {
        self.workspace_id = Some(id.into());
        self
    }

    pub fn event_data(mut self, data: impl Into<String>) -> Self {
        self.event_data = Some(data.into());
        self
    }

    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn build_window_focused(self) -> crate::RawEvent {
        self.build_with_event_type("window.focused")
    }

    pub fn build_window_opened(self) -> crate::RawEvent {
        self.build_with_event_type("window.opened")
    }

    pub fn build_window_closed(self) -> crate::RawEvent {
        self.build_with_event_type("window.closed")
    }

    pub fn build_workspace_switched(self) -> crate::RawEvent {
        self.build_with_event_type("workspace.switched")
    }

    pub fn window_created(self) -> Self {
        self
    }

    pub fn window_destroyed(self) -> Self {
        self
    }

    pub fn window_focused(self) -> Self {
        self
    }

    pub fn event_type(self, _event_type: WindowManagerEventType) -> Self {
        self
    }

    pub fn custom_data(self, _data: JsonValue) -> Self {
        self
    }

    pub fn build(self) -> crate::RawEvent {
        self.build_window_focused()
    }

    fn build_with_event_type(self, event_type: &str) -> crate::RawEvent {
        let mut payload = json!({
            "focused_at": self.timestamp.unwrap_or_else(Utc::now).to_rfc3339(),
        });

        if let Some(address) = self.window_address {
            payload["window_address"] = json!(address);
        }

        if let Some(class) = self.window_class {
            payload["window_class"] = json!(class);
        }

        if let Some(title) = self.window_title {
            payload["window_title"] = json!(title);
        }

        if let Some(workspace) = self.workspace_id {
            payload["workspace_id"] = json!(workspace);
        }

        if let Some(data) = self.event_data {
            payload["event_data"] = json!(data);
        }

        let mut builder = RawEventBuilder::new(&self.source_name, event_type, payload)
            .with_host(&self.host)
            .with_ingestor_version(&self.ingestor_version);

        if let Some(ts) = self.timestamp {
            builder = builder.with_orig_timestamp(ts);
        }

        builder.build()
    }
}

// ===== System Event Builder =====

pub struct SystemEventBuilder {
    source_name: String,
    host: String,
    ingestor_version: String,
    message: Option<String>,
    priority: Option<u8>,
    unit: Option<String>,
    pid: Option<u32>,
    cursor: Option<String>,
    fields: HashMap<String, String>,
    timestamp: Option<DateTime<Utc>>,
}

impl SystemEventBuilder {
    pub fn new(source_name: &str, host: &str, ingestor_version: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            host: host.to_string(),
            ingestor_version: ingestor_version.to_string(),
            message: None,
            priority: None,
            unit: None,
            pid: None,
            cursor: None,
            fields: HashMap::new(),
            timestamp: None,
        }
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    pub fn pid(mut self, pid: u32) -> Self {
        self.pid = Some(pid);
        self
    }

    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    pub fn field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn agent_name(self, name: impl Into<String>) -> Self {
        self.message(format!("Agent: {}", name.into()))
    }

    pub fn heartbeat(self) -> Self {
        self.message("Heartbeat")
    }

    pub fn error(self, error: impl Into<String>) -> Self {
        self.message(format!("Error: {}", error.into()))
    }

    pub fn build(self) -> crate::RawEvent {
        self.build_journal_entry()
    }

    pub fn build_journal_entry(self) -> crate::RawEvent {
        let message = self.message.unwrap_or_default();
        let timestamp = self.timestamp.unwrap_or_else(Utc::now);

        let mut payload = json!({
            "message": message,
            "timestamp": timestamp.to_rfc3339(),
            "timestamp_us": timestamp.timestamp_micros(),
        });

        if let Some(priority) = self.priority {
            payload["priority"] = json!(priority);
        }

        if let Some(unit) = self.unit {
            payload["unit"] = json!(unit);
        }

        if let Some(pid) = self.pid {
            payload["pid"] = json!(pid);
        }

        if let Some(cursor) = self.cursor {
            payload["cursor"] = json!(cursor);
        }

        if !self.fields.is_empty() {
            payload["fields"] = json!(self.fields);
        }

        let mut builder = RawEventBuilder::new(&self.source_name, "entry.written", payload)
            .with_host(&self.host)
            .with_ingestor_version(&self.ingestor_version);

        builder = builder.with_orig_timestamp(timestamp);

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources;

    #[test]
    fn test_filesystem_event_builder() {
        let factory = EventFactory::new(sources::FS);
        let event = factory
            .filesystem()
            .path("/test/file.txt")
            .created()
            .size(1024)
            .permissions(0o644)
            .build();

        assert_eq!(event.source, sources::FS);
        assert_eq!(event.event_type, event_type_constants::filesystem::FILE_CREATED);
        assert_eq!(event.payload["path"], "/test/file.txt");
        assert_eq!(event.payload["size"], 1024);
    }

    #[test]
    fn test_terminal_event_builder() {
        let factory = EventFactory::new(sources::SHELL_KITTY);
        let event = factory
            .terminal()
            .command("ls -la")
            .success()
            .duration_ms(150)
            .working_dir("/home/user")
            .build_executed();

        assert_eq!(event.source, sources::SHELL_KITTY);
        assert_eq!(event.event_type, "command.executed");
        assert_eq!(event.payload["command"], "ls -la");
        assert_eq!(event.payload["exit_status"], 0);
        assert_eq!(event.payload["execution_time_ms"], 150);
    }

    #[test]
    fn test_clipboard_event_builder() {
        let factory = EventFactory::new(sources::CLIPBOARD);
        let event = factory
            .clipboard()
            .text("test content")
            .source_app("firefox")
            .build();

        assert_eq!(event.source, sources::CLIPBOARD);
        assert_eq!(event.event_type, "clipboard.copied");
        assert_eq!(event.payload["text_preview"], "test content");
        assert_eq!(event.payload["source_app"], "firefox");
    }
}