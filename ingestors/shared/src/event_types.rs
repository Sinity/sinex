use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_db::models::RawEvent;
use sinex_ulid::Ulid;

/// Builder for creating new raw events
pub struct RawEventBuilder {
    source: String,
    event_type: String,
    ts_orig: Option<DateTime<Utc>>,
    host: String,
    ingestor_version: String,
    payload_schema_id: Option<Ulid>,
    payload: JsonValue,
}

impl RawEventBuilder {
    pub fn new(
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: JsonValue,
    ) -> Self {
        let hostname = gethostname::gethostname()
            .to_string_lossy()
            .into_owned();

        Self {
            source: source.into(),
            event_type: event_type.into(),
            ts_orig: None,
            host: hostname,
            ingestor_version: env!("CARGO_PKG_VERSION").to_string(),
            payload_schema_id: None,
            payload,
        }
    }

    pub fn with_orig_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.ts_orig = Some(ts);
        self
    }

    pub fn with_schema_id(mut self, schema_id: Ulid) -> Self {
        self.payload_schema_id = Some(schema_id);
        self
    }

    pub fn with_ingestor_version(mut self, version: impl Into<String>) -> Self {
        self.ingestor_version = version.into();
        self
    }

    /// Build the event (ID will be set by database gen_ulid())
    pub fn build(self) -> RawEvent {
        RawEvent {
            id: uuid::Uuid::new_v4(), // Will be replaced by database gen_ulid()
            source: self.source,
            event_type: self.event_type,
            ts_orig: self.ts_orig,
            host: self.host,
            ingestor_version: Some(self.ingestor_version),
            payload_schema_id: self.payload_schema_id.map(|ulid| uuid::Uuid::from_bytes(ulid.to_bytes())),
            payload: self.payload,
        }
    }
}

/// Event sources
pub mod sources {
    pub const HYPRLAND: &str = "hyprland";
    pub const TERMINAL_KITTY: &str = "terminal.kitty";
    pub const FILESYSTEM: &str = "filesystem";
    pub const SINEX: &str = "sinex";
}

/// Event types for each source
pub mod event_types {
    pub mod hyprland {
        pub const WINDOW_FOCUSED: &str = "window_focused";
        pub const WORKSPACE_CHANGED: &str = "workspace_changed";
        pub const CLIPBOARD_CHANGED: &str = "clipboard_changed";
        pub const STATE_SNAPSHOT: &str = "state_snapshot";
    }

    pub mod terminal {
        pub const COMMAND_EXECUTED: &str = "command_executed";
    }

    pub mod filesystem {
        pub const FILE_CREATED: &str = "file_created";
        pub const FILE_MODIFIED: &str = "file_modified";
        pub const FILE_DELETED: &str = "file_deleted";
        pub const FILE_RENAMED: &str = "file_renamed";
    }

    pub mod sinex {
        pub const AGENT_HEARTBEAT: &str = "agent.heartbeat";
        pub const AGENT_ERROR: &str = "agent.error";
        pub const AGENT_DLQ_EVENT_WRITTEN: &str = "agent.dlq_event_written";
        pub const SCHEMA_CHANGE: &str = "schema.change";
    }
}