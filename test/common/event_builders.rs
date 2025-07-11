//! Event builders for test code
//!
//! This module re-exports the event builders from sinex-core for test compatibility
//! and provides additional test-specific builders not suitable for production code.

// Re-export everything from sinex-events event builders
pub use sinex_events::event_builders::*;

// Additional type aliases for test compatibility
pub type HyprlandEventType = WindowManagerEventType;

/// Generic event builder that can create any type of event
pub struct GenericEventBuilder {
    factory: EventFactory,
    event_type: String,
    payload: Option<serde_json::Value>,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

impl GenericEventBuilder {
    pub fn new(source: &str, event_type: &str) -> Self {
        Self {
            factory: EventFactory::new(source),
            event_type: event_type.to_string(),
            payload: None,
            timestamp: None,
        }
    }

    pub fn payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn timestamp(mut self, ts: chrono::DateTime<chrono::Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn build(self) -> sinex_core::RawEvent {
        let payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        self.factory.create_event(&self.event_type, payload)
    }

    // Terminal-specific methods
    pub fn command(self, cmd: impl Into<String>) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["command"] = serde_json::json!(cmd.into());
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn success(self) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["exit_status"] = serde_json::json!(0);
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn duration_ms(self, ms: u64) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["execution_time_ms"] = serde_json::json!(ms);
        Self {
            payload: Some(payload),
            ..self
        }
    }

    // Agent-specific methods
    pub fn name(self, name: impl Into<String>) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["agent_name"] = serde_json::json!(name.into());
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn heartbeat(self) -> Self {
        let mut new_builder = Self {
            event_type: "agent.heartbeat".to_string(),
            ..self
        };
        let mut payload = new_builder.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["status"] = serde_json::json!("running");
        new_builder.payload = Some(payload);
        new_builder
    }

    pub fn startup(self) -> Self {
        let mut new_builder = Self {
            event_type: "agent.startup".to_string(),
            ..self
        };
        let mut payload = new_builder.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["status"] = serde_json::json!("starting");
        new_builder.payload = Some(payload);
        new_builder
    }

    pub fn error(self, error_msg: impl Into<String>) -> Self {
        let mut new_builder = Self {
            event_type: "agent.error".to_string(),
            ..self
        };
        let mut payload = new_builder.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["error_message"] = serde_json::json!(error_msg.into());
        payload["status"] = serde_json::json!("error");
        new_builder.payload = Some(payload);
        new_builder
    }

    pub fn uptime_seconds(self, seconds: u64) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["uptime_seconds"] = serde_json::json!(seconds);
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn version(self, version: impl Into<String>) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["version"] = serde_json::json!(version.into());
        Self {
            payload: Some(payload),
            ..self
        }
    }

    pub fn events_processed(self, count: u64) -> Self {
        let mut payload = self.payload.unwrap_or_else(|| serde_json::json!({}));
        payload["events_processed_session"] = serde_json::json!(count);
        Self {
            payload: Some(payload),
            ..self
        }
    }
}

/// Generic event builder for test flexibility
pub struct EventBuilder;

impl EventBuilder {
    /// Create a generic event builder with source and type
    pub fn generic(source: &str, event_type: &str) -> GenericEventBuilder {
        GenericEventBuilder::new(source, event_type)
    }

    /// Create a filesystem event builder
    pub fn filesystem() -> FilesystemEventBuilder {
        let factory = EventFactory::new("fs");
        factory.filesystem()
    }

    /// Create a terminal event builder  
    pub fn terminal() -> GenericEventBuilder {
        GenericEventBuilder::new("shell.kitty", "command.executed")
    }

    /// Create a clipboard event builder
    pub fn clipboard() -> ClipboardEventBuilder {
        let factory = EventFactory::new("clipboard");
        factory.clipboard()
    }

    /// Create a hyprland event builder
    pub fn hyprland() -> WindowManagerEventBuilder {
        let factory = EventFactory::new("wm.hyprland");
        factory.window_manager()
    }

    /// Create an agent event builder
    pub fn agent() -> GenericEventBuilder {
        GenericEventBuilder::new("sinex", "agent.heartbeat")
    }
}
