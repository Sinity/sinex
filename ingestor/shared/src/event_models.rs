use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::collections::HashMap;
use sinex_db::models::RawEvent;
use crate::RawEventBuilder;

/// Type-safe event payloads with derive macros for automatic schema generation
/// Each event type should have validation rules

// Terminal events
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum TerminalEvent {
    #[serde(rename = "command_executed")]
    CommandExecuted {
        command_string: String,
        cwd: PathBuf,
        exit_code: i32,
        ts_start_orig: DateTime<Utc>,
        ts_end_orig: DateTime<Utc>,
        #[serde(skip_serializing_if = "Option::is_none")]
        environment: Option<Vec<(String, String)>>,
    },
    
    #[serde(rename = "terminal_resized")]
    TerminalResized {
        rows: u16,
        cols: u16,
        window_id: String,
    },
}

// Filesystem events
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum FilesystemEvent {
    #[serde(rename = "file_created")]
    FileCreated {
        path: PathBuf,
        size_bytes: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        blake3_hash: Option<String>,
    },
    
    #[serde(rename = "file_modified")]
    FileModified {
        path: PathBuf,
        size_bytes: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        blake3_hash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        previous_hash: Option<String>,
    },
    
    #[serde(rename = "file_deleted")]
    FileDeleted {
        path: PathBuf,
    },
}

// Hyprland events
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum HyprlandEvent {
    #[serde(rename = "window_focused")]
    WindowFocused {
        window_id: String,
        window_class: String,
        window_title: String,
        workspace_id: i32,
        #[serde(skip_serializing_if = "Option::is_none")]
        process_info: Option<ProcessInfo>,
    },
    
    #[serde(rename = "workspace_changed")]
    WorkspaceChanged {
        from_workspace: i32,
        to_workspace: i32,
        monitor: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub pid: u32,
    pub cmdline: Vec<String>,
    pub exe_path: PathBuf,
}

/// Trait for type-safe event creation
pub trait EventPayload: Serialize + Send + Sync {
    fn event_source(&self) -> &'static str;
    fn event_type(&self) -> &'static str;
    fn validate(&self) -> Result<(), ValidationError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
    
    #[error("Missing required field: {0}")]
    MissingField(String),
    
    #[error("Invalid value: {field} = {value}")]
    InvalidValue { field: String, value: String },
}

// Implementations
impl EventPayload for TerminalEvent {
    fn event_source(&self) -> &'static str {
        "terminal"
    }
    
    fn event_type(&self) -> &'static str {
        match self {
            Self::CommandExecuted { .. } => "command_executed",
            Self::TerminalResized { .. } => "terminal_resized",
        }
    }
    
    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::CommandExecuted { command_string, ts_start_orig, ts_end_orig, .. } => {
                if command_string.is_empty() {
                    return Err(ValidationError::MissingField("command_string".to_string()));
                }
                if ts_end_orig < ts_start_orig {
                    return Err(ValidationError::InvalidTimestamp(
                        "end time before start time".to_string()
                    ));
                }
                Ok(())
            }
            Self::TerminalResized { rows, cols, .. } => {
                if *rows == 0 || *cols == 0 {
                    return Err(ValidationError::InvalidValue {
                        field: "dimensions".to_string(),
                        value: format!("{}x{}", rows, cols),
                    });
                }
                Ok(())
            }
        }
    }
}

impl EventPayload for FilesystemEvent {
    fn event_source(&self) -> &'static str {
        "filesystem"
    }
    
    fn event_type(&self) -> &'static str {
        match self {
            Self::FileCreated { .. } => "file_created",
            Self::FileModified { .. } => "file_modified",
            Self::FileDeleted { .. } => "file_deleted",
        }
    }
    
    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::FileCreated { path, .. } | 
            Self::FileModified { path, .. } |
            Self::FileDeleted { path } => {
                if path.as_os_str().is_empty() {
                    return Err(ValidationError::InvalidPath("empty path".to_string()));
                }
                // Could add more validation like checking for path traversal
                Ok(())
            }
        }
    }
}

// Builder pattern for complex events
pub struct EventBuilder<T: EventPayload> {
    payload: T,
    metadata: HashMap<String, serde_json::Value>,
}

impl<T: EventPayload> EventBuilder<T> {
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            metadata: HashMap::new(),
        }
    }
    
    pub fn with_metadata(mut self, key: &str, value: impl Serialize) -> Result<Self, ValidationError> {
        match serde_json::to_value(value) {
            Ok(v) => {
                self.metadata.insert(key.to_string(), v);
                Ok(self)
            }
            Err(e) => Err(ValidationError::InvalidValue {
                field: key.to_string(),
                value: format!("Failed to serialize: {}", e),
            }),
        }
    }
    
    pub fn build(self) -> Result<RawEvent, ValidationError> {
        self.payload.validate()?;
        
        let mut value = serde_json::to_value(&self.payload)
            .map_err(|e| ValidationError::InvalidValue {
                field: "payload".to_string(),
                value: format!("Failed to serialize payload: {}", e),
            })?;
        
        // Add metadata
        if let Some(obj) = value.as_object_mut() {
            for (k, v) in self.metadata {
                obj.insert(k, v);
            }
        }
        
        Ok(RawEventBuilder::new(
            self.payload.event_source(),
            self.payload.event_type(),
            value,
        ).build())
    }
}