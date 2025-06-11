use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use thiserror::Error;
use tracing::warn;

use crate::models::RawEvent;
use sinex_ulid::Ulid;

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Missing required field: {field}")]
    MissingField { field: String },
    
    #[error("Invalid field type: {field} should be {expected}, got {actual}")]
    InvalidType {
        field: String,
        expected: String,
        actual: String,
    },
    
    #[error("Invalid value: {field} - {reason}")]
    InvalidValue { field: String, reason: String },
    
    #[error("Unknown source/event_type combination: {event_source}/{event_type}")]
    UnknownEventType { event_source: String, event_type: String },
    
    #[error("Schema validation failed: {0}")]
    SchemaValidation(String),
    
    #[error("Schema not found for ID: {0}")]
    SchemaNotFound(Ulid),
}

/// Combined event validator that supports both hardcoded rules and JSON schema validation
pub struct EventValidator {
    /// Hardcoded validation rules for specific event types
    rules: HashMap<(String, String), Box<dyn Fn(&Value) -> Result<(), ValidationError> + Send + Sync>>,
    /// JSON schema validators loaded from database
    schemas: HashMap<Ulid, jsonschema::JSONSchema>,
}

impl EventValidator {
    /// Create a new validator with hardcoded rules
    pub fn new() -> Self {
        let mut validator = Self {
            rules: HashMap::new(),
            schemas: HashMap::new(),
        };
        
        // Register hardcoded validation rules
        validator.register_filesystem_rules();
        validator.register_window_manager_rules();
        validator.register_terminal_rules();
        validator.register_sinex_rules();
        
        validator
    }
    
    /// Load JSON schemas from database and create a validator
    pub async fn load_from_db(pool: &PgPool) -> Result<Self> {
        let mut validator = Self::new();
        
        // Load all active schemas from database
        let schemas = sqlx::query!(
            "SELECT id::uuid as \"id!\", event_source, event_type, json_schema_definition FROM sinex_schemas.event_payload_schemas WHERE is_active = true"
        )
        .fetch_all(pool)
        .await?;
        
        for schema_record in schemas {
            match jsonschema::JSONSchema::compile(&schema_record.json_schema_definition) {
                Ok(compiled_schema) => {
                    validator.schemas.insert(Ulid::from_uuid(schema_record.id), compiled_schema);
                }
                Err(e) => {
                    warn!("Failed to compile schema {}/{}: {}", schema_record.event_source, schema_record.event_type, e);
                }
            }
        }
        
        Ok(validator)
    }
    
    /// Validate an event using both hardcoded rules and JSON schema if available
    pub fn validate(&self, event: &RawEvent) -> Result<(), ValidationError> {
        // First try JSON schema validation if schema_id is specified
        if let Some(schema_id) = event.payload_schema_id {
            if let Some(schema) = self.schemas.get(&schema_id) {
                if let Err(e) = schema.validate(&event.payload) {
                    return Err(ValidationError::SchemaValidation(
                        e.map(|err| err.to_string()).collect::<Vec<_>>().join(", ")
                    ));
                }
                // If schema validation passes, we're done
                return Ok(());
            } else {
                // Schema ID specified but schema not found
                return Err(ValidationError::SchemaNotFound(schema_id));
            }
        }
        
        // Fall back to hardcoded rules
        self.validate_with_rules(&event.source, &event.event_type, &event.payload)
    }
    
    /// Validate using hardcoded rules only
    pub fn validate_with_rules(&self, source: &str, event_type: &str, payload: &Value) -> Result<(), ValidationError> {
        let key = (source.to_string(), event_type.to_string());
        
        match self.rules.get(&key) {
            Some(validator) => validator(payload),
            None => {
                // For unknown event types, just ensure it's an object
                if !payload.is_object() {
                    return Err(ValidationError::InvalidType {
                        field: "payload".to_string(),
                        expected: "object".to_string(),
                        actual: format!("{:?}", payload),
                    });
                }
                Ok(())
            }
        }
    }
    
    fn register_rule<F>(&mut self, source: &str, event_type: &str, validator: F)
    where
        F: Fn(&Value) -> Result<(), ValidationError> + Send + Sync + 'static,
    {
        self.rules.insert(
            (source.to_string(), event_type.to_string()),
            Box::new(validator),
        );
    }
    
    fn register_filesystem_rules(&mut self) {
        // file.created validation
        self.register_rule(
            "filesystem",
            "file.created",
            |payload| {
                // Required: path (string), size (number >= 0)
                let path = payload.get("path")
                    .ok_or_else(|| ValidationError::MissingField { field: "path".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "path".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("path")),
                    })?;
                
                if path.is_empty() {
                    return Err(ValidationError::InvalidValue {
                        field: "path".to_string(),
                        reason: "cannot be empty".to_string(),
                    });
                }
                
                let _size = payload.get("size")
                    .ok_or_else(|| ValidationError::MissingField { field: "size".to_string() })?
                    .as_u64()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "size".to_string(),
                        expected: "positive integer".to_string(),
                        actual: format!("{:?}", payload.get("size")),
                    })?;
                
                // Optional: permissions (string matching pattern)
                if let Some(perms) = payload.get("permissions") {
                    let perms_str = perms.as_str()
                        .ok_or_else(|| ValidationError::InvalidType {
                            field: "permissions".to_string(),
                            expected: "string".to_string(),
                            actual: format!("{:?}", perms),
                        })?;
                    
                    if !perms_str.chars().all(|c| c >= '0' && c <= '7') || 
                       (perms_str.len() != 3 && perms_str.len() != 4) {
                        return Err(ValidationError::InvalidValue {
                            field: "permissions".to_string(),
                            reason: "must be 3 or 4 octal digits".to_string(),
                        });
                    }
                }
                
                Ok(())
            },
        );
        
        // file.modified validation
        self.register_rule(
            "filesystem",
            "file.modified",
            |payload| {
                // Required: path
                payload.get("path")
                    .ok_or_else(|| ValidationError::MissingField { field: "path".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "path".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("path")),
                    })?;
                
                // At least one of: old_size/new_size, modification_type
                let has_size_info = payload.get("old_size").is_some() || payload.get("new_size").is_some();
                let has_mod_type = payload.get("modification_type").is_some();
                
                if !has_size_info && !has_mod_type {
                    return Err(ValidationError::MissingField {
                        field: "modification info (old_size/new_size or modification_type)".to_string(),
                    });
                }
                
                Ok(())
            },
        );
        
        // file.deleted validation
        self.register_rule(
            "filesystem",
            "file.deleted",
            |payload| {
                // Required: path
                payload.get("path")
                    .ok_or_else(|| ValidationError::MissingField { field: "path".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "path".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("path")),
                    })?;
                
                // Optional: was_directory (boolean)
                if let Some(was_dir) = payload.get("was_directory") {
                    was_dir.as_bool()
                        .ok_or_else(|| ValidationError::InvalidType {
                            field: "was_directory".to_string(),
                            expected: "boolean".to_string(),
                            actual: format!("{:?}", was_dir),
                        })?;
                }
                
                Ok(())
            },
        );
        
        // file.renamed validation
        self.register_rule(
            "filesystem",
            "file.renamed",
            |payload| {
                // Required: old_path, new_path
                payload.get("old_path")
                    .ok_or_else(|| ValidationError::MissingField { field: "old_path".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "old_path".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("old_path")),
                    })?;
                
                payload.get("new_path")
                    .ok_or_else(|| ValidationError::MissingField { field: "new_path".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "new_path".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("new_path")),
                    })?;
                
                Ok(())
            },
        );
    }
    
    fn register_window_manager_rules(&mut self) {
        // window.focused validation
        self.register_rule(
            "window_manager.hyprland",
            "window.focused",
            |payload| {
                // Required: window (object or string)
                payload.get("window")
                    .ok_or_else(|| ValidationError::MissingField { field: "window".to_string() })?;
                
                // Optional but common: workspace
                if let Some(workspace) = payload.get("workspace") {
                    if !workspace.is_number() && !workspace.is_string() {
                        return Err(ValidationError::InvalidType {
                            field: "workspace".to_string(),
                            expected: "number or string".to_string(),
                            actual: format!("{:?}", workspace),
                        });
                    }
                }
                
                Ok(())
            },
        );
        
        // workspace.changed validation
        self.register_rule(
            "window_manager.hyprland",
            "workspace.changed",
            |payload| {
                // Required: workspace (number or string)
                let workspace = payload.get("workspace")
                    .ok_or_else(|| ValidationError::MissingField { field: "workspace".to_string() })?;
                
                if !workspace.is_number() && !workspace.is_string() {
                    return Err(ValidationError::InvalidType {
                        field: "workspace".to_string(),
                        expected: "number or string".to_string(),
                        actual: format!("{:?}", workspace),
                    });
                }
                
                Ok(())
            },
        );
    }
    
    fn register_terminal_rules(&mut self) {
        // command.executed validation
        self.register_rule(
            "terminal.kitty",
            "command.executed",
            |payload| {
                // Required: command
                payload.get("command")
                    .ok_or_else(|| ValidationError::MissingField { field: "command".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "command".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("command")),
                    })?;
                
                // Optional: exit_code (number), duration (number)
                if let Some(exit_code) = payload.get("exit_code") {
                    exit_code.as_i64()
                        .ok_or_else(|| ValidationError::InvalidType {
                            field: "exit_code".to_string(),
                            expected: "integer".to_string(),
                            actual: format!("{:?}", exit_code),
                        })?;
                }
                
                Ok(())
            },
        );
    }
    
    fn register_sinex_rules(&mut self) {
        // agent.heartbeat validation
        self.register_rule(
            "sinex",
            "agent.heartbeat",
            |payload| {
                // Required: agent_name
                payload.get("agent_name")
                    .ok_or_else(|| ValidationError::MissingField { field: "agent_name".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "agent_name".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("agent_name")),
                    })?;
                
                // Required: status
                payload.get("status")
                    .ok_or_else(|| ValidationError::MissingField { field: "status".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "status".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("status")),
                    })?;
                
                // Required: version
                payload.get("version")
                    .ok_or_else(|| ValidationError::MissingField { field: "version".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "version".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("version")),
                    })?;
                
                // Optional numeric fields
                if let Some(uptime) = payload.get("uptime_seconds") {
                    uptime.as_u64()
                        .ok_or_else(|| ValidationError::InvalidType {
                            field: "uptime_seconds".to_string(),
                            expected: "non-negative integer".to_string(),
                            actual: format!("{:?}", uptime),
                        })?;
                }
                
                if let Some(events) = payload.get("events_processed_session") {
                    events.as_u64()
                        .ok_or_else(|| ValidationError::InvalidType {
                            field: "events_processed_session".to_string(),
                            expected: "non-negative integer".to_string(),
                            actual: format!("{:?}", events),
                        })?;
                }
                
                if let Some(dlq_size) = payload.get("dlq_size") {
                    dlq_size.as_u64()
                        .ok_or_else(|| ValidationError::InvalidType {
                            field: "dlq_size".to_string(),
                            expected: "non-negative integer".to_string(),
                            actual: format!("{:?}", dlq_size),
                        })?;
                }
                
                Ok(())
            },
        );
        
        // agent.error validation
        self.register_rule(
            "sinex",
            "agent.error",
            |payload| {
                // Required: agent_name, error_message
                payload.get("agent_name")
                    .ok_or_else(|| ValidationError::MissingField { field: "agent_name".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "agent_name".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("agent_name")),
                    })?;
                
                payload.get("error_message")
                    .ok_or_else(|| ValidationError::MissingField { field: "error_message".to_string() })?
                    .as_str()
                    .ok_or_else(|| ValidationError::InvalidType {
                        field: "error_message".to_string(),
                        expected: "string".to_string(),
                        actual: format!("{:?}", payload.get("error_message")),
                    })?;
                
                // Optional: severity (must be valid level)
                if let Some(severity) = payload.get("severity") {
                    let sev = severity.as_str()
                        .ok_or_else(|| ValidationError::InvalidType {
                            field: "severity".to_string(),
                            expected: "string".to_string(),
                            actual: format!("{:?}", severity),
                        })?;
                    
                    if !["warning", "error", "critical"].contains(&sev) {
                        return Err(ValidationError::InvalidValue {
                            field: "severity".to_string(),
                            reason: "must be one of: warning, error, critical".to_string(),
                        });
                    }
                }
                
                Ok(())
            },
        );
    }
}

impl Default for EventValidator {
    fn default() -> Self {
        Self::new()
    }
}
