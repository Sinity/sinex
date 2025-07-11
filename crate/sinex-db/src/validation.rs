use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;
use tracing::warn;

use crate::security::{SecurityError, SecurityValidator};
use crate::RawEvent; // Re-exported from sinex-core
use sinex_core::{CoreError, ValidationChain};
use sinex_ulid::Ulid;

/// Convert ValidationChain result to local ValidationError type
fn convert_validation_result<T>(
    result: std::result::Result<T, CoreError>,
) -> std::result::Result<T, ValidationError> {
    result.map_err(|e| match e {
        CoreError::Validation(msg) => {
            // Try to parse structured error message
            if msg.contains("cannot be empty") {
                if let Some(field) = extract_field_from_error(&msg) {
                    ValidationError::InvalidValue {
                        field: field.to_string(),
                        reason: "cannot be empty".to_string(),
                    }
                } else {
                    ValidationError::InvalidValue {
                        field: "unknown".to_string(),
                        reason: msg,
                    }
                }
            } else {
                ValidationError::InvalidValue {
                    field: "unknown".to_string(),
                    reason: msg,
                }
            }
        }
        other => ValidationError::InvalidValue {
            field: "unknown".to_string(),
            reason: other.to_string(),
        },
    })
}

/// Helper function to validate required JSON fields using ValidationChain
fn validate_required_field<T, F>(
    payload: &Value,
    field_name: &str,
    extractor: F,
) -> Result<T, ValidationError>
where
    F: FnOnce(&Value) -> Option<T>,
{
    let value = payload
        .get(field_name)
        .ok_or_else(|| ValidationError::MissingField {
            field: field_name.to_string(),
        })?;
    extractor(value).ok_or_else(|| ValidationError::InvalidType {
        field: field_name.to_string(),
        expected: "valid value".to_string(),
        actual: format!("{:?}", value),
    })
}

/// Helper function to validate required string fields with empty check
fn validate_required_string_field(
    payload: &Value,
    field_name: &str,
) -> Result<String, ValidationError> {
    let value = payload
        .get(field_name)
        .ok_or_else(|| ValidationError::MissingField {
            field: field_name.to_string(),
        })?;

    let string_value = value
        .as_str()
        .ok_or_else(|| ValidationError::InvalidType {
            field: field_name.to_string(),
            expected: "string".to_string(),
            actual: format!("{:?}", value),
        })?
        .to_string();

    convert_validation_result(
        ValidationChain::validate(string_value.clone(), field_name)
            .not_empty()
            .into_result(),
    )?;

    Ok(string_value)
}

/// Helper function to validate optional fields with type extraction
fn validate_optional_field<T, F>(
    payload: &Value,
    field_name: &str,
    extractor: F,
    expected_type: &str,
) -> Result<Option<T>, ValidationError>
where
    F: FnOnce(&Value) -> Option<T>,
{
    match payload.get(field_name) {
        Some(value) => {
            let extracted = extractor(value).ok_or_else(|| ValidationError::InvalidType {
                field: field_name.to_string(),
                expected: expected_type.to_string(),
                actual: format!("{:?}", value),
            })?;
            Ok(Some(extracted))
        }
        None => Ok(None),
    }
}

/// Enhanced ValidationChain helper for numeric field validation
fn validate_required_numeric_field<T>(
    payload: &Value,
    field_name: &str,
    extractor: fn(&Value) -> Option<T>,
) -> Result<T, ValidationError>
where
    T: Clone + std::fmt::Debug,
{
    let value = payload
        .get(field_name)
        .ok_or_else(|| ValidationError::MissingField {
            field: field_name.to_string(),
        })?;

    // Validate the field is a number (manual check for now)
    if !value.is_number() {
        return Err(ValidationError::InvalidType {
            field: field_name.to_string(),
            expected: "number".to_string(),
            actual: format!("{:?}", value),
        });
    }

    extractor(value).ok_or_else(|| ValidationError::InvalidType {
        field: field_name.to_string(),
        expected: "number".to_string(),
        actual: format!("{:?}", value),
    })
}

/// Enhanced ValidationChain helper for optional numeric field validation
fn validate_optional_numeric_field<T>(
    payload: &Value,
    field_name: &str,
    extractor: fn(&Value) -> Option<T>,
) -> Result<Option<T>, ValidationError>
where
    T: Clone + std::fmt::Debug,
{
    match payload.get(field_name) {
        Some(value) => {
            // Validate the field is a number (manual check for now)
            if !value.is_number() {
                return Err(ValidationError::InvalidType {
                    field: field_name.to_string(),
                    expected: "number".to_string(),
                    actual: format!("{:?}", value),
                });
            }

            let extracted = extractor(value).ok_or_else(|| ValidationError::InvalidType {
                field: field_name.to_string(),
                expected: "number".to_string(),
                actual: format!("{:?}", value),
            })?;
            Ok(Some(extracted))
        }
        None => Ok(None),
    }
}

/// Extract field name from error message (best effort)
fn extract_field_from_error(msg: &str) -> Option<&str> {
    // Parse field name from ValidationError format: "Invalid value for field {field}: {message}"
    if let Some(field_start) = msg.find("field ") {
        let field_part = &msg[field_start + 6..]; // Skip "field "
        if let Some(colon_pos) = field_part.find(':') {
            return Some(&field_part[..colon_pos]);
        }
    }

    // Fallback: try other formats
    None
}

/// Database-specific validation error type
#[derive(Error, Debug, Clone)]
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
    UnknownEventType {
        event_source: String,
        event_type: String,
    },

    #[error("Schema validation failed: {0}")]
    SchemaValidation(String),

    #[error("Schema not found for ID: {0}")]
    SchemaNotFound(Ulid),

    #[error("Security validation failed: {0}")]
    SecurityValidation(String),
}

/// Type alias for validation functions to reduce complexity
type ValidationRuleFn = Box<dyn Fn(&Value) -> Result<(), ValidationError> + Send + Sync>;

/// Combined event validator that supports both hardcoded rules and JSON schema validation
pub struct EventValidator {
    /// Hardcoded validation rules for specific event types
    rules: HashMap<(String, String), ValidationRuleFn>,
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

        // Add default security validation for all events
        validator.add_default_security_rules();

        // Register hardcoded validation rules
        validator.register_filesystem_rules();
        validator.register_window_manager_rules();
        validator.register_terminal_rules();
        validator.register_sinex_rules();

        validator
    }

    /// Load JSON schemas from database and create a validator
    pub async fn load_from_db(pool: crate::DbPoolRef<'_>) -> Result<Self> {
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
                    validator
                        .schemas
                        .insert(Ulid::from_uuid(schema_record.id), compiled_schema);
                }
                Err(e) => {
                    warn!(
                        "Failed to compile schema {}/{}: {}",
                        schema_record.event_source, schema_record.event_type, e
                    );
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
                        e.map(|err| err.to_string()).collect::<Vec<_>>().join(", "),
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
    pub fn validate_with_rules(
        &self,
        source: &str,
        event_type: &str,
        payload: &Value,
    ) -> Result<(), ValidationError> {
        // Security validation for source field (where SQL/command injection payloads come in tests)
        let sanitized_source = match SecurityValidator::sanitize_unicode(source) {
            std::borrow::Cow::Owned(s) => s,
            std::borrow::Cow::Borrowed(s) => s.to_string(),
        };

        // Basic field validation using ValidationChain
        convert_validation_result(
            ValidationChain::validate(sanitized_source, "source")
                .not_empty()
                .into_result(),
        )?;

        convert_validation_result(
            ValidationChain::validate(event_type.to_string(), "event_type")
                .not_empty()
                .into_result(),
        )?;

        // First check for exact match
        let key = (source.to_string(), event_type.to_string());
        if let Some(validator) = self.rules.get(&key) {
            return validator(payload);
        }

        // Check for wildcard validator
        let wildcard_key = ("*".to_string(), "*".to_string());
        if let Some(validator) = self.rules.get(&wildcard_key) {
            validator(payload)?;
        }

        // For unknown event types, just ensure it's an object using manual validation
        // (ValidationChain for JSON doesn't have custom method, needs to be added in future)
        if !payload.is_object() {
            return Err(ValidationError::InvalidType {
                field: "payload".to_string(),
                expected: "object".to_string(),
                actual: format!("{:?}", payload),
            });
        }

        Ok(())
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
        self.register_rule("filesystem", "file.created", |payload| {
            // Required: path (string), size (number >= 0)
            let path = validate_required_string_field(payload, "path")?;

            // Sanitize the path
            let _sanitized_path = SecurityValidator::sanitize_path(&path)
                .map_err(|e| ValidationError::SecurityValidation(e.to_string()))?;

            let _size = validate_required_numeric_field(payload, "size", |v| v.as_u64())?;

            // Optional: permissions (string matching pattern)
            if let Some(perms_str) = validate_optional_field(
                payload,
                "permissions",
                |v| v.as_str().map(|s| s.to_string()),
                "string",
            )? {
                if !perms_str.chars().all(|c| ('0'..='7').contains(&c))
                    || (perms_str.len() != 3 && perms_str.len() != 4)
                {
                    return Err(ValidationError::InvalidValue {
                        field: "permissions".to_string(),
                        reason: "must be 3 or 4 octal digits".to_string(),
                    });
                }
            }

            Ok(())
        });

        // file.modified validation
        self.register_rule("filesystem", "file.modified", |payload| {
            // Required: path
            let path = validate_required_string_field(payload, "path")?;

            // Sanitize the path
            let _sanitized_path = SecurityValidator::sanitize_path(&path)
                .map_err(|e| ValidationError::SecurityValidation(e.to_string()))?;

            // At least one of: old_size/new_size, modification_type
            let has_size_info =
                payload.get("old_size").is_some() || payload.get("new_size").is_some();
            let has_mod_type = payload.get("modification_type").is_some();

            if !has_size_info && !has_mod_type {
                return Err(ValidationError::MissingField {
                    field: "modification info (old_size/new_size or modification_type)".to_string(),
                });
            }

            Ok(())
        });

        // file.deleted validation
        self.register_rule("filesystem", "file.deleted", |payload| {
            // Required: path
            let path = validate_required_string_field(payload, "path")?;

            // Sanitize the path
            let _sanitized_path = SecurityValidator::sanitize_path(&path)
                .map_err(|e| ValidationError::SecurityValidation(e.to_string()))?;

            // Optional: was_directory (boolean)
            let _was_directory =
                validate_optional_field(payload, "was_directory", |v| v.as_bool(), "boolean")?;

            Ok(())
        });

        // file.renamed validation
        self.register_rule("filesystem", "file.renamed", |payload| {
            // Required: old_path, new_path
            let old_path = validate_required_string_field(payload, "old_path")?;
            let new_path = validate_required_string_field(payload, "new_path")?;

            // Sanitize both paths
            let _sanitized_old = SecurityValidator::sanitize_path(&old_path)
                .map_err(|e| ValidationError::SecurityValidation(e.to_string()))?;
            let _sanitized_new = SecurityValidator::sanitize_path(&new_path)
                .map_err(|e| ValidationError::SecurityValidation(e.to_string()))?;

            Ok(())
        });
    }

    fn register_window_manager_rules(&mut self) {
        // window.focused validation
        self.register_rule("window_manager.hyprland", "window.focused", |payload| {
            // Required: window (object or string)
            let _window = validate_required_field(payload, "window", |v| Some(v.clone()))?;

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
        });

        // workspace.changed validation
        self.register_rule("window_manager.hyprland", "workspace.changed", |payload| {
            // Required: workspace (number or string)
            let workspace = validate_required_field(payload, "workspace", |v| Some(v.clone()))?;

            if !workspace.is_number() && !workspace.is_string() {
                return Err(ValidationError::InvalidType {
                    field: "workspace".to_string(),
                    expected: "number or string".to_string(),
                    actual: format!("{:?}", workspace),
                });
            }

            Ok(())
        });
    }

    fn register_terminal_rules(&mut self) {
        // command.executed validation
        self.register_rule("terminal.kitty", "command.executed", |payload| {
            // Required: command
            let _command = validate_required_string_field(payload, "command")?;

            // Optional: exit_code (number), duration (number)
            let _exit_code = validate_optional_numeric_field(payload, "exit_code", |v| v.as_i64())?;

            Ok(())
        });
    }

    fn add_default_security_rules(&mut self) {
        // Add a catch-all security validator for JSON payloads
        self.register_rule(
            "*", // Special source to match all
            "*", // Special event_type to match all
            |payload| {
                // Maximum allowed depth for JSON (prevent stack overflow)
                const MAX_JSON_DEPTH: usize = 1000;
                // Maximum allowed elements in JSON (prevent memory exhaustion)
                const MAX_JSON_ELEMENTS: usize = 10_000_000;

                // Check JSON depth
                SecurityValidator::check_json_depth(payload, MAX_JSON_DEPTH)
                    .map_err(|e| ValidationError::SecurityValidation(e.to_string()))?;

                // Check JSON size
                SecurityValidator::check_json_size(payload, MAX_JSON_ELEMENTS)
                    .map_err(|e| ValidationError::SecurityValidation(e.to_string()))?;

                // Check string fields for security issues
                if let Value::Object(map) = payload {
                    for (key, value) in map {
                        // Check for path traversal in path-like fields
                        if key.contains("path") || key == "file" || key == "directory" {
                            if let Value::String(s) = value {
                                match SecurityValidator::sanitize_path(s) {
                                    Ok(_) => {
                                        // Path was sanitized successfully
                                    }
                                    Err(SecurityError::NullByteInjection) => {
                                        return Err(ValidationError::SecurityValidation(
                                            "Null byte in path".to_string(),
                                        ));
                                    }
                                    Err(e) => {
                                        return Err(ValidationError::SecurityValidation(
                                            e.to_string(),
                                        ));
                                    }
                                }
                            }
                        }

                        // Check all string values for null bytes
                        if let Value::String(s) = value {
                            if s.contains('\0') {
                                return Err(ValidationError::SecurityValidation(
                                    "Null byte injection detected".to_string(),
                                ));
                            }
                        }
                    }
                }

                Ok(())
            },
        );
    }

    fn register_sinex_rules(&mut self) {
        // agent.heartbeat validation
        self.register_rule("sinex", "agent.heartbeat", |payload| {
            // Required: agent_name, status, version
            let _agent_name = validate_required_string_field(payload, "agent_name")?;
            let _status = validate_required_string_field(payload, "status")?;
            let _version = validate_required_string_field(payload, "version")?;

            // Optional numeric fields
            let _uptime =
                validate_optional_numeric_field(payload, "uptime_seconds", |v| v.as_u64())?;
            let _events =
                validate_optional_numeric_field(payload, "events_processed_session", |v| {
                    v.as_u64()
                })?;
            let _dlq_size = validate_optional_numeric_field(payload, "dlq_size", |v| v.as_u64())?;

            Ok(())
        });

        // agent.error validation
        self.register_rule("sinex", "agent.error", |payload| {
            // Required: agent_name, error_message
            let _agent_name = validate_required_string_field(payload, "agent_name")?;
            let _error_message = validate_required_string_field(payload, "error_message")?;

            // Optional: severity (must be valid level)
            if let Some(sev) = validate_optional_field(
                payload,
                "severity",
                |v| v.as_str().map(|s| s.to_string()),
                "string",
            )? {
                if !["warning", "error", "critical"].contains(&&*sev) {
                    return Err(ValidationError::InvalidValue {
                        field: "severity".to_string(),
                        reason: "must be one of: warning, error, critical".to_string(),
                    });
                }
            }

            Ok(())
        });
    }
}

impl Default for EventValidator {
    fn default() -> Self {
        Self::new()
    }
}
