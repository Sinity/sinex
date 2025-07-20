use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::Value;
use sinex_core_types::ValidationChain;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::query_helpers::uuid_to_ulid;
use crate::queries::SchemaQueries;
use crate::security::{SecurityError, SecurityValidator};
use crate::{DbPool, RawEvent}; // Re-exported from sinex-events
use sinex_ulid::Ulid;
use sqlx::FromRow;

/// Record structure for active schema queries
#[derive(Debug, FromRow)]
struct ActiveSchemaRecord {
    pub schema_id: String,
    pub event_source: String,
    pub event_type: String,
    pub schema_version: Option<i32>,
    pub schema_content: Value,
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

    // Use the ValidationChain directly since it returns Result<T, CoreError>
    let _validated_string = ValidationChain::validate(string_value.clone(), field_name)
        .not_empty()
        .into_result()
        .map_err(|e| ValidationError::InvalidValue {
            field: field_name.to_string(),
            reason: e.to_string(),
        })?;

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

/// Data integrity validator for comprehensive system-wide validation
///
/// Provides validation for:
/// - Schema validation testing
/// - ULID ordering verification  
/// - Checkpoint consistency checks
/// - Data corruption detection
pub struct DataIntegrityValidator<'a> {
    pool: &'a DbPool,
    event_validator: EventValidator,
}

/// Results from a comprehensive data integrity check
#[derive(Debug, Clone)]
pub struct IntegrityCheckReport {
    pub schema_violations: Vec<SchemaViolation>,
    pub ulid_ordering_violations: Vec<UlidOrderingViolation>,
    pub checkpoint_inconsistencies: Vec<CheckpointInconsistency>,
    pub data_corruption_indicators: Vec<DataCorruptionIndicator>,
    pub total_events_checked: u64,
    pub check_duration: Duration,
    pub severity: IntegritySeverity,
}

/// Schema validation violation found during integrity checks
#[derive(Debug, Clone)]
pub struct SchemaViolation {
    pub event_id: Ulid,
    pub source: String,
    pub event_type: String,
    pub violation_type: SchemaViolationType,
    pub details: String,
    pub payload_sample: Option<Value>,
}

/// ULID ordering violation found during integrity checks
#[derive(Debug, Clone)]
pub struct UlidOrderingViolation {
    pub event_id_1: Ulid,
    pub event_id_2: Ulid,
    pub timestamp_1: DateTime<Utc>,
    pub timestamp_2: DateTime<Utc>,
    pub violation_type: OrderingViolationType,
    pub details: String,
}

/// Checkpoint consistency issue found during integrity checks
#[derive(Debug, Clone)]
pub struct CheckpointInconsistency {
    pub automaton_name: String,
    pub checkpoint_ulid: Option<Ulid>,
    pub last_processed_ulid: Option<Ulid>,
    pub inconsistency_type: CheckpointInconsistencyType,
    pub details: String,
    pub events_potentially_missed: u64,
}

/// Data corruption indicator found during integrity checks
#[derive(Debug, Clone)]
pub struct DataCorruptionIndicator {
    pub event_id: Ulid,
    pub corruption_type: DataCorruptionType,
    pub details: String,
    pub recovery_suggestion: String,
}

/// Types of schema validation violations
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaViolationType {
    MalformedPayload,
    MissingRequiredField,
    InvalidFieldType,
    SchemaNotFound,
    PayloadTooLarge,
    InvalidCharacters,
}

/// Types of ULID ordering violations
#[derive(Debug, Clone, PartialEq, strum::Display)]
pub enum OrderingViolationType {
    TimestampRegression,
    UlidRegression,
    InvalidTimestamp,
    ClockSkew,
}

/// Types of checkpoint inconsistencies
#[derive(Debug, Clone, PartialEq, Eq, Hash, strum::Display)]
pub enum CheckpointInconsistencyType {
    CheckpointBehindEvents,
    CheckpointAheadOfEvents,
    MissingCheckpoint,
    InvalidCheckpointFormat,
    StaleCheckpoint,
}

/// Types of data corruption indicators
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataCorruptionType {
    InvalidUlid,
    NullPayload,
    TruncatedData,
    EncodingError,
    ForeignKeyViolation,
}

impl std::fmt::Display for DataCorruptionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUlid => write!(f, "Invalid ULID"),
            Self::NullPayload => write!(f, "Null Payload"),
            Self::TruncatedData => write!(f, "Truncated Data"),
            Self::EncodingError => write!(f, "Encoding Error"),
            Self::ForeignKeyViolation => write!(f, "Foreign Key Violation"),
        }
    }
}

/// Overall severity of integrity check results
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq)]
pub enum IntegritySeverity {
    Clean,
    Minor,
    Warning,
    Critical,
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
        let schemas = SchemaQueries::get_all_active_schemas()
            .fetch_all::<ActiveSchemaRecord>(pool)
            .await?;

        for schema_record in schemas {
            match jsonschema::JSONSchema::compile(&schema_record.schema_content) {
                Ok(compiled_schema) => {
                    if let Ok(schema_ulid) = Ulid::from_str(&schema_record.schema_id) {
                        validator.schemas.insert(schema_ulid, compiled_schema);
                    } else {
                        warn!(
                            "Invalid ULID format for schema {}: {}",
                            schema_record.schema_id, schema_record.schema_id
                        );
                    }
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
        let _validated_source = ValidationChain::validate(sanitized_source, "source")
            .not_empty()
            .into_result()
            .map_err(|e| ValidationError::InvalidValue {
                field: "source".to_string(),
                reason: e.to_string(),
            })?;

        let _validated_event_type = ValidationChain::validate(event_type.to_string(), "event_type")
            .not_empty()
            .into_result()
            .map_err(|e| ValidationError::InvalidValue {
                field: "event_type".to_string(),
                reason: e.to_string(),
            })?;

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
        self.register_rule("sinex", "automaton.heartbeat", |payload| {
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
        self.register_rule("sinex", "automaton.error", |payload| {
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

// =============================================================================
// Data Integrity Validator Implementation
// =============================================================================

impl<'a> DataIntegrityValidator<'a> {
    /// Create a new data integrity validator
    pub async fn new(pool: &'a DbPool) -> Result<Self> {
        let event_validator = EventValidator::load_from_db(pool).await?;
        Ok(Self {
            pool,
            event_validator,
        })
    }

    /// Get the database pool
    pub fn pool(&self) -> &'static DbPool {
        self.pool
    }

    /// Perform comprehensive data integrity validation
    pub async fn validate_integrity(&self) -> Result<IntegrityCheckReport> {
        let start_time = Instant::now();

        // Run all integrity checks in parallel
        let (
            schema_violations,
            ulid_violations,
            checkpoint_issues,
            corruption_indicators,
            total_events,
        ) = tokio::try_join!(
            self.check_schema_violations(),
            self.check_ulid_ordering_violations(),
            self.check_checkpoint_consistency(),
            self.check_data_corruption(),
            self.count_total_events()
        )?;

        let check_duration = start_time.elapsed();
        let severity = self.determine_severity(
            &schema_violations,
            &ulid_violations,
            &checkpoint_issues,
            &corruption_indicators,
        );

        Ok(IntegrityCheckReport {
            schema_violations,
            ulid_ordering_violations: ulid_violations,
            checkpoint_inconsistencies: checkpoint_issues,
            data_corruption_indicators: corruption_indicators,
            total_events_checked: total_events,
            check_duration,
            severity,
        })
    }

    /// Check for schema validation violations
    async fn check_schema_violations(&self) -> Result<Vec<SchemaViolation>> {
        let mut violations = Vec::new();

        // Sample recent events to check for schema violations
        let recent_events = sqlx::query_as!(
            RawEventRecord,
            r#"
            SELECT 
                event_id::uuid as "id!",
                source,
                event_type,
                ts_orig,
                ts_ingest,
                host,
                payload,
                source_event_ids::uuid[] as "source_event_ids?",
                source_material_id::uuid as "source_material_id?",
                source_material_offset_start,
                source_material_offset_end,
                anchor_byte,
                associated_blob_ids::uuid[] as "associated_blob_ids?",
                ingestor_version,
                payload_schema_id::uuid as "payload_schema_id?"
            FROM core.events 
            WHERE ts_ingest > NOW() - INTERVAL '1 hour'
            ORDER BY ts_ingest DESC
            LIMIT 1000
            "#
        )
        .fetch_all(self.pool)
        .await?;

        for event_record in recent_events {
            let raw_event = RawEvent::try_from(event_record)?;

            // Check schema validation
            if let Err(validation_error) = self.event_validator.validate(&raw_event) {
                let violation_type = match validation_error {
                    ValidationError::MissingField { .. } => {
                        SchemaViolationType::MissingRequiredField
                    }
                    ValidationError::InvalidType { .. } => SchemaViolationType::InvalidFieldType,
                    ValidationError::SchemaNotFound(_) => SchemaViolationType::SchemaNotFound,
                    ValidationError::SecurityValidation(_) => {
                        SchemaViolationType::InvalidCharacters
                    }
                    _ => SchemaViolationType::MalformedPayload,
                };

                violations.push(SchemaViolation {
                    event_id: raw_event.id,
                    source: raw_event.source.clone(),
                    event_type: raw_event.event_type.clone(),
                    violation_type,
                    details: validation_error.to_string(),
                    payload_sample: Some(raw_event.payload.clone()),
                });
            }

            // Check for payload size violations
            let payload_str = raw_event.payload.to_string();
            if payload_str.len() > 1_000_000 {
                // 1MB limit
                violations.push(SchemaViolation {
                    event_id: raw_event.id,
                    source: raw_event.source.clone(),
                    event_type: raw_event.event_type.clone(),
                    violation_type: SchemaViolationType::PayloadTooLarge,
                    details: format!("Payload size {} exceeds 1MB limit", payload_str.len()),
                    payload_sample: None, // Don't include large payloads in report
                });
            }
        }

        Ok(violations)
    }

    /// Check for ULID ordering violations that indicate data corruption
    async fn check_ulid_ordering_violations(&self) -> Result<Vec<UlidOrderingViolation>> {
        let mut violations = Vec::new();

        // Check for timestamp regressions in recent events
        let potential_violations = sqlx::query!(
            r#"
            WITH ordered_events AS (
                SELECT 
                    event_id::uuid as id,
                    ts_orig,
                    ts_ingest,
                    LAG(event_id::uuid) OVER (ORDER BY event_id) as prev_id,
                    LAG(ts_orig) OVER (ORDER BY event_id) as prev_ts_orig,
                    LAG(ts_ingest) OVER (ORDER BY event_id) as prev_ts_ingest
                FROM core.events
                WHERE ts_ingest > NOW() - INTERVAL '1 hour'
                ORDER BY event_id
                LIMIT 10000
            )
            SELECT 
                id, ts_orig, ts_ingest,
                prev_id, prev_ts_orig, prev_ts_ingest
            FROM ordered_events
            WHERE prev_id IS NOT NULL
              AND (ts_orig < prev_ts_orig OR ts_ingest < prev_ts_ingest)
            "#
        )
        .fetch_all(self.pool)
        .await?;

        for violation in potential_violations {
            if let (Some(id), Some(prev_id), Some(ts_orig), Some(prev_ts_orig)) = (
                violation.id,
                violation.prev_id,
                violation.ts_orig,
                violation.prev_ts_orig,
            ) {
                let event_id = Ulid::from_uuid(id);
                let prev_event_id = Ulid::from_uuid(prev_id);

                let violation_type = if ts_orig < prev_ts_orig {
                    OrderingViolationType::TimestampRegression
                } else {
                    OrderingViolationType::UlidRegression
                };

                violations.push(UlidOrderingViolation {
                    event_id_1: prev_event_id,
                    event_id_2: event_id,
                    timestamp_1: prev_ts_orig,
                    timestamp_2: ts_orig,
                    violation_type,
                    details: format!("ULID ordering violated: {} -> {}", prev_event_id, event_id),
                });
            }
        }

        // Check for invalid timestamps (too far in future/past)
        let _now = Utc::now();
        let invalid_timestamps = sqlx::query!(
            r#"
            SELECT event_id::uuid as id, ts_orig, ts_ingest
            FROM core.events
            WHERE ts_orig > NOW() + INTERVAL '1 hour'
               OR ts_orig < '2020-01-01'::timestamptz
               OR ts_ingest > NOW() + INTERVAL '1 hour'
            LIMIT 100
            "#
        )
        .fetch_all(self.pool)
        .await?;

        for invalid in invalid_timestamps {
            if let Some(id) = invalid.id {
                let event_id = Ulid::from_uuid(id);
                let ts_orig = invalid.ts_orig.unwrap_or_else(|| Utc::now());
                violations.push(UlidOrderingViolation {
                    event_id_1: event_id,
                    event_id_2: event_id,
                    timestamp_1: ts_orig,
                    timestamp_2: invalid.ts_ingest,
                    violation_type: OrderingViolationType::InvalidTimestamp,
                    details: format!(
                        "Invalid timestamp detected: orig={}, ingest={}",
                        ts_orig, invalid.ts_ingest
                    ),
                });
            }
        }

        Ok(violations)
    }

    /// Check checkpoint consistency across all automata
    async fn check_checkpoint_consistency(&self) -> Result<Vec<CheckpointInconsistency>> {
        let mut inconsistencies = Vec::new();

        // Get all active automatons and their checkpoints
        let checkpoints = sqlx::query!(
            r#"
            SELECT 
                automaton_name,
                last_processed_id,
                processed_count,
                last_activity,
                state_data
            FROM core.automaton_checkpoints
            "#
        )
        .fetch_all(self.pool)
        .await?;

        for checkpoint in checkpoints {
            let automaton_name = checkpoint.automaton_name;

            // Check if checkpoint refers to valid event
            if let Some(last_processed_str) = &checkpoint.last_processed_id {
                if let Ok(last_processed_ulid) = Ulid::from_str(last_processed_str) {
                    // Check if this event exists
                    let event_exists = sqlx::query_scalar!(
                        "SELECT 1 FROM core.events WHERE event_id::text = $1 LIMIT 1",
                        last_processed_str
                    )
                    .fetch_optional(self.pool)
                    .await?
                    .is_some();

                    if !event_exists {
                        inconsistencies.push(CheckpointInconsistency {
                            automaton_name: automaton_name.clone(),
                            checkpoint_ulid: Some(last_processed_ulid),
                            last_processed_ulid: Some(last_processed_ulid),
                            inconsistency_type:
                                CheckpointInconsistencyType::CheckpointAheadOfEvents,
                            details: format!(
                                "Checkpoint references non-existent event {}",
                                last_processed_ulid
                            ),
                            events_potentially_missed: 0,
                        });
                    }

                    // Check if there are newer events that should have been processed
                    let newer_events_count = sqlx::query_scalar!(
                        r#"
                        SELECT COUNT(*)::bigint
                        FROM core.events
                        WHERE event_id::text > $1
                          AND ts_ingest < NOW() - INTERVAL '5 minutes'
                        "#,
                        last_processed_str
                    )
                    .fetch_one(self.pool)
                    .await?
                    .unwrap_or(0);

                    if newer_events_count > 0 {
                        inconsistencies.push(CheckpointInconsistency {
                            automaton_name: automaton_name.clone(),
                            checkpoint_ulid: Some(last_processed_ulid),
                            last_processed_ulid: Some(last_processed_ulid),
                            inconsistency_type: CheckpointInconsistencyType::CheckpointBehindEvents,
                            details: format!(
                                "Checkpoint is behind by {} events",
                                newer_events_count
                            ),
                            events_potentially_missed: newer_events_count as u64,
                        });
                    }
                } else {
                    // Invalid ULID format in checkpoint
                    inconsistencies.push(CheckpointInconsistency {
                        automaton_name: automaton_name.clone(),
                        checkpoint_ulid: None,
                        last_processed_ulid: None,
                        inconsistency_type: CheckpointInconsistencyType::InvalidCheckpointFormat,
                        details: format!(
                            "Invalid ULID format in checkpoint: {}",
                            last_processed_str
                        ),
                        events_potentially_missed: 0,
                    });
                }
            }

            // Check for stale checkpoints (not updated recently)
            let last_activity = checkpoint.last_activity;
            let time_since_update = Utc::now().signed_duration_since(last_activity);
            if time_since_update > ChronoDuration::hours(1) {
                inconsistencies.push(CheckpointInconsistency {
                    automaton_name: automaton_name.clone(),
                    checkpoint_ulid: checkpoint
                        .last_processed_id
                        .as_ref()
                        .and_then(|s| Ulid::from_str(s).ok()),
                    last_processed_ulid: checkpoint
                        .last_processed_id
                        .as_ref()
                        .and_then(|s| Ulid::from_str(s).ok()),
                    inconsistency_type: CheckpointInconsistencyType::StaleCheckpoint,
                    details: format!(
                        "Checkpoint not updated for {} hours",
                        time_since_update.num_hours()
                    ),
                    events_potentially_missed: 0,
                });
            }
        }

        Ok(inconsistencies)
    }

    /// Check for data corruption indicators
    async fn check_data_corruption(&self) -> Result<Vec<DataCorruptionIndicator>> {
        let mut indicators = Vec::new();

        // Check for events with null or empty payloads
        let null_payloads = sqlx::query!(
            r#"
            SELECT event_id::uuid as id, source, event_type
            FROM core.events
            WHERE payload IS NULL OR payload = 'null'::jsonb
            LIMIT 100
            "#
        )
        .fetch_all(self.pool)
        .await?;

        for null_payload in null_payloads {
            indicators.push(DataCorruptionIndicator {
                event_id: Ulid::from_uuid(null_payload.id.unwrap()),
                corruption_type: DataCorruptionType::NullPayload,
                details: format!(
                    "Event has null payload: {}/{}",
                    null_payload.source, null_payload.event_type
                ),
                recovery_suggestion:
                    "Investigate data ingestion pipeline for null payload injection".to_string(),
            });
        }

        // Check for events with invalid ULIDs (should not happen with proper constraints)
        let invalid_ulids = sqlx::query!(
            r#"
            SELECT event_id::text as id_str, source, event_type
            FROM core.events
            WHERE LENGTH(event_id::text) != 36  -- UUID string length
            LIMIT 100
            "#
        )
        .fetch_all(self.pool)
        .await?;

        for invalid_ulid in invalid_ulids {
            indicators.push(DataCorruptionIndicator {
                event_id: Ulid::new(), // Generate placeholder since original is invalid
                corruption_type: DataCorruptionType::InvalidUlid,
                details: format!(
                    "Invalid ULID format: {} for {}/{}",
                    invalid_ulid.id_str.unwrap_or_default(),
                    invalid_ulid.source,
                    invalid_ulid.event_type
                ),
                recovery_suggestion: "Check database constraints and ULID generation logic"
                    .to_string(),
            });
        }

        // Check for potential encoding issues in text fields
        let encoding_issues = sqlx::query!(
            r#"
            SELECT event_id::uuid as id, source, event_type
            FROM core.events
            WHERE source ~ '[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]'
               OR event_type ~ '[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]'
               OR host ~ '[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]'
            LIMIT 100
            "#
        )
        .fetch_all(self.pool)
        .await?;

        for encoding_issue in encoding_issues {
            indicators.push(DataCorruptionIndicator {
                event_id: Ulid::from_uuid(encoding_issue.id.unwrap()),
                corruption_type: DataCorruptionType::EncodingError,
                details: format!(
                    "Control characters detected in event fields: {}/{}",
                    encoding_issue.source, encoding_issue.event_type
                ),
                recovery_suggestion: "Check input sanitization and encoding validation".to_string(),
            });
        }

        Ok(indicators)
    }

    /// Count total events for reporting
    async fn count_total_events(&self) -> Result<u64> {
        let count: i64 = sqlx::query_scalar!("SELECT COUNT(*)::bigint FROM core.events")
            .fetch_one(self.pool)
            .await?
            .unwrap_or(0);
        Ok(count as u64)
    }

    /// Determine overall severity based on findings
    fn determine_severity(
        &self,
        schema_violations: &[SchemaViolation],
        ulid_violations: &[UlidOrderingViolation],
        checkpoint_issues: &[CheckpointInconsistency],
        corruption_indicators: &[DataCorruptionIndicator],
    ) -> IntegritySeverity {
        // Critical issues
        if corruption_indicators.iter().any(|i| {
            matches!(
                i.corruption_type,
                DataCorruptionType::InvalidUlid | DataCorruptionType::NullPayload
            )
        }) || ulid_violations.iter().any(|v| {
            matches!(
                v.violation_type,
                OrderingViolationType::TimestampRegression | OrderingViolationType::UlidRegression
            )
        }) || checkpoint_issues.iter().any(|c| {
            matches!(
                c.inconsistency_type,
                CheckpointInconsistencyType::CheckpointAheadOfEvents
            )
        }) {
            return IntegritySeverity::Critical;
        }

        // Warning issues
        if !schema_violations.is_empty()
            || !checkpoint_issues.is_empty()
            || corruption_indicators
                .iter()
                .any(|i| matches!(i.corruption_type, DataCorruptionType::EncodingError))
        {
            return IntegritySeverity::Warning;
        }

        // Minor issues
        if ulid_violations
            .iter()
            .any(|v| matches!(v.violation_type, OrderingViolationType::ClockSkew))
            || checkpoint_issues.iter().any(|c| {
                matches!(
                    c.inconsistency_type,
                    CheckpointInconsistencyType::StaleCheckpoint
                )
            })
        {
            return IntegritySeverity::Minor;
        }

        IntegritySeverity::Clean
    }

    /// Validate a specific event's schema and integrity
    pub fn validate_event_integrity(
        &self,
        event: &RawEvent,
    ) -> Result<Vec<ValidationError>, ValidationError> {
        let mut errors = Vec::new();

        // Schema validation
        if let Err(e) = self.event_validator.validate(event) {
            errors.push(e);
        }

        // ULID validation
        if event.id.is_nil() {
            errors.push(ValidationError::InvalidValue {
                field: "id".to_string(),
                reason: "ULID cannot be nil".to_string(),
            });
        }

        // Timestamp validation
        let now = Utc::now();
        if let Some(ts_orig) = event.ts_orig {
            if ts_orig > now + ChronoDuration::minutes(5) {
                errors.push(ValidationError::InvalidValue {
                    field: "ts_orig".to_string(),
                    reason: "Timestamp too far in future".to_string(),
                });
            }

            if ts_orig < DateTime::from_timestamp(946684800, 0).unwrap() {
                // 2000-01-01
                errors.push(ValidationError::InvalidValue {
                    field: "ts_orig".to_string(),
                    reason: "Timestamp too far in past".to_string(),
                });
            }
        }

        if errors.is_empty() {
            Ok(errors)
        } else {
            Err(errors.into_iter().next().unwrap()) // Return first error for compatibility
        }
    }
}

// Helper struct for database queries
#[derive(Debug)]
struct RawEventRecord {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub ts_orig: Option<DateTime<Utc>>,
    pub ts_ingest: DateTime<Utc>,
    pub host: String,
    pub payload: Value,
    pub source_event_ids: Option<Vec<Uuid>>,
    pub source_material_id: Option<Uuid>,
    pub source_material_offset_start: Option<i64>,
    pub source_material_offset_end: Option<i64>,
    pub anchor_byte: Option<i64>,
    pub associated_blob_ids: Option<Vec<Uuid>>,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Uuid>,
}

impl TryFrom<RawEventRecord> for RawEvent {
    type Error = anyhow::Error;

    fn try_from(record: RawEventRecord) -> Result<Self, Self::Error> {
        Ok(RawEvent {
            id: Ulid::from_uuid(record.id),
            source: record.source,
            event_type: record.event_type,
            ts_orig: record.ts_orig,
            ts_ingest: record.ts_ingest,
            host: record.host,
            payload: record.payload,
            source_event_ids: record
                .source_event_ids
                .map(|uuids| uuids.into_iter().map(|uuid| uuid_to_ulid(uuid)).collect()),
            source_material_id: record.source_material_id.map(Ulid::from_uuid),
            source_material_offset_start: record.source_material_offset_start,
            source_material_offset_end: record.source_material_offset_end,
            anchor_byte: record.anchor_byte,
            associated_blob_ids: record
                .associated_blob_ids
                .map(|uuids| uuids.into_iter().map(|uuid| uuid_to_ulid(uuid)).collect()),
            ingestor_version: record.ingestor_version,
            payload_schema_id: record.payload_schema_id.map(Ulid::from_uuid),
        })
    }
}
