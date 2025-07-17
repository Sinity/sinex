//! Event validation for the ingestion daemon

use crate::IngestdResult;
use sinex_db::{queries::SchemaQueries, SqlxPgPool as PgPool};
use sinex_events::RawEvent;
use sqlx::FromRow;
use std::collections::HashMap;
use tracing::{debug, warn};

/// Schema record from database
#[derive(Debug, FromRow)]
struct ActiveSchemaRow {
    schema_id: Option<String>,
    event_source: String,
    event_type: String,
    schema_version: Option<i32>,
    schema_content: serde_json::Value,
}

/// Event validator that checks events against JSON schemas
#[derive(Debug)]
pub struct EventValidator {
    schemas: HashMap<String, jsonschema::JSONSchema>,
    schema_versions: HashMap<String, String>,
    validation_enabled: bool,
}

impl EventValidator {
    /// Create a new event validator
    pub fn new(validation_enabled: bool) -> Self {
        Self {
            schemas: HashMap::new(),
            schema_versions: HashMap::new(),
            validation_enabled,
        }
    }

    /// Load schemas from database
    pub async fn load_schemas_from_db(
        pool: &PgPool,
        validation_enabled: bool,
    ) -> IngestdResult<Self> {
        let mut validator = Self::new(validation_enabled);

        if !validation_enabled {
            debug!("Schema validation disabled");
            return Ok(validator);
        }

        // Load all active schemas from the database using the query system
        let rows: Vec<ActiveSchemaRow> = SchemaQueries::get_all_active_schemas()
            .fetch_all(pool)
            .await?;

        for row in rows {
            let schema_key = row.schema_id.unwrap_or_default();

            match jsonschema::JSONSchema::compile(&row.schema_content) {
                Ok(compiled_schema) => {
                    validator
                        .schemas
                        .insert(schema_key.clone(), compiled_schema);
                    debug!(
                        schema_id = %schema_key,
                        event_source = %row.event_source,
                        event_type = %row.event_type,
                        version = ?row.schema_version,
                        "Loaded schema"
                    );
                }
                Err(e) => {
                    warn!(
                        schema_id = %schema_key,
                        event_source = %row.event_source,
                        event_type = %row.event_type,
                        version = ?row.schema_version,
                        error = %e,
                        "Failed to compile schema, skipping"
                    );
                }
            }
        }

        debug!(
            schema_count = validator.schemas.len(),
            "Loaded schemas from database"
        );

        Ok(validator)
    }

    /// Validate a raw event
    pub fn validate_event(&self, event: &RawEvent) -> IngestdResult<ValidationResult> {
        if !self.validation_enabled {
            return Ok(ValidationResult::Skipped);
        }

        // If no schema is specified, allow the event
        let schema_id = match &event.payload_schema_id {
            Some(id) => id,
            None => return Ok(ValidationResult::NoSchema),
        };

        // Find the schema
        let schema_key = schema_id.to_string();
        let schema = match self.schemas.get(&schema_key) {
            Some(schema) => schema,
            None => {
                warn!(
                    schema_id = %schema_id,
                    "Schema not found"
                );
                return Ok(ValidationResult::SchemaNotFound {
                    schema_id: *schema_id,
                });
            }
        };

        // Validate the payload
        match schema.validate(&event.payload) {
            Ok(()) => {
                debug!(
                    source = %event.source,
                    event_type = %event.event_type,
                    schema = %schema_key,
                    "Event validation passed"
                );
                Ok(ValidationResult::Valid)
            }
            Err(validation_errors) => {
                let errors: Vec<String> =
                    validation_errors.map(|error| error.to_string()).collect();

                warn!(
                    source = %event.source,
                    event_type = %event.event_type,
                    schema = %schema_key,
                    errors = ?errors,
                    "Event validation failed"
                );

                Ok(ValidationResult::Invalid { errors })
            }
        }
    }

    /// Validate a batch of events
    pub fn validate_batch(&self, events: &[RawEvent]) -> IngestdResult<Vec<ValidationResult>> {
        events
            .iter()
            .map(|event| self.validate_event(event))
            .collect()
    }

    /// Get available schemas
    pub fn get_available_schemas(&self) -> Vec<SchemaInfo> {
        self.schema_versions
            .iter()
            .map(|(name, version)| SchemaInfo {
                name: name.clone(),
                version: version.clone(),
                schema_key: format!("{}:{}", name, version),
            })
            .collect()
    }

    /// Check if a schema is available by ID
    pub fn has_schema_by_id(&self, schema_id: &sinex_ulid::Ulid) -> bool {
        let schema_key = schema_id.to_string();
        self.schemas.contains_key(&schema_key)
    }

    /// Get schema count
    pub fn schema_count(&self) -> usize {
        self.schemas.len()
    }

    /// Reload schemas from database
    pub async fn reload_schemas(&mut self, pool: &PgPool) -> IngestdResult<usize> {
        let new_validator = Self::load_schemas_from_db(pool, self.validation_enabled).await?;
        let old_count = self.schemas.len();

        self.schemas = new_validator.schemas;
        self.schema_versions = new_validator.schema_versions;

        let new_count = self.schemas.len();
        debug!(
            old_count = old_count,
            new_count = new_count,
            "Reloaded schemas from database"
        );

        Ok(new_count)
    }
}

/// Result of event validation
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Event is valid
    Valid,
    /// Validation was skipped (disabled)
    Skipped,
    /// No schema specified for the event
    NoSchema,
    /// Schema not found
    SchemaNotFound { schema_id: sinex_ulid::Ulid },
    /// Event is invalid
    Invalid { errors: Vec<String> },
}

impl ValidationResult {
    /// Check if the event should be accepted
    pub fn should_accept(&self) -> bool {
        match self {
            ValidationResult::Valid | ValidationResult::Skipped | ValidationResult::NoSchema => {
                true
            }
            ValidationResult::SchemaNotFound { .. } => true, // Accept but warn
            ValidationResult::Invalid { .. } => false,
        }
    }

    /// Check if the validation failed
    pub fn is_failure(&self) -> bool {
        matches!(self, ValidationResult::Invalid { .. })
    }

    /// Get error message if validation failed
    pub fn error_message(&self) -> Option<String> {
        match self {
            ValidationResult::Invalid { errors } => {
                Some(format!("Validation errors: {}", errors.join(", ")))
            }
            ValidationResult::SchemaNotFound { schema_id } => {
                Some(format!("Schema not found: {}", schema_id))
            }
            _ => None,
        }
    }
}

/// Information about a schema
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub name: String,
    pub version: String,
    pub schema_key: String,
}

/// Validation statistics
#[derive(Debug, Clone, Default)]
pub struct ValidationStats {
    pub total_validated: u64,
    pub valid_count: u64,
    pub invalid_count: u64,
    pub no_schema_count: u64,
    pub schema_not_found_count: u64,
    pub skipped_count: u64,
}

impl ValidationStats {
    /// Add a validation result to the stats
    pub fn add_result(&mut self, result: &ValidationResult) {
        self.total_validated += 1;

        match result {
            ValidationResult::Valid => self.valid_count += 1,
            ValidationResult::Invalid { .. } => self.invalid_count += 1,
            ValidationResult::NoSchema => self.no_schema_count += 1,
            ValidationResult::SchemaNotFound { .. } => self.schema_not_found_count += 1,
            ValidationResult::Skipped => self.skipped_count += 1,
        }
    }

    /// Get validation success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_validated == 0 {
            0.0
        } else {
            (self.valid_count + self.no_schema_count + self.skipped_count) as f64
                / self.total_validated as f64
        }
    }

    /// Reset the statistics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}
