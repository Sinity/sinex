#![doc = include_str!("../doc/validator.md")]

//! Event validation wrapper that reuses sinex-core's shared validator logic while
//! keeping ingestd-specific ergonomics (stats, enum result, etc.).

use crate::IngestdResult;
use sinex_core::db::validation::{
    EventValidator as CoreEventValidator, SchemaInfo, SchemaValidationOutcome,
};
use sinex_core::db::SqlxPgPool as PgPool;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_core::types::error::SinexError;
use sinex_core::types::ulid::Ulid;
use sinex_core::JsonValue;
use std::sync::Arc;

/// Event validator that wraps the shared sinex-core validator.
#[derive(Clone)]
pub struct EventValidator {
    inner: CoreEventValidator,
    validation_enabled: bool,
}

impl EventValidator {
    /// Create a new event validator (schemas can be loaded later).
    pub fn new(validation_enabled: bool) -> Self {
        Self {
            inner: CoreEventValidator::with_validation_enabled(validation_enabled),
            validation_enabled,
        }
    }

    /// Load schemas from the database.
    pub async fn load_schemas_from_db(
        pool: &PgPool,
        validation_enabled: bool,
    ) -> IngestdResult<Self> {
        let inner = CoreEventValidator::load_from_db_with_options(pool, validation_enabled)
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to load event schemas: {e}"))
                    .with_operation("validator.load_schemas")
            })?;

        Ok(Self {
            inner,
            validation_enabled,
        })
    }

    /// Reload schemas while keeping the existing validation toggle.
    pub async fn reload_schemas(&mut self, pool: &PgPool) -> IngestdResult<usize> {
        self.inner.reload_schemas(pool).await.map_err(|e| {
            SinexError::database(format!("Failed to reload schemas: {e}"))
                .with_operation("validator.reload_schemas")
        })
    }

    /// Use the shared validator to check payloads and convert into ingestd-specific outcomes.
    pub fn validate_payload_for(
        &self,
        source: &str,
        event_type: &str,
        payload: &JsonValue,
    ) -> ValidationResult {
        if !self.validation_enabled {
            return ValidationResult::Skipped;
        }

        match self.inner.validate_payload_for(source, event_type, payload) {
            SchemaValidationOutcome::Valid => ValidationResult::Valid,
            SchemaValidationOutcome::NoSchema => ValidationResult::NoSchema,
            SchemaValidationOutcome::SchemaNotFound { schema_id } => {
                ValidationResult::SchemaNotFound { schema_id }
            }
            SchemaValidationOutcome::Invalid { errors } => ValidationResult::Invalid { errors },
        }
    }

    /// Validate a full event structure (used in tests and pipelines).
    pub fn validate_event(
        &self,
        event: &sinex_core::db::models::event::Event<JsonValue>,
    ) -> ValidationResult {
        if !self.validation_enabled {
            return ValidationResult::Skipped;
        }

        match self.inner.validate(event) {
            Ok(()) => ValidationResult::Valid,
            Err(err) => ValidationResult::Invalid {
                errors: vec![err.to_string()],
            },
        }
    }

    /// Get current schema count.
    pub fn schema_count(&self) -> usize {
        self.inner.schema_count()
    }

    /// Schema diagnostics for admin endpoints.
    pub fn get_available_schemas(&self) -> Vec<SchemaInfo> {
        self.inner.get_available_schemas()
    }

    /// Lookup schema ID for a source/event pair.
    pub fn get_schema_id(&self, source: &EventSource, event_type: &EventType) -> Option<Ulid> {
        self.inner.get_schema_id(source, event_type)
    }

    /// Lookup schema version for a source/event pair.
    pub fn get_schema_version(
        &self,
        source: &EventSource,
        event_type: &EventType,
    ) -> Option<Arc<String>> {
        self.inner.get_schema_version(source, event_type)
    }

    /// Load all schema versions (used by replay tooling).
    pub async fn load_all_schema_versions(&mut self, pool: &PgPool) -> IngestdResult<()> {
        self.inner
            .load_all_schema_versions(pool)
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to load schema versions: {e}"))
                    .with_operation("validator.load_all_schema_versions")
            })
    }
}

/// Result of event validation.
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Event is valid
    Valid,
    /// Validation was skipped (disabled)
    Skipped,
    /// No schema specified for the event
    NoSchema,
    /// Schema not found
    SchemaNotFound { schema_id: Ulid },
    /// Event is invalid
    Invalid { errors: Vec<String> },
}

impl ValidationResult {
    /// Check if the event should be accepted
    pub fn should_accept(&self) -> bool {
        matches!(
            self,
            ValidationResult::Valid
                | ValidationResult::Skipped
                | ValidationResult::NoSchema
                | ValidationResult::SchemaNotFound { .. }
        )
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
                Some(format!("Schema not found: {schema_id}"))
            }
            _ => None,
        }
    }
}

/// Information about a schema (re-exported from sinex-core)

/// Validation statistics (kept for compatibility although not currently used).
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

    pub fn success_rate(&self) -> f64 {
        if self.total_validated == 0 {
            0.0
        } else {
            (self.valid_count + self.no_schema_count + self.skipped_count) as f64
                / self.total_validated as f64
        }
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}
