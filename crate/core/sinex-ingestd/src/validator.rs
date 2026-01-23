#![doc = include_str!("../docs/validator.md")]

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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Validation statistics counters for observability.
#[derive(Debug, Default)]
pub struct ValidationStats {
    pub valid: AtomicU64,
    pub skipped: AtomicU64,
    pub no_schema: AtomicU64,
    pub schema_not_found: AtomicU64,
    pub invalid: AtomicU64,
}

impl ValidationStats {
    /// Get a snapshot of current stats.
    pub fn snapshot(&self) -> ValidationStatsSnapshot {
        ValidationStatsSnapshot {
            valid: self.valid.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            no_schema: self.no_schema.load(Ordering::Relaxed),
            schema_not_found: self.schema_not_found.load(Ordering::Relaxed),
            invalid: self.invalid.load(Ordering::Relaxed),
        }
    }

    fn record(&self, result: &ValidationResult) {
        match result {
            ValidationResult::Valid => self.valid.fetch_add(1, Ordering::Relaxed),
            ValidationResult::Skipped => self.skipped.fetch_add(1, Ordering::Relaxed),
            ValidationResult::NoSchema => self.no_schema.fetch_add(1, Ordering::Relaxed),
            ValidationResult::SchemaNotFound { .. } => {
                self.schema_not_found.fetch_add(1, Ordering::Relaxed)
            }
            ValidationResult::Invalid { .. } => self.invalid.fetch_add(1, Ordering::Relaxed),
        };
    }
}

/// Point-in-time snapshot of validation statistics.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct ValidationStatsSnapshot {
    pub valid: u64,
    pub skipped: u64,
    pub no_schema: u64,
    pub schema_not_found: u64,
    pub invalid: u64,
}

impl ValidationStatsSnapshot {
    /// Total events validated.
    pub fn total(&self) -> u64 {
        self.valid + self.skipped + self.no_schema + self.schema_not_found + self.invalid
    }

    /// Coverage percentage: events with schema / total validated (excluding skipped).
    pub fn coverage_pct(&self) -> f64 {
        let with_schema = self.valid + self.invalid;
        let total_validated = with_schema + self.no_schema + self.schema_not_found;
        if total_validated == 0 {
            100.0
        } else {
            (with_schema as f64 / total_validated as f64) * 100.0
        }
    }
}

/// Event validator that wraps the shared sinex-core validator.
#[derive(Clone)]
pub struct EventValidator {
    inner: CoreEventValidator,
    validation_enabled: bool,
    strict_mode: bool,
    stats: Arc<ValidationStats>,
}

impl EventValidator {
    /// Create a new event validator (schemas can be loaded later).
    pub fn new(validation_enabled: bool) -> Self {
        Self {
            inner: CoreEventValidator::with_validation_enabled(validation_enabled),
            validation_enabled,
            strict_mode: false,
            stats: Arc::new(ValidationStats::default()),
        }
    }

    /// Create a validator with strict mode enabled.
    ///
    /// In strict mode, events without registered schemas are rejected.
    pub fn new_strict(validation_enabled: bool) -> Self {
        Self {
            inner: CoreEventValidator::with_validation_enabled(validation_enabled),
            validation_enabled,
            strict_mode: true,
            stats: Arc::new(ValidationStats::default()),
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
            strict_mode: false,
            stats: Arc::new(ValidationStats::default()),
        })
    }

    /// Load schemas with strict mode enabled.
    pub async fn load_schemas_from_db_strict(
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
            strict_mode: true,
            stats: Arc::new(ValidationStats::default()),
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
        let result = if !self.validation_enabled {
            ValidationResult::Skipped
        } else {
            match self.inner.validate_payload_for(source, event_type, payload) {
                SchemaValidationOutcome::Valid => ValidationResult::Valid,
                SchemaValidationOutcome::NoSchema => ValidationResult::NoSchema,
                SchemaValidationOutcome::SchemaNotFound { schema_id } => {
                    ValidationResult::SchemaNotFound { schema_id }
                }
                SchemaValidationOutcome::Invalid { errors } => ValidationResult::Invalid { errors },
            }
        };
        self.stats.record(&result);
        result
    }

    /// Validate a full event structure (used in tests and pipelines).
    pub fn validate_event(
        &self,
        event: &sinex_core::db::models::event::Event<JsonValue>,
    ) -> ValidationResult {
        let result = if !self.validation_enabled {
            ValidationResult::Skipped
        } else {
            match self.inner.validate(event) {
                Ok(()) => ValidationResult::Valid,
                Err(err) => ValidationResult::Invalid {
                    errors: vec![err.to_string()],
                },
            }
        };
        self.stats.record(&result);
        result
    }

    /// Get current schema count.
    pub fn schema_count(&self) -> usize {
        self.inner.schema_count()
    }

    /// Get validation statistics snapshot.
    pub fn stats(&self) -> ValidationStatsSnapshot {
        self.stats.snapshot()
    }

    /// Check if strict validation mode is enabled.
    pub fn is_strict_mode(&self) -> bool {
        self.strict_mode
    }

    /// Get a reference to the stats Arc for sharing with metrics emitters.
    pub fn stats_handle(&self) -> Arc<ValidationStats> {
        Arc::clone(&self.stats)
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
