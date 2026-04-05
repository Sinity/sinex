#![doc = include_str!("../docs/validator.md")]

//! Event validation wrapper that reuses sinex-db's shared validator logic while
//! keeping ingestd-specific ergonomics (stats, enum result, etc.).

use crate::IngestdResult;
use sinex_db::validation::{
    EventValidator as CoreEventValidator, SchemaInfo, SchemaValidationOutcome,
};
use sinex_primitives::JsonValue;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::error::SinexError;
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

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
            ValidationResult::Valid { .. } => self.valid.fetch_add(1, Ordering::Relaxed),
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
    #[must_use]
    pub fn total(&self) -> u64 {
        self.valid + self.skipped + self.no_schema + self.schema_not_found + self.invalid
    }

    /// Coverage percentage: events with schema / total validated (excluding skipped).
    #[must_use]
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

/// Event validator that wraps the shared sinex-db validator.
#[derive(Clone)]
pub struct EventValidator {
    inner: CoreEventValidator,
    validation_enabled: bool,
    strict_mode: bool,
    stats: Arc<ValidationStats>,
}

impl EventValidator {
    /// Create a new event validator (schemas can be loaded later).
    #[must_use]
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
    #[must_use]
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

    /// Load fresh schemas from the database and atomically swap the inner validator.
    ///
    /// Unlike `reload_schemas`, this does all I/O outside any external lock so callers
    /// can minimize the window during which they hold a write lock:
    ///
    /// ```rust,ignore
    /// // Load outside the write lock (does DB I/O):
    /// let new_inner = validator.read().await.load_fresh_schemas(&pool).await?;
    /// // Swap under a brief write lock (no I/O):
    /// validator.write().await.swap_inner(new_inner);
    /// ```
    pub async fn load_fresh_schemas(&self, pool: &PgPool) -> IngestdResult<CoreEventValidator> {
        CoreEventValidator::load_from_db_with_options(pool, self.validation_enabled)
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to load fresh schemas: {e}"))
                    .with_operation("validator.load_fresh_schemas")
            })
    }

    /// Swap in a freshly-loaded inner validator. Intended to be called under a
    /// brief write lock after `load_fresh_schemas` has done its I/O work.
    pub fn swap_inner(&mut self, new_inner: CoreEventValidator) {
        self.inner = new_inner;
    }

    /// Use the shared validator to check payloads and convert into ingestd-specific outcomes.
    #[must_use]
    pub fn validate_payload_for(
        &self,
        source: &str,
        event_type: &str,
        payload: &JsonValue,
    ) -> ValidationResult {
        let result = if self.validation_enabled {
            match self.inner.validate_payload_for(source, event_type, payload) {
                SchemaValidationOutcome::Valid { schema_id } => {
                    ValidationResult::Valid { schema_id }
                }
                SchemaValidationOutcome::NoSchema => ValidationResult::NoSchema,
                SchemaValidationOutcome::SchemaNotFound { schema_id } => {
                    ValidationResult::SchemaNotFound { schema_id }
                }
                SchemaValidationOutcome::Invalid { errors } => ValidationResult::Invalid { errors },
            }
        } else {
            ValidationResult::Skipped
        };
        self.stats.record(&result);
        result
    }

    /// Validate a full event structure (used in tests and pipelines).
    #[must_use]
    pub fn validate_event(&self, event: &sinex_db::models::Event<JsonValue>) -> ValidationResult {
        let result = if self.validation_enabled {
            match self.inner.validate(event) {
                Ok(()) => match self.inner.validate_payload_for(
                    event.source.as_ref(),
                    event.event_type.as_ref(),
                    &event.payload,
                ) {
                    SchemaValidationOutcome::Valid { schema_id } => {
                        ValidationResult::Valid { schema_id }
                    }
                    SchemaValidationOutcome::NoSchema => ValidationResult::NoSchema,
                    SchemaValidationOutcome::SchemaNotFound { schema_id } => {
                        ValidationResult::SchemaNotFound { schema_id }
                    }
                    SchemaValidationOutcome::Invalid { errors } => {
                        ValidationResult::Invalid { errors }
                    }
                },
                Err(err) => ValidationResult::Invalid {
                    errors: vec![err.to_string()],
                },
            }
        } else {
            ValidationResult::Skipped
        };
        self.stats.record(&result);
        result
    }

    /// Get current schema count.
    #[must_use]
    pub fn schema_count(&self) -> usize {
        self.inner.schema_count()
    }

    /// Get validation statistics snapshot.
    #[must_use]
    pub fn stats(&self) -> ValidationStatsSnapshot {
        self.stats.snapshot()
    }

    /// Check if strict validation mode is enabled.
    #[must_use]
    pub fn is_strict_mode(&self) -> bool {
        self.strict_mode
    }

    /// Get a reference to the stats Arc for sharing with metrics emitters.
    #[must_use]
    pub fn stats_handle(&self) -> Arc<ValidationStats> {
        Arc::clone(&self.stats)
    }

    /// Schema diagnostics for admin endpoints.
    #[must_use]
    pub fn get_available_schemas(&self) -> Vec<SchemaInfo> {
        self.inner.get_available_schemas()
    }

    /// Lookup schema ID for a source/event pair.
    #[must_use]
    pub fn get_schema_id(&self, source: &EventSource, event_type: &EventType) -> Option<Uuid> {
        self.inner.get_schema_id(source, event_type)
    }

    /// Lookup schema version for a source/event pair.
    #[must_use]
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
    /// Event is valid; carries the UUID of the schema that matched.
    Valid { schema_id: Uuid },
    /// Validation was skipped (disabled)
    Skipped,
    /// No schema specified for the event
    NoSchema,
    /// Schema not found
    SchemaNotFound { schema_id: Uuid },
    /// Event is invalid
    Invalid { errors: Vec<String> },
}

impl ValidationResult {
    /// Check if the event should be accepted
    #[must_use]
    pub fn should_accept(&self) -> bool {
        matches!(
            self,
            ValidationResult::Valid { .. }
                | ValidationResult::Skipped
                | ValidationResult::NoSchema
                | ValidationResult::SchemaNotFound { .. }
        )
    }

    /// Check if the validation failed
    #[must_use]
    pub fn is_failure(&self) -> bool {
        matches!(self, ValidationResult::Invalid { .. })
    }

    /// Get error message if validation failed
    #[must_use]
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_primitives::Id;
    use sinex_primitives::events::{DynamicPayload, SourceMaterial};
    use xtask::sandbox::sinex_test;

    fn event_without_registered_schema() -> sinex_db::models::Event<JsonValue> {
        DynamicPayload::new("validator-test", "validator.test", json!({ "ok": true }))
            .from_material(Id::<SourceMaterial>::new())
            .build()
            .expect("test event should build")
    }

    #[sinex_test]
    async fn validate_event_preserves_no_schema_result() -> xtask::sandbox::TestResult<()> {
        let validator = EventValidator::new(true);
        let result = validator.validate_event(&event_without_registered_schema());

        assert!(
            matches!(result, ValidationResult::NoSchema),
            "validate_event must not fabricate a nil schema UUID when no schema is registered: {result:?}"
        );
        Ok(())
    }
}
