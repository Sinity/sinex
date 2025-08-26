//! Event validation for the ingestion daemon

use crate::{IngestdResult, SinexError};
use ahash::AHashMap;
use sinex_core::db::SqlxPgPool as PgPool;
use sinex_core::types::ulid::Ulid;

use sinex_core::db::models::event::{Event, Provenance};
use sinex_core::types::domain::{EventSource, EventType};
use serde_json::Value as JsonValue;
use sqlx::FromRow;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Schema record from database
#[derive(Debug, FromRow, Clone)]
struct SchemaRecord {
    id: Ulid,
    source: String,
    event_type: String,
    schema_version: String,
    schema_content: serde_json::Value,
    content_hash: String,
}

/// Schema cache entry
#[derive(Clone)]
struct SchemaCacheEntry {
    compiled_schema: Arc<jsonschema::JSONSchema>,
    source: Arc<String>,
    event_type: Arc<String>,
    version: Arc<String>,
    content_hash: Arc<String>,
}

/// Newtype wrapper for schema cache to provide cleaner interface
#[derive(Clone, Debug, Default)]
pub struct SchemaCache {
    cache: Arc<parking_lot::RwLock<AHashMap<Arc<String>, SchemaCacheEntry>>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(parking_lot::RwLock::new(AHashMap::new())),
        }
    }

    pub fn get(&self, key: &Arc<String>) -> Option<SchemaCacheEntry> {
        let cache = self.cache.read();
        cache.get(key).cloned()
    }

    pub fn insert(&self, key: Arc<String>, value: SchemaCacheEntry) {
        let mut cache = self.cache.write();
        cache.insert(key, value);
    }

    pub fn len(&self) -> usize {
        let cache = self.cache.read();
        cache.len()
    }

    pub fn bulk_update(&self, new_cache: AHashMap<Arc<String>, SchemaCacheEntry>) {
        let mut cache = self.cache.write();
        *cache = new_cache;
    }

    pub fn clone_data(&self) -> AHashMap<Arc<String>, SchemaCacheEntry> {
        let cache = self.cache.read();
        cache.clone()
    }

    pub fn iter<F, R>(&self, f: F) -> Vec<R>
    where
        F: Fn((&Arc<String>, &SchemaCacheEntry)) -> R,
    {
        let cache = self.cache.read();
        cache.iter().map(f).collect()
    }
}

/// Newtype wrapper for schema lookup to provide cleaner interface
#[derive(Clone, Debug, Default)]
pub struct SchemaLookup {
    lookup: Arc<parking_lot::RwLock<AHashMap<(Arc<String>, Arc<String>), Arc<String>>>>,
}

impl SchemaLookup {
    pub fn new() -> Self {
        Self {
            lookup: Arc::new(parking_lot::RwLock::new(AHashMap::new())),
        }
    }

    pub fn get(&self, key: &(Arc<String>, Arc<String>)) -> Option<Arc<String>> {
        let lookup = self.lookup.read();
        lookup.get(key).cloned()
    }

    pub fn insert(&self, key: (Arc<String>, Arc<String>), value: Arc<String>) {
        let mut lookup = self.lookup.write();
        lookup.insert(key, value);
    }

    pub fn len(&self) -> usize {
        let lookup = self.lookup.read();
        lookup.len()
    }

    pub fn bulk_update(&self, new_lookup: AHashMap<(Arc<String>, Arc<String>), Arc<String>>) {
        let mut lookup = self.lookup.write();
        *lookup = new_lookup;
    }

    pub fn clone_data(&self) -> AHashMap<(Arc<String>, Arc<String>), Arc<String>> {
        let lookup = self.lookup.read();
        lookup.clone()
    }
}

/// Event validator that checks events against JSON schemas
#[derive(Clone)]
pub struct EventValidator {
    /// In-memory cache of compiled schemas keyed by schema ID
    schema_cache: SchemaCache,
    /// Map of (source, event_type) to schema ID for quick lookups
    schema_lookup: SchemaLookup,
    validation_enabled: bool,
}

impl EventValidator {
    /// Create a new event validator
    pub fn new(validation_enabled: bool) -> Self {
        Self {
            schema_cache: SchemaCache::new(),
            schema_lookup: SchemaLookup::new(),
            validation_enabled,
        }
    }

    /// Load schemas from database
    pub async fn load_schemas_from_db(
        pool: &PgPool,
        validation_enabled: bool,
    ) -> IngestdResult<Self> {
        let validator = Self::new(validation_enabled);

        if !validation_enabled {
            debug!("Schema validation disabled");
            return Ok(validator);
        }

        // Load all active schemas from the database
        // For each source/event_type, we'll use the latest version for new events
        let schemas = sqlx::query_as!(
            SchemaRecord,
            r#"
            SELECT DISTINCT ON (source, event_type)
                id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                schema_version as "schema_version!",
                schema_content as "schema_content!",
                content_hash as "content_hash!"
            FROM sinex_schemas.event_payload_schemas
            WHERE is_active = true
            ORDER BY source, event_type, schema_version DESC
            "#
        )
        .fetch_all(pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("Failed to load event schemas: {}", e))
                .with_operation("validator.load_schemas")
        })?;

        // Process the loaded schemas and build cache
        let mut cache = AHashMap::new();
        let mut lookup = AHashMap::new();
        let mut compiled_count = 0;
        let mut failed_count = 0;

        for schema in schemas {
            let schema_id = Arc::new(schema.id.to_string());
            let source = Arc::new(schema.source);
            let event_type = Arc::new(schema.event_type);
            let version = Arc::new(schema.schema_version);
            let content_hash = Arc::new(schema.content_hash);

            match jsonschema::JSONSchema::compile(&schema.schema_content) {
                Ok(compiled_schema) => {
                    let cache_entry = SchemaCacheEntry {
                        compiled_schema: Arc::new(compiled_schema),
                        source: source.clone(),
                        event_type: event_type.clone(),
                        version: version.clone(),
                        content_hash: content_hash.clone(),
                    };

                    // Note: This is still using the local HashMap variables
                    // These will be assigned to the validator later
                    cache.insert(schema_id.clone(), cache_entry);
                    lookup.insert((source.clone(), event_type.clone()), schema_id.clone());
                    compiled_count += 1;

                    debug!(
                        schema_id = %schema_id,
                        source = %source,
                        event_type = %event_type,
                        version = %version,
                        "Compiled and cached schema"
                    );
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        schema_id = %schema_id,
                        source = %source,
                        event_type = %event_type,
                        error = %e,
                        "Failed to compile schema, skipping"
                    );
                }
            }
        }

        validator.schema_cache.bulk_update(cache);
        validator.schema_lookup.bulk_update(lookup);

        info!(
            compiled = compiled_count,
            failed = failed_count,
            "Loaded and compiled schemas into cache"
        );

        Ok(validator)
    }

    /// Validate an event
    pub fn validate_event(&self, event: &Event<JsonValue>) -> IngestdResult<ValidationResult> {
        if !self.validation_enabled {
            return Ok(ValidationResult::Skipped);
        }

        // If no schema is specified, allow the event
        let schema_id = match &event.payload_schema_id {
            Some(id) => id,
            None => return Ok(ValidationResult::NoSchema),
        };

        // Find the schema in cache
        let schema_key = Arc::new(schema_id.to_string());
        let cache_entry = match self.schema_cache.get(&schema_key) {
            Some(entry) => entry,
            None => {
                warn!(
                    schema_id = %schema_id,
                    "Schema not found in cache"
                );
                return Ok(ValidationResult::SchemaNotFound {
                    schema_id: *schema_id,
                });
            }
        };

        // Clone the Arc to avoid holding the lock during validation
        let schema = cache_entry.compiled_schema.clone();

        // Validate the payload
        let validation_result = schema.as_ref().validate(&event.payload);

        match validation_result {
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
    pub fn validate_batch(
        &self,
        events: &[Event<JsonValue>],
    ) -> IngestdResult<Vec<ValidationResult>> {
        events
            .iter()
            .map(|event| self.validate_event(event))
            .collect()
    }

    /// Get available schemas
    pub fn get_available_schemas(&self) -> Vec<SchemaInfo> {
        self.schema_cache.iter(|(schema_id, entry)| SchemaInfo {
            name: format!("{}.{}", entry.source, entry.event_type),
            version: entry.version.clone(),
            schema_key: schema_id.clone(),
        })
    }

    /// Check if a schema is available by ID
    pub fn has_schema_by_id(&self, schema_id: &sinex_core::types::ulid::Ulid) -> bool {
        let schema_key = Arc::new(schema_id.to_string());
        self.schema_cache.get(&schema_key).is_some()
    }

    /// Get schema ID for a source and event type (latest version)
    pub fn get_schema_id(
        &self,
        source: &EventSource,
        event_type: &EventType,
    ) -> Option<Arc<String>> {
        // Create Arc strings to use as lookup keys
        let source_arc = Arc::new(source.as_str().to_string());
        let event_type_arc = Arc::new(event_type.as_str().to_string());
        self.schema_lookup.get(&(source_arc, event_type_arc))
    }

    /// Get schema version for a source and event type (latest version)
    pub fn get_schema_version(
        &self,
        source: &EventSource,
        event_type: &EventType,
    ) -> Option<Arc<String>> {
        let schema_id = self.get_schema_id(source, event_type)?;
        let cache_entry = self.schema_cache.get(&schema_id)?;
        Some(cache_entry.version.clone())
    }

    /// Load all schema versions from database (for validation of historical events)
    pub async fn load_all_schema_versions(&mut self, pool: &PgPool) -> IngestdResult<()> {
        let all_schemas = sqlx::query_as!(
            SchemaRecord,
            r#"
            SELECT 
                id as "id: Ulid",
                source as "source!",
                event_type as "event_type!",
                schema_version as "schema_version!",
                schema_content as "schema_content!",
                content_hash as "content_hash!"
            FROM sinex_schemas.event_payload_schemas
            WHERE is_active = true
            ORDER BY source, event_type, schema_version
            "#
        )
        .fetch_all(pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("Failed to load event schemas: {}", e))
                .with_operation("validator.load_schemas")
        })?;

        let mut cache = AHashMap::new();
        let mut compiled_count = 0;
        let mut failed_count = 0;

        for schema in all_schemas {
            let schema_id = Arc::new(schema.id.to_string());
            let source = Arc::new(schema.source);
            let event_type = Arc::new(schema.event_type);
            let version = Arc::new(schema.schema_version);
            let content_hash = Arc::new(schema.content_hash);

            match jsonschema::JSONSchema::compile(&schema.schema_content) {
                Ok(compiled_schema) => {
                    let cache_entry = SchemaCacheEntry {
                        compiled_schema: Arc::new(compiled_schema),
                        source: source.clone(),
                        event_type: event_type.clone(),
                        version: version.clone(),
                        content_hash: content_hash.clone(),
                    };

                    cache.insert(schema_id.clone(), cache_entry);
                    compiled_count += 1;

                    debug!(
                        schema_id = %schema_id,
                        source = %source,
                        event_type = %event_type,
                        version = %version,
                        "Compiled and cached schema version"
                    );
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        schema_id = %schema_id,
                        source = %source,
                        event_type = %event_type,
                        version = %version,
                        error = %e,
                        "Failed to compile schema version, skipping"
                    );
                }
            }
        }

        // Update the cache with all versions
        self.schema_cache.bulk_update(cache);

        info!(
            compiled = compiled_count,
            failed = failed_count,
            "Loaded all schema versions into cache"
        );

        Ok(())
    }

    /// Get schema count
    pub fn schema_count(&self) -> usize {
        self.schema_cache.len()
    }

    /// Reload schemas from database
    pub async fn reload_schemas(&mut self, pool: &PgPool) -> IngestdResult<usize> {
        let new_validator = Self::load_schemas_from_db(pool, self.validation_enabled).await?;
        let old_count = self.schema_count();

        // Swap the caches atomically
        self.schema_cache
            .bulk_update(new_validator.schema_cache.clone_data());
        self.schema_lookup
            .bulk_update(new_validator.schema_lookup.clone_data());

        let new_count = self.schema_count();
        info!(
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
    SchemaNotFound {
        schema_id: sinex_core::types::ulid::Ulid,
    },
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
    pub version: Arc<String>,
    pub schema_key: Arc<String>,
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
