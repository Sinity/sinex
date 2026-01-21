//! Event validation utilities shared by ingestd and the sinex-core test suites.
//!
//! The real-time ingestion path relies on the same validator that powers the
//! integration and adversarial tests, ensuring that schema enforcement,
//! provenance validation, and payload guards stay in lock-step.
use crate::db::models::event::{Event, Provenance, SourceMaterial};
#[cfg(feature = "sqlx")]
use crate::db::DbPool;
use crate::types::domain::{EventSource, EventType, HostName};
use crate::types::Id;
use crate::JsonValue;
use ahash::AHashMap;
use chrono::Utc;
use color_eyre::eyre::{Context, Result};
use jsonschema::JSONSchema;
use parking_lot::RwLock;
use serde_json;
use sinex_schema::ulid::Ulid;
#[cfg(feature = "sqlx")]
use sqlx::FromRow;
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, warn};

/// Maximum payload size (in bytes) accepted by the validator before flagging
/// the event as suspicious. This mirrors the guardrails enforced by the ingest
/// daemon to keep caps consistent across tests.
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 512 * 1024; // 512 KiB
const MAX_ULID_DRIFT_SECS: i64 = 5 * 60; // 5 minutes
/// Structured validation errors surfaced to tests.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("Missing required field '{field}'")]
    MissingField { field: String },
    #[error("Invalid type for field '{field}': expected {expected}, got {actual}")]
    InvalidType {
        field: String,
        expected: String,
        actual: String,
    },
    #[error("Invalid value for field '{field}': {reason}")]
    InvalidValue { field: String, reason: String },
    #[error("Security validation failed: {0}")]
    SecurityValidation(String),
    #[error("Payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: usize, max: usize },
    #[error("Schema violation: {message}")]
    SchemaViolation { message: String },
}
pub type ValidationResult = std::result::Result<(), ValidationError>;
/// Outcome of schema validation used by ingestd for streaming pipelines.
#[derive(Debug, Clone)]
pub enum SchemaValidationOutcome {
    Valid,
    NoSchema,
    SchemaNotFound { schema_id: Ulid },
    Invalid { errors: Vec<String> },
}
impl SchemaValidationOutcome {
    pub fn should_accept(&self) -> bool {
        matches!(
            self,
            SchemaValidationOutcome::Valid
                | SchemaValidationOutcome::NoSchema
                | SchemaValidationOutcome::SchemaNotFound { .. }
        )
    }
    pub fn is_failure(&self) -> bool {
        matches!(self, SchemaValidationOutcome::Invalid { .. })
    }
}
#[derive(Clone, Default)]
struct SchemaCache {
    cache: Arc<RwLock<AHashMap<Ulid, SchemaCacheEntry>>>,
}
impl SchemaCache {
    fn new() -> Self {
        Self::default()
    }
    fn get(&self, key: &Ulid) -> Option<SchemaCacheEntry> {
        self.cache.read().get(key).cloned()
    }
    fn bulk_update(&self, new_cache: AHashMap<Ulid, SchemaCacheEntry>) {
        *self.cache.write() = new_cache;
    }
    fn len(&self) -> usize {
        self.cache.read().len()
    }
    fn iter<R, F>(&self, f: F) -> Vec<R>
    where
        F: Fn((&Ulid, &SchemaCacheEntry)) -> R,
    {
        self.cache.read().iter().map(f).collect()
    }
}
type LookupMap = AHashMap<(Arc<String>, Arc<String>), Ulid>;
#[derive(Clone, Default)]
struct SchemaLookup {
    lookup: Arc<RwLock<LookupMap>>,
}
impl SchemaLookup {
    fn new() -> Self {
        Self::default()
    }
    fn get(&self, key: &(Arc<String>, Arc<String>)) -> Option<Ulid> {
        self.lookup.read().get(key).cloned()
    }
    fn bulk_update(&self, new_lookup: LookupMap) {
        *self.lookup.write() = new_lookup;
    }
}
#[derive(Debug, Clone)]
struct SchemaCacheEntry {
    schema_id: Ulid,
    compiled_schema: Arc<JSONSchema>,
    source: Arc<String>,
    event_type: Arc<String>,
    version: Arc<String>,
}
/// Information about a schema for diagnostics/CLI display
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub name: String,
    pub version: Arc<String>,
    pub schema_id: Ulid,
}
/// Lightweight event validator used everywhere in Sinex.
#[derive(Clone)]
pub struct EventValidator {
    schema_cache: SchemaCache,
    schema_lookup: SchemaLookup,
    validation_enabled: bool,
    max_payload_bytes: usize,
}
impl Default for EventValidator {
    fn default() -> Self {
        Self::new()
    }
}
impl EventValidator {
    /// Create a validator with schema enforcement enabled.
    pub fn new() -> Self {
        Self {
            schema_cache: SchemaCache::new(),
            schema_lookup: SchemaLookup::new(),
            validation_enabled: true,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        }
    }
    /// Create a validator with validation toggled on/off.
    pub fn with_validation_enabled(validation_enabled: bool) -> Self {
        Self {
            validation_enabled,
            ..Self::new()
        }
    }
    /// Change the payload limit for oversized payload detection.
    pub fn with_max_payload(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes;
        self
    }
    /// Load schemas from the database and build compiled cache.
    #[cfg(feature = "sqlx")]
    pub async fn load_from_db(pool: &DbPool) -> Result<Self> {
        Self::load_from_db_with_options(pool, true).await
    }
    #[cfg(feature = "sqlx")]
    pub async fn load_from_db_with_options(
        pool: &DbPool,
        validation_enabled: bool,
    ) -> Result<Self> {
        let mut validator = Self::with_validation_enabled(validation_enabled);
        validator.reload_schemas(pool).await?;
        Ok(validator)
    }
    /// Reload latest active schemas from the database, replacing the cache.
    #[cfg(feature = "sqlx")]
    pub async fn reload_schemas(&mut self, pool: &DbPool) -> Result<usize> {
        let schemas = fetch_latest_active_schemas(pool).await?;
        let (cache, lookup, compiled, failed) = compile_schemas(schemas);
        self.schema_cache.bulk_update(cache);
        self.schema_lookup.bulk_update(lookup);
        info!(compiled, failed, "Loaded schema cache into validator");
        Ok(compiled)
    }
    /// Load all schema versions (not just latest) into the cache.
    #[cfg(feature = "sqlx")]
    pub async fn load_all_schema_versions(&mut self, pool: &DbPool) -> Result<()> {
        let schemas = fetch_all_active_schemas(pool).await?;
        let (cache, lookup, compiled, failed) = compile_schemas(schemas);
        self.schema_cache.bulk_update(cache);
        self.schema_lookup.bulk_update(lookup);
        info!(compiled, failed, "Loaded all schema versions into cache");
        Ok(())
    }
    /// Count cached schemas.
    pub fn schema_count(&self) -> usize {
        self.schema_cache.len()
    }
    /// Return basic info for diagnostics.
    pub fn get_available_schemas(&self) -> Vec<SchemaInfo> {
        self.schema_cache.iter(|(_, entry)| SchemaInfo {
            name: format!("{}.{}", entry.source, entry.event_type),
            version: entry.version.clone(),
            schema_id: entry.schema_id,
        })
    }
    /// Lookup schema ID for a source/event_type pair.
    pub fn get_schema_id(&self, source: &EventSource, event_type: &EventType) -> Option<Ulid> {
        let source_key = Arc::new(source.as_str().to_string());
        let event_key = Arc::new(event_type.as_str().to_string());
        self.schema_lookup.get(&(source_key, event_key))
    }
    /// Lookup schema version for a source/event_type pair.
    pub fn get_schema_version(
        &self,
        source: &EventSource,
        event_type: &EventType,
    ) -> Option<Arc<String>> {
        let schema_id = self.get_schema_id(source, event_type)?;
        self.schema_cache.get(&schema_id).map(|entry| entry.version)
    }
    /// Validate a fully constructed event.
    pub fn validate(&self, event: &Event<JsonValue>) -> ValidationResult {
        self.validate_envelope(event.source.as_ref(), event.event_type.as_ref())?;
        self.check_payload_size(&event.payload)?;
        self.ensure_object_payload(&event.payload)?;
        self.validate_domain_specific_rules(event)?;
        self.validate_ulid_timestamp(event)?;
        self.validate_provenance(&event.provenance)?;
        if !self.validation_enabled {
            return Ok(());
        }
        match self.validate_payload_for(
            event.source.as_ref(),
            event.event_type.as_ref(),
            &event.payload,
        ) {
            SchemaValidationOutcome::Invalid { errors } => Err(ValidationError::SchemaViolation {
                message: errors.join(", "),
            }),
            SchemaValidationOutcome::SchemaNotFound { schema_id } => {
                warn!(schema = %schema_id, "Schema referenced but missing from cache");
                Ok(())
            }
            _ => Ok(()),
        }
    }
    /// Validate a payload by specifying source and event type directly.
    pub fn validate_with_rules(
        &self,
        source: &str,
        event_type: &str,
        payload: &JsonValue,
    ) -> ValidationResult {
        let event = Event {
            id: None,
            source: EventSource::from(source.to_string()),
            event_type: EventType::from(event_type.to_string()),
            payload: payload.clone(),
            ts_orig: Some(Utc::now()),
            host: HostName::from_static("validator"),
            ingestor_version: None,
            payload_schema_id: None,
            provenance: Provenance::from_material(Id::<SourceMaterial>::new(), 0, None, None),
            associated_blob_ids: None,
        };
        self.validate(&event)
    }
    /// Validate a payload using the latest schema mapping for a source/event pair.
    pub fn validate_payload_for(
        &self,
        source: &str,
        event_type: &str,
        payload: &JsonValue,
    ) -> SchemaValidationOutcome {
        if !self.validation_enabled {
            return SchemaValidationOutcome::Valid;
        }
        let source_key = Arc::new(source.to_string());
        let event_key = Arc::new(event_type.to_string());
        let schema_id = match self
            .schema_lookup
            .get(&(source_key.clone(), event_key.clone()))
        {
            Some(id) => id,
            None => return SchemaValidationOutcome::NoSchema,
        };
        let cache_entry = match self.schema_cache.get(&schema_id) {
            Some(entry) => entry,
            None => return SchemaValidationOutcome::SchemaNotFound { schema_id },
        };
        let schema = cache_entry.compiled_schema.clone();
        let validation_result = schema.validate(payload);
        match validation_result {
            Ok(_) => SchemaValidationOutcome::Valid,
            Err(errors) => {
                let messages: Vec<String> = errors.map(|err| err.to_string()).collect();
                SchemaValidationOutcome::Invalid { errors: messages }
            }
        }
    }
    fn validate_ulid_timestamp(&self, event: &Event<JsonValue>) -> ValidationResult {
        if let (Some(id), Some(ts_orig)) = (&event.id, event.ts_orig) {
            let ulid_ts = id.timestamp();
            let drift = (ulid_ts.timestamp() - ts_orig.timestamp()).abs();
            if drift > MAX_ULID_DRIFT_SECS {
                return Err(ValidationError::SecurityValidation(format!(
                    "ULID timestamp drift {drift}s exceeds allowed threshold of {MAX_ULID_DRIFT_SECS}s"
                )));
            }
        }
        Ok(())
    }
    fn validate_provenance(&self, provenance: &Provenance) -> ValidationResult {
        match provenance {
            Provenance::Material { .. } => Ok(()),
            Provenance::Synthesis {
                source_event_ids, ..
            } => {
                let mut seen = HashSet::new();
                for event_id in source_event_ids.iter() {
                    if !seen.insert(*event_id.as_ulid()) {
                        return Err(ValidationError::InvalidValue {
                            field: "provenance.source_event_ids".to_string(),
                            reason: "duplicate parent ID detected".to_string(),
                        });
                    }
                }
                Ok(())
            }
        }
    }
    fn validate_envelope(&self, source: &str, event_type: &str) -> ValidationResult {
        if source.trim().is_empty() {
            return Err(ValidationError::InvalidValue {
                field: "source".to_string(),
                reason: "source cannot be empty".to_string(),
            });
        }
        if source.contains('\0') {
            return Err(ValidationError::InvalidValue {
                field: "source".to_string(),
                reason: "source cannot contain null bytes".to_string(),
            });
        }
        if event_type.trim().is_empty() {
            return Err(ValidationError::InvalidValue {
                field: "event_type".to_string(),
                reason: "event type cannot be empty".to_string(),
            });
        }
        if event_type.contains('\0') {
            return Err(ValidationError::InvalidValue {
                field: "event_type".to_string(),
                reason: "event type cannot contain null bytes".to_string(),
            });
        }
        Ok(())
    }
    fn validate_domain_specific_rules(&self, event: &Event<JsonValue>) -> ValidationResult {
        match (event.source.as_ref(), event.event_type.as_ref()) {
            ("fs-watcher", et)
                if matches!(et, "file.created" | "file.modified" | "file.deleted") =>
            {
                Self::validate_filesystem_payload(et, &event.payload)
            }
            (source, "command.executed") if source == "terminal" || source == "terminal.kitty" => {
                Self::validate_terminal_payload(&event.payload)
            }
            _ => Ok(()),
        }
    }
    fn validate_filesystem_payload(event_type: &str, payload: &JsonValue) -> ValidationResult {
        let Some(obj) = payload.as_object() else {
            return Err(ValidationError::InvalidType {
                field: "payload".to_string(),
                expected: "object".to_string(),
                actual: json_type_name(payload).to_string(),
            });
        };
        Self::require_string_field(obj, "path")?;
        if event_type != "file.deleted" {
            Self::require_number_field(obj, "size")?;
        }
        if let Some(perms) = obj.get("permissions") {
            if !perms.is_number() {
                return Err(ValidationError::InvalidType {
                    field: "permissions".to_string(),
                    expected: "number".to_string(),
                    actual: json_type_name(perms).to_string(),
                });
            }
        }
        Ok(())
    }
    fn validate_terminal_payload(payload: &JsonValue) -> ValidationResult {
        let Some(obj) = payload.as_object() else {
            return Err(ValidationError::InvalidType {
                field: "payload".to_string(),
                expected: "object".to_string(),
                actual: json_type_name(payload).to_string(),
            });
        };
        Self::require_string_field(obj, "command")?;
        Self::require_number_field(obj, "exit_code")?;
        if let Some(ts) = obj.get("timestamp") {
            if !ts.is_string() {
                return Err(ValidationError::InvalidType {
                    field: "timestamp".to_string(),
                    expected: "string".to_string(),
                    actual: json_type_name(ts).to_string(),
                });
            }
        }
        Ok(())
    }
    fn require_string_field(
        obj: &serde_json::Map<String, JsonValue>,
        field: &str,
    ) -> ValidationResult {
        match obj.get(field) {
            Some(JsonValue::String(value)) if !value.trim().is_empty() => Ok(()),
            Some(other) => Err(ValidationError::InvalidType {
                field: field.to_string(),
                expected: "string".to_string(),
                actual: json_type_name(other).to_string(),
            }),
            None => Err(ValidationError::MissingField {
                field: field.to_string(),
            }),
        }
    }
    fn require_number_field(
        obj: &serde_json::Map<String, JsonValue>,
        field: &str,
    ) -> ValidationResult {
        match obj.get(field) {
            Some(value) if value.is_number() => Ok(()),
            Some(other) => Err(ValidationError::InvalidType {
                field: field.to_string(),
                expected: "number".to_string(),
                actual: json_type_name(other).to_string(),
            }),
            None => Err(ValidationError::MissingField {
                field: field.to_string(),
            }),
        }
    }
    fn check_payload_size(&self, payload: &JsonValue) -> ValidationResult {
        let payload_bytes = serde_json::to_vec(payload)
            .map(|v| v.len())
            .unwrap_or_default();
        if payload_bytes > self.max_payload_bytes {
            return Err(ValidationError::PayloadTooLarge {
                size: payload_bytes,
                max: self.max_payload_bytes,
            });
        }
        Ok(())
    }
    fn ensure_object_payload(&self, payload: &JsonValue) -> ValidationResult {
        if !payload.is_object() {
            return Err(ValidationError::InvalidType {
                field: "payload".to_string(),
                expected: "object".to_string(),
                actual: json_type_name(payload).to_string(),
            });
        }
        Ok(())
    }
}
#[cfg_attr(feature = "sqlx", derive(FromRow))]
#[derive(Debug)]
struct SchemaRecord {
    id: Ulid,
    source: String,
    event_type: String,
    schema_version: String,
    schema_content: JsonValue,
}
#[cfg(feature = "sqlx")]
async fn fetch_latest_active_schemas(pool: &DbPool) -> Result<Vec<SchemaRecord>> {
    sqlx::query_as!(
        SchemaRecord,
        r#"
        SELECT DISTINCT ON (source, event_type)
            id::uuid as "id!: Ulid",
            source,
            event_type,
            schema_version,
            schema_content as "schema_content!"
        FROM sinex_schemas.event_payload_schemas
        WHERE is_active = true
        ORDER BY source, event_type, updated_at DESC, schema_version DESC
        "#
    )
    .fetch_all(pool)
    .await
    .wrap_err("failed to load active schemas for EventValidator")
}
#[cfg(feature = "sqlx")]
async fn fetch_all_active_schemas(pool: &DbPool) -> Result<Vec<SchemaRecord>> {
    sqlx::query_as!(
        SchemaRecord,
        r#"
        SELECT 
            id::uuid as "id!: Ulid",
            source,
            event_type,
            schema_version,
            schema_content as "schema_content!"
        FROM sinex_schemas.event_payload_schemas
        WHERE is_active = true
        ORDER BY source, event_type, schema_version
        "#
    )
    .fetch_all(pool)
    .await
    .wrap_err("failed to load schema versions")
}
fn compile_schemas(
    schemas: Vec<SchemaRecord>,
) -> (AHashMap<Ulid, SchemaCacheEntry>, LookupMap, usize, usize) {
    let mut cache = AHashMap::new();
    let mut lookup = AHashMap::new();
    let mut compiled = 0;
    let mut failed = 0;
    for schema in schemas {
        match JSONSchema::compile(&schema.schema_content) {
            Ok(compiled_schema) => {
                let source = Arc::new(schema.source);
                let event_type = Arc::new(schema.event_type);
                let version = Arc::new(schema.schema_version);
                let entry = SchemaCacheEntry {
                    schema_id: schema.id,
                    compiled_schema: Arc::new(compiled_schema),
                    source: source.clone(),
                    event_type: event_type.clone(),
                    version: version.clone(),
                };
                cache.insert(schema.id, entry);
                lookup.insert((source, event_type), schema.id);
                compiled += 1;
            }
            Err(err) => {
                failed += 1;
                warn!(schema_id = %schema.id, error = %err, "Failed to compile schema");
            }
        }
    }
    (cache, lookup, compiled, failed)
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}
