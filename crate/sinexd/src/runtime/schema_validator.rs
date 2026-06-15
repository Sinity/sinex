//! RuntimeModule-side schema validation
//!
//! Enables runtime modules to validate events before publishing to NATS,
//! reducing bandwidth and providing early feedback on schema errors.
//!
//! Uses schema broadcasts from event_engine and fetches full schemas from
//! NATS KV to compile and cache validators locally.
//!
//! ## Edge Mode vs. Full Mode
//!
//! - **Edge Mode** (no database): Validates only against cached schemas.
//!   Returns `SchemaNotAvailable` error when schema is missing from cache.
//! - **Full Mode** (with database): Fetches schema metadata from DB on cache miss,
//!   compiles from KV, caches it, then validates.

use ahash::AHashMap;
use async_nats::jetstream::kv::Store;
use jsonschema::Validator;
use parking_lot::RwLock;
use sinex_primitives::JsonValue;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::runtime::RuntimeResult;
use crate::runtime::stream::SchemaBroadcastEntry;

/// Schema validator for runtime modules.
///
/// This validator:
/// 1. Receives schema metadata via NATS broadcasts from event_engine
/// 2. Fetches full schema JSON from NATS KV bucket
/// 3. Compiles schemas using jsonschema crate
/// 4. Caches compiled validators for fast validation
///
/// ## Validation Modes
///
/// **Edge Mode** (`db_pool` is None):
/// - Validates only against cached schemas from NATS broadcasts
/// - Returns `SinexError::validation` with "Schema not available" when schema missing
/// - No database dependency - can run on edge devices
///
/// **Full Mode** (`db_pool` is Some):
/// - Cache-first: validates against cached schemas
/// - Cache-miss: fetches active schema metadata from DB, compiles schema from KV, caches it
/// - Missing/invalid schema is an error (fail-closed)
#[derive(Clone)]
pub struct RuntimeSchemaValidator {
    /// Compiled schemas by `schema_id`
    schemas: Arc<RwLock<AHashMap<Uuid, Arc<Validator>>>>,
    /// Lookup: (source, `event_type`) → `schema_id`
    lookup: Arc<RwLock<AHashMap<(String, String), Uuid>>>,
    /// Optional database pool for cache-miss hydration (None in edge mode)
    db_pool: Option<PgPool>,
    /// NATS KV store for schema fetching
    kv_store: Option<Store>,
}

impl RuntimeSchemaValidator {
    /// Create a new edge-mode validator (cache-only)
    #[must_use]
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(AHashMap::new())),
            lookup: Arc::new(RwLock::new(AHashMap::new())),
            db_pool: None,
            kv_store: None,
        }
    }

    /// Create a full-mode validator with cache-miss hydration from DB + KV
    #[must_use]
    pub fn with_db_hydration(db_pool: PgPool, kv_store: Store) -> Self {
        Self {
            schemas: Arc::new(RwLock::new(AHashMap::new())),
            lookup: Arc::new(RwLock::new(AHashMap::new())),
            db_pool: Some(db_pool),
            kv_store: Some(kv_store),
        }
    }

    /// Check if validator is in edge mode (no DB-backed cache hydration)
    #[must_use]
    pub fn is_edge_mode(&self) -> bool {
        self.db_pool.is_none()
    }

    /// Update cache from broadcast + fetch full schemas from KV
    ///
    /// Called when schema broadcast is received. Fetches full schema JSON
    /// from NATS KV and compiles validators.
    pub async fn update_from_broadcast(
        &self,
        entries: Vec<SchemaBroadcastEntry>,
        kv: &Store,
    ) -> RuntimeResult<usize> {
        let mut new_schemas = AHashMap::new();
        let mut new_lookup = AHashMap::new();
        let mut compiled = 0;

        for entry in entries {
            let schema_id = entry.schema_id.parse::<Uuid>().map_err(|e| {
                crate::runtime::SinexError::validation(format!("Invalid schema ID: {e}"))
            })?;

            let Some(validator) = fetch_and_compile_from_kv(kv, &entry.schema_id).await else {
                continue;
            };

            // Parse source.event_type from name (format: "source.event.type")
            let parts: Vec<&str> = entry.name.split('.').collect();
            if parts.len() < 2 {
                warn!(
                    schema_name = %entry.name,
                    "Invalid schema name format (expected 'source.event.type')"
                );
                continue;
            }
            let source = parts[0].to_string();
            let event_type = parts[1..].join(".");

            new_schemas.insert(schema_id, validator);
            new_lookup.insert((source, event_type), schema_id);
            compiled += 1;
        }

        // Atomic update of caches: hold both write guards together so a
        // concurrent validate() can never observe the schemas map swapped
        // while the lookup map is still stale (or vice-versa). That window —
        // two separate `write()` calls with a removal in between — produced
        // spurious "schema cache is inconsistent" errors. Lock order
        // (schemas then lookup) matches fetch_schema_from_db().
        {
            let mut schemas = self.schemas.write();
            let mut lookup = self.lookup.write();
            *schemas = new_schemas;
            *lookup = new_lookup;
        }

        info!(
            compiled,
            total = self.schema_count(),
            "Updated runtime schema cache"
        );

        Ok(compiled)
    }

    /// Validate event payload against schema with cache-first strategy
    ///
    /// ## Validation Strategy
    ///
    /// 1. **Cache hit**: Schema in cache → validate immediately
    /// 2. **Cache miss**:
    ///    - **Edge mode** (no DB): Return `SchemaNotAvailable` error
    ///    - **Full mode** (with DB): Resolve/compile schema via DB+KV, cache it, then validate
    ///
    /// ## Returns
    ///
    /// - `Ok(schema_id)`: Payload is valid and matched this schema
    /// - `Err(Validation)`: Payload fails schema validation or schema not available in edge mode
    pub async fn validate(
        &self,
        source: &str,
        event_type: &str,
        payload: &JsonValue,
    ) -> RuntimeResult<Uuid> {
        // Cache-first: resolve the id AND its compiled validator under both
        // read guards held together, so a concurrent atomic cache swap can
        // never hand us an id whose schema row was already removed. Lock order
        // (schemas then lookup) matches the writers.
        let cached = {
            let schemas = self.schemas.read();
            let lookup = self.lookup.read();
            lookup
                .get(&(source.to_string(), event_type.to_string()))
                .copied()
                .map(|id| (id, schemas.get(&id).cloned()))
        };

        let (schema_id, validator) = match cached {
            Some((id, Some(validator))) => (id, validator),
            Some((id, None)) => {
                // lookup and schemas are now updated atomically, so a present
                // key with a missing schema is a genuine inconsistency.
                return Err(crate::runtime::SinexError::processing(format!(
                    "Schema cache is inconsistent for {source}.{event_type} (schema_id={id})"
                )));
            }
            None => {
                // Cache miss - hydrate from DB or error in edge mode.
                if self.is_edge_mode() {
                    // Edge mode: strict validation - must have schema in cache
                    return Err(crate::runtime::SinexError::validation(format!(
                        "Schema not available in cache for {source}.{event_type} (edge mode - cache-only)"
                    )));
                }
                let id = self.fetch_schema_from_db(source, event_type).await?;
                let validator = self.schemas.read().get(&id).cloned().ok_or_else(|| {
                    crate::runtime::SinexError::processing(format!(
                        "Schema cache is inconsistent for {source}.{event_type} (schema_id={id})"
                    ))
                })?;
                (id, validator)
            }
        };

        // Validate payload
        let error_messages: Vec<String> = validator
            .iter_errors(payload)
            .map(|e| e.to_string())
            .collect();
        if !error_messages.is_empty() {
            return Err(crate::runtime::SinexError::validation(format!(
                "Schema validation failed for {source}.{event_type}: {}",
                error_messages.join("; ")
            )));
        }

        Ok(schema_id)
    }

    /// Fetch schema from database and add to cache (full mode only)
    ///
    /// Returns the `schema_id` if found and successfully cached.
    async fn fetch_schema_from_db(&self, source: &str, event_type: &str) -> RuntimeResult<Uuid> {
        let db_pool = self.db_pool.as_ref().ok_or_else(|| {
            crate::runtime::SinexError::configuration(
                "DB hydration requested but no database pool configured".to_string(),
            )
        })?;

        let kv_store = self.kv_store.as_ref().ok_or_else(|| {
            crate::runtime::SinexError::configuration(
                "DB hydration requested but no KV store configured".to_string(),
            )
        })?;

        // Query the latest active schema for this source/event_type
        let result = sqlx::query!(
            r#"
            SELECT id::TEXT AS schema_id, schema_version AS version
            FROM sinex_schemas.event_payload_schemas
            WHERE source = $1 AND event_type = $2 AND is_active = true
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
            source,
            event_type
        )
        .fetch_optional(db_pool)
        .await
        .map_err(crate::runtime::SinexError::from)?;

        let row = result.ok_or_else(|| {
            crate::runtime::SinexError::validation(format!(
                "No active schema registered for {source}.{event_type}"
            ))
        })?;

        let schema_id_str = row.schema_id.as_ref().ok_or_else(|| {
            crate::runtime::SinexError::processing("schema_id is NULL in database".to_string())
        })?;

        let schema_id: Uuid = schema_id_str.parse().map_err(|e| {
            crate::runtime::SinexError::processing(format!("Invalid schema_id from DB: {e}"))
        })?;

        let validator = fetch_and_compile_from_kv(kv_store, schema_id_str)
            .await
            .ok_or_else(|| {
                crate::runtime::SinexError::processing(format!(
                    "Failed to compile schema {schema_id_str} from KV"
                ))
            })?;

        {
            let mut schemas = self.schemas.write();
            let mut lookup = self.lookup.write();
            schemas.insert(schema_id, validator);
            lookup.insert((source.to_string(), event_type.to_string()), schema_id);
        }

        info!(
            source = %source,
            event_type = %event_type,
            schema_id = %schema_id,
            "Fetched and cached schema from DB"
        );

        Ok(schema_id)
    }

    /// Get count of cached schemas
    #[must_use]
    pub fn schema_count(&self) -> usize {
        self.schemas.read().len()
    }

    /// Check if validator is empty (no schemas loaded)
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schemas.read().is_empty()
    }
}

/// Fetch a schema from NATS KV by ID, deserialize it, and compile a `JSONSchema` validator.
///
/// Returns `None` (with warnings) if any step fails — fetch, deserialize, or compile.
async fn fetch_and_compile_from_kv(kv: &Store, schema_id_str: &str) -> Option<Arc<Validator>> {
    let key = format!("schema-{schema_id_str}");
    let schema_json = match kv.get(&key).await {
        Ok(Some(kv_entry)) => match serde_json::from_slice::<JsonValue>(&kv_entry) {
            Ok(json) => json,
            Err(e) => {
                warn!(schema_id = %schema_id_str, error = %e, "Failed to deserialize schema from KV");
                return None;
            }
        },
        Ok(None) => {
            debug!(schema_id = %schema_id_str, "Schema not found in KV");
            return None;
        }
        Err(e) => {
            warn!(schema_id = %schema_id_str, error = %e, "Failed to fetch schema from KV");
            return None;
        }
    };

    match jsonschema::validator_for(&schema_json) {
        Ok(v) => Some(Arc::new(v)),
        Err(e) => {
            warn!(schema_id = %schema_id_str, error = %e, "Failed to compile schema");
            None
        }
    }
}

impl Default for RuntimeSchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl RuntimeSchemaValidator {
    pub(crate) fn register_test_schema(
        &self,
        schema_id: Uuid,
        source: &str,
        event_type: &str,
        schema_json: &JsonValue,
    ) -> RuntimeResult<()> {
        let validator = jsonschema::validator_for(schema_json).map_err(|error| {
            crate::runtime::SinexError::validation(format!(
                "Failed to compile test schema for {source}.{event_type}: {error}"
            ))
        })?;

        self.schemas.write().insert(schema_id, Arc::new(validator));
        self.lookup
            .write()
            .insert((source.to_string(), event_type.to_string()), schema_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::prelude::*;

    fn simple_schema() -> JsonValue {
        json!({
            "type": "object",
            "properties": { "n": { "type": "integer" } },
            "required": ["n"]
        })
    }

    #[sinex_test]
    async fn validate_succeeds_for_registered_schema() -> TestResult<()> {
        let validator = RuntimeSchemaValidator::new();
        let schema_id = Uuid::now_v7();
        validator.register_test_schema(schema_id, "test", "event", &simple_schema())?;

        let id = validator.validate("test", "event", &json!({ "n": 1 })).await?;
        assert_eq!(id, schema_id);

        let err = validator
            .validate("test", "event", &json!({ "n": "not-an-int" }))
            .await
            .expect_err("schema-violating payload must be rejected");
        assert!(err.to_string().contains("Schema validation failed"));
        Ok(())
    }

    // Regression for the schema-cache split-write race: the validator now holds
    // both read guards together, so a lookup key whose compiled schema is absent
    // is a genuine inconsistency (no longer a transient window). validate() must
    // surface a clean error rather than panic or mis-route.
    #[sinex_test]
    async fn validate_reports_inconsistency_for_orphan_lookup_entry() -> TestResult<()> {
        let validator = RuntimeSchemaValidator::new();
        let schema_id = Uuid::now_v7();
        validator.register_test_schema(schema_id, "test", "event", &simple_schema())?;

        // Drop the compiled schema while leaving the lookup entry in place.
        validator.schemas.write().remove(&schema_id);

        let err = validator
            .validate("test", "event", &json!({ "n": 1 }))
            .await
            .expect_err("orphan lookup entry must yield an inconsistency error");
        assert!(err.to_string().contains("inconsistent"));
        Ok(())
    }
}
