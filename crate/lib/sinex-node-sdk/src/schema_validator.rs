//! Node-side schema validation
//!
//! Enables nodes to validate events before publishing to NATS,
//! reducing bandwidth and providing early feedback on schema errors.
//!
//! Uses schema broadcasts from ingestd and fetches full schemas from
//! NATS KV to compile and cache validators locally.
//!
//! ## Edge Mode vs. Full Mode
//!
//! - **Edge Mode** (no database): Validates only against cached schemas.
//!   Returns SchemaNotAvailable error when schema is missing from cache.
//! - **Full Mode** (with database): Falls back to DB when schema not in cache,
//!   fetches it, caches it, then validates.

use ahash::AHashMap;
use async_nats::jetstream::kv::Store;
use jsonschema::JSONSchema;
use parking_lot::RwLock;
use sinex_primitives::JsonValue;
use sinex_primitives::Ulid;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::stream_processor::SchemaBroadcastEntry;
use crate::NodeResult;

/// Compiled schema cache entry
///
/// Metadata fields (schema_id, source, event_type, version) are stored for
/// debugging, logging, and potential future introspection of cached schemas.
#[derive(Clone)]
#[allow(dead_code)]
struct CompiledSchema {
    schema_id: Ulid,
    source: String,
    event_type: String,
    version: String,
    validator: Arc<JSONSchema>,
}

/// Schema validator for nodes
///
/// This validator:
/// 1. Receives schema metadata via NATS broadcasts from ingestd
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
/// - DB fallback: if schema not in cache, fetches from DB, caches it, then validates
/// - Provides full schema validation even if broadcasts are missed
#[derive(Clone)]
pub struct NodeSchemaValidator {
    /// Compiled schemas by schema_id
    schemas: Arc<RwLock<AHashMap<Ulid, CompiledSchema>>>,
    /// Lookup: (source, event_type) → schema_id
    lookup: Arc<RwLock<AHashMap<(String, String), Ulid>>>,
    /// Optional database pool for schema fallback (None in edge mode)
    db_pool: Option<PgPool>,
    /// NATS KV store for schema fetching
    kv_store: Option<Store>,
}

impl NodeSchemaValidator {
    /// Create a new edge-mode validator (cache-only, no DB fallback)
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(AHashMap::new())),
            lookup: Arc::new(RwLock::new(AHashMap::new())),
            db_pool: None,
            kv_store: None,
        }
    }

    /// Create a full-mode validator with database fallback
    pub fn with_db_fallback(db_pool: PgPool, kv_store: Store) -> Self {
        Self {
            schemas: Arc::new(RwLock::new(AHashMap::new())),
            lookup: Arc::new(RwLock::new(AHashMap::new())),
            db_pool: Some(db_pool),
            kv_store: Some(kv_store),
        }
    }

    /// Check if validator is in edge mode (no DB fallback)
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
    ) -> NodeResult<usize> {
        let mut new_schemas = AHashMap::new();
        let mut new_lookup = AHashMap::new();
        let mut compiled = 0;

        for entry in entries {
            // Parse schema ID
            let schema_id = entry
                .schema_id
                .parse::<Ulid>()
                .map_err(|e| crate::SinexError::validation(format!("Invalid schema ID: {e}")))?;

            // Fetch full schema JSON from NATS KV
            let key = format!("schema:{}", entry.schema_id);
            let schema_json = match kv.get(&key).await {
                Ok(Some(kv_entry)) => match serde_json::from_slice::<JsonValue>(&kv_entry) {
                    Ok(json) => json,
                    Err(e) => {
                        warn!(
                            schema_id = %entry.schema_id,
                            error = %e,
                            "Failed to deserialize schema from KV"
                        );
                        continue;
                    }
                },
                Ok(None) => {
                    debug!(
                        schema_id = %entry.schema_id,
                        "Schema not found in KV (may not be stored yet)"
                    );
                    continue;
                }
                Err(e) => {
                    warn!(
                        schema_id = %entry.schema_id,
                        error = %e,
                        "Failed to fetch schema from KV"
                    );
                    continue;
                }
            };

            // Compile JSON schema validator
            let validator = match JSONSchema::compile(&schema_json) {
                Ok(v) => Arc::new(v),
                Err(e) => {
                    warn!(
                        schema_id = %entry.schema_id,
                        error = %e,
                        "Failed to compile schema"
                    );
                    continue;
                }
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

            let compiled_schema = CompiledSchema {
                schema_id,
                source: source.clone(),
                event_type: event_type.clone(),
                version: entry.version,
                validator,
            };

            new_schemas.insert(schema_id, compiled_schema);
            new_lookup.insert((source, event_type), schema_id);
            compiled += 1;
        }

        // Atomic update of caches
        *self.schemas.write() = new_schemas;
        *self.lookup.write() = new_lookup;

        info!(
            compiled,
            total = self.schema_count(),
            "Updated node schema cache"
        );

        Ok(compiled)
    }

    /// Validate event payload against schema with cache-first, DB fallback strategy
    ///
    /// ## Validation Strategy
    ///
    /// 1. **Cache hit**: Schema in cache → validate immediately
    /// 2. **Cache miss**:
    ///    - **Edge mode** (no DB): Return SchemaNotAvailable error
    ///    - **Full mode** (with DB): Fetch from DB, cache it, then validate
    ///
    /// ## Returns
    ///
    /// - `Ok(())`: Payload is valid or no schema enforced for this event type
    /// - `Err(Validation)`: Payload fails schema validation or schema not available in edge mode
    pub async fn validate(
        &self,
        source: &str,
        event_type: &str,
        payload: &JsonValue,
    ) -> NodeResult<()> {
        // Try cache first
        let schema_id_opt = {
            let lookup = self.lookup.read();
            lookup
                .get(&(source.to_string(), event_type.to_string()))
                .copied()
        };

        let schema_id = match schema_id_opt {
            Some(id) => id,
            None => {
                // Cache miss - try DB fallback or error in edge mode
                if self.is_edge_mode() {
                    // Edge mode: strict validation - must have schema in cache
                    return Err(crate::SinexError::validation(format!(
                        "Schema not available in cache for {}.{} (edge mode - no DB fallback)",
                        source, event_type
                    )));
                } else {
                    // Full mode: try to fetch from DB
                    match self.fetch_schema_from_db(source, event_type).await {
                        Ok(Some(id)) => id,
                        Ok(None) => {
                            // No schema registered in DB - allow (permissive for unregistered types)
                            debug!(
                                source = %source,
                                event_type = %event_type,
                                "No schema registered for event type, allowing"
                            );
                            return Ok(());
                        }
                        Err(e) => {
                            warn!(
                                source = %source,
                                event_type = %event_type,
                                error = %e,
                                "Failed to fetch schema from DB, allowing event"
                            );
                            return Ok(());
                        }
                    }
                }
            }
        };

        // Get compiled validator
        let validator = {
            let schemas = self.schemas.read();
            match schemas.get(&schema_id) {
                Some(s) => s.validator.clone(),
                None => {
                    // Shouldn't happen (lookup and schema cache should be in sync)
                    warn!(
                        source = %source,
                        event_type = %event_type,
                        schema_id = %schema_id,
                        "Schema found in lookup but not in cache"
                    );
                    return Ok(());
                }
            }
        };

        // Validate payload
        if let Err(errors) = validator.validate(payload) {
            let error_messages: Vec<String> = errors.map(|e| e.to_string()).collect();

            return Err(crate::SinexError::validation(format!(
                "Schema validation failed for {}.{}: {}",
                source,
                event_type,
                error_messages.join("; ")
            )));
        }

        Ok(())
    }

    /// Fetch schema from database and add to cache (full mode only)
    ///
    /// Returns the schema_id if found and successfully cached.
    async fn fetch_schema_from_db(
        &self,
        source: &str,
        event_type: &str,
    ) -> NodeResult<Option<Ulid>> {
        let db_pool = self.db_pool.as_ref().ok_or_else(|| {
            crate::SinexError::configuration(
                "DB fallback requested but no database pool configured".to_string(),
            )
        })?;

        let kv_store = self.kv_store.as_ref().ok_or_else(|| {
            crate::SinexError::configuration(
                "DB fallback requested but no KV store configured".to_string(),
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
        .map_err(|e| crate::SinexError::from(e))?;

        let Some(row) = result else {
            debug!(
                source = %source,
                event_type = %event_type,
                "No schema found in database"
            );
            return Ok(None);
        };

        let schema_id_str = row.schema_id.as_ref().ok_or_else(|| {
            crate::SinexError::processing("schema_id is NULL in database".to_string())
        })?;

        let schema_id: Ulid = schema_id_str.parse().map_err(|e| {
            crate::SinexError::processing(format!("Invalid schema_id from DB: {}", e))
        })?;

        // Fetch full schema JSON from NATS KV
        let key = format!("schema:{}", schema_id_str);
        let schema_json = match kv_store.get(&key).await {
            Ok(Some(kv_entry)) => match serde_json::from_slice::<JsonValue>(&kv_entry) {
                Ok(json) => json,
                Err(e) => {
                    warn!(
                        schema_id = %schema_id_str,
                        error = %e,
                        "Failed to deserialize schema from KV during DB fallback"
                    );
                    return Ok(None);
                }
            },
            Ok(None) => {
                warn!(
                    schema_id = %schema_id_str,
                    "Schema not found in KV during DB fallback"
                );
                return Ok(None);
            }
            Err(e) => {
                warn!(
                    schema_id = %schema_id_str,
                    error = %e,
                    "Failed to fetch schema from KV during DB fallback"
                );
                return Ok(None);
            }
        };

        // Compile JSON schema validator
        let validator = match JSONSchema::compile(&schema_json) {
            Ok(v) => Arc::new(v),
            Err(e) => {
                warn!(
                    schema_id = %schema_id_str,
                    error = %e,
                    "Failed to compile schema during DB fallback"
                );
                return Ok(None);
            }
        };

        // Add to cache
        let compiled_schema = CompiledSchema {
            schema_id,
            source: source.to_string(),
            event_type: event_type.to_string(),
            version: row.version,
            validator,
        };

        {
            let mut schemas = self.schemas.write();
            let mut lookup = self.lookup.write();
            schemas.insert(schema_id, compiled_schema);
            lookup.insert((source.to_string(), event_type.to_string()), schema_id);
        }

        info!(
            source = %source,
            event_type = %event_type,
            schema_id = %schema_id,
            "Fetched and cached schema from DB"
        );

        Ok(Some(schema_id))
    }

    /// Get count of cached schemas
    pub fn schema_count(&self) -> usize {
        self.schemas.read().len()
    }

    /// Check if validator is empty (no schemas loaded)
    pub fn is_empty(&self) -> bool {
        self.schemas.read().is_empty()
    }
}

impl Default for NodeSchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use tokio;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_edge_mode_validator_strict() -> Result<(), Box<dyn std::error::Error>> {
        let validator = NodeSchemaValidator::new();

        // Edge mode validator should be strict
        assert!(validator.is_edge_mode());
        assert!(validator.is_empty());

        let payload = json!({"foo": "bar"});
        // No schema in cache + edge mode = error
        let result = validator
            .validate("test-source", "test.event", &payload)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not available"));
        Ok(())
    }

    #[test]
    fn test_schema_cache_operations() {
        let validator = NodeSchemaValidator::new();

        assert_eq!(validator.schema_count(), 0);
        assert!(validator.is_empty());
        assert!(validator.is_edge_mode());
    }
}
