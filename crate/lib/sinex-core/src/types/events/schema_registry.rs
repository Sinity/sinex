//! Schema registry for EventPayload types
//!
//! This module provides runtime access to schema IDs for EventPayload types.
//! The actual schemas are managed by the sinex-schema-manager tool.

use crate::domain::{EventSource, EventType};
use crate::Ulid;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use sinex_schema::ulid_conversions::uuid_to_ulid;
// no Row import needed when using SQLx macros
use std::collections::HashMap;
use std::sync::Arc;

/// Information about a payload type collected by inventory
pub struct PayloadInfo {
    pub type_name: &'static str,
    pub source: &'static str,
    pub event_type: &'static str,
    pub version: &'static str,
    pub schema_fn: fn() -> serde_json::Value,
}

// Register PayloadInfo for inventory collection
inventory::collect!(PayloadInfo);

/// In-memory cache of schema name to schema ID mappings
static SCHEMA_CACHE: Lazy<RwLock<HashMap<String, Ulid>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// In-memory cache of schema ID to version mappings (using Arc to avoid cloning)
static VERSION_CACHE: Lazy<RwLock<HashMap<Ulid, Arc<String>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Look up the schema ID for a given source and event type
///
/// This will check the in-memory cache first, then query the database
/// if needed. The cache is populated lazily as schemas are looked up.
pub async fn lookup_schema_id(
    pool: &sqlx::PgPool,
    source: &EventSource,
    event_type: &EventType,
) -> Option<Ulid> {
    let schema_name = format!("{}.{}", source.as_str(), event_type.as_str());

    // Check cache first
    {
        let cache = SCHEMA_CACHE.read();
        if let Some(&id) = cache.get(&schema_name) {
            return Some(id);
        }
    }

    // Query database - fetch as UUID then convert to ULID
    let result = sqlx::query_scalar!(
        r#"
        SELECT id::uuid as "id!"
        FROM sinex_schemas.event_payload_schemas
        WHERE source = $1
          AND event_type = $2
          AND schema_version = 'v1'
          AND is_active = true
        LIMIT 1
        "#,
        source.as_str(),
        event_type.as_str()
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .map(uuid_to_ulid);

    // Update cache if found
    if let Some(id) = result {
        let mut cache = SCHEMA_CACHE.write();
        cache.insert(schema_name, id);
    }

    result
}

/// Get schema version from cache (synchronous)
///
/// This returns the cached version for a schema ID. The cache must be
/// populated beforehand using `preload_schemas` or `cache_schema_version`.
pub fn get_schema_version(schema_id: Ulid) -> Option<Arc<String>> {
    let cache = VERSION_CACHE.read();
    cache.get(&schema_id).cloned()
}

/// Get schema ID from cache (synchronous)
///
/// This returns the cached schema ID for a given source and event type.
/// The cache must be populated beforehand using `preload_schemas`.
pub fn get_schema_id(source: &EventSource, event_type: &EventType) -> Option<Ulid> {
    let schema_name = format!("{}.{}", source.as_str(), event_type.as_str());
    let cache = SCHEMA_CACHE.read();
    cache.get(&schema_name).cloned()
}

/// Cache a schema version (used during startup or schema registration)
pub fn cache_schema_version(schema_id: Ulid, version: String) {
    let mut cache = VERSION_CACHE.write();
    cache.insert(schema_id, Arc::new(version));
}

/// Look up the schema version for a given schema ID (async)
///
/// This will check the in-memory cache first, then query the database
/// if needed. The cache is populated lazily as versions are looked up.
///
/// Note: Prefer using `get_schema_version` for synchronous access after
/// ensuring the cache is populated.
pub async fn lookup_schema_version(pool: &sqlx::PgPool, schema_id: Ulid) -> Option<String> {
    // Check cache first
    if let Some(version) = get_schema_version(schema_id) {
        return Some((*version).clone());
    }

    // Query database for the schema version
    let result = sqlx::query_scalar!(
        r#"
        SELECT schema_version
        FROM sinex_schemas.event_payload_schemas
        WHERE id = $1
        "#,
        schema_id as _
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    // Cache the result if found
    if let Some(ref version) = result {
        cache_schema_version(schema_id, version.clone());
    }

    result
}

/// Clear the schema cache (useful for testing)
#[cfg(test)]
pub fn clear_cache() {
    let mut cache = SCHEMA_CACHE.write();
    cache.clear();

    let mut version_cache = VERSION_CACHE.write();
    version_cache.clear();
}

/// Initialize the schema cache for version-aware deserialization
///
/// This should be called once at application startup to enable efficient
/// version-aware payload deserialization. Without this, Event::payload<T>()
/// will fall back to direct deserialization without version migration.
///
/// # Example
/// ```ignore
/// // In your main function or initialization code:
/// sinex_core::types::events::initialize_schema_cache(&pool).await
///     .expect("Failed to initialize schema cache");
/// ```
pub async fn initialize_schema_cache(
    pool: &sqlx::PgPool,
) -> Result<usize, crate::error::SinexError> {
    preload_schemas(pool).await.map_err(|e| {
        crate::error::SinexError::database(format!("Failed to initialize schema cache: {e}"))
    })
}

/// Preload all active schemas into cache
///
/// This can be called at startup to avoid lazy loading during runtime.
/// It caches both schema IDs (by name) and versions (by ID).
pub async fn preload_schemas(pool: &sqlx::PgPool) -> Result<usize, sqlx::Error> {
    let schemas = sqlx::query!(
        r#"
        SELECT 
            id as "id: Ulid", 
            source,
            event_type,
            schema_version
        FROM sinex_schemas.event_payload_schemas
        WHERE is_active = true
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut cache = SCHEMA_CACHE.write();
    let mut version_cache = VERSION_CACHE.write();

    for schema in &schemas {
        let schema_name = format!("{}.{}", schema.source, schema.event_type);
        cache.insert(schema_name, schema.id);
        version_cache.insert(schema.id, Arc::new(schema.schema_version.clone()));
    }

    Ok(schemas.len())
}

/// Get all registered payload types via inventory
pub fn get_all_payloads() -> impl Iterator<Item = &'static PayloadInfo> {
    inventory::iter::<PayloadInfo>()
}

/// Generate schemas for all registered payload types
pub fn generate_all_schemas() -> HashMap<(String, String, String), serde_json::Value> {
    let mut schemas = HashMap::new();

    for payload in get_all_payloads() {
        let key = (
            payload.source.to_string(),
            payload.event_type.to_string(),
            payload.version.to_string(),
        );
        let schema = (payload.schema_fn)();
        schemas.insert(key, schema);
    }

    schemas
}
