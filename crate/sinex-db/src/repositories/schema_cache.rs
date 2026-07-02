//! Schema Cache Repository
//!
//! Centralized payload-schema lookup and caching for event validation.
//! This consolidates schema access patterns from:
//! - `types/events/schema_registry.rs` (lazy lookup by namespace pair)
//! - `db/validation.rs` (bulk loading for `EventValidator`)
//! - `sinexd/service.rs` (schema content for NATS KV)
//!
//! The lookup key remains `(source, event_type)` for existing schema rows and
//! compatibility with payload inventory. EventContract/AdmissionPolicy catalogs
//! are the semantic authority above this physical schema cache.

use crate::repositories::common::db_error;
use crate::repositories::events::EventPayloadSchema;
use crate::{DbResult, JsonValue};
use serde::{Deserialize, Serialize};
use sinex_primitives::Id;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{EventSource, EventType};
use sqlx::PgPool;
use uuid::Uuid;

/// Minimal schema record for cache operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedSchema {
    pub id: Id<EventPayloadSchema>,
    pub source: EventSource,
    pub event_type: EventType,
    pub schema_version: String,
    pub schema_content: JsonValue,
    pub updated_at: Timestamp,
}

/// Repository for schema cache operations
pub struct SchemaCacheRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> SchemaCacheRepository<'a> {
    #[must_use]
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Look up the physical payload schema ID for a namespace pair.
    ///
    /// Returns the most recently updated active schema.
    /// Callers should cache the result to avoid repeated DB queries.
    pub async fn lookup_schema_id(
        &self,
        source: &EventSource,
        event_type: &EventType,
    ) -> DbResult<Option<Id<EventPayloadSchema>>> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT id::uuid as "id!"
            FROM sinex_schemas.event_payload_schemas
            WHERE source = $1
              AND event_type = $2
              AND is_active = true
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
            source.as_str(),
            event_type.as_str()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "lookup schema id"))?;

        Ok(result.map(Id::from_uuid))
    }

    /// Look up the schema version for a given schema ID
    pub async fn lookup_schema_version(
        &self,
        schema_id: Id<EventPayloadSchema>,
    ) -> DbResult<Option<String>> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT schema_version
            FROM sinex_schemas.event_payload_schemas
            WHERE id = $1::uuid
            "#,
            schema_id.as_uuid()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "lookup schema version"))?;

        Ok(result)
    }

    /// Fetch the full schema content for a given schema ID
    ///
    /// Used by sinexd to store schemas in NATS KV.
    pub async fn get_schema_content(
        &self,
        schema_id: Id<EventPayloadSchema>,
    ) -> DbResult<Option<JsonValue>> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT schema_content
            FROM sinex_schemas.event_payload_schemas
            WHERE id = $1 AND is_active = true
            "#,
            schema_id.as_uuid()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema content"))?;

        Ok(result)
    }

    /// Load latest active schemas (one per `source/event_type` pair)
    ///
    /// Used by `EventValidator` to populate its cache on startup.
    /// Returns the most recently updated schema for each `source/event_type`.
    pub async fn fetch_latest_active_schemas(&self) -> DbResult<Vec<CachedSchema>> {
        let rows = sqlx::query!(
            r#"
            SELECT DISTINCT ON (source, event_type)
                id as "id!: Uuid",
                source,
                event_type,
                schema_version,
                schema_content as "schema_content!",
                updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
            FROM sinex_schemas.event_payload_schemas
            WHERE is_active = true
            ORDER BY source, event_type, updated_at DESC, schema_version DESC
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "fetch latest active schemas"))?;

        Ok(rows
            .into_iter()
            .map(|row| CachedSchema {
                id: Id::from_uuid(row.id),
                source: row.source.into(),
                event_type: row.event_type.into(),
                schema_version: row.schema_version,
                schema_content: row.schema_content,
                updated_at: row.updated_at,
            })
            .collect())
    }

    /// Load all active schemas (including multiple versions per `source/event_type`)
    ///
    /// Used by `EventValidator` for version-aware deserialization.
    pub async fn fetch_all_active_schemas(&self) -> DbResult<Vec<CachedSchema>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id as "id!: Uuid",
                source,
                event_type,
                schema_version,
                schema_content as "schema_content!",
                updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
            FROM sinex_schemas.event_payload_schemas
            WHERE is_active = true
            ORDER BY source, event_type, schema_version
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "fetch all active schemas"))?;

        Ok(rows
            .into_iter()
            .map(|row| CachedSchema {
                id: Id::from_uuid(row.id),
                source: row.source.into(),
                event_type: row.event_type.into(),
                schema_version: row.schema_version,
                schema_content: row.schema_content,
                updated_at: row.updated_at,
            })
            .collect())
    }

    /// Bulk fetch schema content for multiple schema IDs
    ///
    /// Used by sinexd to efficiently load schemas for NATS KV storage.
    pub async fn get_schemas_by_ids(
        &self,
        schema_ids: &[Id<EventPayloadSchema>],
    ) -> DbResult<Vec<CachedSchema>> {
        if schema_ids.is_empty() {
            return Ok(Vec::new());
        }

        let uuids: Vec<_> = schema_ids
            .iter()
            .map(|schema_id| *schema_id.as_uuid())
            .collect();

        let rows = sqlx::query!(
            r#"
            SELECT
                id as "id!: Uuid",
                source,
                event_type,
                schema_version,
                schema_content as "schema_content!",
                updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
            FROM sinex_schemas.event_payload_schemas
            WHERE id::uuid = ANY($1) AND is_active = true
            "#,
            &uuids[..]
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get schemas by ids"))?;

        Ok(rows
            .into_iter()
            .map(|row| CachedSchema {
                id: Id::from_uuid(row.id),
                source: row.source.into(),
                event_type: row.event_type.into(),
                schema_version: row.schema_version,
                schema_content: row.schema_content,
                updated_at: row.updated_at,
            })
            .collect())
    }

    /// Preload all active schemas for in-memory caching
    ///
    /// Returns tuples of (id, source, `event_type`, `schema_version`) for efficient cache population.
    /// This is optimized for the use case where only metadata is needed (no `schema_content`).
    pub async fn preload_schema_metadata(
        &self,
    ) -> DbResult<Vec<(Id<EventPayloadSchema>, EventSource, EventType, String)>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id as "id!: Uuid",
                source,
                event_type,
                schema_version
            FROM sinex_schemas.event_payload_schemas
            WHERE is_active = true
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "preload schema metadata"))?;

        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    Id::from_uuid(row.id),
                    row.source.into(),
                    row.event_type.into(),
                    row.schema_version,
                )
            })
            .collect())
    }
}

#[cfg(test)]
#[path = "schema_cache_test.rs"]
mod tests;
