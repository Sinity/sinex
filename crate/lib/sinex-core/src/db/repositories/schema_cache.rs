//! Schema Cache Repository
//!
//! Centralized schema lookup and caching for event validation.
//! This consolidates schema access patterns from:
//! - `types/events/schema_registry.rs` (lazy lookup by source/event_type)
//! - `db/validation.rs` (bulk loading for EventValidator)
//! - `sinex-ingestd/service.rs` (schema content for NATS KV)

use crate::db::repositories::common::db_error;
use crate::types::domain::{EventSource, EventType};
use crate::types::ulid::Ulid;
use crate::{DbResult, JsonValue};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_schema::ulid_conversions::uuid_to_ulid;
use sqlx::PgPool;

/// Minimal schema record for cache operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedSchema {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    pub schema_version: String,
    pub schema_content: JsonValue,
    pub updated_at: DateTime<Utc>,
}

/// Repository for schema cache operations
pub struct SchemaCacheRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> SchemaCacheRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Look up the schema ID for a given source and event type
    ///
    /// This queries for the latest active schema (v1 hardcoded for now).
    /// Callers should cache the result to avoid repeated DB queries.
    pub async fn lookup_schema_id(
        &self,
        source: &EventSource,
        event_type: &EventType,
    ) -> DbResult<Option<Ulid>> {
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
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "lookup schema id"))?
        .map(uuid_to_ulid);

        Ok(result)
    }

    /// Look up the schema version for a given schema ID
    pub async fn lookup_schema_version(&self, schema_id: Ulid) -> DbResult<Option<String>> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT schema_version
            FROM sinex_schemas.event_payload_schemas
            WHERE id = $1::uuid::ulid
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
    /// Used by sinex-ingestd to store schemas in NATS KV.
    pub async fn get_schema_content(&self, schema_id: Ulid) -> DbResult<Option<JsonValue>> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT schema_content
            FROM sinex_schemas.event_payload_schemas
            WHERE id::uuid = $1 AND is_active = true
            "#,
            schema_id.as_uuid()
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema content"))?;

        Ok(result)
    }

    /// Load latest active schemas (one per source/event_type pair)
    ///
    /// Used by EventValidator to populate its cache on startup.
    /// Returns the most recently updated schema for each source/event_type.
    pub async fn fetch_latest_active_schemas(&self) -> DbResult<Vec<CachedSchema>> {
        let rows = sqlx::query!(
            r#"
            SELECT DISTINCT ON (source, event_type)
                id::uuid as "id!: Ulid",
                source,
                event_type,
                schema_version,
                schema_content as "schema_content!",
                updated_at
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
                id: row.id,
                source: row.source,
                event_type: row.event_type,
                schema_version: row.schema_version,
                schema_content: row.schema_content,
                updated_at: row.updated_at,
            })
            .collect())
    }

    /// Load all active schemas (including multiple versions per source/event_type)
    ///
    /// Used by EventValidator for version-aware deserialization.
    pub async fn fetch_all_active_schemas(&self) -> DbResult<Vec<CachedSchema>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id::uuid as "id!: Ulid",
                source,
                event_type,
                schema_version,
                schema_content as "schema_content!",
                updated_at
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
                id: row.id,
                source: row.source,
                event_type: row.event_type,
                schema_version: row.schema_version,
                schema_content: row.schema_content,
                updated_at: row.updated_at,
            })
            .collect())
    }

    /// Bulk fetch schema content for multiple schema IDs
    ///
    /// Used by sinex-ingestd to efficiently load schemas for NATS KV storage.
    pub async fn get_schemas_by_ids(&self, schema_ids: &[Ulid]) -> DbResult<Vec<CachedSchema>> {
        if schema_ids.is_empty() {
            return Ok(Vec::new());
        }

        let uuids: Vec<_> = schema_ids.iter().map(|id| id.as_uuid()).collect();

        let rows = sqlx::query!(
            r#"
            SELECT
                id::uuid as "id!: Ulid",
                source,
                event_type,
                schema_version,
                schema_content as "schema_content!",
                updated_at
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
                id: row.id,
                source: row.source,
                event_type: row.event_type,
                schema_version: row.schema_version,
                schema_content: row.schema_content,
                updated_at: row.updated_at,
            })
            .collect())
    }

    /// Preload all active schemas for in-memory caching
    ///
    /// Returns tuples of (id, source, event_type, schema_version) for efficient cache population.
    /// This is optimized for the use case where only metadata is needed (no schema_content).
    pub async fn preload_schema_metadata(&self) -> DbResult<Vec<(Ulid, String, String, String)>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id::uuid as "id!: Ulid",
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
            .map(|row| (row.id, row.source, row.event_type, row.schema_version))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::repositories::schema_management::{NewEventSchema, SchemaManagementRepository};

    async fn setup_test_schema(pool: &PgPool) -> DbResult<Ulid> {
        let repo = SchemaManagementRepository::new(pool);
        let schema = NewEventSchema {
            source: "test-source".to_string(),
            event_type: "test.event".to_string(),
            schema_version: "v1".to_string(),
            schema_content: serde_json::json!({
                "type": "object",
                "properties": {
                    "test": {"type": "string"}
                }
            }),
        };
        let result = repo.register_schema(schema).await?;
        Ok(result.id)
    }

    #[sqlx::test]
    async fn test_lookup_schema_id(pool: PgPool) -> DbResult<()> {
        let schema_id = setup_test_schema(&pool).await?;
        let cache_repo = SchemaCacheRepository::new(&pool);

        let source = EventSource::from("test-source".to_string());
        let event_type = EventType::from("test.event".to_string());

        let found_id = cache_repo.lookup_schema_id(&source, &event_type).await?;
        assert_eq!(found_id, Some(schema_id));

        Ok(())
    }

    #[sqlx::test]
    async fn test_lookup_schema_version(pool: PgPool) -> DbResult<()> {
        let schema_id = setup_test_schema(&pool).await?;
        let cache_repo = SchemaCacheRepository::new(&pool);

        let version = cache_repo.lookup_schema_version(schema_id).await?;
        assert_eq!(version, Some("v1".to_string()));

        Ok(())
    }

    #[sqlx::test]
    async fn test_get_schema_content(pool: PgPool) -> DbResult<()> {
        let schema_id = setup_test_schema(&pool).await?;
        let cache_repo = SchemaCacheRepository::new(&pool);

        let content = cache_repo.get_schema_content(schema_id).await?;
        assert!(content.is_some());
        let json = content.unwrap();
        assert_eq!(json["type"], "object");

        Ok(())
    }

    #[sqlx::test]
    async fn test_fetch_latest_active_schemas(pool: PgPool) -> DbResult<()> {
        setup_test_schema(&pool).await?;
        let cache_repo = SchemaCacheRepository::new(&pool);

        let schemas = cache_repo.fetch_latest_active_schemas().await?;
        assert!(!schemas.is_empty());

        let test_schema = schemas
            .iter()
            .find(|s| s.source == "test-source" && s.event_type == "test.event");
        assert!(test_schema.is_some());

        Ok(())
    }

    #[sqlx::test]
    async fn test_get_schemas_by_ids(pool: PgPool) -> DbResult<()> {
        let schema_id = setup_test_schema(&pool).await?;
        let cache_repo = SchemaCacheRepository::new(&pool);

        let schemas = cache_repo.get_schemas_by_ids(&[schema_id]).await?;
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].id, schema_id);

        Ok(())
    }

    #[sqlx::test]
    async fn test_preload_schema_metadata(pool: PgPool) -> DbResult<()> {
        let schema_id = setup_test_schema(&pool).await?;
        let cache_repo = SchemaCacheRepository::new(&pool);

        let metadata = cache_repo.preload_schema_metadata().await?;
        assert!(!metadata.is_empty());

        let found = metadata.iter().find(|(id, _, _, _)| *id == schema_id);
        assert!(found.is_some());

        let (id, source, event_type, version) = found.unwrap();
        assert_eq!(*id, schema_id);
        assert_eq!(source, "test-source");
        assert_eq!(event_type, "test.event");
        assert_eq!(version, "v1");

        Ok(())
    }
}
