//! Source material repository for managing raw data sources
//!
//! This repository handles registration and tracking of source materials
//! (files, streams, etc.) that contain events to be processed.

use super::common::{db_error, DbResult, Repository};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sinex_core_types::ids::MaterialId;
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Source material record matching raw.source_material_registry
#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct SourceMaterial {
    #[sqlx(rename = "blob_id")]
    pub id: MaterialId,
    pub material_type: String,
    pub source_uri: Option<String>,
    pub ingestion_time: DateTime<Utc>,
    pub file_size_bytes: Option<i64>,
    pub checksum_blake3: Option<String>,
    pub mime_type: Option<String>,
    pub encoding: Option<String>,
    pub metadata: JsonValue,
    pub content_preview: Option<String>,
    pub is_archived: bool,
    pub archive_time: Option<DateTime<Utc>>,
    pub retention_policy: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// New source material to register
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewSourceMaterial {
    pub material_type: String,
    pub source_uri: Option<String>,
    pub file_size_bytes: Option<i64>,
    pub checksum_blake3: Option<String>,
    pub mime_type: Option<String>,
    pub encoding: Option<String>,
    pub metadata: Option<JsonValue>,
    pub content_preview: Option<String>,
    pub retention_policy: Option<String>,
}

/// Source material repository
pub struct SourceMaterialRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for SourceMaterialRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl<'a> SourceMaterialRepository<'a> {
    /// Register a new source material
    pub async fn register_material(&self, material: NewSourceMaterial) -> DbResult<SourceMaterial> {
        let id = MaterialId::new();
        let metadata = material.metadata.unwrap_or(serde_json::json!({}));

        sqlx::query_as!(
            SourceMaterial,
            r#"
            INSERT INTO raw.source_material_registry (
                blob_id,
                material_type,
                source_uri,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                retention_policy
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING 
                blob_id as "id: MaterialId",
                material_type,
                source_uri,
                ingestion_time,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at
            "#,
            *id.as_ulid() as _,
            material.material_type,
            material.source_uri,
            material.file_size_bytes,
            material.checksum_blake3,
            material.mime_type,
            material.encoding,
            metadata,
            material.content_preview,
            material.retention_policy
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "register material"))
    }

    /// Get source material by ID
    pub async fn get_by_id(&self, id: MaterialId) -> DbResult<Option<SourceMaterial>> {
        sqlx::query_as!(
            SourceMaterial,
            r#"
            SELECT
                blob_id as "id: MaterialId",
                material_type,
                source_uri,
                ingestion_time,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at
            FROM raw.source_material_registry
            WHERE blob_id = $1
            "#,
            *id.as_ulid() as _
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get material by id"))
    }

    /// Find source material by checksum
    pub async fn find_by_checksum(&self, checksum: &str) -> DbResult<Option<SourceMaterial>> {
        sqlx::query_as!(
            SourceMaterial,
            r#"
            SELECT
                blob_id as "id: MaterialId",
                material_type,
                source_uri,
                ingestion_time,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at
            FROM raw.source_material_registry
            WHERE checksum_blake3 = $1
            "#,
            checksum
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find material by checksum"))
    }

    /// Get recent materials
    pub async fn get_recent(&self, limit: i64) -> DbResult<Vec<SourceMaterial>> {
        sqlx::query_as!(
            SourceMaterial,
            r#"
            SELECT
                blob_id as "id: MaterialId",
                material_type,
                source_uri,
                ingestion_time,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at
            FROM raw.source_material_registry
            ORDER BY ingestion_time DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent materials"))
    }

    /// Get materials by type
    pub async fn get_by_type(
        &self,
        material_type: &str,
        limit: Option<i64>,
    ) -> DbResult<Vec<SourceMaterial>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            SourceMaterial,
            r#"
            SELECT
                blob_id as "id: MaterialId",
                material_type,
                source_uri,
                ingestion_time,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at
            FROM raw.source_material_registry
            WHERE material_type = $1
            ORDER BY ingestion_time DESC
            LIMIT $2
            "#,
            material_type,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get materials by type"))
    }

    /// Search materials by metadata
    pub async fn search_by_metadata(
        &self,
        key: &str,
        value: &JsonValue,
        limit: Option<i64>,
    ) -> DbResult<Vec<SourceMaterial>> {
        let limit = limit.unwrap_or(100);
        let search_obj = serde_json::json!({ key: value });

        sqlx::query_as!(
            SourceMaterial,
            r#"
            SELECT
                blob_id as "id: MaterialId",
                material_type,
                source_uri,
                ingestion_time,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at
            FROM raw.source_material_registry
            WHERE metadata @> $1
            ORDER BY ingestion_time DESC
            LIMIT $2
            "#,
            search_obj,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "search materials by metadata"))
    }

    /// Archive a material
    pub async fn archive_material(&self, id: MaterialId) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET 
                is_archived = true,
                archive_time = NOW(),
                updated_at = NOW()
            WHERE blob_id = $1 AND NOT is_archived
            "#,
            *id.as_ulid() as _
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "archive material"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Get non-archived materials older than a certain date
    pub async fn get_materials_for_archival(
        &self,
        older_than: DateTime<Utc>,
        limit: Option<i64>,
    ) -> DbResult<Vec<SourceMaterial>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            SourceMaterial,
            r#"
            SELECT
                blob_id as "id: MaterialId",
                material_type,
                source_uri,
                ingestion_time,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at
            FROM raw.source_material_registry
            WHERE NOT is_archived 
              AND ingestion_time < $1
            ORDER BY ingestion_time ASC
            LIMIT $2
            "#,
            older_than,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get materials for archival"))
    }

    /// Update material metadata
    pub async fn update_metadata(
        &self,
        id: MaterialId,
        metadata: JsonValue,
    ) -> DbResult<Option<SourceMaterial>> {
        sqlx::query_as!(
            SourceMaterial,
            r#"
            UPDATE raw.source_material_registry
            SET 
                metadata = $2,
                updated_at = NOW()
            WHERE blob_id = $1
            RETURNING 
                blob_id as "id: MaterialId",
                material_type,
                source_uri,
                ingestion_time,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at
            "#,
            *id.as_ulid() as _,
            metadata
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "update material metadata"))
    }

    /// Count materials by type
    pub async fn count_by_type(&self, material_type: &str) -> DbResult<i64> {
        let result = sqlx::query!(
            r#"
            SELECT COUNT(*) as count
            FROM raw.source_material_registry
            WHERE material_type = $1
            "#,
            material_type
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count materials by type"))?;

        Ok(result.count.unwrap_or(0))
    }

    /// Get total size of materials by type
    pub async fn get_total_size_by_type(&self, material_type: &str) -> DbResult<i64> {
        let result = sqlx::query!(
            r#"
            SELECT COALESCE(SUM(file_size_bytes), 0)::BIGINT as total_size
            FROM raw.source_material_registry
            WHERE material_type = $1
            "#,
            material_type
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get total size by type"))?;

        Ok(result.total_size.unwrap_or(0))
    }

    /// Register in-flight source material (for Stage-as-You-Go pattern)
    pub async fn register_in_flight(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
    ) -> DbResult<SourceMaterial> {
        let id = MaterialId::new();
        let content_preview = Some("[IN-FLIGHT]".to_string());

        sqlx::query_as!(
            SourceMaterial,
            r#"
            INSERT INTO raw.source_material_registry (
                blob_id, material_type, source_uri, metadata, content_preview
            ) VALUES (
                $1, $2, $3, $4, $5
            )
            RETURNING 
                blob_id as "id: MaterialId",
                material_type,
                source_uri,
                ingestion_time,
                file_size_bytes,
                checksum_blake3,
                mime_type,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at
            "#,
            *id.as_ulid() as _,
            material_type,
            source_uri,
            metadata,
            content_preview
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "register in-flight material"))
    }

    /// Finalize in-flight source material
    pub async fn finalize_in_flight(
        &self,
        id: Ulid,
        file_size_bytes: i64,
        checksum_blake3: String,
        mime_type: Option<&str>,
        encoding: Option<&str>,
        content_preview: Option<String>,
    ) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET file_size_bytes = $2,
                checksum_blake3 = $3,
                mime_type = $4,
                encoding = $5,
                content_preview = $6,
                updated_at = NOW()
            WHERE blob_id = $1
            "#,
            id as _,
            file_size_bytes,
            checksum_blake3,
            mime_type,
            encoding,
            content_preview
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "finalize in-flight material"))?;

        Ok(())
    }
}

/// Transaction support for SourceMaterialRepository
impl<'a> super::common::TransactionSupport for SourceMaterialRepository<'a> {
    type Item = SourceMaterialRepositoryTx<'a>;

    fn with_tx(self, _tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Self::Item {
        SourceMaterialRepositoryTx::new(self.pool)
    }
}

/// Transaction wrapper for SourceMaterialRepository
pub struct SourceMaterialRepositoryTx<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for SourceMaterialRepositoryTx<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl<'a> SourceMaterialRepositoryTx<'a> {
    pub async fn register_material(&self, material: NewSourceMaterial) -> DbResult<SourceMaterial> {
        SourceMaterialRepository::new(self.pool)
            .register_material(material)
            .await
    }

    pub async fn get_by_id(&self, id: MaterialId) -> DbResult<Option<SourceMaterial>> {
        SourceMaterialRepository::new(self.pool).get_by_id(id).await
    }

    pub async fn find_by_checksum(&self, checksum: &str) -> DbResult<Option<SourceMaterial>> {
        SourceMaterialRepository::new(self.pool)
            .find_by_checksum(checksum)
            .await
    }

    pub async fn archive_material(&self, id: MaterialId) -> DbResult<bool> {
        SourceMaterialRepository::new(self.pool)
            .archive_material(id)
            .await
    }

    pub async fn update_metadata(
        &self,
        id: MaterialId,
        metadata: JsonValue,
    ) -> DbResult<Option<SourceMaterial>> {
        SourceMaterialRepository::new(self.pool)
            .update_metadata(id, metadata)
            .await
    }
}
