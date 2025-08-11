//! Source material repository for managing raw data sources
//!
//! This repository handles registration and tracking of source materials
//! (files, streams, etc.) that contain events to be processed.

use super::common::{db_error, DbResult, EnhancedRepository, Repository};
use crate::db::schema::SourceMaterials;
use crate::types::Id;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::PgPool;

/// Source material record matching raw.source_material_registry
#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct SourceMaterialRecord {
    pub id: Id<SourceMaterialRecord>,
    pub material_type: String,
    pub source_uri: Option<String>,
    pub ingestion_time: DateTime<Utc>,
    pub encoding: Option<String>,
    pub metadata: JsonValue,
    pub content_preview: Option<String>,
    pub is_archived: bool,
    pub archive_time: Option<DateTime<Utc>>,
    pub retention_policy: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[sqlx(rename = "optional_blob_id")]
    pub blob_id: Option<Id<crate::models::Blob>>,
}

/// Material type constants
pub mod material_types {
    pub const FILE: &str = "file";
    pub const STREAM: &str = "stream";
    pub const BLOB: &str = "blob";
    pub const BLOB_BINARY: &str = "blob.binary";
    pub const BLOB_TEXT: &str = "blob.text";
    pub const CHUNK: &str = "chunk";
}

/// Source material to register
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMaterial {
    pub material_type: String,
    pub source_uri: Option<String>,
    pub encoding: Option<String>,
    pub metadata: Option<JsonValue>,
    pub blob_id: Option<Id<crate::models::Blob>>,
    pub content_preview: Option<String>,
    pub retention_policy: Option<String>,
}

impl SourceMaterial {
    /// Create a file source material
    pub fn file(path: impl Into<String>) -> Self {
        SourceMaterial {
            material_type: material_types::FILE.to_string(),
            source_uri: Some(path.into()),
            encoding: None,
            metadata: None,
            content_preview: None,
            retention_policy: None,
            blob_id: None,
        }
    }

    /// Create a stream source material
    pub fn stream(uri: impl Into<String>) -> Self {
        SourceMaterial {
            material_type: material_types::STREAM.to_string(),
            source_uri: Some(uri.into()),
            encoding: None,
            metadata: None,
            content_preview: None,
            retention_policy: None,
            blob_id: None,
        }
    }

    /// Create a blob source material
    pub fn blob() -> Self {
        SourceMaterial {
            material_type: material_types::BLOB.to_string(),
            source_uri: Some("memory://inline".to_string()),
            encoding: None,
            metadata: None,
            content_preview: None,
            retention_policy: None,
            blob_id: None,
        }
    }

    /// Create a binary blob source material
    pub fn blob_binary(filename: impl Into<String>) -> Self {
        SourceMaterial {
            material_type: material_types::BLOB_BINARY.to_string(),
            source_uri: Some(filename.into()),
            encoding: None,
            metadata: None,
            content_preview: None,
            retention_policy: None,
            blob_id: None,
        }
    }

    /// Create a text blob source material
    pub fn blob_text(filename: impl Into<String>) -> Self {
        SourceMaterial {
            material_type: material_types::BLOB_TEXT.to_string(),
            source_uri: Some(filename.into()),
            encoding: Some("utf-8".to_string()),
            metadata: None,
            content_preview: None,
            retention_policy: None,
            blob_id: None,
        }
    }

    /// Create a chunk source material (for large file processing)
    pub fn chunk(parent_id: impl Into<String>, index: usize) -> Self {
        SourceMaterial {
            material_type: material_types::CHUNK.to_string(),
            source_uri: Some(format!("chunk://{}#{}", parent_id.into(), index)),
            encoding: None,
            metadata: None,
            content_preview: None,
            retention_policy: None,
            blob_id: None,
        }
    }

    /// Fluent method to set blob ID
    pub fn with_blob_id(mut self, blob_id: Id<crate::models::Blob>) -> Self {
        self.blob_id = Some(blob_id);
        self
    }

    /// Fluent method to set encoding
    pub fn with_encoding(mut self, encoding: impl Into<String>) -> Self {
        self.encoding = Some(encoding.into());
        self
    }

    /// Fluent method to set metadata
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Fluent method to set content preview
    pub fn with_content_preview(mut self, preview: impl Into<String>) -> Self {
        self.content_preview = Some(preview.into());
        self
    }

    /// Fluent method to set retention policy
    pub fn with_retention_policy(mut self, policy: impl Into<String>) -> Self {
        self.retention_policy = Some(policy.into());
        self
    }
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

impl<'a> EnhancedRepository<'a> for SourceMaterialRepository<'a> {
    type Table = SourceMaterials;
}

impl<'a> SourceMaterialRepository<'a> {
    /// Register a new source material
    pub async fn register_material(
        &self,
        material: SourceMaterial,
    ) -> DbResult<SourceMaterialRecord> {
        let id = Id::<SourceMaterial>::new();
        let metadata = material.metadata.unwrap_or(serde_json::json!({}));

        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            INSERT INTO raw.source_material_registry (
                id,
                material_type,
                source_uri,
                encoding,
                metadata,
                content_preview,
                retention_policy,
                optional_blob_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING 
                id as "id: Id<SourceMaterialRecord>",
                material_type,
                source_uri,
                ingestion_time,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at,
                optional_blob_id as "blob_id: Id<crate::models::Blob>"
            "#,
            *id.as_ulid() as _,
            material.material_type,
            material.source_uri,
            material.encoding,
            metadata,
            material.content_preview,
            material.retention_policy,
            material.blob_id.map(|id| *id.as_ulid()) as _
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "register material"))
    }

    /// Get source material by ID
    pub async fn get_by_id(
        &self,
        id: Id<SourceMaterialRecord>,
    ) -> DbResult<Option<SourceMaterialRecord>> {
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id as "id: Id<SourceMaterialRecord>",
                material_type,
                source_uri,
                ingestion_time,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at,
                optional_blob_id as "blob_id: Id<crate::models::Blob>"
            FROM raw.source_material_registry
            WHERE id = $1
            "#,
            *id.as_ulid() as _
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get material by id"))
    }

    /// Find source material by blob ID
    pub async fn find_by_blob_id(
        &self,
        blob_id: Id<crate::models::Blob>,
    ) -> DbResult<Option<SourceMaterialRecord>> {
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id as "id: Id<SourceMaterialRecord>",
                material_type,
                source_uri,
                ingestion_time,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at,
                optional_blob_id as "blob_id: Id<crate::models::Blob>"
            FROM raw.source_material_registry
            WHERE optional_blob_id = $1
            "#,
            *blob_id.as_ulid() as _
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find material by checksum"))
    }

    /// Get recent materials
    pub async fn get_recent(&self, limit: i64) -> DbResult<Vec<SourceMaterialRecord>> {
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id as "id: Id<SourceMaterialRecord>",
                material_type,
                source_uri,
                ingestion_time,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at,
                optional_blob_id as "blob_id: Id<crate::models::Blob>"
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
    ) -> DbResult<Vec<SourceMaterialRecord>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id as "id: Id<SourceMaterialRecord>",
                material_type,
                source_uri,
                ingestion_time,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at,
                optional_blob_id as "blob_id: Id<crate::models::Blob>"
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
    ) -> DbResult<Vec<SourceMaterialRecord>> {
        let limit = limit.unwrap_or(100);
        let search_obj = serde_json::json!({ key: value });

        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id as "id: Id<SourceMaterialRecord>",
                material_type,
                source_uri,
                ingestion_time,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at,
                optional_blob_id as "blob_id: Id<crate::models::Blob>"
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
    pub async fn archive_material(&self, id: Id<SourceMaterialRecord>) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET 
                is_archived = true,
                archive_time = NOW(),
                updated_at = NOW()
            WHERE id = $1 AND NOT is_archived
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
    ) -> DbResult<Vec<SourceMaterialRecord>> {
        let limit = limit.unwrap_or(100);

        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id as "id: Id<SourceMaterialRecord>",
                material_type,
                source_uri,
                ingestion_time,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at,
                optional_blob_id as "blob_id: Id<crate::models::Blob>"
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
        id: Id<SourceMaterialRecord>,
        metadata: JsonValue,
    ) -> DbResult<Option<SourceMaterialRecord>> {
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            UPDATE raw.source_material_registry
            SET 
                metadata = $2,
                updated_at = NOW()
            WHERE id = $1
            RETURNING 
                id as "id: Id<SourceMaterialRecord>",
                material_type,
                source_uri,
                ingestion_time,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at,
                optional_blob_id as "blob_id: Id<crate::models::Blob>"
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
            SELECT COALESCE(SUM(b.size_bytes), 0)::BIGINT as total_size
            FROM raw.source_material_registry sm
            LEFT JOIN core.blobs b ON sm.optional_blob_id = b.id
            WHERE sm.material_type = $1
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
    ) -> DbResult<SourceMaterialRecord> {
        let id = Id::<SourceMaterial>::new();
        let content_preview = Some("[IN-FLIGHT]".to_string());

        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            INSERT INTO raw.source_material_registry (
                id, material_type, source_uri, metadata, content_preview
            ) VALUES (
                $1, $2, $3, $4, $5
            )
            RETURNING 
                id as "id: Id<SourceMaterialRecord>",
                material_type,
                source_uri,
                ingestion_time,
                encoding,
                metadata,
                content_preview,
                is_archived,
                archive_time,
                retention_policy,
                created_at,
                updated_at,
                optional_blob_id as "blob_id: Id<crate::models::Blob>"
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
        id: Id<SourceMaterialRecord>,
        blob_id: Option<Id<crate::models::Blob>>,
        encoding: Option<&str>,
        content_preview: Option<String>,
    ) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET optional_blob_id = $2,
                encoding = $3,
                content_preview = $4,
                updated_at = NOW()
            WHERE id = $1
            "#,
            *id.as_ulid() as _,
            blob_id.map(|id| *id.as_ulid()) as _,
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
    pub async fn register_material(
        &self,
        material: SourceMaterial,
    ) -> DbResult<SourceMaterialRecord> {
        SourceMaterialRepository::new(self.pool)
            .register_material(material)
            .await
    }

    pub async fn get_by_id(
        &self,
        id: Id<SourceMaterialRecord>,
    ) -> DbResult<Option<SourceMaterialRecord>> {
        SourceMaterialRepository::new(self.pool).get_by_id(id).await
    }

    pub async fn find_by_blob_id(
        &self,
        blob_id: Id<crate::models::Blob>,
    ) -> DbResult<Option<SourceMaterialRecord>> {
        SourceMaterialRepository::new(self.pool)
            .find_by_blob_id(blob_id)
            .await
    }

    pub async fn archive_material(&self, id: Id<SourceMaterialRecord>) -> DbResult<bool> {
        SourceMaterialRepository::new(self.pool)
            .archive_material(id)
            .await
    }

    pub async fn update_metadata(
        &self,
        id: Id<SourceMaterialRecord>,
        metadata: JsonValue,
    ) -> DbResult<Option<SourceMaterialRecord>> {
        SourceMaterialRepository::new(self.pool)
            .update_metadata(id, metadata)
            .await
    }
}

/// Extension trait for SourceMaterial terminal methods
pub trait SourceMaterialExt {
    /// Register the material in the database
    async fn register(self, pool: &PgPool) -> DbResult<SourceMaterialRecord>;
}

impl SourceMaterialExt for SourceMaterial {
    async fn register(self, pool: &PgPool) -> DbResult<SourceMaterialRecord> {
        SourceMaterialRepository::new(pool)
            .register_material(self)
            .await
    }
}
