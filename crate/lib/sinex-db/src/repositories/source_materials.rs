//! Source material repository for managing raw data sources
//!
//! This repository handles registration and tracking of source materials
//! (files, streams, etc.) that contain events to be processed.
use super::common::{db_error, DbResult, EnhancedRepository, Repository};
use crate::query_helpers::ulid_to_uuid;
use crate::schema::SourceMaterialRegistry;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use sinex_primitives::Id;
use sinex_primitives::Timestamp;
use sinex_schema::schema::records::SourceMaterialRecord;
use sqlx::PgPool;
use time::format_description;
/// Canonical material kinds recognised by the registry
pub mod material_kinds {
    pub const ANNEX: &str = "annex";
    pub const GIT: &str = "git";
}
/// Canonical timing info types
pub mod timing_info_types {
    pub const REALTIME: &str = "realtime";
    pub const INTRINSIC: &str = "intrinsic";
    pub const INFERRED: &str = "inferred";
}
/// Canonical statuses for source material lifecycle
pub mod status {
    pub const SENSING: &str = "sensing";
    pub const COMPLETED: &str = "completed";
    pub const RECOVERED_PARTIAL: &str = "recovered_partial";
    pub const FAILED: &str = "failed";
}
/// Canonical material type constants stored in metadata.
pub mod material_types {
    pub const FILE: &str = "file";
    pub const STREAM: &str = "stream";
    pub const BLOB: &str = "blob";
    pub const BLOB_BINARY: &str = "blob.binary";
    pub const BLOB_TEXT: &str = "blob.text";
    pub const CHUNK: &str = "chunk";
}
/// Source material registration payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMaterial {
    material_kind: String,
    source_identifier: String,
    timing_info_type: String,
    status: String,
    metadata: JsonValue,
    optional_blob_id: Option<Id<crate::Blob>>,
    pub start_time: Option<Timestamp>,
    pub end_time: Option<Timestamp>,
    staged_by: Option<String>,
    staged_on_host: Option<String>,
}
impl SourceMaterial {
    fn new(material_kind: impl Into<String>, source_identifier: impl Into<String>) -> Self {
        Self {
            material_kind: material_kind.into(),
            source_identifier: source_identifier.into(),
            timing_info_type: timing_info_types::INTRINSIC.to_string(),
            status: status::COMPLETED.to_string(),
            metadata: json!({}),
            optional_blob_id: None,
            start_time: None,
            end_time: None,
            staged_by: None,
            staged_on_host: None,
        }
    }
    fn metadata_object_mut(&mut self) -> &mut JsonMap<String, JsonValue> {
        if !self.metadata.is_object() {
            self.metadata = json!({});
        }
        self.metadata
            .as_object_mut()
            .expect("metadata forced to object")
    }
    fn merge_metadata(&mut self, extra: JsonValue) {
        match extra {
            JsonValue::Object(map) => {
                let target = self.metadata_object_mut();
                for (key, value) in map.into_iter() {
                    target.insert(key, value);
                }
            }
            JsonValue::Null => {}
            other => {
                let target = self.metadata_object_mut();
                target.insert("_meta".to_string(), other);
            }
        }
    }
    /// Create a file-backed source material entry.
    pub fn file(path: impl Into<String>) -> Self {
        let path_str = path.into();
        let mut material = Self::new(material_kinds::ANNEX, path_str.clone());
        material
            .metadata_object_mut()
            .insert("source_uri".to_string(), JsonValue::String(path_str));
        material.metadata_object_mut().insert(
            "material_type".to_string(),
            JsonValue::String(material_types::FILE.to_string()),
        );
        material
    }
    /// Create a stream-backed source material entry.
    pub fn stream(uri: impl Into<String>) -> Self {
        let uri_str = uri.into();
        let mut material = Self::new(material_kinds::ANNEX, uri_str.clone());
        material
            .metadata_object_mut()
            .insert("source_uri".to_string(), JsonValue::String(uri_str));
        material.metadata_object_mut().insert(
            "material_type".to_string(),
            JsonValue::String(material_types::STREAM.to_string()),
        );
        material.with_timing_info_type(timing_info_types::REALTIME)
    }
    /// Create an in-memory blob source material entry.
    pub fn blob() -> Self {
        let mut material = Self::new(material_kinds::ANNEX, "memory://inline");
        material.metadata_object_mut().insert(
            "material_type".to_string(),
            JsonValue::String(material_types::BLOB.to_string()),
        );
        material
    }
    /// Create a binary blob source material entry.
    pub fn blob_binary(filename: impl Into<String>) -> Self {
        let filename = filename.into();
        let mut material = Self::new(material_kinds::ANNEX, filename.clone());
        let metadata = material.metadata_object_mut();
        metadata.insert("filename".to_string(), JsonValue::String(filename));
        metadata.insert(
            "material_type".to_string(),
            JsonValue::String(material_types::BLOB_BINARY.to_string()),
        );
        material
    }
    /// Create a text blob source material entry.
    pub fn blob_text(filename: impl Into<String>) -> Self {
        let filename = filename.into();
        let mut material = Self::new(material_kinds::ANNEX, filename.clone());
        {
            let metadata = material.metadata_object_mut();
            metadata.insert("filename".to_string(), JsonValue::String(filename));
            metadata.insert(
                "material_type".to_string(),
                JsonValue::String(material_types::BLOB_TEXT.to_string()),
            );
            metadata.insert(
                "encoding".to_string(),
                JsonValue::String("utf-8".to_string()),
            );
        }
        material
    }
    /// Create a chunk source material (for large file processing)
    pub fn chunk(parent_id: impl Into<String>, index: usize) -> Self {
        let identifier = format!("chunk://{}#{}", parent_id.into(), index);
        let mut material = Self::new(material_kinds::ANNEX, identifier.clone());
        let metadata = material.metadata_object_mut();
        metadata.insert("chunk_uri".to_string(), JsonValue::String(identifier));
        metadata.insert(
            "material_type".to_string(),
            JsonValue::String(material_types::CHUNK.to_string()),
        );
        material
    }
    /// Fluent method to set blob ID
    pub fn with_blob_id(mut self, blob_id: Id<crate::Blob>) -> Self {
        self.optional_blob_id = Some(blob_id);
        self
    }
    /// Fluent method to set encoding (stored in metadata)
    pub fn with_encoding(mut self, encoding: impl Into<String>) -> Self {
        self.metadata_object_mut()
            .insert("encoding".to_string(), JsonValue::String(encoding.into()));
        self
    }
    /// Fluent method to set metadata (merged with existing entries)
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.merge_metadata(metadata);
        self
    }
    /// Fluent method to set content preview (stored in metadata)
    pub fn with_content_preview(mut self, preview: impl Into<String>) -> Self {
        self.metadata_object_mut().insert(
            "content_preview".to_string(),
            JsonValue::String(preview.into()),
        );
        self
    }
    /// Fluent method to set retention policy (stored in metadata)
    pub fn with_retention_policy(mut self, policy: impl Into<String>) -> Self {
        self.metadata_object_mut().insert(
            "retention_policy".to_string(),
            JsonValue::String(policy.into()),
        );
        self
    }
    /// Fluent method to override the status
    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        self.status = status.into();
        self
    }
    /// Fluent method to override the timing info type
    pub fn with_timing_info_type(mut self, timing: impl Into<String>) -> Self {
        self.timing_info_type = timing.into();
        self
    }
    pub fn with_start_time(mut self, start_time: Timestamp) -> Self {
        self.start_time = Some(start_time);
        self
    }
    pub fn with_end_time(mut self, end_time: Timestamp) -> Self {
        self.end_time = Some(end_time);
        self
    }
    pub fn with_staged_by(mut self, staged_by: impl Into<String>) -> Self {
        self.staged_by = Some(staged_by.into());
        self
    }
    pub fn with_staged_on_host(mut self, host: impl Into<String>) -> Self {
        self.staged_on_host = Some(host.into());
        self
    }
}
/// Entry for the raw.temporal_ledger table.
///
/// Tracks timing metadata for source materials, including capture windows
/// and clock synchronization information.
#[derive(Debug, Clone)]
pub struct TemporalLedgerEntry {
    /// ID of the source material this entry refers to
    pub source_material_id: sinex_primitives::Ulid,
    /// Start offset within the source material
    pub offset_start: i64,
    /// End offset within the source material
    pub offset_end: i64,
    /// Offset kind (e.g., "byte", "line", "record")
    pub offset_kind: String,
    /// Capture timestamp
    pub ts_capture: Timestamp,
    /// Precision of the capture timing (e.g., "bounded", "exact")
    pub precision: String,
    /// Clock type used (e.g., "wall", "monotonic")
    pub clock: String,
    /// Source type (e.g., "realtime_capture", "batch_import")
    pub source_type: String,
}
impl TemporalLedgerEntry {
    /// Create a new ledger entry for a realtime capture
    pub fn realtime_capture(
        source_material_id: sinex_primitives::Ulid,
        offset_end: i64,
        ts_capture: Timestamp,
    ) -> Self {
        Self {
            source_material_id,
            offset_start: 0,
            offset_end,
            offset_kind: "byte".to_string(),
            ts_capture,
            precision: "bounded".to_string(),
            clock: "wall".to_string(),
            source_type: "realtime_capture".to_string(),
        }
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
    type Table = SourceMaterialRegistry;
}
impl<'a> SourceMaterialRepository<'a> {
    /// Register a new source material
    pub async fn register_material(
        &self,
        material: SourceMaterial,
    ) -> DbResult<SourceMaterialRecord> {
        use crate::query_helpers::{
            set_repeatable_read, with_retry_transaction_idempotent, IdempotentTransaction,
            RetryConfig,
        };
        let id = Id::<SourceMaterial>::new();
        // Clone material for retry closure
        let material = material.clone();
        with_retry_transaction_idempotent(
            self.pool,
            RetryConfig::default(),
            IdempotentTransaction::new(),
            move |tx| {
                let id = id.clone();
                let material = material.clone();
                let start_time_offset = material.start_time;
                let end_time_offset = material.end_time;
                Box::pin(async move {
                    set_repeatable_read(tx).await?;
                    sqlx::query_as!(
                        SourceMaterialRecord,
                        r#"
                        INSERT INTO raw.source_material_registry (
                            id,
                            material_kind,
                            source_identifier,
                            status,
                            timing_info_type,
                            metadata,
                            start_time,
                            end_time,
                            staged_by,
                            staged_on_host,
                            optional_blob_id
                        ) VALUES (
                            ($1::uuid)::ulid,
                            $2,
                            $3,
                            $4,
                            $5,
                            $6,
                            $7,
                            $8,
                            $9,
                            $10,
                            ($11::uuid)::ulid
                        )
                        RETURNING
                            id::uuid as "id!: Ulid",
                            material_kind,
                            source_identifier,
                            status,
                            timing_info_type,
                            metadata,
                            staged_at as "staged_at: Timestamp",
                            start_time as "start_time: Timestamp",
                            end_time as "end_time: Timestamp",
                            staged_by,
                            staged_on_host,
                            optional_blob_id::uuid as "optional_blob_id: Ulid"
                        "#,
                        ulid_to_uuid(*id.as_ulid()),
                        material.material_kind,
                        material.source_identifier,
                        material.status,
                        material.timing_info_type,
                        material.metadata,
                        start_time_offset.map(|t| *t),
                        end_time_offset.map(|t| *t),
                        material.staged_by,
                        material.staged_on_host,
                        material
                            .optional_blob_id
                            .map(|id| ulid_to_uuid(*id.as_ulid()))
                    )
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(|e| db_error(e, "register material"))
                })
            },
        )
        .await
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
                id::uuid as "id!: crate::Ulid",
                material_kind,
                source_identifier,
                status,
                timing_info_type,
                metadata,
                staged_at as "staged_at: Timestamp",
                start_time as "start_time: Timestamp",
                end_time as "end_time: Timestamp",
                staged_by,
                staged_on_host,
                optional_blob_id::uuid as "optional_blob_id?: crate::Ulid"
            FROM raw.source_material_registry
            WHERE id::uuid = $1
            "#,
            ulid_to_uuid(*id.as_ulid())
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get material by id"))
    }
    /// Find source material by blob ID
    pub async fn find_by_blob_id(
        &self,
        blob_id: Id<crate::Blob>,
    ) -> DbResult<Option<SourceMaterialRecord>> {
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id::uuid as "id!: crate::Ulid",
                material_kind,
                source_identifier,
                status,
                timing_info_type,
                metadata,
                staged_at as "staged_at: Timestamp",
                start_time as "start_time: Timestamp",
                end_time as "end_time: Timestamp",
                staged_by,
                staged_on_host,
                optional_blob_id::uuid as "optional_blob_id?: crate::Ulid"
            FROM raw.source_material_registry
            WHERE optional_blob_id::uuid = $1
            "#,
            ulid_to_uuid(*blob_id.as_ulid())
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find material by blob id"))
    }
    /// Get recent materials ordered by staged time
    pub async fn get_recent(&self, limit: i64) -> DbResult<Vec<SourceMaterialRecord>> {
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id::uuid as "id!: crate::Ulid",
                material_kind,
                source_identifier,
                status,
                timing_info_type,
                metadata,
                staged_at as "staged_at: Timestamp",
                start_time as "start_time: Timestamp",
                end_time as "end_time: Timestamp",
                staged_by,
                staged_on_host,
                optional_blob_id::uuid as "optional_blob_id?: crate::Ulid"
            FROM raw.source_material_registry
            ORDER BY staged_at DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent materials"))
    }
    /// Get materials filtered by canonical material kind
    pub async fn get_by_kind(
        &self,
        material_kind: &str,
        limit: Option<i64>,
    ) -> DbResult<Vec<SourceMaterialRecord>> {
        let limit = limit.unwrap_or(100);
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id::uuid as "id!: crate::Ulid",
                material_kind,
                source_identifier,
                status,
                timing_info_type,
                metadata,
                staged_at as "staged_at: Timestamp",
                start_time as "start_time: Timestamp",
                end_time as "end_time: Timestamp",
                staged_by,
                staged_on_host,
                optional_blob_id::uuid as "optional_blob_id?: crate::Ulid"
            FROM raw.source_material_registry
            WHERE material_kind = $1
            ORDER BY staged_at DESC
            LIMIT $2
            "#,
            material_kind,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get materials by kind"))
    }
    /// Search materials by metadata containment
    pub async fn search_by_metadata(
        &self,
        key: &str,
        value: &JsonValue,
        limit: Option<i64>,
    ) -> DbResult<Vec<SourceMaterialRecord>> {
        let limit = limit.unwrap_or(100);
        let search_obj = json!({ key: value });
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id::uuid as "id!: crate::Ulid",
                material_kind,
                source_identifier,
                status,
                timing_info_type,
                metadata,
                staged_at as "staged_at: Timestamp",
                start_time as "start_time: Timestamp",
                end_time as "end_time: Timestamp",
                staged_by,
                staged_on_host,
                optional_blob_id::uuid as "optional_blob_id?: crate::Ulid"
            FROM raw.source_material_registry
            WHERE metadata @> $1
            ORDER BY staged_at DESC
            LIMIT $2
            "#,
            search_obj,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "search materials by metadata"))
    }
    /// Mark a material as archived via metadata flag
    pub async fn archive_material(&self, id: Id<SourceMaterialRecord>) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET metadata = core.jsonb_merge_deep(metadata, jsonb_build_object('archived', true, 'archived_at', NOW()))
            WHERE id::uuid = $1
            "#,
            ulid_to_uuid(*id.as_ulid())
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "archive material"))?;
        Ok(result.rows_affected() > 0)
    }
    /// Retrieve materials eligible for archival (no archived flag and older than threshold)
    pub async fn get_materials_for_archival(
        &self,
        older_than: Timestamp,
        limit: Option<i64>,
    ) -> DbResult<Vec<SourceMaterialRecord>> {
        let older_than_offset = older_than;
        let limit = limit.unwrap_or(100);
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id::uuid as "id!: crate::Ulid",
                material_kind,
                source_identifier,
                status,
                timing_info_type,
                metadata,
                staged_at as "staged_at: Timestamp",
                start_time as "start_time: Timestamp",
                end_time as "end_time: Timestamp",
                staged_by,
                staged_on_host,
                optional_blob_id::uuid as "optional_blob_id?: crate::Ulid"
            FROM raw.source_material_registry
            WHERE (metadata->>'archived') IS DISTINCT FROM 'true'
              AND staged_at < $1
            ORDER BY staged_at ASC
            LIMIT $2
            "#,
            *older_than_offset,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get materials for archival"))
    }
    /// Update material metadata (merged at the database level)
    pub async fn update_metadata(
        &self,
        id: Id<SourceMaterialRecord>,
        metadata: JsonValue,
    ) -> DbResult<Option<SourceMaterialRecord>> {
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            UPDATE raw.source_material_registry
            SET metadata = core.jsonb_merge_deep(metadata, $2)
            WHERE id::uuid = $1
            RETURNING
                id::uuid as "id!: crate::Ulid",
                material_kind,
                source_identifier,
                status,
                timing_info_type,
                metadata,
                staged_at as "staged_at: Timestamp",
                start_time as "start_time: Timestamp",
                end_time as "end_time: Timestamp",
                staged_by,
                staged_on_host,
                optional_blob_id::uuid as "optional_blob_id: Ulid"
            "#,
            ulid_to_uuid(*id.as_ulid()),
            metadata
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "update material metadata"))
    }
    /// Count materials by canonical kind
    pub async fn count_by_kind(&self, material_kind: &str) -> DbResult<i64> {
        let result = sqlx::query!(
            r#"
            SELECT COUNT(*) as count
            FROM raw.source_material_registry
            WHERE material_kind = $1
            "#,
            material_kind
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count materials by kind"))?;
        Ok(result.count.unwrap_or(0))
    }
    /// Get total size of materials by canonical kind (sourced from core.blobs)
    pub async fn get_total_size_by_kind(&self, material_kind: &str) -> DbResult<i64> {
        let total_size: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT COALESCE(SUM(b.size_bytes), 0)::BIGINT
            FROM raw.source_material_registry sm
            LEFT JOIN core.blobs b ON sm.optional_blob_id::uuid = b.id::uuid
            WHERE sm.material_kind = $1
            "#,
        )
        .bind(material_kind)
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get total size by kind"))?;
        Ok(total_size.unwrap_or(0))
    }
    async fn register_in_flight_internal(
        &self,
        id: Id<SourceMaterial>,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
        start_time_override: Option<Timestamp>,
    ) -> DbResult<SourceMaterialRecord> {
        use crate::query_helpers::{
            set_repeatable_read, with_retry_transaction_idempotent, IdempotentTransaction,
            RetryConfig,
        };
        // Own the arguments for the closure
        let material_type = material_type.to_string();
        let source_uri = source_uri.map(|s| s.to_string());
        with_retry_transaction_idempotent(
            self.pool,
            RetryConfig::default(),
            IdempotentTransaction::new(),
            move |tx| {
                let id = id.clone();
                let material_type = material_type.clone();
                let source_uri = source_uri.clone();
                let metadata = metadata.clone();
                Box::pin(async move {
                    set_repeatable_read(tx).await?;
                    let mut material = SourceMaterial::new(material_kinds::ANNEX, source_uri.as_deref().unwrap_or("in-flight"));
                    material.status = status::SENSING.to_string();
                    material.timing_info_type = timing_info_types::REALTIME.to_string();
                    material.merge_metadata(metadata);
                    if let Some(uri) = source_uri.as_ref() {
                        material
                            .metadata_object_mut()
                            .insert("source_uri".to_string(), JsonValue::String(uri.clone()));
                    }
                    material.metadata_object_mut().insert(
                        "material_type".to_string(),
                        JsonValue::String(material_type.clone()),
                    );
                    // Ensure we have a start time for the session; reuse on conflicts.
                    let start_time = start_time_override
                        .or(material.start_time)
                        .unwrap_or_else(Timestamp::now);
                    material.start_time = Some(start_time);
                    material.staged_by = Some("sinex-core".to_string());
                    material.staged_on_host = Some(gethostname::gethostname().to_string_lossy().to_string());
                    // 1. Try to update existing record first.
                    let update_sql = r#"
                        UPDATE raw.source_material_registry
                        SET
                            status = CASE
                                WHEN raw.source_material_registry.status IN ('completed', 'failed') THEN raw.source_material_registry.status
                                ELSE $3
                            END,
                            metadata = core.jsonb_merge_deep(raw.source_material_registry.metadata, $4),
                            staged_by = COALESCE($5, raw.source_material_registry.staged_by),
                            staged_on_host = COALESCE($6, raw.source_material_registry.staged_on_host)
                        WHERE source_identifier = $1 AND material_kind = $2
                        RETURNING
                            id::uuid as id,
                            material_kind,
                            source_identifier,
                            status,
                            timing_info_type,
                            metadata,
                            staged_at,
                            start_time,
                            end_time,
                            staged_by,
                            staged_on_host,
                            optional_blob_id::uuid as optional_blob_id
                    "#;
                    let update_result = sqlx::query_as::<_, SourceMaterialRecord>(update_sql)
                        .bind(&material.source_identifier)
                        .bind(&material.material_kind)
                        .bind(&material.status)
                        .bind(&material.metadata)
                        .bind(&material.staged_by)
                        .bind(&material.staged_on_host)
                        .fetch_optional(&mut **tx)
                        .await
                        .map_err(|e| db_error(e, "update in-flight source material"))?;
                    if let Some(record) = update_result {
                        return Ok(record);
                    }
                    // 2. If not found, insert new record.
                    let insert_sql = r#"
                        INSERT INTO raw.source_material_registry (
                            id,
                            material_kind,
                            source_identifier,
                            status,
                            timing_info_type,
                            metadata,
                            start_time,
                            staged_by,
                            staged_on_host
                        ) VALUES (
                            ($1::uuid)::ulid,
                            $2,
                            $3,
                            $4,
                            $5,
                            $6,
                            $7,
                            $8,
                            $9
                        )
                        RETURNING
                            id::uuid as id,
                            material_kind,
                            source_identifier,
                            status,
                            timing_info_type,
                            metadata,
                            staged_at,
                            start_time,
                            end_time,
                            staged_by,
                            staged_on_host,
                            optional_blob_id::uuid as optional_blob_id
                    "#;
                    sqlx::query_as::<_, SourceMaterialRecord>(insert_sql)
                        .bind(ulid_to_uuid(*id.as_ulid()))
                        .bind(&material.material_kind)
                        .bind(&material.source_identifier)
                        .bind(&material.status)
                        .bind(&material.timing_info_type)
                        .bind(&material.metadata)
                        .bind(material.start_time)
                        .bind(&material.staged_by)
                        .bind(&material.staged_on_host)
                        .fetch_one(&mut **tx)
                        .await
                        .map_err(|e| db_error(e, "insert in-flight source material"))
                })
            },
        )
        .await
    }
    pub async fn register_in_flight(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
    ) -> DbResult<SourceMaterialRecord> {
        let id = Id::<SourceMaterial>::new();
        self.register_in_flight_internal(id, material_type, source_uri, metadata, None)
            .await
    }
    pub async fn register_external_in_flight(
        &self,
        material_id: crate::Ulid,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
        started_at: Timestamp,
    ) -> DbResult<SourceMaterialRecord> {
        let id = Id::<SourceMaterial>::from_ulid(material_id);
        self.register_in_flight_internal(id, material_type, source_uri, metadata, Some(started_at))
            .await
    }
    /// Mark an in-flight source material as failed
    pub async fn mark_as_failed(
        &self,
        id: Id<SourceMaterialRecord>,
        error_reason: &str,
    ) -> DbResult<()> {
        let metadata_update = {
            let mut map = JsonMap::new();
            map.insert(
                "failure_reason".to_string(),
                JsonValue::String(error_reason.to_string()),
            );
            map.insert(
                "failed_at".to_string(),
                JsonValue::String(
                    Timestamp::now()
                        .format(&format_description::well_known::Rfc3339)
                        .unwrap(),
                ),
            );
            JsonValue::Object(map)
        };
        sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET metadata = core.jsonb_merge_deep(metadata, $2),
                status = $3,
                end_time = COALESCE(end_time, NOW())
            WHERE id::uuid = $1
            "#,
            ulid_to_uuid(*id.as_ulid()),
            metadata_update,
            status::FAILED
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "mark material as failed"))?;
        Ok(())
    }
    /// Finalize in-flight source material
    pub async fn finalize_in_flight(
        &self,
        id: Id<SourceMaterialRecord>,
        blob_id: Option<Id<crate::Blob>>,
        encoding: Option<&str>,
        content_preview: Option<String>,
        total_bytes: Option<i64>,
    ) -> DbResult<()> {
        self.finalize_in_flight_with_executor(
            self.pool,
            id,
            blob_id,
            encoding,
            content_preview,
            total_bytes,
        )
        .await
    }
    /// Finalize in-flight source material with a specific executor (e.g. for transactions)
    pub async fn finalize_in_flight_with_executor<'e, E>(
        &self,
        executor: E,
        id: Id<SourceMaterialRecord>,
        blob_id: Option<Id<crate::Blob>>,
        encoding: Option<&str>,
        content_preview: Option<String>,
        total_bytes: Option<i64>,
    ) -> DbResult<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let metadata_update = {
            let mut map = JsonMap::new();
            if let Some(bytes) = total_bytes {
                map.insert(
                    "file_size_bytes".to_string(),
                    JsonValue::Number(bytes.into()),
                );
            }
            if let Some(enc) = encoding {
                map.insert("encoding".to_string(), JsonValue::String(enc.to_string()));
            }
            if let Some(preview) = content_preview {
                map.insert("content_preview".to_string(), JsonValue::String(preview));
            }
            JsonValue::Object(map)
        };
        sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET optional_blob_id = ($2::uuid)::ulid,
                metadata = core.jsonb_merge_deep(metadata, $3),
                status = $4,
                end_time = COALESCE(end_time, NOW())
            WHERE id::uuid = $1
            "#,
            ulid_to_uuid(*id.as_ulid()),
            blob_id.map(|bid| ulid_to_uuid(*bid.as_ulid())),
            metadata_update,
            status::COMPLETED
        )
        .execute(executor)
        .await
        .map_err(|e| db_error(e, "finalize in-flight material"))?;
        Ok(())
    }
    // ========== Temporal Ledger ==========
    /// Append an entry to the temporal ledger for a source material.
    ///
    /// The temporal ledger tracks timing metadata for captures, including
    /// offset ranges, capture timestamps, and clock information.
    pub async fn append_temporal_ledger(&self, entry: TemporalLedgerEntry) -> DbResult<()> {
        sqlx::query!(
            r#"
            INSERT INTO raw.temporal_ledger
                (source_material_id, offset_start, offset_end, offset_kind, ts_capture, precision, clock, source_type)
            VALUES (($1::uuid)::ulid, $2, $3, $4, $5, $6, $7, $8)
            "#,
            ulid_to_uuid(entry.source_material_id),
            entry.offset_start,
            entry.offset_end,
            entry.offset_kind,
            *entry.ts_capture,
            entry.precision,
            entry.clock,
            entry.source_type
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "append temporal ledger entry"))?;
        Ok(())
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
