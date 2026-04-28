//! Source material repository for managing raw data sources
//!
//! This repository handles registration and tracking of source materials
//! (files, streams, etc.) that contain events to be processed.
use super::common::{DbResult, EnhancedRepository, Repository, db_error};
use crate::schema::SourceMaterialRegistry;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use sinex_primitives::domain::{TemporalClock, TemporalPrecision, TemporalSourceType};
use sinex_primitives::{Id, SinexError, Timestamp, events::OffsetKind};
pub use sinex_schema::schema::records::SourceMaterialLinkRecord;
use sinex_schema::schema::records::SourceMaterialRecord;
use sqlx::PgPool;
use time::format_description;
use uuid::Uuid;
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
    pub const CANCELLED: &str = "cancelled";
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
/// Canonical relation types for source-material evidence links.
pub mod relation_types {
    /// The source material on the left is backed by auxiliary evidence on the right.
    ///
    /// Example: a JSONL row-stream material backed by a `SQLite` snapshot material.
    pub const BACKED_BY: &str = "backed_by";
}
/// Top-level metadata keys reserved for system use.
///
/// These keys are set exclusively by the DB layer or the node SDK and must not
/// be overwritten by caller-supplied payloads passed to `update_metadata`.
/// The `update_metadata` path re-applies existing values for these keys on top
/// of any caller merge, so the system always wins on conflicts.
const RESERVED_METADATA_KEYS: &[&str] = &[
    "encoding",
    "content_preview",
    "retention_policy",
    "blake3",
    "total_bytes",
    "staged_by",
    "staged_on_host",
    "_meta",
];

/// Strip reserved system keys from a caller-supplied metadata object so they
/// cannot be overwritten via `update_metadata`. Non-object values are
/// returned unchanged (they carry no top-level keys to strip).
fn strip_reserved_metadata_keys(mut metadata: JsonValue) -> JsonValue {
    if let JsonValue::Object(ref mut map) = metadata {
        for key in RESERVED_METADATA_KEYS {
            map.remove(*key);
        }
    }
    metadata
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
    #[allow(clippy::expect_used)] // invariant: metadata set to object on line above
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
                for (key, value) in map {
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    pub fn with_start_time(mut self, start_time: Timestamp) -> Self {
        self.start_time = Some(start_time);
        self
    }
    #[must_use]
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
/// Directional evidence link between two source materials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMaterialLink {
    pub from_material_id: Uuid,
    pub to_material_id: Uuid,
    pub relation_type: String,
    pub metadata: JsonValue,
}

impl SourceMaterialLink {
    /// Create a source-material link with empty metadata.
    pub fn new(
        from_material_id: impl Into<Uuid>,
        to_material_id: impl Into<Uuid>,
        relation_type: impl Into<String>,
    ) -> Self {
        Self {
            from_material_id: from_material_id.into(),
            to_material_id: to_material_id.into(),
            relation_type: relation_type.into(),
            metadata: json!({}),
        }
    }

    /// Create a canonical `backed_by` evidence link.
    pub fn backed_by(from_material_id: impl Into<Uuid>, to_material_id: impl Into<Uuid>) -> Self {
        Self::new(from_material_id, to_material_id, relation_types::BACKED_BY)
    }

    /// Deep-merge additional metadata into this link payload.
    ///
    /// Existing keys are overwritten by `metadata` on conflict; nested objects
    /// are merged recursively (rather than replaced wholesale).
    #[must_use]
    pub fn with_metadata(mut self, metadata: JsonValue) -> Self {
        match (&mut self.metadata, metadata) {
            (JsonValue::Object(existing), JsonValue::Object(incoming)) => {
                merge_json_objects(existing, incoming);
            }
            (existing, incoming) => {
                *existing = incoming;
            }
        }
        self
    }
}

/// Recursively merge `incoming` into `target`, with incoming values winning on conflict.
fn merge_json_objects(target: &mut JsonMap<String, JsonValue>, incoming: JsonMap<String, JsonValue>) {
    for (key, incoming_val) in incoming {
        match (target.get_mut(&key), incoming_val) {
            (Some(JsonValue::Object(existing_obj)), JsonValue::Object(incoming_obj)) => {
                merge_json_objects(existing_obj, incoming_obj);
            }
            (existing_slot, incoming_val) => {
                if let Some(slot) = existing_slot {
                    *slot = incoming_val;
                } else {
                    target.insert(key, incoming_val);
                }
            }
        }
    }
}

/// Entry for the `raw.temporal_ledger` table.
///
/// Tracks timing metadata for source materials, including capture windows
/// and clock synchronization information.
#[derive(Debug, Clone)]
pub struct TemporalLedgerEntry {
    /// ID of the source material this entry refers to
    pub source_material_id: uuid::Uuid,
    /// Start offset within the source material
    pub offset_start: i64,
    /// End offset within the source material
    pub offset_end: i64,
    /// Offset kind for the recorded range.
    pub offset_kind: OffsetKind,
    /// Capture timestamp
    pub ts_capture: Timestamp,
    /// Precision of the capture timing
    pub precision: TemporalPrecision,
    /// Clock type used
    pub clock: TemporalClock,
    /// How the capture timestamp was determined
    pub source_type: TemporalSourceType,
}
impl TemporalLedgerEntry {
    /// Create a new ledger entry for a realtime capture
    #[must_use]
    pub fn realtime_capture(
        source_material_id: uuid::Uuid,
        offset_end: i64,
        ts_capture: Timestamp,
    ) -> Self {
        Self {
            source_material_id,
            offset_start: 0,
            offset_end,
            offset_kind: OffsetKind::Byte,
            ts_capture,
            precision: TemporalPrecision::Bounded,
            clock: TemporalClock::Wall,
            source_type: TemporalSourceType::RealtimeCapture,
        }
    }

    /// Create a `staged_at` ledger entry — the fallback timestamp for material
    /// events that lack an intrinsic or inferred timestamp from the content.
    ///
    /// Written at material-begin time so that `LedgerReader::derive_ts_orig()`
    /// always finds a persisted timestamp instead of falling back to an
    /// ephemeral `Timestamp::now()`. This makes material `ts_orig` reproducible
    /// across replays.
    #[must_use]
    pub fn staged_at(
        source_material_id: uuid::Uuid,
        offset_end: i64,
        ts_capture: Timestamp,
    ) -> Self {
        Self {
            source_material_id,
            offset_start: 0,
            offset_end,
            offset_kind: OffsetKind::Byte,
            ts_capture,
            precision: TemporalPrecision::Bounded,
            clock: TemporalClock::Wall,
            source_type: TemporalSourceType::StagedAt,
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
impl SourceMaterialRepository<'_> {
    fn validate_link(link: &SourceMaterialLink) -> DbResult<()> {
        if link.from_material_id == link.to_material_id {
            return Err(SinexError::validation(
                "source material links cannot point to the same material",
            )
            .with_context("material_id", link.from_material_id));
        }

        if !is_valid_relation_type(&link.relation_type) {
            return Err(SinexError::validation(
                "source material relation_type must match ^[a-z][a-z0-9_.-]*$",
            )
            .with_context("relation_type", link.relation_type.clone()));
        }

        Ok(())
    }

    /// Create or update a directional source-material evidence link.
    ///
    /// The natural key is `(from_material_id, to_material_id, relation_type)`;
    /// repeated calls preserve the original row identity and deep-merge metadata.
    pub async fn link_materials(
        &self,
        link: SourceMaterialLink,
    ) -> DbResult<SourceMaterialLinkRecord> {
        Self::validate_link(&link)?;

        sqlx::query_as::<_, SourceMaterialLinkRecord>(
            r"
            INSERT INTO raw.source_material_links (
                from_material_id,
                to_material_id,
                relation_type,
                metadata
            ) VALUES ($1::uuid, $2::uuid, $3, $4)
            ON CONFLICT (from_material_id, to_material_id, relation_type)
            DO UPDATE SET
                metadata = core.jsonb_merge_deep(raw.source_material_links.metadata, EXCLUDED.metadata)
            RETURNING
                id,
                from_material_id,
                to_material_id,
                relation_type,
                metadata,
                created_at
            ",
        )
        .bind(link.from_material_id)
        .bind(link.to_material_id)
        .bind(link.relation_type)
        .bind(link.metadata)
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "link source materials"))
    }

    /// Create or update a canonical `backed_by` evidence link.
    pub async fn link_backing_material(
        &self,
        from_material_id: impl Into<Uuid>,
        to_material_id: impl Into<Uuid>,
        metadata: JsonValue,
    ) -> DbResult<SourceMaterialLinkRecord> {
        self.link_materials(
            SourceMaterialLink::backed_by(from_material_id, to_material_id).with_metadata(metadata),
        )
        .await
    }

    /// List links where `material_id` is the source side.
    pub async fn links_from(
        &self,
        material_id: impl Into<Uuid>,
    ) -> DbResult<Vec<SourceMaterialLinkRecord>> {
        sqlx::query_as::<_, SourceMaterialLinkRecord>(
            r"
            SELECT
                id,
                from_material_id,
                to_material_id,
                relation_type,
                metadata,
                created_at
            FROM raw.source_material_links
            WHERE from_material_id = $1::uuid
            ORDER BY created_at ASC, id ASC
            ",
        )
        .bind(material_id.into())
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list source material links from material"))
    }

    /// List links where `material_id` is the evidence/target side.
    pub async fn links_to(
        &self,
        material_id: impl Into<Uuid>,
    ) -> DbResult<Vec<SourceMaterialLinkRecord>> {
        sqlx::query_as::<_, SourceMaterialLinkRecord>(
            r"
            SELECT
                id,
                from_material_id,
                to_material_id,
                relation_type,
                metadata,
                created_at
            FROM raw.source_material_links
            WHERE to_material_id = $1::uuid
            ORDER BY created_at ASC, id ASC
            ",
        )
        .bind(material_id.into())
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list source material links to material"))
    }

    /// List all source-material links touching any supplied material ID.
    pub async fn links_for_materials(
        &self,
        material_ids: &[Uuid],
    ) -> DbResult<Vec<SourceMaterialLinkRecord>> {
        if material_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, SourceMaterialLinkRecord>(
            r"
            SELECT
                id,
                from_material_id,
                to_material_id,
                relation_type,
                metadata,
                created_at
            FROM raw.source_material_links
            WHERE from_material_id = ANY($1::uuid[])
               OR to_material_id = ANY($1::uuid[])
            ORDER BY created_at ASC, id ASC
            ",
        )
        .bind(material_ids)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list source material links for materials"))
    }

    async fn update_material_state<'e, E>(
        &self,
        executor: E,
        id: Id<SourceMaterialRecord>,
        status: &str,
        blob_id: Option<Id<crate::Blob>>,
        metadata_update: JsonValue,
        total_bytes: Option<i64>,
    ) -> DbResult<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        sqlx::query!(
            r#"
            UPDATE raw.source_material_registry
            SET optional_blob_id = COALESCE($2::uuid, optional_blob_id),
                metadata = core.jsonb_merge_deep(metadata, $3),
                status = $4,
                end_time = COALESCE(end_time, NOW()),
                total_bytes = COALESCE($5, total_bytes)
            WHERE id = $1
            "#,
            id.to_uuid(),
            blob_id.map(|bid| bid.to_uuid()),
            metadata_update,
            status,
            total_bytes
        )
        .execute(executor)
        .await
        .map_err(|e| db_error(e, "update material state"))?;
        Ok(())
    }

    async fn insert_material_with_id(
        &self,
        id: Id<SourceMaterial>,
        material: SourceMaterial,
    ) -> DbResult<SourceMaterialRecord> {
        use crate::query_helpers::{
            IdempotentTransaction, RetryConfig, set_repeatable_read,
            with_retry_transaction_idempotent,
        };

        let material = material.clone();
        with_retry_transaction_idempotent(
            self.pool,
            RetryConfig::default(),
            IdempotentTransaction::new(),
            move |tx| {
                let id = id;
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
                            $1::uuid,
                            $2,
                            $3,
                            $4,
                            $5,
                            $6,
                            $7,
                            $8,
                            $9,
                            $10,
                            $11::uuid
                        )
                        RETURNING
                            id as "id!: Uuid",
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
                            optional_blob_id as "optional_blob_id: Uuid",
                            total_bytes as "total_bytes?: i64"
                        "#,
                        id.to_uuid(),
                        material.material_kind,
                        material.source_identifier,
                        material.status,
                        material.timing_info_type,
                        material.metadata,
                        start_time_offset.map(|t| *t),
                        end_time_offset.map(|t| *t),
                        material.staged_by,
                        material.staged_on_host,
                        material.optional_blob_id.map(|id| id.to_uuid())
                    )
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(|e| db_error(e, "register material"))
                })
            },
        )
        .await
    }

    /// Register a new source material
    pub async fn register_material(
        &self,
        material: SourceMaterial,
    ) -> DbResult<SourceMaterialRecord> {
        let id = Id::<SourceMaterial>::new();
        self.insert_material_with_id(id, material).await
    }

    /// Register a completed source material with a caller-provided identifier.
    ///
    /// This is used by ingress surfaces that must choose the material ID before
    /// the event is published so provenance can reference an already-registered row.
    pub async fn register_external_material(
        &self,
        material_id: uuid::Uuid,
        material: SourceMaterial,
    ) -> DbResult<SourceMaterialRecord> {
        self.insert_material_with_id(Id::<SourceMaterial>::from_uuid(material_id), material)
            .await
    }
    /// Get source material by ID
    pub async fn get_by_id(
        &self,
        id: Id<SourceMaterialRecord>,
    ) -> DbResult<Option<SourceMaterialRecord>> {
        self.get_by_id_with_executor(self.pool, id).await
    }

    pub async fn get_by_id_with_executor<'e, E>(
        &self,
        executor: E,
        id: Id<SourceMaterialRecord>,
    ) -> DbResult<Option<SourceMaterialRecord>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id as "id!: uuid::Uuid",
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
                optional_blob_id as "optional_blob_id?: uuid::Uuid",
                total_bytes as "total_bytes?: i64"
            FROM raw.source_material_registry
            WHERE id = $1
            "#,
            id.to_uuid()
        )
        .fetch_optional(executor)
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
                id as "id!: uuid::Uuid",
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
                optional_blob_id as "optional_blob_id?: uuid::Uuid",
                total_bytes as "total_bytes?: i64"
            FROM raw.source_material_registry
            WHERE optional_blob_id = $1
            "#,
            blob_id.to_uuid()
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
                id as "id!: uuid::Uuid",
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
                optional_blob_id as "optional_blob_id?: uuid::Uuid",
                total_bytes as "total_bytes?: i64"
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
    /// Get recent materials filtered by `material_kind`, ordered by staged time.
    ///
    /// When `material_kind` is Some, the filter is pushed to SQL (indexed column)
    /// rather than filtering in application code.
    pub async fn get_recent_by_kind(
        &self,
        material_kind: Option<&str>,
        limit: i64,
    ) -> DbResult<Vec<SourceMaterialRecord>> {
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            SELECT
                id as "id!: uuid::Uuid",
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
                optional_blob_id as "optional_blob_id?: uuid::Uuid",
                total_bytes as "total_bytes?: i64"
            FROM raw.source_material_registry
            WHERE ($2::text IS NULL OR material_kind = $2)
            ORDER BY staged_at DESC
            LIMIT $1
            "#,
            limit,
            material_kind
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "get recent materials by kind"))
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
                id as "id!: uuid::Uuid",
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
                optional_blob_id as "optional_blob_id?: uuid::Uuid",
                total_bytes as "total_bytes?: i64"
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
            WHERE id = $1
            "#,
            id.to_uuid()
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
                id as "id!: uuid::Uuid",
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
                optional_blob_id as "optional_blob_id?: uuid::Uuid",
                total_bytes as "total_bytes?: i64"
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
        self.update_metadata_with_executor(self.pool, id, metadata)
            .await
    }

    pub async fn update_metadata_with_executor<'e, E>(
        &self,
        executor: E,
        id: Id<SourceMaterialRecord>,
        metadata: JsonValue,
    ) -> DbResult<Option<SourceMaterialRecord>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        // Strip caller-supplied values for reserved system keys before merging,
        // then re-apply existing system values on top so they always win.
        // Reserved keys are set exclusively by the DB/SDK internals and must not
        // be overwritten by caller-supplied metadata.
        let caller_metadata = strip_reserved_metadata_keys(metadata);
        sqlx::query_as!(
            SourceMaterialRecord,
            r#"
            UPDATE raw.source_material_registry
            SET metadata = core.jsonb_merge_deep(
                    core.jsonb_merge_deep(metadata, $2),
                    -- Re-apply existing reserved keys on top so system wins.
                    (SELECT jsonb_object_agg(k, metadata->k)
                     FROM unnest($3::text[]) AS k
                     WHERE metadata ? k)
                )
            WHERE id = $1
            RETURNING
                id as "id!: uuid::Uuid",
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
                optional_blob_id as "optional_blob_id: Uuid",
                total_bytes as "total_bytes?: i64"
            "#,
            id.to_uuid(),
            caller_metadata,
            RESERVED_METADATA_KEYS as &[&str],
        )
        .fetch_optional(executor)
        .await
        .map_err(|e| db_error(e, "update material metadata"))
    }
    /// Register an in-flight source material using an atomic upsert.
    ///
    /// This uses INSERT ON CONFLICT to avoid serialization failures that occur
    /// with the previous UPDATE-then-INSERT pattern under REPEATABLE READ isolation.
    /// The upsert is atomic and idempotent, making it safe for concurrent calls
    /// from multiple material assembler threads.
    ///
    /// # Conflict Resolution
    ///
    /// The table has a unique constraint on `source_identifier`, making it the natural key.
    /// On conflict (same `source_identifier)`:
    /// - The existing row is updated (id is preserved)
    /// - The row is reset to the active sensing state for the restarted import
    /// - Metadata is deep-merged with new values
    /// - `end_time` is cleared so terminal state does not leak into the rerun
    /// - `staged_by` and `staged_on_host` are updated if not null
    async fn register_in_flight_by_source_identifier_with_executor<'e, E>(
        &self,
        executor: E,
        id: Id<SourceMaterial>,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
        start_time_override: Option<Timestamp>,
    ) -> DbResult<SourceMaterialRecord>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        // Build the material struct for metadata preparation
        let mut material =
            SourceMaterial::new(material_kinds::ANNEX, source_uri.unwrap_or("in-flight"));
        material.status = status::SENSING.to_string();
        material.timing_info_type = timing_info_types::REALTIME.to_string();
        material.merge_metadata(metadata);
        if let Some(uri) = source_uri {
            material
                .metadata_object_mut()
                .insert("source_uri".to_string(), JsonValue::String(uri.to_string()));
        }
        material.metadata_object_mut().insert(
            "material_type".to_string(),
            JsonValue::String(material_type.to_string()),
        );
        let start_time = start_time_override
            .or(material.start_time)
            .unwrap_or_else(Timestamp::now);
        material.start_time = Some(start_time);
        material.staged_by = Some("sinex-db".to_string());
        material.staged_on_host = Some(gethostname::gethostname().to_string_lossy().to_string());

        // Atomic upsert: INSERT with ON CONFLICT DO UPDATE
        // This avoids serialization failures from REPEATABLE READ isolation
        // NOTE: source_identifier is the natural key (unique constraint), not id
        let upsert_sql = r"
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
                $1::uuid,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9
            )
            ON CONFLICT (source_identifier) DO UPDATE SET
                status = EXCLUDED.status,
                -- Deep merge metadata, preserving existing values
                metadata = core.jsonb_merge_deep(raw.source_material_registry.metadata, EXCLUDED.metadata),
                start_time = COALESCE(EXCLUDED.start_time, raw.source_material_registry.start_time),
                end_time = NULL,
                -- Update staging info
                staged_by = COALESCE(EXCLUDED.staged_by, raw.source_material_registry.staged_by),
                staged_on_host = COALESCE(EXCLUDED.staged_on_host, raw.source_material_registry.staged_on_host)
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
                optional_blob_id::uuid as optional_blob_id,
                total_bytes
        ";

        sqlx::query_as::<_, SourceMaterialRecord>(upsert_sql)
            .bind(id.to_uuid())
            .bind(&material.material_kind)
            .bind(&material.source_identifier)
            .bind(&material.status)
            .bind(&material.timing_info_type)
            .bind(&material.metadata)
            .bind(material.start_time)
            .bind(&material.staged_by)
            .bind(&material.staged_on_host)
            .fetch_one(executor)
            .await
            .map_err(|e| db_error(e, "upsert in-flight source material"))
    }

    /// External registrations carry an explicit material id that downstream slices,
    /// end markers, and ledger entries all reference directly. Reusing a
    /// `source_identifier` with a different explicit id is therefore invalid and
    /// must fail honestly rather than silently aliasing the new material onto an
    /// older row.
    async fn register_external_in_flight_by_id_with_executor<'e, E>(
        &self,
        executor: E,
        id: Id<SourceMaterial>,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
        start_time_override: Option<Timestamp>,
    ) -> DbResult<SourceMaterialRecord>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let mut material =
            SourceMaterial::new(material_kinds::ANNEX, source_uri.unwrap_or("in-flight"));
        material.status = status::SENSING.to_string();
        material.timing_info_type = timing_info_types::REALTIME.to_string();
        material.merge_metadata(metadata);
        if let Some(uri) = source_uri {
            material
                .metadata_object_mut()
                .insert("source_uri".to_string(), JsonValue::String(uri.to_string()));
        }
        material.metadata_object_mut().insert(
            "material_type".to_string(),
            JsonValue::String(material_type.to_string()),
        );
        let start_time = start_time_override
            .or(material.start_time)
            .unwrap_or_else(Timestamp::now);
        material.start_time = Some(start_time);
        material.staged_by = Some("sinex-db".to_string());
        material.staged_on_host = Some(gethostname::gethostname().to_string_lossy().to_string());

        let upsert_sql = r"
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
                $1::uuid,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9
            )
            ON CONFLICT (id) DO UPDATE SET
                material_kind = EXCLUDED.material_kind,
                source_identifier = EXCLUDED.source_identifier,
                status = EXCLUDED.status,
                timing_info_type = EXCLUDED.timing_info_type,
                metadata = core.jsonb_merge_deep(raw.source_material_registry.metadata, EXCLUDED.metadata),
                start_time = COALESCE(EXCLUDED.start_time, raw.source_material_registry.start_time),
                end_time = NULL,
                staged_by = COALESCE(EXCLUDED.staged_by, raw.source_material_registry.staged_by),
                staged_on_host = COALESCE(EXCLUDED.staged_on_host, raw.source_material_registry.staged_on_host)
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
                optional_blob_id::uuid as optional_blob_id,
                total_bytes
        ";

        sqlx::query_as::<_, SourceMaterialRecord>(upsert_sql)
            .bind(id.to_uuid())
            .bind(&material.material_kind)
            .bind(&material.source_identifier)
            .bind(&material.status)
            .bind(&material.timing_info_type)
            .bind(&material.metadata)
            .bind(material.start_time)
            .bind(&material.staged_by)
            .bind(&material.staged_on_host)
            .fetch_one(executor)
            .await
            .map_err(|e| db_error(e, "upsert external in-flight source material"))
    }
    pub async fn register_in_flight(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
    ) -> DbResult<SourceMaterialRecord> {
        let id = Id::<SourceMaterial>::new();
        self.register_in_flight_by_source_identifier_with_executor(
            self.pool,
            id,
            material_type,
            source_uri,
            metadata,
            None,
        )
        .await
    }
    pub async fn register_external_in_flight(
        &self,
        material_id: uuid::Uuid,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
        started_at: Timestamp,
    ) -> DbResult<SourceMaterialRecord> {
        let id = Id::<SourceMaterial>::from_uuid(material_id);
        self.register_external_in_flight_by_id_with_executor(
            self.pool,
            id,
            material_type,
            source_uri,
            metadata,
            Some(started_at),
        )
        .await
    }

    pub async fn register_external_in_flight_with_executor<'e, E>(
        &self,
        executor: E,
        material_id: uuid::Uuid,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: JsonValue,
        started_at: Timestamp,
    ) -> DbResult<SourceMaterialRecord>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let id = Id::<SourceMaterial>::from_uuid(material_id);
        self.register_external_in_flight_by_id_with_executor(
            executor,
            id,
            material_type,
            source_uri,
            metadata,
            Some(started_at),
        )
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
                    #[allow(clippy::expect_used)]
                    // RFC3339 formatting can't fail on valid timestamps
                    Timestamp::now()
                        .format(&format_description::well_known::Rfc3339)
                        .expect("RFC3339 format always valid for timestamps"),
                ),
            );
            JsonValue::Object(map)
        };
        self.update_material_state(self.pool, id, status::FAILED, None, metadata_update, None)
            .await
    }

    /// Mark an in-flight source material as partially recovered.
    pub async fn mark_as_recovered_partial(
        &self,
        id: Id<SourceMaterialRecord>,
        recovery_reason: &str,
        metadata_update: JsonValue,
    ) -> DbResult<()> {
        let mut update = serde_json::Map::new();
        update.insert(
            "recovery_info".to_string(),
            json!({
                "recovered_at": Timestamp::now(),
                "recovery_reason": recovery_reason,
                "original_status": status::SENSING,
            }),
        );
        match metadata_update {
            JsonValue::Object(extra) => {
                for (key, value) in extra {
                    update.insert(key, value);
                }
            }
            JsonValue::Null => {}
            other => {
                update.insert("_meta".to_string(), other);
            }
        }
        self.update_material_state(
            self.pool,
            id,
            status::RECOVERED_PARTIAL,
            None,
            JsonValue::Object(update),
            None,
        )
        .await
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
        self.finalize_in_flight_as(
            self.pool,
            id,
            status::COMPLETED,
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
        self.finalize_in_flight_as(
            executor,
            id,
            status::COMPLETED,
            blob_id,
            encoding,
            content_preview,
            total_bytes,
        )
        .await
    }

    /// Finalize in-flight source material with an explicit terminal status.
    pub async fn finalize_in_flight_as<'e, E>(
        &self,
        executor: E,
        id: Id<SourceMaterialRecord>,
        final_status: &str,
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
        self.update_material_state(
            executor,
            id,
            final_status,
            blob_id,
            metadata_update,
            total_bytes,
        )
        .await
    }
    // ========== Temporal Ledger ==========
    /// Append an entry to the temporal ledger for a source material.
    ///
    /// The temporal ledger tracks timing metadata for captures, including
    /// offset ranges, capture timestamps, and clock information.
    pub async fn append_temporal_ledger(&self, entry: TemporalLedgerEntry) -> DbResult<()> {
        self.append_temporal_ledger_with_executor(self.pool, entry)
            .await
    }

    pub async fn append_temporal_ledger_with_executor<'e, E>(
        &self,
        executor: E,
        entry: TemporalLedgerEntry,
    ) -> DbResult<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        sqlx::query!(
            r#"
            INSERT INTO raw.temporal_ledger
                (source_material_id, offset_start, offset_end, offset_kind, ts_capture, precision, clock, source_type)
            VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)
            "#,
            entry.source_material_id,
            entry.offset_start,
            entry.offset_end,
            entry.offset_kind.as_wire_str(),
            *entry.ts_capture,
            entry.precision.to_string() as String,
            entry.clock.to_string() as String,
            entry.source_type.to_string() as String
        )
        .execute(executor)
        .await
        .map_err(|e| db_error(e, "append temporal ledger entry"))?;
        Ok(())
    }
}

fn is_valid_relation_type(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    first.is_ascii_lowercase()
        && chars.all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '.' | '-')
        })
}
/// Extension trait for `SourceMaterial` terminal methods
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

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn realtime_capture_uses_typed_byte_offset_kind() -> ::xtask::sandbox::TestResult<()> {
        let entry =
            TemporalLedgerEntry::realtime_capture(uuid::Uuid::now_v7(), 42, Timestamp::now());

        assert_eq!(entry.offset_kind, OffsetKind::Byte);
        Ok(())
    }
}
