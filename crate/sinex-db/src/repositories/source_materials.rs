//! Source material repository for managing raw data sources
//!
//! This repository handles registration and tracking of source materials
//! (files, streams, etc.) that contain events to be processed.
use super::common::{DbResult, EnhancedRepository, Repository, db_error};
use crate::schema::SourceMaterialRegistry;
pub use crate::schema::defs::records::SourceMaterialLinkRecord;
use crate::schema::defs::records::SourceMaterialRecord;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use sinex_primitives::domain::{
    MaterialStatus, SourceMaterialTimingInfoType, TemporalClock, TemporalPrecision,
    TemporalSourceType,
};
use sinex_primitives::rpc::sources::{
    CaveatSeverity, SourceCaveat, SourceMaterialMetadataContract, SourceReadiness,
    SourceReadinessCost, SourceReadinessStatus, caveat_codes,
};
use sinex_primitives::{Id, SinexError, Timestamp, events::OffsetKind};
use sqlx::PgPool;
use time::format_description;
use uuid::Uuid;

mod helpers;
mod types;

use helpers::{
    RESERVED_METADATA_KEYS, contract_for_source, derive_source_family, format_for_material_type,
    is_valid_relation_type, redact_identifier_for_display, strip_reserved_metadata_keys,
};
pub use types::{
    SourceMaterial, SourceMaterialLink, TemporalLedgerEntry, material_kinds, material_types,
    relation_types, timing_info_types,
};

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
        status: MaterialStatus,
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
                metadata = CASE
                    WHEN $5::bigint IS NOT NULL
                         AND metadata ? 'source_material_contract'
                         AND jsonb_typeof(metadata->'source_material_contract') = 'object'
                         AND metadata->'source_material_contract'->>'version' = '1'
                         AND metadata->'source_material_contract'->>'format' IN (
                            'json', 'jsonl', 'sqlite', 'markdown', 'text', 'csv',
                            'tsv', 'html', 'pdf', 'directory', 'repository', 'image',
                            'audio', 'video', 'archive', 'binary', 'unknown'
                         )
                         AND metadata->'source_material_contract'->>'timing' IN (
                            'realtime', 'intrinsic', 'inferred', 'declared',
                            'atemporal', 'staged_at'
                         )
                    THEN core.jsonb_merge_deep(
                        core.jsonb_merge_deep(metadata, $3),
                        jsonb_build_object(
                            'source_material_contract',
                            jsonb_build_object(
                                'version', 1,
                                'statistics', jsonb_build_object('total_bytes', $5::bigint)
                            )
                        )
                    )
                    ELSE core.jsonb_merge_deep(metadata, $3)
                END,
                status = $4,
                end_time = COALESCE(end_time, NOW()),
                total_bytes = COALESCE($5, total_bytes)
            WHERE id = $1
            "#,
            id.to_uuid(),
            blob_id.map(|bid| bid.to_uuid()),
            metadata_update,
            status.as_str(),
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
                            total_bytes as "total_bytes?: i64",
                coverage_contract as "coverage_contract!: JsonValue",
                privacy_class as "privacy_class!: String"
                        "#,
                        id.to_uuid(),
                        material.material_kind.as_str(),
                        material.source_identifier,
                        material.status.as_str(),
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
                total_bytes as "total_bytes?: i64",
                coverage_contract as "coverage_contract!: JsonValue",
                privacy_class as "privacy_class!: String"
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
                total_bytes as "total_bytes?: i64",
                coverage_contract as "coverage_contract!: JsonValue",
                privacy_class as "privacy_class!: String"
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
                total_bytes as "total_bytes?: i64",
                coverage_contract as "coverage_contract!: JsonValue",
                privacy_class as "privacy_class!: String"
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
    /// Get recent materials filtered by storage/backend `material_kind`,
    /// ordered by staged time.
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
                total_bytes as "total_bytes?: i64",
                coverage_contract as "coverage_contract!: JsonValue",
                privacy_class as "privacy_class!: String"
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
                total_bytes as "total_bytes?: i64",
                coverage_contract as "coverage_contract!: JsonValue",
                privacy_class as "privacy_class!: String"
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
                total_bytes as "total_bytes?: i64",
                coverage_contract as "coverage_contract!: JsonValue",
                privacy_class as "privacy_class!: String"
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
                total_bytes as "total_bytes?: i64",
                coverage_contract as "coverage_contract!: JsonValue",
                privacy_class as "privacy_class!: String"
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
        material.status = MaterialStatus::Sensing;
        material.timing_info_type = timing_info_types::REALTIME.to_string();
        let caller_contract = SourceMaterialMetadataContract::from_metadata(&metadata);
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
        let contract_is_explicit = caller_contract.is_some();
        if let Some(contract) = caller_contract {
            material.timing_info_type = contract.timing.to_string();
        } else {
            let contract = contract_for_source(
                format_for_material_type(material_type, source_uri),
                SourceMaterialTimingInfoType::Realtime,
                source_uri,
                None,
            );
            material.merge_metadata(contract.metadata_patch());
        }
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
                timing_info_type = CASE
                    WHEN NOT $10::boolean
                         AND raw.source_material_registry.metadata ? 'source_material_contract'
                    THEN raw.source_material_registry.timing_info_type
                    ELSE EXCLUDED.timing_info_type
                END,
                -- Deep merge metadata, preserving existing values
                metadata = CASE
                    WHEN NOT $10::boolean
                         AND raw.source_material_registry.metadata ? 'source_material_contract'
                    THEN jsonb_set(
                        core.jsonb_merge_deep(
                            raw.source_material_registry.metadata,
                            EXCLUDED.metadata
                        ),
                        '{source_material_contract}',
                        raw.source_material_registry.metadata->'source_material_contract',
                        true
                    )
                    ELSE core.jsonb_merge_deep(
                        raw.source_material_registry.metadata,
                        EXCLUDED.metadata
                    )
                END,
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
                total_bytes,
                coverage_contract,
                privacy_class
        ";

        sqlx::query_as::<_, SourceMaterialRecord>(upsert_sql)
            .bind(id.to_uuid())
            .bind(material.material_kind.as_str())
            .bind(&material.source_identifier)
            .bind(material.status)
            .bind(&material.timing_info_type)
            .bind(&material.metadata)
            .bind(material.start_time)
            .bind(&material.staged_by)
            .bind(&material.staged_on_host)
            .bind(contract_is_explicit)
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
        material.status = MaterialStatus::Sensing;
        material.timing_info_type = timing_info_types::REALTIME.to_string();
        let caller_contract = SourceMaterialMetadataContract::from_metadata(&metadata);
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
        let contract_is_explicit = caller_contract.is_some();
        if let Some(contract) = caller_contract {
            material.timing_info_type = contract.timing.to_string();
        } else {
            let contract = contract_for_source(
                format_for_material_type(material_type, source_uri),
                SourceMaterialTimingInfoType::Realtime,
                source_uri,
                None,
            );
            material.merge_metadata(contract.metadata_patch());
        }
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
                timing_info_type = CASE
                    WHEN NOT $10::boolean
                         AND raw.source_material_registry.metadata ? 'source_material_contract'
                    THEN raw.source_material_registry.timing_info_type
                    ELSE EXCLUDED.timing_info_type
                END,
                metadata = CASE
                    WHEN NOT $10::boolean
                         AND raw.source_material_registry.metadata ? 'source_material_contract'
                    THEN jsonb_set(
                        core.jsonb_merge_deep(
                            raw.source_material_registry.metadata,
                            EXCLUDED.metadata
                        ),
                        '{source_material_contract}',
                        raw.source_material_registry.metadata->'source_material_contract',
                        true
                    )
                    ELSE core.jsonb_merge_deep(
                        raw.source_material_registry.metadata,
                        EXCLUDED.metadata
                    )
                END,
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
                total_bytes,
                coverage_contract,
                privacy_class
        ";

        sqlx::query_as::<_, SourceMaterialRecord>(upsert_sql)
            .bind(id.to_uuid())
            .bind(material.material_kind.as_str())
            .bind(&material.source_identifier)
            .bind(material.status)
            .bind(&material.timing_info_type)
            .bind(&material.metadata)
            .bind(material.start_time)
            .bind(&material.staged_by)
            .bind(&material.staged_on_host)
            .bind(contract_is_explicit)
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
        self.update_material_state(
            self.pool,
            id,
            MaterialStatus::Failed,
            None,
            metadata_update,
            None,
        )
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
                "original_status": MaterialStatus::Sensing.as_str(),
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
            MaterialStatus::RecoveredPartial,
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
            MaterialStatus::Completed,
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
            MaterialStatus::Completed,
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
        final_status: MaterialStatus,
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

    // ========== Source readiness (#1099) ==========

    /// List readiness reports for every source observed in the registry.
    ///
    /// Sources are grouped at `source_identifier` granularity; counts and the
    /// worst-case status are aggregated across all storage/backend
    /// `material_kind` values for the same identifier so callers cannot get a
    /// healthy storage kind silently masking a stale or failed kind. The kind
    /// list is preserved in
    /// `evidence.material_kinds` for diagnostics. The derivation runs purely
    /// from `raw.source_material_registry` and `core.events`; source-binding
    /// declarations live in Nix/config manifests and parser/source-worker run
    /// evidence is owned by operation/debt surfaces. Caveats record those
    /// join boundaries so readiness does not pretend to be a full job monitor.
    ///
    /// `stale_after_seconds` controls when a recent-success source flips to
    /// `Stale`. Defaults to 7 days when `None`.
    pub async fn list_source_readiness(
        &self,
        source_family: Option<&str>,
        stale_after_seconds: Option<i64>,
    ) -> DbResult<Vec<SourceReadiness>> {
        self.readiness_query(None, source_family, stale_after_seconds)
            .await
    }

    /// Internal: build readiness rows. If `only_identifier` is `Some`, the
    /// SQL filters at WHERE-clause granularity on the canonical raw
    /// identifier — used by [`get_source_readiness`] to avoid the
    /// display-redaction match ambiguity.
    async fn readiness_query(
        &self,
        only_identifier: Option<&str>,
        source_family: Option<&str>,
        stale_after_seconds: Option<i64>,
    ) -> DbResult<Vec<SourceReadiness>> {
        let stale_after = stale_after_seconds.unwrap_or(7 * 24 * 3600);

        let rows = sqlx::query!(
            r#"
            SELECT
                sm.source_identifier        AS "source_identifier!",
                ARRAY_AGG(DISTINCT sm.material_kind ORDER BY sm.material_kind)
                                            AS "material_kinds!: Vec<String>",
                COUNT(*)                    AS "material_count!",
                COUNT(*) FILTER (WHERE sm.status = 'completed')         AS "completed_count!",
                COUNT(*) FILTER (WHERE sm.status = 'sensing')           AS "sensing_count!",
                COUNT(*) FILTER (WHERE sm.status = 'failed')            AS "failed_count!",
                COUNT(*) FILTER (WHERE sm.status = 'cancelled')         AS "cancelled_count!",
                COUNT(*) FILTER (WHERE sm.status = 'recovered_partial') AS "partial_count!",
                MAX(sm.staged_at) FILTER (WHERE sm.status = 'completed') AS "last_success_at: time::OffsetDateTime"
            FROM raw.source_material_registry sm
            WHERE $1::text IS NULL OR sm.source_identifier = $1
            GROUP BY sm.source_identifier
            ORDER BY sm.source_identifier
            "#,
            only_identifier,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list source readiness"))?;

        let now = time::OffsetDateTime::now_utc();
        let mut out = Vec::with_capacity(rows.len());

        for row in rows {
            // For family classification, prefer the most-specific kind we saw;
            // sorted ascending, the last element is the alphabetically-greatest
            // kind, which is fine as a stable tiebreaker. Family is advisory.
            let representative_kind = row.material_kinds.last().map_or("", String::as_str);
            let family = derive_source_family(&row.source_identifier, representative_kind);
            if let Some(filter) = source_family
                && family != filter
            {
                continue;
            }

            // Parsed-event count: count events referencing any material from
            // this source identifier across ALL material_kinds — matches the
            // identifier-granular aggregation above.
            let parsed_event_count = sqlx::query_scalar!(
                r#"
                SELECT COUNT(*)::BIGINT AS "count!"
                FROM core.events e
                WHERE e.source_material_id IN (
                    SELECT id FROM raw.source_material_registry
                    WHERE source_identifier = $1
                )
                "#,
                row.source_identifier,
            )
            .fetch_one(self.pool)
            .await
            .map_err(|e| db_error(e, "count parsed events for readiness"))?;

            let freshness_seconds = row.last_success_at.map(|ts| (now - ts).whole_seconds());

            let display_identifier = redact_identifier_for_display(&row.source_identifier);

            let mut caveats = Vec::new();
            // Bindings live in Nix configuration; surface as info caveat.
            caveats.push(SourceCaveat {
                code: caveat_codes::BINDINGS_NOT_IN_DB.to_string(),
                severity: CaveatSeverity::Info,
                message:
                    "Source bindings live in Nix configuration; binding-level evidence is not joined here."
                        .to_string(),
                evidence_ref: None,
            });
            caveats.push(SourceCaveat {
                code: caveat_codes::PARSER_OPERATION_EVIDENCE_UNJOINED.to_string(),
                severity: CaveatSeverity::Info,
                message:
                    "Readiness is derived from raw material and admitted events; parser/source-worker operation evidence is reported by operation and debt surfaces."
                        .to_string(),
                evidence_ref: None,
            });

            // Status classification.
            let status = if row.completed_count == 0 && row.failed_count > 0 {
                caveats.push(SourceCaveat {
                    code: caveat_codes::PARSER_FAILED_RECENTLY.to_string(),
                    severity: CaveatSeverity::Degraded,
                    message: format!(
                        "{} material(s) in failed state and no completed staging recorded.",
                        row.failed_count
                    ),
                    evidence_ref: None,
                });
                SourceReadinessStatus::Error
            } else if row.completed_count == 0 && row.material_count > 0 {
                caveats.push(SourceCaveat {
                    code: caveat_codes::MATERIAL_STAGED_UNPARSED.to_string(),
                    severity: CaveatSeverity::Degraded,
                    message: "Material is registered but no successful staging has finalized."
                        .to_string(),
                    evidence_ref: None,
                });
                SourceReadinessStatus::Partial
            } else if parsed_event_count == 0 && row.completed_count > 0 {
                caveats.push(SourceCaveat {
                    code: caveat_codes::MATERIAL_STAGED_UNPARSED.to_string(),
                    severity: CaveatSeverity::Degraded,
                    message: "Material is staged but no parsed events reference it.".to_string(),
                    evidence_ref: None,
                });
                SourceReadinessStatus::Partial
            } else if let Some(secs) = freshness_seconds {
                if secs > stale_after {
                    caveats.push(SourceCaveat {
                        code: caveat_codes::MATERIAL_NO_RECENT_SNAPSHOT.to_string(),
                        severity: CaveatSeverity::Warning,
                        message: format!(
                            "Last successful staging was {secs}s ago, exceeding stale threshold {stale_after}s."
                        ),
                        evidence_ref: None,
                    });
                    SourceReadinessStatus::Stale
                } else {
                    SourceReadinessStatus::Available
                }
            } else {
                SourceReadinessStatus::Unknown
            };

            // Cost: registry-only data is local and cheap. Replay/refresh of an
            // archive_kind material would be local-heavy; we don't have that
            // signal here so we report local_fast and let consumers escalate.
            let cost = SourceReadinessCost::LocalFast;

            let evidence = serde_json::json!({
                "material_kinds": row.material_kinds,
                "material_count": row.material_count,
                "completed_count": row.completed_count,
                "sensing_count": row.sensing_count,
                "failed_count": row.failed_count,
                "cancelled_count": row.cancelled_count,
                "recovered_partial_count": row.partial_count,
            });

            out.push(SourceReadiness {
                binding_id: None,
                source_family: family.into(),
                source_id: None,
                parser_id: None,
                source_identifier: display_identifier,
                status,
                cost,
                freshness_seconds,
                #[allow(clippy::cast_sign_loss)]
                material_count: row.material_count.max(0) as u64,
                #[allow(clippy::cast_sign_loss)]
                parsed_event_count: Some(parsed_event_count.max(0) as u64),
                last_success_at: row.last_success_at.map(|ts| ts.to_string()),
                caveats,
                evidence,
            });
        }

        Ok(out)
    }

    /// Get the readiness report for a single source identifier.
    ///
    /// `source_identifier` is the canonical raw identifier stored in
    /// `raw.source_material_registry` and is matched at SQL granularity, NOT
    /// against the redacted display form. Multiple raw identifiers can
    /// collapse to the same redacted display string, so display-form
    /// matching would silently return the wrong source on collision.
    /// Redaction is applied only when populating the response struct.
    ///
    /// Returns `Ok(None)` when no material is registered for that identifier.
    pub async fn get_source_readiness(
        &self,
        source_identifier: &str,
        source_family: Option<&str>,
        stale_after_seconds: Option<i64>,
    ) -> DbResult<Option<SourceReadiness>> {
        let mut rows = self
            .readiness_query(Some(source_identifier), source_family, stale_after_seconds)
            .await?;
        Ok(rows.pop())
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

    /// Read all temporal-ledger entries for a material.
    ///
    /// Used by the event_engine admission stage to resolve `ts_orig` for material
    /// events whose timing was deferred to persistence (#1570 Prong B). Entries
    /// describe sub-material offset ranges (wrapped streams / per-chunk timing).
    pub async fn read_temporal_ledger(
        &self,
        material_id: Uuid,
    ) -> DbResult<Vec<TemporalLedgerEntry>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                offset_start,
                offset_end,
                offset_kind,
                ts_capture as "ts_capture: Timestamp",
                precision,
                clock,
                source_type
            FROM raw.temporal_ledger
            WHERE source_material_id = $1::uuid
            "#,
            material_id
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "read temporal ledger"))?;

        rows.into_iter()
            .map(|row| {
                Ok(TemporalLedgerEntry {
                    source_material_id: material_id,
                    offset_start: row.offset_start,
                    offset_end: row.offset_end,
                    offset_kind: OffsetKind::try_from_wire_str(&row.offset_kind)?,
                    ts_capture: row.ts_capture,
                    precision: row.precision.parse::<TemporalPrecision>().map_err(|e| {
                        SinexError::database(format!("invalid temporal_ledger precision: {e}"))
                    })?,
                    clock: row.clock.parse::<TemporalClock>().map_err(|e| {
                        SinexError::database(format!("invalid temporal_ledger clock: {e}"))
                    })?,
                    source_type: row.source_type.parse::<TemporalSourceType>().map_err(|e| {
                        SinexError::database(format!("invalid temporal_ledger source_type: {e}"))
                    })?,
                })
            })
            .collect()
    }

    // -------------------------------------------------------------------------
    // Tombstone-driven cleanup (#987 delete-on-tombstone)
    // -------------------------------------------------------------------------

    /// Collect distinct `source_material_id` values referenced by a set of
    /// archived event IDs (in `audit.archived_events`).
    ///
    /// Used by the tombstone path to capture candidate materials for orphan
    /// detection BEFORE `execute_cascade_tombstone` deletes the archived rows.
    pub async fn material_ids_for_archived_events(
        &self,
        archived_event_ids: &[Uuid],
    ) -> DbResult<Vec<Uuid>> {
        if archived_event_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = sqlx::query_scalar!(
            r#"
            SELECT DISTINCT source_material_id AS "id!: Uuid"
            FROM audit.archived_events
            WHERE id = ANY($1::uuid[])
              AND source_material_id IS NOT NULL
            "#,
            archived_event_ids
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "material_ids_for_archived_events"))?;
        Ok(ids)
    }

    /// Filter a set of material IDs to those with no remaining references in
    /// `core.events` or `audit.archived_events`.
    ///
    /// Returns IDs that are safe to delete from the registry — no live or
    /// archived event still claims this material as its provenance root.
    /// Tombstones (`core.event_tombstones`) carry only metadata, not
    /// `source_material_id`, so they don't keep materials alive.
    pub async fn find_orphan_materials(&self, candidate_ids: &[Uuid]) -> DbResult<Vec<Uuid>> {
        if candidate_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = sqlx::query_scalar!(
            r#"
            SELECT id AS "id!: Uuid"
            FROM unnest($1::uuid[]) AS candidates(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM core.events e
                WHERE e.source_material_id = candidates.id
            )
              AND NOT EXISTS (
                SELECT 1 FROM audit.archived_events ae
                WHERE ae.source_material_id = candidates.id
            )
            "#,
            candidate_ids
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find_orphan_materials"))?;
        Ok(ids)
    }

    /// Delete a source material registry row by ID. Returns `true` if a row was
    /// actually removed. Caller is responsible for dropping the associated
    /// blob from the content store separately — this only removes the DB row.
    pub async fn delete_material(&self, id: Id<SourceMaterialRecord>) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            DELETE FROM raw.source_material_registry
            WHERE id = $1
            "#,
            id.to_uuid()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete material"))?;
        Ok(result.rows_affected() > 0)
    }
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
