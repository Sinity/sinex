//! Source Material query registry for external data provenance
//!
//! This module provides all database queries related to source material tracking,
//! replacing the legacy artifact system with a more unified provenance approach.
//! All queries automatically handle ULID/UUID conversion and provide consistent error handling.

use crate::query_builder::{QueryBuilder, QueryParam};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;

/// Source Material query registry with centralized source material operations
pub struct SourceMaterialQueries;

impl SourceMaterialQueries {
    /// Get source material by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<SourceMaterialRecord>(pool)`
    pub fn get_by_id(blob_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("raw.source_material_registry")
            .columns(&[
                "blob_id::uuid as \"blob_id!\"",
                "material_type as \"material_type!\"",
                "source_uri",
                "ingestion_time as \"ingestion_time!\"",
                "file_size_bytes",
                "checksum_blake3",
                "mime_type",
                "encoding",
                "metadata as \"metadata!\"",
                "content_preview",
                "is_archived as \"is_archived!\"",
                "archive_time",
                "retention_policy",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("blob_id", QueryParam::Ulid(blob_id))
    }

    /// Insert new source material
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<SourceMaterialRecord>(pool)`
    pub fn insert(
        material_type: String,
        source_uri: Option<String>,
        file_size_bytes: Option<i64>,
        checksum_blake3: Option<String>,
        mime_type: Option<String>,
        encoding: Option<String>,
        metadata: JsonValue,
        content_preview: Option<String>,
    ) -> QueryBuilder {
        QueryBuilder::insert("raw.source_material_registry")
            .columns(&[
                "material_type",
                "source_uri",
                "file_size_bytes",
                "checksum_blake3",
                "mime_type",
                "encoding",
                "metadata",
                "content_preview",
            ])
            .values(&[
                QueryParam::String(material_type),
                QueryParam::OptionalString(source_uri),
                QueryParam::OptionalInteger(file_size_bytes),
                QueryParam::OptionalString(checksum_blake3),
                QueryParam::OptionalString(mime_type),
                QueryParam::OptionalString(encoding),
                QueryParam::Json(metadata),
                QueryParam::OptionalString(content_preview),
            ])
            .returning(&[
                "blob_id::uuid as \"blob_id!\"",
                "material_type as \"material_type!\"",
                "source_uri",
                "ingestion_time as \"ingestion_time!\"",
                "file_size_bytes",
                "checksum_blake3",
                "mime_type",
                "encoding",
                "metadata as \"metadata!\"",
                "content_preview",
                "is_archived as \"is_archived!\"",
                "archive_time",
                "retention_policy",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
    }

    /// Find source material by checksum
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<SourceMaterialRecord>(pool)`
    pub fn find_by_checksum(checksum_blake3: String) -> QueryBuilder {
        QueryBuilder::select("raw.source_material_registry")
            .columns(&[
                "blob_id::uuid as \"blob_id!\"",
                "material_type as \"material_type!\"",
                "source_uri",
                "ingestion_time as \"ingestion_time!\"",
                "file_size_bytes",
                "checksum_blake3",
                "mime_type",
                "encoding",
                "metadata as \"metadata!\"",
            ])
            .where_eq("checksum_blake3", QueryParam::String(checksum_blake3))
            .where_eq("is_archived", QueryParam::Boolean(false))
    }

    /// Get recent source materials with pagination
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SourceMaterialRecord>(pool)`
    pub fn get_recent(material_type: Option<String>, limit: Option<i64>, offset: Option<i64>) -> QueryBuilder {
        let mut builder = QueryBuilder::select("raw.source_material_registry")
            .columns(&[
                "blob_id::uuid as \"blob_id!\"",
                "material_type as \"material_type!\"",
                "source_uri",
                "ingestion_time as \"ingestion_time!\"",
                "file_size_bytes",
                "checksum_blake3",
                "mime_type",
                "encoding",
                "metadata as \"metadata!\"",
                "content_preview",
                "is_archived as \"is_archived!\"",
            ])
            .where_eq("is_archived", QueryParam::Boolean(false))
            .order_by("ingestion_time", "DESC");

        if let Some(material_type) = material_type {
            builder = builder.where_eq("material_type", QueryParam::String(material_type));
        }

        if let Some(limit) = limit {
            builder = builder.limit(limit);
        }

        if let Some(offset) = offset {
            builder = builder.offset(offset);
        }

        builder
    }

    /// Search source materials by URI pattern
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SourceMaterialRecord>(pool)`
    pub fn search_by_uri(uri_pattern: String) -> QueryBuilder {
        QueryBuilder::select("raw.source_material_registry")
            .columns(&[
                "blob_id::uuid as \"blob_id!\"",
                "material_type as \"material_type!\"",
                "source_uri",
                "ingestion_time as \"ingestion_time!\"",
                "file_size_bytes",
                "mime_type",
                "metadata as \"metadata!\"",
            ])
            .where_op(
                "source_uri",
                "ILIKE",
                QueryParam::String(format!("%{}%", uri_pattern)),
            )
            .where_eq("is_archived", QueryParam::Boolean(false))
            .order_by("ingestion_time", "DESC")
    }

    /// Get source materials by metadata key-value
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SourceMaterialRecord>(pool)`
    pub fn get_by_metadata(key: String, value: JsonValue) -> QueryBuilder {
        QueryBuilder::select("raw.source_material_registry")
            .columns(&[
                "blob_id::uuid as \"blob_id!\"",
                "material_type as \"material_type!\"",
                "source_uri",
                "ingestion_time as \"ingestion_time!\"",
                "metadata as \"metadata!\"",
            ])
            .where_op("metadata", "->", QueryParam::String(key.clone()))
            .where_op("metadata", "@>", QueryParam::Json(
                serde_json::json!({ key: value })
            ))
            .where_eq("is_archived", QueryParam::Boolean(false))
            .order_by("ingestion_time", "DESC")
    }

    /// Update source material metadata
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_metadata(blob_id: Ulid, metadata: JsonValue) -> QueryBuilder {
        QueryBuilder::update("raw.source_material_registry")
            .set("metadata", QueryParam::Json(metadata))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("blob_id", QueryParam::Ulid(blob_id))
    }

    /// Archive source material
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn archive(blob_id: Ulid, retention_policy: Option<String>) -> QueryBuilder {
        let mut builder = QueryBuilder::update("raw.source_material_registry")
            .set("is_archived", QueryParam::Boolean(true))
            .set("archive_time", QueryParam::Timestamp(Utc::now()))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("blob_id", QueryParam::Ulid(blob_id));

        if let Some(policy) = retention_policy {
            builder = builder.set("retention_policy", QueryParam::String(policy));
        }

        builder
    }

    /// Get source materials ingested within time range
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SourceMaterialRecord>(pool)`
    pub fn get_by_time_range(
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        material_type: Option<String>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::select("raw.source_material_registry")
            .columns(&[
                "blob_id::uuid as \"blob_id!\"",
                "material_type as \"material_type!\"",
                "source_uri",
                "ingestion_time as \"ingestion_time!\"",
                "file_size_bytes",
                "mime_type",
                "metadata as \"metadata!\"",
            ])
            .where_op("ingestion_time", ">=", QueryParam::Timestamp(start_time))
            .where_op("ingestion_time", "<=", QueryParam::Timestamp(end_time))
            .where_eq("is_archived", QueryParam::Boolean(false))
            .order_by("ingestion_time", "DESC");

        if let Some(material_type) = material_type {
            builder = builder.where_eq("material_type", QueryParam::String(material_type));
        }

        builder
    }

    /// Get storage statistics by material type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<MaterialTypeStats>(pool)`
    pub fn get_storage_stats() -> QueryBuilder {
        QueryBuilder::select("raw.source_material_registry")
            .columns(&[
                "material_type as \"material_type!\"",
                "COUNT(*) as \"count!\"",
                "SUM(file_size_bytes) as \"total_size_bytes\"",
                "AVG(file_size_bytes) as \"avg_size_bytes\"",
                "MIN(ingestion_time) as \"oldest_ingestion!\"",
                "MAX(ingestion_time) as \"newest_ingestion!\"",
            ])
            .where_eq("is_archived", QueryParam::Boolean(false))
            .group_by("material_type")
            .order_by("COUNT(*)", "DESC")
    }

    /// Link source material to event
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn link_to_event(
        event_id: Ulid,
        source_material_id: Ulid,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
        anchor_byte: Option<i64>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::update("core.events")
            .set("source_material_id", QueryParam::Ulid(source_material_id))
            .where_eq("event_id", QueryParam::Ulid(event_id));

        if let Some(start) = offset_start {
            builder = builder.set("source_material_offset_start", QueryParam::Integer(start));
        }

        if let Some(end) = offset_end {
            builder = builder.set("source_material_offset_end", QueryParam::Integer(end));
        }

        if let Some(anchor) = anchor_byte {
            builder = builder.set("anchor_byte", QueryParam::Integer(anchor));
        }

        builder
    }

    /// Get events linked to source material
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<EventRecord>(pool)`
    pub fn get_linked_events(source_material_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("core.events")
            .columns(&[
                "event_id::uuid as \"event_id!\"",
                "event_type as \"event_type!\"",
                "source as \"source!\"",
                "ts_orig",
                "host as \"host!\"",
                "source_material_offset_start",
                "source_material_offset_end",
                "anchor_byte",
            ])
            .where_eq("source_material_id", QueryParam::Ulid(source_material_id))
            .order_by("ts_ingest", "DESC")
    }

    /// Get archived materials ready for cleanup
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SourceMaterialRecord>(pool)`
    pub fn get_expired_archives(retention_cutoff: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("raw.source_material_registry")
            .columns(&[
                "blob_id::uuid as \"blob_id!\"",
                "material_type as \"material_type!\"",
                "source_uri",
                "archive_time",
                "retention_policy",
                "file_size_bytes",
            ])
            .where_eq("is_archived", QueryParam::Boolean(true))
            .where_op("archive_time", "<", QueryParam::Timestamp(retention_cutoff))
            .order_by("archive_time", "ASC")
    }

    /// Register in-flight source material for Stage-as-You-Go pattern
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<SourceMaterialRecord>(pool)`
    pub fn register_in_flight(
        material_type: String,
        source_uri: Option<String>,
        metadata: JsonValue,
    ) -> QueryBuilder {
        QueryBuilder::insert("raw.source_material_registry")
            .columns(&[
                "material_type",
                "source_uri",
                "metadata",
                "content_preview",
            ])
            .values(&[
                QueryParam::String(material_type),
                QueryParam::OptionalString(source_uri),
                QueryParam::Json(metadata),
                QueryParam::String("[In-flight - content pending]".to_string()),
            ])
            .returning(&["blob_id::uuid as \"blob_id!\""])
    }

    /// Finalize in-flight source material with actual content details
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn finalize_in_flight(
        blob_id: Ulid,
        file_size_bytes: i64,
        checksum_blake3: String,
        mime_type: Option<String>,
        encoding: Option<String>,
        content_preview: Option<String>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::update("raw.source_material_registry")
            .set("file_size_bytes", QueryParam::Integer(file_size_bytes))
            .set("checksum_blake3", QueryParam::String(checksum_blake3))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("blob_id", QueryParam::Ulid(blob_id));

        if let Some(mime) = mime_type {
            builder = builder.set("mime_type", QueryParam::String(mime));
        }

        if let Some(enc) = encoding {
            builder = builder.set("encoding", QueryParam::String(enc));
        }

        if let Some(preview) = content_preview {
            builder = builder.set("content_preview", QueryParam::String(preview));
        } else {
            builder = builder.set("content_preview", QueryParam::String("[Content stored]".to_string()));
        }

        builder
    }
}