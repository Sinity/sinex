//! Artifacts query registry for centralized artifact and blob operations
//!
//! This module provides all database queries related to artifact storage,
//! blob management, and associated operations. All queries automatically
//! handle ULID/UUID conversion and provide consistent error handling.

use crate::query_builder::{QueryBuilder, QueryParam};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;

/// Artifacts query registry with centralized artifact operations
pub struct ArtifactQueries;

impl ArtifactQueries {
    /// Get artifact by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<ArtifactRecord>(pool)`
    pub fn get_by_id(artifact_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "\"type\" as \"artifact_type!\"",
                "title as \"title!\"",
                "source_url",
                "original_path",
                "mime_type",
                "size_bytes",
                "checksum",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "deleted_at",
                "created_from_event_id::uuid as \"created_from_event_id\"",
                "blob_id::uuid as \"blob_id\"",
            ])
            .where_eq("id", QueryParam::Ulid(artifact_id))
            .where_is_null("deleted_at")
    }

    /// Get artifact by blob ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<ArtifactRecord>(pool)`
    pub fn get_by_blob_id(blob_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("blob_id", QueryParam::Ulid(blob_id))
    }

    /// Insert new artifact
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<ArtifactRecord>(pool)`
    pub fn insert_artifact(
        blob_id: Ulid,
        title: String,
        description: Option<String>,
        metadata: JsonValue,
    ) -> QueryBuilder {
        QueryBuilder::insert("core.artifacts")
            .columns(&["blob_id", "title", "description", "metadata"])
            .values(&[
                QueryParam::Ulid(blob_id),
                QueryParam::String(title),
                QueryParam::OptionalString(description),
                QueryParam::Json(metadata),
            ])
            .returning(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
    }

    /// Insert new artifact with full input
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<ArtifactRecord>(pool)`
    pub fn insert_artifact_full(
        artifact_type: String,
        title: String,
        source_url: Option<String>,
        original_path: Option<String>,
        mime_type: Option<String>,
        size_bytes: Option<i64>,
        checksum: Option<String>,
        metadata: JsonValue,
        created_from_event_id: Option<Ulid>,
        blob_id: Option<Ulid>,
    ) -> QueryBuilder {
        QueryBuilder::insert("core.artifacts")
            .columns(&[
                "type",
                "title",
                "source_url",
                "original_path",
                "mime_type",
                "size_bytes",
                "checksum",
                "metadata",
                "created_from_event_id",
                "blob_id",
            ])
            .values(&[
                QueryParam::String(artifact_type),
                QueryParam::String(title),
                QueryParam::OptionalString(source_url),
                QueryParam::OptionalString(original_path),
                QueryParam::OptionalString(mime_type),
                QueryParam::OptionalInteger(size_bytes),
                QueryParam::OptionalString(checksum),
                QueryParam::Json(metadata),
                QueryParam::OptionalUlid(created_from_event_id),
                QueryParam::OptionalUlid(blob_id),
            ])
            .returning(&[
                "id::uuid as \"id!\"",
                "\"type\" as \"artifact_type!\"",
                "title as \"title!\"",
                "source_url",
                "original_path",
                "mime_type",
                "size_bytes",
                "checksum",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "deleted_at",
                "created_from_event_id::uuid as \"created_from_event_id\"",
                "blob_id::uuid as \"blob_id\"",
            ])
    }

    /// Get recent artifacts with pagination
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn get_recent(limit: Option<i64>, offset: Option<i64>) -> QueryBuilder {
        let mut builder = QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "\"type\" as \"artifact_type!\"",
                "title as \"title!\"",
                "source_url",
                "original_path",
                "mime_type",
                "size_bytes",
                "checksum",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "deleted_at",
                "created_from_event_id::uuid as \"created_from_event_id\"",
                "blob_id::uuid as \"blob_id\"",
            ])
            .where_is_null("deleted_at")
            .order_by("created_at", "DESC");

        if let Some(limit) = limit {
            builder = builder.limit(limit);
        }

        if let Some(offset) = offset {
            builder = builder.offset(offset);
        }

        builder
    }

    /// Search artifacts by title
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn search_by_title(title_pattern: String) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op(
                "title",
                "ILIKE",
                QueryParam::String(format!("%{}%", title_pattern)),
            )
            .order_by("created_at", "DESC")
    }

    /// Search artifacts by description
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn search_by_description(description_pattern: String) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op(
                "description",
                "ILIKE",
                QueryParam::String(format!("%{}%", description_pattern)),
            )
            .order_by("created_at", "DESC")
    }

    /// Get artifacts by metadata key-value
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn get_by_metadata_key_value(key: String, value: JsonValue) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op("metadata", "->", QueryParam::String(key))
            .where_op("metadata", "@>", QueryParam::Json(value))
            .order_by("created_at", "DESC")
    }

    /// Count total artifacts
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_all() -> QueryBuilder {
        QueryBuilder::select("core.artifacts").columns(&["COUNT(*) as count"])
    }

    /// Update artifact title
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_title(artifact_id: Ulid, title: String) -> QueryBuilder {
        QueryBuilder::update("core.artifacts")
            .set("title", QueryParam::String(title))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("id", QueryParam::Ulid(artifact_id))
    }

    /// Update artifact description
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_description(artifact_id: Ulid, description: Option<String>) -> QueryBuilder {
        QueryBuilder::update("core.artifacts")
            .set("description", QueryParam::OptionalString(description))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("id", QueryParam::Ulid(artifact_id))
    }

    /// Update artifact metadata
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_metadata(artifact_id: Ulid, metadata: JsonValue) -> QueryBuilder {
        QueryBuilder::update("core.artifacts")
            .set("metadata", QueryParam::Json(metadata))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("id", QueryParam::Ulid(artifact_id))
    }

    /// Delete artifact by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_by_id(artifact_id: Ulid) -> QueryBuilder {
        QueryBuilder::delete("core.artifacts").where_eq("id", QueryParam::Ulid(artifact_id))
    }

    /// Delete artifact by blob ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_by_blob_id(blob_id: Ulid) -> QueryBuilder {
        QueryBuilder::delete("core.artifacts").where_eq("blob_id", QueryParam::Ulid(blob_id))
    }

    /// Get artifacts created within time range
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn get_by_time_range(
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op("created_at", ">=", QueryParam::Timestamp(start_time))
            .where_op("created_at", "<=", QueryParam::Timestamp(end_time))
            .order_by("created_at", "DESC");

        if let Some(limit) = limit {
            builder = builder.limit(limit);
        }

        if let Some(offset) = offset {
            builder = builder.offset(offset);
        }

        builder
    }

    /// Get artifacts by multiple IDs
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn get_by_ids(artifact_ids: Vec<Ulid>) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_in("id", QueryParam::UlidArray(artifact_ids))
            .order_by("created_at", "DESC")
    }

    /// Get artifacts by multiple blob IDs
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn get_by_blob_ids(blob_ids: Vec<Ulid>) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_in("blob_id", QueryParam::UlidArray(blob_ids))
            .order_by("created_at", "DESC")
    }

    /// Get artifact statistics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<ArtifactStatsRecord>(pool)`
    pub fn get_artifact_stats() -> QueryBuilder {
        QueryBuilder::select("core.artifacts").columns(&[
            "COUNT(*) as \"total_artifacts!\"",
            "COUNT(DISTINCT blob_id) as \"unique_blobs!\"",
            "MIN(created_at) as \"oldest_artifact\"",
            "MAX(created_at) as \"newest_artifact\"",
            "AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as \"avg_update_delay\"",
        ])
    }

    /// Get artifacts with missing descriptions
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn get_without_description() -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op("description", "IS", QueryParam::OptionalString(None))
            .order_by("created_at", "DESC")
    }

    /// Get artifacts updated after timestamp
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn get_updated_after(timestamp: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op("updated_at", ">", QueryParam::Timestamp(timestamp))
            .order_by("updated_at", "DESC")
    }

    /// Get artifacts that haven't been updated for a while
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn get_stale_artifacts(threshold: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op("updated_at", "<", QueryParam::Timestamp(threshold))
            .order_by("updated_at", "ASC")
    }

    /// Full text search across title and description
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ArtifactRecord>(pool)`
    pub fn full_text_search(search_term: String) -> QueryBuilder {
        QueryBuilder::select("core.artifacts")
            .columns(&[
                "id::uuid as \"id!\"",
                "blob_id::uuid as \"blob_id!\"",
                "title as \"title!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op(
                "to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(description, ''))",
                "@@",
                QueryParam::String(format!("plainto_tsquery('english', '{}')", search_term)),
            )
            .order_by("created_at", "DESC")
    }

    // ========================================================================
    // Blob-specific queries
    // ========================================================================

    /// Find blob by blake3 hash
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<BlobRecord>(pool)`
    pub fn find_blob_by_blake3(blake3_hash: String) -> QueryBuilder {
        QueryBuilder::select("core.blobs")
            .columns(&[
                "id::uuid as \"id!\"",
                "annex_key as \"annex_key!\"",
                "original_filename as \"original_filename!\"",
                "size_bytes as \"size_bytes!\"",
                "mime_type",
                "checksum_sha256 as \"checksum_sha256!\"",
                "checksum_md5",
                "storage_backend as \"storage_backend!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "last_verified_at",
                "verification_status",
            ])
            .where_eq("checksum_blake3", QueryParam::String(blake3_hash))
    }

    /// Insert new blob
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<BlobRecord>(pool)`
    pub fn insert_blob(blob: &crate::models::BlobRecord) -> QueryBuilder {
        QueryBuilder::insert("core.blobs")
            .columns(&[
                "annex_key",
                "original_filename",
                "size_bytes",
                "mime_type",
                "checksum_sha256",
                "checksum_blake3",
                "storage_backend",
                "metadata",
                "verification_status",
            ])
            .values(&[
                QueryParam::String(blob.annex_key.clone()),
                QueryParam::String(blob.original_filename.clone()),
                QueryParam::Integer(blob.size_bytes),
                QueryParam::OptionalString(blob.mime_type.clone()),
                QueryParam::String(blob.checksum_sha256.clone()),
                QueryParam::OptionalString(blob.checksum_blake3.clone()),
                QueryParam::String(blob.storage_backend.clone()),
                QueryParam::Json(blob.metadata.clone()),
                QueryParam::OptionalString(blob.verification_status.clone()),
            ])
            .returning(&[
                "id::uuid as \"id!\"",
                "annex_key as \"annex_key!\"",
                "original_filename as \"original_filename!\"",
                "size_bytes as \"size_bytes!\"",
                "mime_type",
                "checksum_sha256 as \"checksum_sha256!\"",
                "checksum_blake3",
                "storage_backend as \"storage_backend!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "last_verified_at",
                "verification_status",
            ])
    }

    /// Get blob by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<BlobRecord>(pool)`
    pub fn get_blob_by_id(blob_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("core.blobs")
            .columns(&[
                "id::uuid as \"id!\"",
                "annex_key as \"annex_key!\"",
                "original_filename as \"original_filename!\"",
                "size_bytes as \"size_bytes!\"",
                "mime_type",
                "checksum_sha256 as \"checksum_sha256!\"",
                "checksum_blake3",
                "storage_backend as \"storage_backend!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "last_verified_at",
                "verification_status",
            ])
            .where_eq("id", QueryParam::Ulid(blob_id))
    }

    /// Update blob verification status
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_verification_status(blob_id: Ulid, status: String) -> QueryBuilder {
        QueryBuilder::update("core.blobs")
            .set("verification_status", QueryParam::String(status))
            .set("last_verified_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("id", QueryParam::Ulid(blob_id))
    }

    /// Update blob original filename
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_original_filename(blob_id: Ulid, filename: String) -> QueryBuilder {
        QueryBuilder::update("core.blobs")
            .set("original_filename", QueryParam::String(filename))
            .where_eq("id", QueryParam::Ulid(blob_id))
    }

    /// Get storage statistics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<StorageStatsRecord>(pool)`
    pub fn get_storage_stats() -> QueryBuilder {
        QueryBuilder::select("core.blobs").columns(&[
            "COUNT(*) as \"total_blobs!\"",
            "SUM(size_bytes) as \"total_size_bytes!\"",
            "COUNT(DISTINCT checksum_sha256) as \"unique_files!\"",
            "AVG(size_bytes) as \"avg_file_size!\"",
            "MAX(size_bytes) as \"max_file_size!\"",
            "MIN(created_at) as \"oldest_blob!\"",
            "MAX(created_at) as \"newest_blob!\"",
        ])
    }
}
