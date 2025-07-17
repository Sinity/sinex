//! Annotations query registry for centralized annotation operations
//!
//! This module provides all database queries related to event annotations,
//! storage, retrieval, and management. All queries automatically handle ULID/UUID
//! conversion and provide consistent error handling.

use crate::query_builder::{QueryBuilder, QueryParam};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;

/// Annotations query registry with centralized annotation operations
pub struct AnnotationQueries;

impl AnnotationQueries {
    /// Get annotation by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<AnnotationRecord>(pool)`
    pub fn get_by_id(annotation_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("core.event_annotations")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_id::uuid as \"event_id!\"",
                "annotation_type as \"annotation_type!\"",
                "content as \"content!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "created_by as \"created_by!\"",
            ])
            .where_eq("id", QueryParam::Ulid(annotation_id))
    }

    /// Get annotations for a specific event
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<AnnotationRecord>(pool)`
    pub fn get_by_event_id(event_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("core.event_annotations")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_id::uuid as \"event_id!\"",
                "annotation_type as \"annotation_type!\"",
                "content as \"content!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "created_by as \"created_by!\"",
            ])
            .where_eq("event_id", QueryParam::Ulid(event_id))
            .order_by("created_at", "DESC")
    }

    /// Insert new annotation
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<AnnotationRecord>(pool)`
    pub fn insert_annotation(
        event_id: Ulid,
        annotation_type: String,
        content: String,
        metadata: JsonValue,
        created_by: String,
    ) -> QueryBuilder {
        QueryBuilder::insert("core.event_annotations")
            .columns(&[
                "event_id",
                "annotation_type",
                "content",
                "metadata",
                "created_by",
            ])
            .values(&[
                QueryParam::Ulid(event_id),
                QueryParam::String(annotation_type),
                QueryParam::String(content),
                QueryParam::Json(metadata),
                QueryParam::String(created_by),
            ])
            .returning(&[
                "id::uuid as \"id!\"",
                "event_id::uuid as \"event_id!\"",
                "annotation_type as \"annotation_type!\"",
                "content as \"content!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "created_by as \"created_by!\"",
            ])
    }

    /// Update annotation content
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<AnnotationRecord>(pool)`
    pub fn update_content(annotation_id: Ulid, new_content: String) -> QueryBuilder {
        QueryBuilder::update("core.event_annotations")
            .set("content", QueryParam::String(new_content))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("id", QueryParam::Ulid(annotation_id))
            .returning(&[
                "id::uuid as \"id!\"",
                "event_id::uuid as \"event_id!\"",
                "annotation_type as \"annotation_type!\"",
                "content as \"content!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "created_by as \"created_by!\"",
            ])
    }

    /// Delete annotation by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_by_id(annotation_id: Ulid) -> QueryBuilder {
        QueryBuilder::delete("core.event_annotations")
            .where_eq("id", QueryParam::Ulid(annotation_id))
    }

    /// Get recent annotations with pagination
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<AnnotationRecord>(pool)`
    pub fn get_recent(limit: i64) -> QueryBuilder {
        QueryBuilder::select("core.event_annotations")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_id::uuid as \"event_id!\"",
                "annotation_type as \"annotation_type!\"",
                "content as \"content!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "created_by as \"created_by!\"",
            ])
            .order_by("created_at", "DESC")
            .limit(limit)
    }

    /// Count annotations for an event
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_by_event_id(event_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("core.event_annotations")
            .columns(&["COUNT(*) as count"])
            .where_eq("event_id", QueryParam::Ulid(event_id))
    }

    /// Get annotations by annotation type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<AnnotationRecord>(pool)`
    pub fn get_by_annotation_type(annotation_type: String) -> QueryBuilder {
        QueryBuilder::select("core.event_annotations")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_id::uuid as \"event_id!\"",
                "annotation_type as \"annotation_type!\"",
                "content as \"content!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "created_by as \"created_by!\"",
            ])
            .where_eq("annotation_type", QueryParam::String(annotation_type))
            .order_by("created_at", "DESC")
    }

    /// Search annotations by content
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<AnnotationRecord>(pool)`
    pub fn search_by_content(search_term: String) -> QueryBuilder {
        QueryBuilder::select("core.event_annotations")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_id::uuid as \"event_id!\"",
                "annotation_type as \"annotation_type!\"",
                "content as \"content!\"",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "created_by as \"created_by!\"",
            ])
            .where_op(
                "content",
                "ILIKE",
                QueryParam::String(format!("%{}%", search_term)),
            )
            .order_by("created_at", "DESC")
    }
}