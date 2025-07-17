//! Schema query registry for centralized schema operations
//!
//! This module provides all database queries related to schema validation,
//! metadata, and management. All queries automatically handle ULID/UUID
//! conversion and provide consistent error handling.

use crate::query_builder::{QueryBuilder, QueryParam};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;

/// Schema query registry with centralized schema operations
pub struct SchemaQueries;

impl SchemaQueries {
    /// Get schema by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<SchemaRecord>(pool)`
    pub fn get_by_id(schema_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_type as \"event_type!\"",
                "schema_version as \"schema_version!\"",
                "schema_data as \"schema_data!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("id", QueryParam::Ulid(schema_id))
    }

    /// Get schema by event type and version
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<SchemaRecord>(pool)`
    pub fn get_by_event_type_and_version(event_type: String, schema_version: i32) -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_type as \"event_type!\"",
                "schema_version as \"schema_version!\"",
                "schema_data as \"schema_data!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("event_type", QueryParam::String(event_type))
            .where_eq("schema_version", QueryParam::Integer(schema_version as i64))
    }

    /// Get latest schema for event type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<SchemaRecord>(pool)`
    pub fn get_latest_for_event_type(event_type: String) -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_type as \"event_type!\"",
                "schema_version as \"schema_version!\"",
                "schema_data as \"schema_data!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("event_type", QueryParam::String(event_type))
            .order_by("schema_version", "DESC")
            .limit(1)
    }

    /// Insert new schema
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<SchemaRecord>(pool)`
    pub fn insert_schema(
        event_type: String,
        schema_version: i32,
        schema_data: JsonValue,
    ) -> QueryBuilder {
        QueryBuilder::insert("sinex_schemas.event_payload_schemas")
            .columns(&["event_type", "schema_version", "schema_data"])
            .values(&[
                QueryParam::String(event_type),
                QueryParam::Integer(schema_version as i64),
                QueryParam::Json(schema_data),
            ])
            .returning(&[
                "id::uuid as \"id!\"",
                "event_type as \"event_type!\"",
                "schema_version as \"schema_version!\"",
                "schema_data as \"schema_data!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
    }

    /// Get all schemas for event type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SchemaRecord>(pool)`
    pub fn get_all_for_event_type(event_type: String) -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_type as \"event_type!\"",
                "schema_version as \"schema_version!\"",
                "schema_data as \"schema_data!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_eq("event_type", QueryParam::String(event_type))
            .order_by("schema_version", "DESC")
    }

    /// Get all event types with schemas
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<(String,)>(pool)`
    pub fn get_all_event_types() -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&["DISTINCT event_type"])
            .order_by("event_type", "ASC")
    }

    /// Count schemas by event type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(i64,)>(pool)`
    pub fn count_by_event_type(event_type: String) -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&["COUNT(*) as count"])
            .where_eq("event_type", QueryParam::String(event_type))
    }

    /// Delete schema by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_by_id(schema_id: Ulid) -> QueryBuilder {
        QueryBuilder::delete("sinex_schemas.event_payload_schemas")
            .where_eq("id", QueryParam::Ulid(schema_id))
    }

    /// Delete schemas by event type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_by_event_type(event_type: String) -> QueryBuilder {
        QueryBuilder::delete("sinex_schemas.event_payload_schemas")
            .where_eq("event_type", QueryParam::String(event_type))
    }

    /// Update schema data
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_schema_data(schema_id: Ulid, schema_data: JsonValue) -> QueryBuilder {
        QueryBuilder::update("sinex_schemas.event_payload_schemas")
            .set("schema_data", QueryParam::Json(schema_data))
            .set("updated_at", QueryParam::Timestamp(Utc::now()))
            .where_eq("id", QueryParam::Ulid(schema_id))
    }

    /// Get schemas created after timestamp
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SchemaRecord>(pool)`
    pub fn get_created_after(timestamp: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_type as \"event_type!\"",
                "schema_version as \"schema_version!\"",
                "schema_data as \"schema_data!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op("created_at", ">", QueryParam::Timestamp(timestamp))
            .order_by("created_at", "DESC")
    }

    /// Get schemas updated after timestamp
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<SchemaRecord>(pool)`
    pub fn get_updated_after(timestamp: DateTime<Utc>) -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&[
                "id::uuid as \"id!\"",
                "event_type as \"event_type!\"",
                "schema_version as \"schema_version!\"",
                "schema_data as \"schema_data!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
            ])
            .where_op("updated_at", ">", QueryParam::Timestamp(timestamp))
            .order_by("updated_at", "DESC")
    }

    /// Get schema versions for event type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<(i32,)>(pool)`
    pub fn get_versions_for_event_type(event_type: String) -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&["schema_version"])
            .where_eq("event_type", QueryParam::String(event_type))
            .order_by("schema_version", "DESC")
    }

    /// Check if schema exists
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<(bool,)>(pool)`
    pub fn exists(event_type: String, schema_version: i32) -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&["EXISTS(SELECT 1 FROM sinex_schemas.event_payload_schemas WHERE event_type = $1 AND schema_version = $2) as exists"])
            .where_eq("event_type", QueryParam::String(event_type))
            .where_eq("schema_version", QueryParam::Integer(schema_version as i64))
    }

    /// Get schema statistics
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<SchemaStatsRecord>(pool)`
    pub fn get_schema_stats() -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas").columns(&[
            "COUNT(*) as \"total_schemas!\"",
            "COUNT(DISTINCT event_type) as \"total_event_types!\"",
            "MAX(schema_version) as \"max_version\"",
            "MIN(created_at) as \"oldest_schema\"",
            "MAX(updated_at) as \"newest_update\"",
        ])
    }

    /// Get all active schemas from event_payload_schemas table
    ///
    /// This query is specifically for the ingestd validator to load schemas
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ActiveSchemaRecord>(pool)`
    pub fn get_all_active_schemas() -> QueryBuilder {
        QueryBuilder::select("sinex_schemas.event_payload_schemas")
            .columns(&[
                "id::text as \"schema_id\"",
                "event_source as \"event_source!\"",
                "event_type as \"event_type!\"",
                "schema_version",
                "json_schema_definition as \"schema_content!\"",
            ])
            .where_eq("is_active", QueryParam::Boolean(true))
            .order_by("event_source", "ASC")
            .order_by("event_type", "ASC")
            .order_by("schema_version", "DESC")
    }
}
