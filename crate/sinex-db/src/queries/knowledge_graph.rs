//! Knowledge graph query registry for entity and relation operations
//!
//! This module provides all database queries related to the knowledge graph,
//! including entity and relation management. All queries automatically handle
//! ULID/UUID conversion and provide consistent error handling.

use crate::query_builder::{QueryBuilder, QueryParam};
use crate::query_helpers::{db_error, DbResult};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;
use sqlx::PgPool;

/// Knowledge graph query registry with centralized entity and relation operations
pub struct KnowledgeGraphQueries;

impl KnowledgeGraphQueries {
    /// Create a new entity
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<EntityRecord>(pool)`
    pub fn create_entity(
        entity_type: String,
        name: String,
        canonical_name: String,
        aliases: Vec<String>,
        description: Option<String>,
        metadata: JsonValue,
    ) -> QueryBuilder {
        QueryBuilder::insert("core.entities")
            .columns(&[
                "\"type\"",
                "name",
                "canonical_name",
                "aliases",
                "description",
                "metadata",
            ])
            .values(&[
                QueryParam::String(entity_type),
                QueryParam::String(name),
                QueryParam::String(canonical_name),
                QueryParam::String(aliases.join(",")), // Convert array to string for now
                QueryParam::OptionalString(description),
                QueryParam::Json(metadata),
            ])
            .returning(&[
                "id::uuid as \"id!\"",
                "\"type\" as \"entity_type!\"",
                "name as \"name!\"",
                "canonical_name as \"canonical_name!\"",
                "aliases as \"aliases!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "merged_into_id::uuid as \"merged_into_id\"",
            ])
    }

    /// Create a new entity relation
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_one::<EntityRelationRecord>(pool)`
    pub fn create_relation(
        from_entity_id: Ulid,
        to_entity_id: Ulid,
        relation_type: String,
        strength: Option<f64>,
        metadata: JsonValue,
        valid_from: DateTime<Utc>,
        valid_until: Option<DateTime<Utc>>,
        created_from_event_id: Option<Ulid>,
    ) -> QueryBuilder {
        QueryBuilder::insert("core.entity_relations")
            .columns(&[
                "from_entity_id",
                "to_entity_id",
                "relation_type",
                "strength",
                "metadata",
                "valid_from",
                "valid_until",
                "created_from_event_id",
            ])
            .values(&[
                QueryParam::Ulid(from_entity_id),
                QueryParam::Ulid(to_entity_id),
                QueryParam::String(relation_type),
                QueryParam::OptionalFloat(strength),
                QueryParam::Json(metadata),
                QueryParam::Timestamp(valid_from),
                QueryParam::OptionalTimestamp(valid_until),
                QueryParam::OptionalUlid(created_from_event_id),
            ])
            .returning(&[
                "id::uuid as \"id!\"",
                "from_entity_id::uuid as \"from_entity_id!\"",
                "to_entity_id::uuid as \"to_entity_id!\"",
                "relation_type as \"relation_type!\"",
                "strength",
                "metadata as \"metadata!\"",
                "valid_from as \"valid_from!\"",
                "valid_until",
                "created_at as \"created_at!\"",
                "created_from_event_id::uuid as \"created_from_event_id\"",
            ])
    }

    /// Get entity by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<EntityRecord>(pool)`
    pub fn get_entity_by_id(entity_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("core.entities")
            .columns(&[
                "id::uuid as \"id!\"",
                "\"type\" as \"entity_type!\"",
                "name as \"name!\"",
                "canonical_name as \"canonical_name!\"",
                "aliases as \"aliases!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "merged_into_id::uuid as \"merged_into_id\"",
            ])
            .where_eq("id", QueryParam::Ulid(entity_id))
            .where_is_null("merged_into_id")
    }

    /// Get entities by type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<EntityRecord>(pool)`
    pub fn get_entities_by_type(entity_type: String, limit: i64) -> QueryBuilder {
        QueryBuilder::select("core.entities")
            .columns(&[
                "id::uuid as \"id!\"",
                "\"type\" as \"entity_type!\"",
                "name as \"name!\"",
                "canonical_name as \"canonical_name!\"",
                "aliases as \"aliases!\"",
                "description",
                "metadata as \"metadata!\"",
                "created_at as \"created_at!\"",
                "updated_at as \"updated_at!\"",
                "merged_into_id::uuid as \"merged_into_id\"",
            ])
            .where_eq("\"type\"", QueryParam::String(entity_type))
            .where_is_null("merged_into_id")
            .order_by("created_at", "DESC")
            .limit(limit)
    }

    /// Get relations for an entity
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<EntityRelationRecord>(pool)`
    pub fn get_entity_relations(_entity_id: Ulid) -> QueryBuilder {
        let builder = QueryBuilder::select("core.entity_relations")
            .columns(&[
                "id::uuid as \"id!\"",
                "from_entity_id::uuid as \"from_entity_id!\"",
                "to_entity_id::uuid as \"to_entity_id!\"",
                "relation_type as \"relation_type!\"",
                "strength",
                "metadata as \"metadata!\"",
                "valid_from as \"valid_from!\"",
                "valid_until",
                "created_at as \"created_at!\"",
                "created_from_event_id::uuid as \"created_from_event_id\"",
            ])
            .order_by("created_at", "DESC");

        // This is a complex WHERE clause that needs custom handling
        // We'll need to use raw SQL for the OR condition
        builder
    }

    /// Get relation by ID
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<EntityRelationRecord>(pool)`
    pub fn get_relation_by_id(relation_id: Ulid) -> QueryBuilder {
        QueryBuilder::select("core.entity_relations")
            .columns(&[
                "id::uuid as \"id!\"",
                "from_entity_id::uuid as \"from_entity_id!\"",
                "to_entity_id::uuid as \"to_entity_id!\"",
                "relation_type as \"relation_type!\"",
                "strength",
                "metadata as \"metadata!\"",
                "valid_from as \"valid_from!\"",
                "valid_until",
                "created_at as \"created_at!\"",
                "created_from_event_id::uuid as \"created_from_event_id\"",
            ])
            .where_eq("id", QueryParam::Ulid(relation_id))
    }

    /// Search entities by name (case-insensitive)
    ///
    /// This uses raw SQL for ILIKE pattern matching
    pub async fn search_entities(
        pool: &PgPool,
        search_term: &str,
        limit: i64,
    ) -> DbResult<Vec<EntityRecord>> {
        let search_pattern = format!("%{}%", search_term);

        let rows = sqlx::query_as!(
            EntityRecord,
            r#"
            SELECT 
                id::uuid as "id!",
                "type" as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                description,
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                merged_into_id::uuid as "merged_into_id"
            FROM core.entities 
            WHERE (name ILIKE $1 OR canonical_name ILIKE $1) 
            AND merged_into_id IS NULL
            ORDER BY 
                CASE WHEN name = $2 THEN 1 
                     WHEN canonical_name = $2 THEN 2
                     ELSE 3 
                END,
                created_at DESC
            LIMIT $3
            "#,
            search_pattern,
            search_term,
            limit
        )
        .fetch_all(pool)
        .await
        .map_err(|e| db_error(e, "search entities"))?;

        Ok(rows)
    }

    /// Get entity relations with complex OR clause
    ///
    /// This uses raw SQL for the complex WHERE clause
    pub async fn get_entity_relations_complex(
        pool: &PgPool,
        entity_id: Ulid,
    ) -> DbResult<Vec<EntityRelationRecord>> {
        let entity_uuid = crate::query_helpers::ulid_to_uuid(entity_id);

        let rows = sqlx::query_as!(
            EntityRelationRecord,
            r#"
            SELECT 
                id::uuid as "id!",
                from_entity_id::uuid as "from_entity_id!",
                to_entity_id::uuid as "to_entity_id!",
                relation_type as "relation_type!",
                strength,
                metadata as "metadata!",
                valid_from as "valid_from!",
                valid_until,
                created_at as "created_at!",
                created_from_event_id::uuid as "created_from_event_id"
            FROM core.entity_relations 
            WHERE (from_entity_id::uuid = $1 OR to_entity_id::uuid = $1)
            AND (valid_until IS NULL OR valid_until > NOW())
            ORDER BY created_at DESC
            "#,
            entity_uuid
        )
        .fetch_all(pool)
        .await
        .map_err(|e| db_error(e, "get entity relations"))?;

        Ok(rows)
    }

    /// Create entity with raw SQL for string array handling
    ///
    /// This is needed because aliases is a text[] column
    pub async fn create_entity_raw(
        pool: &PgPool,
        entity_type: String,
        name: String,
        canonical_name: String,
        aliases: &[String],
        description: Option<String>,
        metadata: JsonValue,
    ) -> DbResult<EntityRecord> {
        let row = sqlx::query_as!(
            EntityRecord,
            r#"
            INSERT INTO core.entities (
                "type", name, canonical_name, aliases, description, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING 
                id::uuid as "id!",
                "type" as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                description,
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                merged_into_id::uuid as "merged_into_id"
            "#,
            entity_type,
            name,
            canonical_name,
            aliases,
            description,
            metadata
        )
        .fetch_one(pool)
        .await
        .map_err(|e| db_error(e, "create entity"))?;

        Ok(row)
    }
}

/// Record type for entity results
#[derive(Debug, sqlx::FromRow)]
pub struct EntityRecord {
    pub id: sqlx::types::Uuid,
    pub entity_type: String,
    pub name: String,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub description: Option<String>,
    pub metadata: JsonValue,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub merged_into_id: Option<sqlx::types::Uuid>,
}

/// Record type for entity relation results
#[derive(Debug, sqlx::FromRow)]
pub struct EntityRelationRecord {
    pub id: sqlx::types::Uuid,
    pub from_entity_id: sqlx::types::Uuid,
    pub to_entity_id: sqlx::types::Uuid,
    pub relation_type: String,
    pub strength: Option<f64>,
    pub metadata: JsonValue,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub created_from_event_id: Option<sqlx::types::Uuid>,
}
