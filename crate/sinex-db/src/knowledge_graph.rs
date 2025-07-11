use crate::models::{CreateEntityInput, CreateRelationInput, Entity, EntityRelation};
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::DbPoolRef;
use anyhow::Result;
use chrono::Utc;
use sinex_ulid::Ulid;
use sqlx::types::Uuid;

/// Create a new entity following the exact same pattern as add_to_work_queue
pub async fn create_entity(pool: DbPoolRef<'_>, input: CreateEntityInput) -> Result<Entity> {
    let metadata = input.metadata.unwrap_or_else(|| serde_json::json!({}));
    let canonical_name = input.canonical_name.unwrap_or_else(|| input.name.clone());
    let aliases = input.aliases.unwrap_or_default();

    let record = sqlx::query!(
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
        input.entity_type,
        input.name,
        canonical_name,
        &aliases,
        input.description,
        metadata
    )
    .fetch_one(pool)
    .await?;

    Ok(Entity {
        entity_id: uuid_to_ulid(record.id),
        entity_type: record.entity_type,
        name: record.name,
        canonical_name: record.canonical_name,
        aliases: record.aliases,
        description: record.description,
        metadata: record.metadata,
        created_at: record.created_at,
        updated_at: record.updated_at,
        merged_into_id: record.merged_into_id.map(uuid_to_ulid),
    })
}

/// Create a new entity relation
pub async fn create_relation(
    pool: DbPoolRef<'_>,
    input: CreateRelationInput,
) -> Result<EntityRelation> {
    let metadata = input.metadata.unwrap_or_else(|| serde_json::json!({}));
    let from_uuid: Uuid = ulid_to_uuid(input.from_entity_id);
    let to_uuid: Uuid = ulid_to_uuid(input.to_entity_id);
    let valid_from = input.valid_from.unwrap_or_else(Utc::now);
    let created_from_event_uuid: Option<Uuid> = input.created_from_event_id.map(ulid_to_uuid);

    let record = sqlx::query!(
        r#"
        INSERT INTO core.entity_relations (
            from_entity_id, to_entity_id, relation_type, strength, metadata, 
            valid_from, valid_until, created_from_event_id
        ) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8::uuid)
        RETURNING 
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
        "#,
        from_uuid,
        to_uuid,
        input.relation_type,
        input.strength,
        metadata,
        valid_from,
        input.valid_until,
        created_from_event_uuid
    )
    .fetch_one(pool)
    .await?;

    Ok(EntityRelation {
        relation_id: uuid_to_ulid(record.id),
        from_entity_id: uuid_to_ulid(record.from_entity_id),
        to_entity_id: uuid_to_ulid(record.to_entity_id),
        relation_type: record.relation_type,
        strength: record.strength,
        metadata: record.metadata,
        valid_from: record.valid_from,
        valid_until: record.valid_until,
        created_at: record.created_at,
        created_from_event_id: record.created_from_event_id.map(uuid_to_ulid),
    })
}

/// Get entity by ID
pub async fn get_entity_by_id(pool: DbPoolRef<'_>, entity_id: Ulid) -> Result<Option<Entity>> {
    let entity_uuid: Uuid = ulid_to_uuid(entity_id);

    let record = sqlx::query!(
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
        WHERE id::uuid = $1 AND merged_into_id IS NULL
        "#,
        entity_uuid
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| Entity {
        entity_id: uuid_to_ulid(r.id),
        entity_type: r.entity_type,
        name: r.name,
        canonical_name: r.canonical_name,
        aliases: r.aliases,
        description: r.description,
        metadata: r.metadata,
        created_at: r.created_at,
        updated_at: r.updated_at,
        merged_into_id: r.merged_into_id.map(uuid_to_ulid),
    }))
}

/// Get entities by type
pub async fn get_entities_by_type(
    pool: DbPoolRef<'_>,
    entity_type: &str,
    limit: i64,
) -> Result<Vec<Entity>> {
    let records = sqlx::query!(
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
        WHERE "type" = $1 AND merged_into_id IS NULL
        ORDER BY created_at DESC
        LIMIT $2
        "#,
        entity_type,
        limit
    )
    .fetch_all(pool)
    .await?;

    let entities = records
        .into_iter()
        .map(|r| Entity {
            entity_id: uuid_to_ulid(r.id),
            entity_type: r.entity_type,
            name: r.name,
            canonical_name: r.canonical_name,
            aliases: r.aliases,
            description: r.description,
            metadata: r.metadata,
            created_at: r.created_at,
            updated_at: r.updated_at,
            merged_into_id: r.merged_into_id.map(uuid_to_ulid),
        })
        .collect();

    Ok(entities)
}

/// Get relations for an entity
pub async fn get_entity_relations(
    pool: DbPoolRef<'_>,
    entity_id: Ulid,
) -> Result<Vec<EntityRelation>> {
    let entity_uuid: Uuid = ulid_to_uuid(entity_id);

    let records = sqlx::query!(
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
    .await?;

    let relations = records
        .into_iter()
        .map(|r| EntityRelation {
            relation_id: uuid_to_ulid(r.id),
            from_entity_id: uuid_to_ulid(r.from_entity_id),
            to_entity_id: uuid_to_ulid(r.to_entity_id),
            relation_type: r.relation_type,
            strength: r.strength,
            metadata: r.metadata,
            valid_from: r.valid_from,
            valid_until: r.valid_until,
            created_at: r.created_at,
            created_from_event_id: r.created_from_event_id.map(uuid_to_ulid),
        })
        .collect();

    Ok(relations)
}

/// Get relation by ID
pub async fn get_relation_by_id(
    pool: DbPoolRef<'_>,
    relation_id: Ulid,
) -> Result<Option<EntityRelation>> {
    let relation_uuid: Uuid = ulid_to_uuid(relation_id);

    let record = sqlx::query!(
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
        WHERE id::uuid = $1
        "#,
        relation_uuid
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| EntityRelation {
        relation_id: uuid_to_ulid(r.id),
        from_entity_id: uuid_to_ulid(r.from_entity_id),
        to_entity_id: uuid_to_ulid(r.to_entity_id),
        relation_type: r.relation_type,
        strength: r.strength,
        metadata: r.metadata,
        valid_from: r.valid_from,
        valid_until: r.valid_until,
        created_at: r.created_at,
        created_from_event_id: r.created_from_event_id.map(uuid_to_ulid),
    }))
}

/// Search entities by name or canonical name
pub async fn search_entities(
    pool: DbPoolRef<'_>,
    search_term: &str,
    limit: i64,
) -> Result<Vec<Entity>> {
    let search_pattern = format!("%{}%", search_term);

    let records = sqlx::query!(
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
    .await?;

    let entities = records
        .into_iter()
        .map(|r| Entity {
            entity_id: uuid_to_ulid(r.id),
            entity_type: r.entity_type,
            name: r.name,
            canonical_name: r.canonical_name,
            aliases: r.aliases,
            description: r.description,
            metadata: r.metadata,
            created_at: r.created_at,
            updated_at: r.updated_at,
            merged_into_id: r.merged_into_id.map(uuid_to_ulid),
        })
        .collect();

    Ok(entities)
}
