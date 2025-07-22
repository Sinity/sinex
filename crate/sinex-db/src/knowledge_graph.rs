use crate::models::{CreateEntityInput, CreateRelationInput, Entity, EntityRelation};
use crate::queries::KnowledgeGraphQueries;
use crate::queries::knowledge_graph::{EntityRecord, EntityRelationRecord};
use crate::query_helpers::uuid_to_ulid;
use crate::DbPoolRef;
use anyhow::Result;
use chrono::Utc;
use sinex_ulid::Ulid;

/// Create a new entity following the exact same pattern as add_to_work_queue
// #[sinex_macros::auto_db_metrics(operation = "create_entity")]
pub async fn create_entity(pool: DbPoolRef<'_>, input: CreateEntityInput) -> Result<Entity> {
    let metadata = input.metadata.unwrap_or_else(|| serde_json::json!({}));
    let canonical_name = input.canonical_name.unwrap_or_else(|| input.name.clone());
    let aliases = input.aliases.unwrap_or_default();

    // Use the raw query method that handles text[] array properly
    let record = KnowledgeGraphQueries::create_entity_raw(
        pool,
        input.entity_type,
        input.name,
        canonical_name,
        &aliases,
        input.description,
        metadata,
    )
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
    let valid_from = input.valid_from.unwrap_or_else(Utc::now);

    let record: EntityRelationRecord = KnowledgeGraphQueries::create_relation(
        input.from_entity_id,
        input.to_entity_id,
        input.relation_type,
        input.strength,
        metadata,
        valid_from,
        input.valid_until,
        input.created_from_event_id,
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
    let record = KnowledgeGraphQueries::get_entity_by_id(entity_id)
        .fetch_optional(pool)
        .await?;

    Ok(record.map(|r: EntityRecord| Entity {
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
    let records = KnowledgeGraphQueries::get_entities_by_type(entity_type.to_string(), limit)
        .fetch_all(pool)
        .await?;

    let entities = records
        .into_iter()
        .map(|r: EntityRecord| Entity {
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
    // Use the complex query method that handles OR conditions
    let records = KnowledgeGraphQueries::get_entity_relations_complex(pool, entity_id).await?;

    let relations = records
        .into_iter()
        .map(|r: EntityRelationRecord| EntityRelation {
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
    let record = KnowledgeGraphQueries::get_relation_by_id(relation_id)
        .fetch_optional(pool)
        .await?;

    Ok(record.map(|r: EntityRelationRecord| EntityRelation {
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
    let records = KnowledgeGraphQueries::search_entities(pool, search_term, limit).await?;

    let entities = records
        .into_iter()
        .map(|r: EntityRecord| Entity {
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
