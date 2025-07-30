use crate::repositories::{common::*, Repository};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_core_types::ids::{EntityId, EventId, RelationId};
use sqlx::PgPool;

/// Entity types supported by the knowledge graph
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Person,
    Project,
    Topic,
    Organization,
    Location,
    Concept,
    Tool,
    Event,
}

impl EntityType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Person => "person",
            Self::Project => "project",
            Self::Topic => "topic",
            Self::Organization => "organization",
            Self::Location => "location",
            Self::Concept => "concept",
            Self::Tool => "tool",
            Self::Event => "event",
        }
    }
}

/// An entity in the knowledge graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: EntityId,
    pub entity_type: String,
    pub name: String,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub description: Option<String>,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub merged_into_id: Option<EntityId>,
}

/// Data for creating a new entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEntity {
    pub entity_type: EntityType,
    pub name: String,
    pub canonical_name: Option<String>,
    pub aliases: Option<Vec<String>>,
    pub description: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

/// A relationship between entities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityRelation {
    pub id: RelationId,
    pub from_entity_id: EntityId,
    pub to_entity_id: EntityId,
    pub relation_type: String,
    pub strength: f64,
    pub metadata: serde_json::Value,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub created_from_event_id: Option<EventId>,
}

/// Data for creating a new entity relation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEntityRelation {
    pub from_entity_id: EntityId,
    pub to_entity_id: EntityId,
    pub relation_type: String,
    pub strength: Option<f64>,
    pub metadata: Option<serde_json::Value>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub created_from_event_id: Option<EventId>,
}

/// Repository for knowledge graph operations
pub struct KnowledgeGraphRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for KnowledgeGraphRepository<'a> {
    fn pool(&self) -> &'a PgPool {
        self.pool
    }

    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }
}

impl<'a> KnowledgeGraphRepository<'a> {
    /// Create a new entity
    pub async fn create_entity(&self, entity: NewEntity) -> DbResult<Entity> {
        let id = EntityId::new();
        let canonical_name = entity
            .canonical_name
            .unwrap_or_else(|| entity.name.to_lowercase().replace(' ', "_"));
        let aliases = entity.aliases.unwrap_or_default();
        let metadata = entity
            .metadata
            .unwrap_or(serde_json::Value::Object(Default::default()));

        sqlx::query_as!(
            Entity,
            r#"
            INSERT INTO core.entities (
                id, type, name, canonical_name, aliases, description, metadata
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7
            )
            RETURNING 
                id as "id: EntityId",
                type as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                description,
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                merged_into_id as "merged_into_id: EntityId"
            "#,
            *id.as_ulid() as _,
            entity.entity_type.as_str(),
            entity.name,
            canonical_name,
            &aliases,
            entity.description,
            metadata
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "create entity"))
    }

    /// Get an entity by ID
    pub async fn get_entity(&self, id: EntityId) -> DbResult<Option<Entity>> {
        sqlx::query_as!(
            Entity,
            r#"
            SELECT 
                id as "id: EntityId",
                type as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                description,
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                merged_into_id as "merged_into_id: EntityId"
            FROM core.entities
            WHERE id = $1
            "#,
            *id.as_ulid() as _
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get entity"))
    }

    /// Find entities by name or alias
    pub async fn find_entities_by_name(&self, name: &str) -> DbResult<Vec<Entity>> {
        let normalized = name.to_lowercase();

        sqlx::query_as!(
            Entity,
            r#"
            SELECT 
                id as "id: EntityId",
                type as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                description,
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                merged_into_id as "merged_into_id: EntityId"
            FROM core.entities
            WHERE 
                LOWER(name) = $1 
                OR LOWER(canonical_name) = $1
                OR $1 = ANY(SELECT LOWER(unnest(aliases)))
            ORDER BY created_at DESC
            "#,
            normalized
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find entities by name"))
    }

    /// Search entities by partial name match
    pub async fn search_entities(
        &self,
        query: &str,
        entity_type: Option<EntityType>,
        limit: Option<i64>,
    ) -> DbResult<Vec<Entity>> {
        let pattern = format!("%{}%", query.to_lowercase());
        let limit = limit.unwrap_or(100);

        match entity_type {
            Some(et) => {
                sqlx::query_as!(
                    Entity,
                    r#"
                    SELECT 
                        id as "id: EntityId",
                        type as "entity_type!",
                        name as "name!",
                        canonical_name as "canonical_name!",
                        aliases as "aliases!",
                        description,
                        metadata as "metadata!",
                        created_at as "created_at!",
                        updated_at as "updated_at!",
                        merged_into_id as "merged_into_id: EntityId"
                    FROM core.entities
                    WHERE 
                        type = $3
                        AND (
                            LOWER(name) LIKE $1 
                            OR LOWER(canonical_name) LIKE $1
                            OR EXISTS (
                                SELECT 1 FROM unnest(aliases) AS alias 
                                WHERE LOWER(alias) LIKE $1
                            )
                        )
                    ORDER BY created_at DESC
                    LIMIT $2
                    "#,
                    pattern,
                    limit,
                    et.as_str()
                )
                .fetch_all(self.pool)
                .await
            }
            None => {
                sqlx::query_as!(
                    Entity,
                    r#"
                    SELECT 
                        id as "id: EntityId",
                        type as "entity_type!",
                        name as "name!",
                        canonical_name as "canonical_name!",
                        aliases as "aliases!",
                        description,
                        metadata as "metadata!",
                        created_at as "created_at!",
                        updated_at as "updated_at!",
                        merged_into_id as "merged_into_id: EntityId"
                    FROM core.entities
                    WHERE 
                        LOWER(name) LIKE $1 
                        OR LOWER(canonical_name) LIKE $1
                        OR EXISTS (
                            SELECT 1 FROM unnest(aliases) AS alias 
                            WHERE LOWER(alias) LIKE $1
                        )
                    ORDER BY created_at DESC
                    LIMIT $2
                    "#,
                    pattern,
                    limit
                )
                .fetch_all(self.pool)
                .await
            }
        }
        .map_err(|e| db_error(e, "search entities"))
    }

    /// Update an entity
    pub async fn update_entity(
        &self,
        id: EntityId,
        name: Option<String>,
        description: Option<String>,
        aliases: Option<Vec<String>>,
        metadata: Option<serde_json::Value>,
    ) -> DbResult<Entity> {
        sqlx::query_as!(
            Entity,
            r#"
            UPDATE core.entities
            SET 
                name = COALESCE($2, name),
                description = COALESCE($3, description),
                aliases = COALESCE($4, aliases),
                metadata = COALESCE($5, metadata),
                updated_at = NOW()
            WHERE id = $1
            RETURNING 
                id as "id: EntityId",
                type as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                description,
                metadata as "metadata!",
                created_at as "created_at!",
                updated_at as "updated_at!",
                merged_into_id as "merged_into_id: EntityId"
            "#,
            *id.as_ulid() as _,
            name,
            description,
            aliases.as_deref(),
            metadata
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "update entity"))
    }

    /// Merge one entity into another
    pub async fn merge_entities(&self, source_id: EntityId, target_id: EntityId) -> DbResult<()> {
        // Update the source entity to point to target
        sqlx::query!(
            r#"
            UPDATE core.entities
            SET merged_into_id = $2
            WHERE id = $1
            "#,
            *source_id.as_ulid() as _,
            *target_id.as_ulid() as _
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "merge entities"))?;

        // Update all relations pointing to source to point to target
        sqlx::query!(
            r#"
            UPDATE core.entity_relations
            SET from_entity_id = $2
            WHERE from_entity_id = $1
            "#,
            *source_id.as_ulid() as _,
            *target_id.as_ulid() as _
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update source relations"))?;

        sqlx::query!(
            r#"
            UPDATE core.entity_relations
            SET to_entity_id = $2
            WHERE to_entity_id = $1
            "#,
            *source_id.as_ulid() as _,
            *target_id.as_ulid() as _
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update target relations"))?;

        Ok(())
    }

    /// Create a new entity relation
    pub async fn create_relation(&self, relation: NewEntityRelation) -> DbResult<EntityRelation> {
        let id = RelationId::new();
        let strength = relation.strength.unwrap_or(1.0);
        let metadata = relation
            .metadata
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let valid_from = relation.valid_from.unwrap_or_else(Utc::now);

        sqlx::query_as!(
            EntityRelation,
            r#"
            INSERT INTO core.entity_relations (
                id, from_entity_id, to_entity_id, relation_type, 
                strength, metadata, valid_from, valid_until, created_from_event_id
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9
            )
            RETURNING 
                id as "id: RelationId",
                from_entity_id as "from_entity_id: EntityId",
                to_entity_id as "to_entity_id: EntityId",
                relation_type as "relation_type!",
                strength as "strength!",
                metadata as "metadata!",
                valid_from as "valid_from!",
                valid_until,
                created_at as "created_at!",
                created_from_event_id as "created_from_event_id: EventId"
            "#,
            *id.as_ulid() as _,
            *relation.from_entity_id.as_ulid() as _,
            *relation.to_entity_id.as_ulid() as _,
            relation.relation_type,
            strength,
            metadata,
            valid_from,
            relation.valid_until,
            relation.created_from_event_id.map(|id| *id.as_ulid()) as _
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "create relation"))
    }

    /// Get all relations for an entity
    pub async fn get_entity_relations(
        &self,
        entity_id: EntityId,
        relation_type: Option<&str>,
        include_inactive: bool,
    ) -> DbResult<Vec<EntityRelation>> {
        match (relation_type, include_inactive) {
            (Some(rt), false) => {
                sqlx::query_as!(
                    EntityRelation,
                    r#"
                    SELECT 
                        id as "id: RelationId",
                        from_entity_id as "from_entity_id: EntityId",
                        to_entity_id as "to_entity_id: EntityId",
                        relation_type as "relation_type!",
                        strength as "strength!",
                        metadata as "metadata!",
                        valid_from as "valid_from!",
                        valid_until,
                        created_at as "created_at!",
                        created_from_event_id as "created_from_event_id: EventId"
                    FROM core.entity_relations
                    WHERE 
                        (from_entity_id = $1 OR to_entity_id = $1)
                        AND relation_type = $2
                        AND (valid_until IS NULL OR valid_until > NOW())
                    ORDER BY created_at DESC
                    "#,
                    *entity_id.as_ulid() as _,
                    rt
                )
                .fetch_all(self.pool)
                .await
            }
            (Some(rt), true) => {
                sqlx::query_as!(
                    EntityRelation,
                    r#"
                    SELECT 
                        id as "id: RelationId",
                        from_entity_id as "from_entity_id: EntityId",
                        to_entity_id as "to_entity_id: EntityId",
                        relation_type as "relation_type!",
                        strength as "strength!",
                        metadata as "metadata!",
                        valid_from as "valid_from!",
                        valid_until,
                        created_at as "created_at!",
                        created_from_event_id as "created_from_event_id: EventId"
                    FROM core.entity_relations
                    WHERE 
                        (from_entity_id = $1 OR to_entity_id = $1)
                        AND relation_type = $2
                    ORDER BY created_at DESC
                    "#,
                    *entity_id.as_ulid() as _,
                    rt
                )
                .fetch_all(self.pool)
                .await
            }
            (None, false) => {
                sqlx::query_as!(
                    EntityRelation,
                    r#"
                    SELECT 
                        id as "id: RelationId",
                        from_entity_id as "from_entity_id: EntityId",
                        to_entity_id as "to_entity_id: EntityId",
                        relation_type as "relation_type!",
                        strength as "strength!",
                        metadata as "metadata!",
                        valid_from as "valid_from!",
                        valid_until,
                        created_at as "created_at!",
                        created_from_event_id as "created_from_event_id: EventId"
                    FROM core.entity_relations
                    WHERE 
                        (from_entity_id = $1 OR to_entity_id = $1)
                        AND (valid_until IS NULL OR valid_until > NOW())
                    ORDER BY created_at DESC
                    "#,
                    *entity_id.as_ulid() as _
                )
                .fetch_all(self.pool)
                .await
            }
            (None, true) => {
                sqlx::query_as!(
                    EntityRelation,
                    r#"
                    SELECT 
                        id as "id: RelationId",
                        from_entity_id as "from_entity_id: EntityId",
                        to_entity_id as "to_entity_id: EntityId",
                        relation_type as "relation_type!",
                        strength as "strength!",
                        metadata as "metadata!",
                        valid_from as "valid_from!",
                        valid_until,
                        created_at as "created_at!",
                        created_from_event_id as "created_from_event_id: EventId"
                    FROM core.entity_relations
                    WHERE 
                        from_entity_id = $1 OR to_entity_id = $1
                    ORDER BY created_at DESC
                    "#,
                    *entity_id.as_ulid() as _
                )
                .fetch_all(self.pool)
                .await
            }
        }
        .map_err(|e| db_error(e, "get entity relations"))
    }

    /// Update relation validity period
    pub async fn update_relation_validity(
        &self,
        id: RelationId,
        valid_until: Option<DateTime<Utc>>,
    ) -> DbResult<EntityRelation> {
        sqlx::query_as!(
            EntityRelation,
            r#"
            UPDATE core.entity_relations
            SET valid_until = $2
            WHERE id = $1
            RETURNING 
                id as "id: RelationId",
                from_entity_id as "from_entity_id: EntityId",
                to_entity_id as "to_entity_id: EntityId",
                relation_type as "relation_type!",
                strength as "strength!",
                metadata as "metadata!",
                valid_from as "valid_from!",
                valid_until,
                created_at as "created_at!",
                created_from_event_id as "created_from_event_id: EventId"
            "#,
            *id.as_ulid() as _,
            valid_until
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "update relation validity"))
    }

    /// Find paths between two entities
    pub async fn find_paths(
        &self,
        _from_entity: EntityId,
        _to_entity: EntityId,
        _max_depth: i32,
    ) -> DbResult<Vec<Vec<EntityRelation>>> {
        // This is a placeholder for graph traversal logic
        // In a real implementation, this would use recursive CTEs
        // or a graph database for efficient path finding
        Ok(vec![])
    }

    /// Get entity statistics
    pub async fn get_entity_statistics(&self, entity_id: EntityId) -> DbResult<serde_json::Value> {
        let stats = sqlx::query!(
            r#"
            WITH relation_counts AS (
                SELECT 
                    COUNT(CASE WHEN from_entity_id = $1 THEN 1 END) as outgoing_relations,
                    COUNT(CASE WHEN to_entity_id = $1 THEN 1 END) as incoming_relations,
                    COUNT(DISTINCT relation_type) as relation_types
                FROM core.entity_relations
                WHERE from_entity_id = $1 OR to_entity_id = $1
            )
            SELECT 
                rc.outgoing_relations as "outgoing_relations!",
                rc.incoming_relations as "incoming_relations!",
                rc.relation_types as "relation_types!",
                e.created_at,
                e.updated_at,
                ARRAY_LENGTH(e.aliases, 1) as alias_count
            FROM core.entities e
            CROSS JOIN relation_counts rc
            WHERE e.id = $1
            "#,
            *entity_id.as_ulid() as _
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get entity statistics"))?;

        match stats {
            Some(s) => Ok(serde_json::json!({
                "outgoing_relations": s.outgoing_relations,
                "incoming_relations": s.incoming_relations,
                "total_relations": s.outgoing_relations + s.incoming_relations,
                "relation_types": s.relation_types,
                "alias_count": s.alias_count.unwrap_or(0),
                "created_at": s.created_at,
                "updated_at": s.updated_at
            })),
            None => Ok(serde_json::json!({})),
        }
    }
}

/// Transaction support for KnowledgeGraphRepository
impl<'a> TransactionSupport for KnowledgeGraphRepository<'a> {
    type Item = KnowledgeGraphRepositoryTx<'a>;

    fn with_tx(self, _tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Self::Item {
        KnowledgeGraphRepositoryTx::new(self.pool)
    }
}

/// Transaction wrapper for KnowledgeGraphRepository
pub struct KnowledgeGraphRepositoryTx<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for KnowledgeGraphRepositoryTx<'a> {
    fn pool(&self) -> &'a PgPool {
        self.pool
    }

    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }
}

// Implement all the same methods for the transaction wrapper
impl<'a> KnowledgeGraphRepositoryTx<'a> {
    pub async fn create_entity(&self, entity: NewEntity) -> DbResult<Entity> {
        KnowledgeGraphRepository::new(self.pool)
            .create_entity(entity)
            .await
    }

    pub async fn get_entity(&self, id: EntityId) -> DbResult<Option<Entity>> {
        KnowledgeGraphRepository::new(self.pool)
            .get_entity(id)
            .await
    }

    pub async fn find_entities_by_name(&self, name: &str) -> DbResult<Vec<Entity>> {
        KnowledgeGraphRepository::new(self.pool)
            .find_entities_by_name(name)
            .await
    }

    pub async fn search_entities(
        &self,
        query: &str,
        entity_type: Option<EntityType>,
        limit: Option<i64>,
    ) -> DbResult<Vec<Entity>> {
        KnowledgeGraphRepository::new(self.pool)
            .search_entities(query, entity_type, limit)
            .await
    }

    pub async fn update_entity(
        &self,
        id: EntityId,
        name: Option<String>,
        description: Option<String>,
        aliases: Option<Vec<String>>,
        metadata: Option<serde_json::Value>,
    ) -> DbResult<Entity> {
        KnowledgeGraphRepository::new(self.pool)
            .update_entity(id, name, description, aliases, metadata)
            .await
    }

    pub async fn merge_entities(&self, source_id: EntityId, target_id: EntityId) -> DbResult<()> {
        KnowledgeGraphRepository::new(self.pool)
            .merge_entities(source_id, target_id)
            .await
    }

    pub async fn create_relation(&self, relation: NewEntityRelation) -> DbResult<EntityRelation> {
        KnowledgeGraphRepository::new(self.pool)
            .create_relation(relation)
            .await
    }

    pub async fn get_entity_relations(
        &self,
        entity_id: EntityId,
        relation_type: Option<&str>,
        include_inactive: bool,
    ) -> DbResult<Vec<EntityRelation>> {
        KnowledgeGraphRepository::new(self.pool)
            .get_entity_relations(entity_id, relation_type, include_inactive)
            .await
    }

    pub async fn update_relation_validity(
        &self,
        id: RelationId,
        valid_until: Option<DateTime<Utc>>,
    ) -> DbResult<EntityRelation> {
        KnowledgeGraphRepository::new(self.pool)
            .update_relation_validity(id, valid_until)
            .await
    }

    pub async fn find_paths(
        &self,
        from_entity: EntityId,
        to_entity: EntityId,
        max_depth: i32,
    ) -> DbResult<Vec<Vec<EntityRelation>>> {
        KnowledgeGraphRepository::new(self.pool)
            .find_paths(from_entity, to_entity, max_depth)
            .await
    }

    pub async fn get_entity_statistics(&self, entity_id: EntityId) -> DbResult<serde_json::Value> {
        KnowledgeGraphRepository::new(self.pool)
            .get_entity_statistics(entity_id)
            .await
    }
}
