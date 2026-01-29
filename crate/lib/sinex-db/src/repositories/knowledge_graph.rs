use crate::repositories::{common::*, Repository};
use crate::schema::Entities;
use serde::{Deserialize, Serialize};
use sinex_primitives::error::SinexError;
use sinex_primitives::{Id, Timestamp, Ulid};
use sqlx::PgPool;
use std::collections::HashSet;

use crate::models::{Entity, EntityRelation, Event, JsonValue};

/// Entity types supported by the knowledge graph
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
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

/// An entity record from the knowledge graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityRecord {
    pub id: Id<Entity>,
    pub entity_type: String,
    pub name: String,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub properties: serde_json::Value,
    pub source_event_ids: Vec<Id<Event<JsonValue>>>,
    pub confidence_score: f64,
    pub is_merged: bool,
    pub merged_into_id: Option<Id<Entity>>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Entity to create
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEntity {
    pub entity_type: EntityType,
    pub name: String,
    pub canonical_name: Option<String>,
    pub aliases: Option<Vec<String>>,
    pub properties: Option<serde_json::Value>,
    pub source_event_ids: Option<Vec<Id<Event<JsonValue>>>>,
    pub confidence_score: Option<f64>,
}

impl CreateEntity {
    /// Create a person entity
    pub fn person(name: impl Into<String>) -> Self {
        CreateEntity {
            entity_type: EntityType::Person,
            name: name.into(),
            canonical_name: None,
            aliases: None,
            properties: None,
            source_event_ids: None,
            confidence_score: None,
        }
    }

    /// Create a project entity
    pub fn project(name: impl Into<String>) -> Self {
        CreateEntity {
            entity_type: EntityType::Project,
            name: name.into(),
            canonical_name: None,
            aliases: None,
            properties: None,
            source_event_ids: None,
            confidence_score: None,
        }
    }

    /// Create a topic entity
    pub fn topic(name: impl Into<String>) -> Self {
        CreateEntity {
            entity_type: EntityType::Topic,
            name: name.into(),
            canonical_name: None,
            aliases: None,
            properties: None,
            source_event_ids: None,
            confidence_score: None,
        }
    }

    /// Create an organization entity
    pub fn organization(name: impl Into<String>) -> Self {
        CreateEntity {
            entity_type: EntityType::Organization,
            name: name.into(),
            canonical_name: None,
            aliases: None,
            properties: None,
            source_event_ids: None,
            confidence_score: None,
        }
    }

    /// Create a location entity
    pub fn location(name: impl Into<String>) -> Self {
        CreateEntity {
            entity_type: EntityType::Location,
            name: name.into(),
            canonical_name: None,
            aliases: None,
            properties: None,
            source_event_ids: None,
            confidence_score: None,
        }
    }

    /// Create a concept entity
    pub fn concept(name: impl Into<String>) -> Self {
        CreateEntity {
            entity_type: EntityType::Concept,
            name: name.into(),
            canonical_name: None,
            aliases: None,
            properties: None,
            source_event_ids: None,
            confidence_score: None,
        }
    }

    /// Create a tool entity
    pub fn tool(name: impl Into<String>) -> Self {
        CreateEntity {
            entity_type: EntityType::Tool,
            name: name.into(),
            canonical_name: None,
            aliases: None,
            properties: None,
            source_event_ids: None,
            confidence_score: None,
        }
    }

    /// Create an event entity
    pub fn event(name: impl Into<String>) -> Self {
        CreateEntity {
            entity_type: EntityType::Event,
            name: name.into(),
            canonical_name: None,
            aliases: None,
            properties: None,
            source_event_ids: None,
            confidence_score: None,
        }
    }

    /// Fluent method to set canonical name
    pub fn with_canonical_name(mut self, name: impl Into<String>) -> Self {
        self.canonical_name = Some(name.into());
        self
    }

    /// Fluent method to add aliases
    pub fn with_aliases<I, S>(mut self, aliases: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.aliases = Some(aliases.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Fluent method to set properties
    pub fn with_properties(mut self, properties: serde_json::Value) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Fluent method to set source event IDs
    pub fn with_source_event_ids(mut self, ids: Vec<Id<Event<JsonValue>>>) -> Self {
        self.source_event_ids = Some(ids);
        self
    }

    /// Fluent method to set confidence score
    pub fn with_confidence_score(mut self, score: f64) -> Self {
        self.confidence_score = Some(score);
        self
    }
}

/// A relationship record between entities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityRelationRecord {
    pub id: Id<EntityRelation>,
    pub from_entity_id: Id<Entity>,
    pub to_entity_id: Id<Entity>,
    pub relation_type: String,
    pub properties: serde_json::Value,
    pub source_event_ids: Vec<Id<Event<JsonValue>>>,
    pub confidence_score: f64,
    pub is_active: bool,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Entity relation to create
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEntityRelation {
    pub from_entity_id: Id<Entity>,
    pub to_entity_id: Id<Entity>,
    pub relation_type: String,
    pub properties: Option<serde_json::Value>,
    pub source_event_ids: Option<Vec<Id<Event<JsonValue>>>>,
    pub confidence_score: Option<f64>,
    pub is_active: Option<bool>,
}

impl CreateEntityRelation {
    /// Create a new entity relation
    pub fn new(
        from_entity_id: Id<Entity>,
        to_entity_id: Id<Entity>,
        relation_type: impl Into<String>,
    ) -> Self {
        CreateEntityRelation {
            from_entity_id,
            to_entity_id,
            relation_type: relation_type.into(),
            properties: None,
            source_event_ids: None,
            confidence_score: None,
            is_active: None,
        }
    }

    /// Fluent method to set properties
    pub fn with_properties(mut self, properties: serde_json::Value) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Fluent method to set source event IDs
    pub fn with_source_event_ids(mut self, ids: Vec<Id<Event<JsonValue>>>) -> Self {
        self.source_event_ids = Some(ids);
        self
    }

    /// Fluent method to set confidence score
    pub fn with_confidence_score(mut self, score: f64) -> Self {
        self.confidence_score = Some(score);
        self
    }

    /// Fluent method to set is_active status
    pub fn with_is_active(mut self, active: bool) -> Self {
        self.is_active = Some(active);
        self
    }
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

impl<'a> EnhancedRepository<'a> for KnowledgeGraphRepository<'a> {
    type Table = Entities;
}

impl<'a> KnowledgeGraphRepository<'a> {
    /// Create a new entity
    pub async fn create_entity(&self, entity: CreateEntity) -> DbResult<EntityRecord> {
        self.create_entity_with_executor(self.pool, entity).await
    }

    /// Create a new entity with a specific executor (e.g. for transactions)
    pub async fn create_entity_with_executor<'e, E>(
        &self,
        executor: E,
        entity: CreateEntity,
    ) -> DbResult<EntityRecord>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let id = Id::<Entity>::new();
        let canonical_name = entity
            .canonical_name
            .unwrap_or_else(|| entity.name.to_lowercase().replace(' ', "_"));
        let aliases = entity.aliases.unwrap_or_default();
        let properties = entity
            .properties
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let source_event_ids = entity.source_event_ids.unwrap_or_default();
        let confidence_score = entity.confidence_score.unwrap_or(1.0);

        sqlx::query_as!(
            EntityRecord,
            r#"
            INSERT INTO core.entities (
                id, entity_type, name, canonical_name, aliases, properties, source_event_ids, confidence_score
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING 
                id::uuid as "id!: Id<Entity>",
                entity_type as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                properties as "properties!",
                source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                confidence_score as "confidence_score!",
                is_merged as "is_merged!",
                merged_into_id::uuid as "merged_into_id?: Id<Entity>",
                created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
            "#,
            *id.as_ulid() as _,
            entity.entity_type.as_str(),
            entity.name,
            canonical_name,
            &aliases,
            properties,
            source_event_ids
                .iter()
                .map(|id| *id.as_ulid())
                .collect::<Vec<_>>() as _,
            confidence_score
        )
        .fetch_one(executor)
        .await
        .map_err(|e| db_error(e, "create entity"))
    }

    /// Get an entity by ID
    pub async fn get_entity(&self, id: Id<Entity>) -> DbResult<Option<EntityRecord>> {
        sqlx::query_as!(
            EntityRecord,
            r#"
            SELECT 
                id::uuid as "id!: Id<Entity>",
                entity_type as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                properties as "properties!",
                source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                confidence_score as "confidence_score!",
                is_merged as "is_merged!",
                merged_into_id::uuid as "merged_into_id?: Id<Entity>",
                created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
            FROM core.entities
            WHERE id::uuid = $1
            "#,
            *id.as_ulid() as _
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get entity"))
    }

    /// Find entities by name or alias
    pub async fn find_entities_by_name(&self, name: &str) -> DbResult<Vec<EntityRecord>> {
        let normalized = name.to_lowercase();

        sqlx::query_as!(
            EntityRecord,
            r#"
            SELECT 
                id::uuid as "id!: Id<Entity>",
                entity_type as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                properties as "properties!",
                source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                confidence_score as "confidence_score!",
                is_merged as "is_merged!",
                merged_into_id::uuid as "merged_into_id?: Id<Entity>",
                created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
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
    ) -> DbResult<Vec<EntityRecord>> {
        let pattern = format!("%{}%", query.to_lowercase());
        let limit = limit.unwrap_or(100);

        match entity_type {
            Some(et) => {
                sqlx::query_as!(
                    EntityRecord,
                    r#"
                    SELECT 
                        id::uuid as "id!: Id<Entity>",
                        entity_type as "entity_type!",
                        name as "name!",
                        canonical_name as "canonical_name!",
                        aliases as "aliases!",
                        properties as "properties!",
                        source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                        confidence_score as "confidence_score!",
                        is_merged as "is_merged!",
                        merged_into_id::uuid as "merged_into_id?: Id<Entity>",
                        created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                        updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
                    FROM core.entities
                    WHERE 
                        entity_type = $3
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
                    EntityRecord,
                    r#"
                    SELECT 
                        id::uuid as "id!: Id<Entity>",
                        entity_type as "entity_type!",
                        name as "name!",
                        canonical_name as "canonical_name!",
                        aliases as "aliases!",
                        properties as "properties!",
                        source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                        confidence_score as "confidence_score!",
                        is_merged as "is_merged!",
                        merged_into_id::uuid as "merged_into_id?: Id<Entity>",
                        created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                        updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
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
        id: Id<Entity>,
        name: Option<String>,
        properties: Option<serde_json::Value>,
        aliases: Option<Vec<String>>,
        confidence_score: Option<f64>,
    ) -> DbResult<EntityRecord> {
        sqlx::query_as!(
            EntityRecord,
            r#"
            UPDATE core.entities
            SET 
                name = COALESCE($2, name),
                properties = COALESCE($3, properties),
                aliases = COALESCE($4, aliases),
                confidence_score = COALESCE($5, confidence_score),
                updated_at = NOW()
            WHERE id::uuid = $1
            RETURNING 
                id::uuid as "id!: Id<Entity>",
                entity_type as "entity_type!",
                name as "name!",
                canonical_name as "canonical_name!",
                aliases as "aliases!",
                properties as "properties!",
                source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                confidence_score as "confidence_score!",
                is_merged as "is_merged!",
                merged_into_id::uuid as "merged_into_id?: Id<Entity>",
                created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
            "#,
            *id.as_ulid() as _,
            name,
            properties,
            aliases.as_deref(),
            confidence_score
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "update entity"))
    }

    /// Merge one entity into another
    pub async fn merge_entities(
        &self,
        source_id: Id<Entity>,
        target_id: Id<Entity>,
    ) -> DbResult<()> {
        if source_id == target_id {
            return Err(SinexError::validation("Cannot merge an entity into itself"));
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin entity merge transaction"))?;
        set_repeatable_read(&mut tx).await?;

        let (first_id, second_id) = if source_id.to_uuid() <= target_id.to_uuid() {
            (source_id, target_id)
        } else {
            (target_id, source_id)
        };

        let first = fetch_entity_for_update(&mut tx, first_id).await?;
        let second = fetch_entity_for_update(&mut tx, second_id).await?;
        let (source, target) = if source_id == first.id {
            (first, second)
        } else {
            (second, first)
        };

        if source.entity_type != target.entity_type {
            return Err(SinexError::validation(
                "Cannot merge entities with different types",
            ));
        }

        if target.is_merged {
            return Err(SinexError::validation(
                "Cannot merge into an entity that is already merged",
            ));
        }

        if source.is_merged && source.merged_into_id != Some(target_id) {
            return Err(SinexError::validation(
                "Source entity is already merged into another target",
            ));
        }

        let (merged_aliases, aliases_added) = merge_aliases(&target, &source);
        let (merged_properties, conflicts) =
            merge_json_values(target.properties.clone(), source.properties.clone());
        let (merged_source_event_ids, source_event_ids_added) =
            merge_source_event_ids(&target, &source);
        let merged_confidence = target.confidence_score.max(source.confidence_score);
        let merged_created_at = if source.created_at < target.created_at {
            source.created_at
        } else {
            target.created_at
        };
        let merged_created_at_offset =
            Timestamp::from_unix_timestamp(merged_created_at.unix_timestamp())
                .unwrap_or(Timestamp::now());

        sqlx::query!(
            r#"
            UPDATE core.entities
            SET
                aliases = $2,
                properties = $3,
                source_event_ids = $4,
                confidence_score = $5,
                created_at = $6,
                updated_at = NOW()
            WHERE id::uuid = $1
            "#,
            *target.id.as_ulid() as _,
            &merged_aliases,
            merged_properties,
            merged_source_event_ids
                .iter()
                .map(|id| *id.as_ulid())
                .collect::<Vec<_>>() as _,
            merged_confidence,
            *merged_created_at_offset
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e, "update merged target entity"))?;

        sqlx::query!(
            r#"
            UPDATE core.entities
            SET is_merged = true,
                merged_into_id = $2,
                updated_at = NOW()
            WHERE id::uuid = $1
            "#,
            *source.id.as_ulid() as _,
            *target.id.as_ulid() as _
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e, "update merged source entity"))?;

        // Update all relations pointing to source to point to target
        sqlx::query!(
            r#"
            UPDATE core.entity_relations
            SET from_entity_id = $2
            WHERE from_entity_id::uuid = $1
            "#,
            *source.id.as_ulid() as _,
            *target.id.as_ulid() as _
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e, "update source relations"))?;

        sqlx::query!(
            r#"
            UPDATE core.entity_relations
            SET to_entity_id = $2
            WHERE to_entity_id::uuid = $1
            "#,
            *source.id.as_ulid() as _,
            *target.id.as_ulid() as _
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e, "update target relations"))?;

        let scope = serde_json::json!({
            "source_id": source.id.to_string(),
            "target_id": target.id.to_string(),
            "source_entity_type": source.entity_type,
            "target_entity_type": target.entity_type,
        });

        let summary = serde_json::json!({
            "merged_at": sinex_primitives::temporal::now(),
            "aliases_added": aliases_added,
            "source_event_ids_added": source_event_ids_added
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>(),
            "conflicts": conflicts,
            "resolution": "target_wins",
        });

        sqlx::query!(
            r#"
            INSERT INTO core.operations_log (
                operation_type,
                operator,
                scope,
                result_status,
                result_message,
                preview_summary
            ) VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            "entity_merge",
            "knowledge_graph",
            scope,
            "success",
            "merge complete",
            summary
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e, "log entity merge operation"))?;

        tx.commit()
            .await
            .map_err(|e| db_error(e, "commit entity merge transaction"))?;

        Ok(())
    }

    /// Create a new entity relation
    pub async fn create_relation(
        &self,
        relation: CreateEntityRelation,
    ) -> DbResult<EntityRelationRecord> {
        let id = Id::<EntityRelation>::new();
        let properties = relation
            .properties
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let source_event_ids = relation.source_event_ids.unwrap_or_default();
        let confidence_score = relation.confidence_score.unwrap_or(1.0);
        let is_active = relation.is_active.unwrap_or(true);

        sqlx::query_as!(
            EntityRelationRecord,
            r#"
            INSERT INTO core.entity_relations (
                id, from_entity_id, to_entity_id, relation_type, 
                properties, source_event_ids, confidence_score, is_active
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            RETURNING 
                id::uuid as "id!: Id<EntityRelation>",
                from_entity_id::uuid as "from_entity_id!: Id<Entity>",
                to_entity_id::uuid as "to_entity_id!: Id<Entity>",
                relation_type as "relation_type!",
                properties as "properties!",
                source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                confidence_score as "confidence_score!",
                is_active as "is_active!",
                created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
            "#,
            *id.as_ulid() as _,
            *relation.from_entity_id.as_ulid() as _,
            *relation.to_entity_id.as_ulid() as _,
            relation.relation_type,
            properties,
            source_event_ids
                .iter()
                .map(|id| *id.as_ulid())
                .collect::<Vec<_>>() as _,
            confidence_score,
            is_active
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "create relation"))
    }

    /// Get all relations for an entity
    pub async fn get_entity_relations(
        &self,
        entity_id: Id<Entity>,
        relation_type: Option<&str>,
        include_inactive: bool,
    ) -> DbResult<Vec<EntityRelationRecord>> {
        match (relation_type, include_inactive) {
            (Some(rt), false) => {
                sqlx::query_as!(
                    EntityRelationRecord,
                    r#"
                    SELECT 
                        id::uuid as "id!: Id<EntityRelation>",
                        from_entity_id::uuid as "from_entity_id!: Id<Entity>",
                        to_entity_id::uuid as "to_entity_id!: Id<Entity>",
                        relation_type as "relation_type!",
                        properties as "properties!",
                        source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                        confidence_score as "confidence_score!",
                        is_active as "is_active!",
                        created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                        updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
                    FROM core.entity_relations
                    WHERE 
                        (from_entity_id = $1 OR to_entity_id = $1)
                        AND relation_type = $2
                        AND is_active = true
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
                    EntityRelationRecord,
                    r#"
                    SELECT 
                        id::uuid as "id!: Id<EntityRelation>",
                        from_entity_id::uuid as "from_entity_id!: Id<Entity>",
                        to_entity_id::uuid as "to_entity_id!: Id<Entity>",
                        relation_type as "relation_type!",
                        properties as "properties!",
                        source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                        confidence_score as "confidence_score!",
                        is_active as "is_active!",
                        created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                        updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
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
                    EntityRelationRecord,
                    r#"
                    SELECT 
                        id::uuid as "id!: Id<EntityRelation>",
                        from_entity_id::uuid as "from_entity_id!: Id<Entity>",
                        to_entity_id::uuid as "to_entity_id!: Id<Entity>",
                        relation_type as "relation_type!",
                        properties as "properties!",
                        source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                        confidence_score as "confidence_score!",
                        is_active as "is_active!",
                        created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                        updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
                    FROM core.entity_relations
                    WHERE 
                        (from_entity_id = $1 OR to_entity_id = $1)
                        AND is_active = true
                    ORDER BY created_at DESC
                    "#,
                    *entity_id.as_ulid() as _
                )
                .fetch_all(self.pool)
                .await
            }
            (None, true) => {
                sqlx::query_as!(
                    EntityRelationRecord,
                    r#"
                    SELECT 
                        id::uuid as "id!: Id<EntityRelation>",
                        from_entity_id::uuid as "from_entity_id!: Id<Entity>",
                        to_entity_id::uuid as "to_entity_id!: Id<Entity>",
                        relation_type as "relation_type!",
                        properties as "properties!",
                        source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                        confidence_score as "confidence_score!",
                        is_active as "is_active!",
                        created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                        updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
                    FROM core.entity_relations
                    WHERE 
                        from_entity_id::uuid = $1 OR to_entity_id::uuid = $1
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

    /// Update relation active status
    pub async fn update_relation_status(
        &self,
        id: Id<EntityRelation>,
        is_active: bool,
    ) -> DbResult<EntityRelationRecord> {
        sqlx::query_as!(
            EntityRelationRecord,
            r#"
            UPDATE core.entity_relations
            SET is_active = $2, updated_at = NOW()
            WHERE id::uuid = $1
            RETURNING 
                id::uuid as "id!: Id<EntityRelation>",
                from_entity_id::uuid as "from_entity_id!: Id<Entity>",
                to_entity_id::uuid as "to_entity_id!: Id<Entity>",
                relation_type as "relation_type!",
                properties as "properties!",
                source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                confidence_score as "confidence_score!",
                is_active as "is_active!",
                created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
            "#,
            *id.as_ulid() as _,
            is_active
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "update relation status"))
    }

    /// Find paths between two entities
    pub async fn find_paths(
        &self,
        from_entity: Id<Entity>,
        to_entity: Id<Entity>,
        max_depth: i32,
    ) -> DbResult<Vec<Vec<EntityRelationRecord>>> {
        if max_depth <= 0 {
            return Ok(Vec::new());
        }

        let rows = sqlx::query!(
            r#"
            WITH RECURSIVE path AS (
                SELECT 
                    ARRAY[id::uuid] AS relation_ids,
                    from_entity_id,
                    to_entity_id,
                    1 AS depth
                FROM core.entity_relations
                WHERE from_entity_id::uuid = $1
                  AND is_active = true

                UNION ALL

                SELECT
                    path.relation_ids || rel.id::uuid,
                    rel.from_entity_id,
                    rel.to_entity_id,
                    path.depth + 1
                FROM path
                JOIN core.entity_relations rel
                  ON rel.from_entity_id = path.to_entity_id
                WHERE path.depth < $3
                  AND rel.is_active = true
                  AND NOT rel.id::uuid = ANY(path.relation_ids)
            )
            SELECT relation_ids as "relation_ids!: Vec<Ulid>"
            FROM path
            WHERE to_entity_id::uuid = $2
            "#,
            *from_entity.as_ulid() as _,
            *to_entity.as_ulid() as _,
            max_depth
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find paths"))?;

        let mut paths = Vec::new();
        for row in rows {
            let relation_ids = row.relation_ids;
            let records = sqlx::query_as!(
                EntityRelationRecord,
                r#"
                SELECT 
                    id::uuid as "id!: Id<EntityRelation>",
                    from_entity_id::uuid as "from_entity_id!: Id<Entity>",
                    to_entity_id::uuid as "to_entity_id!: Id<Entity>",
                    relation_type as "relation_type!",
                    properties as "properties!",
                    source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
                    confidence_score as "confidence_score!",
                    is_active as "is_active!",
                    created_at as "created_at!: sinex_primitives::temporal::Timestamp",
                    updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
                FROM core.entity_relations
                WHERE id::uuid = ANY($1)
                ORDER BY array_position($1, id::uuid)
                "#,
                &relation_ids as _
            )
            .fetch_all(self.pool)
            .await
            .map_err(|e| db_error(e, "load relation path"))?;

            paths.push(records);
        }

        Ok(paths)
    }

    /// Get entity statistics
    pub async fn get_entity_statistics(
        &self,
        entity_id: Id<Entity>,
    ) -> DbResult<serde_json::Value> {
        let stats = sqlx::query!(
            r#"
            WITH relation_counts AS (
                SELECT 
                    COUNT(CASE WHEN from_entity_id::uuid = $1 THEN 1 END) as outgoing_relations,
                    COUNT(CASE WHEN to_entity_id::uuid = $1 THEN 1 END) as incoming_relations,
                    COUNT(DISTINCT relation_type) as relation_types
                FROM core.entity_relations
                WHERE from_entity_id::uuid = $1 OR to_entity_id::uuid = $1
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
            WHERE e.id::uuid = $1
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

async fn fetch_entity_for_update(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Id<Entity>,
) -> DbResult<EntityRecord> {
    sqlx::query_as!(
        EntityRecord,
        r#"
        SELECT 
            id::uuid as "id!: Id<Entity>",
            entity_type as "entity_type!",
            name as "name!",
            canonical_name as "canonical_name!",
            aliases as "aliases!",
            properties as "properties!",
            source_event_ids::uuid[] as "source_event_ids!: Vec<Id<Event<JsonValue>>>",
            confidence_score as "confidence_score!",
            is_merged as "is_merged!",
            merged_into_id::uuid as "merged_into_id?: Id<Entity>",
            created_at as "created_at!: sinex_primitives::temporal::Timestamp",
            updated_at as "updated_at!: sinex_primitives::temporal::Timestamp"
        FROM core.entities
        WHERE id::uuid = $1
        FOR UPDATE
        "#,
        *id.as_ulid() as _
    )
    .fetch_one(tx.as_mut())
    .await
    .map_err(|e| db_error(e, "fetch entity for update"))
}

async fn set_repeatable_read(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> DbResult<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set repeatable read isolation"))?;
    Ok(())
}

fn merge_aliases(target: &EntityRecord, source: &EntityRecord) -> (Vec<String>, Vec<String>) {
    let mut merged = Vec::new();
    let mut added = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let mut push_alias = |alias: &str, track_added: bool| {
        let trimmed = alias.trim();
        if trimmed.is_empty() {
            return;
        }
        let value = trimmed.to_string();
        if seen.insert(value.clone()) {
            merged.push(value.clone());
            if track_added {
                added.push(value);
            }
        }
    };

    for alias in &target.aliases {
        push_alias(alias, false);
    }

    for alias in &source.aliases {
        push_alias(alias, true);
    }
    push_alias(&source.name, true);
    push_alias(&source.canonical_name, true);

    (merged, added)
}

fn merge_source_event_ids(
    target: &EntityRecord,
    source: &EntityRecord,
) -> (Vec<Id<Event<JsonValue>>>, Vec<Id<Event<JsonValue>>>) {
    let mut merged = Vec::new();
    let mut added = Vec::new();
    let mut seen = HashSet::new();

    for id in &target.source_event_ids {
        merged.push(id.clone());
        seen.insert(*id.as_ulid());
    }

    for id in &source.source_event_ids {
        if seen.insert(*id.as_ulid()) {
            merged.push(id.clone());
            added.push(id.clone());
        }
    }

    (merged, added)
}

fn merge_json_values(
    mut target: serde_json::Value,
    source: serde_json::Value,
) -> (serde_json::Value, Vec<serde_json::Value>) {
    let mut conflicts = Vec::new();
    merge_json_value_in_place(&mut target, &source, "", &mut conflicts);
    (target, conflicts)
}

fn merge_json_value_in_place(
    target: &mut serde_json::Value,
    source: &serde_json::Value,
    path: &str,
    conflicts: &mut Vec<serde_json::Value>,
) {
    match (target, source) {
        (serde_json::Value::Object(target_map), serde_json::Value::Object(source_map)) => {
            for (key, source_value) in source_map {
                let next_path = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path}.{key}")
                };
                match target_map.get_mut(key) {
                    Some(target_value) => {
                        merge_json_value_in_place(
                            target_value,
                            source_value,
                            &next_path,
                            conflicts,
                        );
                    }
                    None => {
                        target_map.insert(key.clone(), source_value.clone());
                    }
                }
            }
        }
        (serde_json::Value::Array(target_values), serde_json::Value::Array(source_values)) => {
            for source_value in source_values {
                if !target_values.contains(source_value) {
                    target_values.push(source_value.clone());
                }
            }
        }
        (target_value, source_value) => {
            if target_value != source_value {
                let conflict_path = if path.is_empty() { "<root>" } else { path };
                conflicts.push(serde_json::json!({
                    "path": conflict_path,
                    "target": target_value.clone(),
                    "source": source_value.clone(),
                    "resolution": "target",
                }));
            }
        }
    }
}

/// Extension trait for Entity terminal methods
pub trait EntityExt {
    /// Create the entity in the database
    async fn create(self, pool: &PgPool) -> DbResult<EntityRecord>;
}

impl EntityExt for CreateEntity {
    async fn create(self, pool: &PgPool) -> DbResult<EntityRecord> {
        KnowledgeGraphRepository::new(pool)
            .create_entity(self)
            .await
    }
}

/// Extension trait for EntityRelation terminal methods
pub trait EntityRelationExt {
    /// Create the entity relation in the database
    async fn create(self, pool: &PgPool) -> DbResult<EntityRelationRecord>;
}

impl EntityRelationExt for CreateEntityRelation {
    async fn create(self, pool: &PgPool) -> DbResult<EntityRelationRecord> {
        KnowledgeGraphRepository::new(pool)
            .create_relation(self)
            .await
    }
}
