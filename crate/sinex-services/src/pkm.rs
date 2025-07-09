//! Personal Knowledge Management (PKM) service

use crate::error::ServiceResult;
use sinex_db::{annotations, knowledge_graph, DbPool, ulid_to_uuid};
use sinex_db::models::{CreateAnnotationInput, CreateEntityInput, CreateRelationInput};
use sinex_ulid::Ulid;
use std::collections::HashMap;

pub struct PkmService {
    pool: DbPool,
}

impl PkmService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
    
    /// Create a note annotation on an event
    pub async fn create_note(
        &self,
        event_id: Ulid,
        content: &str,
        tags: Vec<String>,
        created_by: &str,
    ) -> ServiceResult<Ulid> {
        let metadata = serde_json::json!({
            "tags": tags,
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        
        let annotation = annotations::create_annotation(
            &self.pool,
            CreateAnnotationInput {
                event_id,
                annotation_type: "note".to_string(),
                content: content.to_string(),
                metadata: Some(metadata),
                created_by: created_by.to_string(),
            },
        )
        .await?;
        
        Ok(annotation.annotation_id)
    }
    
    /// Create knowledge graph entities from event
    pub async fn extract_entities(
        &self,
        event_id: Ulid,
        entities: Vec<(String, String)>, // (name, type)
    ) -> ServiceResult<Vec<Ulid>> {
        let mut entity_ids = Vec::new();
        
        for (name, entity_type) in entities {
            let entity = knowledge_graph::create_entity(
                &self.pool,
                CreateEntityInput {
                    entity_type,
                    name,
                    canonical_name: None,
                    aliases: None,
                    description: None,
                    metadata: Some(serde_json::json!({
                        "source_event_id": event_id.to_string()
                    })),
                },
            )
            .await?;
            
            entity_ids.push(entity.entity_id);
        }
        
        Ok(entity_ids)
    }
    
    /// Create relationships between entities
    pub async fn link_entities(
        &self,
        from_entity_id: Ulid,
        to_entity_id: Ulid,
        relationship_type: &str,
        properties: HashMap<String, serde_json::Value>,
    ) -> ServiceResult<Ulid> {
        let relationship = knowledge_graph::create_relation(
            &self.pool,
            CreateRelationInput {
                from_entity_id,
                to_entity_id,
                relation_type: relationship_type.to_string(),
                strength: None,
                metadata: Some(serde_json::json!(properties)),
                valid_from: None,
                valid_until: None,
                created_from_event_id: None,
            },
        )
        .await?;
        
        Ok(relationship.relation_id)
    }
    
    /// Search for similar events based on content
    pub async fn find_similar_events(
        &self,
        event_id: Ulid,
        limit: i32,
    ) -> ServiceResult<Vec<(Ulid, f64)>> {
        // This is a simplified implementation
        // In a real system, you'd use vector embeddings or full-text search
        
        let event_uuid = ulid_to_uuid(event_id);
        let event = sqlx::query!(
            "SELECT source, event_type FROM raw.events WHERE id::uuid = $1",
            event_uuid
        )
        .fetch_one(&self.pool)
        .await?;
        
        let similar = sqlx::query!(
            r#"
            SELECT id::text as event_id, 
                   CASE 
                     WHEN source = $1 AND event_type = $2 THEN 1.0::float8
                     WHEN source = $1 THEN 0.8::float8
                     WHEN event_type = $2 THEN 0.6::float8
                     ELSE 0.4::float8
                   END as "similarity!"
            FROM raw.events
            WHERE id::uuid != $3
            ORDER BY CASE 
                     WHEN source = $1 AND event_type = $2 THEN 1.0::float8
                     WHEN source = $1 THEN 0.8::float8
                     WHEN event_type = $2 THEN 0.6::float8
                     ELSE 0.4::float8
                   END DESC
            LIMIT $4
            "#,
            event.source,
            event.event_type,
            event_uuid,
            limit as i64
        )
        .fetch_all(&self.pool)
        .await?;
        
        Ok(similar
            .into_iter()
            .filter_map(|r| {
                r.event_id.and_then(|id| id.parse::<Ulid>().ok().map(|ulid| (ulid, r.similarity)))
            })
            .collect())
    }
}