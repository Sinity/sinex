//! Personal Knowledge Management (PKM) service

use crate::error::{ServiceError, ServiceResult};
use sinex_db::{annotations, knowledge_graph, DbPool};
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
            annotations::CreateAnnotationInput {
                event_id,
                annotation_type: "note".to_string(),
                content: content.to_string(),
                metadata: Some(metadata),
                created_by: created_by.to_string(),
            },
        )
        .await?;
        
        Ok(annotation.id)
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
                knowledge_graph::CreateEntityInput {
                    name,
                    entity_type,
                    properties: serde_json::json!({}),
                    source_event_id: Some(event_id),
                },
            )
            .await?;
            
            entity_ids.push(entity.id);
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
        let relationship = knowledge_graph::create_relationship(
            &self.pool,
            knowledge_graph::CreateRelationshipInput {
                from_entity_id,
                to_entity_id,
                relationship_type: relationship_type.to_string(),
                properties: serde_json::json!(properties),
            },
        )
        .await?;
        
        Ok(relationship.id)
    }
    
    /// Search for similar events based on content
    pub async fn find_similar_events(
        &self,
        event_id: Ulid,
        limit: i32,
    ) -> ServiceResult<Vec<(Ulid, f64)>> {
        // This is a simplified implementation
        // In a real system, you'd use vector embeddings or full-text search
        
        let event = sqlx::query!(
            "SELECT source, event_type FROM raw.events WHERE id = $1::uuid",
            event_id.to_uuid()
        )
        .fetch_one(&self.pool)
        .await?;
        
        let similar = sqlx::query!(
            r#"
            SELECT id::text as event_id, 
                   CASE 
                     WHEN source = $1 AND event_type = $2 THEN 1.0
                     WHEN source = $1 THEN 0.8
                     WHEN event_type = $2 THEN 0.6
                     ELSE 0.4
                   END as similarity
            FROM raw.events
            WHERE id != $3::uuid
            ORDER BY similarity DESC
            LIMIT $4
            "#,
            event.source,
            event.event_type,
            event_id.to_uuid(),
            limit
        )
        .fetch_all(&self.pool)
        .await?;
        
        Ok(similar
            .into_iter()
            .filter_map(|r| match (r.event_id, r.similarity) {
                (Some(id), Some(sim)) => Ulid::from_string(&id).ok().map(|ulid| (ulid, sim)),
                _ => None,
            })
            .collect())
    }
}