//! Personal Knowledge Management (PKM) service

use crate::error::ServiceResult;
use sinex_db::{annotations, knowledge_graph, DbPool};
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
    
    /// Create knowledge graph entities from a provided list
    pub async fn create_entities_from_list(
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
    
}