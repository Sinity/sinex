//! Personal Knowledge Management (PKM) service

use crate::error::ServiceResult;
use sinex_core_types::ids::{EntityId, EventId};
use sinex_db::repositories::{
    EntityType, EventRepository, KnowledgeGraphRepository, NewEntity, NewEntityRelation,
    NewSourceMaterial, Repository, SourceMaterialRepository,
};
use sinex_db::DbPool;
use sinex_ulid::Ulid;
use std::collections::HashMap;
use tracing::{debug, info};

pub struct PkmService {
    pool: DbPool,
}

impl PkmService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Create a note annotation on an event with source material tracking
    pub async fn create_note(
        &self,
        event_id: Ulid,
        content: &str,
        tags: Vec<String>,
        created_by: &str,
        source_material_id: Option<Ulid>,
    ) -> ServiceResult<Ulid> {
        let metadata = serde_json::json!({
            "tags": tags,
            "created_at": chrono::Utc::now().to_rfc3339(),
            "source_material_id": source_material_id.map(|id| id.to_string()),
        });

        let event_repo = EventRepository::new(&self.pool);
        let annotation = event_repo
            .add_annotation(
                EventId::from_ulid(event_id),
                "note",
                content,
                metadata,
                created_by,
            )
            .await?;

        info!(
            annotation_id = %annotation.id,
            event_id = %event_id,
            source_material_id = ?source_material_id,
            "Created note annotation with provenance"
        );

        Ok(annotation.id.as_ulid().clone())
    }

    /// Create knowledge graph entities from source material
    pub async fn create_entities_from_source_material(
        &self,
        source_material_id: Ulid,
        entities: Vec<(String, String)>, // (name, type)
        created_by: &str,
    ) -> ServiceResult<Vec<Ulid>> {
        // Verify source material exists
        let repo = SourceMaterialRepository::new(&self.pool);
        let source_material = repo.get_by_id(source_material_id.into()).await?;

        if source_material.is_none() {
            return Err(crate::error::ServiceError::NotFound(format!(
                "Source material {} not found",
                source_material_id
            )));
        }

        let mut entity_ids = Vec::new();

        for (name, entity_type) in entities {
            let kg_repo = KnowledgeGraphRepository::new(&self.pool);
            let entity = kg_repo
                .create_entity(NewEntity {
                    entity_type: match entity_type.as_str() {
                        "person" => EntityType::Person,
                        "project" => EntityType::Project,
                        "topic" => EntityType::Topic,
                        "organization" => EntityType::Organization,
                        "location" => EntityType::Location,
                        "concept" => EntityType::Concept,
                        "tool" => EntityType::Tool,
                        "event" => EntityType::Event,
                        _ => EntityType::Concept,
                    },
                    name,
                    canonical_name: None,
                    aliases: None,
                    description: None,
                    metadata: Some(serde_json::json!({
                        "source_material_id": source_material_id.to_string(),
                        "created_by": created_by,
                        "extraction_method": "manual",
                    })),
                })
                .await?;

            entity_ids.push(entity.id.as_ulid().clone());
        }

        info!(
            source_material_id = %source_material_id,
            entity_count = entity_ids.len(),
            "Created entities from source material"
        );

        Ok(entity_ids)
    }

    /// Create relationships between entities with source material provenance
    pub async fn link_entities(
        &self,
        from_entity_id: Ulid,
        to_entity_id: Ulid,
        relationship_type: &str,
        properties: HashMap<String, serde_json::Value>,
        source_material_id: Option<Ulid>,
    ) -> ServiceResult<Ulid> {
        let mut metadata = serde_json::json!(properties);

        if let Some(sm_id) = source_material_id {
            metadata["source_material_id"] = serde_json::json!(sm_id.to_string());
        }

        let kg_repo = KnowledgeGraphRepository::new(&self.pool);
        let relationship = kg_repo
            .create_relation(NewEntityRelation {
                from_entity_id: EntityId::from_ulid(from_entity_id),
                to_entity_id: EntityId::from_ulid(to_entity_id),
                relation_type: relationship_type.to_string(),
                strength: None,
                metadata: Some(metadata),
                valid_from: None,
                valid_until: None,
                created_from_event_id: None,
            })
            .await?;

        info!(
            relation_id = %relationship.id,
            from_entity_id = %from_entity_id,
            to_entity_id = %to_entity_id,
            relationship_type = relationship_type,
            source_material_id = ?source_material_id,
            "Created entity relationship with provenance"
        );

        Ok(relationship.id.as_ulid().clone())
    }

    /// Register external content as source material
    pub async fn register_source_material(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        content: &[u8],
        mime_type: Option<&str>,
        metadata: serde_json::Value,
    ) -> ServiceResult<Ulid> {
        // Calculate checksum
        let checksum = blake3::hash(content).to_hex().to_string();

        // Check if already exists
        let repo = SourceMaterialRepository::new(&self.pool);
        let existing = repo.find_by_checksum(&checksum).await?;

        if let Some(existing) = existing {
            debug!(
                blob_id = %existing.id,
                "Source material already exists with same checksum"
            );
            return Ok(existing.id.into());
        }

        // Create content preview (first 500 chars if text)
        let content_preview = if mime_type.map(|m| m.starts_with("text/")).unwrap_or(false) {
            String::from_utf8_lossy(&content[..content.len().min(500)]).to_string()
        } else {
            format!("[Binary content - {} bytes]", content.len())
        };

        // Insert new source material
        let new_material = NewSourceMaterial {
            material_type: material_type.to_string(),
            source_uri: source_uri.map(String::from),
            file_size_bytes: Some(content.len() as i64),
            checksum_blake3: Some(checksum),
            mime_type: mime_type.map(String::from),
            encoding: None, // encoding - could be detected
            metadata: Some(metadata),
            content_preview: Some(content_preview),
            retention_policy: None,
        };
        let source_material = repo.register_material(new_material).await?;

        info!(
            blob_id = %source_material.id,
            material_type = material_type,
            size_bytes = content.len(),
            "Registered new source material"
        );

        Ok(source_material.id.into())
    }

    /// Register in-flight source material for Stage-as-You-Go pattern
    pub async fn register_in_flight_material(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: serde_json::Value,
    ) -> ServiceResult<Ulid> {
        let repo = SourceMaterialRepository::new(&self.pool);
        let source_material = repo
            .register_in_flight(material_type, source_uri, metadata)
            .await?;

        info!(
            blob_id = %source_material.id,
            material_type = material_type,
            "Registered in-flight source material"
        );

        Ok(source_material.id.into())
    }

    /// Finalize in-flight source material with actual content
    pub async fn finalize_in_flight_material(
        &self,
        blob_id: Ulid,
        content: &[u8],
        mime_type: Option<&str>,
    ) -> ServiceResult<()> {
        let checksum = blake3::hash(content).to_hex().to_string();

        let content_preview = if mime_type.map(|m| m.starts_with("text/")).unwrap_or(false) {
            Some(String::from_utf8_lossy(&content[..content.len().min(500)]).to_string())
        } else {
            None
        };

        let repo = SourceMaterialRepository::new(&self.pool);
        repo.finalize_in_flight(
            blob_id.into(),
            content.len() as i64,
            checksum,
            mime_type,
            None, // encoding
            content_preview,
        )
        .await?;

        info!(
            blob_id = %blob_id,
            size_bytes = content.len(),
            "Finalized in-flight source material"
        );

        Ok(())
    }

    /// Get recent source materials for PKM context
    pub async fn get_recent_source_materials(
        &self,
        material_type: Option<&str>,
        limit: Option<i64>,
    ) -> ServiceResult<Vec<serde_json::Value>> {
        let repo = SourceMaterialRepository::new(&self.pool);
        let materials = repo.get_recent(limit.unwrap_or(50)).await?;

        // Filter by material_type if specified
        let filtered_materials = if let Some(filter_type) = material_type {
            materials
                .into_iter()
                .filter(|m| m.material_type == filter_type)
                .collect()
        } else {
            materials
        };

        Ok(filtered_materials
            .into_iter()
            .map(|m| {
                serde_json::json!({
                    "blob_id": m.id.to_string(),
                    "material_type": m.material_type,
                    "source_uri": m.source_uri,
                    "ingestion_time": m.ingestion_time,
                    "file_size_bytes": m.file_size_bytes,
                    "mime_type": m.mime_type,
                    "metadata": m.metadata,
                    "content_preview": m.content_preview,
                })
            })
            .collect())
    }

    /// Search source materials by metadata
    pub async fn search_materials_by_metadata(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> ServiceResult<Vec<serde_json::Value>> {
        let repo = SourceMaterialRepository::new(&self.pool);
        let materials = repo.search_by_metadata(key, &value, None).await?;

        Ok(materials
            .into_iter()
            .map(|m| {
                serde_json::json!({
                    "blob_id": m.id.to_string(),
                    "material_type": m.material_type,
                    "source_uri": m.source_uri,
                    "ingestion_time": m.ingestion_time,
                    "metadata": m.metadata,
                })
            })
            .collect())
    }
}
