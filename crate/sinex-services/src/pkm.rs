//! Personal Knowledge Management (PKM) service

use crate::error::ServiceResult;
use sinex_db::models::{CreateAnnotationInput, CreateEntityInput, CreateRelationInput};
use sinex_db::queries::SourceMaterialQueries;
use sinex_db::{annotations, knowledge_graph, DbPool};
use sinex_ulid::Ulid;
use std::collections::HashMap;
use tracing::{info, debug};

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

        info!(
            annotation_id = %annotation.annotation_id,
            event_id = %event_id,
            source_material_id = ?source_material_id,
            "Created note annotation with provenance"
        );

        Ok(annotation.annotation_id)
    }

    /// Create knowledge graph entities from source material
    pub async fn create_entities_from_source_material(
        &self,
        source_material_id: Ulid,
        entities: Vec<(String, String)>, // (name, type)
        created_by: &str,
    ) -> ServiceResult<Vec<Ulid>> {
        // Verify source material exists
        let source_material: Option<sinex_db::models::SourceMaterialRecord> = SourceMaterialQueries::get_by_id(source_material_id)
            .fetch_optional(&self.pool)
            .await?;

        if source_material.is_none() {
            return Err(crate::error::ServiceError::NotFound(
                format!("Source material {} not found", source_material_id)
            ));
        }

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
                        "source_material_id": source_material_id.to_string(),
                        "created_by": created_by,
                        "extraction_method": "manual",
                    })),
                },
            )
            .await?;

            entity_ids.push(entity.entity_id);
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

        let relationship = knowledge_graph::create_relation(
            &self.pool,
            CreateRelationInput {
                from_entity_id,
                to_entity_id,
                relation_type: relationship_type.to_string(),
                strength: None,
                metadata: Some(metadata),
                valid_from: None,
                valid_until: None,
                created_from_event_id: None,
            },
        )
        .await?;

        info!(
            relation_id = %relationship.relation_id,
            from_entity_id = %from_entity_id,
            to_entity_id = %to_entity_id,
            relationship_type = relationship_type,
            source_material_id = ?source_material_id,
            "Created entity relationship with provenance"
        );

        Ok(relationship.relation_id)
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
        let existing: Option<sinex_db::models::SourceMaterialRecord> = SourceMaterialQueries::find_by_checksum(checksum.clone())
            .fetch_optional(&self.pool)
            .await?;
        
        if let Some(existing) = existing 
        {
            debug!(
                blob_id = %existing.blob_id,
                "Source material already exists with same checksum"
            );
            return Ok(existing.blob_id);
        }

        // Create content preview (first 500 chars if text)
        let content_preview = if mime_type.map(|m| m.starts_with("text/")).unwrap_or(false) {
            String::from_utf8_lossy(&content[..content.len().min(500)]).to_string()
        } else {
            format!("[Binary content - {} bytes]", content.len())
        };

        // Insert new source material
        let source_material: sinex_db::models::SourceMaterialRecord = SourceMaterialQueries::insert(
            material_type.to_string(),
            source_uri.map(String::from),
            Some(content.len() as i64),
            Some(checksum),
            mime_type.map(String::from),
            None, // encoding - could be detected
            metadata,
            Some(content_preview),
        )
        .fetch_one(&self.pool)
        .await?;

        info!(
            blob_id = %source_material.blob_id,
            material_type = material_type,
            size_bytes = content.len(),
            "Registered new source material"
        );

        Ok(source_material.blob_id)
    }

    /// Register in-flight source material for Stage-as-You-Go pattern
    pub async fn register_in_flight_material(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: serde_json::Value,
    ) -> ServiceResult<Ulid> {
        let source_material: sinex_db::models::SourceMaterialRecord = SourceMaterialQueries::register_in_flight(
            material_type.to_string(),
            source_uri.map(String::from),
            metadata,
        )
        .fetch_one(&self.pool)
        .await?;

        info!(
            blob_id = %source_material.blob_id,
            material_type = material_type,
            "Registered in-flight source material"
        );

        Ok(source_material.blob_id)
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

        SourceMaterialQueries::finalize_in_flight(
            blob_id,
            content.len() as i64,
            checksum,
            mime_type.map(String::from),
            None, // encoding
            content_preview,
        )
        .execute(&self.pool)
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
        let materials: Vec<sinex_db::models::SourceMaterialRecord> = SourceMaterialQueries::get_recent(
            material_type.map(String::from),
            limit.or(Some(50)),
            None,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(materials.into_iter().map(|m| {
            serde_json::json!({
                "blob_id": m.blob_id.to_string(),
                "material_type": m.material_type,
                "source_uri": m.source_uri,
                "ingestion_time": m.ingestion_time,
                "file_size_bytes": m.file_size_bytes,
                "mime_type": m.mime_type,
                "metadata": m.metadata,
                "content_preview": m.content_preview,
            })
        }).collect())
    }

    /// Search source materials by metadata
    pub async fn search_materials_by_metadata(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> ServiceResult<Vec<serde_json::Value>> {
        let materials: Vec<sinex_db::models::SourceMaterialRecord> = SourceMaterialQueries::get_by_metadata(
            key.to_string(),
            value,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(materials.into_iter().map(|m| {
            serde_json::json!({
                "blob_id": m.blob_id.to_string(),
                "material_type": m.material_type,
                "source_uri": m.source_uri,
                "ingestion_time": m.ingestion_time,
                "metadata": m.metadata,
            })
        }).collect())
    }
}
