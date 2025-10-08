//! Personal Knowledge Management (PKM) service

use crate::error::{Result as ServiceResult, SinexError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::db::repositories::source_materials::SourceMaterial;
use sinex_core::db::DbPool;
use sinex_core::types::ulid::Ulid;
use sinex_core::types::Id;
use sinex_core::{Entity as DbEntity, Event, JsonValue};

use sinex_core::{CreateEntity, CreateEntityRelation, DbPoolExt};
use std::collections::HashMap;
use tracing::{debug, info};

pub struct PkmService {
    pool: DbPool,
}

/// Material summary for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterialSummary {
    pub blob_id: String,
    pub material_type: String,
    pub source_uri: Option<String>,
    pub ingestion_time: chrono::DateTime<chrono::Utc>,
    pub file_size_bytes: Option<serde_json::Value>,
    pub mime_type: Option<serde_json::Value>,
    pub metadata: serde_json::Value,
    pub content_preview: Option<String>,
}

/// Metadata builder for consistent JSON structure
struct MetadataBuilder {
    data: serde_json::Map<String, serde_json::Value>,
}

impl MetadataBuilder {
    pub fn new() -> Self {
        Self {
            data: serde_json::Map::new(),
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.data.insert("tags".to_string(), json!(tags));
        self
    }

    pub fn with_created_at(mut self, timestamp: chrono::DateTime<chrono::Utc>) -> Self {
        self.data
            .insert("created_at".to_string(), json!(timestamp.to_rfc3339()));
        self
    }

    pub fn with_source_material_id(mut self, id: Option<Ulid>) -> Self {
        if let Some(id) = id {
            self.data
                .insert("source_material_id".to_string(), json!(id.to_string()));
        }
        self
    }

    pub fn with_created_by(mut self, created_by: &str) -> Self {
        self.data
            .insert("created_by".to_string(), json!(created_by));
        self
    }

    pub fn with_extraction_method(mut self, method: &str) -> Self {
        self.data
            .insert("extraction_method".to_string(), json!(method));
        self
    }

    pub fn build(self) -> serde_json::Value {
        serde_json::Value::Object(self.data)
    }
}

/// Entity type mapping with From trait for better maintainability
struct EntityTypeMapper;

impl EntityTypeMapper {
    pub fn create_entity_from_type(name: &str, entity_type: &str) -> CreateEntity {
        match entity_type {
            "person" => CreateEntity::person(name),
            "project" => CreateEntity::project(name),
            "topic" => CreateEntity::topic(name),
            "organization" => CreateEntity::organization(name),
            "location" => CreateEntity::location(name),
            "concept" => CreateEntity::concept(name),
            "tool" => CreateEntity::tool(name),
            "event" => CreateEntity::event(name),
            _ => CreateEntity::concept(name), // Default fallback
        }
    }
}

impl PkmService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Create a note annotation on an event with source material tracking
    pub async fn create_note(
        &self,
        event_id: Id<Event<JsonValue>>,
        content: &str,
        tags: Vec<String>,
        created_by: &str,
        source_material_id: Option<Ulid>,
    ) -> ServiceResult<Ulid> {
        let metadata = MetadataBuilder::new()
            .with_tags(tags)
            .with_created_at(chrono::Utc::now())
            .with_source_material_id(source_material_id)
            .build();

        let annotation = self
            .pool
            .events()
            .add_annotation(event_id.clone(), "note", content, metadata, created_by)
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

        let source_material = self
            .pool
            .source_materials()
            .get_by_id(source_material_id.into())
            .await?;

        if source_material.is_none() {
            return Err(SinexError::not_found(format!(
                "Source material {} not found",
                source_material_id
            ))
            .with_id("source_material_id", source_material_id));
        }

        let mut entity_ids = Vec::new();

        for (name, entity_type) in entities {
            let metadata = MetadataBuilder::new()
                .with_source_material_id(Some(source_material_id))
                .with_created_by(created_by)
                .with_extraction_method("manual")
                .build();

            let entity = EntityTypeMapper::create_entity_from_type(&name, &entity_type)
                .with_properties(serde_json::to_value(metadata)?);

            let entity = self.pool.knowledge_graph().create_entity(entity).await?;

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
        from_entity_id: Id<DbEntity>,
        to_entity_id: Id<DbEntity>,
        relationship_type: &str,
        properties: HashMap<String, serde_json::Value>,
        source_material_id: Option<Ulid>,
    ) -> ServiceResult<Ulid> {
        let mut metadata = serde_json::json!(properties);

        if let Some(sm_id) = source_material_id {
            metadata["source_material_id"] = serde_json::json!(sm_id.to_string());
        }

        let relationship = self
            .pool
            .knowledge_graph()
            .create_relation(
                CreateEntityRelation::new(
                    from_entity_id.clone(),
                    to_entity_id.clone(),
                    relationship_type,
                )
                .with_properties(serde_json::to_value(metadata)?),
            )
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
        // Calculate checksums
        let (blake3_checksum, _) = calculate_checksums(content);
        let checksum = blake3_checksum;

        // Check if blob already exists
        let existing_blob = self
            .pool
            .blobs()
            .find_by_blake3(&checksum)
            .await
            .map_err(|e| SinexError::service(format!("blob lookup failed: {}", e)))?;

        if let Some(blob) = existing_blob {
            // Check if there's a source material for this blob
            let existing_material = self
                .pool
                .source_materials()
                .find_by_blob_id(blob.id)
                .await?;

            if let Some(material) = existing_material {
                debug!(
                    source_material_id = %material.id,
                    "Source material already exists with same checksum"
                );
                return Ok(material.id.into());
            }
        }

        let content_preview = Self::create_safe_content_preview(content, mime_type);

        // Enhance metadata with file size, checksum, and mime type
        let mut enhanced_metadata = metadata;
        enhanced_metadata["file_size_bytes"] = json!(content.len() as i64);
        enhanced_metadata["checksum"] = json!(checksum);
        if let Some(mt) = mime_type {
            enhanced_metadata["mime_type"] = json!(mt);
        }

        // Insert new source material
        let new_material = match material_type {
            "file" => SourceMaterial::file(source_uri.unwrap_or("unknown")),
            "stream" => SourceMaterial::stream(source_uri.unwrap_or("unknown")),
            "blob" => SourceMaterial::blob(),
            "blob.binary" => SourceMaterial::blob_binary(source_uri.unwrap_or("binary")),
            "blob.text" => SourceMaterial::blob_text(source_uri.unwrap_or("text")),
            _ => SourceMaterial::blob(),
        }
        .with_metadata(enhanced_metadata)
        .with_content_preview(content_preview);

        let source_material = self
            .pool
            .source_materials()
            .register_material(new_material)
            .await?;

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
        let source_material = self
            .pool
            .source_materials()
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
        id: Ulid,
        content: &[u8],
        mime_type: Option<&str>,
    ) -> ServiceResult<()> {
        use sinex_core::Blob;

        let (blake3_checksum, sha256_checksum) = calculate_checksums(content);

        // Create a blob record
        let blob = Blob::builder()
            .annex_backend("SHA256E".to_string())
            .content_hash(sha256_checksum.clone())
            .original_filename("inline-content".to_string())
            .size_bytes(content.len() as i64)
            .mime_type(
                mime_type
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "application/octet-stream".to_string()),
            )
            .checksum_blake3(blake3_checksum)
            .build();

        // Insert the blob
        let inserted_blob = self
            .pool
            .blobs()
            .insert(blob)
            .await
            .map_err(|e| SinexError::service(format!("blob insert failed: {}", e)))?;

        let content_preview = if mime_type.map(|m| m.starts_with("text/")).unwrap_or(false) {
            Some(Self::create_safe_content_preview(content, mime_type))
        } else {
            None
        };

        self.pool
            .source_materials()
            .finalize_in_flight(
                id.into(),
                Some(inserted_blob.id), // blob_id
                None,                   // encoding
                content_preview,
                Some(content.len() as i64), // total_bytes
            )
            .await?;

        info!(
            blob_id = %id,
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
        let materials = self
            .pool
            .source_materials()
            .get_recent(limit.unwrap_or(50))
            .await?;

        // Filter by material_type if specified
        let filtered_materials = if let Some(filter_type) = material_type {
            materials
                .into_iter()
                .filter(|m| m.material_kind == filter_type)
                .collect()
        } else {
            materials
        };

        let summaries: Vec<MaterialSummary> = filtered_materials
            .into_iter()
            .map(|m| {
                let meta = m.metadata.clone();
                MaterialSummary {
                    blob_id: m.id.to_string(),
                    material_type: m.material_kind,
                    source_uri: Some(m.source_identifier),
                    ingestion_time: m.staged_at,
                    file_size_bytes: meta.get("file_size_bytes").cloned(),
                    mime_type: meta.get("mime_type").cloned(),
                    metadata: meta,
                    content_preview: None, // Field doesn't exist in SourceMaterialRecord
                }
            })
            .collect();

        Ok(summaries
            .into_iter()
            .map(|s| serde_json::to_value(s).unwrap())
            .collect())
    }

    /// Search source materials by metadata
    pub async fn search_materials_by_metadata(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> ServiceResult<Vec<serde_json::Value>> {
        let materials = self
            .pool
            .source_materials()
            .search_by_metadata(key, &value, None)
            .await?;

        let summaries: Vec<MaterialSummary> = materials
            .into_iter()
            .map(|m| MaterialSummary {
                blob_id: m.id.to_string(),
                material_type: m.material_kind,
                source_uri: Some(m.source_identifier),
                ingestion_time: m.staged_at,
                file_size_bytes: None,
                mime_type: None,
                metadata: m.metadata,
                content_preview: None,
            })
            .collect();

        Ok(summaries
            .into_iter()
            .map(|s| serde_json::to_value(s).unwrap())
            .collect())
    }

    /// Create a safe content preview with UTF-8 character boundary awareness
    fn create_safe_content_preview(content: &[u8], mime_type: Option<&str>) -> String {
        if mime_type.map(|m| m.starts_with("text/")).unwrap_or(false) {
            let max_chars = 500;
            // Convert to string and safely truncate at character boundaries
            let content_str = String::from_utf8_lossy(content);
            if content_str.chars().count() <= max_chars {
                content_str.into_owned()
            } else {
                let truncated: String = content_str.chars().take(max_chars).collect();
                format!("{}...", truncated)
            }
        } else {
            format!("[Binary content - {} bytes]", content.len())
        }
    }
}

/// Helper function to calculate both BLAKE3 and SHA256 checksums
fn calculate_checksums(content: &[u8]) -> (String, String) {
    let blake3_hash = blake3::hash(content).to_hex().to_string();
    let sha256_hash = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(content);
        format!("{:x}", hasher.finalize())
    };
    (blake3_hash, sha256_hash)
}
