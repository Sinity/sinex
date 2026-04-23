#![doc = include_str!("../docs/pkm.md")]

//! Personal Knowledge Management (PKM) orchestrator.

use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_db::DbPool;
use sinex_db::repositories::source_materials::SourceMaterial;
use sinex_db::repositories::state::Operation;
use sinex_primitives::Id;
use sinex_primitives::domain::{Entity as DbEntity, OperationStatus};
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::{Event, JsonValue};
use uuid::Uuid;

use sinex_db::repositories::DbPoolExt;
use sinex_db::repositories::{CreateEntity, CreateEntityRelation};
use std::collections::HashMap;
use std::time::{Duration, Instant};
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
    pub ingestion_time: sinex_primitives::Timestamp,
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
    pub(crate) fn new() -> Self {
        Self {
            data: serde_json::Map::new(),
        }
    }

    pub(crate) fn with_tags(mut self, tags: &[String]) -> Self {
        self.data.insert("tags".to_string(), json!(tags));
        self
    }

    pub(crate) fn with_created_at(mut self, timestamp: sinex_primitives::Timestamp) -> Self {
        self.data.insert(
            "created_at".to_string(),
            json!(sinex_primitives::temporal::format_rfc3339(timestamp)),
        );
        self
    }

    pub(crate) fn with_source_material_id(mut self, id: Option<Uuid>) -> Self {
        if let Some(id) = id {
            self.data
                .insert("source_material_id".to_string(), json!(id.to_string()));
        }
        self
    }

    pub(crate) fn with_created_by(mut self, created_by: &str) -> Self {
        self.data
            .insert("created_by".to_string(), json!(created_by));
        self
    }

    pub(crate) fn with_extraction_method(mut self, method: &str) -> Self {
        self.data
            .insert("extraction_method".to_string(), json!(method));
        self
    }

    pub(crate) fn build(self) -> serde_json::Value {
        serde_json::Value::Object(self.data)
    }
}

const CALLER_METADATA_KEY: &str = "caller_metadata";
const SYSTEM_METADATA_KEY: &str = "_system_metadata";
const PKM_ENTITY_CREATE_OPERATION: &str = "pkm.entity.create";
const PKM_ENTITY_LINK_OPERATION: &str = "pkm.entity.link";

fn attach_system_metadata(
    metadata: serde_json::Value,
    system_metadata: serde_json::Value,
) -> serde_json::Value {
    match metadata {
        serde_json::Value::Object(mut map) => {
            let key = if map.contains_key(SYSTEM_METADATA_KEY) {
                format!("{SYSTEM_METADATA_KEY}_generated")
            } else {
                SYSTEM_METADATA_KEY.to_string()
            };
            map.insert(key, system_metadata);
            serde_json::Value::Object(map)
        }
        serde_json::Value::Null => {
            json!({
                SYSTEM_METADATA_KEY: system_metadata
            })
        }
        other => {
            json!({
                CALLER_METADATA_KEY: other,
                SYSTEM_METADATA_KEY: system_metadata
            })
        }
    }
}

/// Entity type mapping with From trait for better maintainability
struct EntityTypeMapper;

impl EntityTypeMapper {
    const VALID_TYPES: [&'static str; 8] = [
        "person",
        "project",
        "topic",
        "organization",
        "location",
        "concept",
        "tool",
        "event",
    ];

    pub(crate) fn create_entity_from_type(name: &str, entity_type: &str) -> Result<CreateEntity> {
        let normalized = entity_type.trim().to_lowercase();
        if normalized.is_empty() {
            return Err(SinexError::validation("Entity type is required")
                .with_context("entity_type", entity_type));
        }

        let entity = match normalized.as_str() {
            "person" => CreateEntity::person(name),
            "project" => CreateEntity::project(name),
            "topic" => CreateEntity::topic(name),
            "organization" => CreateEntity::organization(name),
            "location" => CreateEntity::location(name),
            "concept" => CreateEntity::concept(name),
            "tool" => CreateEntity::tool(name),
            "event" => CreateEntity::event(name),
            _ => {
                return Err(SinexError::validation("Unknown entity type")
                    .with_context("entity_type", entity_type)
                    .with_context("allowed_types", Self::VALID_TYPES.join(", ")));
            }
        };

        Ok(entity)
    }
}

impl PkmService {
    #[must_use]
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    fn required_source_uri<'a>(
        material_type: &str,
        source_uri: Option<&'a str>,
    ) -> Result<&'a str> {
        source_uri
            .map(str::trim)
            .filter(|uri| !uri.is_empty())
            .ok_or_else(|| {
                SinexError::validation(format!(
                    "source_uri is required for source material type '{material_type}'"
                ))
            })
    }

    /// Create a note annotation on an event with source material tracking
    pub async fn create_note(
        &self,
        event_id: Id<Event<JsonValue>>,
        content: &str,
        tags: Vec<String>,
        created_by: &str,
        source_material_id: Option<Uuid>,
    ) -> Result<Uuid> {
        let metadata = MetadataBuilder::new()
            .with_tags(&tags)
            .with_created_at(sinex_primitives::temporal::now())
            .with_source_material_id(source_material_id)
            .build();

        let annotation = self
            .pool
            .events()
            .add_annotation(event_id, "note", content, metadata, created_by)
            .await?;

        info!(
            annotation_id = %annotation.id,
            event_id = %event_id,
            source_material_id = ?source_material_id,
            "Created note annotation with provenance"
        );

        Ok(*annotation.id.as_uuid())
    }

    /// Create knowledge graph entities from source material
    pub async fn create_entities_from_source_material(
        &self,
        source_material_id: Uuid,
        entities: Vec<(String, String)>, // (name, type)
        created_by: &str,
    ) -> Result<Vec<Uuid>> {
        // Verify source material exists
        let source_material = self
            .pool
            .source_materials()
            .get_by_id(source_material_id.into())
            .await?;

        if source_material.is_none() {
            return Err(SinexError::not_found(format!(
                "Source material {source_material_id} not found"
            ))
            .with_id("source_material_id", source_material_id));
        }

        let mut tx = self.pool.begin().await?;
        let mut entity_ids = Vec::new();

        for (name, entity_type) in entities {
            let metadata = MetadataBuilder::new()
                .with_source_material_id(Some(source_material_id))
                .with_created_by(created_by)
                .with_extraction_method("manual")
                .build();

            let entity = EntityTypeMapper::create_entity_from_type(&name, &entity_type)?
                .with_properties(serde_json::to_value(metadata)?);

            let entity = self
                .pool
                .knowledge_graph()
                .create_entity_with_executor(&mut *tx, entity)
                .await?;

            let entity_id = *entity.id.as_uuid();
            self.pool
                .state()
                .log_operation_with_executor(
                    &mut *tx,
                    Operation {
                        id: None,
                        operation_type: PKM_ENTITY_CREATE_OPERATION.to_string(),
                        operator: created_by.to_string(),
                        scope: Some(json!({
                            "entity_id": entity_id,
                            "source_material_id": source_material_id,
                            "payload": {
                                "name": entity.name,
                                "entity_type": entity.entity_type,
                                "properties": entity.properties,
                            }
                        })),
                        result_status: OperationStatus::Success,
                        result_message: None,
                        preview_summary: Some(json!({ "entity_id": entity_id })),
                        duration_ms: None,
                    },
                )
                .await?;

            entity_ids.push(entity_id);
        }

        tx.commit().await?;

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
        source_material_id: Option<Uuid>,
        created_by: &str,
    ) -> Result<Uuid> {
        let started = Instant::now();
        let metadata = serde_json::json!(properties);
        let mut system_metadata = serde_json::json!({});

        if let Some(sm_id) = source_material_id {
            system_metadata["source_material_id"] = serde_json::json!(sm_id.to_string());
        }

        let metadata = attach_system_metadata(metadata, system_metadata);
        let mut tx = self.pool.begin().await?;

        let relationship = self
            .pool
            .knowledge_graph()
            .create_relation_with_executor(
                &mut *tx,
                CreateEntityRelation::new(from_entity_id, to_entity_id, relationship_type)
                    .with_properties(serde_json::to_value(metadata)?),
            )
            .await?;
        let relation_id = *relationship.id.as_uuid();

        self.pool
            .state()
            .log_operation_with_executor(
                &mut *tx,
                Operation {
                    id: None,
                    operation_type: PKM_ENTITY_LINK_OPERATION.to_string(),
                    operator: created_by.to_string(),
                    scope: Some(json!({
                        "relation_id": relation_id,
                        "from_entity_id": from_entity_id,
                        "to_entity_id": to_entity_id,
                        "source_material_id": source_material_id,
                        "payload": {
                            "relationship_type": relationship_type,
                            "properties": relationship.properties,
                        }
                    })),
                    result_status: OperationStatus::Success,
                    result_message: None,
                    preview_summary: Some(json!({ "relation_id": relation_id })),
                    duration_ms: elapsed_ms(started.elapsed()),
                },
            )
            .await?;

        tx.commit().await?;

        info!(
            relation_id = %relation_id,
            from_entity_id = %from_entity_id,
            to_entity_id = %to_entity_id,
            relationship_type = relationship_type,
            source_material_id = ?source_material_id,
            created_by = created_by,
            "Created entity relationship with provenance"
        );

        Ok(relation_id)
    }

    /// Register external content as source material
    pub async fn register_source_material(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        content: &[u8],
        mime_type: Option<&str>,
        metadata: serde_json::Value,
    ) -> Result<Uuid> {
        // Calculate checksums
        let (blake3_checksum, _) = calculate_checksums(content);
        let checksum = blake3_checksum;

        // Check if blob already exists
        let existing_blob = self
            .pool
            .blobs()
            .find_by_blake3(&checksum)
            .await
            .map_err(|e| SinexError::service(format!("blob lookup failed: {e}")))?;

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
                return Ok(material.id);
            }
        }

        let content_preview = Self::create_safe_content_preview(content, mime_type);

        // Enhance metadata with file size, checksum, and mime type
        let mut system_metadata = serde_json::json!({});
        system_metadata["file_size_bytes"] = json!(content.len() as i64);
        system_metadata["checksum"] = json!(checksum);
        if let Some(mt) = mime_type {
            system_metadata["mime_type"] = json!(mt);
        }
        let enhanced_metadata = attach_system_metadata(metadata, system_metadata);

        // Insert new source material
        let new_material = match material_type {
            "file" => SourceMaterial::file(Self::required_source_uri(material_type, source_uri)?),
            "stream" => {
                SourceMaterial::stream(Self::required_source_uri(material_type, source_uri)?)
            }
            "blob" => SourceMaterial::blob(),
            "blob.binary" => {
                SourceMaterial::blob_binary(Self::required_source_uri(material_type, source_uri)?)
            }
            "blob.text" => {
                SourceMaterial::blob_text(Self::required_source_uri(material_type, source_uri)?)
            }
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

        Ok(source_material.id)
    }

    /// Register in-flight source material for Stage-as-You-Go pattern
    pub async fn register_in_flight_material(
        &self,
        material_type: &str,
        source_uri: Option<&str>,
        metadata: serde_json::Value,
    ) -> Result<Uuid> {
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

        Ok(source_material.id)
    }

    /// Finalize in-flight source material with actual content
    pub async fn finalize_in_flight_material(
        &self,
        id: Uuid,
        content: &[u8],
        mime_type: Option<&str>,
    ) -> Result<()> {
        use sinex_db::models::blob::Blob;

        let (blake3_checksum, sha256_checksum) = calculate_checksums(content);

        // Create a blob record
        let blob = Blob::builder()
            .storage_backend("SHA256E".to_string())
            .content_hash(sha256_checksum.clone())
            .original_filename("inline-content".to_string())
            .size_bytes(content.len() as i64)
            .mime_type(mime_type.map_or_else(
                || "application/octet-stream".to_string(),
                std::string::ToString::to_string,
            ))
            .checksum_blake3(blake3_checksum)
            .build();

        let mut tx = self.pool.begin().await?;

        // Insert the blob
        let inserted_blob = self
            .pool
            .blobs()
            .insert_with_executor(&mut *tx, blob)
            .await
            .map_err(|e| SinexError::service(format!("blob insert failed: {e}")))?;

        let content_preview = if mime_type.is_some_and(|m| m.starts_with("text/")) {
            Some(Self::create_safe_content_preview(content, mime_type))
        } else {
            None
        };

        self.pool
            .source_materials()
            .finalize_in_flight_with_executor(
                &mut *tx,
                id.into(),
                Some(inserted_blob.id), // blob_id
                None,                   // encoding
                content_preview,
                Some(content.len() as i64), // total_bytes
            )
            .await?;

        tx.commit().await?;

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
    ) -> Result<Vec<serde_json::Value>> {
        let limit = limit
            .unwrap_or(50)
            .clamp(1, sinex_primitives::Pagination::MAX_LIMIT);
        let materials = self
            .pool
            .source_materials()
            .get_recent_by_kind(material_type, limit)
            .await?;

        let summaries: Vec<MaterialSummary> = materials
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

        summaries
            .into_iter()
            .map(Self::material_summary_to_json)
            .collect::<Result<Vec<_>>>()
    }

    /// Search source materials by metadata
    pub async fn search_materials_by_metadata(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>> {
        let materials = self
            .pool
            .source_materials()
            .search_by_metadata(key, &value, Some(sinex_primitives::Pagination::MAX_LIMIT))
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

        summaries
            .into_iter()
            .map(Self::material_summary_to_json)
            .collect::<Result<Vec<_>>>()
    }

    /// Create a safe content preview with UTF-8 character boundary awareness
    fn create_safe_content_preview(content: &[u8], mime_type: Option<&str>) -> String {
        if mime_type.is_some_and(|m| m.starts_with("text/")) {
            let max_chars = 500;
            // Convert to string and safely truncate at character boundaries
            let content_str = String::from_utf8_lossy(content);
            match content_str.char_indices().nth(max_chars) {
                None => content_str.into_owned(),
                Some((byte_pos, _)) => format!("{}...", &content_str[..byte_pos]),
            }
        } else {
            format!("[Binary content - {} bytes]", content.len())
        }
    }

    fn material_summary_to_json(summary: MaterialSummary) -> Result<serde_json::Value> {
        serde_json::to_value(summary).map_err(|err| {
            SinexError::serialization("Failed to serialize material summary")
                .with_source(err.to_string())
        })
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

fn elapsed_ms(duration: Duration) -> Option<i32> {
    let millis = duration.as_millis().min(i32::MAX as u128);
    i32::try_from(millis).ok()
}
