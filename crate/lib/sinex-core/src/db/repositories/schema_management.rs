//! Simplified Event Payload Schema Management
//!
//! This module provides functionality for managing JSON schemas that define
//! the structure and validation rules for event payloads in the Sinex system.

use crate::db::db_error;
use crate::types::{Id, Ulid};
use crate::{DbResult, Event, JsonValue};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// Event payload schema record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPayloadSchema {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    pub schema_version: String,
    pub schema_content: JsonValue,
    pub content_hash: String,
    pub is_active: bool,
    pub updated_at: DateTime<Utc>,
}

/// Input for registering a new schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEventSchema {
    pub source: String,
    pub event_type: String,
    pub schema_version: String,
    pub schema_content: JsonValue,
}

impl NewEventSchema {
    /// Calculate the content hash for the schema
    pub fn calculate_content_hash(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.source.as_bytes());
        hasher.update(b":");
        hasher.update(self.event_type.as_bytes());
        hasher.update(b":");
        hasher.update(self.schema_version.as_bytes());
        hasher.update(b":");
        let serialized = serde_json::to_vec(&self.schema_content).unwrap();
        hasher.update(&serialized);
        hasher.finalize().to_hex().to_string()
    }
}

/// Schema validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<String>,
}

/// Validation error detail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub path: String,
    pub message: String,
    pub error_type: String,
}

/// Result of synchronizing code-generated schemas with the database
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct SchemaSyncResult {
    pub discovered: usize,
    pub created: usize,
    pub updated: usize,
    pub unchanged: usize,
}

/// Repository for event payload schema management
pub struct SchemaManagementRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> SchemaManagementRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Synchronize the discovered schemas (from inventory) with the database
    pub async fn sync_discovered_schemas<I>(
        &self,
        discovered: I,
    ) -> DbResult<SchemaSyncResult>
    where
        I: IntoIterator<Item = ((String, String, String), JsonValue)>,
    {
        let mut candidates = Vec::new();
        for ((source, event_type, version), schema_content) in discovered.into_iter() {
            candidates.push(SchemaCandidate::new(source, event_type, version, schema_content));
        }

        let discovered_count = candidates.len();
        let existing = self.load_active_schema_map().await?;

        let mut created = 0;
        let mut updated = 0;
        let mut unchanged = 0;

        for candidate in candidates {
            let key = candidate.key();
            if let Some(record) = existing.get(&key) {
                if record
                    .content_hash
                    .as_ref()
                    .map(|hash| hash == &candidate.content_hash)
                    .unwrap_or(false)
                {
                    unchanged += 1;
                } else {
                    self.update_existing_schema(record.id, &candidate).await?;
                    updated += 1;
                }
            } else {
                self.insert_new_schema(&candidate).await?;
                created += 1;
            }
        }

        Ok(SchemaSyncResult {
            discovered: discovered_count,
            created,
            updated,
            unchanged,
        })
    }

    /// Register a new event payload schema
    pub async fn register_schema(
        &self,
        new_schema: NewEventSchema,
    ) -> DbResult<EventPayloadSchema> {
        let id_ulid = sinex_schema::ulid::Ulid::new();
        let id_uuid = id_ulid.to_uuid();
        let NewEventSchema {
            source,
            event_type,
            schema_version,
            schema_content,
        } = new_schema;
        let content_hash = NewEventSchema {
            source: source.clone(),
            event_type: event_type.clone(),
            schema_version: schema_version.clone(),
            schema_content: schema_content.clone(),
        }
        .calculate_content_hash();

        // Check if this exact schema already exists
        if let Ok(existing) = self.find_schema_by_hash(&content_hash).await {
            return Ok(existing);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin schema registration transaction"))?;

        // Deactivate existing active schemas for this source/event_type
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.event_payload_schemas
            SET is_active = false, updated_at = NOW()
            WHERE source = $1
              AND event_type = $2
              AND is_active = true
            "#,
            source.as_str(),
            event_type.as_str()
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e, "deactivate previous schemas"))?;

        // Insert the new schema
        let row = sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas (
                id, source, event_type, schema_version, schema_content,
                content_hash, is_active
            ) VALUES (
                $1::uuid::ulid, $2, $3, $4, $5, $6, true
            )
            RETURNING 
                id as "id!: Ulid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at
            "#,
            id_uuid,
            source.as_str(),
            event_type.as_str(),
            schema_version,
            schema_content,
            content_hash
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| db_error(e, "register schema"))?;

        tx.commit()
            .await
            .map_err(|e| db_error(e, "commit schema registration transaction"))?;

        Ok(EventPayloadSchema {
            id: row.id,
            source: row.source,
            event_type: row.event_type,
            schema_version: row.schema_version,
            schema_content: row.schema_content,
            content_hash: row.content_hash,
            is_active: row.is_active,
            updated_at: row.updated_at,
        })
    }

    /// Find a schema by its content hash
    pub async fn find_schema_by_hash(&self, content_hash: &str) -> DbResult<EventPayloadSchema> {
        let row = sqlx::query!(
            r#"
            SELECT 
                id as "id!: Ulid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at
            FROM sinex_schemas.event_payload_schemas
            WHERE content_hash = $1
            "#,
            content_hash
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "find schema by hash"))?;

        Ok(EventPayloadSchema {
            id: row.id,
            source: row.source,
            event_type: row.event_type,
            schema_version: row.schema_version,
            schema_content: row.schema_content,
            content_hash: row.content_hash,
            is_active: row.is_active,
            updated_at: row.updated_at,
        })
    }

    /// Get the active schema for a source and event type
    pub async fn get_active_schema(
        &self,
        source: &str,
        event_type: &str,
    ) -> DbResult<EventPayloadSchema> {
        let row = sqlx::query!(
            r#"
            SELECT 
                id as "id!: Ulid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at
            FROM sinex_schemas.event_payload_schemas
            WHERE source = $1 AND event_type = $2 AND is_active = true
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
            source,
            event_type
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get active schema"))?;

        Ok(EventPayloadSchema {
            id: row.id,
            source: row.source,
            event_type: row.event_type,
            schema_version: row.schema_version,
            schema_content: row.schema_content,
            content_hash: row.content_hash,
            is_active: row.is_active,
            updated_at: row.updated_at,
        })
    }

    /// Get a schema by ID
    pub async fn get_schema_by_id(&self, schema_id: &Ulid) -> DbResult<EventPayloadSchema> {
        let row = sqlx::query!(
            r#"
            SELECT 
                id as "id!: Ulid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at
            FROM sinex_schemas.event_payload_schemas
            WHERE id = $1::uuid::ulid
            "#,
            schema_id.as_uuid()
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema by id"))?;

        Ok(EventPayloadSchema {
            id: row.id,
            source: row.source,
            event_type: row.event_type,
            schema_version: row.schema_version,
            schema_content: row.schema_content,
            content_hash: row.content_hash,
            is_active: row.is_active,
            updated_at: row.updated_at,
        })
    }

    /// List all schemas for a source
    pub async fn list_schemas_for_source(
        &self,
        source: &str,
        include_inactive: bool,
    ) -> DbResult<Vec<EventPayloadSchema>> {
        // Use a single query with conditional logic
        let rows = sqlx::query!(
            r#"
            SELECT 
                id as "id!: Ulid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at
            FROM sinex_schemas.event_payload_schemas
            WHERE source = $1 
                AND ($2 OR is_active = true)
            ORDER BY event_type, updated_at DESC
            "#,
            source,
            include_inactive
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list schemas for source"))?;

        Ok(rows
            .into_iter()
            .map(|row| EventPayloadSchema {
                id: row.id,
                source: row.source,
                event_type: row.event_type,
                schema_version: row.schema_version,
                schema_content: row.schema_content,
                content_hash: row.content_hash,
                is_active: row.is_active,
                updated_at: row.updated_at,
            })
            .collect())
    }

    /// Validate a typed event payload using its built-in source/type information
    pub async fn validate_typed_event<T>(&self, event: &Event<T>) -> DbResult<ValidationResult>
    where
        T: crate::types::events::EventPayload + serde::Serialize,
    {
        // For typed events, we can use the type's constants directly
        let schema = self
            .get_active_schema(T::SOURCE.as_str(), T::EVENT_TYPE.as_str())
            .await?;

        // Serialize the typed payload to JSON for validation
        let payload_json = serde_json::to_value(&event.payload).map_err(|e| {
            crate::repositories::common::db_error(
                sqlx::Error::Decode(Box::new(e)),
                "serialize typed payload",
            )
        })?;

        // Validate using jsonschema
        let result = match jsonschema::JSONSchema::compile(&schema.schema_content) {
            Ok(compiled) => match compiled.validate(&payload_json) {
                Ok(_) => ValidationResult {
                    is_valid: true,
                    errors: vec![],
                    warnings: vec![],
                },
                Err(errors) => {
                    let validation_errors: Vec<ValidationError> = errors
                        .map(|e| ValidationError {
                            path: e.instance_path.to_string(),
                            message: e.to_string(),
                            error_type: "schema_validation".to_string(),
                        })
                        .collect();

                    ValidationResult {
                        is_valid: false,
                        errors: validation_errors,
                        warnings: vec![],
                    }
                }
            },
            Err(e) => ValidationResult {
                is_valid: false,
                errors: vec![ValidationError {
                    path: "".to_string(),
                    message: format!("Invalid schema: {e}"),
                    error_type: "schema_error".to_string(),
                }],
                warnings: vec![],
            },
        };

        Ok(result)
    }

    /// Validate an event payload against a schema using basic JSON Schema validation
    pub async fn validate_event_payload(
        &self,
        event: &Event<JsonValue>,
        schema_id: Option<Ulid>,
    ) -> DbResult<ValidationResult> {
        // Get the appropriate schema
        let schema = if let Some(sid) = schema_id {
            self.get_schema_by_id(&sid).await?
        } else {
            self.get_active_schema(event.source.as_ref(), event.event_type.as_ref())
                .await?
        };

        // Basic validation using jsonschema crate
        let result = match jsonschema::JSONSchema::compile(&schema.schema_content) {
            Ok(compiled) => match compiled.validate(&event.payload) {
                Ok(_) => ValidationResult {
                    is_valid: true,
                    errors: vec![],
                    warnings: vec![],
                },
                Err(errors) => {
                    let validation_errors: Vec<ValidationError> = errors
                        .map(|e| ValidationError {
                            path: e.instance_path.to_string(),
                            message: e.to_string(),
                            error_type: "schema_validation".to_string(),
                        })
                        .collect();

                    ValidationResult {
                        is_valid: false,
                        errors: validation_errors,
                        warnings: vec![],
                    }
                }
            },
            Err(e) => ValidationResult {
                is_valid: false,
                errors: vec![ValidationError {
                    path: "".to_string(),
                    message: format!("Invalid schema: {e}"),
                    error_type: "schema_error".to_string(),
                }],
                warnings: vec![],
            },
        };

        Ok(result)
    }

    /// Deprecate a schema
    pub async fn deprecate_schema(&self, schema_id: &Ulid) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.event_payload_schemas
            SET is_active = false, updated_at = NOW()
            WHERE id = $1::uuid::ulid
            "#,
            schema_id.as_uuid()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "deprecate schema"))?;

        Ok(())
    }

    /// Get schema statistics
    pub async fn get_schema_statistics(&self) -> DbResult<SchemaStatistics> {
        let stats = sqlx::query!(
            r#"
            SELECT 
                COUNT(*) as total_schemas,
                COUNT(*) FILTER (WHERE is_active = true) as active_schemas,
                COUNT(DISTINCT source) as unique_sources,
                COUNT(DISTINCT event_type) as unique_event_types
            FROM sinex_schemas.event_payload_schemas
            "#
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema statistics"))?;

        Ok(SchemaStatistics {
            total_schemas: stats.total_schemas.unwrap_or(0) as u64,
            active_schemas: stats.active_schemas.unwrap_or(0) as u64,
            unique_sources: stats.unique_sources.unwrap_or(0) as u64,
            unique_event_types: stats.unique_event_types.unwrap_or(0) as u64,
        })
    }

    /// Associate a schema with an event
    pub async fn set_event_schema(
        &self,
        event_id: &Id<Event<JsonValue>>,
        schema_id: &Ulid,
    ) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE core.events 
            SET payload_schema_id = $1::uuid
            WHERE id = $2::uuid::ulid
            "#,
            schema_id.as_uuid(),
            event_id.as_ulid().as_uuid()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "set event schema"))?;

        Ok(())
    }

    async fn load_active_schema_map(
        &self,
    ) -> DbResult<HashMap<(String, String, String), SchemaRecord>> {
        let rows = sqlx::query!(
            r#"
            SELECT 
                id as "id: Ulid",
                source,
                event_type,
                schema_version,
                content_hash
            FROM sinex_schemas.event_payload_schemas
            WHERE is_active = true
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "load active schemas for sync"))?;

        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            map.insert(
                (row.source, row.event_type, row.schema_version),
                SchemaRecord {
                    id: row.id,
                    content_hash: row.content_hash.map(Into::into),
                },
            );
        }

        Ok(map)
    }

    async fn update_existing_schema(
        &self,
        schema_id: Ulid,
        candidate: &SchemaCandidate,
    ) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.event_payload_schemas
            SET schema_content = $1,
                content_hash = $2,
                updated_at = NOW()
            WHERE id = $3::uuid::ulid
            "#,
            &candidate.schema.schema_content,
            candidate.content_hash.as_str(),
            schema_id.as_uuid()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update schema content"))?;

        Ok(())
    }

    async fn insert_new_schema(&self, candidate: &SchemaCandidate) -> DbResult<Ulid> {
        let id = Ulid::new();

        sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas (
                id, source, event_type, schema_version, schema_content,
                content_hash, is_active, updated_at
            ) VALUES (
                $1::uuid::ulid, $2, $3, $4, $5, $6, true, NOW()
            )
            "#,
            id.as_uuid(),
            candidate.schema.source.as_str(),
            candidate.schema.event_type.as_str(),
            candidate.schema.schema_version.as_str(),
            &candidate.schema.schema_content,
            candidate.content_hash.as_str(),
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "insert new schema"))?;

        Ok(id)
    }
}

/// Schema statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaStatistics {
    pub total_schemas: u64,
    pub active_schemas: u64,
    pub unique_sources: u64,
    pub unique_event_types: u64,
}

#[derive(Debug, Clone)]
struct SchemaCandidate {
    schema: NewEventSchema,
    content_hash: String,
}

impl SchemaCandidate {
    fn new(
        source: String,
        event_type: String,
        schema_version: String,
        schema_content: JsonValue,
    ) -> Self {
        let schema = NewEventSchema {
            source,
            event_type,
            schema_version,
            schema_content,
        };
        let content_hash = schema.calculate_content_hash();
        Self { schema, content_hash }
    }

    fn key(&self) -> (String, String, String) {
        (
            self.schema.source.clone(),
            self.schema.event_type.clone(),
            self.schema.schema_version.clone(),
        )
    }
}

#[derive(Debug, Clone)]
struct SchemaRecord {
    id: Ulid,
    content_hash: Option<String>,
}
