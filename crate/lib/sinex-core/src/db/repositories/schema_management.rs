//! Simplified Event Payload Schema Management
//!
//! This module provides functionality for managing JSON schemas that define
//! the structure and validation rules for event payloads in the Sinex system.

use crate::db::db_error;
use crate::types::Id;
use crate::{DbResult, Event, JsonValue};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// Event payload schema record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPayloadSchema {
    pub id: String, // Store as string to avoid ULID type issues
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
        let content = format!(
            "{}:{}:{}:{}",
            self.source,
            self.event_type,
            self.schema_version,
            serde_json::to_string(&self.schema_content).unwrap()
        );

        // Simple hash using std::hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        format!("{:x}", hasher.finish())
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

/// Repository for event payload schema management
pub struct SchemaManagementRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> SchemaManagementRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Register a new event payload schema
    pub async fn register_schema(
        &self,
        new_schema: NewEventSchema,
    ) -> DbResult<EventPayloadSchema> {
        let id = sinex_schema::ulid::Ulid::new().to_string();
        let content_hash = new_schema.calculate_content_hash();

        // Check if this exact schema already exists
        if let Ok(existing) = self.find_schema_by_hash(&content_hash).await {
            return Ok(existing);
        }

        // Deactivate existing active schemas for this source/event_type
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.event_payload_schemas
            SET is_active = false, updated_at = NOW()
            WHERE source = $1 AND event_type = $2 AND is_active = true
            "#,
            new_schema.source,
            new_schema.event_type
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "deactivate existing schemas"))?;

        // Insert the new schema
        let row = sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas (
                id, source, event_type, schema_version, schema_content,
                content_hash, is_active
            ) VALUES (
                $1::text::uuid, $2, $3, $4, $5, $6, true
            )
            RETURNING 
                id::text as id,
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at
            "#,
            id,
            new_schema.source,
            new_schema.event_type,
            new_schema.schema_version,
            new_schema.schema_content,
            content_hash
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "register schema"))?;

        Ok(EventPayloadSchema {
            id: row.id.unwrap_or_default(),
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
                id::text as id,
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
            id: row.id.unwrap_or_default(),
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
                id::text as id,
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
            id: row.id.unwrap_or_default(),
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
    pub async fn get_schema_by_id(&self, schema_id: &str) -> DbResult<EventPayloadSchema> {
        let row = sqlx::query!(
            r#"
            SELECT 
                id::text as id,
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at
            FROM sinex_schemas.event_payload_schemas
            WHERE id::text = $1
            "#,
            schema_id
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema by id"))?;

        Ok(EventPayloadSchema {
            id: row.id.unwrap_or_default(),
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
                id::text as id,
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
            .filter_map(|row| {
                row.id.map(|id| EventPayloadSchema {
                    id,
                    source: row.source,
                    event_type: row.event_type,
                    schema_version: row.schema_version,
                    schema_content: row.schema_content,
                    content_hash: row.content_hash,
                    is_active: row.is_active,
                    updated_at: row.updated_at,
                })
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
                    message: format!("Invalid schema: {}", e),
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
        schema_id: Option<String>,
    ) -> DbResult<ValidationResult> {
        // Get the appropriate schema
        let schema = if let Some(sid) = schema_id {
            self.get_schema_by_id(&sid).await?
        } else {
            self.get_active_schema(&event.source.to_string(), &event.event_type.to_string())
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
                    message: format!("Invalid schema: {}", e),
                    error_type: "schema_error".to_string(),
                }],
                warnings: vec![],
            },
        };

        Ok(result)
    }

    /// Deprecate a schema
    pub async fn deprecate_schema(&self, schema_id: &str) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.event_payload_schemas
            SET is_active = false, updated_at = NOW()
            WHERE id::text = $1
            "#,
            schema_id
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
        schema_id: &str,
    ) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE core.events 
            SET payload_schema_id = $1::text::uuid
            WHERE id = $2::uuid::ulid
            "#,
            schema_id,
            event_id.as_ulid().as_uuid()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "set event schema"))?;

        Ok(())
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
