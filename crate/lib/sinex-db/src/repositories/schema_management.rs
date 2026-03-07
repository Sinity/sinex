//! Simplified Event Payload Schema Management
//!
//! This module provides functionality for managing JSON schemas that define
//! the structure and validation rules for event payloads in the Sinex system.

use crate::db_error;
use crate::repositories::events::EventPayloadSchema;
use crate::{DbResult, Event, JsonValue};
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::{EventSource, EventType, SchemaVersion};
use sinex_primitives::error::SinexError;
use sinex_primitives::{Id, Timestamp};
use sqlx::PgPool;
use uuid::Uuid;

/// Input structure for registering a new event payload schema.
///
/// Used to capture schema information from code generation or external sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEventSchema {
    /// Event source identifier
    pub source: EventSource,
    /// Event type identifier
    pub event_type: EventType,
    /// Semantic version of the schema
    pub schema_version: String,
    /// JSON Schema content for validating event payloads
    pub schema_content: JsonValue,
}

impl NewEventSchema {
    /// Calculate the content hash for the schema
    pub fn calculate_content_hash(&self) -> Result<String, sinex_primitives::error::SinexError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.source.as_str().as_bytes());
        hasher.update(b":");
        hasher.update(self.event_type.as_str().as_bytes());
        hasher.update(b":");
        hasher.update(self.schema_version.as_bytes());
        hasher.update(b":");
        let serialized = serde_json::to_vec(&self.schema_content).map_err(|e| {
            sinex_primitives::error::SinexError::validation(format!(
                "Failed to serialize schema content for hashing: {e}"
            ))
        })?;
        hasher.update(&serialized);
        Ok(hasher.finalize().to_hex().to_string())
    }
}

/// Result of validating a payload against a JSON schema.
///
/// Contains detailed information about any schema validation failures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether the payload is valid according to the schema
    pub is_valid: bool,
    /// List of validation errors found
    pub errors: Vec<ValidationError>,
    /// Non-fatal warnings about the payload
    pub warnings: Vec<String>,
}

/// Details of a single schema validation error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// JSON path where the error occurred (e.g., "$.field.subfield")
    pub path: String,
    /// Human-readable error message
    pub message: String,
    /// Type of validation error (e.g., "`schema_validation`", "`type_error`")
    pub error_type: String,
}

/// Result of synchronizing code-generated schemas with the database.
///
/// Tracks how many schemas were discovered, created, updated, or remained unchanged.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct SchemaSyncResult {
    /// Number of schemas discovered from code
    pub discovered: usize,
    /// Number of new schemas created in the database
    pub created: usize,
    /// Number of existing schemas updated with new content
    pub updated: usize,
    /// Number of schemas that were already in sync
    pub unchanged: usize,
}

/// Repository for event payload schema management
pub struct SchemaManagementRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> SchemaManagementRepository<'a> {
    #[must_use] 
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Synchronize the discovered schemas (from inventory) with the database
    pub async fn sync_discovered_schemas<I>(&self, discovered: I) -> DbResult<SchemaSyncResult>
    where
        I: IntoIterator<Item = ((String, String, String), JsonValue)>,
    {
        let mut candidates = Vec::new();
        for ((source, event_type, version), schema_content) in discovered {
            candidates.push(SchemaCandidate::new(
                source,
                event_type,
                version,
                schema_content,
            )?);
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
                    .is_some_and(|hash| hash == &candidate.content_hash)
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
        let NewEventSchema {
            source,
            event_type,
            schema_version,
            schema_content,
        } = new_schema;

        // Validate schema version format (must be semver X.Y.Z)
        SchemaVersion::new(&schema_version)
            .validate()
            .map_err(|e| {
                SinexError::validation(format!("Invalid schema version '{schema_version}': {e}"))
            })?;

        let content_hash = NewEventSchema {
            source: source.clone(),
            event_type: event_type.clone(),
            schema_version: schema_version.clone(),
            schema_content: schema_content.clone(),
        }
        .calculate_content_hash()?;

        // Check if this exact schema already exists
        if let Ok(existing) = self.find_schema_by_hash(&content_hash).await {
            if existing.is_active {
                return Ok(existing);
            }

            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| db_error(e, "begin schema reactivation transaction"))?;
            set_repeatable_read(&mut tx).await?;

            sqlx::query!(
                r#"
                UPDATE sinex_schemas.event_payload_schemas
                SET is_active = false, updated_at = NOW()
                WHERE source = $1
                  AND event_type = $2
                  AND is_active = true
                "#,
                existing.source.as_str(),
                existing.event_type.as_str()
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e, "deactivate conflicting schemas"))?;

            let row = sqlx::query!(
                r#"
                UPDATE sinex_schemas.event_payload_schemas
                SET is_active = true, updated_at = NOW()
                WHERE id = $1::uuid
                RETURNING 
                    id as "id!: Uuid",
                    source,
                    event_type,
                    schema_version,
                    schema_content,
                    content_hash,
                    is_active,
                    updated_at as "updated_at: Timestamp"
                "#,
                existing.id.to_uuid()
            )
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| db_error(e, "reactivate schema"))?;

            tx.commit()
                .await
                .map_err(|e| db_error(e, "commit schema reactivation transaction"))?;

            return Ok(EventPayloadSchema {
                id: Id::from_uuid(row.id),
                source: row.source.into(),
                event_type: row.event_type.into(),
                schema_version: SchemaVersion::new(row.schema_version),
                schema_content: row.schema_content,
                content_hash: row.content_hash,
                is_active: row.is_active,
                updated_at: row.updated_at,
            });
        }

        let id_uuid = uuid::Uuid::now_v7();

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error(e, "begin schema registration transaction"))?;
        set_repeatable_read(&mut tx).await?;

        let existing_version = sqlx::query!(
            r#"
            SELECT content_hash
            FROM sinex_schemas.event_payload_schemas
            WHERE source = $1
              AND event_type = $2
              AND schema_version = $3
            "#,
            source.as_str(),
            event_type.as_str(),
            schema_version.as_str()
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| db_error(e, "check schema version conflict"))?;

        if let Some(row) = existing_version
            && row.content_hash != content_hash {
                return Err(SinexError::validation(format!(
                    "schema version already exists for {source}/{event_type} at {schema_version}"
                )));
            }

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
                $1::uuid, $2, $3, $4, $5, $6, true
            )
            RETURNING 
                id as "id!: Uuid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at as "updated_at: Timestamp"
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
            id: Id::from_uuid(row.id),
            source: row.source.into(),
            event_type: row.event_type.into(),
            schema_version: SchemaVersion::new(row.schema_version),
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
                id as "id!: Uuid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at as "updated_at: Timestamp"
            FROM sinex_schemas.event_payload_schemas
            WHERE content_hash = $1
            "#,
            content_hash
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "find schema by hash"))?;

        Ok(EventPayloadSchema {
            id: Id::from_uuid(row.id),
            source: row.source.into(),
            event_type: row.event_type.into(),
            schema_version: SchemaVersion::new(row.schema_version),
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
                id as "id!: Uuid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at as "updated_at: Timestamp"
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
            id: Id::from_uuid(row.id),
            source: row.source.into(),
            event_type: row.event_type.into(),
            schema_version: SchemaVersion::new(row.schema_version),
            schema_content: row.schema_content,
            content_hash: row.content_hash,
            is_active: row.is_active,
            updated_at: row.updated_at,
        })
    }

    /// Get a schema by ID
    pub async fn get_schema_by_id(&self, schema_id: &Uuid) -> DbResult<EventPayloadSchema> {
        let row = sqlx::query!(
            r#"
            SELECT 
                id as "id!: Uuid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at as "updated_at: Timestamp"
            FROM sinex_schemas.event_payload_schemas
            WHERE id = $1::uuid
            "#,
            schema_id
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "get schema by id"))?;

        Ok(EventPayloadSchema {
            id: Id::from_uuid(row.id),
            source: row.source.into(),
            event_type: row.event_type.into(),
            schema_version: SchemaVersion::new(row.schema_version),
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
                id as "id!: Uuid",
                source,
                event_type,
                schema_version,
                schema_content,
                content_hash,
                is_active,
                updated_at as "updated_at: Timestamp"
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
                id: Id::from_uuid(row.id),
                source: row.source.into(),
                event_type: row.event_type.into(),
                schema_version: SchemaVersion::new(row.schema_version),
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
        T: sinex_primitives::events::EventPayload + serde::Serialize,
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
        let result = match jsonschema::validator_for(&schema.schema_content) {
            Ok(validator) => {
                let validation_errors: Vec<ValidationError> = validator
                    .iter_errors(&payload_json)
                    .map(|e| ValidationError {
                        path: e.instance_path().to_string(),
                        message: e.to_string(),
                        error_type: "schema_validation".to_string(),
                    })
                    .collect();

                if validation_errors.is_empty() {
                    ValidationResult {
                        is_valid: true,
                        errors: vec![],
                        warnings: vec![],
                    }
                } else {
                    ValidationResult {
                        is_valid: false,
                        errors: validation_errors,
                        warnings: vec![],
                    }
                }
            }
            Err(e) => ValidationResult {
                is_valid: false,
                errors: vec![ValidationError {
                    path: String::new(),
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
        schema_id: Option<Uuid>,
    ) -> DbResult<ValidationResult> {
        // Get the appropriate schema
        let schema = if let Some(sid) = schema_id {
            self.get_schema_by_id(&sid).await?
        } else {
            self.get_active_schema(event.source.as_ref(), event.event_type.as_ref())
                .await?
        };

        let resolved_schema_id = schema_id.unwrap_or_else(|| *schema.id.as_uuid());
        if let Some(event_id) = event.id.as_ref().map(|id| *id.as_uuid())
            && let Some(cached) = self
                .fetch_cached_validation(&event_id, &resolved_schema_id)
                .await?
            {
                return Ok(cached);
            }

        let result = Self::run_json_validation(&schema.schema_content, &event.payload);

        if let Some(event_id) = event.id.as_ref().map(|id| *id.as_uuid()) {
            self.store_validation_cache(&event_id, &resolved_schema_id, &result)
                .await?;
        }

        Ok(result)
    }

    /// Validate and cache payloads directly by event ID.
    pub async fn validate_event_payload_by_event_id(
        &self,
        event_id: &Uuid,
    ) -> DbResult<ValidationResult> {
        let event_row = sqlx::query!(
            r#"
            SELECT
                source,
                event_type,
                payload as "payload!",
                payload_schema_id::uuid as "payload_schema_id?: Uuid"
            FROM core.events
            WHERE id = $1::uuid
            "#,
            event_id
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "load event for validation"))?;

        let schema = if let Some(schema_id) = event_row.payload_schema_id {
            self.get_schema_by_id(&schema_id).await?
        } else {
            self.get_active_schema(&event_row.source, &event_row.event_type)
                .await?
        };

        let schema_id_for_cache = event_row
            .payload_schema_id
            .unwrap_or_else(|| *schema.id.as_uuid());

        if let Some(cached) = self
            .fetch_cached_validation(event_id, &schema_id_for_cache)
            .await?
        {
            return Ok(cached);
        }

        let result = Self::run_json_validation(&schema.schema_content, &event_row.payload);

        self.store_validation_cache(event_id, &schema_id_for_cache, &result)
            .await?;

        Ok(result)
    }

    fn run_json_validation(schema_content: &JsonValue, payload: &JsonValue) -> ValidationResult {
        match jsonschema::validator_for(schema_content) {
            Ok(validator) => {
                let validation_errors: Vec<ValidationError> = validator
                    .iter_errors(payload)
                    .map(|e| ValidationError {
                        path: e.instance_path().to_string(),
                        message: e.to_string(),
                        error_type: "schema_validation".to_string(),
                    })
                    .collect();

                if validation_errors.is_empty() {
                    ValidationResult {
                        is_valid: true,
                        errors: vec![],
                        warnings: vec![],
                    }
                } else {
                    ValidationResult {
                        is_valid: false,
                        errors: validation_errors,
                        warnings: vec![],
                    }
                }
            }
            Err(e) => ValidationResult {
                is_valid: false,
                errors: vec![ValidationError {
                    path: String::new(),
                    message: format!("Invalid schema: {e}"),
                    error_type: "schema_error".to_string(),
                }],
                warnings: vec![],
            },
        }
    }

    async fn fetch_cached_validation(
        &self,
        event_id: &Uuid,
        schema_id: &Uuid,
    ) -> DbResult<Option<ValidationResult>> {
        let row = sqlx::query!(
            r#"
            SELECT
                is_valid,
                validation_errors as "validation_errors?: JsonValue"
            FROM sinex_schemas.validation_cache
            WHERE event_id = $1::uuid
              AND schema_id = $2::uuid
            "#,
            event_id,
            schema_id
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "fetch validation cache entry"))?;

        Ok(row.map(|row| ValidationResult {
            is_valid: row.is_valid,
            errors: row
                .validation_errors
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default(),
            warnings: Vec::new(),
        }))
    }

    async fn store_validation_cache(
        &self,
        event_id: &Uuid,
        schema_id: &Uuid,
        result: &ValidationResult,
    ) -> DbResult<()> {
        let errors_json = if result.errors.is_empty() {
            None
        } else {
            Some(serde_json::to_value(&result.errors).map_err(|e| {
                SinexError::serialization(format!("serialize validation errors: {e}"))
            })?)
        };

        if let Err(e) = sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.validation_cache (
                event_id, schema_id, is_valid, validation_errors
            ) VALUES (
                $1::uuid, $2::uuid, $3, $4
            )
            ON CONFLICT (event_id, schema_id) DO UPDATE
            SET is_valid = EXCLUDED.is_valid,
                validation_errors = EXCLUDED.validation_errors,
                validated_at = NOW()
            "#,
            event_id,
            schema_id,
            result.is_valid,
            errors_json
        )
        .execute(self.pool)
        .await
        {
            return Err(db_error(e, "upsert validation cache entry"));
        }

        Ok(())
    }

    /// Deprecate a schema
    pub async fn deprecate_schema(&self, schema_id: &Uuid) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.event_payload_schemas
            SET is_active = false, updated_at = NOW()
            WHERE id = $1::uuid
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
        schema_id: &Uuid,
    ) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE core.events 
            SET payload_schema_id = $1::uuid
            WHERE id = $2::uuid
            "#,
            schema_id,
            event_id.to_uuid()
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "set event schema"))?;

        Ok(())
    }

    async fn load_active_schema_map(
        &self,
    ) -> DbResult<std::collections::HashMap<(String, String, String), SchemaRecord>> {
        let rows = sqlx::query!(
            r#"
            SELECT 
                id as "id!: Uuid",
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

        let mut map = std::collections::HashMap::with_capacity(rows.len());
        for row in rows {
            map.insert(
                (row.source, row.event_type, row.schema_version),
                SchemaRecord {
                    id: row.id,
                    content_hash: Some(row.content_hash),
                },
            );
        }

        Ok(map)
    }

    async fn update_existing_schema(
        &self,
        schema_id: Uuid,
        candidate: &SchemaCandidate,
    ) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.event_payload_schemas
            SET schema_content = $1,
                content_hash = $2,
                updated_at = NOW()
            WHERE id = $3::uuid
            "#,
            &candidate.schema.schema_content,
            candidate.content_hash.as_str(),
            schema_id
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update schema content"))?;

        Ok(())
    }

    async fn insert_new_schema(&self, candidate: &SchemaCandidate) -> DbResult<Uuid> {
        let id = Uuid::now_v7();

        sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas (
                id, source, event_type, schema_version, schema_content,
                content_hash, is_active, updated_at
            ) VALUES (
                $1::uuid, $2, $3, $4, $5, $6, true, NOW()
            )
            "#,
            id,
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

async fn set_repeatable_read(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> DbResult<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error(e, "set repeatable read isolation"))?;
    Ok(())
}

/// Aggregated statistics about registered event payload schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaStatistics {
    /// Total number of schema records in the database
    pub total_schemas: u64,
    /// Number of currently active schemas
    pub active_schemas: u64,
    /// Number of unique event sources with schemas
    pub unique_sources: u64,
    /// Number of unique event types with schemas
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
    ) -> Result<Self, sinex_primitives::error::SinexError> {
        // Validate schema version format (must be semver X.Y.Z)
        SchemaVersion::new(&schema_version)
            .validate()
            .map_err(|e| {
                SinexError::validation(format!("Invalid schema version '{schema_version}': {e}"))
            })?;

        let schema = NewEventSchema {
            source: EventSource::new(source)?,
            event_type: EventType::new(event_type)?,
            schema_version,
            schema_content,
        };
        let content_hash = schema.calculate_content_hash()?;
        Ok(Self {
            schema,
            content_hash,
        })
    }

    fn key(&self) -> (String, String, String) {
        (
            self.schema.source.to_string(),
            self.schema.event_type.to_string(),
            self.schema.schema_version.clone(),
        )
    }
}

#[derive(Debug, Clone)]
struct SchemaRecord {
    id: Uuid,
    content_hash: Option<String>,
}
