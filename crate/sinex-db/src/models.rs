use crate::{JsonValue, OptionalTimestamp, Timestamp};
use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use sqlx::FromRow;

// RawEvent is now re-exported from sinex-events
// This eliminates type conflicts and provides a single source of truth

/// Event payload schema registry entry
/// 
/// # Technical Implementation Module: Event Schema Registry
/// 
/// **Maturity Level**: L4 - Implemented  
/// **Implementation**: 98% (Comprehensive implementation)  
/// **Dependencies**: PostgreSQL, JSONB support, ULID generation, Git repository  
/// **Blocks**: Event validation, schema evolution, type safety, code generation  
/// 
/// ## Overview
/// 
/// Central registry for JSON Schema definitions describing event payload structures.
/// Enables data integrity, documentation, interoperability, and schema evolution management.
/// 
/// ## Database Schema
/// 
/// Maps to `sinex_schemas.event_payload_schemas` table:
/// 
/// ```sql
/// CREATE TABLE sinex_schemas.event_payload_schemas (
///     id                      ULID PRIMARY KEY DEFAULT gen_ulid(),
///     event_source            TEXT NOT NULL,
///     event_type              TEXT NOT NULL,
///     schema_version          TEXT NOT NULL, -- e.g., "v1.0", "v2.0"
///     json_schema_definition  JSONB NOT NULL, -- The actual JSON Schema
///     description             TEXT,
///     created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
///     is_active               BOOLEAN NOT NULL DEFAULT TRUE,
///     UNIQUE (event_source, event_type, schema_version)
/// );
/// ```
/// 
/// ## GitOps Management Strategy
/// 
/// 1. **Source of Truth**: JSON Schema files in `/schemas` directory
/// 2. **CI/CD Pipeline**: Validates and deploys schema changes
/// 3. **Backward Compatibility**: Automatic checking on updates
/// 4. **Schema Evolution**: Version-based with activation flags
/// 
/// ## Usage Pattern
/// 
/// ```rust
/// // Ingestors lookup schema ID during initialization
/// let schema = EventPayloadSchema::find_active(
///     &pool,
///     "filesystem",
///     "file_created"
/// ).await?;
/// 
/// // Attach schema ID to events
/// event.payload_schema_id = Some(schema.id);
/// ```
/// 
/// ## Schema Change Eventification
/// 
/// Changes to schemas are automatically logged as events via database trigger,
/// creating `sinex.schema.definition_changed` events for audit trail.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EventPayloadSchema {
    pub id: Ulid,
    pub event_source: String,
    pub event_type: String,
    pub schema_version: String,
    pub json_schema_definition: JsonValue,
    pub description: Option<String>,
    pub created_at: Timestamp,
    pub is_active: bool,
}

/// Service manifest (worker registration)
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AutomatonManifest {
    pub automaton_name: String,
    pub description: Option<String>,
    pub version: String,
    pub status: String,
    pub automaton_type: String,
    pub config_template_json: Option<JsonValue>,
    pub produces_event_types: Option<JsonValue>,
    pub subscribes_to_event_types: Option<JsonValue>,
    pub required_capabilities: Option<JsonValue>,
    pub llm_dependencies: Option<JsonValue>,
    pub repo_url: Option<String>,
    pub last_heartbeat_ts: OptionalTimestamp,
    pub last_error_ts: OptionalTimestamp,
    pub last_error_summary: Option<String>,
    pub registered_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Dead Letter Queue (DLQ) event entry
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DlqEvent {
    pub dlq_id: Ulid,
    pub failed_event_id: Ulid,
    pub automaton_name: String,
    pub source: String,
    pub event_type: String,
    pub failure_reason: String,
    pub error_category: String,
    pub retry_count: i32,
    pub failed_at: Timestamp,
    pub last_retry_at: OptionalTimestamp,
    pub next_retry_at: OptionalTimestamp,
    pub original_event_payload: JsonValue,
    pub additional_metadata: Option<JsonValue>,
    pub resolved_at: OptionalTimestamp,
    pub resolved_by: Option<String>,
}

/// Error categories for DLQ events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DlqErrorCategory {
    Retryable,
    Permanent,
    System,
    User,
}

impl DlqErrorCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::System => "system",
            Self::User => "user",
        }
    }
}

impl From<String> for DlqErrorCategory {
    fn from(s: String) -> Self {
        match s.as_str() {
            "retryable" => Self::Retryable,
            "permanent" => Self::Permanent,
            "system" => Self::System,
            "user" => Self::User,
            _ => Self::Permanent,
        }
    }
}

/// Blob record for Git-annex managed large files
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct BlobRecord {
    pub id: Ulid,
    pub annex_key: String,
    pub original_filename: String,
    pub size_bytes: i64,
    pub mime_type: Option<String>,
    pub checksum_sha256: String,
    pub checksum_blake3: Option<String>,
    pub storage_backend: String,
    pub metadata: JsonValue,
    pub created_at: Timestamp,
    pub last_verified_at: OptionalTimestamp,
    pub verification_status: Option<String>,
}

/// Source Material record for external data provenance
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct SourceMaterialRecord {
    pub blob_id: Ulid,
    pub material_type: String,
    pub source_uri: Option<String>,
    pub ingestion_time: Timestamp,
    pub file_size_bytes: Option<i64>,
    pub checksum_blake3: Option<String>,
    pub mime_type: Option<String>,
    pub encoding: Option<String>,
    pub metadata: JsonValue,
    pub content_preview: Option<String>,
    pub is_archived: bool,
    pub archive_time: OptionalTimestamp,
    pub retention_policy: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl From<&str> for DlqErrorCategory {
    fn from(s: &str) -> Self {
        match s {
            "retryable" => Self::Retryable,
            "permanent" => Self::Permanent,
            "system" => Self::System,
            "user" => Self::User,
            _ => Self::Permanent,
        }
    }
}

/// Resolution types for DLQ events  
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DlqResolutionType {
    Reprocessed,
    Manual,
    Purged,
}

impl DlqResolutionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reprocessed => "reprocessed",
            Self::Manual => "manual",
            Self::Purged => "purged",
        }
    }
}

impl From<String> for DlqResolutionType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "reprocessed" => Self::Reprocessed,
            "manual" => Self::Manual,
            "purged" => Self::Purged,
            _ => Self::Manual,
        }
    }
}

impl From<&str> for DlqResolutionType {
    fn from(s: &str) -> Self {
        match s {
            "reprocessed" => Self::Reprocessed,
            "manual" => Self::Manual,
            "purged" => Self::Purged,
            _ => Self::Manual,
        }
    }
}

/// Agent status values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Running,
    Stopped,
    ErrorState,
    DisabledByUser,
    PendingRegistration,
    Degraded,
    Unknown,
}

impl AgentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::ErrorState => "error_state",
            Self::DisabledByUser => "disabled_by_user",
            Self::PendingRegistration => "pending_registration",
            Self::Degraded => "degraded",
            Self::Unknown => "unknown",
        }
    }
}

impl From<String> for AgentStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "running" => Self::Running,
            "stopped" => Self::Stopped,
            "error_state" => Self::ErrorState,
            "disabled_by_user" => Self::DisabledByUser,
            "pending_registration" => Self::PendingRegistration,
            "degraded" => Self::Degraded,
            _ => Self::Unknown,
        }
    }
}

impl From<&str> for AgentStatus {
    fn from(s: &str) -> Self {
        match s {
            "running" => Self::Running,
            "stopped" => Self::Stopped,
            "error_state" => Self::ErrorState,
            "disabled_by_user" => Self::DisabledByUser,
            "pending_registration" => Self::PendingRegistration,
            "degraded" => Self::Degraded,
            _ => Self::Unknown,
        }
    }
}

/// Event for service heartbeat
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceHeartbeat {
    pub automaton_name: String,
    pub status: String, // "running", "degraded", "erroring"
    pub uptime_seconds: u64,
    pub events_processed_session: u64,
    pub dlq_size: u64,
    pub version: String,
}

// Legacy alias for compatibility
// Legacy type alias removed - use ServiceHeartbeat directly


// ============================================================================
// Event Annotations API Models
// ============================================================================

/// Event annotation
/// Maps to core.event_annotations table
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EventAnnotation {
    #[sqlx(rename = "id")]
    pub annotation_id: Ulid,
    pub event_id: Ulid,
    pub annotation_type: String,
    pub content: String,
    pub metadata: JsonValue,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub created_by: String,
}

/// Input for creating an annotation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAnnotationInput {
    pub event_id: Ulid,
    pub annotation_type: String,
    pub content: String,
    pub metadata: Option<JsonValue>,
    pub created_by: String,
}

// ============================================================================
// Knowledge Graph API Models
// ============================================================================

/// Knowledge graph entity
/// Maps to core.entities table
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Entity {
    #[sqlx(rename = "id")]
    pub entity_id: Ulid,
    #[sqlx(rename = "type")]
    pub entity_type: String,
    pub name: String,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub description: Option<String>,
    pub metadata: JsonValue,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub merged_into_id: Option<Ulid>,
}

/// Relationship between entities
/// Maps to core.entity_relations table
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EntityRelation {
    #[sqlx(rename = "id")]
    pub relation_id: Ulid,
    pub from_entity_id: Ulid,
    pub to_entity_id: Ulid,
    pub relation_type: String,
    pub strength: Option<f64>,
    pub metadata: JsonValue,
    pub valid_from: Timestamp,
    pub valid_until: OptionalTimestamp,
    pub created_at: Timestamp,
    pub created_from_event_id: Option<Ulid>,
}

/// Input for creating an entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEntityInput {
    pub entity_type: String,
    pub name: String,
    pub canonical_name: Option<String>,
    pub aliases: Option<Vec<String>>,
    pub description: Option<String>,
    pub metadata: Option<JsonValue>,
}

/// Input for creating a relation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRelationInput {
    pub from_entity_id: Ulid,
    pub to_entity_id: Ulid,
    pub relation_type: String,
    pub strength: Option<f64>,
    pub metadata: Option<JsonValue>,
    pub valid_from: OptionalTimestamp,
    pub valid_until: OptionalTimestamp,
    pub created_from_event_id: Option<Ulid>,
}
