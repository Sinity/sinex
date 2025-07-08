use crate::{JsonValue, OptionalTimestamp, Timestamp};
use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use sqlx::FromRow;

// RawEvent and RawEventBuilder are now re-exported from sinex-core
// This eliminates type conflicts and provides a single source of truth

/// Event payload schema
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

/// Agent manifest
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AgentManifest {
    pub agent_name: String,
    pub description: Option<String>,
    pub version: String,
    pub status: String,
    pub agent_type: String,
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

/// Work queue item (formerly promotion queue)
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WorkQueueItem {
    pub queue_id: Ulid,
    pub raw_event_id: Ulid,
    pub target_agent_name: String,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub last_attempt_ts: OptionalTimestamp,
    pub next_retry_ts: OptionalTimestamp,
    pub error_message_last: Option<String>,
    pub created_at: Timestamp,
    pub processing_worker_id: Option<String>,
    pub processed_at: OptionalTimestamp, // New: TTL policy tracking
    pub failure_reason: Option<String>,  // New: Detailed failure information
}

/// Legacy alias for backward compatibility during transition
#[deprecated(note = "Use WorkQueueItem instead")]
pub type PromotionQueueItem = WorkQueueItem;

/// Status values for work queue
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueStatus {
    Pending,
    Processing,
    Succeeded, // New: Successfully processed
    Failed,    // New: Permanently failed
    FailedRetryable,
}

impl QueueStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::FailedRetryable => "failed_retryable",
        }
    }
}

impl From<String> for QueueStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "pending" => Self::Pending,
            "processing" => Self::Processing,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "failed_retryable" => Self::FailedRetryable,
            "completed" => Self::Succeeded, // Map legacy to succeeded
            _ => Self::Pending,
        }
    }
}

impl From<&str> for QueueStatus {
    fn from(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "processing" => Self::Processing,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "failed_retryable" => Self::FailedRetryable,
            "completed" => Self::Succeeded, // Map legacy to succeeded
            _ => Self::Pending,
        }
    }
}

/// Dead Letter Queue (DLQ) event entry
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DlqEvent {
    pub dlq_id: Ulid,
    pub failed_event_id: Ulid,
    pub agent_name: String,
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

/// Event for agent heartbeat
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHeartbeat {
    pub agent_name: String,
    pub status: String, // "running", "degraded", "erroring"
    pub uptime_seconds: u64,
    pub events_processed_session: u64,
    pub dlq_size: u64,
    pub version: String,
}

// ============================================================================
// Artifacts API Models
// ============================================================================

/// Core artifact representing conceptual documents/items
/// Maps to core.artifacts table
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Artifact {
    #[sqlx(rename = "id")]
    pub artifact_id: Ulid,
    #[sqlx(rename = "type")]
    pub artifact_type: String,
    pub title: String,
    pub source_url: Option<String>,
    pub original_path: Option<String>,
    pub mime_type: Option<String>,
    pub size_bytes: Option<i64>,
    pub checksum: Option<String>,
    pub metadata: JsonValue,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub deleted_at: OptionalTimestamp,
    pub created_from_event_id: Option<Ulid>,
    pub blob_id: Option<Ulid>,
}

/// Artifact content version
/// Maps to core.artifact_contents table
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ArtifactContent {
    #[sqlx(rename = "id")]
    pub content_id: Ulid,
    pub artifact_id: Ulid,
    pub version: i32,
    #[sqlx(rename = "content")]
    pub content_text: String,
    #[sqlx(rename = "content_type")]
    pub content_format: String,
    pub extracted_text: Option<String>,
    pub word_count: Option<i32>,
    pub char_count: Option<i32>,
    pub metadata: JsonValue,
    pub created_at: Timestamp,
    pub created_from_event_id: Option<Ulid>,
}

/// Input for creating a new artifact
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateArtifactInput {
    pub artifact_type: String,
    pub title: String,
    pub source_url: Option<String>,
    pub original_path: Option<String>,
    pub mime_type: Option<String>,
    pub size_bytes: Option<i64>,
    pub checksum: Option<String>,
    pub metadata: Option<JsonValue>,
    pub created_from_event_id: Option<Ulid>,
    pub blob_id: Option<Ulid>,
}

/// Input for creating artifact content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateArtifactContentInput {
    pub artifact_id: Ulid,
    pub version: Option<i32>,
    pub content_text: String,
    pub content_format: String,
    pub extracted_text: Option<String>,
    pub metadata: Option<JsonValue>,
    pub created_from_event_id: Option<Ulid>,
}

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
