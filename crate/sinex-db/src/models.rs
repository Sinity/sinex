use crate::{JsonValue, OptionalTimestamp, Timestamp};
use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use sqlx::FromRow;

/// Raw event structure
/// 
/// This is the canonical event structure used throughout the system.
/// NOTE: This struct uses ULID directly. When using with SQLX queries,
/// use type overrides like: `id::uuid as "id: _"` for proper type inference
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct RawEvent {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: Timestamp,
    pub ts_orig: OptionalTimestamp,
    pub host: String,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub payload: JsonValue,
}

impl RawEvent {
    /// Extract ingestion timestamp from ULID (convenience method)
    pub fn ts_ingest_from_ulid(&self) -> Timestamp {
        self.id.timestamp()
    }
}

/// Builder for creating RawEvent instances
pub struct RawEventBuilder {
    source: String,
    event_type: String,
    payload: JsonValue,
    ts_orig: OptionalTimestamp,
    host: Option<String>,
    ingestor_version: Option<String>,
    payload_schema_id: Option<Ulid>,
}

impl RawEventBuilder {
    pub fn new(source: impl Into<String>, event_type: impl Into<String>, payload: JsonValue) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: None,
            host: None,
            ingestor_version: None,
            payload_schema_id: None,
        }
    }

    pub fn with_orig_timestamp(mut self, ts: Timestamp) -> Self {
        self.ts_orig = Some(ts);
        self
    }

    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn with_ingestor_version(mut self, version: impl Into<String>) -> Self {
        self.ingestor_version = Some(version.into());
        self
    }

    pub fn with_payload_schema_id(mut self, id: Ulid) -> Self {
        self.payload_schema_id = Some(id);
        self
    }

    pub fn build(self) -> RawEvent {
        let id = Ulid::new();
        let hostname = self.host.unwrap_or_else(|| {
            gethostname::gethostname()
                .to_string_lossy()
                .to_string()
        });

        RawEvent {
            id,
            source: self.source,
            event_type: self.event_type,
            ts_ingest: chrono::Utc::now(),
            ts_orig: self.ts_orig,
            host: hostname,
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id,
            payload: self.payload,
        }
    }
}

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
    pub processed_at: OptionalTimestamp,  // New: TTL policy tracking
    pub failure_reason: Option<String>,       // New: Detailed failure information
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
    Succeeded,       // New: Successfully processed
    Failed,          // New: Permanently failed
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
    pub status: String,  // "running", "degraded", "erroring"
    pub uptime_seconds: u64,
    pub events_processed_session: u64,
    pub dlq_size: u64,
    pub version: String,
}

