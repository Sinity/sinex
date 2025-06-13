use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use sqlx::FromRow;

/// Raw event from the events table
/// 
/// NOTE: This struct uses ULID directly. When using with SQLX queries,
/// use type overrides like: `id::uuid as "id: _"` for proper type inference
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct RawEvent {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: DateTime<Utc>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub host: String,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub payload: serde_json::Value,
}

impl RawEvent {
    /// Extract ingestion timestamp from ULID (convenience method)
    pub fn ts_ingest_from_ulid(&self) -> DateTime<Utc> {
        self.id.timestamp()
    }
}

/// Event payload schema
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EventPayloadSchema {
    pub id: Ulid,
    pub event_source: String,
    pub event_type: String,
    pub schema_version: String,
    pub json_schema_definition: serde_json::Value,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
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
    pub config_template_json: Option<serde_json::Value>,
    pub produces_event_types: Option<serde_json::Value>,
    pub subscribes_to_event_types: Option<serde_json::Value>,
    pub required_capabilities: Option<serde_json::Value>,
    pub llm_dependencies: Option<serde_json::Value>,
    pub repo_url: Option<String>,
    pub last_heartbeat_ts: Option<DateTime<Utc>>,
    pub last_error_ts: Option<DateTime<Utc>>,
    pub last_error_summary: Option<String>,
    pub registered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Promotion queue item
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PromotionQueueItem {
    pub queue_id: Ulid,
    pub raw_event_id: Ulid,
    pub target_agent_name: String,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub last_attempt_ts: Option<DateTime<Utc>>,
    pub next_retry_ts: Option<DateTime<Utc>>,
    pub error_message_last: Option<String>,
    pub created_at: DateTime<Utc>,
    pub processing_worker_id: Option<String>,
}

/// Status values for promotion queue
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueStatus {
    Pending,
    Processing,
    FailedRetryable,
}

impl QueueStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::FailedRetryable => "failed_retryable",
        }
    }
}

impl From<String> for QueueStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "pending" => Self::Pending,
            "processing" => Self::Processing,
            "failed_retryable" => Self::FailedRetryable,
            _ => Self::Pending,
        }
    }
}

impl From<&str> for QueueStatus {
    fn from(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "processing" => Self::Processing,
            "failed_retryable" => Self::FailedRetryable,
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
    pub failed_at: DateTime<Utc>,
    pub last_retry_at: Option<DateTime<Utc>>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub original_event_payload: serde_json::Value,
    pub additional_metadata: Option<serde_json::Value>,
    pub resolved_at: Option<DateTime<Utc>>,
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

