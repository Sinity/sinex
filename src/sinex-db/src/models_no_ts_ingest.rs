use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use sqlx::FromRow;
use uuid::Uuid;

/// Raw event from the events table
/// Note: ts_ingest removed as ULID contains ingestion timestamp
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct RawEvent {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub ts_orig: Option<DateTime<Utc>>,
    pub host: String,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Uuid>,
    pub payload: serde_json::Value,
}

impl RawEvent {
    /// Convert database UUID to ULID for application layer
    pub fn id_as_ulid(&self) -> Result<Ulid, sinex_ulid::Error> {
        Ulid::from_bytes(*self.id.as_bytes())
    }
    
    /// Extract ingestion timestamp from ULID
    pub fn ts_ingest(&self) -> Result<DateTime<Utc>, sinex_ulid::Error> {
        let ulid = self.id_as_ulid()?;
        // Extract timestamp from ULID (first 48 bits contain milliseconds since epoch)
        let timestamp_ms = ulid.timestamp_ms();
        Ok(DateTime::from_timestamp_millis(timestamp_ms as i64)
            .expect("valid timestamp from ULID"))
    }
    
    pub fn payload_schema_id_as_ulid(&self) -> Option<Result<Ulid, sinex_ulid::Error>> {
        self.payload_schema_id.map(|uuid| Ulid::from_bytes(*uuid.as_bytes()))
    }
}

// Rest of the models remain the same...
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EventPayloadSchema {
    pub id: Uuid,
    pub event_source: String,
    pub event_type: String,
    pub schema_version: String,
    pub json_schema_definition: serde_json::Value,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub is_active: bool,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PromotionQueueItem {
    pub queue_id: Uuid,
    pub raw_event_id: Uuid,
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

// Enums remain the same
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHeartbeat {
    pub agent_name: String,
    pub timestamp_iso: String,
    pub status_reported: String,
    pub metrics_snapshot: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_status_conversion() {
        assert_eq!(QueueStatus::from("pending"), QueueStatus::Pending);
        assert_eq!(QueueStatus::from("processing"), QueueStatus::Processing);
        assert_eq!(QueueStatus::from("failed_retryable"), QueueStatus::FailedRetryable);
        assert_eq!(QueueStatus::from("unknown"), QueueStatus::Pending); // Default
        
        assert_eq!(QueueStatus::Pending.as_str(), "pending");
        assert_eq!(QueueStatus::Processing.as_str(), "processing");
        assert_eq!(QueueStatus::FailedRetryable.as_str(), "failed_retryable");
    }

    #[test]
    fn test_agent_status_conversion() {
        assert_eq!(AgentStatus::from("running"), AgentStatus::Running);
        assert_eq!(AgentStatus::from("stopped"), AgentStatus::Stopped);
        assert_eq!(AgentStatus::from("error_state"), AgentStatus::ErrorState);
        assert_eq!(AgentStatus::from("disabled_by_user"), AgentStatus::DisabledByUser);
        assert_eq!(AgentStatus::from("pending_registration"), AgentStatus::PendingRegistration);
        assert_eq!(AgentStatus::from("degraded"), AgentStatus::Degraded);
        assert_eq!(AgentStatus::from("whatever"), AgentStatus::Unknown);
        
        assert_eq!(AgentStatus::Running.as_str(), "running");
        assert_eq!(AgentStatus::ErrorState.as_str(), "error_state");
    }

    #[test]
    fn test_queue_status_serde() {
        let status = QueueStatus::Processing;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"processing\"");
        
        let deserialized: QueueStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, QueueStatus::Processing);
    }

    #[test]
    fn test_agent_status_serde() {
        let status = AgentStatus::ErrorState;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"error_state\"");
        
        let deserialized: AgentStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, AgentStatus::ErrorState);
    }

    #[test]
    fn test_agent_heartbeat_serialization() {
        let heartbeat = AgentHeartbeat {
            agent_name: "TestAgent".to_string(),
            timestamp_iso: "2024-01-01T00:00:00Z".to_string(),
            status_reported: "healthy".to_string(),
            metrics_snapshot: Some(serde_json::json!({
                "processed": 100,
                "errors": 0
            })),
        };
        
        let json = serde_json::to_string(&heartbeat).unwrap();
        let deserialized: AgentHeartbeat = serde_json::from_str(&json).unwrap();
        
        assert_eq!(deserialized.agent_name, heartbeat.agent_name);
        assert_eq!(deserialized.timestamp_iso, heartbeat.timestamp_iso);
        assert_eq!(deserialized.status_reported, heartbeat.status_reported);
        assert_eq!(deserialized.metrics_snapshot, heartbeat.metrics_snapshot);
    }
    
    #[test]
    fn test_raw_event_ts_ingest_extraction() {
        // Create a test ULID with known timestamp
        let ulid = Ulid::new();
        let expected_ts = ulid.timestamp_ms();
        
        let event = RawEvent {
            id: Uuid::from_bytes(*ulid.as_bytes()),
            source: "test".to_string(),
            event_type: "test".to_string(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: serde_json::json!({}),
        };
        
        let extracted_ts = event.ts_ingest().unwrap();
        assert_eq!(extracted_ts.timestamp_millis() as u64, expected_ts);
    }
}