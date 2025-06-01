use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::{event_types, sources, RawEvent};

/// Agent status enum
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentStatus {
    Running,
    Degraded,
    Erroring,
}

/// Agent heartbeat payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHeartbeat {
    pub agent_name: String,
    pub status: AgentStatus,
    pub uptime_seconds: u64,
    pub events_processed_session: u64,
    pub dlq_size: u64,
    pub version: String,
}

/// Agent error severity
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErrorSeverity {
    Warning,
    Error,
    Critical,
}

/// Agent error payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentError {
    pub agent_name: String,
    pub error_message: String,
    pub error_context: String,
    pub severity: ErrorSeverity,
    pub original_event_id_if_related: Option<String>,
}

/// DLQ event written payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEventWritten {
    pub agent_name: String,
    pub failed_event_source: String,
    pub failed_event_type: String,
    pub dlq_file_path: String,
    pub failure_reason: String,
}

/// Helper functions to create agent events
pub fn create_heartbeat_event(heartbeat: AgentHeartbeat) -> RawEvent {
    RawEvent::new(
        sources::SINEX,
        event_types::event_types::sinex::AGENT_HEARTBEAT,
        serde_json::to_value(heartbeat).unwrap(),
    )
}

pub fn create_error_event(error: AgentError) -> RawEvent {
    RawEvent::new(
        sources::SINEX,
        event_types::event_types::sinex::AGENT_ERROR,
        serde_json::to_value(error).unwrap(),
    )
}

pub fn create_dlq_event(dlq: DlqEventWritten) -> RawEvent {
    RawEvent::new(
        sources::SINEX,
        event_types::event_types::sinex::AGENT_DLQ_EVENT_WRITTEN,
        serde_json::to_value(dlq).unwrap(),
    )
}

/// Agent metrics tracker
pub struct AgentMetrics {
    pub start_time: DateTime<Utc>,
    pub events_processed: u64,
    pub dlq_count: u64,
    agent_name: String,
    version: String,
}

impl AgentMetrics {
    pub fn new(agent_name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            start_time: Utc::now(),
            events_processed: 0,
            dlq_count: 0,
            agent_name: agent_name.into(),
            version: version.into(),
        }
    }

    pub fn increment_processed(&mut self) {
        self.events_processed += 1;
    }

    pub fn increment_dlq(&mut self) {
        self.dlq_count += 1;
    }

    pub fn uptime_seconds(&self) -> u64 {
        (Utc::now() - self.start_time).num_seconds() as u64
    }

    pub fn create_heartbeat(&self, status: AgentStatus) -> AgentHeartbeat {
        AgentHeartbeat {
            agent_name: self.agent_name.clone(),
            status,
            uptime_seconds: self.uptime_seconds(),
            events_processed_session: self.events_processed,
            dlq_size: self.dlq_count,
            version: self.version.clone(),
        }
    }
}