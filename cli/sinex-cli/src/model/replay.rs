use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Replay plan information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayPlan {
    /// Plan ID (ULID)
    pub id: String,
    /// Event count estimate
    pub event_count: u64,
    /// Query specification
    pub query: String,
    /// Created at
    pub created_at: DateTime<Utc>,
}

/// Replay operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayOperation {
    /// Operation ID (ULID)
    pub id: String,
    /// Plan ID
    pub plan_id: String,
    /// Operation status
    pub status: ReplayStatus,
    /// Progress (0.0 - 1.0)
    pub progress: f64,
    /// Events processed so far
    pub events_processed: u64,
    /// Total events
    pub total_events: u64,
    /// Created at
    pub created_at: DateTime<Utc>,
    /// Started at (if started)
    pub started_at: Option<DateTime<Utc>>,
    /// Completed at (if completed)
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message (if failed)
    pub error: Option<String>,
}

/// Replay operation status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplayStatus {
    /// Operation is planned but not started
    Planned,
    /// Operation is approved for execution
    Approved,
    /// Operation is running
    Running,
    /// Operation completed successfully
    Completed,
    /// Operation was cancelled
    Cancelled,
    /// Operation failed
    Failed,
}

impl std::fmt::Display for ReplayStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Planned => write!(f, "planned"),
            Self::Approved => write!(f, "approved"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Dead letter queue information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqInfo {
    /// Subject name
    pub subject: String,
    /// Message count
    pub message_count: u64,
    /// First message timestamp (if any)
    pub first_message_at: Option<DateTime<Utc>>,
    /// Last message timestamp (if any)
    pub last_message_at: Option<DateTime<Utc>>,
}

/// DLQ message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessage {
    /// Message ID
    pub id: String,
    /// Subject
    pub subject: String,
    /// Payload (JSON)
    pub payload: serde_json::Value,
    /// Received at
    pub received_at: DateTime<Utc>,
    /// Error reason (if available)
    pub error: Option<String>,
}
