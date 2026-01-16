use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_core::types::Ulid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Checkpoint {
    None,
    External {
        position: serde_json::Value,
        description: String,
    },
    Internal {
        event_id: Ulid,
        message_count: u64,
    },
    Stream {
        message_id: String,
        event_id: Option<Ulid>,
    },
    Timestamp {
        timestamp: DateTime<Utc>,
        metadata: Option<serde_json::Value>,
    },
}

impl Checkpoint {
    pub fn external(position: serde_json::Value, description: impl Into<String>) -> Self {
        Self::External {
            position,
            description: description.into(),
        }
    }

    pub fn internal(event_id: Ulid, message_count: u64) -> Self {
        Self::Internal {
            event_id,
            message_count,
        }
    }

    pub fn stream(message_id: impl Into<String>, event_id: Option<Ulid>) -> Self {
        Self::Stream {
            message_id: message_id.into(),
            event_id,
        }
    }

    pub fn timestamp(timestamp: DateTime<Utc>, metadata: Option<serde_json::Value>) -> Self {
        Self::Timestamp {
            timestamp,
            metadata,
        }
    }

    pub fn description(&self) -> String {
        match self {
            Checkpoint::None => "start".to_string(),
            Checkpoint::External { description, .. } => description.clone(),
            Checkpoint::Internal {
                event_id,
                message_count,
            } => format!("event {} (#{message_count})", event_id),
            Checkpoint::Stream {
                message_id,
                event_id,
            } => {
                if let Some(event_id) = event_id {
                    format!("stream {} (event {})", message_id, event_id)
                } else {
                    format!("stream {}", message_id)
                }
            }
            Checkpoint::Timestamp { timestamp, .. } => {
                format!("timestamp {}", timestamp.format("%Y-%m-%d %H:%M:%S UTC"))
            }
        }
    }
}
