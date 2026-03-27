use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::Timestamp;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Checkpoint {
    None,
    External {
        position: serde_json::Value,
        description: String,
    },
    Internal {
        event_id: Uuid,
        message_count: u64,
    },
    Stream {
        message_id: String,
        event_id: Option<Uuid>,
    },
    Timestamp {
        timestamp: Timestamp,
        metadata: Option<serde_json::Value>,
    },
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self::None
    }
}

impl Checkpoint {
    pub fn external(position: serde_json::Value, description: impl Into<String>) -> Self {
        Self::External {
            position,
            description: description.into(),
        }
    }

    #[must_use]
    pub fn internal(event_id: Uuid, message_count: u64) -> Self {
        Self::Internal {
            event_id,
            message_count,
        }
    }

    pub fn stream(message_id: impl Into<String>, event_id: Option<Uuid>) -> Self {
        Self::Stream {
            message_id: message_id.into(),
            event_id,
        }
    }

    #[must_use]
    pub fn timestamp(timestamp: Timestamp, metadata: Option<serde_json::Value>) -> Self {
        Self::Timestamp {
            timestamp,
            metadata,
        }
    }

    #[must_use]
    pub fn description(&self) -> String {
        match self {
            Checkpoint::None => "start".to_string(),
            Checkpoint::External { description, .. } => description.clone(),
            Checkpoint::Internal {
                event_id,
                message_count,
            } => format!("event {event_id} (#{message_count})"),
            Checkpoint::Stream {
                message_id,
                event_id,
            } => {
                if let Some(event_id) = event_id {
                    format!("stream {message_id} (event {event_id})")
                } else {
                    format!("stream {message_id}")
                }
            }
            Checkpoint::Timestamp { timestamp, .. } => {
                format!("timestamp {}", timestamp.format_rfc3339())
            }
        }
    }
}
