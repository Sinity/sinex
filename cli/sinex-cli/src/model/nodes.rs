use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::NodeRole;

/// Node information returned from the gateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID (ULID)
    pub id: String,
    /// Node name
    pub name: String,
    /// Node role
    pub role: NodeRole,
    /// Node status
    pub status: NodeStatus,
    /// Last heartbeat timestamp
    pub last_heartbeat: DateTime<Utc>,
    /// Leader status (if applicable)
    pub is_leader: Option<bool>,
}

/// Node status
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    /// Node is active and processing
    Active,
    /// Node is draining (not accepting new work)
    Draining,
    /// Node is inactive
    Inactive,
    /// Node is in error state
    Error,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Draining => write!(f, "draining"),
            Self::Inactive => write!(f, "inactive"),
            Self::Error => write!(f, "error"),
        }
    }
}

/// Node health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    /// Node ID
    pub id: String,
    /// Whether the node is healthy
    pub healthy: bool,
    /// Health check details
    pub details: Option<String>,
    /// Last check timestamp
    pub checked_at: DateTime<Utc>,
}
